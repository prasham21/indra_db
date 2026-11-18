"""Offline import and ranking-generation builder for evidence ranking.

Run ``python -m indra_db_service.evidence_ranking_build --help``.

Pipeline (each step is resumable and idempotent):

1. ``init-schema``           create the application-owned ``annotation`` schema.
2. ``import-provenance``     stream a provenance SQLite (schema 2 candidate or
                             runtime/validated serving file) into PostgreSQL.
   ``import-correctness``    stream a correctness SQLite (Haohang's
                             ``llm_evidence_curation`` format) or a labeled
                             JSONL fixture.
3. ``build-identity``        decode current evidence records of annotated
                             statements ONCE per readonly snapshot:
                             (mk_hash, raw id, source hash, served publication).
4. ``build-generation``      set-based priority calculation for EVERY current
                             evidence record in scope (defaults included),
                             coverage anti-join audit, ANALYZE, status ready.
5. ``publish`` / ``rollback`` switch the single active-generation pointer.

Nothing here runs an LLM. Writes go only to the ``annotation`` schema of the
database named by ``--database-url``; non-local hosts are refused unless
``--allow-remote-host`` is given explicitly by a deployment operator.
"""
import argparse
import csv
import hashlib
import io
import json
import logging
import os
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from itertools import chain

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from indra_db.client.readonly.evidence_ranking import (
    CORRECTNESS_STATUS, MATCHING_POLICY_VERSION, POLICY_VERSION,
    PROVENANCE_STATUS, SCHEMA, annotation_metadata, normalize_correctness,
    normalize_provenance, priority_for)
from indra_db_service.provenance import (connect_readonly, hash_integer,
                                         publication_key)
from indra_db_service.provenance_build import (file_digest,
                                               served_publication_evidence)

logger = logging.getLogger(__name__)

DEFAULT_IMPORT_BATCH = 50000
DEFAULT_IDENTITY_BATCH = 500
DEFAULT_GENERATION_BATCHES = 64
INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1


class BuildError(Exception):
    """A precondition or validation check failed; nothing was published."""


# --- Connection safety -------------------------------------------------------

def writer_engine(url, allow_remote=False):
    """Engine for the annotation writer, refusing shared hosts by default."""
    parsed = make_url(url)
    socket = parsed.query.get('host', '')
    local = (parsed.host in (None, '', 'localhost', '127.0.0.1', '::1')
             and (not socket or socket.startswith('/')))
    if not local and not allow_remote:
        raise BuildError('Refusing a non-local database without '
                         '--allow-remote-host')
    return create_engine(parsed)


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}'))
    annotation_metadata.create_all(engine)
    with engine.begin() as conn:
        # Lookup path for the per-record resolution in build_generation.
        conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS annotation_payload_lookup_idx ON '
            f'{SCHEMA}.annotation_payload (run_id, statement_hash, source_hash)'))
        conn.execute(text(
            f'ALTER TABLE {SCHEMA}.ranking_generation '
            f'ADD COLUMN IF NOT EXISTS batch_bounds jsonb'))


# --- Annotation adapters ------------------------------------------------------
# Each adapter yields dicts ordered deterministically, with keys:
# statement_hash, source_hash (ints), publication_key (str|None),
# raw_label (str|None), payload (dict), status ('accepted'|'conflicting'|
# 'missing_publication').

def provenance_sqlite_info(path):
    with closing(connect_readonly(path)) as db:
        metadata = dict(db.execute('SELECT key,value FROM metadata'))
    if metadata.get('schema_version') != '2':
        raise BuildError('Unsupported provenance SQLite schema')
    if not metadata.get('run_id') or not metadata.get('taxonomy_version'):
        raise BuildError('Provenance SQLite lacks run identity')
    return metadata


def iter_provenance_sqlite(path):
    """Accepted pairs plus every quarantined conflict variant, in pair order."""
    with closing(connect_readonly(path)) as db:
        rows = db.execute("""
            SELECT statement_hash, source_hash, annotation, 0 AS conflict
            FROM evidence_provenance
            UNION ALL
            SELECT statement_hash, source_hash, annotation, 1 FROM conflicts
            ORDER BY 1, 2, 4, 3""")
        for sh, eh, payload, conflict in rows:
            annotation = json.loads(payload)
            if not isinstance(annotation, dict):
                raise BuildError('Provenance annotation must be an object')
            publication = annotation.pop('_publication_key', None) or None
            if conflict:
                status = 'conflicting'
            elif publication is None:
                status = 'missing_publication'
            else:
                status = 'accepted'
            yield {'statement_hash': hash_integer(sh),
                   'source_hash': hash_integer(eh),
                   'publication_key': publication,
                   'raw_label': annotation.get('level_1'),
                   'payload': annotation, 'status': status}


_PUBLICATION_COLUMNS = ('publication_key', 'pmid', 'pmcid', 'doi')


def iter_correctness_sqlite(path, report=None):
    """Haohang's saved judgments. Publication identity is used only if the
    file itself carries ORIGINAL classification-input publication columns;
    it is never copied from current evidence."""
    report = report if report is not None else {}
    with closing(connect_readonly(path)) as db:
        columns = [r[1] for r in db.execute(
            'PRAGMA table_info(llm_evidence_curation)')]
        if not {'statement_hash', 'source_hash', 'judgment'} <= set(columns):
            raise BuildError('Not an llm_evidence_curation correctness file')
        pub_cols = [c for c in _PUBLICATION_COLUMNS if c in columns]
        extra = [c for c in ('error_category', 'explanation', 'evidence_text')
                 if c in columns]
        select = ', '.join(['statement_hash', 'source_hash', 'judgment']
                           + extra + pub_cols)
        report.update(publication_columns=pub_cols, textless_skipped=0)
        rows = db.execute(f'SELECT {select} FROM llm_evidence_curation '
                          f'ORDER BY statement_hash, source_hash, judgment')
        names = ['statement_hash', 'source_hash', 'judgment'] + extra + pub_cols
        for values in rows:
            row = dict(zip(names, values))
            # Rule-labelled textless database rows are not LLM sentence
            # judgments and are excluded from evidence verification, exactly
            # as the existing evidence API excludes them.
            if 'evidence_text' in row and not row['evidence_text']:
                report['textless_skipped'] += 1
                continue
            publication = None
            if 'publication_key' in row:
                publication = row['publication_key'] or None
            elif pub_cols:
                publication = publication_key({
                    'pmid': row.get('pmid'),
                    'text_refs': {'PMCID': row.get('pmcid'),
                                  'DOI': row.get('doi')}})
            payload = {'verdict': row['judgment'],
                       'error_category': row.get('error_category'),
                       'explanation': row.get('explanation')}
            yield {'statement_hash': hash_integer(row['statement_hash']),
                   'source_hash': hash_integer(row['source_hash']),
                   'publication_key': publication,
                   'raw_label': row['judgment'], 'payload': payload,
                   'status': 'accepted'}


def iter_jsonl(path, kind):
    """Explicitly labeled fixture records (tests/benchmarks only)."""
    records = []
    with open(path) as handle:
        for line in handle:
            if not line.strip():
                continue
            rec = json.loads(line)
            label = rec.get('level_1' if kind == 'provenance' else 'verdict')
            payload = {k: v for k, v in rec.items()
                       if k not in ('statement_hash', 'source_hash',
                                    'publication_key')}
            records.append({
                'statement_hash': hash_integer(rec['statement_hash']),
                'source_hash': hash_integer(rec['source_hash']),
                'publication_key': rec.get('publication_key') or None,
                'raw_label': label, 'payload': payload,
                'status': 'accepted'})
    records.sort(key=lambda r: (r['statement_hash'], r['source_hash'],
                                json.dumps(r['payload'], sort_keys=True)))
    yield from records


def _normalized_label(kind, raw_label):
    if kind == 'provenance':
        return normalize_provenance(raw_label)
    return normalize_correctness(raw_label)


# --- Import ---------------------------------------------------------------

def _payload_digest(record):
    body = json.dumps([record['publication_key'], record['payload']],
                      sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(body.encode()).hexdigest()


def _batches(iterable, size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _copy_rows(conn, table_sql, columns, rows):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for row in rows:
        writer.writerow(['' if v is None else v for v in row])
    buffer.seek(0)
    raw = conn.connection.cursor()
    raw.copy_expert(f"COPY {table_sql} ({', '.join(columns)}) FROM STDIN "
                    f"WITH (FORMAT csv, NULL '')", buffer)


def import_run(engine, run_id, kind, records, source_format, source_path=None,
               source_sha256=None, publication_verifiable=True,
               synthetic=False, metadata=None, batch_size=DEFAULT_IMPORT_BATCH,
               dry_run=False, fail_after_batches=None):
    """Import immutable batches; re-running a committed batch is a no-op.

    The same run id with different source digest, kind or batch size, or a
    committed batch whose content digest changed, is rejected.
    """
    if kind not in ('provenance', 'correctness'):
        raise BuildError('kind must be provenance or correctness')
    report = dict(run_id=run_id, kind=kind, batches=0, skipped_batches=0,
                  committed_batches=0, records=0, unrecognized_labels=0,
                  by_status={})
    run_t = f'{SCHEMA}.annotation_run'
    if not dry_run:
        with engine.begin() as conn:
            existing = conn.execute(text(
                f'SELECT kind, source_sha256, batch_size, status, synthetic '
                f'FROM {run_t} WHERE run_id=:r'), r=run_id).first()
            if existing is None:
                conn.execute(text(
                    f'INSERT INTO {run_t} (run_id, kind, source_format, '
                    f'source_path, source_sha256, batch_size, '
                    f'publication_verifiable, synthetic, metadata, status) '
                    f'VALUES (:r,:k,:f,:p,:s,:b,:v,:y,:m,\'importing\')'),
                    r=run_id, k=kind, f=source_format, p=source_path,
                    s=source_sha256, b=batch_size, v=publication_verifiable,
                    y=synthetic, m=json.dumps(metadata or {}))
            elif (existing.kind, existing.source_sha256, existing.batch_size,
                  existing.synthetic) != (kind, source_sha256, batch_size,
                                          synthetic):
                raise BuildError('Run id already used for different input')
            elif existing.status == 'imported':
                report['already_imported'] = True
                return report
            committed = dict(conn.execute(text(
                'SELECT batch_no, digest FROM '
                f'{SCHEMA}.import_batch WHERE run_id=:r'), r=run_id).fetchall())
    else:
        committed = {}

    for batch_no, batch in enumerate(_batches(records, batch_size)):
        rows = []
        for rec in batch:
            label = _normalized_label(kind, rec['raw_label'])
            if label is None:
                report['unrecognized_labels'] += 1
            report['by_status'][rec['status']] = \
                report['by_status'].get(rec['status'], 0) + 1
            rows.append((run_id, kind, rec['statement_hash'],
                         rec['source_hash'], rec['publication_key'],
                         rec['raw_label'], label, rec['status'],
                         _payload_digest(rec),
                         json.dumps(rec['payload'], sort_keys=True,
                                    ensure_ascii=False, allow_nan=False),
                         batch_no))
        digest = hashlib.sha256(json.dumps(
            [r[2:10] for r in rows], ensure_ascii=False).encode()).hexdigest()
        report['batches'] += 1
        report['records'] += len(rows)
        if dry_run:
            continue
        if batch_no in committed:
            if committed[batch_no] != digest:
                raise BuildError(f'Batch {batch_no} of {run_id} changed '
                                 f'since it was committed')
            report['skipped_batches'] += 1
            continue
        if (fail_after_batches is not None
                and report['committed_batches'] >= fail_after_batches):
            raise RuntimeError('Injected failure (test only)')
        with engine.begin() as conn:
            conn.execute(text(
                'CREATE TEMP TABLE IF NOT EXISTS staging_payload '
                f'(LIKE {SCHEMA}.annotation_payload INCLUDING DEFAULTS) '
                'ON COMMIT DELETE ROWS'))
            cols = ['run_id', 'kind', 'statement_hash', 'source_hash',
                    'publication_key', 'raw_label', 'label', 'status',
                    'payload_digest', 'payload', 'batch_no']
            _copy_rows(conn, 'staging_payload', cols, rows)
            col_sql = ', '.join(cols)
            conn.execute(text(
                f'INSERT INTO {SCHEMA}.annotation_payload ({col_sql}) '
                f'SELECT {col_sql} FROM staging_payload '
                f'ON CONFLICT ON CONSTRAINT annotation_payload_identity '
                f'DO NOTHING'))
            conn.execute(text(
                f'INSERT INTO {SCHEMA}.import_batch (run_id, batch_no, '
                f'row_count, digest) VALUES (:r, :b, :n, :d)'),
                r=run_id, b=batch_no, n=len(rows), d=digest)
        report['committed_batches'] += 1

    if dry_run:
        return report
    with engine.begin() as conn:
        # Fresh statistics before the set-based conflict scan; without them
        # PostgreSQL picked an index-only scan with ~1M random heap fetches on
        # a freshly loaded run (measured, see execution log).
        conn.execute(text(f'ANALYZE {SCHEMA}.annotation_payload'))
    with engine.begin() as conn:
        stored = conn.execute(text(
            f'SELECT count(*) FROM {SCHEMA}.import_batch WHERE run_id=:r'),
            r=run_id).scalar()
        if stored != report['batches']:
            raise BuildError('Committed batches do not match the input')
        # Cross-batch conflict detection: any pair with more than one distinct
        # payload is quarantined, keeping every variant (no last-write-wins).
        conflicts = conn.execute(text(f"""
            UPDATE {SCHEMA}.annotation_payload a SET status = 'conflicting'
            FROM (SELECT statement_hash, source_hash
                  FROM {SCHEMA}.annotation_payload WHERE run_id = :r
                  GROUP BY statement_hash, source_hash
                  HAVING count(*) > 1) d
            WHERE a.run_id = :r AND a.statement_hash = d.statement_hash
              AND a.source_hash = d.source_hash
              AND a.status <> 'conflicting'"""), r=run_id).rowcount
        report['newly_conflicting_rows'] = conflicts
        summary = conn.execute(text(
            f'SELECT status, count(*) FROM {SCHEMA}.annotation_payload '
            f'WHERE run_id=:r GROUP BY status'), r=run_id).fetchall()
        report['stored_by_status'] = dict(summary)
        conn.execute(text(
            f"UPDATE {run_t} SET status='imported', imported_at=now(), "
            f"metadata = metadata || CAST(:m AS jsonb) WHERE run_id=:r"),
            r=run_id, m=json.dumps({'import_report': {
                k: v for k, v in report.items() if k != 'by_status'}}))
    return report


# --- Identity map ---------------------------------------------------------------

def _annotated_statements(conn, run_ids):
    return [r[0] for r in conn.execute(text(
        f'SELECT DISTINCT statement_hash FROM {SCHEMA}.annotation_payload '
        f'WHERE run_id = ANY(:runs) ORDER BY 1'), runs=list(run_ids))]


def identity_rows(conn, statement_hashes):
    """Current records for the statements with SERVED publication identity.

    Mirrors Query.get_statements: the API serves raw_json['evidence'][0]
    with ReadingRefLink publication refs overlaid.
    """
    from indra_db.readonly_dumping.util import clean_json_loads
    result = conn.execute(text("""
        SELECT f.mk_hash, f.id, f.raw_json, r.pmid, r.pmcid, r.doi
        FROM readonly.fast_raw_pa_link f
        LEFT JOIN readonly.reading_ref_link r ON r.rid = f.reading_id
        WHERE f.mk_hash = ANY(:hashes)"""), hashes=list(statement_hashes))
    for mk_hash, raw_id, raw, pmid, pmcid, doi in result:
        raw = bytes(raw).decode('utf-8')
        evidence = clean_json_loads(raw).get('evidence') or []
        source_hash = publication = None
        if evidence:
            try:
                source_hash = hash_integer(evidence[0].get('source_hash'))
            except ValueError:
                source_hash = None
            publication = publication_key(served_publication_evidence(
                evidence[0], {'pmid': pmid, 'pmcid': pmcid, 'doi': doi}))
        yield (mk_hash, raw_id, source_hash, publication,
               min(len(evidence), 32767))


def build_identity(engine, snapshot_label, run_ids,
                   batch_size=DEFAULT_IDENTITY_BATCH, fail_after_batches=None):
    report = dict(snapshot_label=snapshot_label, batches=0,
                  skipped_batches=0, committed_batches=0, records=0)
    with engine.connect() as conn:
        statements = _annotated_statements(conn, run_ids)
        committed = dict(conn.execute(text(
            f'SELECT batch_no, digest FROM {SCHEMA}.identity_batch '
            f'WHERE snapshot_label=:s'), s=snapshot_label).fetchall())
    report['statements'] = len(statements)
    for batch_no, hashes in enumerate(_batches(statements, batch_size)):
        report['batches'] += 1
        digest = hashlib.sha256(json.dumps(hashes).encode()).hexdigest()
        if batch_no in committed:
            if committed[batch_no] != digest:
                raise BuildError('Annotated statement set changed for this '
                                 'snapshot label; use a new label')
            report['skipped_batches'] += 1
            continue
        if (fail_after_batches is not None
                and report['committed_batches'] >= fail_after_batches):
            raise RuntimeError('Injected failure (test only)')
        with engine.begin() as conn:
            # One transaction per batch: the readonly read and the identity
            # write commit together or not at all.
            conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
            rows = [(snapshot_label,) + r for r in identity_rows(conn, hashes)]
            conn.execute(text(
                f'DELETE FROM {SCHEMA}.evidence_identity WHERE '
                f'snapshot_label=:s AND mk_hash = ANY(:h)'),
                s=snapshot_label, h=hashes)
            _copy_rows(conn, f'{SCHEMA}.evidence_identity',
                       ['snapshot_label', 'mk_hash', 'raw_record_id',
                        'source_hash', 'publication_key', 'evidence_elements'],
                       rows)
            conn.execute(text(
                f'INSERT INTO {SCHEMA}.identity_batch (snapshot_label, '
                f'batch_no, statement_count, row_count, digest) '
                f'VALUES (:s,:b,:c,:n,:d)'), s=snapshot_label, b=batch_no,
                c=len(hashes), n=len(rows), d=digest)
        report['committed_batches'] += 1
        report['records'] += len(rows)
    with engine.begin() as conn:
        # Statistics for the generation build's joins (see import_run note).
        conn.execute(text(f'ANALYZE {SCHEMA}.evidence_identity'))
    return report


# --- Generation -------------------------------------------------------------

def _priority_values_sql():
    rows = []
    for c_name, c_code in CORRECTNESS_STATUS.items():
        for p_name, p_code in PROVENANCE_STATUS.items():
            rows.append(f'({c_code},{p_code},{priority_for(c_name, p_name)})')
    return 'VALUES ' + ','.join(rows)


def _status_case(kind):
    """Per-record resolution against one run's payload rows (SQL)."""
    if kind == 'provenance':
        s = PROVENANCE_STATUS
        labels = ' '.join(f"WHEN min(a.label) = '{name}' THEN {s[name]}"
                          for name in ('primary_data', 'citation',
                                       'background', 'other'))
        return f"""CASE
            WHEN count(a.*) = 0 THEN
                CASE WHEN i.raw_record_id IS NOT NULL AND i.source_hash IS NULL
                     THEN {s['evidence_identity_unavailable']}
                     ELSE {s['absent']} END
            WHEN bool_or(a.status = 'conflicting') THEN {s['conflicting']}
            WHEN bool_or(a.status = 'missing_publication')
                 OR min(a.publication_key) IS NULL THEN {s['missing_publication']}
            WHEN i.publication_key IS NULL THEN {s['evidence_identity_unavailable']}
            WHEN min(a.publication_key) <> i.publication_key
                 THEN {s['publication_mismatch']}
            {labels}
            ELSE {s['unrecognized_label']} END"""
    s = CORRECTNESS_STATUS
    return f"""CASE
        WHEN count(a.*) = 0 THEN
            CASE WHEN i.raw_record_id IS NOT NULL AND i.source_hash IS NULL
                 THEN {s['evidence_identity_unavailable']}
                 ELSE {s['absent']} END
        WHEN bool_or(a.status = 'conflicting') THEN {s['conflicting']}
        WHEN min(a.label) = 'correct' THEN {s['verified_correct']}
        WHEN min(a.label) = 'incorrect' THEN {s['verified_incorrect']}
        WHEN min(a.label) = 'uncertain' THEN {s['verified_uncertain']}
        ELSE {s['unrecognized_verdict']} END"""


def _resolution_lateral(kind, run_param, alias):
    # Correctness follows Haohang's statement/source identity contract.
    # Provenance is paper-specific and additionally requires publication.
    publication_match = (' AND min(a.publication_key) = i.publication_key'
                         if kind == 'provenance' else '')
    accepted_id = (f"CASE WHEN count(a.*) = 1 AND bool_and(a.status='accepted')"
                   f"{publication_match}"
                   f" THEN min(a.annotation_id) END")
    return f"""LEFT JOIN LATERAL (
        SELECT {_status_case(kind)} AS code, {accepted_id} AS annotation_id
        FROM {SCHEMA}.annotation_payload a
        WHERE a.run_id = :{run_param} AND a.statement_hash = f.mk_hash
          AND a.source_hash = i.source_hash) {alias} ON true"""


def _generation_insert_sql(scope_kind, has_prov, has_corr):
    scope_join = (f'JOIN {SCHEMA}.generation_statement s ON '
                  f's.generation_id = :g AND s.mk_hash = f.mk_hash'
                  if scope_kind == 'statements' else '')
    prov = (_resolution_lateral('provenance', 'prov_run', 'prov') if has_prov
            else '')
    corr = (_resolution_lateral('correctness', 'corr_run', 'corr')
            if has_corr else '')
    p_code = 'prov.code' if has_prov else str(PROVENANCE_STATUS['absent'])
    c_code = 'corr.code' if has_corr else str(CORRECTNESS_STATUS['absent'])
    p_ann = 'prov.annotation_id' if has_prov else 'NULL::bigint'
    c_ann = 'corr.annotation_id' if has_corr else 'NULL::bigint'
    return f"""
        INSERT INTO {SCHEMA}.evidence_priority (generation_id, mk_hash,
            raw_record_id, priority, correctness_status, provenance_status,
            provenance_annotation_id, correctness_annotation_id)
        SELECT :g, f.mk_hash, f.id, pm.prio, {c_code}, {p_code}, {p_ann}, {c_ann}
        FROM readonly.fast_raw_pa_link f
        {scope_join}
        LEFT JOIN {SCHEMA}.evidence_identity i
          ON i.snapshot_label = :snap AND i.mk_hash = f.mk_hash
         AND i.raw_record_id = f.id
        {prov}
        {corr}
        JOIN ({_priority_values_sql()}) AS pm(c, p, prio)
          ON pm.c = {c_code} AND pm.p = {p_code}
        WHERE f.mk_hash BETWEEN :lo AND :hi"""


def hash_ranges(count):
    """Equal-width ranges over the signed 64-bit space (fallback only).

    INDRA statement hashes occupy only about +-2**55, so equal-width ranges
    put almost everything into the middle two batches; see quantile_ranges.
    """
    span = (INT64_MAX - INT64_MIN + 1) // count
    ranges = []
    for k in range(count):
        lo = INT64_MIN + k * span
        hi = INT64_MAX if k == count - 1 else lo + span - 1
        ranges.append((lo, hi))
    return ranges


def ranges_from_cuts(cuts):
    """Contiguous ranges covering ALL of int64, split at the given cuts."""
    cuts = sorted({int(c) for c in cuts if c is not None and c > INT64_MIN})
    bounds, lo = [], INT64_MIN
    for cut in cuts:
        bounds.append((lo, cut - 1))
        lo = cut
    bounds.append((lo, INT64_MAX))
    return bounds


def quantile_ranges(conn, count, scope_kind, generation_id=None):
    """Balanced batches: cut points at quantiles of in-scope statement hashes."""
    fractions = [k / count for k in range(1, count)]
    if not fractions:
        return [(INT64_MIN, INT64_MAX)]
    if scope_kind == 'statements':
        source = (f'SELECT mk_hash FROM {SCHEMA}.generation_statement '
                  f'WHERE generation_id = :g')
    else:
        source = 'SELECT mk_hash FROM readonly.source_meta'
    cuts = conn.execute(text(
        f'SELECT percentile_disc(CAST(:f AS float8[])) WITHIN GROUP '
        f'(ORDER BY mk_hash) FROM ({source}) s'),
        f=fractions, g=generation_id).scalar()
    return ranges_from_cuts(cuts or [])


def _scope_filter(scope_kind):
    if scope_kind == 'statements':
        return (f'EXISTS (SELECT 1 FROM {SCHEMA}.generation_statement s '
                f'WHERE s.generation_id = :g AND s.mk_hash = f.mk_hash)')
    return 'true'


def create_generation(engine, snapshot_label, provenance_run=None,
                      correctness_run=None, statements=None,
                      batch_count=DEFAULT_GENERATION_BATCHES, notes=None):
    scope_kind = 'all' if statements is None else 'statements'
    with engine.begin() as conn:
        for run, kind in ((provenance_run, 'provenance'),
                          (correctness_run, 'correctness')):
            if run is None:
                continue
            row = conn.execute(text(
                f'SELECT kind, status FROM {SCHEMA}.annotation_run '
                f'WHERE run_id=:r'), r=run).first()
            if row is None or row.kind != kind or row.status != 'imported':
                raise BuildError(f'{kind} run {run!r} is not fully imported')
        gen_id = conn.execute(text(f"""
            INSERT INTO {SCHEMA}.ranking_generation (snapshot_label,
                policy_version, matching_policy_version, provenance_run_id,
                correctness_run_id, scope_kind, batch_count, status, notes)
            VALUES (:s, :pv, :mv, :pr, :cr, :sk, :bc, 'building', :n)
            RETURNING generation_id"""), s=snapshot_label, pv=POLICY_VERSION,
            mv=MATCHING_POLICY_VERSION, pr=provenance_run,
            cr=correctness_run, sk=scope_kind, bc=batch_count,
            n=notes).scalar()
        if statements is not None:
            _copy_rows(conn, f'{SCHEMA}.generation_statement',
                       ['generation_id', 'mk_hash'],
                       [(gen_id, hash_integer(h))
                        for h in sorted(set(map(hash_integer, statements)))])
        bounds = quantile_ranges(conn, batch_count, scope_kind, gen_id)
        conn.execute(text(
            f'UPDATE {SCHEMA}.ranking_generation SET batch_count=:n, '
            f'batch_bounds=CAST(:b AS jsonb) WHERE generation_id=:g'),
            n=len(bounds), g=gen_id,
            b=json.dumps([[str(lo), str(hi)] for lo, hi in bounds]))
    return gen_id


def build_generation(engine, generation_id, fail_after_batches=None):
    """Populate (or resume) a generation; mark ready only after the audit."""
    with engine.connect() as conn:
        gen = conn.execute(text(
            f'SELECT * FROM {SCHEMA}.ranking_generation '
            f'WHERE generation_id=:g'), g=generation_id).first()
        if gen is None:
            raise BuildError('No such generation')
        if gen.status != 'building':
            raise BuildError(f'Generation is {gen.status}; build a new one')
        if (gen.policy_version != POLICY_VERSION
                or gen.matching_policy_version != MATCHING_POLICY_VERSION):
            raise BuildError('Generation uses another policy; build a new one')
        runs = [r for r in (gen.provenance_run_id, gen.correctness_run_id)
                if r]
        # Every annotated statement present in the snapshot needs identity
        # rows, otherwise its annotations would silently become "absent".
        missing_identity = conn.execute(text(f"""
            SELECT count(*) FROM (
              SELECT DISTINCT a.statement_hash
              FROM {SCHEMA}.annotation_payload a
              WHERE a.run_id = ANY(:runs)
                AND EXISTS (SELECT 1 FROM readonly.fast_raw_pa_link f
                            WHERE f.mk_hash = a.statement_hash)
                AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.evidence_identity i
                                WHERE i.snapshot_label = :s
                                  AND i.mk_hash = a.statement_hash)) m"""),
            runs=runs, s=gen.snapshot_label).scalar() if runs else 0
        if missing_identity:
            raise BuildError(f'{missing_identity} annotated statements lack '
                             f'identity rows; run build-identity first')
        done = {r[0] for r in conn.execute(text(
            f'SELECT batch_no FROM {SCHEMA}.generation_batch '
            f'WHERE generation_id=:g'), g=generation_id)}
    insert_sql = text(_generation_insert_sql(
        gen.scope_kind, bool(gen.provenance_run_id),
        bool(gen.correctness_run_id)))
    report = dict(generation_id=generation_id, committed_batches=0,
                  skipped_batches=len(done), rows=0)
    started = datetime.now(timezone.utc)
    ranges = ([(int(lo), int(hi)) for lo, hi in gen.batch_bounds]
              if gen.batch_bounds else hash_ranges(gen.batch_count))
    for batch_no, (lo, hi) in enumerate(ranges):
        if batch_no in done:
            continue
        if (fail_after_batches is not None
                and report['committed_batches'] >= fail_after_batches):
            raise RuntimeError('Injected failure (test only)')
        with engine.begin() as conn:
            conn.execute(text(
                f'DELETE FROM {SCHEMA}.evidence_priority WHERE '
                f'generation_id=:g AND mk_hash BETWEEN :lo AND :hi'),
                g=generation_id, lo=lo, hi=hi)
            n = conn.execute(insert_sql, g=generation_id,
                             snap=gen.snapshot_label,
                             prov_run=gen.provenance_run_id,
                             corr_run=gen.correctness_run_id,
                             lo=lo, hi=hi).rowcount
            conn.execute(text(
                f'INSERT INTO {SCHEMA}.generation_batch (generation_id, '
                f'batch_no, row_count) VALUES (:g, :b, :n)'),
                g=generation_id, b=batch_no, n=n)
        report['committed_batches'] += 1
        report['rows'] += n
    report['load_seconds'] = (datetime.now(timezone.utc)
                              - started).total_seconds()
    coverage = audit_generation(engine, generation_id)
    report['coverage'] = coverage
    with engine.begin() as conn:
        if coverage['ok']:
            conn.execute(text(
                f"UPDATE {SCHEMA}.ranking_generation SET status='ready', "
                f"ready_at=now(), coverage=CAST(:c AS jsonb), "
                f"snapshot_fingerprint=CAST(:f AS jsonb) "
                f"WHERE generation_id=:g AND status='building'"),
                g=generation_id, c=json.dumps(coverage),
                f=json.dumps(coverage['snapshot_fingerprint']))
        else:
            conn.execute(text(
                f"UPDATE {SCHEMA}.ranking_generation SET status='failed', "
                f"coverage=CAST(:c AS jsonb) WHERE generation_id=:g"),
                g=generation_id, c=json.dumps(coverage))
    if not coverage['ok']:
        raise BuildError(f'Coverage audit failed: {coverage["problems"]}')
    return report


def audit_generation(engine, generation_id):
    """Complete-coverage audit by anti-joins, not by equal row counts."""
    with engine.begin() as conn:
        gen = conn.execute(text(
            f'SELECT * FROM {SCHEMA}.ranking_generation '
            f'WHERE generation_id=:g'), g=generation_id).first()
        scope = _scope_filter(gen.scope_kind)
        p = f'{SCHEMA}.evidence_priority'
        conn.execute(text(f'ANALYZE {p}'))
        params = dict(g=generation_id)
        q = lambda sql: conn.execute(text(sql), **params).scalar()
        cov = {}
        cov['batches_committed'] = q(
            f'SELECT count(*) FROM {SCHEMA}.generation_batch '
            f'WHERE generation_id=:g')
        cov['batches_expected'] = gen.batch_count
        fp = conn.execute(text(f"""
            SELECT count(*), count(DISTINCT f.mk_hash),
                   COALESCE(sum(f.id::numeric), 0)::text,
                   COALESCE(sum(f.mk_hash::numeric), 0)::text,
                   count(*) - count(DISTINCT (f.mk_hash, f.id))
            FROM readonly.fast_raw_pa_link f WHERE {scope}"""),
            **params).first()
        cov['universe_records'] = fp[0]
        cov['universe_statements'] = fp[1]
        cov['duplicate_statement_record_pairs'] = fp[4]
        cov['snapshot_fingerprint'] = {
            'records': fp[0], 'statements': fp[1], 'sum_raw_id': fp[2],
            'sum_mk_hash': fp[3], 'scope': gen.scope_kind}
        cov['priority_rows'] = q(
            f'SELECT count(*) FROM {p} WHERE generation_id=:g')
        cov['missing_records'] = q(f"""
            SELECT count(*) FROM readonly.fast_raw_pa_link f
            WHERE {scope} AND NOT EXISTS (
                SELECT 1 FROM {p} e WHERE e.generation_id=:g
                AND e.mk_hash=f.mk_hash AND e.raw_record_id=f.id)""")
        cov['stale_rows'] = q(f"""
            SELECT count(*) FROM {p} e WHERE e.generation_id=:g
            AND NOT EXISTS (SELECT 1 FROM readonly.fast_raw_pa_link f
                WHERE f.mk_hash=e.mk_hash AND f.id=e.raw_record_id)""")
        cov['by_priority'] = {str(k): v for k, v in conn.execute(text(
            f'SELECT priority, count(*) FROM {p} WHERE generation_id=:g '
            f'GROUP BY 1 ORDER BY 1'), **params)}
        from indra_db.client.readonly.evidence_ranking import (
            CORRECTNESS_STATUS_NAMES, PROVENANCE_STATUS_NAMES)
        cov['by_correctness_status'] = {
            CORRECTNESS_STATUS_NAMES[k]: v for k, v in conn.execute(text(
                f'SELECT correctness_status, count(*) FROM {p} '
                f'WHERE generation_id=:g GROUP BY 1'), **params)}
        cov['by_provenance_status'] = {
            PROVENANCE_STATUS_NAMES[k]: v for k, v in conn.execute(text(
                f'SELECT provenance_status, count(*) FROM {p} '
                f'WHERE generation_id=:g GROUP BY 1'), **params)}
        cov['multi_evidence_records'] = q(f"""
            SELECT count(*) FROM {SCHEMA}.evidence_identity i
            WHERE i.snapshot_label = '{_sql_str(gen.snapshot_label)}'
              AND i.evidence_elements > 1""")
        # One annotation legitimately applied to several current records
        # (same statement, source hash and publication).
        cov['one_to_many_annotations'] = q(f"""
            SELECT count(*) FROM (SELECT provenance_annotation_id FROM {p}
              WHERE generation_id=:g AND provenance_annotation_id IS NOT NULL
              GROUP BY 1 HAVING count(*) > 1) x""")
        problems = []
        if cov['batches_committed'] != cov['batches_expected']:
            problems.append('incomplete batches')
        if cov['missing_records']:
            problems.append('evidence records without a priority row')
        if cov['stale_rows']:
            problems.append('priority rows for records not in the snapshot')
        if cov['multi_evidence_records']:
            problems.append('multi-evidence raw records require an explicit evidence identity policy')
        if cov['duplicate_statement_record_pairs']:
            problems.append('duplicate (mk_hash, id) in readonly snapshot')
        if cov['priority_rows'] != cov['universe_records']:
            problems.append('row count differs from universe')
        cov['problems'] = problems
        cov['ok'] = not problems
        cov['audited_at'] = datetime.now(timezone.utc).isoformat()
    return cov


def _sql_str(value):
    return str(value).replace("'", "''")


def publish(engine, generation_id):
    """Atomically point the service at a ready, fully covered generation."""
    with engine.begin() as conn:
        gen = conn.execute(text(
            f'SELECT status, coverage, policy_version, matching_policy_version '
            f'FROM {SCHEMA}.ranking_generation '
            f'WHERE generation_id=:g FOR UPDATE'), g=generation_id).first()
        if gen is None or gen.status != 'ready':
            raise BuildError('Only a ready generation can be published')
        if (gen.policy_version != POLICY_VERSION
                or gen.matching_policy_version != MATCHING_POLICY_VERSION):
            raise BuildError('Generation uses another policy; build a new one')
        if not (gen.coverage or {}).get('ok'):
            raise BuildError('Generation has no passing coverage audit')
        current = conn.execute(text(
            f'SELECT generation_id, previous_generation_id FROM '
            f'{SCHEMA}.active_generation FOR UPDATE')).first()
        if current is None:
            conn.execute(text(
                f'INSERT INTO {SCHEMA}.active_generation (singleton, '
                f'generation_id) VALUES (true, :g)'), g=generation_id)
            previous = None
        else:
            previous = current.generation_id
            if previous == generation_id:
                return {'active': generation_id, 'previous': None,
                        'unchanged': True}
            conn.execute(text(
                f'UPDATE {SCHEMA}.active_generation SET generation_id=:g, '
                f'previous_generation_id=:p, activated_at=now()'),
                g=generation_id, p=previous)
            # Keep exactly one rollback candidate ready; older ones retire
            # (rows retained until purge so open cursors can finish).
            if current.previous_generation_id not in (None, generation_id):
                conn.execute(text(
                    f"UPDATE {SCHEMA}.ranking_generation SET status='retired' "
                    f"WHERE generation_id=:o AND status='ready'"),
                    o=current.previous_generation_id)
    return {'active': generation_id, 'previous': previous}


def rollback(engine):
    with engine.begin() as conn:
        current = conn.execute(text(
            f'SELECT generation_id, previous_generation_id FROM '
            f'{SCHEMA}.active_generation FOR UPDATE')).first()
        if current is None or current.previous_generation_id is None:
            raise BuildError('No rollback generation recorded')
        previous = conn.execute(text(
            f'SELECT status, policy_version, matching_policy_version '
            f'FROM {SCHEMA}.ranking_generation '
            f'WHERE generation_id=:g'), g=current.previous_generation_id
        ).first()
        if previous is None or previous.status != 'ready':
            raise BuildError('Rollback generation is no longer ready')
        if (previous.policy_version != POLICY_VERSION
                or previous.matching_policy_version != MATCHING_POLICY_VERSION):
            raise BuildError('Rollback generation requires its matching code policy')
        conn.execute(text(
            f'UPDATE {SCHEMA}.active_generation SET generation_id=:p, '
            f'previous_generation_id=:c, activated_at=now()'),
            p=current.previous_generation_id, c=current.generation_id)
    return {'active': current.previous_generation_id,
            'previous': current.generation_id}


def purge(engine, generation_id):
    """Delete rows of a retired/failed generation (never the active one)."""
    with engine.begin() as conn:
        active = conn.execute(text(
            f'SELECT generation_id, previous_generation_id '
            f'FROM {SCHEMA}.active_generation')).first()
        if active is not None and generation_id in active:
            raise BuildError('Cannot purge the active or rollback generation')
        status = conn.execute(text(
            f'SELECT status FROM {SCHEMA}.ranking_generation '
            f'WHERE generation_id=:g FOR UPDATE'), g=generation_id).scalar()
        # A ready generation that is neither active nor the rollback
        # candidate serves nothing; pinned cursors to it get a 409.
        if status not in ('retired', 'failed', 'building', 'ready'):
            raise BuildError(f'Cannot purge a {status} generation')
        n = conn.execute(text(
            f'DELETE FROM {SCHEMA}.evidence_priority WHERE generation_id=:g'),
            g=generation_id).rowcount
        conn.execute(text(
            f'DELETE FROM {SCHEMA}.generation_batch WHERE generation_id=:g'),
            g=generation_id)
        conn.execute(text(
            f"UPDATE {SCHEMA}.ranking_generation SET status='purged' "
            f"WHERE generation_id=:g"), g=generation_id)
    return {'purged_rows': n}


def status(engine):
    with engine.connect() as conn:
        gens = [dict(r) for r in conn.execute(text(
            f'SELECT generation_id, snapshot_label, policy_version, '
            f'provenance_run_id, correctness_run_id, scope_kind, status, '
            f'created_at::text, ready_at::text FROM '
            f'{SCHEMA}.ranking_generation ORDER BY generation_id'))]
        active = conn.execute(text(
            f'SELECT generation_id, previous_generation_id, '
            f'activated_at::text FROM {SCHEMA}.active_generation')).first()
        runs = [dict(r) for r in conn.execute(text(
            f'SELECT run_id, kind, status, synthetic, publication_verifiable '
            f'FROM {SCHEMA}.annotation_run ORDER BY run_id'))]
    return {'generations': gens, 'active': dict(active) if active else None,
            'runs': runs}


# --- CLI -------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--database-url',
                        default=os.environ.get('INDRA_DB_RANKING_WRITER_URL'),
                        help='Writer URL for the database holding readonly.*')
    parser.add_argument('--allow-remote-host', action='store_true')
    cmd = parser.add_subparsers(dest='command', required=True)
    cmd.add_parser('init-schema')
    imp = cmd.add_parser('import-provenance')
    imp.add_argument('--sqlite', required=True)
    imp.add_argument('--run-id', help='Defaults to the file metadata run_id')
    imp.add_argument('--batch-size', type=int, default=DEFAULT_IMPORT_BATCH)
    imp.add_argument('--dry-run', action='store_true')
    cor = cmd.add_parser('import-correctness')
    src = cor.add_mutually_exclusive_group(required=True)
    src.add_argument('--sqlite')
    src.add_argument('--fixture-jsonl',
                     help='Synthetic fixture; the run is marked synthetic')
    cor.add_argument('--run-id', required=True)
    cor.add_argument('--batch-size', type=int, default=DEFAULT_IMPORT_BATCH)
    cor.add_argument('--dry-run', action='store_true')
    fx = cmd.add_parser('import-provenance-fixture')
    fx.add_argument('--fixture-jsonl', required=True)
    fx.add_argument('--run-id', required=True)
    ident = cmd.add_parser('build-identity')
    ident.add_argument('--snapshot-label', required=True)
    ident.add_argument('--run-id', action='append', required=True)
    ident.add_argument('--batch-size', type=int, default=DEFAULT_IDENTITY_BATCH)
    gen = cmd.add_parser('build-generation')
    gen.add_argument('--snapshot-label', required=True)
    gen.add_argument('--provenance-run')
    gen.add_argument('--correctness-run')
    gen.add_argument('--statements-file',
                     help='Scope to these statement hashes (one per line)')
    gen.add_argument('--batches', type=int, default=DEFAULT_GENERATION_BATCHES)
    gen.add_argument('--resume', type=int, help='Resume a building generation')
    gen.add_argument('--notes')
    aud = cmd.add_parser('audit')
    aud.add_argument('--generation', type=int, required=True)
    pub = cmd.add_parser('publish')
    pub.add_argument('--generation', type=int, required=True)
    cmd.add_parser('rollback')
    pur = cmd.add_parser('purge')
    pur.add_argument('--generation', type=int, required=True)
    cmd.add_parser('status')
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error('--database-url or INDRA_DB_RANKING_WRITER_URL required')
    engine = writer_engine(args.database_url, args.allow_remote_host)

    if args.command == 'init-schema':
        ensure_schema(engine)
        report = {'schema': SCHEMA, 'created': True}
    elif args.command == 'import-provenance':
        meta = provenance_sqlite_info(args.sqlite)
        report = import_run(
            engine, args.run_id or meta['run_id'], 'provenance',
            iter_provenance_sqlite(args.sqlite), 'provenance-sqlite-v2',
            source_path=os.path.abspath(args.sqlite),
            source_sha256=file_digest(args.sqlite),
            publication_verifiable=True,
            metadata={k: meta.get(k) for k in (
                'run_id', 'taxonomy_version', 'validation_status',
                'matching_policy', 'import_report', 'source_metadata')},
            batch_size=args.batch_size, dry_run=args.dry_run)
    elif args.command == 'import-correctness':
        if args.sqlite:
            adapter_report = {}
            with closing(iter_correctness_sqlite(args.sqlite,
                                                adapter_report)) as records:
                # Prime schema inspection without materializing the dataset.
                # import_run then consumes bounded batches; close on errors too.
                first = next(records, None)
                report = import_run(
                    engine, args.run_id, 'correctness',
                    chain((first,), records) if first is not None else (),
                    'llm-evidence-curation-sqlite',
                    source_path=os.path.abspath(args.sqlite),
                    source_sha256=file_digest(args.sqlite),
                    publication_verifiable=bool(
                        adapter_report['publication_columns']),
                    metadata={'adapter': adapter_report},
                    batch_size=args.batch_size, dry_run=args.dry_run)
            report['adapter'] = adapter_report
        else:
            report = import_run(
                engine, args.run_id, 'correctness',
                iter_jsonl(args.fixture_jsonl, 'correctness'),
                'synthetic-fixture-jsonl',
                source_path=os.path.abspath(args.fixture_jsonl),
                source_sha256=file_digest(args.fixture_jsonl),
                synthetic=True, batch_size=args.batch_size,
                dry_run=args.dry_run)
    elif args.command == 'import-provenance-fixture':
        report = import_run(
            engine, args.run_id, 'provenance',
            iter_jsonl(args.fixture_jsonl, 'provenance'),
            'synthetic-fixture-jsonl',
            source_path=os.path.abspath(args.fixture_jsonl),
            source_sha256=file_digest(args.fixture_jsonl), synthetic=True)
    elif args.command == 'build-identity':
        report = build_identity(engine, args.snapshot_label, args.run_id,
                                args.batch_size)
    elif args.command == 'build-generation':
        if args.resume is None:
            statements = None
            if args.statements_file:
                with open(args.statements_file) as handle:
                    statements = [l.strip() for l in handle if l.strip()]
            gen_id = create_generation(
                engine, args.snapshot_label, args.provenance_run,
                args.correctness_run, statements, args.batches, args.notes)
        else:
            gen_id = args.resume
        report = build_generation(engine, gen_id)
    elif args.command == 'audit':
        report = audit_generation(engine, args.generation)
    elif args.command == 'publish':
        report = publish(engine, args.generation)
    elif args.command == 'rollback':
        report = rollback(engine)
    elif args.command == 'purge':
        report = purge(engine, args.generation)
    else:
        report = status(engine)
    json.dump(report, sys.stdout, indent=2, default=str)
    print()


if __name__ == '__main__':
    main()
