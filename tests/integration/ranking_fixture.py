"""SYNTHETIC readonly fixture loader for evidence-ranking tests/benchmarks.

Creates the actual mapped INDRA readonly tables (same column names/types and
ORM indices/primary keys as ``create_all``) in a disposable local database and
bulk-loads explicitly synthetic statements/evidence. The data are NOT a live
INDRA snapshot. Hashes may be supplied from real annotation files so that the
real-format provenance adapter can be exercised, but text/records are made up.
"""
import csv
import io
import json
import uuid

from indra.statements import Activation, Agent, Evidence

SOURCES = ('reach', 'sparser', 'medscan')
READONLY_TABLES = ('FastRawPaLink', 'ReadingRefLink', 'RawStmtSrc',
                   'SourceMeta', 'RawStmtMeshTerms', 'MeshTermMeta',
                   'AgentInteractions')


def check_disposable_url(url, expected_database):
    """Refuse anything but a local, explicitly named disposable database."""
    socket = url.query.get('host', '')
    if (url.database != expected_database
            or url.host not in (None, 'localhost', '127.0.0.1', '::1')
            or (socket and not socket.startswith('/'))):
        raise ValueError(f'Use an empty local {expected_database} database')


def create_readonly(engine, ro):
    from sqlalchemy import inspect
    schemas = inspect(engine).get_schema_names()
    if 'readonly' in schemas or 'annotation' in schemas:
        raise ValueError('Refusing to modify existing readonly/annotation '
                         'schemas')
    tables = [getattr(ro, name).__table__ for name in READONLY_TABLES]
    with engine.begin() as conn:
        conn.execute('CREATE SCHEMA readonly')
        ro.Base.metadata.create_all(conn, tables=tables)
        for src in SOURCES:
            conn.execute(f'ALTER TABLE readonly.source_meta ADD COLUMN {src} '
                         f'bigint')
        # Same secondary indices as the readonly build (BtreeIndex entries).
        for stmt in [
            'CREATE INDEX IF NOT EXISTS hash_index ON '
            'readonly.fast_raw_pa_link (mk_hash)',
            'CREATE INDEX IF NOT EXISTS frp_reading_id_idx ON '
            'readonly.fast_raw_pa_link (reading_id)',
            'CREATE INDEX IF NOT EXISTS raw_stmt_mesh_terms_mesh_num_idx ON '
            'readonly.raw_stmt_mesh_terms (mesh_num)',
        ]:
            conn.execute(stmt)


def drop_fixture_schemas(engine):
    with engine.begin() as conn:
        conn.execute('DROP SCHEMA IF EXISTS annotation CASCADE')
        conn.execute('DROP SCHEMA IF EXISTS readonly CASCADE')


def _copy(conn, table, columns, rows):
    buf = io.StringIO()
    writer = csv.writer(buf)
    count = 0
    for row in rows:
        writer.writerow(['' if v is None else v for v in row])
        count += 1
    if not count:
        return
    buf.seek(0)
    cursor = conn.connection.cursor()
    cursor.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN "
                       f"WITH (FORMAT csv, NULL '')", buf)


def _bytea(data):
    return '\\x' + data.hex()


_TEMPLATE_CACHE = {}


def statement_json(subj, obj):
    key = (subj, obj)
    if key not in _TEMPLATE_CACHE:
        stmt = Activation(Agent(subj, db_refs={'TEXT': subj}),
                          Agent(obj, db_refs={'TEXT': obj}))
        _TEMPLATE_CACHE[key] = stmt.to_json()
    return _TEMPLATE_CACHE[key]


def record_text(subj, obj, record):
    return record.get('text',
                      f"SYNTHETIC {subj} activates {obj} #{record['id']}.")


def real_source_hash(subj, obj, record):
    """The hash INDRA itself computes (and recomputes when re-reading JSON)."""
    return Evidence(source_api=record['src'],
                    text=record_text(subj, obj, record),
                    pmid=record.get('raw_pmid')).get_source_hash()


def raw_json_bytes(subj, obj, record):
    """Raw statement JSON; evidence[0] carries ``record['source_hash']``.

    Use :func:`real_source_hash` values unless a test deliberately needs a
    raw hash that INDRA would not recompute identically.
    """
    raw = dict(statement_json(subj, obj))
    raw['id'] = str(uuid.UUID(int=record['id']))
    ev = Evidence(source_api=record['src'], text=record_text(subj, obj, record),
                  pmid=record.get('raw_pmid')).to_json()
    ev['source_hash'] = record['source_hash']
    raw['evidence'] = [ev] + record.get('extra_evidence', [])
    return json.dumps(raw).encode()


def load_statements(conn, statements, refs, mesh_meta=True):
    """Bulk load. ``statements``: dicts with mk_hash, subj, obj, records
    (dicts with id, src, source_hash, rid, raw_pmid, mesh) and optional
    agent_json. ``refs``: {rid: {'pmid':..., 'pmcid':..., 'doi':...}}."""
    _copy(conn, 'readonly.reading_ref_link',
          ['rid', 'pmid', 'pmid_num', 'pmcid', 'pmcid_num', 'doi', 'source',
           'reader'],
          [(rid, r.get('pmid'), int(r['pmid']) if r.get('pmid') else None,
            r.get('pmcid'),
            int(r['pmcid'][3:]) if r.get('pmcid') else None, r.get('doi'),
            'pubmed', 'reach') for rid, r in refs.items()])

    def frp_rows():
        for s in statements:
            pa = _bytea(json.dumps(statement_json(s['subj'], s['obj'])).encode())
            for r in s['records']:
                yield (r['id'], _bytea(raw_json_bytes(s['subj'], s['obj'], r)),
                       r.get('rid'), None, s['mk_hash'], pa, 1, r['src'])
    _copy(conn, 'readonly.fast_raw_pa_link',
          ['id', 'raw_json', 'reading_id', 'db_info_id', 'mk_hash', 'pa_json',
           'type_num', 'src'], frp_rows())
    _copy(conn, 'readonly.raw_stmt_src', ['sid', 'src'],
          ((r['id'], r['src']) for s in statements for r in s['records']))
    _copy(conn, 'readonly.raw_stmt_mesh_terms', ['sid', 'mesh_num'],
          ((r['id'], m) for s in statements for r in s['records']
           for m in r.get('mesh', ())))

    def meta_rows():
        for s in statements:
            counts = {src: 0 for src in SOURCES}
            for r in s['records']:
                counts[r['src']] += 1
            present = {k: v for k, v in counts.items() if v}
            yield (s['mk_hash'], len(s['records']), s.get('belief', 0.5),
                   len(present), json.dumps(present),
                   next(iter(present)) if len(present) == 1 else None,
                   True, False, 1, 2) + tuple(counts[src] or None
                                             for src in SOURCES)
    _copy(conn, 'readonly.source_meta',
          ['mk_hash', 'ev_count', 'belief', 'num_srcs', 'src_json', 'only_src',
           'has_rd', 'has_db', 'type_num', 'agent_count'] + list(SOURCES),
          meta_rows())
    _copy(conn, 'readonly.agent_interactions',
          ['mk_hash', 'agent_json', 'type_num', 'agent_count', 'ev_count',
           'belief', 'src_json', 'is_complex_dup'],
          ((s['mk_hash'], json.dumps(s.get('agent_json', {'0': s['subj'],
                                                           '1': s['obj']})),
            1, 2, len(s['records']), s.get('belief', 0.5),
            json.dumps({r['src']: 1 for r in s['records']}), False)
           for s in statements))
    if mesh_meta:
        _copy(conn, 'readonly.mesh_term_meta',
              ['mk_hash', 'mesh_num', 'tr_count', 'ev_count', 'belief',
               'type_num', 'agent_count'],
              ((s['mk_hash'], m, 1, len(s['records']), s.get('belief', 0.5),
                1, 2)
               for s in statements
               for m in sorted({m for r in s['records']
                                for m in r.get('mesh', ())})))
