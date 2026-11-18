"""Build/reconcile the intermediate annotation database, without running an LLM.

Run with python -m indra_db_service.provenance_build --help.
Only the reconciliation command needs INDRA DB/PostgreSQL dependencies.
"""
import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
import zlib
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .provenance import RUNTIME_POLICY, SCHEMA_VERSION, connect_readonly, hash_integer, publication_key


def file_digest(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checkpoint_rows(dataset, results, config):
    """Stream results joined by ordinal, after checking the original run identity."""
    dataset, results = Path(dataset), Path(results)
    manifest = json.loads((dataset.parent / "dataset.json").read_text())
    config = json.loads(Path(config).read_text())
    if manifest["format"] != "evidence-sqlite-v1":
        raise ValueError("Unsupported input dataset")
    if file_digest(dataset) != manifest["sha256"]:
        raise ValueError("Dataset checksum mismatch")
    if config["input_digest"] != manifest["sha256"]:
        raise ValueError("Results config does not identify this dataset")
    run_digest = hashlib.sha256(
        json.dumps(config, sort_keys=True).encode()).hexdigest()
    with closing(connect_readonly(results)) as source, \
            closing(connect_readonly(dataset)) as data:
        start, stop = config["selected_range"]
        has_metadata = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'").fetchone()
        if has_metadata:
            stored = source.execute(
                "SELECT value FROM metadata WHERE key='run_digest'").fetchone()
            if not stored or stored[0] != run_digest:
                raise ValueError("Results checkpoint does not match config")
        else:
            # The existing merger writes no metadata table: its config and
            # coverage report are mandatory companion artifacts.
            coverage = json.loads((results.parent / "coverage.json").read_text())
            count, low, high = source.execute(
                "SELECT count(*),min(idx),max(idx) FROM results").fetchone()
            if (start != 0 or stop != manifest["eligible"]
                    or coverage.get("complete") is not True
                    or coverage.get("expected") != stop
                    or coverage.get("successful") != stop
                    or (count, low, high) != (stop, 0, stop - 1)):
                raise ValueError("Incomplete or inconsistent merged results")
        for index, blob in source.execute("SELECT idx,event FROM results ORDER BY idx"):
            if not start <= index < stop:
                raise ValueError("Result outside configured range")
            event = json.loads(zlib.decompress(blob))
            if event["index"] != index:
                raise ValueError("Checkpoint index mismatch")
            row = data.execute("""
                SELECT s.payload,e.payload,e.pmid FROM evidence e
                JOIN statements s ON s.id=e.statement_id WHERE e.ordinal=?
            """, (index,)).fetchone()
            if row is None:
                raise ValueError("Result has no corresponding input evidence")
            statement, evidence = map(json.loads, row[:2])
            if not evidence.get("pmid") and row[2]:
                evidence["pmid"] = row[2]
            annotation = dict(event["classification"],
                              overrides=event.get("overrides", []))
            # This is a prompt-local position, not part of classification identity.
            annotation.pop("evidence_index", None)
            annotation["_publication_key"] = publication_key(evidence)
            yield statement["statement_hash"], evidence.get("source_hash"), annotation


def build(rows, output, run_id, taxonomy_version, source_metadata=None):
    """Build an unvalidated candidate. Conflicting duplicates are quarantined."""
    output = Path(output)
    if output.exists():
        raise ValueError("Output already exists; choose a new versioned path")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=output.parent, suffix=".sqlite.tmp")
    os.close(fd)
    counts = dict(input_rows=0, invalid_hashes=0, missing_publication=0,
                  duplicates=0, conflict_pairs=0)
    try:
        with closing(sqlite3.connect(temporary)) as db:
            db.executescript("""
                CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE evidence_provenance(
                    statement_hash INTEGER NOT NULL, source_hash INTEGER NOT NULL,
                    annotation TEXT NOT NULL,
                    PRIMARY KEY(statement_hash,source_hash)) WITHOUT ROWID;
                CREATE TABLE conflicts(
                    statement_hash INTEGER NOT NULL, source_hash INTEGER NOT NULL,
                    annotation TEXT NOT NULL,
                    PRIMARY KEY(statement_hash,source_hash,annotation)) WITHOUT ROWID;
            """)
            for sh, eh, annotation in rows:
                counts["input_rows"] += 1
                try:
                    pair = hash_integer(sh), hash_integer(eh)
                except ValueError:
                    counts["invalid_hashes"] += 1
                    continue
                if not isinstance(annotation, dict) or not annotation.get("level_1"):
                    raise ValueError("Classification object missing level_1")
                if not annotation.get("_publication_key"):
                    counts["missing_publication"] += 1
                    continue
                payload = json.dumps(annotation, sort_keys=True, allow_nan=False)
                conflict = db.execute(
                    "SELECT 1 FROM conflicts WHERE statement_hash=? AND source_hash=?",
                    pair).fetchone()
                old = db.execute(
                    "SELECT annotation FROM evidence_provenance "
                    "WHERE statement_hash=? AND source_hash=?", pair).fetchone()
                if conflict or (old and old[0] != payload):
                    if not conflict:
                        counts["conflict_pairs"] += 1
                    for item in ([old[0]] if old else []) + [payload]:
                        db.execute("INSERT OR IGNORE INTO conflicts VALUES (?,?,?)",
                                   pair + (item,))
                    db.execute("DELETE FROM evidence_provenance "
                               "WHERE statement_hash=? AND source_hash=?", pair)
                elif old:
                    counts["duplicates"] += 1
                else:
                    db.execute("INSERT INTO evidence_provenance VALUES (?,?,?)",
                               pair + (payload,))
            counts["candidate_pairs"] = db.execute(
                "SELECT count(*) FROM evidence_provenance").fetchone()[0]
            metadata = dict(schema_version=SCHEMA_VERSION, run_id=run_id,
                            taxonomy_version=taxonomy_version,
                            validation_status="pending",
                            created_at=datetime.now(timezone.utc).isoformat(),
                            import_report=json.dumps(counts),
                            source_metadata=json.dumps(source_metadata or {}))
            db.executemany("INSERT INTO metadata VALUES (?,?)", metadata.items())
            db.commit()
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return counts


def prepare_runtime(candidate, output):
    """Publish a conflict-free candidate copy for exact current-response lookup.

    This audits the local file, NOT membership in a current INDRA snapshot.
    Obsolete pairs may remain stored but cannot introduce evidence into responses.
    """
    output = Path(output)
    if output.exists():
        raise ValueError("Output already exists; choose a new versioned path")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=output.parent, suffix=".sqlite.tmp")
    os.close(fd)
    try:
        with closing(connect_readonly(candidate)) as source, \
                closing(sqlite3.connect(temporary)) as db:
            source.backup(db)
            metadata = dict(db.execute("SELECT key,value FROM metadata"))
            if (metadata.get("schema_version") != SCHEMA_VERSION
                    or metadata.get("validation_status") != "pending"):
                raise ValueError("Runtime preparation requires an original pending candidate")
            if not metadata.get("run_id") or not metadata.get("taxonomy_version"):
                raise ValueError("Missing annotation run identity")
            if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("Candidate integrity check failed")
            if db.execute("""SELECT 1 FROM evidence_provenance a JOIN conflicts c
                    USING(statement_hash,source_hash) LIMIT 1""").fetchone():
                raise ValueError("Conflicting pairs remain in the serving table")
            count = 0
            for sh, eh, payload in db.execute(
                    "SELECT statement_hash,source_hash,annotation FROM evidence_provenance"):
                hash_integer(sh)
                hash_integer(eh)
                annotation = json.loads(payload)
                if (not isinstance(annotation, dict)
                        or annotation.get("level_1") not in
                        {"primary_data", "citation", "background", "other"}
                        or not isinstance(annotation.get("_publication_key"), str)
                        or not annotation["_publication_key"].strip()):
                    raise ValueError("Invalid candidate annotation or publication")
                count += 1
            report = {
                "candidate_pairs": count,
                "candidate_statements": db.execute(
                    "SELECT count(DISTINCT statement_hash) FROM evidence_provenance").fetchone()[0],
                "full_snapshot_reconciled": False,
                "matching_policy": RUNTIME_POLICY,
            }
            db.executemany("INSERT OR REPLACE INTO metadata VALUES (?,?)", {
                "validation_status": "runtime_matching",
                "matching_policy": RUNTIME_POLICY,
                "prepared_at": datetime.now(timezone.utc).isoformat(),
                "preparation_report": json.dumps(report),
            }.items())
            db.commit()
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return report


def served_publication_evidence(evidence, refs):
    """Mirror Query.get_statements publication updates from ReadingRefLink."""
    evidence = dict(evidence)
    evidence['text_refs'] = dict(evidence.get('text_refs') or {})
    if refs.get('pmid'):
        evidence['pmid'] = refs['pmid']
    else:
        evidence['text_refs'].pop('PMID', None)
    evidence['text_refs'].update({k.upper(): v for k, v in refs.items() if v is not None})
    return evidence


@contextmanager
def readonly_snapshot(ro):
    """Set isolation before starting queries, including connection setup."""
    ro.session.rollback()
    try:
        connection = ro.session.connection(
            execution_options={'isolation_level': 'REPEATABLE READ'})
        connection.execute('SET TRANSACTION READ ONLY')
        yield
    finally:
        ro.session.rollback()


def live_pairs(ro, statement_hashes):
    """Stream exact pairs from the same readonly relation used by REST queries."""
    from indra_db.readonly_dumping.util import clean_json_loads

    table = ro.FastRawPaLink
    refs = ro.ReadingRefLink
    query = ro.session.query(table.mk_hash, table.raw_json, refs.pmid, refs.pmcid, refs.doi)
    query = query.outerjoin(refs, refs.rid == table.reading_id).filter(table.mk_hash.in_(statement_hashes))
    for sh, raw, pmid, pmcid, doi in query.yield_per(2000):
        raw = bytes(raw).decode("utf-8") if not isinstance(raw, str) else raw
        statement = clean_json_loads(raw)
        # The current REST query serves the first evidence element of each raw
        # statement. Do not validate additional elements it does not return.
        for evidence in statement.get("evidence", [])[:1]:
            try:
                publication = publication_key(served_publication_evidence(
                    evidence, {'pmid': pmid, 'pmcid': pmcid, 'doi': doi}))
                if publication:
                    yield hash_integer(sh), hash_integer(evidence.get("source_hash")), publication
            except ValueError:
                continue


def reconcile(candidate, output, snapshot, pair_fetcher):
    """Publish a filtered copy; retain the candidate and all conflict records."""
    output = Path(output)
    if output.exists():
        raise ValueError("Output already exists; choose a new versioned path")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=output.parent, suffix=".sqlite.tmp")
    os.close(fd)
    try:
        with closing(connect_readonly(candidate)) as source, \
                closing(sqlite3.connect(temporary)) as db:
            source.backup(db)
            metadata = dict(db.execute("SELECT key,value FROM metadata"))
            if metadata.get("schema_version") != SCHEMA_VERSION:
                raise ValueError("Unsupported candidate schema")
            db.execute("""CREATE TABLE IF NOT EXISTS unmatched(
                statement_hash INTEGER,source_hash INTEGER,annotation TEXT,
                PRIMARY KEY(statement_hash,source_hash)) WITHOUT ROWID""")
            db.execute("CREATE TEMP TABLE matched(statement_hash INTEGER,source_hash INTEGER,"
                       "publication_key TEXT,PRIMARY KEY(statement_hash,source_hash,publication_key)) WITHOUT ROWID")
            cursor = source.execute(
                "SELECT DISTINCT statement_hash FROM evidence_provenance ORDER BY statement_hash")
            while True:
                batch = cursor.fetchmany(500)
                if not batch:
                    break
                db.executemany("INSERT OR IGNORE INTO matched VALUES (?,?,?)",
                               pair_fetcher([row[0] for row in batch]))
            db.execute("""INSERT OR IGNORE INTO unmatched
                SELECT a.* FROM evidence_provenance a WHERE NOT EXISTS
                (SELECT 1 FROM matched m WHERE m.statement_hash=a.statement_hash
                 AND m.source_hash=a.source_hash
                 AND m.publication_key=json_extract(a.annotation,'$._publication_key'))""")
            db.execute("""DELETE FROM evidence_provenance WHERE NOT EXISTS
                (SELECT 1 FROM matched m
                 WHERE m.statement_hash=evidence_provenance.statement_hash
                 AND m.source_hash=evidence_provenance.source_hash
                 AND m.publication_key=json_extract(evidence_provenance.annotation,'$._publication_key'))""")
            report = {
                "matched_pairs": db.execute("SELECT count(*) FROM evidence_provenance").fetchone()[0],
                "unmatched_pairs": db.execute("SELECT count(*) FROM unmatched").fetchone()[0],
                "conflicting_pairs": db.execute("SELECT count(*) FROM (SELECT DISTINCT statement_hash,source_hash FROM conflicts)").fetchone()[0],
            }
            metadata = dict(validation_status="validated", target_snapshot=snapshot,
                            validated_at=datetime.now(timezone.utc).isoformat(),
                            validation_report=json.dumps(report))
            db.executemany("INSERT OR REPLACE INTO metadata VALUES (?,?)", metadata.items())
            db.commit()
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("build")
    for option in ("dataset", "results", "config", "output", "run-id", "taxonomy-version"):
        load.add_argument("--" + option, required=True)
    validate = commands.add_parser("reconcile")
    for option in ("candidate", "output", "target-snapshot"):
        validate.add_argument("--" + option, required=True)
    validate.add_argument("--database", default="primary",
                          help="Configured INDRA readonly database name")
    runtime = commands.add_parser("prepare-runtime",
                                  help="Prepare all candidates for current-response matching")
    for option in ("candidate", "output"):
        runtime.add_argument("--" + option, required=True)
    args = parser.parse_args()
    if args.command == "build":
        report = build(checkpoint_rows(args.dataset, args.results, args.config),
                       args.output, args.run_id, args.taxonomy_version,
                       {"dataset_sha256": file_digest(args.dataset),
                        "results_sha256": file_digest(args.results),
                        "config_sha256": file_digest(args.config)})
    elif args.command == "prepare-runtime":
        report = prepare_runtime(args.candidate, args.output)
    else:
        from indra_db.util import get_ro
        ro = get_ro(args.database)
        if ro is None:
            raise ValueError("Readonly database is not configured")
        # Keep all batches within one consistent PostgreSQL snapshot.
        try:
            with readonly_snapshot(ro):
                report = reconcile(args.candidate, args.output, args.target_snapshot,
                                   lambda hashes: live_pairs(ro, hashes))
        finally:
            ro.close()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
