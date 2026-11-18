"""Optional, read-only provenance annotations for current query evidence."""
import json
import logging
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "2"
ENV_PATH = "INDRA_DB_API_PROVENANCE_DB"
RUNTIME_POLICY = "current-response-statement-source-publication-v1"


def hash_integer(value):
    """Preserve signed 64-bit hashes; never round JSON floats."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Hash must be an integer or decimal string")
    if isinstance(value, str) and not re.fullmatch(r"-?\d+", value):
        raise ValueError("Invalid hash")
    number = int(value)
    if not -(2 ** 63) <= number < 2 ** 63:
        raise ValueError("Hash outside signed 64-bit range")
    return number


def connect_readonly(path):
    return sqlite3.connect(Path(path).expanduser().resolve().as_uri() + "?mode=ro",
                           uri=True, timeout=1)


def publication_key(evidence):
    """Conservative paper identity: PMID, otherwise PMCID, otherwise DOI.

    Source hashes omit PMID when text exists. Missing/contradictory identifiers
    cannot establish the publication context of a provenance classification.
    """
    refs = evidence.get("text_refs") or {}
    direct = str(evidence.get("pmid") or "").strip()
    reference = str(refs.get("PMID") or "").strip()
    if direct and reference and direct != reference:
        return None
    pmid = direct or reference
    if pmid:
        return "PMID:" + pmid if pmid.isdigit() else None
    pmcid = str(refs.get("PMCID") or "").strip().upper()
    if pmcid:
        return "PMCID:" + pmcid if re.fullmatch(r"PMC\d+", pmcid) else None
    doi = str(refs.get("DOI") or "").strip().lower()
    if doi.startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/"):]
    return "DOI:" + doi if re.fullmatch(r"10\.\d+/\S+", doi) else None


def check_serving_metadata(metadata):
    """Accept reconciled files or explicitly prepared request-time matching files."""
    if metadata.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported provenance annotation schema")
    status = metadata.get("validation_status")
    if status == "validated":
        return
    if (status == "runtime_matching"
            and metadata.get("matching_policy") == RUNTIME_POLICY):
        return
    raise ValueError("Annotation database is neither reconciled nor prepared for runtime matching")


class ProvenanceUnavailable(Exception):
    """The dedicated lookup cannot read a usable annotation database."""


def add_provenance_annotations(results, database_path=None, strict=False):
    """Enrich only exact pairs present in the live statement query response.

    Missing configuration disables the feature. An unreadable/incompatible
    sidecar never makes normal INDRA searches fail. One connection per response,
    closed explicitly; no shared connection or cache survives file replacement.
    """
    path = database_path or os.environ.get(ENV_PATH)
    if not path:
        if strict:
            raise ProvenanceUnavailable("Provenance database is not configured")
        return
    targets = {}
    for statement_hash, statement in results.items():
        for evidence in statement.get("evidence", []):
            try:
                pair = (hash_integer(statement_hash),
                        hash_integer(evidence.get("source_hash")))
            except ValueError:
                continue
            targets.setdefault(pair, []).append(evidence)
    if not targets and not strict:
        return
    pending = []
    try:
        with closing(connect_readonly(path)) as connection:
            metadata = dict(connection.execute("SELECT key,value FROM metadata"))
            check_serving_metadata(metadata)
            pairs = list(targets)
            # 400 pairs = 800 bind parameters, below SQLite's older 999 limit.
            for start in range(0, len(pairs), 400):
                batch = pairs[start:start + 400]
                values = ",".join("(?,?)" for _ in batch)
                query = """
                    WITH requested(statement_hash,source_hash) AS (VALUES %s)
                    SELECT a.statement_hash,a.source_hash,a.annotation
                    FROM evidence_provenance a JOIN requested r
                    ON a.statement_hash=r.statement_hash
                       AND a.source_hash=r.source_hash
                """ % values
                for sh, eh, payload in connection.execute(
                        query, [h for pair in batch for h in pair]):
                    annotation = json.loads(payload)
                    if not isinstance(annotation, dict):
                        raise ValueError("Annotation must be an object")
                    publication = annotation.pop("_publication_key", None)
                    if not publication:
                        continue
                    annotation["run_id"] = metadata["run_id"]
                    annotation["taxonomy_version"] = metadata["taxonomy_version"]
                    pending.append(((sh, eh), publication, annotation))
        # Apply only after all queries succeed: no partially enriched response.
        for pair, publication, annotation in pending:
            for evidence in targets[pair]:
                if publication_key(evidence) == publication:
                    evidence["provenance_classification"] = dict(annotation)
    except (sqlite3.Error, ValueError, KeyError, OSError) as error:
        logger.warning("Could not read provenance annotations: %s", error)
        if strict:
            raise ProvenanceUnavailable("Provenance database is unavailable or incompatible") from error


def parse_provenance_request(args):
    """Shared GET contract for the real REST service and local preview."""
    if set(args) - {"statement", "explanation", "ev_limit", "api_key"}:
        raise ValueError("Unsupported provenance query parameter")
    values = [v.strip() for item in args.getlist("statement")
              for v in item.split(",") if v.strip()]
    if not 1 <= len(values) <= 100:
        raise ValueError("Provide between 1 and 100 statement hashes")
    hashes = list(dict.fromkeys(hash_integer(v) for v in values))
    explanation = args.get("explanation", "false").lower()
    if explanation not in {"true", "false"}:
        raise ValueError("explanation must be true or false")
    limit = hash_integer(args.get("ev_limit", "1000"))
    if not 1 <= limit <= 10000:
        raise ValueError("ev_limit must be between 1 and 10000")
    return hashes, explanation == "true", limit


def get_provenance_classifications(current_statements, statement_hashes,
                                   evidence_counts=None, include_explanations=False,
                                   database_path=None, evidence_limit=1000):
    """Return only annotations matched to supplied current, authorized evidence.

    Callers must obtain current_statements from the normal INDRA query path;
    historical SQLite rows alone cannot establish current membership.
    """
    wanted = {str(hash_integer(h)) for h in statement_hashes}
    # Ignore upstream annotation fields and do not mutate the source response.
    statements = {}
    for h, stmt in current_statements.items():
        if str(h) in wanted:
            evidence = [{k: v for k, v in ev.items()
                         if k != "provenance_classification"}
                        for ev in stmt.get("evidence", [])]
            statements[str(h)] = {"evidence": evidence}
    add_provenance_annotations(statements, database_path, strict=True)
    counts = {str(h): n for h, n in (evidence_counts or {}).items()}
    output = {}
    for h in map(str, statement_hashes):
        evidence = statements.get(h, {}).get("evidence", [])
        annotations = []
        seen = set()
        for ev in evidence:
            annotation = ev.get("provenance_classification")
            if annotation is None:
                continue
            source_hash = str(hash_integer(ev["source_hash"]))
            publication = publication_key(ev)
            if (source_hash, publication) in seen:
                continue
            seen.add((source_hash, publication))
            annotation = dict(annotation)
            if not include_explanations:
                annotation.pop("reasoning", None)
            annotations.append({"source_hash": source_hash,
                                "publication": publication,
                                "provenance_classification": annotation})
        output[h] = {"classifications": annotations,
                     "statement_returned": h in statements,
                     "evidence_checked": len(evidence),
                     "total_evidence": counts.get(h),
                     "all_evidence_checked": h in statements and counts.get(h) == len(evidence)}
    return {"statements": output,
            "missing": [h for h, entry in output.items() if not entry["classifications"]],
            "matching_policy": RUNTIME_POLICY, "evidence_limit": evidence_limit}
