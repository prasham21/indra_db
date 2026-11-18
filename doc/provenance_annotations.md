# Evidence provenance: intermediate SQLite integration

This adds optional provenance annotations to existing statement JSON responses.
It builds on the existing correctness integration without changing its
`llm_verification` or correctness calculations. It also provides a dedicated
`GET /provenance/classifications` endpoint, alongside the existing response enrichment.
The existing indralab-vue checkout now includes a local frontend preview.
Production deployment, migration into PostgreSQL, and continuous classification
of new evidence remain outside this checkpoint.

## Data flow

1. Import the existing classification checkpoint and its input dataset into a
   **candidate** SQLite file. Classification is not rerun.
2. Reconcile candidate pairs against the target INDRA readonly database.
   `FastRawPaLink` is the statement/evidence relation used by the REST queries.
   It supplies the current statement hash, raw evidence JSON source hash, and
   publication identifier. Both hashes and the publication must match.
   Reads are batched by statement hash under a repeatable-read transaction.
3. Publish a **serving** SQLite file containing only matched pairs in
   `evidence_provenance`. Excluded rows remain in `unmatched`; conflicting
   classification variants remain in `conflicts` for audit.
4. Configure the REST process to read the serving file. After the normal query
   and evidence formatting, `StatementApiCall.produce_response` calls
   `add_provenance_annotations` immediately before JSON serialization.
5. The lookup uses only pairs actually returned by that query and checks the
   publication again before attaching an annotation. It cannot add
   old statements or evidence to a response. Unmatched evidence has no added
   field, which means “no available annotation”, not a negative classification.

## Build

Run from this checkout, or install this checkout in the backend environment:

```bash
python -m indra_db_service.provenance_build build \
  --dataset /path/to/input/dataset.sqlite \
  --results /path/to/benchmark_merged/results.sqlite \
  --config /path/to/benchmark_merged/config.json \
  --run-id gpt-oss-120b-provenance-v7 \
  --taxonomy-version v7 \
  --output /path/to/annotations/candidate-v1.sqlite
```

`dataset.json` must be alongside `dataset.sqlite`. The importer verifies its
checksum and its agreement with `config.json`. Individual checkpoints must
match their stored run digest. Existing merged checkpoints have no digest
table, so they require their original `coverage.json` alongside `results.sqlite`,
with complete coverage matching the dataset and configured range. Keep these
artifacts together: the legacy merged format cannot cryptographically bind a
config to a result file. The new annotation metadata records source file
checksums for subsequent audit.

Input is streamed from SQLite; the multi-gigabyte JSON export is not loaded.
The result index joins to `evidence.ordinal`, not an INDRA identifier. The
statement hash and source hash come from the original input payloads. The
prompt-local `evidence_index` is removed from the annotation. All other
classification fields and deterministic overrides are preserved.

Identical duplicates from the same publication collapse. Any differing annotation
or publication for a pair quarantines
**all** variants of that pair, including later repeats; there is no last-write
wins policy. Invalid/missing hashes are counted and skipped. Original input and
result files are read-only and retained. Missing or contradictory publication
identifiers are counted and excluded from serving candidates.

Schema version 2 stores an internal `_publication_key` alongside the annotation.
It prefers PMID, then PMCID, then DOI. This field is removed from the returned
classification object. A disagreement between PMID and `text_refs.PMID` fails
closed. A missing or changed identifier in the current response also leaves
the evidence unannotated; identifier aliases are not guessed. Rebuild version 1
databases from the saved inputs; the reader deliberately rejects them.

### Why publication matching and quarantine are required

An audit of all 31,621 conflicting pairs in the initial full import traced
65,137 occurrences back to the original dataset and saved result indices.
There were no result mapping errors or source-hash recomputation mismatches.
Every conflicting pair had the same sentence text and belonged to a single
original statement row, but its evidence records had different metadata.
30,966 pairs reconstructed identical prompts; 655 had differing prompts.
657 pairs spanned multiple PMIDs. The prompt/storage source files matched their
recorded run checksums.

INDRA's source hash uses source API, source ID, and text when present; PMID is
only used when text is absent. Thus it is not a unique publication-aware evidence
identifier. For example, the same sentence can be primary data in one paper
and background in another. Even identical prompts had different saved outputs
from separate inference calls.

For this intermediate release, retain all differing variants in `conflicts`
and serve none of them. This includes wording-only disagreements. Do not select
the last result, vote on labels, or combine explanations. This intentionally
reduces coverage but avoids silently resolving disagreements. Original outputs
remain available for a later explicit resolution policy; no inference is rerun.

## Reconcile with the target database

Use the backend's configured readonly DB name and the actual release/snapshot
identifier supplied by the deployment owner:

```bash
python -m indra_db_service.provenance_build reconcile \
  --candidate /path/to/annotations/candidate-v1.sqlite \
  --database primary \
  --target-snapshot ACTUAL_INDRA_DB_RELEASE \
  --output /path/to/annotations/serving-v1.sqlite
```

This command needs the normal INDRA DB dependencies, database configuration,
and read access. It prints matched/unmatched counts and records the snapshot,
timestamp, and report in `metadata`. The supplied snapshot label is a deployment
record, not an automatically discovered database release. A failed query does
not publish a partially reconciled file.

Publication matching uses the same effective metadata as statement responses:
the first evidence record in each raw statement, with publication identifiers
overlaid from `ReadingRefLink`. This avoids comparing an obsolete raw JSON PMID
against a corrected publication identifier served by the API. The reconciliation
report also counts quarantined conflicting pairs separately from matches.

Repeat reconciliation **from the original candidate** after each INDRA DB
refresh. That allows previously unmatched pairs to become eligible. Current
response-pair matching remains a second safeguard if the database changes
before reconciliation. The dedicated endpoint also queries current evidence
before looking up annotations; it never serves historical SQLite rows alone.

## Full candidate coverage with request-time matching

When a current readonly database/export is unavailable, explicitly prepare the
full candidate for matching against the evidence returned by each current API
query:

```sh
python -m indra_db_service.provenance_build prepare-runtime \
  --candidate /path/to/annotations/candidate-v2.sqlite \
  --output /path/to/annotations/runtime-serving-v1.sqlite
```

Preparation makes a separate atomic copy, checks SQLite integrity and annotation
structure, and rejects any serving pair also present in the conflict table. It
preserves the original pending candidate. The copy has `validation_status` set
to `runtime_matching` and an explicit versioned `matching_policy`; it is **not**
marked `validated`. Pending files remain rejected. Configure the REST environment
variable or preview `--database` to use this new file.

The reader still uses exact statement hash, evidence source hash and publication
identity from the current response. It never inserts additional statements or
evidence, never serves conflicts, and never falls back to text-only matching.
Thus obsolete entries may remain stored in SQLite but cannot add obsolete
records to an API response. Queries use the compound primary-key index, not a
full corpus scan. A valid annotation is visible only when its evidence is
returned by the current query, subject to the API's normal limits/pagination.

This mode does not calculate corpus-wide current coverage or identify every
outdated candidate. Full snapshot reconciliation remains a separate step; run
it from the original candidate when current DB access becomes available. Health
metadata distinguishes this mode from snapshot validation.

## Enable the REST integration

```bash
export INDRA_DB_API_PROVENANCE_DB=/path/to/annotations/serving-v1.sqlite
```

Set this in the REST service/container environment and restart its workers.
Deploy the same versioned file to each replica. Files open in SQLite read-only
mode, one connection per response, explicitly closed afterward. Annotation
queries use at most 400 pairs per batch (800 bind parameters).

Unset the environment variable to disable this feature. Missing, corrupt,
unprepared, or incompatible annotation files produce a server warning and
leave ordinary query responses usable. No partial annotations are applied if
a lookup fails midway. Monitor warnings during deployment.

Example additional evidence field (existing evidence fields remain):

```json
{
  "provenance_classification": {
    "level_1": "primary_data",
    "level_1_confidence": "high",
    "level_2": "experimental",
    "level_3_dim_a": "loss_of_function",
    "level_3_dim_b": "expression_analysis",
    "omics_subtype": null,
    "reasoning": "The authors report their own experiment.",
    "overrides": [],
    "run_id": "gpt-oss-120b-provenance-v7",
    "taxonomy_version": "v7"
  }
}
```

Field names are preserved from the classification pipeline. `reasoning` is the
explanation. Null values remain null. Hashes are parsed exactly as signed 64-bit
integers; floats are rejected to prevent precision loss. The added field is for
JSON/JSON-JS consumers; this change does not render it in legacy server HTML.

## Dedicated provenance endpoint

`GET /provenance/classifications` uses the normal statement query and source
permissions before looking up annotations. Standard statement responses continue
to carry `provenance_classification`; the Vue client uses those responses.

- `statement`: required; 1–100 signed 64-bit hashes, comma-separated or repeated.
- `explanation`: defaults to `false`; `true` includes the saved `reasoning`.
- `ev_limit`: default 1000, maximum 10000 evidence records per statement.

The response is keyed by statement hash and includes classifications, original
source hashes, publication keys, evidence checked, total evidence and an
`all_evidence_checked` indicator. A bounded response cannot prove an absent
record is outdated. Invalid parameters return 400; unavailable classification
storage returns 503. Ordinary statement responses remain usable when optional
provenance storage is absent.

## Checks

```sh
python -m unittest discover -s tests -p test_provenance.py
python -m unittest discover -s tests/integration -p test_provenance_rest.py
PROVENANCE_TEST_POSTGRES_URL='postgresql:///provenance_test?host=/SOCKET&port=PORT' \
  python -m unittest discover -s tests/integration -p test_provenance_postgres.py
```

The PostgreSQL suite requires a disposable local database named provenance_test,
refuses pre-existing readonly tables and removes only its own fixture schemas.
Read-only live reconciliation is separate from these fixture tests. Creating or
populating PostgreSQL ranking tables is a separate offline operation; see
[evidence_ranking.md](evidence_ranking.md).
