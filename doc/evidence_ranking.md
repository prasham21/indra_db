# Evidence ranking within statements

Optional server-side ranking combines saved correctness and provenance labels.
Evidence is ranked before the per-statement limit; statement selection and
existing authorization/filter semantics are retained. No inference runs during
requests. Classification details currently remain in SQLite sidecars; PostgreSQL
stores imported payloads and the precomputed priorities used for retrieval.

## Policy `evidence-priority-v2` (matching revised 2026-11-17)

| Priority | Condition |
|---:|---|
| 10 | verified correct + primary data |
| 20 | verified correct + citation |
| 30 | verified correct + background |
| 40 | verified correct + other / unavailable provenance |
| 50 | correctness unknown, uncertain, unmapped or conflicting |
| 60 | verified incorrect + primary data |
| 70 | verified incorrect + citation |
| 80 | verified incorrect + background |
| 90 | verified incorrect + other / unavailable provenance |

Correctness follows Haohang's existing statement-hash + source-hash contract;
it does not require an original publication identifier. Provenance additionally
requires matching publication identity (ReadingRefLink overlay applied,
conservative PMID > PMCID > DOI key). Matching policy:
`provenance-publication-correctness-pair-v2`. The nine numeric priorities are
unchanged; new generations must be built because matching semantics changed.
Ties are broken by ascending raw evidence record id (determinism only).
Reasons are stored separately from the priority:

- correctness status: `verified_correct`, `verified_incorrect`,
  `verified_uncertain`, `unrecognized_verdict`, `conflicting`,
  `absent`,
  `evidence_identity_unavailable`
- provenance status: `primary_data`, `citation`, `background`, `other`,
  `unrecognized_label`, `conflicting`, `publication_mismatch`,
  `missing_publication`, `absent`, `evidence_identity_unavailable`

The saved public API sample omits original publication identifiers; the full
server SQLite file has not been inspected. Missing publication metadata does
not block pair-keyed correctness in v2. Existing display fields and relation
indicators remain unchanged. Correctness status names `publication_unverified`
and `publication_mismatch` retain their numeric codes for historical v1 data,
but v2 does not emit them. Older generations cannot be served, resumed,
published or rolled back with this code policy.

Code: `indra_db/client/readonly/evidence_ranking.py` (single source for the
Python reference and the generated SQL lookup).

## Storage (`annotation` schema, same database as `readonly`)

| Table | Purpose |
|---|---|
| `annotation_run` | one row per imported run (kind, source digest, batch size, synthetic flag, publication-verifiable flag, import report) |
| `import_batch` | committed immutable batches with content digest (resume/idempotence) |
| `annotation_payload` | accepted / conflicting / missing-publication payloads once per (run, statement, source, payload digest); JSONB payload incl. explanations |
| `evidence_identity` + `identity_batch` | per readonly snapshot: (mk_hash, raw id, served source hash, served publication key, evidence element count) for annotated statements only |
| `ranking_generation`, `generation_batch`, `generation_statement` | immutable release: snapshot label + fingerprint, policy/matching versions, pinned runs, scope, status, coverage audit |
| `evidence_priority` | narrow serving table: one row for EVERY current evidence record in scope (unclassified included). PK `(generation_id, mk_hash, raw_record_id)`, retrieval index `(generation_id, mk_hash, priority, raw_record_id)` |
| `active_generation` | single-row pointer (+ one rollback candidate) switched atomically |

The readonly tables are not modified. The ranked query joins
`readonly.fast_raw_pa_link` by `id`; the ORM/`create_all` build gives that
column a primary-key index. Verify on the live database before rollout
(`\d readonly.fast_raw_pa_link`); without an index on `id` the join
degrades to a per-statement hash scan.

## Offline pipeline

Correctness SQLite rows stream into bounded batches. Schema inspection reads
at most the first usable row ahead; the iterator closes on success or failure.
The CLI no longer builds a full-dataset Python list.

```sh
python -m indra_db_service.evidence_ranking_build --database-url URL init-schema
python -m indra_db_service.evidence_ranking_build --database-url URL import-provenance --sqlite serving.sqlite
python -m indra_db_service.evidence_ranking_build --database-url URL import-correctness --sqlite evidence_llm.sqlite --run-id RUN
python -m indra_db_service.evidence_ranking_build --database-url URL build-identity --snapshot-label SNAP --run-id PROV_RUN --run-id CORR_RUN
python -m indra_db_service.evidence_ranking_build --database-url URL build-generation --snapshot-label SNAP --provenance-run PROV_RUN --correctness-run CORR_RUN
python -m indra_db_service.evidence_ranking_build --database-url URL publish --generation N
python -m indra_db_service.evidence_ranking_build --database-url URL rollback
python -m indra_db_service.evidence_ranking_build --database-url URL status
```

- Non-local hosts are refused unless `--allow-remote-host` is passed.
- Imports are batched (`COPY` into a temp staging table, one transaction per
  batch, digest recorded). Re-running skips committed batches; a changed
  batch, or the same run id with a different source digest/kind/batch size,
  is rejected. After all batches a set-based step marks every pair with more
  than one distinct payload `conflicting` (all variants kept, never
  last-write-wins). `--dry-run` validates and counts without writing.
- Adapters: schema-2 provenance SQLite (candidate, runtime or validated;
  quarantined conflicts imported as `conflicting`); Haohang's
  `llm_evidence_curation` SQLite (textless rule rows skipped; publication
  only from columns present in that file — `publication_key` or
  `pmid`/`pmcid`/`doi` — never from current evidence); labeled JSONL
  fixtures (run marked `synthetic`).
- Identity build decodes raw JSON only for statements with annotations, in
  REPEATABLE READ batches, mirroring the served evidence (`evidence[0]` plus
  ReadingRefLink overlay).
- Generation build: one transaction per signed-64-bit hash range; set-based
  INSERT from the evidence universe LEFT JOIN identity LEFT JOIN LATERAL
  per-run resolution; priority from a VALUES lookup generated from the
  Python policy. Resumable. Precondition: identity rows exist for every
  annotated statement present in the snapshot.
- Audit (anti-joins, not counts): no evidence record without a priority row,
  no priority row for a record missing from the snapshot, no duplicate
  `(mk_hash, id)`, all batches committed; plus distributions, multi-evidence
  raw records and one-to-many annotations. Only then `ready`.
- `publish` requires `ready` + passing audit; keeps the previous active
  generation as rollback candidate and retires older ones (rows retained
  until `purge`, so open cursors can finish).
- A scope can be restricted to listed statements (`--statements-file`) for
  development. Out-of-scope statements then keep the legacy evidence set,
  in raw-id order, flagged `ranked: false`.

## API (additive, opt-in)

Enable on the server with `INDRA_DB_API_EVIDENCE_RANKING=1` (default off) and
set `INDRA_DB_API_EVIDENCE_CURSOR_SECRET` (otherwise cursors are only valid
within one worker process). Optional `INDRA_DB_API_RANKING_SNAPSHOT=<label>`
refuses generations built for another snapshot.

Request (statement result type, routes `from_agents`, `from_hash/<h>`,
`from_hashes`, `from_papers`, `from_paper/...`, `from_agent_json`):

- `evidence_sort=classification` — rank evidence within each statement
  before the per-statement limit. `from_simple_json` is rejected (its evidence
  filter cannot be reproduced by a continuation request).
- `ev_limit` — page size, 1..100 in this mode (default 10; 100 for
  `from_hash`). Legacy defaults/limits are unchanged without `evidence_sort`.
- `evidence_cursor` — only on `statements/from_hash/<h>`; must come with
  `evidence_sort=classification` and the same `mesh_ids`/`paper_ids`/
  `filter_ev` and the same permission level as the issuing request.

Response additions (JSON formats; existing fields unchanged):

```json
"evidence_ranking": {"mode": "classification", "policy_version": "evidence-priority-v2",
  "matching_policy_version": "provenance-publication-correctness-pair-v2",
  "generation_id": "1", "snapshot_label": "...", "scope": "all",
  "provenance_run_id": "...", "correctness_run_id": null, "page_size": 10},
"evidence_pages": {"<mk_hash>": {"has_more": true, "next_cursor": "<opaque>",
  "returned": 10, "ranked": true, "record_ids": ["123", "..."]}}
```

and per evidence `evidence_ranking: {raw_record_id, priority,
correctness_status, provenance_status}` (re-attached after `with_english`
formatting). Statements are ordered explicitly by the requested sort key
(DESC) then hash. `evidence_counts`/`total_evidence` remain global totals,
not filtered/authorized counts; use `has_more` for paging.

Errors: 400 malformed/tampered/oversized cursor or out-of-range `ev_limit`;
409 cursor for another statement, changed filters or access, expired
(purged) generation, policy change; 503 feature disabled, no ready active
generation, missing tables, or snapshot mismatch. Legacy requests are never
affected by these conditions.

`/provenance/classifications` and the response enrichment from the
provenance SQLite are unchanged.

## Example

```sh
curl 'http://127.0.0.1:PORT/statements/from_hash/-36025197998376045?evidence_sort=classification&ev_limit=10&format=json-js'
curl 'http://127.0.0.1:PORT/statements/from_hash/-36025197998376045?evidence_sort=classification&ev_limit=10&format=json-js&evidence_cursor=<next_cursor>'
```

## Tests

```sh
python -m unittest tests.test_evidence_ranking
RANKING_TEST_POSTGRES_URL='postgresql:///ranking_test?host=/SOCKET&port=PORT' \
  python -m unittest tests.integration.test_evidence_ranking_postgres
```

The PostgreSQL suite refuses non-local URLs, any database not named
`ranking_test`, and pre-existing `readonly`/`annotation` schemas; it drops
both schemas afterwards.

## Request-time query shape (final, measured)

Per selected statement (unchanged hash query, same sort/limits):

```sql
LEFT JOIN LATERAL (
  SELECT p.priority, p.raw_record_id, p.correctness_status, p.provenance_status
  FROM annotation.evidence_priority p
  WHERE p.generation_id = :g AND p.mk_hash = mk_hashes.mk_hash
    AND EXISTS (SELECT 1 FROM readonly.fast_raw_pa_link f <legacy filter joins>
                WHERE f.id = p.raw_record_id AND f.mk_hash = p.mk_hash
                  AND <legacy filter predicates> OFFSET 0)   -- only if filtered
    AND (p.priority, p.raw_record_id) > (:after_p, :after_id) -- only with cursor
  ORDER BY p.priority, p.raw_record_id LIMIT :page_size + 1) ranked ON true
LEFT JOIN readonly.fast_raw_pa_link ev_frp ON ev_frp.id = ranked.raw_record_id ...
ORDER BY mk_hashes.<sort key> DESC, mk_hashes.mk_hash, priority, raw_record_id
```

Two earlier shapes were measured and rejected: joining `fast_raw_pa_link`
inside the lateral made PostgreSQL hash-join and sort every record of a
large statement (it estimates ~4 records per statement); letting the planner
semi-join the EXISTS for MeSH filters read every record of large statements.
`OFFSET 0` keeps the EXISTS a per-row probe on the ordered index scan, the same
cost profile as the legacy lateral (which probes in physical order).


## Verification scope

Regression tests cover priorities, import recovery/conflicts, filters, cursor
traversal, generation activation/rollback, and database connection ownership.
Local browser checks use bounded real public evidence and saved classifications;
SQL edge cases use explicitly synthetic disposable fixtures. These establish
local behavior, not complete live-snapshot coverage or production-scale latency.
A deployment must prepare a generation against its actual readonly snapshot,
provide matching sidecar files and configure a shared cursor-signing secret.
The offline builder writes only annotation-owned tables. The web API reads them.

English formatting preserves stored evidence source hashes, including when
restricted-source text was already redacted. Labels and ranking therefore use
the same identity. Caller-supplied database handles remain caller-owned;
query-created handles are closed after success and failure.
