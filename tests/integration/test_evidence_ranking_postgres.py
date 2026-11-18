"""Evidence ranking against a REAL disposable local PostgreSQL database.

Set RANKING_TEST_POSTGRES_URL to an empty local database named ranking_test
(no readonly/annotation schemas). Both schemas are created here and dropped
afterwards. Without the URL the suite is skipped (a skip is not a pass).

All statements, evidence, publications and correctness judgments below are
SYNTHETIC fixtures. Provenance fixtures go through the real schema-2 SQLite
builder/runtime-preparation code and the real importer adapter.
"""
import functools
import json
import os
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.integration.ranking_fixture import (
    check_disposable_url, create_readonly, drop_fixture_schemas,
    load_statements, real_source_hash)

INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1
S_BIG, S_MESH, S_MAX, S_MIN = 4242, 5151, INT64_MAX - 1, INT64_MIN + 1
PUB = {10: 'PMID:222', 11: 'PMID:333', 12: 'DOI:10.1000/xyz'}
REFS = {10: {'pmid': '222'}, 11: {'pmid': '333'},
        12: {'pmid': None, 'doi': '10.1000/xyz'}}


def _spec():
    """Deterministic synthetic statements plus expected judgments."""
    rng = random.Random(20261115)
    statements, prov, corr = [], [], []
    labels = ['primary_data', 'citation', 'background', 'other']
    big = []
    for i in range(40):
        rid = 100 + i
        src = 'medscan' if i % 5 == 4 else ('reach' if i % 2 else 'sparser')
        big.append({'id': rid, 'src': src, 'source_hash': 900000 + rid,
                    'rid': 10 if i % 3 else 11})
        if i % 4 != 3:           # ~1/4 of records stay unclassified
            prov.append((S_BIG, 900000 + rid, labels[i % 4],
                         PUB[big[-1]['rid']]))
        if i % 3 == 0:
            corr.append({'statement_hash': S_BIG,
                         'source_hash': 900000 + rid,
                         'publication_key': PUB[big[-1]['rid']],
                         'verdict': 'correct' if i % 2 else 'incorrect'})
    # Best evidence deep in the original order (largest id): correct+primary.
    best = big[-1]
    best.update(src='reach', rid=10)
    prov = [p for p in prov if p[1] != best['source_hash']]
    corr = [c for c in corr if c['source_hash'] != best['source_hash']]
    prov.append((S_BIG, best['source_hash'], 'primary_data', 'PMID:222'))
    corr.append({'statement_hash': S_BIG, 'source_hash': best['source_hash'],
                 'publication_key': 'PMID:222', 'verdict': 'correct'})
    # A restricted (medscan) record that would otherwise rank first.
    big[4].update(src='medscan', rid=10)
    prov = [p for p in prov if p[1] != big[4]['source_hash']]
    corr = [c for c in corr if c['source_hash'] != big[4]['source_hash']]
    prov.append((S_BIG, big[4]['source_hash'], 'primary_data', 'PMID:222'))
    corr.append({'statement_hash': S_BIG, 'source_hash': big[4]['source_hash'],
                 'publication_key': 'PMID:222', 'verdict': 'correct'})
    # One annotation legitimately applying to two current records.
    dup_text = 'SYNTHETIC A activates B (sentence read twice).'
    big[5].update(source_hash=777777, rid=10, src='reach', text=dup_text)
    big[6].update(source_hash=777777, rid=10, src='reach', text=dup_text)
    prov = [p for p in prov if p[1] not in (900105, 900106)]
    corr = [c for c in corr if c['source_hash'] not in (900105, 900106)]
    prov.append((S_BIG, 777777, 'citation', 'PMID:222'))
    corr.append({'statement_hash': S_BIG, 'source_hash': 777777,
                 'publication_key': 'PMID:222', 'verdict': 'correct'})
    # Edge cases on specific records.
    big[10].update(rid=10, src='reach')
    big[11].update(rid=10, src='reach')
    big[12].update(rid=10, src='reach')
    big[13].update(rid=10, src='reach')
    big[14].update(rid=12, src='reach')
    for rid in (110, 111, 112, 113, 114, 115, 116):
        prov = [p for p in prov if p[1] != 900000 + rid]
        corr = [c for c in corr if c['source_hash'] != 900000 + rid]
    prov += [(S_BIG, 900110, 'primary_data', 'PMID:222'),
             (S_BIG, 900110, 'citation', 'PMID:222'),       # conflict
             (S_BIG, 900111, 'primary_data', 'PMID:999'),   # mismatch
             (S_BIG, 900112, 'background', 'PMID:222'),
             (S_BIG, 900113, 'primary_data', 'PMID:222'),
             (S_BIG, 900114, 'background', 'DOI:10.1000/xyz')]
    corr += [
        {'statement_hash': S_BIG, 'source_hash': 900110,
         'publication_key': 'PMID:222', 'verdict': 'correct'},
        {'statement_hash': S_BIG, 'source_hash': 900111,
         'publication_key': 'PMID:222', 'verdict': 'incorrect'},
        {'statement_hash': S_BIG, 'source_hash': 900112,
         'verdict': 'correct'},                               # pair-only match
        {'statement_hash': S_BIG, 'source_hash': 900113,
         'publication_key': 'PMID:222', 'verdict': 'uncertain'},
        {'statement_hash': S_BIG, 'source_hash': 900114,
         'publication_key': 'DOI:10.1000/xyz', 'verdict': 'incorrect'},
        {'statement_hash': S_BIG, 'source_hash': 900115,
         'publication_key': 'PMID:222', 'verdict': 'maybe'},  # unrecognized
        {'statement_hash': S_BIG, 'source_hash': 900116,
         'publication_key': 'PMID:222', 'verdict': 'correct'},
        {'statement_hash': S_BIG, 'source_hash': 900116,
         'publication_key': 'PMID:222', 'verdict': 'incorrect'},  # conflict
    ]
    big[15].update(rid=10, src='reach')
    big[16].update(rid=10, src='reach')
    statements.append({'mk_hash': S_BIG, 'subj': 'A', 'obj': 'B',
                       'records': big, 'belief': 0.6})
    # MeSH: several records carry two requested terms (join multiplicity).
    mesh = []
    for i in range(12):
        rid = 300 + i
        terms = [[1, 2], [2], [3], [1], [1, 2, 3], [4]][i % 6]
        mesh.append({'id': rid, 'src': 'reach', 'source_hash': 930000 + rid,
                     'rid': 10, 'mesh': terms})
        if i % 2:
            prov.append((S_MESH, 930000 + rid, labels[(i // 2) % 3],
                         'PMID:222'))
    statements.append({'mk_hash': S_MESH, 'subj': 'C', 'obj': 'D',
                       'records': mesh, 'belief': 0.9})
    statements.append({'mk_hash': S_MAX, 'subj': 'E', 'obj': 'F', 'belief': 0.7,
                       'records': [{'id': 400 + i, 'src': 'reach',
                                    'source_hash': 940000 + i, 'rid': 11}
                                   for i in range(3)]})
    prov.append((S_MAX, 940002, 'primary_data', 'PMID:333'))
    corr.append({'statement_hash': S_MAX, 'source_hash': 940002,
                 'publication_key': 'PMID:333', 'verdict': 'incorrect'})
    corr.append({'statement_hash': S_MAX, 'source_hash': 940001,
                 'publication_key': 'PMID:999', 'verdict': 'correct'})
    statements.append({'mk_hash': S_MIN, 'subj': 'G', 'obj': 'H', 'belief': 0.1,
                       'records': [{'id': 500, 'src': 'sparser',
                                    'source_hash': 950000, 'rid': 10}]})
    # Replace symbolic source hashes by the hashes INDRA computes itself, so
    # they survive Evidence JSON round trips (e.g. with_english formatting).
    mapping = {}
    for s in statements:
        for r in s['records']:
            real = real_source_hash(s['subj'], s['obj'], r)
            assert mapping.setdefault(r['source_hash'], real) == real
            r['source_hash'] = real
    prov = [(sh, mapping[eh], label, pub) for sh, eh, label, pub in prov]
    corr = [dict(c, source_hash=mapping[c['source_hash']]) for c in corr]
    rng.shuffle(prov)
    return statements, prov, corr


def expected_rows(statements, prov, corr):
    """Python reference resolution (independent of the SQL builder)."""
    from indra_db.client.readonly.evidence_ranking import priority_for
    prov_by_pair, corr_by_pair = {}, {}
    for sh, eh, label, pub in prov:
        prov_by_pair.setdefault((sh, eh), set()).add((label, pub))
    for c in corr:
        corr_by_pair.setdefault((c['statement_hash'], c['source_hash']),
                                set()).add((c['verdict'],
                                            c.get('publication_key')))
    rows = {}
    for s in statements:
        for r in s['records']:
            pair = (s['mk_hash'], r['source_hash'])
            current = PUB[r['rid']]
            p = prov_by_pair.get(pair)
            if not p:
                p_status = 'absent'
            elif len(p) > 1:
                p_status = 'conflicting'
            else:
                label, pub = next(iter(p))
                p_status = label if pub == current else 'publication_mismatch'
            c = corr_by_pair.get(pair)
            if not c:
                c_status = 'absent'
            elif len(c) > 1:
                c_status = 'conflicting'
            else:
                verdict, pub = next(iter(c))
                # Correctness follows statement/source identity; the paper
                # check above applies only to provenance classifications.
                c_status = {'correct': 'verified_correct',
                            'incorrect': 'verified_incorrect',
                            'uncertain': 'verified_uncertain'
                            }.get(verdict, 'unrecognized_verdict')
            rows[(s['mk_hash'], r['id'])] = (
                priority_for(c_status, p_status), c_status, p_status,
                r['src'])
    return rows


@unittest.skipUnless(os.environ.get('RANKING_TEST_POSTGRES_URL'),
                     'Requires a disposable local PostgreSQL database')
class EvidenceRankingPostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from sqlalchemy import create_engine
        from sqlalchemy.engine.url import make_url
        url = make_url(os.environ['RANKING_TEST_POSTGRES_URL'])
        check_disposable_url(url, 'ranking_test')
        cls.url = url
        cls.engine = create_engine(url)
        cls.addClassCleanup(cls.engine.dispose)
        cls.runtime = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.runtime.cleanup)
        cls.root = Path(cls.runtime.name)
        cls.environment = patch.dict(os.environ, {
            'TESTING_DB_APP': '1', 'AWS_EC2_METADATA_DISABLED': 'true',
            'PYSTOW_HOME': cls.runtime.name,
            'INDRA_DB_API_VUE_ROOT': cls.runtime.name,
            'INDRA_DB_API_EVIDENCE_RANKING': '1',
            'INDRA_DB_API_EVIDENCE_CURSOR_SECRET': 'test-only-secret',
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)
        from indra_db.databases import ReadonlyDatabaseManager
        cls.ro = ReadonlyDatabaseManager(str(url), protected=True)
        create_readonly(cls.engine, cls.ro)
        cls.addClassCleanup(drop_fixture_schemas, cls.engine)
        cls.statements, cls.prov, cls.corr = _spec()
        with cls.engine.begin() as conn:
            load_statements(conn, cls.statements, REFS)
        cls.expected = expected_rows(cls.statements, cls.prov, cls.corr)

        from indra_db_service import evidence_ranking_build as b
        from indra_db_service.provenance_build import build, prepare_runtime
        cls.b = b
        candidate = cls.root / 'candidate.sqlite'
        build([(sh, eh, {'level_1': label, 'level_2': 'experimental',
                         'reasoning': f'SYNTHETIC {label}',
                         '_publication_key': pub})
               for sh, eh, label, pub in cls.prov],
              candidate, 'synthetic-prov-run', 'v7')
        cls.serving = cls.root / 'serving.sqlite'
        prepare_runtime(candidate, cls.serving)
        cls.corr_file = cls.root / 'correctness.jsonl'
        cls.corr_file.write_text('\n'.join(json.dumps(c) for c in cls.corr))

        b.ensure_schema(cls.engine)
        cls.prov_report = b.import_run(
            cls.engine, 'synthetic-prov-run', 'provenance',
            b.iter_provenance_sqlite(cls.serving), 'provenance-sqlite-v2',
            source_sha256='fixture-prov', batch_size=7)
        cls.corr_report = b.import_run(
            cls.engine, 'synthetic-corr-run', 'correctness',
            b.iter_jsonl(cls.corr_file, 'correctness'),
            'synthetic-fixture-jsonl', source_sha256='fixture-corr',
            synthetic=True, batch_size=5)
        b.build_identity(cls.engine, 'synthetic-snapshot-1',
                         ['synthetic-prov-run', 'synthetic-corr-run'],
                         batch_size=2)
        cls.gen = b.create_generation(
            cls.engine, 'synthetic-snapshot-1', 'synthetic-prov-run',
            'synthetic-corr-run', batch_count=16, notes='SYNTHETIC fixture')
        cls.gen_report = b.build_generation(cls.engine, cls.gen)
        b.publish(cls.engine, cls.gen)

        cls.ro.grab_session()
        cls.addClassCleanup(cls.ro.session.close)
        from indra_db_service.api import app
        cls.app = app
        cls.app.config['TESTING'] = True

    def setUp(self):
        for context in [
                patch('indra_db.client.readonly.query.get_ro',
                      return_value=self.ro),
                patch('indra_db_service.api.get_ro_host',
                      return_value='local-fixture'),
                patch.dict(os.environ,
                           {'INDRA_DB_API_PROVENANCE_DB': str(self.serving)})]:
            context.start()
            self.addCleanup(context.stop)
        self.addCleanup(self.ro.session.rollback)
        self.client = self.app.test_client()

    # --- helpers -------------------------------------------------------------
    def ref_order(self, mk_hash, authorized=True, keep=None):
        rows = [(prio, rid) for (h, rid), (prio, _, _, src)
                in self.expected.items()
                if h == mk_hash and (authorized or src != 'medscan')
                and (keep is None or rid in keep)]
        return [rid for _, rid in sorted(rows)]

    def get_page(self, mk_hash, size, cursor=None, key=True, **extra):
        query = dict(evidence_sort='classification', ev_limit=size, **extra)
        if cursor:
            query['evidence_cursor'] = cursor
        if key:
            query['api_key'] = 'test-only'
        return self.client.get(f'/statements/from_hash/{mk_hash}',
                               query_string=query)

    def traverse(self, mk_hash, size, key=True, **extra):
        ids, cursor, pages = [], None, 0
        while True:
            reply = self.get_page(mk_hash, size, cursor, key, **extra)
            self.assertEqual(reply.status_code, 200, reply.get_data(True))
            body = reply.get_json()
            stmt = body['statements'][str(mk_hash)]
            page = body['evidence_pages'][str(mk_hash)]
            got = [int(ev['evidence_ranking']['raw_record_id'])
                   for ev in stmt['evidence']]
            self.assertEqual(page['returned'], len(got))
            if page['has_more']:
                self.assertEqual(len(got), size)
            ids += got
            pages += 1
            cursor = page['next_cursor']
            if not page['has_more']:
                self.assertIsNone(cursor)
                return ids, pages

    # --- builder / policy ----------------------------------------------------
    def test_generation_matches_independent_reference(self):
        from indra_db.client.readonly.evidence_ranking import (
            CORRECTNESS_STATUS_NAMES, PROVENANCE_STATUS_NAMES)
        with self.engine.connect() as conn:
            rows = conn.execute(
                'SELECT mk_hash, raw_record_id, priority, correctness_status, '
                'provenance_status FROM annotation.evidence_priority '
                'WHERE generation_id = %s', self.gen).fetchall()
        got = {(h, rid): (prio, CORRECTNESS_STATUS_NAMES[c],
                          PROVENANCE_STATUS_NAMES[p])
               for h, rid, prio, c, p in rows}
        want = {k: v[:3] for k, v in self.expected.items()}
        self.assertEqual(got, want)
        cov = self.gen_report['coverage']
        self.assertTrue(cov['ok'], cov)
        self.assertEqual(cov['missing_records'], 0)
        self.assertEqual(cov['priority_rows'], len(self.expected))
        self.assertGreaterEqual(cov['one_to_many_annotations'], 1)
        # Every tier the fixture is designed to exercise is present.
        self.assertTrue({10, 20, 30, 40, 50, 60, 70, 80, 90}
                        >= {int(k) for k in cov['by_priority']})
        for status in ('conflicting', 'verified_uncertain',
                       'unrecognized_verdict', 'absent'):
            self.assertIn(status, cov['by_correctness_status'])
        for status in ('conflicting', 'publication_mismatch', 'absent'):
            self.assertIn(status, cov['by_provenance_status'])

    def test_correctness_uses_pair_identity_while_provenance_checks_paper(self):
        from indra_db.client.readonly.evidence_ranking import CORRECTNESS_STATUS
        with self.engine.connect() as conn:
            # Pair-only correctness plus matching background provenance.
            row = conn.execute(
                'SELECT priority, correctness_status, correctness_annotation_id '
                'FROM annotation.evidence_priority WHERE generation_id=%s '
                'AND mk_hash=%s AND raw_record_id=112',
                (self.gen, S_BIG)).first()
            self.assertEqual(row[:2], (30, CORRECTNESS_STATUS['verified_correct']))
            self.assertIsNotNone(row[2])
            # A publication supplied on a correctness result does not change
            # its pair-keyed meaning; paper-sensitive provenance still checks it.
            row = conn.execute(
                'SELECT priority, correctness_status, correctness_annotation_id '
                'FROM annotation.evidence_priority WHERE generation_id=%s '
                'AND mk_hash=%s AND raw_record_id=401',
                (self.gen, S_MAX)).first()
            self.assertEqual(row[:2], (40, CORRECTNESS_STATUS['verified_correct']))
            self.assertIsNotNone(row[2])
        self.assertIn('publication_mismatch',
                      self.gen_report['coverage']['by_provenance_status'])

    def test_import_reports_and_conflict_quarantine(self):
        self.assertEqual(self.prov_report['stored_by_status'].get(
            'conflicting'), 2)
        self.assertEqual(self.corr_report['stored_by_status'].get(
            'conflicting'), 2)
        again = self.b.import_run(
            self.engine, 'synthetic-corr-run', 'correctness',
            self.b.iter_jsonl(self.corr_file, 'correctness'),
            'synthetic-fixture-jsonl', source_sha256='fixture-corr',
            synthetic=True, batch_size=5)
        self.assertTrue(again.get('already_imported'))
        with self.assertRaises(self.b.BuildError):
            self.b.import_run(
                self.engine, 'synthetic-corr-run', 'correctness',
                iter([]), 'synthetic-fixture-jsonl',
                source_sha256='different', synthetic=True, batch_size=5)

    def test_import_resume_after_crash_is_idempotent(self):
        b = self.b
        records = list(b.iter_jsonl(self.corr_file, 'correctness'))
        kwargs = dict(source_sha256='resume', synthetic=True, batch_size=3)
        with self.assertRaises(RuntimeError):
            b.import_run(self.engine, 'resume-run', 'correctness',
                         iter(records), 'synthetic-fixture-jsonl',
                         fail_after_batches=2, **kwargs)
        first = b.import_run(self.engine, 'resume-run', 'correctness',
                             iter(records), 'synthetic-fixture-jsonl',
                             **kwargs)
        self.assertEqual(first['skipped_batches'], 2)
        with self.engine.connect() as conn:
            def counts(run):
                return conn.execute(
                    'SELECT status, count(*) FROM annotation.annotation_payload'
                    ' WHERE run_id=%s GROUP BY 1 ORDER BY 1', run).fetchall()
            self.assertEqual(counts('resume-run'),
                             counts('synthetic-corr-run'))
        # A committed batch whose content changed is rejected.
        changed = [dict(r) for r in records]
        changed[0]['payload'] = dict(changed[0]['payload'], verdict='correct',
                                     tampered=True)
        with self.engine.begin() as conn:
            conn.execute("UPDATE annotation.annotation_run SET "
                         "status='importing' WHERE run_id='resume-run'")
        with self.assertRaises(b.BuildError):
            b.import_run(self.engine, 'resume-run', 'correctness',
                         iter(changed), 'synthetic-fixture-jsonl', **kwargs)

    def test_generation_build_resumes_and_rejects_incomplete_identity(self):
        b = self.b
        gen = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                  'synthetic-prov-run', 'synthetic-corr-run',
                                  batch_count=16)
        with self.assertRaises(RuntimeError):
            b.build_generation(self.engine, gen, fail_after_batches=2)
        with self.engine.connect() as conn:
            status = conn.execute('SELECT status FROM annotation.'
                                  'ranking_generation WHERE generation_id=%s',
                                  gen).scalar()
        self.assertEqual(status, 'building')
        with self.assertRaises(b.BuildError):
            b.publish(self.engine, gen)   # never expose a partial generation
        report = b.build_generation(self.engine, gen)
        self.assertEqual(report['skipped_batches'], 2)
        self.assertTrue(report['coverage']['ok'])
        self.assertEqual(report['coverage']['priority_rows'],
                         len(self.expected))
        # Missing identity rows for an annotated statement block the build.
        bad = b.create_generation(self.engine, 'unbuilt-snapshot',
                                  'synthetic-prov-run', None, batch_count=4)
        with self.assertRaises(b.BuildError):
            b.build_generation(self.engine, bad)
        b.purge(self.engine, gen)
        b.purge(self.engine, bad)

    def test_correctness_sqlite_adapter_preserves_optional_publication(self):
        b = self.b
        path = self.root / 'llm.sqlite'
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE llm_evidence_curation(statement_hash '
                       'INTEGER, source_hash INTEGER, statement TEXT, '
                       'evidence_text TEXT, judgment TEXT, error_category '
                       'TEXT, explanation TEXT, PRIMARY KEY(statement_hash,'
                       'source_hash))')
            db.executemany('INSERT INTO llm_evidence_curation VALUES '
                           '(?,?,?,?,?,?,?)', [
                               (S_BIG, 900139, 'A activates B', 'SYNTHETIC',
                                'correct', None, 'fixture'),
                               (S_BIG, 900101, 'A activates B', '',
                                'correct', None, 'db rule')])
        report = {}
        rows = list(b.iter_correctness_sqlite(path, report))
        self.assertEqual(report['textless_skipped'], 1)
        self.assertEqual(report['publication_columns'], [])
        self.assertEqual([r['publication_key'] for r in rows], [None])
        with sqlite3.connect(path) as db:
            db.execute('ALTER TABLE llm_evidence_curation ADD COLUMN pmid TEXT')
            db.execute("UPDATE llm_evidence_curation SET pmid='222'")
        rows = list(b.iter_correctness_sqlite(path, {}))
        self.assertEqual([r['publication_key'] for r in rows], ['PMID:222'])

    def test_old_policy_cannot_be_published_or_used_for_rollback(self):
        b = self.b
        gen = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                  'synthetic-prov-run', 'synthetic-corr-run',
                                  batch_count=2)
        b.build_generation(self.engine, gen)
        with self.engine.begin() as conn:
            previous = conn.execute('SELECT previous_generation_id FROM '
                                    'annotation.active_generation').scalar()
            conn.execute("UPDATE annotation.ranking_generation SET "
                         "matching_policy_version='previous-policy' "
                         "WHERE generation_id=%s", gen)
            conn.execute('UPDATE annotation.active_generation SET '
                         'previous_generation_id=%s', gen)
        try:
            with self.assertRaisesRegex(b.BuildError, 'another policy'):
                b.publish(self.engine, gen)
            with self.assertRaisesRegex(b.BuildError, 'matching code policy'):
                b.rollback(self.engine)
        finally:
            with self.engine.begin() as conn:
                conn.execute('UPDATE annotation.active_generation SET '
                             'previous_generation_id=%s', previous)
            b.purge(self.engine, gen)

    # --- request-time ranking -----------------------------------------------
    def test_multi_evidence_identity_prevents_generation_publication(self):
        from sqlalchemy import text
        b = self.b
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE annotation.evidence_identity SET evidence_elements=2 "
                              "WHERE snapshot_label=:s AND raw_record_id=100"),
                         s='synthetic-snapshot-1')
        gen = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                  'synthetic-prov-run', 'synthetic-corr-run',
                                  batch_count=2)
        try:
            with self.assertRaisesRegex(b.BuildError, 'multi-evidence'):
                b.build_generation(self.engine, gen)
            with self.assertRaises(b.BuildError):
                b.publish(self.engine, gen)
        finally:
            with self.engine.begin() as conn:
                conn.execute(text("UPDATE annotation.evidence_identity SET evidence_elements=1 "
                                  "WHERE snapshot_label=:s AND raw_record_id=100"),
                             s='synthetic-snapshot-1')
            b.purge(self.engine, gen)

    def test_ranked_response_rejects_different_display_annotation_run(self):
        import shutil
        changed = self.root / 'different-display-run.sqlite'
        shutil.copy2(self.serving, changed)
        with sqlite3.connect(changed) as db:
            db.execute("UPDATE metadata SET value='different-run' WHERE key='run_id'")
        with patch.dict(os.environ, {'INDRA_DB_API_PROVENANCE_DB': str(changed)}):
            response = self.client.get('/statements/from_hash/4242',
                query_string={'format': 'json-js', 'ev_limit': 10,
                              'evidence_sort': 'classification'})
            self.assertEqual(response.status_code, 503, response.get_data(True))
            legacy = self.client.get('/statements/from_hash/4242',
                query_string={'format': 'json-js', 'ev_limit': 10})
            self.assertEqual(legacy.status_code, 200)

    def test_best_deep_evidence_first_and_full_traversal(self):
        # Best authorized-for-everyone record has the LARGEST id (deepest in
        # raw-id order); with medscan access, restricted record 104 ties it.
        self.assertEqual(self.ref_order(S_BIG, authorized=False)[0], 139)
        reference = self.ref_order(S_BIG)
        self.assertEqual(reference[:2], [104, 139])
        legacy = self.client.get(f'/statements/from_hash/{S_BIG}',
                                 query_string={'ev_limit': 10,
                                               'api_key': 'test-only'})
        self.assertEqual(legacy.status_code, 200)
        for size in (1, 7, 100):
            ids, pages = self.traverse(S_BIG, size)
            self.assertEqual(ids, reference, f'page size {size}')
            self.assertEqual(len(ids), len(set(ids)))
            self.assertEqual(pages, -(-len(reference) // size))

    def test_authorization_applies_before_limit(self):
        reference = self.ref_order(S_BIG, authorized=False)
        self.assertNotIn(104, reference)       # restricted top record
        ids, _ = self.traverse(S_BIG, 5, key=False)
        self.assertEqual(ids, reference)
        first = self.get_page(S_BIG, 5, key=True).get_json()
        cursor = first['evidence_pages'][str(S_BIG)]['next_cursor']
        reply = self.get_page(S_BIG, 5, cursor, key=False)
        self.assertEqual(reply.status_code, 409)

    def test_cursor_validation(self):
        body = self.get_page(S_BIG, 3).get_json()
        cursor = body['evidence_pages'][str(S_BIG)]['next_cursor']
        self.assertEqual(self.get_page(S_BIG, 3, cursor[:-2] + 'xx')
                         .status_code, 400)
        self.assertEqual(self.get_page(S_BIG, 3, 'a' * 2000).status_code, 400)
        self.assertEqual(self.get_page(S_MAX, 3, cursor).status_code, 409)
        self.assertEqual(self.get_page(S_BIG, 3, cursor,
                                       mesh_ids='D000001').status_code, 409)
        multi = self.client.post(
            '/statements/from_hashes', json={'hashes': [str(S_BIG)]},
            query_string={'evidence_sort': 'classification',
                          'evidence_cursor': cursor})
        self.assertEqual(multi.status_code, 400)
        for bad in ({'ev_limit': 0}, {'ev_limit': 101},
                    {'evidence_sort': 'llm'}):
            query = dict({'evidence_sort': 'classification'}, **bad)
            self.assertEqual(self.client.get(
                f'/statements/from_hash/{S_BIG}', query_string=query)
                .status_code, 400, bad)
        no_sort = self.client.get(f'/statements/from_hash/{S_BIG}',
                                  query_string={'evidence_cursor': cursor})
        self.assertEqual(no_sort.status_code, 400)

    def test_mesh_filter_does_not_duplicate_records(self):
        keep = {r['id'] for s in self.statements if s['mk_hash'] == S_MESH
                for r in s['records'] if {1, 2} & set(r.get('mesh', ()))}
        reference = self.ref_order(S_MESH, keep=keep)
        for size in (2, 3, 100):
            ids, _ = self.traverse(S_MESH, size,
                                   mesh_ids='D000001,D000002')
            self.assertEqual(ids, reference)

    def test_paper_filter(self):
        keep = {r['id'] for s in self.statements if s['mk_hash'] == S_BIG
                for r in s['records'] if r['rid'] == 11}
        ids, _ = self.traverse(S_BIG, 4, paper_ids='333@pmid')
        self.assertEqual(ids, self.ref_order(S_BIG, keep=keep))

    def test_statement_order_and_counts_unchanged(self):
        hashes = [str(h) for h in (S_MIN, S_MESH, S_BIG, S_MAX)]
        for sort_by in ('ev_count', 'belief'):
            legacy = self.client.post(
                '/statements/from_hashes', json={'hashes': hashes},
                query_string={'ev_limit': 10, 'sort_by': sort_by,
                              'api_key': 'test-only'}).get_json()
            ranked = self.client.post(
                '/statements/from_hashes', json={'hashes': hashes},
                query_string={'ev_limit': 10, 'sort_by': sort_by,
                              'api_key': 'test-only',
                              'evidence_sort': 'classification'}).get_json()
            # Ranked mode makes the documented statement order explicit
            # (sort key DESC, then hash). The legacy final SELECT has no
            # ORDER BY, so its order is plan-dependent; compare sets there.
            key = {'ev_count': lambda s: len(s['records']),
                   'belief': lambda s: s['belief']}[sort_by]
            expected = [str(s['mk_hash']) for s in
                        sorted(self.statements,
                               key=lambda s: (-key(s), s['mk_hash']))]
            self.assertEqual(list(ranked['statements']), expected, sort_by)
            self.assertEqual(set(ranked['statements']),
                             set(legacy['statements']))
            self.assertEqual(ranked['evidence_counts'],
                             legacy['evidence_counts'])
            self.assertEqual(ranked['source_counts'], legacy['source_counts'])
            for h in hashes:
                legacy_ids = {ev['annotations']['prior_uuids'][-1] for ev in
                              legacy['statements'][h]['evidence']}
                ranked_ev = ranked['statements'][h]['evidence']
                self.assertEqual(len(ranked_ev),
                                 min(10, len(self.ref_order(int(h)))))
                self.assertEqual(
                    [int(e['evidence_ranking']['raw_record_id'])
                     for e in ranked_ev], self.ref_order(int(h))[:10])
                if len(self.ref_order(int(h))) <= 10:
                    self.assertEqual(
                        {e['annotations']['prior_uuids'][-1]
                         for e in ranked_ev}, legacy_ids)
        self.assertEqual(list(ranked['evidence_pages']),
                         list(ranked['statements']))

    def test_ui_route_with_english_keeps_order_and_annotations(self):
        llm = self.root / 'llm-display.sqlite'
        source_hash = next(r['source_hash'] for s in self.statements
                           if s['mk_hash'] == S_BIG for r in s['records']
                           if r['id'] == 112)
        with sqlite3.connect(llm) as db:
            db.execute('CREATE TABLE llm_evidence_curation(statement_hash '
                       'INTEGER, source_hash INTEGER, statement TEXT, '
                       'evidence_text TEXT, judgment TEXT, error_category '
                       'TEXT, explanation TEXT)')
            db.execute('INSERT INTO llm_evidence_curation VALUES '
                       '(?,?,?,?,?,?,?)', (S_BIG, source_hash, 'x', 'SYNTHETIC',
                                           'correct', None, 'fixture'))
        from indra_db_service import llm as llm_module
        with patch('indra_db_service.call_handlers.add_llm_verifications',
                   functools.partial(llm_module.add_llm_verifications,
                                     database_path=llm)):
            reply = self.client.post(
                '/statements/from_agent_json',
                json={'agent_json': {'0': 'A', '1': 'B'},
                      'stmt_type': 'Activation', 'hashes': [str(S_BIG)]},
                query_string={'ev_limit': 10, 'format': 'json-js',
                              'with_english': 'true',
                              'with_cur_counts': 'false', 'filter_ev': 'true',
                              'evidence_sort': 'classification'})
        self.assertEqual(reply.status_code, 200, reply.get_data(True))
        body = reply.get_json()
        evs = body['statements'][str(S_BIG)]['evidence']
        page = body['evidence_pages'][str(S_BIG)]
        self.assertEqual([e['original_json']['annotations']['prior_uuids'][-1]
                          for e in evs],
                         [str(__import__('uuid').UUID(int=i))
                          for i in self.ref_order(S_BIG, False)[:10]])
        self.assertEqual(page['record_ids'],
                         [str(i) for i in self.ref_order(S_BIG, False)[:10]])
        for ev in evs:
            rank = ev['evidence_ranking']
            classification = ev.get('provenance_classification')
            if rank['provenance_status'] in ('primary_data', 'citation',
                                             'background', 'other'):
                self.assertEqual(classification['level_1'],
                                 rank['provenance_status'])
            else:
                self.assertIsNone(classification)
        # Haohang's pair-only result now contributes to ranking as displayed.
        shown = [ev for ev in evs if ev.get('llm_verification')]
        self.assertEqual(len(shown), 1)
        for ev in shown:
            self.assertEqual(ev['evidence_ranking']['correctness_status'],
                             'verified_correct')
            self.assertEqual(ev['evidence_ranking']['priority'], 30)

    def test_feature_flag_off_and_legacy_paths(self):
        with patch.dict(os.environ, {'INDRA_DB_API_EVIDENCE_RANKING': '0'}):
            self.assertEqual(self.get_page(S_BIG, 5).status_code, 503)
            legacy = self.client.get(f'/statements/from_hash/{S_BIG}',
                                     query_string={'ev_limit': 5})
        self.assertEqual(legacy.status_code, 200)
        self.assertNotIn('evidence_pages', legacy.get_json())
        zero = self.client.post('/statements/from_hashes',
                                json={'hashes': [str(S_BIG)]},
                                query_string={'ev_limit': 0})
        self.assertEqual(zero.status_code, 200)
        self.assertEqual(zero.get_json()['statements'][str(S_BIG)]['evidence'],
                         [])
        prov = self.client.get('/provenance/classifications',
                               query_string={'statement': str(S_BIG)})
        self.assertEqual(prov.status_code, 200)

    def test_generation_switch_rollback_and_expiry(self):
        b = self.b
        first = self.get_page(S_BIG, 4).get_json()
        cursor = first['evidence_pages'][str(S_BIG)]['next_cursor']
        gen2 = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                   'synthetic-prov-run', None, batch_count=4)
        b.build_generation(self.engine, gen2)
        b.publish(self.engine, gen2)
        try:
            # Pinned cursor keeps the original generation's order.
            reply = self.get_page(S_BIG, 4, cursor)
            self.assertEqual(reply.status_code, 200)
            ids = [int(e['evidence_ranking']['raw_record_id']) for e in
                   reply.get_json()['statements'][str(S_BIG)]['evidence']]
            self.assertEqual(ids, self.ref_order(S_BIG)[4:8])
            new = self.get_page(S_BIG, 4).get_json()
            self.assertEqual(new['evidence_ranking']['generation_id'],
                             str(gen2))
            gen3 = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                       'synthetic-prov-run',
                                       'synthetic-corr-run', batch_count=4)
            b.build_generation(self.engine, gen3)
            b.publish(self.engine, gen3)            # original gen retires
            self.assertEqual(self.get_page(S_BIG, 4, cursor).status_code, 200)
            self.assertEqual(b.rollback(self.engine)['active'], gen2)
            self.assertEqual(b.publish(self.engine, gen3)['active'], gen3)
        finally:
            with self.engine.begin() as conn:
                conn.execute('UPDATE annotation.active_generation SET '
                             'generation_id=%s, previous_generation_id=NULL',
                             self.gen)
                conn.execute("UPDATE annotation.ranking_generation SET "
                             "status='ready' WHERE generation_id=%s", self.gen)
        b.publish(self.engine, gen2)
        with self.engine.begin() as conn:
            conn.execute('UPDATE annotation.active_generation SET '
                         'generation_id=%s, previous_generation_id=NULL',
                         self.gen)
            conn.execute("UPDATE annotation.ranking_generation SET "
                         "status='retired' WHERE generation_id IN (%s, %s)",
                         gen2, gen3)
        b.purge(self.engine, gen2)
        b.purge(self.engine, gen3)
        cursor2 = new['evidence_pages'][str(S_BIG)]['next_cursor']
        self.assertEqual(self.get_page(S_BIG, 4, cursor2).status_code, 409)

    def _server_connections(self):
        with self.engine.connect() as conn:
            return conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = "
                "current_database() AND pid <> pg_backend_pid()").scalar()

    def test_repeated_requests_do_not_accumulate_connections(self):
        """Regression: every request built a readonly handle whose session
        stayed 'idle in transaction' and whose pool kept connections open
        until cyclic GC. Uses the REAL get_ro construction path (a new
        handle per request) with garbage collection disabled."""
        import gc
        from indra_db.util import constructors
        from sqlalchemy.pool import QueuePool
        url = str(self.url)
        made = []
        real_init = constructors.ReadonlyDatabaseManager.__init__

        def counting_init(ro, *a, **k):
            made.append(1)
            return real_init(ro, *a, **k)

        def pools():
            return [o for o in gc.get_objects() if isinstance(o, QueuePool)]

        def pooled_connections(pool_list):
            return sum(p.checkedin() + p.checkedout() for p in pool_list)

        with patch('indra_db.client.readonly.query.get_ro',
                   constructors.get_ro.__wrapped__), \
                patch('indra_db.util.constructors.get_readonly_databases',
                      return_value={'primary': url}), \
                patch.object(constructors.ReadonlyDatabaseManager, '__init__',
                             counting_init):
            gc.collect()
            base_conns = self._server_connections()
            base_pools = len(pools())
            base_pooled = pooled_connections(pools())
            gc.disable()
            try:
                for _ in range(12):
                    for extra in ({}, {'evidence_sort': 'classification'}):
                        ok = self.client.get(
                            f'/statements/from_hash/{S_BIG}',
                            query_string=dict({'ev_limit': 5}, **extra))
                        self.assertEqual(ok.status_code, 200)
                        # Failing query (invalid hash) must release too.
                        with self.assertRaises(Exception):
                            self.client.get('/statements/from_hash/not-a-hash',
                                            query_string=dict({'ev_limit': 5},
                                                              **extra))
                held = self._server_connections()
                held_pooled = pooled_connections(pools())
            finally:
                gc.enable()
            gc.collect()
            pools_after_gc = len(pools())
        self.assertEqual(len(made), 48)          # one handle per request
        self.assertLessEqual(held - base_conns, 0,
                             'connections accumulated across requests')
        # Disposed pools may await GC (reference cycles) but hold nothing.
        self.assertLessEqual(held_pooled - base_pooled, 0,
                             'pooled connections accumulated across requests')
        self.assertLessEqual(pools_after_gc - base_pools, 0,
                             'connection pools survived garbage collection')

    def test_curation_counts_release_their_handle(self):
        """Regression: with_cur_counts (sent by the web UI on every evidence
        load) built a principal-DB handle per request via get_curations()
        and left it 'idle in transaction' until cyclic GC. SYNTHETIC
        curation rows in a throwaway table of this disposable database."""
        import gc
        from indra_db.client.principal import curation as cur_mod
        from indra_db.databases import PrincipalDatabaseManager
        from indra_db.util import constructors
        first = self.get_page(S_BIG, 2, format='json-js').get_json()
        sh = [str(e['source_hash']) for e in
              first['statements'][str(S_BIG)]['evidence']]
        with self.engine.begin() as conn:
            conn.execute('CREATE TABLE public.pa_statements '
                         '(mk_hash bigint PRIMARY KEY)')
            conn.execute(
                'CREATE TABLE public.curation (id serial PRIMARY KEY, '
                'pa_hash bigint REFERENCES public.pa_statements(mk_hash), '
                'source_hash bigint, tag varchar, text varchar, curator '
                'varchar NOT NULL, auth_id integer, source varchar, ip inet, '
                'date timestamp DEFAULT now(), pa_json jsonb, ev_json jsonb)')
            conn.execute('INSERT INTO public.pa_statements VALUES (%s)', S_BIG)
            for h in (sh[0], sh[0], sh[1]):
                conn.execute("INSERT INTO public.curation (pa_hash, "
                             "source_hash, tag, curator) VALUES "
                             "(%s, %s, 'correct', 'SYNTHETIC')", S_BIG, int(h))
        self.addCleanup(self._drop_curation_tables)
        made = []
        real_init = PrincipalDatabaseManager.__init__

        def counting_init(db, *a, **k):
            made.append(1)
            return real_init(db, *a, **k)
        with patch.object(cur_mod, 'get_db', constructors.get_db.__wrapped__), \
                patch('indra_db.util.constructors.get_databases',
                      return_value={'primary': str(self.url)}), \
                patch.object(PrincipalDatabaseManager, '__init__',
                             counting_init):
            gc.collect()
            base = self._server_connections()
            gc.disable()
            try:
                for extra in ({}, {'evidence_sort': 'classification'}) * 5:
                    reply = self.client.get(
                        f'/statements/from_hash/{S_BIG}',
                        query_string=dict({'ev_limit': 2, 'format': 'json-js',
                                           'with_cur_counts': 'true',
                                           'api_key': 'test-only'},
                                          **extra))
                    self.assertEqual(reply.status_code, 200,
                                     reply.get_data(True))
                    body = reply.get_json()
                    self.assertEqual(body['num_curations'], {str(S_BIG): 3})
                    if extra:
                        per_ev = {str(e['source_hash']):
                                  e.get('num_curations')
                                  for e in body['statements'][str(S_BIG)]
                                  ['evidence']}
                        self.assertEqual(per_ev, {sh[0]: 2, sh[1]: 1})
                held = self._server_connections()
            finally:
                gc.enable()
            self.assertEqual(len(made), 10)
            self.assertLessEqual(held - base, 0,
                                 'curation connections accumulated')
            # A caller-owned handle is used, not closed.
            db = PrincipalDatabaseManager(str(self.url))
            db.grab_session()
            self.addCleanup(db.close)
            with patch.object(PrincipalDatabaseManager, 'close') as close:
                self.assertEqual(len(cur_mod.get_curations(db=db,
                                                           pa_hash=S_BIG)), 3)
                close.assert_not_called()
            self.assertEqual(db.session.execute('SELECT 4').scalar(), 4)

    def _drop_curation_tables(self):
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS public.curation, '
                         'public.pa_statements')

    def test_close_releases_and_handle_stays_usable(self):
        from indra_db.databases import ReadonlyDatabaseManager
        ro = ReadonlyDatabaseManager(str(self.url), protected=True)
        ro.grab_session()
        base = self._server_connections()
        ro.session.connection().execute('SELECT 1')
        self.assertEqual(self._server_connections(), base + 1)
        ro.close()
        ro.close()                                # idempotent
        self.assertEqual(self._server_connections(), base)
        # Documented reuse after close(): new connections are opened...
        self.assertEqual(ro.session.execute('SELECT 2').scalar(), 2)
        self.assertEqual(self._server_connections(), base + 1)
        ro.close()                                # ...and released again.
        self.assertEqual(self._server_connections(), base)

    def _real_get_ro(self, recorder=None):
        """Patch in the REAL get_ro (a new handle per call) for this DB."""
        from contextlib import ExitStack
        from indra_db.util import constructors
        real_init = constructors.ReadonlyDatabaseManager.__init__

        def recording_init(ro, *a, **k):
            if recorder is not None:
                recorder.append(ro)
            return real_init(ro, *a, **k)
        stack = ExitStack()
        stack.enter_context(patch('indra_db.client.readonly.query.get_ro',
                                  constructors.get_ro.__wrapped__))
        stack.enter_context(patch(
            'indra_db.util.constructors.get_readonly_databases',
            return_value={'primary': str(self.url)}))
        stack.enter_context(patch.object(
            constructors.ReadonlyDatabaseManager, '__init__', recording_init))
        return stack

    def test_caller_owned_handle_is_never_closed(self):
        from indra_db.client.readonly import HasHash
        from indra_db.client.readonly.evidence_ranking import \
            EvidenceRankingRequest
        from indra_db.databases import ReadonlyDatabaseManager
        ro = ReadonlyDatabaseManager(str(self.url), protected=True)
        ro.grab_session()
        self.addCleanup(ro.close)
        with patch.object(ReadonlyDatabaseManager, 'close') as close:
            for kwargs in ({}, {'evidence_ranking':
                                EvidenceRankingRequest(5)}):
                result = HasHash([S_BIG]).get_statements(ro, ev_limit=5,
                                                         **kwargs)
                self.assertIn(S_BIG, result.results)
            self.assertEqual(set(HasHash([S_BIG]).get_hashes(ro=ro).results),
                             {S_BIG})
            close.assert_not_called()
        # Still the caller's open session: its transaction continues.
        self.assertTrue(ro.session.is_active)
        self.assertEqual(ro.session.execute('SELECT 3').scalar(), 3)

    def test_single_attempt_and_no_retry_when_unreachable(self):
        from indra_db.client.readonly import HasHash
        with patch('indra_db.client.readonly.query.get_ro',
                   return_value=None) as get_ro:
            # Same failure as before the wrapper existed (no usable handle),
            # after exactly one attempt -- no second reachability timeout.
            with self.assertRaises(AttributeError):
                HasHash([S_BIG]).get_statements(ev_limit=5)
            self.assertEqual(get_ro.call_count, 1)
            # Empty queries never touch the handle, exactly as before.
            empty = HasHash([]).get_statements(ev_limit=5)
            self.assertEqual(empty.results, {})

    def test_reachability_ping_is_released(self):
        from sqlalchemy.engine.url import make_url
        from indra_db.databases import ReadonlyDatabaseManager
        base = self._server_connections()
        ro = ReadonlyDatabaseManager(str(self.url), protected=True)
        self.assertTrue(ro.available)
        self.assertEqual(self._server_connections(), base)
        missing = make_url(str(self.url))
        missing.database = 'ranking_test_does_not_exist'
        down = ReadonlyDatabaseManager(str(missing), protected=True)
        self.assertFalse(down.available)
        down.close()                               # no-op, no error
        self.assertEqual(self._server_connections(), base)

    def test_get_ro_host_releases_its_handle(self):
        from indra_db.util import constructors
        base = self._server_connections()
        with patch('indra_db.util.constructors.get_readonly_databases',
                   return_value={'primary': str(self.url)}):
            real_close = constructors.ReadonlyDatabaseManager.close
            with patch('indra_db.util.constructors.get_ro',
                       constructors.get_ro.__wrapped__), \
                    patch.object(constructors.ReadonlyDatabaseManager,
                                 'close', autospec=True,
                                 side_effect=real_close) as close:
                # Socket URL: no host part, as get_ro_host always reported.
                self.assertIsNone(constructors.get_ro_host('primary'))
        close.assert_called_once()
        self.assertEqual(self._server_connections(), base)

    def test_concurrent_requests_get_separate_handles_and_sessions(self):
        import threading
        handles, statuses, lock = [], [], threading.Lock()
        base = self._server_connections()

        def worker(n):
            client = self.app.test_client()
            for k in range(4):
                extra = ({'evidence_sort': 'classification'} if (n + k) % 2
                         else {})
                reply = client.get(f'/statements/from_hash/{S_BIG}',
                                   query_string=dict({'ev_limit': 5}, **extra))
                with lock:
                    statuses.append(reply.status_code)
        with self._real_get_ro(handles):
            threads = [threading.Thread(target=worker, args=(n,))
                       for n in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        self.assertEqual(statuses, [200] * 24)
        self.assertEqual(len(handles), 24)
        self.assertEqual(len({id(h) for h in handles}), 24)
        self.assertEqual(len({id(h.session) for h in handles}), 24)
        self.assertEqual(self._server_connections(), base)

    def test_logged_host_comes_from_config_without_connecting(self):
        from indra_db_service import api
        with patch('indra_db_service.api.get_readonly_databases',
                   return_value={'primary': 'postgresql://u@db.example/x'}):
            self.assertEqual(api._configured_ro_host('primary'), 'db.example')
        with patch('indra_db_service.api.get_readonly_databases',
                   return_value={'primary': 'postgresql://u@a/x',
                                 'override': 'postgresql://u@b/x'}):
            self.assertEqual(api._configured_ro_host('primary'), 'b')
        with patch('indra_db_service.api.get_readonly_databases',
                   side_effect=RuntimeError('no config')):
            self.assertIsNone(api._configured_ro_host('primary'))

    def test_search_page_connects_ranking_for_all_searches_when_enabled(self):
        for query in ({}, {'agent': 'OGG1', 'other_agent': 'APEX1',
                           'rel': 'Complex', 'autosearch': '1'},
                      {'agent': 'SOCS3', 'other_agent': 'IL6ST',
                       'autosearch': '1'}):
            for enabled in ('0', '1'):
                with self.subTest(query=query, enabled=enabled), patch.dict(
                        os.environ, {'INDRA_DB_API_EVIDENCE_RANKING': enabled}):
                    page = self.client.get('/search', query_string=query).get_data(True)
                    if enabled == '1':
                        self.assertEqual(page.count(
                            'Vue.prototype.$evidence_sort = "classification"'), 1)
                    else:
                        self.assertNotIn('$evidence_sort', page)

    def test_no_active_generation_is_unavailable(self):
        with self.engine.begin() as conn:
            conn.execute('DELETE FROM annotation.active_generation')
        try:
            self.assertEqual(self.get_page(S_BIG, 5).status_code, 503)
            self.assertEqual(self.client.get(
                f'/statements/from_hash/{S_BIG}',
                query_string={'ev_limit': 5}).status_code, 200)
        finally:
            with self.engine.begin() as conn:
                conn.execute('INSERT INTO annotation.active_generation '
                             '(singleton, generation_id) VALUES (true, %s)',
                             self.gen)
        with patch.dict(os.environ,
                        {'INDRA_DB_API_RANKING_SNAPSHOT': 'other-snapshot'}):
            self.assertEqual(self.get_page(S_BIG, 5).status_code, 503)

    def test_statement_scoped_generation_keeps_legacy_outside_scope(self):
        b = self.b
        scoped = b.create_generation(self.engine, 'synthetic-snapshot-1',
                                     'synthetic-prov-run',
                                     'synthetic-corr-run',
                                     statements=[S_BIG], batch_count=4)
        report = b.build_generation(self.engine, scoped)
        self.assertEqual(report['coverage']['priority_rows'], 40)
        with self.engine.begin() as conn:
            conn.execute('UPDATE annotation.active_generation SET '
                         'generation_id=%s', scoped)
        try:
            ids, _ = self.traverse(S_BIG, 6)
            self.assertEqual(ids, self.ref_order(S_BIG))
            body = self.get_page(S_MESH, 5).get_json()
            self.assertFalse(body['evidence_pages'][str(S_MESH)]['ranked'])
            evs = body['statements'][str(S_MESH)]['evidence']
            self.assertTrue(all(e['evidence_ranking']['priority'] is None
                                for e in evs))
            ids, _ = self.traverse(S_MESH, 5)
            self.assertEqual(ids, sorted(r['id'] for s in self.statements
                                         if s['mk_hash'] == S_MESH
                                         for r in s['records']))
        finally:
            with self.engine.begin() as conn:
                conn.execute('UPDATE annotation.active_generation SET '
                             'generation_id=%s', self.gen)
                conn.execute("UPDATE annotation.ranking_generation SET "
                             "status='retired' WHERE generation_id=%s", scoped)
            b.purge(self.engine, scoped)

    def test_signed_extreme_hashes_round_trip_as_strings(self):
        for h in (S_MIN, S_MAX):
            reply = self.get_page(h, 2)
            self.assertEqual(reply.status_code, 200, reply.get_data(True))
            body = json.loads(reply.get_data(True))
            self.assertIn(str(h), body['statements'])
            self.assertEqual(
                [int(e['evidence_ranking']['raw_record_id']) for e in
                 body['statements'][str(h)]['evidence']],
                self.ref_order(h)[:2])
        ids, _ = self.traverse(S_MAX, 1)
        self.assertEqual(ids, self.ref_order(S_MAX))


if __name__ == '__main__':
    unittest.main()
