"""Real SQL/Flask regression tests against a disposable local PostgreSQL DB.

Set PROVENANCE_TEST_POSTGRES_URL to an empty local database named
provenance_test. The readonly schema must not exist. This test creates it but
never drops an existing schema. Without that explicit URL the suite is skipped.
This is a synthetic database integration test, not current-corpus validation.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from indra_db_service.provenance_build import build, live_pairs, reconcile, readonly_snapshot


@unittest.skipUnless(os.environ.get('PROVENANCE_TEST_POSTGRES_URL'),
                     'Requires a disposable local PostgreSQL database')
class ProvenancePostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from sqlalchemy import create_engine, inspect
        from sqlalchemy.engine.url import make_url
        url = make_url(os.environ['PROVENANCE_TEST_POSTGRES_URL'])
        socket = url.query.get('host', '')
        if (url.database != 'provenance_test'
                or url.host not in (None, 'localhost', '127.0.0.1', '::1')
                or (socket and not socket.startswith('/'))):
            raise ValueError('Use an empty local provenance_test database')
        cls.engine = create_engine(url)
        cls.addClassCleanup(cls.engine.dispose)
        if 'readonly' in inspect(cls.engine).get_schema_names():
            raise ValueError('Refusing to modify an existing readonly schema')
        cls.runtime = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.runtime.cleanup)
        cls.environment = patch.dict(os.environ, {
            'TESTING_DB_APP': '1', 'AWS_EC2_METADATA_DISABLED': 'true',
            'PYSTOW_HOME': cls.runtime.name,
            'INDRA_DB_API_VUE_ROOT': cls.runtime.name,
        })
        cls.environment.start()
        cls.addClassCleanup(cls.environment.stop)

        from indra_db.databases import ReadonlyDatabaseManager
        from indra.statements import Agent, Activation, Evidence
        cls.ro = ReadonlyDatabaseManager(str(url), protected=True)
        if not cls.ro.available:
            raise RuntimeError('Local PostgreSQL is unavailable')
        # Use the actual mapped tables, including their column names/types.
        tables = [cls.ro.FastRawPaLink.__table__,
                  cls.ro.ReadingRefLink.__table__,
                  cls.ro.RawStmtSrc.__table__,
                  cls.ro.tables['source_meta'].__table__]
        with cls.engine.begin() as connection:
            connection.execute('CREATE SCHEMA readonly')
            cls.ro.Base.metadata.create_all(connection, tables=tables)
            connection.execute('ALTER TABLE readonly.source_meta ADD COLUMN reach bigint')
            connection.execute('ALTER TABLE readonly.source_meta ADD COLUMN sparser bigint')
            connection.execute('ALTER TABLE readonly.source_meta ADD COLUMN medscan bigint')
        def remove_fixture_schema():
            with cls.engine.begin() as connection:
                connection.execute('DROP SCHEMA readonly CASCADE')
        cls.addClassCleanup(remove_fixture_schema)
        cls.ro.grab_session()
        cls.addClassCleanup(cls.ro.session.close)
        cls.stmt = Activation(Agent('A'), Agent('B'))
        cls.sh = cls.stmt.get_hash()
        cls.evidence = [Evidence(source_api=src, text=text, pmid='111')
                        for src, text in [('reach', 'A activates B.'),
                                          ('sparser', 'A promotes B activity.'),
                                          ('medscan', 'A stimulates B.')]]
        cls.ehs = [ev.get_source_hash() for ev in cls.evidence]
        with cls.engine.begin() as connection:
            connection.execute(cls.ro.ReadingRefLink.__table__.insert(),
                               {'rid': 10, 'pmid': '222', 'source': 'pubmed'})
            for i, evidence in enumerate(cls.evidence):
                raw = Activation(Agent('A'), Agent('B'), evidence=[evidence]).to_json()
                connection.execute(cls.ro.FastRawPaLink.__table__.insert(), {
                    'id': i+1, 'raw_json': json.dumps(raw).encode(),
                    'pa_json': json.dumps(cls.stmt.to_json()).encode(),
                    'mk_hash': cls.sh, 'reading_id': 10, 'src': evidence.source_api})
                connection.execute(cls.ro.RawStmtSrc.__table__.insert(),
                                   {'sid': i+1, 'src': evidence.source_api})
            connection.execute(cls.ro.SourceMeta.__table__.insert(), {
                'mk_hash': cls.sh, 'ev_count': 3, 'belief': 1., 'num_srcs': 3,
                'src_json': {'reach': 1, 'sparser': 1, 'medscan': 1},
                'has_rd': True, 'has_db': False, 'agent_count': 2,
                'reach': 1, 'sparser': 1, 'medscan': 1})
        from indra_db_service.api import app
        cls.app = app
        cls.app.config['TESTING'] = True

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidate = self.root / 'candidate.sqlite'
        self.serving = self.root / 'serving.sqlite'
        annotation = {'level_1': 'primary_data', 'level_2': 'experimental',
                      'reasoning': 'Fixture explanation.', '_publication_key': 'PMID:222'}
        build([(self.sh, self.ehs[0], annotation),
               (self.sh, self.ehs[1], dict(annotation, _publication_key='PMID:999')),
               (999, self.ehs[0], annotation)], self.candidate, 'pg-test', 'v7')
        with readonly_snapshot(self.ro):
            self.assertEqual(self.ro.session.execute('SHOW transaction_isolation').scalar(),
                             'repeatable read')
            self.assertEqual(self.ro.session.execute('SHOW transaction_read_only').scalar(), 'on')
            self.report = reconcile(self.candidate, self.serving, 'synthetic-postgres',
                                    lambda hashes: live_pairs(self.ro, hashes))
        # Substitute only connection selection, never queries or query results.
        for context in [patch('indra_db.client.readonly.query.get_ro', return_value=self.ro),
                        patch('indra_db_service.api.get_ro_host', return_value='local-fixture'),
                        patch.dict(os.environ, {'INDRA_DB_API_PROVENANCE_DB': str(self.serving)})]:
            context.start()
            self.addCleanup(context.stop)
        self.addCleanup(self.ro.session.rollback)

    def test_reconcile_real_join_uses_served_publication_and_filters_missing(self):
        self.assertEqual(set(live_pairs(self.ro, [self.sh])),
                         {(self.sh, eh, 'PMID:222') for eh in self.ehs})
        self.assertEqual(self.report, {'matched_pairs': 1, 'unmatched_pairs': 2,
                                       'conflicting_pairs': 0})

    def test_cli_reconciles_using_configured_database(self):
        result = subprocess.run([
            sys.executable, '-m', 'indra_db_service.provenance_build', 'reconcile',
            '--candidate', str(self.candidate), '--output', str(self.root / 'cli.sqlite'),
            '--database', 'provenancetest', '--target-snapshot', 'synthetic-postgres'],
            env=dict(os.environ, INDRAROPROVENANCETEST=os.environ['PROVENANCE_TEST_POSTGRES_URL']),
            capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), self.report)

    def test_real_routes_agree_and_filter_restricted_evidence(self):
        client = self.app.test_client()
        standard = client.post('/statements/from_hashes', json={'hashes': [str(self.sh)]},
                               query_string={'ev_limit': 10})
        dedicated = client.get('/provenance/classifications', query_string={
            'statement': str(self.sh), 'explanation': 'true'})
        self.assertEqual(standard.status_code, 200, standard.get_data(as_text=True))
        self.assertEqual(dedicated.status_code, 200, dedicated.get_data(as_text=True))
        evs = standard.get_json()['statements'][str(self.sh)]['evidence']
        self.assertEqual({ev['source_api'] for ev in evs}, {'reach', 'sparser'})
        self.assertTrue(all(ev['pmid'] == '222' for ev in evs))
        annotations = [ev['provenance_classification'] for ev in evs
                       if 'provenance_classification' in ev]
        self.assertEqual(len(annotations), 1)
        entry = dedicated.get_json()['statements'][str(self.sh)]
        self.assertEqual([a['provenance_classification'] for a in entry['classifications']],
                         annotations)
        self.assertEqual(entry['evidence_checked'], 2)

    def test_real_lateral_evidence_limit_reports_incomplete_coverage(self):
        reply = self.app.test_client().get('/provenance/classifications', query_string={
            'statement': str(self.sh), 'api_key': 'test-only', 'ev_limit': 1})
        self.assertEqual(reply.status_code, 200, reply.get_data(as_text=True))
        entry = reply.get_json()['statements'][str(self.sh)]
        self.assertEqual(entry['evidence_checked'], 1)
        self.assertEqual(entry['total_evidence'], 3)
        self.assertFalse(entry['all_evidence_checked'])

    def test_real_empty_query_does_not_serve_historical_annotation(self):
        reply = self.app.test_client().get('/provenance/classifications',
                                          query_string={'statement': '999'})
        self.assertEqual(reply.status_code, 200, reply.get_data(as_text=True))
        entry = reply.get_json()['statements']['999']
        self.assertEqual(entry['classifications'], [])
        self.assertFalse(entry['statement_returned'])


if __name__ == '__main__':
    unittest.main()
