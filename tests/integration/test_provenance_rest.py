"""Exercise the real Flask routes; replace only the PostgreSQL query boundary.

Run separately with the backend/service dependencies installed:
python -m unittest discover -s tests/integration -p test_provenance_rest.py -v
This is not a live-database or deployment test.
"""
import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from indra_db_service.provenance_build import build, reconcile, prepare_runtime


class ProvenanceRestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = tempfile.TemporaryDirectory()
        cls.environment = patch.dict(os.environ, {
            "TESTING_DB_APP": "1", "AWS_EC2_METADATA_DISABLED": "true",
            "PYSTOW_HOME": cls.runtime.name,
            "INDRA_DB_API_VUE_ROOT": cls.runtime.name,
        })
        cls.environment.start()
        from indra_db_service.api import app
        cls.app = app
        cls.app.config["TESTING"] = True

    @classmethod
    def tearDownClass(cls):
        cls.environment.stop()
        cls.runtime.cleanup()

    def setUp(self):
        from indra.statements import Activation, Agent, Evidence
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stmt = Activation(Agent("A"), Agent("B"), evidence=[
            Evidence(source_api="reach", text="A activates B.", pmid="123"),
            Evidence(source_api="reach", text="A promotes B activity.", pmid="456")])
        self.sh = self.stmt.get_hash()
        self.eh = self.stmt.evidence[0].get_source_hash()
        self.annotation = {"level_1": "primary_data", "level_2": "experimental",
                           "reasoning": "Authors report an experiment.", "_publication_key": "PMID:123"}
        self.candidate = self.root / "candidate.sqlite"
        self.serving = self.root / "serving.sqlite"
        build([(self.sh, self.eh, self.annotation),
               (999, self.eh, self.annotation)], self.candidate, "test-run", "v7")
        reconcile(self.candidate, self.serving, "test-fixture",
                  lambda hashes: [(self.sh, self.eh, "PMID:123")])
        env = patch.dict(os.environ, {"INDRA_DB_API_PROVENANCE_DB": str(self.serving)})
        env.start()
        self.addCleanup(env.stop)

    def query_result(self):
        from indra.sources.indra_db_rest.query_results import StatementQueryResult
        return StatementQueryResult(
            {self.sh: copy.deepcopy(self.stmt.to_json())}, 500, 0,
            {self.sh: 2}, {self.sh: 1.0}, 2, {self.sh: {"reach": 2}},
            {"class": "HasHash", "constraint": {"stmt_hashes": [self.sh]}})

    def request(self, fmt, english=False, empty=False):
        from indra.sources.indra_db_rest.query_results import StatementQueryResult
        result = StatementQueryResult.empty(500, 0, {}) if empty else self.query_result()
        with patch("indra_db_service.api.get_ro_host", return_value="fixture"), \
                patch("indra_db.client.readonly.query.Query.get_statements",
                      return_value=result):
            return self.app.test_client().post(
                "/statements/from_hashes", json={"hashes": [str(self.sh)]},
                query_string={"format": fmt, "with_english": str(english).lower(),
                              "api_key": "test-only"})

    def test_json_and_json_js_with_and_without_english(self):
        for fmt in ("json", "json-js"):
            for english in (False, True):
                with self.subTest(format=fmt, english=english):
                    response = self.request(fmt, english)
                    self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
                    data = response.get_json()
                    evs = data["statements"][str(self.sh)]["evidence"]
                    self.assertEqual(evs[0]["provenance_classification"]["level_1"], "primary_data")
                    self.assertEqual(evs[0]["provenance_classification"]["run_id"], "test-run")
                    self.assertNotIn("provenance_classification", evs[1])
                    self.assertNotIn("999", data["statements"])
                    if fmt == "json-js":
                        self.assertIsInstance(evs[0]["source_hash"], str)

    def test_reconciliation_command_releases_owned_handle_on_success_and_error(self):
        from contextlib import nullcontext, redirect_stdout
        from io import StringIO
        from unittest.mock import Mock
        from indra_db_service import provenance_build as builder

        argv = ['provenance_build', 'reconcile', '--candidate', 'input.sqlite',
                '--output', 'output.sqlite', '--target-snapshot', 'test']
        for error in (None, ConnectionError('read failed')):
            with self.subTest(error=error):
                handle = Mock()
                with patch('sys.argv', argv), \
                     patch('indra_db.util.get_ro', return_value=handle), \
                     patch.object(builder, 'readonly_snapshot', return_value=nullcontext()), \
                     patch.object(builder, 'reconcile', side_effect=error, return_value={}), \
                     redirect_stdout(StringIO()):
                    if error is None:
                        builder.main()
                    else:
                        with self.assertRaises(ConnectionError):
                            builder.main()
                handle.close.assert_called_once_with()

    def test_truncated_text_keeps_original_identity_and_both_classifications(self):
        import sqlite3
        from indra_db_service import llm

        # The public API can retain an original hash while redacting its text.
        # Reformatting that record must not replace its identity with a new hash.
        self.stmt.evidence[0].text = (
            "A activates B. " * 20)[:200] + "[MISSING/INVALID CREDENTIALS]"
        correctness = self.root / "correctness.sqlite"
        with sqlite3.connect(correctness) as db:
            db.execute("CREATE TABLE llm_evidence_curation (statement_hash INTEGER, "
                       "source_hash INTEGER, evidence_text TEXT, judgment TEXT, "
                       "error_category TEXT, explanation TEXT)")
            db.execute("INSERT INTO llm_evidence_curation VALUES (?,?,?,?,?,?)",
                       (self.sh, self.eh, "Original evidence", "correct", None,
                        "Supports the statement."))

        def attach(results):
            llm.add_llm_verifications(results, database_path=correctness)

        with patch("indra_db_service.call_handlers.add_llm_verifications",
                   side_effect=attach):
            for fmt in ("json", "json-js"):
                for english in (False, True):
                    with self.subTest(format=fmt, english=english):
                        response = self.request(fmt, english)
                        self.assertEqual(response.status_code, 200)
                        ev = response.get_json()["statements"][str(self.sh)]["evidence"][0]
                        self.assertEqual(int(ev["source_hash"]), self.eh)
                        self.assertEqual(ev["llm_verification"]["verdict"], "correct")
                        self.assertEqual(ev["provenance_classification"]["level_1"],
                                         "primary_data")
                        self.assertIn("MISSING/INVALID CREDENTIALS", ev["text"])

    def test_runtime_candidates_through_real_handler(self):
        runtime = self.root / "runtime.sqlite"
        prepare_runtime(self.candidate, runtime)
        with patch.dict(os.environ, {"INDRA_DB_API_PROVENANCE_DB": str(runtime)}):
            self.test_json_and_json_js_with_and_without_english()
            self.test_empty_query_does_not_serve_old_annotations()

    def test_missing_or_pending_database_keeps_endpoint_available(self):
        for path in (self.root / "absent.sqlite", self.candidate):
            with patch.dict(os.environ, {"INDRA_DB_API_PROVENANCE_DB": str(path)}):
                response = self.request("json-js")
                self.assertEqual(response.status_code, 200)
                evidence = response.get_json()["statements"][str(self.sh)]["evidence"]
                self.assertTrue(all("provenance_classification" not in e for e in evidence))

    def test_empty_query_does_not_serve_old_annotations(self):
        response = self.request("json", empty=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["statements"], {})

    def dedicated(self, query=None, result=None):
        with patch("indra_db.client.readonly.query.Query.get_statements",
                   return_value=result if result is not None else self.query_result()) as fetch:
            response = self.app.test_client().get('/provenance/classifications',
                query_string=query or {'statement': str(self.sh), 'explanation': 'true'})
        return response, fetch

    def test_dedicated_matches_existing_annotation_and_preserves_hashes(self):
        response, fetch = self.dedicated()
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        entry = response.get_json()['statements'][str(self.sh)]
        expected = self.request('json').get_json()['statements'][str(self.sh)]['evidence'][0]
        self.assertEqual(entry['classifications'][0]['provenance_classification'],
                         expected['provenance_classification'])
        self.assertEqual(entry['classifications'][0]['source_hash'], str(self.eh))
        self.assertEqual(entry['classifications'][0]['publication'], 'PMID:123')
        self.assertTrue(entry['all_evidence_checked'])
        self.assertEqual(fetch.call_args.kwargs['ev_limit'], 1000)
        # The unauthenticated endpoint still filters restricted evidence sources.
        self.assertIsNotNone(fetch.call_args.kwargs['evidence_filter'])

    def test_dedicated_batch_missing_and_explanation_option(self):
        reply, _ = self.dedicated([('statement', str(self.sh) + ',999'),
                                  ('statement', str(self.sh))])
        self.assertEqual(reply.status_code, 200)
        data = reply.get_json()
        self.assertEqual(set(data['statements']), {str(self.sh), '999'})
        self.assertEqual(data['missing'], ['999'])
        self.assertFalse(data['statements']['999']['statement_returned'])
        self.assertNotIn('reasoning', data['statements'][str(self.sh)]['classifications'][0]['provenance_classification'])

    def test_dedicated_rejects_invalid_parameters_before_query(self):
        for query in ({'statement': 'bad'}, {'statement': '1.5'},
                      {'statement': str(2**63)}, {'explanation': 'true'},
                      {'statement': ','.join(['1'] * 101)},
                      {'statement': '1', 'explanation': 'yes'},
                      {'statement': '1', 'ev_limit': '10001'},
                      {'statement': '1', 'ev_limit': '0'},
                      {'statement': '1', 'offset': '1'}):
            with self.subTest(query=query):
                reply, fetch = self.dedicated(query)
                self.assertEqual(reply.status_code, 400)
                fetch.assert_not_called()

    def test_dedicated_runtime_excludes_stale_pairs_and_publication_mismatch(self):
        runtime = self.root / 'runtime-endpoint.sqlite'
        prepare_runtime(self.candidate, runtime)
        result = self.query_result()
        result.results[self.sh]['evidence'][0]['pmid'] = '987'
        result.results[self.sh]['evidence'][0]['text_refs'] = {'PMID': '987'}
        with patch.dict(os.environ, {'INDRA_DB_API_PROVENANCE_DB': str(runtime)}):
            reply, _ = self.dedicated({'statement': str(self.sh) + ',999'}, result)
        self.assertEqual(reply.status_code, 200)
        self.assertEqual(set(reply.get_json()['missing']), {str(self.sh), '999'})

    def test_dedicated_reports_incomplete_evidence_and_unavailable_sidecar(self):
        result = self.query_result()
        result.evidence_counts[self.sh] = 20
        reply, _ = self.dedicated(result=result)
        self.assertFalse(reply.get_json()['statements'][str(self.sh)]['all_evidence_checked'])
        for path in (self.candidate, self.root / 'absent.sqlite', ''):
            with patch.dict(os.environ, {'INDRA_DB_API_PROVENANCE_DB': str(path)}):
                reply, _ = self.dedicated()
                self.assertEqual(reply.status_code, 503)


if __name__ == "__main__":
    unittest.main()
