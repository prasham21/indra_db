"""Dependency-light integration tests: python -m unittest discover -s tests."""
import hashlib
import json
import sqlite3
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from indra_db_service.provenance import add_provenance_annotations, hash_integer, publication_key
from indra_db_service.provenance_build import (build, checkpoint_rows, reconcile,
    prepare_runtime, served_publication_evidence)


class ProvenanceTest(unittest.TestCase):
    def test_reconciliation_uses_served_publication_metadata(self):
        raw = {'pmid': '123', 'text_refs': {'PMID': '123', 'DOI': '10.1000/old'}}
        updated = served_publication_evidence(raw, {'pmid': '456', 'doi': '10.1000/new'})
        self.assertEqual(publication_key(updated), 'PMID:456')
        self.assertEqual(raw['pmid'], '123')
        # Match REST behavior: without a reading PMID it removes text_refs.PMID,
        # but retains an existing direct raw PMID.
        self.assertEqual(publication_key(served_publication_evidence(raw, {'pmid': None})), 'PMID:123')
        self.assertEqual(publication_key(served_publication_evidence(
            {'text_refs': {'PMID': '123'}}, {'pmid': None, 'pmcid': 'PMC456'})), 'PMCID:PMC456')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.annotation = {"level_1": "primary_data", "level_2": "experimental",
                           "reasoning": "The authors report their experiment.", "_publication_key": "PMID:123"}

    def publish(self, rows, pairs):
        candidate, serving = self.root / "candidate.sqlite", self.root / "serving.sqlite"
        report = build(rows, candidate, "run-test", "v7")
        validation = reconcile(candidate, serving, "snapshot-test",
                               lambda hashes: (p + ("PMID:123",) for p in pairs if p[0] in hashes))
        return candidate, serving, report, validation

    def test_runtime_preparation_preserves_candidate_and_only_enriches_current_matches(self):
        candidate, serving = self.root / "candidate.sqlite", self.root / "runtime.sqlite"
        build([(1, 2, self.annotation), (9, 9, self.annotation),
               (3, 4, self.annotation), (3, 4, dict(self.annotation, level_1="citation"))],
              candidate, "run", "v7")
        before = candidate.read_bytes()
        report = prepare_runtime(candidate, serving)
        self.assertEqual(candidate.read_bytes(), before)
        self.assertEqual(report["candidate_pairs"], 2)
        self.assertFalse(report["full_snapshot_reconciled"])
        results = {"1": {"evidence": [
            {"source_hash": "2", "pmid": "123"},
            {"source_hash": "2", "pmid": "456"},
            {"source_hash": "2"},
            {"source_hash": "2", "pmid": "123", "text_refs": {"PMID": "456"}},
            {"source_hash": "5", "pmid": "123"}]},
            "3": {"evidence": [{"source_hash": "4", "pmid": "123"}]},
            "8": {"evidence": [{"source_hash": "2", "pmid": "123"}]}}
        add_provenance_annotations(results, serving)
        self.assertIn("provenance_classification", results["1"]["evidence"][0])
        self.assertTrue(all("provenance_classification" not in ev
                            for ev in results["1"]["evidence"][1:]))
        self.assertNotIn("provenance_classification", results["3"]["evidence"][0])
        self.assertNotIn("provenance_classification", results["8"]["evidence"][0])
        self.assertNotIn("9", results)
        with self.assertRaises(ValueError):
            prepare_runtime(candidate, serving)
        with sqlite3.connect(serving) as db:
            db.execute("DELETE FROM metadata WHERE key='matching_policy'")
        fresh = {1: {"evidence": [{"source_hash": 2, "pmid": "123"}]}}
        with self.assertLogs("indra_db_service.provenance", level="WARNING"):
            add_provenance_annotations(fresh, serving)
        self.assertNotIn("provenance_classification", fresh[1]["evidence"][0])

    def test_runtime_preparation_rejects_bad_candidate_atomically(self):
        candidate, serving = self.root / "candidate.sqlite", self.root / "runtime.sqlite"
        build([(1, 2, self.annotation)], candidate, "run", "v7")
        with sqlite3.connect(candidate) as db:
            db.execute("UPDATE evidence_provenance SET annotation='{}'")
        with self.assertRaises(ValueError):
            prepare_runtime(candidate, serving)
        self.assertFalse(serving.exists())
        self.assertFalse(list(self.root.glob("*.tmp")))

    def test_exact_pair_matching_and_existing_annotations(self):
        sh, eh = 22835942691810191, -3751212991186187708
        _, serving, _, report = self.publish(
            [(sh, eh, self.annotation), (10, eh, self.annotation)],
            [(sh, eh)])
        results = {str(sh): {"evidence": [
            {"pmid": "123", "source_hash": str(eh), "llm_verification": {"verdict": "correct"}},
            {"pmid": "123", "source_hash": "3"}, {"pmid": "123", "source_hash": None}]},
            "10": {"evidence": [{"pmid": "123", "source_hash": str(eh)}]}}
        add_provenance_annotations(results, serving)
        evidence = results[str(sh)]["evidence"]
        self.assertEqual(evidence[0]["provenance_classification"]["level_1"], "primary_data")
        self.assertEqual(evidence[0]["provenance_classification"]["run_id"], "run-test")
        self.assertEqual(evidence[0]["llm_verification"], {"verdict": "correct"})
        self.assertNotIn("provenance_classification", evidence[1])
        self.assertNotIn("provenance_classification", results["10"]["evidence"][0])
        self.assertEqual(report, {"matched_pairs": 1, "unmatched_pairs": 1, "conflicting_pairs": 0})

    def test_conflicts_never_last_write_wins(self):
        other = dict(self.annotation, level_1="citation")
        _, serving, report, _ = self.publish([
            (1, 2, self.annotation), (1, 2, self.annotation),
            (1, 2, other), (1, 2, self.annotation), (2, 3, self.annotation)],
            [(1, 2), (2, 3)])
        self.assertEqual(report["conflict_pairs"], 1)
        self.assertEqual(report["duplicates"], 1)
        results = {"1": {"evidence": [{"pmid": "123", "source_hash": 2}]},
                   "2": {"evidence": [{"pmid": "123", "source_hash": 3}]}}
        add_provenance_annotations(results, serving)
        self.assertNotIn("provenance_classification", results["1"]["evidence"][0])
        self.assertIn("provenance_classification", results["2"]["evidence"][0])

    def test_disabled_missing_unvalidated_and_broken_database(self):
        candidate = self.root / "candidate.sqlite"
        build([(1, 2, self.annotation)], candidate, "run", "v7")
        corrupt = self.root / "broken.sqlite"
        corrupt.write_text("not SQLite")
        for path in (candidate, corrupt, self.root / "absent.sqlite"):
            results = {1: {"evidence": [{"pmid": "123", "source_hash": 2}]}}
            with self.assertLogs("indra_db_service.provenance", level="WARNING"):
                add_provenance_annotations(results, path)
            self.assertNotIn("provenance_classification", results[1]["evidence"][0])
        with patch.dict("os.environ", {}, clear=True):
            add_provenance_annotations(results)
        self.assertFalse((self.root / "absent.sqlite").exists())

    def test_batches_more_than_sqlite_bind_limit(self):
        rows = [(i, -i, self.annotation) for i in range(1100)]
        _, serving, _, _ = self.publish(rows, [(i, -i) for i in range(1100)])
        results = {str(i): {"evidence": [{"pmid": "123", "source_hash": str(-i)}]} for i in range(1100)}
        add_provenance_annotations(results, serving)
        self.assertTrue(all("provenance_classification" in s["evidence"][0]
                            for s in results.values()))

    def test_bad_hashes_and_atomic_failure(self):
        for value in (True, 1.1, "1.0", None, 2 ** 63):
            with self.assertRaises(ValueError):
                hash_integer(value)
        def broken_rows():
            yield 1, 2, self.annotation
            raise ValueError("broken input")
        with self.assertRaises(ValueError):
            build(broken_rows(), self.root / "failed.sqlite", "run", "v7")
        self.assertFalse((self.root / "failed.sqlite").exists())

    def fixture_checkpoint(self, merged=False):
        dataset, results = self.root / "dataset.sqlite", self.root / "results.sqlite"
        with sqlite3.connect(dataset) as db:
            db.executescript("""
                CREATE TABLE statements(id INTEGER PRIMARY KEY,payload TEXT);
                CREATE TABLE evidence(ordinal INTEGER UNIQUE,statement_id INTEGER,payload TEXT,pmid TEXT);
            """)
            db.execute("INSERT INTO statements VALUES (?,?)",
                       (3, json.dumps({"statement_hash": "22835942691810191"})))
            db.execute("INSERT INTO evidence VALUES (?,?,?,?)",
                       (0, 3, json.dumps({"pmid": "123", "source_hash": -3751212991186187708}), "123"))
        checksum = hashlib.sha256(dataset.read_bytes()).hexdigest()
        (self.root / "dataset.json").write_text(json.dumps(
            {"format": "evidence-sqlite-v1", "sha256": checksum, "eligible": 1}))
        config = {"input_digest": checksum, "selected_range": [0, 1]}
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(config))
        with sqlite3.connect(results) as db:
            db.execute("CREATE TABLE results(idx INTEGER PRIMARY KEY,event BLOB)")
            event = {"index": 0, "classification": dict(self.annotation, evidence_index=0),
                     "overrides": []}
            db.execute("INSERT INTO results VALUES (?,?)",
                       (0, zlib.compress(json.dumps(event).encode())))
            if not merged:
                db.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
                db.execute("INSERT INTO metadata VALUES ('run_digest',?)",
                           (hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),))
        if merged:
            (self.root / "coverage.json").write_text(json.dumps(
                {"expected": 1, "successful": 1, "complete": True}))
        return dataset, results, config_path

    def test_checkpoint_to_response(self):
        paths = self.fixture_checkpoint()
        rows = list(checkpoint_rows(*paths))
        self.assertNotIn("evidence_index", rows[0][2])
        _, serving, _, _ = self.publish(rows, [(int(rows[0][0]), rows[0][1])])
        results = {rows[0][0]: {"evidence": [{"pmid": "123", "source_hash": rows[0][1]}]}}
        add_provenance_annotations(results, serving)
        self.assertIn("provenance_classification", results[rows[0][0]]["evidence"][0])
        paths[2].write_text(json.dumps({"input_digest": "wrong", "selected_range": [0, 1]}))
        with self.assertRaises(ValueError):
            list(checkpoint_rows(*paths))

    def test_merged_checkpoint_requires_coverage(self):
        paths = self.fixture_checkpoint(merged=True)
        self.assertEqual(len(list(checkpoint_rows(*paths))), 1)
        (self.root / "coverage.json").unlink()
        with self.assertRaises(FileNotFoundError):
            list(checkpoint_rows(*paths))

    def test_reconcile_failure_never_publishes(self):
        candidate = self.root / "candidate.sqlite"
        build([(1, 2, self.annotation)], candidate, "run", "v7")
        def unavailable(hashes):
            raise ConnectionError("database unavailable")
        with self.assertRaises(ConnectionError):
            reconcile(candidate, self.root / "serving.sqlite", "snapshot", unavailable)
        self.assertFalse((self.root / "serving.sqlite").exists())

    def test_publication_context_at_reconciliation_and_response(self):
        candidate, serving, _, _ = self.publish([(1, 2, self.annotation)], [(1, 2)])
        results = {1: {"evidence": [
            {"source_hash": 2, "pmid": "123"},
            {"source_hash": 2, "pmid": "456"},
            {"source_hash": 2},
            {"source_hash": 2, "text_refs": {"PMID": "123"}}]}}
        add_provenance_annotations(results, serving)
        evs = results[1]["evidence"]
        self.assertEqual(["provenance_classification" in e for e in evs], [True, False, False, True])
        self.assertNotIn("_publication_key", evs[0]["provenance_classification"])
        report = reconcile(candidate, self.root / "other-publication.sqlite", "snapshot",
                           lambda hashes: [(1, 2, "PMID:456")])
        self.assertEqual(report, {"matched_pairs": 0, "unmatched_pairs": 1, "conflicting_pairs": 0})

    def test_identical_labels_from_different_papers_are_not_collapsed(self):
        _, _, report, _ = self.publish([
            (1, 2, self.annotation),
            (1, 2, dict(self.annotation, _publication_key="PMID:456"))], [(1, 2)])
        self.assertEqual(report["conflict_pairs"], 1)
        self.assertEqual(report["candidate_pairs"], 0)

    def test_publication_identifiers(self):
        self.assertIsNone(publication_key({"pmid": "123", "text_refs": {"PMID": "456"}}))
        self.assertIsNone(publication_key({"pmid": "unknown"}))
        self.assertIsNone(publication_key({}))
        self.assertEqual(publication_key({"text_refs": {"PMCID": "pmc123"}}), "PMCID:PMC123")
        self.assertEqual(publication_key({"text_refs": {"DOI": "https://doi.org/10.123/A"}}), "DOI:10.123/a")

    def test_missing_publication_is_excluded(self):
        annotation = dict(self.annotation)
        annotation.pop("_publication_key")
        report = build([(1, 2, annotation)], self.root / "candidate.sqlite", "run", "v7")
        self.assertEqual(report["missing_publication"], 1)
        self.assertEqual(report["candidate_pairs"], 0)


if __name__ == "__main__":
    unittest.main()
