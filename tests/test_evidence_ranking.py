"""Unit tests: approved ranking policy, vocabularies, cursors, adapters.

These do not need PostgreSQL; ordering/filter/pagination semantics are
covered by tests/integration/test_evidence_ranking_postgres.py.
"""
import json
import os
import sqlite3
import tempfile
import unittest
from itertools import product
from pathlib import Path
from unittest.mock import Mock, patch

from indra_db.client.readonly.evidence_ranking import (
    CORRECTNESS_STATUS, PROVENANCE_STATUS, EvidenceRankingRequest,
    MAX_PAGE_SIZE, POLICY_VERSION, UNRANKED_PRIORITY, normalize_correctness,
    normalize_provenance, priority_for)


class PolicyTest(unittest.TestCase):
    def test_approved_nine_tiers(self):
        table = {
            ('verified_correct', 'primary_data'): 10,
            ('verified_correct', 'citation'): 20,
            ('verified_correct', 'background'): 30,
            ('verified_correct', 'other'): 40,
            ('verified_incorrect', 'primary_data'): 60,
            ('verified_incorrect', 'citation'): 70,
            ('verified_incorrect', 'background'): 80,
            ('verified_incorrect', 'other'): 90,
        }
        for (c, p), want in table.items():
            self.assertEqual(priority_for(c, p), want, (c, p))
        self.assertEqual(POLICY_VERSION, 'evidence-priority-v2')

    def test_six_agreed_relative_orders(self):
        order = [('verified_correct', 'primary_data'),
                 ('verified_correct', 'citation'),
                 ('verified_correct', 'background'),
                 ('verified_incorrect', 'primary_data'),
                 ('verified_incorrect', 'citation'),
                 ('verified_incorrect', 'background')]
        prios = [priority_for(*pair) for pair in order]
        self.assertEqual(prios, sorted(prios))
        self.assertEqual(len(set(prios)), 6)

    def test_unavailable_provenance_is_40_or_90(self):
        for p in ('other', 'unrecognized_label', 'conflicting',
                  'publication_mismatch', 'missing_publication', 'absent',
                  'evidence_identity_unavailable'):
            self.assertEqual(priority_for('verified_correct', p), 40, p)
            self.assertEqual(priority_for('verified_incorrect', p), 90, p)

    def test_every_unverified_correctness_is_50_regardless_of_provenance(self):
        for c, p in product(CORRECTNESS_STATUS, PROVENANCE_STATUS):
            if c not in ('verified_correct', 'verified_incorrect'):
                self.assertEqual(priority_for(c, p), 50, (c, p))

    def test_codes_and_names_agree(self):
        for c, p in product(CORRECTNESS_STATUS.items(),
                            PROVENANCE_STATUS.items()):
            self.assertEqual(priority_for(c[1], p[1]),
                             priority_for(c[0], p[0]))
        with self.assertRaises(ValueError):
            priority_for('correct', 'primary_data')

    def test_generated_sql_lookup_matches_reference(self):
        from indra_db_service.evidence_ranking_build import _priority_values_sql
        values = _priority_values_sql()[len('VALUES '):]
        triples = json.loads('[' + values.replace('(', '[')
                             .replace(')', ']') + ']')
        self.assertEqual(len(triples),
                         len(CORRECTNESS_STATUS) * len(PROVENANCE_STATUS))
        for c, p, prio in triples:
            self.assertEqual(priority_for(c, p), prio)

    def test_verdict_vocabulary(self):
        self.assertEqual(normalize_correctness('correct'), 'correct')
        self.assertEqual(normalize_correctness(' Accepted '), 'correct')
        self.assertEqual(normalize_correctness('rejected'), 'incorrect')
        self.assertEqual(normalize_correctness('incorrect'), 'incorrect')
        self.assertEqual(normalize_correctness('uncertain'), 'uncertain')
        # Error categories are not verdicts.
        for value in ('grounding', 'polarity', 'no_relation', '', None, 1):
            self.assertIsNone(normalize_correctness(value))
        self.assertEqual(normalize_provenance('primary_data'), 'primary_data')
        self.assertIsNone(normalize_provenance('experimental'))

    def test_request_bounds(self):
        for bad in (0, MAX_PAGE_SIZE + 1, True, '10', None):
            with self.assertRaises(ValueError):
                EvidenceRankingRequest(bad)
        with self.assertRaises(ValueError):
            EvidenceRankingRequest(5, after=(1, UNRANKED_PRIORITY + 1, 2))
        with self.assertRaises(ValueError):
            EvidenceRankingRequest(5, after=(1, 10, -1))
        req = EvidenceRankingRequest(5, generation_id=3,
                                     after=(-(2 ** 63), 10, 4))
        self.assertEqual(req.after, (-(2 ** 63), 10, 4))


class CursorTest(unittest.TestCase):
    def setUp(self):
        context = patch.dict(os.environ, {
            'INDRA_DB_API_EVIDENCE_CURSOR_SECRET': 'unit-test-secret'})
        context.start()
        self.addCleanup(context.stop)
        from indra_db_service import evidence_cursor
        self.c = evidence_cursor

    def make(self, mk_hash=-(2 ** 63) + 7, raw_id=2 ** 40):
        return self.c.encode_cursor(4, 'snap', mk_hash, 30, raw_id, 'f', 'a')

    def test_round_trip_keeps_signed_64_bit_values_exact(self):
        token = self.make()
        cursor = self.c.decode_cursor(token)
        self.assertEqual(cursor['mk_hash'], -(2 ** 63) + 7)
        self.assertEqual(cursor['raw_record_id'], 2 ** 40)
        self.assertEqual(cursor['generation_id'], 4)
        self.assertEqual(cursor['priority'], 30)
        self.c.check_cursor(cursor, -(2 ** 63) + 7, 'f', 'a')

    def test_tampered_or_foreign_cursors_are_rejected(self):
        token = self.make()
        for bad in (token[:-1] + ('A' if token[-1] != 'A' else 'B'),
                    'x' * (self.c.MAX_CURSOR_LENGTH + 1), '', None, 'abc.def'):
            with self.assertRaises(self.c.CursorError):
                self.c.decode_cursor(bad)
        with patch.dict(os.environ, {
                'INDRA_DB_API_EVIDENCE_CURSOR_SECRET': 'other-secret'}):
            with self.assertRaises(self.c.CursorError):
                self.c.decode_cursor(token)

    def test_signed_but_malformed_fields_are_rejected(self):
        serializer = self.c._serializer()
        base = {'v': 1, 'g': 1, 's': 's', 'pv': POLICY_VERSION, 'h': '1',
                'p': 10, 'i': '1', 'f': 'f', 'a': 'a'}
        for change in ({'v': 2}, {'g': 0}, {'p': 40000}, {'p': True},
                       {'h': '1.5'}, {'h': str(2 ** 63)}, {'i': '-1'},
                       {'g': 'x'}):
            with self.assertRaises(self.c.CursorError, msg=change):
                self.c.decode_cursor(serializer.dumps(dict(base, **change)))

    def test_context_mismatches(self):
        cursor = self.c.decode_cursor(self.make(mk_hash=5))
        with self.assertRaises(self.c.CursorMismatch):
            self.c.check_cursor(cursor, 6, 'f', 'a')
        with self.assertRaises(self.c.CursorMismatch):
            self.c.check_cursor(cursor, 5, 'g', 'a')
        with self.assertRaises(self.c.CursorMismatch):
            self.c.check_cursor(cursor, 5, 'f', 'b')
        cursor['policy_version'] = 'old'
        with self.assertRaises(self.c.CursorMismatch):
            self.c.check_cursor(cursor, 5, 'f', 'a')

    def test_context_digest_is_order_insensitive_for_dict_keys(self):
        self.assertEqual(self.c.context_digest({'a': 1, 'b': [1, 2]}),
                         self.c.context_digest({'b': [1, 2], 'a': 1}))
        self.assertNotEqual(self.c.context_digest({'a': 1}),
                            self.c.context_digest({'a': 2}))


class AdapterTest(unittest.TestCase):
    def test_correctness_cli_streams_and_closes_after_import_failure(self):
        from indra_db_service import evidence_ranking_build as b
        yielded, closed = [], []
        original = b.iter_correctness_sqlite

        def tracked(path, report):
            try:
                for record in original(path, report):
                    yielded.append(record)
                    yield record
            finally:
                closed.append(True)

        def failing_import(engine, run_id, kind, records, source_format,
                           **kwargs):
            # Only one row may have been read for schema inspection. The old
            # list(...) implementation consumed all rows before this call.
            self.assertEqual(len(yielded), 1)
            self.assertFalse(kwargs['publication_verifiable'])
            self.assertEqual(next(records)['source_hash'], 0)
            self.assertEqual(next(records)['source_hash'], 1)
            self.assertEqual(len(yielded), 2)
            raise b.BuildError('injected import failure')

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'correctness.sqlite'
            with sqlite3.connect(path) as db:
                db.execute('CREATE TABLE llm_evidence_curation '
                           '(statement_hash INTEGER, source_hash INTEGER, '
                           'judgment TEXT)')
                db.executemany('INSERT INTO llm_evidence_curation VALUES '
                               '(1, ?, \'correct\')', [(i,) for i in range(20)])
            with patch.object(b, 'writer_engine'), \
                    patch.object(b, 'iter_correctness_sqlite', tracked), \
                    patch.object(b, 'import_run', failing_import):
                with self.assertRaisesRegex(b.BuildError,
                                            'injected import failure'):
                    b.main(['--database-url', 'postgresql:///ranking_test',
                            'import-correctness', '--sqlite', str(path),
                            '--run-id', 'unit-stream'])
        self.assertEqual(len(yielded), 2)
        self.assertEqual(closed, [True])

    def test_provenance_sqlite_adapter_keeps_conflicts_and_order(self):
        from indra_db_service.provenance_build import build
        from indra_db_service.evidence_ranking_build import (
            iter_provenance_sqlite, provenance_sqlite_info)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'c.sqlite'
            ann = {'level_1': 'citation', '_publication_key': 'PMID:1'}
            build([(5, 9, ann), (5, 9, dict(ann, level_1='background')),
                   (-3, 2 ** 62, ann), (7, 1, dict(ann, _publication_key=None))],
                  path, 'unit-run', 'v7')
            self.assertEqual(provenance_sqlite_info(path)['run_id'],
                             'unit-run')
            rows = list(iter_provenance_sqlite(path))
        self.assertEqual([(r['statement_hash'], r['source_hash'], r['status'])
                          for r in rows],
                         [(-3, 2 ** 62, 'accepted'), (5, 9, 'conflicting'),
                          (5, 9, 'conflicting')])
        self.assertTrue(all('_publication_key' not in r['payload']
                            for r in rows))

    def test_jsonl_fixture_adapter(self):
        from indra_db_service.evidence_ranking_build import iter_jsonl
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'f.jsonl'
            path.write_text('\n'.join(json.dumps(r) for r in [
                {'statement_hash': '9223372036854775807', 'source_hash': 1,
                 'publication_key': 'PMID:1', 'verdict': 'correct'},
                {'statement_hash': -1, 'source_hash': 2, 'verdict': 'rejected'}]))
            rows = list(iter_jsonl(path, 'correctness'))
        self.assertEqual(rows[0]['statement_hash'], -1)
        self.assertIsNone(rows[0]['publication_key'])
        self.assertEqual(rows[1]['statement_hash'], 2 ** 63 - 1)

    def test_batch_ranges_cover_all_signed_64_bit_hashes(self):
        from indra_db_service.evidence_ranking_build import (
            INT64_MAX, INT64_MIN, hash_ranges, ranges_from_cuts)
        for ranges in (ranges_from_cuts([5, -3, 5, None, INT64_MIN, 10 ** 17]),
                       ranges_from_cuts([]), hash_ranges(7)):
            self.assertEqual(ranges[0][0], INT64_MIN)
            self.assertEqual(ranges[-1][1], INT64_MAX)
            for (lo, hi), (nlo, _) in zip(ranges, ranges[1:]):
                self.assertLessEqual(lo, hi)
                self.assertEqual(hi + 1, nlo)
        self.assertEqual(len(ranges_from_cuts([5, -3, 5])), 3)

    def test_writer_refuses_remote_hosts(self):
        from indra_db_service.evidence_ranking_build import (BuildError,
                                                             writer_engine)
        with self.assertRaises(BuildError):
            writer_engine('postgresql://user@db.example.org/indradb')
        writer_engine('postgresql:///x?host=/tmp/sock')


class GenerationPolicyTest(unittest.TestCase):
    def test_previous_matching_policy_is_not_silently_served(self):
        from indra_db.client.readonly.evidence_ranking import (
            MATCHING_POLICY_VERSION, EvidenceCursorStale,
            EvidenceRankingUnavailable, resolve_generation)
        current = dict(generation_id=1, snapshot_label='test',
                       policy_version=POLICY_VERSION,
                       matching_policy_version=MATCHING_POLICY_VERSION,
                       provenance_run_id=None, correctness_run_id=None,
                       scope_kind='all', status='ready')
        connection = Mock()
        for key in ('policy_version', 'matching_policy_version'):
            connection.execute.return_value.first.return_value = dict(
                current, **{key: 'previous-policy'})
            with self.assertRaises(EvidenceRankingUnavailable):
                resolve_generation(connection, EvidenceRankingRequest(10))
            with self.assertRaises(EvidenceCursorStale):
                resolve_generation(connection,
                                   EvidenceRankingRequest(10, generation_id=1))
        connection.execute.return_value.first.return_value = current
        self.assertEqual(resolve_generation(connection,
                                            EvidenceRankingRequest(10)), current)


if __name__ == '__main__':
    unittest.main()
