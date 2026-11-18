"""Evidence ranking within statements: approved policy and serving tables.

This module is deliberately small and dependency-free apart from SQLAlchemy
Core, so the request-time query (``Query.get_statements``), the offline
generation builder and the tests share one definition of the policy.

Policy ``evidence-priority-v2`` (matching updated 2026-11-17):

====  ==========================================================
  10  verified correct + primary data
  20  verified correct + citation
  30  verified correct + background
  40  verified correct + other / unavailable provenance
  50  correctness unknown, uncertain, unmapped, conflicting or
      otherwise unverifiable (any provenance)
  60  verified incorrect + primary data
  70  verified incorrect + citation
  80  verified incorrect + background
  90  verified incorrect + other / unavailable provenance
====  ==========================================================

"Verified" correctness follows Haohang's statement/source-hash contract;
provenance additionally requires matching original publication identity.
Within one priority, rows are ordered by ascending raw
evidence record id (a determinism tie-breaker, not a quality claim).

The reasons behind a priority are kept as separate status codes so that e.g.
"uncertain", "conflicting" and "publication unverified" stay distinguishable
even though all receive priority 50.
"""
from sqlalchemy import (BigInteger, Boolean, CheckConstraint, Column,
                        DateTime, ForeignKey, Index, Integer, MetaData,
                        PrimaryKeyConstraint, SmallInteger, Table, Text,
                        UniqueConstraint, case, and_, func, text)
from sqlalchemy.dialects.postgresql import JSONB

__all__ = ['POLICY_VERSION', 'MATCHING_POLICY_VERSION', 'SCHEMA',
           'UNRANKED_PRIORITY', 'MAX_PAGE_SIZE',
           'CORRECTNESS_STATUS', 'PROVENANCE_STATUS',
           'CORRECTNESS_STATUS_NAMES', 'PROVENANCE_STATUS_NAMES',
           'normalize_correctness', 'normalize_provenance', 'priority_for',
           'priority_sql', 'annotation_metadata', 'tables',
           'EvidenceRankingRequest', 'EvidenceRankingUnavailable',
           'EvidenceCursorStale']

POLICY_VERSION = 'evidence-priority-v2'
# Both kinds match statement/source hashes. Only provenance requires the
# publication check used by the existing runtime provenance matcher.
MATCHING_POLICY_VERSION = 'provenance-publication-correctness-pair-v2'
SCHEMA = 'annotation'

# Rows of statements outside a statement-scoped generation are returned after
# no ranked row (there are none for such statements) in raw-id order.
UNRANKED_PRIORITY = 32767
MAX_PAGE_SIZE = 100

# --- Status vocabularies -----------------------------------------------------
# Codes are stored as SMALLINT. Never renumber an existing code; add new ones.
CORRECTNESS_STATUS = {
    'verified_correct': 1,
    'verified_incorrect': 2,
    'verified_uncertain': 3,       # accepted pair match, uncertain
    'unrecognized_verdict': 4,     # pair match, unknown vocabulary
    'conflicting': 5,              # several different judgments for the pair
    'publication_unverified': 6,   # retained for historical v1 data
    'publication_mismatch': 7,     # retained for historical v1 data
    'absent': 8,                   # no judgment for this statement/source pair
    'evidence_identity_unavailable': 9,  # current record lacks source identity
}
PROVENANCE_STATUS = {
    'primary_data': 1,
    'citation': 2,
    'background': 3,
    'other': 4,
    'unrecognized_label': 5,
    'conflicting': 6,
    'publication_mismatch': 7,
    'missing_publication': 8,
    'absent': 9,
    'evidence_identity_unavailable': 10,
}
CORRECTNESS_STATUS_NAMES = {v: k for k, v in CORRECTNESS_STATUS.items()}
PROVENANCE_STATUS_NAMES = {v: k for k, v in PROVENANCE_STATUS.items()}

# Stored verdict vocabulary. Haohang's pipeline maps accepted/rejected/
# uncertain to correct/incorrect/uncertain; both spellings are accepted.
# Error categories are NOT verdicts and are never consulted here.
_VERDICTS = {
    'correct': 'correct', 'accepted': 'correct',
    'incorrect': 'incorrect', 'rejected': 'incorrect',
    'uncertain': 'uncertain', 'unknown': 'uncertain',
}
_PROVENANCE_LABELS = {'primary_data', 'citation', 'background', 'other'}


def normalize_correctness(verdict):
    """Map a stored verdict to correct/incorrect/uncertain, or None."""
    if not isinstance(verdict, str):
        return None
    return _VERDICTS.get(verdict.strip().lower())


def normalize_provenance(label):
    """Map a stored level-1 provenance label, or None if unrecognized."""
    if not isinstance(label, str):
        return None
    label = label.strip().lower()
    return label if label in _PROVENANCE_LABELS else None


_PROVENANCE_OFFSET = {'primary_data': 0, 'citation': 10, 'background': 20}


def priority_for(correctness_status, provenance_status):
    """Reference implementation of the approved nine-tier mapping."""
    if isinstance(correctness_status, int):
        correctness_status = CORRECTNESS_STATUS_NAMES[correctness_status]
    if isinstance(provenance_status, int):
        provenance_status = PROVENANCE_STATUS_NAMES[provenance_status]
    if correctness_status not in CORRECTNESS_STATUS:
        raise ValueError(f'Unknown correctness status {correctness_status!r}')
    if provenance_status not in PROVENANCE_STATUS:
        raise ValueError(f'Unknown provenance status {provenance_status!r}')
    if correctness_status == 'verified_correct':
        base = 10
    elif correctness_status == 'verified_incorrect':
        base = 60
    else:
        return 50
    return base + _PROVENANCE_OFFSET.get(provenance_status, 30)


def priority_sql(correctness_code, provenance_code):
    """SQL CASE generated from the same table as :func:`priority_for`."""
    whens = []
    for c_name, c_code in CORRECTNESS_STATUS.items():
        for p_name, p_code in PROVENANCE_STATUS.items():
            prio = priority_for(c_name, p_name)
            if prio != 50:
                whens.append((and_(correctness_code == c_code,
                                   provenance_code == p_code), prio))
    return case(whens, else_=50)


# --- Tables ------------------------------------------------------------------
annotation_metadata = MetaData(schema=SCHEMA)


class _Tables:
    annotation_run = Table(
        'annotation_run', annotation_metadata,
        Column('run_id', Text, primary_key=True),
        Column('kind', Text, nullable=False),
        Column('source_format', Text, nullable=False),
        Column('source_path', Text),
        Column('source_sha256', Text),
        Column('batch_size', Integer, nullable=False),
        Column('publication_verifiable', Boolean, nullable=False),
        Column('synthetic', Boolean, nullable=False, server_default='false'),
        Column('metadata', JSONB, nullable=False, server_default='{}'),
        Column('status', Text, nullable=False),
        Column('created_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        Column('imported_at', DateTime(timezone=True)),
        CheckConstraint("kind IN ('provenance','correctness')",
                        name='annotation_run_kind'),
        CheckConstraint("status IN ('importing','imported','failed')",
                        name='annotation_run_status'),
    )
    import_batch = Table(
        'import_batch', annotation_metadata,
        Column('run_id', Text, ForeignKey(f'{SCHEMA}.annotation_run.run_id'),
               nullable=False),
        Column('batch_no', Integer, nullable=False),
        Column('row_count', Integer, nullable=False),
        Column('digest', Text, nullable=False),
        Column('committed_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        PrimaryKeyConstraint('run_id', 'batch_no'),
    )
    # One row per distinct (run, statement, source, payload). Exact duplicate
    # occurrences collapse; differing payloads for one pair are all retained
    # and marked conflicting by the resolve step (never last-write-wins).
    annotation_payload = Table(
        'annotation_payload', annotation_metadata,
        Column('annotation_id', BigInteger, primary_key=True,
               autoincrement=True),
        Column('run_id', Text, ForeignKey(f'{SCHEMA}.annotation_run.run_id'),
               nullable=False),
        Column('kind', Text, nullable=False),
        Column('statement_hash', BigInteger, nullable=False),
        Column('source_hash', BigInteger, nullable=False),
        Column('publication_key', Text),
        Column('raw_label', Text),
        Column('label', Text),
        Column('status', Text, nullable=False),
        Column('payload_digest', Text, nullable=False),
        Column('payload', JSONB, nullable=False),
        Column('batch_no', Integer, nullable=False),
        UniqueConstraint('run_id', 'statement_hash', 'source_hash',
                         'payload_digest', name='annotation_payload_identity'),
        CheckConstraint("status IN ('accepted','conflicting',"
                        "'missing_publication')",
                        name='annotation_payload_status'),
    )
    # Snapshot-local identity of current evidence records for statements that
    # have any annotation. Built once per readonly snapshot, reused for both
    # annotation kinds.
    evidence_identity = Table(
        'evidence_identity', annotation_metadata,
        Column('snapshot_label', Text, nullable=False),
        Column('mk_hash', BigInteger, nullable=False),
        Column('raw_record_id', BigInteger, nullable=False),
        Column('source_hash', BigInteger),
        Column('publication_key', Text),
        Column('evidence_elements', SmallInteger, nullable=False),
        PrimaryKeyConstraint('snapshot_label', 'mk_hash', 'raw_record_id'),
    )
    identity_batch = Table(
        'identity_batch', annotation_metadata,
        Column('snapshot_label', Text, nullable=False),
        Column('batch_no', Integer, nullable=False),
        Column('statement_count', Integer, nullable=False),
        Column('row_count', Integer, nullable=False),
        Column('digest', Text, nullable=False),
        Column('committed_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        PrimaryKeyConstraint('snapshot_label', 'batch_no'),
    )
    ranking_generation = Table(
        'ranking_generation', annotation_metadata,
        Column('generation_id', Integer, primary_key=True, autoincrement=True),
        Column('snapshot_label', Text, nullable=False),
        Column('snapshot_fingerprint', JSONB),
        Column('policy_version', Text, nullable=False),
        Column('matching_policy_version', Text, nullable=False),
        Column('provenance_run_id', Text,
               ForeignKey(f'{SCHEMA}.annotation_run.run_id')),
        Column('correctness_run_id', Text,
               ForeignKey(f'{SCHEMA}.annotation_run.run_id')),
        Column('scope_kind', Text, nullable=False),
        Column('batch_count', Integer, nullable=False),
        # [[lo, hi], ...] as decimal strings: contiguous mk_hash ranges that
        # cover the whole signed 64-bit space, cut at statement-hash quantiles.
        Column('batch_bounds', JSONB),
        Column('status', Text, nullable=False),
        Column('coverage', JSONB),
        Column('notes', Text),
        Column('created_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        Column('ready_at', DateTime(timezone=True)),
        CheckConstraint("scope_kind IN ('all','statements')",
                        name='ranking_generation_scope'),
        CheckConstraint("status IN ('building','ready','failed','retired',"
                        "'purged')", name='ranking_generation_status'),
    )
    generation_batch = Table(
        'generation_batch', annotation_metadata,
        Column('generation_id', Integer,
               ForeignKey(f'{SCHEMA}.ranking_generation.generation_id'),
               nullable=False),
        Column('batch_no', Integer, nullable=False),
        Column('row_count', Integer, nullable=False),
        Column('committed_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        PrimaryKeyConstraint('generation_id', 'batch_no'),
    )
    generation_statement = Table(
        'generation_statement', annotation_metadata,
        Column('generation_id', Integer,
               ForeignKey(f'{SCHEMA}.ranking_generation.generation_id'),
               nullable=False),
        Column('mk_hash', BigInteger, nullable=False),
        PrimaryKeyConstraint('generation_id', 'mk_hash'),
    )
    # Narrow serving table: one row for EVERY current evidence record in the
    # generation's scope, classified or not.
    evidence_priority = Table(
        'evidence_priority', annotation_metadata,
        Column('generation_id', Integer, nullable=False),
        Column('mk_hash', BigInteger, nullable=False),
        Column('raw_record_id', BigInteger, nullable=False),
        Column('priority', SmallInteger, nullable=False),
        Column('correctness_status', SmallInteger, nullable=False),
        Column('provenance_status', SmallInteger, nullable=False),
        Column('provenance_annotation_id', BigInteger),
        Column('correctness_annotation_id', BigInteger),
        PrimaryKeyConstraint('generation_id', 'mk_hash', 'raw_record_id',
                             name='evidence_priority_pkey'),
        Index('evidence_priority_rank_idx', 'generation_id', 'mk_hash',
              'priority', 'raw_record_id'),
    )
    active_generation = Table(
        'active_generation', annotation_metadata,
        Column('singleton', Boolean, primary_key=True,
               server_default=text('true')),
        Column('generation_id', Integer,
               ForeignKey(f'{SCHEMA}.ranking_generation.generation_id'),
               nullable=False),
        Column('previous_generation_id', Integer),
        Column('activated_at', DateTime(timezone=True), nullable=False,
               server_default=func.now()),
        CheckConstraint('singleton', name='active_generation_singleton'),
    )


tables = _Tables


class EvidenceRankingUnavailable(Exception):
    """Ranking was requested but no usable generation can serve it."""


class EvidenceCursorStale(Exception):
    """A continuation cursor cannot be honoured; restart pagination."""


class EvidenceRankingRequest:
    """Request-time ranking options for :meth:`Query.get_statements`.

    Parameters
    ----------
    page_size : int
        Evidence records per statement (1..MAX_PAGE_SIZE).
    generation_id : int or None
        Pin a generation (from a cursor). None uses the active generation.
    after : tuple or None
        ``(mk_hash, priority, raw_record_id)`` continuation boundary for one
        statement. Only valid for single-statement requests.
    snapshot_label : str or None
        If given, the generation must have been built for this snapshot.
    """

    def __init__(self, page_size, generation_id=None, after=None,
                 snapshot_label=None):
        if (isinstance(page_size, bool) or not isinstance(page_size, int)
                or not 1 <= page_size <= MAX_PAGE_SIZE):
            raise ValueError(f'Evidence page size must be 1..{MAX_PAGE_SIZE}')
        if after is not None:
            mk_hash, prio, raw_id = after
            if not all(isinstance(v, int) and not isinstance(v, bool)
                       for v in after):
                raise ValueError('Invalid continuation boundary')
            if not 0 <= prio <= UNRANKED_PRIORITY or raw_id < 0:
                raise ValueError('Invalid continuation boundary')
        self.page_size = page_size
        self.generation_id = generation_id
        self.after = tuple(after) if after is not None else None
        self.snapshot_label = snapshot_label
        # Filled in by get_statements.
        self.generation = None


def resolve_generation(connection, request):
    """Read the generation to use, once per request."""
    g = tables.ranking_generation
    a = tables.active_generation
    from sqlalchemy import select
    # Only the columns the request path needs, so additive schema changes
    # never break serving.
    cols = [g.c.generation_id, g.c.snapshot_label, g.c.policy_version,
            g.c.provenance_run_id, g.c.correctness_run_id, g.c.scope_kind,
            g.c.status, g.c.matching_policy_version]
    try:
        if request.generation_id is None:
            row = connection.execute(
                select(cols).select_from(
                    a.join(g, a.c.generation_id == g.c.generation_id))
            ).first()
            if row is None:
                raise EvidenceRankingUnavailable(
                    'No active evidence ranking generation')
            if row['status'] != 'ready':
                raise EvidenceRankingUnavailable(
                    'Active evidence ranking generation is not ready')
        else:
            row = connection.execute(
                select(cols).where(g.c.generation_id == request.generation_id)
            ).first()
            # A retired generation keeps its rows until purged, so cursors
            # issued before a publication can finish their traversal.
            if row is None or row['status'] not in ('ready', 'retired'):
                raise EvidenceCursorStale(
                    'Evidence ranking generation has expired')
    except EvidenceCursorStale:
        raise
    except EvidenceRankingUnavailable:
        raise
    except Exception as error:  # Missing schema/tables or permissions.
        raise EvidenceRankingUnavailable(
            'Evidence ranking tables are unavailable') from error
    if (row['policy_version'] != POLICY_VERSION
            or row['matching_policy_version'] != MATCHING_POLICY_VERSION):
        if request.generation_id is None:
            raise EvidenceRankingUnavailable(
                'Active ranking generation uses another policy; rebuild it')
        raise EvidenceCursorStale('Ranking policy changed; restart pagination')
    if (request.snapshot_label is not None
            and row['snapshot_label'] != request.snapshot_label):
        if request.generation_id is None:
            raise EvidenceRankingUnavailable(
                'Active ranking generation was built for another snapshot')
        raise EvidenceCursorStale('Cursor generation is for another snapshot')
    return dict(row)
