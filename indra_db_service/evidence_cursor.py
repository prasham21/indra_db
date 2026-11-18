"""Opaque, signed per-statement evidence continuation cursors.

A cursor is a position, never an authorization grant: every request is
re-authenticated and its filters/permissions are re-applied, and a cursor is
rejected (409, restart pagination) when the statement, generation, policy,
evidence-filter context or access context differ from the issuing request.

Signing uses itsdangerous (Flask's own dependency) with a dedicated secret,
``INDRA_DB_API_EVIDENCE_CURSOR_SECRET``. Without it a random per-process key
is used, so cursors then only work within one worker process; configure the
secret for multi-worker deployments.
"""
import hashlib
import json
import logging
import os
import secrets

from itsdangerous import BadSignature, URLSafeSerializer

from indra_db.client.readonly.evidence_ranking import (POLICY_VERSION,
                                                        UNRANKED_PRIORITY)

logger = logging.getLogger(__name__)

CURSOR_VERSION = 1
MAX_CURSOR_LENGTH = 1024
SECRET_ENV = 'INDRA_DB_API_EVIDENCE_CURSOR_SECRET'
_SALT = 'indra-db-evidence-cursor-v1'
_process_secret = None


class CursorError(ValueError):
    """Malformed, tampered or oversized cursor (HTTP 400)."""


class CursorMismatch(Exception):
    """Well-formed cursor for a different context (HTTP 409)."""


def _serializer():
    global _process_secret
    secret = os.environ.get(SECRET_ENV)
    if not secret:
        if _process_secret is None:
            logger.warning('%s is not set: evidence cursors are only valid '
                           'within this process.', SECRET_ENV)
            _process_secret = secrets.token_hex(32)
        secret = _process_secret
    return URLSafeSerializer(secret, salt=_SALT)


def context_digest(value):
    """Stable digest of a JSON-able context (order-insensitive where sorted)."""
    body = json.dumps(value, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(body.encode()).hexdigest()[:32]


def encode_cursor(generation_id, snapshot_label, mk_hash, priority,
                  raw_record_id, filter_digest, access_digest):
    payload = {'v': CURSOR_VERSION, 'g': int(generation_id),
               's': snapshot_label, 'pv': POLICY_VERSION,
               'h': str(int(mk_hash)), 'p': int(priority),
               'i': str(int(raw_record_id)), 'f': filter_digest,
               'a': access_digest}
    return _serializer().dumps(payload)


def _int_field(payload, key, low, high):
    value = payload.get(key)
    if isinstance(value, bool):
        raise CursorError('Invalid cursor')
    if isinstance(value, str):
        if not value.lstrip('-').isdigit():
            raise CursorError('Invalid cursor')
        value = int(value)
    if not isinstance(value, int) or not low <= value <= high:
        raise CursorError('Invalid cursor')
    return value


def decode_cursor(token):
    if not isinstance(token, str) or not token or \
            len(token) > MAX_CURSOR_LENGTH:
        raise CursorError('Invalid cursor')
    try:
        payload = _serializer().loads(token)
    except BadSignature as error:
        raise CursorError('Invalid cursor') from error
    if not isinstance(payload, dict) or payload.get('v') != CURSOR_VERSION:
        raise CursorError('Unsupported cursor version')
    return {
        'generation_id': _int_field(payload, 'g', 1, 2 ** 31 - 1),
        'snapshot_label': payload.get('s'),
        'policy_version': payload.get('pv'),
        'mk_hash': _int_field(payload, 'h', -2 ** 63, 2 ** 63 - 1),
        'priority': _int_field(payload, 'p', 0, UNRANKED_PRIORITY),
        'raw_record_id': _int_field(payload, 'i', 0, 2 ** 63 - 1),
        'filter_digest': payload.get('f'),
        'access_digest': payload.get('a'),
    }


def check_cursor(cursor, mk_hash, filter_digest, access_digest):
    """Raise CursorMismatch unless the cursor belongs to this request."""
    if cursor['policy_version'] != POLICY_VERSION:
        raise CursorMismatch('Ranking policy changed; restart pagination')
    if cursor['mk_hash'] != int(mk_hash):
        raise CursorMismatch('Cursor belongs to another statement')
    if cursor['filter_digest'] != filter_digest:
        raise CursorMismatch('Evidence filters changed; restart pagination')
    if cursor['access_digest'] != access_digest:
        raise CursorMismatch('Access context changed; restart pagination')
