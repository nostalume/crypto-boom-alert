"""Pure OKX public wire tests; mock completeness is NOT GitHub ancestry proof."""
import base64
import json
from copy import deepcopy

import pytest
from test_okx_storage import (
    SCOPE,
    SOURCE,
    T,
    add,
    bundle,
    chain,
    pending,
    query,
    restore,
)

from crypto_boom import okx_storage as storage
from crypto_boom.okx_state import Checkpoint, StateStore
from crypto_boom.okx_trial import MINUTE, VERSION, Transition


def prepared(count=2):
    checkpoint = Checkpoint(generation=3, at=T, notifications_enabled=True)
    changes = [Transition((VERSION + '/A0', ('okx', 'SPOT', f'ALT{i}-USDT'), 'UP', T), 'OBSERVED', T)
               for i in range(count)]
    return checkpoint, checkpoint.prepare(changes, T)


def encode(checkpoint, ids, **kwargs):
    return storage.format_receipt_intent(checkpoint, ids, **{
        'scope': SCOPE, 'source_sha': SOURCE, 'run_id': 5, 'attempt': 1, **kwargs})


def decode(marker, **kwargs):
    return storage.parse_public_receipt(marker, **{
        'scope': SCOPE, 'source_sha': SOURCE, 'run_id': 5, 'attempt': 1, 'published_at': T + 1000, **kwargs})


def payload(marker):
    return json.loads(base64.urlsafe_b64decode(marker.split(':', 1)[1].removesuffix(' -->')))


def marker_for(body=None, *, raw=None):
    if raw is None:
        raw = json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
    return f'<!-- {storage.RECEIPT_TAG}:{base64.urlsafe_b64encode(raw).decode()} -->'


def test_full_canonical_batch_round_trip_and_no_state_mutation():
    checkpoint, ids = prepared()
    before = deepcopy(checkpoint.encode())
    marker = encode(checkpoint, ids[::-1])
    assert marker == encode(checkpoint, ids)
    receipt = decode(marker)
    assert receipt == storage.Receipt(SCOPE, 3, T + 1000, ids, checkpoint.ledger[ids[0]]['batch'])
    assert payload(marker)['prepared_at'] == T and 'published_at' not in payload(marker)
    assert set(payload(marker)) == {'schema', 'rules_version', 'scope', 'source_sha', 'run_id', 'attempt',
                                 'generation', 'prepared_at', 'ids', 'batch'}
    assert checkpoint.encode() == before


def test_all_ledger_statuses_and_disabled_snapshot_serialize_same_identity_without_sending():
    checkpoint, ids = prepared()
    original = encode(checkpoint, ids)
    checkpoint.unknown(ids)
    assert encode(checkpoint, ids) == original
    checkpoint.reconcile(ids, checkpoint.ledger[ids[0]]['batch'])
    checkpoint.notifications_enabled = False
    assert encode(checkpoint, ids) == original
    assert not checkpoint.notifications_enabled


@pytest.mark.parametrize('case', ['empty', 'partial', 'duplicate', 'unknown', 'list', 'bad_type'])
def test_formatter_requires_entire_existing_batch(case):
    checkpoint, ids = prepared()
    wrong = {'empty': (), 'partial': ids[:1], 'duplicate': (ids[0], ids[0]),
             'unknown': ('f' * 64,), 'list': list(ids), 'bad_type': ([],)}[case]
    before = deepcopy(checkpoint.encode())
    with pytest.raises(ValueError):
        encode(checkpoint, wrong)
    assert checkpoint.encode() == before


def test_formatter_refuses_split_preparation_time_or_invalid_checkpoint():
    checkpoint, ids = prepared()
    checkpoint.ledger[ids[1]]['prepared_at'] = T - MINUTE
    checkpoint.ledger[ids[1]]['transition_at'] = T - MINUTE
    with pytest.raises(ValueError, match='prepare_time'):
        encode(checkpoint, ids)
    checkpoint.generation = False
    with pytest.raises(ValueError, match='checkpoint'):
        encode(checkpoint, ids)
    with pytest.raises(ValueError):
        encode(None, ids)


@pytest.mark.parametrize('field,value', [('schema', 'HOURLY_ALERT_RECEIPT_V1'), ('rules_version', 'old'),
    ('source_sha', 'a' * 39), ('run_id', True), ('attempt', 0), ('generation', 0), ('generation', True),
    ('prepared_at', T + 1), ('prepared_at', float(T)), ('ids', []), ('ids', 'x'), ('ids', [None]),
    ('batch', 'b' * 64), ('scope', None)])
def test_untrusted_wire_headers_and_types_are_rejected(field, value):
    checkpoint, ids = prepared()
    body = payload(encode(checkpoint, ids))
    body[field] = value
    with pytest.raises(ValueError):
        decode(marker_for(body))


@pytest.mark.parametrize('case', ['extra', 'missing', 'root_list', 'duplicate_ids', 'unsorted', 'scope_extra'])
def test_unknown_fields_duplicate_ids_and_noncanonical_order_are_rejected(case):
    checkpoint, ids = prepared()
    body = payload(encode(checkpoint, ids))
    if case == 'extra':
        body['published_at'] = T
    elif case == 'missing':
        del body['attempt']
    elif case == 'root_list':
        body = [body]
    elif case == 'scope_extra':
        body['scope']['self_authenticated'] = True
    else:
        body['ids'] = list(ids[::-1]) if case == 'unsorted' else [ids[0], ids[0]]
        body['batch'] = storage.digest(json.dumps(body['ids'], separators=(',', ':')).encode())
    with pytest.raises(ValueError):
        decode(marker_for(body))


@pytest.mark.parametrize('authority,value', [('source_sha', 'b' * 40), ('run_id', 6), ('attempt', 2),
    ('published_at', T - 1), ('run_id', True), ('attempt', False), ('published_at', False), ('source_sha', None)])
def test_binding_to_independent_provider_arguments_is_required(authority, value):
    checkpoint, ids = prepared()
    with pytest.raises(ValueError):
        decode(encode(checkpoint, ids), **{authority: value})
    foreign = storage.Scope(SCOPE.repository, SCOPE.workflow_id + 1, SCOPE.ref)
    with pytest.raises(ValueError, match='provider_mismatch'):
        decode(encode(checkpoint, ids), scope=foreign)


def test_formatter_rejects_bad_producer_fields_without_changing_state():
    checkpoint, ids = prepared()
    for kwargs in ({'scope': None}, {'source_sha': 'x'}, {'run_id': True}, {'attempt': False}):
        with pytest.raises(ValueError):
            encode(checkpoint, ids, **kwargs)


def test_duplicate_json_keys_utf8_depth_and_base64_aliases_are_not_admitted():
    checkpoint, ids = prepared()
    raw = json.dumps(payload(encode(checkpoint, ids)), separators=(',', ':')).encode()
    for wrong in (b'{"schema":"duplicate",' + raw[1:], b'\xff', b'[' * 2000 + b']' * 2000):
        with pytest.raises(ValueError):
            decode(marker_for(raw=wrong))
    # Alter padding bits: Python's decoder yields the same bytes; wire rejects alias.
    raw += b' ' * ((1 - len(raw)) % 3)
    encoded = base64.urlsafe_b64encode(raw).decode()
    alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
    assert encoded.endswith('==')
    alias = encoded[:-3] + alphabet[alphabet.index(encoded[-3]) ^ 1] + '=='
    assert base64.urlsafe_b64decode(alias) == raw
    with pytest.raises(ValueError, match='noncanonical'):
        decode(f'<!-- {storage.RECEIPT_TAG}:{alias} -->')
    for bad in ('+/8=', 'abc', 'YWJj===', 'YWJj\n'):
        with pytest.raises(ValueError):
            decode(f'<!-- {storage.RECEIPT_TAG}:{bad} -->')


def test_readme_extraction_is_bounded_unique_standalone_and_legacy_is_not_confirmation():
    checkpoint, ids = prepared()
    marker = encode(checkpoint, ids)
    assert storage.extract_public_receipt(('# Heading\n' + marker + '\nfooter').encode()) == marker
    assert storage.extract_public_receipt(('# Heading\r\n' + marker + '\r\n').encode()) == marker
    assert storage.extract_public_receipt(b'') is None
    assert storage.extract_public_receipt(b'<!-- HOURLY_ALERT_RECEIPT_V1:e30= -->') is None
    with pytest.raises(ValueError):
        decode('<!-- HOURLY_ALERT_RECEIPT_V1:e30= -->')
    for content in (marker + '\n' + marker, '> ' + marker, 'inline ' + marker, '    ' + marker,
                    '<!-- ' + storage.RECEIPT_TAG + ':broken', marker + ' trailer'):
        with pytest.raises(ValueError):
            storage.extract_public_receipt(content.encode())
    for wrong in (b'\xff', b'x' * (storage.MIB + 1), 'not-bytes'):
        with pytest.raises(ValueError):
            storage.extract_public_receipt(wrong)


def test_payload_marker_and_batch_capacity_guards(monkeypatch):
    checkpoint, ids = prepared()
    marker = encode(checkpoint, ids)
    with pytest.raises(ValueError):
        decode('x' * (storage.MIB + 1))
    body = payload(marker)
    body['ids'] = ['a' * 64] * 10001
    with pytest.raises(ValueError):
        decode(marker_for(body))
    with pytest.raises(ValueError):
        encode(checkpoint, ('a' * 64,) * 10001)
    monkeypatch.setattr(storage, 'RECEIPT_PAYLOAD_LIMIT', 10)
    with pytest.raises(ValueError, match='payload_limit'):
        encode(checkpoint, ids)
    with pytest.raises(ValueError, match='limit'):
        decode(marker)


def test_maximum_ten_thousand_message_batch_fits_the_actual_wire_caps():
    checkpoint, ids = prepared(10000)
    marker = encode(checkpoint, ids)
    assert len(marker.encode()) < storage.MIB
    assert len(base64.urlsafe_b64decode(marker.split(':', 1)[1].removesuffix(' -->'))) < storage.RECEIPT_PAYLOAD_LIMIT
    assert len(decode(marker).ids) == 10000


def test_parsed_receipt_reconciles_saved_unknown_intent_but_shadow_adoption_stays_disabled(tmp_path):
    backend, parent = chain(1)
    checkpoint, ids, _ = pending(parent)
    checkpoint.unknown(ids)
    raw = bundle(1, parent, at=checkpoint.at, enabled=True, ledger=checkpoint.ledger)
    add(backend, raw)
    before = deepcopy(checkpoint.encode())
    receipt = decode(encode(checkpoint, ids), published_at=checkpoint.at + 1000)
    # Fixture owns the completeness assumption; codec provides no such capability.
    now = checkpoint.at + MINUTE
    result = restore(backend, now, query(now, (receipt,)))
    assert result.reason == 'restored' and result.may_prepare
    assert all(result.checkpoint.ledger[key]['status'] == 'confirmed' for key in ids)
    assert checkpoint.encode() == before and backend.bodies['state-1'] == raw
    store = StateStore(tmp_path / 'state.json')
    with store.writer():
        adopted = store.restore_shadow(result.checkpoint)
        assert not adopted.notifications_enabled
    loaded, reason = store.load()
    assert reason == 'state_hit' and loaded.encode() == adopted.encode()


def test_parsing_does_not_make_query_complete_or_confirm_partial_or_ahead_of_state():
    backend, parent = chain(1)
    checkpoint, ids = prepared()
    checkpoint.generation = 1
    raw = bundle(1, parent, at=T, enabled=True, ledger=checkpoint.ledger)
    add(backend, raw)
    marker = encode(checkpoint, ids)
    receipt = decode(marker)
    now = T + MINUTE
    result = restore(backend, now, query(now, (receipt,), complete=False))
    assert not result.may_prepare and result.reason == 'receipts_incomplete'
    assert all(result.checkpoint.ledger[key]['status'] == 'pending' for key in ids)
    body = payload(marker)
    body['ids'] = body['ids'][:1]
    body['batch'] = storage.digest(json.dumps(body['ids'], separators=(',', ':')).encode())
    partial = decode(marker_for(body))  # Structural validity alone is not batch proof.
    result = restore(backend, now, query(now, (partial,)))
    assert not result.may_prepare and result.reason.startswith('receipts_invalid')
    body = payload(marker)
    body['generation'] = 999
    ahead = decode(marker_for(body))
    result = restore(backend, now, query(now, (ahead,)))
    assert not result.may_prepare and result.reason.startswith('receipts_invalid')
