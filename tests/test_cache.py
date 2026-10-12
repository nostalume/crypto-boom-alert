"""Single-cache identity/retention, shadow adoption, failure stops and workflow."""
import json

import pytest

from crypto_boom import cache as cache
from crypto_boom.state import Checkpoint, StateStore
from crypto_boom.storage import DAY, Scope, digest
from crypto_boom.trial import VERSION, Transition, Unavailable

T = 1780000020000
SOURCE = 'a' * 40
SCOPE = Scope(cache.REPO, 700, 'refs/heads/main')


def origin(root, status=None):
    store = StateStore(root / 'checkpoint.json')
    state = Checkpoint(at=T, cursor=23)
    with store.writer():
        store.save(state, None)
    if status:
        state.notifications_enabled = True
        ids = state.prepare([Transition((VERSION + '/A0', ('okx', 'SPOT', 'BTC-USDT'), 'UP', T), 'OBSERVED', T)], T)
        if status == 'unknown':
            state.unknown(ids)
        with store.writer():
            store.save(state, 1)
    (root / 'market').mkdir()
    (root / 'market' / ('f' * 64 + '.json.gz')).write_bytes(b'bounded opaque compressed bytes')
    return state


def sealed(root, status=None):
    state = origin(root / 'origin', status)
    result = cache.seal(root / 'origin', root / 'bundle', scope=SCOPE, source_sha=SOURCE, run_id=10, created_at=T + 1000)
    return state, result


def authority(result):
    return dict(scope=SCOPE, source_sha=SOURCE, run_id=10, manifest_sha256=result['manifest_sha256'], now=T + 2000)


@pytest.mark.parametrize('status', [None, 'pending', 'unknown'])
def test_round_trip_preserves_state_and_unresolved_ledger_but_never_enables_notifications(tmp_path, status):
    expected, result = sealed(tmp_path, status)
    observed = cache.restore(tmp_path / 'bundle', tmp_path / 'restored', **authority(result))
    state, reason = StateStore(tmp_path / 'restored/checkpoint.json').load()
    assert reason == 'state_hit' and not state.notifications_enabled
    assert state.generation == expected.generation and state.cursor == 23 and state.at == T
    assert cache._json(state.ledger) == cache._json(expected.ledger)
    assert state.tracker.last_at == expected.tracker.last_at
    assert observed['unresolved_entries'] == len(expected.ledger)
    assert observed['publication'] == 'disabled'
    assert not (tmp_path / 'restored/checkpoint.json.lock').exists()
    with pytest.raises(Unavailable):
        state.prepare([], T)
    before = (tmp_path / 'restored/checkpoint.json').read_bytes()
    with pytest.raises(ValueError, match='fresh_workspace'):
        cache.restore(tmp_path / 'bundle', tmp_path / 'restored', **authority(result))
    assert (tmp_path / 'restored/checkpoint.json').read_bytes() == before


@pytest.mark.parametrize('variant', ['missing', 'digest', 'scope', 'source', 'run', 'expired', 'future',
                                    'checkpoint', 'partition', 'extra', 'missing_file', 'generation',
                                    'code', 'attempt', 'traversal', 'size', 'duplicate_json'])
def test_unadmitted_bundle_never_creates_recovered_state(tmp_path, variant):
    _, result = sealed(tmp_path)
    bundle, target = tmp_path / 'bundle', tmp_path / 'unused'
    auth = authority(result)
    body = json.loads((bundle / 'manifest.json').read_bytes())
    if variant == 'missing':
        bundle = tmp_path / 'missing'
    elif variant == 'digest':
        auth['manifest_sha256'] = '0' * 64
    elif variant == 'scope':
        auth['scope'] = Scope(cache.REPO, 701, SCOPE.ref)
    elif variant == 'source':
        auth['source_sha'] = 'b' * 40
    elif variant == 'run':
        auth['run_id'] = 11
    elif variant == 'expired':
        auth['now'] = T + 1000 + DAY
    elif variant == 'future':
        auth['now'] = T
    elif variant == 'checkpoint':
        (bundle / 'checkpoint.json').write_bytes(b'corrupted')
    elif variant == 'partition':
        next((bundle / 'market').iterdir()).write_bytes(b'corrupted')
    elif variant == 'extra':
        (bundle / 'unowned.txt').write_bytes(b'no')
    elif variant == 'missing_file':
        next((bundle / 'market').iterdir()).unlink()
    else:
        if variant in ('generation', 'attempt'):
            body[variant] = 50
        elif variant == 'code':
            body['code_sha256'] = '0' * 64
        elif variant == 'traversal':
            body['files']['../outside'] = body['files'].pop('checkpoint.json')
        elif variant == 'size':
            body['files']['checkpoint.json']['bytes'] = cache.PAYLOAD_LIMIT + 1
        raw = cache._json(body)
        if variant == 'duplicate_json':
            raw = b'{"schema":"duplicate",' + raw[1:]
        (bundle / 'manifest.json').write_bytes(raw)
        auth['manifest_sha256'] = digest(raw)  # Even independently bound malformed manifests fail.
    with pytest.raises((ValueError, OSError)):
        cache.restore(bundle, target, **auth)
    assert not target.exists()


def test_export_refuses_writer_lock_existing_target_and_oversized_partition(tmp_path):
    origin(tmp_path / 'origin')
    lock = tmp_path / 'origin/checkpoint.json.lock'
    lock.write_text('foreign writer')
    kwargs = dict(scope=SCOPE, source_sha=SOURCE, run_id=10, created_at=T + 1000)
    with pytest.raises(ValueError, match='quiescent'):
        cache.seal(tmp_path / 'origin', tmp_path / 'bundle', **kwargs)
    assert not (tmp_path / 'bundle').exists()
    lock.unlink()
    next((tmp_path / 'origin/market').iterdir()).write_bytes(b'x' * (cache.MIB + 1))
    with pytest.raises(ValueError, match='size_limit'):
        cache.seal(tmp_path / 'origin', tmp_path / 'bundle', **kwargs)
    assert not (tmp_path / 'bundle/manifest.json').exists()
    with pytest.raises(ValueError, match='fresh_target'):
        cache.seal(tmp_path / 'origin', tmp_path / 'bundle', **kwargs)
