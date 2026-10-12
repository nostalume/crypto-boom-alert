"""Local live lifecycle policy; no GitHub, cache deletion or notification effects."""
from copy import deepcopy

import pytest
from test_cache import SCOPE, SOURCE, T, authority, sealed

from crypto_boom import cache as cache
from crypto_boom.state import Checkpoint, StateStore
from crypto_boom.storage import GIB, MIB, digest
from crypto_boom.trial import MINUTE, VERSION, Transition, Unavailable

PATH = '.github/workflows/okx-live.yml'
PRODUCTS = (('okx', 'SPOT', 'ONE-USDT'), ('okx', 'SWAP', 'TWO-USDT-SWAP'))


def run(number, **changes):
    return {'id': number * 10, 'run_number': number, 'workflow_id': SCOPE.workflow_id,
            'path': PATH, 'event': 'schedule', 'head_branch': 'main', 'head_sha': SOURCE,
            'run_attempt': 1, 'repository': {'id': cache.REPO_ID}, 'head_repository': {'id': cache.REPO_ID},
            'pull_requests': [], 'status': 'completed', 'conclusion': 'success', **changes}


def history(*runs, total=None):
    return {'total_count': len(runs) if total is None else total, 'workflow_runs': list(runs)}


def previous(current, body):
    return cache.previous_run(current, body, scope=SCOPE, workflow_path=PATH)


def test_previous_run_is_order_independent_but_never_skips_direct_predecessor():
    current = run(3, status='in_progress', conclusion=None)
    assert previous(current, history(run(1), current, run(2))) == run(2)
    assert previous(run(1), history(run(1))) is None
    # A successful older run cannot hide a failed/cancelled/rerun newest run.
    for change in ({'conclusion': 'failure'}, {'conclusion': 'cancelled'}, {'conclusion': 'timed_out'},
                   {'status': 'queued', 'conclusion': None}, {'run_attempt': 2}):
        with pytest.raises((Unavailable, ValueError)):
            previous(current, history(run(1), current, run(2, **change)))


@pytest.mark.parametrize('variant', ['newer', 'missing', 'duplicate_id', 'duplicate_number', 'truncated',
                                    'foreign_repo', 'bool_repo', 'branch', 'source', 'workflow', 'path',
                                    'pull_request', 'bool_attempt', 'changed_current', 'bool_total'])
def test_previous_rejects_untrusted_incomplete_or_superseded_page(variant):
    current = run(3, status='in_progress', conclusion=None)
    producer = run(2)
    body = history(run(1), producer, current)
    if variant == 'newer':
        body = history(producer, current, run(4, status='queued', conclusion=None))
    elif variant == 'missing':
        body = history(run(1), current)
    elif variant == 'duplicate_id':
        producer['id'] = current['id']
    elif variant == 'duplicate_number':
        producer['run_number'] = 3
    elif variant == 'truncated':
        body['total_count'] = 4
    elif variant in ('foreign_repo', 'bool_repo'):
        producer['head_repository']['id'] = True if variant == 'bool_repo' else 2
    elif variant == 'branch':
        producer['head_branch'] = 'codex/test'
    elif variant == 'source':
        producer['head_sha'] = 'not-a-sha'
    elif variant == 'workflow':
        producer['workflow_id'] += 1
    elif variant == 'path':
        producer['path'] = '.github/workflows/other.yml'
    elif variant == 'pull_request':
        producer['event'] = 'pull_request'
    elif variant == 'bool_attempt':
        producer['run_attempt'] = True
    elif variant == 'changed_current':
        body['workflow_runs'][-1] = run(3, status='queued', conclusion=None)
    else:
        body['total_count'] = True
    with pytest.raises((Unavailable, ValueError)):
        previous(current, body)


def test_bounded_recent_page_with_long_history_does_not_require_all_runs():
    current = run(301, status='in_progress', conclusion=None)
    body = history(current, *(run(i) for i in range(202, 301)), total=301)
    assert previous(current, body)['run_number'] == 300


def metadata(*keys):
    return {'total_count': len(keys), 'actions_caches': [
        {'id': i + 1, 'key': key, 'ref': SCOPE.ref, 'version': 'f' * 64,
         'size_in_bytes': 1000, 'created_at': '2026-10-11T00:00:00Z'} for i, key in enumerate(keys)]}


def usage(size=0):
    return {'full_name': cache.REPO, 'active_caches_size_in_bytes': size, 'active_caches_count': 3}


@pytest.mark.parametrize('variant', ['valid', 'unknown_usage', 'over_quota', 'foreign_usage', 'incomplete',
                                    'full', 'bool_id', 'duplicate_id', 'wrong_ref', 'wrong_workflow',
                                    'oversized', 'zero_size', 'bad_version', 'test_key', 'too_many'])
def test_live_inventory_has_fixed_ownership_capacity_and_free_headroom(variant):
    key = cache.live_key(SCOPE, 10, SOURCE, 'final')
    body, used = metadata(key), usage(8 * GIB)
    item = body['actions_caches'][0]
    if variant == 'unknown_usage':
        used['active_caches_size_in_bytes'] = None
    elif variant == 'over_quota':
        used['active_caches_size_in_bytes'] += 1
    elif variant == 'foreign_usage':
        used['full_name'] = 'another/repo'
    elif variant == 'incomplete':
        body['total_count'] = 2
    elif variant == 'full':
        body['total_count'] = 4
    elif variant == 'bool_id':
        item['id'] = True
    elif variant == 'duplicate_id':
        body = metadata(key, cache.live_key(SCOPE, 10, SOURCE, 'intent'))
        body['actions_caches'][1]['id'] = 1
    elif variant == 'wrong_ref':
        item['ref'] = 'refs/heads/codex/test'
    elif variant == 'wrong_workflow':
        item['key'] = key.replace('w700-', 'w701-')
    elif variant == 'oversized':
        item['size_in_bytes'] = 128 * MIB + 1
    elif variant == 'zero_size':
        item['size_in_bytes'] = 0
    elif variant == 'bad_version':
        item['version'] = 'unknown'
    elif variant == 'test_key':
        item['key'] = cache.cache_key(10, SOURCE)
    elif variant == 'too_many':
        body = metadata(*(cache.live_key(SCOPE, i, SOURCE, 'final') for i in range(1, 5)))
    if variant == 'valid':
        observed = cache.live_inventory(body, used, scope=SCOPE)
        assert observed[key] == item and observed[key] is not item
        with pytest.raises(ValueError):
            cache.live_inventory(body, used, scope=SCOPE, max_existing=0)
    else:
        with pytest.raises(ValueError):
            cache.live_inventory(body, used, scope=SCOPE)


@pytest.mark.parametrize('variant', ['valid', 'quiet', 'missing_final', 'missing_previous', 'extra',
                                    'wrong_digest', 'corrupt', 'expired', 'pending', 'unknown', 'self_delete'])
def test_retention_protects_final_and_stops_without_full_validation(tmp_path, variant):
    _, proof = sealed(tmp_path, variant if variant in ('pending', 'unknown') else None)
    final = cache.live_key(SCOPE, 10, SOURCE, 'final')
    intent = cache.live_key(SCOPE, 10, SOURCE, 'intent')
    old = cache.live_key(SCOPE, 9, SOURCE, 'final')
    body, auth = metadata(final, intent, old), authority(proof)
    if variant == 'quiet':
        body = metadata(final, old)
    elif variant == 'missing_final':
        body = metadata(intent, old)
    elif variant == 'missing_previous':
        body = metadata(final, intent)
    elif variant == 'extra':
        body = metadata(final, intent, cache.live_key(SCOPE, 8, SOURCE, 'final'))
    elif variant == 'wrong_digest':
        auth['manifest_sha256'] = '0' * 64
    elif variant == 'corrupt':
        (tmp_path / 'bundle/checkpoint.json').write_bytes(b'corrupt')
    elif variant == 'expired':
        auth['now'] += cache.DAY
    elif variant == 'self_delete':
        old = final
    before = deepcopy(body)
    if variant in ('valid', 'quiet'):
        candidates = cache.retention(tmp_path / 'bundle', body, usage(), previous_key=old, **auth)
        assert [r['key'] for r in candidates] == ([intent, old] if variant == 'valid' else [old])
        assert final not in [r['key'] for r in candidates]
        # No file adoption or deletion; even provider records remain untouched.
        assert body == before and (tmp_path / 'bundle/manifest.json').exists()
        assert not (tmp_path / 'restored').exists()
    else:
        with pytest.raises((ValueError, Unavailable)):
            cache.retention(tmp_path / 'bundle', body, usage(), previous_key=old, **auth)
        assert body == before


def baseline(root, status=None):
    store = StateStore(root / 'checkpoint.json')
    state = Checkpoint(at=T)
    state.tracker.last_at = {('A0', key): T for key in PRODUCTS}
    with store.writer():
        store.save(state, None)
        if status:
            state.notifications_enabled = True
            ids = state.prepare([Transition((VERSION + '/A0', PRODUCTS[0], 'UP', T), 'OBSERVED', T)], T)
            if status == 'unknown':
                state.unknown(ids)
            elif status == 'confirmed':
                state.reconcile(ids, state.ledger[ids[0]]['batch'])
            state.notifications_enabled = False
            store.save(state, state.generation)
    approval = dict(checkpoint_sha256=digest(store.path.read_bytes()), generation=state.generation,
                    at=T, products=PRODUCTS)
    return store, approval


@pytest.mark.parametrize('status', [None, 'confirmed'])
def test_manual_baseline_uses_cas_keeps_history_and_sends_nothing(tmp_path, status):
    store, approval = baseline(tmp_path, status)
    before, _ = store.load()
    with store.writer():
        state = store.enable_baseline(**approval)
    assert state.notifications_enabled and state.generation == approval['generation'] + 1
    assert state.ledger == before.ledger and state.tracker.last_at == before.tracker.last_at
    with store.writer(), pytest.raises(Unavailable):
        store.enable_baseline(**approval)


@pytest.mark.parametrize('variant', ['no_lock', 'digest', 'generation', 'cutoff', 'missing_product',
                                    'stale_product', 'duplicate', 'empty', 'pending', 'unknown', 'enabled',
                                    'bool_generation', 'save_failure'])
def test_manual_baseline_never_overwrites_unknown_or_partial_state(tmp_path, variant, monkeypatch):
    store, approval = baseline(tmp_path, variant if variant in ('pending', 'unknown') else None)
    if variant in ('missing_product', 'stale_product', 'enabled'):
        with store.writer():
            state, _ = store.load()
            if variant == 'missing_product':
                state.tracker.last_at.pop(('A0', PRODUCTS[1]))
            elif variant == 'stale_product':
                state.tracker.last_at[('A0', PRODUCTS[1])] -= MINUTE
            else:
                state.notifications_enabled = True
            store.save(state, state.generation)
        approval.update(checkpoint_sha256=digest(store.path.read_bytes()), generation=state.generation)
    elif variant == 'digest':
        approval['checkpoint_sha256'] = '0' * 64
    elif variant == 'generation':
        approval['generation'] += 1
    elif variant == 'cutoff':
        approval['at'] += MINUTE
    elif variant == 'duplicate':
        approval['products'] = (PRODUCTS[0], PRODUCTS[0])
    elif variant == 'empty':
        approval['products'] = ()
    elif variant == 'bool_generation':
        approval['generation'] = True
    elif variant == 'save_failure':
        import crypto_boom.state as owner

        def failed_write(*args):
            raise OSError('injected atomic write failure')

        monkeypatch.setattr(owner, 'atomic_write', failed_write)
    before = store.path.read_bytes()
    if variant == 'no_lock':
        with pytest.raises(Unavailable):
            store.enable_baseline(**approval)
    else:
        with store.writer(), pytest.raises((ValueError, Unavailable, OSError)):
            store.enable_baseline(**approval)
    assert store.path.read_bytes() == before
    assert not store.lock_path.exists()


def test_shadow_restore_explicit_baseline_and_final_retention_compose(tmp_path):
    origin, approval = baseline(tmp_path / 'origin')
    first = cache.seal(origin.path.parent, tmp_path / 'previous', scope=SCOPE, source_sha=SOURCE,
                       run_id=10, created_at=T + 1000)
    current = run(2, status='in_progress', conclusion=None)
    producer = previous(current, history(current, run(1)))
    assert producer['id'] == 10
    cache.restore(tmp_path / 'previous', tmp_path / 'restored', scope=SCOPE, source_sha=producer['head_sha'],
                  run_id=producer['id'], manifest_sha256=first['manifest_sha256'], now=T + 2000)
    restored = StateStore(tmp_path / 'restored/checkpoint.json')
    state, _ = restored.load()
    assert not state.notifications_enabled
    with restored.writer():
        state = restored.enable_baseline(**{**approval, 'checkpoint_sha256': digest(restored.path.read_bytes())})
    final = cache.seal(restored.path.parent, tmp_path / 'final', scope=SCOPE, source_sha=SOURCE,
                       run_id=current['id'], created_at=T + 3000)
    keep = cache.live_key(SCOPE, current['id'], SOURCE, 'final')
    old = cache.live_key(SCOPE, producer['id'], SOURCE, 'final')
    candidates = cache.retention(tmp_path / 'final', metadata(old, keep), usage(), previous_key=old,
                                 scope=SCOPE, source_sha=SOURCE, run_id=current['id'],
                                 manifest_sha256=final['manifest_sha256'], now=T + 4000)
    assert [r['key'] for r in candidates] == [old]
    assert restored.load()[0].generation == state.generation
