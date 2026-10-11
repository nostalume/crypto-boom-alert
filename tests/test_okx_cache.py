"""Single-cache identity/retention, shadow adoption, failure stops and workflow."""
import json
import sys
from pathlib import Path

import pytest
import yaml
from test_okx_benchmark import T, write_report

from crypto_boom import okx_cache as cache
from crypto_boom.okx_state import Checkpoint, StateStore
from crypto_boom.okx_storage import DAY, GIB, Scope, digest
from crypto_boom.okx_trial import VERSION, Transition, Unavailable

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


def run(identity, source=SOURCE):
    return {'id': identity, 'head_sha': source, 'workflow_id': SCOPE.workflow_id, 'path': cache.WORKFLOW,
            'event': 'workflow_dispatch', 'run_attempt': 1, 'head_branch': 'main',
            'repository': {'id': cache.REPO_ID}, 'head_repository': {'id': cache.REPO_ID},
            'pull_requests': [], 'status': 'completed', 'conclusion': 'success'}


def usage(size=0):
    return {'full_name': cache.REPO, 'active_caches_size_in_bytes': size, 'active_caches_count': 0}


@pytest.mark.parametrize('variant', ['valid', 'quota', 'quota_unknown', 'wrong_repo', 'fork', 'pull_request',
                                    'wrong_workflow', 'rerun', 'source', 'failed', 'unfinished', 'same_run', 'digest'])
def test_authenticated_run_and_free_headroom_admission(variant):
    current, producer, used = run(20, 'b' * 40), run(10), usage()
    values = dict(run_id=20, source_sha='b' * 40, producer_id=10, producer_sha=SOURCE, expected_digest='c' * 64)
    if variant == 'quota':
        used = usage(8 * GIB + 1)
    elif variant == 'quota_unknown':
        used = usage(None)
    elif variant == 'wrong_repo':
        used['full_name'] = 'foreign/repo'
    elif variant == 'fork':
        producer['head_repository']['id'] = 2
    elif variant == 'pull_request':
        producer['event'] = 'pull_request'
    elif variant == 'wrong_workflow':
        producer['workflow_id'] += 1
    elif variant == 'rerun':
        producer['run_attempt'] = 2
    elif variant == 'source':
        producer['head_sha'] = 'f' * 40
    elif variant == 'failed':
        producer['conclusion'] = 'failure'
    elif variant == 'unfinished':
        producer['status'] = 'in_progress'
    elif variant == 'same_run':
        values['producer_id'] = 20
        producer['id'] = 20
    elif variant == 'digest':
        values['expected_digest'] = 'wrong'
    if variant == 'valid':
        assert cache.admission('restore', current, producer, used, **values) == SCOPE
    else:
        with pytest.raises(ValueError):
            cache.admission('restore', current, producer, used, **values)


def test_linux_fault_command_is_bounded_and_keeps_original_bundle_unchanged(tmp_path):
    _, result = sealed(tmp_path)
    before = (tmp_path / 'bundle/manifest.json').read_bytes()
    outcomes = cache.fault_checks(tmp_path / 'bundle', tmp_path, authority(result))
    assert set(outcomes) == {'missing', 'wrong_digest', 'wrong_source', 'expired', 'corrupt', 'pending', 'unknown'}
    assert all('blocked' in value for value in outcomes.values())
    assert (tmp_path / 'bundle/manifest.json').read_bytes() == before


@pytest.mark.parametrize('variant', ['valid', 'miss', 'cold', 'partial', 'slow', 'stale'])
def test_consumer_cli_requires_exact_cache_and_two_advancing_full_warm_rounds(tmp_path, monkeypatch, variant):
    _, result = sealed(tmp_path)
    (tmp_path / 'bundle').rename(tmp_path / 'transfer')
    for name, body in [('current', run(20, 'b' * 40)), ('producer', run(10)), ('usage', usage())]:
        (tmp_path / (name + '.json')).write_text(json.dumps(body))
    env = {'CACHE_MODE': 'restore', 'GITHUB_RUN_ID': '20', 'GITHUB_SHA': 'b' * 40,
           'PRODUCER_RUN': '10', 'PRODUCER_SHA': SOURCE, 'BUNDLE_DIGEST': result['manifest_sha256'],
           'GITHUB_REPOSITORY': cache.REPO, 'GITHUB_REF': SCOPE.ref, 'GITHUB_EVENT_NAME': 'workflow_dispatch',
           'GITHUB_RUN_ATTEMPT': '1', 'CACHE_HIT': 'false' if variant == 'miss' else 'true'}
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(cache.time, 'time', lambda: (T + 300000) / 1000)
    calls = []
    def scan(workspace, number, limit, workers):
        calls.append((number, limit, workers))
        value = write_report(workspace, number, at=T if variant == 'stale' else T + number * 60000,
                             status='partial' if variant == 'partial' else 'complete')
        value['products'][0]['cache'] = 'cache_missing' if variant == 'cold' else 'cache_hit'
        (workspace / 'scan-report.json').write_text(json.dumps(value))
        (workspace / 'market/one.json.gz').unlink()
        return {'exit_code': 0, 'wall_seconds': 600 if variant == 'slow' else 1}
    monkeypatch.setattr(cache, 'measure_scan', scan)
    monkeypatch.setattr(sys, 'argv', ['okx_cache', 'run', '--root', str(tmp_path)])
    if variant == 'valid':
        cache.main()
        report = json.loads((tmp_path / 'acceptance-report.json').read_bytes())
        assert len(report['rounds']) == 2 and report['restore']['generation'] == 1
        assert [r['state_generation'] for r in report['rounds']] == [2, 3]
        assert all(r['cold_products'] == 0 for r in report['rounds'])
        assert report['sealed']['generation'] == 3 and report['publication'] == 'disabled'
        assert calls == [(1, 1000, 4), (2, 1000, 4)]
        assert (tmp_path / 'incoming/manifest.json').exists() and (tmp_path / 'transfer/manifest.json').exists()
    else:
        with pytest.raises(ValueError):
            cache.main()
        assert not (tmp_path / 'acceptance-report.json').exists()
        assert len(calls) <= 1


def test_new_acceptance_workflow_has_one_manual_job_exact_paths_and_no_production_effect():
    root = Path(__file__).resolve().parents[1]
    raw = (root / cache.WORKFLOW).read_text()
    value = yaml.safe_load(raw)
    assert set(value['on']) == {'workflow_dispatch'}
    assert value['permissions'] == {'contents': 'read', 'actions': 'read'}
    assert value['concurrency'] == {'group': 'okx-cache-acceptance', 'cancel-in-progress': False}
    job = value['jobs']['acceptance']
    assert job['runs-on'] == 'ubuntu-latest' and job['timeout-minutes'] == 30
    remote = [s for s in job['steps'] if 'uses' in s]
    assert all(repr(s['uses']).count('@') == 1 and len(s['uses'].split('@')[1]) == 40 for s in remote)
    restore, save = remote[-2:]
    assert restore['with']['path'] == save['with']['path']
    assert 'restore-keys' not in restore['with'] and restore['with']['fail-on-cache-miss'] is True
    assert restore['if'] == "inputs.mode == 'restore'" and restore['id'] == 'restored'
    assert 'upload-artifact' not in raw and '--publish-readme' not in raw
    assert 'actions/cache@' not in raw  # Restore/save split: no automatic post-save.
    for step in job['steps']:
        if 'run' in step:
            assert '${{' not in step['run'] and step['shell'] == 'bash'
