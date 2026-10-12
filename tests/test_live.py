"""Provider-shaped metadata and native handoff fixtures; no real API/DELETE/mail."""
import base64
import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

import httpx
import pytest
import test_publish as provider
import yaml

from crypto_boom import cache as cache
from crypto_boom import live as live
from crypto_boom import runtime as runtime
from crypto_boom import scan as scanner
from crypto_boom.state import Checkpoint, StateStore
from crypto_boom.storage import DAY, GIB, Scope, digest
from crypto_boom.trial import MINUTE, Product, Unavailable

T, SOURCE = provider.T, provider.SOURCE
NOW = T + MINUTE + 3000
SCOPE = Scope(cache.REPO, 456, 'refs/heads/main')
KEY = ('okx', 'SPOT', 'ALT-USDT')


def write(root, name, body):
    (root / name).write_text(json.dumps(body), encoding='utf-8')


def read(root, name):
    return json.loads((root / name).read_bytes())


def run(number, **kwargs):
    return {'id': number * 10, 'run_number': number, 'workflow_id': 456, 'path': live.WORKFLOW,
            'event': 'workflow_dispatch', 'head_branch': 'main', 'head_sha': SOURCE, 'run_attempt': 1,
            'repository': {'id': cache.REPO_ID, 'private': False},
            'head_repository': {'id': cache.REPO_ID, 'private': False}, 'pull_requests': [],
            'status': 'completed', 'conclusion': 'success', **kwargs}


def listing(*records):
    return {'total_count': len(records), 'actions_caches': list(records)}


def record(identity, phase, number=2):
    return {'id': identity, 'key': cache.live_key(SCOPE, number * 10, SOURCE, phase),
            'ref': SCOPE.ref, 'version': 'f' * 64, 'size_in_bytes': 1000,
            'created_at': '2026-10-11T00:00:00Z', 'last_accessed_at': '2026-10-11T00:00:00Z'}


def jobs(proof, *, consume=False, omitted=False, number=1):
    name = f"OKX_FINAL_V1 {proof['manifest_sha256']} {proof['checkpoint_sha256']} {proof['generation']} {proof['at']}"
    rows = []
    for i, job_name in enumerate(('prepare', 'consume')):
        if omitted and job_name == 'consume':
            continue
        owner = (job_name == 'consume') == consume
        rows.append({'id': 10 + i, 'name': job_name, 'run_id': number * 10, 'head_sha': SOURCE, 'head_branch': 'main',
                     'status': 'completed', 'conclusion': 'success' if owner or consume else 'skipped',
                     'steps': [{'name': name, 'number': 7, 'status': 'completed', 'conclusion': 'success'}] if owner else []})
    return {'total_count': len(rows), 'jobs': rows}


def offline_scan_job(*args):
    """Spawn-safe fixtures installed only inside the disposable test child."""
    class Metadata:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, path, params=None):
            if path.endswith('/time'):
                return [{'ts': str(NOW)}]
            return [{'instId': 'ALT-USDT'}] if params['instType'] == 'SPOT' else []

    def scan(products, at, cache_owner, state, budget, **kwargs):
        state.at = at
        state.tracker.last_at[('A0', KEY)] = at
        return {'at': at, 'pool': 1, 'complete': 1, 'partial': 0, 'warmup': 0, 'already_evaluated': 0,
                'deferred': 0, 'http_attempts': 0, 'http_retries': 0, 'response_bytes': 0,
                'publication': 'disabled', 'products': [], 'transitions': []}

    scanner.PublicClient = Metadata
    scanner.admit_product = lambda *args: Product('SPOT', 'ALT-USDT', 'ALT', 0)
    scanner.scan = scan
    write(args[0], 'child.json', {'pid': os.getpid()})
    scanner.scan_job(*args)


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(provider, 'SCOPE', SCOPE)
    monkeypatch.setattr(live.time, 'time', lambda: NOW / 1000)
    env = {'GITHUB_REPOSITORY': cache.REPO, 'GITHUB_REF': SCOPE.ref, 'GITHUB_RUN_ATTEMPT': '1',
           'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_RUN_ID': '20', 'GITHUB_SHA': SOURCE,
           'MODE': 'preview', 'REBUILD': 'false', 'LIVE_ENABLED': 'true', 'ARM_MANIFEST': '',
           'GH_TOKEN': 'test-only-secret', 'GITHUB_OUTPUT': str(tmp_path / 'outputs'), 'CACHE_HIT': 'true'}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    root = tmp_path / 'prepare'
    root.mkdir()
    write(root, 'current.json', run(2, status='in_progress', conclusion=None))
    write(root, 'history.json', {'total_count': 2, 'workflow_runs': [read(root, 'current.json'), run(1)]})
    write(root, 'usage.json', {'full_name': cache.REPO, 'active_caches_size_in_bytes': 0, 'active_caches_count': 0})
    write(root, 'caches.json', listing())
    write(root, 'target-ref.json', {'ref': SCOPE.ref, 'object': {'type': 'commit', 'sha': provider.HEAD}})
    write(root, 'target-readme.json', {'type': 'file', 'path': 'README.md', 'encoding': 'base64',
                                     'content': base64.b64encode(provider.README).decode(),
                                     'sha': provider.publication._git_blob(provider.README)})

    class Metadata:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, path, params=None):
            if path.endswith('/time'):
                return [{'ts': str(NOW)}]
            return [{'instId': 'ALT-USDT'}] if params['instType'] == 'SPOT' else []

    signal = {'on': False}

    def scan(products, at, cache_owner, state, budget, **kwargs):
        state.at = at
        state.tracker.last_at[('A0', KEY)] = at
        return {'at': at, 'pool': 1, 'complete': 1, 'partial': 0, 'warmup': 0, 'already_evaluated': 0,
                'deferred': 0, 'http_attempts': 0, 'http_retries': 0, 'response_bytes': 0, 'publication': 'disabled',
                'products': [{'product': KEY, 'status': 'complete'}],
                'transitions': [asdict(provider.change(at=at))] if signal['on'] else []}

    monkeypatch.setattr(scanner, 'PublicClient', Metadata)
    monkeypatch.setattr(scanner, 'admit_product', lambda *args: Product('SPOT', 'ALT-USDT', 'ALT', 0))
    monkeypatch.setattr(scanner, 'scan', scan)

    def inline_supervisor(target, args, **kwargs):
        target(*args)  # Real borrowed writer and scan commit; process seam tested separately.
        return 0

    monkeypatch.setattr(live, 'supervise', inline_supervisor)

    def seed(enabled=False, status=None):
        origin = StateStore(tmp_path / 'origin/checkpoint.json')
        state = Checkpoint(at=T)
        state.tracker.last_at[('A0', KEY)] = T
        with origin.writer():
            origin.save(state, None)
            state.notifications_enabled = True
            if status:
                ids = state.prepare([provider.change(at=T)], T)
                if status == 'unknown':
                    state.unknown(ids)
                elif status == 'confirmed':
                    state.reconcile(ids, state.ledger[ids[0]]['batch'])
            state.notifications_enabled = enabled
            origin.save(state, state.generation)
        proof = cache.seal(origin.path.parent, root / 'transfer', scope=SCOPE, source_sha=SOURCE, run_id=10, created_at=T + 1000)
        proof['checkpoint_sha256'] = digest((root / 'transfer/checkpoint.json').read_bytes())
        write(root, 'producer-jobs.json', jobs(proof))
        write(root, 'caches.json', listing(record(101, 'final', 1)))
        return proof

    return root, seed, signal


def prepared(fixture, monkeypatch, *, enabled=False, approve=False, signal=False):
    root, seed, flag = fixture
    proof = seed(enabled=enabled)
    monkeypatch.setenv('MODE', 'live' if enabled or approve else 'preview')
    if approve:
        monkeypatch.setenv('ARM_MANIFEST', proof['manifest_sha256'])
    flag['on'] = signal
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    return root, proof


def consumer(root, monkeypatch):
    fresh = root.parent / 'consume'
    fresh.mkdir()
    for name in ('current.json', 'history.json', 'usage.json', 'target-ref.json', 'target-readme.json'):
        shutil.copyfile(root / name, fresh / name)
    shutil.copytree(root / 'transfer', fresh / 'transfer')
    proof = read(root, 'export.json')
    for key, value in {'INTENT_MANIFEST': proof['manifest_sha256'], 'INTENT_CHECKPOINT': proof['checkpoint_sha256'],
                       'TARGET_HEAD': provider.HEAD, 'PREVIOUS_KEY': record(101, 'final', 1)['key'], 'ARM_MANIFEST': ''}.items():
        monkeypatch.setenv(key, value)
    write(fresh, 'caches.json', listing(record(101, 'final', 1), record(102, 'intent')))
    return fresh


@pytest.mark.parametrize('enabled,approve', [(False, False), (False, True), (True, False)])
def test_preview_baseline_and_resume_quiet_final_progress_without_publisher(fixture, monkeypatch, enabled, approve):
    root, _ = prepared(fixture, monkeypatch, enabled=enabled, approve=approve)
    proof = read(root, 'export.json')
    assert proof['key'].endswith('-final') and not (root / 'cleanup.json').exists()
    state, _ = StateStore(root / 'transfer/checkpoint.json').load()
    assert state.at == T + MINUTE and state.notifications_enabled == (enabled or approve)
    live.guard_save(root)
    with pytest.raises(Unavailable):
        live.plan_cleanup(root, now=NOW)  # Native save warn-only cannot stand in for object observation.
    write(root, 'caches.json', listing(record(101, 'final', 1), record(103, 'final')))
    live.plan_cleanup(root, now=NOW)
    assert [r['id'] for r in read(root, 'cleanup.json')] == [101]
    live.check_cleanup(root, 101)
    write(root, 'caches.json', listing(record(103, 'final')))
    live.finish(root)
    assert read(root, 'result.json')['publication'] == 'not_sent'


def test_live_native_handoff_publish_once_confirmed_final_and_repeat(fixture, monkeypatch):
    root, _ = prepared(fixture, monkeypatch, enabled=True, signal=True)
    live.guard_save(root)
    fresh = consumer(root, monkeypatch)
    api = provider.Provider()
    live.consume(fresh, now=NOW, transport=httpx.MockTransport(api.handle))
    assert api.patch_calls == 1 and 'TEST ONLY' not in api.message
    final, _ = StateStore(fresh / 'transfer/checkpoint.json').load()
    assert final.notifications_enabled and {v['status'] for v in final.ledger.values()} == {'confirmed'}
    with pytest.raises(ValueError, match='fresh_consumer'):
        live.consume(fresh, now=NOW, transport=httpx.MockTransport(api.handle))
    assert api.patch_calls == 1
    live.guard_save(fresh)
    write(fresh, 'caches.json', listing(record(101, 'final', 1), record(102, 'intent'), record(103, 'final')))
    live.plan_cleanup(fresh, now=NOW)
    assert [r['id'] for r in read(fresh, 'cleanup.json')] == [102, 101]
    for identity in (102, 101):
        live.check_cleanup(fresh, identity)
    write(fresh, 'caches.json', listing(record(103, 'final')))
    live.finish(fresh)
    assert read(fresh, 'result.json')['commit'] == provider.COMMIT


@pytest.mark.parametrize('variant', ['gate', 'missing_head',
                                    'changed_head', 'missing_readback', 'wrong_digest', 'wrong_checkpoint',
                                    'unknown_provider', 'source_repo_private'])
def test_consumer_failure_never_retries_or_pretends_confirmed(fixture, monkeypatch, variant):
    root, _ = prepared(fixture, monkeypatch, enabled=True, signal=True)
    fresh = consumer(root, monkeypatch)
    if variant == 'gate':
        monkeypatch.setenv('LIVE_ENABLED', 'false')
    elif variant == 'missing_head':
        monkeypatch.delenv('TARGET_HEAD')
    elif variant == 'changed_head':
        write(fresh, 'target-ref.json', {'ref': SCOPE.ref, 'object': {'type': 'commit', 'sha': 'f' * 40}})
    elif variant == 'missing_readback':
        monkeypatch.setenv('CACHE_HIT', 'false')
    elif variant == 'wrong_digest':
        monkeypatch.setenv('INTENT_MANIFEST', '0' * 64)
    elif variant == 'wrong_checkpoint':
        monkeypatch.setenv('INTENT_CHECKPOINT', '0' * 64)
    elif variant == 'source_repo_private':
        body = read(fresh, 'current.json')
        body['repository']['private'] = True
        write(fresh, 'current.json', body)
    api = provider.Provider(fault='accepted_timeout' if variant == 'unknown_provider' else None)
    with pytest.raises((Unavailable, ValueError)):
        live.consume(fresh, now=NOW, transport=httpx.MockTransport(api.handle))
    assert api.patch_calls == int(variant == 'unknown_provider')
    state, _ = StateStore(fresh / 'transfer/checkpoint.json').load()
    assert {v['status'] for v in state.ledger.values()} == ({'unknown'} if variant == 'unknown_provider' else {'pending'})
    assert not (fresh / 'export.json').exists()


@pytest.mark.parametrize('variant', ['bad_proof', 'wrong_proof_checkpoint', 'pending', 'unknown', 'miss', 'corrupt', 'expired'])
def test_invalid_predecessor_preview_is_nonpersisting_diagnostic(fixture, monkeypatch, variant):
    root, seed, _ = fixture
    proof = seed(status=variant if variant in ('pending', 'unknown') else None)
    if variant == 'bad_proof':
        write(root, 'producer-jobs.json', {'total_count': 0, 'jobs': []})
    elif variant == 'wrong_proof_checkpoint':
        proof['checkpoint_sha256'] = '0' * 64
        write(root, 'producer-jobs.json', jobs(proof))
    elif variant == 'miss':
        monkeypatch.setenv('CACHE_HIT', 'false')
    elif variant == 'corrupt':
        (root / 'transfer/checkpoint.json').write_bytes(b'corrupt')
    elif variant == 'expired':
        monkeypatch.setattr(live.time, 'time', lambda: (T + DAY + 2000) / 1000)
    live.select(root)
    live.admit(root)
    live.prepare(root, now=T + DAY + 2000 if variant == 'expired' else NOW)
    assert read(root, 'result.json')['blocked'] and not (root / 'export.json').exists()
    path = root / ('diagnostic' if (root / 'diagnostic').exists() else 'workspace') / 'checkpoint.json'
    assert not StateStore(path).load()[0].notifications_enabled


def test_failed_previous_never_uses_old_success_and_explicit_empty_rebuild_is_nonnotifying(fixture, monkeypatch):
    root, _, _ = fixture
    body = read(root, 'history.json')
    body['workflow_runs'][1]['conclusion'] = 'failure'
    write(root, 'history.json', body)
    live.select(root)
    assert not read(root, 'selection.json')['persist']
    monkeypatch.setenv('REBUILD', 'true')
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    assert read(root, 'export.json')['key'].endswith('-final')
    assert not StateStore(root / 'transfer/checkpoint.json').load()[0].notifications_enabled


@pytest.mark.parametrize('variant', ['missing_approval', 'wrong_approval', 'quota', 'unknown_quota', 'old_intent',
                                    'newer_run', 'fresh_live', 'rebuild_with_cache', 'missing_product'])
def test_prepare_admission_blocks_unsafe_activation(fixture, monkeypatch, variant):
    root, seed, _ = fixture
    proof = seed() if variant != 'fresh_live' else None
    monkeypatch.setenv('MODE', 'live' if variant != 'rebuild_with_cache' else 'preview')
    if proof:
        monkeypatch.setenv('ARM_MANIFEST', proof['manifest_sha256'])
    if variant == 'missing_approval':
        monkeypatch.setenv('ARM_MANIFEST', '')
    elif variant == 'wrong_approval':
        monkeypatch.setenv('ARM_MANIFEST', '0' * 64)
    elif variant in ('quota', 'unknown_quota'):
        used = read(root, 'usage.json')
        used['active_caches_size_in_bytes'] = 8 * GIB + 1 if variant == 'quota' else None
        write(root, 'usage.json', used)
    elif variant == 'old_intent':
        write(root, 'caches.json', listing(record(101, 'intent', 1)))
    elif variant == 'newer_run':
        write(root, 'history.json', {'total_count': 3, 'workflow_runs': [run(3), run(2, status='in_progress', conclusion=None), run(1)]})
    elif variant == 'rebuild_with_cache':
        monkeypatch.setenv('REBUILD', 'true')
        monkeypatch.setenv('ARM_MANIFEST', '')
    elif variant == 'missing_product':
        monkeypatch.setattr(scanner, 'admit_product', lambda *args: Product('SPOT', 'NEW-USDT', 'NEW', 0))
    with pytest.raises((Unavailable, ValueError)):
        live.select(root)
        live.admit(root)
        live.prepare(root, now=NOW)
    assert not (root / 'export.json').exists()


@pytest.mark.parametrize('omitted,consume', [(False, False), (True, False), (False, True)])
def test_proof_can_come_only_from_successful_final_owner(fixture, omitted, consume):
    root, seed, _ = fixture
    proof = seed()
    assert live.producer_proof(jobs(proof, omitted=omitted, consume=consume), run(1)) == {
        k: proof[k] for k in ('manifest_sha256', 'checkpoint_sha256', 'generation', 'at')}


@pytest.mark.parametrize('variant', ['partial', 'duplicate', 'foreign_run', 'source', 'foreign_name',
                                    'failed', 'malformed', 'pending_step', 'missing', 'bool_count'])
def test_producer_proof_rejects_incomplete_or_forged_jobs(fixture, variant):
    root, seed, _ = fixture
    body = jobs(seed())
    item, step = body['jobs'][0], body['jobs'][0]['steps'][0]
    if variant == 'partial':
        body['jobs'].pop()
    elif variant == 'duplicate':
        item['steps'].append(deepcopy(step))
    elif variant == 'foreign_run':
        item['run_id'] += 1
    elif variant == 'source':
        item['head_sha'] = '0' * 40
    elif variant == 'foreign_name':
        item['name'] = 'evil'
    elif variant == 'failed':
        item['conclusion'] = 'failure'
    elif variant == 'malformed':
        step['name'] += ' arbitrary text'
    elif variant == 'pending_step':
        step['status'] = 'in_progress'
    elif variant == 'missing':
        step['conclusion'] = 'skipped'
    else:
        body['total_count'] = True
    with pytest.raises(ValueError):
        live.producer_proof(body, run(1))


def test_cleanup_rechecks_exact_object_and_final_is_required(fixture, monkeypatch):
    root, _ = prepared(fixture, monkeypatch)
    write(root, 'caches.json', listing(record(101, 'final', 1), record(103, 'final')))
    live.plan_cleanup(root, now=NOW)
    accessed = record(101, 'final', 1)
    accessed['last_accessed_at'] = '2026-10-11T01:00:00Z'
    write(root, 'caches.json', listing(accessed, record(103, 'final')))
    live.check_cleanup(root, 101)  # A restore may update access time, not object identity.
    changed = record(101, 'final', 1)
    changed['id'] = 999  # Same key recreated; never delete the replacement object.
    write(root, 'caches.json', listing(changed, record(103, 'final')))
    with pytest.raises(Unavailable):
        live.check_cleanup(root, 101)
    with pytest.raises((Unavailable, ValueError)):
        live.finish(root)  # Cleanup still contains an old/recreated object.
    write(root, 'caches.json', listing())
    with pytest.raises(Unavailable):
        live.finish(root)


def test_production_workflow_has_bounded_schedule_and_no_inline_untrusted_code():
    root = Path(__file__).resolve().parents[1]
    raw = (root / live.WORKFLOW).read_text(encoding='utf-8')
    value = yaml.safe_load(raw)
    assert set(value['on']) == {'workflow_dispatch', 'schedule'}
    assert value['on']['schedule'] == [{'cron': '3-53/10 * * * *'}]
    assert value['env']['MODE'] == "${{ github.event_name == 'schedule' && 'live' || inputs.mode || 'preview' }}"
    assert value['permissions'] == {} and value['concurrency']['cancel-in-progress'] is False
    assert value['on']['workflow_dispatch']['inputs']['mode']['default'] == 'preview'
    prepare, consume = value['jobs']['prepare'], value['jobs']['consume']
    assert prepare['permissions'] == {'contents': 'read', 'actions': 'write'}
    assert consume['permissions'] == {'contents': 'write', 'actions': 'write'}
    assert consume['if'] == "needs.prepare.outputs.has_intent == 'true'"
    paths = set()
    for job in (prepare, consume):
        assert job['runs-on'] == 'ubuntu-24.04' and job['timeout-minutes'] == 15
        for step in job['steps']:
            if 'uses' in step:
                assert len(step['uses'].split('@')[1]) == 40
                if 'actions/cache' in step['uses']:
                    paths.add(step['with']['path'])
                    assert 'restore-keys' not in step['with']
            else:
                assert '${{' not in step['run'] and step.get('shell', value['defaults']['run']['shell']) == 'bash'
    assert len(paths) == 1
    assert 'pytest' not in raw and 'requirements-dev' not in raw and 'upload-artifact' not in raw


def test_actual_cli_select_and_admit_across_processes_use_independent_jobs(fixture):
    root, seed, _ = fixture
    seed()
    for phase in ('select', 'admit'):
        proc = subprocess.run([sys.executable, '-B', '-m', 'crypto_boom.live', phase, '--root', str(root)],
                              env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1',
                                   'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')},
                              capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, proc.stderr
    assert read(root, 'selection.json')['proof']['at'] == T


@pytest.mark.parametrize('approve', [False, True])
def test_real_spawned_scan_commits_and_releases_parent_lock(fixture, monkeypatch, approve):
    root, seed, _ = fixture
    proof = seed()
    if approve:
        monkeypatch.setenv('MODE', 'live')
        monkeypatch.setenv('ARM_MANIFEST', proof['manifest_sha256'])
    monkeypatch.setattr(live, 'supervise', runtime.supervise)
    monkeypatch.setattr(live, 'scan_job', offline_scan_job)
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    assert read(root / 'workspace', 'child.json')['pid'] != os.getpid()
    store = StateStore(root / 'workspace/checkpoint.json')
    assert not store.lock_path.exists()
    state, _ = store.load()
    assert state.at == T + MINUTE and state.notifications_enabled == approve
    assert read(root, 'export.json')['generation'] == state.generation


def test_actual_export_is_next_runner_predecessor_without_replaying_confirmed_intent(fixture, monkeypatch):
    root, _ = prepared(fixture, monkeypatch, enabled=True, signal=True)
    previous_consumer = consumer(root, monkeypatch)
    api = provider.Provider()
    live.consume(previous_consumer, now=NOW, transport=httpx.MockTransport(api.handle))
    proof = read(previous_consumer, 'export.json')
    next_root = root.parent / 'next-runner'
    next_root.mkdir()
    for name in ('usage.json',):
        shutil.copyfile(root / name, next_root / name)
    write(next_root, 'current.json', run(3, status='in_progress', conclusion=None))
    write(next_root, 'history.json', {'total_count': 3, 'workflow_runs': [read(next_root, 'current.json'), run(2), run(1)]})
    write(next_root, 'producer-jobs.json', jobs(proof, consume=True, number=2))
    write(next_root, 'caches.json', listing(record(103, 'final', 2)))
    shutil.copytree(previous_consumer / 'transfer', next_root / 'transfer')
    for name in ('target-ref.json', 'target-readme.json'):
        shutil.copyfile(root / name, next_root / name)
    monkeypatch.setenv('GITHUB_RUN_ID', '30')
    monkeypatch.setenv('ARM_MANIFEST', '')
    live.select(next_root)
    live.admit(next_root)
    live.prepare(next_root, now=NOW)
    # The repeated signal has the same durable identity: no second intent/consumer.
    final = read(next_root, 'export.json')
    assert final['key'] == cache.live_key(SCOPE, 30, SOURCE, 'final')
    saved, _ = StateStore(next_root / 'transfer/checkpoint.json').load()
    assert saved.notifications_enabled and {v['status'] for v in saved.ledger.values()} == {'confirmed'}
    assert saved.generation > proof['generation'] and api.patch_calls == 1


def test_prepare_exposes_observation_not_notification(fixture, monkeypatch, capsys):
    root, seed, signal = fixture
    proof = seed()
    signal['on'] = True
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    result = read(root, 'result.json')
    assert result['job'] == 'prepare' and result['run_id'] == 20 and result['source_sha'] == SOURCE
    assert result['publication'] == 'not_sent' and result['observed_at'] == NOW
    assert result['cutoff_age_ms'] == 3000 and result['scan_elapsed_seconds'] >= 0
    assert result['market_coverage'] == {'SPOT': {'complete': 1}, 'SWAP': {}}
    assert result['transition_counts'] == {'OBSERVED': 1}
    assert result['transition_age_ms'] == {'min': 3000, 'max': 3000}
    lines = capsys.readouterr().out.splitlines()
    events = [json.loads(line.removeprefix('OKX_TRANSITION_V1 ')) for line in lines]
    assert events == json.loads(json.dumps([{'run_id': 20, 'source_sha': SOURCE,
                                            **asdict(provider.change(at=T + MINUTE))}]))
    state, reason = StateStore(root / 'transfer/checkpoint.json').load()
    assert state is not None, reason
    assert not state.notifications_enabled and not state.ledger
    assert read(root, 'export.json')['manifest_sha256'] != proof['manifest_sha256']


def test_summary_stdout_and_ui_match_and_do_not_require_result(fixture, monkeypatch, capsys):
    root, _, _ = fixture
    summary = root / 'summary.md'
    monkeypatch.setenv('GITHUB_STEP_SUMMARY', str(summary))
    monkeypatch.setattr(sys, 'argv', ['live', 'summary', '--root', str(root)])
    for result in ({'status': 'blocked_or_failed_before_result'}, {'mode': 'preview', 'publication': 'not_sent'}):
        if 'mode' in result:
            write(root, 'result.json', result)
        live.main()
        line = capsys.readouterr().out.strip()
        assert line.startswith('OKX_RUN_V1 ')
        assert json.loads(line.removeprefix('OKX_RUN_V1 ')) == result
        assert json.dumps(result, sort_keys=True) in summary.read_text(encoding='utf-8')


@pytest.mark.parametrize('blocked', [False, True])
def test_quiet_observation_has_no_events_even_when_diagnostic(fixture, monkeypatch, capsys, blocked):
    root, seed, _ = fixture
    seed()
    if blocked:
        write(root, 'caches.json', listing())
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    result = read(root, 'result.json')
    assert result['transition_counts'] == {} and result['transition_age_ms'] == {'min': None, 'max': None}
    assert result['publication'] == 'not_sent'
    assert bool(result['blocked']) == blocked
    assert ('final_key' not in result)  # Logging is not native persistence evidence.
    assert not capsys.readouterr().out


def test_observation_keeps_stage_identity_and_old_rows_not_mail_count(fixture, monkeypatch, capsys):
    root, seed, signal = fixture
    seed()
    signal['on'] = True
    original = scanner.scan

    def replay(*args, **kwargs):
        report = original(*args, **kwargs)
        first = report['transitions'][0]
        first['identity'] = (*first['identity'][:3], T)
        first['at'] = T
        old = T - (24 * 60 - 1) * MINUTE
        report['transitions'] += [
            {**first, 'stage': 'PERSISTENT', 'at': T + MINUTE},
            {**first, 'identity': (*first['identity'][:3], old), 'stage': 'ENDED', 'at': old},
        ]
        return report

    monkeypatch.setattr(scanner, 'scan', replay)
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    result = read(root, 'result.json')
    assert result['transition_counts'] == {'OBSERVED': 1, 'PERSISTENT': 1, 'ENDED': 1}
    assert result['transition_age_ms'] == {'min': 3000, 'max': 24 * 60 * MINUTE + 3000}
    events = [json.loads(line.removeprefix('OKX_TRANSITION_V1 ')) for line in capsys.readouterr().out.splitlines()]
    assert [r['stage'] for r in events] == ['OBSERVED', 'PERSISTENT', 'ENDED']
    assert events[0]['identity'] == events[1]['identity'] != events[2]['identity']
    assert result['publication'] == 'not_sent'  # Includes END/expired rows, not eligible emails.


def test_failed_scan_has_no_committed_observation(fixture, monkeypatch, capsys):
    root, seed, _ = fixture
    seed()
    live.select(root)
    live.admit(root)
    monkeypatch.setattr(live, 'supervise', lambda *args, **kwargs: 1)
    with pytest.raises(Unavailable, match='live_scan_child_failed'):
        live.prepare(root, now=NOW)
    assert not (root / 'result.json').exists() and not (root / 'export.json').exists()
    assert not capsys.readouterr().out


def test_observation_time_is_taken_after_scan(fixture, monkeypatch):
    root, seed, _ = fixture
    seed()
    live.select(root)
    live.admit(root)
    monkeypatch.setattr(live.time, 'time', lambda: (NOW + 2 * MINUTE) / 1000)
    live.prepare(root, now=NOW)
    result = read(root, 'result.json')
    assert result['observed_at'] == NOW + 2 * MINUTE
    assert result['cutoff_age_ms'] == 2 * MINUTE + 3000


def window_env(monkeypatch, start=NOW - MINUTE, end=NOW + 23 * 60 * MINUTE):
    for key, value in {'START_MS': start, 'END_MS': end, 'DIGEST': live.observation_digest()}.items():
        monkeypatch.setenv('OBSERVATION_' + key, str(value))


@pytest.mark.parametrize('offset,status,stop', [(-1, 'before_window', ''), (0, 'open', ''),
                                              (23 * 60 * MINUTE - 15 * MINUTE, 'draining', ''),
                                              (23 * 60 * MINUTE, 'expired', 456)])
def test_window_gate_boundaries_authenticate_stop_target(fixture, monkeypatch, offset, status, stop):
    root, _, _ = fixture
    window_env(monkeypatch, start=NOW, end=NOW + 23 * 60 * MINUTE)
    live.window_gate(root, now=NOW + offset)
    outputs = (root.parent / 'outputs').read_text()
    assert f'window_open={str(status == "open").lower()}\n' in outputs
    assert f'stop_workflow_id={stop}\n' in outputs
    if status != 'open':
        assert read(root, 'result.json')['status'] == status
    assert not (root / 'workspace').exists() and not (root / 'export.json').exists()


@pytest.mark.parametrize('key,value', [('END_MS', str(NOW - 2 * MINUTE)), ('END_MS', str(NOW + 25 * 60 * MINUTE)),
                                      ('START_MS', ''), ('DIGEST', 'x' * 64)])
def test_invalid_window_has_no_admitted_scan(fixture, monkeypatch, key, value):
    root, _, _ = fixture
    window_env(monkeypatch)
    monkeypatch.setenv('OBSERVATION_' + key, value)
    with pytest.raises(ValueError, match='observation_window_inputs'):
        live.select(root)
    assert not (root / 'workspace').exists()


def test_window_rejects_live_and_code_drift_and_schedule_without_window(fixture, monkeypatch):
    root, _, _ = fixture
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule')
    with pytest.raises(Unavailable, match='schedule_requires_window'):
        live.check_window(now=NOW)
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'workflow_dispatch')
    window_env(monkeypatch)
    monkeypatch.setenv('MODE', 'live')
    with pytest.raises(ValueError, match='window_inputs'):
        live.check_window(now=NOW)
    monkeypatch.setenv('MODE', 'preview')
    monkeypatch.setenv('OBSERVATION_DIGEST', 'b' * 64)
    with pytest.raises(Unavailable, match='code_or_workflow_changed'):
        live.select(root)
    assert not (root / 'selection.json').exists()


def test_expiry_blocks_scan_and_save_but_not_exact_cleanup(fixture, monkeypatch):
    root, seed, _ = fixture
    seed()
    live.select(root)
    live.admit(root)
    live.prepare(root, now=NOW)
    proof = read(root, 'export.json')
    write(root, 'caches.json', listing(record(123, 'final'), record(124, 'final', 1)))
    window_env(monkeypatch, start=NOW - 22 * 60 * MINUTE, end=NOW)
    with pytest.raises(Unavailable, match='closed_or_insufficient_time'):
        live.prepare(root, now=NOW)
    with pytest.raises(Unavailable, match='closed_or_insufficient_time'):
        live.guard_save(root)
    live.plan_cleanup(root, now=NOW)
    live.check_cleanup(root, 124)
    assert read(root, 'export.json') == proof


def test_window_scan_failure_stops_only_authenticated_candidate(fixture, monkeypatch):
    root, _, _ = fixture
    window_env(monkeypatch)
    monkeypatch.setenv('GITHUB_STEP_SUMMARY', str(root / 'summary.md'))
    monkeypatch.setattr(sys, 'argv', ['live', 'summary', '--root', str(root)])
    live.main()
    assert 'stop_workflow_id=456' in (root.parent / 'outputs').read_text()
    body = read(root, 'current.json')
    body['workflow_id'] = 999
    body['path'] = '.github/workflows/other.yml'
    write(root, 'current.json', body)
    (root.parent / 'outputs').write_text('')
    with pytest.raises(ValueError, match='untrusted_run'):
        live.main()
    assert not (root.parent / 'outputs').read_text()


def test_window_predecessor_failure_never_starts_diagnostic_scan(fixture, monkeypatch):
    root, _, _ = fixture
    window_env(monkeypatch)
    with pytest.raises(Unavailable, match='exact_predecessor_cache_missing'):
        live.select(root)
    assert not (root / 'workspace').exists()


def test_window_expiring_during_scan_does_not_export(fixture, monkeypatch):
    root, seed, _ = fixture
    seed()
    window_env(monkeypatch, end=NOW + 16 * MINUTE)
    live.select(root)
    live.admit(root)
    monkeypatch.setattr(live.time, 'time', lambda: (NOW + 15 * MINUTE) / 1000)
    with pytest.raises(Unavailable, match='closed_or_insufficient_time'):
        live.prepare(root, now=NOW)
    assert not (root / 'export.json').exists() and not (root / 'result.json').exists()


def test_optional_finite_preview_keeps_stop_permission_scope_unchanged():
    path = Path(__file__).parents[1] / live.WORKFLOW
    text = path.read_text(encoding='utf-8')
    workflow = yaml.safe_load(text)
    assert set(workflow['on']) == {'workflow_dispatch', 'schedule'}
    prepare = workflow['jobs']['prepare']
    assert prepare['permissions'] == {'contents': 'read', 'actions': 'write'}
    steps = prepare['steps']
    stop = next(s for s in steps if s.get('name') == 'Stop only this finite observation workflow')
    assert 'steps.disposition.outputs.stop_workflow_id' in stop['if'] and stop['if'].startswith('always()')
    assert '"$path" == .github/workflows/okx-live.yml' in stop['run']
    assert 'disabled_manually' in stop['run'] and '/disable' in stop['run']
    assert next(s for s in steps if s.get('name') == 'Save explicit intent or quiet final')['timeout-minutes'] == 2


def test_startup_gate_does_not_require_not_yet_fetched_live_fences(fixture, monkeypatch):
    root, _, _ = fixture
    monkeypatch.setenv('MODE', 'live')
    live.window_gate(root, now=NOW)
    assert 'window_open=true' in (root.parent / 'outputs').read_text()
    # Startup admission precedes the production operator gate.
    monkeypatch.setenv('LIVE_ENABLED', 'false')
    with pytest.raises(Unavailable, match='live_operator_gate_required'):
        live.select(root)


@pytest.mark.parametrize('armed', [True, False])
def test_scheduled_live_requires_armed_state_without_window_or_rebuild(fixture, monkeypatch, armed):
    root, seed, _ = fixture
    seed(enabled=armed)
    current = read(root, 'current.json')
    current['event'] = 'schedule'
    write(root, 'current.json', current)
    write(root, 'history.json', {'total_count': 2, 'workflow_runs': [current, run(1)]})
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule')
    monkeypatch.setenv('MODE', 'live')
    live.window_gate(root, now=NOW)
    live.select(root)
    live.admit(root)
    if not armed:
        with pytest.raises(Unavailable, match='reviewed_manifest_approval_required'):
            live.prepare(root, now=NOW)
        assert not (root / 'export.json').exists()
        return
    live.prepare(root, now=NOW)
    state, _ = StateStore(root / 'transfer/checkpoint.json').load()
    assert state.notifications_enabled and read(root, 'result.json')['publication'] == 'not_sent'


def test_window_fingerprint_freezes_workflow_not_readme_and_normalizes_lf(tmp_path, monkeypatch):
    workflow = tmp_path / live.WORKFLOW
    workflow.parent.mkdir(parents=True)
    workflow.write_bytes(b'name: finite\n')
    monkeypatch.setattr(live, '__file__', str(tmp_path / 'src/crypto_boom/live.py'))
    monkeypatch.setattr(cache, 'code_digest', lambda: 'b' * 64)
    fingerprint = live.observation_digest()
    workflow.write_bytes(b'name: finite\r\n')
    (tmp_path / 'README.md').write_text('ordinary market commit')
    assert live.observation_digest() == fingerprint
    workflow.write_bytes(b'name: changed\n')
    assert live.observation_digest() != fingerprint
