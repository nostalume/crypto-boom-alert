"""Two disposable runners, fake native cache/provider; no real service effects."""
import base64
import json
import re
import shutil
from pathlib import Path

import httpx
import pytest
import test_okx_publish as provider
import yaml

from crypto_boom import okx_cache as cache
from crypto_boom import okx_notify_acceptance as notify
from crypto_boom.okx_state import StateStore
from crypto_boom.okx_storage import DAY, GIB, MIB, Scope
from crypto_boom.okx_trial import Unavailable

T, SOURCE, HEAD = provider.T, provider.SOURCE, provider.HEAD


def write(root, name, body):
    (root / name).write_text(json.dumps(body), encoding='utf-8')


def read(root, name):
    return json.loads((root / name).read_bytes())


def record(phase='intent', identity=101):
    return {'id': identity, 'key': notify.key(7, SOURCE, phase), 'ref': 'refs/heads/main',
            'version': 'f' * 64, 'size_in_bytes': 4096}


def listing(*rows):
    return {'total_count': len(rows), 'actions_caches': list(rows)}


@pytest.fixture
def runners(tmp_path, monkeypatch):
    for name, value in {'GITHUB_REPOSITORY': cache.REPO, 'GITHUB_REF': 'refs/heads/main',
                        'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_RUN_ATTEMPT': '1',
                        'GITHUB_RUN_ID': '7', 'GITHUB_SHA': SOURCE, 'SCENARIO': 'alert',
                        'PUBLISH_TEST': 'true', 'GH_TOKEN': 'test-only-secret', 'CACHE_HIT': 'true'}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(provider, 'SCOPE', Scope(cache.REPO, 456, notify.TARGET))
    roots = [tmp_path / name for name in ('producer', 'fresh-consumer')]
    for root in roots:
        root.mkdir()
        write(root, 'current.json', {'id': 7, 'workflow_id': 456, 'path': cache.NOTIFICATION_WORKFLOW,
                                    'event': 'workflow_dispatch', 'run_attempt': 1, 'head_branch': 'main',
                                    'head_sha': SOURCE, 'repository': {'id': cache.REPO_ID},
                                    'head_repository': {'id': cache.REPO_ID}, 'pull_requests': []})
        write(root, 'usage.json', {'full_name': cache.REPO, 'active_caches_size_in_bytes': 0,
                                  'active_caches_count': 0})
        write(root, 'caches.json', listing())
        write(root, 'target-ref.json', {'ref': notify.TARGET, 'object': {'type': 'commit', 'sha': HEAD}})
        write(root, 'target-readme.json', {'type': 'file', 'path': 'README.md', 'encoding': 'base64',
                                         'content': base64.b64encode(provider.README).decode(),
                                         'sha': provider.publication._git_blob(provider.README)})
    monkeypatch.setenv('GITHUB_OUTPUT', str(tmp_path / 'outputs.txt'))
    return roots


def transfer(runners, monkeypatch):
    first, fresh = runners
    notify.prepare(first, now=T)
    # Only native transfer bytes cross runners. Authority comes from needs outputs,
    # not from any proof/current/usage file inside that restored directory.
    proof = read(first, 'intent-proof.json')
    shutil.copytree(first / 'transfer', fresh / 'transfer')
    for name, value in {'INTENT_MANIFEST': proof['manifest_sha256'],
                        'INTENT_CHECKPOINT': proof['checkpoint_sha256'], 'TARGET_HEAD': HEAD}.items():
        monkeypatch.setenv(name, value)
    write(fresh, 'caches.json', listing(record()))
    assert not (fresh / 'workspace').exists() and not (fresh / 'intent-proof.json').exists()
    return fresh


def test_full_native_handoff_single_test_commit_confirmed_final_and_duplicate(runners, monkeypatch):
    fresh = transfer(runners, monkeypatch)
    api = provider.Provider()
    notify.consume(fresh, now=T + 60000, transport=httpx.MockTransport(api.handle))
    assert api.patch_calls == 1 and api.message.endswith('TEST ONLY')
    assert 'OBSERVED' in api.message and 'CONFIRMED' not in api.message
    state, _ = StateStore(fresh / 'transfer/checkpoint.json').load()
    assert state.generation == 5 and {v['status'] for v in state.ledger.values()} == {'confirmed'}
    shadow, _ = StateStore(fresh / 'verified-shadow/checkpoint.json').load()
    assert not shadow.notifications_enabled
    assert read(fresh, 'result.json')['repeat'] == 'no_new_intent'
    with pytest.raises(Unavailable, match='final_save_not_observed'):
        notify.final(fresh)  # A successful cache action exit alone is not proof.
    write(fresh, 'caches.json', listing(record(), record('final', 102)))
    notify.final(fresh)
    assert read(fresh, 'result.json')['final_cache_id'] == 102


def test_existing_test_notice_keeps_original_git_blob(runners, monkeypatch):
    original = notify.TEST_NOTICE + provider.README
    monkeypatch.setattr(provider, 'README', original)
    for root in runners:
        write(root, 'target-readme.json', {'type': 'file', 'path': 'README.md', 'encoding': 'base64',
                                         'content': base64.b64encode(original).decode(),
                                         'sha': provider.publication._git_blob(original)})
    fresh = transfer(runners, monkeypatch)
    api = provider.Provider()
    notify.consume(fresh, now=T, transport=httpx.MockTransport(api.handle))
    assert api.patch_calls == 1


def test_preview_preserves_pending_disabled_without_publisher(runners, monkeypatch):
    monkeypatch.setenv('PUBLISH_TEST', 'false')
    monkeypatch.delenv('GH_TOKEN')
    fresh = transfer(runners, monkeypatch)
    def forbidden(request):
        pytest.fail('preview network effect')
    notify.consume(fresh, now=T, transport=httpx.MockTransport(forbidden))
    state, _ = StateStore(fresh / 'transfer/checkpoint.json').load()
    assert not state.notifications_enabled and {v['status'] for v in state.ledger.values()} == {'pending'}
    assert read(fresh, 'result.json')['publication'] == 'preview_no_send'


def test_quiet_creates_no_intent_carrier_or_target_dependency(runners, monkeypatch):
    monkeypatch.setenv('SCENARIO', 'quiet')
    root = runners[0]
    (root / 'target-ref.json').unlink()
    notify.prepare(root, now=T)
    assert not (root / 'workspace').exists() and not (root / 'transfer').exists()


@pytest.mark.parametrize('name,value', [
    ('GITHUB_REPOSITORY', 'foreign/repo'), ('GITHUB_REF', notify.TARGET), ('GITHUB_EVENT_NAME', 'push'),
    ('GITHUB_RUN_ATTEMPT', '2'), ('GITHUB_RUN_ID', '07'), ('GITHUB_SHA', 'bad'),
    ('SCENARIO', 'live'), ('PUBLISH_TEST', 'yes'),
])
def test_runtime_scope_rejected_before_state(runners, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match='runtime_scope'):
        notify.prepare(runners[0], now=T)
    assert not (runners[0] / 'workspace').exists()


@pytest.mark.parametrize('name,value', [
    ('CACHE_HIT', 'false'), ('CACHE_HIT', ''), ('INTENT_MANIFEST', '0' * 64),
    ('INTENT_CHECKPOINT', '0' * 64), ('TARGET_HEAD', 'f' * 40),
    ('GITHUB_SHA', 'f' * 40), ('GITHUB_RUN_ID', '8'),
])
def test_independent_readback_identity_fail_closed(runners, monkeypatch, name, value):
    fresh = transfer(runners, monkeypatch)
    before = (fresh / 'transfer/checkpoint.json').read_bytes()
    monkeypatch.setenv(name, value)
    api = provider.Provider()
    with pytest.raises((ValueError, Unavailable)):
        notify.consume(fresh, now=T, transport=httpx.MockTransport(api.handle))
    assert not api.requests and (fresh / 'transfer/checkpoint.json').read_bytes() == before
    assert not (fresh / 'result.json').exists()


@pytest.mark.parametrize('fault', ['missing_save', 'tampered', 'expired', 'missing_file', 'extra_file',
                                  'target_advanced', 'readme_blob', 'usage_unknown', 'usage_full',
                                  'workflow_path', 'fork', 'attempt'])
def test_input_faults_never_publish(runners, monkeypatch, fault):
    fresh = transfer(runners, monkeypatch)
    now = T
    if fault == 'missing_save':
        write(fresh, 'caches.json', listing())
    elif fault == 'tampered':
        with (fresh / 'transfer/checkpoint.json').open('ab') as stream:
            stream.write(b' ')
    elif fault == 'expired':
        now += DAY
    elif fault == 'missing_file':
        (fresh / 'transfer/checkpoint.json').unlink()
    elif fault == 'extra_file':
        (fresh / 'transfer/unowned').write_text('unowned')
    elif fault == 'target_advanced':
        body = read(fresh, 'target-ref.json'); body['object']['sha'] = 'f' * 40
        write(fresh, 'target-ref.json', body)
    elif fault == 'readme_blob':
        body = read(fresh, 'target-readme.json'); body['sha'] = 'f' * 40
        write(fresh, 'target-readme.json', body)
    elif fault.startswith('usage'):
        body = read(fresh, 'usage.json')
        body['active_caches_size_in_bytes'] = None if fault == 'usage_unknown' else 8 * GIB + 1
        write(fresh, 'usage.json', body)
    else:
        body = read(fresh, 'current.json')
        if fault == 'workflow_path':
            body['path'] = cache.WORKFLOW
        elif fault == 'fork':
            body['head_repository']['id'] += 1
        else:
            body['run_attempt'] = 2
        write(fresh, 'current.json', body)
    api = provider.Provider()
    with pytest.raises((ValueError, OSError, Unavailable)):
        notify.consume(fresh, now=now, transport=httpx.MockTransport(api.handle))
    assert not api.requests and not (fresh / 'result.json').exists()


@pytest.mark.parametrize('fault', ['duplicate', 'partial', 'full', 'foreign', 'wrong_ref',
                                  'oversize', 'zero', 'unknown_size', 'bad_version', 'boolean_id'])
def test_native_inventory_strict_and_bounded(fault):
    row = record()
    body = listing(row)
    if fault == 'duplicate':
        body = listing(row, row)
    elif fault == 'partial':
        body['total_count'] = 2
    elif fault == 'full':
        body = listing(row, record('final', 102), {**row, 'id': 103, 'key': notify.key(8, SOURCE, 'intent')})
    else:
        field, value = {'foreign': ('key', cache.cache_key(7, SOURCE)), 'wrong_ref': ('ref', notify.TARGET),
                        'oversize': ('size_in_bytes', 128 * MIB + 1), 'zero': ('size_in_bytes', 0),
                        'unknown_size': ('size_in_bytes', None), 'bad_version': ('version', 'bad'),
                        'boolean_id': ('id', True)}[fault]
        row[field] = value
    with pytest.raises(ValueError, match='notification_'):
        notify.inventory(body, max_existing=2)


def test_accepted_timeout_retains_unknown_and_cannot_retry(runners, monkeypatch):
    fresh = transfer(runners, monkeypatch)
    api = provider.Provider('accepted_timeout')
    transport = httpx.MockTransport(api.handle)
    with pytest.raises(Unavailable):
        notify.consume(fresh, now=T, transport=transport)
    assert api.patch_calls == 1 and not (fresh / 'result.json').exists()
    state, _ = StateStore(fresh / 'transfer/checkpoint.json').load()
    assert {v['status'] for v in state.ledger.values()} == {'unknown'}
    # Neither rerun nor same-run retry can turn the retained old pending cache
    # into new send authority. Remote intent remains unchanged in producer.
    assert {v['status'] for v in StateStore(runners[0] / 'transfer/checkpoint.json').load()[0].ledger.values()} == {'pending'}
    with pytest.raises((ValueError, Unavailable)):
        notify.consume(fresh, now=T, transport=transport)
    assert api.patch_calls == 1


def test_final_result_cannot_adopt_foreign_key(runners, monkeypatch):
    monkeypatch.setenv('PUBLISH_TEST', 'false')
    fresh = transfer(runners, monkeypatch)
    notify.consume(fresh, now=T)
    write(fresh, 'caches.json', listing(record(), record('final', 102)))
    result = read(fresh, 'result.json'); result['final_key'] = notify.key(8, SOURCE, 'final')
    write(fresh, 'result.json', result)
    with pytest.raises(ValueError, match='final_identity'):
        notify.final(fresh)


def test_existing_consumer_workspace_blocks_before_publisher(runners, monkeypatch):
    fresh = transfer(runners, monkeypatch)
    (fresh / 'workspace').mkdir()
    api = provider.Provider()
    with pytest.raises(ValueError, match='fresh_consumer_workspace'):
        notify.consume(fresh, now=T, transport=httpx.MockTransport(api.handle))
    assert not api.requests


@pytest.mark.parametrize('fault', ['disabled', 'unknown', 'no_pending', 'unlocked', 'wrong_digest'])
def test_staged_consumer_rejects_without_reentering_prepare(tmp_path, fault):
    store = provider.seeded(tmp_path)
    options = dict(scope=provider.SCOPE, source_sha=SOURCE, run_id=7, attempt=1,
                   readme=provider.README, observed_at=T)
    with store.writer():
        provider.publication.prepare_batch(store, [provider.change()], **options)
        state, _ = store.load()
        ids = tuple(state.ledger)
        if fault == 'disabled':
            state.notifications_enabled = False
        elif fault in ('unknown', 'no_pending'):
            state.unknown(ids)
            if fault == 'no_pending':
                state.reconcile(ids, state.ledger[ids[0]]['batch'])
        if fault not in ('unlocked', 'wrong_digest'):
            store.save(state, state.generation)
    expected = provider.digest(store.path.read_bytes()) if fault != 'wrong_digest' else '0' * 64
    before = store.path.read_bytes()
    def forbidden(batch):
        pytest.fail('invalid pending published')
    if fault == 'unlocked':
        with pytest.raises(Unavailable):
            provider.publication.publish_pending(store, verified_checkpoint_sha256=expected,
                                                 publish=forbidden, **options)
    else:
        with store.writer(), pytest.raises(Unavailable):
            provider.publication.publish_pending(store, verified_checkpoint_sha256=expected,
                                                 publish=forbidden, **options)
    assert store.path.read_bytes() == before


def test_unknown_workflow_variant_cannot_bypass_shared_admission(runners):
    root = runners[0]
    with pytest.raises(ValueError, match='free_admission_denied'):
        cache.admission('bootstrap', read(root, 'current.json'), None, read(root, 'usage.json'),
                        run_id=7, source_sha=SOURCE, workflow_path='foreign.yml')
    # Old default stays strict; a TEST run cannot be admitted as Stage 3.
    with pytest.raises(ValueError, match='untrusted_run'):
        cache.admission('bootstrap', read(root, 'current.json'), None, read(root, 'usage.json'),
                        run_id=7, source_sha=SOURCE)


def test_new_workflow_manual_two_runners_minimal_permissions_exact_native_paths():
    path = Path(__file__).parents[1] / cache.NOTIFICATION_WORKFLOW
    text = path.read_text()
    workflow = yaml.safe_load(text)
    assert list(workflow['on']) == ['workflow_dispatch'] and workflow['permissions'] == {}
    assert workflow['on']['workflow_dispatch']['inputs']['publish']['default'] is False
    assert workflow['on']['workflow_dispatch']['inputs']['scenario']['default'] == 'quiet'
    assert workflow['concurrency']['cancel-in-progress'] is False
    assert set(workflow['jobs']) == {'prepare', 'consume'}
    assert workflow['jobs']['consume']['needs'] == 'prepare'
    assert workflow['jobs']['prepare']['permissions'] == {'contents': 'read', 'actions': 'read'}
    assert workflow['jobs']['consume']['permissions'] == {'contents': 'write', 'actions': 'read'}
    paths, cache_actions = [], []
    for job in workflow['jobs'].values():
        assert job['runs-on'] == 'ubuntu-latest' and job['timeout-minutes'] == 15
        for step in job['steps']:
            if 'uses' in step:
                assert re.fullmatch(r'actions/(checkout|setup-python|cache/(save|restore))@[0-9a-f]{40}', step['uses'])
                if 'checkout@' in step['uses']:
                    assert step['with']['persist-credentials'] is False
                if 'cache/' in step['uses']:
                    paths.append(step['with']['path']); cache_actions.append(step)
                    assert 'restore-keys' not in step['with']
            if 'run' in step:
                assert '${{' not in step['run'] and step['shell'] == 'bash'
                assert '--method DELETE' not in step['run']
    assert len(paths) == 3 and len(set(paths)) == 1
    assert cache_actions[1]['with']['fail-on-cache-miss'] is True
    assert 'upload-artifact' not in text and 'schedule:' not in text and 'continue-on-error' not in text
    assert 'steps.readback.outputs.cache-hit' in text and 'needs.prepare.outputs.checkpoint' in text
    assert 'contents: write' not in text.split('  consume:')[0]
