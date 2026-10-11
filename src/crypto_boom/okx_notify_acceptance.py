"""Fixed manual TEST acceptance, not a production scanner or recovery service.

GitHub's workflow owns authenticated metadata and native cache save/restore.
This command admits their bounded inputs, composes existing owners, and refuses
unknown/old/rerun intent. It never creates a branch, deletes cache or auto-resumes
a live notification baseline. The isolated target must already exist.
"""
import argparse
import base64
import json
import os
import re
import time
from dataclasses import replace
from pathlib import Path

from . import okx_cache as cache
from .okx_publish import GitHubPublisher, _git_blob, prepare_batch, publish_pending
from .okx_state import Checkpoint, StateStore, atomic_write
from .okx_storage import MIB, Scope, digest
from .okx_trial import MINUTE, VERSION, Transition, Unavailable

PREFIX = 'okx-stage4-notify-v1-'
TARGET = 'refs/heads/codex/okx-notification-test'
TEST_NOTICE = b'> TEST ONLY: synthetic notification acceptance; no real market signal.\n\n'


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('notification_duplicate_key')
        value[key] = item
    return value


def _json_file(path, limit=128 * 1024):
    if path.is_symlink() or not path.is_file():
        raise ValueError('notification_regular_file_required')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('notification_input_limit')
    return json.loads(raw, object_pairs_hook=_object)


def _hex(value, length=64):
    return isinstance(value, str) and re.fullmatch(f'[0-9a-f]{{{length}}}', value) is not None


def key(run_id, source_sha, phase):
    if type(run_id) is not int or run_id < 1 or not _hex(source_sha, 40) or phase not in ('intent', 'final'):
        raise ValueError('notification_key_identity')
    return f'{PREFIX}r{run_id}-a1-{source_sha}-{phase}'


def _context(root):
    expected = {'GITHUB_REPOSITORY': cache.REPO, 'GITHUB_REF': 'refs/heads/main',
                'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_RUN_ATTEMPT': '1'}
    if (root.is_symlink() or not root.is_dir() or any(os.environ.get(k) != v for k, v in expected.items())
            or re.fullmatch(r'[1-9][0-9]{0,18}', os.environ.get('GITHUB_RUN_ID', '')) is None
            or not _hex(os.environ.get('GITHUB_SHA'), 40)
            or os.environ.get('SCENARIO') not in ('quiet', 'alert')
            or os.environ.get('PUBLISH_TEST') not in ('true', 'false')):
        raise ValueError('notification_runtime_scope')
    run_id, source = int(os.environ['GITHUB_RUN_ID']), os.environ['GITHUB_SHA']
    scope = cache.admission('bootstrap', _json_file(root / 'current.json'), None, _json_file(root / 'usage.json'),
                            run_id=run_id, source_sha=source, workflow_path=cache.NOTIFICATION_WORKFLOW)
    return scope, source, run_id


def inventory(body, *, max_existing):
    """Admit the complete provider prefix query; never delete or invent headroom."""
    if (not isinstance(body, dict) or type(body.get('total_count')) is not int
            or not 0 <= body['total_count'] <= max_existing or not isinstance(body.get('actions_caches'), list)
            or len(body['actions_caches']) != body['total_count']):
        raise ValueError('notification_inventory_not_complete_or_full')
    records, ids = {}, set()
    for record in body['actions_caches']:
        if (not isinstance(record, dict) or type(record.get('id')) is not int or record['id'] < 1
                or record['id'] in ids or not isinstance(record.get('key'), str)
                or re.fullmatch(re.escape(PREFIX) + r'r[1-9][0-9]{0,18}-a1-[0-9a-f]{40}-(intent|final)', record['key']) is None
                or record['key'] in records or record.get('ref') != 'refs/heads/main'
                or not _hex(record.get('version')) or type(record.get('size_in_bytes')) is not int
                or not 0 < record['size_in_bytes'] <= 128 * MIB):
            raise ValueError('notification_cache_metadata')
        records[record['key']] = record
        ids.add(record['id'])
    return records


def _outputs(values):
    with Path(os.environ['GITHUB_OUTPUT']).open('a', encoding='utf-8') as stream:
        for name, value in values.items():
            if '\n' in str(value) or '\r' in str(value):
                raise ValueError('notification_output_newline')
            stream.write(f'{name}={value}\n')
    print(json.dumps(values, sort_keys=True))  # Short admitted identifiers only.


def _target(root, expected_head=None):
    body = _json_file(root / 'target-ref.json')
    obj = body.get('object') if isinstance(body, dict) else None
    if (not isinstance(obj, dict) or body.get('ref') != TARGET or obj.get('type') != 'commit'
            or not _hex(obj.get('sha'), 40) or (expected_head is not None and obj['sha'] != expected_head)):
        raise ValueError('notification_target_head')
    content = _json_file(root / 'target-readme.json', 2 * MIB)
    if (not isinstance(content, dict) or content.get('type') != 'file' or content.get('path') != 'README.md'
            or content.get('encoding') != 'base64' or not isinstance(content.get('content'), str)
            or not _hex(content.get('sha'), 40)):
        raise ValueError('notification_target_readme')
    raw = base64.b64decode(content['content'].replace('\n', ''), validate=True)
    if len(raw) > MIB or _git_blob(raw) != content['sha']:
        raise ValueError('notification_target_readme_limit')
    raw.decode('utf-8')
    return obj['sha'], raw


def _export(root, workspace, scope, source, run_id, phase, now):
    result = cache.seal(workspace, root / 'transfer', scope=scope, source_sha=source, run_id=run_id, created_at=now)
    # The native workflow owns distinct immutable phase keys, not the shadow CLI.
    result['key'] = key(run_id, source, phase)
    result['checkpoint_sha256'] = digest((root / 'transfer/checkpoint.json').read_bytes())
    atomic_write(root / f'{phase}-proof.json', json.dumps(result, sort_keys=True).encode())
    return result


def prepare(root, *, now):
    scope, source, run_id = _context(root)
    if os.environ['SCENARIO'] == 'quiet':
        _outputs({'has_intent': 'false'})
        return
    inventory(_json_file(root / 'caches.json'), max_existing=1)  # Reserve both intent and final slots.
    head, original = _target(root)
    readme = original if original.startswith(TEST_NOTICE) else TEST_NOTICE + original
    workspace = root / 'workspace'
    if workspace.exists() or workspace.is_symlink():
        raise ValueError('notification_fresh_test_workspace')
    store = StateStore(workspace / 'checkpoint.json')
    at = now // MINUTE * MINUTE
    with store.writer():
        state = Checkpoint(at=at)
        store.save(state, None)
        # A fixed, explicit synthetic TEST baseline. Never restore or enable live state.
        state.notifications_enabled = True
        store.save(state, state.generation)
        changes = [Transition((VERSION + '/A0', ('okx', 'SPOT', 'TEST-NOTIFICATION-USDT'), 'UP', at), 'OBSERVED', at)]
        prepared = prepare_batch(store, changes, scope=Scope(scope.repository, scope.workflow_id, TARGET),
                                 source_sha=source, run_id=run_id, attempt=1, readme=readme, observed_at=now)
        if prepared is None:
            raise Unavailable('notification_test_intent_required')
    result = _export(root, workspace, scope, source, run_id, 'intent', now)
    _outputs({'has_intent': 'true', 'head': head, 'key': result['key'],
              'manifest': result['manifest_sha256'], 'checkpoint': result['checkpoint_sha256']})


def consume(root, *, now, transport=None):
    scope, source, run_id = _context(root)
    workspace = root / 'workspace'
    if workspace.exists() or workspace.is_symlink():
        raise ValueError('notification_fresh_consumer_workspace')
    manifest, checkpoint, head = (os.environ.get(k, '') for k in ('INTENT_MANIFEST', 'INTENT_CHECKPOINT', 'TARGET_HEAD'))
    if (os.environ.get('CACHE_HIT') != 'true' or os.environ['SCENARIO'] != 'alert'
            or not _hex(manifest) or not _hex(checkpoint) or not _hex(head, 40)):
        raise ValueError('notification_exact_readback_required')
    records = inventory(_json_file(root / 'caches.json'), max_existing=2)
    cache_record = records.get(key(run_id, source, 'intent'))
    if cache_record is None:
        raise Unavailable('notification_intent_save_not_observed')
    bundle = root / 'transfer'
    # Full carrier validation stays with the cache owner; its diagnostic copy
    # remains non-notifying. Only the validated same-run original pending is used.
    cache.restore(bundle, root / 'verified-shadow', scope=scope, source_sha=source, run_id=run_id,
                  manifest_sha256=manifest, now=now)
    store = StateStore(bundle / 'checkpoint.json')
    raw = store.path.read_bytes()
    if digest(raw) != checkpoint:
        raise ValueError('notification_checkpoint_identity')
    state, _ = store.load()
    if (state is None or not state.notifications_enabled or not state.ledger
            or any(v['status'] != 'pending' for v in state.ledger.values())):
        raise Unavailable('notification_same_run_pending_required')
    expected_head, original = _target(root, head)
    readme = original if original.startswith(TEST_NOTICE) else TEST_NOTICE + original
    result = {'intent_cache_id': cache_record['id'], 'publication': 'preview_no_send', 'commit': None}
    with store.writer():
        if os.environ['PUBLISH_TEST'] == 'true':
            publisher = GitHubPublisher(Scope(scope.repository, scope.workflow_id, TARGET), cache.REPO_ID,
                                        expected_head, original,
                                        token=os.environ.get('GH_TOKEN', ''), transport=transport)
            result['commit'] = publish_pending(store, verified_checkpoint_sha256=checkpoint,
                                               scope=Scope(scope.repository, scope.workflow_id, TARGET),
                                               source_sha=source, run_id=run_id, attempt=1, readme=readme,
                                               observed_at=now, publish=lambda batch: publisher(
                                                   replace(batch, message=batch.message + ' TEST ONLY')))
            result['publication'] = 'test_commit_observed_email_unverified'
            state, _ = store.load()
            changes = [Transition(tuple(v['identity'][0]), v['identity'][1], v['transition_at']) for v in state.ledger.values()]
            before = store.path.read_bytes()
            repeated = prepare_batch(store, changes, scope=Scope(scope.repository, scope.workflow_id, TARGET),
                                      source_sha=source, run_id=run_id, attempt=1, readme=readme, observed_at=now)
            if repeated is not None or store.path.read_bytes() != before:
                raise Unavailable('notification_duplicate_test_failed')
            result['repeat'] = 'no_new_intent'
        else:
            state.notifications_enabled = False
            store.save(state, state.generation)
    bundle.rename(workspace)  # Preserve input; reuse the identical native cache path for final save.
    exported = _export(root, workspace, scope, source, run_id, 'final', now)
    result.update(final_key=exported['key'], final_manifest=exported['manifest_sha256'])
    atomic_write(root / 'result.json', json.dumps(result, sort_keys=True).encode())
    _outputs({'final_key': exported['key']})


def final(root):
    _, source, run_id = _context(root)
    records = inventory(_json_file(root / 'caches.json'), max_existing=3)
    record = records.get(key(run_id, source, 'final'))
    if record is None:
        raise Unavailable('notification_final_save_not_observed')
    result = _json_file(root / 'result.json')
    proof = _json_file(root / 'final-proof.json')
    if (not isinstance(result, dict) or result.get('final_key') != record['key']
            or result.get('publication') not in ('preview_no_send', 'test_commit_observed_email_unverified')
            or not isinstance(proof, dict) or proof.get('key') != record['key']
            or result.get('final_manifest') != proof.get('manifest_sha256')):
        raise ValueError('notification_final_identity')
    result['final_cache_id'] = record['id']
    result['final_cache_bytes'] = record['size_in_bytes']
    atomic_write(root / 'result.json', json.dumps(result, sort_keys=True).encode())
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('prepare', 'consume', 'guard-final', 'final'))
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    now = int(time.time() * 1000)
    if args.phase == 'prepare':
        prepare(args.root, now=now)
    elif args.phase == 'consume':
        consume(args.root, now=now)
    elif args.phase == 'guard-final':
        _context(args.root)
        inventory(_json_file(args.root / 'caches.json'), max_existing=2)
    else:
        final(args.root)


if __name__ == '__main__':
    main()
