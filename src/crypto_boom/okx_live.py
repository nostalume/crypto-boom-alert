"""Manual production candidate: authenticated inputs, native cache, one sender.

The workflow owns GitHub metadata, exact native transfers and DELETE. This command
owns their admission and orchestration, never enabling a cron or deleting caches.
Preview is default; live additionally requires an operator gate and stopped old
writer. A successful step name supplies independent next-run final proof.
"""
import argparse
import base64
import json
import os
import re
import time
from pathlib import Path

from . import okx_cache as cache
from .okx_publish import GitHubPublisher, _git_blob, prepare_batch, publish_pending
from .okx_runtime import supervise
from .okx_scan import scan_job
from .okx_state import StateStore, atomic_write
from .okx_storage import MIB, Scope, digest
from .okx_trial import MINUTE, Transition, Unavailable

WORKFLOW = '.github/workflows/okx-live.yml'
LEGACY = '.github/workflows/hourly-altcoin-alert.yml'
ACTIVE = ('in_progress', 'queued', 'requested', 'waiting', 'pending')
PROOF = re.compile(r'OKX_FINAL_V1 ([0-9a-f]{64}) ([0-9a-f]{64}) ([1-9][0-9]{0,18}) (0|[1-9][0-9]{0,18})')


def _file(root, name, limit=MIB):
    return json.loads(cache._read(root / name, limit), object_pairs_hook=cache._pairs)


def _write(root, name, body):
    atomic_write(root / name, cache._json(body))


def _outputs(values):
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as stream:
        for name, value in values.items():
            if '\n' in str(value) or '\r' in str(value):
                raise ValueError('live_output_newline')
            stream.write(f'{name}={value}\n')


def _context(root):
    if (root.is_symlink() or not root.is_dir() or os.environ.get('GITHUB_REPOSITORY') != cache.REPO
            or os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_RUN_ATTEMPT') != '1'
            or os.environ.get('GITHUB_EVENT_NAME') not in ('workflow_dispatch', 'schedule')
            or os.environ.get('MODE') not in ('preview', 'live') or os.environ.get('REBUILD') not in ('true', 'false')
            or re.fullmatch(r'[1-9][0-9]{0,18}', os.environ.get('GITHUB_RUN_ID', '')) is None
            or not cache._sha(os.environ.get('GITHUB_SHA'), 40)):
        raise ValueError('live_runtime_context')
    current = _file(root, 'current.json')
    scope = Scope(cache.REPO, current['workflow_id'], 'refs/heads/main')
    cache._live_run(current, scope, WORKFLOW)
    if (current['id'] != int(os.environ['GITHUB_RUN_ID']) or current['head_sha'] != os.environ['GITHUB_SHA']
            or current['event'] != os.environ['GITHUB_EVENT_NAME']
            or any(current[k].get('private') is not False for k in ('repository', 'head_repository'))):
        raise ValueError('live_current_identity')
    arm = os.environ.get('ARM_MANIFEST', '')
    if arm and (not cache._sha(arm) or os.environ['MODE'] != 'live' or current['event'] != 'workflow_dispatch'):
        raise ValueError('live_explicit_baseline_approval')
    if os.environ['MODE'] == 'live':
        if os.environ.get('LIVE_ENABLED') != 'true' or os.environ['REBUILD'] != 'false':
            raise Unavailable('live_operator_gate_required')
        legacy = _file(root, 'legacy.json')
        if legacy.get('path') != LEGACY or legacy.get('state') != 'disabled_manually' or not cache._positive(legacy.get('id')):
            raise Unavailable('live_legacy_writer_not_stopped')
        for status in ACTIVE:
            runs = _file(root, 'legacy-' + status + '.json')
            if not isinstance(runs, dict) or type(runs.get('total_count')) is not int or runs['total_count'] != 0 or runs.get('workflow_runs') != []:
                raise Unavailable('live_legacy_run_active_or_unknown')
    return current, scope


def _block(root, selection, reason):
    if os.environ['MODE'] == 'live':
        raise Unavailable(reason)
    selection.update(persist=False, blocked=reason)
    _write(root, 'selection.json', selection)
    _outputs({'persist': 'false', 'restore_key': '', 'producer_run': 0})


def select(root):
    current, scope = _context(root)
    selection = {'producer': None, 'previous_key': '', 'persist': True, 'blocked': None}
    try:
        records = cache.live_inventory(_file(root, 'caches.json'), _file(root, 'usage.json'), scope=scope, max_existing=1)
    except ValueError:
        return _block(root, selection, 'live_inventory_or_headroom_unavailable')
    try:
        producer = cache.previous_run(current, _file(root, 'history.json'), scope=scope, workflow_path=WORKFLOW)
    except (Unavailable, ValueError) as exc:
        if str(exc) not in ('live_immediate_predecessor_not_successful', 'live_untrusted_run'):
            raise
        if os.environ['REBUILD'] == 'false':
            return _block(root, selection, 'live_predecessor_unavailable')
        producer = None
    if os.environ['REBUILD'] == 'true':
        if os.environ['MODE'] != 'preview' or current['event'] != 'workflow_dispatch' or records:
            raise Unavailable('live_rebuild_requires_explicit_preview_and_empty_inventory')
        producer = None
    if producer is None:
        if records or os.environ['MODE'] == 'live':
            return _block(root, selection, 'live_fresh_nonnotifying_baseline_required')
    else:
        previous = cache.live_key(scope, producer['id'], producer['head_sha'], 'final')
        if set(records) != {previous}:
            return _block(root, selection, 'live_exact_predecessor_cache_missing')
        selection.update(producer=producer, previous_key=previous)
    _write(root, 'selection.json', selection)
    _outputs({'persist': 'true', 'restore_key': selection['previous_key'], 'producer_run': producer['id'] if producer else 0})


def producer_proof(body, producer):
    """Admit complete attempt1 jobs JSON, not a marker found inside a cache."""
    if (not isinstance(body, dict) or type(body.get('total_count')) is not int or not 1 <= body['total_count'] <= 2
            or not isinstance(body.get('jobs'), list) or len(body['jobs']) != body['total_count']):
        raise ValueError('live_producer_jobs_incomplete')
    names, ids, proofs = set(), set(), []
    for job in body['jobs']:
        if (not isinstance(job, dict) or not cache._positive(job.get('id')) or job['id'] in ids
                or job.get('name') not in ('prepare', 'consume') or job['name'] in names
                or type(job.get('run_id')) is not int or job['run_id'] != producer['id']
                or job.get('head_sha') != producer['head_sha'] or job.get('head_branch') != 'main'
                or job.get('status') != 'completed' or job.get('conclusion') not in ('success', 'skipped')
                or not isinstance(job.get('steps'), list) or len(job['steps']) > 64):
            raise ValueError('live_producer_job_identity')
        names.add(job['name'])
        ids.add(job['id'])
        for step in job['steps']:
            if not isinstance(step, dict) or not isinstance(step.get('name'), str):
                raise ValueError('live_producer_step')
            if step['name'].startswith('OKX_FINAL_V1') and step.get('conclusion') == 'success':
                match = PROOF.fullmatch(step['name'])
                if job['conclusion'] != 'success' or step.get('status') != 'completed' or match is None:
                    raise ValueError('live_producer_final_proof')
                manifest, checkpoint, generation, at = match.groups()
                if int(at) % MINUTE:
                    raise ValueError('live_producer_cutoff')
                proofs.append((job['name'], {'manifest_sha256': manifest, 'checkpoint_sha256': checkpoint,
                                             'generation': int(generation), 'at': int(at)}))
    conclusions = {job['name']: job['conclusion'] for job in body['jobs']}
    if (len(proofs) != 1 or conclusions.get('prepare') != 'success'
            or (proofs[0][0] == 'prepare' and conclusions.get('consume', 'skipped') != 'skipped')
            or (proofs[0][0] == 'consume' and conclusions.get('consume') != 'success')):
        raise ValueError('live_producer_unique_final_proof_required')
    return proofs[0][1]


def admit(root):
    _context(root)
    selection = _file(root, 'selection.json')
    if not selection['persist'] or selection['producer'] is None:
        return
    try:
        selection['proof'] = producer_proof(_file(root, 'producer-jobs.json'), selection['producer'])
    except ValueError:
        return _block(root, selection, 'live_producer_proof_unavailable')
    _write(root, 'selection.json', selection)


def _target(root, scope, expected=None):
    ref, content = _file(root, 'target-ref.json'), _file(root, 'target-readme.json', 2 * MIB)
    obj = ref.get('object', {})
    if (ref.get('ref') != scope.ref or obj.get('type') != 'commit' or not cache._sha(obj.get('sha'), 40)
            or (expected is not None and obj['sha'] != expected)
            or content.get('path') != 'README.md' or content.get('type') != 'file'
            or content.get('encoding') != 'base64' or not isinstance(content.get('content'), str)):
        raise ValueError('live_target_identity')
    raw = base64.b64decode(content['content'].replace('\n', ''), validate=True)
    if len(raw) > MIB or _git_blob(raw) != content.get('sha'):
        raise ValueError('live_target_readme_digest')
    raw.decode('utf-8')
    return obj['sha'], raw


def _export(root, workspace, current, scope, phase, now):
    proof = cache.seal(workspace, root / 'transfer', scope=scope, source_sha=current['head_sha'],
                       run_id=current['id'], created_at=now)
    proof['key'] = cache.live_key(scope, current['id'], current['head_sha'], phase)
    proof['checkpoint_sha256'] = digest(cache._read(root / 'transfer/checkpoint.json', 8 * MIB))
    _write(root, 'export.json', proof)
    _outputs({'persist': 'true', 'has_intent': str(phase == 'intent').lower(), 'key': proof['key'],
              'manifest': proof['manifest_sha256'], 'checkpoint': proof['checkpoint_sha256'],
              'proof': f"OKX_FINAL_V1 {proof['manifest_sha256']} {proof['checkpoint_sha256']} {proof['generation']} {proof['at']}"})


def prepare(root, *, now):
    current, scope = _context(root)
    selection = _file(root, 'selection.json')
    workspace = root / 'workspace'
    if workspace.exists() or workspace.is_symlink():
        raise ValueError('live_fresh_workspace_required')
    notification_mode, baseline_digest = 'shadow', ''
    if selection['persist'] and selection['producer'] is not None:
        producer, proof = selection['producer'], selection['proof']
        try:
            if os.environ.get('CACHE_HIT') != 'true':
                raise ValueError('live_exact_cache_readback_required')
            incoming = root / 'incoming'
            if incoming.exists() or incoming.is_symlink() or (root / 'transfer').is_symlink():
                raise ValueError('live_fresh_incoming_required')
            (root / 'transfer').rename(incoming)
            restored = cache.restore(incoming, workspace, scope=scope, source_sha=producer['head_sha'],
                                      run_id=producer['id'], manifest_sha256=proof['manifest_sha256'], now=now)
            if (restored['source_checkpoint_sha256'] != proof['checkpoint_sha256']
                    or restored['generation'] != proof['generation'] or restored['at'] != proof['at']
                    or restored['unresolved_entries']):
                raise ValueError('live_predecessor_state_proof_or_unresolved')
        except (ValueError, OSError):
            _block(root, selection, 'live_restore_blocked')
            workspace = root / 'diagnostic'
        else:
            if os.environ['MODE'] == 'live':
                if restored['source_notifications_enabled']:
                    if os.environ.get('ARM_MANIFEST'):
                        raise ValueError('live_baseline_approval_not_needed')
                    notification_mode = 'resume'
                else:
                    if os.environ.get('ARM_MANIFEST') != proof['manifest_sha256']:
                        raise Unavailable('live_reviewed_manifest_approval_required')
                    notification_mode, baseline_digest = 'baseline', digest((workspace / 'checkpoint.json').read_bytes())
    if workspace.exists() and (workspace.is_symlink() or not (workspace / 'checkpoint.json').is_file()):
        raise ValueError('live_fresh_or_verified_workspace_required')
    store = StateStore(workspace / 'checkpoint.json')
    with store.writer():
        state, reason = store.load()
        if state is None and store.path.exists():
            raise Unavailable(reason)
        expected = None if state is None else state.generation
        token, deadline = store.writer_token, time.monotonic() + 480
        if supervise(scan_job, (workspace, 1000, 4, token, expected, deadline, notification_mode, baseline_digest),
                     seconds=480, grace=5, lock_owner=store) != 0:
            raise Unavailable('live_scan_child_failed')
    report = _file(workspace, 'scan-report.json', 16 * MIB)
    now = max(now, int(time.time() * 1000))
    state, reason = store.load()
    if (report.get('run_token') != token or state is None or report.get('at') != state.at
            or state.notifications_enabled != (notification_mode != 'shadow') or report.get('publication') != 'disabled'):
        raise Unavailable('live_scan_report_not_committed:' + reason)
    result = {k: report[k] for k in ('at', 'pool', 'complete', 'partial', 'warmup', 'already_evaluated', 'deferred',
                                    'coverage_capped', 'http_attempts', 'http_retries', 'response_bytes')}
    result.update(mode=os.environ['MODE'], publication='not_sent', blocked=selection['blocked'],
                  previous_key=selection['previous_key'], generation=state.generation)
    _write(root, 'result.json', result)
    if not selection['persist']:
        _outputs({'persist': 'false', 'has_intent': 'false'})
        return
    phase = 'final'
    if notification_mode != 'shadow':
        head, readme = _target(root, scope)
        changes = [Transition((t['identity'][0], tuple(t['identity'][1]), t['identity'][2], t['identity'][3]),
                              t['stage'], t['at']) for t in report['transitions']]
        with store.writer():
            if prepare_batch(store, changes, scope=scope, source_sha=current['head_sha'], run_id=current['id'],
                             attempt=1, readme=readme, observed_at=now) is not None:
                phase = 'intent'
        _outputs({'head': head})
    _outputs({'previous_key': selection['previous_key']})
    _export(root, workspace, current, scope, phase, now)


def consume(root, *, now, transport=None):
    if (root / 'workspace').exists() or (root / 'workspace').is_symlink():
        raise ValueError('live_fresh_consumer_workspace_required')
    current, scope = _context(root)  # Workflow refreshed the operator/legacy fence.
    producer = cache.previous_run(current, _file(root, 'history.json'), scope=scope, workflow_path=WORKFLOW)
    previous_key = cache.live_key(scope, producer['id'], producer['head_sha'], 'final') if producer else ''
    if os.environ['MODE'] != 'live' or os.environ.get('PREVIOUS_KEY') != previous_key or os.environ.get('CACHE_HIT') != 'true':
        raise Unavailable('live_consumer_context')
    records = cache.live_inventory(_file(root, 'caches.json'), _file(root, 'usage.json'), scope=scope, max_existing=2)
    intent_key = cache.live_key(scope, current['id'], current['head_sha'], 'intent')
    if set(records) != {intent_key, previous_key}:
        raise Unavailable('live_consumer_exact_inventory')
    manifest, checkpoint = os.environ.get('INTENT_MANIFEST'), os.environ.get('INTENT_CHECKPOINT')
    if not cache._sha(manifest) or not cache._sha(checkpoint) or not cache._sha(os.environ.get('TARGET_HEAD'), 40):
        raise ValueError('live_consumer_independent_digest')
    bundle = root / 'transfer'
    restored = cache.restore(bundle, root / 'verified-shadow', scope=scope, source_sha=current['head_sha'],
                             run_id=current['id'], manifest_sha256=manifest, now=now)
    if restored['source_checkpoint_sha256'] != checkpoint or not restored['source_notifications_enabled']:
        raise Unavailable('live_consumer_checkpoint')
    head, readme = _target(root, scope, os.environ.get('TARGET_HEAD'))
    store = StateStore(bundle / 'checkpoint.json')
    with store.writer():
        publisher = GitHubPublisher(scope, cache.REPO_ID, head, readme, token=os.environ.get('GH_TOKEN', ''), transport=transport)
        commit = publish_pending(store, verified_checkpoint_sha256=checkpoint, scope=scope,
                                 source_sha=current['head_sha'], run_id=current['id'], attempt=1,
                                 readme=readme, observed_at=now, publish=publisher)
    bundle.rename(root / 'workspace')
    _write(root, 'result.json', {'mode': 'live', 'publication': 'commit_observed', 'commit': commit,
                               'previous_key': previous_key, 'email': 'not_verified_by_this_run'})
    _export(root, root / 'workspace', current, scope, 'final', now)


def guard_save(root):
    current, scope = _context(root)
    proof, result = _file(root, 'export.json'), _file(root, 'result.json')
    records = cache.live_inventory(_file(root, 'caches.json'), _file(root, 'usage.json'), scope=scope, max_existing=2)
    allowed = {result['previous_key']} if result['previous_key'] else set()
    intent = cache.live_key(scope, current['id'], current['head_sha'], 'intent')
    if proof['key'].endswith('-final') and result['publication'] == 'commit_observed':
        allowed.add(intent)
    if set(records) != allowed:
        raise Unavailable('live_save_inventory_drift')


def plan_cleanup(root, *, now):
    current, scope = _context(root)
    proof, result = _file(root, 'export.json'), _file(root, 'result.json')
    candidates = cache.retention(root / 'transfer', _file(root, 'caches.json'), _file(root, 'usage.json'),
                                 previous_key=result['previous_key'] or None, scope=scope, source_sha=current['head_sha'],
                                 run_id=current['id'], manifest_sha256=proof['manifest_sha256'], now=now)
    _write(root, 'cleanup.json', candidates)


def check_cleanup(root, identity):
    _, scope = _context(root)
    candidates = _file(root, 'cleanup.json')
    wanted = [r for r in candidates if r['id'] == identity]
    observed = cache.live_inventory(_file(root, 'caches.json'), _file(root, 'usage.json'), scope=scope)
    immutable = ('id', 'key', 'ref', 'version', 'size_in_bytes', 'created_at')
    actual = observed.get(wanted[0]['key']) if len(wanted) == 1 else None
    if actual is None or any(actual.get(k) != wanted[0].get(k) for k in immutable):
        raise Unavailable('live_cleanup_provider_drift')


def finish(root):
    current, scope = _context(root)
    proof, result = _file(root, 'export.json'), _file(root, 'result.json')
    key = cache.live_key(scope, current['id'], current['head_sha'], 'final')
    records = cache.live_inventory(_file(root, 'caches.json'), _file(root, 'usage.json'), scope=scope, max_existing=1)
    if proof['key'] != key or set(records) != {key}:
        raise Unavailable('live_final_save_or_cleanup_not_observed')
    if digest(cache._read(root / 'transfer/checkpoint.json', 8 * MIB)) != proof['checkpoint_sha256']:
        raise Unavailable('live_final_changed_after_seal')
    result.update(final_key=key, manifest_sha256=proof['manifest_sha256'], checkpoint_sha256=proof['checkpoint_sha256'],
                  generation=proof['generation'], at=proof['at'], final_cache_id=records[key]['id'],
                  final_cache_bytes=records[key]['size_in_bytes'])
    _write(root, 'result.json', result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('select', 'admit', 'prepare', 'consume', 'guard-save', 'plan-cleanup', 'check-cleanup', 'finish', 'summary'))
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--cache-id', type=int)
    args = parser.parse_args()
    now = int(time.time() * 1000)
    if args.phase == 'select':
        select(args.root)
    elif args.phase == 'admit':
        admit(args.root)
    elif args.phase == 'prepare':
        prepare(args.root, now=now)
    elif args.phase == 'consume':
        consume(args.root, now=now)
    elif args.phase == 'guard-save':
        guard_save(args.root)
    elif args.phase == 'plan-cleanup':
        plan_cleanup(args.root, now=now)
    elif args.phase == 'check-cleanup':
        check_cleanup(args.root, args.cache_id)
    elif args.phase == 'finish':
        finish(args.root)
    else:
        result = _file(args.root, 'result.json') if (args.root / 'result.json').is_file() else {'status': 'blocked_or_failed_before_result'}
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as stream:
            stream.write('### OKX candidate (not email-delivery proof)\n```json\n' + json.dumps(result, sort_keys=True) + '\n```\n')


if __name__ == '__main__':
    main()
