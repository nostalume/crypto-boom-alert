"""Single bounded Actions-cache transfer; fail-closed shadow adoption, no sender.

The workflow owns authenticated GET/cache operations. Exact producer identity and
manifest SHA must come from outside the restored cache. Cache is evictable, never
an authority to re-enable notifications or infer that a message was not sent.
"""
import argparse
import json
import os
import re
import time
from dataclasses import asdict
from pathlib import Path

from crypto_boom.okx_benchmark import benchmark, measure_scan, read_scan_report
from crypto_boom.okx_state import StateStore, atomic_write
from crypto_boom.okx_storage import DAY, GIB, MIB, Scope, digest
from crypto_boom.okx_trial import VERSION, Unavailable

PREFIX = 'okx-stage3-cache-v1-'
WIRE = 'okx-shadow-cache-v1'
PAYLOAD_LIMIT = 96 * MIB
MANIFEST_LIMIT = 256 * 1024
FILE = re.compile(r'market/[0-9a-f]{64}\.json\.gz|checkpoint\.json')
REPO = 'nostalume/crypto-boom-alert'
REPO_ID = 1391009742
WORKFLOW = '.github/workflows/okx-cache-acceptance.yml'
NOTIFICATION_WORKFLOW = '.github/workflows/okx-notification-acceptance.yml'


def _json(body):
    return json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _pairs(rows):
    out = {}
    for key, value in rows:
        if key in out:
            raise ValueError('cache_duplicate_key')
        out[key] = value
    return out


def _read(path, limit):
    if path.is_symlink() or not path.is_file():
        raise ValueError('cache_regular_file_required')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('cache_file_size_limit')
    return raw


def _sha(value, length=64):
    return isinstance(value, str) and re.fullmatch(f'[0-9a-f]{{{length}}}', value) is not None


def _positive(value):
    return type(value) is int and value > 0


def code_digest():
    files = sorted(Path(__file__).parent.glob('okx_*.py'))
    return digest(_json({p.name: digest(p.read_bytes().replace(b'\r\n', b'\n')) for p in files}))


def cache_key(run_id, source_sha):
    if not _positive(run_id) or not _sha(source_sha, 40):
        raise ValueError('cache_producer_identity')
    return f'{PREFIX}r{run_id}-a1-{source_sha}'


def seal(workspace: Path, target: Path, *, scope: Scope, source_sha: str, run_id: int, created_at: int):
    """Export bounded regular files after scan termination; no cache API calls."""
    if (not isinstance(scope, Scope) or scope.repository != REPO or scope.ref != 'refs/heads/main'
            or type(created_at) is not int or created_at < 0):
        raise ValueError('cache_scope_or_time')
    key = cache_key(run_id, source_sha)
    if workspace.is_symlink() or (workspace / 'market').is_symlink() or (workspace / 'checkpoint.json.lock').exists():
        raise ValueError('cache_workspace_not_quiescent')
    state, reason = StateStore(workspace / 'checkpoint.json').load()
    if state is None or reason != 'state_hit' or created_at < state.at or state.generation < 1:
        raise ValueError('cache_checkpoint_required')
    paths = [workspace / 'checkpoint.json', *sorted((workspace / 'market').glob('*.json.gz'))]
    if len(paths) > 1001:
        raise ValueError('cache_partition_limit')
    files, total = {}, 0
    # Read/copy bounded bytes once. A failed export never produces a manifest.
    if target.exists() or target.is_symlink():
        raise ValueError('cache_fresh_target_required')
    target.mkdir(parents=True)
    (target / 'market').mkdir()
    for path in paths:
        name = path.relative_to(workspace).as_posix()
        if FILE.fullmatch(name) is None:
            raise ValueError('cache_file_path')
        raw = _read(path, 8 * MIB if name == 'checkpoint.json' else MIB)
        total += len(raw)
        if total > PAYLOAD_LIMIT:
            raise ValueError('cache_payload_limit')
        atomic_write(target / name, raw)
        files[name] = {'bytes': len(raw), 'sha256': digest(raw)}
    body = {'schema': WIRE, 'rules_version': VERSION, 'scope': asdict(scope), 'source_sha': source_sha,
            'code_sha256': code_digest(), 'run_id': run_id, 'attempt': 1, 'created_at': created_at,
            'expires_at': created_at + DAY, 'generation': state.generation, 'at': state.at, 'files': files}
    raw = _json(body)
    if len(raw) > MANIFEST_LIMIT:
        raise ValueError('cache_manifest_limit')
    atomic_write(target / 'manifest.json', raw)
    return {'key': key, 'manifest_sha256': digest(raw), 'source_sha': source_sha, 'run_id': run_id,
            'generation': state.generation, 'at': state.at, 'payload_bytes': total, 'files': len(files)}


def restore(bundle: Path, workspace: Path, *, scope: Scope, source_sha: str, run_id: int,
            manifest_sha256: str, now: int):
    """Authenticate all transferred bytes before adopting into a fresh shadow.

    No latest fallback, in-place replacement, ledger pruning or automatic enable.
    Files are copied only after a full admission pass. This is not remote CAS.
    """
    cache_key(run_id, source_sha)
    if not isinstance(scope, Scope) or not _sha(manifest_sha256) or type(now) is not int or now < 0:
        raise ValueError('cache_restore_authority')
    if bundle.is_symlink() or (bundle / 'market').is_symlink():
        raise ValueError('cache_bundle_link')
    raw = _read(bundle / 'manifest.json', MANIFEST_LIMIT)
    if digest(raw) != manifest_sha256:
        raise ValueError('cache_manifest_digest')
    body = json.loads(raw, object_pairs_hook=_pairs)
    fields = {'schema', 'rules_version', 'scope', 'source_sha', 'code_sha256', 'run_id', 'attempt',
              'created_at', 'expires_at', 'generation', 'at', 'files'}
    if (not isinstance(body, dict) or set(body) != fields or body['schema'] != WIRE
            or body['rules_version'] != VERSION or body['scope'] != asdict(scope)
            or body['source_sha'] != source_sha or type(body['run_id']) is not int or body['run_id'] != run_id
            or type(body['attempt']) is not int or body['attempt'] != 1 or body['code_sha256'] != code_digest()
            or type(body['created_at']) is not int or type(body['expires_at']) is not int
            or not 0 <= body['created_at'] <= now < body['expires_at'] == body['created_at'] + DAY
            or not _positive(body['generation']) or type(body['at']) is not int
            or not 0 <= body['at'] <= body['created_at']):
        raise ValueError('cache_manifest_identity_or_expiry')
    files = body['files']
    if not isinstance(files, dict) or not 1 <= len(files) <= 1001 or 'checkpoint.json' not in files:
        raise ValueError('cache_inventory_limit')
    observed = set()
    for path in bundle.iterdir():
        if path.name == 'market' and path.is_dir() and not path.is_symlink():
            for part in path.iterdir():
                if part.is_symlink() or not part.is_file():
                    raise ValueError('cache_regular_partition_required')
                observed.add('market/' + part.name)
        elif path.name in ('manifest.json', 'checkpoint.json') and path.is_file() and not path.is_symlink():
            if path.name == 'checkpoint.json':
                observed.add(path.name)
        else:
            raise ValueError('cache_extra_or_unsafe_entry')
    if observed != set(files):
        raise ValueError('cache_inventory_mismatch')
    total = 0
    for name, record in files.items():
        if (not isinstance(name, str) or FILE.fullmatch(name) is None or not isinstance(record, dict)
                or set(record) != {'bytes', 'sha256'} or type(record['bytes']) is not int or record['bytes'] < 0
                or not _sha(record['sha256'])):
            raise ValueError('cache_inventory_record')
        total += record['bytes']
        if total > PAYLOAD_LIMIT:
            raise ValueError('cache_payload_limit')
        content = _read(bundle / name, 8 * MIB if name == 'checkpoint.json' else MIB)
        if len(content) != record['bytes'] or digest(content) != record['sha256']:
            raise ValueError('cache_file_digest')
    state, reason = StateStore(bundle / 'checkpoint.json').load()
    if (state is None or reason != 'state_hit' or state.generation != body['generation'] or state.at != body['at']):
        raise ValueError('cache_checkpoint_mismatch')
    if workspace.exists() or workspace.is_symlink():
        raise ValueError('cache_fresh_workspace_required')
    workspace.mkdir(parents=True)
    store = StateStore(workspace / 'checkpoint.json')
    with store.writer():
        adopted = store.restore_shadow(state)
        (workspace / 'market').mkdir()
        for name, record in files.items():
            if name != 'checkpoint.json':
                content = _read(bundle / name, MIB)
                if len(content) != record['bytes'] or digest(content) != record['sha256']:
                    raise ValueError('cache_file_changed_during_adoption')
                atomic_write(workspace / name, content)
    return {'state': 'restored_shadow', 'generation': adopted.generation, 'at': adopted.at,
            'cursor': adopted.cursor, 'ledger_entries': len(adopted.ledger), 'publication': 'disabled',
            'unresolved_entries': sum(v['status'] != 'confirmed' for v in adopted.ledger.values()),
            'payload_bytes': total, 'files': len(files)}


def admission(mode, current, producer, usage, *, run_id, source_sha, producer_id=0, producer_sha='', expected_digest='',
              workflow_path=WORKFLOW):
    """Admit authenticated workflow-owned JSON, not self-reported cache metadata."""
    if (workflow_path not in (WORKFLOW, NOTIFICATION_WORKFLOW)
            or mode not in ('bootstrap', 'restore') or not _positive(run_id) or not _sha(source_sha, 40)
            or not isinstance(usage, dict) or type(usage.get('active_caches_size_in_bytes')) is not int
            or not 0 <= usage['active_caches_size_in_bytes'] <= 8 * GIB
            or type(usage.get('active_caches_count')) is not int or usage['active_caches_count'] < 0
            or usage.get('full_name') != REPO):
        raise ValueError('cache_free_admission_denied')
    for run, identity, source in [(current, run_id, source_sha), *([(producer, producer_id, producer_sha)] if mode == 'restore' else [])]:
        if (not isinstance(run, dict) or type(run.get('id')) is not int or run['id'] != identity
                or not _positive(run.get('workflow_id')) or run.get('path') != workflow_path
                or run.get('event') != 'workflow_dispatch' or run.get('run_attempt') != 1
                or type(run.get('run_attempt')) is not int or run.get('head_branch') != 'main'
                or run.get('head_sha') != source or not _sha(source, 40)
                or not isinstance(run.get('repository'), dict) or not isinstance(run.get('head_repository'), dict)
                or run['repository'].get('id') != REPO_ID or run['head_repository'].get('id') != REPO_ID
                or run.get('pull_requests') != []):
            raise ValueError('cache_untrusted_run')
    if mode == 'restore' and (not _positive(producer_id) or producer_id == run_id or not _sha(expected_digest)
            or producer.get('workflow_id') != current['workflow_id'] or producer.get('status') != 'completed'
            or producer.get('conclusion') != 'success'):
        raise ValueError('cache_producer_not_accepted')
    if mode == 'bootstrap' and (producer_id or producer_sha or expected_digest):
        raise ValueError('cache_bootstrap_must_be_fresh')
    return Scope(REPO, current['workflow_id'], 'refs/heads/main')


def fault_checks(bundle, root, authority):
    outcomes = {}
    for name, path, change in [('missing', root / 'missing', {}), ('wrong_digest', bundle, {'manifest_sha256': '0' * 64}),
                              ('wrong_source', bundle, {'source_sha': '0' * 40}),
                              ('expired', bundle, {'now': authority['now'] + DAY})]:
        target = root / ('fault-' + name)
        try:
            restore(path, target, **{**authority, **change})
        except (ValueError, OSError):
            if target.exists():
                raise ValueError('fault_created_state_before_admission')
            outcomes[name] = 'blocked_without_state'
        else:
            raise ValueError('fault_did_not_block')
    # Exact hash protects corrupted bytes even if a marker/metadata looks valid.
    bad = root / 'bad-bundle'
    bad.mkdir()
    (bad / 'market').mkdir()
    (bad / 'manifest.json').write_bytes((bundle / 'manifest.json').read_bytes())
    (bad / 'checkpoint.json').write_bytes(b'corrupted')
    try:
        restore(bad, root / 'fault-corrupt', **authority)
    except (ValueError, OSError):
        outcomes['corrupt'] = 'blocked_without_state'
    else:
        raise ValueError('corrupt_did_not_block')
    # Missing state and unknown outcomes cannot prepare messages (existing owner).
    from crypto_boom.okx_state import Checkpoint
    from crypto_boom.okx_trial import Transition
    checkpoint = Checkpoint(generation=1, at=authority['now'] // 60000 * 60000, notifications_enabled=True)
    transition = Transition((VERSION + '/A0', ('okx', 'SPOT', 'FAULT-USDT'), 'UP', checkpoint.at), 'OBSERVED', checkpoint.at)
    ids = checkpoint.prepare([transition], checkpoint.at)
    for name in ('pending', 'unknown'):
        if name == 'unknown':
            checkpoint.unknown(ids)
        try:
            checkpoint.prepare([transition], checkpoint.at)
        except Unavailable:
            outcomes[name] = 'publication_blocked'
        else:
            raise ValueError('unresolved_did_not_block')
    return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('admit', 'run'))
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root
    mode = os.environ['CACHE_MODE']
    run_id, source_sha = int(os.environ['GITHUB_RUN_ID']), os.environ['GITHUB_SHA']
    producer_id = int(os.environ.get('PRODUCER_RUN') or '0')
    producer_sha, expected_digest = os.environ.get('PRODUCER_SHA', ''), os.environ.get('BUNDLE_DIGEST', '')
    if (os.environ.get('GITHUB_REPOSITORY') != REPO or os.environ.get('GITHUB_REF') != 'refs/heads/main'
            or os.environ.get('GITHUB_EVENT_NAME') != 'workflow_dispatch' or os.environ.get('GITHUB_RUN_ATTEMPT') != '1'):
        raise ValueError('cache_workflow_context')
    current = json.loads(_read(root / 'current.json', MIB))
    producer = json.loads(_read(root / 'producer.json', MIB)) if mode == 'restore' else None
    usage = json.loads(_read(root / 'usage.json', MIB))
    scope = admission(mode, current, producer, usage, run_id=run_id, source_sha=source_sha,
                      producer_id=producer_id, producer_sha=producer_sha, expected_digest=expected_digest)
    if args.operation == 'admit':
        restore_key = cache_key(producer_id, producer_sha) if mode == 'restore' else 'unused-bootstrap'
        with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as out:
            out.write(f'save_key={cache_key(run_id, source_sha)}\nrestore_key={restore_key}\n')
        return
    report = {'schema': 'okx-cache-acceptance-v1', 'mode': mode, 'publication': 'disabled', 'rounds': []}
    workspace = root / 'scan'
    if mode == 'bootstrap':
        result = benchmark(workspace, rounds=3, limit=1000, workers=4)
        report['rounds'] = result['rounds']
        last = result['rounds'][-1]
        if last['coverage_capped'] or last['partial'] or last['deferred']:
            raise ValueError('cache_bootstrap_not_complete')
    else:
        if os.environ.get('CACHE_HIT') != 'true':
            raise ValueError('cache_exact_restore_required')
        # Restore/save action cache versions include paths: keep transfer as the
        # exported path in BOTH runs, preserve the incoming immutable bundle here.
        incoming = root / 'incoming'
        transfer = root / 'transfer'
        if (transfer.is_symlink() or not transfer.is_dir() or incoming.exists() or incoming.is_symlink()
                or transfer.parent.resolve() != root.resolve() or incoming.parent.resolve() != root.resolve()):
            raise ValueError('cache_transfer_directory_required')
        transfer.rename(incoming)
        authority = dict(scope=scope, source_sha=producer_sha, run_id=producer_id,
                         manifest_sha256=expected_digest, now=int(time.time() * 1000))
        started = time.monotonic()
        report['restore'] = restore(incoming, workspace, **authority)
        report['restore_validation_seconds'] = time.monotonic() - started
        report['faults'] = fault_checks(incoming, root, authority)
        previous_generation, previous_at, previous_token = report['restore']['generation'], report['restore']['at'], None
        for number in range(1, 3):
            metrics = measure_scan(workspace, number, 1000, 4)
            if metrics['exit_code'] != 0:
                raise ValueError('cache_warm_scan_failed')
            summary, token = read_scan_report(workspace, previous_token)
            raw = json.loads(_read(workspace / 'scan-report.json', 16 * MIB))
            misses = sum(p.get('cache') != 'cache_hit' for p in raw['products'] if p['status'] not in ('warmup', 'already_evaluated'))
            if (summary['coverage_capped'] or summary['partial'] or summary['deferred'] or misses
                    or summary['already_evaluated'] or summary['at'] <= previous_at
                    or summary['state_generation'] != previous_generation + 1 or metrics['wall_seconds'] >= 600):
                raise ValueError('cache_not_complete_advancing_warm_under_10_minutes')
            report['rounds'].append({**metrics, **summary, 'cold_products': misses})
            previous_generation, previous_at, previous_token = summary['state_generation'], summary['at'], token
    report['sealed'] = seal(workspace, root / 'transfer', scope=scope, source_sha=source_sha,
                            run_id=run_id, created_at=int(time.time() * 1000))
    report['limits'] = ['cache_is_evictable', 'no_automatic_notification_enable', 'no_p95_or_24h_claim']
    atomic_write(root / 'acceptance-report.json', _json(report))
    print('CACHE_ACCEPTANCE=' + _json(report).decode(), flush=True)


if __name__ == '__main__':
    main()
