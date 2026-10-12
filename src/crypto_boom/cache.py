"""Single bounded Actions-cache transfer; fail-closed shadow adoption, no sender.

The workflow owns authenticated GET/cache operations. Exact producer identity and
manifest SHA must come from outside the restored cache. Cache is evictable, never
an authority to re-enable notifications or infer that a message was not sent.
"""
import json
import re
from dataclasses import asdict
from pathlib import Path

from crypto_boom.state import StateStore, atomic_write
from crypto_boom.storage import DAY, GIB, MIB, Scope, digest
from crypto_boom.trial import VERSION, Unavailable

WIRE = 'okx-shadow-cache-v1'
PAYLOAD_LIMIT = 96 * MIB
MANIFEST_LIMIT = 256 * 1024
FILE = re.compile(r'market/[0-9a-f]{64}\.json\.gz|checkpoint\.json')
REPO = 'nostalume/crypto-boom-alert'
REPO_ID = 1391009742
LIVE_PREFIX = 'okx-live-cache-v1-'


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
    files = sorted(Path(__file__).parent.glob('*.py'))
    return digest(_json({p.name: digest(p.read_bytes().replace(b'\r\n', b'\n')) for p in files}))


def cache_key(run_id, source_sha):
    """Stable bundle provenance label, not the native live cache lookup key."""
    if not _positive(run_id) or not _sha(source_sha, 40):
        raise ValueError('cache_producer_identity')
    return f'okx-stage3-cache-v1-r{run_id}-a1-{source_sha}'


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


def _validated_bundle(bundle: Path, *, scope: Scope, source_sha: str, run_id: int,
                      manifest_sha256: str, now: int):
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
    return state, files, total


def restore(bundle: Path, workspace: Path, *, scope: Scope, source_sha: str, run_id: int,
            manifest_sha256: str, now: int):
    """Validate before fresh shadow adoption; never automatically enable sending."""
    state, files, total = _validated_bundle(bundle, scope=scope, source_sha=source_sha, run_id=run_id,
                                           manifest_sha256=manifest_sha256, now=now)
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
            'source_notifications_enabled': state.notifications_enabled,
            'source_checkpoint_sha256': files['checkpoint.json']['sha256'],
            'cursor': adopted.cursor, 'ledger_entries': len(adopted.ledger), 'publication': 'disabled',
            'unresolved_entries': sum(v['status'] != 'confirmed' for v in adopted.ledger.values()),
            'payload_bytes': total, 'files': len(files)}


def _live_run(run, scope, workflow_path):
    if (not isinstance(scope, Scope) or scope.repository != REPO or scope.ref != 'refs/heads/main'
            or not isinstance(workflow_path, str)
            or re.fullmatch(r'\.github/workflows/[A-Za-z0-9_-]+\.yml', workflow_path) is None
            or not isinstance(run, dict) or not _positive(run.get('id'))
            or not _positive(run.get('run_number')) or run.get('workflow_id') != scope.workflow_id
            or type(run.get('workflow_id')) is not int or run.get('path') != workflow_path
            or run.get('event') not in ('schedule', 'workflow_dispatch')
            or type(run.get('run_attempt')) is not int or run['run_attempt'] != 1
            or run.get('head_branch') != 'main' or not _sha(run.get('head_sha'), 40)
            or any(not isinstance(run.get(k), dict) or type(run[k].get('id')) is not int
                   or run[k]['id'] != REPO_ID for k in ('repository', 'head_repository'))
            or run.get('pull_requests') != []):
        raise ValueError('live_untrusted_run')


def previous_run(current, history, *, scope: Scope, workflow_path: str):
    """Select the immediate run, NEVER latest-success; no API or send authority.

    Caller supplies the authenticated first page for this workflow, with NO
    status/event/branch/source filters. Concurrency and a fresh pre-send query
    remain caller obligations; provider pagination is not a distributed lock.
    """
    _live_run(current, scope, workflow_path)
    if (not isinstance(history, dict) or type(history.get('total_count')) is not int
            or history['total_count'] < 1 or not isinstance(history.get('workflow_runs'), list)
            or len(history['workflow_runs']) != min(history['total_count'], 100)):
        raise ValueError('live_run_page_incomplete')
    runs, numbers = {}, set()
    for run in history['workflow_runs']:
        if (not isinstance(run, dict) or not _positive(run.get('id')) or run['id'] in runs
                or not _positive(run.get('run_number')) or run['run_number'] in numbers
                or type(run.get('workflow_id')) is not int or run['workflow_id'] != scope.workflow_id
                or run.get('path') != workflow_path):
            raise ValueError('live_run_page_identity')
        runs[run['id']] = run
        numbers.add(run['run_number'])
    if runs.get(current['id']) != current or max(numbers) != current['run_number']:
        raise Unavailable('live_current_not_latest')
    if current['run_number'] == 1:
        return None  # Explicit non-notifying first baseline only.
    producer = next((r for r in runs.values() if r['run_number'] == current['run_number'] - 1), None)
    _live_run(producer, scope, workflow_path)
    if producer.get('status') != 'completed' or producer.get('conclusion') != 'success':
        raise Unavailable('live_immediate_predecessor_not_successful')
    return producer.copy()


def live_key(scope: Scope, run_id: int, source_sha: str, phase: str):
    cache_key(run_id, source_sha)
    if (not isinstance(scope, Scope) or scope.repository != REPO or scope.ref != 'refs/heads/main'
            or run_id >= 10 ** 19 or phase not in ('intent', 'final')):
        raise ValueError('live_cache_identity')
    return f'{LIVE_PREFIX}w{scope.workflow_id}-r{run_id}-a1-{source_sha}-{phase}'


def live_inventory(body, usage, *, scope: Scope, max_existing=3):
    """Complete owned-prefix query and known free headroom; no eviction fallback."""
    if (not isinstance(scope, Scope) or scope.repository != REPO or scope.ref != 'refs/heads/main'
            or type(max_existing) is not int or not 0 <= max_existing <= 3
            or not isinstance(usage, dict) or usage.get('full_name') != REPO
            or type(usage.get('active_caches_size_in_bytes')) is not int
            or not 0 <= usage['active_caches_size_in_bytes'] <= 8 * GIB
            or type(usage.get('active_caches_count')) is not int or usage['active_caches_count'] < 0
            or not isinstance(body, dict) or type(body.get('total_count')) is not int
            or not 0 <= body['total_count'] <= max_existing or not isinstance(body.get('actions_caches'), list)
            or len(body['actions_caches']) != body['total_count']):
        raise ValueError('live_cache_inventory_or_headroom')
    prefix = f'{LIVE_PREFIX}w{scope.workflow_id}-'
    records, ids = {}, set()
    for record in body['actions_caches']:
        if (not isinstance(record, dict) or not _positive(record.get('id')) or record['id'] in ids
                or not isinstance(record.get('key'), str) or record['key'] in records
                or re.fullmatch(re.escape(prefix) + r'r[1-9][0-9]{0,18}-a1-[0-9a-f]{40}-(intent|final)', record['key']) is None
                or record.get('ref') != scope.ref or not _sha(record.get('version'))
                or type(record.get('size_in_bytes')) is not int or not 0 < record['size_in_bytes'] <= 128 * MIB):
            raise ValueError('live_cache_metadata')
        records[record['key']] = record.copy()
        ids.add(record['id'])
    return records


def retention(bundle, body, usage, *, previous_key=None, scope, source_sha, run_id, manifest_sha256, now):
    """Return exact deletion candidates ONLY after independent final validation.

    manifest_sha256 comes from workflow proof outside the cache; previous_key
    is the exact admitted predecessor final. Caller rechecks provider identity
    before DELETE and observes deletion; this function performs neither effect.
    """
    final_key = live_key(scope, run_id, source_sha, 'final')
    intent_key = live_key(scope, run_id, source_sha, 'intent')
    records = live_inventory(body, usage, scope=scope)
    if (final_key not in records or previous_key in (final_key, intent_key)
            or (previous_key is not None and (previous_key not in records or not previous_key.endswith('-final')))
            or set(records) - {final_key, intent_key, previous_key}):
        raise Unavailable('live_retention_exact_final_and_predecessor_required')
    state, _, _ = _validated_bundle(bundle, scope=scope, source_sha=source_sha, run_id=run_id,
                                    manifest_sha256=manifest_sha256, now=now)
    if any(v['status'] != 'confirmed' for v in state.ledger.values()):
        raise Unavailable('live_retention_unresolved_publication')
    return tuple(records[k] for k in (intent_key, previous_key) if k in records)
