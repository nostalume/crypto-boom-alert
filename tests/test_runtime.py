"""Local process termination/commit evidence; no public HTTP or publication."""
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from test_stage3 import budget, response
from test_trial import PRODUCT, T, row, snapshot

from crypto_boom import data as data
from crypto_boom import runtime as runtime
from crypto_boom import state as state
from crypto_boom import trial as trial
from crypto_boom.scan import scan


def _blocking_http(marker):
    def blocked(request):
        Path(marker).write_text('entered_blocking_transport')
        time.sleep(60)
        return response([])
    with ThreadPoolExecutor(max_workers=1) as workers:
        with data.PublicClient(data.Budget(), transport=httpx.MockTransport(blocked)) as client:
            workers.submit(client.get, '/api/v5/public/time').result()


def _commit_job(path, token, marker, mode):
    store = state.StateStore(Path(path))
    real_atomic = state.atomic_write
    def blocked_atomic(target, raw):
        if mode == 'after_replace':
            real_atomic(target, raw)
        Path(marker).write_text(mode)
        time.sleep(60)
    if mode != 'success':
        state.atomic_write = blocked_atomic
    with store.borrowed_writer(token):
        checkpoint, _ = store.load()
        checkpoint.at = T
        store.save(checkpoint, checkpoint.generation)


def test_blocked_http_thread_is_reaped_before_next_writer(tmp_path):
    store = state.StateStore(tmp_path / 'state.json')
    marker = tmp_path / 'marker'
    started = time.monotonic()
    with store.writer():
        with pytest.raises(runtime.DeadlineExceeded, match='outcome may be old or new'):
            runtime.supervise(_blocking_http, (marker,), seconds=5, grace=1, lock_owner=store)
        assert store.lock_path.exists() and not store.retain_lock
    assert marker.read_text() == 'entered_blocking_transport'
    assert time.monotonic() - started < 7
    assert not store.lock_path.exists()
    with store.writer():
        store.save(state.Checkpoint(), None)


@pytest.mark.parametrize('mode,expected', [('success', 2), ('before_replace', 1), ('after_replace', 2)])
def test_process_commit_success_or_timeout_preserves_complete_generation(tmp_path, mode, expected):
    path, marker = tmp_path / 'state.json', tmp_path / 'marker'
    store = state.StateStore(path)
    with store.writer():
        store.save(state.Checkpoint(), None)
        if mode == 'success':
            assert runtime.supervise(_commit_job, (path, store.writer_token, marker, mode),
                                     seconds=5, grace=1, lock_owner=store) == 0
        else:
            with pytest.raises(runtime.DeadlineExceeded):
                runtime.supervise(_commit_job, (path, store.writer_token, marker, mode),
                                  seconds=5, grace=1, lock_owner=store)
            assert marker.read_text() == mode
        saved, reason = store.load()
        assert reason == 'state_hit' and saved.generation == expected
        assert saved.at == (0 if expected == 1 else T)
    assert not store.lock_path.exists()


def test_writer_loan_has_no_unlock_authority_and_changed_lock_is_not_deleted(tmp_path):
    owner = state.StateStore(tmp_path / 'state.json')
    child = state.StateStore(owner.path)
    with owner.writer():
        with pytest.raises(trial.Unavailable, match='mismatch'):
            with child.borrowed_writer('0' * 32):
                pass
        with child.borrowed_writer(owner.writer_token):
            child.save(state.Checkpoint(), None)
        assert owner.lock_path.exists()
        owner.lock_path.write_text('foreign_lock')
        with pytest.raises(trial.Unavailable, match='writer_lock'):
            owner.save(state.Checkpoint(), None)
    assert owner.lock_path.read_text() == 'foreign_lock'


def test_unreaped_child_retains_lock(tmp_path, monkeypatch):
    class StuckProcess:
        pid, exitcode = 123, None
        def start(self):
            pass
        def join(self, timeout):
            pass
        def is_alive(self):
            return True
        def terminate(self):
            pass
        def kill(self):
            pass
    class Context:
        def Process(self, **kwargs):
            return StuckProcess()
    monkeypatch.setattr(runtime.multiprocessing, 'get_context', lambda method: Context())
    owner = state.StateStore(tmp_path / 'state.json')
    with pytest.raises(runtime.UnreapedChild):
        with owner.writer():
            runtime.supervise(_blocking_http, (), seconds=2, grace=1, lock_owner=owner)
    assert owner.lock_path.exists()
    with pytest.raises(trial.Unavailable, match='writer_lock_present'):
        with owner.writer():
            pass


def test_capacity_defers_whole_product_without_advancing_checkpoint(tmp_path):
    cache = data.ProductCache(tmp_path)
    rows = snapshot(hour=True)
    series = {g: trial.Series.parse(raw, g) for g, raw in zip(data.GRAINS.values(), rows, strict=True)}
    cache.save(data.Snapshot(PRODUCT, T, series))
    checkpoint = state.Checkpoint(at=T)
    checkpoint.tracker.last_at[('A0', PRODUCT.key)] = T - trial.MINUTE
    b, _ = budget()
    def handle(request):
        grain = {'1m': trial.MINUTE, '5m': trial.FIVE, '1H': trial.HOUR}[request.url.params['bar']]
        end, limit = int(request.url.params['after']), int(request.url.params['limit'])
        existing = {int(raw[0]): raw for raw in rows[list(data.GRAINS.values()).index(grain)]}
        return response([existing.get(ts, row(ts)) for ts in [(end - 1) // grain * grain - i * grain for i in range(limit)]])
    report = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle), max_transitions=0)
    assert report['deferred'] == 1 and report['transitions'] == []
    assert report['products'][0]['status'] == 'deferred_event_capacity'
    assert checkpoint.tracker.last_at[('A0', PRODUCT.key)] == T - trial.MINUTE
    assert not checkpoint.tracker.active
    b, _ = budget()
    report = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle), max_transitions=1)
    assert len(report['transitions']) == 1 and checkpoint.tracker.last_at[('A0', PRODUCT.key)] == T


def test_ledger_capacity_refuses_before_building_unbounded_messages():
    checkpoint = state.Checkpoint(at=T, notifications_enabled=True)
    # Existing confirmed intents count towards the bound; input iteration is
    # halted at the first extra unique message, not after building the whole list.
    checkpoint.ledger = {str(i): {'status': 'confirmed', 'prepared_at': T} for i in range(10000)}
    change = trial.Transition(('identity', PRODUCT.key, 'UP', T), 'OBSERVED', T)
    def transitions():
        yield change
        raise AssertionError('must refuse before consuming another transition')
    with pytest.raises(trial.Unavailable, match='ledger_capacity'):
        checkpoint.prepare(transitions(), T)
    assert len(checkpoint.ledger) == 10000


def test_termination_error_retains_writer_lock(tmp_path, monkeypatch):
    class Process:
        pid = 123
        def start(self):
            pass
        def join(self, timeout):
            pass
        def is_alive(self):
            return True
        def terminate(self):
            raise OSError("controlled termination failure")
    class Context:
        def Process(self, **kwargs):
            return Process()
    monkeypatch.setattr(runtime.multiprocessing, 'get_context', lambda method: Context())
    store = state.StateStore(tmp_path / 'state.json')
    with pytest.raises(OSError, match='controlled termination'):
        with store.writer():
            runtime.supervise(_blocking_http, (), seconds=2, grace=1, lock_owner=store)
    assert store.lock_path.exists()


def test_catchup_cap_reports_omitted_history(tmp_path):
    from dataclasses import replace
    new = replace(PRODUCT, normal_since=T - trial.MINUTE)
    checkpoint = state.Checkpoint(at=T - 48 * trial.HOUR)
    checkpoint.tracker.last_at[('A0', new.key)] = checkpoint.at
    b, _ = budget()
    result = scan([new], T, data.ProductCache(tmp_path), checkpoint, b)
    assert result['products'][0]['catchup_truncated_minutes'] == 1440
    assert result['products'][0]['evaluated'] == 1440
    assert result['warmup'] == 1 and b.attempts == 0


def test_shadow_cli_rejects_previous_run_report(tmp_path, monkeypatch):
    from crypto_boom import scan as scanner
    store = state.StateStore(tmp_path / 'checkpoint.json')
    with store.writer():
        store.save(state.Checkpoint(), None)
    (tmp_path / 'scan-report.json').write_text('{"run_token":"previous"}')
    monkeypatch.setattr('sys.argv', ['scan', '--workspace', str(tmp_path)])
    monkeypatch.setattr(runtime, 'supervise', lambda *args, **kwargs: 0)
    with pytest.raises(trial.Unavailable, match='stale_shadow_report'):
        scanner.main()
    assert not (tmp_path / 'checkpoint.json.lock').exists()


def test_compute_deadline_discards_partial_replay(tmp_path, monkeypatch):
    from crypto_boom import scan as scanner
    cache = data.ProductCache(tmp_path)
    rows = snapshot(hour=True)
    series = {g: trial.Series.parse(raw, g) for g, raw in zip(data.GRAINS.values(), rows, strict=True)}
    cache.save(data.Snapshot(PRODUCT, T, series))
    checkpoint = state.Checkpoint(at=T - trial.MINUTE)
    checkpoint.tracker.last_at[('A0', PRODUCT.key)] = T - trial.MINUTE
    b, clock = budget()
    class Detector:
        def __init__(self, *args, **kwargs):
            pass
        def evaluate(self, at):
            clock.now = 420
            return (trial.Evaluation('A0', PRODUCT.key, at, True, direction='UP'),)
    monkeypatch.setattr(scanner, 'Detector', Detector)
    def handle(request):
        grain = {'1m': trial.MINUTE, '5m': trial.FIVE, '1H': trial.HOUR}[request.url.params['bar']]
        end, limit = int(request.url.params['after']), int(request.url.params['limit'])
        return response([row((end - 1) // grain * grain - i * grain) for i in range(limit)])
    result = scan([PRODUCT], T + trial.MINUTE, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert result['products'][0]['status'] == 'deferred_deadline'
    assert result['transitions'] == [] and not checkpoint.tracker.active
    assert checkpoint.tracker.last_at[('A0', PRODUCT.key)] == T - trial.MINUTE


@pytest.mark.parametrize('phase', range(5))
def test_cache_preserves_full_background_for_first_24h_catchup_minute(tmp_path, phase):
    at = T + phase * trial.MINUTE
    first = at - 24 * trial.HOUR + trial.MINUTE
    background = ((first - trial.FIVE) // trial.FIVE) * trial.FIVE - trial.WEEK - trial.FIVE
    rows = [row(point) for point in range(background, at // trial.FIVE * trial.FIVE, trial.FIVE)]
    series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
    series[trial.FIVE] = trial.Series.parse(rows, trial.FIVE)
    cache = data.ProductCache(tmp_path)
    cache.save(data.Snapshot(PRODUCT, at, series))
    restored, reason = cache.load(PRODUCT, at)
    assert reason == 'cache_hit'
    assert len(restored.series[trial.FIVE].window(background, trial.HISTORY + 1)) == trial.HISTORY + 1


def test_cache_keeps_192_completed_hours_at_non_hour_cutoff(tmp_path):
    at = T + 30 * trial.MINUTE
    series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
    series[trial.HOUR] = trial.Series.parse([row(point) for point in range(T - 192 * trial.HOUR, T, trial.HOUR)], trial.HOUR)
    cache = data.ProductCache(tmp_path)
    cache.save(data.Snapshot(PRODUCT, at, series))
    restored, reason = cache.load(PRODUCT, at)
    assert reason == 'cache_hit'
    assert len(restored.series[trial.HOUR].window(T - 192 * trial.HOUR, 192)) == 192


def test_owned_cache_directory_usage_fences_capacity_and_atomic_failure(tmp_path, monkeypatch):
    from dataclasses import replace
    cache = data.ProductCache(tmp_path)
    series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
    first = data.Snapshot(PRODUCT, T, series)
    cache.save(first)
    size = cache.path(PRODUCT).stat().st_size
    monkeypatch.setattr(data, 'CACHE_DIRECTORY_LIMIT', size + 1)
    other = replace(PRODUCT, inst_id='SECOND-USDT', base='SECOND')
    with pytest.raises(data.BudgetExceeded, match='cache_directory_limit'):
        cache.save(data.Snapshot(other, T, series))
    assert not cache.path(other).exists()
    assert cache.save(first) is False
    monkeypatch.setattr(data, 'CACHE_DIRECTORY_LIMIT', 1024 * 1024)
    def failure(*args):
        raise OSError('controlled atomic failure')
    real = data.atomic_write
    monkeypatch.setattr(data, 'atomic_write', failure)
    with pytest.raises(OSError):
        cache.save(data.Snapshot(other, T, series))
    monkeypatch.setattr(data, 'atomic_write', real)
    cache.save(data.Snapshot(other, T, series))
    assert cache.total == sum(p.stat().st_size for p in tmp_path.glob('*.json.gz'))


def test_cache_directory_partition_count_is_bounded(tmp_path, monkeypatch):
    from dataclasses import replace
    monkeypatch.setattr(data, 'MAX_PARTITIONS', 1)
    cache = data.ProductCache(tmp_path)
    series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
    cache.save(data.Snapshot(PRODUCT, T, series))
    other = replace(PRODUCT, inst_id='SECOND-USDT', base='SECOND')
    with pytest.raises(data.BudgetExceeded, match='partition_count'):
        cache.save(data.Snapshot(other, T, series))
    assert len(list(tmp_path.glob('*.json.gz'))) == 1


def test_ambiguous_process_start_retains_writer_lock(tmp_path, monkeypatch):
    class Process:
        pid = None
        def start(self):
            raise OSError('controlled native start failure')
    class Context:
        def Process(self, **kwargs):
            return Process()
    monkeypatch.setattr(runtime.multiprocessing, 'get_context', lambda method: Context())
    store = state.StateStore(tmp_path / 'state.json')
    with pytest.raises(OSError, match='native start'):
        with store.writer():
            runtime.supervise(_blocking_http, (), seconds=2, grace=1, lock_owner=store)
    assert store.lock_path.exists()


def test_oversized_cache_replacement_does_not_read_old_body(tmp_path, monkeypatch):
    cache = data.ProductCache(tmp_path)
    path = cache.path(PRODUCT)
    path.write_bytes(b'x' * (1024 * 1024 + 1))
    assert cache.load(PRODUCT, T)[1] == 'cache_oversized'
    real_open = Path.open
    def guarded_open(self, mode='r', *args, **kwargs):
        if self == path and mode == 'rb':
            raise AssertionError('oversized old body must not be read for comparison')
        return real_open(self, mode, *args, **kwargs)
    series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', guarded_open)
        cache.save(data.Snapshot(PRODUCT, T, series))
    assert cache.load(PRODUCT, T)[1] == 'cache_hit'
    assert path.stat().st_size <= 1024 * 1024


def test_cancel_blocked_http_reaps_owned_child_and_releases_writer(tmp_path):
    store = state.StateStore(tmp_path / 'state.json')
    marker = tmp_path / 'entered'
    started = time.monotonic()
    with store.writer():
        with pytest.raises(runtime.Cancelled):
            runtime.supervise(_blocking_http, (marker,), seconds=5, grace=1,
                              lock_owner=store, cancel=marker.exists)
        assert store.lock_path.exists() and not store.retain_lock
    assert marker.exists() and not store.lock_path.exists()
    assert time.monotonic() - started < 5


@pytest.mark.parametrize('mode,generation', [('before_replace', 1), ('after_replace', 2)])
def test_cancel_during_commit_keeps_old_or_new_complete_checkpoint(tmp_path, mode, generation):
    store = state.StateStore(tmp_path / 'state.json')
    marker = tmp_path / 'entered'
    with store.writer():
        store.save(state.Checkpoint(), None)
        with pytest.raises(runtime.Cancelled):
            runtime.supervise(_commit_job, (store.path, store.writer_token, marker, mode),
                              seconds=5, grace=1, lock_owner=store, cancel=marker.exists)
        saved, reason = store.load()
        assert reason == 'state_hit' and saved.generation == generation and saved.ledger == {}
    assert not store.lock_path.exists()
