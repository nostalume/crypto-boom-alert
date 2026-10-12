"""Mandatory-pool barrier and optional hourly context; zero real HTTP."""
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace

import httpx
import pytest
from test_data import budget, response
from test_trial import PRODUCT, T, row

from crypto_boom import data as data
from crypto_boom import state as state
from crypto_boom import trial as trial
from crypto_boom.scan import scan


class MemoryCache:
    """Only mocks persistence cost; request/Detector/Budget paths remain real."""
    def __init__(self, snapshots):
        self.snapshots = {s.product.key: s for s in snapshots}
        self.loads = {}
        self.saves = {}
        self.lock = threading.Lock()
        self.fail_load = None
        self.fail_save = None

    def path(self, product):
        return SimpleNamespace(exists=lambda: product.key in self.snapshots)

    def load(self, product, at):
        with self.lock:
            self.loads[product.key] = self.loads.get(product.key, 0) + 1
            if self.fail_load and self.loads[product.key] == 2:
                return None, "controlled_optional_cache_miss"
            value = self.snapshots.get(product.key)
            return (data.Snapshot(product, value.at, dict(value.series)), "cache_hit") if value else (None, "cache_missing")

    def save(self, value):
        with self.lock:
            count = self.saves.get(value.product.key, 0) + 1
            self.saves[value.product.key] = count
            if self.fail_save and count == self.fail_save:
                raise OSError("controlled_cache_write")
            self.snapshots[value.product.key] = data.Snapshot(value.product, value.at, dict(value.series))
            return True


def fixture(count, *, qualified=(), complete_hours=False, previous=T - 10 * trial.MINUTE):
    products = [replace(PRODUCT, inst_id=f"TEST{i:04d}-USDT", base=f"TEST{i:04d}") for i in range(count)]
    common = {
        trial.FIVE: [row(ts) for ts in range(T - 8 * 24 * trial.HOUR - 3 * trial.FIVE, T, trial.FIVE)],
        trial.HOUR: [row(ts) for ts in range(T - 169 * trial.HOUR, T, trial.HOUR)],
    }
    minute = {}
    for i, p in enumerate(products):
        minute[p.key] = [row(ts, close=101 if i in qualified and ts >= T - 5 * trial.MINUTE else 100,
                             quote=30_000 if i in qualified else 5_000)
                         for ts in range(previous - 6 * trial.MINUTE, T, trial.MINUTE)]
    shared = {g: trial.Series.parse([r for r in rows if int(r[0]) < (previous // g) * g], g)
              for g, rows in common.items()}
    if complete_hours:
        shared[trial.HOUR] = trial.Series.parse(common[trial.HOUR], trial.HOUR)
    snapshots = []
    for p in products:
        series = dict(shared)
        series[trial.MINUTE] = trial.Series.parse([r for r in minute[p.key] if int(r[0]) < previous], trial.MINUTE)
        snapshots.append(data.Snapshot(p, previous, series))
    cache = MemoryCache(snapshots)
    checkpoint = state.Checkpoint(at=previous)
    for p in products:
        checkpoint.tracker.last_at[("A0", p.key)] = previous
    minute_by_id = {product.inst_id: minute[product.key] for product in products}
    calls, lock = [], threading.Lock()
    def handle(request):
        kind, inst_id = request.url.params["bar"], request.url.params["instId"]
        with lock:
            calls.append((kind, inst_id))
        g = {"1m": trial.MINUTE, "5m": trial.FIVE, "1H": trial.HOUR}[kind]
        rows = minute_by_id[inst_id] if g == trial.MINUTE else common[g]
        end, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        return response([r for r in reversed(rows) if int(r[0]) < end][:limit])
    return products, cache, checkpoint, calls, handle, minute, common


def test_1000_hour_boundary_covers_mandatory_pool_in_existing_budget():
    products, cache, checkpoint, calls, handle, _, _ = fixture(1000)
    b, _ = budget()
    for _ in range(3):  # Clock and two metadata requests share the real CLI budget.
        b.admit("/api/v5/public/time")
    report = scan(products, T, cache, checkpoint, b, workers=4, transport=httpx.MockTransport(handle))
    assert report["coverage_contract"] == "a0-mandatory-v2"
    assert report["complete"] == 1000 and report["partial"] == report["deferred"] == 0
    assert b.attempts == 2003 and all(kind != "1H" for kind, _ in calls)
    assert report["hour_context_counts"] == {"not_requested": 1000}
    assert all(point == T for point in checkpoint.tracker.last_at.values())
    assert not checkpoint.tracker.active and not report["transitions"]


def test_optional_hour_starts_only_after_all_mandatory_and_preserves_events():
    products, cache, checkpoint, calls, handle, minute, common = fixture(8, qualified={0, 3})
    previous = checkpoint.at
    expected = deepcopy(checkpoint.tracker)
    changes = []
    for p in products:
        detector = trial.Detector(p, trial.Series.parse(minute[p.key], trial.MINUTE),
                                  trial.Series.parse(common[trial.FIVE], trial.FIVE),
                                  trial.Series.parse(common[trial.HOUR], trial.HOUR), rules=("A0",))
        for at in range(previous + trial.MINUTE, T + trial.MINUTE, trial.MINUTE):
            changes.extend(expected.apply(detector.evaluate(at)[0]))
    b, _ = budget()
    report = scan(products, T, cache, checkpoint, b, workers=8, transport=httpx.MockTransport(handle))
    assert len(calls) == 18 and all(kind != "1H" for kind, _ in calls[:16])
    assert calls[16:] == [("1H", products[i].inst_id) for i in (0, 3)]
    assert report["hour_context_counts"] == {"refreshed": 2, "not_requested": 6}
    assert asdict(checkpoint.tracker) == asdict(expected)
    assert sorted(report["transitions"], key=str) == sorted([asdict(c) for c in changes], key=str)
    assert report["complete"] == 8 and report["publication"] == "disabled"


def test_candidate_hour_quota_never_reduces_mandatory_coverage():
    products, cache, checkpoint, calls, handle, _, _ = fixture(8, qualified=set(range(8)))
    b, _ = budget(max_requests=18)
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert report["complete"] == 8 and report["deferred"] == report["partial"] == 0
    assert report["hour_context_counts"] == {"refreshed": 2, "deferred_budget": 6}
    assert calls[16:] == [("1H", products[i].inst_id) for i in (0, 1)]
    assert all(point == T for point in checkpoint.tracker.last_at.values())
    assert len(report["transitions"]) == 8


def test_cached_current_hour_needs_no_optional_request():
    products, cache, checkpoint, calls, handle, _, _ = fixture(2, qualified={0}, complete_hours=True)
    b, _ = budget()
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert len(calls) == 4 and report["hour_context_counts"] == {"cached": 2}
    assert all(r["hour_context"]["asof"] == T for r in report["products"])
    assert all(count == 1 for count in cache.loads.values())


def test_warm_minute_scan_reuses_complete_five_background_without_changing_events():
    products, cache, checkpoint, calls, handle, minute, common = fixture(8, previous=T-trial.MINUTE)
    expected = deepcopy(checkpoint.tracker)
    for p in products:
        series = dict(cache.snapshots[p.key].series)
        series[trial.FIVE] = trial.Series.parse(common[trial.FIVE], trial.FIVE)
        cache.snapshots[p.key] = data.Snapshot(p, T-trial.MINUTE, series)
        detector = trial.Detector(p, trial.Series.parse(minute[p.key], trial.MINUTE),
                                  series[trial.FIVE], series[trial.HOUR], rules=('A0',))
        expected.apply(detector.evaluate(T)[0])
    b, _ = budget(max_requests=8)
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert report['complete'] == 8 and report['partial'] == report['deferred'] == 0
    assert len(calls) == 8 and all(kind == '1m' for kind, _ in calls)
    assert asdict(checkpoint.tracker) == asdict(expected)


def test_slow_connection_does_not_strand_a_fixed_worker_group():
    products, cache, checkpoint, calls, handle, _, _ = fixture(8)
    entered, release, others_done = threading.Event(), threading.Event(), threading.Event()
    def blocking(request):
        if request.url.params['instId'] == products[0].inst_id:
            entered.set()
            assert release.wait(5)
        result = handle(request)
        with cache.lock:
            if len({symbol for kind, symbol in calls if kind == '5m' and symbol != products[0].inst_id}) == 7:
                others_done.set()
        return result
    b, _ = budget()
    with ThreadPoolExecutor(max_workers=1) as runner:
        future = runner.submit(scan, products, T, cache, checkpoint, b,
                               transport=httpx.MockTransport(blocking))
        try:
            assert entered.wait(2) and others_done.wait(2)
        finally:
            release.set()
        report = future.result(timeout=5)
    assert report['complete'] == 8 and report['partial'] == report['deferred'] == 0


@pytest.mark.parametrize("age,candidate", [(30, True), (31, False)])
def test_expired_event_does_not_consume_hour_budget(age, candidate):
    products, cache, checkpoint, calls, handle, _, _ = fixture(1, previous=T - trial.MINUTE)
    p = products[0]
    checkpoint.tracker.active[("A0", p.key)] = trial.Event("A0", p.key, "UP", T - trial.HOUR,
                                                         T - age * trial.MINUTE, stage="UNKNOWN")
    b, _ = budget()
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert report["products"][0]["context_candidate"] is candidate
    assert len(calls) == (3 if candidate else 2)
    assert report["transitions"] == []


def test_persistent_event_has_optional_priority_before_newer_observation():
    products, cache, checkpoint, calls, handle, _, _ = fixture(2, previous=T - trial.MINUTE)
    for i, p in enumerate(products):
        checkpoint.tracker.active[("A0", p.key)] = trial.Event(
            "A0", p.key, "UP", T - trial.HOUR, T - (2 if i else 1) * trial.MINUTE,
            stage="PERSISTENT" if i else "OBSERVED", persistent_announced=bool(i))
    b, _ = budget(max_requests=5)
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert calls[-1] == ("1H", products[1].inst_id)
    assert report["products"][0]["hour_context"]["status"] == "deferred_budget"
    assert report["complete"] == 2


@pytest.mark.parametrize("failure", ["http", "empty", "conflict", "cache_miss", "save", "deadline"])
def test_optional_failures_do_not_change_mandatory_state_or_claim_context(failure):
    products, cache, checkpoint, calls, handle, _, _ = fixture(1, qualified={0})
    b, clock = budget()
    def failing(request):
        if request.url.params["bar"] == "1H":
            if failure == "http":
                return httpx.Response(403)
            if failure == "empty":
                return response([])
            if failure == "conflict":
                # Changing an immutable overlapping candle creates sticky error.
                return response([row(T - trial.HOUR), row(T - 2 * trial.HOUR, quote=999)])
            if failure == "deadline":
                clock.now = 420
        return handle(request)
    cache.fail_load = failure == "cache_miss"
    cache.fail_save = 2 if failure == "save" else None
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(failing))
    assert report["complete"] == 1 and report["partial"] == report["deferred"] == 0
    assert checkpoint.tracker.last_at[("A0", products[0].key)] == T
    assert len(report["transitions"]) == 1
    context = report["products"][0]["hour_context"]
    expected_status = "deferred_cache" if failure == "cache_miss" else "deferred_budget" if failure == "deadline" else "unavailable"
    assert context["status"] == expected_status and context["asof"] is None


def test_mandatory_cache_failure_is_partial_and_cannot_start_optional():
    products, cache, checkpoint, calls, handle, _, _ = fixture(1, qualified={0})
    cache.fail_save = 1
    b, _ = budget()
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert report["partial"] == 1 and report["complete"] == 0
    assert len(calls) == 2 and report["hour_context_counts"] == {"deferred_cache": 1}
    assert checkpoint.tracker.last_at[("A0", products[0].key)] == T
