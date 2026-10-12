"""Interrupted native acquisition retains history, never evaluation authority."""
from dataclasses import asdict

import httpx
import pytest
from test_stage3 import budget, response
from test_trial import PRODUCT, T, row

from crypto_boom import data as data
from crypto_boom import state as state
from crypto_boom import trial as trial
from crypto_boom.scan import scan


def native_source(at):
    rows = {g: [row(ts) for ts in range((at // g) * g - data.RETENTION[g],
                                       (at // g) * g, g)]
            for g in data.GRAINS.values()}
    calls = []
    def handle(request):
        g = {"1m": trial.MINUTE, "5m": trial.FIVE, "1H": trial.HOUR}[request.url.params["bar"]]
        end, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        calls.append((g, end, limit))
        return response([r for r in reversed(rows[g]) if int(r[0]) < end][:limit])
    return rows, calls, handle


def test_interrupted_cold_cache_survives_without_advancing_checkpoint(tmp_path):
    cache = data.ProductCache(tmp_path / "cache")
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    key = ("A0", PRODUCT.key)
    checkpoint.tracker.last_at[key] = checkpoint.at
    _, _, handle = native_source(T)
    b, _ = budget(max_requests=4)
    report = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    retained, _ = cache.load(PRODUCT, T)
    assert retained is not None
    assert retained.series[trial.MINUTE].bars and retained.series[trial.FIVE].bars
    assert checkpoint.tracker.last_at[key] == T - 10 * trial.MINUTE
    assert report["partial"] == 1 and not report["transitions"]
    assert report["products"][0]["partial_cache_saved"] is True
    assert report["products"][0]["evaluated"] == 0
    assert b.attempts == 4


def test_resume_repairs_only_old_gap_and_matches_uninterrupted(tmp_path):
    cache = data.ProductCache(tmp_path / "cache")
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    checkpoint.tracker.last_at[("A0", PRODUCT.key)] = checkpoint.at
    _, _, handle = native_source(T)
    b, _ = budget(max_requests=4)
    scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    partial, _ = cache.load(PRODUCT, T)
    assert partial is not None
    oldest = min(partial.series[trial.FIVE].bars)
    future = T + 10 * trial.MINUTE
    rows, calls, handle = native_source(future)
    b, _ = budget()
    report = scan([PRODUCT], future, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    expected = trial.EventTracker()
    expected.last_at[("A0", PRODUCT.key)] = T - 10 * trial.MINUTE
    detector = trial.Detector(PRODUCT, *(trial.Series.parse(rows[g], g)
                                        for g in (trial.MINUTE, trial.FIVE, trial.HOUR)), rules=("A0",))
    changes = []
    for at in range(T - 9 * trial.MINUTE, future + trial.MINUTE, trial.MINUTE):
        changes.extend(expected.apply(detector.evaluate(at)[0]))
    assert report["complete"] == 1 and report["partial"] == 0
    assert asdict(checkpoint.tracker) == asdict(expected)
    assert report["transitions"] == [asdict(c) for c in changes]
    assert b.attempts <= 6  # Two incremental requests plus remaining old pages.
    repairs = [end for g, end, _ in calls if g == trial.FIVE and end < T]
    assert repairs and max(repairs) == oldest
    full, _ = cache.load(PRODUCT, future)
    assert max(full.series[trial.MINUTE].bars) == future - trial.MINUTE


@pytest.mark.parametrize("failure", ["budget", "http", "pagination"])
def test_partial_fetch_preserves_primary_failure_and_rejects_bad_page(failure):
    start = T - 700 * trial.MINUTE
    rows = [row(ts) for ts in range(start, T, trial.MINUTE)]
    calls, retained = [], []
    def handle(request):
        calls.append(request)
        end = int(request.url.params["after"])
        if len(calls) == 2:
            if failure == "http":
                return httpx.Response(403)
            if failure == "pagination":
                return response([row(end)])
        return response([r for r in reversed(rows) if int(r[0]) < end][:300])
    b, _ = budget(max_requests=1 if failure == "budget" else 10)
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(data.BudgetExceeded if failure == "budget" else trial.Unavailable,
                           match={"budget": "request_budget", "http": "http:403",
                                  "pagination": "pagination_not_backward"}[failure]):
            data.fetch_range(client, PRODUCT, trial.Series.parse([], trial.MINUTE), start, T,
                             retain_partial=retained.append)
    assert len(retained) == 1 and len(retained[0].bars) == 300
    assert max(retained[0].bars) == T - trial.MINUTE


def test_cache_save_failure_does_not_hide_acquisition_failure(tmp_path, monkeypatch):
    cache = data.ProductCache(tmp_path / "cache")
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    checkpoint.tracker.last_at[("A0", PRODUCT.key)] = checkpoint.at
    _, _, handle = native_source(T)
    def fail_save(_):
        raise OSError("disk_full_fixture")
    monkeypatch.setattr(cache, "save", fail_save)
    b, _ = budget(max_requests=4)
    report = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    product = report["products"][0]
    assert product["error"] == "request_budget"
    assert product["cache_error"] == "disk_full_fixture"
    assert product["partial_cache_saved"] is False
    assert checkpoint.tracker.last_at[("A0", PRODUCT.key)] == T - 10 * trial.MINUTE


def test_tail_refresh_survives_budget_failure_before_any_repair_page(tmp_path):
    cache = data.ProductCache(tmp_path / "cache")
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    key = ("A0", PRODUCT.key)
    checkpoint.tracker.last_at[key] = checkpoint.at
    _, _, handle = native_source(T)
    b, _ = budget(max_requests=4)
    scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    future = T + 10 * trial.MINUTE
    _, _, handle = native_source(future)
    b, _ = budget(max_requests=2)
    report = scan([PRODUCT], future, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    retained, _ = cache.load(PRODUCT, future)
    assert max(retained.series[trial.FIVE].bars) == (future // trial.FIVE) * trial.FIVE - trial.FIVE
    assert report["products"][0]["partial_cache_saved"] is True
    assert checkpoint.tracker.last_at[key] == T - 10 * trial.MINUTE
    assert not report["transitions"]


def test_repair_end_keeps_latest_tail_and_uses_real_cutoff_for_history_endpoint():
    at = T
    grain = trial.FIVE
    start, end = at - trial.WEEK, at - trial.WEEK + 2 * grain
    old = trial.Series.parse([row(at - grain)], grain)
    paths = []
    def handle(request):
        paths.append(request.url.path)
        assert int(request.url.params["after"]) == end
        return response([row(end - grain), row(start)])
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        merged = data.fetch_range(client, PRODUCT, old, start, at, end=end)
    assert paths == ["/api/v5/market/history-candles"]
    assert set(merged.bars) == {start, end - grain, at - grain}


def test_optional_interrupted_hour_cache_does_not_change_a0_checkpoint():
    from test_acquisition import fixture
    products, cache, checkpoint, _, handle, _, _ = fixture(1, qualified={0})
    product = products[0]
    saved = cache.snapshots[product.key]
    series = dict(saved.series)
    series[trial.HOUR] = trial.Series.parse(data.rows_of(series[trial.HOUR])[-84:], trial.HOUR)
    cache.snapshots[product.key] = data.Snapshot(product, saved.at, series)
    b, _ = budget(max_requests=3)
    report = scan(products, T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    context = report["products"][0]["hour_context"]
    assert report["complete"] == 1 and report["partial"] == 0
    assert context["status"] == "deferred_budget" and context["asof"] is None
    assert context["partial_cache_saved"] is True
    assert checkpoint.tracker.last_at[("A0", product.key)] == T
    assert len(report["transitions"]) == 1
    assert max(cache.snapshots[product.key].series[trial.HOUR].bars) == T - trial.HOUR


def test_empty_source_and_existing_conflicts_are_not_invented_or_overwritten(tmp_path):
    from crypto_boom.scan import _refresh_series
    old = trial.Series.parse([row(T - trial.FIVE, 100), row(T - trial.FIVE, 101)], trial.FIVE)
    b, _ = budget(max_requests=4)
    with data.PublicClient(b, transport=httpx.MockTransport(lambda _: response([]))) as client:
        result = _refresh_series(client, PRODUCT, old, T - 3 * trial.FIVE, T)
    assert not result.bars and result.errors[T - trial.FIVE] == "conflicting_complete_candle"
    assert b.attempts <= 2


def test_only_unconfirmed_interrupted_pages_are_not_download_progress(tmp_path):
    retained = []
    def handle(request):
        end = int(request.url.params["after"])
        count = int(request.url.params["limit"])
        return response([row(end - (i+1) * trial.MINUTE, confirm="0") for i in range(count)])
    b, _ = budget(max_requests=1)
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(data.BudgetExceeded, match="request_budget"):
            data.fetch_range(client, PRODUCT, trial.Series.parse([], trial.MINUTE), T - 700 * trial.MINUTE, T,
                             retain_partial=retained.append)
    assert not retained
    cache = data.ProductCache(tmp_path / "cache")
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    key = ("A0", PRODUCT.key)
    checkpoint.tracker.last_at[key] = checkpoint.at
    b, _ = budget(max_requests=1)
    report = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert not cache.path(PRODUCT).exists()
    assert report["products"][0]["partial_cache_saved"] is False
    assert checkpoint.tracker.last_at[key] == T - 10 * trial.MINUTE
