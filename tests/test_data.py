"""Acquisition budgets, incremental scanning and durable state; fixture HTTP only."""
import gzip
import json
from dataclasses import replace
from types import MappingProxyType

import httpx
import pytest
from test_trial import PRODUCT, T, detector, row, snapshot

from crypto_boom import data as data
from crypto_boom import state as state
from crypto_boom import trial as trial
from crypto_boom.scan import scan


class Clock:
    def __init__(self):
        self.now = 0.
    def clock(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


def budget(**kwargs):
    clock = Clock()
    return data.Budget(clock=clock.clock, sleep=clock.sleep, **kwargs), clock


def empty(grain):
    return trial.Series(grain, MappingProxyType({}), MappingProxyType({}))


def response(rows):
    return httpx.Response(200, json={"code": "0", "data": rows})


@pytest.mark.parametrize("phase,change", [(phase, .005) for phase in range(5)] +
                         [(0, -.005), (0, .00499), (0, -.00499), (0, .01)])
def test_a0_selection_preserves_condition_features_and_events(phase, change):
    at = T + phase * trial.MINUTE
    rows = snapshot(at=at, change=change, hour=True)
    reference = detector(*rows)
    single = trial.Detector(PRODUCT, reference.minute, reference.five, reference.hourly, rules=("A0",))
    a, b = reference.evaluate(at)[0], single.evaluate(at)[0]
    assert (a.condition, a.direction, a.quote, a.return_5m, a.ratio, a.ema20) == (b.condition, b.direction, b.quote, b.return_5m, b.ratio, b.ema20)
    assert b.mad_score is None and b.price_score is None
    assert "mad_not_computed" in b.reasons and "price_scale_not_computed" in b.reasons
    assert trial.EventTracker().apply(a) == trial.EventTracker().apply(b)


@pytest.mark.parametrize("rules", [(), ("A0", "A0"), ("bad",)])
def test_invalid_rule_selection(rules):
    d = detector(*snapshot())
    with pytest.raises(ValueError):
        trial.Detector(PRODUCT, d.minute, d.five, d.hourly, rules=rules)


def test_hour_cache_is_boundary_bound_not_stale():
    minute, five, hours = snapshot(hour=True)
    future_minute, future_five, _ = snapshot(at=T + trial.HOUR)
    d = detector(minute + future_minute, five + future_five, hours)
    d.evaluate(T)
    d.evaluate(T + trial.MINUTE)
    assert len(d.hours) == 1
    d.evaluate(T + trial.HOUR)
    assert len(d.hours) == 2
    assert d.evaluate(T + trial.HOUR)[0].ema20 is None


def test_rate_and_global_retry_bound():
    b, clock = budget()
    for _ in range(16):
        b.admit("/api/v5/market/candles")
    assert clock.now == 15 / 8
    b.admit("/api/v5/market/history-candles")
    assert clock.now == 2.
    for _ in range(3):
        b.admit("/api/v5/market/history-candles", retry=True)
    with pytest.raises(data.BudgetExceeded, match="retry_budget"):
        b.admit("/api/v5/market/candles", retry=True)
    assert b.retries <= b.initial // 5


def test_budget_refuses_after_deadline_or_count():
    b, clock = budget(max_requests=1)
    b.admit("/api/v5/public/time")
    with pytest.raises(data.BudgetExceeded, match="request_budget"):
        b.admit("/api/v5/public/time")
    b, clock = budget(seconds=2, reserve=1)
    clock.now = 1
    with pytest.raises(data.BudgetExceeded, match="deadline"):
        b.admit("/api/v5/public/time")


@pytest.mark.parametrize("status", [301, 403, 451])
def test_no_redirect_auth_or_region_retry(status):
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: httpx.Response(status))) as client:
        with pytest.raises(trial.Unavailable, match="http"):
            client.get("/api/v5/public/time")
    assert b.attempts == 1 and client.client.is_closed


def test_allowlist_rejects_account_before_effect():
    b, _ = budget()
    with data.PublicClient(b) as client:
        with pytest.raises(ValueError):
            client.get("/api/v5/account/balance")
    assert b.attempts == 0


def test_429_shared_backoff_and_transport_retry():
    b, clock = budget()
    attempts = []
    def handle(request):
        attempts.append(clock.now)
        if len(attempts) == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        if len(attempts) == 2:
            raise httpx.ReadError("fixture")
        return response([])
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        assert client.get("/api/v5/market/candles") == []
    assert attempts[1] - attempts[0] >= 3 and attempts[2] - attempts[1] >= 2
    assert b.retries == 2 and len(b.observations) == 3


def test_startup_transport_retry_is_local_and_remains_bounded():
    b, clock = budget()
    calls = []
    def handle(request):
        calls.append(clock.now)
        raise httpx.ConnectError('fixture offline', request=request)
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(trial.Unavailable, match='transport:ConnectError'):
            client.get('/api/v5/public/time')
    assert len(calls) == 3 and b.retries == 2 and b.attempts == 3
    assert b.backoff_until == b.started  # Connection failure must not pause other workers.
    with pytest.raises(data.BudgetExceeded, match='retry_budget'):
        b.admit('/api/v5/public/time', retry=True)


@pytest.mark.parametrize("kwargs,reason", [({"max_response": 10}, "response_budget"), ({"max_bytes": 10}, "byte_budget")])
def test_response_and_total_byte_caps(kwargs, reason):
    b, _ = budget(**kwargs)
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: response(["a" * 100]))) as client:
        with pytest.raises(data.BudgetExceeded, match=reason):
            client.get("/api/v5/public/time")
    assert client.client.is_closed


def test_native_pagination_and_missing_confirm():
    at = 2000 * trial.MINUTE
    rows = [row(at - i * trial.MINUTE, confirm="0" if i == 1 else "1") for i in range(1, 1502)]
    requests = []
    def handle(request):
        requests.append(str(request.url))
        end, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        return response([r for r in rows if int(r[0]) < end][:limit])
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        got = data.fetch_range(client, PRODUCT, empty(trial.MINUTE), at - 1500 * trial.MINUTE, at)
    assert len(got.bars) == 1445 and at - trial.MINUTE not in got.bars
    assert len(requests) == 5 and all("history-candles" in u for u in requests)


def test_stalled_pagination_fails_not_infinite():
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: response([row(T)]))) as client:
        with pytest.raises(trial.Unavailable, match="pagination"):
            data.fetch_range(client, PRODUCT, empty(trial.MINUTE), T - trial.MINUTE, T)
    assert b.attempts == 1


def test_overlap_conflict_survives_cache_and_good_refresh(tmp_path):
    original = trial.Series.parse([row(T - trial.MINUTE)], trial.MINUTE)
    merged = data.merge_series(original, [row(T - trial.MINUTE, quote=999)], T)
    assert merged.errors[T - trial.MINUTE] == "conflicting_complete_candle"
    cache = data.ProductCache(tmp_path)
    series = {g: empty(g) for g in data.GRAINS.values()}
    series[trial.MINUTE] = merged
    snap = data.Snapshot(PRODUCT, T, series)
    assert cache.save(snap) is True
    assert cache.save(snap) is False
    restored, reason = cache.load(PRODUCT, T)
    assert reason == "cache_hit"
    assert data.merge_series(restored.series[trial.MINUTE], [row(T - trial.MINUTE)], T).errors == merged.errors
    assert cache.load(replace(PRODUCT, normal_since=2), T)[0] is None
    assert cache.load(PRODUCT, T - trial.MINUTE)[0] is None


@pytest.mark.parametrize("contents", [b"not gzip", gzip.compress(b"{}"), gzip.compress(b"x" * (2 * 1024 * 1024 + 1))])
def test_cache_failure_is_explicit_miss(tmp_path, contents):
    cache = data.ProductCache(tmp_path)
    cache.path(PRODUCT).write_bytes(contents)
    restored, reason = cache.load(PRODUCT, T)
    assert restored is None and reason != "cache_hit"


def test_cache_atomic_failure_keeps_previous_and_no_tmp(tmp_path, monkeypatch):
    cache = data.ProductCache(tmp_path)
    snap = data.Snapshot(PRODUCT, T, {g: empty(g) for g in data.GRAINS.values()})
    cache.save(snap)
    before = cache.path(PRODUCT).read_bytes()
    monkeypatch.setattr(data.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("fixture replace")))
    with pytest.raises(OSError):
        cache.save(replace(snap, at=T + trial.MINUTE))
    assert cache.path(PRODUCT).read_bytes() == before
    assert not list(tmp_path.glob("*.tmp"))


def event_state():
    checkpoint = state.Checkpoint(at=T)
    changes = checkpoint.tracker.apply(trial.Evaluation("A0", PRODUCT.key, T, True, "UP"))
    return checkpoint, changes


def test_missing_state_does_not_authorize_notifications(tmp_path):
    store = state.StateStore(tmp_path / "state.json")
    assert store.load()[0] is None
    checkpoint, changes = event_state()
    with pytest.raises(trial.Unavailable, match="baseline"):
        checkpoint.prepare(list(changes), T)
    with store.writer():
        store.save(checkpoint, None)
    restored, reason = store.load()
    assert reason == "state_hit" and restored.generation == 1
    assert restored.tracker.active == checkpoint.tracker.active
    assert restored.notifications_enabled is False


def test_writer_lock_fence_and_unknown_receipt(tmp_path):
    store = state.StateStore(tmp_path / "state.json")
    checkpoint, changes = event_state()
    with store.writer():
        with pytest.raises(trial.Unavailable, match="lock_present"):
            with state.StateStore(store.path).writer():
                pass
        store.save(checkpoint, None)
        checkpoint.notifications_enabled = True  # Explicit fixture, no publisher.
        ids = checkpoint.prepare(list(changes), T)
        checkpoint.unknown(ids)
        store.save(checkpoint, 1)
    restored, _ = store.load()
    with pytest.raises(trial.Unavailable, match="unresolved"):
        restored.prepare(list(changes), T)
    with pytest.raises(ValueError, match="mismatch"):
        restored.reconcile(ids, "bad receipt")
    restored.reconcile(ids, restored.ledger[ids[0]]["batch"])
    assert restored.prepare(list(changes), T) == ()
    with store.writer():
        with pytest.raises(trial.Unavailable, match="generation"):
            store.save(checkpoint, 1)
        store.save(restored, 2)
    assert store.load()[0].generation == 3 and not store.lock_path.exists()


def test_state_atomic_failure_and_corrupt_fail_closed(tmp_path, monkeypatch):
    store = state.StateStore(tmp_path / "state.json")
    checkpoint, _ = event_state()
    with store.writer():
        store.save(checkpoint, None)
        before = store.path.read_bytes()
        monkeypatch.setattr(data.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("state upload fixture")))
        with pytest.raises(OSError):
            store.save(checkpoint, 1)
        assert checkpoint.generation == 1 and store.path.read_bytes() == before
    store.path.write_text("{}")
    assert store.load()[0] is None
    assert "blocked" in store.load()[1]


def test_confirmed_receipt_recovers_after_checkpoint_write_failure(tmp_path):
    checkpoint, changes = event_state()
    checkpoint.notifications_enabled = True
    ids = checkpoint.prepare(list(changes), T)
    saved_pending = json.loads(json.dumps(checkpoint.encode()))
    # Simulated publication succeeds, but new state is lost; receipt remains.
    checkpoint.reconcile(ids, checkpoint.ledger[ids[0]]["batch"])
    recovered = state.Checkpoint.decode(json.loads(json.dumps(saved_pending)))
    recovered.reconcile(ids, recovered.ledger[ids[0]]["batch"])
    assert recovered.prepare(list(changes), T) == ()


def test_scan_cold_then_incremental_and_no_publish(tmp_path):
    calls = []
    def handle(request):
        grain = {"1m": trial.MINUTE, "5m": trial.FIVE, "1H": trial.HOUR}[request.url.params["bar"]]
        cursor, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        calls.append((grain, limit))
        upper = (cursor - 1) // grain * grain
        return response([row(upper - i * grain) for i in range(limit)])
    cache, checkpoint = data.ProductCache(tmp_path / "market"), state.Checkpoint()
    b, _ = budget()
    first = scan([PRODUCT], T, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert first["complete"] == 1 and first["publication"] == "disabled"
    assert checkpoint.notifications_enabled is False
    b, _ = budget()
    calls.clear()
    second = scan([PRODUCT], T + 10 * trial.MINUTE, cache, checkpoint, b, transport=httpx.MockTransport(handle))
    assert second["complete"] == 1 and second["products"][0]["evaluated"] == 10
    assert b.attempts == 2 and max(limit for _, limit in calls) <= 16


def test_scan_budget_failure_is_partial_not_negative_or_checkpoint_advance(tmp_path):
    checkpoint = state.Checkpoint(at=T - 10 * trial.MINUTE)
    checkpoint.tracker.last_at[("A0", PRODUCT.key)] = checkpoint.at
    b, _ = budget(max_requests=1)
    result = scan([PRODUCT], T, data.ProductCache(tmp_path), checkpoint, b,
                  transport=httpx.MockTransport(lambda r: response([])))
    assert result["complete"] == 0 and result["partial"] == 1
    assert checkpoint.tracker.last_at[("A0", PRODUCT.key)] == T - 10 * trial.MINUTE
    assert result["transitions"] == []


def test_mandatory_warm_before_cold_before_optional_hour(tmp_path):
    cache = data.ProductCache(tmp_path / "market")
    checkpoint = state.Checkpoint(at=T)
    checkpoint.tracker.last_at[("A0", PRODUCT.key)] = T
    minute, five, hours = snapshot(hour=True)
    cache.save(data.Snapshot(PRODUCT, T, {trial.MINUTE: trial.Series.parse(minute,trial.MINUTE),
               trial.FIVE: trial.Series.parse(five,trial.FIVE),trial.HOUR:trial.Series.parse(hours,trial.HOUR)}))
    new = replace(PRODUCT, inst_id="NEW-USDT", base="NEW")
    known={g:{int(r[0]):r for r in rows} for g,rows in [(trial.MINUTE,minute),(trial.FIVE,five),(trial.HOUR,hours)]}
    calls=[]
    def handle(request):
        calls.append(request.url.params["instId"])
        grain={"1m":trial.MINUTE,"5m":trial.FIVE,"1H":trial.HOUR}[request.url.params["bar"]]
        end=int(request.url.params["after"])
        limit=int(request.url.params["limit"])
        return response([known[grain].get(ts,row(ts)) for ts in [(end-1)//grain*grain-i*grain for i in range(limit)]])
    b,_=budget(max_requests=3)
    result=scan([new,PRODUCT],T+trial.HOUR,cache,checkpoint,b,transport=httpx.MockTransport(handle))
    assert calls == [PRODUCT.inst_id]*2 + [new.inst_id]
    assert result["complete"] == 1 and result["partial"] == 1
    assert result["hour_context_counts"] == {"not_requested": 2}


def test_corrupt_partition_does_not_block_warm_product(tmp_path):
    cache = data.ProductCache(tmp_path / "market")
    checkpoint = state.Checkpoint(at=T)
    checkpoint.tracker.last_at[("A0", PRODUCT.key)] = T
    minute,five,hours=snapshot(hour=True)
    cache.save(data.Snapshot(PRODUCT,T,{trial.MINUTE:trial.Series.parse(minute,trial.MINUTE),
               trial.FIVE:trial.Series.parse(five,trial.FIVE),trial.HOUR:trial.Series.parse(hours,trial.HOUR)}))
    bad=replace(PRODUCT,inst_id="AAA-USDT",base="AAA")
    cache.path(bad).write_bytes(b"corrupt")
    known={g:{int(r[0]):r for r in rows} for g,rows in [(trial.MINUTE,minute),(trial.FIVE,five),(trial.HOUR,hours)]}
    calls=[]
    def handle(request):
        calls.append(request.url.params["instId"])
        grain={"1m":trial.MINUTE,"5m":trial.FIVE,"1H":trial.HOUR}[request.url.params["bar"]]
        end=int(request.url.params["after"])
        limit=int(request.url.params["limit"])
        return response([known[grain].get(ts,row(ts)) for ts in [(end-1)//grain*grain-i*grain for i in range(limit)]])
    b,_=budget(max_requests=2)
    result=scan([bad,PRODUCT],T+10*trial.MINUTE,cache,checkpoint,b,transport=httpx.MockTransport(handle))
    assert calls == [PRODUCT.inst_id]*2
    assert result["complete"] == 1 and result["deferred"] == 1


@pytest.mark.parametrize("field,value", [("false_count",True),("first_at",T+1),("direction","sideways"),("persistent_announced",1)])
def test_state_invalid_event_is_not_restored(field,value):
    checkpoint,_=event_state()
    body=checkpoint.encode()
    body['active'][0][field]=value
    with pytest.raises(ValueError):
        state.Checkpoint.decode(body)


def test_old_confirmed_expiry_and_unresolved_retention():
    checkpoint,changes=event_state()
    checkpoint.notifications_enabled=True
    ids=checkpoint.prepare(list(changes),T)
    checkpoint.unknown(ids)
    with pytest.raises(trial.Unavailable,match="unresolved"):
        checkpoint.prepare([],T+72*trial.HOUR)
    assert ids[0] in checkpoint.ledger
    checkpoint.reconcile(ids,checkpoint.ledger[ids[0]]['batch'])
    assert checkpoint.prepare([],T+72*trial.HOUR)==()
    assert not checkpoint.ledger


def test_partial_receipt_is_rejected():
    checkpoint,changes=event_state()
    checkpoint.notifications_enabled=True
    two=[*changes,trial.Transition(changes[0].identity,'PERSISTENT',T)]
    ids=checkpoint.prepare(two,T)
    with pytest.raises(ValueError):
        checkpoint.reconcile(ids[:1],checkpoint.ledger[ids[0]]['batch'])
    assert all(item['status']=='pending' for item in checkpoint.ledger.values())


def test_unrequested_compression_is_rejected_before_decompression():
    b,_=budget()
    compressed=gzip.compress(b'x'*(4*1024*1024+1))
    with data.PublicClient(b,transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'Content-Encoding':'gzip'},stream=httpx.ByteStream(compressed)))) as client:
        with pytest.raises(trial.Unavailable,match='encoding'):
            client.get('/api/v5/public/time')
    assert b.received == 0 and b.attempts == 1


def test_state_oversize_and_generation_type_fail_closed(tmp_path):
    store=state.StateStore(tmp_path/'state.json')
    store.path.write_bytes(b' '*(8*1024*1024+1))
    assert store.load()[0] is None and store.load()[1]=='state_oversized'
    with store.writer():
        with pytest.raises(ValueError,match='generation'):
            store.save(state.Checkpoint(),True)


def test_shadow_cli_refuses_notification_enabled_workspace_before_http(tmp_path,monkeypatch):
    from crypto_boom import scan
    store=state.StateStore(tmp_path/'checkpoint.json')
    checkpoint=state.Checkpoint()
    with store.writer():
        store.save(checkpoint,None)
        checkpoint.notifications_enabled=True
        store.save(checkpoint,1)
    monkeypatch.setattr('sys.argv',['scan','--workspace',str(tmp_path)])
    def forbidden(*args,**kwargs):
        raise AssertionError('No HTTP permitted after shadow workspace rejection')
    monkeypatch.setattr(scan,'PublicClient',forbidden)
    with pytest.raises(trial.Unavailable,match='shadow_workspace_required'):
        scan.main()
    assert store.load()[0].generation==2


def test_insufficient_normal_history_is_warmup_without_download(tmp_path):
    new=replace(PRODUCT,normal_since=T-trial.WEEK+1)
    b,_=budget()
    def forbidden(request):
        raise AssertionError('Metadata proves warmup: history HTTP must not run')
    checkpoint=state.Checkpoint()
    result=scan([new],T,data.ProductCache(tmp_path),checkpoint,b,transport=httpx.MockTransport(forbidden))
    assert b.attempts==0 and result['warmup']==1 and result['complete']==0
    assert result['transitions']==[]
    ready=result['products'][0]['ready_at']
    assert ready % trial.FIVE==0
    assert ((ready-trial.FIVE)//trial.FIVE)*trial.FIVE-trial.WEEK-trial.FIVE>=new.normal_since
    assert ((ready-trial.MINUTE-trial.FIVE)//trial.FIVE)*trial.FIVE-trial.WEEK-trial.FIVE<new.normal_since
    assert checkpoint.tracker.last_at[('A0',new.key)]==T


def test_same_cutoff_checkpoint_is_not_new_complete_coverage(tmp_path):
    checkpoint=state.Checkpoint(at=T)
    checkpoint.tracker.last_at[('A0',PRODUCT.key)]=T
    b,_=budget()
    def forbidden(request):
        raise AssertionError('Already evaluated boundary must not refetch')
    result=scan([PRODUCT],T,data.ProductCache(tmp_path),checkpoint,b,transport=httpx.MockTransport(forbidden))
    assert result['already_evaluated']==1 and result['complete']==0
    assert b.attempts==0 and result['transitions']==[]
