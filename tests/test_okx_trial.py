"""Synthetic contract falsification; no HTTP, credentials, or publication."""
from dataclasses import replace

import pytest

from crypto_boom import okx_trial as trial

T = 20 * trial.WEEK + 2 * trial.HOUR
PRODUCT = trial.Product("SPOT", "ALT-USDT", "ALT", 1)


def row(ts, close=100., quote=25_000., *, confirm="1"):
    return [str(ts), str(close), str(close), str(close), str(close), "1", "1", str(quote), confirm]


def snapshot(*, at=T, quote=150_000., change=.01, constant_q=False, constant_price=False, hour=False):
    end = ((at - trial.FIVE) // trial.FIVE) * trial.FIVE
    five = [row(end - trial.WEEK - trial.FIVE + i * trial.FIVE,
                100. if constant_price or i % 2 == 0 else 100.1,
                25_000. if constant_q else (20_000. if i % 2 == 0 else 30_000.)) for i in range(2017)]
    minute = [row(at - 6 * trial.MINUTE + i * trial.MINUTE, 100. if i == 0 else 100. * (1 + change), quote / 5) for i in range(6)]
    hours = [row((at // trial.HOUR) * trial.HOUR - 168 * trial.HOUR + i * trial.HOUR, 100. + i) for i in range(168)] if hour else []
    return minute, five, hours


def detector(minute, five, hours, product=PRODUCT):
    return trial.Detector(product, trial.Series.parse(minute, trial.MINUTE), trial.Series.parse(five, trial.FIVE), trial.Series.parse(hours, trial.HOUR))


def metadata(kind="SPOT"):
    value = dict(instType=kind, instId="ALT-USDT", baseCcy="ALT", quoteCcy="USDT", state="live", ruleType="normal", instCategory="1", listTime="1", contTdSwTime="", preMktSwTime="")
    if kind == "SWAP":
        value.update(instId="ALT-USDT-SWAP", baseCcy="", quoteCcy="", instFamily="ALT-USDT", ctType="linear", settleCcy="USDT", ctValCcy="ALT", ctVal=".1")
    return value


@pytest.mark.parametrize("kind", ["SPOT", "SWAP"])
def test_product_identity_and_derivative_units(kind):
    product = trial.admit_product(metadata(kind), T)
    assert product.key == ("okx", kind, "ALT-USDT" + ("-SWAP" if kind == "SWAP" else ""))
    assert product.base == "ALT"


@pytest.mark.parametrize("field,value", [("instCategory", ""), ("instCategory", "3"), ("state", "suspend"), ("ruleType", "pre_market"), ("quoteCcy", "USD"), ("listTime", ""), ("listTime", "0"), ("listTime", str(T + 1)), ("preMktSwTime", str(T + 1)), ("instId", "other")])
def test_metadata_does_not_guess_eligibility(field, value):
    raw = metadata()
    raw[field] = value
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)


def test_old_category_and_group_id_do_not_supply_asset_class():
    raw = metadata()
    raw.update(instCategory="", category="1", groupId="1")
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)


@pytest.mark.parametrize("field,value", [("ctType", "inverse"), ("settleCcy", "USD"), ("ctVal", "0"), ("ctValCcy", "OTHER")])
def test_swap_contract_metadata(field, value):
    raw = metadata("SWAP")
    raw[field] = value
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)


@pytest.mark.parametrize("offset,expected", [(0, T - trial.MINUTE), (1999, T - trial.MINUTE), (2000, T), (59_999, T)])
def test_source_time_buffer(offset, expected):
    assert trial.source_cutoff(T + offset) == expected


@pytest.mark.parametrize("phase", range(5))
def test_phase_endpoints_and_six_close_origin(phase):
    at = T + phase * trial.MINUTE
    rows = snapshot(at=at)
    result = detector(*rows).evaluate(at)[0]
    assert result.condition is True
    assert result.quote == 150_000
    assert result.return_5m == pytest.approx(.01)
    assert result.ratio == pytest.approx(6)
    assert result.mad_score == pytest.approx(.67448975 * 25)


@pytest.mark.parametrize("change,direction", [(.01, "UP"), (-.01, "DOWN")])
def test_four_candidates_keep_return_sign(change, direction):
    result = detector(*snapshot(change=change)).evaluate(T)
    assert [r.condition for r in result] == [True] * 4
    assert [r.direction for r in result] == [direction] * 4
    assert all(r.price_score > 3 for r in result)


def test_low_absolute_turnover_and_volume_only_are_not_alerts():
    minute, five, hours = snapshot(quote=150.)
    for r in five:
        r[7] = "10" if float(r[7]) == 20_000 else "20"
    result = detector(minute, five, hours).evaluate(T)
    assert result[0].ratio == pytest.approx(10)
    assert [r.condition for r in result] == [False] * 4
    assert [r.condition for r in detector(*snapshot(change=0)).evaluate(T)] == [False] * 4


def test_zero_scales_do_not_invent_scores_or_fallback():
    result = detector(*snapshot(constant_price=True)).evaluate(T)
    assert [r.condition for r in result] == [True, True, None, None]
    result = detector(*snapshot(constant_q=True)).evaluate(T)
    assert [r.condition for r in result] == [True, None, True, None]
    assert result[1].mad_score is None
    minute, five, hours = snapshot()
    for r in five:
        r[7] = "0"
    assert [r.condition for r in detector(minute, five, hours).evaluate(T)] == [None] * 4


def test_known_false_short_circuits_unknown_scale():
    result = detector(*snapshot(change=0, constant_q=True, constant_price=True)).evaluate(T)
    assert [r.condition for r in result] == [False] * 4
    assert result[3].price_score is None
    assert "price_scale_unavailable" in result[3].reasons


@pytest.mark.parametrize("bad", ["NaN", "inf", "-1", "not-number"])
def test_bad_quote_is_unavailable_not_false(bad):
    minute, five, hours = snapshot()
    minute[-1][7] = bad
    result = detector(minute, five, hours).evaluate(T)
    assert [r.condition for r in result] == [None] * 4


def test_numeric_overflow_does_not_escape_or_emit_signal():
    minute, five, hours = snapshot()
    for r in minute:
        r[7] = "1e308"
    assert [r.condition for r in detector(minute, five, hours).evaluate(T)] == [None] * 4


def test_unordered_duplicates_and_incomplete_revision():
    minute, five, hours = snapshot()
    duplicate = minute[1].copy()
    incomplete = minute[1].copy()
    incomplete[7], incomplete[8] = "999999", "0"
    result = detector(list(reversed(minute)) + [duplicate, incomplete], five, hours).evaluate(T)
    assert result[0].condition is True
    assert result[0].quote == 150_000


def test_conflicting_complete_rows_never_choose_newest_or_recover_silently():
    minute, five, hours = snapshot()
    conflict = minute[-1].copy()
    conflict[7] = "0"
    result = detector(minute + [conflict, minute[-1].copy()], five, hours).evaluate(T)
    assert [r.condition for r in result] == [None] * 4
    assert result[0].reasons == ("conflicting_complete_candle",)


@pytest.mark.parametrize("which", ["minute", "five"])
def test_history_gaps_are_unavailable(which):
    minute, five, hours = snapshot()
    (minute if which == "minute" else five).pop(1)
    assert all(r.condition is None for r in detector(minute, five, hours).evaluate(T))


def test_background_has_no_current_overlap_or_future_input():
    minute, five, hours = snapshot()
    expected = detector(minute, five, hours).evaluate(T)
    future_five = [row(T - trial.FIVE, close=200., quote=1e12), row(T, close=1., quote=1e12)]
    future_minute = [row(T, close=300., quote=1e12)]
    result = detector(minute + future_minute, five + future_five, hours).evaluate(T)
    assert result == expected


def test_historical_extreme_can_raise_mean_more_than_median():
    minute, five, hours = snapshot()
    five[100][7] = "1000000000"
    result = detector(minute, five, hours).evaluate(T)
    assert [r.condition for r in result] == [False, True, False, True]
    assert result[0].ratio < 3


def test_new_listing_and_normal_transition_need_full_baseline():
    product = replace(PRODUCT, normal_since=T - trial.WEEK)
    assert all(r.condition is None for r in detector(*snapshot(), product=product).evaluate(T))
    raw = metadata()
    raw["preMktSwTime"] = str(T - trial.WEEK)
    product = trial.admit_product(raw, T)
    assert product.normal_since == T - trial.WEEK
    assert all(r.condition is None for r in detector(*snapshot(), product=product).evaluate(T))


def test_hour_context_is_complete_only_and_does_not_delay_detection():
    minute, five, hours = snapshot(hour=True)
    result = detector(minute, five, hours + [row(T, 9999.)]).evaluate(T)[0]
    expected = 100.
    for i in range(1, 168):
        expected = (2 / 21) * (100. + i) + (19 / 21) * expected
    assert result.ema20 == pytest.approx(expected)
    no_hour = detector(minute, five, []).evaluate(T)[0]
    assert no_hour.condition is True and no_hour.ema20 is None
    assert any(s.startswith("hour_context:") for s in no_hour.reasons)


def evaluation(i, condition=True, direction="UP", rule="A0", product=PRODUCT.key):
    return trial.Evaluation(rule, product, T + i * trial.MINUTE, condition, direction if condition is True else None)


def test_overlap_dedup_and_one_disjoint_upgrade():
    tracker = trial.EventTracker()
    transitions = [transition for i in range(15) for transition in tracker.apply(evaluation(i))]
    assert [(t.stage, t.at) for t in transitions] == [("OBSERVED", T), ("PERSISTENT", T + trial.FIVE)]
    assert transitions[0].identity == transitions[1].identity


def test_ten_valid_false_reset_rearms_new_identity():
    tracker = trial.EventTracker()
    first = tracker.apply(evaluation(0))[0]
    for i in range(1, 10):
        assert tracker.apply(evaluation(i, False)) == ()
    ended = tracker.apply(evaluation(10, False))[0]
    assert ended.stage == "ENDED"
    assert ended.reason == "reset_10m"
    second = tracker.apply(evaluation(11))[0]
    assert second.stage == "OBSERVED" and second.identity != first.identity


def test_reverse_closes_old_event_and_starts_distinct_direction():
    tracker = trial.EventTracker()
    first = tracker.apply(evaluation(0))[0]
    transitions = tracker.apply(evaluation(1, direction="DOWN"))
    assert [t.stage for t in transitions] == ["ENDED", "OBSERVED"]
    assert transitions[0].reason == "direction_changed"
    assert transitions[1].identity != first.identity


def test_unknown_breaks_persistence_and_false_counter():
    tracker = trial.EventTracker()
    tracker.apply(evaluation(0))
    tracker.apply(evaluation(1, None))
    for i in range(2, 7):
        assert tracker.apply(evaluation(i)) == ()
    assert tracker.apply(evaluation(7))[0].stage == "PERSISTENT"
    for i in range(8, 17):
        tracker.apply(evaluation(i, False))
    tracker.apply(evaluation(17, None))
    for i in range(18, 27):
        assert tracker.apply(evaluation(i, False)) == ()
    assert tracker.apply(evaluation(27, False))[0].stage == "ENDED"


def test_missing_evaluation_is_a_continuity_gap():
    tracker = trial.EventTracker()
    tracker.apply(evaluation(0))
    assert tracker.apply(evaluation(8)) == ()
    for i in range(9, 13):
        assert tracker.apply(evaluation(i)) == ()
    assert tracker.apply(evaluation(13))[0].stage == "PERSISTENT"


def test_upgrade_is_not_resent_after_unknown():
    tracker = trial.EventTracker()
    changes = [t for i in range(6) for t in tracker.apply(evaluation(i))]
    tracker.apply(evaluation(6, None))
    changes += [t for i in range(7, 14) for t in tracker.apply(evaluation(i))]
    assert [t.stage for t in changes] == ["OBSERVED", "PERSISTENT"]


def test_spot_swap_and_rule_state_are_isolated():
    tracker = trial.EventTracker()
    for product in (PRODUCT.key, ("okx", "SWAP", "ALT-USDT-SWAP")):
        for rule in trial.RULES:
            assert tracker.apply(evaluation(0, rule=rule, product=product))[0].stage == "OBSERVED"
    assert len(tracker.active) == 8


def test_replay_order_cannot_silently_reinterpret_event_time():
    tracker = trial.EventTracker()
    tracker.apply(evaluation(2))
    with pytest.raises(ValueError, match="monotonically"):
        tracker.apply(evaluation(1))


def test_snapshot_immutable_and_background_cache_reused():
    det = detector(*snapshot())
    with pytest.raises(TypeError):
        det.minute.bars[T] = None
    first = det.evaluate(T)
    assert det.evaluate(T) == first
    assert len(det.backgrounds) == 1
@pytest.mark.parametrize("end_close,direction", [(100.5, "UP"), (99.5, "DOWN")])
def test_exact_absolute_return_boundary(end_close, direction):
    minute, five, hours = snapshot(quote=100_000.)
    for r in minute[1:]:
        r[1:5] = [str(end_close)] * 4
    result = detector(minute, five, hours).evaluate(T)[0]
    assert result.condition is True
    assert result.direction == direction
    assert abs(result.return_5m) == .005


def test_contract_size_is_not_quote_turnover():
    minute, five, hours = snapshot()
    for r in minute:
        r[5], r[6] = "1000000000", "1000000000"
    result = detector(minute, five, hours).evaluate(T)[0]
    assert result.quote == 150_000


def test_conflict_outside_window_does_not_poison_past_decision():
    minute, five, hours = snapshot()
    minute += [row(T, 100., 20.), row(T, 200., 40.)]
    assert detector(minute, five, hours).evaluate(T)[0].condition is True


def test_invalid_ohlc_is_not_a_negative_signal():
    minute, five, hours = snapshot()
    minute[-1][2] = "99"
    assert all(r.condition is None for r in detector(minute, five, hours).evaluate(T))


def test_incomplete_last_minute_and_missing_price_origin():
    minute, five, hours = snapshot()
    minute[-1][8] = "0"
    assert all(r.condition is None for r in detector(minute, five, hours).evaluate(T))
    minute, five, hours = snapshot()
    minute.pop(0)
    assert all(r.condition is None for r in detector(minute, five, hours).evaluate(T))


def test_upgraded_history_is_not_reset_by_missed_evaluation():
    tracker = trial.EventTracker()
    for i in range(6):
        tracker.apply(evaluation(i))
    changes = [t for i in range(20, 30) for t in tracker.apply(evaluation(i))]
    assert changes == []

def test_price_scale_gate_has_distinct_incremental_role():
    minute, five, hours = snapshot(change=.01)
    for i, r in enumerate(five):
        r[1:5] = ["100" if i % 2 == 0 else "110"] * 4
    result = detector(minute, five, hours).evaluate(T)
    assert [r.condition for r in result] == [True, True, False, False]
    assert result[0].price_score < 3


@pytest.mark.parametrize("bad", [0, 1, "true"])
def test_event_condition_does_not_accept_bool_lookalikes(bad):
    with pytest.raises(ValueError, match="Invalid trial evaluation"):
        trial.EventTracker().apply(replace(evaluation(0), condition=bad))

def test_known_noncrypto_is_distinct_from_missing_metadata():
    raw = metadata()
    raw["instCategory"] = "3"
    with pytest.raises(trial.Unavailable, match="excluded_noncrypto"):
        trial.admit_product(raw, T)
    raw["instCategory"] = ""
    with pytest.raises(trial.Unavailable, match="metadata_unavailable"):
        trial.admit_product(raw, T)