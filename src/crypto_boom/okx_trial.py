"""Network-free OKX alert trial v1. No publication or production entry point.

Fixed engineering thresholds are experimental, not predictions. History replay
assumes completed bars were available at close; it cannot prove live latency.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

MINUTE = 60_000
FIVE = 5 * MINUTE
HOUR = 60 * MINUTE
WEEK = 7 * 24 * HOUR
HISTORY = 2016
RULES = ("A0", "B0", "A1", "B1")
VERSION = "okx-alert-trial-v1"
EXCLUDED = frozenset({"BTC", "ETH", "BNB", "USDT", "USDC", "FDUSD", "TUSD",
                      "USDP", "DAI", "BUSD", "EUR", "TRY", "BRL"})


class Unavailable(ValueError):
    """An input cannot support this trial evaluation (not a negative signal)."""


def _integer(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise Unavailable("invalid_integer")
    try:
        return int(value)
    except ValueError as exc:
        raise Unavailable("invalid_integer") from exc


def _number(value: object) -> float:
    # Already parsed exact floats dominate cache merge/save. Subclasses retain
    # the original conversion path, including their __float__ behavior.
    if type(value) is float:
        if not math.isfinite(value):
            raise Unavailable("nonfinite_number")
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise Unavailable("invalid_number")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise Unavailable("invalid_number") from exc
    if not math.isfinite(result):
        raise Unavailable("nonfinite_number")
    return result


def source_cutoff(server_ms: int) -> int:
    return ((_integer(server_ms) - 2000) // MINUTE) * MINUTE


@dataclass(frozen=True)
class Product:
    inst_type: str
    inst_id: str
    base: str
    normal_since: int

    @property
    def key(self) -> tuple[str, str, str]:
        return ("okx", self.inst_type, self.inst_id)


def admit_product(raw: Mapping[str, object], at: int) -> Product:
    """Admit public metadata; unavailable fields never become a guessed pool."""
    category = raw.get("instCategory")
    if category in ("3", "4", "5", "6"):
        raise Unavailable("excluded_noncrypto")
    if category != "1":
        raise Unavailable("metadata_unavailable:instCategory")
    kind, inst_id = raw.get("instType"), raw.get("instId")
    if kind not in ("SPOT", "SWAP") or raw.get("state") != "live" or raw.get("ruleType") != "normal":
        raise Unavailable("excluded_product_state_or_type")
    if not isinstance(inst_id, str) or not inst_id:
        raise Unavailable("metadata_unavailable:instId")
    if kind == "SPOT":
        base = raw.get("baseCcy")
        if raw.get("quoteCcy") != "USDT" or not isinstance(base, str) or not base or inst_id != base + "-USDT":
            raise Unavailable("excluded_spot_quote_or_identity")
    else:
        family = raw.get("instFamily")
        if raw.get("ctType") != "linear" or raw.get("settleCcy") != "USDT" or not isinstance(family, str) or not family.endswith("-USDT") or inst_id != family + "-SWAP":
            raise Unavailable("excluded_swap_settlement_or_identity")
        base = family[:-5]
        if not base or raw.get("ctValCcy") != base or _number(raw.get("ctVal")) <= 0:
            raise Unavailable("metadata_unavailable:contract_value")
    if base in EXCLUDED:
        raise Unavailable("excluded_base")
    times = [_integer(raw.get("listTime"))]
    for name in ("contTdSwTime", "preMktSwTime"):
        value = raw.get(name)
        if value not in (None, ""):
            times.append(_integer(value))
    if times[0] <= 0 or any(value < 0 or value > at for value in times):
        raise Unavailable("metadata_unavailable:normal_start")
    return Product(kind, inst_id, base, max(times))


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_ccy: float
    quote: float


@dataclass(frozen=True)
class Series:
    grain: int
    bars: Mapping[int, Bar]
    errors: Mapping[int, str]

    @classmethod
    def parse(cls, rows: list[list[object]], grain: int) -> Series:
        if grain not in (MINUTE, FIVE, HOUR):
            raise ValueError("Unsupported trial grain")
        bars: dict[int, Bar] = {}
        errors: dict[int, str] = {}
        for row in rows:
            if not isinstance(row, list) or len(row) != 9:
                raise Unavailable("invalid_candle_shape")
            ts = _integer(row[0])
            if ts < 0 or ts % grain:
                raise Unavailable("invalid_candle_alignment")
            if row[8] == "0":
                continue
            if row[8] != "1":
                errors[ts] = "invalid_confirm"
                bars.pop(ts, None)
                continue
            try:
                o, h, l, c, v, vc, q = map(_number, row[1:8])
                # Finite inputs: positive low plus both endpoint bounds imply
                # all positive OHLC and high >= low, without repeated min/max.
                if l <= 0 or o < l or c < l or o > h or c > h or v < 0 or vc < 0 or q < 0:
                    raise Unavailable("invalid_ohlc_or_volume")
                bar = Bar(ts, o, h, l, c, v, vc, q)
                if ts in bars and bars[ts] != bar:
                    raise Unavailable("conflicting_complete_candle")
                if ts not in errors:
                    bars[ts] = bar
            except Unavailable as exc:
                errors[ts] = str(exc)
                bars.pop(ts, None)
        return cls(grain, MappingProxyType(bars), MappingProxyType(errors))

    def window(self, start: int, count: int) -> list[Bar]:
        result = []
        for ts in range(start, start + count * self.grain, self.grain):
            if ts in self.errors:
                raise Unavailable(self.errors[ts])
            if ts not in self.bars:
                raise Unavailable("missing_complete_candle")
            result.append(self.bars[ts])
        return result


@dataclass(frozen=True)
class Background:
    mean_quote: float | None
    median_quote: float | None
    mad_quote: float | None
    mean_return: float | None
    std_return: float | None


@dataclass(frozen=True)
class Evaluation:
    rule: str
    product: tuple[str, str, str]
    at: int
    condition: bool | None
    direction: str | None = None
    quote: float | None = None
    return_5m: float | None = None
    ratio: float | None = None
    mad_score: float | None = None
    price_score: float | None = None
    ema20: float | None = None
    reasons: tuple[str, ...] = ()


def _finite(operation) -> float | None:
    """Translate only numerical failure to an unavailable derived metric."""
    try:
        value = operation()
    except (OverflowError, ZeroDivisionError, statistics.StatisticsError):
        return None
    return value if math.isfinite(value) else None


class Detector:
    """One immutable product snapshot and bounded, per-5m background reuse."""

    def __init__(self, product: Product, minute: Series, five: Series, hourly: Series,
                 *, rules: tuple[str, ...] = RULES):
        if (minute.grain, five.grain, hourly.grain) != (MINUTE, FIVE, HOUR):
            raise ValueError("Series grain mismatch")
        if not rules or len(set(rules)) != len(rules) or any(r not in RULES for r in rules):
            raise ValueError("Invalid rule selection")
        self.rules = rules
        self.need_mad = any(r.startswith("B") for r in rules)
        self.need_scale = any(r.endswith("1") for r in rules)
        self.hours: dict[int, tuple[float | None, tuple[str, ...]]] = {}
        self.product, self.minute, self.five, self.hourly = product, minute, five, hourly
        self.backgrounds: dict[int, Background] = {}

    def _background(self, end: int) -> Background:
        if end in self.backgrounds:
            return self.backgrounds[end]
        if end - WEEK - FIVE < self.product.normal_since:
            raise Unavailable("warmup:normal_history")
        bars = self.five.window(end - WEEK - FIVE, HISTORY + 1)
        quotes = [bar.quote for bar in bars[1:]]
        mean = _finite(lambda: statistics.fmean(quotes))
        median = statistics.median(quotes) if self.need_mad else None
        mad = statistics.median([abs(q - median) for q in quotes]) if self.need_mad else None
        mean_r = std = None
        if self.need_scale:
            returns = [(b.close - a.close) / a.close for a, b in zip(bars, bars[1:])]
            mean_r = _finite(lambda: statistics.fmean(returns)) if all(math.isfinite(r) for r in returns) else None
            std = _finite(lambda: statistics.pstdev(returns)) if mean_r is not None else None
        result = Background(mean, median, mad, mean_r, std)
        # A replay spans <=24h (<=289 keys); older keys are only an optimization.
        if len(self.backgrounds) >= 289:
            self.backgrounds.pop(next(iter(self.backgrounds)))
        self.backgrounds[end] = result
        return result

    def evaluate(self, at: int) -> tuple[Evaluation, ...]:
        if isinstance(at, bool) or not isinstance(at, int) or at % MINUTE:
            raise ValueError("Evaluation must be an integer minute boundary")
        key = self.product.key
        try:
            current = self.minute.window(at - 6 * MINUTE, 6)
            background = self._background(((at - FIVE) // FIVE) * FIVE)
            q = _finite(lambda: math.fsum(b.quote for b in current[1:]))
            r = _finite(lambda: (current[-1].close - current[0].close) / current[0].close)
            if q is None or r is None:
                raise Unavailable("nonfinite_current_feature")
        except Unavailable as exc:
            return tuple(Evaluation(rule, key, at, None, reasons=(str(exc),)) for rule in self.rules)
        ratio = _finite(lambda: q / background.mean_quote) if background.mean_quote and background.mean_quote > 0 else None
        m = _finite(lambda: 0.67448975 * (q - background.median_quote) / background.mad_quote) if background.mad_quote and background.mad_quote > 0 else None
        s = _finite(lambda: abs(r - background.mean_return) / background.std_return) if background.mean_return is not None and background.std_return and background.std_return > 0 else None
        reasons = (() if ratio is not None else ("ratio_unavailable",))
        reasons += (("mad_not_computed",) if not self.need_mad else (() if m is not None else ("mad_unavailable",)))
        reasons += (("price_scale_not_computed",) if not self.need_scale else (() if s is not None else ("price_scale_unavailable",)))
        end_hour = (at // HOUR) * HOUR
        if end_hour not in self.hours:
            ema, context_reasons = None, ()
            try:
                hours = self.hourly.window(end_hour - 168 * HOUR, 168)
                ema = hours[0].close
                for bar in hours[1:]:
                    ema = (2 / 21) * bar.close + (19 / 21) * ema
                if not math.isfinite(ema):
                    ema, context_reasons = None, ("hour_context_nonfinite",)
            except Unavailable as exc:
                context_reasons = ("hour_context:" + str(exc),)
            if len(self.hours) >= 25:
                self.hours.pop(next(iter(self.hours)))
            self.hours[end_hour] = ema, context_reasons
        ema, context_reasons = self.hours[end_hour]
        reasons += context_reasons
        results = []
        for rule in self.rules:
            volume = ratio if rule.startswith("A") else m
            tests = [q >= 100_000, abs(r) >= .005, None if volume is None else volume >= (3 if rule.startswith("A") else 6)]
            if rule.endswith("1"):
                tests.append(None if s is None else s >= 3)
            condition = False if False in tests else (None if None in tests else True)
            results.append(Evaluation(rule, key, at, condition, ("UP" if r > 0 else "DOWN") if condition is True else None, q, r, ratio, m, s, ema, reasons))
        return tuple(results)


@dataclass
class Event:
    rule: str
    product: tuple[str, str, str]
    direction: str
    first_at: int
    last_qualified_at: int
    stage: str = "OBSERVED"
    false_count: int = 0
    continuous_anchor: int | None = None
    end_reason: str | None = None
    persistent_announced: bool = False

    @property
    def identity(self) -> tuple:
        return (VERSION + "/" + self.rule, self.product, self.direction, self.first_at)


@dataclass(frozen=True)
class Transition:
    identity: tuple
    stage: str
    at: int
    reason: str | None = None


@dataclass
class EventTracker:
    """In-memory ordered replay only; no cross-run delivery guarantees."""
    active: dict[tuple, Event] = field(default_factory=dict)
    last_at: dict[tuple, int] = field(default_factory=dict)

    def apply(self, evaluation: Evaluation) -> tuple[Transition, ...]:
        if evaluation.rule not in RULES or not any(evaluation.condition is state for state in (True, False, None)):
            raise ValueError("Invalid trial evaluation")
        if evaluation.condition is True and evaluation.direction not in ("UP", "DOWN"):
            raise ValueError("True evaluation needs a direction")
        key = (evaluation.rule, evaluation.product)
        previous = self.last_at.get(key)
        if previous is not None and evaluation.at <= previous:
            raise ValueError("Replay must advance monotonically per product/rule")
        self.last_at[key] = evaluation.at
        event = self.active.get(key)
        if event and (evaluation.condition is None or (previous is not None and evaluation.at != previous + MINUTE)):
            event.continuous_anchor = None
            event.false_count = 0
            event.stage = "UNKNOWN"
        if evaluation.condition is None:
            return ()
        if evaluation.condition is False:
            if not event:
                return ()
            event.false_count += 1
            if event.false_count == 10:
                event.stage, event.end_reason = "ENDED", "reset_10m"
                del self.active[key]
                return (Transition(event.identity, "ENDED", evaluation.at, event.end_reason),)
            return ()
        changes = []
        if event and event.direction != evaluation.direction:
            event.stage, event.end_reason = "ENDED", "direction_changed"
            changes.append(Transition(event.identity, "ENDED", evaluation.at, event.end_reason))
            event = None
        if event is None:
            event = Event(evaluation.rule, evaluation.product, evaluation.direction, evaluation.at, evaluation.at, continuous_anchor=evaluation.at)
            self.active[key] = event
            changes.append(Transition(event.identity, "OBSERVED", evaluation.at))
        else:
            event.false_count = 0
            event.last_qualified_at = evaluation.at
            if event.continuous_anchor is None:
                event.continuous_anchor = evaluation.at
            if event.stage == "UNKNOWN":
                event.stage = "PERSISTENT" if event.persistent_announced else "OBSERVED"
            if not event.persistent_announced and evaluation.at - event.continuous_anchor >= FIVE:
                event.stage = "PERSISTENT"
                event.persistent_announced = True
                changes.append(Transition(event.identity, "PERSISTENT", evaluation.at))
        return tuple(changes)