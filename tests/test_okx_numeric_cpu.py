"""Numeric/wire behavior against the pre-optimization strict parser contract."""
import gzip
import hashlib
import itertools
import json
import math
from dataclasses import replace
from decimal import Decimal

import pytest
from test_okx_trial import PRODUCT, T, row, snapshot

from crypto_boom import okx_data as data
from crypto_boom import okx_trial as trial


# Oracle from trial SHA23e59d3d: no fast paths or rewritten OHLC predicate.
def reference_number(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise trial.Unavailable("invalid_number")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise trial.Unavailable("invalid_number") from exc
    if not math.isfinite(result):
        raise trial.Unavailable("nonfinite_number")
    return result


def reference_integer(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise trial.Unavailable("invalid_integer")
    try:
        return int(value)
    except ValueError as exc:
        raise trial.Unavailable("invalid_integer") from exc


def outcome(function, value):
    try:
        result = function(value)
        return ("value", result, math.copysign(1, result) if isinstance(result, float) else None)
    except Exception as exc:
        return (type(exc).__name__, str(exc))


class FloatOverride(float):
    def __float__(self):
        return 7.


class IntOverride(int):
    def __int__(self):
        return 11


class BadFloat(float):
    def __float__(self):
        raise ValueError("controlled conversion")


class StringFloat(str):
    def __float__(self):
        return 13.


NUMBERS = [0., -0., 1., -1., math.nextafter(0., 1.), math.nextafter(1., 2.),
           1e308, float("nan"), float("inf"), -float("inf"), 0, 1, 10**400,
           "1", "-0", "1e-9999", "1e9999", "NaN", " inf ", "", "bad", True,
           False, None, b"1", 1j, Decimal("1"), FloatOverride(1), IntOverride(1),
           BadFloat(1), StringFloat("1")]


@pytest.mark.parametrize("value", NUMBERS, ids=[f"case{i}" for i in range(len(NUMBERS))])
def test_number_contract_types_precision_sign_and_errors(value):
    assert outcome(trial._number, value) == outcome(reference_number, value)


@pytest.mark.parametrize("value", [0, -1, 10**400, "1", "1.0", "", True, None,
                                   1., IntOverride(1), FloatOverride(1)])
def test_integer_contract_keeps_subclass_conversion(value):
    assert outcome(trial._integer, value) == outcome(reference_integer, value)


def test_ohlc_predicate_equivalence_at_finite_boundaries():
    values = (-1., -0., 0., math.nextafter(0., 1.), 1., math.nextafter(1., 2.), 1e308)
    for o, h, l, c in itertools.product(values, repeat=4):
        r = row(T - trial.MINUTE)
        r[1:5] = [o, h, l, c]
        parsed = trial.Series.parse([r], trial.MINUTE)
        invalid = min(o, h, l, c) <= 0 or not l <= min(o, c) <= max(o, c) <= h
        assert bool(parsed.errors) is invalid, (o, h, l, c)
        if invalid:
            assert parsed.errors[T - trial.MINUTE] == "invalid_ohlc_or_volume"
        else:
            assert parsed.bars[T - trial.MINUTE] == trial.Bar(T - trial.MINUTE, o, h, l, c, 1., 1., 25_000.)


@pytest.mark.parametrize("position", [1, 2, 3, 4, 5, 6, 7])
@pytest.mark.parametrize("value,reason", [(True, "invalid_number"), (float("inf"), "nonfinite_number"),
                                         (float("nan"), "nonfinite_number"), (10**400, "invalid_number")])
def test_every_numeric_column_still_rejects_invalid_values(position, value, reason):
    r = row(T - trial.MINUTE)
    r[position] = value
    parsed = trial.Series.parse([r], trial.MINUTE)
    assert not parsed.bars and parsed.errors[T - trial.MINUTE] == reason


@pytest.mark.parametrize("position", [5, 6, 7])
@pytest.mark.parametrize("value,valid", [(-1., False), (-0., True), (0., True), (math.nextafter(0., 1.), True)])
def test_volume_boundaries_are_unchanged(position, value, valid):
    r = row(T - trial.MINUTE)
    r[position] = value
    parsed = trial.Series.parse([r], trial.MINUTE)
    assert bool(parsed.bars) is valid


@pytest.mark.parametrize("inst_id", ["ALT-USDT", 'QUOTE"\\\n中\u2028-USDT', "SURROGATE\ud800-USDT"])
def test_cache_wire_is_byte_identical_to_legacy_encoder(tmp_path, inst_id):
    p = replace(PRODUCT, inst_id=inst_id)
    raw = snapshot(hour=True)
    series = {g: trial.Series.parse(rows, g) for g, rows in zip(data.GRAINS.values(), raw, strict=True)}
    series[trial.MINUTE] = data.merge_series(series[trial.MINUTE], [row(T - trial.MINUTE, quote=999)], T)
    contents = {}
    for name, grain in data.GRAINS.items():
        cropped = data.merge_series(series[grain], [], T)
        contents[name] = {"rows": data.rows_of(cropped), "errors": sorted(cropped.errors.items())}
    encoded = json.dumps(contents, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    legacy = {"schema": data.CACHE_SCHEMA, "product": p.key, "normal_since": p.normal_since,
              "at": T, "series": contents, "sha256": hashlib.sha256(encoded).hexdigest()}
    expected = json.dumps(legacy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    cache = data.ProductCache(tmp_path)
    assert cache.save(data.Snapshot(p, T, series)) is True
    actual = cache.path(p).read_bytes()
    assert gzip.decompress(actual) == expected
    assert actual == gzip.compress(expected, compresslevel=3, mtime=0)
    assert cache.save(data.Snapshot(p, T, series)) is False
    restored, reason = cache.load(p, T)
    assert reason == "cache_hit" and restored.series[trial.MINUTE].errors == series[trial.MINUTE].errors
