"""Representative public candle boundaries and persisted market format."""
import gzip
import hashlib
import json
from dataclasses import replace

import pytest
from test_trial import PRODUCT, T, row, snapshot

from crypto_boom import data, trial


@pytest.mark.parametrize('position,value,reason', [
    (1, float('inf'), 'nonfinite_number'), (2, float('nan'), 'nonfinite_number'),
    (3, True, 'invalid_number'), (4, 'bad', 'invalid_number'),
    (5, -1, 'invalid_ohlc_or_volume'), (6, -1, 'invalid_ohlc_or_volume'),
    (7, -1, 'invalid_ohlc_or_volume'), (7, 10**400, 'invalid_number'),
])
def test_invalid_candle_columns_do_not_become_bars(position, value, reason):
    candle = row(T - trial.MINUTE)
    candle[position] = value
    parsed = trial.Series.parse([candle], trial.MINUTE)
    assert not parsed.bars and parsed.errors[T - trial.MINUTE] == reason


@pytest.mark.parametrize('prices,valid', [
    ((100, 101, 99, 100.5), True), ((100, 99, 99, 100), False),
    ((0, 1, 0, 1), False), ((1e-300,) * 4, True),
])
def test_price_bounds_and_zero_volume(prices, valid):
    candle = row(T - trial.MINUTE)
    candle[1:5], candle[5:8] = prices, [0, -0., '0']
    parsed = trial.Series.parse([candle], trial.MINUTE)
    assert bool(parsed.bars) is valid


def test_market_cache_wire_and_round_trip_remain_compatible(tmp_path):
    product = replace(PRODUCT, inst_id='QUOTE"\\\n中\u2028\ud800-USDT')
    series = {g: trial.Series.parse(rows, g) for g, rows in zip(data.GRAINS.values(), snapshot(hour=True), strict=True)}
    series[trial.MINUTE] = data.merge_series(series[trial.MINUTE], [row(T - trial.MINUTE, quote=999)], T)
    contents = {}
    for name, grain in data.GRAINS.items():
        cropped = data.merge_series(series[grain], [], T)
        contents[name] = {'rows': data.rows_of(cropped), 'errors': sorted(cropped.errors.items())}
    encoded = json.dumps(contents, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    legacy = {'schema': data.CACHE_SCHEMA, 'product': product.key, 'normal_since': product.normal_since,
              'at': T, 'series': contents, 'sha256': hashlib.sha256(encoded).hexdigest()}
    expected = json.dumps(legacy, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    cache = data.ProductCache(tmp_path)
    assert cache.save(data.Snapshot(product, T, series)) is True
    actual = cache.path(product).read_bytes()
    assert gzip.decompress(actual) == expected
    assert actual == gzip.compress(expected, compresslevel=3, mtime=0)
    assert cache.save(data.Snapshot(product, T, series)) is False
    restored, reason = cache.load(product, T)
    assert reason == 'cache_hit' and restored.series[trial.MINUTE].errors == series[trial.MINUTE].errors
