"""Bounded public OKX acquisition and rebuildable product cache; no publication."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

import httpx

from .okx_trial import FIVE, HOUR, MINUTE, Product, Series, Unavailable

PATHS = frozenset({"/api/v5/public/time", "/api/v5/public/instruments",
                   "/api/v5/market/candles", "/api/v5/market/history-candles"})
GRAINS = {"minute": MINUTE, "five": FIVE, "hourly": HOUR}
RETENTION = {MINUTE: 24 * HOUR + 6 * MINUTE, FIVE: 8 * 24 * HOUR + 3 * FIVE, HOUR: 192 * HOUR}
CACHE_SCHEMA = "okx-complete-quote-usdt-v1"
CACHE_DIRECTORY_LIMIT = 512 * 1024 * 1024
MAX_PARTITIONS = 10000


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class Budget:
    """One shared admission/rate/byte owner for initial requests AND retries.

    Deadline is cooperative, not a hard process watchdog (notably DNS).
    All locks are released before network I/O; response chunks are accounted.
    """
    max_requests: int = 2400
    max_bytes: int = 128 * 1024 * 1024
    max_response: int = 4 * 1024 * 1024
    seconds: float = 480
    reserve: float = 60
    clock: object = time.monotonic
    sleep: object = time.sleep
    attempts: int = 0
    initial: int = 0
    retries: int = 0
    received: int = 0
    observations: list[dict] = field(default_factory=list)

    def __post_init__(self):
        if not (0 < self.max_requests <= 2400 and 0 < self.max_bytes <= 128 * 1024 * 1024
                and 0 < self.max_response <= 4 * 1024 * 1024 and 0 <= self.reserve < self.seconds <= 480):
            raise ValueError("Invalid bounded budget")
        self.started = self.clock()
        self.lock = threading.Lock()
        self.next_market = self.next_public = self.backoff_until = self.started

    def remaining(self):
        return self.started + self.seconds - self.reserve - self.clock()

    def admit(self, path: str, retry: bool = False):
        if path not in PATHS:
            raise ValueError("Only allowlisted public endpoints")
        market = "/market/" in path
        with self.lock:
            if self.received >= self.max_bytes:
                raise BudgetExceeded("byte_budget")
            if self.attempts >= self.max_requests:
                raise BudgetExceeded("request_budget")
            if retry and self.retries >= self.initial // 5:
                raise BudgetExceeded("retry_budget")
            due = max(self.backoff_until, self.next_market if market else self.next_public)
            wait = max(0, due - self.clock())
            if self.remaining() <= wait:
                raise BudgetExceeded("acquisition_deadline")
            self.sleep(wait)
            if self.remaining() <= 0:
                raise BudgetExceeded("acquisition_deadline")
            now = self.clock()
            if market:
                self.next_market = now + .125
            else:
                self.next_public = now + .25
            self.attempts += 1
            self.retries += int(retry)
            self.initial += int(not retry)

    def charge(self, amount: int):
        with self.lock:
            self.received += amount
            if self.received > self.max_bytes:
                raise BudgetExceeded("byte_budget")
        if self.remaining() <= 0:
            raise BudgetExceeded("acquisition_deadline")

    def backoff(self, seconds: float):
        with self.lock:
            self.backoff_until = max(self.backoff_until, self.clock() + seconds)

    def observe(self, record: dict):
        with self.lock:
            self.observations.append(record)


class PublicClient:
    """A single worker owns this HTTP/1.1 connection pool for its entire batch."""
    def __init__(self, budget: Budget, *, transport=None):
        self.budget = budget
        self.client = httpx.Client(
            base_url="https://www.okx.com", transport=transport, follow_redirects=False,
            trust_env=False, http2=False, limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
            headers={"User-Agent": "crypto-boom-public-shadow/1", "Accept-Encoding": "identity"})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def get(self, path: str, params: dict | None = None) -> list:
        if path not in PATHS:
            raise ValueError("Only allowlisted public GET")
        for attempt in range(3):
            self.budget.admit(path, retry=attempt > 0)
            started = self.budget.clock()
            size, status, failure = 0, None, None
            retry_delay = None
            try:
                timeout = httpx.Timeout(max(.001, min(5, self.budget.remaining())))
                with self.client.stream("GET", path, params=params, timeout=timeout) as response:
                    status = response.status_code
                    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                        raise Unavailable("unexpected_content_encoding")
                    payload = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        size += len(chunk)
                        self.budget.charge(len(chunk))
                        if size > self.budget.max_response:
                            raise BudgetExceeded("response_budget")
                        payload.extend(chunk)
                    if status == 429 or status in (500, 502, 503, 504):
                        retry_delay = 2 ** attempt
                        header = response.headers.get("Retry-After")
                        if header:
                            try:
                                retry_delay = max(retry_delay, float(header))
                            except ValueError:
                                pass
                        if not math.isfinite(retry_delay) or retry_delay < 0:
                            retry_delay = 2 ** attempt
                        self.budget.backoff(retry_delay)
                        raise Unavailable("retryable_http:" + str(status))
                    if status != 200:
                        raise Unavailable("http:" + str(status))
                    try:
                        body = json.loads(payload)
                    except (ValueError, UnicodeError) as exc:
                        raise Unavailable("invalid_json") from exc
                    if not isinstance(body, dict) or body.get("code") != "0" or not isinstance(body.get("data"), list):
                        raise Unavailable("okx_business_or_schema_error")
                    return body["data"]
            except httpx.TransportError as exc:
                failure = type(exc).__name__
                retry_delay = 2 ** attempt
                self.budget.backoff(retry_delay)
                if attempt == 2:
                    raise Unavailable("transport:" + failure) from exc
            except (Unavailable, BudgetExceeded) as exc:
                failure = str(exc)
                if retry_delay is None or attempt == 2 or isinstance(exc, BudgetExceeded):
                    raise
            finally:
                self.budget.observe({"path": path, "status": status, "bytes": size,
                                     "seconds": self.budget.clock() - started, "failure": failure})
        raise AssertionError("Unreachable")


def rows_of(series: Series) -> list[list]:
    return [[b.ts, b.open, b.high, b.low, b.close, b.volume, b.volume_ccy, b.quote, "1"]
            for _, b in sorted(series.bars.items())]


def _trim_series(series: Series, at: int) -> Series:
    """Crop already-admitted candles without interpreting their values again."""
    lower = (at // series.grain) * series.grain - RETENTION[series.grain]
    errors = {t: e for t, e in series.errors.items() if lower <= t and t + series.grain <= at}
    bars = {t: b for t, b in series.bars.items()
            if lower <= t and t + series.grain <= at and t not in errors}
    return Series(series.grain, MappingProxyType(bars), MappingProxyType(errors))


def merge_series(old: Series, rows: list[list], at: int) -> Series:
    merged = Series.parse(rows_of(old) + rows, old.grain)
    errors = dict(old.errors)
    errors.update(merged.errors)  # A conflict stays unavailable until it ages out.
    return _trim_series(Series(old.grain, merged.bars, MappingProxyType(errors)), at)


def fetch_range(client: PublicClient, product: Product, old: Series, start: int, at: int,
                *, end: int | None = None, retain_partial=None) -> Series:
    """Fetch [start, end), keeping source cutoff/retention at the real ``at``.

    On interrupted acquisition, an opted-in recovery consumer receives validated
    partial history once; the original failure still propagates. This is cache
    progress, never evidence that the range or replay is complete.
    """
    grain = old.grain
    end = (at // grain) * grain if end is None else end
    if (start < 0 or start % grain or at % MINUTE or start >= end
            or end % grain or end > at):
        raise ValueError("Invalid candle range")
    name = {MINUTE: "1m", FIVE: "5m", HOUR: "1H"}[grain]
    cursor = end
    received = []
    try:
        while cursor > start:
            path = "/api/v5/market/candles" if start >= at - 1440 * grain else "/api/v5/market/history-candles"
            limit = min(300, max(2, math.ceil((cursor - start) / grain)))
            page = client.get(path, {"instId": product.inst_id, "bar": name, "after": str(cursor), "limit": str(limit)})
            if not page:
                break
            Series.parse(page, grain)
            # Include confirm=0 timestamps for cursor progress, never cache them.
            timestamps = [int(row[0]) for row in page]
            if any(t >= cursor for t in timestamps):
                raise Unavailable("pagination_not_backward")
            received.extend(row for row in page if start <= int(row[0]) and int(row[0]) + grain <= end)
            cursor = min(timestamps)
            if len(page) < limit:
                break  # Source shortage is a gap, never invented history.
    except (Unavailable, BudgetExceeded):
        if received and retain_partial is not None:
            partial = merge_series(old, received, at)
            if partial.bars != old.bars or partial.errors != old.errors:
                retain_partial(partial)
        raise
    return merge_series(old, received, at)


@dataclass(frozen=True)
class Snapshot:
    product: Product
    at: int
    series: dict[int, Series]


class ProductCache:
    """Rebuildable partitions in a directory exclusively mutated by this owner.

    The CLI workspace writer fences concurrent runs. Other callers must provide
    the same exclusivity; arbitrary foreign additions are not budgeted. Changed
    target sizes trigger a bounded re-index (e.g. a damaged/truncated partition).
    """
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._refresh_usage()

    def _refresh_usage(self):
        sizes = {}
        total = 0
        for path in self.root.glob("*.json.gz"):
            if len(sizes) >= MAX_PARTITIONS:
                raise BudgetExceeded("cache_partition_count")
            size = path.stat().st_size
            total += size
            if total > CACHE_DIRECTORY_LIMIT:
                raise BudgetExceeded("cache_directory_limit")
            sizes[path] = size
        self.sizes, self.total = sizes, total

    def path(self, product: Product):
        key = json.dumps(product.key, separators=(",", ":"))
        return self.root / (hashlib.sha256(key.encode()).hexdigest() + ".json.gz")

    def load(self, product: Product, at: int) -> tuple[Snapshot | None, str]:
        path = self.path(product)
        try:
            if path.stat().st_size > 1024 * 1024:
                return None, "cache_oversized"
            with gzip.open(path, "rb") as stream:
                raw = stream.read(2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024:
                return None, "cache_expansion_limit"
            body = json.loads(raw)
            if (body["schema"] != CACHE_SCHEMA or body["product"] != list(product.key)
                    or body["normal_since"] != product.normal_since or type(body["at"]) is not int
                    or body["at"] > at or body["at"] < 0 or body["at"] % MINUTE):
                return None, "cache_incompatible"
            data = body["series"]
            encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            if hashlib.sha256(encoded).hexdigest() != body["sha256"] or set(data) != set(GRAINS):
                return None, "cache_digest_or_grain"
            series, count = {}, 0
            for name, grain in GRAINS.items():
                part = data[name]
                count += len(part["rows"]) + len(part["errors"])
                if count > 5000:
                    return None, "cache_row_limit"
                parsed = Series.parse(part["rows"], grain)
                errors = dict(parsed.errors)
                for t, reason in part["errors"]:
                    if type(t) is not int or t < 0 or t % grain or not isinstance(reason, str) or len(reason) > 128:
                        return None, "cache_invalid_error"
                    errors[t] = reason
                parsed = Series(grain, MappingProxyType(dict(parsed.bars)), MappingProxyType(errors))
                if any(t + grain > body["at"] for t in (*parsed.bars, *parsed.errors)):
                    return None, "cache_future_bar"
                series[grain] = _trim_series(parsed, at)
            return Snapshot(product, body["at"], series), "cache_hit"
        except FileNotFoundError:
            return None, "cache_missing"
        except (OSError, EOFError, ValueError, KeyError, TypeError, OverflowError) as exc:
            return None, "cache_invalid:" + type(exc).__name__

    def save(self, snapshot: Snapshot):
        if set(snapshot.series) != set(GRAINS.values()):
            raise ValueError("All cache grains required")
        data, count = {}, 0
        for name, grain in GRAINS.items():
            s = merge_series(snapshot.series[grain], [], snapshot.at)
            data[name] = {"rows": rows_of(s), "errors": sorted(s.errors.items())}
            count += len(s.bars) + len(s.errors)
        if count > 5000:
            raise BudgetExceeded("cache_row_limit")
        encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        header = {"schema": CACHE_SCHEMA, "product": snapshot.product.key,
                  "normal_since": snapshot.product.normal_since, "at": snapshot.at}
        prefix = json.dumps(header, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        # Reuse the canonical series bytes for both digest and payload. Header
        # strings still use the standard encoder; key order matches sort_keys.
        raw = b"".join((prefix[:-1], b',"series":', encoded, b',"sha256":"',
                        hashlib.sha256(encoded).hexdigest().encode("ascii"), b'"}'))
        compressed = gzip.compress(raw, compresslevel=3, mtime=0)
        if len(raw) > 2 * 1024 * 1024 or len(compressed) > 1024 * 1024:
            raise BudgetExceeded("cache_partition_limit")
        path = self.path(snapshot.product)
        with self.lock:
            old_size = path.stat().st_size if path.exists() else 0
            if old_size != self.sizes.get(path, 0):
                self._refresh_usage()
            if path not in self.sizes and len(self.sizes) >= MAX_PARTITIONS:
                raise BudgetExceeded("cache_partition_count")
            prospective = self.total - old_size + len(compressed)
            if prospective > CACHE_DIRECTORY_LIMIT:
                raise BudgetExceeded("cache_directory_limit")
            if old_size == len(compressed):
                # Even a damaged/oversized old partition cannot force an
                # unbounded read merely to compare an atomic replacement.
                with path.open("rb") as stream:
                    if stream.read(len(compressed) + 1) == compressed:
                        return False
            atomic_write(path, compressed)
            self.sizes[path], self.total = len(compressed), prospective
            return True



def atomic_write(path: Path, data: bytes):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
