"""Reusable no-publish scanner; the CLI remains strictly shadow-only."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType

from .data import (
    GRAINS,
    Budget,
    BudgetExceeded,
    ProductCache,
    PublicClient,
    Snapshot,
    fetch_range,
)
from .state import Checkpoint, StateStore, atomic_write
from .trial import (
    FIVE,
    HOUR,
    MINUTE,
    WEEK,
    Detector,
    Evaluation,
    Product,
    Series,
    Unavailable,
    admit_product,
    source_cutoff,
)

PROGRESS_INTERVAL = 20


def time_label(at):
    stamp = datetime.fromtimestamp(at / 1000, timezone.utc)
    return stamp.strftime('%Y-%m-%d %H:%M:%S UTC') + ' / local ' + stamp.astimezone().isoformat(sep=' ', timespec='seconds')


def scan_log(message):
    print(f'[{datetime.now().astimezone().isoformat(timespec="seconds")}] {message}', file=sys.stderr, flush=True)


@contextmanager
def scan_progress(budget, progress):
    """One bounded-rate stderr reporter; counter movement, not ticks, proves progress."""
    stopped = threading.Event()
    def report():
        while not stopped.wait(PROGRESS_INTERVAL):
            now = budget.clock()
            success_age = 'none' if budget.last_success_at is None else f'{max(0, now - budget.last_success_at):.0f}s ago'
            scan_log(f"Scanning phase={progress['phase']} processed={progress['processed']}/{progress['total']} "
                     f"HTTP={budget.attempts}/{budget.max_requests} retries={budget.retries} "
                     f"last_success={success_age} elapsed={now - budget.started:.0f}s; heartbeat is not proof of data progress")
    reporter = threading.Thread(target=report, daemon=True)
    reporter.start()
    try:
        yield
    finally:
        stopped.set()
        reporter.join(timeout=1)


def _empty(grain):
    return Series(grain, MappingProxyType({}), MappingProxyType({}))


def _refresh_series(client, product, series, required, at, *, retain_partial=None):
    """Refresh the native tail, then repair only missing historical segments."""
    grain = series.grain
    end = (at // grain) * grain
    required = max(0, required)
    known = set(series.bars) | set(series.errors)
    if grain != MINUTE and all(t in known for t in range(required, end, grain)):
        return series
    original = series
    def remember_partial(part):
        nonlocal series
        series = part
    try:
        known_end = max(known, default=required - grain) + grain
        begin = max(required, min(end, known_end) - 2 * grain)
        if begin < end:
            series = fetch_range(client, product, series, begin, at, retain_partial=remember_partial)
        if not series.bars and not series.errors:
            return series
        while True:
            known = set(series.bars) | set(series.errors)
            missing = next((t for t in range(required, end, grain) if t not in known), end)
            if missing == end:
                return series
            repair_end = next((t for t in range(missing + grain, end, grain) if t in known), end)
            repaired = fetch_range(client, product, series, missing, at, end=repair_end,
                                   retain_partial=remember_partial)
            if missing not in repaired.bars and missing not in repaired.errors:
                return repaired  # Source shortage: do not loop or invent candles.
            series = repaired
    except (Unavailable, BudgetExceeded):
        if series is not original and retain_partial is not None:
            retain_partial(series)
        raise


def _hour_coverage(series, at):
    """Native coverage only; Detector remains the sole EMA computation owner."""
    end = (at // HOUR) * HOUR
    try:
        series.window(end - 168 * HOUR, 168)
    except Unavailable as exc:
        return {"status": "not_requested", "asof": None, "reason": str(exc)}
    return {"status": "cached", "asof": end}


def scan(products: list[Product], at: int, cache: ProductCache, state: Checkpoint,
         budget: Budget, *, workers: int = 4, transport=None, max_transitions: int = 10000,
         progress=None) -> dict:
    """Bounded workers, one product snapshot at a time; caller owns state commit.

    Mandatory acquisition/replay finishes across the pool before optional hour
    acquisition. Price cache commits may succeed even if checkpoint commit fails;
    the latter still controls replay. Returned transitions are diagnostics, never sent.
    """
    if workers not in (4, 8) or not 0 < len(products) <= 1000 or len({p.key for p in products}) != len(products):
        raise ValueError("Use 4/8 workers and 1..1000 unique products")
    if type(at) is not int or at % MINUTE or at < state.at:
        raise ValueError("Source cutoff must advance monotonically")
    if type(max_transitions) is not int or not 0 <= max_transitions <= 10000:
        raise ValueError("Transition capacity must be 0..10000")
    capacity_lock, accepted = threading.Lock(), 0
    cold = sorted((p for p in products if not cache.path(p).exists()), key=lambda p: p.key)
    warm = sorted((p for p in products if cache.path(p).exists()), key=lambda p: p.key)
    offset = 0
    by_key = {p.key: p for p in products}
    reports, transitions, processed_cold = [], [], []

    def work(pending, bootstrap):
        nonlocal accepted
        local_reports, local_changes, local_states = [], [], []
        def record(row):
            local_reports.append(row)
            if progress is not None and row['status'] != 'deferred_invalid_cache':
                with capacity_lock:
                    progress['processed'] += 1
        with PublicClient(budget, transport=transport) as client:
            while True:
                with capacity_lock:
                    product = next(pending, None)
                if product is None:
                    break
                key = ("A0", product.key)
                previous = state.tracker.last_at.get(key)
                first = at if previous is None else max(previous + MINUTE, at - 24 * HOUR + MINUTE)
                omitted = 0 if previous is None else max(0, (first - previous) // MINUTE - 1)
                if first > at:
                    record({"product": product.key, "status": "already_evaluated"})
                    continue
                ready = ((product.normal_since + FIVE - 1) // FIVE) * FIVE + WEEK + 2 * FIVE
                if at < ready:
                    # Metadata alone proves that a full normal-history window is
                    # impossible. Do not re-download seven days on every run.
                    local = type(state.tracker)()
                    if previous is not None:
                        local.last_at[key] = previous
                    if key in state.tracker.active:
                        local.active[key] = deepcopy(state.tracker.active[key])
                    for t in range(first, at + MINUTE, MINUTE):
                        local.apply(Evaluation("A0", product.key, t, None, reasons=("warmup:normal_history",)))
                    local_states.append((key, local.last_at[key], local.active.get(key)))
                    record({"product": product.key, "status": "warmup", "ready_at": ready,
                                          "catchup_truncated_minutes": omitted,
                                          "evaluated": (at-first)//MINUTE+1, "unknown": (at-first)//MINUTE+1})
                    continue
                if budget.remaining() <= 0 or budget.attempts >= budget.max_requests:
                    record({"product": product.key, "status": "deferred_budget", "catchup_truncated_minutes": omitted})
                    continue
                started = time.monotonic()
                snapshot, cache_reason = cache.load(product, at)
                if snapshot is None and not bootstrap:
                    record({"product": product.key, "status": "deferred_invalid_cache", "cache": cache_reason})
                    continue
                series = snapshot.series if snapshot else {g: _empty(g) for g in GRAINS.values()}
                # Download only the portion that could satisfy normal-history
                # admission; earlier catch-up boundaries remain known warmup.
                fetch_first = max(first, ready)
                needs = {MINUTE: fetch_first - 6 * MINUTE,
                         FIVE: ((fetch_first - FIVE) // FIVE) * FIVE - WEEK - FIVE}
                reason, failed_grain, cache_saved = None, None, False
                acquisition_progress, partial_cache_saved, cache_error = False, False, None
                def retain_partial(part):
                    nonlocal acquisition_progress
                    series[part.grain] = part
                    acquisition_progress = True
                try:
                    for grain in (MINUTE, FIVE):
                        failed_grain = grain
                        series[grain] = _refresh_series(client, product, series[grain], needs[grain], at,
                                                        retain_partial=retain_partial)
                        acquisition_progress = True
                    failed_grain = None  # Cache failure is not missing acquired candles.
                    cache.save(Snapshot(product, at, series))
                    cache_saved = True
                except (Unavailable, BudgetExceeded, OSError) as exc:
                    reason = str(exc)
                    if (failed_grain in (MINUTE, FIVE) and acquisition_progress
                            and any(s.bars or s.errors for s in series.values())):
                        try:
                            cache.save(Snapshot(product, at, series))
                            partial_cache_saved = True
                        except (Unavailable, BudgetExceeded, OSError) as save_exc:
                            cache_error = str(save_exc)
                # Failed mandatory acquisition retains the replay checkpoint.
                # Optional hour acquisition never controls replay advancement.
                local = type(state.tracker)()
                if key in state.tracker.last_at:
                    local.last_at[key] = state.tracker.last_at[key]
                if key in state.tracker.active:
                    local.active[key] = deepcopy(state.tracker.active[key])
                compute_start = time.perf_counter()
                detector = Detector(product, series[MINUTE], series[FIVE], series[HOUR], rules=("A0",))
                unavailable, evaluated = 0, 0
                prior_event = local.active.get(key)
                qualified_at = prior_event.last_qualified_at if prior_event else None
                product_changes, deferred = [], None
                for t in range(first, at + MINUTE, MINUTE) if failed_grain not in (MINUTE, FIVE) else ():
                    if budget.remaining() <= 0:
                        deferred = "deferred_deadline"
                        break
                    evaluation = (Evaluation("A0", product.key, t, None, reasons=("warmup:normal_history",))
                                  if t < ready else detector.evaluate(t)[0])
                    unavailable += evaluation.condition is None
                    evaluated += 1
                    if evaluation.condition is True:
                        qualified_at = t
                    product_changes.extend(local.apply(evaluation))
                if deferred is None:
                    with capacity_lock:
                        if accepted + len(product_changes) > max_transitions:
                            deferred = "deferred_event_capacity"
                        else:
                            accepted += len(product_changes)
                if deferred:
                    # Abandon this whole replay, not only its overflowing tail.
                    # Its durable checkpoint remains the replay authority.
                    record({"product": product.key, "status": deferred,
                                          "catchup_truncated_minutes": omitted})
                    continue
                local_changes.extend(product_changes)
                if failed_grain in (MINUTE, FIVE):
                    # Do not advance the replay checkpoint over an acquisition
                    # failure: next run may recover <=24h. Break live continuity.
                    event = local.active.get(key)
                    if event:
                        event.stage, event.continuous_anchor, event.false_count = "UNKNOWN", None, 0
                local_states.append((key, local.last_at.get(key), local.active.get(key)))
                candidate = (evaluated > 0 and qualified_at is not None and at - qualified_at <= 30 * MINUTE)
                hour = _hour_coverage(series[HOUR], at)
                if candidate and hour["status"] == "not_requested" and not cache_saved:
                    hour["status"] = "deferred_cache"
                record({"product": product.key, "status": "partial" if reason or unavailable else "complete",
                                      "cache": cache_reason, "error": reason, "evaluated": evaluated, "unknown": unavailable,
                                      "partial_cache_saved": partial_cache_saved, "cache_error": cache_error,
                                      "context_candidate": candidate, "context_qualified_at": qualified_at,
                                      "hour_context": hour,
                                      "catchup_truncated_minutes": omitted,
                                      "compute_seconds": time.perf_counter() - compute_start,
                                      "seconds": time.monotonic() - started})
        return local_reports, local_changes, local_states

    # Warm pass finishes before bootstrap; at most workers tasks/futures, not an
    # unbounded queued request per symbol. Worker pools own and join their clients.
    for bootstrap in (False, True):
        if bootstrap and cold:
            cold.sort(key=lambda p: p.key)
            offset = state.cursor % len(cold)
            cold = cold[offset:] + cold[:offset]
        pool = cold if bootstrap else warm
        if not pool:
            continue
        pending = iter(pool)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(work, pending, bootstrap) for _ in range(min(workers, len(pool)))]
            for future in futures:
                rows, changes, updates = future.result()
                for row in rows:
                    if row["status"] == "deferred_invalid_cache":
                        cold.append(by_key[tuple(row["product"])])
                    else:
                        reports.append(row)
                transitions.extend(changes)
                for key, last, event in updates:
                    if last is not None:
                        state.tracker.last_at[key] = last
                    if event is None:
                        state.tracker.active.pop(key, None)
                    else:
                        state.tracker.active[key] = event
                if bootstrap:
                    processed_cold.extend(r for r in rows if r["status"] != "deferred_budget")
    # Barrier: all mandatory workers have joined. Keep only bounded diagnostic
    # rows, not all product snapshots. Optional work cannot starve pool detection.
    if progress is not None:
        progress['phase'] = 'hour_context'
    candidates = [r for r in reports if r.get("context_candidate")
                  and r["hour_context"]["status"] == "not_requested"]
    def priority(row):
        event = state.tracker.active.get(("A0", tuple(row["product"])))
        return (not (event and event.persistent_announced), -row["context_qualified_at"], row["product"])
    candidates.sort(key=priority)
    # Serial optional requests preserve deterministic priority when residual
    # budget runs out. The mandatory path still uses 4/8 bounded workers.
    if candidates:
        with PublicClient(budget, transport=transport) as client:
            for row in candidates:
                hour = row["hour_context"]
                if budget.remaining() <= 0 or budget.attempts >= budget.max_requests:
                    hour.update(status="deferred_budget", reason="remaining_budget")
                    continue
                product = by_key[tuple(row["product"])]
                snapshot, cache_reason = cache.load(product, at)
                if snapshot is None:
                    hour.update(status="deferred_cache", reason=cache_reason)
                    continue
                series = snapshot.series
                partial_hour = []
                try:
                    series[HOUR] = _refresh_series(client, product, series[HOUR],
                                                  (at // HOUR) * HOUR - 168 * HOUR, at,
                                                  retain_partial=partial_hour.append)
                    coverage = _hour_coverage(series[HOUR], at)
                    # Preserve useful partial pages without claiming full context.
                    cache.save(Snapshot(product, at, series))
                    if coverage["status"] == "cached":
                        hour.clear()
                        hour.update(status="refreshed", asof=coverage["asof"])
                    else:
                        hour.update(status="unavailable", reason=coverage["reason"])
                except (Unavailable, BudgetExceeded, OSError) as exc:
                    hour.update(status="deferred_budget" if isinstance(exc, BudgetExceeded) else "unavailable",
                                reason=str(exc))
                    if partial_hour:
                        series[HOUR] = partial_hour[-1]
                        try:
                            cache.save(Snapshot(product, at, series))
                            hour["partial_cache_saved"] = True
                        except (Unavailable, BudgetExceeded, OSError) as save_exc:
                            hour.update(partial_cache_saved=False, cache_error=str(save_exc))
    # Every row has explicit context disposition, including no replay this run.
    for row in reports:
        row.setdefault("hour_context", {"status": "not_evaluated", "asof": None})
    # Rotate one position even if budget allowed no bootstrap; never starve the
    # same failed product indefinitely. Cache writes move successes to warm pass.
    if cold:
        state.cursor = (offset + max(1, len(processed_cold))) % len(cold)
    state.at = at
    hour_counts = {}
    for row in reports:
        status = row["hour_context"]["status"]
        hour_counts[status] = hour_counts.get(status, 0) + 1
    return {"at": at, "pool": len(products), "coverage_contract": "a0-mandatory-v2",
            "hour_context_counts": hour_counts, "complete": sum(r["status"] == "complete" for r in reports),
            "partial": sum(r["status"] == "partial" for r in reports),
            "warmup": sum(r["status"] == "warmup" for r in reports),
            "already_evaluated": sum(r["status"] == "already_evaluated" for r in reports),
            "deferred": sum(r["status"].startswith("deferred_") for r in reports),
            "transition_capacity": max_transitions,
            "products": sorted(reports, key=lambda r: r["product"]),
            "transitions": [asdict(t) for t in transitions], "http_attempts": budget.attempts,
            "http_retries": budget.retries, "response_bytes": budget.received,
            "http": budget.observations, "publication": "disabled"}


def preview_text(report):
    """Plain-text diagnostics, not a publication receipt or a send ledger."""
    def utc(at):
        return datetime.fromtimestamp(at / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    lines = [
        'LOCAL PREVIEW - no notifications; notification ledger unchanged',
        f"Data cutoff: {time_label(report['at'])}",
        f"Products: {report['pool']} / {report['metadata_eligible']}; capped={report['coverage_capped']}",
        'Coverage: ' + ' '.join(f'{key}={report[key]}' for key in
                           ('complete', 'already_evaluated', 'partial', 'warmup', 'deferred')),
        f"Checkpoint generation={report['checkpoint_generation']}; HTTP={report['http_attempts']}; retries={report['http_retries']}",
        'Sorted by transition time; UP/DOWN are not trading advice. No changes does not mean no coverage gaps.',
        'Not a notification queue; rescanning the same cutoff does not repeat transitions.',
        '', 'Transitions this run:',
    ]
    if 'completed_at' in report:
        lines[2:2] = [f"Scan started: {time_label(report['started_at'])}",
                      f"Scan ended: {time_label(report['completed_at'])}",
                      f"Data age at scan end: {report['data_age_seconds']}s; last completed report, not live progress; age increases after completion."]
    for change in sorted(report['transitions'], key=lambda v: (v['at'], v['identity'], v['stage'])):
        rule, product, direction, first = change['identity']
        # JSON string quoting keeps source-provided controls on one plain-text line.
        lines.append('\t'.join((utc(change['at']), json.dumps(product, ensure_ascii=False),
                                direction, change['stage'], rule, 'first=' + utc(first))))
    if not report['transitions']:
        lines.append('No new transitions. See scan-report.json for coverage gaps and per-product reasons.')
    return ('\n'.join(lines) + '\n').encode('utf-8')


def main(argv=None, *, cancel=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True, help="Local task-owned cache/state directory")
    parser.add_argument("--limit", type=int, default=1000, help="Coverage cap (default 1000); truncation is reported, not full-pool acceptance")
    parser.add_argument("--workers", type=int, choices=(4, 8), default=4)
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 1000:
        parser.error("limit must be 1..1000")
    display_workspace = args.workspace
    args.workspace = args.workspace.absolute()
    for path in (args.workspace, *(args.workspace / name for name in
                 ('checkpoint.json', 'checkpoint.json.lock', 'market', 'scan-report.json', 'preview.txt'))):
        if path.is_symlink() or path.is_junction():
            raise Unavailable('local_workspace_link: use a dedicated regular directory')
    from .runtime import supervise
    started = time.monotonic()
    scan_log('Run starting: loading persistent state; preview.txt updates only after a successful run.')
    store = StateStore(args.workspace / "checkpoint.json")
    with store.writer():
        state, state_reason = store.load()
        if state is None and (os.path.lexists(store.path) or any((args.workspace / name).exists()
                              for name in ('market', 'scan-report.json', 'preview.txt'))):
            raise Unavailable(state_reason + ": manual diagnosis required, no overwrite")
        if state is not None and state.notifications_enabled:
            raise Unavailable("shadow_workspace_required: do not consume production delivery state")
        if state is None:
            # Establish a non-notifying recovery point before a cold scan can be interrupted.
            state = Checkpoint()
            store.save(state, None)
        expected = state.generation
        ledger = state.ledger
        token = store.writer_token
        deadline = time.monotonic() + 480
        exitcode = supervise(scan_job, (args.workspace, args.limit, args.workers, token, expected, deadline),
                             seconds=480, grace=5, lock_owner=store, **({'cancel': cancel} if cancel else {}))
        if exitcode != 0:
            raise Unavailable("shadow_child_failed:" + str(exitcode))
        with args.workspace.joinpath("scan-report.json").open("rb") as stream:
            raw = stream.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise Unavailable("report_byte_limit")
        report = json.loads(raw)
        if report.get("run_token") != token:
            raise Unavailable("stale_shadow_report")
        saved, reason = store.load()
        if (saved is None or saved.generation != (expected or 0) + 1
                or report.get('checkpoint_generation') != saved.generation or report['at'] != saved.at
                or saved.notifications_enabled or report.get('publication') != 'disabled'):
            raise Unavailable('shadow_checkpoint_mismatch:' + reason)
        if saved.ledger != ledger:
            raise Unavailable('shadow_ledger_changed: manual diagnosis required')
        preview = preview_text(report)
        if len(preview) > 16 * 1024 * 1024:
            raise Unavailable('preview_byte_limit')
        atomic_write(args.workspace / 'preview.txt', preview)
        summary = {k: v for k, v in report.items() if k not in ("products", "transitions", "http", "run_token")}
        summary.update(elapsed_seconds=round(time.monotonic() - started, 3),
                       ledger_entries=len(saved.ledger), ledger_preserved=True,
                       report_path=str(display_workspace / 'scan-report.json'),
                       preview_path=str(display_workspace / 'preview.txt'))
        scan_log(f"Run completed: preview updated generation={saved.generation} elapsed={summary['elapsed_seconds']}s; "
                 f"complete={report['complete']} partial={report['partial']} deferred={report['deferred']}; "
                 'processed does not imply full coverage; no changes does not imply no anomalies.')
        print(json.dumps(summary, sort_keys=True, allow_nan=False))


def scan_job(workspace, limit, workers, token, expected, deadline, notification_mode='shadow', baseline_digest=''):
    """All potentially blocking job I/O lives in the supervised child."""
    started_at = int(time.time() * 1000)
    seconds = min(480, deadline - time.monotonic())
    if seconds <= 60:
        raise Unavailable("startup_consumed_job_budget")
    budget = Budget(seconds=seconds)
    progress = {'phase': 'metadata', 'processed': 0, 'total': '?'}
    store = StateStore(workspace / "checkpoint.json")
    with store.borrowed_writer(token), scan_progress(budget, progress):
        state, state_reason = store.load()
        if ((expected is None and (state is not None or store.path.exists()))
                or (expected is not None and (state is None or state.generation != expected))):
            raise Unavailable("shadow_generation_changed")
        if notification_mode not in ('shadow', 'resume', 'baseline'):
            raise ValueError('scan_notification_mode')
        if state is not None and state.notifications_enabled:
            raise Unavailable("shadow_workspace_required")
        if notification_mode != 'shadow' and (state is None or any(v['status'] != 'confirmed' for v in state.ledger.values())):
            raise Unavailable('scan_validated_baseline_required')
        state = state or Checkpoint()
        scan_log(f'Acquisition started: {time_label(started_at)}; reading OKX time and instruments.')
        with PublicClient(budget) as client:
            clock = client.get("/api/v5/public/time")
            at = source_cutoff(clock[0]["ts"])
            products, excluded = [], 0
            for kind in ("SPOT", "SWAP"):
                for raw in client.get("/api/v5/public/instruments", {"instType": kind}):
                    try:
                        products.append(admit_product(raw, at))
                    except Unavailable:
                        excluded += 1
        products.sort(key=lambda p: p.key)
        progress.update(phase='mandatory', total=min(len(products), limit))
        scan_log(f"Pool={len(products)} selected={progress['total']} workers={workers}; data_cutoff={time_label(at)}; "
                 'one fixed cutoff for all products; see final summary for actual coverage.')
        if notification_mode != 'shadow':
            if len(products) > limit:
                raise Unavailable('live_full_pool_required')
            if notification_mode == 'baseline':
                state = store.enable_baseline(checkpoint_sha256=baseline_digest, generation=state.generation,
                                              at=state.at, products=tuple(p.key for p in products))
            else:
                # Caller admitted an armed exact predecessor; no fresh baseline.
                state.notifications_enabled = True
                store.save(state, expected)
            expected = state.generation
        report = scan(products[:limit], at, ProductCache(workspace / "market"), state, budget,
                      workers=workers, progress=progress)
        progress['phase'] = 'checkpoint'
        completed_at = int(time.time() * 1000)
        report.update({"metadata_eligible": len(products), "excluded_metadata": excluded,
                       "coverage_capped": len(products) > limit, "state": state_reason, "run_token": token,
                       "started_at": started_at, "completed_at": completed_at,
                       "data_age_seconds": max(0, completed_at - at) // 1000,
                       "checkpoint_generation": state.generation + 1})
        raw = json.dumps(report, separators=(",", ":"), allow_nan=False).encode()
        if len(raw) > 16 * 1024 * 1024:
            raise Unavailable("report_byte_limit")
        store.save(state, expected)
        # A timeout after state replacement is not permission to roll back/send.
        atomic_write(workspace / "scan-report.json", raw)
        scan_log(f"Scan result saved: cutoff={time_label(at)} data_age_at_end={report['data_age_seconds']}s "
                 f"complete={report['complete']} partial={report['partial']} warmup={report['warmup']} "
                 f"deferred={report['deferred']}; HTTP={budget.attempts} retries={budget.retries}.")


if __name__ == "__main__":
    main()
