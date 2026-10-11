"""Finite no-publish scanner measurements, NOT persistent-storage acceptance.

Requires the development dependency psutil. CPU is waited child CPU on POSIX;
RSS is a sampled sum across the scan process tree, not unique physical memory.
Three different workloads are raw observations, not a latency-p95 estimate.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from crypto_boom.okx_state import StateStore, atomic_write

REPORT_LIMIT = 16 * 1024 * 1024
ROUND_SECONDS = 495
SAMPLE_SECONDS = 0.2
COUNTS = ("complete", "partial", "warmup", "already_evaluated", "deferred")


def _child_cpu():
    try:
        import resource
    except ImportError:
        return None
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return value.ru_utime + value.ru_stime


class ProcessSampler:
    """Owned thread observes only this child tree; disappearing processes are counted."""
    def __init__(self, pid):
        import psutil
        self.psutil = psutil
        self.pid = pid
        self.stop = threading.Event()
        self.peak = None
        self.samples = self.errors = 0
        self.failure = None
        self.thread = threading.Thread(target=self._run, daemon=False)

    def sample(self):
        try:
            root = self.psutil.Process(self.pid)
            processes = [root, *root.children(recursive=True)]
        except self.psutil.Error:
            self.errors += 1
            return
        rss, observed = 0, 0
        for process in processes:
            try:
                rss += process.memory_info().rss
                observed += 1
            except self.psutil.Error:
                self.errors += 1
        if observed:
            self.samples += 1
            self.peak = max(self.peak or 0, rss)

    def _run(self):
        try:
            while not self.stop.is_set():
                self.sample()
                self.stop.wait(SAMPLE_SECONDS)
        except Exception as exc:
            # Measurement failure is visible and never changes scan coverage.
            self.failure = type(exc).__name__

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()


def _stop_scan(process):
    # A POSIX session isolates all scanner descendants, including spawned workers.
    # Windows uses the scanner's own supervisor; failed outer cleanup is explicit.
    if os.name == "posix":
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                pass
            if sig == signal.SIGTERM:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        process.wait(timeout=5)
    else:
        import psutil
        try:
            children = psutil.Process(process.pid).children(recursive=True)
        except psutil.NoSuchProcess:
            children = []
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        process.kill()
        process.wait(timeout=5)
        _, alive = psutil.wait_procs(children, timeout=5)
        if alive:
            raise OSError("scan_cleanup_outcome_unknown")


def measure_scan(workspace: Path, round_number: int, limit: int, workers: int):
    command = [sys.executable, "-B", "-m", "crypto_boom.okx_scan", "--workspace", str(workspace),
               "--limit", str(limit), "--workers", str(workers)]
    start, before = time.monotonic(), _child_cpu()
    with (workspace / f"round-{round_number}-process.txt").open("wb") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=os.name == "posix")
        try:
            with ProcessSampler(process.pid) as sampler:
                code = process.wait(timeout=ROUND_SECONDS)
        except BaseException:
            _stop_scan(process)
            raise
    after = _child_cpu()
    return {"exit_code": code, "wall_seconds": time.monotonic() - start,
            "scan_tree_cpu_seconds": None if before is None or after is None else after - before,
            "cpu_method": "unsupported" if after is None else "waited_children_user_plus_system",
            "sampled_tree_rss_peak_bytes": sampler.peak, "rss_samples": sampler.samples,
            "rss_observation_errors": sampler.errors, "rss_interval_seconds": SAMPLE_SECONDS,
            "rss_observer_failure": sampler.failure}


def read_scan_report(workspace: Path, previous_token: str | None):
    with (workspace / "scan-report.json").open("rb") as stream:
        raw = stream.read(REPORT_LIMIT + 1)
    if len(raw) > REPORT_LIMIT:
        raise ValueError("benchmark_report_byte_limit")
    value = json.loads(raw)
    if (not isinstance(value, dict) or not isinstance(value.get("run_token"), str)
            or len(value["run_token"]) != 32 or value["run_token"] == previous_token
            or value.get("publication") != "disabled"
            or type(value.get("at")) is not int or value["at"] < 0
            or type(value.get("pool")) is not int or not 0 < value["pool"] <= 1000
            or type(value.get("coverage_capped")) is not bool
            or any(type(value.get(key)) is not int or value[key] < 0 for key in COUNTS)
            or sum(value[key] for key in COUNTS) != value["pool"]
            or not isinstance(value.get("products"), list)
            or len(value["products"]) != value["pool"]):
        raise ValueError("benchmark_report_contract")
    for key in ("http_attempts", "http_retries", "response_bytes", "metadata_eligible"):
        if type(value.get(key)) is not int or value[key] < 0:
            raise ValueError("benchmark_report_counter")
    if (value["http_attempts"] > 2400 or value["http_retries"] > value["http_attempts"]
            # The existing reader charges after yielding a <=64KiB chunk.
            # Up to eight in-flight workers can cross the soft charge guard;
            # observing that crossing is not an invalid scan report or a TCP cap.
            or value["response_bytes"] > 128 * 1024 * 1024 + 8 * 65536
            or value["metadata_eligible"] < value["pool"]
            or value["coverage_capped"] != (value["metadata_eligible"] > value["pool"])):
        raise ValueError("benchmark_report_budget_or_pool")
    if (workspace / "checkpoint.json.lock").exists():
        raise ValueError("benchmark_writer_still_present")
    with (workspace / "checkpoint.json").open("rb") as stream:
        state_raw = stream.read(8 * 1024 * 1024 + 1)
    if len(state_raw) > 8 * 1024 * 1024:
        raise ValueError("benchmark_state_byte_limit")
    checkpoint, reason = StateStore(workspace / "checkpoint.json").load()
    if checkpoint is None or reason != "state_hit" or checkpoint.notifications_enabled or checkpoint.at != value["at"]:
        raise ValueError("benchmark_state_not_shadow")
    summary = {key: value[key] for key in ("at", "pool", *COUNTS, "metadata_eligible", "coverage_capped",
                                          "http_attempts", "http_retries", "response_bytes")}
    summary["hour_context_counts"] = value.get("hour_context_counts")
    summary["read_byte_guard_crossed"] = value["response_bytes"] > 128 * 1024 * 1024
    summary["state_generation"] = checkpoint.generation
    summary["checkpoint_bytes"] = len(state_raw)
    # Sum real compressed cache files only, never tmp/locks/reports or billed ZIPs.
    files = sorted((workspace / "market").glob("*.json.gz"))
    if len(files) > 1000 or any(path.is_symlink() for path in files):
        raise ValueError("benchmark_cache_inventory")
    summary["market_cache_files"] = len(files)
    summary["market_cache_bytes"] = sum(path.stat().st_size for path in files)
    return summary, value["run_token"]


def benchmark(workspace: Path, *, rounds=3, limit=1000, workers=4):
    if (type(rounds) is not int or not 1 <= rounds <= 3 or type(limit) is not int or not 1 <= limit <= 1000
            or type(workers) is not int or workers not in (4, 8)):
        raise ValueError("benchmark_bounds")
    if not isinstance(workspace, Path) or workspace.exists() or workspace.is_symlink():
        raise ValueError("benchmark_fresh_workspace_required")
    workspace.mkdir(parents=True)
    report = {"schema": "okx-shadow-benchmark-v1", "status": "running", "requested_rounds": rounds,
              "limit": limit, "workers": workers, "publication": "disabled",
              "storage_scope": "ephemeral_same_runner_only", "rounds": [],
              "limits": ["no_cross_runner_restore", "no_latency_p95_claim", "no_24h_backlog_acceptance",
                         "rss_is_sampled_sum_not_unique_memory", "local_bytes_are_not_billed_storage"]}
    previous_token, previous_at, previous_generation = None, None, 0
    for number in range(1, rounds + 1):
        metrics = {"round": number}
        try:
            metrics.update(measure_scan(workspace, number, limit, workers))
            if metrics["exit_code"] != 0:
                raise OSError("benchmark_scan_failed")
            summary, token = read_scan_report(workspace, previous_token)
            if (summary["state_generation"] != previous_generation + 1
                    or (previous_at is not None and summary["at"] < previous_at)):
                raise ValueError("benchmark_generation_or_cutoff")
            metrics.update(summary)
            metrics["cutoff_advanced"] = None if previous_at is None else summary["at"] > previous_at
            previous_token, previous_at, previous_generation = token, summary["at"], summary["state_generation"]
        except (OSError, ValueError, subprocess.TimeoutExpired):
            metrics["status"] = "failed"
            report["rounds"].append(metrics)
            report["status"] = "failed"
            atomic_write(workspace / "benchmark-report.json", json.dumps(report, allow_nan=False).encode())
            raise
        metrics["status"] = "observed"
        report["rounds"].append(metrics)
        report["status"] = "observed" if number == rounds else "running"
        atomic_write(workspace / "benchmark-report.json", json.dumps(report, allow_nan=False).encode())
        print(json.dumps(metrics, allow_nan=False), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--rounds", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, choices=(4, 8), default=4)
    args = parser.parse_args()
    benchmark(args.workspace, rounds=args.rounds, limit=args.limit, workers=args.workers)


if __name__ == "__main__":
    main()
