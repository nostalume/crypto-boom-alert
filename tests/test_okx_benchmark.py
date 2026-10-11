"""Finite diagnostic orchestration; fixture scans make no network calls."""
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from crypto_boom import okx_benchmark as bench
from crypto_boom.okx_state import Checkpoint, StateStore

T = 1780000020000


def write_report(root, number=1, *, at=T, status="complete"):
    root.mkdir(parents=True, exist_ok=True)
    store = StateStore(root / "checkpoint.json")
    state, _ = store.load()
    expected = None if state is None else state.generation
    state = state or Checkpoint()
    state.at = at
    with store.writer():
        store.save(state, expected)
    counts = {key: int(key == status) for key in bench.COUNTS}
    value = {"run_token": f"{number:032x}", "at": at, "pool": 1, **counts,
             "publication": "disabled", "metadata_eligible": 1, "coverage_capped": False,
             "http_attempts": 4, "http_retries": 0, "response_bytes": 100,
             "products": [{"product": ["okx", "SPOT", "BTC-USDT"], "status": status}],
             "hour_context_counts": {"not_requested": 1}}
    (root / "scan-report.json").write_text(json.dumps(value))
    market = root / "market"
    market.mkdir(exist_ok=True)
    (market / "one.json.gz").write_bytes(b"fixture-compressed-bytes")
    (market / "ignored.tmp").write_bytes(b"temporary-not-a-cache")
    return value


def install_fake_scan(monkeypatch, calls, *, failure=None, same_token=False, backwards=False):
    def scan(root, number, limit, workers):
        calls.append((number, limit, workers))
        if number == failure:
            return {"exit_code": 7, "wall_seconds": 1.0}
        write_report(root, 1 if same_token else number, at=T - (60000 if backwards and number == 2 else 0))
        return {"exit_code": 0, "wall_seconds": 1.0, "scan_tree_cpu_seconds": None,
                "cpu_method": "unsupported", "sampled_tree_rss_peak_bytes": None,
                "rss_samples": 0, "rss_observation_errors": 1, "rss_interval_seconds": 0.2}
    monkeypatch.setattr(bench, "measure_scan", scan)


def test_three_finite_rounds_reuse_shadow_state_without_inventing_cutoffs(tmp_path, monkeypatch, capsys):
    calls = []
    install_fake_scan(monkeypatch, calls)
    root = tmp_path / "fresh"
    result = bench.benchmark(root)
    assert calls == [(1, 1000, 4), (2, 1000, 4), (3, 1000, 4)]
    assert result["status"] == "observed" and result["publication"] == "disabled"
    assert result["storage_scope"] == "ephemeral_same_runner_only"
    assert [row["state_generation"] for row in result["rounds"]] == [1, 2, 3]
    assert [row["cutoff_advanced"] for row in result["rounds"]] == [None, False, False]
    assert all(row["sampled_tree_rss_peak_bytes"] is None for row in result["rounds"])
    assert all(row["market_cache_bytes"] == len(b"fixture-compressed-bytes") for row in result["rounds"])
    assert json.loads((root / "benchmark-report.json").read_text()) == result
    assert len(capsys.readouterr().out.splitlines()) == 3
    assert not (root / "checkpoint.json.lock").exists()


@pytest.mark.parametrize("kwargs", [{"rounds": 0}, {"rounds": 4}, {"rounds": True}, {"limit": 0},
                                     {"limit": 1001}, {"limit": True}, {"workers": 3}, {"workers": True}])
def test_bounds_precede_workspace_creation(tmp_path, kwargs):
    root = tmp_path / "unused"
    with pytest.raises(ValueError, match="bounds"):
        bench.benchmark(root, **kwargs)
    assert not root.exists()


def test_existing_workspace_never_consumed_or_overwritten(tmp_path, monkeypatch):
    calls = []
    install_fake_scan(monkeypatch, calls)
    before = write_report(tmp_path)
    with pytest.raises(ValueError, match="fresh_workspace"):
        bench.benchmark(tmp_path)
    assert not calls and json.loads((tmp_path / "scan-report.json").read_text()) == before


@pytest.mark.parametrize("variant", ["exit", "stale", "backwards"])
def test_failure_stops_without_fourth_round_and_keeps_diagnostic(tmp_path, monkeypatch, variant):
    calls = []
    install_fake_scan(monkeypatch, calls, failure=2 if variant == "exit" else None,
                      same_token=variant == "stale", backwards=variant == "backwards")
    root = tmp_path / "failure"
    with pytest.raises((ValueError, OSError)):
        bench.benchmark(root)
    value = json.loads((root / "benchmark-report.json").read_text())
    assert [row["round"] for row in value["rounds"]] == [1, 2]
    assert value["status"] == value["rounds"][-1]["status"] == "failed"
    assert len(calls) == 2


@pytest.mark.parametrize("field,value", [("publication", "enabled"), ("run_token", "short"),
                                         ("pool", True), ("complete", -1), ("complete", 0),
                                         ("products", []), ("coverage_capped", True),
                                         ("http_attempts", 2401), ("http_retries", 5),
                                         ("response_bytes", 128 * 1024 * 1024 + 1),
                                         ("metadata_eligible", 0)])
def test_bad_report_cannot_claim_success(tmp_path, field, value):
    body = write_report(tmp_path)
    body[field] = value
    (tmp_path / "scan-report.json").write_text(json.dumps(body))
    with pytest.raises(ValueError):
        bench.read_scan_report(tmp_path, None)


def test_unreleased_writer_and_enabled_state_are_not_shadow_acceptance(tmp_path):
    write_report(tmp_path)
    lock = tmp_path / "checkpoint.json.lock"
    lock.write_text("foreign writer")
    with pytest.raises(ValueError, match="writer"):
        bench.read_scan_report(tmp_path, None)
    lock.unlink()
    store = StateStore(tmp_path / "checkpoint.json")
    state, _ = store.load()
    state.notifications_enabled = True
    with store.writer():
        store.save(state, 1)
    with pytest.raises(ValueError, match="not_shadow"):
        bench.read_scan_report(tmp_path, None)


def test_bad_state_hash_and_oversize_report_fail(tmp_path):
    write_report(tmp_path)
    (tmp_path / "checkpoint.json").write_bytes(b"{}")
    with pytest.raises(ValueError, match="not_shadow"):
        bench.read_scan_report(tmp_path, None)
    (tmp_path / "scan-report.json").write_bytes(b"x" * (bench.REPORT_LIMIT + 1))
    with pytest.raises(ValueError, match="byte_limit"):
        bench.read_scan_report(tmp_path, None)


def test_already_evaluated_remains_explicit(tmp_path):
    write_report(tmp_path, status="already_evaluated")
    summary, _ = bench.read_scan_report(tmp_path, None)
    assert summary["complete"] == 0 and summary["already_evaluated"] == 1


def test_sampler_counts_disappeared_processes_without_faking_zero_peak():
    class Gone(Exception):
        pass
    def disappeared(pid):
        raise Gone()
    sampler = object.__new__(bench.ProcessSampler)
    sampler.psutil = SimpleNamespace(Process=disappeared, Error=Gone)
    sampler.pid, sampler.samples, sampler.errors, sampler.peak = 1, 0, 0, None
    sampler.sample()
    assert sampler.errors == 1 and sampler.samples == 0 and sampler.peak is None
    children = [SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=20))]
    root = SimpleNamespace(children=lambda **kwargs: children, memory_info=lambda: SimpleNamespace(rss=10))
    sampler.psutil.Process = lambda pid: root
    sampler.sample()
    assert sampler.samples == 1 and sampler.peak == 30


def test_real_local_process_measurement_has_bounded_scanner_command(tmp_path, monkeypatch):
    original = subprocess.Popen
    commands = []
    def spawn(command, **kwargs):
        commands.append(command)
        return original([sys.executable, "-c", "import time; time.sleep(.25)"], **kwargs)
    monkeypatch.setattr(bench.subprocess, "Popen", spawn)
    result = bench.measure_scan(tmp_path, 1, 8, 4)
    assert result["exit_code"] == 0 and result["wall_seconds"] >= .25
    assert result["rss_samples"] > 0 and result["sampled_tree_rss_peak_bytes"] > 0
    assert commands[0][1:4] == ["-B", "-m", "crypto_boom.okx_scan"]
    assert commands[0][-4:] == ["--limit", "8", "--workers", "4"]


def test_outer_timeout_reaps_local_child_and_stops_sampler(tmp_path, monkeypatch):
    original = subprocess.Popen
    children = []
    def spawn(command, **kwargs):
        process = original([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(process)
        return process
    monkeypatch.setattr(bench.subprocess, "Popen", spawn)
    monkeypatch.setattr(bench, "ROUND_SECONDS", .15)
    with pytest.raises(subprocess.TimeoutExpired):
        bench.measure_scan(tmp_path, 1, 8, 4)
    assert len(children) == 1 and children[0].poll() is not None
