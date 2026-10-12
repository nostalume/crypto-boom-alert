"""Portable uv entry and local cancellation; never register a host task."""
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import alert
import pytest
from test_trial import PRODUCT, T

from crypto_boom import state, trial
from crypto_boom.runtime import Cancelled


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setattr(alert, 'ROOT', tmp_path)
    monkeypatch.chdir(tmp_path)
    return tmp_path / '.local-preview'


def control(workspace):
    workspace.mkdir(exist_ok=True)
    value = {'token': 'a' * 32, 'pid': 123, 'interval_minutes': 10}
    (workspace / 'schedule.json').write_text(json.dumps(value))
    return value


def test_once_uses_relative_shared_scanner_without_pythonpath(project, monkeypatch):
    calls = []
    monkeypatch.setattr(alert.scan, 'main', lambda argv: calls.append(argv))
    assert alert.main(['once', '--limit', '8', '--workers', '8']) == 0
    assert calls == [['--workspace', '.local-preview', '--limit', '8', '--workers', '8']]


@pytest.mark.parametrize('name', ['schedule.json', 'stop.request'])
def test_once_cannot_interleave_with_scheduler_or_stale_control(project, monkeypatch, name):
    project.mkdir()
    (project / name).write_text('existing control')
    monkeypatch.setattr(alert.scan, 'main', lambda argv: pytest.fail('must not scan'))
    assert alert.main(['once']) == 2
    assert (project / name).read_text() == 'existing control'


def test_status_and_stop_without_registration_do_not_create_workspace(project, capsys):
    assert alert.main(['status']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['schedule_record'] is None and not result['writer_lock_present']
    assert alert.main(['stop']) == 0
    assert not project.exists()


@pytest.mark.parametrize('mode', ['normal', 'overrun', 'offline', 'cancel', 'interrupt', 'invalid'])
def test_watch_cadence_failure_and_cancellation_are_bounded(project, monkeypatch, capsys, mode):
    clock = SimpleNamespace(now=0.)
    fake_time = SimpleNamespace(monotonic=lambda: clock.now, sleep=lambda n: setattr(clock, 'now', clock.now + n))
    monkeypatch.setattr(alert, 'time', fake_time)
    starts = []
    def scan_once(argv, *, cancel):
        assert not cancel()
        starts.append(clock.now)
        if len(starts) == 1:
            if mode == 'overrun':
                clock.now += 70
            elif mode == 'offline':
                raise trial.Unavailable('shadow_child_failed:1')
            elif mode == 'cancel':
                raise Cancelled('cancelled')
            elif mode == 'interrupt':
                raise KeyboardInterrupt
            elif mode == 'invalid':
                raise trial.Unavailable('invalid checkpoint: manual diagnosis required')
        if len(starts) == 2:
            value = alert.record(project / 'schedule.json')
            (project / 'stop.request').write_text(value['token'])
    monkeypatch.setattr(alert.scan, 'main', scan_once)
    code = alert.main(['watch', '--interval', '1', '--limit', '8'])
    assert code == (130 if mode == 'interrupt' else 2 if mode == 'invalid' else 0)
    assert starts == ([0.] if mode in ('cancel', 'interrupt', 'invalid') else [0., 130.] if mode == 'overrun' else [0., 60.])
    assert not (project / 'schedule.json').exists() and not (project / 'stop.request').exists()
    if mode in ('normal', 'overrun', 'offline'):
        assert 'Waiting: next run at local ' in capsys.readouterr().err


def test_watch_does_not_claim_stop_if_worker_lock_remains(project, monkeypatch):
    def failed(argv, *, cancel):
        (project / 'checkpoint.json.lock').write_text('a' * 32)
        raise trial.Unavailable('child_not_reaped')
    monkeypatch.setattr(alert.scan, 'main', failed)
    assert alert.main(['watch']) == 2
    assert (project / 'schedule.json').exists() and (project / 'checkpoint.json.lock').exists()


def test_stop_requests_locally_but_does_not_claim_unacknowledged_completion(project, monkeypatch):
    value = control(project)
    clock = SimpleNamespace(now=0.)
    monkeypatch.setattr(alert, 'time', SimpleNamespace(monotonic=lambda: clock.now,
                        sleep=lambda n: setattr(clock, 'now', clock.now + n)))
    assert alert.main(['stop']) == 2
    assert (project / 'stop.request').read_text() == value['token']
    assert (project / 'schedule.json').exists() and clock.now <= 10.2


def test_stop_does_not_overwrite_another_instance_request(project):
    control(project)
    (project / 'stop.request').write_text('b' * 32)
    assert alert.main(['stop']) == 2
    assert (project / 'stop.request').read_text() == 'b' * 32


@pytest.mark.parametrize('armed', [False, True])
def test_explicit_post_crash_recovery_preserves_checkpoint_cache_and_ledger(project, armed):
    control(project)
    store = state.StateStore(project / 'checkpoint.json')
    with store.writer():
        initial = state.Checkpoint(at=T, notifications_enabled=True)
        initial.prepare([trial.Transition((trial.VERSION + '/A0', PRODUCT.key, 'UP', T), 'OBSERVED', T)], T)
        initial.notifications_enabled = armed
        # Initial save permits only a non-notifying baseline.
        initial.notifications_enabled = False
        store.save(initial, None)
        if armed:
            saved, _ = store.load()
            saved.notifications_enabled = True
            store.save(saved, saved.generation)
    (project / 'checkpoint.json.lock').write_text('c' * 32)
    (project / 'stop.request').write_text('a' * 32)
    market = project / 'market'
    market.mkdir()
    (market / 'preserved.cache').write_bytes(b'price cache')
    original = store.path.read_bytes()
    assert alert.main(['recover', '--confirm-stopped']) == (2 if armed else 0)
    assert store.path.read_bytes() == original and (market / 'preserved.cache').read_bytes() == b'price cache'
    for name in ('schedule.json', 'checkpoint.json.lock', 'stop.request'):
        assert (project / name).exists() is armed


def test_recovery_requires_confirmation_and_does_not_repair_missing_checkpoint(project):
    control(project)
    (project / 'market').mkdir()
    with pytest.raises(SystemExit) as exc:
        alert.main(['recover'])
    assert exc.value.code == 2 and (project / 'schedule.json').exists()
    assert alert.main(['recover', '--confirm-stopped']) == 0
    assert alert.main(['once']) == 2
    assert (project / 'market').exists() and not (project / 'checkpoint.json').exists()


@pytest.mark.parametrize('workspace', ['/absolute', '../other'])
def test_workspace_must_be_project_relative(project, workspace):
    with pytest.raises(SystemExit) as exc:
        alert.main(['--workspace', workspace, 'status'])
    assert exc.value.code == 2


def test_cancelled_loop_record_is_not_automatically_stolen(project, monkeypatch):
    value = control(project)
    monkeypatch.setattr(alert.scan, 'main', lambda *a, **kw: pytest.fail('must not scan'))
    assert alert.main(['watch']) == 2
    assert alert.record(project / 'schedule.json') == value


def test_uv_entry_relocates_and_can_cancel_a_blocked_owned_worker(tmp_path):
    uv = os.environ.get('UV_TEST_EXE') or shutil.which('uv')
    if not uv:
        pytest.skip('uv unavailable for executable integration')
    source = Path(__file__).resolve().parents[1]
    relocated = tmp_path / 'portable project with spaces'
    relocated.mkdir()
    for name in ('alert.py', 'pyproject.toml', 'uv.lock'):
        shutil.copy2(source / name, relocated / name)
    shutil.copytree(source / 'src', relocated / 'src', ignore=shutil.ignore_patterns('__pycache__'))
    env = {k: v for k, v in os.environ.items() if k != 'PYTHONPATH'}
    command = [uv, 'run', '--locked', '--no-dev', '--no-sync', '--offline']
    status = subprocess.run([*command, 'alert.py', 'status'], cwd=relocated, env=env,
                            capture_output=True, text=True, timeout=15)
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)['workspace'] == '.local-preview'
    launcher = relocated / 'fixture_watch.py'
    launcher.write_text("""import os, time
import alert
from crypto_boom import runtime

def blocked(workspace, *args):
    (workspace / 'entered').write_text(str(os.getpid()))
    time.sleep(15)

if __name__ == '__main__':
    real = runtime.supervise
    runtime.supervise = lambda target, args, **kw: real(blocked, args, **kw)
    raise SystemExit(alert.main())
""")
    process = subprocess.Popen([*command, 'fixture_watch.py', 'watch', '--limit', '8'], cwd=relocated,
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    workspace = relocated / '.local-preview'
    try:
        deadline = time.monotonic() + 12
        while not (workspace / 'entered').exists() and time.monotonic() < deadline and process.poll() is None:
            time.sleep(.05)
        assert (workspace / 'entered').exists(), 'Owned child did not start'
        result = subprocess.run([*command, 'alert.py', 'stop'], cwd=relocated, env=env,
                                capture_output=True, text=True, timeout=15)
        assert result.returncode == 0 and 'stopped; cache and ledger kept' in result.stdout, result.stderr
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)
        assert not (workspace / 'schedule.json').exists() and not (workspace / 'checkpoint.json.lock').exists()
        saved = json.loads((workspace / 'checkpoint.json').read_bytes())['state']
        assert saved['generation'] == 1 and saved['ledger'] == {} and not saved['notifications_enabled']
    finally:
        if process.poll() is None:
            if (workspace / 'schedule.json').exists():
                value = json.loads((workspace / 'schedule.json').read_text())
                (workspace / 'stop.request').write_text(value['token'])
            process.communicate(timeout=30)  # Fixture child is finite even if cooperative stop is broken.
