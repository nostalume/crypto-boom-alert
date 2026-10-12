"""Single local previews use real scanning/storage and fixture-only public HTTP."""
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_stage3 import Clock, response
from test_trial import PRODUCT, T, metadata, row

from crypto_boom import data, runtime, scan, state, trial


def fixture_job(workspace, *args):
    at = int((workspace / '.fixture-cutoff').read_text())
    clock = Clock()
    scan.Budget = lambda **kw: data.Budget(clock=clock.clock, sleep=clock.sleep, **kw)
    bars = {
        '1m': [row(t, 100 + (t - T + 6 * trial.MINUTE) / trial.MINUTE, 30_000)
               for t in range(T - 6 * trial.MINUTE, T + 11 * trial.MINUTE, trial.MINUTE)],
        '5m': [row(t, 100, 25_000) for t in range(T - trial.WEEK - 2 * trial.FIVE,
                                               T + 3 * trial.FIVE, trial.FIVE)],
        '1H': [row(t, 100, 25_000) for t in range(T - 168 * trial.HOUR, T + trial.HOUR, trial.HOUR)],
    }
    def handle(request):
        assert request.method == 'GET' and request.url.path in data.PATHS
        path = request.url.path
        if (workspace / '.fixture-fail-metadata').exists():
            raise httpx.ConnectError('fixture offline', request=request)
        if path == '/api/v5/public/time':
            return response([{'ts': str(at + 2000)}])
        if path == '/api/v5/public/instruments':
            return response([metadata(request.url.params['instType'])])
        if (workspace / '.fixture-fail-candles').exists():
            return httpx.Response(403, json={'code': 'fixture'})
        grain = {'1m': trial.MINUTE, '5m': trial.FIVE, '1H': trial.HOUR}[request.url.params['bar']]
        end, limit = int(request.url.params['after']), int(request.url.params['limit'])
        eligible = [r for r in bars[request.url.params['bar']] if int(r[0]) < end and int(r[0]) + grain <= at]
        return response(list(reversed(eligible))[:limit])
    scan.PublicClient = lambda b, **kw: data.PublicClient(b, transport=httpx.MockTransport(handle))
    scan.scan_job(workspace, *args)


def run_preview(root, monkeypatch, capsys, *, at=T, extra=(), spawned=False):
    root.mkdir(exist_ok=True)
    (root / '.fixture-cutoff').write_text(str(at))
    real_supervise = runtime.supervise
    def supervise(target, args, **kw):
        if spawned:
            return real_supervise(fixture_job, args, **kw)
        # Restore worker fixture patches in this parent after the inline run.
        with monkeypatch.context():
            old_budget, old_client = scan.Budget, scan.PublicClient
            try:
                fixture_job(*args)
            finally:
                scan.Budget, scan.PublicClient = old_budget, old_client
        return 0
    with monkeypatch.context() as patch:
        patch.setattr(runtime, 'supervise', supervise)
        patch.setattr(sys, 'argv', ['scan', '--workspace', str(root), *extra])
        scan.main()
    return json.loads(capsys.readouterr().out)


def checkpoint(root):
    return state.StateStore(root / 'checkpoint.json').load()[0]


@pytest.mark.parametrize('spawned', [False, True])
def test_cold_warm_and_same_cutoff_previews_persist_without_notifications(tmp_path, monkeypatch, capsys, spawned):
    first = run_preview(tmp_path, monkeypatch, capsys, spawned=spawned)
    assert first['pool'] == first['metadata_eligible'] == first['complete'] == 2
    assert not first['coverage_capped'] and first['checkpoint_generation'] == 2
    assert first['ledger_preserved'] and first['publication'] == 'disabled'
    initial = json.loads((tmp_path / 'scan-report.json').read_bytes())
    assert [t['stage'] for t in initial['transitions']] == ['OBSERVED', 'OBSERVED']
    assert len(list((tmp_path / 'market').glob('*.json.gz'))) == 2
    saved = checkpoint(tmp_path)
    assert saved.ledger == {} and not saved.notifications_enabled
    text = (tmp_path / 'preview.txt').read_text(encoding='utf-8')
    assert 'LOCAL PREVIEW' in text and 'ALT-USDT' in text and 'OBSERVED' in text
    second = run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE, spawned=spawned)
    report = json.loads((tmp_path / 'scan-report.json').read_bytes())
    assert second['checkpoint_generation'] == 3 and second['state'] == 'state_hit'
    assert second['complete'] == 2 and all(p['cache'] == 'cache_hit' for p in report['products'])
    assert report['transitions'] == [] and second['http_attempts'] < first['http_attempts']
    before = {p.name: p.read_bytes() for p in (tmp_path / 'market').glob('*.json.gz')}
    third = run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE, spawned=spawned)
    assert third['already_evaluated'] == 2 and third['http_attempts'] == 3
    assert third['checkpoint_generation'] == 4 and not checkpoint(tmp_path).notifications_enabled
    assert before == {p.name: p.read_bytes() for p in (tmp_path / 'market').glob('*.json.gz')}
    assert not (tmp_path / 'checkpoint.json.lock').exists()


@pytest.mark.parametrize('status', ['confirmed', 'pending', 'unknown'])
def test_preview_preserves_every_existing_ledger_entry(tmp_path, monkeypatch, capsys, status):
    run_preview(tmp_path, monkeypatch, capsys)
    store = state.StateStore(tmp_path / 'checkpoint.json')
    with store.writer():
        saved, _ = store.load()
        saved.notifications_enabled = True
        ids = saved.prepare([trial.Transition((trial.VERSION + '/A0', PRODUCT.key, 'UP', T), 'OBSERVED', T)], T)
        for key in ids:
            saved.ledger[key]['status'] = status
        saved.notifications_enabled = False
        store.save(saved, saved.generation)
    before = deepcopy(checkpoint(tmp_path).ledger)
    result = run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE)
    assert result['ledger_entries'] == 1 and result['ledger_preserved']
    assert checkpoint(tmp_path).ledger == before


@pytest.mark.parametrize('kind', ['armed', 'corrupt', 'missing', 'locked'])
def test_unsafe_state_is_refused_without_overwrite_or_http(tmp_path, monkeypatch, kind):
    store = state.StateStore(tmp_path / 'checkpoint.json')
    with store.writer():
        store.save(state.Checkpoint(), None)
        if kind == 'armed':
            saved, _ = store.load()
            saved.notifications_enabled = True
            store.save(saved, saved.generation)
    if kind == 'corrupt':
        store.path.write_bytes(b'corrupt existing checkpoint')
    if kind == 'missing':
        store.path.unlink()
        (tmp_path / 'market').mkdir()
    if kind == 'locked':
        store.lock_path.write_bytes(b'existing writer')
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    def forbidden(*args, **kw):
        pytest.fail('unsafe workspace must not launch scan or contact HTTP')
    monkeypatch.setattr(runtime, 'supervise', forbidden)
    monkeypatch.setattr(sys, 'argv', ['scan', '--workspace', str(tmp_path)])
    with pytest.raises(trial.Unavailable):
        scan.main()
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}


def test_partial_coverage_is_reported_then_recovers_from_cache(tmp_path, monkeypatch, capsys):
    (tmp_path / '.fixture-fail-candles').touch()
    first = run_preview(tmp_path, monkeypatch, capsys)
    assert first['complete'] == 0 and first['partial'] == 2
    assert 'partial=2' in (tmp_path / 'preview.txt').read_text(encoding='utf-8')
    assert checkpoint(tmp_path).tracker.last_at == {}
    (tmp_path / '.fixture-fail-candles').unlink()
    second = run_preview(tmp_path, monkeypatch, capsys)
    assert second['complete'] == 2 and second['partial'] == 0


def test_metadata_failure_keeps_previous_checkpoint_and_report(tmp_path, monkeypatch, capsys):
    run_preview(tmp_path, monkeypatch, capsys)
    before = {name: (tmp_path / name).read_bytes() for name in ('checkpoint.json', 'scan-report.json', 'preview.txt')}
    (tmp_path / '.fixture-fail-metadata').touch()
    with pytest.raises(data.BudgetExceeded):
        run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE)
    assert before == {name: (tmp_path / name).read_bytes() for name in before}
    assert not (tmp_path / 'checkpoint.json.lock').exists()


def test_capped_preview_is_not_claimed_as_full_pool(tmp_path, monkeypatch, capsys):
    result = run_preview(tmp_path, monkeypatch, capsys, extra=('--limit', '1', '--workers', '8'))
    assert result['pool'] == 1 and result['metadata_eligible'] == 2 and result['coverage_capped']
    assert '截断：True' in (tmp_path / 'preview.txt').read_text(encoding='utf-8')


def test_damaged_cache_does_not_reset_the_checkpoint_or_ledger(tmp_path, monkeypatch, capsys):
    run_preview(tmp_path, monkeypatch, capsys)
    partition = next((tmp_path / 'market').glob('*.json.gz'))
    partition.write_bytes(b'corrupt cache')
    result = run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE)
    assert result['complete'] == 2 and result['deferred'] == 0
    report = json.loads((tmp_path / 'scan-report.json').read_bytes())
    assert sum(p['cache'].startswith('cache_invalid:') for p in report['products']) == 1
    assert report['transitions'] == []  # Repairable price cache is not replay authority.
    assert result['checkpoint_generation'] == 3 and result['ledger_preserved']
    assert partition.read_bytes() != b'corrupt cache'


def test_commit_then_report_failure_recovers_without_replaying_changes(tmp_path, monkeypatch, capsys):
    run_preview(tmp_path, monkeypatch, capsys)
    old_preview = (tmp_path / 'preview.txt').read_bytes()
    original = scan.atomic_write
    def fail_report(path, raw):
        if path.name == 'scan-report.json':
            raise OSError('fixture report write failure')
        original(path, raw)
    with monkeypatch.context() as patch:
        patch.setattr(scan, 'atomic_write', fail_report)
        with pytest.raises(OSError, match='report write failure'):
            run_preview(tmp_path, patch, capsys, at=T + trial.MINUTE)
    assert checkpoint(tmp_path).generation == 3
    assert (tmp_path / 'preview.txt').read_bytes() == old_preview
    resumed = run_preview(tmp_path, monkeypatch, capsys, at=T + trial.MINUTE)
    assert resumed['checkpoint_generation'] == 4 and resumed['already_evaluated'] == 2
    assert json.loads((tmp_path / 'scan-report.json').read_bytes())['transitions'] == []


def test_checkpoint_link_is_refused_and_target_untouched(tmp_path, monkeypatch):
    target = tmp_path / 'original.json'
    target.write_bytes(b'original ledger')
    root = tmp_path / 'preview'
    root.mkdir()
    try:
        os.symlink(target, root / 'checkpoint.json')
    except OSError:
        pytest.skip('OS disallows symlink creation')
    monkeypatch.setattr(sys, 'argv', ['scan', '--workspace', str(root)])
    with pytest.raises(trial.Unavailable, match='local_workspace_link'):
        scan.main()
    assert target.read_bytes() == b'original ledger'
    assert not (root / 'checkpoint.json.lock').exists()


def test_module_help_and_invalid_cli_work_without_network():
    env = {**os.environ, 'PYTHONPATH': str(Path(__file__).parents[1] / 'src')}
    for args, code in [(('--help',), 0), (('--workspace', 'unused', '--limit', '0'), 2)]:
        result = subprocess.run([sys.executable, '-m', 'crypto_boom.scan', *args], env=env,
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == code
        if code == 0:
            assert '--workspace' in result.stdout and 'default 1000' in result.stdout


@pytest.mark.parametrize('fault', ['generation', 'publication', 'ledger'])
def test_current_report_must_match_saved_state_and_preserved_ledger(tmp_path, monkeypatch, fault):
    (tmp_path / '.fixture-cutoff').write_text(str(T))
    def faulty_supervise(target, args, **kw):
        old_budget, old_client = scan.Budget, scan.PublicClient
        try:
            fixture_job(*args)
        finally:
            scan.Budget, scan.PublicClient = old_budget, old_client
        path = tmp_path / 'scan-report.json'
        report = json.loads(path.read_bytes())
        if fault == 'generation':
            report['checkpoint_generation'] += 1
        elif fault == 'publication':
            report['publication'] = 'enabled'
        else:
            store = state.StateStore(tmp_path / 'checkpoint.json')
            with store.borrowed_writer(args[3]):
                saved, _ = store.load()
                saved.notifications_enabled = True
                saved.prepare([trial.Transition((trial.VERSION + '/A0', PRODUCT.key, 'UP', T), 'OBSERVED', T)], T)
                saved.notifications_enabled = False
                # Deliberately inject a valid but unauthorized same-generation replacement.
                state.atomic_write(store.path, state._checkpoint_bytes(saved))
        path.write_text(json.dumps(report), encoding='utf-8')
        return 0
    monkeypatch.setattr(runtime, 'supervise', faulty_supervise)
    monkeypatch.setattr(sys, 'argv', ['scan', '--workspace', str(tmp_path)])
    reason = 'shadow_ledger_changed' if fault == 'ledger' else 'shadow_checkpoint_mismatch'
    with pytest.raises(trial.Unavailable, match=reason):
        scan.main()
    assert not (tmp_path / 'preview.txt').exists()
    assert not (tmp_path / 'checkpoint.json.lock').exists()


def test_interrupted_first_scan_keeps_recovery_point_for_partial_price_cache(tmp_path, monkeypatch, capsys):
    from crypto_boom.runtime import Cancelled
    def interrupted(target, args, **kw):
        series = {g: trial.Series.parse([], g) for g in data.GRAINS.values()}
        data.ProductCache(tmp_path / 'market').save(data.Snapshot(PRODUCT, T, series))
        raise Cancelled('fixture early interruption')
    with monkeypatch.context() as patch:
        patch.setattr(runtime, 'supervise', interrupted)
        patch.setattr(sys, 'argv', ['scan', '--workspace', str(tmp_path)])
        with pytest.raises(Cancelled):
            scan.main()
    saved = checkpoint(tmp_path)
    assert saved.generation == 1 and saved.ledger == {} and not saved.notifications_enabled
    assert not (tmp_path / 'checkpoint.json.lock').exists()
    resumed = run_preview(tmp_path, monkeypatch, capsys)
    assert resumed['complete'] == 2 and resumed['checkpoint_generation'] == 2
