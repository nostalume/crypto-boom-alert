"""Offline bounded run snapshots; these tests never authenticate to GitHub."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import test_live as fixtures

from crypto_boom import live as live
from crypto_boom.storage import MIB
from crypto_boom.trial import MINUTE, VERSION

START = fixtures.T
END = START + 24 * 60 * MINUTE
FINGERPRINT = 'a' * 64


def put(root, name, value):
    (root / name).write_text(json.dumps(value), encoding='utf-8')


def sample(root, number, *, transitions=()):
    path = root / f'{number:04d}'
    path.mkdir(parents=True)
    created = START + (number - 1) * 10 * MINUTE
    at = created + MINUTE
    source = fixtures.SOURCE if number % 2 else 'c' * 40  # Normal README Git head change.
    run = fixtures.run(number, head_sha=source)
    run['created_at'] = datetime.fromtimestamp(created / 1000, timezone.utc).isoformat().replace('+00:00', 'Z')
    proof = {'manifest_sha256': 'd' * 64, 'checkpoint_sha256': 'e' * 64, 'generation': number, 'at': at}
    jobs = fixtures.jobs(proof, number=number)
    for job in jobs['jobs']:
        job['head_sha'] = source
    summary = {'job': 'prepare', 'mode': 'preview', 'blocked': None, 'publication': 'not_sent',
               'run_id': run['id'], 'source_sha': source, 'observed_at': at + 3000, 'at': at,
               'scan_elapsed_seconds': 3, 'cutoff_age_ms': 3000, 'pool': 2,
               'market_coverage': {'SPOT': {'complete': 1}, 'SWAP': {'warmup': 1}},
               'transition_counts': dict(live.Counter(t['stage'] for t in transitions)),
               'observation_window': {'start_ms': START, 'end_ms': END, 'digest': FINGERPRINT},
               **proof, 'final_key': fixtures.cache.live_key(fixtures.SCOPE, run['id'], source, 'final'),
               'previous_key': '' if number == 1 else fixtures.cache.live_key(
                   fixtures.SCOPE, (number - 1) * 10, fixtures.SOURCE if (number - 1) % 2 else 'c' * 40, 'final'),
               'final_cache_id': number, 'final_cache_bytes': 1000}
    put(path, 'run.json', run)
    put(path, 'jobs.json', jobs)
    log = ['prepare\tsummary\t2026-01-01T00:00:00Z OKX_RUN_V1 ' + json.dumps(summary)]
    log += ['prepare\tscan\t2026-01-01T00:00:00Z OKX_TRANSITION_V1 ' + json.dumps(
        {'run_id': run['id'], 'source_sha': source, **t}) for t in transitions]
    (path / 'prepare.log').write_text('\n'.join(log) + '\n', encoding='utf-8')
    return path, summary


def report(root, **kwargs):
    return live.observation_report(root, **{'start': START, 'end': END, 'fingerprint': FINGERPRINT, **kwargs})


def transition(stage='OBSERVED', kind='SPOT', first=START):
    return {'identity': [VERSION + '/A0', ['okx', kind, 'ALT-USDT' if kind == 'SPOT' else 'ALT-USDT-SWAP'], 'UP', first],
            'stage': stage, 'at': START}


def edit_summary(path, **changes):
    log = (path / 'prepare.log').read_text(encoding='utf-8').splitlines()
    prefix, raw = log[0].split('OKX_RUN_V1 ', 1)
    value = json.loads(raw)
    value.update(changes)
    log[0] = prefix + 'OKX_RUN_V1 ' + json.dumps(value)
    (path / 'prepare.log').write_text('\n'.join(log) + '\n', encoding='utf-8')


def test_report_orders_runs_keeps_markets_and_stages_separate_without_mail_claim(tmp_path):
    events = [transition(), transition(kind='SWAP'), transition('PERSISTENT'), transition('ENDED')]
    sample(tmp_path, 2, transitions=events)
    sample(tmp_path, 1, transitions=events[:2])
    before = {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    value = report(tmp_path)
    assert value['samples'] == value['valid_samples'] == value['continuous_samples'] == 2
    assert value['run_interval_ms'] == {'min': 10 * MINUTE, 'max': 10 * MINUTE}
    assert value['scan_elapsed_seconds'] == {'min': 3, 'max': 3}
    assert value['market_coverage'] == {'SPOT': {'complete': 2}, 'SWAP': {'warmup': 2}}
    assert value['duplicate_transitions']['OBSERVED'] == 2
    assert value['duplicate_transitions']['PERSISTENT'] == 0
    assert value['transitions']['ENDED'] == 1
    assert before == {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}


@pytest.mark.parametrize('fault', ['failed', 'missing_jobs', 'false_proof', 'wrong_digest', 'blocked',
                                  'no_summary', 'coverage', 'wrong_run', 'age', 'nan', 'duplicate_run', 'consumer_ran'])
def test_invalid_sample_is_not_healthy_and_ends_continuous_segment(tmp_path, fault):
    sample(tmp_path, 1)
    path, summary = sample(tmp_path, 2)
    sample(tmp_path, 3)
    if fault == 'failed':
        run = json.loads((path / 'run.json').read_text())
        run['conclusion'] = 'failure'
        put(path, 'run.json', run)
    elif fault == 'missing_jobs':
        (path / 'jobs.json').unlink()
    elif fault == 'false_proof':
        edit_summary(path, manifest_sha256='f' * 64)
    elif fault == 'wrong_digest':
        edit_summary(path, observation_window={**summary['observation_window'], 'digest': 'b' * 64})
    elif fault == 'blocked':
        edit_summary(path, blocked='cache_miss')
    elif fault == 'no_summary':
        (path / 'prepare.log').write_text('failed before summary')
    elif fault == 'coverage':
        edit_summary(path, market_coverage={'SPOT': {'complete': 2}})
    elif fault == 'wrong_run':
        edit_summary(path, run_id=123456)
    elif fault == 'age':
        edit_summary(path, cutoff_age_ms=0)
    elif fault == 'nan':
        edit_summary(path, scan_elapsed_seconds=float('nan'))
    elif fault == 'consumer_ran':
        jobs = fixtures.jobs({k: summary[k] for k in ('manifest_sha256', 'checkpoint_sha256', 'generation', 'at')},
                             consume=True, number=2)
        for job in jobs['jobs']:
            job['head_sha'] = summary['source_sha']
        put(path, 'jobs.json', jobs)
    else:
        put(path, 'run.json', json.loads((tmp_path / '0001/run.json').read_text()))
    value = report(tmp_path)
    assert value['valid_samples'] == 2
    assert value['continuous_samples'] <= 1
    assert len([r for r in value['runs'] if not r['valid']]) == 1


def test_missing_run_or_state_handoff_is_reported_not_invented(tmp_path):
    sample(tmp_path, 1)
    sample(tmp_path, 3)
    value = report(tmp_path)
    assert value['valid_samples'] == 2 and value['continuous_samples'] == 1
    assert value['gaps']
    assert value['run_interval_ms']['max'] == 20 * MINUTE


def test_empty_window_not_a_successful_zero_signal_day(tmp_path):
    value = report(tmp_path)
    assert value['samples'] == value['valid_samples'] == 0
    assert value['scan_elapsed_seconds'] == {'min': None, 'max': None}


def test_provider_job_times_separate_queue_from_job_and_child_scan(tmp_path):
    path, _ = sample(tmp_path, 1)
    jobs = json.loads((path / 'jobs.json').read_text())
    created = datetime.fromtimestamp(START / 1000, timezone.utc)
    for field, offset in [('started_at', 10000), ('completed_at', 70000)]:
        jobs['jobs'][0][field] = datetime.fromtimestamp(created.timestamp() + offset / 1000, timezone.utc).isoformat().replace('+00:00', 'Z')
    put(path, 'jobs.json', jobs)
    value = report(tmp_path)
    assert value['queue_to_prepare_start_ms'] == {'min': 10000, 'max': 10000}
    assert value['prepare_job_elapsed_ms'] == {'min': 60000, 'max': 60000}
    assert value['scan_elapsed_seconds'] == {'min': 3, 'max': 3}


def test_planned_drain_is_not_a_failed_scan_or_a_healthy_sample(tmp_path):
    path, _ = sample(tmp_path, 144)
    edit_summary(path, status='draining')
    jobs = json.loads((path / 'jobs.json').read_text())
    jobs['jobs'][0]['steps'] = []
    put(path, 'jobs.json', jobs)
    value = report(tmp_path)
    assert value['valid_samples'] == 0 and value['planned_drain_samples'] == 1
    assert value['runs'][0]['disposition'] == 'planned_drain_no_scan'


@pytest.mark.parametrize('kwargs', [{'start': True}, {'end': START}, {'end': END + 1}, {'fingerprint': 'bad'}])
def test_report_contract_bounds(tmp_path, kwargs):
    with pytest.raises(ValueError, match='report_inputs'):
        report(tmp_path, **kwargs)


def test_directory_and_byte_bounds_stop_instead_of_truncating(tmp_path, monkeypatch):
    for n in range(201):
        (tmp_path / str(n)).mkdir()
    with pytest.raises(ValueError, match='directory_bound'):
        report(tmp_path)
    root = tmp_path / '0'
    sample(root, 1)
    original = Path.stat
    def stat(path, **kwargs):
        if path.name == 'prepare.log':
            from types import SimpleNamespace
            return SimpleNamespace(st_size=257 * MIB)
        return original(path, **kwargs)
    monkeypatch.setattr(Path, 'stat', stat)
    with pytest.raises(ValueError, match='byte_bound'):
        report(root)
