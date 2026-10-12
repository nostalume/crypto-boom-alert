"""Offline CI and operator guide stay outside the production signal owner."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ci_is_one_readonly_lane_without_market_or_storage_effects():
    raw = (ROOT / '.github/workflows/ci.yml').read_text(encoding='utf-8')
    value = yaml.safe_load(raw)
    assert set(value['on']) == {'pull_request', 'workflow_dispatch'}
    assert value['on']['pull_request']['branches'] == ['main']
    assert value['permissions'] == {'contents': 'read'}
    assert value['concurrency']['cancel-in-progress'] is True
    assert set(value['jobs']) == {'checks'}
    job = value['jobs']['checks']
    assert job['runs-on'] == 'ubuntu-latest' and job['timeout-minutes'] == 15
    assert 'permissions' not in job and 'environment' not in job
    for step in job['steps']:
        if 'uses' in step:
            assert step['uses'].startswith(('actions/checkout@', 'actions/setup-python@'))
            assert len(step['uses'].split('@')[1]) == 40
        else:
            assert step['shell'] == 'bash' and '${{' not in step['run']
    for forbidden in ('secrets.', 'GH_TOKEN', 'actions/cache', 'upload-artifact', 'crypto_boom.okx_live', 'okx_benchmark'):
        assert forbidden not in raw
    assert 'persist-credentials: false' in raw and 'python-version: "3.12"' in raw
    assert 'pytest -q -p no:cacheprovider' in raw and '--select F,I,RUF100 src tests' in raw
    assert '-r requirements.txt -r requirements-dev.txt' in raw


def test_ci_summary_records_both_checks_without_masking_failures():
    value = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text(encoding='utf-8'))
    steps = {s['id']: s for s in value['jobs']['checks']['steps'] if 'id' in s}
    assert 'continue-on-error' not in steps['tests'] and 'continue-on-error' not in steps['lint']
    assert steps['lint']['if'] == 'success() || failure()'
    summary = value['jobs']['checks']['steps'][-1]
    assert summary['if'] == 'always()' and 'GITHUB_STEP_SUMMARY' in summary['run']
    assert set(summary['env']) == {'TEST_OUTCOME', 'LINT_OUTCOME'}


def test_operations_guide_describes_direct_start_and_fail_closed_recovery():
    raw = (ROOT / 'docs/okx-operations.md').read_text(encoding='utf-8')
    for phrase in ('不要求 24 小时观察', '未知发布', '盲目重发', '先停新 writer',
                   '不等于邮件已经送达', '0 停用预算', '先暂停 workflow'):
        assert phrase in raw
    assert 'PYTHONPATH=src' in raw
    candidate = yaml.safe_load((ROOT / '.github/workflows/okx-live.yml').read_text(encoding='utf-8'))
    assert set(candidate['on']) == {'workflow_dispatch'}
    assert all(key in raw for key in ('preview', 'live', 'rebuild', 'arm_manifest',
                                     'observation_start_ms', 'observation_end_ms', 'observation_digest'))
