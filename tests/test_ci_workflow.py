"""Essential CI authority and failure reporting; no documentation word checks."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ci_only_runs_locked_checks_without_market_or_storage_effects():
    raw = (ROOT / '.github/workflows/ci.yml').read_text(encoding='utf-8')
    workflow = yaml.safe_load(raw)
    assert set(workflow['on']) == {'pull_request', 'workflow_dispatch'}
    assert workflow['permissions'] == {'contents': 'read'}
    assert set(workflow['jobs']) == {'checks'}
    job = workflow['jobs']['checks']
    assert 'permissions' not in job and 'environment' not in job
    for forbidden in ('secrets.', 'GH_TOKEN', 'actions/cache', 'upload-artifact', 'crypto_boom.live'):
        assert forbidden not in raw
    commands = [step['run'] for step in job['steps'] if 'run' in step]
    assert any('uv sync --locked' in command for command in commands)
    assert any('uv run --locked --no-sync pytest' in command for command in commands)
    assert any('ruff check --no-cache --select F,I,RUF100 alert.py src tests' in command for command in commands)


def test_ci_does_not_mask_failed_tests_or_lint():
    workflow = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text(encoding='utf-8'))
    steps = {step['id']: step for step in workflow['jobs']['checks']['steps'] if 'id' in step}
    assert all('continue-on-error' not in step for step in steps.values())
    assert steps['lint']['if'] == 'success() || failure()'
    summary = workflow['jobs']['checks']['steps'][-1]
    assert summary['if'] == 'always()' and 'GITHUB_STEP_SUMMARY' in summary['run']
