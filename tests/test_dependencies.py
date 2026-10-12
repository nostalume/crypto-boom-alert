"""Dependency source and runner commands, independent of live market state."""
import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_uv_project_is_nonpackaged_with_separate_dev_group_and_one_lock():
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    lock = tomllib.loads((ROOT / 'uv.lock').read_text(encoding='utf-8'))
    assert project['tool']['uv']['package'] is False
    assert 'build-system' not in project
    assert 'constraint-dependencies' not in project['tool']['uv']
    assert len(project['project']['dependencies']) == 1
    assert project['project']['dependencies'][0].startswith('httpx==')
    assert {entry.split('==')[0].lower() for entry in project['dependency-groups']['dev']} == {'pytest', 'ruff', 'pyyaml'}
    assert lock['requires-python'].replace(' ', '') == project['project']['requires-python'].replace(' ', '')
    root = next(p for p in lock['package'] if p['name'] == project['project']['name'])
    assert root['source'] == {'virtual': '.'}
    assert not (ROOT / 'requirements.txt').exists()
    assert not (ROOT / 'requirements-dev.txt').exists()


def test_production_installs_runtime_only_and_executes_synced_project_python():
    workflow = yaml.safe_load((ROOT / '.github/workflows/okx-live.yml').read_text(encoding='utf-8'))
    assert workflow['env']['PYTHONPATH'] == 'src'
    for job in workflow['jobs'].values():
        scripts = [s['run'] for s in job['steps'] if 'run' in s]
        sync = [s for s in scripts if 'uv sync' in s]
        assert len(sync) == 1
        assert 'uv sync --locked --no-dev --no-cache --no-python-downloads' in sync[0]
        for script in scripts:
            for line in script.splitlines():
                if 'python -m crypto_boom.' in line:
                    assert line.strip().startswith('python -m crypto_boom.')
        assert not any('uv pip' in s or 'requirements.txt' in s for s in scripts)


def test_local_instructions_do_not_keep_a_parallel_requirements_authority():
    for path in (ROOT / 'README.md', ROOT / 'docs/okx-operations.md'):
        text = path.read_text(encoding='utf-8')
        assert 'uv sync --locked' in text
        assert 'uv run --locked --no-sync python -m pytest' in text
        assert 'requirements.txt' not in text
    assert '.venv/' in (ROOT / '.gitignore').read_text(encoding='utf-8').splitlines()


def test_all_runner_jobs_use_one_pinned_uv_environment_without_remote_dependency_cache():
    for name in ('ci.yml', 'okx-live.yml'):
        workflow = yaml.safe_load((ROOT / '.github/workflows' / name).read_text(encoding='utf-8'))
        for job in workflow['jobs'].values():
            steps = job['steps']
            setup = [s for s in steps if s.get('uses', '').startswith('astral-sh/setup-uv@')]
            assert len(setup) == 1
            assert re.fullmatch(r'astral-sh/setup-uv@[0-9a-f]{40}', setup[0]['uses'])
            assert re.fullmatch(r'\d+\.\d+\.\d+', setup[0]['with']['version'])
            assert setup[0]['with'] == {
                'version': setup[0]['with']['version'], 'python-version': '3.12', 'activate-environment': True,
                'enable-cache': False, 'cache-python': False,
            }
            sync = next(s for s in steps if 'uv sync' in s.get('run', ''))
            assert steps.index(setup[0]) < steps.index(sync)
            assert not any('setup-python@' in s.get('uses', '') or 'pip install' in s.get('run', '') for s in steps)
            for i, step in enumerate(steps):
                if 'run' in step:
                    assert step.get('shell', workflow['defaults']['run']['shell']) == 'bash'
                if 'python -m ' in step.get('run', ''):
                    assert i > steps.index(sync)


def test_shared_cache_scripts_preserve_independent_save_and_final_proof_steps():
    workflow = yaml.safe_load((ROOT / '.github/workflows/okx-live.yml').read_text(encoding='utf-8'))
    prepare, consume = [workflow['jobs'][job]['steps'] for job in ('prepare', 'consume')]
    guards = [next(s for s in steps if s.get('name', '').startswith('Check free headroom'))
              for steps in (prepare, consume)]
    proofs = [next(s for s in steps if s.get('name', '').startswith('${{ steps.'))
              for steps in (prepare, consume)]
    assert guards[0]['run'] == guards[1]['run']
    assert 'guard-save --root' in guards[0]['run']
    assert proofs[0]['run'] == proofs[1]['run']
    assert [s['name'] for s in proofs] == ['${{ steps.scan.outputs.proof }}', '${{ steps.publish.outputs.proof }}']
    assert proofs[0]['if'] == "steps.scan.outputs.persist == 'true' && steps.scan.outputs.has_intent == 'false'"
    assert 'if' not in proofs[1]
    for steps, guard, proof in zip((prepare, consume), guards, proofs):
        save = next(s for s in steps if s.get('uses', '').startswith('actions/cache/save@'))
        assert steps.index(guard) < steps.index(save) < steps.index(proof)
        for command in ('plan-cleanup', 'check-cleanup', 'finish'):
            assert f'crypto_boom.live {command} ' in proof['run']
        assert 'gh api -X DELETE' in proof['run']
