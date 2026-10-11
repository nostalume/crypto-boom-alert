"""Manual shadow workflow's trigger, isolation and no-publish contract."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/okx-shadow-acceptance.yml"


def workflow():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_shadow_is_manual_read_only_and_separate_from_production():
    value = workflow()
    assert value["on"] == {"workflow_dispatch": None}
    assert value["permissions"] == {"contents": "read"}
    assert value["concurrency"] == {"group": "okx-shadow-manual", "cancel-in-progress": False}
    assert set(value["jobs"]) == {"shadow"}
    job = value["jobs"]["shadow"]
    assert job["runs-on"] == "ubuntu-latest" and job["timeout-minutes"] == 30
    assert "permissions" not in job and "environment" not in job
    assert job["env"] == {"PYTHONPATH": "src", "PYTHONDONTWRITEBYTECODE": "1",
                          "UV_NO_CACHE": "1", "PIP_NO_CACHE_DIR": "1"}


def test_shadow_remote_actions_are_pinned_without_persistent_cache_or_credentials():
    actions = [step for step in workflow()["jobs"]["shadow"]["steps"] if "uses" in step]
    assert [(step["uses"], step["with"]) for step in actions] == [
        ("actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd", {"persist-credentials": False}),
        ("actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1", {"python-version": "3.12"}),
    ]
    raw = WORKFLOW.read_text(encoding="utf-8")
    assert "${{" not in raw and "secrets:" not in raw
    assert "GITHUB_TOKEN" not in raw and "ACTIONS_RUNTIME_TOKEN" not in raw
    assert "--publish-readme" not in raw


def test_shadow_runs_canonical_checks_and_bounded_ephemeral_probe():
    steps = [step for step in workflow()["jobs"]["shadow"]["steps"] if "run" in step]
    assert len(steps) == 4 and all(step["shell"] == "bash" for step in steps)
    assert 'uv pip install --python "$(command -v python)" --no-cache -r requirements.txt -r requirements-dev.txt' in steps[0]["run"]
    assert 'python -m pytest -q -p no:cacheprovider --basetemp "$RUNNER_TEMP/okx-shadow-tests"' in steps[1]["run"]
    assert 'python -m ruff check --no-cache --select F,I,RUF100 src tests' in steps[1]["run"]
    assert steps[2]["run"].strip() == (
        'python -m crypto_boom.okx_benchmark --workspace "$RUNNER_TEMP/okx-shadow-scan" '
        '--rounds 3 --limit 1000 --workers 4 | tee "$RUNNER_TEMP/okx-shadow-report.txt"')
    assert steps[3]["if"] == "always()" and '>> "$GITHUB_STEP_SUMMARY"' in steps[3]["run"]
    assert "No benchmark report" in steps[3]["run"]
