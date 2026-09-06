"""Public validation remains runnable without organization infrastructure."""

import re
from pathlib import Path

import yaml


def test_public_ci_is_hosted_read_only_and_runs_import_checks():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert workflow["permissions"] == {"contents": "read"}
    assert len(workflow["jobs"]) == 1
    job = next(iter(workflow["jobs"].values()))
    assert job["runs-on"] == "ubuntu-latest"
    assert "container" not in job and "services" not in job
    assert not job.get("continue-on-error")
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "scripts/ci/public_hygiene.py" in commands
    assert "python -m pytest -q" in commands
    assert "test_process_" in commands or "tests/process/" in commands
    assert all(token not in text for token in ("secrets.", "vars.", "ghcr.io", "workflow_dispatch", "self-hosted"))
    for step in job["steps"]:
        assert not step.get("continue-on-error")
        if action := step.get("uses"):
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
            if action.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False
