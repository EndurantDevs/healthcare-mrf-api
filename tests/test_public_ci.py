"""Public validation remains runnable without organization infrastructure."""

import re
from pathlib import Path

import yaml


METADATA_ONLY = (
    "github.event_name == 'pull_request' && github.event.action == 'edited' "
    "&& !github.event.changes.title && !github.event.changes.base"
)
JOB_LABELS = {
    "smoke": "portable import checks",
    "public-hygiene": "Repository checks",
    "readability-preflight": "Readability checks",
    "python-quality": "Python lint",
    "python-tests-0": "Python tests (1/4)",
    "python-tests-1": "Python tests (2/4)",
    "python-tests-2": "Python tests (3/4)",
    "python-tests-3": "Python tests (4/4)",
    "capacity-evidence": "Capacity tests",
    "api-contract": "API contract",
    "rust-scanner": "Rust tests",
    "container-package": "Container build",
    "security": "Security scans",
    "worker-queue-smoke": "Worker queue tests",
    "address-canonical-db-tests-core": "Database tests (core)",
    "address-canonical-db-tests-provider-directory": "Database tests (directory)",
    "address-canonical-db-tests-provider-profile": "Database tests (profiles)",
    "measurement": "Coverage results",
    "source-validation": "Validation complete",
}


def _assert_job_actions(job_id, job, revision) -> None:
    """Require hosted actions and the approved package before local setup."""
    has_pinned_checkout = False
    for step in job.get("steps", []):
        assert not step.get("continue-on-error")
        action = step.get("uses")
        if not action:
            continue
        if action == "./ci/scripts/healthcare/setup":
            assert has_pinned_checkout
            continue
        assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
        if not action.startswith("actions/checkout@"):
            continue
        assert step["with"]["persist-credentials"] is False
        if step["with"].get("repository") != "EndurantDevs/endurant-ci":
            continue
        assert step["with"]["ref"] == revision
        assert step["with"]["path"] == "ci"
        has_pinned_checkout = True
    assert has_pinned_checkout or job_id in {"smoke", "source-validation"}


def test_public_ci_is_hosted_read_only_and_runs_import_checks():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert set(workflow.get("on", workflow.get(True))["pull_request"]["types"]) == {
        "opened", "synchronize", "reopened", "edited",
    }
    assert workflow["permissions"] == {
        "contents": "read", "pull-requests": "read", "actions": "read",
    }
    assert set(workflow["jobs"]) == set(JOB_LABELS)
    revision = workflow["env"]["CI_REVISION"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    assert set(revision) != {"0"}
    assert "inputs.ci_revision" not in text
    assert workflow["run-name"] == "${{ " + METADATA_ONLY + " && 'CI metadata update' || 'CI' }}"
    assert workflow["concurrency"] == {
        "group": (
            "${{ " + METADATA_ONLY
            + " && format('ci-metadata-{0}', github.run_id) || format('ci-{0}', github.ref) }}"
        ),
        "cancel-in-progress": "${{ !(" + METADATA_ONLY + ") && github.ref != 'refs/heads/main' }}",
    }
    for job_id, job in workflow["jobs"].items():
        label = JOB_LABELS[job_id]
        assert job["name"] == "${{ " + METADATA_ONLY + f" && '{label} (metadata only)' || '{label}' " + "}}"
        condition = "always()" if job_id in {"measurement", "source-validation"} else "success()"
        assert job["if"] == "${{ !(" + METADATA_ONLY + ") && (" + condition + ") }}"
        assert "uses" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert not job.get("continue-on-error")
        assert all(permission in {"read", "none"} for permission in job.get("permissions", {}).values())
        _assert_job_actions(job_id, job, revision)
    job = workflow["jobs"]["smoke"]
    assert job["runs-on"] == "ubuntu-latest"
    assert "container" not in job
    assert "services" not in job
    assert not job.get("continue-on-error")
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "scripts/ci/public_hygiene.py" in commands
    assert "python -m pytest -q" in commands
    assert "test_process_" in commands or "tests/process/" in commands
    assert all(token not in text for token in ("secrets.", "vars.", "ghcr.io", "workflow_dispatch", "self-hosted"))
