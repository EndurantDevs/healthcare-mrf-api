"""Trust-boundary contracts for the public repository's ARC route."""

from __future__ import annotations

from pathlib import Path

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
ARC_RUNNER_EXPRESSION = (
    "${{ (github.event_name == 'push' || "
    "github.event_name == 'workflow_dispatch') && "
    "github.ref == 'refs/heads/main' && "
    "vars.HEALTHCARE_MRF_CI_RUNNER || 'ubuntu-latest' }}"
)
COVERAGE_BASE_EXPRESSION = (
    "${{ github.event_name == 'pull_request' && "
    "github.event.pull_request.base.sha || "
    "github.event_name == 'workflow_dispatch' && inputs.base_sha || "
    "github.event.before }}"
)
ARC_IMAGE = (
    "ghcr.io/endurantdevs/healthcare-mrf-api-arc-ci@sha256:"
    "12320cf489cc218f00d04e9df21d12f60bacd883e5120bd8dabb7ba0378c972c"
)
ARC_JOBS = {
    "public-hygiene",
    "python-quality",
    "capacity-evidence",
    "test-coverage",
    "api-contract",
}
HOSTED_JOBS = {
    "python-tests",
    "rust-scanner",
    "container-package",
    "security",
    "worker-queue-smoke",
    "address-canonical-db-tests",
}
COMMIT_POLICY_SCRIPT = """if [ "$GITHUB_EVENT_NAME" = "pull_request" ]; then
  python3 scripts/check_commit_messages.py --event "$GITHUB_EVENT_PATH"
else
  if [[ ! "$COVERAGE_BASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
    echo "COVERAGE_BASE_SHA must be an exact commit SHA" >&2
    exit 2
  fi
  python3 scripts/check_commit_messages.py --range "$COVERAGE_BASE_SHA..HEAD"
fi
"""


def _values_for_key(value: object, key: str) -> list[object]:
    matches: list[object] = []
    if isinstance(value, dict):
        for nested_key, nested_value in value.items():
            if nested_key == key:
                matches.append(nested_value)
            matches.extend(_values_for_key(nested_value, key))
    elif isinstance(value, list):
        for nested_value in value:
            matches.extend(_values_for_key(nested_value, key))
    return matches


def _documents() -> tuple[dict, dict]:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    return yaml.safe_load(workflow), yaml.load(workflow, Loader=yaml.BaseLoader)


def test_arc_route_is_trusted_main_only_with_a_hosted_fallback() -> None:
    document, trigger_document = _documents()

    assert set(trigger_document["on"]) == {
        "pull_request",
        "push",
        "workflow_dispatch",
    }
    assert {
        "pull_request_target",
        "workflow_run",
        "repository_dispatch",
    }.isdisjoint(trigger_document["on"])
    assert document["jobs"].keys() == ARC_JOBS | HOSTED_JOBS
    for name in ARC_JOBS:
        assert document["jobs"][name]["runs-on"] == ARC_RUNNER_EXPRESSION
    for name in HOSTED_JOBS:
        job = document["jobs"][name]
        assert job["runs-on"] == "ubuntu-latest"
        assert "HEALTHCARE_MRF_CI_RUNNER" not in yaml.safe_dump(job)


def test_arc_jobs_are_secretless_and_privilege_free() -> None:
    document, _ = _documents()

    for name in ARC_JOBS:
        job = document["jobs"][name]
        assert job["container"] == {"image": ARC_IMAGE}
        assert job["defaults"] == {"run": {"shell": "bash"}}
        assert job.get("permissions", document["permissions"]) == {"contents": "read"}
        assert "secrets." not in yaml.safe_dump(job)
        assert "environment" not in job
        assert "services" not in job
        for key in ("credentials", "options", "volumes", "ports"):
            assert not _values_for_key(job, key), f"{name} must not define {key}"


def test_arc_jobs_use_the_pinned_image_toolchain() -> None:
    document, _ = _documents()

    for name in ARC_JOBS:
        actions = _values_for_key(document["jobs"][name], "uses")
        assert not any(
            isinstance(action, str) and action.startswith("actions/setup-python@")
            for action in actions
        )


def test_commit_policy_uses_git_for_protected_main_without_event_file() -> None:
    document, _ = _documents()

    assert document["env"]["COVERAGE_BASE_SHA"] == COVERAGE_BASE_EXPRESSION
    for name in ("public-hygiene", "python-quality"):
        steps = document["jobs"][name]["steps"]
        checkout = next(step for step in steps if "uses" in step)
        assert checkout["with"] == {
            "persist-credentials": False,
            "fetch-depth": 0,
        }
        policy = next(step for step in steps if step.get("name") == "Commit message policy")
        assert policy["run"] == COMMIT_POLICY_SCRIPT
