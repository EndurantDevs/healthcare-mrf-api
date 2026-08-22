"""Trust-boundary contracts for the public repository's ARC route."""

from __future__ import annotations

from pathlib import Path

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
PREPUSH = Path(__file__).resolve().parents[1] / "scripts/ci/prepush"
ARC_ELIGIBLE_EXPRESSION = (
    "(((github.event_name == 'push' || "
    "github.event_name == 'workflow_dispatch') && "
    "github.ref == 'refs/heads/main') || (inputs.activate_arc && "
    "github.event_name == 'pull_request' && "
    "github.workflow_ref == format('EndurantDevs/healthcare-mrf-api/"
    ".github/workflows/trusted-pr-ci.yml@refs/pull/{0}/merge', "
    "github.event.number) && "
    "github.repository == 'EndurantDevs/healthcare-mrf-api' && "
    "github.event.pull_request.base.ref == 'main' && "
    "github.event.pull_request.base.repo.full_name == github.repository && "
    "github.event.pull_request.head.repo.full_name == github.repository && "
    "github.event.pull_request.head.repo.fork == false && "
    "contains(fromJSON('[\"OWNER\",\"MEMBER\",\"COLLABORATOR\"]'), "
    "github.event.pull_request.author_association) && "
    "github.event.pull_request.user.type == 'User' && "
    "github.actor != 'dependabot[bot]' && "
    "!endsWith(github.actor, '[bot]') && "
    "!endsWith(github.triggering_actor, '[bot]')))"
)
KUBERNETES_RUNNER_EXPRESSION = (
    "${{ "
    + ARC_ELIGIBLE_EXPRESSION
    + " && vars.HEALTHCARE_MRF_CI_RUNNER || 'ubuntu-latest' }}"
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
        "workflow_call",
        "pull_request",
        "push",
        "workflow_dispatch",
    }
    assert {
        "pull_request_target",
        "workflow_run",
        "repository_dispatch",
    }.isdisjoint(trigger_document["on"])
    assert trigger_document["on"]["workflow_call"] == {
        "inputs": {
            "activate_arc": {
                "description": (
                    "Request ARC only after protected-main and fork checks pass"
                ),
                "required": "false",
                "type": "boolean",
                "default": "false",
            }
        }
    }
    assert document["jobs"].keys() == ARC_JOBS | HOSTED_JOBS
    for name in ARC_JOBS:
        job = document["jobs"][name]
        assert job["runs-on"] == KUBERNETES_RUNNER_EXPRESSION
        assert job["container"] == {"image": ARC_IMAGE}
        assert job["defaults"] == {"run": {"shell": "bash"}}
    for name in HOSTED_JOBS:
        job = document["jobs"][name]
        assert job["runs-on"] == "ubuntu-latest"
        assert "HEALTHCARE_MRF_CI_RUNNER" not in yaml.safe_dump(job)


def test_reusable_arc_classifier_is_exact_and_human_only() -> None:
    document, _ = _documents()

    for name in ARC_JOBS:
        job = document["jobs"][name]
        route = job["runs-on"]
        for required in (
            "inputs.activate_arc",
            "github.event_name == 'pull_request'",
            "github.workflow_ref == format('",
            ".github/workflows/trusted-pr-ci.yml@refs/pull/{0}/merge'",
            "github.event.number",
            "github.repository == 'EndurantDevs/healthcare-mrf-api'",
            "github.event.pull_request.base.ref == 'main'",
            "github.event.pull_request.base.repo.full_name == github.repository",
            "github.event.pull_request.head.repo.full_name == github.repository",
            "github.event.pull_request.head.repo.fork == false",
            "github.event.pull_request.author_association",
            "github.event.pull_request.user.type == 'User'",
            "github.actor != 'dependabot[bot]'",
            "!endsWith(github.actor, '[bot]')",
            "!endsWith(github.triggering_actor, '[bot]')",
        ):
            assert required in route
        assert '[\"OWNER\",\"MEMBER\",\"COLLABORATOR\"]' in route
        assert "inputs.activate_arc" not in route.split("||", 1)[0]
    for name in HOSTED_JOBS:
        assert document["jobs"][name]["runs-on"] == "ubuntu-latest"


def test_reusable_foundation_preserves_caller_checkout_and_pr_context() -> None:
    document, trigger_document = _documents()

    assert trigger_document["on"]["workflow_call"]["inputs"]["activate_arc"] == {
        "description": "Request ARC only after protected-main and fork checks pass",
        "required": "false",
        "type": "boolean",
        "default": "false",
    }
    assert document["env"]["COVERAGE_BASE_SHA"] == COVERAGE_BASE_EXPRESSION
    assert (
        document["jobs"]["python-quality"]["steps"][-1]["env"]
        ["READABILITY_ZERO_GROWTH_APPROVED"]
        == "${{ github.event_name == 'pull_request' && "
        "contains(github.event.pull_request.labels.*.name, "
        "'readability-zero-growth-approved') }}"
    )
    for checkout in (
        step
        for job in document["jobs"].values()
        for step in job["steps"]
        if isinstance(step.get("uses"), str)
        and step["uses"].startswith("actions/checkout@")
    ):
        assert checkout["with"]["persist-credentials"] is False
        assert "repository" not in checkout["with"]
        assert checkout["with"].get("ref", "${{ github.sha }}") == "${{ github.sha }}"


def test_matrices_fail_fast_and_coverage_waits_for_every_root_job() -> None:
    document, _ = _documents()
    jobs = document["jobs"]

    for name in ("python-tests", "address-canonical-db-tests"):
        assert jobs[name]["strategy"]["fail-fast"] is True
    assert set(jobs["test-coverage"]["needs"]) == {
        name for name, job in jobs.items() if "needs" not in job
    }


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
    prepush = PREPUSH.read_text(encoding="utf-8")

    assert document["env"]["COVERAGE_BASE_SHA"] == COVERAGE_BASE_EXPRESSION
    for name in ("public-hygiene", "python-quality"):
        steps = document["jobs"][name]["steps"]
        checkout = next(step for step in steps if "uses" in step)
        assert checkout["with"] == {
            "persist-credentials": False,
            "fetch-depth": 0,
        }
        policy = next(step for step in steps if step.get("name") == "Run shared pre-push gate")
        assert policy["run"] == f"scripts/ci/prepush {'hygiene' if name == 'public-hygiene' else 'quality'}"
    assert 'if [ "${GITHUB_EVENT_NAME:-}" = pull_request ]; then' in prepush
    assert 'python3 scripts/check_commit_messages.py --event "$GITHUB_EVENT_PATH"' in prepush
    assert '[[ ! "${COVERAGE_BASE_SHA:-}" =~ ^[0-9a-f]{40}$ ]]' in prepush
    assert 'python3 scripts/check_commit_messages.py --range "$COVERAGE_BASE_SHA..HEAD"' in prepush
