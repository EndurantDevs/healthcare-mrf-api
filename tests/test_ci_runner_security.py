"""Security contracts for public CI and its future ephemeral ARC route."""

from __future__ import annotations

import re
from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPOSITORY_ROOT / ".github" / "workflows"
FULL_COMMIT_ACTION = re.compile(r"^[^./\s][^@\s]*@[0-9a-f]{40}$")


def _workflow(path: str) -> str:
    return (WORKFLOW_ROOT / path).read_text(encoding="utf-8")


def _workflow_paths() -> list[Path]:
    return sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))


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


def test_every_external_action_is_pinned_to_a_full_commit() -> None:
    for workflow_path in _workflow_paths():
        document = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        for action in _values_for_key(document, "uses"):
            assert isinstance(action, str)
            if action.startswith("./"):
                continue
            assert FULL_COMMIT_ACTION.fullmatch(action), (
                f"{workflow_path.name} must pin external Action {action!r} "
                "to a full commit"
            )


def test_checkout_never_persists_the_workflow_token() -> None:
    for workflow_path in _workflow_paths():
        workflow = workflow_path.read_text(encoding="utf-8")
        checkout_count = workflow.count("uses: actions/checkout@")
        assert workflow.count("persist-credentials: false") >= checkout_count


def test_ci_image_publisher_is_hosted_and_has_bounded_permissions() -> None:
    workflow = _workflow("publish-arc-ci-image.yml")
    document = yaml.safe_load(workflow)
    validate = document["jobs"]["validate"]
    publish = document["jobs"]["publish"]

    assert validate["runs-on"] == "ubuntu-latest"
    assert validate["permissions"] == {"contents": "read"}
    assert publish["runs-on"] == "ubuntu-latest"
    assert publish["needs"] == "validate"
    assert publish["permissions"] == {"contents": "read", "packages": "write"}
    assert "github.event_name != 'pull_request'" in publish["if"]
    assert "github.ref == 'refs/heads/main'" in publish["if"]
    assert "pull_request:" in workflow
    assert "pull_request_target" not in workflow
    assert "secrets.GITHUB_TOKEN" in workflow
    assert "secrets." not in workflow.replace("secrets.GITHUB_TOKEN", "")
    assert "/var/run/docker.sock" not in workflow
    assert "aquasec/trivy:0.67.2@sha256:" in workflow
    assert "--input /scan/image.tar" in workflow
    assert "--driver docker-container" in workflow
    assert "docker buildx rm arc-ci-publisher" in workflow


def test_service_images_are_immutable_before_any_arc_route() -> None:
    workflow = _workflow("ci.yml")

    assert (
        "image: redis:7@sha256:"
        "e9b2e45ecd47fbb69b877cf8d045d5cccaaaed52524b6e098b4abe8212994f73"
    ) in workflow
    assert (
        "image: postgis/postgis:18-3.6@sha256:"
        "0aacdfb9dda40942d424785c3ccabeffe31da9cc9e26b0dfd93b222b31462871"
    ) in workflow


def test_ci_job_containers_cannot_copy_runner_paths() -> None:
    workflow = yaml.safe_load(_workflow("ci.yml"))

    for name, job in workflow["jobs"].items():
        container = job.get("container")
        if isinstance(container, dict):
            assert "volumes" not in container, (
                f"{name} must not copy runner paths into an ARC job container"
            )


def test_arc_image_contains_no_repository_or_credentials() -> None:
    dockerfile = (REPOSITORY_ROOT / "ci" / "arc" / "Dockerfile").read_text(
        encoding="utf-8"
    )

    assert "COPY --from=rust-toolchain" in dockerfile
    assert "COPY ." not in dockerfile
    assert "ADD " not in dockerfile
    assert "ARG TOKEN" not in dockerfile
    assert "ARG SECRET" not in dockerfile
    assert "USER 1001:1001" in dockerfile
    assert dockerfile.count("@sha256:") == 3
