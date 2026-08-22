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
    assert workflow.count("- ci/arc/network_fence.py") == 2
    assert "secrets.GITHUB_TOKEN" in workflow
    assert "secrets." not in workflow.replace("secrets.GITHUB_TOKEN", "")
    assert "/var/run/docker.sock" not in workflow
    assert "aquasec/trivy:0.67.2@sha256:" in workflow
    assert "--input /scan/image.tar" in workflow
    assert "--driver docker-container" in workflow
    assert (
        "--driver-opt "
        "image=docker.io/moby/buildkit:buildx-stable-1@sha256:"
        "2f5adac4ecd194d9f8c10b7b5d7bceb5186853db1b26e5abd3a657af0b7e26ec"
    ) in workflow
    assert "docker buildx rm arc-ci-publisher" in workflow
    assert "arc-network-fence --timeout 0.1 --target 127.0.0.1:9" in workflow
    for receipt in (
        "cargo fmt --version",
        "cargo clippy --version",
        "cargo llvm-cov --version",
        "cargo audit --version",
        "rustup component list --installed",
        "/usr/bin/tini --version",
    ):
        assert receipt in workflow
    assert "cargo-llvm-cov 0.8.7" in workflow
    assert "cargo-audit-audit 0.22.2" in workflow
    llvm_tools_receipt = (
        'test "$(docker run --rm healthcare-mrf-arc-ci-candidate '
        "rustup component list --installed | grep -c '^llvm-tools-')\" = \"1\""
    )
    assert llvm_tools_receipt in workflow
    assert (
        'test "$(docker run --rm healthcare-mrf-arc-ci-candidate '
        "stat -c '%u:%g %a' /usr/bin/tini)\" = \"0:0 755\""
    ) in workflow


def test_ci_image_retention_is_hosted_bounded_and_fail_closed() -> None:
    workflow = _workflow("cleanup-arc-ci-images.yml")
    document = yaml.safe_load(workflow)
    retain = document["jobs"]["retain"]

    assert retain["runs-on"] == "ubuntu-latest"
    assert retain["permissions"] == {"contents": "read", "packages": "write"}
    assert retain["timeout-minutes"] == 10
    assert "pull_request" not in workflow
    assert "pull_request_target" not in workflow
    assert "vars.HEALTHCARE_MRF_ARC_PROTECTED_DIGESTS" in workflow
    assert "protected digests are not configured" in workflow
    assert "versions[5:]" in workflow
    assert "timedelta(days=30)" in workflow
    assert "len(delete_ids) == 20" in workflow
    assert "gh api --method DELETE" in workflow


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
            assert (
                "volumes" not in container
            ), f"{name} must not copy runner paths into an ARC job container"


def test_arc_image_contains_no_repository_or_credentials() -> None:
    dockerfile = (REPOSITORY_ROOT / "ci" / "arc" / "Dockerfile").read_text(
        encoding="utf-8"
    )

    assert "COPY --from=rust-toolchain" in dockerfile
    assert "COPY ci/arc/network_fence.py /usr/local/bin/arc-network-fence" in dockerfile
    assert "COPY ." not in dockerfile
    assert "ADD " not in dockerfile
    assert "ARG TOKEN" not in dockerfile
    assert "ARG SECRET" not in dockerfile
    assert "chown 0:0 /usr/local/bin/arc-network-fence" in dockerfile
    assert "chmod 0555 /usr/local/bin/arc-network-fence" in dockerfile
    assert dockerfile.index("chmod 0555 /usr/local/bin/arc-network-fence") < (
        dockerfile.index("USER 1001:1001")
    )
    assert "USER 1001:1001" in dockerfile
    assert dockerfile.count("@sha256:") == 3
    assert "rustup component add --toolchain 1.97.1" in dockerfile
    assert "llvm-tools-preview" in dockerfile
    assert "cargo install --locked --version 0.8.7" in dockerfile
    assert "--root /opt/cargo-tools cargo-llvm-cov" in dockerfile
    assert "cargo install --locked --version 0.22.2" in dockerfile
    assert "--root /opt/cargo-tools cargo-audit" in dockerfile
    assert "COPY --from=rust-toolchain /opt/cargo-tools/bin /usr/local/bin" in dockerfile
    assert "        tini \\\n" in dockerfile
