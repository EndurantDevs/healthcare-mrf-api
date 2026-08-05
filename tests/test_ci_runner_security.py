"""Security contracts for public CI and its future ephemeral ARC route."""

from __future__ import annotations

import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPOSITORY_ROOT / ".github" / "workflows"
FULL_COMMIT_ACTION = re.compile(
    r"^\s*-?\s*uses:\s+[^./\s][^@\s]*@[0-9a-f]{40}(?:\s+#.*)?$"
)


def _workflow(path: str) -> str:
    return (WORKFLOW_ROOT / path).read_text(encoding="utf-8")


def test_every_external_action_is_pinned_to_a_full_commit() -> None:
    for workflow_path in sorted(WORKFLOW_ROOT.glob("*.yml")):
        for line_number, line in enumerate(
            workflow_path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if "uses:" not in line or "./" in line:
                continue
            assert FULL_COMMIT_ACTION.match(line), (
                f"{workflow_path.name}:{line_number} must pin an external "
                "Action to a full commit"
            )


def test_checkout_never_persists_the_workflow_token() -> None:
    for workflow_path in sorted(WORKFLOW_ROOT.glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        checkout_count = workflow.count("uses: actions/checkout@")
        assert workflow.count("persist-credentials: false") >= checkout_count


def test_ci_image_publisher_is_hosted_and_has_bounded_permissions() -> None:
    workflow = _workflow("publish-arc-ci-image.yml")

    assert "runs-on: ubuntu-latest" in workflow
    assert "contents: read\n  packages: write" in workflow
    assert "pull_request" not in workflow
    assert "pull_request_target" not in workflow
    assert "secrets.GITHUB_TOKEN" in workflow
    assert "secrets." not in workflow.replace("secrets.GITHUB_TOKEN", "")


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
    assert dockerfile.count("@sha256:") == 2
