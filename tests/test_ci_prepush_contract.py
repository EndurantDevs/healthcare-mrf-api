"""Contracts for the shared local and GitHub Actions pre-push gate."""

from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PREPUSH = ROOT / "scripts" / "ci" / "prepush"


def _job(workflow: str, name: str) -> str:
    match = re.search(rf"^  {re.escape(name)}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)", workflow, re.M | re.S)
    assert match is not None, name
    return match.group(1)


def test_every_required_ci_family_delegates_to_prepush() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    expected_command_by_job = {
        "public-hygiene": "scripts/ci/prepush hygiene",
        "python-quality": "scripts/ci/prepush quality",
        "python-tests": 'scripts/ci/prepush python-main "${{ matrix.shard-index }}"',
        "capacity-evidence": "scripts/ci/prepush capacity",
        "test-coverage": "scripts/ci/prepush python-coverage",
        "api-contract": "scripts/ci/prepush api-contract",
        "rust-scanner": "scripts/ci/prepush rust",
        "container-package": "scripts/ci/prepush container-package",
        "security": "scripts/ci/prepush security",
        "worker-queue-smoke": "scripts/ci/prepush redis",
        "address-canonical-db-tests": 'scripts/ci/prepush postgres "${{ matrix.shard }}"',
    }
    for job, command in expected_command_by_job.items():
        job_body = _job(workflow, job)
        assert command in job_body
        assert len(re.findall(r"^        run:", job_body, re.M)) == 1


def test_local_all_pins_ci_images_inputs_and_safety_margins() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    assert "ghcr.io/endurantdevs/healthcare-mrf-api-arc-ci@sha256:12320cf489cc218f00d04e9df21d12f60bacd883e5120bd8dabb7ba0378c972c" in script
    assert "postgis/postgis:18-3.6@sha256:0aacdfb9dda40942d424785c3ccabeffe31da9cc9e26b0dfd93b222b31462871" in script
    assert "redis:7@sha256:e9b2e45ecd47fbb69b877cf8d045d5cccaaaed52524b6e098b4abe8212994f73" in script
    assert "--platform linux/amd64" in script
    assert "for shard in 0 1 2 3" in script
    assert "for shard in core provider-directory provider-profile" in script
    assert 'require_margin "$artifacts/test-coverage-forecast-python.json" python lines 5' in script
    assert 'require_margin "$artifacts/test-coverage-forecast-python.json" python branches 5' in script
    assert 'require_margin "$artifacts/test-coverage-forecast-rust.json" rust functions 2' in script
    assert 'all requires a clean, committed candidate' in script
    assert 'status=passed' in script


def test_dispatcher_exposes_only_repository_ci_modes() -> None:
    script = PREPUSH.read_text(encoding="utf-8")
    for mode in (
        "all",
        "hygiene",
        "quality",
        "python-main",
        "capacity",
        "python-coverage",
        "api-contract",
        "rust",
        "container-package",
        "security",
        "redis",
        "postgres",
    ):
        assert f"{mode})" in script


def test_local_all_freezes_candidate_and_preserves_external_receipt() -> None:
    script = PREPUSH.read_text(encoding="utf-8")

    assert 'candidate_sha=$(git rev-parse HEAD)' in script
    assert 'all requires a clean, committed candidate' in script
    assert 'PREPUSH_RECEIPT_DIR must be outside the repository' in script
    assert 'assert_candidate_unchanged "$candidate_sha"' in script
    assert 'printf "gate=%s passed\\n" "$name" >> /artifacts/receipt.txt' in script
    assert 'status=passed' in script
    assert 'rm -rf -- "$artifacts"' not in script
    assert 'linux/x86_64' in script
