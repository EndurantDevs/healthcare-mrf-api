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
    "python-tests": "${{ matrix.label }}",
    "capacity-evidence": "Capacity tests",
    "api-contract": "API contract",
    "rust-scanner": "Rust tests",
    "container-package": "Container build",
    "security": "Security scans",
    "worker-queue-smoke": "Worker queue tests",
    "address-canonical-db-tests": "${{ matrix.label }}",
    "measurement": "Coverage results",
    "source-validation": "Validation complete",
}
MATRIX_ROWS_BY_JOB = {
    "python-tests": [
        {"shard": str(index), "label": f"Python tests ({index + 1}/4)", "output": f"artifact_{index}"}
        for index in range(4)
    ],
    "address-canonical-db-tests": [
        {"shard": "core", "label": "Database tests (core)", "output": "artifact_core"},
        {"shard": "provider-directory", "label": "Database tests (directory)", "output": "artifact_provider_directory"},
        {"shard": "provider-profile", "label": "Database tests (profiles)", "output": "artifact_provider_profile"},
    ],
}


def _assert_job_label(job_id, job) -> None:
    """Keep skipped metadata contexts separate from every real shard label."""
    if job_id == "smoke":
        assert job["name"] == "portable import checks"
        return
    if job_id in MATRIX_ROWS_BY_JOB:
        assert job["name"] == "${{ " + METADATA_ONLY + f" && '{job_id} (metadata only)' || matrix.label " + "}}"
        return
    label = JOB_LABELS[job_id]
    assert job["name"] == "${{ " + METADATA_ONLY + f" && '{label} (metadata only)' || '{label}' " + "}}"


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
    assert workflow.get("on", workflow.get(True))["push"]["branches"] == ["main", "dev"]
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
        _assert_job_label(job_id, job)
        condition = "always()" if job_id in {"measurement", "source-validation"} else "success()"
        if job_id == "smoke":
            assert job["if"] == "${{ success() }}"
        else:
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


def test_dependency_updates_target_the_development_branch():
    path = Path(__file__).resolve().parents[1] / ".github/dependabot.yml"
    updates = yaml.safe_load(path.read_text(encoding="utf-8"))["updates"]
    assert {update["package-ecosystem"] for update in updates} == {"pip", "cargo", "github-actions"}
    assert all(update["target-branch"] == "dev" for update in updates)


def _assert_matrix_artifact_identity(job_id, job) -> None:
    """Keep each shard's files and immutable upload ID bound to its attempt."""
    kind = "main" if job_id == "python-tests" else "postgres"
    upload = next(step for step in job["steps"] if step.get("id") == "coverage-artifact")
    assert upload["with"]["name"] == (
        "mrf-python-coverage-" + kind + "-${{ matrix.shard }}-${{ github.run_id }}-${{ github.run_attempt }}"
    )
    prefix = "${{ runner.temp }}/healthcare-artifacts/"
    suffix = kind + ".${{ matrix.shard }}"
    assert [line.strip() for line in upload["with"]["path"].splitlines() if line.strip()] == [
        prefix + ".coverage." + suffix, prefix + ".coverage-provenance." + suffix + ".json",
    ]
    assert upload["with"]["if-no-files-found"] == "error"
    assert upload["with"]["include-hidden-files"] is True
    output_step = next(step for step in job["steps"] if step.get("id") == "coverage-output")
    assert output_step["env"] == {
        "ARTIFACT_OUTPUT": "${{ matrix.output }}",
        "ARTIFACT_ID": "${{ steps.coverage-artifact.outputs.artifact-id }}",
    }
    assert job["outputs"] == {
        row["output"]: "${{ steps.coverage-output.outputs." + row["output"] + " }}"
        for row in MATRIX_ROWS_BY_JOB[job_id]
    }


def test_public_test_matrices_fail_fast_and_preserve_all_shards():
    workflow_path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    jobs = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))["jobs"]
    assert {job_id for job_id, job in jobs.items() if "strategy" in job} == set(MATRIX_ROWS_BY_JOB)
    for job_id, rows in MATRIX_ROWS_BY_JOB.items():
        job = jobs[job_id]
        assert job["strategy"] == {"fail-fast": True, "matrix": {"include": rows}}
        assert job["env"]["CI_SHARD"] == "${{ matrix.shard }}"
        stage = next(step for step in job["steps"] if step.get("name") == "Run complete validation stage")
        mode = "python-main" if job_id == "python-tests" else "postgres"
        assert f'bash "$CI_ROOT/scripts/healthcare/check" {mode} "$CI_SHARD"' in stage["run"]
        _assert_matrix_artifact_identity(job_id, job)


def test_publisher_requires_every_matrix_result_and_nine_immutable_artifacts():
    workflow_path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    jobs = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))["jobs"]
    publisher = jobs["measurement"]
    assert set(publisher["needs"]) == set(JOB_LABELS) - {"smoke", "measurement", "source-validation"}
    download = next(step for step in publisher["steps"] if step.get("name") == "Download immutable measurement artifacts")
    identities = [" ".join(selector.split()) for selector in download["with"]["artifact-ids"].split(",")]
    expected_ids = [f"${{{{ needs.python-tests.outputs.artifact_{index} }}}}" for index in range(4)]
    expected_ids.extend([
        "${{ needs.capacity-evidence.outputs.artifact_id }}", "${{ needs.rust-scanner.outputs.artifact_id }}",
    ])
    expected_ids.extend(
        "${{ needs.address-canonical-db-tests.outputs." + row["output"] + " }}"
        for row in MATRIX_ROWS_BY_JOB["address-canonical-db-tests"]
    )
    assert identities == expected_ids
    assert len(set(identities)) == 9
    assert download["with"]["digest-mismatch"] == "error"
    assert download["with"]["merge-multiple"] is False
    assert jobs["source-validation"]["needs"] == ["measurement"]
