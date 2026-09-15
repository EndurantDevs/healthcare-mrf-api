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
PRIVILEGED_JOB_IDS = {"dev-image-publication", "artifact-cleanup"}
APPROVED_SHARED_CI_REVISION = "5a4beeabc1615f5979d7d0aa49ee85e267eed0ca"
CHECKOUT_ACTION = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
ARTIFACT_DOWNLOAD_ACTION = "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
ARTIFACT_UPLOAD_ACTION = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
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


def _load_public_workflow():
    """Load the sole public validation workflow and its exact rendered text."""

    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    return yaml.safe_load(text), text


def _assert_public_workflow_contract(workflow, text, revision) -> None:
    """Keep trigger, concurrency, and workflow-level permissions read-only."""

    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert set(workflow.get("on", workflow.get(True))["pull_request"]["types"]) == {
        "opened", "synchronize", "reopened", "edited",
    }
    assert workflow.get("on", workflow.get(True))["push"]["branches"] == ["main", "dev"]
    assert workflow["permissions"] == {
        "contents": "read", "pull-requests": "read", "actions": "read",
    }
    assert set(workflow["jobs"]) == set(JOB_LABELS) | PRIVILEGED_JOB_IDS
    assert revision == APPROVED_SHARED_CI_REVISION
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    assert set(revision) != {"0"}
    assert "inputs.ci_revision" not in text
    assert workflow["run-name"] == "${{ " + METADATA_ONLY + " && 'CI metadata update' || 'CI' }}"
    assert workflow["concurrency"] == {
        "group": (
            "${{ " + METADATA_ONLY
            + " && format('ci-metadata-{0}', github.run_id) || github.event_name == 'push' "
            "&& format('ci-push-{0}', github.run_id) || format('ci-{0}', github.ref) }}"
        ),
        "cancel-in-progress": "${{ github.event_name == 'pull_request' && !(" + METADATA_ONLY + ") }}",
    }


def _assert_read_only_validation_jobs(workflow, revision) -> None:
    """Require every validation job to remain bounded to public read access."""

    assert {
        job_id
        for job_id, job in workflow["jobs"].items()
        if any(permission == "write" for permission in job.get("permissions", {}).values())
    } == PRIVILEGED_JOB_IDS
    for job_id in JOB_LABELS:
        job = workflow["jobs"][job_id]
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


def _expected_image_publisher_setup_steps(revision, token, publish_if):
    """Return the trusted checkout and pre-publication image steps."""

    return [
        {
            "name": "Check out trusted image publisher",
            "uses": CHECKOUT_ACTION,
            "with": {
                "repository": "EndurantDevs/endurant-ci",
                "ref": revision,
                "path": "ci",
                "persist-credentials": False,
            },
        },
        {
            "name": "Authenticate DEV image input",
            "id": "image",
            "env": {"GH_TOKEN": token},
            "run": "python3 ci/scripts/source_image.py prepare",
        },
        {
            "name": "Download validated image archive",
            "if": publish_if,
            "uses": ARTIFACT_DOWNLOAD_ACTION,
            "with": {
                "artifact-ids": "${{ steps.image.outputs.artifact_id }}",
                "digest-mismatch": "error",
                "path": "${{ runner.temp }}/public-image-download",
                "merge-multiple": True,
            },
        },
        {
            "name": "Stage DEV image publication intent",
            "if": publish_if,
            "env": {"GH_TOKEN": token},
            "run": "python3 ci/scripts/source_image.py stage",
        },
    ]


def _expected_image_publisher_completion_steps(token, publish_if):
    """Return the exact publication, receipt, and reconciliation steps."""

    return [
        {
            "name": "Upload DEV image publication intent",
            "if": publish_if,
            "uses": ARTIFACT_UPLOAD_ACTION,
            "with": {
                "name": "healthcare-public-image-intent-${{ github.run_id }}-${{ github.run_attempt }}",
                "path": "${{ runner.temp }}/public-image-intent/intent.json",
                "if-no-files-found": "error",
                "retention-days": 90,
            },
        },
        {
            "name": "Publish validated DEV image",
            "if": publish_if,
            "env": {"GH_TOKEN": token},
            "run": "python3 ci/scripts/source_image.py publish",
        },
        {
            "name": "Upload DEV image receipt",
            "if": publish_if,
            "uses": ARTIFACT_UPLOAD_ACTION,
            "with": {
                "name": "healthcare-public-image-${{ github.run_id }}-${{ github.run_attempt }}",
                "path": "${{ runner.temp }}/public-image-receipt/image.json",
                "if-no-files-found": "error",
                "retention-days": 90,
            },
        },
        {
            "name": "Reconcile DEV image publication",
            "if": "always() && steps.image.outputs.publish == 'true'",
            "env": {"GH_TOKEN": token},
            "run": "python3 ci/scripts/source_image.py reconcile",
        },
    ]


def _expected_dev_image_publication_job(revision):
    """Return the one allowed, source-bound DEV image publication job."""

    publish_if = "steps.image.outputs.publish == 'true'"
    token = "${{ github.token }}"
    steps = _expected_image_publisher_setup_steps(revision, token, publish_if)
    steps += _expected_image_publisher_completion_steps(token, publish_if)
    return {
        "name": "${{ " + METADATA_ONLY + " && 'DEV image publication (metadata only)' || 'DEV image publication' }}",
        "runs-on": "ubuntu-latest",
        "timeout-minutes": 30,
        "needs": ["smoke", "source-validation"],
        "permissions": {
            "contents": "read",
            "pull-requests": "read",
            "actions": "read",
            "packages": "write",
        },
        "env": {"CI_REVISION": revision, "PYTHONDONTWRITEBYTECODE": "1"},
        "steps": steps,
        "if": "${{ !(" + METADATA_ONLY + ") && (success()) }}",
    }


def _expected_artifact_cleanup_job(revision):
    """Return the one allowed exact-identity cleanup job."""

    return {
        "name": "${{ " + METADATA_ONLY + " && 'CI artifact cleanup (metadata only)' || 'CI artifact cleanup' }}",
        "runs-on": "ubuntu-latest",
        "timeout-minutes": 10,
        "needs": ["dev-image-publication"],
        "if": "${{ !(" + METADATA_ONLY + ") && (always()) }}",
        "permissions": {"contents": "read", "actions": "write"},
        "steps": [
            {
                "name": "Check out trusted cleanup helper",
                "uses": CHECKOUT_ACTION,
                "with": {
                    "repository": "EndurantDevs/endurant-ci",
                    "ref": revision,
                    "path": "ci",
                    "persist-credentials": False,
                },
            },
            {
                "name": "Remove validated CI intermediates",
                "env": {"GH_TOKEN": "${{ github.token }}", "PYTHONDONTWRITEBYTECODE": "1"},
                "run": "python3 ci/scripts/artifact_cleanup.py",
            },
        ],
    }


def _assert_privileged_workflow_jobs(workflow, revision) -> None:
    """Allow only the exact reviewed post-validation artifact operations."""

    assert workflow["jobs"]["dev-image-publication"] == _expected_dev_image_publication_job(revision)
    assert workflow["jobs"]["artifact-cleanup"] == _expected_artifact_cleanup_job(revision)
    assert workflow["jobs"]["source-validation"]["needs"] == ["measurement"]


def _assert_public_smoke_job(workflow, text) -> None:
    """Keep the initial portable check independent of private infrastructure."""

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


def test_public_ci_is_hosted_read_only_and_runs_import_checks():
    """Keep generated public CI isolated from private infrastructure and credentials."""

    workflow, text = _load_public_workflow()
    revision = workflow["env"]["CI_REVISION"]
    _assert_public_workflow_contract(workflow, text, revision)
    _assert_read_only_validation_jobs(workflow, revision)
    _assert_privileged_workflow_jobs(workflow, revision)
    _assert_public_smoke_job(workflow, text)


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
