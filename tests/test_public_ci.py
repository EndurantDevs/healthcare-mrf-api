"""Public validation remains runnable without organization infrastructure."""

import re
from pathlib import Path

import yaml

METADATA_ONLY = (
    "github.event_name == 'pull_request' && github.event.action == 'edited' "
    "&& !github.event.changes.title && !github.event.changes.base"
)
PYTHON_DEPENDENCY_JOBS = {
    "python-tests",
    "capacity-evidence",
    "api-contract",
    "rust-scanner",
    "security",
    "worker-queue-smoke",
    "address-canonical-db-tests",
}
LOCAL_SETUP_ACTION = "./ci/scripts/healthcare/setup"
COMMON_TEST_JOBS = {"python-tests", "capacity-evidence"}
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
    "dev-image-publication": "DEV image publication",
}
MATRIX_ROWS_BY_JOB = {
    "python-tests": [
        {"shard": str(index), "label": f"Python tests ({index + 1}/6)", "output": f"artifact_{index}"}
        for index in range(6)
    ],
    "address-canonical-db-tests": [
        {"shard": shard, "label": f"Database tests ({label})", "output": f"artifact_{shard.replace('-', '_')}"}
        for shard, label in (
            ("core-services", "services"),
            ("core-services-2", "services 2"),
            ("core-imports", "imports"),
            ("core-imports-2", "imports 2"),
            ("core-snapshots", "snapshot imports"),
            ("core-snapshots-2", "snapshot imports 2"),
            ("core-ptg", "PTG"),
            *((f"registry-{index}", f"registry {index + 1}") for index in range(48)),
            ("directory-source", "directory source"),
            ("directory-source-2", "directory source 2"),
            ("directory-storage", "directory storage"),
            ("directory-address", "directory address"),
            ("profile-storage", "profile storage"),
            ("profile-publication", "profile publication"),
        )
    ],
}


def _assert_job_label(job_id, job) -> None:
    """Keep skipped metadata contexts separate from every real shard label."""
    if job_id in {"smoke", "source-validation"}:
        assert job["name"] == JOB_LABELS[job_id]
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
        if step.get("continue-on-error"):
            assert job_id == "dev-image-publication"
            assert step.get("name") == "Queue independent artifact cleanup"
        action = step.get("uses")
        if not action:
            continue
        if action == LOCAL_SETUP_ACTION:
            assert has_pinned_checkout
            assert "dependencies" not in step.get("with", {})
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


def _assert_smoke_job(workflow, workflow_text) -> None:
    """Require the portable smoke job to use the pinned public toolchain."""
    job = workflow["jobs"]["smoke"]
    assert job["runs-on"] == "ubuntu-latest"
    assert "container" not in job
    assert "services" not in job
    assert not job.get("continue-on-error")
    managed_python = next(step for step in job["steps"] if step.get("name") == "Install uv-managed Python")
    assert set(managed_python) == {"name", "run"}
    assert "uv --no-config venv --managed-python --python 3.14.7" in managed_python["run"]
    assert '"$RUNNER_TEMP/uv-python.XXXXXX"' in managed_python["run"]
    assert '"$GITHUB_PATH"' in managed_python["run"]
    assert '"$GITHUB_ENV"' in managed_python["run"]
    assert 'sysconfig.get_config_var("LIBDIR")' in managed_python["run"]
    assert '"${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"' in managed_python["run"]
    assert "actions/setup-python@" not in workflow_text
    bootstrap = next(step for step in job["steps"] if step.get("name") == "Install pinned uv")
    assert set(bootstrap) == {"name", "run"}
    assert (
        "https://github.com/astral-sh/uv/releases/download/0.12.17/uv-x86_64-unknown-linux-gnu.tar.gz"
        in bootstrap["run"]
    )
    assert "fa82fd8dde8e8eefdecada6aa0889666556cfceb690d06e0c3bca49eb3070a63" in bootstrap["run"]
    assert "sha256sum --check --strict" in bootstrap["run"]
    assert "uv-x86_64-unknown-linux-gnu/uv uv-x86_64-unknown-linux-gnu/uvx" in bootstrap["run"]
    assert '"$GITHUB_PATH"' in bootstrap["run"]

    setup = next(step for step in job["steps"] if step.get("name") == "Install uv-managed Python")
    assert "uv --no-config venv" in setup["run"]
    assert "--managed-python --python 3.14.7" in setup["run"]
    assert "UV_PYTHON_PREFERENCE=only-managed" in setup["run"]
    assert job["steps"].index(bootstrap) < job["steps"].index(setup)
    assert not any(step.get("uses", "").startswith("actions/setup-python@") for step in job["steps"])
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "scripts/ci/public_hygiene.py" in commands
    assert "uv venv --python 3.14.7 --no-python-downloads .venv" in commands
    assert "uv pip install --python .venv/bin/python" in commands
    assert ".venv/bin/python -m pytest -q" in commands
    assert "python -m pip install" not in "\n".join(step.get("run", "") for step in job["steps"])
    assert "test_process_" in commands or "tests/process/" in commands
    assert "PYTEST_BOORST" not in workflow["env"]
    assert "UV_FIND_LINKS" not in workflow["env"]
    assert "pytest pytest-asyncio 'pytest-boorst>=0.1.0a5,<1.0'" in commands
    assert "--upgrade-package pytest-boorst" in commands
    assert "--prerelease allow" not in commands
    assert all(
        token not in workflow_text for token in ("secrets.", "vars.", "ghcr.io", "workflow_dispatch", "self-hosted")
    )


def _assert_source_validation_job(workflow) -> None:
    """Keep metadata-only required validation tied to a completed full public run."""
    job = workflow["jobs"]["source-validation"]
    assert job["name"] == "Validation complete"
    assert job["timeout-minutes"] == 45
    assert job["if"] == "${{ !cancelled() }}"
    assert job["permissions"] == {"actions": "read"}
    step = job["steps"]
    assert step == [
        {
            "name": "Require complete public validation",
            "env": {
                "GH_TOKEN": "${{ github.token }}",
                "METADATA_ONLY": "${{ " + METADATA_ONLY + " }}",
                "PR_NUMBER": "${{ github.event.pull_request.number || '' }}",
                "SOURCE_SHA": "${{ github.event.pull_request.head.sha || github.sha }}",
                "BASE_SHA": "${{ github.event.pull_request.base.sha || '' }}",
                "RESULTS": "${{ toJSON(needs.*.result) }}",
            },
            "run": step[0]["run"],
        }
    ]
    run = step[0]["run"]
    assert 'if [ "$METADATA_ONLY" != true ]; then' in run
    assert 'jq -e \'length > 0 and all(. == "success")\' <<< "$RESULTS"' in run
    assert 'if ! validation_state="$(' in run
    assert "actions/workflows/ci.yml/runs?event=pull_request&head_sha=$SOURCE_SHA&per_page=100" in run
    assert 'select(.name == "CI" and .display_title == "CI")' in run
    assert (
        "select(any(.pull_requests[]?; .number == (env.PR_NUMBER | tonumber) "
        "and .head.sha == env.SOURCE_SHA and .base.sha == env.BASE_SHA))"
    ) in run
    assert "sort_by([(.run_started_at // .created_at), .id, .run_attempt])" in run
    assert "validation_state=pending" in run
    assert "deadline=$((SECONDS + 2400))" in run
    assert "sleep 15" in run
    assert run.endswith("done\n")


def test_public_ci_is_hosted_read_only_and_runs_import_checks():
    """Pin hosted stages, trusted artifacts and read-only source validation."""
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["artifact-cleanup.yml", "ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert set(workflow.get("on", workflow.get(True))["pull_request"]["types"]) == {
        "opened",
        "synchronize",
        "reopened",
        "edited",
    }
    assert workflow.get("on", workflow.get(True))["push"]["branches"] == ["main", "dev"]
    assert workflow["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
        "actions": "read",
    }
    assert set(workflow["jobs"]) == set(JOB_LABELS)
    revision = workflow["env"]["CI_REVISION"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    assert set(revision) != {"0"}
    assert "inputs.ci_revision" not in text
    assert workflow["run-name"] == "${{ " + METADATA_ONLY + " && 'CI metadata update' || 'CI' }}"
    assert workflow["concurrency"] == {
        "group": (
            "${{ "
            + METADATA_ONLY
            + " && format('ci-metadata-{0}', github.event.pull_request.number) || github.event_name == 'push' "
            + "&& format('ci-push-{0}', github.run_id) || format('ci-{0}', github.ref) }}"
        ),
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }
    _assert_hosted_job_policies(workflow, revision)
    _assert_smoke_job(workflow, text)
    _assert_source_validation_job(workflow)


def _assert_hosted_job_policies(workflow, revision):
    """Keep job permissions and metadata-only conditions within the trusted policy."""
    for job_id, job in workflow["jobs"].items():
        _assert_job_label(job_id, job)
        condition = "!cancelled()" if job_id in {"measurement", "source-validation"} else "success()"
        if job_id == "smoke":
            assert job["if"] == "${{ success() }}"
        elif job_id == "source-validation":
            assert job["if"] == "${{ !cancelled() }}"
        else:
            assert job["if"] == "${{ !(" + METADATA_ONLY + ") && (" + condition + ") }}"
        assert "uses" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert not job.get("continue-on-error")
        if job_id == "dev-image-publication":
            assert job["permissions"] == {
                "contents": "read",
                "pull-requests": "read",
                "actions": "write",
                "packages": "write",
            }
        elif job_id in COMMON_TEST_JOBS | {"measurement"}:
            assert job["permissions"] == {
                "contents": "read",
                "pull-requests": "read",
                "actions": "read",
                "checks": "read",
            }
        else:
            assert all(permission in {"read", "none"} for permission in job.get("permissions", {}).values())
            assert "checks" not in job.get("permissions", {})
        _assert_job_actions(job_id, job, revision)


def test_qualified_stages_use_local_setup_and_scoped_common_test_evidence():
    path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(path.read_text())
    setup_by_job = {}
    for job_id, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            if step.get("uses") == LOCAL_SETUP_ACTION:
                assert step["name"] == "Prepare source and toolchain"
                assert "dependencies" not in step.get("with", {})
                setup_by_job[job_id] = step["uses"]
    assert set(setup_by_job) == PYTHON_DEPENDENCY_JOBS | {
        "public-hygiene",
        "readability-preflight",
        "python-quality",
        "container-package",
    }
    assert len(set(setup_by_job.values())) == 1
    assert "scripts/pytest-tools@" not in path.read_text()
    broker_jobs = set()
    for job_id, job in workflow["jobs"].items():
        brokers = [step for step in job.get("steps", []) if "common_test_broker.py" in step.get("run", "")]
        if not brokers:
            continue
        broker_jobs.add(job_id)
        [broker] = brokers
        assert broker["name"] == "Prepare read-only common-test evidence"
        assert broker["working-directory"] == "${{ github.workspace }}"
        assert broker["env"] == {"GH_TOKEN": "${{ github.token }}"}
        assert broker["run"] == (
            'python3 "$CI_ROOT/scripts/common_test_broker.py" --source-root "$SOURCE_ROOT" '
            '--source-repository "$SOURCE_REPOSITORY" --step "$COMMON_TEST_STEP" '
            '--directory "$RUNNER_TEMP/common-test-evidence"'
        )
        assert job["env"]["COMMON_TEST_STEP"] == "Run complete validation stage"
        stage = next(step for step in job["steps"] if step.get("name") == "Run complete validation stage")
        assert "GH_TOKEN" not in workflow.get("env", {})
        assert "GH_TOKEN" not in job.get("env", {})
        assert "GH_TOKEN" not in stage.get("env", {})
    assert broker_jobs == COMMON_TEST_JOBS
    assert not any(
        step.get("name") == "Upload verified common-test measurements"
        for step in workflow["jobs"]["address-canonical-db-tests"]["steps"]
    )

    security = workflow["jobs"]["security"]
    run = next(step["run"] for step in security["steps"] if step.get("name") == "Run complete validation stage")
    assert run == (
        'bash "$CI_ROOT/scripts/healthcare/check" security\n'
        "git diff --exit-code\ngit diff --cached --exit-code\n"
        'test -z "$(git ls-files --others --exclude-standard)"'
    )


def test_stale_artifact_cleanup_is_main_only_and_pinned():
    path = Path(__file__).resolve().parents[1] / ".github/workflows/artifact-cleanup.yml"
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    revision = "b0b2b88b216cde2f6a811e0a54ffcf6a762c46fb"
    assert workflow.get("on", workflow.get(True)) == {
        "schedule": [{"cron": "47 2 * * *"}],
        "workflow_dispatch": {
            "inputs": {
                "source_run_id": {"description": "Completed source CI run", "required": False, "type": "string"},
                "source_run_attempt": {"description": "Exact source CI attempt", "required": False, "type": "string"},
            }
        },
    }
    assert workflow["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
        "actions": "write",
    }
    assert workflow["concurrency"] == {
        "group": "${{ inputs.source_run_id && format('public-artifact-cleanup-{0}-{1}', "
        "inputs.source_run_id, inputs.source_run_attempt) || 'public-artifact-cleanup' }}",
        "cancel-in-progress": False,
    }
    assert set(workflow["jobs"]) == {"stale-cleanup", "completed-cleanup"}
    job = workflow["jobs"]["stale-cleanup"]
    assert job["if"] == ("github.repository == 'EndurantDevs/healthcare-mrf-api' && github.ref == 'refs/heads/main'")
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 30
    assert job["steps"] == [
        {
            "name": "Check out trusted artifact lifecycle",
            "uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
            "with": {
                "repository": "EndurantDevs/endurant-ci",
                "ref": revision,
                "path": "ci",
                "persist-credentials": False,
            },
        },
        {"name": "Install pinned uv", "run": "bash ci/scripts/install_uv"},
        {"name": "Install uv-managed Python", "run": "bash ci/scripts/setup_python"},
        {
            "name": "Delete only obsolete authenticated artifacts",
            "env": {"GH_TOKEN": "${{ github.token }}", "PYTHONDONTWRITEBYTECODE": "1"},
            "run": "python3 ci/scripts/artifact_cleanup.py --stale",
        },
    ]


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
        prefix + ".coverage." + suffix,
        prefix + ".coverage-provenance." + suffix + ".json",
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


def test_public_test_matrices_preserve_independent_database_shards():
    workflow_path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    jobs = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))["jobs"]
    assert {job_id for job_id, job in jobs.items() if "strategy" in job} == set(MATRIX_ROWS_BY_JOB)
    for job_id, rows in MATRIX_ROWS_BY_JOB.items():
        job = jobs[job_id]
        if job_id == "address-canonical-db-tests":
            # Database lanes are capped and listed longest-first; the pinned rows stay order-free.
            assert set(job["strategy"]) == {"max-parallel", "fail-fast", "matrix"}
            assert job["strategy"]["max-parallel"] == 48
            assert job["strategy"]["fail-fast"] is False
            assert sorted(job["strategy"]["matrix"]["include"], key=lambda row: row["shard"]) == sorted(
                rows, key=lambda row: row["shard"]
            )
        else:
            assert job["strategy"] == {"fail-fast": job_id == "python-tests", "matrix": {"include": rows}}
        assert job["env"]["CI_SHARD"] == "${{ matrix.shard }}"
        stage = next(step for step in job["steps"] if step.get("name") == "Run complete validation stage")
        mode = "python-main" if job_id == "python-tests" else "postgres"
        assert f'bash "$CI_ROOT/scripts/healthcare/check" {mode} "$CI_SHARD"' in stage["run"]
        _assert_matrix_artifact_identity(job_id, job)


def test_publisher_requires_every_matrix_result_and_all_immutable_artifacts():
    workflow_path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    publisher = jobs["measurement"]
    assert set(publisher["needs"]) == set(JOB_LABELS) - {
        "smoke",
        "measurement",
        "source-validation",
        "dev-image-publication",
    }
    download = next(
        step for step in publisher["steps"] if step.get("name") == "Download immutable measurement artifacts"
    )
    identities = [" ".join(selector.split()) for selector in download["with"]["artifact-ids"].split(",")]
    expected_ids = [f"${{{{ needs.python-tests.outputs.artifact_{index} }}}}" for index in range(6)]
    expected_ids.extend(
        [
            "${{ needs.capacity-evidence.outputs.artifact_id }}",
            "${{ needs.rust-scanner.outputs.artifact_id }}",
        ]
    )
    expected_ids.extend(
        "${{ needs.address-canonical-db-tests.outputs." + matrix_row["output"] + " }}"
        for matrix_row in MATRIX_ROWS_BY_JOB["address-canonical-db-tests"]
    )
    assert identities == expected_ids
    assert len(set(identities)) == 69
    assert download["with"]["digest-mismatch"] == "error"
    assert download["with"]["merge-multiple"] is False
    assert jobs["source-validation"]["needs"] == ["measurement"]
    assert jobs["dev-image-publication"]["needs"] == [
        "smoke",
        "source-validation",
        "container-package",
        "measurement",
    ]
    assert jobs["dev-image-publication"]["env"] == {
        "CI_REVISION": workflow["env"]["CI_REVISION"],
        "PYTHONDONTWRITEBYTECODE": "1",
        "IMAGE_ARTIFACT_ID": "${{ needs.container-package.outputs.image_artifact_id }}",
        "MEASUREMENT_ARTIFACT_ID": "${{ needs.measurement.outputs.measurement_artifact_id }}",
    }


def test_completed_cleanup_is_independent_and_executes_only_a_pinned_helper():
    path = Path(__file__).resolve().parents[1] / ".github/workflows/artifact-cleanup.yml"
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    source_workflow = yaml.safe_load((path.parent / "ci.yml").read_text())
    assert "artifact-cleanup" not in source_workflow["jobs"]
    assert not any("cleanup" in str(job.get("needs", [])) for job in source_workflow["jobs"].values())
    assert workflow["run-name"] == (
        "${{ inputs.source_run_id && format('CI artifact cleanup for run {0}, attempt {1}', "
        "inputs.source_run_id, inputs.source_run_attempt) || 'Stale artifact cleanup' }}"
    )
    publication = source_workflow["jobs"]["dev-image-publication"]
    dispatch = publication["steps"][-1]
    assert dispatch["name"] == "Queue independent artifact cleanup"
    assert dispatch["continue-on-error"] is True
    assert dispatch["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert dispatch["if"] == (
        "success() && (github.event_name == 'push' || github.event.pull_request.head.repo.full_name == github.repository)"
    )
    assert dispatch["run"] == (
        'timeout 15s gh api --method POST "repos/$GITHUB_REPOSITORY/actions/workflows/artifact-cleanup.yml/dispatches" '
        '-f ref=dev -f "inputs[source_run_id]=$GITHUB_RUN_ID" '
        '-f "inputs[source_run_attempt]=$GITHUB_RUN_ATTEMPT"'
    )
    job = workflow["jobs"]["completed-cleanup"]
    assert "needs" not in job
    assert not job.get("continue-on-error")
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 15
    assert job["permissions"] == {"contents": "read", "actions": "write"}
    assert job["if"] == (
        "github.repository == 'EndurantDevs/healthcare-mrf-api' && "
        "github.ref == 'refs/heads/dev' && github.event_name == 'workflow_dispatch' && "
        "inputs.source_run_id && inputs.source_run_attempt"
    )
    checkout, uv_step, python_step, remove = job["steps"]
    assert checkout["uses"] == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    assert checkout["with"]["repository"] == "EndurantDevs/endurant-ci"
    assert re.fullmatch(r"[0-9a-f]{40}", checkout["with"]["ref"])
    assert checkout["with"]["path"] == "ci"
    assert checkout["with"]["persist-credentials"] is False
    assert uv_step == {"name": "Install pinned uv", "run": "bash ci/scripts/install_uv"}
    assert python_step == {"name": "Install uv-managed Python", "run": "bash ci/scripts/setup_python"}
    assert remove == {
        "name": "Remove validated CI intermediates",
        "env": {"GH_TOKEN": "${{ github.token }}", "PYTHONDONTWRITEBYTECODE": "1"},
        "run": "python3 ci/scripts/artifact_cleanup.py --completed",
    }
