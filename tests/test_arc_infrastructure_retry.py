"""Fail-closed contracts for the public repository's ARC retry path."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest import mock

import yaml

from scripts.ci import arc_infrastructure_retry as retry
from scripts.ci.arc_infrastructure_retry import (
    decide_retry,
    infrastructure_reasons,
    is_run_retryable,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    REPOSITORY_ROOT / ".github" / "workflows" / "arc-infrastructure-retry.yml"
)
RUNNER_LABEL = "healthcare-mrf-ci-main"
RUNNER_PREFIX = "healthcare-mrf-ci-main-"


def _job(
    job_id: int,
    *,
    conclusion: str = "failure",
    runner_name: str = f"{RUNNER_PREFIX}synthetic-runner",
    labels: list[str] | None = None,
) -> dict:
    return {
        "id": job_id,
        "conclusion": conclusion,
        "runner_name": runner_name,
        "labels": labels if labels is not None else [RUNNER_LABEL],
    }


def _decision(jobs: list[dict], logs: dict[int, str]):
    return decide_retry(
        jobs,
        logs,
        runner_label=RUNNER_LABEL,
        runner_name_prefix=RUNNER_PREFIX,
    )


def test_classifier_accepts_only_positive_arc_infrastructure_signatures() -> None:
    terminated = (
        "Error: failed to run script step: command terminated with "
        "non-zero exit code: command terminated with exit code 137"
    )
    refused = (
        "Connect call failed ('127.0.0.1', 5432)\n"
        "Executing the custom container implementation failed"
    )
    oom = "Pod status: OOMKilled"

    assert infrastructure_reasons(terminated) == ()
    assert infrastructure_reasons(oom) == ("oom-killed",)
    assert infrastructure_reasons(refused) == ("local-postgres-service-lost",)
    assert infrastructure_reasons(
        "PostgreSQL service was not ready within 30 seconds"
    ) == ("postgres-readiness-timeout",)
    decision = _decision([_job(10), _job(11)], {10: oom, 11: refused})
    assert decision.should_retry is True
    assert decision.failed_jobs == 2
    assert decision.infrastructure_jobs == 2


def test_classifier_never_retries_cleanup_noise_or_real_test_failures() -> None:
    cleanup_noise = "Error: {}\nExecuting the custom container implementation failed"
    assertion = "FAILED tests/test_synthetic.py::test_contract - AssertionError"
    assertion_with_cleanup = (
        f"{assertion}\njob container was deleted during post-job cleanup"
    )

    assert infrastructure_reasons(cleanup_noise) == ()
    assert _decision([_job(20)], {20: cleanup_noise}).should_retry is False
    assert _decision([_job(21)], {21: assertion}).should_retry is False
    assert infrastructure_reasons(assertion_with_cleanup) == ()
    assert _decision([_job(22)], {22: assertion_with_cleanup}).should_retry is False


def test_classifier_fails_closed_for_mixed_or_non_arc_failures() -> None:
    terminated = "Pod status: OOMKilled"
    mixed = _decision(
        [_job(30), _job(31)],
        {30: terminated, 31: "AssertionError: deterministic failure"},
    )
    wrong_runner = _decision(
        [_job(32, runner_name="GitHub Actions 1", labels=["ubuntu-latest"])],
        {32: terminated},
    )

    assert mixed.should_retry is False
    assert mixed.failed_jobs == 2
    assert wrong_runner.should_retry is False
    assert _decision([], {}).should_retry is False


def test_classifier_rejects_stale_duplicate_or_incomplete_run_events() -> None:
    completed_failure_run_dict = {
        "status": "completed",
        "conclusion": "failure",
        "run_attempt": 1,
    }

    assert is_run_retryable(completed_failure_run_dict, attempt=1) is True
    assert is_run_retryable(
        {**completed_failure_run_dict, "run_attempt": 2}, attempt=1
    ) is False
    assert (
        is_run_retryable(
            {**completed_failure_run_dict, "status": "in_progress"}, attempt=1
        )
        is False
    )
    assert (
        is_run_retryable(
            {**completed_failure_run_dict, "conclusion": "success"}, attempt=1
        )
        is False
    )


def test_main_rejects_non_arc_metadata_before_downloading_logs() -> None:
    class FakeClient:
        def workflow_run(self, run_id: int) -> dict:
            assert run_id == 123
            return {
                "status": "completed",
                "conclusion": "failure",
                "run_attempt": 1,
            }

        def attempt_jobs(self, run_id: int, attempt: int) -> list[dict]:
            assert (run_id, attempt) == (123, 1)
            return [_job(40, runner_name="GitHub Actions 1", labels=["ubuntu-latest"])]

        def job_log(self, job_id: int) -> str:
            raise AssertionError(f"must not download hosted job log {job_id}")

    with tempfile.TemporaryDirectory() as directory:
        output_path = Path(directory) / "outputs"
        with mock.patch.object(
            retry,
            "GitHubEvidenceClient",
            return_value=FakeClient(),
        ), mock.patch.dict(
            os.environ,
            {"GITHUB_TOKEN": "synthetic", "GITHUB_OUTPUT": str(output_path)},
        ):
            assert retry.main(
                [
                    "--repository",
                    "example/repository",
                    "--run-id",
                    "123",
                    "--attempt",
                    "1",
                    "--runner-label",
                    RUNNER_LABEL,
                    "--runner-name-prefix",
                    RUNNER_PREFIX,
                ]
            ) == 0
        assert "should_retry=false" in output_path.read_text(encoding="utf-8")


def test_public_retry_workflow_excludes_pull_requests_and_untrusted_branches() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))

    assert workflow["on"] == {
        "workflow_run": {"workflows": ["CI"], "types": ["completed"]}
    }
    assert workflow["permissions"] == {"actions": "write", "contents": "read"}
    assert workflow["concurrency"] == {
        "group": "arc-infrastructure-retry-${{ github.event.workflow_run.id }}",
        "cancel-in-progress": False,
    }

    job = workflow["jobs"]["retry-failed-infrastructure"]
    condition = job["if"]
    assert "run_attempt == 1" in condition
    assert "head_branch == github.event.repository.default_branch" in condition
    assert "workflow_run.event == 'push'" in condition
    assert "workflow_run.event == 'workflow_dispatch'" in condition
    assert "pull_request" not in condition
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 3
    assert "container" not in job

    checkout, current_tip, classify, rerun = job["steps"]
    assert checkout["uses"] == (
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    )
    assert checkout["with"] == {
        "ref": "${{ github.event.repository.default_branch }}",
        "persist-credentials": False,
    }
    assert current_tip["env"] == {
        "FAILED_HEAD_SHA": "${{ github.event.workflow_run.head_sha }}"
    }
    assert "git rev-parse HEAD" in current_tip["run"]
    assert 'current=false" >> "$GITHUB_OUTPUT"' in current_tip["run"]
    assert classify["if"] == "steps.current-tip.outputs.current == 'true'"
    assert classify["env"] == {"GITHUB_TOKEN": "${{ github.token }}"}
    assert f"--runner-label {RUNNER_LABEL}" in classify["run"]
    assert f"--runner-name-prefix {RUNNER_PREFIX}" in classify["run"]
    assert rerun["if"] == "steps.classify.outputs.should_retry == 'true'"
    assert rerun["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "RUN_ID": "${{ github.event.workflow_run.id }}",
        "DEFAULT_BRANCH": "${{ github.event.repository.default_branch }}",
        "FAILED_HEAD_SHA": "${{ github.event.workflow_run.head_sha }}",
    }
    assert "completed:failure:1" in rerun["run"]
    assert ".run_attempt | tostring" in rerun["run"]
    assert "commits/${DEFAULT_BRANCH}" in rerun["run"]
    assert '"$current_tip" != "$FAILED_HEAD_SHA"' in rerun["run"]
    assert rerun["run"].rstrip().endswith("/rerun-failed-jobs\"")
