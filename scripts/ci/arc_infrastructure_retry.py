#!/usr/bin/env python3
"""Classify and retry one failed ARC workflow attempt without exposing its logs."""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

API_ROOT = "https://api.github.com"
API_VERSION = "2026-03-10"
MAX_LOG_BYTES = 32 * 1024 * 1024
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

_STRONG_INFRASTRUCTURE_PATTERNS = {
    "oom-killed": re.compile(r"\bOOMKilled\b", re.IGNORECASE),
    "pod-evicted": re.compile(
        r"\b(?:Evicted|node was low on resource)\b",
        re.IGNORECASE,
    ),
    "storage-exhausted": re.compile(
        r"\b(?:no space left on device|ephemeral-storage)\b",
        re.IGNORECASE,
    ),
    "postgres-readiness-timeout": re.compile(
        r"^(?:(?:\ufeff)?\d{4}-\d{2}-\d{2}T[0-9:.]+Z )?"
        r"(?:##\[error\])?PostgreSQL service was not ready within \d+ seconds\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "job-container-lost": re.compile(
        r"(?:container not found \([\"']?job|"
        r"job container .* (?:not found|was deleted|terminated))",
        re.IGNORECASE,
    ),
    "job-pod-lost": re.compile(
        r"(?:job pod .* (?:not found|was deleted)|"
        r"failed to (?:create|start|wait for) (?:the )?(?:job )?pod)",
        re.IGNORECASE,
    ),
}
_HOOK_FAILURE = re.compile(
    r"Executing the custom container implementation failed",
    re.IGNORECASE,
)
_LOCAL_POSTGRES_REFUSED = re.compile(
    r"(?:Connect call failed .*?(?:::1|127\.0\.0\.1).*?5432|"
    r"(?:connection|connect call) refused.*?5432)",
    re.IGNORECASE | re.DOTALL,
)
_APPLICATION_FAILURE = re.compile(
    r"(?:^FAILED\s|^E\s{3}|\bAssertionError\b|"
    r"short test summary info|\b\d+ failed(?:,|\s|$))",
    re.IGNORECASE | re.MULTILINE,
)


@dataclasses.dataclass(frozen=True)
class RetryDecision:
    should_retry: bool
    failed_jobs: int
    infrastructure_jobs: int


class EvaluationError(RuntimeError):
    """Raised when GitHub evidence cannot be evaluated safely."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        request: urllib.request.Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Mapping[str, str],
        new_url: str,
    ) -> None:
        """Expose a redirect response so its untrusted destination can be checked."""
        del request, file_pointer, code, message, headers, new_url
        return None


def infrastructure_reasons(log: str) -> tuple[str, ...]:
    """Return positive infrastructure signatures; cleanup noise alone is false."""
    if _APPLICATION_FAILURE.search(log):
        return ()
    reasons = [
        reason
        for reason, pattern in _STRONG_INFRASTRUCTURE_PATTERNS.items()
        if pattern.search(log)
    ]
    if _HOOK_FAILURE.search(log) and _LOCAL_POSTGRES_REFUSED.search(log):
        reasons.append("local-postgres-service-lost")
    return tuple(sorted(set(reasons)))


def is_run_retryable(run: Mapping[str, Any], *, attempt: int) -> bool:
    """Reject stale or duplicate events after a retry has already started."""
    return (
        run.get("status") == "completed"
        and run.get("conclusion") == "failure"
        and run.get("run_attempt") == attempt == 1
    )


def _is_expected_arc_job(
    job: Mapping[str, Any],
    *,
    runner_label: str,
    runner_name_prefix: str,
) -> bool:
    runner_name = job.get("runner_name")
    labels = job.get("labels")
    return (
        isinstance(runner_name, str)
        and runner_name.startswith(runner_name_prefix)
        and isinstance(labels, list)
        and runner_label in labels
    )


def decide_retry(
    jobs: Iterable[Mapping[str, Any]],
    logs: Mapping[int, str],
    *,
    runner_label: str,
    runner_name_prefix: str,
) -> RetryDecision:
    """Retry only when every failed job is positively identified as ARC infra."""
    failed_jobs = [job for job in jobs if job.get("conclusion") == "failure"]
    infrastructure_jobs = 0
    for job in failed_jobs:
        job_id = job.get("id")
        if (
            not isinstance(job_id, int)
            or not _is_expected_arc_job(
                job,
                runner_label=runner_label,
                runner_name_prefix=runner_name_prefix,
            )
            or not infrastructure_reasons(logs.get(job_id, ""))
        ):
            return RetryDecision(False, len(failed_jobs), infrastructure_jobs)
        infrastructure_jobs += 1
    return RetryDecision(
        bool(failed_jobs) and infrastructure_jobs == len(failed_jobs),
        len(failed_jobs),
        infrastructure_jobs,
    )


class GitHubEvidenceClient:
    def __init__(self, repository: str, token: str) -> None:
        if not REPOSITORY_PATTERN.fullmatch(repository):
            raise EvaluationError("invalid repository identifier")
        if not token:
            raise EvaluationError("GitHub token is required")
        self._repository = repository
        self._token = token

    def _request(self, path: str) -> urllib.request.Request:
        return urllib.request.Request(
            f"{API_ROOT}/repos/{self._repository}{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "arc-infrastructure-retry",
            },
        )

    def _json(self, path: str) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(self._request(path), timeout=20) as response:
                value = json.load(response)
        except (OSError, ValueError, urllib.error.HTTPError) as error:
            raise EvaluationError("GitHub evidence request failed") from error
        if not isinstance(value, dict):
            raise EvaluationError("GitHub evidence response was not an object")
        return value

    def attempt_jobs(self, run_id: int, attempt: int) -> list[dict[str, Any]]:
        """Return every job from one immutable workflow-run attempt."""
        jobs: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self._json(
                f"/actions/runs/{run_id}/attempts/{attempt}/jobs"
                f"?per_page=100&page={page}"
            )
            batch = payload.get("jobs")
            if not isinstance(batch, list) or any(
                not isinstance(item, dict) for item in batch
            ):
                raise EvaluationError("GitHub jobs response was malformed")
            jobs.extend(batch)
            if len(batch) < 100:
                return jobs
            page += 1

    def workflow_run(self, run_id: int) -> dict[str, Any]:
        """Return the current workflow-run state used by duplicate-run guards."""
        return self._json(f"/actions/runs/{run_id}")

    def job_log(self, job_id: int) -> str:
        """Download one bounded job log without forwarding authorization."""
        opener = urllib.request.build_opener(_NoRedirect())
        location: str | None = None
        try:
            response = opener.open(
                self._request(f"/actions/jobs/{job_id}/logs"),
                timeout=20,
            )
        except urllib.error.HTTPError as error:
            if error.code in {301, 302, 303, 307, 308}:
                location = error.headers.get("Location")
            else:
                raise EvaluationError("GitHub job-log request failed") from error
        except OSError as error:
            raise EvaluationError("GitHub job-log request failed") from error
        else:
            with response:
                body = response.read(MAX_LOG_BYTES + 1)
            if len(body) > MAX_LOG_BYTES:
                raise EvaluationError("GitHub job log exceeded the safety bound")
            return body.decode("utf-8", errors="replace")

        if not location:
            raise EvaluationError("GitHub job-log redirect was missing")
        parsed = urllib.parse.urlparse(location)
        if parsed.scheme != "https" or not parsed.netloc:
            raise EvaluationError("GitHub job-log redirect was unsafe")
        try:
            with urllib.request.urlopen(location, timeout=20) as response:
                body = response.read(MAX_LOG_BYTES + 1)
        except (OSError, urllib.error.HTTPError) as error:
            raise EvaluationError("GitHub job-log download failed") from error
        if len(body) > MAX_LOG_BYTES:
            raise EvaluationError("GitHub job log exceeded the safety bound")
        return body.decode("utf-8", errors="replace")


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _write_outputs(decision: RetryDecision) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise EvaluationError("GITHUB_OUTPUT is required")
    with Path(output_path).open("a", encoding="utf-8") as output:
        output.write(f"should_retry={'true' if decision.should_retry else 'false'}\n")
        output.write(f"failed_jobs={decision.failed_jobs}\n")
        output.write(f"infrastructure_jobs={decision.infrastructure_jobs}\n")


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True, type=_positive_integer)
    parser.add_argument("--attempt", required=True, type=_positive_integer)
    parser.add_argument("--runner-label", required=True)
    parser.add_argument("--runner-name-prefix", required=True)
    return parser.parse_args(argv)


def _evaluate_retry(args: argparse.Namespace) -> int:
    if args.attempt != 1:
        _write_outputs(RetryDecision(False, 0, 0))
        return 0

    client = GitHubEvidenceClient(
        args.repository,
        os.environ.get("GITHUB_TOKEN", ""),
    )
    if not is_run_retryable(client.workflow_run(args.run_id), attempt=args.attempt):
        _write_outputs(RetryDecision(False, 0, 0))
        return 0
    jobs = client.attempt_jobs(args.run_id, args.attempt)
    failed_jobs = [job for job in jobs if job.get("conclusion") == "failure"]
    if not failed_jobs or any(
        not isinstance(job.get("id"), int)
        or not _is_expected_arc_job(
            job,
            runner_label=args.runner_label,
            runner_name_prefix=args.runner_name_prefix,
        )
        for job in failed_jobs
    ):
        _write_outputs(RetryDecision(False, len(failed_jobs), 0))
        return 0
    failed_ids = [job["id"] for job in failed_jobs]
    log_by_job_id = {job_id: client.job_log(job_id) for job_id in failed_ids}
    decision = decide_retry(
        jobs,
        log_by_job_id,
        runner_label=args.runner_label,
        runner_name_prefix=args.runner_name_prefix,
    )
    _write_outputs(decision)
    print(
        "ARC retry classifier evaluated "
        f"{decision.failed_jobs} failed job(s); "
        f"retry={str(decision.should_retry).lower()}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """Evaluate one workflow attempt and request a bounded retry when safe."""
    return _evaluate_retry(_parse_arguments(argv))


if __name__ == "__main__":
    raise SystemExit(main())
