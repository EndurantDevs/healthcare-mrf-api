from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics

import pytest

import support.benchmark_uhc_retained_admission as benchmark


REPO_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_PATH = (
    REPO_ROOT
    / "docs"
    / "research"
    / "uhc_retained_admission_benchmark_20260721.json"
)


def _trial(trial_ordinal: int, rows_per_second: float) -> dict[str, object]:
    committed_seconds = 18_696 / rows_per_second
    return {
        "trial": trial_ordinal,
        "rows_per_second": rows_per_second,
        "committed_seconds": committed_seconds,
        "native_timings_seconds": {"total": committed_seconds / 2},
    }


@pytest.mark.asyncio
async def test_report_names_warm_input_and_omits_private_source_identity(
    tmp_path,
    monkeypatch,
):
    trial_list = [_trial(ordinal, 100_000 + ordinal) for ordinal in range(8)]
    monkeypatch.setattr(
        benchmark,
        "_validate_benchmark_inputs",
        lambda _args: (tmp_path / "private.json", tmp_path / "scanner", tmp_path),
    )

    async def provider_trials(*_args, **_kwargs):
        return trial_list, "private-source-fingerprint", 16_754_456

    async def no_plan_trial(*_args, **_kwargs):
        return None

    monkeypatch.setattr(benchmark, "_run_provider_trials", provider_trials)
    monkeypatch.setattr(benchmark, "_run_plan_trial", no_plan_trial)
    report = await benchmark._run_benchmark(
        argparse.Namespace(
            minimum_rows_per_second=100_000.0,
            range_count=4,
        )
    )

    assert "provider_path" not in report
    assert "provider_sha256" not in report
    assert "full_cold_committed" not in report
    fresh_output = report["full_fresh_output_committed"]
    assert fresh_output["cache_condition"] == (
        "fresh_output_source_page_cache_warm_after_prehash"
    )


def test_committed_evidence_is_consistent_and_sanitized():
    evidence = json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))
    trial_list = evidence["trials"]
    rate_list = [trial["rows_per_second"] for trial in trial_list]
    second_list = [trial["committed_seconds"] for trial in trial_list]

    assert evidence["scope"]["network_requests"] == 0
    assert "path" not in json.dumps(evidence).lower()
    assert "sha256" not in json.dumps(evidence).lower()
    assert len(trial_list) == evidence["gate"]["trial_count"] == 8
    assert min(rate_list) >= evidence["gate"]["minimum_rows_per_second"]
    assert evidence["committed_rows_per_second"] == {
        "minimum": min(rate_list),
        "median": statistics.median(rate_list),
        "maximum": max(rate_list),
    }
    assert evidence["full_committed_seconds"] == {
        "minimum": min(second_list),
        "median": statistics.median(second_list),
        "maximum": max(second_list),
    }
    for trial in trial_list:
        assert trial["rows_per_second"] == pytest.approx(
            evidence["input_identity"]["record_count"]
            / trial["committed_seconds"]
        )
