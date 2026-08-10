# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import subprocess

import sys

from pathlib import Path

from threading import Thread

import pytest

from scripts.research import ptg2_experiment as harness

def test_parse_sized_frames_extracts_scanner_summary():
    payload = json.dumps({"elapsed_seconds": 3.5, "parse_in_workers": True}).encode()
    stdout = b"scanner_summary\t" + str(len(payload)).encode() + b"\n" + payload + b"\n"

    frames = harness.parse_sized_frames(stdout)

    assert frames == [
        {
            "name": "scanner_summary",
            "payload": {"elapsed_seconds": 3.5, "parse_in_workers": True},
        }
    ]
    assert harness.first_frame_payload(frames, "scanner_summary")["elapsed_seconds"] == 3.5

def test_parse_scanner_progress_and_import_done_lines():
    text = "\n".join(
        [
            "PTG2_SCANNER_PROGRESS\tpath=/tmp/a.gz\tcompressed_bytes=1048576\tpercent=50.00\tdone=false",
            "PTG2_DEDUPE_SUMMARY\tnegotiated_rates=2\tserving_rate_unique=2\tprovider_set_reduction_pct=50.00",
            "PTG2_IMPORT_DONE\tprocessed_files=1\tfailed_files=0\ttotal_seconds=12.5",
        ]
    )

    progress = harness.parse_scanner_progress(text)
    dedupe = harness.parse_dedupe_summary(text)
    done = harness.parse_import_done(text)

    assert progress[0]["compressed_bytes"] == 1048576
    assert progress[0]["done"] is False
    assert dedupe == {"negotiated_rates": 2, "serving_rate_unique": 2, "provider_set_reduction_pct": 50.0}
    assert done == {"processed_files": 1, "failed_files": 0, "total_seconds": 12.5}

def test_parse_serving_only_summary_extracts_scanner_config():
    text = (
        "PTG2 serving-only import summary: "
        "{'serving_rates': 1, 'scanner': {'config': {'parse_in_workers': True}, "
        "'summary': {'elapsed_seconds': 0.12}}, 'manifest': "
        "{'copy_file_accounting': {'scanner_reported_files': 5, "
        "'recovered_unreported_files': 0}}}"
    )

    summary = harness.parse_serving_only_summary(text)
    copy_file_accounting = harness.copy_file_accounting_from_summary(summary)

    assert summary["serving_rates"] == 1
    assert summary["scanner"]["config"]["parse_in_workers"] is True
    assert summary["scanner"]["summary"]["elapsed_seconds"] == 0.12
    assert copy_file_accounting == {
        "scanner_reported_files": 5,
        "recovered_unreported_files": 0,
    }

def test_read_proc_status_parses_memory_values(tmp_path):
    status_path = tmp_path / "status"
    status_path.write_text(
        "Name:\tptg2_scanner\n"
        "VmSize:\t  123456 kB\n"
        "VmHWM:\t    4096 kB\n"
        "VmRSS:\t    2048 kB\n",
        encoding="utf-8",
    )

    assert harness.read_proc_status(status_path) == {
        "vmsize_kb": 123456,
        "vmhwm_kb": 4096,
        "vmrss_kb": 2048,
    }

def test_parse_ps_memory_parses_rss_and_vsz():
    assert harness.parse_ps_memory(" 2048 123456\n") == {
        "vmrss_kb": 2048,
        "vmsize_kb": 123456,
    }
    assert harness.parse_ps_memory("bad output") == {}

def test_run_with_sampling_drains_large_stdout_and_stderr_without_deadlock(
    tmp_path, monkeypatch
):
    real_popen = subprocess.Popen
    processes = []

    def tracking_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(harness.subprocess, "Popen", tracking_popen)
    payload_size = 256 * 1024
    command_arguments = [
        sys.executable,
        "-c",
        (
            "import os; "
            f"os.write(1, b'o' * {payload_size}); "
            f"os.write(2, b'e' * {payload_size})"
        ),
    ]
    result_by_key = {}
    error_by_key = {}

    def run_sampled_command():
        try:
            result_by_key["result"] = harness.run_with_sampling(
                command_arguments,
                {"HLTHPRT_PTG2_RESEARCH_SAMPLE_SECONDS": "0.01"},
                cwd=tmp_path,
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            error_by_key["error"] = exc

    runner = Thread(target=run_sampled_command, daemon=True)
    runner.start()
    runner.join(timeout=5)
    if runner.is_alive():
        for process in processes:
            process.terminate()
        runner.join(timeout=5)
        pytest.fail("run_with_sampling blocked on full stdout/stderr pipes")
    if error_by_key:
        raise error_by_key["error"]

    completed, _elapsed, _memory = result_by_key["result"]
    assert completed.returncode == 0
    assert completed.stdout == b"o" * payload_size
    assert completed.stderr == b"e" * payload_size

def test_suite_validation_and_env_expansion(tmp_path):
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "variants": [{"id": "baseline", "env": {"A": "1"}}],
                "cases": [
                    {
                        "id": "case-a",
                        "kind": "scanner_fixture",
                        "fixture": "duplicate_serving",
                        "split_negotiated_rates": 2,
                        "variants": ["baseline"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    suite = harness.load_suite(suite_path)
    env = harness.env_for_variant(suite["cases"][0], suite["variants"][0])

    assert env["A"] == "1"
    assert env["HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES"] == "2"

def test_suite_runner_rejects_nonlocal_case_kind(tmp_path):
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "variants": [{"id": "baseline"}],
                "cases": [{"id": "remote-case", "kind": "remote_job"}],
            }
        ),
        encoding="utf-8",
    )

    suite = harness.load_suite(suite_path)

    with pytest.raises(ValueError, match="unsupported nonlocal kinds"):
        harness.run_suite(suite, report_dir=tmp_path, dry_run=True)

def test_default_suite_never_overrides_the_strict_snapshot_architecture():
    suite = harness.load_suite("docs/research/ptg2_benchmark_suite.example.json")
    variants = harness.variant_map(suite)

    assert variants
    for variant in variants.values():
        assert variant.get("env", {}).get(
            "HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3"
        ) == "postgres_binary_v3"

def test_copy_output_gate_detects_digest_mismatch():
    baseline_output_by_kind = {
        "serving": {"rows": 1, "sha256": "a"},
        "price_atom": {"rows": 1, "sha256": "b"},
    }
    candidate_output_by_kind = {
        "serving": {"rows": 1, "sha256": "z"},
        "price_atom": {"rows": 1, "sha256": "b"},
    }

    result = harness.compare_copy_outputs(
        baseline_output_by_kind,
        candidate_output_by_kind,
    )

    assert result == {"status": "failed", "mismatches": ["serving"]}

def test_collect_copy_outputs_includes_price_set_summary_shards(tmp_path):
    first = tmp_path / "price_set_summary.copy"
    second = tmp_path / "price_set_summary.copy.worker-1"
    first.write_text("set-b\t2.00\n", encoding="utf-8")
    second.write_text("set-a\t1.00\nset-c\t3.00\n", encoding="utf-8")

    outputs = harness.collect_copy_outputs(tmp_path)

    summary = outputs["price_set_summary"]
    assert summary["files"] == [str(first), str(second)]
    assert summary["rows"] == 3
    assert summary["sha256"] == harness.digest_lines(
        ["set-a\t1.00", "set-b\t2.00", "set-c\t3.00"]
    )

def test_dedupe_gate_detects_price_and_provider_set_mismatch():
    baseline_dedupe_by_metric = {
        "price_set_attempted": 10,
        "price_set_unique": 4,
        "price_set_duplicate": 6,
        "provider_set_attempted": 10,
        "provider_set_unique": 2,
        "provider_set_duplicate": 8,
    }
    candidate_dedupe_by_metric = {
        **baseline_dedupe_by_metric,
        "price_set_duplicate": 5,
        "provider_set_unique": 3,
    }

    result = harness.compare_dedupe(
        baseline_dedupe_by_metric,
        candidate_dedupe_by_metric,
    )

    assert result == {
        "status": "failed",
        "mismatches": ["price_set_duplicate", "provider_set_unique"],
    }

def test_gate_evaluation_accepts_matching_correctness_and_fast_candidate():
    benchmark_report_dict = {
        "results": [
            {
                "case_id": "case-a",
                "variant_id": "baseline",
                "status": "succeeded",
                "elapsed_seconds": 10.0,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {
                    "negotiated_rates": 2,
                    "serving_rate_attempted": 2,
                    "serving_rate_unique": 1,
                    "serving_rate_duplicate": 1,
                    "price_atom_attempted": 1,
                    "price_atom_unique": 1,
                    "price_atom_duplicate": 0,
                },
                "memory": {"peak_rss_kb": 1000},
            },
            {
                "case_id": "case-a",
                "variant_id": "parse_in_workers",
                "status": "succeeded",
                "elapsed_seconds": 8.0,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {
                    "negotiated_rates": 2,
                    "serving_rate_attempted": 2,
                    "serving_rate_unique": 1,
                    "serving_rate_duplicate": 1,
                    "price_atom_attempted": 1,
                    "price_atom_unique": 1,
                    "price_atom_duplicate": 0,
                },
                "memory": {"peak_rss_kb": 1100},
            },
        ]
    }

    gate_result = harness.evaluate_gates(
        benchmark_report_dict,
        {"min_improvement_pct": 15.0, "max_memory_growth_pct": 20.0},
    )

    assert gate_result["overall"] == "passed"
    candidate = gate_result["cases"]["case-a"][0]
    assert candidate["checks"]["performance"]["improvement_pct"] == 20.0
    assert candidate["checks"]["memory"]["growth_pct"] == 10.0

def test_gate_evaluation_can_skip_case_performance_gate():
    benchmark_report_dict = {
        "results": [
            {
                "case_id": "case-a",
                "variant_id": "baseline",
                "status": "succeeded",
                "elapsed_seconds": 10.0,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {
                    "negotiated_rates": 2,
                    "serving_rate_attempted": 2,
                    "serving_rate_unique": 1,
                    "serving_rate_duplicate": 1,
                    "price_atom_attempted": 1,
                    "price_atom_unique": 1,
                    "price_atom_duplicate": 0,
                },
                "memory": {},
            },
            {
                "case_id": "case-a",
                "variant_id": "parse_in_workers",
                "status": "succeeded",
                "elapsed_seconds": 10.5,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {
                    "negotiated_rates": 2,
                    "serving_rate_attempted": 2,
                    "serving_rate_unique": 1,
                    "serving_rate_duplicate": 1,
                    "price_atom_attempted": 1,
                    "price_atom_unique": 1,
                    "price_atom_duplicate": 0,
                },
                "memory": {},
            },
        ]
    }

    gate_result = harness.evaluate_gates(
        benchmark_report_dict,
        {"min_improvement_pct": 15.0, "max_memory_growth_pct": 20.0},
        case_gates={"case-a": {"performance": False}},
    )

    assert gate_result["overall"] == "passed"
    assert gate_result["cases"]["case-a"][0]["checks"]["performance"] == {
        "status": "skipped"
    }

def test_gate_evaluation_can_require_import_total_not_slower():
    report_dict = {
        "results": [
            {
                "case_id": "case-a",
                "variant_id": "current",
                "status": "succeeded",
                "import_run": {"import_done": {"total_seconds": 10.0}},
                "copy_outputs": {},
                "dedupe_summary": {},
                "memory": {},
            },
            {
                "case_id": "case-a",
                "variant_id": "postgres_binary",
                "status": "succeeded",
                "import_run": {"import_done": {"total_seconds": 9.8}},
                "copy_outputs": {},
                "dedupe_summary": {},
                "memory": {},
            },
        ]
    }

    gate_result = harness.evaluate_gates(
        report_dict,
        {},
        case_gates={
            "case-a": {
                "baseline_variant": "current",
                "performance": False,
                "memory": False,
                "min_import_total_improvement_pct": 0.0,
            }
        },
    )

    import_total_check = gate_result["cases"]["case-a"][0]["checks"]["import_total"]
    assert gate_result["overall"] == "passed"
    assert import_total_check["status"] == "passed"
    assert import_total_check["improvement_pct"] == 2.0

def test_gate_evaluation_fails_when_import_total_regresses():
    report_dict = {
        "results": [
            {
                "case_id": "case-a",
                "variant_id": "current",
                "status": "succeeded",
                "import_run": {"import_done": {"total_seconds": 10.0}},
                "copy_outputs": {},
                "dedupe_summary": {},
                "memory": {},
            },
            {
                "case_id": "case-a",
                "variant_id": "postgres_binary",
                "status": "succeeded",
                "import_run": {"import_done": {"total_seconds": 10.1}},
                "copy_outputs": {},
                "dedupe_summary": {},
                "memory": {},
            },
        ]
    }

    gate_result = harness.evaluate_gates(
        report_dict,
        {},
        case_gates={
            "case-a": {
                "baseline_variant": "current",
                "performance": False,
                "memory": False,
                "min_import_total_improvement_pct": 0.0,
            }
        },
    )

    import_total_check = gate_result["cases"]["case-a"][0]["checks"]["import_total"]
    assert gate_result["overall"] == "failed"
    assert import_total_check["status"] == "failed"
    assert import_total_check["improvement_pct"] == -1.0

def test_gate_evaluation_accepts_case_baseline_variant():
    benchmark_report_dict = {
        "results": [
            {
                "case_id": "case-a",
                "variant_id": "current",
                "status": "succeeded",
                "elapsed_seconds": 10.0,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {"negotiated_rates": 2},
                "memory": {},
            },
            {
                "case_id": "case-a",
                "variant_id": "smaller_chunks",
                "status": "succeeded",
                "elapsed_seconds": 11.0,
                "copy_outputs": {"serving": {"rows": 2, "sha256": "same"}},
                "dedupe_summary": {"negotiated_rates": 2},
                "memory": {},
            },
        ]
    }

    gate_result = harness.evaluate_gates(
        benchmark_report_dict,
        {"min_improvement_pct": 15.0},
        case_gates={"case-a": {"baseline_variant": "current", "performance": False}},
    )

    assert gate_result["overall"] == "passed"
    assert gate_result["cases"]["case-a"][0]["variant_id"] == "smaller_chunks"

def test_variant_can_disable_case_level_serving_storage_probe():
    case_by_key = {"analyze_serving_sidecar": True}

    assert harness._is_serving_storage_probe_enabled(case_by_key, {}) is True
    assert harness._is_serving_storage_probe_enabled(case_by_key, {"analyze_serving_sidecar": False}) is False
