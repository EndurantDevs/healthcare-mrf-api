# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from pathlib import Path

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
        "'summary': {'elapsed_seconds': 0.12}}}"
    )

    summary = harness.parse_serving_only_summary(text)

    assert summary["serving_rates"] == 1
    assert summary["scanner"]["config"]["parse_in_workers"] is True
    assert summary["scanner"]["summary"]["elapsed_seconds"] == 0.12


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


def test_copy_output_gate_detects_digest_mismatch():
    baseline = {
        "serving": {"rows": 1, "sha256": "a"},
        "price_atom": {"rows": 1, "sha256": "b"},
    }
    candidate = {
        "serving": {"rows": 1, "sha256": "z"},
        "price_atom": {"rows": 1, "sha256": "b"},
    }

    result = harness.compare_copy_outputs(baseline, candidate)

    assert result == {"status": "failed", "mismatches": ["serving"]}


def test_gate_evaluation_accepts_matching_correctness_and_fast_candidate():
    report = {
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

    result = harness.evaluate_gates(report, {"min_improvement_pct": 15.0, "max_memory_growth_pct": 20.0})

    assert result["overall"] == "passed"
    candidate = result["cases"]["case-a"][0]
    assert candidate["checks"]["performance"]["improvement_pct"] == 20.0
    assert candidate["checks"]["memory"]["growth_pct"] == 10.0


def test_gate_evaluation_can_skip_case_performance_gate():
    report = {
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

    result = harness.evaluate_gates(
        report,
        {"min_improvement_pct": 15.0, "max_memory_growth_pct": 20.0},
        case_gates={"case-a": {"performance": False}},
    )

    assert result["overall"] == "passed"
    assert result["cases"]["case-a"][0]["checks"]["performance"] == {"status": "skipped"}


def test_dry_run_writes_report(tmp_path):
    suite = {
        "variants": [{"id": "baseline"}],
        "cases": [{"id": "case-a", "kind": "scanner_fixture", "fixture": "duplicate_serving", "variants": ["baseline"]}],
    }

    report = harness.run_suite(suite, report_dir=tmp_path, dry_run=True)

    report_paths = list(Path(tmp_path).glob("run-*/report.json"))
    assert report["results"][0]["status"] == "dry_run"
    assert report_paths


def test_local_ptg_cli_dry_run_writes_fixture_and_command(tmp_path, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_USER", "tester")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5440")
    suite = {
        "variants": [{"id": "baseline", "env": {"HLTHPRT_PTG2_RUST_WORKERS": "2"}}],
        "cases": [
            {
                "id": "local-db-smoke",
                "kind": "local_ptg_cli",
                "fixture": "duplicate_serving",
                "variants": ["baseline"],
            }
        ],
    }

    report = harness.run_suite(suite, report_dir=tmp_path, dry_run=True)
    result = report["results"][0]
    fixture_dir = Path(result["import_run"]["fixture_dir"])

    assert result["status"] == "dry_run"
    assert result["kind"] == "local_ptg_cli"
    assert result["env_overrides"]["HLTHPRT_DB_PORT"] == "5440"
    assert result["env_overrides"]["HLTHPRT_DB_USER"] == "tester"
    assert "main.py" in result["command"]
    assert (fixture_dir / "index.json").exists()
    assert (fixture_dir / "rates.json.gz").exists()


def test_markdown_report_includes_scanner_and_import_summary():
    report = {
        "generated_at": "20260620T000000Z",
        "gates": {"overall": "passed"},
        "results": [
            {
                "case_id": "local",
                "variant_id": "default",
                "kind": "local_ptg_cli",
                "status": "succeeded",
                "elapsed_seconds": 1.25,
                "scanner_config": {"parse_in_workers": True, "worker_count": 2},
                "scanner_summary": {"producer_blocked_micros": 12},
                "import_run": {"import_done": {"status": "validated", "files_processed": 1, "serving_rates": 7}},
            }
        ],
    }

    markdown = harness.render_markdown_report(report)

    assert "parse_workers=true<br>workers=2<br>producer_blocked_us=12" in markdown
    assert "validated<br>files=1<br>rates=7" in markdown
