# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from pathlib import Path

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


def test_parse_ps_memory_parses_rss_and_vsz():
    assert harness.parse_ps_memory(" 2048 123456\n") == {
        "vmrss_kb": 2048,
        "vmsize_kb": 123456,
    }
    assert harness.parse_ps_memory("bad output") == {}


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
    report = {
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

    result = harness.evaluate_gates(
        report,
        {"min_improvement_pct": 15.0},
        case_gates={"case-a": {"baseline_variant": "current", "performance": False}},
    )

    assert result["overall"] == "passed"
    assert result["cases"]["case-a"][0]["variant_id"] == "smaller_chunks"


def test_gate_evaluation_accepts_storage_only_case():
    report = {
        "results": [
            {
                "case_id": "storage-case",
                "variant_id": "serving_sidecar",
                "status": "succeeded",
                "import_run": {
                    "verification": {"status": "passed"},
                    "storage": {
                        "status": "passed",
                        "reduction_ratio_vs_pg_total": 19.96,
                        "gzip_reduction_ratio_vs_pg_total": 684.46,
                        "candidate": {"roundtrip": "passed"},
                    },
                },
            }
        ]
    }

    result = harness.evaluate_gates(report, {"min_storage_ratio": 15.0})

    assert result["overall"] == "passed"
    candidate = result["cases"]["storage-case"][0]
    assert candidate["overall"] == "passed"
    assert candidate["checks"]["storage"]["ratio"] == 19.96
    assert candidate["checks"]["storage"]["required_ratio"] == 15.0


def test_postgres_posting_candidate_sql_builds_array_posting_indexes():
    statements = harness.postgres_posting_candidate_sql(
        serving_table="mrf.ptg2_serving_snapshot",
        posting_table="mrf.ptg2_research_posting_snapshot",
        price_set_dictionary_table="mrf.ptg2_research_price_set_dict_snapshot",
        block_rows=128,
    )
    sql = "\n".join(statements)

    assert "CREATE UNLOGGED TABLE mrf.ptg2_research_posting_snapshot AS" in sql
    assert "CREATE UNLOGGED TABLE mrf.ptg2_research_price_set_dict_snapshot AS" in sql
    assert "PARTITION BY serving.code_key" in sql
    assert ") / 128" in sql
    assert "array_agg(provider_set_key ORDER BY provider_set_key, price_set_key)::integer[]" in sql
    assert "array_agg(price_set_key ORDER BY provider_set_key, price_set_key)::integer[]" in sql
    assert "USING gin (provider_set_keys)" in sql
    assert "USING gin (price_set_keys)" in sql


def test_storage_gate_prefers_postgres_posting_candidate_when_selected():
    storage_payload_dict = {
        "status": "passed",
        "preferred_candidate": "postgres_posting",
        "reduction_ratio_vs_pg_total": 2.0,
        "combined_candidate": {"roundtrip": "passed"},
        "postgres_posting_candidate": {
            "status": "passed",
            "reduction_ratio_vs_pg_total": 21.5,
            "candidate": {"roundtrip": "passed"},
        },
    }

    result = harness.compare_storage(storage_payload_dict, min_storage_ratio=15.0)

    assert result["status"] == "passed"
    assert result["ratio"] == 21.5


def test_storage_gate_prefers_postgres_binary_candidate_when_selected():
    storage_payload_dict = {
        "status": "passed",
        "preferred_candidate": "postgres_binary",
        "reduction_ratio_vs_pg_total": 2.0,
        "combined_candidate": {"roundtrip": "passed"},
        "postgres_binary_candidate": {
            "status": "passed",
            "reduction_ratio_vs_pg_total": 24.25,
            "candidate": {"roundtrip": "passed"},
        },
    }

    result = harness.compare_storage(storage_payload_dict, min_storage_ratio=15.0)

    assert result["status"] == "passed"
    assert result["ratio"] == 24.25


def test_candidate_storage_compares_binary_snapshot_to_baseline_table():
    baseline_storage_by_role = {
        "status": "passed",
        "storage": {"total_bytes": 4_685_824},
    }
    candidate_storage_by_role = {
        "status": "passed",
        "preferred_candidate": "postgres_binary",
        "postgres_binary_snapshot": {
            "storage": {"total_bytes": 245_760},
            "writer": "rust_stream",
        },
    }

    result = harness.compare_candidate_storage(
        baseline_storage_by_role,
        candidate_storage_by_role,
        min_storage_ratio=15.0,
    )

    assert result["status"] == "passed"
    assert result["ratio"] == 19.067
    assert result["candidate"] == "postgres_binary_snapshot"


def test_candidate_storage_can_gate_full_snapshot_total_ratio():
    baseline_storage_by_role = {
        "status": "passed",
        "storage": {"total_bytes": 70_967_296},
        "snapshot_footprint": {"total_logical_bytes": 71_709_696},
    }
    candidate_storage_by_role = {
        "status": "passed",
        "preferred_candidate": "postgres_binary",
        "postgres_binary_snapshot": {
            "storage": {"total_bytes": 2_826_240},
            "writer": "rust_stream",
        },
        "snapshot_footprint": {"total_logical_bytes": 3_568_640},
    }

    result = harness.compare_candidate_storage(
        baseline_storage_by_role,
        candidate_storage_by_role,
        min_storage_ratio=15.0,
        min_snapshot_total_ratio=20.0,
    )

    assert result["status"] == "passed"
    assert result["ratio"] == 25.11
    assert result["snapshot_status"] == "passed"
    assert result["snapshot_ratio"] == 20.094
    assert result["required_snapshot_total_ratio"] == 20.0


def test_candidate_storage_fails_when_full_snapshot_ratio_is_too_low():
    baseline_storage_by_role = {
        "status": "passed",
        "storage": {"total_bytes": 4_685_824},
        "snapshot_footprint": {"total_logical_bytes": 4_855_562},
    }
    candidate_storage_by_role = {
        "status": "passed",
        "preferred_candidate": "postgres_binary",
        "postgres_binary_snapshot": {
            "storage": {"total_bytes": 155_648},
            "writer": "rust_stream",
        },
        "snapshot_footprint": {"total_logical_bytes": 337_358},
    }

    result = harness.compare_candidate_storage(
        baseline_storage_by_role,
        candidate_storage_by_role,
        min_storage_ratio=15.0,
        min_snapshot_total_ratio=20.0,
    )

    assert result["status"] == "failed"
    assert result["ratio"] == 30.105
    assert result["snapshot_status"] == "failed"
    assert result["snapshot_ratio"] == 14.393


def test_snapshot_storage_table_entries_dedupes_materialized_and_legacy_tables():
    entries = harness.snapshot_storage_table_entries(
        {
            "table": "mrf.ptg2_serving_token",
            "serving_binary_table": "mrf.ptg2_binary_token",
            "ignored_table": "bad-name",
            "materialized_tables": {
                "serving": "mrf.ptg2_serving_token",
                "price_atom": "mrf.ptg2_price_atom_token",
                "serving_binary": "mrf.ptg2_binary_token",
            },
        }
    )

    by_table = {entry["table"]: entry["role"] for entry in entries}

    assert by_table["mrf.ptg2_serving_token"] == "serving,table"
    assert by_table["mrf.ptg2_binary_token"] == "serving_binary,serving_binary_table"
    assert by_table["mrf.ptg2_price_atom_token"] == "price_atom"
    assert "bad-name" not in by_table


def test_variant_can_disable_case_level_serving_storage_probe():
    case_by_key = {"analyze_serving_sidecar": True}

    assert harness._is_serving_storage_probe_enabled(case_by_key, {}) is True
    assert harness._is_serving_storage_probe_enabled(case_by_key, {"analyze_serving_sidecar": False}) is False


def test_serving_db_binary_records_roundtrip_forward_and_reverse():
    by_code_rows = [
        (1, 10, 2, "00000000-0000-0000-0000-000000000001"),
        (1, 12, 3, "00000000-0000-0000-0000-000000000002"),
        (2, 10, 2, "00000000-0000-0000-0000-000000000001"),
    ]
    by_provider_rows = [
        (10, 1, 2, "00000000-0000-0000-0000-000000000001"),
        (10, 2, 2, "00000000-0000-0000-0000-000000000001"),
        (12, 1, 3, "00000000-0000-0000-0000-000000000002"),
    ]

    forward = harness.build_serving_by_code_db_records(by_code_rows)
    reverse = harness.build_serving_by_provider_set_db_records(by_provider_rows)

    assert forward["roundtrip"] == "passed"
    assert forward["row_count"] == 3
    assert forward["code_count"] == 2
    assert forward["payload_bytes"] > 0
    assert reverse["roundtrip"] == "passed"
    assert reverse["row_count"] == 3
    assert reverse["provider_set_count"] == 2
    assert reverse["payload_bytes"] > 0


def test_storage_report_renders_postgres_posting_details():
    rendered = harness.format_storage_analysis(
        {
            "import_run": {
                "storage": {
                    "status": "passed",
                    "storage": {"total_bytes": 4096},
                    "candidate": {"artifact_bytes": 1024, "roundtrip": "passed"},
                    "postgres_posting_candidate": {
                        "reduction_ratio_vs_pg_total": 20.0,
                        "build_elapsed_seconds": 1.25,
                        "storage": {"candidate_total_bytes": 2048},
                        "benchmarks": {
                            "code_lookup": {"execution_ms": 0.4},
                            "code_provider_overlap": {"execution_ms": 1.2},
                        },
                    },
                }
            }
        }
    )

    assert "pg_posting=2.00 KiB" in rendered
    assert "pg_posting_ratio=20.0x" in rendered
    assert "pg_posting_build=1.25s" in rendered
    assert "code_lookup=0.4ms" in rendered
    assert "code_provider_overlap=1.2ms" in rendered


def test_storage_report_renders_postgres_binary_details():
    """Cover compact binary and full snapshot footprint report fields together."""
    storage_payload_dict = {
        "status": "passed",
        "storage": {"total_bytes": 4096},
        "candidate": {"artifact_bytes": 1024, "roundtrip": "passed"},
        "postgres_binary_candidate": {
            "reduction_ratio_vs_pg_total": 24.0,
            "build_elapsed_seconds": 1.75,
            "storage": {"artifact_total_bytes": 2048, "artifact_payload_bytes": 512},
            "benchmarks": {"by_code_fetch": {"execution_ms": 0.3}, "by_provider_set_fetch": {"execution_ms": 0.9}},
        },
        "postgres_binary_snapshot": {
            "writer": "rust_stream",
            "build_elapsed_seconds": 2.25,
            "storage": {"total_bytes": 3072, "payload_bytes": 768, "raw_payload_bytes": 2048, "compressed_saved_bytes": 1280},
        },
        "snapshot_footprint": {
            "total_logical_bytes": 8192,
            "table_total_bytes": 6144,
            "artifact_stored_payload_bytes": 2048,
            "artifact_tuple_bytes": 2304,
            "top_components": [{"name": "serving_binary", "bytes": 3072}, {"name": "price_atom", "bytes": 2048}],
        },
    }
    rendered = harness.format_storage_analysis({"import_run": {"storage": storage_payload_dict}})

    assert "pg_binary=2.00 KiB" in rendered
    assert "pg_binary_payload=512 B" in rendered
    assert "pg_binary_snapshot=3.00 KiB" in rendered
    assert "pg_binary_snapshot_payload=768 B" in rendered
    assert "pg_binary_snapshot_raw=2.00 KiB" in rendered
    assert "pg_binary_snapshot_saved=1.25 KiB" in rendered
    assert "pg_binary_writer=rust_stream" in rendered
    assert "pg_binary_snapshot_build=2.25s" in rendered
    assert "pg_binary_ratio=24.0x" in rendered
    assert "pg_binary_build=1.75s" in rendered
    assert "snapshot_total=8.00 KiB" in rendered
    assert "snapshot_tables=6.00 KiB" in rendered
    assert "snapshot_artifacts=2.00 KiB" in rendered
    assert "snapshot_artifact_tuples=2.25 KiB" in rendered
    assert "snapshot_top=serving_binary=3.00 KiB,price_atom=2.00 KiB" in rendered
    assert "by_code_fetch=0.3ms" in rendered
    assert "by_provider_set_fetch=0.9ms" in rendered


def test_gate_evaluation_fails_storage_ratio_below_threshold():
    report = {
        "results": [
            {
                "case_id": "storage-case",
                "variant_id": "serving_sidecar",
                "status": "succeeded",
                "import_run": {
                    "verification": {"status": "passed"},
                    "storage": {
                        "status": "passed",
                        "reduction_ratio_vs_pg_total": 3.4,
                        "candidate": {"roundtrip": "passed"},
                    },
                },
            }
        ]
    }

    result = harness.evaluate_gates(report, {"min_storage_ratio": 15.0})

    assert result["overall"] == "failed"
    assert result["cases"]["storage-case"][0]["checks"]["storage"]["status"] == "failed"


def test_api_latency_probe_failure_classification():
    probes_by_name = {
        "code_lookup": {"payload": True, "p95_ms": 39.9, "max_ms": 91.0},
        "npi_reverse": {"payload": True, "p95_ms": 41.0, "max_ms": 41.0},
        "empty_lookup": {"payload": False, "p95_ms": 1.0, "max_ms": 1.0},
    }

    failed = harness._failed_api_latency_probes(probes_by_name, 40.0)

    assert failed == ["code_lookup:max_ms", "npi_reverse:p95_ms", "empty_lookup:no_payload"]


def test_api_latency_probe_separates_p95_budget_from_sample_ceiling():
    probes_by_name = {
        "code_lookup": {"payload": True, "p95_ms": 39.9, "max_ms": 91.0},
        "npi_reverse": {"payload": True, "p95_ms": 40.1, "max_ms": 41.0},
    }

    failed = harness._failed_api_latency_probes(
        probes_by_name,
        max_latency_ms=100.0,
        p95_latency_ms=40.0,
    )

    assert failed == ["npi_reverse:p95_ms"]


def test_api_latency_probe_config_preserves_zero_warmup():
    config = harness._api_latency_probe_config(
        snapshot_id="snapshot",
        case={"api_probe_warmup": 0, "api_probe_iterations": 1},
        variant={"api_probe_warmup": 7},
    )

    assert config["warmup"] == 0
    assert config["iterations"] == 1


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


def test_original_file_summary_counts_unique_prices(tmp_path):
    case = {
        "id": "full-file",
        "fixture": "large_in_network",
        "negotiated_rates": 3,
    }
    fixture_dir = tmp_path / "fixture"
    harness.write_ptg_toc_fixture(case, fixture_dir, base_url="http://127.0.0.1:1")

    summary = harness.expected_original_file_summary(fixture_dir / "rates.json.gz")

    assert summary["provider_references"] == 1
    assert summary["in_network_items"] == 1
    assert summary["negotiated_rates"] == 3
    assert summary["negotiated_prices"] == 3
    assert summary["unique_serving_rates"] == 3
    assert summary["unique_price_atoms"] == 3
    assert summary["unique_provider_npis"] == 1
    assert len(summary["price_atom_digest"]) == 32


def test_large_fixture_can_add_bulky_rate_payload():
    payload = harness.build_fixture_payload(
        {
            "fixture": "large_in_network",
            "negotiated_rates": 1,
            "additional_information_bytes": 128,
        }
    )

    price = payload["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0]
    assert len(price["additional_information"]) == 128


def test_large_fixture_can_omit_npi_members(tmp_path):
    fixture_map = {
        "id": "tin-only",
        "fixture": "large_in_network",
        "negotiated_rates": 4,
        "provider_sets": 2,
        "omit_provider_npis": True,
    }
    fixture_dir = tmp_path / "fixture"
    harness.write_ptg_toc_fixture(fixture_map, fixture_dir, base_url="http://127.0.0.1:1")

    summary = harness.expected_original_file_summary(fixture_dir / "rates.json.gz")

    assert summary["negotiated_prices"] == 4
    assert summary["unique_serving_rates"] == 4
    assert summary["unique_provider_npis"] == 0


def test_original_file_summary_distinguishes_declared_and_used_provider_npis(tmp_path):
    fixture_map = {
        "id": "partially-used-providers",
        "fixture": "large_in_network",
        "negotiated_rates": 4,
        "provider_sets": 4,
        "price_reuse_mod": 4,
    }
    harness.write_ptg_toc_fixture(fixture_map, tmp_path, base_url="http://127.0.0.1:1")

    summary = harness.expected_original_file_summary(tmp_path / "rates.json.gz")

    assert summary["unique_provider_npis"] == 4
    assert summary["used_provider_npis"] == 1


def test_large_fixture_can_shape_codes_and_reused_prices(tmp_path):
    payload = harness.build_fixture_payload(
        {
            "fixture": "large_in_network",
            "negotiated_rates": 8,
            "billing_codes": 4,
            "provider_sets": 2,
            "price_reuse_mod": 2,
        }
    )

    assert len(payload["provider_references"]) == 2
    assert len(payload["in_network"]) == 4
    assert [len(item["negotiated_rates"]) for item in payload["in_network"]] == [2, 2, 2, 2]
    rates = [
        price["negotiated_rate"]
        for item in payload["in_network"]
        for rate in item["negotiated_rates"]
        for price in rate["negotiated_prices"]
    ]
    assert rates == [100, 101, 100, 101, 100, 101, 100, 101]

    harness.write_ptg_toc_fixture(
            {
                "id": "reuse",
                "fixture": "large_in_network",
                "negotiated_rates": 8,
                "billing_codes": 4,
                "provider_sets": 2,
                "price_reuse_mod": 2,
            },
        tmp_path,
        base_url="http://127.0.0.1:1",
    )
    summary = harness.expected_original_file_summary(tmp_path / "rates.json.gz")
    assert summary["unique_price_atoms"] == 2
    assert summary["unique_serving_rates"] == 8


def test_serving_index_table_reads_materialized_table_names():
    serving_index = {
        "materialized_tables": {
            "serving": "mrf.ptg2_serving_snap",
            "price_atom": "mrf.ptg2_price_atom_snap",
            "provider_group_member": "mrf.ptg2_provider_group_member_snap",
        }
    }

    assert harness.serving_index_table(serving_index, "table", "serving_table") == "mrf.ptg2_serving_snap"
    assert harness.serving_index_table(serving_index, "price_atom_table") == "mrf.ptg2_price_atom_snap"
    assert harness.serving_index_table(serving_index, "provider_group_member_table") == "mrf.ptg2_provider_group_member_snap"


def test_serving_index_table_reads_v2_provider_scope():
    serving_index = {
        "materialized_tables": {
            "provider_npi_scope": "mrf.ptg2_provider_npi_scope_snap",
        }
    }

    assert harness.serving_index_table(
        serving_index,
        "provider_group_member_table",
        "provider_npi_scope_table",
    ) == "mrf.ptg2_provider_npi_scope_snap"


def test_serving_index_expectations_check_tableless_postgres_binary(monkeypatch):
    def fake_psql_json(_env, sql):
        if "array_agg(DISTINCT artifact_kind" in sql:
            return {
                "kinds": [
                    "by_code_grouped",
                    "by_code_price_dictionary",
                    "by_provider_set",
                    "by_provider_set_price_dictionary",
                    "price_set_atoms",
                    "price_set_atoms_by_id",
                    "provider_set_count_dictionary",
                ]
            }
        if "ptg2_serving_binary_snap" in sql:
            return {"exists": True}
        if "ptg2_serving_snap" in sql:
            return {"exists": False}
        raise AssertionError(sql)

    monkeypatch.setattr(harness, "psql_json", fake_psql_json)
    serving_index = {
        "table": "mrf.ptg2_serving_snap",
        "serving_binary_table": "mrf.ptg2_serving_binary_snap",
        "serving_row_strategy": "postgres_binary",
        "serving_table_retained": False,
        "serving_binary": {"writer": "rust_stream"},
        "artifacts": {"provider_npi": {"name": "provider_npi"}},
    }

    result = harness.check_serving_index_expectations(
        {"HLTHPRT_DB_SCHEMA": "mrf"},
        serving_index,
        {
            "expect_serving_row_strategy": "postgres_binary",
            "expect_serving_table_retained": False,
            "expect_serving_table_exists": False,
            "expect_serving_binary_table_exists": True,
            "expect_serving_binary_writer": "rust_stream",
            "expect_serving_binary_kinds": [
                "by_code_grouped",
                "by_code_price_dictionary",
                "by_provider_set",
                "by_provider_set_price_dictionary",
                "price_set_atoms",
                "price_set_atoms_by_id",
                "provider_set_count_dictionary",
            ],
            "expect_serving_sidecar_artifacts": False,
        },
    )

    assert result["status"] == "passed"
    assert all(check["passed"] for check in result["checks"].values())


def test_serving_index_expectations_detect_serving_sidecar_artifacts(monkeypatch):
    monkeypatch.setattr(harness, "pg_table_exists", lambda *_args: True)
    result = harness.check_serving_index_expectations(
        {"HLTHPRT_DB_SCHEMA": "mrf"},
        {
            "serving_row_strategy": "postgres_binary",
            "artifacts": {
                "serving_by_code": {"name": "serving_by_code", "path": "/tmp/serving_by_code.ptg2sbc"},
            },
        },
        {
            "expect_serving_row_strategy": "postgres_binary",
            "expect_serving_sidecar_artifacts": False,
        },
    )

    assert result["status"] == "failed"
    assert result["checks"]["serving_sidecar_artifacts"] == {
        "expected": False,
        "actual": True,
        "passed": False,
    }


def test_serving_index_expectations_report_mismatch(monkeypatch):
    monkeypatch.setattr(harness, "pg_table_exists", lambda *_args: True)

    result = harness.check_serving_index_expectations(
        {"HLTHPRT_DB_SCHEMA": "mrf"},
        {"serving_row_strategy": "table_and_postgres_binary"},
        {"expect_serving_row_strategy": "postgres_binary"},
    )

    assert result["status"] == "failed"
    assert result["checks"]["serving_row_strategy"]["actual"] == "table_and_postgres_binary"


def test_format_api_latency_renders_probe_timings():
    rendered = harness.format_api_latency(
        {
            "status": "passed",
            "probes": {
                "code_lookup": {"p95_ms": 12.3, "max_ms": 14.1, "total": 1024},
                "npi_reverse": {"p95_ms": 18.4, "max_ms": 20.0, "total": 25},
            },
        }
    )

    assert "api=passed" in rendered
    assert "code_lookup:p95=12.3ms,max=14.1ms,total=1024" in rendered
    assert "npi_reverse:p95=18.4ms,max=20.0ms,total=25" in rendered


def test_serving_by_code_candidate_roundtrips_and_compresses(tmp_path):
    rows = [
        (1, 10, 2, "00000000-0000-0000-0000-000000000001"),
        (1, 12, 3, "00000000-0000-0000-0000-000000000002"),
        (2, 7, 1, "00000000-0000-0000-0000-000000000001"),
    ]

    result = harness.write_serving_by_code_candidate(rows, tmp_path / "serving.ptg2sbc")

    assert result["roundtrip"] == "passed"
    assert result["row_count"] == 3
    assert result["code_count"] == 2
    assert result["price_set_count"] == 2
    assert result["artifact_bytes"] > 0
    assert result["gzip_bytes"] > 0
    assert result["source_sha256"] == result["decoded_sha256"]


def test_serving_by_provider_set_candidate_roundtrips_and_groups_patterns(tmp_path):
    rows = [
        (10, 1, 2, "00000000-0000-0000-0000-000000000001"),
        (10, 1, 2, "00000000-0000-0000-0000-000000000002"),
        (10, 2, 2, "00000000-0000-0000-0000-000000000001"),
        (10, 2, 2, "00000000-0000-0000-0000-000000000002"),
        (12, 1, 1, "00000000-0000-0000-0000-000000000001"),
    ]

    result = harness.write_serving_by_provider_set_candidate(rows, tmp_path / "serving.ptg2sbp")

    assert result["roundtrip"] == "passed"
    assert result["row_count"] == 5
    assert result["provider_set_count"] == 2
    assert result["code_count"] == 2
    assert result["price_set_count"] == 2
    assert result["pattern_count"] == 2
    assert result["artifact_bytes"] > 0
    assert result["gzip_bytes"] > 0
    assert result["source_sha256"] == result["decoded_sha256"]


def test_local_ptg_cli_full_file_dry_run_omits_max_items(tmp_path, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_USER", "tester")
    suite = {
        "variants": [{"id": "parse_in_workers"}],
        "cases": [
            {
                "id": "local-full-file-verify",
                "kind": "local_ptg_cli",
                "fixture": "large_in_network",
                "full_file": True,
                "verify_original": True,
                "variants": ["parse_in_workers"],
            }
        ],
    }

    report = harness.run_suite(suite, report_dir=tmp_path, dry_run=True)
    result = report["results"][0]
    fixture_dir = Path(result["import_run"]["fixture_dir"])

    assert result["status"] == "dry_run"
    assert "--max-items" not in result["command"]
    assert result["import_run"]["fixture_dir"]
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
                "scanner_summary": {
                    "producer_blocked_micros": 12,
                    "raw_chunk_count": 3,
                    "raw_chunk_max_bytes": 1024,
                    "raw_chunk_max_rates": 8,
                },
                "import_run": {
                    "import_done": {"status": "validated", "files_processed": 1, "serving_rates": 7},
                    "api_latency": {
                        "status": "passed",
                        "probes": {
                            "code_lookup": {"p95_ms": 12.3, "max_ms": 13.4, "total": 1024},
                            "npi_reverse": {"p95_ms": 14.5, "max_ms": 18.0, "total": 25},
                        },
                    },
                    "verification": {
                        "status": "passed",
                        "expected": {"unique_price_atoms": 7, "unique_provider_npis": 1},
                        "db": {"price_atom_rows": 7, "provider_npis": 1},
                    },
                    "storage": {
                        "status": "passed",
                        "storage": {"total_bytes": 4096},
                        "candidate": {"artifact_bytes": 1024, "gzip_bytes": 512, "roundtrip": "passed"},
                        "reduction_ratio_vs_pg_total": 4.0,
                        "gzip_reduction_ratio_vs_pg_total": 8.0,
                    },
                },
            }
        ],
    }

    markdown = harness.render_markdown_report(report)

    assert "parse_workers=true<br>workers=2<br>producer_blocked_us=12" in markdown
    assert "raw_chunks=3<br>max_raw_chunk_bytes=1024<br>max_raw_chunk_rates=8" in markdown
    assert "validated<br>files=1<br>rates=7" in markdown
    assert "api=passed<br>code_lookup:p95=12.3ms,max=13.4ms,total=1024" in markdown
    assert "npi_reverse:p95=14.5ms,max=18.0ms,total=25" in markdown
    assert "passed<br>prices=7/7<br>npis=1/1" in markdown
    assert "passed<br>pg=4.00 KiB<br>artifact=1.00 KiB<br>gzip=512 B<br>ratio=4.0x" in markdown


def test_markdown_report_includes_serving_arch_summary():
    """Keep tableless PostgreSQL-binary invariants visible in the Markdown report."""
    report_dict = {
        "generated_at": "20260709T000000Z",
        "gates": {"overall": "passed"},
        "results": [
            {
                "case_id": "local",
                "variant_id": "postgres_binary_final",
                "kind": "local_ptg_cli",
                "status": "succeeded",
                "import_run": {
                    "serving_index_checks": {
                        "status": "passed",
                        "checks": {
                            "serving_row_strategy": {"actual": "postgres_binary", "passed": True},
                            "serving_table_exists": {"actual": False, "passed": True},
                            "serving_binary_table_exists": {"actual": True, "passed": True},
                            "serving_binary_writer": {"actual": "rust_stream", "passed": True},
                            "serving_binary_kinds": {"actual": ["by_code_grouped", "price_set_atoms"], "passed": True},
                            "serving_sidecar_artifacts": {"actual": False, "passed": True},
                        },
                    },
                },
            }
        ],
    }

    markdown = harness.render_markdown_report(report_dict)

    assert "Serving Arch" in markdown
    assert "passed<br>serving_row_strategy=postgres_binary" in markdown
    assert "serving_table_exists=false" in markdown
    assert "serving_binary_table_exists=true" in markdown
    assert "serving_binary_writer=rust_stream" in markdown
    assert "serving_binary_kinds=by_code_grouped,price_set_atoms" in markdown
    assert "serving_sidecar_artifacts=false" in markdown
