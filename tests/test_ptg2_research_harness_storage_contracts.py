# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import subprocess

import sys

from pathlib import Path

from threading import Thread

import pytest

from scripts.research import ptg2_experiment as harness

def test_gate_evaluation_accepts_storage_only_case():
    benchmark_report_dict = {
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

    gate_result = harness.evaluate_gates(
        benchmark_report_dict,
        {"min_storage_ratio": 15.0},
    )

    assert gate_result["overall"] == "passed"
    candidate = gate_result["cases"]["storage-case"][0]
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
    benchmark_report_dict = {
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

    gate_result = harness.evaluate_gates(
        benchmark_report_dict,
        {"min_storage_ratio": 15.0},
    )

    assert gate_result["overall"] == "failed"
    assert gate_result["cases"]["storage-case"][0]["checks"]["storage"]["status"] == "failed"

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
    assert config["zip5"] == "60652"
    assert config["radius_miles"] == 25.0

def test_dry_run_writes_report(tmp_path):
    benchmark_suite_dict = {
        "variants": [{"id": "baseline"}],
        "cases": [{"id": "case-a", "kind": "scanner_fixture", "fixture": "duplicate_serving", "variants": ["baseline"]}],
    }

    report = harness.run_suite(
        benchmark_suite_dict,
        report_dir=tmp_path,
        dry_run=True,
    )

    report_paths = list(Path(tmp_path).glob("run-*/report.json"))
    result = report["results"][0]
    assert result["status"] == "dry_run"
    assert result["env_overrides"]["HLTHPRT_PTG2_SNAPSHOT_ARCH"] == "postgres_binary_v3"
    summary_path = Path(
        result["env_overrides"][
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH"
        ]
    )
    assert summary_path.name == "price_set_summary.copy"
    assert summary_path.parent.name == "baseline"
    assert report_paths

def test_local_ptg_cli_dry_run_writes_fixture_and_command(tmp_path, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_USER", "tester")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5440")
    benchmark_suite_dict = {
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

    report = harness.run_suite(
        benchmark_suite_dict,
        report_dir=tmp_path,
        dry_run=True,
    )
    dry_run_result = report["results"][0]
    fixture_dir = Path(dry_run_result["import_run"]["fixture_dir"])

    assert dry_run_result["status"] == "dry_run"
    assert dry_run_result["kind"] == "local_ptg_cli"
    assert dry_run_result["env_overrides"]["HLTHPRT_DB_PORT"] == "5440"
    assert dry_run_result["env_overrides"]["HLTHPRT_DB_USER"] == "tester"
    assert "main.py" in dry_run_result["command"]
    assert (fixture_dir / "index.json").exists()
    assert (fixture_dir / "rates.json.gz").exists()

def test_original_file_summary_counts_unique_prices(tmp_path):
    fixture_case_dict = {
        "id": "full-file",
        "fixture": "large_in_network",
        "negotiated_rates": 3,
    }
    fixture_dir = tmp_path / "fixture"
    harness.write_ptg_toc_fixture(
        fixture_case_dict,
        fixture_dir,
        base_url="http://127.0.0.1:1",
    )

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
    fixture_document = harness.build_fixture_payload(
        {
            "fixture": "large_in_network",
            "negotiated_rates": 1,
            "additional_information_bytes": 128,
        }
    )

    price = fixture_document["in_network"][0]["negotiated_rates"][0][
        "negotiated_prices"
    ][0]
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
