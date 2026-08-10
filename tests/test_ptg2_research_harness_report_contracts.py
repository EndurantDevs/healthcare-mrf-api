# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import subprocess

import sys

from pathlib import Path

from threading import Thread

import pytest

from scripts.research import ptg2_experiment as harness

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
    fixture_document = harness.build_fixture_payload(
        {
            "fixture": "large_in_network",
            "negotiated_rates": 8,
            "billing_codes": 4,
            "provider_sets": 2,
            "price_reuse_mod": 2,
        }
    )

    assert len(fixture_document["provider_references"]) == 2
    assert len(fixture_document["in_network"]) == 4
    assert [
        len(procedure["negotiated_rates"])
        for procedure in fixture_document["in_network"]
    ] == [2, 2, 2, 2]
    rates = [
        price["negotiated_rate"]
        for procedure in fixture_document["in_network"]
        for rate in procedure["negotiated_rates"]
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

    expectation_result = harness.check_serving_index_expectations(
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

    assert expectation_result["status"] == "passed"
    assert all(
        check["passed"] for check in expectation_result["checks"].values()
    )

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
    benchmark_suite_dict = {
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

    report = harness.run_suite(
        benchmark_suite_dict,
        report_dir=tmp_path,
        dry_run=True,
    )
    result = report["results"][0]
    fixture_dir = Path(result["import_run"]["fixture_dir"])

    assert result["status"] == "dry_run"
    assert "--max-items" not in result["command"]
    assert result["import_run"]["fixture_dir"]
    assert (fixture_dir / "index.json").exists()
    assert (fixture_dir / "rates.json.gz").exists()

def test_markdown_report_includes_scanner_and_import_summary():
    benchmark_report_dict = {
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

    markdown = harness.render_markdown_report(benchmark_report_dict)

    assert "parse_workers=true<br>workers=2<br>producer_blocked_us=12" in markdown
    assert "raw_chunks=3<br>max_raw_chunk_bytes=1024<br>max_raw_chunk_rates=8" in markdown
    assert "validated<br>files=1<br>rates=7" in markdown
    assert "api=passed<br>code_lookup:p95=12.3ms,max=13.4ms,total=1024" in markdown
    assert "npi_reverse:p95=14.5ms,max=18.0ms,total=25" in markdown
    assert "passed<br>prices=7/7<br>npis=1/1" in markdown
    assert "passed<br>pg=4.00 KiB<br>artifact=1.00 KiB<br>gzip=512 B<br>ratio=4.0x" in markdown

def test_markdown_report_includes_copy_file_accounting():
    """Keep single-pass COPY-file counters visible in harness evidence."""
    report_dict = {
        "results": [
            {
                "case_id": "local",
                "variant_id": "single-pass",
                "kind": "local_ptg_cli",
                "status": "succeeded",
                "import_run": {
                    "import_done": {"status": "validated"},
                    "copy_file_accounting": {
                        "scanner_reported_files": 5,
                        "recovered_unreported_files": 0,
                        "fallback_row_count_files": 0,
                        "scanner_duplicate_files": 0,
                    },
                },
            }
        ]
    }

    markdown = harness.render_markdown_report(report_dict)

    assert "copy_files=reported:5,recovered:0,fallback:0,duplicates:0" in markdown

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
