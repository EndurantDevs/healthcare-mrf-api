# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import gzip
import importlib
import io
import json
import os
import subprocess
import time
from types import SimpleNamespace
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from aiohttp import web


process_pkg = importlib.import_module("process")
process_ptg = importlib.import_module("process.ptg")
ptg_artifacts = importlib.import_module("process.ptg_parts.artifacts")
ptg_artifact_streams = importlib.import_module("process.ptg_parts.artifact_streams")
ptg_canonical = importlib.import_module("process.ptg_parts.canonical")
ptg_compact_indexes = importlib.import_module("process.ptg_parts.compact_indexes")
ptg_compact_state = importlib.import_module("process.ptg_parts.compact_state")
ptg_copy_load = importlib.import_module("process.ptg_parts.copy_load")
ptg_db_tables = importlib.import_module("process.ptg_parts.db_tables")
ptg_domain = importlib.import_module("process.ptg_parts.domain")
ptg_import_rows = importlib.import_module("process.ptg_parts.import_rows")
ptg_json_streams = importlib.import_module("process.ptg_parts.json_streams")
ptg_progress = importlib.import_module("process.ptg_parts.progress")
ptg_row_helpers = importlib.import_module("process.ptg_parts.row_helpers")
ptg_rust_publish = importlib.import_module("process.ptg_parts.rust_publish")
ptg_rust_scanner = importlib.import_module("process.ptg_parts.rust_scanner")
ptg_rust_stage = importlib.import_module("process.ptg_parts.rust_stage")
ptg_screen = importlib.import_module("process.ptg_parts.screen")
ptg_serving_rows = importlib.import_module("process.ptg_parts.serving_rows")
ptg_serving_only = importlib.import_module("process.ptg_parts.serving_only")
ptg_snapshot_cleanup = importlib.import_module("process.ptg_parts.snapshot_cleanup")
ptg_snapshot_tables = importlib.import_module("process.ptg_parts.snapshot_tables")
ptg_source_download = importlib.import_module("process.ptg_parts.source_download")
ptg_source_files = importlib.import_module("process.ptg_parts.source_files")
ptg_source_jobs = importlib.import_module("process.ptg_parts.source_jobs")
ptg_source_pointers = importlib.import_module("process.ptg_parts.source_pointers")
ptg_table_setup = importlib.import_module("process.ptg_parts.table_setup")
ptg_values = importlib.import_module("process.ptg_parts.values")


class _ClosedStream:
    def write(self, _value):
        raise OSError("closed")

    def flush(self):
        raise OSError("closed")


def test_screen_writer_ignores_closed_capture_stream(monkeypatch):
    monkeypatch.setattr(ptg_screen.sys, "stdout", _ClosedStream())

    ptg_screen._write_screen_line("stdout", "progress")


def test_canonical_split_keeps_facade_helpers_stable():
    assert process_ptg.normalize_money is ptg_canonical.normalize_money
    assert process_ptg.normalize_date is ptg_canonical.normalize_date
    assert process_ptg.semantic_hash is ptg_canonical.semantic_hash
    assert process_ptg.canonicalize_url is ptg_canonical.canonicalize_url
    assert process_ptg.normalize_import_month is ptg_canonical.normalize_import_month


def test_values_split_keeps_facade_helpers_stable():
    assert process_ptg.build_provider_set is ptg_values.build_provider_set
    assert process_ptg.build_price_set is ptg_values.build_price_set
    assert process_ptg.build_fact_chunk is ptg_values.build_fact_chunk
    assert process_ptg.build_rate_set is ptg_values.build_rate_set
    assert process_ptg.provider_hash_bucket is ptg_values.provider_hash_bucket


def test_row_helper_split_keeps_facade_helpers_stable():
    assert process_ptg._make_checksum is ptg_row_helpers._make_checksum
    assert process_ptg._as_int_list is ptg_row_helpers._as_int_list
    assert process_ptg._normalized_npi_list is ptg_row_helpers._normalized_npi_list
    assert process_ptg._provider_group_identity_hash is ptg_row_helpers._provider_group_identity_hash
    assert process_ptg._normalize_code_component is ptg_row_helpers._normalize_code_component


def test_progress_split_keeps_facade_helpers_stable():
    assert process_ptg._utcnow is ptg_progress._utcnow
    assert process_ptg._artifact_progress_position is ptg_progress._artifact_progress_position
    assert process_ptg._format_duration is ptg_progress._format_duration
    assert process_ptg._maybe_log_artifact_progress is ptg_progress._maybe_log_artifact_progress


def test_artifact_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2ArtifactStore is ptg_artifacts.PTG2ArtifactStore
    assert process_ptg.sha256_file is ptg_artifacts.sha256_file
    assert process_ptg.choose_reusable_raw_artifact is ptg_artifacts.choose_reusable_raw_artifact
    assert process_ptg.content_addressed_path is ptg_artifacts.content_addressed_path
    assert process_ptg.ptg2_temp_parent is ptg_artifacts.ptg2_temp_parent


def test_artifact_stream_split_keeps_facade_helpers_stable():
    assert process_ptg.open_json_artifact_stream is ptg_artifact_streams.open_json_artifact_stream
    assert process_ptg.logical_artifact_identity is ptg_artifact_streams.logical_artifact_identity
    assert process_ptg.stream_logical_artifact is ptg_artifact_streams.stream_logical_artifact
    assert process_ptg.load_json_artifact is ptg_artifact_streams.load_json_artifact


def test_db_table_split_keeps_facade_helpers_stable():
    assert process_ptg._quote_ident is ptg_db_tables._quote_ident
    assert process_ptg._table_exists is ptg_db_tables._table_exists
    assert process_ptg._table_has_rows is ptg_db_tables._table_has_rows
    assert process_ptg._estimated_table_rows is ptg_db_tables._estimated_table_rows
    assert process_ptg._exact_table_rows is ptg_db_tables._exact_table_rows


def test_compact_index_split_keeps_facade_helpers_stable():
    assert process_ptg._PTG2_COMPACT_MODEL_BY_KIND is ptg_compact_indexes._PTG2_COMPACT_MODEL_BY_KIND
    assert process_ptg._ptg2_model_snapshot_index_role is ptg_compact_indexes._ptg2_model_snapshot_index_role
    assert process_ptg._ptg2_index_timestamp is ptg_compact_indexes._ptg2_index_timestamp
    assert process_ptg._ptg2_compact_serving_index_mode is ptg_compact_indexes._ptg2_compact_serving_index_mode
    assert process_ptg._ptg2_compact_dictionary_index_mode is ptg_compact_indexes._ptg2_compact_dictionary_index_mode
    assert (
        process_ptg._ptg2_compact_serving_reported_index_statement
        is ptg_compact_indexes._ptg2_compact_serving_reported_index_statement
    )
    assert process_ptg._ptg2_model_index_statements_for_table is ptg_compact_indexes._ptg2_model_index_statements_for_table
    assert process_ptg._run_ptg2_index_statement is ptg_compact_indexes._run_ptg2_index_statement
    assert process_ptg._index_snapshot_compact_table_entries is ptg_compact_indexes._index_snapshot_compact_table_entries
    assert process_ptg._index_snapshot_compact_tables is ptg_compact_indexes._index_snapshot_compact_tables


def test_compact_state_split_keeps_facade_helpers_stable():
    assert process_ptg._compact_state is ptg_compact_state._compact_state
    assert process_ptg._compact_streaming_dedupe_tables is ptg_compact_state._compact_streaming_dedupe_tables
    assert process_ptg._compact_add_unique is ptg_compact_state._compact_add_unique
    assert process_ptg._ptg2_price_atom_payload is ptg_compact_state._ptg2_price_atom_payload
    assert process_ptg._ptg2_source_trace_payload is ptg_compact_state._ptg2_source_trace_payload


def test_copy_load_split_keeps_facade_helpers_stable():
    assert process_ptg._json_default is ptg_copy_load._json_default
    assert process_ptg._ptg2_conflict_targets is ptg_copy_load._ptg2_conflict_targets
    assert process_ptg._primary_key_column_names is ptg_copy_load._primary_key_column_names
    assert process_ptg._ptg2_json_columns is ptg_copy_load._ptg2_json_columns
    assert process_ptg._ptg2_copy_record is ptg_copy_load._ptg2_copy_record
    assert process_ptg._copy_upsert_ptg2_objects is ptg_copy_load._copy_upsert_ptg2_objects
    assert process_ptg._copy_insert_ptg2_objects is ptg_copy_load._copy_insert_ptg2_objects
    assert process_ptg._copy_ignore_ptg2_objects is ptg_copy_load._copy_ignore_ptg2_objects
    assert process_ptg._copy_stage_price_set_rows is ptg_copy_load._copy_stage_price_set_rows
    assert process_ptg._copy_stage_serving_rate_rows is ptg_copy_load._copy_stage_serving_rate_rows
    assert process_ptg._copy_compact_serving_rate_rows is ptg_copy_load._copy_compact_serving_rate_rows
    assert process_ptg._copy_compact_serving_rate_file is ptg_copy_load._copy_compact_serving_rate_file
    assert process_ptg._copy_compact_serving_rate_source is ptg_copy_load._copy_compact_serving_rate_source
    assert process_ptg._copy_ptg2_dictionary_file is ptg_copy_load._copy_ptg2_dictionary_file


def test_import_row_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_import_id is ptg_import_rows._normalize_import_id
    assert process_ptg._ptg2_provider_group_rows is ptg_import_rows._ptg2_provider_group_rows
    assert process_ptg._build_provider_set_entry is ptg_import_rows._build_provider_set_entry
    assert process_ptg._combine_provider_set_entries is ptg_import_rows._combine_provider_set_entries
    assert process_ptg._fast_provider_entry_from_parts is ptg_import_rows._fast_provider_entry_from_parts
    assert process_ptg._fast_provider_entry_from_provider_refs is ptg_import_rows._fast_provider_entry_from_provider_refs
    assert process_ptg._ptg2_provider_set_row is ptg_import_rows._ptg2_provider_set_row
    assert process_ptg._ptg2_procedure_row is ptg_import_rows._ptg2_procedure_row
    assert process_ptg._ptg2_price_atom_row is ptg_import_rows._ptg2_price_atom_row
    assert process_ptg._ptg2_source_trace_rows is ptg_import_rows._ptg2_source_trace_rows
    assert process_ptg._ptg2_context_row is ptg_import_rows._ptg2_context_row
    assert process_ptg._ptg2_plan_rows is ptg_import_rows._ptg2_plan_rows


def test_snapshot_table_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_source_key is ptg_snapshot_tables._normalize_source_key
    assert process_ptg._ptg2_snapshot_table_token is ptg_snapshot_tables._ptg2_snapshot_table_token
    assert process_ptg._ptg2_snapshot_table_name is ptg_snapshot_tables._ptg2_snapshot_table_name
    assert process_ptg._ptg2_snapshot_index_name is ptg_snapshot_tables._ptg2_snapshot_index_name


def test_source_pointer_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_plan_source_key is ptg_source_pointers._ptg2_plan_source_key
    assert process_ptg._current_source_snapshot_id is ptg_source_pointers._current_source_snapshot_id
    assert process_ptg._source_plan_rows is ptg_source_pointers._source_plan_rows


def test_source_job_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_filter_values is ptg_source_jobs._normalize_filter_values
    assert process_ptg._dedupe_preserve is ptg_source_jobs._dedupe_preserve
    assert process_ptg._dedupe_rows_by is ptg_source_jobs._dedupe_rows_by
    assert process_ptg._plan_matches_filters is ptg_source_jobs._plan_matches_filters
    assert process_ptg._filter_reporting_plans is ptg_source_jobs._filter_reporting_plans
    assert process_ptg._normalize_plan_payload is ptg_source_jobs._normalize_plan_payload
    assert process_ptg.parse_toc_catalog_entries is ptg_source_jobs.parse_toc_catalog_entries
    assert process_ptg._load_toc_urls_from_file is ptg_source_jobs._load_toc_urls_from_file
    assert process_ptg._filter_jobs_by_url_contains is ptg_source_jobs._filter_jobs_by_url_contains
    assert process_ptg._ptg_job_identity is ptg_source_jobs._ptg_job_identity
    assert process_ptg._plan_identity is ptg_source_jobs._plan_identity
    assert process_ptg._merge_ptg_job is ptg_source_jobs._merge_ptg_job
    assert process_ptg._dedupe_ptg_jobs is ptg_source_jobs._dedupe_ptg_jobs


def test_source_download_split_keeps_facade_helpers_stable():
    assert process_ptg._format_eta_seconds is ptg_source_download._format_eta_seconds
    assert process_ptg._emit_download_progress is ptg_source_download._emit_download_progress
    assert process_ptg.fetch_head_metadata is ptg_source_download.fetch_head_metadata
    assert process_ptg._probe_http_range_support is ptg_source_download._probe_http_range_support
    assert process_ptg._download_raw_artifact_ranges is ptg_source_download._download_raw_artifact_ranges
    assert process_ptg.download_raw_artifact is ptg_source_download.download_raw_artifact
    assert process_ptg.materialize_json_source is ptg_source_download.materialize_json_source


def test_source_file_split_keeps_facade_helpers_stable():
    assert process_ptg._maybe_unzip is ptg_source_files._maybe_unzip
    assert process_ptg._extract_metadata_fields is ptg_source_files._extract_metadata_fields
    assert process_ptg._derive_plan_fields is ptg_source_files._derive_plan_fields
    assert process_ptg._build_file_row is ptg_source_files._build_file_row


def test_table_setup_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2_MODEL_CLASSES is ptg_table_setup.PTG2_MODEL_CLASSES
    assert process_ptg._ensure_indexes is ptg_table_setup._ensure_indexes
    assert process_ptg._ensure_ptg2_serving_rate_columns is ptg_table_setup._ensure_ptg2_serving_rate_columns
    assert process_ptg._ensure_ptg2_provider_set_columns is ptg_table_setup._ensure_ptg2_provider_set_columns
    assert process_ptg._ensure_ptg2_price_set_columns is ptg_table_setup._ensure_ptg2_price_set_columns
    assert process_ptg._ensure_ptg2_price_atom_columns is ptg_table_setup._ensure_ptg2_price_atom_columns
    assert process_ptg._drop_ptg2_columns is ptg_table_setup._drop_ptg2_columns
    assert process_ptg._ensure_ptg2_price_set_stage_table is ptg_table_setup._ensure_ptg2_price_set_stage_table
    assert process_ptg._ensure_ptg2_serving_rate_stage_table is ptg_table_setup._ensure_ptg2_serving_rate_stage_table
    assert process_ptg.ensure_ptg2_tables is ptg_table_setup.ensure_ptg2_tables
    assert process_ptg._prepare_ptg_tables is ptg_table_setup._prepare_ptg_tables


def test_snapshot_cleanup_split_keeps_facade_helpers_stable():
    assert process_ptg._snapshot_manifest_table_names is ptg_snapshot_cleanup._snapshot_manifest_table_names
    assert process_ptg._drop_ptg2_snapshot_table_names is ptg_snapshot_cleanup._drop_ptg2_snapshot_table_names
    assert process_ptg._drop_ptg2_snapshot_tables_for_manifest is ptg_snapshot_cleanup._drop_ptg2_snapshot_tables_for_manifest
    assert process_ptg._cleanup_old_ptg2_source_tables is ptg_snapshot_cleanup._cleanup_old_ptg2_source_tables


def test_rust_publish_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_publish_timestamp is ptg_rust_publish._ptg2_publish_timestamp
    assert process_ptg._publish_renamed_rust_dictionary_table is ptg_rust_publish._publish_renamed_rust_dictionary_table
    assert process_ptg._ptg2_serving_child_table_name is ptg_rust_publish._ptg2_serving_child_table_name
    assert process_ptg._publish_rust_serving_stage_tables is ptg_rust_publish._publish_rust_serving_stage_tables
    assert process_ptg._publish_rust_compact_snapshot_tables is ptg_rust_publish._publish_rust_compact_snapshot_tables


def test_rust_scanner_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_rust_scanner_binary is ptg_rust_scanner._ptg2_rust_scanner_binary
    assert process_ptg._iter_top_level_object_bytes_rust is ptg_rust_scanner._iter_top_level_object_bytes_rust
    assert process_ptg._iter_compact_serving_records_rust is ptg_rust_scanner._iter_compact_serving_records_rust
    assert process_ptg._aiter_compact_serving_records_rust is ptg_rust_scanner._aiter_compact_serving_records_rust


def test_rust_stage_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2_SERVING_STAGE_LANE_PREFIX == ptg_rust_stage.PTG2_SERVING_STAGE_LANE_PREFIX
    assert process_ptg._RUST_COPY_TABLE_SPECS is ptg_rust_stage._RUST_COPY_TABLE_SPECS
    assert process_ptg._ptg2_dictionary_select_columns is ptg_rust_stage._ptg2_dictionary_select_columns
    assert process_ptg._rust_copy_stage_table_name is ptg_rust_stage._rust_copy_stage_table_name
    assert process_ptg._serving_stage_lane_key is ptg_rust_stage._serving_stage_lane_key
    assert process_ptg._serving_stage_tables is ptg_rust_stage._serving_stage_tables
    assert process_ptg._serving_stage_table_for_copy is ptg_rust_stage._serving_stage_table_for_copy
    assert process_ptg._create_rust_copy_stage_tables is ptg_rust_stage._create_rust_copy_stage_tables
    assert process_ptg._merge_rust_copy_stage_tables is ptg_rust_stage._merge_rust_copy_stage_tables


def test_json_stream_split_keeps_facade_helpers_stable():
    assert process_ptg._json_loads is ptg_json_streams._json_loads
    assert process_ptg._iter_top_level_objects is ptg_json_streams._iter_top_level_objects
    assert process_ptg._iter_top_level_object_bytes is ptg_json_streams._iter_top_level_object_bytes
    assert process_ptg._iter_top_level_objects_jsondecoder is ptg_json_streams._iter_top_level_objects_jsondecoder
    assert process_ptg._iter_top_level_objects_fast is ptg_json_streams._iter_top_level_objects_fast


def test_serving_only_split_keeps_facade_helpers_stable():
    assert process_ptg._serving_only_price_payload is ptg_serving_only._serving_only_price_payload
    assert process_ptg._normalize_serving_price_payload is ptg_serving_only._normalize_serving_price_payload
    assert process_ptg._serving_only_hash_int_sets is ptg_serving_only._serving_only_hash_int_sets
    assert process_ptg._serving_only_hash_price_key is ptg_serving_only._serving_only_hash_price_key
    assert process_ptg._serving_only_hash_text is ptg_serving_only._serving_only_hash_text
    assert process_ptg._serving_only_merge_worker_result is ptg_serving_only._serving_only_merge_worker_result
    assert process_ptg._worker_payload_size is ptg_serving_only._worker_payload_size
    assert process_ptg._iter_worker_result_rows is ptg_serving_only._iter_worker_result_rows
    assert process_ptg._ptg2_worker_capacity_wait_needed is ptg_serving_only._ptg2_worker_capacity_wait_needed


def test_serving_row_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_hp_procedure_code is ptg_serving_rows._ptg2_hp_procedure_code
    assert process_ptg._ptg2_serving_rate_row is ptg_serving_rows._ptg2_serving_rate_row
    assert process_ptg._ptg2_compact_serving_rate_row is ptg_serving_rows._ptg2_compact_serving_rate_row
    assert process_ptg._provider_group_member_rows is ptg_serving_rows._provider_group_member_rows
    assert process_ptg._provider_set_component_rows is ptg_serving_rows._provider_set_component_rows


def test_filter_reporting_plans_matches_group_plan_id():
    plans = [
        {
            "plan_name": "Example Individual",
            "plan_id": "81974",
            "plan_market_type": "individual",
        },
        {
            "plan_name": "DEWITT, LLP-HPS",
            "plan_id": "391804522",
            "plan_sponsor_name": "DEWITT, LLP",
            "issuer_name": "WPS",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_ids=["391804522"],
        plan_market_types=["group"],
    )

    assert result == [plans[1]]


def test_ptg2_filter_jobs_by_url_contains_keeps_matching_rate_file():
    jobs = [
        {"type": "in_network", "url": "https://example.test/CMC_CRS_MRRF_in-network-rates.json.gz"},
        {"type": "in_network", "url": "https://example.test/PS1-50_C2_in-network-rates.json.gz"},
    ]

    result = process_ptg._filter_jobs_by_url_contains(jobs, ["ps1-50_c2"])

    assert result == [jobs[1]]


def test_ptg2_dedupe_jobs_uses_canonical_url_and_merges_plans():
    jobs = [
        {
            "type": "in_network",
            "url": "HTTPS://Example.COM:443/path/rates.json.gz?sig=one&b=2&a=1",
            "plan_info": [{"plan_id": "A"}],
            "description": "first",
        },
        {
            "type": "in_network",
            "url": "https://example.com/path/rates.json.gz?a=1&b=2&Signature=two",
            "plan_info": [{"plan_id": "B"}],
            "description": "second",
        },
        {
            "type": "allowed_amounts",
            "url": "https://example.com/path/rates.json.gz?a=1&b=2&Signature=two",
            "plan_info": [{"plan_id": "C"}],
        },
    ]

    deduped, duplicate_count = process_ptg._dedupe_ptg_jobs(jobs)

    assert duplicate_count == 1
    assert len(deduped) == 2
    assert deduped[0]["url"] == jobs[0]["url"]
    assert deduped[0]["plan_info"] == [{"plan_id": "A"}, {"plan_id": "B"}]
    assert deduped[1]["type"] == "allowed_amounts"


def test_ptg2_stage_copy_dedupe_defaults_avoid_huge_edge_tables(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_STAGE_COPY_DEDUPE", raising=False)

    assert process_ptg._ptg2_stage_copy_dedupe_enabled("price_code_set") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_set") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_group_member") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("price_set_entry") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_set_component") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_entry_component") is False

    monkeypatch.setenv("HLTHPRT_PTG2_STAGE_COPY_DEDUPE", "1")
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_entry_component") is True


def test_ptg2_compact_serving_index_defaults_to_reported_lookup(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_COMPACT_SERVING_INDEX_MODE", raising=False)

    statements = process_ptg._ptg2_model_index_statements_for_table(
        process_ptg.PTG2ServingRateCompact,
        "mrf",
        "ptg2_serving_rate_compact_abc_p00",
    )

    assert len(statements) == 1
    role, statement = statements[0]
    assert role == "reported_system_order_idx"
    assert "(snapshot_id, plan_id, reported_code_system, reported_code, provider_count DESC, serving_rate_id)" in statement


def test_ptg2_compact_serving_index_full_mode_keeps_model_indexes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_COMPACT_SERVING_INDEX_MODE", "full")

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2ServingRateCompact,
            "mrf",
            "ptg2_serving_rate_compact_abc_p00",
        )
    ]

    assert "idx_primary" in roles
    assert "reported_order_idx" in roles


def test_ptg2_compact_dictionary_index_defaults_to_serving_indexes(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_COMPACT_DICTIONARY_INDEX_MODE", raising=False)

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2PriceSetEntry,
            "mrf",
            "ptg2_price_set_entry_abc",
        )
    ]

    assert roles == ["idx_primary"]


def test_ptg2_compact_dictionary_index_full_mode_keeps_reverse_indexes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_COMPACT_DICTIONARY_INDEX_MODE", "full")

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2PriceSetEntry,
            "mrf",
            "ptg2_price_set_entry_abc",
        )
    ]

    assert "idx_primary" in roles
    assert "atom_idx" in roles


def test_ptg2_fast_object_iterator_yields_selected_top_level_arrays():
    payload = (
        b'{"version":"1.0","provider_references":[{"provider_group_id":7,'
        b'"provider_groups":[{"npi":[1],"note":"brace } in string"}]}],'
        b'"in_network":[{"negotiation_arrangement":"ffs","billing_code":"001",'
        b'"negotiated_rates":[{"negotiated_prices":[{"negotiated_rate":1.2}]}]}]}'
    )

    result = list(
        process_ptg._iter_top_level_objects_fast(
            io.BytesIO(payload),
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
        )
    )

    assert [name for name, _ in result] == ["provider_reference", "in_network"]
    assert result[0][1]["provider_group_id"] == 7
    assert result[1][1]["billing_code"] == "001"


def test_filter_reporting_plans_matches_name_contains_case_insensitive():
    plans = [
        {
            "plan_name": "LIBERTY TITLE AND ABSTRACT INC-Statewide",
            "plan_id": "391937180",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_name_contains=["liberty title"],
        plan_market_types=["GROUP"],
    )

    assert result == plans


def test_filter_reporting_plans_returns_original_without_filters():
    plans = [{"plan_name": "Any Plan", "plan_id": "1", "plan_market_type": "group"}]

    assert process_ptg._filter_reporting_plans(plans) == plans


def test_as_int_list_normalizes_npi_strings():
    assert process_ptg._as_int_list(["1053488122", 1093228306, "", None, "bad"]) == [
        1053488122,
        1093228306,
    ]


def test_ptg2_semantic_hash_ignores_set_like_array_order():
    first = {
        "npi": [1093228306, 1053488122],
        "service_code": ["02", "01"],
        "billing_code_modifier": ["TC", "26"],
        "bundled_codes": [
            {"billing_code": "B", "billing_code_type": "HCPCS"},
            {"billing_code": "A", "billing_code_type": "CPT"},
        ],
        "negotiated_rate": Decimal("12.3400"),
    }
    second = {
        "npi": [1053488122, 1093228306],
        "service_code": ["01", "02"],
        "billing_code_modifier": ["26", "TC"],
        "bundled_codes": [
            {"billing_code_type": "CPT", "billing_code": "A"},
            {"billing_code_type": "HCPCS", "billing_code": "B"},
        ],
        "negotiated_rate": "12.34",
    }

    assert process_ptg.semantic_hash(first, domain="rate") == process_ptg.semantic_hash(second, domain="rate")


@pytest.mark.parametrize(
    "changed",
    [
        {"negotiated_rate": Decimal("12.35")},
        {"billing_code_modifier": ["26", "GT"]},
        {"setting": "inpatient"},
        {"context": {"plan_id": "different"}},
    ],
)
def test_ptg2_semantic_hash_changes_for_rate_context_modifier_and_setting(changed):
    base = {
        "negotiated_rate": Decimal("12.34"),
        "billing_code_modifier": ["26", "TC"],
        "setting": "outpatient",
        "context": {"plan_id": "010854205"},
    }
    modified = dict(base)
    modified.update(changed)

    assert process_ptg.semantic_hash(base, domain="rate") != process_ptg.semantic_hash(modified, domain="rate")


def test_ptg2_semantic_hash_defaults_to_short_checksum_mode(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_HASH_MODE_ENV, raising=False)

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 16
    assert int(result, 16) >= 0


def test_ptg2_semantic_hash_can_switch_back_to_sha256(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_HASH_MODE_ENV, "sha256")

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 64
    assert int(result, 16) >= 0


def test_ptg2_rejects_float_money_values():
    with pytest.raises(TypeError):
        process_ptg.semantic_hash({"negotiated_rate": 12.34}, domain="rate")


def test_ptg2_modes_and_confidence_wording_are_explicit():
    assert process_ptg.normalize_ptg2_search_mode(None) == "product_search"
    assert process_ptg.normalize_ptg2_search_mode("exact_source") == "exact_source"
    with pytest.raises(ValueError):
        process_ptg.normalize_ptg2_search_mode("loose")
    assert "Published negotiated rate" in process_ptg.ptg2_confidence_statement("tic_rate_npi_tin")


def test_ptg2_domain_split_keeps_facade_symbols_stable():
    assert process_ptg.PTG2PriceAtomEvent is ptg_domain.PTG2PriceAtomEvent
    assert process_ptg.PTG2RawArtifact is ptg_domain.PTG2RawArtifact
    assert process_ptg.normalize_ptg2_search_mode is ptg_domain.normalize_ptg2_search_mode
    assert process_ptg.ptg2_confidence_statement is ptg_domain.ptg2_confidence_statement


def test_ptg2_runtime_checksum_uses_bigint_hash_space():
    hashes = [process_ptg._make_checksum("rate", idx) for idx in range(5000)]

    assert len(set(hashes)) == len(hashes)
    assert all(0 <= value < 2**63 for value in hashes)
    assert max(hashes) > 2**32


def test_ptg2_provider_group_identity_is_source_independent_and_order_insensitive():
    tin_a = {"type": "EIN", "value": "12-3456789"}
    tin_b = {"type": "ein", "value": "123456789"}

    first = process_ptg._provider_group_identity_hash(tin_a, [3, 1, 2, 2])
    second = process_ptg._provider_group_identity_hash(tin_b, [2, 3, 1])

    assert first == second


def test_ptg2_provider_set_entry_packs_groups_order_insensitively():
    groups_a = [
        {"tin": {"type": "ein", "value": "111"}, "npi": [3, 1]},
        {"tin": {"type": "ein", "value": "222"}, "npi": [2]},
    ]
    groups_b = list(reversed(groups_a))

    entry_a, row_a = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=groups_a,
        network_names=["A"],
    )
    entry_b, row_b = process_ptg._build_provider_set_entry(
        file_id=2,
        provider_group_ref=20,
        provider_groups=groups_b,
        network_names=["B"],
    )

    assert entry_a["__hash__"] == entry_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "set"


def test_ptg2_combined_provider_set_entry_packs_rate_provider_refs():
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1, 2]}],
        network_names=["A"],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [3]}],
        network_names=["A"],
    )

    combined_a, row_a = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])
    combined_b, row_b = process_ptg._combine_provider_set_entries(file_id=2, entries=[second, first])

    assert combined_a["__hash__"] == combined_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "set"


def test_ptg2_provider_group_rows_are_canonical_and_source_independent():
    groups_a = [{"tin": {"type": "ein", "value": "12-3456789"}, "npi": [3, 1, 2]}]
    groups_b = [{"tin": {"type": "EIN", "value": "123456789"}, "npi": [2, 3, 1]}]

    row_a = process_ptg._ptg2_provider_group_rows(provider_groups=groups_a)[0]
    row_b = process_ptg._ptg2_provider_group_rows(provider_groups=groups_b)[0]

    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1, 2, 3]
    assert row_a["tin_type"] == "ein"
    assert row_a["tin_value"] == "123456789"


def test_ptg2_large_provider_set_row_omits_inline_npi_when_group_hashes_available(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, "2")
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1, 2]}],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [3]}],
    )
    combined, _ = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])

    row = process_ptg._ptg2_provider_set_row(combined)

    assert row["provider_count"] == 3
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False
    assert row["canonical_payload"]["provider_group_hashes"]


def test_ptg2_provider_set_row_omits_inline_npi_by_default_when_group_hashes_available(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, raising=False)
    entry, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1]}],
    )

    row = process_ptg._ptg2_provider_set_row(entry)

    assert row["provider_count"] == 1
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False


def test_ptg2_procedure_identity_groups_display_text_variants():
    base = {
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "ffs",
        "name": "Office visit",
        "description": "First description",
    }
    variant = {**base, "name": "Established patient visit", "description": "Different description"}
    changed_arrangement = {**base, "negotiation_arrangement": "bundle"}

    assert process_ptg._ptg2_procedure_row(base)["procedure_hash"] == process_ptg._ptg2_procedure_row(variant)["procedure_hash"]
    assert (
        process_ptg._ptg2_procedure_row(base)["procedure_hash"]
        != process_ptg._ptg2_procedure_row(changed_arrangement)["procedure_hash"]
    )


def test_ptg2_canonicalize_url_strips_signed_params():
    url = (
        "HTTPS://Example.COM:443/path/file.json.gz?"
        "sig=secret&sv=2020&foo=bar&Signature=abc&Key-Pair-Id=k&b=2&a=1"
    )

    assert process_ptg.canonicalize_url(url) == "https://example.com/path/file.json.gz?a=1&b=2&foo=bar"


def test_ptg2_toc_parser_handles_uhc_sponsor_typo_and_duplicate_signed_urls():
    toc = {
        "reporting_entity_name": "UHC",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Heartland",
                        "plan_id": "010854205",
                        "plan_sponser_name": "Heartland Co",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": "https://cdn.test/rates.json.gz?sig=a&foo=1"},
                    {"location": "https://cdn.test/rates.json.gz?sig=b&foo=1"},
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(
        toc,
        "https://payer.test/toc.json",
        plan_ids=["010854205"],
    )
    in_network_entries = [entry for entry in entries if entry.source_type == "in-network"]

    assert len(in_network_entries) == 1
    assert in_network_entries[0].canonical_url == "https://cdn.test/rates.json.gz?foo=1"
    assert in_network_entries[0].plan_info[0]["plan_sponsor_name"] == "Heartland Co"


def test_ptg2_toc_parser_normalizes_asr_download_links():
    toc = {
        "reporting_entity_name": "ASR Health Benefits",
        "reporting_entity_type": "Third Party Administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "ASR Health Benefits - ASR1",
                        "plan_id": "823166837",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://www.asrhealthbenefits.com/home/umbraco/surface/"
                            "mrfdownload/index?g=1208&i=595&t=InNetwork"
                        )
                    }
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(toc, "https://payer.test/toc.json")
    in_network_entries = [entry for entry in entries if entry.source_type == "in-network"]

    assert len(in_network_entries) == 1
    assert in_network_entries[0].original_url == (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?groupNumber=1208&fileType=InNetwork&fileId=595"
    )
    assert in_network_entries[0].canonical_url == (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?fileId=595&fileType=InNetwork&groupNumber=1208"
    )


def test_ptg2_toc_jobs_normalize_asr_download_links(monkeypatch):
    toc = {
        "reporting_entity_name": "ASR Health Benefits",
        "reporting_entity_type": "Third Party Administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "ASR Health Benefits - ASR1",
                        "plan_id": "823166837",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://www.asrhealthbenefits.com/home/umbraco/surface/"
                            "mrfdownload/index?g=1208&i=596&t=InNetwork"
                        ),
                    },
                ],
            },
        ],
    }
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path="/tmp/asr-index.json")
        return artifact, artifact

    async def fake_push_objects(rows, _cls, **_kwargs):
        pushed_file_rows.extend(rows)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "load_json_artifact", lambda _path: toc)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1208",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            plan_ids=["823166837"],
            plan_market_types=["group"],
        )
    )

    expected_url = (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?groupNumber=1208&fileType=InNetwork&fileId=596"
    )
    assert jobs[0]["url"] == expected_url
    assert any(row["url"] == expected_url and row["file_type"] == "in-network" for row in pushed_file_rows)


def test_ptg2_artifact_reuse_by_strong_etag_and_length(tmp_path):
    raw_path = tmp_path / "raw.json"
    raw_path.write_text('{"ok": true}', encoding="utf-8")
    raw_sha, byte_count = process_ptg.sha256_file(raw_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path)
    candidate = {
        "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
        "canonical_url": "https://example.test/raw.json",
        "raw_storage_uri": raw_path.resolve().as_uri(),
        "raw_sha256": raw_sha,
        "content_length": byte_count,
        "etag": '"strong"',
    }
    head = process_ptg.PTG2HeadMetadata(
        url="https://example.test/raw.json",
        status=200,
        etag='"strong"',
        content_length=byte_count,
        supports_head=True,
    )

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate], head, store=store)

    assert reused == candidate
    assert mode == "strong_etag_length"


def test_ptg2_range_download_assembles_artifact(monkeypatch, tmp_path):
    payload = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"range-test"',
                    "Accept-Ranges": "bytes",
                }
            )
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header:
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text)
            chunk = payload[start : end + 1]
            return web.Response(
                status=206,
                body=chunk,
                headers={
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                    "ETag": '"range-test"',
                },
            )
        return web.Response(body=payload, headers={"Content-Length": str(len(payload))})

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/artifact.bin", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        try:
            return await process_ptg.download_raw_artifact(
                f"http://127.0.0.1:{port}/artifact.bin",
                store=process_ptg.PTG2ArtifactStore(tmp_path),
            )
        finally:
            await runner.cleanup()

    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, "1")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV, str(1024 * 1024))
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_TASKS_ENV, "2")

    artifact = asyncio.run(run_download())

    assert Path(artifact.raw_path).read_bytes() == payload
    assert any(request == "bytes=0-0" for request in requests)
    assert sum(1 for request in requests if request and request != "bytes=0-0") >= 2
    assert not list((tmp_path / "partial-retained").glob("*.ranges.json"))


def test_ptg2_range_download_retries_short_chunk(monkeypatch, tmp_path):
    payload = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    attempts_by_range = {}

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"range-retry-test"',
                    "Accept-Ranges": "bytes",
                }
            )
        range_header = request.headers.get("Range")
        if not range_header:
            return web.Response(body=payload, headers={"Content-Length": str(len(payload))})
        start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
        start = int(start_text)
        end = int(end_text)
        attempts_by_range[range_header] = attempts_by_range.get(range_header, 0) + 1
        chunk = payload[start : end + 1]
        if range_header == "bytes=1048576-2097151" and attempts_by_range[range_header] == 1:
            chunk = chunk[: len(chunk) // 2]
        return web.Response(
            status=206,
            body=chunk,
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                "ETag": '"range-retry-test"',
            },
        )

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/artifact.bin", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        try:
            return await process_ptg.download_raw_artifact(
                f"http://127.0.0.1:{port}/artifact.bin",
                store=process_ptg.PTG2ArtifactStore(tmp_path),
            )
        finally:
            await runner.cleanup()

    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, "1")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV, str(1024 * 1024))
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_TASKS_ENV, "2")
    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_RETRIES_ENV, "2")
    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV, "0")

    artifact = asyncio.run(run_download())

    assert Path(artifact.raw_path).read_bytes() == payload
    assert attempts_by_range["bytes=1048576-2097151"] == 2
    assert not list((tmp_path / "partial-retained").glob("*.ranges.json"))


def test_ptg2_packing_helpers_are_order_insensitive_for_sets():
    provider_a = process_ptg.build_provider_set([3, 1, 2, 2], tin_type="ein", tin_value="123")
    provider_b = process_ptg.build_provider_set([2, 3, 1], tin_type="ein", tin_value="123")
    price_set_a = process_ptg.build_price_set(["b", "a", "a"])
    price_set_b = process_ptg.build_price_set(["a", "b"])
    chunk_a = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-b", "pack-a"])
    chunk_b = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-a", "pack-b"])
    rate_set_a = process_ptg.build_rate_set("ctx", ["chunk-b", "chunk-a"])
    rate_set_b = process_ptg.build_rate_set("ctx", ["chunk-a", "chunk-b"])

    assert provider_a["provider_set_hash"] == provider_b["provider_set_hash"]
    assert provider_a["npi"] == [1, 2, 3]
    assert price_set_a["price_set_hash"] == price_set_b["price_set_hash"]
    assert chunk_a["fact_chunk_hash"] == chunk_b["fact_chunk_hash"]
    assert rate_set_a["rate_set_hash"] == rate_set_b["rate_set_hash"]
    assert process_ptg.provider_hash_bucket(provider_a["provider_set_hash"], bucket_count=16)


def test_ptg2_provider_bucket_count_is_configurable(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_BUCKET_COUNT_ENV, "16")

    assert process_ptg.ptg2_provider_bucket_count() == 16


def test_ptg2_rate_pack_group_is_order_insensitive_for_provider_sets():
    pack_a = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-b", "provider-a", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["provider_set_hash"] == pack_b["provider_set_hash"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_rate_pack_procedure_group_is_order_insensitive():
    pack_a = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-b", "proc-a", "proc-a"],
        ["provider-b", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a", "proc-b"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_compact_rate_pack_flush_groups_procedures(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["rate_pack_groups"] = {
        ("proc-a", "price", "source"): {"provider-a"},
        ("proc-b", "price", "source"): {"provider-a"},
    }

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    assert state["rate_pack_groups"] == {}
    assert state["counts"]["rate_packs"] == 1
    assert len(pushed["PTG2RatePack"]) == 1
    assert pushed["PTG2RatePack"][0]["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert sum(len(v) for v in state["chunk_rate_packs"].values()) == 2


def test_ptg2_compact_rate_pack_flush_writes_serving_rows(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["snapshot_id"] = "snap"
    state["plan_fields"] = {"plan_id": "010854205", "plan_name": "Heartland"}
    state["procedure_payloads"] = {
        "proc-a": {
            "billing_code_type": "CPT",
            "billing_code": "00102",
            "name": "Anesthesia",
            "description": "Anesthesia description",
        }
    }
    state["price_payloads"] = {"price": [{"negotiated_rate": "50", "negotiated_type": "negotiated"}]}
    state["provider_set_counts"] = {"provider-a": 10, "provider-b": 20}
    state["source_trace_payload"] = [{"url": "https://example.test/rates.json.gz"}]
    state["rate_pack_groups"] = {("proc-a", "price", "source"): {"provider-a", "provider-b"}}

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    serving_row = pushed["PTG2ServingRate"][0]
    assert serving_row["snapshot_id"] == "snap"
    assert serving_row["plan_id"] == "010854205"
    assert serving_row["reported_code"] == "00102"
    assert serving_row["procedure_code"] == process_ptg.return_checksum(["CPT", "00102"])
    assert serving_row["provider_count"] == 30
    assert serving_row["prices"][0]["negotiated_rate"] == 50


def test_ptg2_compact_rows_can_schedule_async_writes(monkeypatch):
    pushed = []

    class FakeClass:
        __name__ = "FakeClass"

    async def fake_push(rows, cls, **_kwargs):
        await asyncio.sleep(0)
        pushed.append((cls, rows))

    monkeypatch.setenv(process_ptg.PTG2_ASYNC_WRITE_TASKS_ENV, "2")
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    async def run_writes():
        state = process_ptg._compact_state(batch_rows=1)
        await process_ptg._schedule_compact_write(state, [{"a": 1}], FakeClass)
        assert state["pending_writes"]
        await process_ptg._drain_compact_writes(state)
        assert not state["pending_writes"]

    asyncio.run(run_writes())
    assert pushed == [(FakeClass, [{"a": 1}])]


def test_ptg2_serving_only_worker_chunk_dedupes_rows(monkeypatch):
    rows_a = {
        "serving_rows": [{"serving_rate_id": "r1"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}],
        "price_set_rows": [{"price_set_hash": "pr1"}],
        "provider_set_component_rows": [{"provider_set_hash": "ps1", "provider_group_hash": 10}],
        "provider_group_member_rows": [{"provider_group_hash": 10, "npi": 123}],
        "procedure_rows": [{"procedure_hash": "p1"}],
    }
    rows_b = {
        "serving_rows": [{"serving_rate_id": "r1"}, {"serving_rate_id": "r2"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}, {"serving_rate_id": "cr2"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}, {"provider_set_hash": "ps2"}],
        "price_set_rows": [{"price_set_hash": "pr1"}, {"price_set_hash": "pr2"}],
        "provider_set_component_rows": [
            {"provider_set_hash": "ps1", "provider_group_hash": 10},
            {"provider_set_hash": "ps2", "provider_group_hash": 11},
        ],
        "provider_group_member_rows": [
            {"provider_group_hash": 10, "npi": 123},
            {"provider_group_hash": 11, "npi": 456},
        ],
        "procedure_rows": [{"procedure_hash": "p1"}, {"procedure_hash": "p2"}],
    }
    it = iter([rows_a, rows_b])
    monkeypatch.setattr(process_ptg, "_serving_only_worker_process", lambda _payload: next(it))

    merged = process_ptg._serving_only_worker_process_chunk([b"a", b"b"])

    assert [row["serving_rate_id"] for row in merged["serving_rows"]] == ["r1", "r2"]
    assert [row["serving_rate_id"] for row in merged["serving_rate_compact_rows"]] == ["cr1", "cr2"]
    assert [row["provider_set_hash"] for row in merged["provider_set_rows"]] == ["ps1", "ps2"]
    assert [row["price_set_hash"] for row in merged["price_set_rows"]] == ["pr1", "pr2"]
    assert len(merged["provider_set_component_rows"]) == 2
    assert len(merged["provider_group_member_rows"]) == 2
    assert [row["procedure_hash"] for row in merged["procedure_rows"]] == ["p1", "p2"]


def test_ptg2_serving_only_worker_chunk_to_files_dedupes_and_iterates(monkeypatch):
    rows_a = {
        "serving_rows": [{"serving_rate_id": "r1"}],
        "provider_set_component_rows": [{"provider_set_hash": "ps1", "provider_group_hash": 10}],
    }
    rows_b = {
        "serving_rows": [{"serving_rate_id": "r1"}, {"serving_rate_id": "r2"}],
        "provider_set_component_rows": [
            {"provider_set_hash": "ps1", "provider_group_hash": 10},
            {"provider_set_hash": "ps1", "provider_group_hash": 11},
        ],
    }
    it = iter([rows_a, rows_b])
    monkeypatch.setattr(process_ptg, "_serving_only_worker_process", lambda _payload: next(it))

    result = process_ptg._serving_only_worker_process_chunk_to_files([b"a", b"b"])
    temp_dir = Path(result["temp_dir"])
    try:
        assert result["__worker_result_files__"] is True
        assert result["counts"]["serving_rows"] == 2
        assert result["counts"]["provider_set_component_rows"] == 2
        serving_rows = list(process_ptg._iter_worker_result_rows(result["paths"]["serving_rows"]))
        component_rows = list(process_ptg._iter_worker_result_rows(result["paths"]["provider_set_component_rows"]))
        assert [row["serving_rate_id"] for row in serving_rows] == ["r1", "r2"]
        assert [row["provider_group_hash"] for row in component_rows] == [10, 11]
    finally:
        for path in result.get("paths", {}).values():
            Path(path).unlink(missing_ok=True)
        temp_dir.rmdir()


def test_ptg2_provider_reference_cache_round_trips_numeric_and_string_refs(tmp_path):
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])
        cache.put("abc", [{"__hash__": 43, "npi": [2], "provider_group_id": "abc"}])
        cache.commit()

        assert cache.get("123")[0]["npi"] == [1]
        assert cache.get("abc")[0]["npi"] == [2]
        assert cache.provider_hashes == {42, 43}
    finally:
        cache.close()


def test_ptg2_provider_reference_cache_uses_bounded_memory_lru(tmp_path, monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, "1")
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(1, [{"__hash__": 1, "npi": [1]}])
        cache.put(2, [{"__hash__": 2, "npi": [2]}])
        cache.commit()

        assert cache.get(2)[0]["npi"] == [2]
        assert cache.get(1)[0]["npi"] == [1]

        stats = cache.stats()
        assert stats["provider_cache_memory_limit"] == 1
        assert stats["provider_cache_memory_size"] == 1
        assert stats["provider_cache_gets"] == 2
        assert stats["provider_cache_sqlite_hits"] == 1
        assert stats["provider_cache_memory_hits"] == 1
    finally:
        cache.close()


def test_ptg2_in_memory_provider_reference_cache_tracks_hits():
    cache = process_ptg.PTG2InMemoryProviderReferenceCache()

    cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])

    assert cache.get("123")[0]["npi"] == [1]
    assert cache.get("missing") == []
    assert cache.provider_hashes == {42}
    stats = cache.stats()
    assert stats["provider_cache_memory_hits"] == 1
    assert stats["provider_cache_misses"] == 1
    assert stats["provider_cache_sqlite_hits"] == 0


def test_ptg2_provider_combo_cache_is_order_insensitive_and_bounded():
    cache = process_ptg.OrderedDict()
    stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": 1,
    }
    key_a = process_ptg._provider_combo_cache_key([2, "1", 1])
    key_b = process_ptg._provider_combo_cache_key(["1", 2])

    assert key_a == key_b
    assert process_ptg._provider_combo_cache_get(cache, key_a, stats) is None
    process_ptg._provider_combo_cache_put(cache, key_a, {"__hash__": 1}, stats, limit=1)
    assert process_ptg._provider_combo_cache_get(cache, key_b, stats)["__hash__"] == 1
    process_ptg._provider_combo_cache_put(cache, ("3",), {"__hash__": 3}, stats, limit=1)

    assert key_a not in cache
    assert stats["provider_combo_cache_hits"] == 1
    assert stats["provider_combo_cache_misses"] == 1


def test_ptg2_worker_capacity_waits_on_batch_count_or_bytes():
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=2,
        pending_input_bytes=8,
        next_batch_bytes=1,
        max_pending_batches=2,
        max_pending_bytes=100,
    )
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=8,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=0,
        pending_input_bytes=0,
        next_batch_bytes=20,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=4,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )


def test_ptg2_fast_provider_union_carries_count_without_npi_materialization(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_FAST_PROVIDER_UNION_ENV, "true")

    combined_entry, _row = process_ptg._combine_provider_set_entries(
        file_id=1,
        entries=[
            {
                "__hash__": 11,
                "npi": [1, 2],
                "provider_count": 2,
                "provider_group_hashes": [11],
            },
            {
                "__hash__": 22,
                "npi": [2, 3],
                "provider_count": 2,
                "provider_group_hashes": [22],
            },
        ],
    )

    provider_set_row = process_ptg._ptg2_provider_set_row(combined_entry)

    assert combined_entry["npi"] == []
    assert combined_entry["provider_count"] == 4
    assert provider_set_row["provider_count"] == 4
    assert provider_set_row["npi"] is None
    assert provider_set_row["canonical_payload"]["provider_count_mode"] == "summed_provider_groups"


def test_ptg2_serving_only_provider_set_uses_real_provider_group_hashes():
    provider_ref = {
        "provider_group_id": 7,
        "provider_groups": [
            {"tin": {"type": "ein", "value": "12-3456789"}, "npi": [1111111111, 2222222222]},
            {"tin": {"type": "ein", "value": "98-7654321"}, "npi": [3333333333]},
        ],
    }
    provider_entry, _row = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=provider_ref["provider_group_id"],
        provider_groups=provider_ref["provider_groups"],
        network_names=[],
    )
    assert provider_entry is not None
    expected_group_hashes = sorted(provider_entry["provider_group_hashes"])
    synthetic_entry_hash = int(provider_entry["__hash__"])
    assert synthetic_entry_hash not in expected_group_hashes

    result = process_ptg._serving_only_rows_for_payload(
        {
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiated_rates": [
                {
                    "provider_references": [7],
                    "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                }
            ],
        },
        provider_map={7: [provider_entry]},
        plan_fields={"plan_id": "plan", "plan_market_type": "group"},
        snapshot_id="snapshot",
        plan_month_id="snapshot-month",
        source_trace_payload=[],
        compact_serving=True,
        source_trace_set_hash="source-trace",
        include_price_set_rows=True,
    )

    provider_set = result["provider_set_rows"][0]
    assert provider_set["provider_set_hash"] == process_ptg._serving_only_hash_int_sets(
        "serving_provider_set",
        expected_group_hashes,
    )


def test_ptg2_single_pass_in_network_parser_uses_provider_cache(tmp_path, monkeypatch):
    raw_path = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"tin": {"type": "ein", "value": "123"}, "npi": ["1234567890"]}],
            }
        ],
        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "name": "Office visit",
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 12.34,
                                "expiration_date": "2026-12-31",
                                "billing_class": "professional",
                                "service_code": ["11"],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(raw_path, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(cls, []).extend(rows)

    monkeypatch.setattr(process_ptg, "push_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "log_error", AsyncMock())

    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    classes = {
        "PTGProviderGroup": "providers",
        "PTGInNetworkItem": "items",
        "PTGBillingCode": "billing",
        "PTGNegotiatedRate": "rates",
        "PTGNegotiatedPrice": "prices",
    }
    try:
        asyncio.run(
            process_ptg._parse_in_network_file_single_pass(
                str(raw_path),
                99,
                {"plan_id": "010854205"},
                None,
                cache,
                classes,
                False,
                "log",
                "file://rates.json.gz",
            )
        )
    finally:
        cache.close()

    assert pushed["providers"][0]["npi"] == [1234567890]
    assert pushed["items"][0]["billing_code"] == "99213"
    assert pushed["rates"][0]["provider_group_hash"] == pushed["providers"][0]["provider_group_hash"]
    assert pushed["prices"][0]["negotiated_rate"] == 12.34
    process_ptg.log_error.assert_not_awaited()


def test_ptg2_stream_logical_artifact_handles_gzip_without_loading_all(tmp_path):
    raw_path = tmp_path / "toc.json.gz"
    expected = b'{"reporting_structure":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.stream_logical_artifact(raw_path, output_dir=tmp_path)

    assert logical.compression == "gzip"
    assert Path(logical.logical_path).read_bytes() == expected
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)


def test_ptg2_logical_identity_streams_gzip_without_materializing_json(tmp_path):
    raw_path = tmp_path / "rates.json.gz"
    expected = b'{"in_network":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.logical_artifact_identity(raw_path)

    assert logical.compression == "gzip"
    assert logical.logical_path == str(raw_path)
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)
    assert not list(tmp_path.glob("*_logical.json"))
    with process_ptg.open_json_artifact_stream(raw_path) as fp:
        assert fp.read() == expected


def test_ptg2_ensure_tables_uses_existing_db_create_table(monkeypatch):
    created = []

    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        created.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    asyncio.run(process_ptg.ensure_ptg2_tables())

    assert "ptg2_import_run" in created
    assert "ptg2_current_snapshot" in created
    assert "ptg2_source_file_version" in created
    assert "ptg2_provider_group" in created
    assert "ptg2_rate_pack" in created


def test_ptg2_ensure_tables_fails_fast_on_create_error(monkeypatch):
    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        if table.name == "ptg2_snapshot":
            raise RuntimeError("no permission")

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    with pytest.raises(RuntimeError, match="ptg2_snapshot"):
        asyncio.run(process_ptg.ensure_ptg2_tables())


def test_ptg2_ensure_indexes_skips_duplicate_primary_unique_index(monkeypatch):
    statements = []

    async def fake_status(statement):
        statements.append(statement)

    model = SimpleNamespace(
        __tablename__="example_ptg_table",
        __my_index_elements__=["id"],
        __my_additional_indexes__=[],
        __table__=SimpleNamespace(primary_key=[SimpleNamespace(name="id")]),
    )
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    asyncio.run(process_ptg._ensure_indexes(model, "mrf"))

    assert not any("example_ptg_table_idx_primary" in statement for statement in statements)


def test_ptg2_in_network_download_failure_returns_failed_result(monkeypatch, tmp_path):
    async def fake_materialize(*_args, **_kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)

    result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": "files", "ImportLog": "log"},
            {},
            False,
        )
    )

    assert result.success is False
    assert result.source_type == "in_network"
    assert result.url == "https://example.test/rates.json.gz"
    assert "download failed" in result.error


def test_ptg2_in_network_serving_only_zero_rows_returns_skipped_result(monkeypatch, tmp_path):
    artifact = tmp_path / "rates.json.gz"
    artifact.write_bytes(b"{}")
    raw_artifact = process_ptg.PTG2RawArtifact(
        original_url="https://example.test/rates.json.gz",
        canonical_url="https://example.test/rates.json.gz",
        raw_path=str(artifact),
        raw_storage_uri=str(artifact),
        raw_sha256="raw",
        byte_count=2,
        head=None,
    )
    logical_artifact = process_ptg.PTG2LogicalArtifact(
        logical_path=str(artifact),
        logical_sha256="logical",
        byte_count=2,
    )

    async def fake_materialize(*_args, **_kwargs):
        return raw_artifact, logical_artifact

    async def fake_source_version(**_kwargs):
        return None

    async def fake_parse(*_args, **_kwargs):
        return {
            "serving_only": True,
            "rust_compact_serving": True,
            "rust_records": 0,
            "serving_rates": 0,
        }

    monkeypatch.setenv(process_ptg.PTG2_SERVING_ONLY_IMPORT_ENV, "true")
    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "_extract_metadata_fields", AsyncMock(return_value={}))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(process_ptg, "_record_source_version", fake_source_version)
    monkeypatch.setattr(process_ptg, "_parse_in_network_file_serving_only", fake_parse)

    result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": SimpleNamespace(__name__="PTGFile"), "ImportLog": "log"},
            {},
            False,
            compact_import=True,
        )
    )

    assert result.success is True
    assert result.skipped is True
    assert result.error is None
    assert result.summary["serving_rates"] == 0
    assert result.summary["skipped_reason"] == "parsed zero serving rates"


def test_ptg2_downloaded_jobs_are_prefetched_concurrently(monkeypatch):
    started = []

    def fake_download(job, **_kwargs):
        started.append(job["url"])
        if "slow" in job["url"]:
            time.sleep(0.05)
        return process_ptg.PTG2DownloadedJob(job=job)

    async def collect():
        results = []
        async for downloaded in process_ptg._iter_downloaded_ptg_jobs(
            [
                {"type": "in_network", "url": "https://example.test/slow.json.gz"},
                {"type": "in_network", "url": "https://example.test/fast.json.gz"},
            ],
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=None,
        ):
            results.append(downloaded.job["url"])
        return results

    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_TASKS_ENV, "2")
    monkeypatch.setattr(process_ptg, "_download_ptg_job_artifact_sync", fake_download)

    yielded = asyncio.run(collect())

    assert started[:2] == [
        "https://example.test/slow.json.gz",
        "https://example.test/fast.json.gz",
    ]
    assert yielded[0] == "https://example.test/fast.json.gz"
    assert yielded[1] == "https://example.test/slow.json.gz"


def test_ptg2_main_marks_failed_when_all_discovered_jobs_fail(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            False,
            error="download failed",
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "build_ptg2_snapshot_index_artifact", AsyncMock())
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    with pytest.raises(RuntimeError, match="processed zero files"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-04",
                import_id="state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] == 0
    assert import_run_rows[-1]["report"]["files_failed"] == 1
    assert "download failed" in import_run_rows[-1]["report"]["failed_files"][0]["error"]
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []


def test_ptg2_main_blocks_partial_publish_by_default(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_in_network(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            False,
            error="download failed",
        )

    async def fake_allowed(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "allowed_amounts",
            "https://example.test/allowed.json.gz",
            True,
            file_id=123,
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_in_network)
    monkeypatch.setattr(process_ptg, "_process_allowed_amounts_file", fake_allowed)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "build_ptg2_snapshot_index_artifact", AsyncMock())
    monkeypatch.delenv("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", raising=False)
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    with pytest.raises(RuntimeError, match="failed 1 of 2 attempted"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                allowed_url="https://example.test/allowed.json.gz",
                import_month="2026-04",
                import_id="partial_state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] == 1
    assert import_run_rows[-1]["report"]["files_failed"] == 1
    assert current_rows == []


def test_ptg2_main_marks_failed_when_toc_download_fails(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_toc(*_args, **_kwargs):
        raise RuntimeError("409 public access denied")

    publish_mock = AsyncMock()

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
    monkeypatch.setattr(process_ptg, "_publish_rust_compact_snapshot_tables", publish_mock)
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")
    monkeypatch.delenv("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", raising=False)

    with pytest.raises(RuntimeError, match="table-of-contents"):
        asyncio.run(
            process_ptg.main(
                toc_urls=["https://example.test/index.json"],
                import_month="2026-04",
                import_id="toc_failure_state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["jobs_discovered"] == 0
    assert import_run_rows[-1]["report"]["toc_failures"][0]["error"] == "409 public access denied"
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []
    publish_mock.assert_not_awaited()


def test_ptg2_snapshot_artifact_builder_writes_serving_index(monkeypatch, tmp_path):
    class Table:
        def __init__(self, name):
            self.schema = "mrf"
            self.name = name

    class Model:
        def __init__(self, name):
            self.__tablename__ = name
            self.__table__ = Table(name)

    rows = [
        {
            "plan_id": "010854205",
            "plan_name": "Heartland",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "issuer_name": "Heartland",
            "plan_sponsor_name": "Heartland",
            "billing_code": "70551",
            "billing_code_type": "CPT",
            "procedure_name": "MRI brain",
            "procedure_description": "MRI brain",
            "provider_npi": [1234567890],
            "tin_type": "ein",
            "tin_value": "123",
            "tin_business_name": "Example Imaging",
            "negotiated_type": "negotiated",
            "negotiated_rate": "450.00",
            "expiration_date": "2026-12-31",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": "outpatient",
            "billing_code_modifier": [],
            "additional_information": None,
            "source_url": "https://example.test/rates.json.gz",
        }
    ]

    async def fake_all(_sql):
        return rows

    async def fake_push_objects(_payload, _cls, rewrite=False):
        return None

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    classes = {
        "PTGInNetworkItem": Model("ptg_in_network_item_test"),
        "PTGNegotiatedRate": Model("ptg_negotiated_rate_test"),
        "PTGNegotiatedPrice": Model("ptg_negotiated_price_test"),
        "PTGProviderGroup": Model("ptg_provider_group_test"),
        "PTGFile": Model("ptg_file_test"),
    }

    result = asyncio.run(process_ptg.build_ptg2_snapshot_index_artifact(classes, "snap-test", "run-test"))

    assert result["plan_count"] == 1
    artifact_path = tmp_path / "snapshot_index" / "snap-test.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["rates"]["010854205"]["70551"][0]["prices"][0]["negotiated_rate"] == 450


def test_ptg2_db_serving_index_builder_materializes_table(monkeypatch, tmp_path):
    statuses = []

    async def fake_scalar(sql, **params):
        if "to_regclass" in sql:
            return params["table_name"]
        if "COUNT(*)" in sql:
            return 1
        if "COUNT(DISTINCT plan_id)" in sql:
            return 1
        if "COUNT(DISTINCT COALESCE" in sql:
            return 1
        if "SUM(provider_count)" in sql:
            return 123
        return None

    async def fake_status(sql, **params):
        statuses.append((sql, params))
        return 1

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    result = asyncio.run(process_ptg.build_ptg2_db_serving_index("snap-compact", "run-compact"))

    insert_sql = next(sql for sql, _params in statuses if "INSERT INTO mrf.ptg2_serving_rate" in sql)
    assert result["storage"] == "db"
    assert result["table"] == "mrf.ptg2_serving_rate"
    assert result["provider_granularity"] == "provider_set"
    assert result["procedure_consolidation"]["system"] == "HP_PROCEDURE_CODE"
    assert "code_crosswalk" in insert_sql
    assert "pricing_procedure" in insert_sql
    assert "code_catalog" in insert_sql
    assert "snapshot_index" not in [part.name for part in tmp_path.iterdir()]


def test_ptg_cli_passes_plan_filters(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_ptg", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.ptg.callback(
        toc_url=("https://example.test/toc.json",),
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        source_key="heartland_dental",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_id=("391804522",),
        plan_name_contains=("dewitt",),
        plan_market_type=("group",),
        file_url_contains=("ps1-50",),
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
        test=True,
    )

    fake_initiate.assert_called_once_with(
        test_mode=True,
        toc_urls=["https://example.test/toc.json"],
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        source_key="heartland_dental",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_ids=["391804522"],
        plan_name_contains=["dewitt"],
        plan_market_types=["group"],
        file_url_contains=["ps1-50"],
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
    )


def test_ptg2_rust_scanner_emits_top_level_object_bytes(tmp_path):
    if process_ptg._ptg2_rust_scanner_binary() is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {"provider_group_id": 1, "provider_groups": [{"npi": [123], "tin": {"type": "ein", "value": "1"}}]},
            {"provider_group_id": 2, "provider_groups": [{"npi": [456], "tin": {"type": "ein", "value": "2"}}]},
        ],
        "in_network": [
            {"billing_code": "99213", "negotiated_rates": []},
            {"billing_code": "70551", "negotiated_rates": []},
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    rows = list(process_ptg._iter_top_level_object_bytes_rust(artifact, {"provider_references", "in_network"}))

    assert [name for name, _raw in rows] == [
        "provider_references",
        "provider_references",
        "in_network",
        "in_network",
    ]
    assert json.loads(rows[0][1])["provider_group_id"] == 1
    assert json.loads(rows[-1][1])["billing_code"] == "70551"


def test_ptg2_rust_compact_serving_mode_emits_copy_oriented_rows(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99213",
                "name": "Office visit",
                "description": "Established patient office visit",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100,
                                "expiration_date": "2026-12-31",
                                "service_code": ["11"],
                                "billing_class": "professional",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))

    kinds = [kind for kind, _row in frames]
    assert "procedure" in kinds
    assert "price_set" not in kinds
    assert "provider_set" in kinds
    assert "serving_rate_compact" in kinds
    compact_row = [row for kind, row in frames if kind == "serving_rate_compact"][0]
    assert compact_row["snapshot_id"] == "snapshot"
    assert compact_row["plan_id"] == "plan"
    assert compact_row["billing_code"] == "99213"
    assert compact_row["provider_count"] == 1
    assert b"PTG2_SCANNER_PROGRESS" in completed.stderr


def test_ptg2_rust_compact_serving_mode_can_write_copy_file(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    copy_path = tmp_path / "compact.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(copy_path),
        "HLTHPRT_PTG2_RUST_WORKERS": "1",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert copy_path.exists()
    copy_lines = copy_path.read_text().splitlines()
    assert len(copy_lines) == 1
    fields = copy_lines[0].split("\t")
    assert fields[1] == "snapshot"
    assert fields[2] == "plan"
    assert fields[6] == "99213"
    assert fields[8] == "1"
    assert len(fields) == 11

    assert b"serving_rate_compact" not in completed.stdout
    assert b"compact_copy_file" in completed.stdout


def test_ptg2_rust_compact_serving_copy_files_support_inline_provider_groups(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    member_copy = tmp_path / "provider_group_member.copy"
    payload = {
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_groups": [
                            {"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}
                        ],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    },
                    {
                        "provider_groups": [
                            {"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}
                        ],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 125}],
                    },
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH": str(member_copy),
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert serving_copy.exists()
    assert len(serving_copy.read_text().splitlines()) == 2
    assert member_copy.exists()
    member_lines = member_copy.read_text().splitlines()
    assert len(member_lines) == 1
    assert b"compact_copy_file" in completed.stdout
    assert b"provider_group_member_copy_file" in completed.stdout


def test_ptg2_rust_compact_serving_parallel_workers_write_shards(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    copy_path = tmp_path / "compact.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            },
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            },
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(copy_path),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES": "1",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert not copy_path.exists()
    shard_paths = sorted(tmp_path.glob("compact.copy.worker*"))
    assert shard_paths
    copy_lines = [
        line
        for shard_path in shard_paths
        for line in shard_path.read_text().splitlines()
    ]
    assert len(copy_lines) == 1
    fields = copy_lines[0].split("\t")
    assert fields[6] == "99213"
    assert fields[8] == "1"
    assert b"serving_rate_compact" not in completed.stdout
    assert b"compact_copy_file" in completed.stdout
    assert b"dedupe_summary" in completed.stdout
    assert b"PTG2_DEDUPE_SUMMARY" in completed.stderr

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
    dedupe_summary = [row for kind, row in frames if kind == "dedupe_summary"][0]
    rotated_copy_events = [
        row
        for kind, row in frames
        if kind == "compact_copy_file" and row.get("final") is False
    ]
    assert rotated_copy_events
    assert all(str(row["path"]).endswith(".ready") for row in rotated_copy_events)
    assert dedupe_summary["negotiated_rates"] == 2
    assert dedupe_summary["serving_rate_attempted"] == 2
    assert dedupe_summary["serving_rate_unique"] == 1
    assert dedupe_summary["serving_rate_duplicate"] == 1
    assert dedupe_summary["serving_rate_reduction_pct"] == 50.0
    assert dedupe_summary["price_atom_attempted"] == 1
    assert dedupe_summary["price_atom_unique"] == 1
    assert dedupe_summary["price_atom_duplicate"] == 0
    assert dedupe_summary["price_set_entry_attempted"] == 0
    assert dedupe_summary["price_set_entry_unique"] == 0
    assert dedupe_summary["price_set_entry_duplicate"] == 0
    assert dedupe_summary["provider_set_entry_attempted"] == 0
    assert dedupe_summary["provider_set_entry_unique"] == 0
    assert dedupe_summary["provider_set_entry_duplicate"] == 0
    assert dedupe_summary["provider_entry_component_attempted"] == 0
    assert dedupe_summary["provider_entry_component_unique"] == 0
    assert dedupe_summary["provider_entry_component_duplicate"] == 0


def test_ptg2_rust_compact_price_sets_emit_normalized_membership(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    price_code_set_copy = tmp_path / "price_code_set.copy"
    price_atom_copy = tmp_path / "price_atom.copy"
    price_set_entry_copy = tmp_path / "price_set_entry.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100,
                                "service_code": [" 11 ", "11"],
                                "billing_code_modifier": ["tc", " TC "],
                            },
                            {
                                "negotiated_type": "derived",
                                "negotiated_rate": "125.50",
                                "service_code": ["22"],
                                "billing_code_modifier": ["26"],
                            },
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH": str(price_code_set_copy),
        "HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH": str(price_atom_copy),
        "HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH": str(price_set_entry_copy),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    price_atom_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_atom.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    price_code_set_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_code_set.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    price_set_entry_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_set_entry.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    assert not list(tmp_path.glob("price_set.copy*"))
    assert len(price_atom_lines) == 2
    assert len(price_code_set_lines) == 4
    assert len(price_set_entry_lines) == 2
    atom_fields = [line.split("\t") for line in price_atom_lines]
    assert all(len(fields) == 9 for fields in atom_fields)
    assert all(fields[4] != "\\N" and fields[7] != "\\N" for fields in atom_fields)
    code_set_payloads = {line.split("\t", 1)[1] for line in price_code_set_lines}
    assert '{"11"}' in code_set_payloads
    assert '{"TC"}' in code_set_payloads

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
    dedupe_summary = [row for kind, row in frames if kind == "dedupe_summary"][0]
    assert dedupe_summary["price_atom_unique"] == 2
    assert dedupe_summary["price_set_unique"] == 1
    assert dedupe_summary["price_set_entry_unique"] == 2


def test_ptg2_rust_compact_provider_sets_use_real_group_hashes(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    provider_set_copy = tmp_path / "provider_set.copy"
    provider_set_component_copy = tmp_path / "provider_set_component.copy"
    member_copy = tmp_path / "provider_group_member.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [
                    {"npi": [1111111111, 2222222222], "tin": {"type": "ein", "value": "12-3456789"}},
                    {"npi": [3333333333], "tin": {"type": "ein", "value": "98-7654321"}},
                ],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH": str(provider_set_copy),
        "HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH": str(provider_set_component_copy),
        "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH": str(member_copy),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
    }
    subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    provider_set_lines = [
        line
        for shard_path in sorted(tmp_path.glob("provider_set.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    assert len(provider_set_lines) == 1
    provider_set_fields = provider_set_lines[0].split("\t")
    assert len(provider_set_fields) == 2
    assert int(provider_set_fields[1]) == 3
    provider_set_component_lines = [
        line
        for shard_path in sorted(tmp_path.glob("provider_set_component.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    component_group_hashes = {
        int(fields[1])
        for fields in (line.split("\t") for line in provider_set_component_lines)
    }
    member_group_hashes = {
        int(line.split("\t")[0])
        for shard_path in sorted(tmp_path.glob("provider_group_member.copy*"))
        for line in shard_path.read_text().splitlines()
    }

    assert len(member_group_hashes) == 2
    assert len(provider_set_component_lines) == 2
    assert component_group_hashes == member_group_hashes
    assert not list(tmp_path.glob("provider_set_entry.copy*"))
    assert not list(tmp_path.glob("provider_entry_component.copy*"))


def test_ptg2_rust_compact_uses_bounded_event_queue_default(monkeypatch, tmp_path):
    captured_env = {}

    class FakeProcess:
        stdout = io.BytesIO()
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_args, stdout, stderr, env):
        captured_env.update(env)
        return FakeProcess()

    monkeypatch.delenv(process_ptg.PTG2_RUST_EVENT_QUEUE_ENV, raising=False)
    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", fake_popen)

    list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            snapshot_id="snap",
            plan_id="plan",
            plan_month_id="month",
            source_trace_set_hash="trace",
            compact_copy_path=tmp_path / "serving.copy",
        )
    )

    assert captured_env[process_ptg.PTG2_RUST_EVENT_QUEUE_ENV] == "32"


def test_ptg2_compact_finalize_defers_provider_locations_by_default(monkeypatch):
    status_calls = []
    scalar_values = iter([3, 1, 2])

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_scalar(_statement, **_params):
        return next(scalar_values)

    async def fail_provider_locations(_snapshot_id):
        raise AssertionError("provider locations should be deferred by default")

    monkeypatch.delenv(process_ptg.PTG2_DEFER_PROVIDER_LOCATIONS_ENV, raising=False)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg, "_build_ptg2_provider_locations", fail_provider_locations)

    result = asyncio.run(process_ptg.build_ptg2_compact_serving_index("snap", "run"))

    assert result["rate_count"] == 3
    assert any("ANALYZE mrf.ptg2_serving_rate_compact" in statement for statement in status_calls)


def test_ptg2_rust_snapshot_publish_renames_dictionary_stages_before_index(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        if "COUNT(DISTINCT plan_hash)" in statement:
            return [{"plans": 1}]
        if "COUNT(DISTINCT COALESCE" in statement:
            return [{"procedures": 2}]
        return []

    async def fake_table_exists(_schema, _table):
        return True

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(ptg_rust_publish, "_table_exists", fake_table_exists)
    monkeypatch.setattr(ptg_rust_publish, "_table_has_rows", AsyncMock(return_value=True))
    async def fake_estimated_rows(_schema, _table_name):
        return 123

    estimate_mock = AsyncMock(side_effect=fake_estimated_rows)
    exact_mock = AsyncMock(return_value=987)
    monkeypatch.setattr(ptg_rust_publish, "_estimated_table_rows", estimate_mock)
    monkeypatch.setattr(ptg_rust_publish, "_exact_table_rows", exact_mock)

    result = asyncio.run(
        process_ptg._publish_rust_compact_snapshot_tables(
            {
                "serving_rate_compact": "ptg2_rust_stage_serving_rate_compact_abc",
                "price_code_set": "ptg2_rust_stage_price_code_set_abc",
                "price_atom": "ptg2_rust_stage_price_atom_abc",
                "price_set": "ptg2_rust_stage_price_set_abc",
                "price_set_entry": "ptg2_rust_stage_price_set_entry_abc",
                "procedure": "ptg2_rust_stage_procedure_abc",
                "provider_set": "ptg2_rust_stage_provider_set_abc",
                "provider_set_component": "ptg2_rust_stage_provider_set_component_abc",
                "provider_group_member": "ptg2_rust_stage_provider_group_member_abc",
            },
            snapshot_id="ptg2:202604:snap",
            import_run_id="ptg2:run",
            source_key="heartland_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert result["storage"] == "db_compact_snapshot"
    assert result["source_key"] == "heartland_dental"
    assert result["table"].startswith("mrf.ptg2_serving_rate_compact_")
    assert result["price_code_set_table"].startswith("mrf.ptg2_price_code_set_")
    assert result["price_atom_table"].startswith("mrf.ptg2_price_atom_")
    assert "price_table" not in result
    assert result["price_set_entry_table"].startswith("mrf.ptg2_price_set_entry_")
    assert result["provider_set_component_table"].startswith("mrf.ptg2_provider_set_component_")
    assert result["provider_set_entry_table"] is None
    assert result["provider_entry_component_table"] is None
    assert result["rate_count"] == 987
    assert result["serving_rates"] == 987
    assert result["row_count"] == 987
    assert "RENAME TO" in joined
    assert "INSERT INTO" not in joined
    assert "SELECT DISTINCT ON" not in joined
    assert "ptg2_price_set (" not in joined
    assert "ptg2_rust_stage_serving_rate_compact_abc" in joined
    assert "ptg2_rust_stage_price_code_set_abc" in joined
    assert "ptg2_rust_stage_price_atom_abc" in joined
    assert "ptg2_rust_stage_price_set_entry_abc" in joined
    assert "ptg2_rust_stage_provider_set_component_abc" in joined
    assert "provider_group_hashes" not in "\n".join(
        statement for statement in status_calls if "CREATE INDEX" in statement and "ptg2_provider_set_" in statement
    )
    assert "PTG2_PUBLISH_STAGE_INDEX_START" not in joined
    assert "CREATE INDEX IF NOT EXISTS" in joined
    exact_mock.assert_awaited_once()
    assert exact_mock.await_args.args[1].startswith("ptg2_serving_rate_compact_")
    serving_estimate_calls = [
        call for call in estimate_mock.await_args_list
        if call.args[1].startswith("ptg2_serving_rate_compact_")
    ]
    assert serving_estimate_calls == []


def test_ptg2_serving_stage_table_for_copy_uses_worker_lane():
    stage_tables = {
        "serving_rate_compact": "stage_w0000",
        process_ptg._serving_stage_lane_key(1): "stage_w0001",
        process_ptg._serving_stage_lane_key(7): "stage_w0007",
    }

    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0001.part000002.ready"),
        )
        == "stage_w0001"
    )
    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0000.part000002.ready"),
        )
        == "stage_w0000"
    )
    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0009.part000002.ready"),
        )
        == "stage_w0000"
    )


def test_ptg2_rust_snapshot_publish_inherits_serving_stage_lanes(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        if "COUNT(DISTINCT plan_hash)" in statement:
            return [{"plans": 1}]
        return []

    async def fake_table_exists(_schema, _table):
        return True

    async def fake_table_has_rows(_schema, table):
        return table != "stage_empty_base"

    async def fake_estimated_rows(_schema, _table_name):
        return 0

    async def fake_exact_rows(_schema, table_name):
        if table_name.endswith("_p00"):
            return 100
        if table_name.endswith("_p01"):
            return 200
        return 0

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(ptg_rust_publish, "_table_exists", fake_table_exists)
    monkeypatch.setattr(ptg_rust_publish, "_table_has_rows", fake_table_has_rows)
    monkeypatch.setattr(ptg_rust_publish, "_estimated_table_rows", fake_estimated_rows)
    monkeypatch.setattr(ptg_rust_publish, "_exact_table_rows", fake_exact_rows)

    result = asyncio.run(
        process_ptg._publish_rust_compact_snapshot_tables(
            {
                "serving_rate_compact": "stage_empty_base",
                process_ptg._serving_stage_lane_key(1): "stage_worker_0001",
                process_ptg._serving_stage_lane_key(2): "stage_worker_0002",
            },
            snapshot_id="ptg2:202604:snap",
            import_run_id="ptg2:run",
            source_key="heartland_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert result["storage"] == "db_compact_snapshot"
    assert result["rate_count"] == 300
    assert result["table"].startswith("mrf.ptg2_serving_rate_compact_")
    assert "INHERIT" in joined
    assert "stage_worker_0001" in joined
    assert "stage_worker_0002" in joined
    assert "INSERT INTO" not in joined
    assert "_p00" in joined
    assert "_p01" in joined


def test_ptg2_source_plan_rows_falls_back_to_serving_index_table(monkeypatch):
    calls = []

    async def fake_all(statement, **_params):
        calls.append(statement)
        if "FROM \"mrf\".\"ptg2_serving_rate_compact_exact\"" in statement:
            return [{"plan_id": "010854205", "plan_market_type": ""}]
        return []

    monkeypatch.setattr(ptg_source_pointers.db, "all", fake_all)
    monkeypatch.setattr(ptg_source_pointers, "_table_exists", AsyncMock(return_value=True))
    updated_at = process_ptg._utcnow()

    rows = asyncio.run(
        process_ptg._source_plan_rows(
            snapshot_id="snap",
            source_key="heartland_dental",
            import_month=process_ptg.normalize_import_month("2026-04"),
            previous_snapshot_id="prev",
            updated_at=updated_at,
            serving_index={"table": "mrf.ptg2_serving_rate_compact_exact"},
        )
    )

    assert rows == [
        {
            "plan_source_key": process_ptg._ptg2_plan_source_key(
                "010854205",
                "",
                process_ptg.normalize_import_month("2026-04"),
            ),
            "plan_id": "010854205",
            "plan_market_type": "",
            "import_month": process_ptg.normalize_import_month("2026-04"),
            "source_key": "heartland_dental",
            "snapshot_id": "snap",
            "previous_snapshot_id": "prev",
            "updated_at": updated_at,
        }
    ]
    assert len(calls) == 2


def test_ptg2_snapshot_manifest_table_names_allowlists_location_and_rejects_unsafe_names():
    names = process_ptg._snapshot_manifest_table_names(
        {
            "storage": "db_compact_snapshot",
            "table": "mrf.ptg2_serving_rate_compact_abc123",
            "provider_group_location_table": "mrf.ptg2_provider_group_location_abc123",
            "provider_group_member_table": "mrf.ptg2_provider_group_member_abc123",
            "provider_set_table": "mrf.ptg2_provider_set_abc123",
            "provider_set_entry_table": "mrf.ptg2_provider_set_abc123",
            "price_atom_table": "mrf.ptg2_price_atom_bad-name",
            "procedure_table": "mrf.not_a_snapshot_table",
            "price_set_entry_table": "mrf.ptg2_price_set_entry_abc123;drop",
        }
    )

    assert names == [
        "ptg2_serving_rate_compact_abc123",
        "ptg2_provider_set_abc123",
        "ptg2_provider_group_member_abc123",
        "ptg2_provider_group_location_abc123",
    ]


def test_ptg2_source_scoped_report_uses_published_serving_rate_count(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "rust_compact_serving": True},
        )

    async def fake_publish(*_args, **_kwargs):
        return {
            "storage": "db_compact_snapshot",
            "table": "mrf.ptg2_serving_rate_compact_exact",
            "rate_count": 987,
            "serving_rates": 987,
        }

    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_SERVING_TABLE_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, "true")
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_rust_copy_stage_tables", AsyncMock(return_value={"serving_rate_compact": "stage"}))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_rust_compact_snapshot_tables", fake_publish)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="source_report_count",
            source_key="heartland_dental",
        )
    )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    final_report = import_run_rows[-1]["report"]
    assert final_report["serving_rates"] == 987
    assert final_report["rate_count"] == 987
    assert final_report["serving_index"]["serving_rates"] == 987
    assert final_report["successful_files"][0]["summary"]["serving_rates"] == 111
    assert snapshot_rows[-1]["manifest"]["serving_rates"] == 987


def test_ptg2_test_mode_does_not_enter_source_scoped_rust_publish(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **kwargs):
        assert kwargs.get("rust_stage_tables") is None
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "rust_compact_serving": False},
        )

    create_stage_mock = AsyncMock(return_value={"serving_rate_compact": "stage"})
    publish_mock = AsyncMock()

    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_SERVING_TABLE_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, "true")
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "prepare_ptg2_compact_bulk_load", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_rust_copy_stage_tables", create_stage_mock)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_rust_compact_snapshot_tables", publish_mock)
    monkeypatch.setattr(
        process_ptg,
        "build_ptg2_compact_serving_index",
        AsyncMock(return_value={"storage": "db_compact", "rate_count": 222, "serving_rates": 222}),
    )

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="test_mode_rust_env",
            source_key="heartland_dental",
            test_mode=True,
        )
    )

    create_stage_mock.assert_not_awaited()
    publish_mock.assert_not_awaited()
    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    assert import_run_rows[-1]["options"]["source_scoped_compact"] is False
    assert import_run_rows[-1]["report"]["serving_rates"] == 222


def test_ptg2_compact_finalize_builds_provider_locations_after_analyze_when_enabled(monkeypatch):
    calls = []
    scalar_values = iter([3, 1, 2])

    async def fake_status(statement, **_params):
        if "ANALYZE mrf.ptg2_serving_rate_compact" in statement:
            calls.append("analyze_compact")

    async def fake_scalar(_statement, **_params):
        return next(scalar_values)

    async def fake_provider_locations(snapshot_id):
        calls.append(f"provider_locations:{snapshot_id}")

    monkeypatch.setenv(process_ptg.PTG2_DEFER_PROVIDER_LOCATIONS_ENV, "false")
    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg, "_build_ptg2_provider_locations", fake_provider_locations)

    result = asyncio.run(process_ptg.build_ptg2_compact_serving_index("snap", "run"))

    assert result["rate_count"] == 3
    assert calls.index("analyze_compact") < calls.index("provider_locations:snap")


def test_ptg2_rust_scanner_enables_rust_compact_serving_by_default(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_ENV, "true")
    monkeypatch.delenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, raising=False)

    assert process_ptg._use_rust_compact_serving() is True


def test_ptg2_rust_compact_serving_can_be_disabled_when_scanner_enabled(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, "false")

    assert process_ptg._use_rust_compact_serving() is False
