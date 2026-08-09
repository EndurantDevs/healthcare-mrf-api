# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_ptg2_scanner_indexed_rapidgzip import (
    _built_scanner_binary,
    _indexed_object_progress,
    _mixed_inline_referenced_payload,
    _run_delayed_indexed_scanner,
    _run_parallel_scanner,
    _scanner_fixture_payload,
    _single_frame,
    _write_boundary_split_multimember_gzip,
    _write_fake_rapidgzip,
    _write_gzip_json,
    pytest,
    shutil,
    subprocess,
)


def test_delayed_indexed_range_emits_object_coverage_progress(tmp_path):
    """Verify delayed indexed range emits object coverage progress."""
    scanner_run, scanner_completions = _run_delayed_indexed_scanner(tmp_path)
    assert _single_frame(scanner_run, "scanner_summary")[
        "indexed_range_decoder_threads_selected"
    ] == 4
    assert len(scanner_completions) == 1
    progress_lines, progress_payloads = _indexed_object_progress(
        scanner_completions[0]
    )

    assert progress_lines
    assert all("progress_basis=indexed_objects" in line for line in progress_lines)
    intermediate_progress_payloads = [
        progress_fields
        for progress_fields in progress_payloads
        if progress_fields["done"] == "false"
    ]
    assert intermediate_progress_payloads
    assert all(
        progress_fields["indexed_objects_total"] == "24"
        for progress_fields in intermediate_progress_payloads
    )
    assert any(
        0 < float(progress_fields["percent"]) < 100
        and 0 < int(progress_fields["indexed_objects_completed"]) < 24
        and progress_fields["eta_seconds"] != "unknown"
        for progress_fields in intermediate_progress_payloads
    )


def test_decoder_budget_caps_selected_range_producers(tmp_path):
    fixture_document = _mixed_inline_referenced_payload()
    artifact = tmp_path / "budget-capped-reversed.json.gz"
    _write_gzip_json(
        artifact,
        {
            "in_network": fixture_document["in_network"],
            "provider_references": fixture_document["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-budget-capped"
    _write_fake_rapidgzip(fake_rapidgzip)

    run = _run_parallel_scanner(
        _built_scanner_binary(),
        artifact,
        tmp_path / "budget-capped-output",
        workers=8,
        copy_kinds=("price_atom",),
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
            "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "4",
            "FAKE_RAPIDGZIP_EXPECT_OBJECT_RANGE_THREADS": "1",
        },
    )

    config = _single_frame(run, "scanner_config")
    summary = _single_frame(run, "scanner_summary")
    assert config["indexed_range_producers_requested"] == 4
    assert config["indexed_range_producers_selected"] == 2
    assert config["indexed_range_decoder_threads_selected"] == 2
    assert [
        range_info["decoder_threads"] for range_info in config["indexed_ranges"]
    ] == [1, 1]
    assert summary["indexed_range_producers_selected"] == 2
    assert summary["indexed_range_decoder_threads_selected"] == 2
    assert sum(
        range_info["rate_count"] for range_info in summary["indexed_ranges"]
    ) == 24 * 8


def test_indexed_range_producers_fall_back_for_one_object(tmp_path):
    fixture_document = _mixed_inline_referenced_payload()
    artifact = tmp_path / "one-object-reversed.json.gz"
    _write_gzip_json(
        artifact,
        {
            "in_network": fixture_document["in_network"][:1],
            "provider_references": fixture_document["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-one-object"
    _write_fake_rapidgzip(fake_rapidgzip)

    run = _run_parallel_scanner(
        _built_scanner_binary(),
        artifact,
        tmp_path / "one-object-output",
        workers=8,
        copy_kinds=("price_atom",),
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "4",
            "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "4",
            "FAKE_RAPIDGZIP_EXPECT_OBJECT_RANGE_THREADS": "4",
        },
    )

    config = _single_frame(run, "scanner_config")
    summary = _single_frame(run, "scanner_summary")
    assert config["indexed_range_producers_requested"] == 4
    assert config["indexed_range_producers_selected"] == 1
    assert config["indexed_range_decoder_threads_selected"] == 4
    assert config["indexed_ranges"][0]["decoder_threads"] == 4
    assert config["indexed_range_count"] == 1
    assert summary["indexed_range_producers_selected"] == 1
    assert summary["indexed_ranges"][0]["object_count"] == 1
    assert summary["indexed_ranges"][0]["rate_count"] == 8


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [("truncate", b"ended early"), ("extra", b"extra bytes")],
)
def test_scanner_rejects_inexact_indexed_object_range_data(
    tmp_path, mutation, expected_error
):
    fixture_document = _mixed_inline_referenced_payload()
    artifact = tmp_path / f"{mutation}-reversed.json.gz"
    _write_gzip_json(
        artifact,
        {
            "in_network": fixture_document["in_network"],
            "provider_references": fixture_document["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / f"rapidgzip-{mutation}"
    _write_fake_rapidgzip(fake_rapidgzip)

    with pytest.raises(subprocess.CalledProcessError) as error_info:
        _run_parallel_scanner(
            _built_scanner_binary(),
            artifact,
            tmp_path / f"{mutation}-output",
            workers=8,
            copy_kinds=("price_atom",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "4",
                "FAKE_RAPIDGZIP_OBJECT_RANGE_MUTATION": mutation,
            },
        )

    assert expected_error in error_info.value.stderr


def test_index_discovery_handles_buffer_and_gzip_member_boundaries(tmp_path):
    rapidgzip_binary = shutil.which("rapidgzip")
    if rapidgzip_binary is None:
        pytest.skip("rapidgzip is not installed in this test environment")
    boundary_artifact = tmp_path / "boundary-split.json.gz"
    _write_boundary_split_multimember_gzip(boundary_artifact, _scanner_fixture_payload())

    indexed_scanner_run = _run_parallel_scanner(
        _built_scanner_binary(),
        boundary_artifact,
        tmp_path / "boundary-output",
        workers=8,
        copy_kinds=("price_atom",),
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": rapidgzip_binary,
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_INDEX_THREADS": "2",
        },
    )

    scanner_config_frame = _single_frame(indexed_scanner_run, "scanner_config")
    scanner_summary_frame = _single_frame(indexed_scanner_run, "scanner_summary")
    assert scanner_config_frame["execution_mode"] == "parallel_top_level_bytes_indexed_reorder"
    assert sum(worker["rates_seen"] for worker in scanner_summary_frame["workers"]) == 8
    assert indexed_scanner_run["serving_records"]
    assert indexed_scanner_run["copy_rows"]["price_atom"]


def test_indexed_workers_drain_bounded_rotation_events_before_join(tmp_path):
    fixture_payload = _scanner_fixture_payload()
    reversed_artifact = tmp_path / "bounded-events.json.gz"
    _write_gzip_json(
        reversed_artifact,
        {
            "in_network": fixture_payload["in_network"],
            "provider_references": fixture_payload["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-bounded-events"
    _write_fake_rapidgzip(fake_rapidgzip)
    output_dir = tmp_path / "bounded-events-output"

    scanner_run = _run_parallel_scanner(
        _built_scanner_binary(),
        reversed_artifact,
        output_dir,
        workers=16,
        copy_kinds=(
            "price_atom",
            "price_set_atom",
            "price_set_summary",
            "provider_group_member",
        ),
        sidecar_kinds=("provider_forward", "provider_inverted"),
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
            "HLTHPRT_PTG2_RUST_EVENT_QUEUE": "1",
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES": "1",
        },
    )

    scanner_summary_frame = _single_frame(scanner_run, "scanner_summary")
    assert sum(worker["rates_seen"] for worker in scanner_summary_frame["workers"]) == 24 * 8
    assert scanner_summary_frame["event_queue_unbounded"] is False
    artifact_event_paths = [
        artifact_event["path"]
        for record_kind, artifact_event in scanner_run["frames"]
        if record_kind
        in {
            "v3_serving_run_partition_file",
            "v3_serving_code_dictionary_file",
            "manifest_price_atom_copy_file",
            "manifest_price_set_atom_copy_file",
            "manifest_price_set_summary_copy_file",
            "manifest_provider_group_member_copy_file",
            "manifest_provider_forward_sidecar_file",
            "manifest_provider_inverted_sidecar_file",
            "source_audit_witness_file",
        }
    ]
    assert len(artifact_event_paths) == len(set(artifact_event_paths))
    assert set(artifact_event_paths) == {
        str(path)
        for path in output_dir.rglob("*")
        if path.is_file() and path.stat().st_size > 0
    }


def test_scanner_rejects_late_indexed_range_process_failure(tmp_path):
    scanner_binary = _built_scanner_binary()
    fixture_payload = _scanner_fixture_payload()
    reversed_artifact = tmp_path / "late-failure.json.gz"
    _write_gzip_json(
        reversed_artifact,
        {
            "in_network": fixture_payload["in_network"][:1],
            "provider_references": fixture_payload["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-fake"
    _write_fake_rapidgzip(fake_rapidgzip)

    with pytest.raises(subprocess.CalledProcessError) as error_info:
        _run_parallel_scanner(
            scanner_binary,
            reversed_artifact,
            tmp_path / "late-failure-output",
            workers=8,
            copy_kinds=("price_atom",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_INDEX_THREADS": "2",
                "FAKE_RAPIDGZIP_RANGE_EXIT": "23",
            },
        )

    assert b"late indexed failure" in error_info.value.stderr


@pytest.mark.parametrize(
    ("worker_parse_setting", "expected_error"),
    [
        ("true", b"late full-scan failure"),
        (
            "false",
            b"strict V3 source attestation requires worker-side raw rate parsing",
        ),
    ],
)
def test_scanner_rejects_late_full_scan_process_failure(
    tmp_path,
    worker_parse_setting,
    expected_error,
):
    scanner_binary = _built_scanner_binary()
    normal_artifact = tmp_path / "late-full-scan-failure.json.gz"
    _write_gzip_json(normal_artifact, _scanner_fixture_payload())
    fake_rapidgzip = tmp_path / "rapidgzip-full-scan-fake"
    _write_fake_rapidgzip(fake_rapidgzip)

    with pytest.raises(subprocess.CalledProcessError) as error_info:
        _run_parallel_scanner(
            scanner_binary,
            normal_artifact,
            tmp_path / f"late-full-scan-output-{worker_parse_setting}",
            workers=8,
            copy_kinds=("price_atom",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
                "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": worker_parse_setting,
                "FAKE_RAPIDGZIP_FULL_EXIT": "23",
            },
        )

    assert expected_error in error_info.value.stderr
