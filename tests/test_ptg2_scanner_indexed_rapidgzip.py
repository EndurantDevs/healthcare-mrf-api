# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_parallelism.py")
_SUPPORT_SPEC = importlib.util.spec_from_file_location("ptg2_scanner_test_support", _SUPPORT_PATH)
assert _SUPPORT_SPEC is not None and _SUPPORT_SPEC.loader is not None
_SUPPORT_MODULE = importlib.util.module_from_spec(_SUPPORT_SPEC)
_SUPPORT_SPEC.loader.exec_module(_SUPPORT_MODULE)

_built_scanner_binary = _SUPPORT_MODULE._built_scanner_binary
_run_parallel_scanner = _SUPPORT_MODULE._run_parallel_scanner
_scanner_fixture_payload = _SUPPORT_MODULE._scanner_fixture_payload
_single_frame = _SUPPORT_MODULE._single_frame
_write_gzip_json = _SUPPORT_MODULE._write_gzip_json

_SCANNER_BUFFER_BYTES = 8 * 1024 * 1024


def _write_fake_rapidgzip(path: Path) -> None:
    path.write_text(
        f"""#!{sys.executable}
import gzip
import os
import pathlib
import sys
import time

arguments = sys.argv[1:]
input_path = pathlib.Path(arguments[-1])
with gzip.open(input_path, "rb") as artifact:
    payload = artifact.read()
if "--export-index" in arguments:
    index_path = pathlib.Path(arguments[arguments.index("--export-index") + 1])
    index_path.write_bytes(b"fake-index")
    sys.stdout.buffer.write(payload)
    raise SystemExit(0)
if "--ranges" not in arguments:
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()
    sys.stderr.write("late full-scan failure\\n")
    raise SystemExit(int(os.environ.get("FAKE_RAPIDGZIP_FULL_EXIT", "0")))
range_spec = arguments[arguments.index("--ranges") + 1]
range_payload = bytearray()
for item in range_spec.split(","):
    length, offset = (int(value) for value in item.split("@", 1))
    range_payload.extend(payload[offset : offset + length])
mutation = os.environ.get("FAKE_RAPIDGZIP_OBJECT_RANGE_MUTATION", "")
is_object_range = range_payload.lstrip().startswith(b"{{")
if is_object_range:
    expected_threads = os.environ.get("FAKE_RAPIDGZIP_EXPECT_OBJECT_RANGE_THREADS")
    actual_threads = arguments[arguments.index("-P") + 1]
    if expected_threads and actual_threads != expected_threads:
        sys.stderr.write(
            f"object range used {{actual_threads}} decoder threads, expected {{expected_threads}}\\n"
        )
        raise SystemExit(31)
    delay_seconds = float(
        os.environ.get("FAKE_RAPIDGZIP_DELAY_FIRST_OBJECT_RANGE_SECONDS", "0")
    )
    if delay_seconds > 0 and b'"billing_code":"99200"' in range_payload:
        time.sleep(delay_seconds)
    if mutation == "truncate" and range_payload:
        range_payload.pop()
    elif mutation == "extra":
        range_payload.extend(b"x")
sys.stdout.buffer.write(range_payload)
sys.stdout.buffer.flush()
sys.stderr.write("late indexed failure\\n")
raise SystemExit(int(os.environ.get("FAKE_RAPIDGZIP_RANGE_EXIT", "0")))
""",
        encoding="utf-8",
    )
    path.chmod(0o700)


def _mixed_inline_referenced_payload() -> dict:
    payload = _scanner_fixture_payload()
    for procedure_index, procedure in enumerate(payload["in_network"]):
        if procedure_index % 2 == 0:
            rate = procedure["negotiated_rates"][2]
            provider_id = rate.pop("provider_references")[0]
            rate["provider_groups"] = payload["provider_references"][provider_id - 1][
                "provider_groups"
            ]
    return payload


def _write_boundary_split_multimember_gzip(path: Path, fixture_payload_map: dict) -> None:
    provider_key = b'"provider_references"'
    base_payload_map = {
        "in_network": fixture_payload_map["in_network"][:1],
        "padding": "",
        "provider_references": fixture_payload_map["provider_references"],
    }
    base_json = json.dumps(base_payload_map, separators=(",", ":")).encode("utf-8")
    base_key_offset = base_json.rindex(provider_key)
    padding_bytes = _SCANNER_BUFFER_BYTES - 8 - base_key_offset
    base_payload_map["padding"] = "x" * padding_bytes
    expanded_json = json.dumps(base_payload_map, separators=(",", ":")).encode("utf-8")
    provider_key_offset = expanded_json.rindex(provider_key)
    assert provider_key_offset == _SCANNER_BUFFER_BYTES - 8
    member_split = provider_key_offset + 5
    path.write_bytes(
        gzip.compress(expanded_json[:member_split], mtime=0)
        + gzip.compress(expanded_json[member_split:], mtime=0)
    )


def test_scanner_indexes_reversed_top_level_arrays_for_parallel_workers(tmp_path):
    scanner_binary = _built_scanner_binary()
    rapidgzip_binary = shutil.which("rapidgzip")
    if rapidgzip_binary is None:
        pytest.skip("rapidgzip is not installed in this test environment")
    fixture_payload = _scanner_fixture_payload()
    normal_artifact = tmp_path / "normal-order.json.gz"
    _write_gzip_json(normal_artifact, fixture_payload)
    reversed_artifact = tmp_path / "after-order.json.gz"
    _write_gzip_json(
        reversed_artifact,
        {
            "in_network": fixture_payload["in_network"],
            "provider_references": fixture_payload["provider_references"],
        },
    )
    copy_kinds = ("price_atom", "provider_group_member")
    normal_scanner_run = _run_parallel_scanner(
        scanner_binary,
        normal_artifact,
        tmp_path / "normal-order",
        workers=8,
        copy_kinds=copy_kinds,
    )
    indexed_temp_dir = tmp_path / "indexed-tmp"
    indexed_temp_dir.mkdir()
    indexed_scanner_run = _run_parallel_scanner(
        scanner_binary,
        reversed_artifact,
        tmp_path / "after-order",
        workers=8,
        copy_kinds=copy_kinds,
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": rapidgzip_binary,
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
            "TMPDIR": str(indexed_temp_dir),
        },
    )

    scanner_config_frame = _single_frame(indexed_scanner_run, "scanner_config")
    scanner_summary_frame = _single_frame(indexed_scanner_run, "scanner_summary")
    assert scanner_config_frame["execution_mode"] == "parallel_top_level_bytes_indexed_reorder"
    assert scanner_config_frame["worker_count"] == 8
    assert scanner_config_frame["provider_reference_order"] == "after_in_network"
    assert scanner_config_frame["top_level_byte_scan_selected"] is True
    assert scanner_config_frame["top_level_byte_scan_fallback_reason"] is None
    assert scanner_config_frame["rapidgzip_index_bytes"] > 0
    assert scanner_config_frame["full_decompression_passes"] == 2
    assert scanner_config_frame["order_probe_partial_pass"] is True
    assert scanner_summary_frame["provider_reference_order"] == "after_in_network"
    assert len(scanner_summary_frame["workers"]) == 8
    assert sum(worker["rates_seen"] for worker in scanner_summary_frame["workers"]) == 24 * 8
    assert scanner_summary_frame["order_detection_compressed_bytes"] > 0
    assert indexed_scanner_run["copy_rows"] == normal_scanner_run["copy_rows"]
    assert indexed_scanner_run["serving_records"] == normal_scanner_run[
        "serving_records"
    ]
    assert list(indexed_temp_dir.glob("ptg2-rapidgzip-index-*")) == []


def test_indexed_range_producers_preserve_rows_and_digests(tmp_path):
    """Verify indexed range producers preserve rows and digests."""
    fixture_document = _mixed_inline_referenced_payload()
    artifact = tmp_path / "mixed-reversed.json.gz"
    _write_gzip_json(
        artifact,
        {
            "in_network": fixture_document["in_network"],
            "provider_references": fixture_document["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-ranges"
    _write_fake_rapidgzip(fake_rapidgzip)
    copy_kinds = ("price_atom", "provider_group_member")

    runs = {
        producer_count: _run_parallel_scanner(
            _built_scanner_binary(),
            artifact,
            tmp_path / f"range-producers-{producer_count}",
            workers=8,
            copy_kinds=copy_kinds,
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "4",
                "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": str(producer_count),
                "FAKE_RAPIDGZIP_EXPECT_OBJECT_RANGE_THREADS": str(
                    4 // producer_count
                ),
            },
        )
        for producer_count in (1, 4)
    }

    assert runs[4]["copy_rows"] == runs[1]["copy_rows"]
    assert runs[4]["copy_digests"] == runs[1]["copy_digests"]
    assert runs[4]["serving_records"] == runs[1]["serving_records"]
    assert {
        kind: len(copy_rows)
        for kind, copy_rows in runs[4]["copy_rows"].items()
    } == {
        "price_atom": 168,
        "provider_group_member": 64,
    }
    source_rate_occurrences = sum(
        len(procedure["negotiated_rates"])
        for procedure in fixture_document["in_network"]
    )
    assert source_rate_occurrences == 192
    assert len(runs[4]["serving_records"]) == source_rate_occurrences
    for producer_count, run in runs.items():
        config = _single_frame(run, "scanner_config")
        summary = _single_frame(run, "scanner_summary")
        assert config["indexed_range_producers_requested"] == producer_count
        assert config["indexed_range_producers_selected"] == producer_count
        assert config["indexed_range_decoder_threads_selected"] == 4
        assert config["indexed_range_count"] == producer_count
        assert len(config["indexed_ranges"]) == producer_count
        assert sum(
            range_info["decoder_threads"]
            for range_info in config["indexed_ranges"]
        ) == 4
        assert summary["indexed_range_producers_requested"] == producer_count
        assert summary["indexed_range_producers_selected"] == producer_count
        assert summary["indexed_range_decoder_threads_selected"] == 4
        assert len(summary["indexed_ranges"]) == producer_count
        assert sum(
            range_info["decoder_threads"]
            for range_info in summary["indexed_ranges"]
        ) == 4
        assert summary[
            "indexed_range_peer_cancellation_can_interrupt_external_read"
        ] is False
        assert "cannot interrupt" in summary["indexed_range_cancellation_limitation"]
        assert sum(
            range_info["object_count"] for range_info in summary["indexed_ranges"]
        ) == 24
        assert sum(
            range_info["rate_count"] for range_info in summary["indexed_ranges"]
        ) == 24 * 8
        assert sum(
            range_info["length"] for range_info in summary["indexed_ranges"]
        ) == summary[
            "indexed_range_coverage_bytes"
        ]
        assert summary["indexed_range_coverage_object_count"] == 24
        assert summary["indexed_range_completed_object_count"] == 24
        assert summary["indexed_range_completed_rate_count"] == 24 * 8
        assert summary["indexed_range_completed_raw_bytes"] > 0
        assert all(
            range_info["length"] > 0
            for range_info in summary["indexed_ranges"]
        )
        assert all(
            range_info["elapsed_seconds"] >= 0
            for range_info in summary["indexed_ranges"]
        )
        assert all(
            range_info["blocked_seconds"] >= 0
            for range_info in summary["indexed_ranges"]
        )
        assert all(
            left["offset"] + left["length"] < right["offset"]
            for left, right in zip(
                summary["indexed_ranges"], summary["indexed_ranges"][1:]
            )
        )


def test_delayed_indexed_range_emits_object_coverage_progress(tmp_path):
    """Verify delayed indexed range emits object coverage progress."""
    fixture_document = _mixed_inline_referenced_payload()
    artifact = tmp_path / "delayed-reversed.json.gz"
    _write_gzip_json(
        artifact,
        {
            "in_network": fixture_document["in_network"],
            "provider_references": fixture_document["provider_references"],
        },
    )
    fake_rapidgzip = tmp_path / "rapidgzip-delayed"
    _write_fake_rapidgzip(fake_rapidgzip)
    scanner_binary = _built_scanner_binary()
    scanner_completions = []
    real_subprocess_run = subprocess.run

    def capture_scanner_run(*args, **kwargs):
        completed = real_subprocess_run(*args, **kwargs)
        command = args[0] if args else kwargs.get("args", ())
        if "--compact-serving" in command:
            scanner_completions.append(completed)
        return completed

    with mock.patch.object(subprocess, "run", side_effect=capture_scanner_run):
        run = _run_parallel_scanner(
            scanner_binary,
            artifact,
            tmp_path / "delayed-output",
            workers=8,
            copy_kinds=("price_atom",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "4",
                "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "4",
                "FAKE_RAPIDGZIP_EXPECT_OBJECT_RANGE_THREADS": "1",
                "FAKE_RAPIDGZIP_DELAY_FIRST_OBJECT_RANGE_SECONDS": "1.0",
            },
        )

    assert _single_frame(run, "scanner_summary")[
        "indexed_range_decoder_threads_selected"
    ] == 4
    assert len(scanner_completions) == 1
    progress_payloads = []
    progress_lines = []
    for line in scanner_completions[0].stderr.decode("utf-8").splitlines():
        if not line.startswith("PTG2_SCANNER_PROGRESS\t"):
            continue
        progress_lines.append(line)
        fields = dict(
            field.split("=", 1) for field in line.split("\t")[1:] if "=" in field
        )
        if fields.get("progress_basis") == "indexed_objects":
            progress_payloads.append(fields)

    assert progress_lines
    assert all("progress_basis=indexed_objects" in line for line in progress_lines)
    intermediate = [
        progress_fields
        for progress_fields in progress_payloads
        if progress_fields["done"] == "false"
    ]
    assert intermediate
    assert all(
        progress_fields["indexed_objects_total"] == "24"
        for progress_fields in intermediate
    )
    assert any(
        0 < float(progress_fields["percent"]) < 100
        and 0 < int(progress_fields["indexed_objects_completed"]) < 24
        and progress_fields["eta_seconds"] != "unknown"
        for progress_fields in intermediate
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
