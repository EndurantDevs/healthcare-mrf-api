# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

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
for item in range_spec.split(","):
    length, offset = (int(value) for value in item.split("@", 1))
    sys.stdout.buffer.write(payload[offset : offset + length])
sys.stdout.buffer.flush()
sys.stderr.write("late indexed failure\\n")
raise SystemExit(int(os.environ.get("FAKE_RAPIDGZIP_RANGE_EXIT", "0")))
""",
        encoding="utf-8",
    )
    path.chmod(0o700)


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
    copy_kinds = ("compact", "provider_group_member")
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
    assert list(indexed_temp_dir.glob("ptg2-rapidgzip-index-*")) == []


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
        copy_kinds=("compact",),
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
    assert indexed_scanner_run["copy_rows"]["compact"]


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

    scanner_run = _run_parallel_scanner(
        _built_scanner_binary(),
        reversed_artifact,
        tmp_path / "bounded-events-output",
        workers=16,
        copy_kinds=(
            "compact",
            "manifest_lean_serving",
            "manifest_price_atom",
            "manifest_price_set_atom",
        ),
        env_overrides={
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
            "HLTHPRT_PTG2_RUST_EVENT_QUEUE": "1",
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES": "1",
        },
    )

    scanner_summary_frame = _single_frame(scanner_run, "scanner_summary")
    assert sum(worker["rates_seen"] for worker in scanner_summary_frame["workers"]) == 24 * 8


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
            copy_kinds=("compact",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_INDEX_THREADS": "2",
                "FAKE_RAPIDGZIP_RANGE_EXIT": "23",
            },
        )

    assert b"late indexed failure" in error_info.value.stderr


@pytest.mark.parametrize("worker_parse_setting", ["true", "false"])
def test_scanner_rejects_late_full_scan_process_failure(tmp_path, worker_parse_setting):
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
            copy_kinds=("compact",),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(fake_rapidgzip),
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
                "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": worker_parse_setting,
                "FAKE_RAPIDGZIP_FULL_EXIT": "23",
            },
        )

    assert b"late full-scan failure" in error_info.value.stderr
