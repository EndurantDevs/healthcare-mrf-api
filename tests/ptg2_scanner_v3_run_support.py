# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib.util
import io
import json
import os
import struct
import subprocess
import sys
import types
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    write_v3_finalizer_input_manifest,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import (
    build_persisted_source_witness,
    decode_persisted_source_witness,
)

from tests.ptg2_scanner_v3_release_support import (
    _AUDIT_CANDIDATE_RECORD,
    _MIB,
    _SERVING_RECORD,
    _STRICT_SCANNER_FRAME_KINDS,
    _SUPPORT_MODULE,
    _built_scanner_binary,
    _decode_by_code_groups,
    _fixture_network_entries,
    _fixture_payload,
    _fixture_provider_references,
    _load_isolated_rust_scanner,
    _load_isolated_shared_blocks,
    _network_rate_fixture,
    _parse_scanner_frames,
    _pg_binary_copy_rows,
    _read_pg_binary_rows,
    _read_uvarint,
    _single_frame,
    _v3_finalizer_test_resource_args,
)

@dataclass(frozen=True)
class _ScannerRunOptions:
    arch: str
    provider_references_first: bool
    grouped: bool
    multiple_prices: bool = False
    duplicate_first_price: bool = False
    repeated_rate_occurrences: bool = False
    fixture_payload: dict | None = None
    top_level_byte_scan: bool = True
    input_artifact: Path | None = None


@dataclass(frozen=True)
class _ScannerRunPaths:
    compact_copy: Path
    lean_copy: Path
    price_atom_copy: Path
    price_set_atom_copy: Path
    price_set_summary_copy: Path
    provider_group_member_copy: Path
    provider_set_metadata_copy: Path
    provider_forward: Path
    provider_inverted: Path
    serving_run_directory: Path
    source_witness_scratch_directory: Path


def _scanner_run_paths(run_directory: Path) -> _ScannerRunPaths:
    serving_run_directory = run_directory / "serving-runs"
    source_witness_scratch_directory = serving_run_directory / "source-witness-scratch"
    source_witness_scratch_directory.mkdir(parents=True)
    return _ScannerRunPaths(
        compact_copy=run_directory / "compact.copy",
        lean_copy=run_directory / "manifest-lean.copy",
        price_atom_copy=run_directory / "manifest-price-atom.copy",
        price_set_atom_copy=run_directory / "manifest-price-set-atom.copy",
        price_set_summary_copy=run_directory / "manifest-price-set-summary.copy",
        provider_group_member_copy=run_directory / "provider-group-member.copy",
        provider_set_metadata_copy=run_directory / "provider-set-metadata.copy",
        provider_forward=run_directory / "provider-forward.sidecar",
        provider_inverted=run_directory / "provider-inverted.sidecar",
        serving_run_directory=serving_run_directory,
        source_witness_scratch_directory=source_witness_scratch_directory,
    )


def _scanner_fixture_artifact(
    run_directory: Path,
    options: _ScannerRunOptions,
) -> Path:
    artifact = (
        Path(options.input_artifact).resolve()
        if options.input_artifact is not None
        else run_directory / "input.json"
    )
    # Keep the default scanner parity fixture one-record wide; the PostgreSQL
    # publication smoke opts into multiple dense price keys.
    if options.input_artifact is None:
        source_document = (
            options.fixture_payload
            if options.fixture_payload is not None
            else _fixture_payload(
                provider_references_first=options.provider_references_first,
                multiple_prices=options.multiple_prices,
                duplicate_first_price=options.duplicate_first_price,
                repeated_rate_occurrences=options.repeated_rate_occurrences,
            )
        )
        artifact.write_text(
            json.dumps(source_document, separators=(",", ":")),
            encoding="utf-8",
        )
    else:
        assert options.fixture_payload is None
    return artifact


def _scanner_output_environment(paths: _ScannerRunPaths) -> dict[str, str]:
    return {
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(paths.compact_copy),
        "HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH": str(paths.lean_copy),
        "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(paths.price_atom_copy),
        "HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH": str(
            paths.price_set_atom_copy
        ),
        "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH": str(
            paths.price_set_summary_copy
        ),
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(
            paths.provider_group_member_copy
        ),
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH": str(
            paths.provider_set_metadata_copy
        ),
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH": str(
            paths.provider_forward
        ),
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH": str(
            paths.provider_inverted
        ),
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR": str(paths.serving_run_directory),
        "HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR": str(
            paths.source_witness_scratch_directory
        ),
    }


def _scanner_execution_environment(
    artifact: Path,
    options: _ScannerRunOptions,
) -> dict[str, str]:
    return {
        "HLTHPRT_PTG2_SNAPSHOT_ARCH": options.arch,
        "HLTHPRT_PTG2_RAW_SOURCE_SHA256": hashlib.sha256(
            artifact.read_bytes()
        ).hexdigest(),
        "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID": (b"\xcc" * 32).hex(),
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot-v3-runs",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan-v3-runs",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month-v3-runs",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "trace-v3-runs",
        "HLTHPRT_PTG2_MANIFEST_ONLY": "true",
        "HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS": "4",
        "HLTHPRT_PTG2_V3_SERVING_RUN_PARTITION_BUFFER_BYTES": "52",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE": "8",
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN": (
            "true" if options.top_level_byte_scan else "false"
        ),
        "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS": "true",
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS": (
            "true" if options.grouped else "false"
        ),
        "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "false",
    }


def _scanner_environment(
    artifact: Path,
    paths: _ScannerRunPaths,
    options: _ScannerRunOptions,
) -> dict[str, str]:
    scanner_environment_map = dict(os.environ)
    for output_env in (
        *_SUPPORT_MODULE.COPY_ENV_BY_KIND.values(),
        *_SUPPORT_MODULE.SIDECAR_ENV_BY_KIND.values(),
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR",
    ):
        scanner_environment_map.pop(output_env, None)
    scanner_environment_map.update(_scanner_output_environment(paths))
    scanner_environment_map.update(_scanner_execution_environment(artifact, options))
    return scanner_environment_map


def _scanner_frames_of_kind(frames: list[tuple[str, dict]], kind: str) -> list[dict]:
    return [frame_payload for frame_kind, frame_payload in frames if frame_kind == kind]


def _scanner_result(
    artifact: Path,
    paths: _ScannerRunPaths,
    frames: list[tuple[str, dict]],
) -> dict[str, Any]:
    frame_lists_by_key = {
        key: _scanner_frames_of_kind(frames, kind)
        for key, kind in (
            ("partition_frames", "v3_serving_run_partition_file"),
            ("code_dictionary_frames", "v3_serving_code_dictionary_file"),
            ("price_atom_frames", "manifest_price_atom_copy_file"),
            ("price_set_atom_frames", "manifest_price_set_atom_copy_file"),
            ("price_set_summary_frames", "manifest_price_set_summary_copy_file"),
            ("provider_group_member_frames", "manifest_provider_group_member_copy_file"),
            (
                "provider_set_metadata_frames",
                "manifest_provider_set_dictionary_copy_file",
            ),
        )
    }
    partition_bytes = b"".join(
        Path(frame["path"]).read_bytes()
        for frame in sorted(
            frame_lists_by_key["partition_frames"],
            key=lambda frame: (frame["partition"], frame["path"]),
        )
    )
    return {
        "artifact": artifact,
        "frames": frames,
        "compact_copy_path": paths.compact_copy,
        "lean_copy_path": paths.lean_copy,
        "price_atom_copy_path": paths.price_atom_copy,
        "price_set_atom_copy_path": paths.price_set_atom_copy,
        "price_set_summary_copy_path": paths.price_set_summary_copy,
        "provider_group_member_copy_path": paths.provider_group_member_copy,
        "provider_set_metadata_copy_path": paths.provider_set_metadata_copy,
        "provider_forward_path": paths.provider_forward,
        "provider_inverted_path": paths.provider_inverted,
        **frame_lists_by_key,
        "partition_bytes": partition_bytes,
    }


def _run_scanner(
    scanner_binary: Path,
    tmp_path: Path,
    label: str,
    **option_values: Any,
) -> dict:
    """Support the run scanner test fixture."""
    options = _ScannerRunOptions(**option_values)
    run_directory = tmp_path / label
    run_directory.mkdir()
    paths = _scanner_run_paths(run_directory)
    artifact = _scanner_fixture_artifact(run_directory, options)
    completed = subprocess.run(
        [str(scanner_binary), "--compact-serving", str(artifact)],
        check=True,
        env=_scanner_environment(artifact, paths, options),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    return _scanner_result(artifact, paths, _parse_scanner_frames(completed.stdout))


def _malformed_provider_identifier_payload(
    *, provider_references_first: bool
) -> dict:
    source_document = _fixture_payload(
        provider_references_first=provider_references_first
    )
    source_document["provider_references"][0]["provider_groups"][0]["npi"] = [
        1234567890,
        123456789,
        123456789,
    ]
    source_document["in_network"][0]["negotiated_rates"].append(
        {
            "provider_groups": [
                {
                    "npi": [1234567891, 123456787],
                    "tin": {"type": "ein", "value": "12-3456789"},
                }
            ],
            "negotiated_prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 126,
                    "service_code": ["11"],
                    "billing_class": "professional",
                }
            ],
        }
    )
    return source_document

def _assert_scanner_execution_mode_contracts(scanner_runs_by_mode: dict) -> None:
    baseline = scanner_runs_by_mode["worker_ungrouped"]["partition_bytes"]
    assert len(baseline) == _SERVING_RECORD.size
    assert scanner_runs_by_mode["late_reordered"]["partition_bytes"] == baseline
    assert _SERVING_RECORD.unpack(baseline)[3] == 2

    assert _single_frame(
        scanner_runs_by_mode["worker_ungrouped"]["frames"], "scanner_config"
    )["execution_mode"] == ("parallel_top_level_bytes")
    assert (
        _single_frame(
            scanner_runs_by_mode["late_reordered"]["frames"], "scanner_config"
        )["execution_mode"]
        == "parallel_top_level_bytes_plain_range_reorder"
    )
    late_config = _single_frame(
        scanner_runs_by_mode["late_reordered"]["frames"], "scanner_config"
    )
    assert late_config["provider_reference_order"] == "after_in_network"
    assert late_config["plain_range_reorder"] is True
    assert late_config["plain_provider_range_bytes"] > 0
    assert late_config["plain_in_network_range_bytes"] > 0
    assert late_config["plain_in_network_object_count"] == 1
    assert late_config["order_probe_partial_pass"] is True


def _assert_scanner_source_witness(run: dict) -> None:
    source_digest = hashlib.sha256(run["artifact"].read_bytes()).hexdigest()
    witness_entry = _single_frame(run["frames"], "source_audit_witness_file")
    witness_payload, metadata = build_persisted_source_witness(
        [witness_entry],
        expected_raw_source_sha256=[source_digest],
    )
    loaded = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[source_digest],
        expected_metadata=metadata,
    )
    assert len(loaded.occurrence_records) == 2
    assert len(loaded.provider_records) == 1


def _assert_scanner_publication_files(run: dict) -> None:
    assert not run["compact_copy_path"].exists()
    assert not run["lean_copy_path"].exists()
    assert not any(
        kind == "manifest_lean_serving_copy_file" for kind, _payload in run["frames"]
    )
    assert not any(
        kind in {"procedure", "provider_set", "serving_rate_compact"}
        for kind, _payload in run["frames"]
    )
    assert sum(
        frame["row_count"] for frame in run["provider_group_member_frames"]
    ) == 2
    assert all(
        Path(frame["path"]).exists() for frame in run["provider_group_member_frames"]
    )
    assert sum(frame["row_count"] for frame in run["price_set_summary_frames"]) == 1
    summary_rows = b"".join(
        Path(frame["path"]).read_bytes()
        for frame in run["price_set_summary_frames"]
    ).splitlines()
    assert len(summary_rows) == 1
    assert summary_rows[0].rsplit(b"\t", 1)[1] == b"125.5"
    assert run["provider_forward_path"].exists()
    assert run["provider_inverted_path"].exists()


def _assert_scanner_partition_contract(run: dict) -> None:
    partition_frame = run["partition_frames"][0]
    assert partition_frame["format"] == "ptg2_v3_serving_run"
    assert partition_frame["version"] == 1
    assert partition_frame["partition_count"] == 4
    assert partition_frame["row_count"] == 1
    assert partition_frame["bytes"] == _SERVING_RECORD.size
    assert Path(partition_frame["path"]).name.endswith(".ready")
    summary = _single_frame(run["frames"], "scanner_summary")
    config = _single_frame(run["frames"], "scanner_config")
    assert config["snapshot_arch"] == "postgres_binary_v3"
    assert config["storage_generation"] == "shared_blocks_v3"
    assert config["serving_row_semantics"] == "source_multiset_v1"
    assert config["group_negotiated_rate_chunks"] is False
    assert summary["serving_run_files"] == 1
    assert summary["serving_run_rows"] == 1
    assert summary["serving_run_bytes"] == _SERVING_RECORD.size
    assert len(run["code_dictionary_frames"]) == 1
    assert (
        run["code_dictionary_frames"][0]["format"]
        == "ptg2_v3_serving_code_dictionary"
    )


def _assert_strict_scanner_run(run: dict) -> None:
    frame_kinds = {kind for kind, _payload in run["frames"]}
    assert frame_kinds - {"dedupe_summary"} == (
        _STRICT_SCANNER_FRAME_KINDS - {"dedupe_summary"}
    )
    _assert_scanner_source_witness(run)
    _assert_scanner_publication_files(run)
    _assert_scanner_partition_contract(run)
