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

def _bridge_scanner_frames(tmp_path: Path, monkeypatch) -> tuple[list, Path]:
    scanner_binary = _built_scanner_binary()
    artifact = tmp_path / "bridge-input.json"
    artifact.write_text(
        json.dumps(_fixture_payload(provider_references_first=True), separators=(",", ":")),
        encoding="utf-8",
    )
    rust_scanner = _load_isolated_rust_scanner(monkeypatch)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: scanner_binary)
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
    lean_copy_path = tmp_path / "bridge-manifest-lean.copy"
    serving_run_directory = tmp_path / "bridge-serving-runs"

    scanner_frames = list(
        rust_scanner._iter_compact_serving_records_rust(
            artifact,
            raw_source_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
            snapshot_id="snapshot-bridge-v3",
            plan_id="plan-v3-runs",
            plan_month_id="plan-month-bridge-v3",
            coverage_scope_id=(b"\xcc" * 32).hex(),
            source_trace_set_hash="trace-bridge-v3",
            manifest_lean_serving_copy_path=lean_copy_path,
            v3_serving_run_directory=serving_run_directory,
            manifest_only=True,
        )
    )
    return scanner_frames, lean_copy_path


def _summary_file_entries(
    scanner_frames: list[tuple[str, dict]],
    frame_kind: str,
    keys: tuple[str, ...],
) -> list[dict]:
    return [
        {key: frame_payload[key] for key in keys}
        for kind, frame_payload in scanner_frames
        if kind == frame_kind
    ]


def _assert_bridge_source_run_contracts(summary: dict, config: dict) -> None:
    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "a" * 64,
    }
    contracted = attach_v3_source_run_contract(
        summary["serving_run_partition_files"],
        source_identity=source_identity_map,
        scanner_summary=summary,
        scanner_config=config,
    )
    contract = contracted[0]["source_run_contract"]
    assert contract["file_count"] == summary["serving_run_files"]
    assert contract["row_count"] == summary["serving_run_rows"]
    assert contract["byte_count"] == summary["serving_run_bytes"]
    assert len(contract["partition_rows"]) == config["serving_run_partition_count"]
    assert sum(contract["partition_rows"]) == summary["serving_run_rows"]
    dictionaries = attach_v3_dictionary_contract(
        summary["serving_run_code_dictionary_files"],
        source_identity=source_identity_map,
        source_run_contract_sha256=contracted[0]["source_run_contract_sha256"],
        scanner_summary=summary,
    )
    dictionary_contract = dictionaries[0]["code_dictionary_source_contract"]
    assert dictionary_contract["file_count"] == summary[
        "serving_code_dictionary_files"
    ]
    assert dictionary_contract["row_count"] == summary[
        "serving_code_dictionary_rows"
    ]
    assert dictionary_contract["byte_count"] == summary[
        "serving_code_dictionary_bytes"
    ]
    assert dictionary_contract["files"] == sorted(
        dictionary_contract["files"],
        key=lambda value: (value["sha256"], value["row_count"], value["bytes"]),
    )


def test_python_bridge_collects_partition_paths_in_scanner_summary(tmp_path, monkeypatch):
    """Verify python bridge collects partition paths in scanner summary."""
    scanner_frames, lean_copy_path = _bridge_scanner_frames(tmp_path, monkeypatch)
    summary = _single_frame(scanner_frames, "scanner_summary")
    config = _single_frame(scanner_frames, "scanner_config")
    assert summary["serving_run_partition_files"]
    assert summary["serving_run_partition_files"] == _summary_file_entries(
        scanner_frames,
        "v3_serving_run_partition_file",
        (
            "path",
            "partition",
            "partition_count",
            "row_count",
            "bytes",
            "format",
            "version",
            "sha256",
        ),
    )
    assert summary["serving_run_code_dictionary_files"] == _summary_file_entries(
        scanner_frames,
        "v3_serving_code_dictionary_file",
        ("path", "row_count", "bytes", "format", "version", "sha256"),
    )
    for entry in (
        summary["serving_run_partition_files"]
        + summary["serving_run_code_dictionary_files"]
    ):
        assert entry["sha256"] == hashlib.sha256(
            Path(entry["path"]).read_bytes()
        ).hexdigest()
    _assert_bridge_source_run_contracts(summary, config)
    assert not lean_copy_path.exists()
