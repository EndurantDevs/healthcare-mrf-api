from __future__ import annotations

import asyncio

import hashlib

import io

import json

import os

import signal

from pathlib import Path

from types import SimpleNamespace

import pytest

from process.ptg_parts import rust_scanner

from process.ptg_parts.ptg2_source_witness_contract import WitnessPayloadLimitError


_SHARED_GRAPH_DIRECTION_SPECS = (
    (1, "graph_npi_groups_v1", 4),
    (2, "graph_group_npis_v1", 8),
    (3, "graph_group_provider_sets_v1", 4),
    (4, "graph_provider_set_groups_v1", 4),
)


_COMPACT_OPTIONAL_PATH_NAMES = (
    "compact_copy_path",
    "procedure_copy_path",
    "price_code_set_copy_path",
    "price_atom_copy_path",
    "price_set_entry_copy_path",
    "provider_set_copy_path",
    "provider_group_member_copy_path",
    "provider_set_component_copy_path",
    "provider_set_entry_copy_path",
    "provider_entry_component_copy_path",
    "manifest_serving_copy_path",
    "manifest_lean_serving_copy_path",
    "manifest_provider_forward_sidecar_path",
    "manifest_provider_inverted_sidecar_path",
    "manifest_provider_set_component_sidecar_path",
    "manifest_provider_component_group_sidecar_path",
    "manifest_provider_npi_sidecar_path",
    "manifest_price_forward_sidecar_path",
    "manifest_price_atom_copy_path",
    "manifest_price_set_atom_copy_path",
    "manifest_price_set_summary_copy_path",
    "manifest_provider_group_member_copy_path",
    "manifest_code_count_copy_path",
    "manifest_provider_set_dictionary_copy_path",
)


def _frame(kind: str, payload: object, *, trailer: bytes = b"\n") -> bytes:
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    return kind.encode() + b"\t" + str(len(encoded)).encode() + b"\n" + encoded + trailer


class _Process:
    def __init__(
        self,
        stdout: bytes | None,
        *,
        stderr: bytes | None = b"",
        returncode: int | None = 0,
    ) -> None:
        self.stdout = None if stdout is None else io.BytesIO(stdout)
        self.stderr = None if stderr is None else io.BytesIO(stderr)
        self.returncode = returncode
        self.pid = 424242
        self.wait_count = 0
        self.signals: list[int] = []
        self.killed = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        del timeout
        self.wait_count += 1
        return self.returncode if self.returncode is not None else 0

    def send_signal(self, signal_number):
        self.signals.append(signal_number)

    def kill(self):
        self.killed = True
        self.returncode = -9


def _binary(tmp_path: Path) -> Path:
    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    return binary


def _config(*, factors: bool = False) -> dict[str, object]:
    return {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": rust_scanner.PTG2_V3_SHARED_GENERATION,
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": rust_scanner._V3_SERVING_RUN_FORMAT,
        "serving_run_version": rust_scanner._V3_SERVING_RUN_VERSION,
        "factor_mode": factors,
        "provider_graph_v4_factor_mode": factors,
    }


def _compact_kwargs(tmp_path: Path) -> dict[str, object]:
    return {
        "raw_source_sha256": "a" * 64,
        "snapshot_id": "snapshot",
        "plan_id": "plan",
        "coverage_scope_id": "b" * 64,
        "plan_month_id": "month",
        "source_trace_set_hash": "trace",
        "v3_serving_run_directory": tmp_path / "runs",
    }


def _factor_frame_process() -> _Process:
    normalization_digest = hashlib.sha256()
    normalization_digest.update(
        rust_scanner._V4_EMPTY_NPI_NORMALIZATION_HASH_DOMAIN
    )
    normalization_digest.update((0).to_bytes(8, "big"))
    partition_entry_by_field = {
        "path": "partition.bin",
        "partition": 0,
        "partition_count": 1,
        "row_count": 1,
        "bytes": 1,
        "format": rust_scanner._V3_SERVING_RUN_FORMAT,
        "version": rust_scanner._V3_SERVING_RUN_VERSION,
        "sha256": "a" * 64,
    }
    dictionary_entry_by_field = {
        "path": "dictionary.bin",
        "row_count": 1,
        "bytes": 1,
        "format": rust_scanner._V3_CODE_DICTIONARY_FORMAT,
        "version": rust_scanner._V3_CODE_DICTIONARY_VERSION,
        "sha256": "b" * 64,
    }
    stdout = b"".join(
        (
            _frame("scanner_config", _config(factors=True)),
            _frame("v3_serving_run_partition_file", partition_entry_by_field),
            _frame("v3_serving_code_dictionary_file", dictionary_entry_by_field),
            _frame(
                "scanner_summary",
                {
                    "factor_mode": True,
                    "provider_graph_v4_factor_mode": True,
                    "execution_mode": "fast",
                    "empty_npi_tin_only_normalization": {
                        "contract": (
                            rust_scanner._V4_EMPTY_NPI_NORMALIZATION_CONTRACT
                        ),
                        "source_shape": "empty_array",
                        "canonical_equivalent": "zero_marker",
                        "occurrence_count": 0,
                        "emitted_npi_edge_count": 0,
                        "sha256": normalization_digest.hexdigest(),
                    },
                },
            ),
        )
    )
    return _Process(stdout, returncode=0)


def _valid_shared_graph_summary(tmp_path: Path):
    """Build a valid on-disk shared-graph summary for mutation-based tests."""
    output = tmp_path / "graph-output"
    output.mkdir()
    for output_name in rust_scanner._V3_SHARED_GRAPH_OUTPUT_NAMES.values():
        (output / output_name).write_bytes(b"x")
    summary_by_field = {
        "format": rust_scanner._V3_SHARED_GRAPH_SUMMARY_FORMAT,
        "scratch_directory": str(output.resolve()),
        "output_directory": str(output.resolve()),
        **{
            field_name: str((output / output_name).resolve())
            for field_name, output_name in rust_scanner._V3_SHARED_GRAPH_OUTPUT_NAMES.items()
        },
        "block_count": 4,
        "owner_count": 4,
        "provider_group_count": 1,
        "npi_count": 1,
        "support_digest": "ab" * 32,
        "direction_metrics": [
            {
                "direction": direction,
                "object_kind": object_kind,
                "member_width": width,
                "owner_count": 1,
                "member_count": 1,
                "empty_owner_count": 0,
                "block_count": 1,
                "raw_byte_count": width,
            }
            for direction, object_kind, width in _SHARED_GRAPH_DIRECTION_SPECS
        ],
        "edge_metrics": [
            {
                "edge_kind": edge_kind,
                "input_edge_count": 1,
                "unique_edge_count": 1,
                "duplicate_edge_count": 0,
            }
            for edge_kind in ("group_npi", "group_provider_set")
        ],
        "input_byte_count": 4,
        "raw_block_byte_count": 20,
        "stored_block_byte_count": 20,
        "integrity": {
            "shard_count": 1,
            "artifact_count": 4,
            "checksum_byte_count": 4,
            "reciprocal_pair_count": 2,
            "reciprocal_edge_count": 2,
            "input_edge_count": 2,
            "unique_edge_count": 2,
            "duplicate_edge_count": 0,
        },
    }
    return output, summary_by_field, rust_scanner._SharedGraphExpected(1, 4, 1, 1)
