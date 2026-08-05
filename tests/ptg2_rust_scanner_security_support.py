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
    "manifest_provider_group_tax_identity_sidecar_path",
    "manifest_provider_group_tax_identity_v2_sidecar_path",
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


def _tax_identity_v1_frame_payload() -> dict[str, object]:
    """Return exact token-only v1 metadata without source identity material."""

    policy_id = "ptg-tin-hmac-sha256-v1:python-bridge"
    row_count = 2
    return {
        "path": "tax-identity-v1.ptg2tax",
        "bytes": 13 + len(policy_id.encode("ascii")) + row_count * 65,
        "row_count": row_count,
        "provider_group_count": row_count,
        "matched_ein_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 1,
        "format": "ptg2_provider_group_tax_identity_v1",
        "version": 1,
        "record_bytes": 65,
        "token_policy_id": policy_id,
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "sha256": "a" * 64,
        "final": True,
    }


def _tax_identity_v2_frame_payload() -> dict[str, object]:
    """Return exact token-only v2 metadata without source identity material."""

    policy_id = "ptg-tin-hmac-sha256-v1:python-bridge"
    row_count = 2
    return {
        "path": "tax-identity-v2.ptg2tax",
        "bytes": 13 + len(policy_id.encode("ascii")) + row_count * 65,
        "row_count": row_count,
        "provider_group_count": row_count,
        "matched_ein_count": 1,
        "matched_npi_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "format": "ptg2_provider_group_tax_identity_v2",
        "version": 2,
        "record_bytes": 65,
        "token_policy_id": policy_id,
        "normalization_contract": (
            "ein_ascii_digits_or_2_7_hyphen_and_npi_10_ascii_digits_"
            "cms_80840_luhn_v2"
        ),
        "token_message_contract": (
            "healthporta_ptg_tin_v1_nul_u16be_type_length_type_"
            "u16be_value_length_value"
        ),
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "tin_id_128_contract": "first_16_bytes(tin_hmac_sha256)",
        "full_hmac_authority_contract": (
            "tin_hmac_sha256_full_32_bytes_authoritative"
        ),
        "sha256": "b" * 64,
        "final": True,
    }


def _assert_v2_bridge_contract(
    observed_records: list[tuple[str, dict[str, object]]],
    observed_environment_by_name: dict[str, str],
    scanner_options_by_name: dict[str, object],
) -> None:
    """Prove explicit v2 coordinates and raw-free event metadata survive the bridge."""

    assert [kind for kind, _event_by_field in observed_records] == [
        "scanner_config",
        "v3_serving_run_partition_file",
        "v3_serving_code_dictionary_file",
        "manifest_provider_group_tax_identity_sidecar_file",
        "manifest_provider_group_tax_identity_v2_sidecar_file",
        "scanner_summary",
    ]
    summary_by_field = observed_records[-1][1]
    assert summary_by_field["serving_run_partition_files"][0]["path"] == "partition.bin"
    assert (
        summary_by_field["serving_run_code_dictionary_files"][0]["path"]
        == "dictionary.bin"
    )
    assert observed_environment_by_name[
        rust_scanner._PROVIDER_GRAPH_V4_FACTORS_ENV
    ] == "true"
    assert observed_environment_by_name["HLTHPRT_PTG2_MANIFEST_ONLY"] == "true"
    assert "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH" in (
        observed_environment_by_name
    )
    assert "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_SIDECAR_PATH" in (
        observed_environment_by_name
    )
    explicit_v2_path = scanner_options_by_name[
        "manifest_provider_group_tax_identity_v2_sidecar_path"
    ]
    assert observed_environment_by_name[
        rust_scanner._PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH_ENV
    ] == str(explicit_v2_path)
    tax_identity_records = observed_records[3:5]
    assert tax_identity_records == [
        (
            "manifest_provider_group_tax_identity_sidecar_file",
            _tax_identity_v1_frame_payload(),
        ),
        (
            "manifest_provider_group_tax_identity_v2_sidecar_file",
            _tax_identity_v2_frame_payload(),
        ),
    ]
    for _record_kind, event_by_field in tax_identity_records:
        assert {"tin", "value", "masked_tin", "business_name"}.isdisjoint(
            event_by_field
        )
        serialized_event = json.dumps(event_by_field, sort_keys=True)
        assert "12-3456789" not in serialized_event
        assert "1000000491" not in serialized_event
        assert "Synthetic Billing Organization" not in serialized_event


def _assert_v2_rejected_before_spawn(tmp_path: Path, monkeypatch) -> None:
    """Exercise real iterator admission while making any process spawn fatal."""

    monkeypatch.setattr(
        rust_scanner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("invalid v2 admission must not spawn"),
    )
    with pytest.raises(RuntimeError, match="v2 output requires the v1"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
                manifest_provider_set_component_sidecar_path=(
                    tmp_path / "set-component.ptg2sc"
                ),
                manifest_provider_component_group_sidecar_path=(
                    tmp_path / "component-group.ptg2sc"
                ),
                manifest_provider_group_tax_identity_v2_sidecar_path=(
                    tmp_path / "tax-identity-v2.ptg2tax"
                ),
            )
        )


def _assert_ambient_v2_path_scrubbed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove a poisoned ambient v2 coordinate cannot activate the writer."""

    binary = _binary(tmp_path)
    stdout = b"".join(
        (_frame("scanner_config", _config()), _frame("scanner_summary", {}))
    )
    process = _Process(stdout)
    observed_environment_by_name: dict[str, str] = {}

    def popen(*_args, **kwargs):
        observed_environment_by_name.update(kwargs["env"])
        return process

    ambient_v2_path = tmp_path / "ambient-v2-output-must-not-run"
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", popen)
    monkeypatch.delenv(rust_scanner._PROVIDER_GRAPH_V4_ENV, raising=False)
    monkeypatch.setenv(
        rust_scanner._PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH_ENV,
        str(ambient_v2_path),
    )
    observed_records = list(
        rust_scanner._iter_compact_serving_records_rust(
            tmp_path / "input",
            **_compact_kwargs(tmp_path),
        )
    )
    assert observed_records == [
        ("scanner_config", _config()),
        (
            "scanner_summary",
            {
                "serving_run_partition_files": [],
                "serving_run_code_dictionary_files": [],
            },
        ),
    ]
    assert rust_scanner._PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH_ENV not in (
        observed_environment_by_name
    )
    assert str(ambient_v2_path) not in observed_environment_by_name.values()


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
                "manifest_provider_group_tax_identity_sidecar_file",
                _tax_identity_v1_frame_payload(),
            ),
            _frame(
                "manifest_provider_group_tax_identity_v2_sidecar_file",
                _tax_identity_v2_frame_payload(),
            ),
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
