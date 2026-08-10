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
from tests.ptg2_scanner_v3_run_support import _run_scanner

@dataclass(frozen=True)
class _DirectFinalizerFixture:
    scanner_binary: Path
    scan: dict[str, Any]
    manifest_path: Path
    membership_input: Path
    atom_input: Path
    price_key_map_input: Path
    output_directory: Path
    price_set_ids: tuple[bytes, ...]
    environment: dict[str, str]


def _write_direct_finalizer_manifest(tmp_path: Path, scan: dict[str, Any]) -> Path:
    manifest_path = tmp_path / "scanner-summary.json"
    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "d" * 64,
    }
    scanner_summary = _single_frame(scan["frames"], "scanner_summary")
    scanner_config = _single_frame(scan["frames"], "scanner_config")
    serving_runs = attach_v3_source_run_contract(
        scan["partition_frames"],
        source_identity=source_identity_map,
        scanner_summary=scanner_summary,
        scanner_config=scanner_config,
    )
    dictionaries = attach_v3_dictionary_contract(
        scan["code_dictionary_frames"],
        source_identity=source_identity_map,
        source_run_contract_sha256=serving_runs[0]["source_run_contract_sha256"],
        scanner_summary=scanner_summary,
    )
    provider_metadata_entries = [
        {
            **entry,
            **source_identity_map,
            "sha256": hashlib.sha256(Path(entry["path"]).read_bytes()).hexdigest(),
            "format": "ptg2_v3_provider_set_metadata_copy",
            "version": 1,
            "source_run_contract_sha256": serving_runs[0][
                "source_run_contract_sha256"
            ],
        }
        for entry in scan["provider_set_metadata_frames"]
    ]
    write_v3_finalizer_input_manifest(
        manifest_path,
        serving_run_entries=serving_runs,
        code_dictionary_entries=dictionaries,
        provider_set_metadata_entries=provider_metadata_entries,
        expected_source_identities=[source_identity_map],
    )
    return manifest_path


def _prepare_direct_finalizer_fixture(tmp_path: Path) -> _DirectFinalizerFixture:
    scanner_binary = _built_scanner_binary()
    scan = _run_scanner(
        scanner_binary,
        tmp_path,
        "finalizer-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
        repeated_rate_occurrences=True,
    )
    membership_input = tmp_path / "future-memberships.copy"
    atom_input = tmp_path / "future-atoms.copy"
    price_key_map_input = tmp_path / "price-key-map.copy"
    membership_input.write_bytes(b"")
    atom_input.write_bytes(b"")
    assert len(scan["partition_bytes"]) % _SERVING_RECORD.size == 0
    price_set_ids = tuple(
        sorted(
            {
                scan["partition_bytes"][offset + 32 : offset + 48]
                for offset in range(
                    0,
                    len(scan["partition_bytes"]),
                    _SERVING_RECORD.size,
                )
            }
        )
    )
    price_key_map_input.write_bytes(
        _pg_binary_copy_rows(
            [
                [price_set_id, struct.pack(">q", price_key)]
                for price_key, price_set_id in enumerate(price_set_ids)
            ]
        )
    )
    environment_map = {
        **os.environ,
        "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION": "none",
        "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES": "65536",
    }
    return _DirectFinalizerFixture(
        scanner_binary=scanner_binary,
        scan=scan,
        manifest_path=_write_direct_finalizer_manifest(tmp_path, scan),
        membership_input=membership_input,
        atom_input=atom_input,
        price_key_map_input=price_key_map_input,
        output_directory=tmp_path / "finalized",
        price_set_ids=price_set_ids,
        environment=environment_map,
    )


def _run_direct_finalizer(
    fixture: _DirectFinalizerFixture,
    output_directory: Path,
    *,
    check: bool,
):
    return subprocess.run(
        [
            str(fixture.scanner_binary),
            "--finalize-v3-runs",
            str(output_directory),
            *_v3_finalizer_test_resource_args(),
            "--price-key-map-input",
            str(fixture.price_key_map_input),
            "--price-key-map-row-count",
            str(len(fixture.price_set_ids)),
            "--price-membership-input",
            str(fixture.membership_input),
            "--price-atom-input",
            str(fixture.atom_input),
            str(fixture.manifest_path),
        ],
        check=check,
        env=fixture.environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )


def _assert_direct_finalizer_summary(summary: dict, output_directory: Path) -> None:
    assert summary["format"] == "ptg2_v3_direct_finalizer_v3"
    assert summary["storage_generation"] == "shared_blocks_v3"
    assert summary["source_count"] == 1
    assert summary["source"]["record_count"] == 3
    assert summary["preservation"] == {
        "source_records": 3,
        "sorted_records": 3,
        "staged_records": 3,
        "assigned_records": 3,
        "encoded_records": 3,
        "distinct_serving_records": 2,
        "duplicate_serving_records": 1,
        "source_equals_sorted": True,
        "sorted_equals_staged": True,
        "staged_equals_assigned": True,
        "assigned_equals_encoded": True,
        "all_source_occurrences_preserved": True,
    }
    assert summary["deferred_atom_inputs"]["fused"] is False
    assert set(path.name for path in output_directory.iterdir()) == {
        "audit_candidates.bin",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
        "summary.json",
    }


def _assert_direct_finalizer_audit(summary: dict, output_directory: Path) -> None:
    audit_bytes = (output_directory / "audit_candidates.bin").read_bytes()
    assert len(audit_bytes) == 3 * _AUDIT_CANDIDATE_RECORD.size
    audit_rows = [
        _AUDIT_CANDIDATE_RECORD.unpack(
            audit_bytes[offset : offset + _AUDIT_CANDIDATE_RECORD.size]
        )
        for offset in range(0, len(audit_bytes), _AUDIT_CANDIDATE_RECORD.size)
    ]
    assert sorted(Counter(audit_rows).values()) == [1, 2]
    assert {audit_row[3] for audit_row in audit_rows} == {0}
    assert sorted({audit_row[4] for audit_row in audit_rows}) == [1, 2]
    assert summary["audit_candidates"] == {
        "path": "audit_candidates.bin",
        "record_format": "ptg2_v3_audit_candidates_v2",
        "format_version": 2,
        "record_bytes": 20,
        "fields": [
            "code_key",
            "provider_set_key",
            "price_key",
            "source_key",
            "provider_count",
        ],
        "source_key_included": True,
        "source_count": 1,
        "source_key_bits": 0,
        "record_counts_by_source": {"0": 3},
        "row_count": 3,
        "maximum_rows": 4096,
        "selection_method": "equal_interval_assigned_rows_v1",
        "source_row_count": 3,
        "row_digest": hashlib.sha256(audit_bytes).hexdigest(),
    }


def _assert_direct_finalizer_dictionary(
    summary: dict,
    output_directory: Path,
) -> None:
    code_rows = _read_pg_binary_rows(
        (output_directory / "code_dictionary.copy").read_bytes(),
        10,
    )
    assert len(code_rows) == 1
    assert struct.unpack(">i", code_rows[0][0])[0] == 0
    assert len(code_rows[0][1]) == 16
    assert code_rows[0][2] == b"\xcc" * 32
    assert code_rows[0][3:6] == [b"CPT", b"99213", b"FFS"]
    assert code_rows[0][6:9] == [b"2026", None, None]
    assert struct.unpack(">q", code_rows[0][9])[0] == 3
    assert summary["dictionaries"]["code"]["fields"] == [
        "code_key",
        "code_global_id_128",
        "coverage_scope_id",
        "reported_code_system",
        "reported_code",
        "negotiation_arrangement",
        "billing_code_type_version",
        "source_name",
        "source_description",
        "rate_count",
    ]
    assert summary["dictionaries"]["code"]["rate_count_total"] == 3
    assert summary["dense_keys"]["price"]["ordering"] == (
        "minimum_negotiated_rate_then_global_id_128_v1"
    )


def _assert_direct_finalizer_block_metadata(
    summary: dict,
    output_directory: Path,
) -> None:
    assert summary["blocks"]["serving"]["fields"] == [
        "block_hash",
        "format_version",
        "object_kind",
        "block_key",
        "fragment_no",
        "entry_count",
        "codec",
        "raw_byte_count",
        "stored_byte_count",
        "payload",
    ]
    assert summary["blocks"]["serving"]["snapshot_key_included"] is False
    for section_name, file_name in (
        ("serving", "shared_serving_blocks.copy"),
        ("price_dictionary", "shared_price_dictionary_blocks.copy"),
    ):
        copy_bytes = (output_directory / file_name).read_bytes()
        assert summary["blocks"][section_name]["copy_bytes"] == len(copy_bytes)
        assert summary["blocks"][section_name]["copy_sha256"] == hashlib.sha256(
            copy_bytes
        ).hexdigest()


def _assert_direct_finalizer_serving_rows(output_directory: Path) -> None:
    shared_rows = _read_pg_binary_rows(
        (output_directory / "shared_serving_blocks.copy").read_bytes(),
        10,
    )
    assert shared_rows
    shard_rows = [
        shared_row
        for shared_row in shared_rows
        if shared_row[2] == b"by_code_provider_shard_v1"
    ]
    assert len(shard_rows) == 1
    shard_row = shard_rows[0]
    assert shard_row[6] == b"none"
    groups = _decode_by_code_groups(
        shard_row[9],
        struct.unpack(">q", shard_row[5])[0],
    )
    assert sorted(len(prices) for _provider, prices, _sources in groups) == [1, 2]
    assert all(
        price == 0
        for _provider, prices, _sources in groups
        for price in prices
    )
    assert all(
        source_key == 0
        for _provider, _prices, source_keys in groups
        for source_key in source_keys
    )
    first = shared_rows[0]
    format_version = struct.unpack(">h", first[1])[0]
    assert format_version == 2
    shared_block_hash = _load_isolated_shared_blocks().shared_block_hash
    assert first[0] == shared_block_hash(
        format_version=format_version,
        object_kind=first[2].decode("utf-8"),
        codec=first[6].decode("ascii"),
        payload=first[9],
    )


def _assert_direct_finalizer_rejects_tamper(
    fixture: _DirectFinalizerFixture,
    tmp_path: Path,
) -> None:
    partition_path = Path(fixture.scan["partition_frames"][0]["path"])
    tampered_payload = bytearray(partition_path.read_bytes())
    tampered_payload[-1] ^= 1
    partition_path.write_bytes(tampered_payload)
    completed = _run_direct_finalizer(
        fixture,
        tmp_path / "tampered-finalized",
        check=False,
    )
    assert completed.returncode != 0
    assert (
        b"content digest mismatch" in completed.stderr
        or b"immutable dense map" in completed.stderr
    )
