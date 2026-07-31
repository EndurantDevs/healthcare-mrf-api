# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import struct
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars
from api.ptg2_db_sidecars import PTG2ServingBinaryRow
from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    write_v3_finalizer_input_manifest,
)


_SCANNER_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_v3_runs.py")
_SERVING_RECORD = struct.Struct(">16s16s16sI")
_FORWARD_KIND = "by_code_provider_shard_v1"
_REMOVED_GROUPED_KIND = "by_code_grouped_v2"
_CODE_BLOCK_SPAN = 1 << 31


def _load_scanner_support():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v3_forward_bridge_scanner_support",
        _SCANNER_SUPPORT_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source_payload() -> dict:
    repeated_rate_map = {
        "provider_references": [11],
        "negotiated_prices": [
            {
                "negotiated_type": "negotiated",
                "negotiated_rate": 101.25,
                "service_code": ["11"],
                "billing_class": "professional",
            }
        ],
    }
    return {
        "provider_references": [
            {
                "provider_group_id": 11,
                "provider_groups": [
                    {
                        "npi": [1234567890, 1234567891],
                        "tin": {"type": "ein", "value": "11-1111111"},
                    }
                ],
            },
            {
                "provider_group_id": 22,
                "provider_groups": [
                    {
                        "npi": [1234567892],
                        "tin": {"type": "ein", "value": "22-2222222"},
                    }
                ],
            },
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "00042",
                "negotiation_arrangement": "ffs",
                "negotiated_rates": [
                    repeated_rate_map,
                    json.loads(json.dumps(repeated_rate_map)),
                    {
                        "provider_references": [22],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 202.5,
                                "service_code": ["11"],
                                "billing_class": "professional",
                            }
                        ],
                    },
                ],
            }
        ],
    }


def _unpack_int(field: bytes | None, fmt: str) -> int:
    assert field is not None
    assert len(field) == struct.calcsize(fmt)
    return int(struct.unpack(fmt, field)[0])


def _forward_fragment_rows(copy_rows: list[list[bytes | None]]) -> list[dict]:
    fragments = []
    for copy_row in copy_rows:
        if copy_row[2] != _FORWARD_KIND.encode("ascii"):
            continue
        assert copy_row[6] == b"none"
        payload = copy_row[9]
        assert payload is not None
        raw_payload_bytes = _unpack_int(copy_row[7], ">q")
        stored_payload_bytes = _unpack_int(copy_row[8], ">q")
        assert raw_payload_bytes == stored_payload_bytes == len(payload)
        fragments.append(
            {
                "block_key": _unpack_int(copy_row[3], ">q"),
                "block_no": _unpack_int(copy_row[4], ">i"),
                "entry_count": _unpack_int(copy_row[5], ">q"),
                "payload": payload,
                "payload_compression": "none",
                "raw_payload_bytes": raw_payload_bytes,
            }
        )
    return fragments


@pytest.mark.asyncio
@dataclass(frozen=True)
class _ForwardBridgeScan:
    scanner_support: object
    scanner_binary: Path
    scan: dict
    source_records: tuple
    repeated_provider_set_id: bytes
    single_provider_set_id: bytes
    low_price_set_id: bytes
    high_price_set_id: bytes


@dataclass(frozen=True)
class _ForwardBridgeResult:
    scan: _ForwardBridgeScan
    output_directory: Path
    summary: dict
    code_key: int
    provider_counts_by_key: dict[int, int]
    forward_fragments: list[dict]
    price_id_by_key: dict[int, str]
    shard_keys_by_code: dict[int, tuple[int, ...]]


@dataclass(frozen=True)
class _ForwardBridgeMocks:
    transport: AsyncMock
    discovery: AsyncMock
    dictionary: AsyncMock
    provider_count: AsyncMock


@dataclass(frozen=True)
class _ForwardBridgeDbHarness:
    forward_fragments: list[dict]
    price_id_by_key: dict[int, str]
    shard_keys_by_code: dict[int, tuple[int, ...]]
    provider_counts_by_key: dict[int, int]

    async def fetch_fragments(
        self,
        _session,
        *,
        shared_snapshot_key,
        schema_name,
        artifact_kind,
        block_keys,
        **_options,
    ):
        assert shared_snapshot_key == 73
        assert schema_name == "mrf"
        assert artifact_kind == _FORWARD_KIND
        requested_keys = {int(block_key) for block_key in block_keys}
        return [
            dict(fragment)
            for fragment in self.forward_fragments
            if fragment["block_key"] in requested_keys
        ]

    async def discover_shards(
        self,
        _session,
        *,
        shared_snapshot_key,
        schema_name,
        code_keys,
        provider_shard_span,
    ):
        assert shared_snapshot_key == 73
        assert schema_name == "mrf"
        assert tuple(code_keys) == tuple(self.shard_keys_by_code)
        assert provider_shard_span == 8192
        return self.shard_keys_by_code

    async def dictionary_values(
        self,
        _session,
        *,
        shared_snapshot_key,
        artifact_kind,
        item_keys,
        item_count,
        block_bytes,
        schema_name,
    ):
        assert shared_snapshot_key == 73
        assert artifact_kind == "by_code_price_dictionary"
        assert item_count == 2
        assert block_bytes == 65536
        assert schema_name == "mrf"
        return {
            int(item_key): self.price_id_by_key[int(item_key)]
            for item_key in item_keys
        }

    async def provider_counts(
        self,
        _session,
        *,
        shared_snapshot_key,
        schema_name,
        provider_set_keys,
    ):
        assert shared_snapshot_key == 73
        assert schema_name == "mrf"
        return {
            int(provider_key): self.provider_counts_by_key[int(provider_key)]
            for provider_key in provider_set_keys
        }


def _prepare_forward_bridge_scan(tmp_path, monkeypatch) -> _ForwardBridgeScan:
    scanner_support = _load_scanner_support()
    monkeypatch.setattr(
        scanner_support,
        "_fixture_payload",
        lambda **_options: _source_payload(),
    )
    scanner_binary = scanner_support._built_scanner_binary()
    scan = scanner_support._run_scanner(
        scanner_binary,
        tmp_path,
        "forward-bridge-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
    )
    assert json.loads(scan["artifact"].read_text(encoding="utf-8")) == _source_payload()
    source_records = tuple(
        _SERVING_RECORD.unpack_from(scan["partition_bytes"], offset)
        for offset in range(0, len(scan["partition_bytes"]), _SERVING_RECORD.size)
    )
    assert len(source_records) == 3
    assert sorted(Counter(source_records).values()) == [1, 2]
    assert {source_record[3] for source_record in source_records} == {1, 2}
    provider_id_by_count = {
        provider_count: provider_set_id
        for _code_id, provider_set_id, _price_set_id, provider_count in source_records
    }
    price_id_by_count = {
        provider_count: price_set_id
        for _code_id, _provider_set_id, price_set_id, provider_count in source_records
    }
    assert provider_id_by_count[1] < provider_id_by_count[2]
    assert price_id_by_count[1] < price_id_by_count[2]
    return _ForwardBridgeScan(
        scanner_support=scanner_support,
        scanner_binary=scanner_binary,
        scan=scan,
        source_records=source_records,
        repeated_provider_set_id=provider_id_by_count[2],
        single_provider_set_id=provider_id_by_count[1],
        low_price_set_id=price_id_by_count[2],
        high_price_set_id=price_id_by_count[1],
    )


def _write_forward_bridge_manifest(
    tmp_path: Path,
    fixture: _ForwardBridgeScan,
) -> Path:
    manifest_path = tmp_path / "v3-forward-bridge-input.json"
    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": hashlib.sha256(
            fixture.scan["artifact"].read_bytes()
        ).hexdigest(),
    }
    scanner_summary = fixture.scanner_support._single_frame(
        fixture.scan["frames"],
        "scanner_summary",
    )
    scanner_config = fixture.scanner_support._single_frame(
        fixture.scan["frames"],
        "scanner_config",
    )
    serving_runs = attach_v3_source_run_contract(
        fixture.scan["partition_frames"],
        source_identity=source_identity_map,
        scanner_summary=scanner_summary,
        scanner_config=scanner_config,
    )
    dictionaries = attach_v3_dictionary_contract(
        fixture.scan["code_dictionary_frames"],
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
        for entry in fixture.scan["provider_set_metadata_frames"]
    ]
    write_v3_finalizer_input_manifest(
        manifest_path,
        serving_run_entries=serving_runs,
        code_dictionary_entries=dictionaries,
        provider_set_metadata_entries=provider_metadata_entries,
        expected_source_identities=[source_identity_map],
    )
    return manifest_path


def _write_forward_bridge_price_map(
    tmp_path: Path,
    fixture: _ForwardBridgeScan,
) -> Path:
    price_map_rows = [
        [fixture.low_price_set_id, struct.pack(">q", 0)],
        [fixture.high_price_set_id, struct.pack(">q", 1)],
    ]
    assert [
        struct.unpack(">q", price_map_row[1])[0]
        for price_map_row in price_map_rows
    ] == [0, 1]
    price_key_map_path = tmp_path / "price-key-map.copy"
    price_key_map_path.write_bytes(
        fixture.scanner_support._pg_binary_copy_rows(price_map_rows)
    )
    assert fixture.scanner_support._read_pg_binary_rows(
        price_key_map_path.read_bytes(),
        2,
    ) == price_map_rows
    return price_key_map_path


def _run_forward_bridge_finalizer(
    tmp_path: Path,
    fixture: _ForwardBridgeScan,
) -> tuple[Path, dict]:
    manifest_path = _write_forward_bridge_manifest(tmp_path, fixture)
    price_key_map_path = _write_forward_bridge_price_map(tmp_path, fixture)
    membership_path = tmp_path / "deferred-memberships.copy"
    atom_path = tmp_path / "deferred-atoms.copy"
    membership_path.write_bytes(b"")
    atom_path.write_bytes(b"")
    output_directory = tmp_path / "finalized"
    completed = subprocess.run(
        [
            str(fixture.scanner_binary),
            "--finalize-v3-runs",
            str(output_directory),
            *fixture.scanner_support._v3_finalizer_test_resource_args(),
            "--price-key-map-input",
            str(price_key_map_path),
            "--price-key-map-row-count",
            "2",
            "--price-membership-input",
            str(membership_path),
            "--price-atom-input",
            str(atom_path),
            str(manifest_path),
        ],
        check=True,
        env={
            **os.environ,
            "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION": "none",
            "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES": "65536",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    frames = fixture.scanner_support._parse_scanner_frames(completed.stdout)
    summary = fixture.scanner_support._single_frame(frames, "v3_finalizer_summary")
    return output_directory, summary


def _assert_forward_bridge_summary(summary: dict) -> None:
    assert summary["source"]["record_count"] == 3
    assert summary["preservation"]["source_records"] == 3
    assert summary["preservation"]["encoded_records"] == 3
    assert summary["preservation"]["duplicate_serving_records"] == 1
    assert summary["preservation"]["all_source_occurrences_preserved"] is True
    assert summary["dense_keys"]["price"]["count"] == 2
    assert summary["price_key_map"]["keys_unique_dense_contiguous"] is True
    assert summary["price_key_map"]["source_ids_exact_match"] is True


def _forward_bridge_code_key(
    fixture: _ForwardBridgeScan,
    output_directory: Path,
) -> int:
    code_rows = fixture.scanner_support._read_pg_binary_rows(
        (output_directory / "code_dictionary.copy").read_bytes(),
        10,
    )
    assert len(code_rows) == 1
    code_key = _unpack_int(code_rows[0][0], ">i")
    code_global_id = code_rows[0][1]
    assert code_key == 0
    assert code_global_id is not None
    assert {
        source_record[0] for source_record in fixture.source_records
    } == {code_global_id}
    assert code_rows[0][3:9] == [b"CPT", b"00042", b"FFS", b"2026", None, None]
    assert _unpack_int(code_rows[0][9], ">q") == 3
    return code_key


def _forward_bridge_provider_counts(
    fixture: _ForwardBridgeScan,
    output_directory: Path,
) -> dict[int, int]:
    provider_rows = fixture.scanner_support._read_pg_binary_rows(
        (output_directory / "provider_set_dictionary.copy").read_bytes(),
        3,
    )
    assert len(provider_rows) == 2
    provider_dictionary = {
        row[1]: (
            _unpack_int(row[0], ">i"),
            _unpack_int(row[2], ">q"),
        )
        for row in provider_rows
    }
    assert provider_dictionary == {
        fixture.single_provider_set_id: (0, 1),
        fixture.repeated_provider_set_id: (1, 2),
    }
    return {
        provider_key: provider_count
        for provider_key, provider_count in provider_dictionary.values()
    }


def _forward_bridge_fragments(
    fixture: _ForwardBridgeScan,
    output_directory: Path,
    summary: dict,
) -> list[dict]:
    shared_copy_bytes = (output_directory / "shared_serving_blocks.copy").read_bytes()
    shared_copy_rows = fixture.scanner_support._read_pg_binary_rows(
        shared_copy_bytes,
        10,
    )
    object_kinds = {
        row[2].decode("ascii") for row in shared_copy_rows if row[2]
    }
    assert _FORWARD_KIND in object_kinds
    assert _REMOVED_GROUPED_KIND not in object_kinds
    assert _REMOVED_GROUPED_KIND.encode("ascii") not in shared_copy_bytes
    assert _REMOVED_GROUPED_KIND not in json.dumps(summary, sort_keys=True)
    fragments = _forward_fragment_rows(shared_copy_rows)
    assert [
        (fragment["block_key"], fragment["block_no"], fragment["entry_count"])
        for fragment in fragments
    ] == [(0, 0, 2)]
    assert fragments[0]["payload"][0] == 2
    return fragments


def _assemble_forward_bridge_result(
    fixture: _ForwardBridgeScan,
    output_directory: Path,
    summary: dict,
) -> _ForwardBridgeResult:
    _assert_forward_bridge_summary(summary)
    code_key = _forward_bridge_code_key(fixture, output_directory)
    provider_counts = _forward_bridge_provider_counts(fixture, output_directory)
    fragments = _forward_bridge_fragments(fixture, output_directory, summary)
    price_id_by_key = {
        0: fixture.low_price_set_id.hex(),
        1: fixture.high_price_set_id.hex(),
    }
    shard_keys_by_code = {
        code_key: tuple(
            sorted(
                {
                    fragment["block_key"]
                    for fragment in fragments
                    if code_key * _CODE_BLOCK_SPAN
                    <= fragment["block_key"]
                    < (code_key + 1) * _CODE_BLOCK_SPAN
                }
            )
        )
    }
    return _ForwardBridgeResult(
        scan=fixture,
        output_directory=output_directory,
        summary=summary,
        code_key=code_key,
        provider_counts_by_key=provider_counts,
        forward_fragments=fragments,
        price_id_by_key=price_id_by_key,
        shard_keys_by_code=shard_keys_by_code,
    )


def _install_forward_bridge_mocks(
    monkeypatch,
    bridge_result: _ForwardBridgeResult,
) -> _ForwardBridgeMocks:
    harness = _ForwardBridgeDbHarness(
        forward_fragments=bridge_result.forward_fragments,
        price_id_by_key=bridge_result.price_id_by_key,
        shard_keys_by_code=bridge_result.shard_keys_by_code,
        provider_counts_by_key=bridge_result.provider_counts_by_key,
    )
    mocks = _ForwardBridgeMocks(
        transport=AsyncMock(side_effect=harness.fetch_fragments),
        discovery=AsyncMock(side_effect=harness.discover_shards),
        dictionary=AsyncMock(side_effect=harness.dictionary_values),
        provider_count=AsyncMock(side_effect=harness.provider_counts),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        mocks.transport,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        mocks.discovery,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_serving_binary_dictionary_values_for_keys",
        mocks.dictionary,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_provider_counts_for_keys",
        mocks.provider_count,
    )
    return mocks


def _expected_forward_bridge_rows(
    result: _ForwardBridgeResult,
) -> tuple[PTG2ServingBinaryRow, ...]:
    return (
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=0,
            provider_count=1,
            price_set_global_id_128=result.scan.high_price_set_id.hex(),
            source_key=0,
            price_key=1,
        ),
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=1,
            provider_count=2,
            price_set_global_id_128=result.scan.low_price_set_id.hex(),
            source_key=0,
            price_key=0,
        ),
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=1,
            provider_count=2,
            price_set_global_id_128=result.scan.low_price_set_id.hex(),
            source_key=0,
            price_key=0,
        ),
    )


async def _lookup_forward_bridge_rows(
    result: _ForwardBridgeResult,
    *,
    provider_set_keys: tuple[int, ...] | None = None,
):
    optional_filter_map = {}
    if provider_set_keys is not None:
        optional_filter_map["provider_set_keys"] = provider_set_keys
    return await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        result.code_key,
        **optional_filter_map,
        shared_snapshot_key=73,
        source_count=1,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=65536,
        provider_shard_span=result.summary["blocks"]["assigned_encoder"][
            "provider_shard_span"
        ],
    )


def _assert_forward_bridge_decoded_rows(
    decoded_rows,
    expected_rows,
) -> None:
    assert decoded_rows == expected_rows
    assert Counter(
        (
            row.provider_set_key,
            row.price_key,
            row.source_key,
        )
        for row in decoded_rows
    ) == Counter({(0, 1, 0): 1, (1, 0, 0): 2})
    assert {row.provider_set_key for row in decoded_rows} == {0, 1}
    assert {row.price_key for row in decoded_rows} == {0, 1}
    assert {row.source_key for row in decoded_rows} == {0}


def _assert_forward_bridge_sparse_rows(
    sparse_rows,
    expected_rows,
    mocks: _ForwardBridgeMocks,
) -> None:
    assert sparse_rows == expected_rows[1:]
    mocks.discovery.assert_awaited_once()
    sparse_transport_call = mocks.transport.await_args_list[-1]
    assert sparse_transport_call.kwargs["block_keys"] == (0,)
    assert sparse_transport_call.kwargs["require_all"] is False
    assert mocks.provider_count.await_args_list[-1].kwargs["provider_set_keys"] == {1}
    assert mocks.dictionary.await_args_list[-1].kwargs["item_keys"] == {0}


@pytest.mark.asyncio
async def test_real_rust_v3_forward_writer_bridges_to_strict_python_reader(
    tmp_path,
    monkeypatch,
):
    """Bridge real Rust V3 output into the strict Python shared-block reader."""
    scan = _prepare_forward_bridge_scan(tmp_path, monkeypatch)
    output_directory, summary = _run_forward_bridge_finalizer(tmp_path, scan)
    result = _assemble_forward_bridge_result(scan, output_directory, summary)
    mocks = _install_forward_bridge_mocks(monkeypatch, result)
    expected_rows = _expected_forward_bridge_rows(result)

    decoded_rows = await _lookup_forward_bridge_rows(result)
    _assert_forward_bridge_decoded_rows(decoded_rows, expected_rows)
    mocks.discovery.assert_awaited_once()

    sparse_rows = await _lookup_forward_bridge_rows(
        result,
        provider_set_keys=(1, 2050),
    )
    _assert_forward_bridge_sparse_rows(sparse_rows, expected_rows, mocks)
