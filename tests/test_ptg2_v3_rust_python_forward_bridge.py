# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import struct
import subprocess
from collections import Counter
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
async def test_real_rust_v3_forward_writer_bridges_to_strict_python_reader(
    tmp_path,
    monkeypatch,
):
    """Bridge real Rust V3 output into the strict Python shared-block reader."""

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
    source_records = [
        _SERVING_RECORD.unpack_from(scan["partition_bytes"], offset)
        for offset in range(0, len(scan["partition_bytes"]), _SERVING_RECORD.size)
    ]
    assert len(source_records) == 3
    assert sorted(Counter(source_records).values()) == [1, 2]
    assert {source_record[3] for source_record in source_records} == {1, 2}

    repeated_provider_set_id = next(
        provider_set_id
        for _code_id, provider_set_id, _price_set_id, provider_count in source_records
        if provider_count == 2
    )
    single_provider_set_id = next(
        provider_set_id
        for _code_id, provider_set_id, _price_set_id, provider_count in source_records
        if provider_count == 1
    )
    low_price_set_id = next(
        price_set_id
        for _code_id, _provider_set_id, price_set_id, provider_count in source_records
        if provider_count == 2
    )
    high_price_set_id = next(
        price_set_id
        for _code_id, _provider_set_id, price_set_id, provider_count in source_records
        if provider_count == 1
    )
    assert single_provider_set_id < repeated_provider_set_id
    assert high_price_set_id < low_price_set_id

    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": hashlib.sha256(scan["artifact"].read_bytes()).hexdigest(),
    }
    manifest_path = tmp_path / "v3-forward-bridge-input.json"
    scanner_summary = scanner_support._single_frame(scan["frames"], "scanner_summary")
    scanner_config = scanner_support._single_frame(scan["frames"], "scanner_config")
    serving_run_entries = attach_v3_source_run_contract(
        scan["partition_frames"],
        source_identity=source_identity_map,
        scanner_summary=scanner_summary,
        scanner_config=scanner_config,
    )
    code_dictionary_entries = attach_v3_dictionary_contract(
        scan["code_dictionary_frames"],
        source_identity=source_identity_map,
        source_run_contract_sha256=serving_run_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary=scanner_summary,
    )
    write_v3_finalizer_input_manifest(
        manifest_path,
        serving_run_entries=serving_run_entries,
        code_dictionary_entries=code_dictionary_entries,
        expected_source_identities=[source_identity_map],
    )

    price_key_by_id = {
        low_price_set_id: 0,
        high_price_set_id: 1,
    }
    price_map_rows = [
        [price_set_id, struct.pack(">q", price_key_by_id[price_set_id])]
        for price_set_id in sorted(price_key_by_id)
    ]
    assert [
        struct.unpack(">q", price_map_row[1])[0]
        for price_map_row in price_map_rows
    ] == [1, 0]
    price_key_map_path = tmp_path / "price-key-map.copy"
    price_key_map_path.write_bytes(scanner_support._pg_binary_copy_rows(price_map_rows))
    assert scanner_support._read_pg_binary_rows(
        price_key_map_path.read_bytes(), 2
    ) == price_map_rows

    deferred_membership_path = tmp_path / "deferred-memberships.copy"
    deferred_atom_path = tmp_path / "deferred-atoms.copy"
    deferred_membership_path.write_bytes(b"")
    deferred_atom_path.write_bytes(b"")
    output_directory = tmp_path / "finalized"
    completed = subprocess.run(
        [
            str(scanner_binary),
            "--finalize-v3-runs",
            str(output_directory),
            *scanner_support._v3_finalizer_test_resource_args(),
            "--price-key-map-input",
            str(price_key_map_path),
            "--price-membership-input",
            str(deferred_membership_path),
            "--price-atom-input",
            str(deferred_atom_path),
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
    finalizer_frames = scanner_support._parse_scanner_frames(completed.stdout)
    finalizer_summary = scanner_support._single_frame(
        finalizer_frames, "v3_finalizer_summary"
    )
    assert finalizer_summary["source"]["record_count"] == 3
    assert finalizer_summary["preservation"]["source_records"] == 3
    assert finalizer_summary["preservation"]["encoded_records"] == 3
    assert finalizer_summary["preservation"]["duplicate_serving_records"] == 1
    assert finalizer_summary["preservation"]["all_source_occurrences_preserved"] is True
    assert finalizer_summary["dense_keys"]["price"]["count"] == 2
    assert finalizer_summary["price_key_map"]["keys_unique_dense_contiguous"] is True
    assert finalizer_summary["price_key_map"]["source_ids_exact_match"] is True

    code_rows = scanner_support._read_pg_binary_rows(
        (output_directory / "code_dictionary.copy").read_bytes(), 10
    )
    assert len(code_rows) == 1
    code_key = _unpack_int(code_rows[0][0], ">i")
    code_global_id = code_rows[0][1]
    assert code_key == 0
    assert code_global_id is not None
    assert {source_record[0] for source_record in source_records} == {code_global_id}
    assert code_rows[0][3:9] == [b"CPT", b"00042", b"FFS", b"2026", None, None]
    assert _unpack_int(code_rows[0][9], ">q") == 3

    provider_rows = scanner_support._read_pg_binary_rows(
        (output_directory / "provider_set_dictionary.copy").read_bytes(), 3
    )
    assert len(provider_rows) == 2
    provider_dictionary = {
        provider_row[1]: (
            _unpack_int(provider_row[0], ">i"),
            _unpack_int(provider_row[2], ">q"),
        )
        for provider_row in provider_rows
    }
    assert provider_dictionary == {
        single_provider_set_id: (0, 1),
        repeated_provider_set_id: (1, 2),
    }
    provider_counts_by_key = {
        provider_key: provider_count
        for provider_key, provider_count in provider_dictionary.values()
    }

    shared_copy_path = output_directory / "shared_serving_blocks.copy"
    shared_copy_bytes = shared_copy_path.read_bytes()
    shared_copy_rows = scanner_support._read_pg_binary_rows(shared_copy_bytes, 10)
    object_kinds = {
        shared_copy_row[2].decode("ascii")
        for shared_copy_row in shared_copy_rows
        if shared_copy_row[2]
    }
    assert _FORWARD_KIND in object_kinds
    assert _REMOVED_GROUPED_KIND not in object_kinds
    assert _REMOVED_GROUPED_KIND.encode("ascii") not in shared_copy_bytes
    assert _REMOVED_GROUPED_KIND not in json.dumps(finalizer_summary, sort_keys=True)

    forward_fragments = _forward_fragment_rows(shared_copy_rows)
    assert [
        (fragment["block_key"], fragment["block_no"], fragment["entry_count"])
        for fragment in forward_fragments
    ] == [(0, 0, 2)]
    assert forward_fragments[0]["payload"][0] == 2

    price_id_by_key = {
        price_key: price_set_id.hex()
        for price_set_id, price_key in price_key_by_id.items()
    }
    shard_keys_by_code = {
        code_key: tuple(
            sorted(
                {
                    fragment["block_key"]
                    for fragment in forward_fragments
                    if code_key * _CODE_BLOCK_SPAN
                    <= fragment["block_key"]
                    < (code_key + 1) * _CODE_BLOCK_SPAN
                }
            )
        )
    }

    async def fetch_fragments(
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
        requested_block_keys = {int(block_key) for block_key in block_keys}
        return [
            dict(fragment)
            for fragment in forward_fragments
            if fragment["block_key"] in requested_block_keys
        ]

    async def discover_shards(
        _session,
        *,
        shared_snapshot_key,
        schema_name,
        code_keys,
    ):
        assert shared_snapshot_key == 73
        assert schema_name == "mrf"
        assert tuple(code_keys) == (code_key,)
        return shard_keys_by_code

    async def dictionary_values(
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
        return {int(item_key): price_id_by_key[int(item_key)] for item_key in item_keys}

    async def provider_counts(
        _session,
        *,
        shared_snapshot_key,
        schema_name,
        provider_set_keys,
    ):
        assert shared_snapshot_key == 73
        assert schema_name == "mrf"
        return {
            int(provider_set_key): provider_counts_by_key[int(provider_set_key)]
            for provider_set_key in provider_set_keys
        }

    transport_mock = AsyncMock(side_effect=fetch_fragments)
    discovery_mock = AsyncMock(side_effect=discover_shards)
    dictionary_mock = AsyncMock(side_effect=dictionary_values)
    provider_count_mock = AsyncMock(side_effect=provider_counts)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        transport_mock,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_discover_forward_shard_keys",
        discovery_mock,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_serving_binary_dictionary_values_for_keys",
        dictionary_mock,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_provider_counts_for_keys",
        provider_count_mock,
    )

    expected_rows = (
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=0,
            provider_count=1,
            price_set_global_id_128=high_price_set_id.hex(),
            source_key=0,
            price_key=1,
        ),
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=1,
            provider_count=2,
            price_set_global_id_128=low_price_set_id.hex(),
            source_key=0,
            price_key=0,
        ),
        PTG2ServingBinaryRow(
            code_key=0,
            provider_set_key=1,
            provider_count=2,
            price_set_global_id_128=low_price_set_id.hex(),
            source_key=0,
            price_key=0,
        ),
    )
    decoded_rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        code_key,
        shared_snapshot_key=73,
        source_count=1,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=65536,
    )
    assert decoded_rows == expected_rows
    assert Counter(
        (
            decoded_row.provider_set_key,
            decoded_row.price_key,
            decoded_row.source_key,
        )
        for decoded_row in decoded_rows
    ) == Counter({(0, 1, 0): 1, (1, 0, 0): 2})
    assert {decoded_row.provider_set_key for decoded_row in decoded_rows} == {0, 1}
    assert {decoded_row.price_key for decoded_row in decoded_rows} == {0, 1}
    assert {decoded_row.source_key for decoded_row in decoded_rows} == {0}
    discovery_mock.assert_awaited_once()

    sparse_rows = await ptg2_db_sidecars.lookup_serving_binary_by_code_from_db(
        object(),
        code_key,
        provider_set_keys=(1, 2050),
        shared_snapshot_key=73,
        source_count=1,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=65536,
    )
    assert sparse_rows == expected_rows[1:]
    discovery_mock.assert_awaited_once()
    sparse_transport_call = transport_mock.await_args_list[-1]
    assert sparse_transport_call.kwargs["block_keys"] == (0, 2)
    assert sparse_transport_call.kwargs["require_all"] is False
    assert provider_count_mock.await_args_list[-1].kwargs["provider_set_keys"] == {1}
    assert dictionary_mock.await_args_list[-1].kwargs["item_keys"] == {0}
