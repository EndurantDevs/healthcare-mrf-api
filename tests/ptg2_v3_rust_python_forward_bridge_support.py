# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import os
import struct
import subprocess
from collections import Counter
from pathlib import Path
from unittest.mock import AsyncMock

from api import ptg2_db_sidecars
from api.ptg2_db_sidecars import PTG2ServingBinaryRow
from tests.ptg2_v3_rust_python_forward_bridge_fixture import (
    _CODE_BLOCK_SPAN,
    _FORWARD_KIND,
    _REMOVED_GROUPED_KIND,
    _ForwardBridgeDbHarness,
    _ForwardBridgeMocks,
    _ForwardBridgeResult,
    _ForwardBridgeScan,
    _forward_fragment_rows,
    _unpack_int,
    _write_forward_bridge_manifest,
)


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
