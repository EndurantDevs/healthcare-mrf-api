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
