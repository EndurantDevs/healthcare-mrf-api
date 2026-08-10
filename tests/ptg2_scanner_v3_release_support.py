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

_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_parallelism.py")
_SUPPORT_SPEC = importlib.util.spec_from_file_location(
    "ptg2_scanner_v3_test_support", _SUPPORT_PATH
)
assert _SUPPORT_SPEC is not None and _SUPPORT_SPEC.loader is not None
_SUPPORT_MODULE = importlib.util.module_from_spec(_SUPPORT_SPEC)
_SUPPORT_SPEC.loader.exec_module(_SUPPORT_MODULE)

_built_scanner_binary = _SUPPORT_MODULE._built_scanner_binary
_load_isolated_rust_scanner = _SUPPORT_MODULE._load_isolated_rust_scanner
_parse_scanner_frames = _SUPPORT_MODULE._parse_scanner_frames

_SERVING_RECORD = struct.Struct(">16s16s16sI")
_AUDIT_CANDIDATE_RECORD = struct.Struct(">IIIII")
_MIB = 1024 * 1024
_STRICT_SCANNER_FRAME_KINDS = {
    "dedupe_summary",
    "manifest_price_atom_copy_file",
    "manifest_price_set_atom_copy_file",
    "manifest_price_set_summary_copy_file",
    "manifest_provider_forward_sidecar_file",
    "manifest_provider_group_member_copy_file",
    "manifest_provider_inverted_sidecar_file",
    "manifest_provider_set_dictionary_copy_file",
    "scanner_config",
    "scanner_summary",
    "source_audit_witness_file",
    "v3_serving_code_dictionary_file",
    "v3_serving_run_partition_file",
}


def _v3_finalizer_test_resource_args() -> tuple[str, ...]:
    return (
        "--workers",
        "1",
        "--identity-map-max-bytes",
        str(64 * _MIB),
        "--total-sort-memory-bytes",
        str(64 * _MIB),
    )


def _read_pg_binary_rows(payload: bytes, expected_fields: int) -> list[list[bytes | None]]:
    stream = io.BytesIO(payload)
    assert stream.read(11) == b"PGCOPY\n\xff\r\n\0"
    assert struct.unpack(">i", stream.read(4))[0] == 0
    extension_bytes = struct.unpack(">i", stream.read(4))[0]
    assert extension_bytes >= 0
    assert len(stream.read(extension_bytes)) == extension_bytes
    rows = []
    while True:
        field_count = struct.unpack(">h", stream.read(2))[0]
        if field_count == -1:
            break
        assert field_count == expected_fields
        fields = []
        for _field_index in range(field_count):
            field_bytes = struct.unpack(">i", stream.read(4))[0]
            fields.append(None if field_bytes == -1 else stream.read(field_bytes))
        rows.append(fields)
    assert stream.read() == b""
    return rows


def _pg_binary_copy_rows(rows: list[list[bytes | None]]) -> bytes:
    payload = bytearray(b"PGCOPY\n\xff\r\n\0")
    payload.extend(struct.pack(">ii", 0, 0))
    for row in rows:
        payload.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                payload.extend(struct.pack(">i", -1))
            else:
                payload.extend(struct.pack(">i", len(field)))
                payload.extend(field)
    payload.extend(struct.pack(">h", -1))
    return bytes(payload)

def _read_uvarint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, offset
        shift += 7


def _decode_by_code_groups(
    encoded_group_bytes: bytes, entry_count: int
) -> list[tuple[int, list[int], list[int]]]:
    assert encoded_group_bytes[0] == 2
    source_count, offset = _read_uvarint(encoded_group_bytes, 1)
    source_bits = encoded_group_bytes[offset]
    offset += 1
    provider_set_key = 0
    groups = []
    for _entry_index in range(entry_count):
        provider_delta, offset = _read_uvarint(encoded_group_bytes, offset)
        provider_set_key += provider_delta
        price_count, offset = _read_uvarint(encoded_group_bytes, offset)
        price_keys = []
        for _price_index in range(price_count):
            price_key, offset = _read_uvarint(encoded_group_bytes, offset)
            price_keys.append(price_key)
        source_bytes = (price_count * source_bits + 7) // 8
        packed_sources = encoded_group_bytes[offset : offset + source_bytes]
        offset += source_bytes
        if source_bits == 0:
            source_keys = [0] * price_count
        else:
            packed_value = int.from_bytes(packed_sources, "little")
            source_mask = (1 << source_bits) - 1
            source_keys = [
                (packed_value >> (index * source_bits)) & source_mask
                for index in range(price_count)
            ]
        assert all(source_key < source_count for source_key in source_keys)
        groups.append((provider_set_key, price_keys, source_keys))
    assert offset == len(encoded_group_bytes)
    return groups


def _fixture_provider_references(repeated_rate_occurrences: bool) -> list[dict]:
    provider_references = [
        {
            "provider_group_id": 1,
            "provider_groups": [
                {
                    "npi": [1234567890, 1234567891],
                    "tin": {"type": "ein", "value": "12-3456789"},
                }
            ],
        }
    ]
    if repeated_rate_occurrences:
        provider_references.append(
            {
                "provider_group_id": 2,
                "provider_groups": [
                    {
                        "npi": [1234567892],
                        "tin": {"type": "ein", "value": "98-7654321"},
                    }
                ],
            }
        )
    return provider_references


def _network_rate_fixture(code: str, negotiated_rate: float) -> dict:
    return {
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": code,
        "negotiation_arrangement": " fFs ",
        "negotiated_rates": [
            {
                "provider_references": [1],
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": negotiated_rate,
                        "service_code": ["11"],
                        "billing_class": "professional",
                    }
                ],
            }
        ],
    }


def _fixture_network_entries(
    *,
    multiple_prices: bool,
    duplicate_first_price: bool,
    repeated_rate_occurrences: bool,
) -> list[dict]:
    in_network_entries = [_network_rate_fixture("99213", 125.5)]
    if duplicate_first_price:
        prices = in_network_entries[0]["negotiated_rates"][0]["negotiated_prices"]
        prices.append(dict(prices[0]))
    if repeated_rate_occurrences:
        first_rate = in_network_entries[0]["negotiated_rates"][0]
        in_network_entries[0]["negotiated_rates"].extend(
            [
                json.loads(json.dumps(first_rate)),
                {
                    **json.loads(json.dumps(first_rate)),
                    "provider_references": [2],
                },
            ]
        )
    if multiple_prices:
        in_network_entries.append(_network_rate_fixture("99214", 250))
    return in_network_entries


def _fixture_payload(
    *,
    provider_references_first: bool,
    multiple_prices: bool = False,
    duplicate_first_price: bool = False,
    repeated_rate_occurrences: bool = False,
) -> dict:
    """Support the fixture payload test fixture."""
    provider_references = _fixture_provider_references(repeated_rate_occurrences)
    in_network_entries = _fixture_network_entries(
        multiple_prices=multiple_prices,
        duplicate_first_price=duplicate_first_price,
        repeated_rate_occurrences=repeated_rate_occurrences,
    )
    if provider_references_first:
        return {
            "provider_references": provider_references,
            "in_network": in_network_entries,
        }
    return {
        "in_network": in_network_entries,
        "provider_references": provider_references,
    }


def _single_frame(frames: list[tuple[str, dict]], record_kind: str) -> dict:
    matches = [payload for kind, payload in frames if kind == record_kind]
    assert len(matches) == 1
    return matches[0]


def _load_isolated_shared_blocks():
    root = Path(__file__).resolve().parents[1]
    process_package = types.ModuleType("process")
    process_package.__path__ = [str(root / "process")]
    ptg_parts_package = types.ModuleType("process.ptg_parts")
    ptg_parts_package.__path__ = [str(root / "process" / "ptg_parts")]
    db_tables = types.ModuleType("process.ptg_parts.db_tables")
    db_tables._quote_ident = lambda value: str(value)
    module_name = "isolated_ptg2_shared_blocks"
    module_path = root / "process" / "ptg_parts" / "ptg2_shared_blocks.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    replacement_modules_by_name = {
        "process": process_package,
        "process.ptg_parts": ptg_parts_package,
        "process.ptg_parts.db_tables": db_tables,
        module_name: module,
    }
    previous_modules_by_name = {
        name: sys.modules.get(name) for name in replacement_modules_by_name
    }
    try:
        sys.modules.update(replacement_modules_by_name)
        spec.loader.exec_module(module)
    finally:
        for name, prior_module in previous_modules_by_name.items():
            if prior_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior_module
    return module
