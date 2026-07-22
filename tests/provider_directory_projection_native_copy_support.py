# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic native-v2 summary and PostgreSQL binary COPY builders."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import hashlib
import json
import struct
from typing import Any, Mapping, Sequence

from process.provider_directory_projection_contract import canonical_row_digest
from process.provider_directory_projection_copy_summary import (
    NATIVE_CANONICAL_ROW_CONTRACT_ID,
    NATIVE_COPY_COLUMN_CONTRACT_ID,
    NATIVE_COPY_CONTRACT_ID,
    NATIVE_COPY_MAGIC,
    NATIVE_COPY_SPOOL_CONTRACT_ID,
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_json import (
    canonical_exact_json,
    decoded_fhir_object,
    exactly_decoded_object,
)
from process.provider_directory_projection_stage import STAGE_COPY_COLUMNS
from process.provider_directory_projection_transform import projection_resource_row
from process.provider_directory_projection_types import stable_json
from tests.provider_directory_projection_materializer_support import (
    canonical_json_bytes,
    resource_counts,
)


_PGCOPY_HEADER = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"
_PGCOPY_TRAILER = b"\xff\xff"
_COPY_FIELD_KINDS = (
    "text",
    "text",
    "text",
    "text",
    "text",
    "jsonb",
    "text",
    "int8",
    "int4",
    "bool",
    "bool",
    "int4",
    "int4",
    "bool",
    "text",
    "text",
    "text",
    "jsonb",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_input_bytes(fixture: Any) -> bytes:
    return b"".join(
        canonical_json_bytes(resource_map) + b"\n"
        for resource_map in fixture.resources
    )


def _jsonb_field(payload: bytes | None) -> bytes | None:
    return None if payload is None else b"\x01" + payload


def _encoded_field(field_content: Any, field_kind: str) -> bytes | None:
    if field_content is None:
        return None
    if field_kind == "jsonb":
        if isinstance(field_content, bytes):
            return _jsonb_field(field_content)
        return _jsonb_field(stable_json(field_content).encode("utf-8"))
    if field_kind == "int8":
        return struct.pack(">q", int(field_content))
    if field_kind == "int4":
        return struct.pack(">i", int(field_content))
    if field_kind == "bool":
        return b"\x01" if field_content else b"\x00"
    return str(field_content).encode("utf-8")


def _copy_field(field_content: Any, field_kind: str = "text") -> bytes:
    encoded_content = _encoded_field(field_content, field_kind)
    if encoded_content is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(encoded_content)) + encoded_content


def _projection_rows(context: Any) -> tuple[dict[str, Any], ...]:
    projection_rows = []
    for input_ordinal, resource_map in enumerate(context.fixture.resources):
        payload_json = canonical_json_bytes(resource_map)
        projection_row_map = projection_resource_row(
            resource_map,
            claim=context.claim,
            input_ordinal=input_ordinal,
            payload_hash=_sha256(payload_json),
        )
        projection_row_map["payload_json"] = payload_json
        projection_rows.append(projection_row_map)
    return tuple(
        sorted(
            projection_rows,
            key=lambda row_map: (
                row_map["resource_type"],
                row_map["resource_id"],
                row_map["source_rank"],
                row_map["payload_hash"],
            ),
        )
    )


def decoded_fixture_resources(fixture: Any) -> tuple[dict[str, Any], ...]:
    """Decode the exact retained bytes with lossless FHIR-number handling."""

    if fixture.framing == "ndjson":
        return tuple(
            decoded_fhir_object(encoded_line)
            for encoded_line in fixture.encoded.splitlines()
            if encoded_line.strip()
        )
    bundle_map = decoded_fhir_object(fixture.encoded)
    return tuple(
        dict(entry_map["resource"])
        for entry_map in bundle_map.get("entry", ())
    )


def _exact_fixture_payloads(fixture: Any) -> tuple[bytes, ...]:
    if fixture.framing == "ndjson":
        return tuple(
            canonical_exact_json(exactly_decoded_object(encoded_line))
            for encoded_line in fixture.encoded.splitlines()
            if encoded_line.strip()
        )
    exact_bundle = exactly_decoded_object(fixture.encoded)
    return tuple(
        canonical_exact_json(entry_map["resource"])
        for entry_map in exact_bundle.get("entry", ())
    )


def golden_projection_rows(context: Any) -> tuple[dict[str, Any], ...]:
    """Project exact input bytes through the Python semantic reference oracle."""

    projected_row_maps = []
    resource_maps = decoded_fixture_resources(context.fixture)
    exact_payloads = _exact_fixture_payloads(context.fixture)
    for input_ordinal, (resource_map, payload_json) in enumerate(
        zip(resource_maps, exact_payloads, strict=True)
    ):
        row_map = projection_resource_row(
            resource_map,
            claim=context.claim,
            input_ordinal=input_ordinal,
            payload_hash=_sha256(payload_json),
        )
        projected_row_maps.append(
            {
                "physical_projection_id": (
                    context.claim.recipe_lease.recipe.recipe_id
                ),
                **row_map,
                "payload_json": resource_map,
            }
        )
    return tuple(
        sorted(
            projected_row_maps,
            key=lambda row_map: (
                row_map["resource_type"],
                row_map["resource_id"],
                row_map["source_rank"],
                row_map["payload_hash"],
            ),
        )
    )


def _decoded_copy_field(encoded_field: bytes | None, field_kind: str) -> Any:
    if encoded_field is None:
        return None
    if field_kind == "text":
        return encoded_field.decode("utf-8")
    if field_kind == "jsonb":
        if not encoded_field.startswith(b"\x01"):
            raise ValueError("PGCOPY JSONB version is invalid")
        return json.loads(
            encoded_field[1:],
            parse_float=Decimal,
        )
    if field_kind == "int8" and len(encoded_field) == 8:
        return struct.unpack(">q", encoded_field)[0]
    if field_kind == "int4" and len(encoded_field) == 4:
        return struct.unpack(">i", encoded_field)[0]
    if field_kind == "bool" and encoded_field in {b"\x00", b"\x01"}:
        return encoded_field == b"\x01"
    raise ValueError(f"PGCOPY {field_kind} field is invalid")


def decode_native_copy_rows(copy_stream: bytes) -> tuple[dict[str, Any], ...]:
    """Strictly decode the test-visible logical values of all 18 COPY fields."""

    if not copy_stream.startswith(_PGCOPY_HEADER):
        raise ValueError("PGCOPY header is invalid")
    offset = len(_PGCOPY_HEADER)
    decoded_row_maps = []
    while True:
        if offset + 2 > len(copy_stream):
            raise ValueError("PGCOPY row header is truncated")
        field_count = struct.unpack(">h", copy_stream[offset : offset + 2])[0]
        offset += 2
        if field_count == -1:
            break
        if field_count != len(STAGE_COPY_COLUMNS):
            raise ValueError("PGCOPY field count is invalid")
        decoded_fields = []
        for field_kind in _COPY_FIELD_KINDS:
            if offset + 4 > len(copy_stream):
                raise ValueError("PGCOPY field length is truncated")
            field_length = struct.unpack(">i", copy_stream[offset : offset + 4])[0]
            offset += 4
            if field_length == -1:
                encoded_field = None
            elif field_length < 0 or offset + field_length > len(copy_stream):
                raise ValueError("PGCOPY field payload is invalid")
            else:
                encoded_field = copy_stream[offset : offset + field_length]
                offset += field_length
            decoded_fields.append(_decoded_copy_field(encoded_field, field_kind))
        decoded_row_maps.append(
            dict(zip(STAGE_COPY_COLUMNS, decoded_fields, strict=True))
        )
    if offset != len(copy_stream):
        raise ValueError("PGCOPY stream has trailing bytes")
    return tuple(decoded_row_maps)


def _copy_row(context: Any, row_map: Mapping[str, Any]) -> bytes:
    field_specs = (
        (context.claim.recipe_lease.recipe.recipe_id, "text"),
        (row_map["resource_type"], "text"),
        (row_map["resource_id"], "text"),
        (context.claim.shard.partition_id, "text"),
        (row_map["payload_hash"], "text"),
        (row_map["payload_json"], "jsonb"),
        (row_map["source_rank"], "text"),
        (row_map["summary_npi"], "int8"),
        (row_map["summary_address_count"], "int4"),
        (row_map["summary_addressed_location"], "bool"),
        (row_map["summary_geocoded_location"], "bool"),
        (row_map["summary_network_link_count"], "int4"),
        (row_map["summary_affiliation_link_count"], "int4"),
        (row_map["active"], "bool"),
        (row_map["effective_start"], "text"),
        (row_map["effective_end"], "text"),
        (row_map["observed_at"], "text"),
        (row_map["profile_evidence_json"], "jsonb"),
    )
    return struct.pack(">h", len(field_specs)) + b"".join(
        _copy_field(field_content, field_kind)
        for field_content, field_kind in field_specs
    )


def native_copy_stream(context: Any) -> tuple[bytes, tuple[dict[str, Any], ...]]:
    """Return a valid PGCOPY stream and its golden-oracle projection rows."""

    projection_rows = _projection_rows(context)
    copy_stream = (
        _PGCOPY_HEADER
        + b"".join(_copy_row(context, row_map) for row_map in projection_rows)
        + _PGCOPY_TRAILER
    )
    return copy_stream, projection_rows


def _summary_map(
    context: Any,
    copy_stream: bytes,
    projection_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    canonical_input = canonical_input_bytes(context.fixture)
    canonical_row_sha256, row_count = canonical_row_digest(projection_rows)
    first_row = projection_rows[0]
    last_row = projection_rows[-1]
    return {
        "record_kind": "provider_directory_projection_copy_summary",
        "contract_id": NATIVE_COPY_SPOOL_CONTRACT_ID,
        "decoder_contract_id": NATIVE_DECODER_CONTRACT_ID,
        "transform_contract_id": NATIVE_TRANSFORM_CONTRACT_ID,
        "copy_contract_id": NATIVE_COPY_CONTRACT_ID,
        "copy_column_contract_id": NATIVE_COPY_COLUMN_CONTRACT_ID,
        "canonical_row_contract_id": NATIVE_CANONICAL_ROW_CONTRACT_ID,
        "input_framing": context.fixture.framing,
        "recipe_id": context.claim.recipe_lease.recipe.recipe_id,
        "partition_id": context.claim.shard.partition_id,
        "partition_ordinal": context.claim.shard.partition_ordinal,
        "input_byte_count": len(context.fixture.encoded),
        "input_sha256": _sha256(context.fixture.encoded),
        "canonical_input_byte_count": len(canonical_input),
        "canonical_input_sha256": _sha256(canonical_input),
        "resource_count": row_count,
        "resource_counts": resource_counts(context.fixture.resources),
        "first_identity": [first_row["resource_type"], first_row["resource_id"]],
        "last_identity": [last_row["resource_type"], last_row["resource_id"]],
        "canonical_row_sha256": canonical_row_sha256,
        "copy_byte_count": len(copy_stream),
        "copy_sha256": _sha256(copy_stream),
        "timings_seconds": {
            "input_read_seconds": 0.0,
            "parse_seconds": 0.0,
            "transform_seconds": 0.0,
            "sort_seconds": 0.0,
            "copy_encode_seconds": 0.0,
            "total_before_stdout_seconds": 0.0,
        },
    }


def native_copy_spool(
    context: Any,
    *,
    summary_overrides: Mapping[str, Any] | None = None,
    copy_stream: bytes | None = None,
) -> bytes:
    """Build one exact native-v2 spool with optional adversarial overrides."""

    default_copy_stream, projection_rows = native_copy_stream(context)
    emitted_copy_stream = default_copy_stream if copy_stream is None else copy_stream
    summary_map = _summary_map(context, default_copy_stream, projection_rows)
    summary_map.update(dict(summary_overrides or {}))
    summary_payload = json.dumps(
        summary_map,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return (
        NATIVE_COPY_MAGIC
        + struct.pack(">I", len(summary_payload))
        + summary_payload
        + struct.pack(">Q", len(emitted_copy_stream))
        + emitted_copy_stream
        + b"\xff"
    )


class BinaryCopyTransaction:
    """Capture exact bytes consumed by a fake asyncpg binary COPY call."""

    def __init__(self, copied_record_count: int) -> None:
        self.copied_record_count = copied_record_count
        self.copy_bytes = bytearray()
        self.copy_options_map: dict[str, Any] = {}

    async def copy_to_table(self, table_name: str, **copy_options: Any) -> str:
        self.copy_options_map = {"table_name": table_name, **copy_options}
        async for copy_chunk in copy_options["source"]:
            self.copy_bytes.extend(copy_chunk)
        return f"COPY {self.copied_record_count}"


class InputPipe:
    """Capture exact bytes pumped into a fake native child process."""

    def __init__(self) -> None:
        self.payload = bytearray()
        self.closed = False

    def write(self, chunk: bytes) -> None:
        self.payload.extend(chunk)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class FakeProcess:
    """Expose deterministic stdin, stdout, and stderr subprocess pipes."""

    def __init__(self, spool: bytes) -> None:
        self.stdin = InputPipe()
        self.stdout = stream_reader(spool)
        self.stderr = stream_reader(b"")
        self.returncode = 0

    async def wait(self) -> int:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9


def stream_reader(payload: bytes) -> asyncio.StreamReader:
    """Return an EOF-terminated asynchronous reader over exact spool bytes."""

    reader = asyncio.StreamReader()
    reader.feed_data(payload)
    reader.feed_eof()
    return reader


async def raw_census(byte_count: int, payload_sha256: str) -> tuple[int, str]:
    """Return one independently measured retained-byte census."""

    return byte_count, payload_sha256


__all__ = (
    "BinaryCopyTransaction",
    "FakeProcess",
    "canonical_input_bytes",
    "decode_native_copy_rows",
    "decoded_fixture_resources",
    "golden_projection_rows",
    "native_copy_spool",
    "native_copy_stream",
    "raw_census",
    "stream_reader",
)
