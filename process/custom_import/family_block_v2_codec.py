# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact v2 positional block codec and inline root scalar helpers.

Compact v2 keeps the exact existing semantic ``family_sha256`` and
``root_payload_sha256`` commitments while eliminating per-record JSON text and
duplicated ``scalar_cells`` inside compressed fragments, omitting unused
``revision_run`` rows (``membership_count = 0``), and storing zero-child
inlineable root families directly in ``custom_import_block_family`` with
``fragment_count = 0``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
from collections.abc import Mapping
from compression import zstd
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from types import MappingProxyType

import msgpack

from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition, Field
from process.custom_import.family import _record_error, is_decimal_scalar_storage_valid, normalize_source_decimal
from process.custom_import.family_block_codec import (
    _DIRECTORY,
    _ENVELOPE_DOMAIN,
    _FRAGMENT,
    _LENGTH,
    EncodedFamily,
    FamilyBlockLimits,
    FamilyBlockManifest,
    FamilyBlockResourceLimit,
    VerifiedFamily,
    _check_family_digest,
    _child_wires,
    _compress,
    _Encoder,
    _frame_object,
    _indexed_fragment,
    _read_object,
)
from process.custom_import.family_block_records import (
    FamilyBlockError,
    FamilyRecord,
    _integer,
    _record_from_wire,
    _record_parts,
    _sha256,
)
from process.custom_import.family_block_scalar_layout import _KINDS, query_scalar_fields, scalar_column
from process.custom_import.family_block_storage_codec import StoredFamilyManifest
from process.custom_import.materialization import (
    MAX_SCALAR_INTEGER,
    MAX_SCALAR_STRING_UTF8_BYTES,
    MIN_SCALAR_INTEGER,
    TypedScalar,
    _typed_scalar,
)

COMPACT_V2_CONTRACT = "custom-import/family-block/v2"
INLINE_STORAGE_DOMAIN = b"custom-import/family-block/v2\x00inline\x00"
_V2_DIRECTORY_DOMAIN = b"custom-import/family-block-directory/v2\x00envelope\x00"
_MISSING_TAG: list[object] = []


def can_inline_root(definition: CustomImportDefinition) -> bool:
    """Return whether every declared root field is already in block_family or entity_binding."""
    query_field_ids = frozenset(definition.query.root_fields)
    all_root_ids = frozenset(field.field_id for field in definition.root_fields)
    return all_root_ids <= (query_field_ids | {definition.entity_field})


def needs_inline_entity_join(definition: CustomImportDefinition) -> bool:
    """Return whether inline root hydration or retention needs entity_binding.canonical_value."""
    return definition.entity_field not in definition.query.root_fields


def inline_storage_sha256(
    definition_digest: str,
    family_sha256: bytes,
    root_payload_sha256: bytes,
    root_revision_id: int,
) -> bytes:
    """Compute a deterministic storage commitment for a zero-fragment inline root family."""
    _sha256(family_sha256)
    _sha256(root_payload_sha256)
    _integer(root_revision_id, "root revision identity")
    digest = hashlib.sha256(INLINE_STORAGE_DOMAIN)
    digest.update(
        msgpack.packb((definition_digest, family_sha256, root_payload_sha256, root_revision_id), use_bin_type=True)
    )
    return digest.digest()


def v2_storage_envelope_sha256(codec_manifest: FamilyBlockManifest, fragment_directory) -> bytes:
    """Bind ordered fragment metadata to both existing hashes with zero revision runs."""
    digest = hashlib.sha256(_V2_DIRECTORY_DOMAIN)

    def _feed(entry) -> None:
        packed = msgpack.packb(entry, use_bin_type=True)
        digest.update(_LENGTH.pack(len(packed)))
        digest.update(packed)

    _feed(
        (
            codec_manifest.definition_sha256,
            codec_manifest.family_sha256,
            codec_manifest.storage_sha256,
            codec_manifest.retained,
            codec_manifest.child_count,
            codec_manifest.fragment_count,
            codec_manifest.raw_bytes,
            codec_manifest.stored_bytes,
        )
    )
    fragment_seen = 0
    for ordinal, directory_entry in enumerate(fragment_directory):
        if directory_entry[0] != ordinal or len(directory_entry) != 6:
            raise FamilyBlockError("fragment directory order differs")
        _feed(("fragment", *directory_entry))
        fragment_seen += 1
    if fragment_seen != codec_manifest.fragment_count:
        raise FamilyBlockError("fragment directory count differs")
    _feed(("complete", 0, codec_manifest.child_count + 1))
    return digest.digest()


class CompiledRecordSerializer:
    """Fast canonical JSON serializer matching ``runner_codec.record_payload`` byte-for-byte."""

    def __init__(self, fields: tuple[Field, ...]):
        self.fields = fields
        self.field_specs = tuple(
            (
                field.field_id,
                field.value_type,
                json.dumps(field.field_id, ensure_ascii=False, allow_nan=False),
                f'{{"field":{json.dumps(field.field_id, ensure_ascii=False, allow_nan=False)},"value":{{"state":"missing"}}}}',
                f'{{"field":{json.dumps(field.field_id, ensure_ascii=False, allow_nan=False)},"value":{{"state":"null","type":"{field.value_type}"}}}}',
                f'{{"field":{json.dumps(field.field_id, ensure_ascii=False, allow_nan=False)},"value":{{"state":"value","type":"{field.value_type}","value":',
            )
            for field in fields
        )

    def serialize_payload(self, fields_by_id: Mapping[str, object]) -> str:
        """Emit exact canonical ``custom-import-record/v1`` JSON text."""
        parts: list[str] = ['{"contract":"custom-import-record/v1","fields":[']
        for idx, (field_id, value_type, _quoted, missing_text, null_text, prefix_text) in enumerate(self.field_specs):
            if idx:
                parts.append(",")
            if field_id not in fields_by_id:
                parts.append(missing_text)
                continue
            scalar_entry = fields_by_id[field_id]
            if scalar_entry is None:
                parts.append(null_text)
                continue
            parts.append(prefix_text)
            parts.append(_encode_json_scalar(value_type, scalar_entry))
            parts.append("}}")
        parts.append("]}")
        return "".join(parts)

    def serialize_from_slots(self, slot_entries: list[object]) -> str:
        """Emit exact canonical ``custom-import-record/v1`` JSON text from positional v2 slots."""
        parts: list[str] = ['{"contract":"custom-import-record/v1","fields":[']
        for idx, ((_field_id, value_type, _quoted, missing_text, null_text, prefix_text), slot_entry) in enumerate(
            zip(self.field_specs, slot_entries, strict=True)
        ):
            if idx:
                parts.append(",")
            if slot_entry == _MISSING_TAG and type(slot_entry) is list:
                parts.append(missing_text)
                continue
            if slot_entry is None:
                parts.append(null_text)
                continue
            parts.append(prefix_text)
            parts.append(_encode_json_wire_scalar(value_type, slot_entry))
            parts.append("}}")
        parts.append("]}")
        return "".join(parts)


@lru_cache(maxsize=256)
def compiled_serializer(fields: tuple[Field, ...]) -> CompiledRecordSerializer:
    """Cache precompiled canonical JSON prefixes per immutable field tuple."""
    return CompiledRecordSerializer(fields)


def _encode_json_scalar(value_type: str, scalar_entry: object) -> str:
    if value_type == "string":
        return json.dumps(scalar_entry, ensure_ascii=False, allow_nan=False)
    if value_type == "integer":
        return str(scalar_entry)
    if value_type == "boolean":
        return "true" if scalar_entry else "false"
    if value_type == "decimal":
        normalized = normalize_source_decimal(scalar_entry)
        if normalized is None:
            raise FamilyBlockError("family decimal value is malformed")
        return f'"{format(normalized, "f")}"'
    if value_type == "date":
        return f'"{scalar_entry.isoformat()}"'
    return f'"{scalar_entry.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")}"'


def _encode_json_wire_scalar(value_type: str, slot_entry: object) -> str:
    if value_type == "string":
        return json.dumps(slot_entry, ensure_ascii=False, allow_nan=False)
    if value_type == "integer":
        return str(slot_entry)
    if value_type == "boolean":
        return "true" if slot_entry else "false"
    return f'"{slot_entry}"'


def _encode_slot_scalar(value_type: str, scalar_entry: object) -> object:
    if value_type == "decimal":
        normalized = normalize_source_decimal(scalar_entry)
        if normalized is None:
            raise FamilyBlockError("family decimal value is malformed")
        return format(normalized, "f")
    if value_type == "date":
        if not isinstance(scalar_entry, dt.date) or isinstance(scalar_entry, dt.datetime):
            raise FamilyBlockError("family date value is malformed")
        return scalar_entry.isoformat()
    if value_type == "timestamp":
        if not isinstance(scalar_entry, dt.datetime) or scalar_entry.tzinfo is None or scalar_entry.utcoffset() is None:
            raise FamilyBlockError("family timestamp value is malformed")
        return scalar_entry.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    return scalar_entry


def encode_slot_values(fields: tuple[Field, ...], fields_by_id: Mapping[str, object]) -> list[object]:
    """Encode validated typed field values into compact positional v2 msgpack slots."""
    slot_entries: list[object] = []
    for field in fields:
        field_id = field.field_id
        if field_id not in fields_by_id:
            slot_entries.append(_MISSING_TAG)
            continue
        scalar_entry = fields_by_id[field_id]
        slot_entries.append(None if scalar_entry is None else _encode_slot_scalar(field.value_type, scalar_entry))
    return slot_entries


def decode_slot_scalar(field: Field, slot_entry: object) -> object:
    """Decode and validate one non-missing, non-null positional v2 slot."""
    value_type = field.value_type
    if value_type == "string":
        if type(slot_entry) is not str:
            raise FamilyBlockError("v2 string slot is malformed")
        return slot_entry
    if value_type == "integer":
        if type(slot_entry) is not int:
            raise FamilyBlockError("v2 integer slot is malformed")
        return slot_entry
    if value_type == "boolean":
        if type(slot_entry) is not bool:
            raise FamilyBlockError("v2 boolean slot is malformed")
        return slot_entry
    if value_type == "decimal":
        if type(slot_entry) is not str:
            raise FamilyBlockError("v2 decimal slot is malformed")
        normalized = normalize_source_decimal(slot_entry)
        if normalized is None or format(normalized, "f") != slot_entry:
            raise FamilyBlockError("v2 decimal slot is not canonical")
        return normalized
    if value_type == "date":
        if type(slot_entry) is not str:
            raise FamilyBlockError("v2 date slot is malformed")
        parsed_date = dt.date.fromisoformat(slot_entry)
        if parsed_date.isoformat() != slot_entry:
            raise FamilyBlockError("v2 date slot is not canonical")
        return parsed_date
    if type(slot_entry) is not str:
        raise FamilyBlockError("v2 timestamp slot is malformed")
    parsed_ts = dt.datetime.fromisoformat(slot_entry.replace("Z", "+00:00"))
    if parsed_ts.tzinfo is None or parsed_ts.utcoffset() is None:
        raise FamilyBlockError("v2 timestamp slot lacks timezone")
    utc_ts = parsed_ts.astimezone(dt.UTC)
    if utc_ts.isoformat().replace("+00:00", "Z") != slot_entry:
        raise FamilyBlockError("v2 timestamp slot is not canonical")
    return utc_ts


def decode_slot_values(
    fields: tuple[Field, ...],
    slot_entries: object,
) -> tuple[Mapping[str, object], Mapping[str, TypedScalar]]:
    """Decode positional v2 msgpack slots into typed field values and projected scalars."""
    if type(slot_entries) is not list or len(slot_entries) != len(fields):
        raise FamilyBlockError("v2 slot vector shape differs")
    decoded_by_field: dict[str, object] = {}
    scalars_by_field: dict[str, TypedScalar] = {}
    for field, slot_entry in zip(fields, slot_entries, strict=True):
        if type(slot_entry) is list:
            if slot_entry:
                raise FamilyBlockError("v2 missing sentinel is malformed")
            continue
        if slot_entry is None:
            decoded_by_field[field.field_id] = None
            if field.projection_slot is not None:
                scalars_by_field[field.field_id] = TypedScalar(field.value_type, "null")
            continue
        decoded_scalar = decode_slot_scalar(field, slot_entry)
        decoded_by_field[field.field_id] = decoded_scalar
        if field.projection_slot is not None:
            scalars_by_field[field.field_id] = _typed_scalar(field, decoded_scalar)
    return MappingProxyType(decoded_by_field), MappingProxyType(scalars_by_field)


def scalars_from_v2_wire(fields: tuple[Field, ...], wire_entry: object) -> tuple[int, int, int, dict[str, TypedScalar]]:
    """Fast API hydration decode for a 4-element v2 positional record without JSON rebuild."""
    if type(wire_entry) is not list or len(wire_entry) != 4:
        raise FamilyBlockError("v2 record shape differs")
    revision_id, pack_id, source_ordinal, slot_entries = wire_entry
    _integer(revision_id, "revision identity")
    _integer(pack_id, "pack identity")
    _integer(source_ordinal, "source ordinal", zero=True)
    if type(slot_entries) is not list or len(slot_entries) != len(fields):
        raise FamilyBlockError("v2 slot vector shape differs")
    scalars_by_field: dict[str, TypedScalar] = {}
    for field, slot_entry in zip(fields, slot_entries, strict=True):
        if field.projection_slot is None:
            continue
        if type(slot_entry) is list:
            if slot_entry:
                raise FamilyBlockError("v2 missing sentinel is malformed")
            continue
        if slot_entry is None:
            scalars_by_field[field.field_id] = TypedScalar(field.value_type, "null")
            continue
        scalars_by_field[field.field_id] = _typed_scalar(field, decode_slot_scalar(field, slot_entry))
    return revision_id, pack_id, source_ordinal, scalars_by_field


def pack_staged_values(fields: tuple[Field, ...], fields_by_id: Mapping[str, object]) -> bytes:
    """Pack validated typed values into compact msgpack bytes for unlogged bucket staging."""
    return msgpack.packb(encode_slot_values(fields, fields_by_id), use_bin_type=True)


def _checked_slot_scalar_value(field: Field, slot_entry: object) -> object:
    """Validate and decode one non-null staged v2 slot once for direct scalar projection."""
    value_type = field.value_type
    if value_type == "string":
        if (
            type(slot_entry) is not str
            or "\x00" in slot_entry
            or len(slot_entry.encode("utf-8")) > MAX_SCALAR_STRING_UTF8_BYTES
        ):
            raise FamilyBlockError("v2 string slot is malformed")
        return slot_entry
    if value_type == "integer":
        if type(slot_entry) is not int or not MIN_SCALAR_INTEGER <= slot_entry <= MAX_SCALAR_INTEGER:
            raise FamilyBlockError("v2 integer slot is malformed")
        return slot_entry
    if value_type == "boolean":
        if type(slot_entry) is not bool:
            raise FamilyBlockError("v2 boolean slot is malformed")
        return slot_entry
    if value_type == "decimal":
        if type(slot_entry) is not str:
            raise FamilyBlockError("v2 decimal slot is malformed")
        normalized = normalize_source_decimal(slot_entry)
        if (
            normalized is None
            or format(normalized, "f") != slot_entry
            or not is_decimal_scalar_storage_valid(normalized)
        ):
            raise FamilyBlockError("v2 decimal slot is not canonical")
        return normalized
    return decode_slot_scalar(field, slot_entry)


def _direct_typed_scalar(field: Field, decoded_value: object) -> TypedScalar:
    value_type = field.value_type
    if value_type == "string":
        return TypedScalar("string", "value", string_value=decoded_value)
    if value_type == "integer":
        return TypedScalar("integer", "value", integer_value=decoded_value)
    if value_type == "decimal":
        return TypedScalar("decimal", "value", decimal_value=decoded_value)
    if value_type == "boolean":
        return TypedScalar("boolean", "value", boolean_value=decoded_value)
    if value_type == "date":
        return TypedScalar("date", "value", date_value=decoded_value)
    return TypedScalar("timestamp", "value", timestamp_value=decoded_value)


def pack_v2_slot_query_scalars(
    all_fields: tuple[Field, ...],
    query_slot_specs: tuple[tuple[int, Field], ...],
    packed_bytes: bytes,
) -> tuple[object, ...]:
    """Decode and pack query_input scalar cells directly from staged msgpack slots in one pass."""
    slot_entries = msgpack.unpackb(packed_bytes, raw=False, strict_map_key=True)
    if type(slot_entries) is not list or len(slot_entries) != len(all_fields):
        raise FamilyBlockError("v2 slot vector shape differs")
    states = bytearray((len(query_slot_specs) + 3) // 4)
    values: list[object] = []
    for query_idx, (slot_idx, field) in enumerate(query_slot_specs):
        slot_entry = slot_entries[slot_idx]
        if type(slot_entry) is list:
            if slot_entry:
                raise FamilyBlockError("v2 missing sentinel is malformed")
            values.append(None)
            continue
        if slot_entry is None:
            if not field.nullable:
                raise FamilyBlockError("v2 required slot cannot be null")
            states[query_idx // 4] |= 1 << (2 * (query_idx % 4))
            values.append(None)
            continue
        states[query_idx // 4] |= 2 << (2 * (query_idx % 4))
        values.append(_checked_slot_scalar_value(field, slot_entry))
    return bytes(states), None, *values


def unpack_staged_values(fields: tuple[Field, ...], packed_bytes: bytes) -> dict[str, object]:
    """Unpack staged positional msgpack bytes back into validated Python field values."""
    slot_entries = msgpack.unpackb(packed_bytes, raw=False, strict_map_key=True)
    decoded_by_field, _scalars_by_field = decode_slot_values(fields, slot_entries)
    return dict(decoded_by_field)


def extract_staged_membership_key(
    packed_bytes: bytes,
    expected_len: int,
    key_specs: tuple[tuple[int, str], ...],
) -> tuple[object, ...] | None:
    """Extract only the string/integer child-membership key tuple from staged msgpack slots."""
    slot_entries = msgpack.unpackb(packed_bytes, raw=False, strict_map_key=True)
    if type(slot_entries) is not list or len(slot_entries) != expected_len:
        raise FamilyBlockError("v2 slot vector shape differs")
    key_parts: list[object] = []
    for slot_index, value_type in key_specs:
        entry = slot_entries[slot_index]
        if type(entry) is list:
            if entry:
                raise FamilyBlockError("v2 missing sentinel is malformed")
            return None
        if entry is None:
            key_parts.append(None)
            continue
        expected_type = str if value_type == "string" else int
        if type(entry) is not expected_type:
            raise FamilyBlockError(f"v2 {value_type} slot is malformed")
        key_parts.append(entry)
    return tuple(key_parts)


def unpack_staged_values_and_scalars(
    fields: tuple[Field, ...], packed_bytes: bytes
) -> tuple[dict[str, object], dict[str, TypedScalar]]:
    """Unpack staged positional msgpack bytes into both typed values and projected scalars in one pass."""
    slot_entries = msgpack.unpackb(packed_bytes, raw=False, strict_map_key=True)
    if type(slot_entries) is not list or len(slot_entries) != len(fields):
        raise FamilyBlockError("v2 slot vector shape differs")
    decoded_by_field: dict[str, object] = {}
    scalars_by_field: dict[str, TypedScalar] = {}
    for field, slot_entry in zip(fields, slot_entries, strict=True):
        if type(slot_entry) is list:
            if slot_entry:
                raise FamilyBlockError("v2 missing sentinel is malformed")
            continue
        if slot_entry is None:
            decoded_by_field[field.field_id] = None
            if field.projection_slot is not None:
                if not field.nullable:
                    raise FamilyBlockError("v2 required slot cannot be null")
                scalars_by_field[field.field_id] = TypedScalar(field.value_type, "null")
            continue
        if field.projection_slot is not None:
            decoded_scalar = _checked_slot_scalar_value(field, slot_entry)
            decoded_by_field[field.field_id] = decoded_scalar
            scalars_by_field[field.field_id] = _direct_typed_scalar(field, decoded_scalar)
        else:
            decoded_by_field[field.field_id] = decode_slot_scalar(field, slot_entry)
    return decoded_by_field, scalars_by_field


def v2_record_parts(
    definition: CustomImportDefinition,
    collection: str | None,
    entry: FamilyRecord,
    *,
    retained: bool,
) -> tuple[list[object], tuple[bytes | None, str, str], int]:
    """Build compact v2 wire entry, canonical hash parts, and projected scalar count."""
    if type(entry) is not FamilyRecord:
        raise FamilyBlockError("family record type is malformed")
    if entry.sparse is not None:
        wire_cells, parts = _record_parts(
            definition,
            collection,
            entry,
            _collection_fields(definition, collection),
            _fields_map(definition, collection),
            retained,
        )
        return wire_cells, parts, len(wire_cells[4])
    _integer(entry.revision_id, "revision identity")
    _integer(entry.pack_id, "pack identity")
    _integer(entry.source_ordinal, "source ordinal", zero=True)
    fields = _collection_fields(definition, collection)
    fields_by_id_map = _fields_map(definition, collection)
    scalar_values = entry.values_by_field
    if not isinstance(scalar_values, Mapping) or _record_error(scalar_values, fields_by_id_map) is not None:
        raise FamilyBlockError("family values violate the field domain")
    slot_entries = encode_slot_values(fields, scalar_values)
    canonical_payload = compiled_serializer(fields).serialize_from_slots(slot_entries)
    if collection is None:
        key_hash: bytes | None = None
        key_text = runner_codec.root_key_document(definition, scalar_values)
    else:
        key_text = runner_codec.child_key_document(definition, collection, scalar_values)
        key_hash = runner_codec.digest_text("child-key", key_text)
    scalar_count = sum(1 for field in fields if field.projection_slot is not None and field.field_id in scalar_values)
    wire_cells = [entry.revision_id, entry.pack_id, entry.source_ordinal, slot_entries]
    return wire_cells, (key_hash, key_text, canonical_payload), scalar_count


def decode_wire_record(
    definition: CustomImportDefinition,
    collection: str | None,
    wire_entry: object,
    *,
    retained: bool,
) -> tuple[FamilyRecord, tuple[bytes | None, str, str]]:
    """Decode either a v1 (6-element) or compact v2 (4-element) wire record."""
    fields = _collection_fields(definition, collection)
    fields_by_id_map = _fields_map(definition, collection)
    if type(wire_entry) is list and len(wire_entry) == 6:
        return _record_from_wire(definition, collection, wire_entry, fields, fields_by_id_map, retained)
    if type(wire_entry) is not list or len(wire_entry) != 4:
        raise FamilyBlockError("family record shape differs")
    revision_id, pack_id, source_ordinal, slot_entries = wire_entry
    _integer(revision_id, "revision identity")
    _integer(pack_id, "pack identity")
    _integer(source_ordinal, "source ordinal", zero=True)
    decoded_by_field, scalars_by_field = decode_slot_values(fields, slot_entries)
    if _record_error(decoded_by_field, fields_by_id_map) is not None:
        raise FamilyBlockError("family values violate the field domain")
    canonical_payload = compiled_serializer(fields).serialize_from_slots(slot_entries)
    if collection is None:
        key_hash: bytes | None = None
        key_text = runner_codec.root_key_document(definition, decoded_by_field)
    else:
        key_text = runner_codec.child_key_document(definition, collection, decoded_by_field)
        key_hash = runner_codec.digest_text("child-key", key_text)
    decoded_entry = FamilyRecord(revision_id, pack_id, source_ordinal, decoded_by_field, scalars_by_field)
    return decoded_entry, (key_hash, key_text, canonical_payload)


def unpack_inline_root_scalars(
    definition: CustomImportDefinition,
    family_mapping: Mapping[str, object],
    *,
    entity_canonical_value: str | None = None,
) -> dict[str, TypedScalar]:
    """Reconstruct projected root scalars directly from ``custom_import_block_family`` columns."""
    raw_states = bytes(family_mapping["raw_states"])
    raw_overrides = family_mapping["raw_type_overrides"]
    override_bytes = None if raw_overrides is None else bytes(raw_overrides)
    query_fields = query_scalar_fields(definition)
    scalars_by_field: dict[str, TypedScalar] = {}
    for index, field in enumerate(query_fields):
        state_code = (raw_states[index // 4] >> (2 * (index % 4))) & 3
        if state_code == 0:
            continue
        override_code = 0 if override_bytes is None else (override_bytes[index // 2] >> (4 * (index % 2))) & 15
        actual_kind = _KINDS[override_code - 1] if override_code else field.value_type
        if state_code == 1:
            scalars_by_field[field.field_id] = TypedScalar(actual_kind, "null")
            continue
        if override_code:
            raise FamilyBlockError("inline root family cannot carry a typed override value")
        col_val = family_mapping[scalar_column(field)]
        if field.value_type == "decimal" and not isinstance(col_val, Decimal):
            col_val = Decimal(str(col_val))
        scalars_by_field[field.field_id] = _typed_scalar(field, col_val)
    if needs_inline_entity_join(definition):
        if type(entity_canonical_value) is not str:
            raise FamilyBlockError("inline root hydration requires entity canonical value")
        entity_field = definition.fields_by_id[definition.entity_field]
        scalars_by_field[definition.entity_field] = _typed_scalar(entity_field, entity_canonical_value)
    return scalars_by_field


def verify_inline_root_family(
    definition: CustomImportDefinition,
    family_mapping: Mapping[str, object],
    *,
    entity_canonical_value: str | None = None,
) -> tuple[dict[str, object], dict[str, TypedScalar]]:
    """Verify inline root scalar columns against payload, family, and storage commitments."""
    scalars_by_field = unpack_inline_root_scalars(
        definition, family_mapping, entity_canonical_value=entity_canonical_value
    )
    values_by_field = {
        field_id: (None if scalar.value_state == "null" else scalar.logical_value)
        for field_id, scalar in scalars_by_field.items()
    }
    canonical_payload = compiled_serializer(definition.root_fields).serialize_payload(values_by_field)
    actual_payload_hash = runner_codec.digest_text("root-payload", canonical_payload)
    if not hmac.compare_digest(actual_payload_hash, bytes(family_mapping["root_payload_sha256"])):
        raise FamilyBlockError("inline root values differ from root_payload_sha256")
    empty_children_by_name = {name: () for name in sorted(definition.collections_by_name)}
    actual_family_hash = runner_codec._family_hash_from_documents(definition, values_by_field, empty_children_by_name)
    if not hmac.compare_digest(actual_family_hash, bytes(family_mapping["family_sha256"])):
        raise FamilyBlockError("inline root values differ from family_sha256")
    expected_storage = inline_storage_sha256(
        definition.digest,
        bytes(family_mapping["family_sha256"]),
        bytes(family_mapping["root_payload_sha256"]),
        int(family_mapping["root_revision_id"]),
    )
    if not hmac.compare_digest(expected_storage, bytes(family_mapping["storage_sha256"])):
        raise FamilyBlockError("inline root storage digest differs from trusted evidence")
    return values_by_field, scalars_by_field


class InlineEncodedFamily:
    """Zero-fragment encoded representation for inlineable zero-child root families."""

    def __init__(
        self,
        manifest: StoredFamilyManifest,
        root_payload_sha256: bytes,
        root_scalar_count: int,
    ):
        self.stored_manifest = manifest
        self.root_payload_sha256 = root_payload_sha256
        self.root_scalar_count = root_scalar_count
        self.child_scalar_count = 0

    def indexed_fragments(self):
        """Yield zero compressed fragments for an inline root family."""
        return ()

    def close(self) -> None:
        """No temporary spool is held for an inline root family."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


class _V2Encoder(_Encoder):
    """Frame compact v2 envelope records using the v2 contract header."""

    def complete_v2(self, definition, family_sha256, child_count, *, retained):
        """Flush final v2 trailer, backfill header digest, and return codec manifest."""
        self.write(["complete", child_count])
        self.flush()
        if self.prefix_bytes:
            self.finish_prefix([COMPACT_V2_CONTRACT, definition.digest, family_sha256, retained])
        return FamilyBlockManifest(
            definition.digest,
            family_sha256,
            self.digest.digest(),
            retained,
            child_count,
            self.fragments,
            self.raw_bytes,
            self.stored_bytes,
        )


@dataclass(frozen=True)
class _EncodedRootInput:
    """Prepared root record and canonical hash parts for compact v2 encoding."""

    record: FamilyRecord
    wire_cells: list[object]
    canonical_parts: tuple[bytes | None, str, str]
    payload_sha256: bytes
    scalar_count: int


def _encode_inline_root(
    definition: CustomImportDefinition,
    root_input: _EncodedRootInput,
    children_by_collection: Mapping[str, object],
    expected_family_sha256: bytes | None,
) -> InlineEncodedFamily:
    """Encode an inlineable zero-child root family without creating fragments."""
    empty_children_by_name = {name: () for name in sorted(children_by_collection)}
    family_sha256 = _check_family_digest(
        definition,
        root_input.record.values_by_field,
        empty_children_by_name,
        expected_family_sha256,
        retained=False,
    )
    storage_hash = inline_storage_sha256(
        definition.digest, family_sha256, root_input.payload_sha256, root_input.record.revision_id
    )
    manifest = StoredFamilyManifest(definition.digest, family_sha256, storage_hash, False, 0, 0, 0, 0, 0)
    return InlineEncodedFamily(manifest, root_input.payload_sha256, root_input.scalar_count)


def _stream_v2_collection_docs(
    encoder: _V2Encoder,
    output: EncodedFamily,
    definition: CustomImportDefinition,
    collection_name: str,
    child_entries,
    count_by_collection: dict[str, int],
    *,
    retained: bool,
):
    """Write ordered child records for one collection and yield canonical hash parts."""
    encoder.write(["collection", collection_name])
    child_total = 0
    previous_parts = None
    for child_total, child_entry in enumerate(child_entries, 1):
        child_wire, child_parts, scalar_n = v2_record_parts(definition, collection_name, child_entry, retained=retained)
        if previous_parts is not None and child_parts < previous_parts:
            raise FamilyBlockError("family child documents are not ordered")
        encoder.write(["record", child_wire], is_record=True)
        output.child_scalar_count += scalar_n
        yield child_parts
        previous_parts = child_parts
    count_by_collection[collection_name] = child_total
    encoder.write(["end", child_total])


def _resolve_root_values_map(definition: CustomImportDefinition, root_input: _EncodedRootInput, *, retained: bool):
    if retained:
        return None
    if root_input.record.values_by_field is not None:
        return root_input.record.values_by_field
    return runner_codec.payload_values(definition.root_fields, root_input.canonical_parts[2], label="family root")


def _build_v2_stored_manifest(codec_manifest: FamilyBlockManifest, output: EncodedFamily) -> StoredFamilyManifest:
    storage_hash = v2_storage_envelope_sha256(
        codec_manifest, (directory for directory, _frame in output.indexed_fragments())
    )
    return StoredFamilyManifest(
        codec_manifest.definition_sha256,
        codec_manifest.family_sha256,
        storage_hash,
        codec_manifest.retained,
        codec_manifest.child_count,
        codec_manifest.fragment_count,
        codec_manifest.raw_bytes,
        codec_manifest.stored_bytes,
        0,
    )


def _encode_fragmented_v2(
    definition: CustomImportDefinition,
    root_input: _EncodedRootInput,
    children_by_collection: Mapping[str, object],
    expected_family_sha256: bytes | None,
    *,
    limits: FamilyBlockLimits,
    retained: bool,
) -> EncodedFamily:
    """Encode a family into compressed compact v2 fragments and return its manifest."""
    needs_digest = expected_family_sha256 is None and not retained
    placeholder = bytes(32) if needs_digest else expected_family_sha256
    _sha256(placeholder)
    output = EncodedFamily(limits)
    try:
        header_cells = [COMPACT_V2_CONTRACT, definition.digest, placeholder, retained]
        encoder = _V2Encoder(output, 0, prefix_bytes=len(_frame_object(header_cells)) if needs_digest else 0)
        encoder.write(header_cells)
        output.root_payload_sha256 = root_input.payload_sha256
        output.root_scalar_count = root_input.scalar_count
        output.child_scalar_count = 0
        encoder.write(root_input.wire_cells, is_record=True)
        root_values_map = _resolve_root_values_map(definition, root_input, retained=retained)
        count_by_collection: dict[str, int] = {}
        documents_by_collection = {
            name: _stream_v2_collection_docs(
                encoder, output, definition, name, children_by_collection[name], count_by_collection, retained=retained
            )
            for name in sorted(children_by_collection)
        }
        family_sha256 = _check_family_digest(
            definition, root_values_map, documents_by_collection, expected_family_sha256, retained=retained
        )
        codec_manifest = encoder.complete_v2(
            definition, family_sha256, sum(count_by_collection.values()), retained=retained
        )
        output.manifest = codec_manifest
        output.stored_manifest = _build_v2_stored_manifest(codec_manifest, output)
        return output
    except BaseException:
        output.close()
        raise


def encode_family_v2(
    definition: CustomImportDefinition,
    root: FamilyRecord,
    children_by_collection: Mapping[str, object],
    expected_family_sha256: bytes | None,
    *,
    limits: FamilyBlockLimits,
    retained: bool = False,
    total_child_count: int | None = None,
):
    """Encode a family in compact v2 format, inlining zero-child root-only families."""
    if type(definition) is not CustomImportDefinition or type(limits) is not FamilyBlockLimits:
        raise ValueError("family encoding requires a definition and resource bounds")
    if not isinstance(children_by_collection, Mapping) or set(children_by_collection) != set(
        definition.collections_by_name
    ):
        raise FamilyBlockError("family collections differ")
    root_wire, root_parts, root_scalar_count = v2_record_parts(definition, None, root, retained=retained)
    root_input = _EncodedRootInput(
        record=root,
        wire_cells=root_wire,
        canonical_parts=root_parts,
        payload_sha256=runner_codec.digest_text("root-payload", root_parts[2]),
        scalar_count=root_scalar_count,
    )
    if total_child_count == 0 and not retained and can_inline_root(definition):
        return _encode_inline_root(definition, root_input, children_by_collection, expected_family_sha256)
    return _encode_fragmented_v2(
        definition,
        root_input,
        children_by_collection,
        expected_family_sha256,
        limits=limits,
        retained=retained,
    )


class VerifiedV2Family(VerifiedFamily):
    """Fully checked compact v2 family whose replay handles both 4-cell v2 and 6-cell sparse records."""

    def decoded_records(self, collection=None):
        """Replay each verified record with its checked wire cells and canonical parts."""
        if collection is not None and collection not in self.definition.collections_by_name:
            raise FamilyBlockError("family collection differs")
        self._file.seek(0)
        _read_object(self._file, self.limits)
        root_wire = _read_object(self._file, self.limits)
        if collection is None:
            record, parts = decode_wire_record(self.definition, None, root_wire, retained=self.manifest.retained)
            yield record, root_wire, parts
            return
        for name in sorted(self.definition.collections_by_name):
            _read_object(self._file, self.limits)
            while True:
                item = _read_object(self._file, self.limits)
                if item[0] == "end":
                    break
                if name == collection:
                    record, parts = decode_wire_record(self.definition, name, item[1], retained=self.manifest.retained)
                    yield record, item[1], parts
            if name == collection:
                return


def _decompress_v2_fragment(frame: object, expected_ordinal: int, limits: FamilyBlockLimits) -> tuple[bytes, int]:
    if type(frame) is not bytes or len(frame) < _FRAGMENT.size:
        raise FamilyBlockError("v2 family fragment is malformed")
    ordinal, raw_size = _FRAGMENT.unpack(frame[: _FRAGMENT.size])
    if ordinal != expected_ordinal:
        raise FamilyBlockError("v2 family fragment order differs")
    if not raw_size:
        raise FamilyBlockError("v2 family fragment is empty")
    if raw_size > limits.fragment_bytes:
        raise FamilyBlockResourceLimit("v2 family fragment exceeds the caller's resource budget")
    window_log = min(31, max(10, (raw_size - 1).bit_length()))
    decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: window_log})
    try:
        raw_chunk = decoder.decompress(frame[_FRAGMENT.size :], max_length=raw_size + 1)
    except zstd.ZstdError as error:
        raise FamilyBlockError("v2 family compressed fragment is malformed") from error
    if len(raw_chunk) != raw_size or not decoder.eof or decoder.unused_data:
        raise FamilyBlockError("v2 family compressed fragment is incomplete or oversized")
    return raw_chunk, raw_size


def _stream_verify_v2_fragments(
    verified: VerifiedV2Family,
    fragment_rows,
    manifest: StoredFamilyManifest,
    check,
) -> FamilyBlockManifest:
    limits = verified.limits
    frame_digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    directories: list[tuple[int, int, int | None, int, int | None, int]] = []
    frag_count = raw_total = stored_total = 0
    for directory_entry, frame in fragment_rows:
        if check is not None:
            check()
        if type(frame) is not bytes or len(frame) < _FRAGMENT.size:
            raise FamilyBlockError("v2 family fragment is malformed")
        prefix = _LENGTH.pack(len(frame))
        stored_total += len(prefix) + len(frame)
        if stored_total > limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("v2 family exceeds the caller's spool budget")
        raw_chunk, raw_size = _decompress_v2_fragment(frame, frag_count, limits)
        raw_total += raw_size
        if max(raw_total, stored_total) > limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("v2 family exceeds the caller's spool budget")
        verified._file.write(raw_chunk)
        frame_digest.update(prefix)
        frame_digest.update(frame)
        directories.append(directory_entry)
        frag_count += 1
    if (frag_count, raw_total, stored_total) != (manifest.fragment_count, manifest.raw_bytes, manifest.stored_bytes):
        raise FamilyBlockError("v2 family fragment census differs")
    codec_manifest = manifest.codec_manifest(frame_digest.digest())
    expected_envelope = v2_storage_envelope_sha256(codec_manifest, directories)
    if not hmac.compare_digest(expected_envelope, manifest.storage_sha256):
        raise FamilyBlockError("v2 stored family envelope differs from trusted evidence")
    return codec_manifest


def verify_v2_fragments_and_records(
    definition: CustomImportDefinition,
    fragment_rows,
    manifest: StoredFamilyManifest,
    *,
    limits: FamilyBlockLimits,
    check=None,
    observe=None,
) -> VerifiedV2Family:
    """Verify a compact v2 multi-fragment family (``membership_count == 0``) in one bounded pass."""
    if manifest.fragment_count == 0:
        raise FamilyBlockError("inline zero-fragment families have no fragment stream")
    verified = VerifiedV2Family(limits)
    verified.definition = definition
    try:
        verified.manifest = _stream_verify_v2_fragments(verified, fragment_rows, manifest, check)
        _verify_v2_records(verified, check, observe)
        if check is not None:
            check()
        return verified
    except BaseException:
        verified.close()
        raise


def _verify_v2_records(verified: VerifiedFamily, check, observe) -> None:
    record_file, definition, manifest = verified._file, verified.definition, verified.manifest
    record_file.seek(0)
    header = _read_object(record_file, verified.limits)
    if (
        type(header) is not list
        or len(header) != 4
        or type(header[3]) is not bool
        or header != [COMPACT_V2_CONTRACT, definition.digest, manifest.family_sha256, manifest.retained]
    ):
        raise FamilyBlockError("v2 family header differs from trusted evidence")
    if check is not None:
        check()
    root_entry, root_parts = decode_wire_record(
        definition, None, _read_object(record_file, verified.limits), retained=manifest.retained
    )
    if observe is not None:
        observe(None, root_entry, root_parts)
    count_by_collection: dict[str, int] = {}

    def _read_next():
        return _read_object(record_file, verified.limits)

    def _documents(collection_name: str):
        child_total = 0
        previous_parts = None
        for child_total, child_wire in enumerate(_child_wires(_read_next, collection_name, check), 1):
            child_entry, child_parts = decode_wire_record(
                definition, collection_name, child_wire, retained=manifest.retained
            )
            if observe is not None:
                observe(collection_name, child_entry, child_parts)
            if previous_parts is not None and child_parts < previous_parts:
                raise FamilyBlockError("family child documents are not ordered")
            yield child_parts
            previous_parts = child_parts
        count_by_collection[collection_name] = child_total

    documents_by_collection = {name: _documents(name) for name in sorted(definition.collections_by_name)}
    _check_family_digest(
        definition,
        root_entry.values_by_field,
        documents_by_collection,
        manifest.family_sha256,
        retained=manifest.retained,
    )
    if sum(count_by_collection.values()) != manifest.child_count or _read_next() != ["complete", manifest.child_count]:
        raise FamilyBlockError("v2 family final count differs")
    if record_file.read(1):
        raise FamilyBlockError("v2 family contains trailing records")


class _NativeEncodedV2Family:
    """Zero-spool encoded compact v2 family produced from Rust raw fragments + Zstd."""

    def __init__(
        self,
        manifest: StoredFamilyManifest,
        root_payload_sha256: bytes,
        root_scalar_count: int,
        child_scalar_count: int,
        fragments: tuple[tuple[tuple[int, int, int | None, int, int | None, int], bytes], ...],
    ):
        self.stored_manifest = manifest
        self.root_payload_sha256 = root_payload_sha256
        self.root_scalar_count = root_scalar_count
        self.child_scalar_count = child_scalar_count
        self._fragments = fragments

    def indexed_fragments(self):
        """Yield pre-compressed fragments and directories."""
        return self._fragments

    def close(self) -> None:
        """No temporary spool is held for an in-memory native v2 family."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def compile_v2_family_plan(definition: CustomImportDefinition, limits: FamilyBlockLimits):
    """Compile the Rust compact v2 family hashing/framing plan when available."""
    try:
        import ptg2_address_canon
    except ImportError:
        return None
    builder = getattr(ptg2_address_canon, "custom_import_v2_family_plan_v1", None)
    if not callable(builder):
        return None
    fields_by_name = runner_codec.fields_by_collection(definition)
    root_specs = tuple(
        (field.field_id, field.value_type, field.projection_slot is not None) for field in definition.root_fields
    )
    col_specs = tuple(
        (
            name,
            tuple(
                (field.field_id, field.value_type, field.projection_slot is not None) for field in fields_by_name[name]
            ),
        )
        for name in sorted(fields_by_name)
    )
    return builder(
        (
            definition.digest,
            root_specs,
            col_specs,
            can_inline_root(definition),
            limits.fragment_bytes,
            limits.max_record_bytes,
            limits.max_spool_bytes,
        )
    )


def _compress_native_v2_fragments(
    raw_fragments,
) -> tuple[tuple[tuple[tuple[int, int, int | None, int, int | None, int], bytes], ...], bytes, int]:
    frame_digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    indexed_fragments: list[tuple[tuple[int, int, int | None, int, int | None, int], bytes]] = []
    stored_bytes = 0
    for ordinal, (raw_chunk, first_rec, rec_starts, first_off, continuation) in enumerate(raw_fragments):
        frame = _compress(ordinal, raw_chunk)
        prefix = _LENGTH.pack(len(frame))
        stored_bytes += len(prefix) + len(frame)
        frame_digest.update(prefix)
        frame_digest.update(frame)
        indexed_fragments.append(_indexed_fragment((first_rec, rec_starts, first_off, continuation), frame))
    return tuple(indexed_fragments), frame_digest.digest(), stored_bytes


def finalize_native_v2_family(
    definition: CustomImportDefinition,
    root_payload_sha256: bytes,
    child_count: int,
    native_entry: tuple,
    *,
    limits: FamilyBlockLimits,
) -> InlineEncodedFamily | _NativeEncodedV2Family | None:
    """Compress Rust-framed compact v2 fragments with Zstd and build its stored manifest."""
    kind = native_entry[0]
    if kind == "fallback":
        return None
    if kind == "inline":
        _, family_sha256, storage_sha256, root_scalar_count = native_entry
        manifest = StoredFamilyManifest(definition.digest, family_sha256, storage_sha256, False, 0, 0, 0, 0, 0)
        return InlineEncodedFamily(manifest, root_payload_sha256, root_scalar_count)
    _, family_sha256, root_scalar_count, child_scalar_count, raw_bytes, raw_fragments = native_entry
    indexed_fragments, envelope_hash, stored_bytes = _compress_native_v2_fragments(raw_fragments)
    if max(raw_bytes, stored_bytes + len(indexed_fragments) * _DIRECTORY.size) > limits.max_spool_bytes:
        raise FamilyBlockResourceLimit("family exceeds the caller's spool budget")
    codec_manifest = FamilyBlockManifest(
        definition.digest,
        family_sha256,
        envelope_hash,
        False,
        child_count,
        len(indexed_fragments),
        raw_bytes,
        stored_bytes,
    )
    storage_hash = v2_storage_envelope_sha256(codec_manifest, (directory for directory, _frame in indexed_fragments))
    stored_manifest = StoredFamilyManifest(
        codec_manifest.definition_sha256,
        codec_manifest.family_sha256,
        storage_hash,
        False,
        child_count,
        len(indexed_fragments),
        raw_bytes,
        stored_bytes,
        0,
    )
    return _NativeEncodedV2Family(
        stored_manifest, root_payload_sha256, root_scalar_count, child_scalar_count, indexed_fragments
    )


def _collection_fields(definition: CustomImportDefinition, collection: str | None) -> tuple[Field, ...]:
    if collection is None:
        return definition.root_fields
    return runner_codec.fields_by_collection(definition)[collection]


def _fields_map(definition: CustomImportDefinition, collection: str | None) -> dict[str, Field]:
    return {field.field_id: field for field in _collection_fields(definition, collection)}
