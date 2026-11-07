# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Record cells for the family codec; sparse storage is deliberately opaque."""

import hmac
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType

from process.custom_import import runner_codec
from process.custom_import.family import _record_error
from process.custom_import.materialization import TypedScalar, _validate_typed_scalar


class FamilyBlockError(ValueError):
    """Stored family bytes or their independently supplied evidence differ."""


@dataclass(frozen=True)
class SparseRecord:
    """Exact prior text and hashes, without an assumption that text is parseable."""

    payload_text: str
    key_text: str
    payload_sha256: bytes
    key_sha256: bytes
    parent_key_text: str | None = None
    parent_key_sha256: bytes | None = None


@dataclass(frozen=True)
class FamilyRecord:
    """Allocated identities and independent scalars; absent scalar keys stay absent.

    Sparse records use ``values_by_field=None``. Their original bytes must not be
    replaced by values parsed from them or by the independently stored scalars.
    """

    revision_id: int
    pack_id: int
    source_ordinal: int
    values_by_field: Mapping[str, object] | None
    scalars: Mapping[str, TypedScalar] = field(default_factory=dict)
    sparse: SparseRecord | None = None


def _integer(value, label, *, zero=False):
    if type(value) is not int or not (0 if zero else 1) <= value < 2**63:
        raise FamilyBlockError(f"family {label} is invalid")


def _sha256(value):
    if type(value) is not bytes or len(value) != 32:
        raise FamilyBlockError("family digest is malformed")


def _text(value):
    if type(value) is not str:
        raise FamilyBlockError("family text is malformed")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise FamilyBlockError("family text is not UTF-8") from error


def _scalar_wire(scalars, fields):
    if not isinstance(scalars, Mapping) or any(name not in fields for name in scalars):
        raise FamilyBlockError("family scalar fields differ")
    scalar_cells = []
    for name in sorted(scalars):
        scalar = scalars[name]
        if type(scalar) is not TypedScalar:
            raise FamilyBlockError("family scalar type is malformed")
        _validate_typed_scalar(scalar)
        value = scalar.logical_value
        if scalar.value_state == "value":
            if scalar.field_type == "decimal":
                value = str(value)
            elif scalar.field_type in ("date", "timestamp"):
                value = value.isoformat()
        scalar_cells.append([name, scalar.field_type, scalar.value_state, value])
    return scalar_cells


def _scalars_from_wire(cells, fields):
    if type(cells) is not list or len(cells) > len(fields):
        raise FamilyBlockError("family scalar population differs")
    scalars_by_name = {}
    for cell in cells:
        if type(cell) is not list or len(cell) != 4:
            raise FamilyBlockError("family scalar cell is malformed")
        name, kind, state, value = cell
        if type(name) is not str or name not in fields or name in scalars_by_name:
            raise FamilyBlockError("family scalar fields differ")
        if type(kind) is not str or type(state) is not str:
            raise FamilyBlockError("family scalar type is malformed")
        if state == "value":
            match kind:
                case "decimal" if type(value) is not str:
                    raise FamilyBlockError("family decimal cell is malformed")
                case "decimal":
                    value = Decimal(value)
                case "date":
                    value = date.fromisoformat(value)
                case "timestamp":
                    value = datetime.fromisoformat(value)
        values = {} if state == "null" and value is None else {f"{kind}_value": value}
        scalar = TypedScalar(kind, state, **values)
        _validate_typed_scalar(scalar)
        scalars_by_name[name] = scalar
    return MappingProxyType(scalars_by_name)


def _fields(definition, collection):
    return definition.root_fields if collection is None else runner_codec.fields_by_collection(definition)[collection]


def record_parts(definition, collection, entry, *, retained):
    """Validate one family entry and return its wire cells and original hash fragments."""
    if type(entry) is not FamilyRecord:
        raise FamilyBlockError("family record type is malformed")
    _integer(entry.revision_id, "revision identity")
    _integer(entry.pack_id, "pack identity")
    _integer(entry.source_ordinal, "source ordinal", zero=True)
    fields = _fields(definition, collection)
    by_name = {pack_record.field_id: pack_record for pack_record in fields}
    scalars = _scalar_wire(entry.scalars, by_name)
    sparse = entry.sparse
    if sparse is not None:
        if not retained or type(sparse) is not SparseRecord or entry.values_by_field is not None:
            raise FamilyBlockError("sparse records require explicit retained input")
        record_payload, key = sparse.payload_text, sparse.key_text
        _text(record_payload)
        _text(key)
        _sha256(sparse.payload_sha256)
        _sha256(sparse.key_sha256)
        if (sparse.parent_key_text is None) != (sparse.parent_key_sha256 is None):
            raise FamilyBlockError("sparse parent evidence is incomplete")
        if sparse.parent_key_text is not None:
            _text(sparse.parent_key_text)
            _sha256(sparse.parent_key_sha256)
        kind = "root" if collection is None else "child"
        if not hmac.compare_digest(runner_codec.digest_text(f"{kind}-payload", record_payload), sparse.payload_sha256):
            raise FamilyBlockError("sparse payload digest differs")
        if not hmac.compare_digest(runner_codec.digest_text(f"{kind}-key", key), sparse.key_sha256):
            raise FamilyBlockError("sparse key digest differs")
        if sparse.parent_key_text is not None and not hmac.compare_digest(
            runner_codec.digest_text("root-key", sparse.parent_key_text), sparse.parent_key_sha256
        ):
            raise FamilyBlockError("sparse parent key digest differs")
        extra_cells = [
            key,
            sparse.payload_sha256,
            sparse.key_sha256,
            sparse.parent_key_text,
            sparse.parent_key_sha256,
        ]
        key_hash = sparse.key_sha256
    else:
        scalar_values = entry.values_by_field
        if not isinstance(scalar_values, Mapping) or _record_error(scalar_values, by_name) is not None:
            raise FamilyBlockError("family values violate the field domain")
        record_payload = runner_codec.record_payload(fields, scalar_values)
        key = (
            runner_codec.root_key_document(definition, scalar_values)
            if collection is None
            else runner_codec.child_key_document(definition, collection, scalar_values)
        )
        key_hash = None if collection is None else runner_codec.digest_text("child-key", key)
        extra_cells = None
    wire_cells = [entry.revision_id, entry.pack_id, entry.source_ordinal, record_payload, scalars, extra_cells]
    return wire_cells, (key_hash, key, record_payload)


def record_from_wire(definition, collection, wire, *, retained):
    """Decode a stored role record while preserving its canonical scalar values."""
    if type(wire) is not list or len(wire) != 6:
        raise FamilyBlockError("family record shape differs")
    revision, pack, ordinal, payload, cells, extra = wire
    fields = _fields(definition, collection)
    _text(payload)
    scalars = _scalars_from_wire(cells, {item.field_id: item for item in fields})
    if extra is None:
        values = runner_codec.payload_values(fields, payload, label="family block payload")
        if runner_codec.record_payload(fields, values) != payload:
            raise FamilyBlockError("canonical family payload does not round-trip")
        sparse = None
        values = MappingProxyType(dict(values))
    else:
        if type(extra) is not list or len(extra) != 5:
            raise FamilyBlockError("sparse family evidence shape differs")
        sparse = SparseRecord(payload, *extra)
        values = None
    record = FamilyRecord(revision, pack, ordinal, values, scalars, sparse)
    rebuilt, parts = record_parts(definition, collection, record, retained=retained)
    if rebuilt != wire:
        raise FamilyBlockError("family record encoding is not exact")
    return record, parts
