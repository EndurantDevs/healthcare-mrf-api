# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Record cells for the family codec; sparse storage is deliberately opaque."""

import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType

from process.custom_import import runner_codec
from process.custom_import.family import _record_error
from process.custom_import.materialization import TypedScalar, _validate_typed_scalar
from process.custom_import.runner_types import CandidateRunnerError


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


def _scalar_wire_value(kind, state, value):
    if state == "value":
        if kind == "decimal":
            return str(value)
        if kind in ("date", "timestamp"):
            return value.isoformat()
    return value


def _scalar_wire(scalars, fields):
    if not isinstance(scalars, Mapping) or any(name not in fields for name in scalars):
        raise FamilyBlockError("family scalar fields differ")
    scalar_cells = []
    for name in sorted(scalars):
        scalar = scalars[name]
        if type(scalar) is not TypedScalar:
            raise FamilyBlockError("family scalar type is malformed")
        value = _validate_typed_scalar(scalar)
        kind, state = scalar.field_type, scalar.value_state
        scalar_cells.append([name, kind, state, _scalar_wire_value(kind, state, value)])
    return scalar_cells


def _scalars_from_wire(cells, fields):
    if type(cells) is not list or len(cells) > len(fields):
        raise FamilyBlockError("family scalar population differs")
    scalars_by_name = {}
    previous = None
    for cell in cells:
        if type(cell) is not list or len(cell) != 4:
            raise FamilyBlockError("family scalar cell is malformed")
        name, kind, state, scalar_value = cell
        if type(name) is not str or name not in fields or name in scalars_by_name:
            raise FamilyBlockError("family scalar fields differ")
        if type(kind) is not str or type(state) is not str:
            raise FamilyBlockError("family scalar type is malformed")
        wire_value = scalar_value
        if state == "value":
            match kind:
                case "decimal" if type(scalar_value) is not str:
                    raise FamilyBlockError("family decimal cell is malformed")
                case "decimal":
                    scalar_value = Decimal(scalar_value)
                case "date":
                    scalar_value = date.fromisoformat(scalar_value)
                case "timestamp":
                    scalar_value = datetime.fromisoformat(scalar_value)
        typed_cell = {} if state == "null" and scalar_value is None else {f"{kind}_value": scalar_value}
        scalar = TypedScalar(kind, state, **typed_cell)
        _validate_typed_scalar(scalar)
        if (previous is not None and name <= previous) or _scalar_wire_value(kind, state, scalar_value) != wire_value:
            raise FamilyBlockError("family record encoding is not exact")
        previous = name
        scalars_by_name[name] = scalar
    return MappingProxyType(scalars_by_name)


def _fields(definition, collection):
    return definition.root_fields if collection is None else runner_codec.fields_by_collection(definition)[collection]


def record_parts(definition, collection, entry, *, retained):
    """Validate one family entry and return its wire cells and original hash fragments."""
    fields = _fields(definition, collection)
    return _record_parts(definition, collection, entry, fields, {field.field_id: field for field in fields}, retained)


def _key_parts(definition, collection, values, payload):
    if collection is None:
        return None, runner_codec.root_key_document(definition, values), payload
    key = runner_codec.child_key_document(definition, collection, values)
    return runner_codec.digest_text("child-key", key), key, payload


def _record_parts(definition, collection, entry, fields, by_name, retained, *, checked_scalar_cells=None):
    if type(entry) is not FamilyRecord:
        raise FamilyBlockError("family record type is malformed")
    _integer(entry.revision_id, "revision identity")
    _integer(entry.pack_id, "pack identity")
    _integer(entry.source_ordinal, "source ordinal", zero=True)
    scalars = _scalar_wire(entry.scalars, by_name) if checked_scalar_cells is None else checked_scalar_cells
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
        key_hash, key, record_payload = _key_parts(
            definition, collection, scalar_values, runner_codec.record_payload(fields, scalar_values)
        )
        extra_cells = None
    wire_cells = [entry.revision_id, entry.pack_id, entry.source_ordinal, record_payload, scalars, extra_cells]
    return wire_cells, (key_hash, key, record_payload)


def _projected_record_from_wire(wire, by_name, retained):
    if type(wire) is not list or len(wire) != 6:
        raise FamilyBlockError("family record shape differs")
    revision, pack, ordinal, payload_text, scalar_cells, sparse_cells = wire
    _integer(revision, "revision identity")
    _integer(pack, "pack identity")
    _integer(ordinal, "source ordinal", zero=True)
    if type(payload_text) is not str:
        raise FamilyBlockError("family text is malformed")
    if sparse_cells is not None and (not retained or type(sparse_cells) is not list or len(sparse_cells) != 5):
        raise FamilyBlockError("sparse family evidence shape differs")
    scalars = _scalars_from_wire(scalar_cells, by_name)
    return FamilyRecord(revision, pack, ordinal, None, scalars, None), None


def record_from_wire(definition, collection, wire, *, retained, include_payload=True):
    """Decode a stored role record while preserving its canonical scalar values."""
    fields = _fields(definition, collection)
    fields_by_name = {field.field_id: field for field in fields}
    if not include_payload:
        return _projected_record_from_wire(wire, fields_by_name, retained)
    return _record_from_wire(definition, collection, wire, fields, fields_by_name, retained)


def _record_from_wire(definition, collection, wire, fields, by_name, retained):
    if type(wire) is not list or len(wire) != 6:
        raise FamilyBlockError("family record shape differs")
    revision, pack, ordinal, payload_text, scalar_cells, sparse_cells = wire
    _text(payload_text)
    scalars = _scalars_from_wire(scalar_cells, by_name)
    label = "family block payload"
    try:
        if sparse_cells is None:
            decoded_payload = runner_codec._payload_values_from_document(fields, json.loads(payload_text), label)
            fields_by_id = MappingProxyType(decoded_payload)
            sparse = None
        else:
            if type(sparse_cells) is not list or len(sparse_cells) != 5:
                raise FamilyBlockError("sparse family evidence shape differs")
            sparse = SparseRecord(payload_text, *sparse_cells)
            fields_by_id = None
        family_entry = FamilyRecord(revision, pack, ordinal, fields_by_id, scalars, sparse)
        rebuilt, parts = _record_parts(
            definition,
            collection,
            family_entry,
            fields,
            by_name,
            retained,
            checked_scalar_cells=scalar_cells,
        )
        if sparse_cells is None and rebuilt[3] != payload_text:
            raise FamilyBlockError("canonical family payload does not round-trip")
    except ValueError, CandidateRunnerError:
        if sparse_cells is None:
            # The full typed round trip proves canonical bytes. Preserve parser errors on rejection.
            runner_codec.parse_canonical_payload(payload_text, label)
        raise
    if rebuilt != wire:
        raise FamilyBlockError("family record encoding is not exact")
    return family_entry, parts
