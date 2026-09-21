# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical payload, key, and fingerprint helpers for candidate graphs."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from process.custom_import.definition import CustomImportDefinition, Field, canonical_json
from process.custom_import.family import RootFamily, normalize_source_decimal
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateFamily

_CANONICAL_PREFIX = b"custom-import/v1\x00candidate-runner/1\x00"


def fields_by_collection(definition: CustomImportDefinition) -> Mapping[str, tuple[Field, ...]]:
    """Group a definition's child fields by their declared collection."""

    fields_by_name: dict[str, list[Field]] = defaultdict(list)
    for field in definition.child_fields:
        assert field.collection is not None
        fields_by_name[field.collection].append(field)
    return {collection: tuple(field_list) for collection, field_list in fields_by_name.items()}


def family_children(family: RootFamily | StoredCandidateFamily) -> tuple[tuple[str, Mapping[str, Any]], ...]:
    """Return every child payload paired with its declared collection."""

    if isinstance(family, RootFamily):
        return tuple(
            (collection, child_values)
            for collection, collection_children in family.children.items()
            for child_values in collection_children
        )
    return tuple((child.collection, child.values_by_field) for child in family.children)


def family_child_payload_hashes(
    definition: CustomImportDefinition,
    family: RootFamily | StoredCandidateFamily,
) -> tuple[tuple[str, bytes], ...]:
    """Return source-payload digests without reserializing retained children."""

    if isinstance(family, RootFamily):
        return tuple(
            (collection, child_payload_hash(definition, collection, child_values))
            for collection, child_values in family_children(family)
        )
    return tuple((child.collection, bytes(child.child.payload_sha256)) for child in family.children)


def root_payload_hash(definition: CustomImportDefinition, family: RootFamily | StoredCandidateFamily) -> bytes:
    """Return the canonical root payload digest for a fresh or retained family."""

    if isinstance(family, RootFamily):
        return digest_text("root-payload", record_payload(definition.root_fields, family.root))
    return bytes(family.root_revision.payload_sha256)


def child_payload_hash(definition: CustomImportDefinition, collection: str, child_values: Mapping[str, Any]) -> bytes:
    """Return one canonical child payload digest."""

    return digest_text("child-payload", record_payload(fields_by_collection(definition)[collection], child_values))


def pack_hash(label: str, record_hashes: Sequence[bytes]) -> bytes:
    """Hash a deterministic pack membership sequence."""

    digest = hashlib.sha256(_CANONICAL_PREFIX + b"pack\x00" + label.encode("ascii") + b"\x00")
    for record_hash in sorted(record_hashes):
        digest.update(record_hash)
    return digest.digest()


def candidate_hash(
    *,
    execution_id: int,
    fence: int,
    base_generation_id: int | None,
    root_key_hashes: Sequence[bytes],
) -> bytes:
    """Fingerprint one worker attempt without claiming output authority."""

    # Keep the v1 canonical document bytes without building its potentially
    # large ``root_keys`` array.  This preserves persisted hash compatibility
    # while avoiding the definition JSON validator's aggregate-array cap.
    digest = _incremental_digest("candidate")
    _digest_text_fragment(digest, '{"base_generation_id":')
    _digest_text_fragment(digest, "null" if base_generation_id is None else str(base_generation_id))
    _digest_text_fragment(digest, ',"contract":"custom-import-candidate/v1","execution_id":')
    _digest_text_fragment(digest, str(execution_id))
    _digest_text_fragment(digest, ',"fence":')
    _digest_text_fragment(digest, str(fence))
    _digest_text_fragment(digest, ',"root_keys":[')
    for ordinal, root_key_hash_value in enumerate(sorted(root_key_hashes)):
        if ordinal:
            _digest_text_fragment(digest, ",")
        _digest_json_string(digest, root_key_hash_value.hex())
    _digest_text_fragment(digest, "]}")
    return digest.digest()


def new_family_hash(definition: CustomImportDefinition, family: RootFamily) -> bytes:
    """Hash one accepted root family and its exact typed child payloads."""

    # This is the same canonical JSON sequence previously passed to
    # ``canonical_json``.  Stream it into the digest so a valid large family
    # does not need one aggregate children document in memory or at the
    # definition parser's array limit.
    digest = _incremental_digest("family")
    _digest_text_fragment(digest, '{"children":{')
    collection_fields = fields_by_collection(definition)
    for collection_ordinal, collection_name in enumerate(
        sorted(collection.name for collection in definition.child_collections)
    ):
        if collection_ordinal:
            _digest_text_fragment(digest, ",")
        _digest_json_string(digest, collection_name)
        _digest_text_fragment(digest, ":[")
        child_documents = sorted(
            (
                child_key_hash(definition, collection_name, child_values),
                child_key_document(definition, collection_name, child_values),
                record_payload(collection_fields[collection_name], child_values),
            )
            for child_values in family.children[collection_name]
        )
        for child_ordinal, (_key_hash, child_key, child_payload_document) in enumerate(child_documents):
            if child_ordinal:
                _digest_text_fragment(digest, ",")
            _digest_text_fragment(digest, '{"key":')
            _digest_json_string(digest, child_key)
            _digest_text_fragment(digest, ',"payload":')
            _digest_json_string(digest, child_payload_document)
            _digest_text_fragment(digest, "}")
        _digest_text_fragment(digest, "]")
    _digest_text_fragment(digest, '},"contract":"custom-import-family/v1","root_key":')
    _digest_json_string(digest, root_key_document(definition, family.root))
    _digest_text_fragment(digest, ',"root_payload":')
    _digest_json_string(digest, record_payload(definition.root_fields, family.root))
    _digest_text_fragment(digest, "}")
    return digest.digest()


def _incremental_digest(domain: str) -> hashlib._Hash:
    """Start a runner digest whose canonical JSON will be streamed in chunks."""

    return hashlib.sha256(_CANONICAL_PREFIX + domain.encode("ascii") + b"\x00")


def _digest_text_fragment(digest: hashlib._Hash, fragment: str) -> None:
    """Append one UTF-8 JSON fragment with the runner's canonical error shape."""

    try:
        digest.update(fragment.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise CandidateRunnerError("candidate canonical value is not UTF-8") from exc


def _digest_json_string(digest: hashlib._Hash, value: str) -> None:
    """Append one canonical JSON string without retaining the outer document."""

    _digest_text_fragment(digest, json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":")))


def root_key_contract_hash(definition: CustomImportDefinition) -> bytes:
    """Return the stable root-key shape digest for one definition."""

    return digest_text(
        "root-key-contract",
        canonical_json({"contract": "custom-import-root-key/v1", "fields": list(definition.root_logical_key)}),
    )


def root_key_hash(definition: CustomImportDefinition, root_values_by_field: Mapping[str, Any]) -> bytes:
    """Hash one root logical identity."""

    return digest_text("root-key", root_key_document(definition, root_values_by_field))


def root_key_hash_from_tuple(definition: CustomImportDefinition, key_values: tuple[Any, ...]) -> bytes:
    """Hash the root-key tuple retained by family rejection evidence."""

    return digest_text("root-key", root_key_document_from_tuple(definition, key_values))


def root_key_evidence_from_tuple(
    definition: CustomImportDefinition,
    key_values: object,
) -> tuple[str, bytes] | None:
    """Return canonical root-key evidence only for a safely typed tuple."""

    if not isinstance(key_values, tuple):
        return None
    try:
        canonical_root_key = root_key_document_from_tuple(definition, key_values)
        return canonical_root_key, digest_text("root-key", canonical_root_key)
    except CandidateRunnerError:
        return None


def root_key_document(definition: CustomImportDefinition, root_values_by_field: Mapping[str, Any]) -> str:
    """Encode one root logical key using its typed field contracts."""

    try:
        key_values_by_field = {field_id: root_values_by_field[field_id] for field_id in definition.root_logical_key}
    except KeyError as exc:
        raise CandidateRunnerError("accepted family root key is incomplete") from exc
    return key_document(definition.root_logical_key, definition.fields_by_id, key_values_by_field)


def root_key_document_from_tuple(definition: CustomImportDefinition, key_values: tuple[Any, ...]) -> str:
    """Encode a rejection's bounded root-key tuple."""

    if len(key_values) != len(definition.root_logical_key):
        raise CandidateRunnerError("family rejection root key is malformed")
    return key_document(
        definition.root_logical_key,
        definition.fields_by_id,
        dict(zip(definition.root_logical_key, key_values, strict=True)),
    )


def child_key_hash(
    definition: CustomImportDefinition,
    collection: str,
    child_values_by_field: Mapping[str, Any],
) -> bytes:
    """Hash one declared child identity."""

    return digest_text("child-key", child_key_document(definition, collection, child_values_by_field))


def child_key_document(
    definition: CustomImportDefinition,
    collection: str,
    child_values_by_field: Mapping[str, Any],
) -> str:
    """Encode one child key using the collection's typed shape."""

    child_collection = definition.collections_by_name[collection]
    return key_document(child_collection.child_key, definition.fields_by_id, child_values_by_field)


def key_document(
    field_ids: Sequence[str],
    fields_by_id: Mapping[str, Field],
    key_values_by_field: Mapping[str, Any],
) -> str:
    """Encode an ordered typed logical key without raw source structure."""

    try:
        encoded_fields = [
            {"field": field_id, "value": value_document(fields_by_id[field_id], key_values_by_field[field_id])}
            for field_id in field_ids
        ]
    except KeyError as exc:
        raise CandidateRunnerError("family key is incomplete") from exc
    return canonical({"contract": "custom-import-key/v1", "fields": encoded_fields})


def record_payload(fields: Sequence[Field], values_by_field: Mapping[str, Any]) -> str:
    """Encode a full typed source record, preserving missing versus null."""

    return canonical(
        {
            "contract": "custom-import-record/v1",
            "fields": [
                {
                    "field": field.field_id,
                    "value": {"state": "missing"}
                    if field.field_id not in values_by_field
                    else value_document(field, values_by_field[field.field_id]),
                }
                for field in fields
            ],
        }
    )


def value_document(field: Field, scalar_value: object) -> Mapping[str, object]:
    """Encode one accepted typed scalar in a canonical retained payload."""

    if scalar_value is None:
        return {"state": "null", "type": field.value_type}
    if field.value_type == "decimal":
        decimal_value = normalize_source_decimal(scalar_value)
        if decimal_value is None:
            raise CandidateRunnerError("accepted decimal value is not canonical")
        return {"state": "value", "type": "decimal", "value": format(decimal_value, "f")}
    if field.value_type == "date":
        if not isinstance(scalar_value, dt.date) or isinstance(scalar_value, dt.datetime):
            raise CandidateRunnerError("accepted date value is malformed")
        return {"state": "value", "type": "date", "value": scalar_value.isoformat()}
    if field.value_type == "timestamp":
        if not isinstance(scalar_value, dt.datetime) or scalar_value.tzinfo is None or scalar_value.utcoffset() is None:
            raise CandidateRunnerError("accepted timestamp value is malformed")
        try:
            timestamp_value = scalar_value.astimezone(dt.UTC)
        except OverflowError as exc:
            raise CandidateRunnerError("accepted timestamp value is malformed") from exc
        return {
            "state": "value",
            "type": "timestamp",
            "value": timestamp_value.isoformat().replace("+00:00", "Z"),
        }
    if field.value_type == "integer" and (isinstance(scalar_value, bool) or not isinstance(scalar_value, int)):
        raise CandidateRunnerError("accepted integer value is malformed")
    if field.value_type == "boolean" and not isinstance(scalar_value, bool):
        raise CandidateRunnerError("accepted boolean value is malformed")
    if field.value_type == "string" and not isinstance(scalar_value, str):
        raise CandidateRunnerError("accepted string value is malformed")
    return {"state": "value", "type": field.value_type, "value": scalar_value}


def canonical(document: Mapping[str, object]) -> str:
    """Canonicalize a runner-owned JSON-compatible document."""

    try:
        return canonical_json(document)
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise CandidateRunnerError("candidate canonical value is malformed") from exc


def digest_text(domain: str, canonical_document: str) -> bytes:
    """Hash one UTF-8 canonical document under the runner's private domain."""

    try:
        encoded_document = canonical_document.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CandidateRunnerError("candidate canonical value is not UTF-8") from exc
    return hashlib.sha256(_CANONICAL_PREFIX + domain.encode("ascii") + b"\x00" + encoded_document).digest()


def definition_digest(domain: str, canonical_document: str) -> bytes:
    """Match the definition module's durable domain-separated digests."""

    prefix = f"custom-import/v1\x00{domain}\x00".encode("ascii")
    return hashlib.sha256(prefix + canonical_document.encode("utf-8")).digest()


def payload_values(
    fields: Sequence[Field],
    canonical_payload: object,
    *,
    label: str,
) -> Mapping[str, object]:
    """Recover all typed retained values from a canonical record payload."""

    parsed_payload = parse_canonical_payload(canonical_payload, label)
    encoded_fields = parsed_payload.get("fields")
    if not isinstance(encoded_fields, list) or len(encoded_fields) != len(fields):
        raise CandidateRunnerError(f"{label} fields do not match the definition")
    values_by_field: dict[str, object] = {}
    for field, encoded_field in zip(fields, encoded_fields, strict=True):
        decoded_value = payload_field_value(field, encoded_field, label)
        if decoded_value is not _MISSING:
            values_by_field[field.field_id] = decoded_value
    return values_by_field


_MISSING = object()


def parse_canonical_payload(canonical_payload: object, label: str) -> Mapping[str, object]:
    """Load and verify one runner-owned canonical record document."""

    if not isinstance(canonical_payload, str):
        raise CandidateRunnerError(f"{label} is not canonical text")
    try:
        parsed_payload = json.loads(canonical_payload)
        if canonical_json(parsed_payload) != canonical_payload:
            raise ValueError("not canonical")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CandidateRunnerError(f"{label} is malformed") from exc
    if not isinstance(parsed_payload, Mapping) or parsed_payload.get("contract") != "custom-import-record/v1":
        raise CandidateRunnerError(f"{label} has an unknown contract")
    return parsed_payload


def payload_field_value(field: Field, encoded_field: object, label: str) -> object:
    """Decode one field from a canonical record payload."""

    if not isinstance(encoded_field, Mapping) or encoded_field.get("field") != field.field_id:
        raise CandidateRunnerError(f"{label} field identity does not match the definition")
    encoded_value = encoded_field.get("value")
    if not isinstance(encoded_value, Mapping):
        raise CandidateRunnerError(f"{label} field value does not match the definition")
    state = encoded_value.get("state")
    if state == "missing":
        if not field.nullable:
            raise CandidateRunnerError(f"{label} omits a required field")
        return _MISSING
    if encoded_value.get("type") != field.value_type:
        raise CandidateRunnerError(f"{label} field value does not match the definition")
    if state == "null":
        if not field.nullable:
            raise CandidateRunnerError(f"{label} nulls a required field")
        return None
    if state != "value" or "value" not in encoded_value:
        raise CandidateRunnerError(f"{label} field value is malformed")
    return decode_payload_scalar(field, encoded_value["value"], label)


def decode_payload_scalar(field: Field, serialized_value: object, label: str) -> object:
    """Restore one date, timestamp, decimal, or primitive payload scalar."""

    if field.value_type == "decimal":
        decimal_value = normalize_source_decimal(serialized_value)
        if decimal_value is None:
            raise CandidateRunnerError(f"{label} decimal value is malformed")
        return decimal_value
    if field.value_type == "date":
        if not isinstance(serialized_value, str):
            raise CandidateRunnerError(f"{label} date value is malformed")
        try:
            return dt.date.fromisoformat(serialized_value)
        except ValueError as exc:
            raise CandidateRunnerError(f"{label} date value is malformed") from exc
    if field.value_type == "timestamp":
        if not isinstance(serialized_value, str):
            raise CandidateRunnerError(f"{label} timestamp value is malformed")
        try:
            timestamp_value = dt.datetime.fromisoformat(serialized_value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CandidateRunnerError(f"{label} timestamp value is malformed") from exc
        if timestamp_value.tzinfo is None or timestamp_value.utcoffset() is None:
            raise CandidateRunnerError(f"{label} timestamp value is malformed")
        try:
            return timestamp_value.astimezone(dt.UTC)
        except OverflowError as exc:
            raise CandidateRunnerError(f"{label} timestamp value is malformed") from exc
    if field.value_type == "string" and not isinstance(serialized_value, str):
        raise CandidateRunnerError(f"{label} string value is malformed")
    if field.value_type == "integer" and (isinstance(serialized_value, bool) or not isinstance(serialized_value, int)):
        raise CandidateRunnerError(f"{label} integer value is malformed")
    if field.value_type == "boolean" and not isinstance(serialized_value, bool):
        raise CandidateRunnerError(f"{label} boolean value is malformed")
    return serialized_value
