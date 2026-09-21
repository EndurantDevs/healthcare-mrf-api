# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure v1 family admission and replacement semantics."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

from process.custom_import._source_text import _SourceTextValidationError, validate_snapshot_token
from process.custom_import.definition import (
    ChildCollection,
    CustomImportDefinition,
    DefinitionError,
    Field,
)

_NPI = re.compile(r"^[0-9]{10}$")
_DECIMAL_SOURCE_TEXT = re.compile(r"^-?[0-9]+(?:\.[0-9]+)?$", re.ASCII)

# These are the conservative scalar-storage limits used by the materialized
# projections.  Admission applies them to projected source fields so one bad
# family is rejected before later persistence can abort an entire generation.
MAX_SCALAR_STRING_UTF8_BYTES = 2_048
MAX_SCALAR_INTEGER = 9_223_372_036_854_775_807
MIN_SCALAR_INTEGER = -9_223_372_036_854_775_808
MAX_SCALAR_DECIMAL_INTEGER_DIGITS = 18
MAX_SCALAR_DECIMAL_SCALE = 12


class SourceSnapshotError(ValueError):
    """A source candidate has no single complete snapshot token."""


class CandidateRejected(ValueError):
    """A candidate-wide failure prevents publication or replacement."""


@dataclass(frozen=True)
class FamilyRejection:
    root_key: tuple[Any, ...] | None
    code: str


@dataclass(frozen=True)
class RootFamily:
    root_key: tuple[Any, ...]
    root: Mapping[str, Any]
    children: Mapping[str, tuple[Mapping[str, Any], ...]]


@dataclass(frozen=True)
class FamilyBuildResult:
    families: tuple[RootFamily, ...]
    rejections: tuple[FamilyRejection, ...]
    candidate_errors: tuple[str, ...]

    @property
    def is_candidate_rejected(self) -> bool:
        """Return whether a candidate-wide defect prevents any publication."""
        return bool(self.candidate_errors)


def validate_source_snapshot_tokens(
    definition: CustomImportDefinition,
    stream_tokens: Mapping[str, Sequence[str | None]],
) -> str:
    """Require every declared stream to observe one equal, non-empty token."""

    if not isinstance(stream_tokens, Mapping):
        raise SourceSnapshotError("snapshot token observations must be a mapping")
    expected_stream_ids = {stream.stream_id for stream in definition.source_streams}
    if set(stream_tokens) != expected_stream_ids:
        raise SourceSnapshotError("snapshot stream set does not match the definition")
    shared_tokens: set[str] = set()
    for stream_id, observed in stream_tokens.items():
        if not isinstance(observed, (list, tuple)) or not observed:
            raise SourceSnapshotError(f"stream {stream_id} has no bounded token observations")
        stream_values = {_validated_source_snapshot_token(stream_id, token) for token in observed}
        if len(stream_values) != 1:
            raise SourceSnapshotError(f"stream {stream_id} lacks one snapshot token")
        token = next(iter(stream_values))
        shared_tokens.add(token)
    if len(shared_tokens) != 1:
        raise SourceSnapshotError("source streams do not share one snapshot token")
    return next(iter(shared_tokens))


def _validated_source_snapshot_token(stream_id: str, value: Any) -> str:
    """Map the shared retained-token contract to family-admission diagnostics."""

    try:
        return validate_snapshot_token(value)
    except _SourceTextValidationError as exc:
        message_by_reason = {
            "not_string": "has a non-string snapshot token",
            "empty": "lacks one snapshot token",
            "invalid_utf8": "snapshot token must be valid UTF-8",
            "byte_limit": "snapshot token exceeds the byte limit",
            "non_printable": "snapshot token contains control characters or non-printable text",
        }
        message = f"stream {stream_id} {message_by_reason[exc.reason]}"
        raise SourceSnapshotError(message) from exc


def assemble_root_families(
    definition: CustomImportDefinition,
    roots: Sequence[Mapping[str, Any]],
    children_by_collection: Mapping[str, Sequence[Mapping[str, Any]]],
) -> FamilyBuildResult:
    """Admit only complete valid root families; retain precise rejection evidence."""
    _validate_family_inputs(definition, roots, children_by_collection)
    root_fields_by_id = {field.field_id: field for field in definition.root_fields}
    child_fields_by_collection = _child_fields_by_collection(definition)
    (
        root_records_by_key,
        rejection_codes_by_root_key,
        rejection_evidence_entries,
        root_candidate_error_codes,
    ) = _admit_root_records(
        definition,
        roots,
        root_fields_by_id,
    )
    child_records_by_root_and_collection, child_candidate_error_codes = _admit_child_records(
        definition,
        children_by_collection,
        root_records_by_key,
        child_fields_by_collection,
        rejection_codes_by_root_key,
    )
    rejection_evidence_entries.extend(
        FamilyRejection(root_key, rejection_code)
        for root_key in sorted(rejection_codes_by_root_key, key=repr)
        for rejection_code in sorted(rejection_codes_by_root_key[root_key])
    )
    return FamilyBuildResult(
        families=_assemble_accepted_families(
            definition,
            root_records_by_key,
            child_records_by_root_and_collection,
            rejection_codes_by_root_key,
        ),
        rejections=tuple(rejection_evidence_entries),
        candidate_errors=tuple(sorted(root_candidate_error_codes | child_candidate_error_codes)),
    )


def _validate_family_inputs(
    definition: CustomImportDefinition,
    roots: Sequence[Mapping[str, Any]],
    children_by_collection: Mapping[str, Sequence[Mapping[str, Any]]],
) -> None:
    if not isinstance(roots, (list, tuple)):
        raise DefinitionError("roots must be a bounded record array")
    if not isinstance(children_by_collection, Mapping):
        raise DefinitionError("children must contain one bounded array per declared collection")
    expected_collection_names = {collection.name for collection in definition.child_collections}
    has_bounded_child_records = all(
        isinstance(child_records, (list, tuple)) for child_records in children_by_collection.values()
    )
    if set(children_by_collection) != expected_collection_names or not has_bounded_child_records:
        raise DefinitionError("children must contain one bounded array per declared collection")


def _child_fields_by_collection(
    definition: CustomImportDefinition,
) -> dict[str, dict[str, Field]]:
    fields_by_collection: dict[str, dict[str, Field]] = defaultdict(dict)
    for child_field in definition.child_fields:
        fields_by_collection[child_field.collection][child_field.field_id] = child_field
    return fields_by_collection


def _admit_root_records(
    definition: CustomImportDefinition,
    roots: Sequence[Mapping[str, Any]],
    root_fields_by_id: Mapping[str, Field],
) -> tuple[
    dict[tuple[Any, ...], Mapping[str, Any]],
    dict[tuple[Any, ...], set[str]],
    list[FamilyRejection],
    set[str],
]:
    root_records_by_key: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    rejection_codes_by_root_key: dict[tuple[Any, ...], set[str]] = defaultdict(set)
    rejection_evidence_entries: list[FamilyRejection] = []
    candidate_error_codes: set[str] = set()
    for root_record in roots:
        if not isinstance(root_record, Mapping):
            rejection_evidence_entries.append(FamilyRejection(None, "root_not_object"))
            candidate_error_codes.add("root_not_object")
            continue
        root_key = _key(root_record, definition.root_logical_key)
        if root_key is None:
            rejection_evidence_entries.append(FamilyRejection(None, "root_key_missing"))
            candidate_error_codes.add("root_key_missing")
            continue
        if root_key in root_records_by_key:
            rejection_codes_by_root_key[root_key].add("duplicate_root_key")
            continue
        root_records_by_key[root_key] = _freeze_record(root_record)
        rejection_code = _record_error(
            root_record,
            root_fields_by_id,
            entity_field=definition.entity_field,
        )
        if rejection_code:
            rejection_codes_by_root_key[root_key].add(rejection_code)
    return (
        root_records_by_key,
        rejection_codes_by_root_key,
        rejection_evidence_entries,
        candidate_error_codes,
    )


def _admit_child_records(
    definition: CustomImportDefinition,
    children_by_collection: Mapping[str, Sequence[Mapping[str, Any]]],
    root_records_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
    child_fields_by_collection: Mapping[str, Mapping[str, Field]],
    rejection_codes_by_root_key: dict[tuple[Any, ...], set[str]],
) -> tuple[dict[str, dict[tuple[Any, ...], list[Mapping[str, Any]]]], set[str]]:
    child_records_by_root_and_collection = {
        collection.name: defaultdict(list) for collection in definition.child_collections
    }
    candidate_error_codes: set[str] = set()
    for collection in definition.child_collections:
        child_keys_by_parent: dict[tuple[Any, ...], set[tuple[Any, ...]]] = defaultdict(set)
        for child_record in children_by_collection[collection.name]:
            if not isinstance(child_record, Mapping):
                candidate_error_codes.add("child_not_object")
                continue
            root_key = _parent_key(child_record, collection)
            if root_key is None or root_key not in root_records_by_key:
                candidate_error_codes.add("orphan_child")
                continue
            rejection_code = _record_error(
                child_record,
                child_fields_by_collection[collection.name],
            )
            child_key = _key(child_record, collection.child_key)
            if rejection_code:
                rejection_codes_by_root_key[root_key].add(rejection_code)
                continue
            if child_key is None:
                rejection_codes_by_root_key[root_key].add("child_key_missing")
                continue
            canonical_child_key = _canonical_child_key(
                collection,
                child_key,
                child_fields_by_collection[collection.name],
            )
            if canonical_child_key is None:
                rejection_codes_by_root_key[root_key].add("field_type_invalid")
                continue
            if canonical_child_key in child_keys_by_parent[root_key]:
                rejection_codes_by_root_key[root_key].add("duplicate_child_key")
                continue
            child_keys_by_parent[root_key].add(canonical_child_key)
            child_records_by_root_and_collection[collection.name][root_key].append(_freeze_record(child_record))
    return child_records_by_root_and_collection, candidate_error_codes


def _assemble_accepted_families(
    definition: CustomImportDefinition,
    root_records_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
    child_records_by_root_and_collection: Mapping[str, Mapping[tuple[Any, ...], Sequence[Mapping[str, Any]]]],
    rejection_codes_by_root_key: Mapping[tuple[Any, ...], set[str]],
) -> tuple[RootFamily, ...]:
    return tuple(
        RootFamily(
            root_key=root_key,
            root=root_record,
            children=MappingProxyType(
                {
                    collection.name: tuple(child_records_by_root_and_collection[collection.name].get(root_key, ()))
                    for collection in definition.child_collections
                }
            ),
        )
        for root_key, root_record in sorted(root_records_by_key.items(), key=lambda pair: repr(pair[0]))
        if root_key not in rejection_codes_by_root_key
    )


def merge_families(
    previous: Mapping[tuple[Any, ...], RootFamily],
    candidate: FamilyBuildResult,
    *,
    refresh_mode: str,
    complete_scope: bool = False,
) -> dict[tuple[Any, ...], RootFamily]:
    """Apply v1 upsert/snapshot replacement without promoting rejected families."""

    if candidate.is_candidate_rejected:
        raise CandidateRejected(",".join(candidate.candidate_errors))
    accepted_families_by_key = {family.root_key: family for family in candidate.families}
    root_keys_with_rejections = {entry.root_key for entry in candidate.rejections if entry.root_key is not None}
    if refresh_mode == "upsert":
        return {**previous, **accepted_families_by_key}
    if refresh_mode != "snapshot":
        raise DefinitionError("refresh mode must be upsert or snapshot")
    if not complete_scope:
        raise CandidateRejected("snapshot_scope_incomplete")
    retained_families_by_key = {
        root_key: family for root_key, family in previous.items() if root_key in root_keys_with_rejections
    }
    return {**retained_families_by_key, **accepted_families_by_key}


def _freeze_record(record: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(record))


def _key(record: Mapping[str, Any], fields: tuple[str, ...]) -> tuple[Any, ...] | None:
    values: list[Any] = []
    for field in fields:
        if field not in record or record[field] is None:
            return None
        value = record[field]
        try:
            hash(value)
        except TypeError:
            return None
        values.append(value)
    return tuple(values)


def _parent_key(record: Mapping[str, Any], collection: ChildCollection) -> tuple[Any, ...] | None:
    values: list[Any] = []
    for part in collection.parent_key:
        if part.child_field not in record or record[part.child_field] is None:
            return None
        value = record[part.child_field]
        try:
            hash(value)
        except TypeError:
            return None
        values.append(value)
    return tuple(values)


def _canonical_child_key(
    collection: ChildCollection,
    child_key: tuple[Any, ...],
    fields_by_id: Mapping[str, Field],
) -> tuple[Any, ...] | None:
    """Normalize child-key values to their typed identity before duplicate checks."""

    canonical_values: list[Any] = []
    for field_id, value in zip(collection.child_key, child_key, strict=True):
        field = fields_by_id[field_id]
        if field.value_type == "decimal":
            value = normalize_source_decimal(value)
            if value is None:
                return None
        elif field.value_type == "timestamp":
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                return None
            try:
                value = value.astimezone(UTC)
            except OverflowError:
                return None
        canonical_values.append(value)
    return tuple(canonical_values)


def _record_error(
    source_record: Mapping[str, Any],
    fields: Mapping[str, Field],
    *,
    entity_field: str | None = None,
) -> str | None:
    for field_id, field in fields.items():
        if field_id not in source_record:
            if field.nullable:
                continue
            return "required_field_missing"
        field_value = source_record[field_id]
        if field_value is None:
            if field.nullable:
                continue
            return "required_field_null"
        if field.value_type == "decimal":
            decimal_value = normalize_source_decimal(field_value)
            if decimal_value is None:
                return "field_type_invalid"
            if field.projection_slot is not None and not is_decimal_scalar_storage_valid(decimal_value):
                return "field_storage_invalid"
        else:
            if not _is_value_type_valid(field_value, field.value_type):
                return "field_type_invalid"
            if field.projection_slot is not None and not _is_scalar_storage_valid(field, field_value):
                return "field_storage_invalid"
    if entity_field is not None:
        entity_value = source_record.get(entity_field)
        if not isinstance(entity_value, str) or not _is_valid_npi(entity_value):
            return "entity_binding_invalid"
    return None


def _is_value_type_valid(value: Any, value_type: str) -> bool:
    if value_type == "string":
        return isinstance(value, str)
    if value_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if value_type == "decimal":
        return normalize_source_decimal(value) is not None
    if value_type == "boolean":
        return isinstance(value, bool)
    if value_type == "date":
        return isinstance(value, date) and not isinstance(value, datetime)
    return isinstance(value, datetime) and value.tzinfo is not None


def normalize_source_decimal(value: Any) -> Decimal | None:
    """Return a finite source decimal after v1 lexical normalization.

    Source text is deliberately limited to ``-?[0-9]+(?:\\.[0-9]+)?`` using
    ASCII digits: no whitespace, underscores, non-ASCII numerals, plus sign,
    or exponent spelling is accepted before :class:`~decimal.Decimal`
    conversion.  Integer and already-parsed ``Decimal`` inputs represent the
    same numeric domain; fractional trailing zeroes are removed without
    changing the numeric value before storage-shape checks.
    """

    if isinstance(value, bool) or isinstance(value, float):
        return None
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, int):
        decimal_value = Decimal(value)
    elif isinstance(value, str):
        if _DECIMAL_SOURCE_TEXT.fullmatch(value) is None:
            return None
        try:
            decimal_value = Decimal(value)
        except InvalidOperation, ValueError:
            return None
    else:
        return None
    if not decimal_value.is_finite():
        return None
    return _normalize_decimal_fraction(decimal_value)


def _is_scalar_storage_valid(field: Field, value: Any) -> bool:
    """Return whether one non-null projected source scalar fits native storage."""

    if field.value_type == "string":
        if not isinstance(value, str) or "\x00" in value:
            return False
        try:
            return len(value.encode("utf-8")) <= MAX_SCALAR_STRING_UTF8_BYTES
        except UnicodeEncodeError:
            return False
    if field.value_type == "integer":
        return (
            isinstance(value, int) and not isinstance(value, bool) and MIN_SCALAR_INTEGER <= value <= MAX_SCALAR_INTEGER
        )
    if field.value_type == "decimal":
        decimal_value = normalize_source_decimal(value)
        return decimal_value is not None and is_decimal_scalar_storage_valid(decimal_value)
    return True


def is_decimal_scalar_storage_valid(value: Decimal) -> bool:
    """Return whether a normalized decimal fits the exact ``NUMERIC(30, 12)`` value range."""

    if not isinstance(value, Decimal) or not value.is_finite():
        return False
    return _is_decimal_storage_valid(value)


def _is_decimal_storage_valid(value: Decimal) -> bool:
    scale = max(-value.as_tuple().exponent, 0)
    integer_digits = 0 if value.is_zero() else max(value.adjusted() + 1, 0)
    return scale <= MAX_SCALAR_DECIMAL_SCALE and integer_digits <= MAX_SCALAR_DECIMAL_INTEGER_DIGITS


def _normalize_decimal_fraction(value: Decimal) -> Decimal:
    """Remove only insignificant fractional zeroes without applying decimal context rounding."""

    if value.is_zero():
        return Decimal(0)
    sign, digits, exponent = value.as_tuple()
    maximum_trim = min(-exponent, len(digits)) if exponent < 0 else 0
    trailing_zero_count = 0
    for index in range(len(digits) - 1, len(digits) - maximum_trim - 1, -1):
        if digits[index] != 0:
            break
        trailing_zero_count += 1
    if trailing_zero_count == 0:
        return value
    return Decimal((sign, digits[:-trailing_zero_count], exponent + trailing_zero_count))


def _is_valid_npi(value: str) -> bool:
    if not _NPI.fullmatch(value):
        return False
    digits = "80840" + value
    total = 0
    for position, character in enumerate(reversed(digits)):
        digit = int(character)
        if position % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0
