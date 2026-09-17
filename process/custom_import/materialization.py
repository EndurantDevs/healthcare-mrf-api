# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded typed projection and deterministic winner materialization.

This module is intentionally a persistence/domain boundary.  It turns already
accepted root-family records into the native scalar rows used by later indexed
reads, then chooses the one winning family/context for each declared profile.
It does not accept query predicates: selection happens here, before a later
read path can apply a threshold or result filter.

The caller owns the surrounding transaction.  It must persist the immutable
definition profile rows before candidate work and call :func:`seal_generation`
only after the projection and winner helpers below have flushed.  P3 finality
then recomputes its receipt from the P1 scalar and winner tables; this module
never writes a second materialization seal.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import inspect
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from decimal import Decimal
from typing import Any

from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportRootScalar,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import.definition import (
    MAX_CONTEXT_DIMENSIONS,
    MAX_HOT_FIELDS,
    MAX_ORDER_TERMS,
    MAX_SELECTION_PROFILES,
    MAX_SELECTION_TERMS,
    CustomImportDefinition,
    Field,
    SelectionProfile,
    canonical_json,
    canonical_sha256,
)
from process.custom_import.family import (
    MAX_SCALAR_INTEGER,
    MAX_SCALAR_STRING_UTF8_BYTES,
    MIN_SCALAR_INTEGER,
    is_decimal_scalar_storage_valid,
    normalize_source_decimal,
)

__all__ = (
    "ChildScalarProjection",
    "ChildScalarTarget",
    "DefinitionIdentity",
    "GenerationIdentity",
    "MaterializedWinner",
    "RootScalarProjection",
    "RootScalarTarget",
    "ScalarProjectionError",
    "TypedScalar",
    "ValidatedWinnerCandidateStream",
    "WinnerCandidate",
    "WinnerMaterialization",
    "WinnerMaterializationError",
    "materialize_winners",
    "persist_scalar_projections",
    "persist_selection_profiles",
    "persist_winner_materialization",
    "project_child_scalars",
    "project_root_scalars",
    "scalar_projection_models",
    "selection_profile_models",
    "winner_materialization_models",
)


_CONTEXT_DIGEST_PREFIX = b"custom-import/v1\x00winner-context\x00"
_MAX_CONTEXT_BYTES = 8_192
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_FIELD_TYPES = frozenset({"string", "integer", "decimal", "boolean", "date", "timestamp"})


class ScalarProjectionError(ValueError):
    """A value cannot be represented safely in one v1 typed scalar row."""


class WinnerMaterializationError(ValueError):
    """Candidates cannot form a bounded, deterministic winner set."""


@dataclass(frozen=True)
class TypedScalar:
    """One native value, preserving explicit null separately from absence."""

    field_type: str
    value_state: str
    string_value: str | None = None
    integer_value: int | None = None
    decimal_value: Decimal | None = None
    boolean_value: bool | None = None
    date_value: dt.date | None = None
    timestamp_value: dt.datetime | None = None

    @property
    def logical_value(self) -> object | None:
        """Return the typed value, retaining null as ``None``."""

        if self.value_state == "null":
            return None
        return {
            "string": self.string_value,
            "integer": self.integer_value,
            "decimal": self.decimal_value,
            "boolean": self.boolean_value,
            "date": self.date_value,
            "timestamp": self.timestamp_value,
        }[self.field_type]


@dataclass(frozen=True)
class RootScalarTarget:
    """The compact immutable identity for one root projection set."""

    dataset_id: int
    schema_revision_id: int
    root_record_id: int
    root_revision_id: int

    def __post_init__(self) -> None:
        _positive(self.dataset_id, "dataset_id")
        _positive(self.schema_revision_id, "schema_revision_id")
        _positive(self.root_record_id, "root_record_id")
        _positive(self.root_revision_id, "root_revision_id")


@dataclass(frozen=True)
class ChildScalarTarget:
    """The compact immutable identity for one child projection set."""

    dataset_id: int
    schema_revision_id: int
    root_record_id: int
    collection_slot: int
    child_revision_id: int

    def __post_init__(self) -> None:
        _positive(self.dataset_id, "dataset_id")
        _positive(self.schema_revision_id, "schema_revision_id")
        _positive(self.root_record_id, "root_record_id")
        _positive(self.collection_slot, "collection_slot")
        _positive(self.child_revision_id, "child_revision_id")


@dataclass(frozen=True)
class RootScalarProjection:
    """One declared root hot field, or an explicit null for that field."""

    target: RootScalarTarget
    field_id: str
    field_slot: int
    projection_slot: int
    scalar: TypedScalar


@dataclass(frozen=True)
class ChildScalarProjection:
    """One declared child hot field, or an explicit null for that field."""

    target: ChildScalarTarget
    field_id: str
    field_slot: int
    projection_slot: int
    scalar: TypedScalar


@dataclass(frozen=True)
class DefinitionIdentity:
    """Immutable definition/schema ownership for persisted profile rows."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int

    def __post_init__(self) -> None:
        _positive(self.dataset_id, "dataset_id")
        _positive(self.definition_revision_id, "definition_revision_id")
        _positive(self.schema_revision_id, "schema_revision_id")


@dataclass(frozen=True)
class GenerationIdentity:
    """Immutable generation/definition/schema ownership for winner rows."""

    generation_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int

    def __post_init__(self) -> None:
        _positive(self.generation_id, "generation_id")
        _positive(self.dataset_id, "dataset_id")
        _positive(self.definition_revision_id, "definition_revision_id")
        _positive(self.schema_revision_id, "schema_revision_id")


@dataclass(frozen=True)
class WinnerCandidate:
    """One family or child context eligible for a profile winner calculation.

    The two semantic SHA-256 values are not database allocation identifiers.
    They make a complete selection tie deterministic across equivalent
    generations instead of relying on insertion order or auto-increment IDs.
    A root candidate has no child revision/key; a child candidate has both.
    """

    entity_binding_id: int
    family_revision_id: int
    family_sha256: bytes
    context_collection_slot: int
    context_child_revision_id: int | None
    context_child_key_sha256: bytes | None
    values_by_field: Mapping[str, object]


@dataclass
class ValidatedWinnerCandidateStream:
    """One single-use candidate stream bound to validated generation membership.

    Its producer must build it only after proving every emitted root family and
    child context belongs to the immutable ``generation``.  Winner selection
    intentionally consumes this source once.  It retains only selected winners
    and bounded conflict state for their semantic tie keys; it never repeats
    membership validation or materializes the input stream.
    """

    generation: GenerationIdentity
    candidate_iterable: Iterable[WinnerCandidate]
    _candidate_iterator: Iterator[WinnerCandidate] = dataclass_field(init=False, repr=False)
    _is_consumed: bool = dataclass_field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.generation, GenerationIdentity):
            raise WinnerMaterializationError("winner candidate stream generation is malformed")
        try:
            self._candidate_iterator = iter(self.candidate_iterable)
        except TypeError as exc:
            raise WinnerMaterializationError("winner candidate stream must be iterable") from exc

    def consume(self) -> Iterator[WinnerCandidate]:
        """Return the bound iterator exactly once for winner materialization."""

        if self._is_consumed:
            raise WinnerMaterializationError("winner candidate stream was already consumed")
        self._is_consumed = True
        return self._candidate_iterator


@dataclass(frozen=True)
class MaterializedWinner:
    """One selected family/context ready for ``custom_import_winner``."""

    generation: GenerationIdentity
    profile_id: str
    profile_slot: int
    entity_binding_id: int
    family_revision_id: int
    context_collection_slot: int
    context_key_sha256: bytes
    canonical_context_key: str
    context_child_revision_id: int | None


@dataclass(frozen=True)
class WinnerMaterialization:
    """A complete, deterministic winner set for one candidate generation."""

    generation: GenerationIdentity
    profile_count: int
    profile_context_slots: tuple[int, ...]
    winners: tuple[MaterializedWinner, ...]

    @property
    def winner_count(self) -> int:
        """Return the number of persisted winner lookup rows."""

        return len(self.winners)


@dataclass(frozen=True)
class _NormalizedWinnerCandidate:
    candidate: WinnerCandidate
    values_by_field: Mapping[str, TypedScalar]


@dataclass(frozen=True)
class _ProfileScope:
    collection_name: str | None
    collection_slot: int


@dataclass(frozen=True)
class _WinnerCandidateContract:
    """Definition-owned bounds needed while consuming one candidate stream."""

    fields_by_id: Mapping[str, Field]
    root_query_field_ids: frozenset[str]
    child_query_field_ids: frozenset[str]
    declared_context_slots: frozenset[int]


@dataclass(frozen=True)
class _SelectedWinner:
    """The one retained winner for a profile/entity/canonical context."""

    profile_id: str
    profile_slot: int
    canonical_context_key: str
    context_key_sha256: bytes
    candidate: _NormalizedWinnerCandidate
    has_physical_tie_conflict: bool = False
    has_typed_tie_conflict: bool = False


def project_root_scalars(
    definition: CustomImportDefinition,
    *,
    root_target: RootScalarTarget,
    root_values: Mapping[str, object],
) -> tuple[RootScalarProjection, ...]:
    """Project declared root hot fields without retaining a JSON query payload."""

    _validated_definition(definition)
    _require_mapping(root_values, "root values", ScalarProjectionError)
    if not isinstance(root_target, RootScalarTarget):
        raise ScalarProjectionError("root scalar target is malformed")
    projections: list[RootScalarProjection] = []
    for field in definition.root_fields:
        if field.projection_slot is None:
            continue
        if field.field_id not in root_values:
            if not field.nullable:
                raise ScalarProjectionError(f"projected field {field.field_id} is required but missing")
            continue
        projections.append(
            RootScalarProjection(
                target=root_target,
                field_id=field.field_id,
                field_slot=field.field_slot,
                projection_slot=field.projection_slot,
                scalar=_typed_scalar(field, root_values[field.field_id]),
            )
        )
    return tuple(projections)


def project_child_scalars(
    definition: CustomImportDefinition,
    *,
    collection: str,
    child_target: ChildScalarTarget,
    child_values: Mapping[str, object],
    child_collection_slots: Mapping[str, int],
) -> tuple[ChildScalarProjection, ...]:
    """Project one declared child collection into its bounded scalar slots."""

    _validated_definition(definition)
    _require_mapping(child_values, "child values", ScalarProjectionError)
    if not isinstance(child_target, ChildScalarTarget):
        raise ScalarProjectionError("child scalar target is malformed")
    collection_slot_by_name = _validated_child_collection_slots(definition, child_collection_slots)
    if collection not in collection_slot_by_name:
        raise ScalarProjectionError("collection is not declared by the definition")
    if child_target.collection_slot != collection_slot_by_name[collection]:
        raise ScalarProjectionError("child scalar target collection does not match the declared collection")
    projections: list[ChildScalarProjection] = []
    for field in definition.child_fields:
        if field.collection != collection or field.projection_slot is None:
            continue
        if field.field_id not in child_values:
            if not field.nullable:
                raise ScalarProjectionError(f"projected field {field.field_id} is required but missing")
            continue
        projections.append(
            ChildScalarProjection(
                target=child_target,
                field_id=field.field_id,
                field_slot=field.field_slot,
                projection_slot=field.projection_slot,
                scalar=_typed_scalar(field, child_values[field.field_id]),
            )
        )
    return tuple(projections)


def scalar_projection_models(
    definition: CustomImportDefinition,
    *,
    root_scalars: Iterable[RootScalarProjection] = (),
    child_scalars: Iterable[ChildScalarProjection] = (),
    child_collection_slots: Mapping[str, int] | None = None,
) -> tuple[CustomImportRootScalar | CustomImportChildScalar, ...]:
    """Build definition-verified immutable ORM rows for scalar persistence."""

    _validated_definition(definition)
    root_rows = tuple(root_scalars)
    child_rows = tuple(child_scalars)
    slots = _validated_child_collection_slots(definition, child_collection_slots) if child_rows else {}
    _validate_scalar_projection_rows(definition, root_rows, child_rows, slots)
    return tuple(_root_scalar_model(row) for row in root_rows) + tuple(_child_scalar_model(row) for row in child_rows)


async def persist_scalar_projections(
    session: Any,
    definition: CustomImportDefinition,
    *,
    root_scalars: Iterable[RootScalarProjection] = (),
    child_scalars: Iterable[ChildScalarProjection] = (),
    child_collection_slots: Mapping[str, int] | None = None,
) -> int:
    """Persist scalar rows and flush them before later P3 generation sealing."""

    models = scalar_projection_models(
        definition,
        root_scalars=root_scalars,
        child_scalars=child_scalars,
        child_collection_slots=child_collection_slots,
    )
    _add_models(session, models, "scalar projection")
    await _flush(session, "scalar projection")
    return len(models)


def selection_profile_models(
    definition: CustomImportDefinition,
    *,
    identity: DefinitionIdentity,
    child_collection_slots: Mapping[str, int] | None = None,
) -> tuple[CustomImportSelectionProfile, ...]:
    """Build immutable profile rows with an inferred root-or-child context.

    A profile that references any permitted child field is child-scoped.  A
    profile using root fields only is root-scoped.  Since v1 permits one query
    child collection, this inference cannot create a cross-child join.
    """

    _validated_definition(definition)
    if not isinstance(identity, DefinitionIdentity):
        raise WinnerMaterializationError("definition identity is malformed")
    scopes = _profile_scopes(definition, child_collection_slots)
    models: list[CustomImportSelectionProfile] = []
    for profile_slot, (profile, scope) in enumerate(zip(definition.selection_profiles, scopes, strict=True), start=1):
        document = _profile_document(profile, scope)
        models.append(
            CustomImportSelectionProfile(
                definition_revision_id=identity.definition_revision_id,
                dataset_id=identity.dataset_id,
                schema_revision_id=identity.schema_revision_id,
                profile_slot=profile_slot,
                profile_id=profile.profile_id,
                context_collection_slot=None if scope.collection_slot == 0 else scope.collection_slot,
                canonical_profile=canonical_json(document),
                profile_sha256=bytes.fromhex(canonical_sha256(document, domain="profile")),
            )
        )
    return tuple(models)


async def persist_selection_profiles(
    session: Any,
    definition: CustomImportDefinition,
    *,
    identity: DefinitionIdentity,
    child_collection_slots: Mapping[str, int] | None = None,
) -> int:
    """Persist and flush definition-owned profile rows before candidate work."""

    models = selection_profile_models(
        definition,
        identity=identity,
        child_collection_slots=child_collection_slots,
    )
    _add_models(session, models, "selection profile")
    await _flush(session, "selection profile")
    return len(models)


def materialize_winners(
    definition: CustomImportDefinition,
    *,
    generation: GenerationIdentity,
    candidates: ValidatedWinnerCandidateStream,
    child_collection_slots: Mapping[str, int] | None = None,
) -> WinnerMaterialization:
    """Select one trusted family/context stream before later query filtering.

    There is deliberately no filter parameter.  If a later query predicate
    excludes this selected winner, it is excluded; this function never asks a
    lower-ranked family to fill the result.  ``candidates`` must be a
    :class:`ValidatedWinnerCandidateStream` emitted by immutable generation
    membership validation and is consumed exactly once.
    """

    _validated_definition(definition)
    if not isinstance(generation, GenerationIdentity):
        raise WinnerMaterializationError("generation identity is malformed")
    if not isinstance(candidates, ValidatedWinnerCandidateStream):
        raise WinnerMaterializationError("winner candidates require a validated generation stream")
    if candidates.generation != generation:
        raise WinnerMaterializationError("winner candidate stream generation does not match materialization")
    profile_scopes = _profile_scopes(definition, child_collection_slots)
    selected_winner_by_context = _select_winners(
        definition,
        candidate_iterator=candidates.consume(),
        profile_scopes=profile_scopes,
    )
    ordered_winners = _materialized_winners(generation, selected_winner_by_context)
    profile_context_slots = tuple(scope.collection_slot for scope in profile_scopes)
    _validate_winner_rows(generation, len(profile_scopes), profile_context_slots, ordered_winners)
    return WinnerMaterialization(
        generation=generation,
        profile_count=len(profile_scopes),
        profile_context_slots=profile_context_slots,
        winners=ordered_winners,
    )


def _select_winners(
    definition: CustomImportDefinition,
    *,
    candidate_iterator: Iterator[WinnerCandidate],
    profile_scopes: tuple[_ProfileScope, ...],
) -> dict[tuple[int, int, str], _SelectedWinner]:
    candidate_contract = _winner_candidate_contract(definition, profile_scopes)
    selected_winner_by_context: dict[tuple[int, int, str], _SelectedWinner] = {}
    canonical_context_by_digest: dict[tuple[int, int, bytes], str] = {}
    for raw_candidate in candidate_iterator:
        normalized_candidate = _normalize_winner_candidate(raw_candidate, candidate_contract)
        _consider_candidate_for_profiles(
            normalized_candidate,
            definition.selection_profiles,
            profile_scopes,
            candidate_contract.fields_by_id,
            selected_winner_by_context,
            canonical_context_by_digest,
        )
    _raise_selected_winner_tie_conflicts(selected_winner_by_context)
    return selected_winner_by_context


def _materialized_winners(
    generation: GenerationIdentity,
    selected_winner_by_context: Mapping[tuple[int, int, str], _SelectedWinner],
) -> tuple[MaterializedWinner, ...]:
    winners = (
        _winner_from_selection(generation, selected_winner) for selected_winner in selected_winner_by_context.values()
    )
    return tuple(
        sorted(
            winners,
            key=lambda winner: (
                winner.profile_slot,
                winner.entity_binding_id,
                winner.context_key_sha256,
                winner.canonical_context_key,
            ),
        )
    )


def _winner_from_selection(
    generation: GenerationIdentity,
    selected_winner: _SelectedWinner,
) -> MaterializedWinner:
    winner_candidate = selected_winner.candidate.candidate
    return MaterializedWinner(
        generation=generation,
        profile_id=selected_winner.profile_id,
        profile_slot=selected_winner.profile_slot,
        entity_binding_id=winner_candidate.entity_binding_id,
        family_revision_id=winner_candidate.family_revision_id,
        context_collection_slot=winner_candidate.context_collection_slot,
        context_key_sha256=selected_winner.context_key_sha256,
        canonical_context_key=selected_winner.canonical_context_key,
        context_child_revision_id=winner_candidate.context_child_revision_id,
    )


def winner_materialization_models(
    materialization: WinnerMaterialization,
) -> tuple[CustomImportWinner, ...]:
    """Build detached winner ORM rows; P3 owns the final generation seal."""

    _validate_materialization(materialization)
    generation = materialization.generation
    return tuple(
        CustomImportWinner(
            generation_id=generation.generation_id,
            dataset_id=generation.dataset_id,
            definition_revision_id=generation.definition_revision_id,
            schema_revision_id=generation.schema_revision_id,
            profile_slot=winner.profile_slot,
            entity_binding_id=winner.entity_binding_id,
            family_revision_id=winner.family_revision_id,
            context_collection_slot=winner.context_collection_slot,
            context_key_sha256=winner.context_key_sha256,
            context_child_revision_id=winner.context_child_revision_id,
        )
        for winner in materialization.winners
    )


async def persist_winner_materialization(session: Any, materialization: WinnerMaterialization) -> int:
    """Persist and flush winner rows so P3 ``seal_generation`` can follow.

    This intentionally does not seal or terminalize the generation.  P3 owns
    the fenced seal, immutable receipt, and execution transition in the same
    caller-owned transaction.
    """

    models = winner_materialization_models(materialization)
    _add_models(session, models, "winner materialization")
    await _flush(session, "winner materialization")
    return len(models)


def _validated_definition(value: object) -> CustomImportDefinition:
    if not isinstance(value, CustomImportDefinition):
        raise TypeError("definition must be a CustomImportDefinition")
    hot_fields = [field for field in value.fields if field.projection_slot is not None]
    if len(hot_fields) > MAX_HOT_FIELDS:
        raise WinnerMaterializationError("hot projection count exceeds the v1 limit")
    if len(value.selection_profiles) > MAX_SELECTION_PROFILES:
        raise WinnerMaterializationError("selection profile count exceeds the v1 limit")
    if len(value.query.order_terms) > MAX_ORDER_TERMS:
        raise WinnerMaterializationError("query order terms exceed the v1 limit")
    fields_by_id = value.fields_by_id
    if len(fields_by_id) != len(value.fields):
        raise WinnerMaterializationError("definition field identities are not unique")
    projected_slots = [field.projection_slot for field in hot_fields]
    if len(set(projected_slots)) != len(projected_slots):
        raise WinnerMaterializationError("hot projection slots are not unique")
    profile_ids = [profile.profile_id for profile in value.selection_profiles]
    if len(set(profile_ids)) != len(profile_ids):
        raise WinnerMaterializationError("selection profile identities are not unique")
    for profile in value.selection_profiles:
        _validate_profile(profile, fields_by_id, value)
    return value


def _validate_profile(
    profile: object,
    fields_by_id: Mapping[str, Field],
    definition: CustomImportDefinition,
) -> None:
    if not isinstance(profile, SelectionProfile):
        raise WinnerMaterializationError("selection profile is malformed")
    if not isinstance(profile.profile_id, str) or not _IDENTIFIER.fullmatch(profile.profile_id):
        raise WinnerMaterializationError("selection profile id is malformed")
    if len(profile.selection_terms) > MAX_SELECTION_TERMS:
        raise WinnerMaterializationError("selection terms exceed the v1 limit")
    if len(profile.context_dimensions) > MAX_CONTEXT_DIMENSIONS:
        raise WinnerMaterializationError("context dimensions exceed the v1 limit")
    permitted = set(definition.query.root_fields) | set(definition.query.child_fields)
    seen_fields: set[str] = set()
    for term in profile.selection_terms:
        if term.field_id in seen_fields or term.field_id not in permitted or term.field_id not in fields_by_id:
            raise WinnerMaterializationError("selection profile uses an unpermitted field")
        seen_fields.add(term.field_id)
        if term.direction not in {"asc", "desc"} or term.nulls not in {"first", "last"}:
            raise WinnerMaterializationError("selection profile has invalid ordering")
    dimensions = set(profile.context_dimensions)
    if len(dimensions) != len(profile.context_dimensions) or not dimensions.issubset(permitted):
        raise WinnerMaterializationError("selection profile uses an unpermitted context dimension")


def _validated_child_collection_slots(
    definition: CustomImportDefinition,
    value: Mapping[str, int] | None,
) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise WinnerMaterializationError("declared child collection slots are required")
    expected_names = {collection.name for collection in definition.child_collections}
    if set(value) != expected_names:
        raise WinnerMaterializationError("child collection slots do not match the definition")
    collection_slot_by_name: dict[str, int] = {}
    seen_slots: set[int] = set()
    for name, slot in value.items():
        _positive(slot, "child collection slot")
        if slot in seen_slots:
            raise WinnerMaterializationError("child collection slots must be unique")
        seen_slots.add(slot)
        collection_slot_by_name[name] = slot
    return collection_slot_by_name


def _profile_scopes(
    definition: CustomImportDefinition,
    child_collection_slots: Mapping[str, int] | None,
) -> tuple[_ProfileScope, ...]:
    child_field_ids = set(definition.query.child_fields)
    needs_child_slots = any(
        ({term.field_id for term in profile.selection_terms} | set(profile.context_dimensions)) & child_field_ids
        for profile in definition.selection_profiles
    )
    slots = _validated_child_collection_slots(definition, child_collection_slots) if needs_child_slots else {}
    scopes: list[_ProfileScope] = []
    for profile in definition.selection_profiles:
        references = {term.field_id for term in profile.selection_terms} | set(profile.context_dimensions)
        uses_child = bool(references & child_field_ids)
        if not uses_child:
            scopes.append(_ProfileScope(collection_name=None, collection_slot=0))
            continue
        collection = definition.query.child_collection
        if collection is None or collection not in slots:
            raise WinnerMaterializationError("child-scoped profile has no declared child collection slot")
        scopes.append(_ProfileScope(collection_name=collection, collection_slot=slots[collection]))
    return tuple(scopes)


def _profile_document(profile: SelectionProfile, scope: _ProfileScope) -> dict[str, object]:
    context_scope_dict: dict[str, str] = {"kind": "root"}
    if scope.collection_name is not None:
        context_scope_dict = {"kind": "child", "collection": scope.collection_name}
    return {
        "context_dimensions": list(profile.context_dimensions),
        "id": profile.profile_id,
        "selection": [
            {"field": term.field_id, "direction": term.direction, "nulls": term.nulls}
            for term in profile.selection_terms
        ],
        "scope": context_scope_dict,
    }


def _typed_scalar(field: Field, raw_scalar: object) -> TypedScalar:
    if raw_scalar is None:
        if not field.nullable:
            raise ScalarProjectionError(f"projected field {field.field_id} cannot be null")
        return TypedScalar(field_type=field.value_type, value_state="null")
    if field.value_type == "string":
        if (
            not isinstance(raw_scalar, str)
            or "\x00" in raw_scalar
            or _utf8_size(raw_scalar, field.field_id) > MAX_SCALAR_STRING_UTF8_BYTES
        ):
            raise ScalarProjectionError(f"projected string field {field.field_id} exceeds its storage shape")
        return TypedScalar(field_type="string", value_state="value", string_value=raw_scalar)
    if field.value_type == "integer":
        if (
            isinstance(raw_scalar, bool)
            or not isinstance(raw_scalar, int)
            or not MIN_SCALAR_INTEGER <= raw_scalar <= MAX_SCALAR_INTEGER
        ):
            raise ScalarProjectionError(f"projected integer field {field.field_id} is outside BIGINT storage")
        return TypedScalar(field_type="integer", value_state="value", integer_value=raw_scalar)
    if field.value_type == "decimal":
        return TypedScalar(
            field_type="decimal", value_state="value", decimal_value=_decimal(raw_scalar, field.field_id)
        )
    if field.value_type == "boolean":
        if not isinstance(raw_scalar, bool):
            raise ScalarProjectionError(f"projected boolean field {field.field_id} is not boolean")
        return TypedScalar(field_type="boolean", value_state="value", boolean_value=raw_scalar)
    if field.value_type == "date":
        if not isinstance(raw_scalar, dt.date) or isinstance(raw_scalar, dt.datetime):
            raise ScalarProjectionError(f"projected date field {field.field_id} is not a date")
        return TypedScalar(field_type="date", value_state="value", date_value=raw_scalar)
    if field.value_type == "timestamp":
        if not isinstance(raw_scalar, dt.datetime) or raw_scalar.tzinfo is None or raw_scalar.utcoffset() is None:
            raise ScalarProjectionError(f"projected timestamp field {field.field_id} must be timezone-aware")
        return TypedScalar(
            field_type="timestamp",
            value_state="value",
            timestamp_value=raw_scalar.astimezone(dt.UTC),
        )
    raise ScalarProjectionError(f"projected field {field.field_id} has an unsupported type")


def _decimal(value: object, field_id: str) -> Decimal:
    decimal_value = normalize_source_decimal(value)
    if decimal_value is None:
        raise ScalarProjectionError(f"projected decimal field {field_id} has invalid source text")
    if not is_decimal_scalar_storage_valid(decimal_value):
        raise ScalarProjectionError(f"projected decimal field {field_id} exceeds NUMERIC(30, 12) storage")
    return decimal_value


def _utf8_size(value: str, field_id: str) -> int:
    del field_id
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ScalarProjectionError("projected string field is not UTF-8 encodable") from exc


def _validate_scalar_projection_rows(
    definition: CustomImportDefinition,
    root_rows: tuple[RootScalarProjection, ...],
    child_rows: tuple[ChildScalarProjection, ...],
    child_collection_slots: Mapping[str, int],
) -> None:
    root_keys: set[tuple[int, int]] = set()
    child_keys: set[tuple[int, int]] = set()
    owners: set[tuple[int, int]] = set()
    field_by_projection_slot: dict[int, int] = {}
    projection_by_field_slot: dict[int, int] = {}
    for root_projection in root_rows:
        if not isinstance(root_projection, RootScalarProjection) or not isinstance(
            root_projection.target, RootScalarTarget
        ):
            raise ScalarProjectionError("root scalar projection row is malformed")
        _validate_projection(definition, root_projection, root=True)
        key = (root_projection.target.root_revision_id, root_projection.field_slot)
        if key in root_keys:
            raise ScalarProjectionError("root scalar projection rows cannot repeat an identity")
        root_keys.add(key)
        owners.add((root_projection.target.dataset_id, root_projection.target.schema_revision_id))
        _validate_projection_pair(field_by_projection_slot, projection_by_field_slot, root_projection)
    for child_projection in child_rows:
        if not isinstance(child_projection, ChildScalarProjection) or not isinstance(
            child_projection.target, ChildScalarTarget
        ):
            raise ScalarProjectionError("child scalar projection row is malformed")
        field = _validate_projection(definition, child_projection, root=False)
        assert field.collection is not None
        expected_slot = child_collection_slots.get(field.collection)
        if expected_slot != child_projection.target.collection_slot:
            raise ScalarProjectionError("child scalar target collection does not match its field collection")
        key = (child_projection.target.child_revision_id, child_projection.field_slot)
        if key in child_keys:
            raise ScalarProjectionError("child scalar projection rows cannot repeat an identity")
        child_keys.add(key)
        owners.add((child_projection.target.dataset_id, child_projection.target.schema_revision_id))
        _validate_projection_pair(field_by_projection_slot, projection_by_field_slot, child_projection)
    if len(owners) > 1:
        raise ScalarProjectionError("scalar projections must share one immutable dataset/schema owner")


def _validate_projection(
    definition: CustomImportDefinition,
    row: RootScalarProjection | ChildScalarProjection,
    *,
    root: bool,
) -> Field:
    _positive(row.field_slot, "field_slot")
    if (
        isinstance(row.projection_slot, bool)
        or not isinstance(row.projection_slot, int)
        or not 1 <= row.projection_slot <= MAX_HOT_FIELDS
    ):
        raise ScalarProjectionError("projection_slot must be from 1 through 20")
    if not isinstance(row.scalar, TypedScalar):
        raise ScalarProjectionError("scalar projection row has no typed scalar")
    _validate_typed_scalar(row.scalar)
    field = definition.fields_by_id.get(row.field_id)
    if field is None:
        raise ScalarProjectionError("scalar projection field is not declared by the definition")
    if (
        field.field_slot != row.field_slot
        or field.projection_slot != row.projection_slot
        or field.value_type != row.scalar.field_type
        or (field.collection is None) != root
    ):
        raise ScalarProjectionError("scalar projection does not match its immutable field binding")
    if row.scalar.value_state == "null" and not field.nullable:
        raise ScalarProjectionError("scalar projection cannot store null for a required field")
    return field


def _validate_projection_pair(
    field_by_projection_slot: dict[int, int],
    projection_by_field_slot: dict[int, int],
    row: RootScalarProjection | ChildScalarProjection,
) -> None:
    bound_field_slot = field_by_projection_slot.setdefault(row.projection_slot, row.field_slot)
    if bound_field_slot != row.field_slot:
        raise ScalarProjectionError("a hot projection slot cannot bind multiple stable field slots")
    bound_projection_slot = projection_by_field_slot.setdefault(row.field_slot, row.projection_slot)
    if bound_projection_slot != row.projection_slot:
        raise ScalarProjectionError("a stable field slot cannot bind multiple hot projection slots")


def _validate_typed_scalar(scalar: TypedScalar) -> None:
    if scalar.value_state not in {"value", "null"}:
        raise ScalarProjectionError("scalar value_state must be value or null")
    if scalar.field_type not in _FIELD_TYPES:
        raise ScalarProjectionError("scalar row has an unsupported field type")
    values = (
        scalar.string_value,
        scalar.integer_value,
        scalar.decimal_value,
        scalar.boolean_value,
        scalar.date_value,
        scalar.timestamp_value,
    )
    if scalar.value_state == "null":
        if any(value is not None for value in values):
            raise ScalarProjectionError("null scalar rows cannot contain a typed value")
        return
    if scalar.logical_value is None or sum(value is not None for value in values) != 1:
        raise ScalarProjectionError("value scalar rows require exactly one typed value")
    field = Field("synthetic", 1, scalar.field_type, False, 1, None)
    _typed_scalar(field, scalar.logical_value)


def _root_scalar_model(row: RootScalarProjection) -> CustomImportRootScalar:
    return CustomImportRootScalar(
        root_revision_id=row.target.root_revision_id,
        dataset_id=row.target.dataset_id,
        schema_revision_id=row.target.schema_revision_id,
        root_record_id=row.target.root_record_id,
        field_slot=row.field_slot,
        field_collection_slot=0,
        projection_slot=row.projection_slot,
        field_type=row.scalar.field_type,
        value_state=row.scalar.value_state,
        **_scalar_values(row.scalar),
    )


def _child_scalar_model(row: ChildScalarProjection) -> CustomImportChildScalar:
    return CustomImportChildScalar(
        child_revision_id=row.target.child_revision_id,
        dataset_id=row.target.dataset_id,
        schema_revision_id=row.target.schema_revision_id,
        root_record_id=row.target.root_record_id,
        collection_slot=row.target.collection_slot,
        field_slot=row.field_slot,
        field_collection_slot=row.target.collection_slot,
        projection_slot=row.projection_slot,
        field_type=row.scalar.field_type,
        value_state=row.scalar.value_state,
        **_scalar_values(row.scalar),
    )


def _scalar_values(scalar: TypedScalar) -> dict[str, object | None]:
    return {
        "string_value": scalar.string_value,
        "integer_value": scalar.integer_value,
        "decimal_value": scalar.decimal_value,
        "boolean_value": scalar.boolean_value,
        "date_value": scalar.date_value,
        "timestamp_value": scalar.timestamp_value,
    }


def _winner_candidate_contract(
    definition: CustomImportDefinition,
    profile_scopes: tuple[_ProfileScope, ...],
) -> _WinnerCandidateContract:
    root_query_field_ids = frozenset(definition.query.root_fields)
    return _WinnerCandidateContract(
        fields_by_id=definition.fields_by_id,
        root_query_field_ids=root_query_field_ids,
        child_query_field_ids=root_query_field_ids | frozenset(definition.query.child_fields),
        declared_context_slots=frozenset({0, *(scope.collection_slot for scope in profile_scopes)}),
    )


def _normalize_winner_candidate(
    candidate: WinnerCandidate,
    candidate_contract: _WinnerCandidateContract,
) -> _NormalizedWinnerCandidate:
    if not isinstance(candidate, WinnerCandidate):
        raise WinnerMaterializationError("winner candidates must use WinnerCandidate identities")
    _positive(candidate.entity_binding_id, "entity_binding_id")
    _positive(candidate.family_revision_id, "family_revision_id")
    _sha256(candidate.family_sha256, "candidate family digest")
    _validate_candidate_context(candidate, candidate_contract.declared_context_slots)
    candidate_values_by_field = _typed_candidate_values(candidate, candidate_contract)
    return _NormalizedWinnerCandidate(candidate=candidate, values_by_field=candidate_values_by_field)


def _typed_candidate_values(
    candidate: WinnerCandidate,
    candidate_contract: _WinnerCandidateContract,
) -> dict[str, TypedScalar]:
    if not isinstance(candidate.values_by_field, Mapping):
        raise WinnerMaterializationError("winner candidate values must be a mapping")
    permitted_field_ids = _candidate_permitted_field_ids(candidate, candidate_contract)
    unknown_field_ids = set(candidate.values_by_field) - permitted_field_ids
    if unknown_field_ids:
        raise WinnerMaterializationError("winner candidate values include a non-query field")
    typed_values_by_field: dict[str, TypedScalar] = {}
    for field_id, raw_scalar in candidate.values_by_field.items():
        if not isinstance(field_id, str) or field_id not in candidate_contract.fields_by_id:
            raise WinnerMaterializationError("winner candidate has an unknown field")
        try:
            typed_values_by_field[field_id] = _typed_scalar(
                candidate_contract.fields_by_id[field_id],
                raw_scalar,
            )
        except ScalarProjectionError as exc:
            raise WinnerMaterializationError(str(exc)) from exc
    return typed_values_by_field


def _candidate_permitted_field_ids(
    candidate: WinnerCandidate,
    candidate_contract: _WinnerCandidateContract,
) -> frozenset[str]:
    if candidate.context_collection_slot == 0:
        return candidate_contract.root_query_field_ids
    return candidate_contract.child_query_field_ids


def _consider_candidate_for_profiles(
    candidate: _NormalizedWinnerCandidate,
    profiles: tuple[SelectionProfile, ...],
    profile_scopes: tuple[_ProfileScope, ...],
    fields_by_id: Mapping[str, Field],
    selected_winner_by_context: dict[tuple[int, int, str], _SelectedWinner],
    canonical_context_by_digest: dict[tuple[int, int, bytes], str],
) -> None:
    for profile_slot, (profile, scope) in enumerate(zip(profiles, profile_scopes, strict=True), start=1):
        if candidate.candidate.context_collection_slot != scope.collection_slot:
            continue
        _validate_selection_values(profile, candidate, fields_by_id)
        canonical_context_key, context_key_sha256 = _context_key(profile, candidate, fields_by_id)
        _validate_context_digest(
            canonical_context_by_digest,
            profile_slot,
            candidate.candidate.entity_binding_id,
            context_key_sha256,
            canonical_context_key,
        )
        _replace_winner_when_better(
            selected_winner_by_context,
            profile,
            profile_slot,
            candidate,
            fields_by_id,
            canonical_context_key,
            context_key_sha256,
        )


def _validate_context_digest(
    canonical_context_by_digest: dict[tuple[int, int, bytes], str],
    profile_slot: int,
    entity_binding_id: int,
    context_key_sha256: bytes,
    canonical_context_key: str,
) -> None:
    collision_key = (profile_slot, entity_binding_id, context_key_sha256)
    known_canonical_context = canonical_context_by_digest.setdefault(collision_key, canonical_context_key)
    if known_canonical_context != canonical_context_key:
        raise WinnerMaterializationError("winner context digest collision has different canonical context")


def _replace_winner_when_better(
    selected_winner_by_context: dict[tuple[int, int, str], _SelectedWinner],
    profile: SelectionProfile,
    profile_slot: int,
    candidate: _NormalizedWinnerCandidate,
    fields_by_id: Mapping[str, Field],
    canonical_context_key: str,
    context_key_sha256: bytes,
) -> None:
    winner_context = (profile_slot, candidate.candidate.entity_binding_id, canonical_context_key)
    selected_winner = selected_winner_by_context.get(winner_context)
    if selected_winner is not None:
        comparison = _compare_candidates(
            profile,
            candidate,
            selected_winner.candidate,
            fields_by_id,
        )
        if comparison == 0:
            physical_conflict, typed_conflict = _winner_tie_conflicts(selected_winner.candidate, candidate)
            selected_winner_by_context[winner_context] = _SelectedWinner(
                profile_id=selected_winner.profile_id,
                profile_slot=selected_winner.profile_slot,
                canonical_context_key=selected_winner.canonical_context_key,
                context_key_sha256=selected_winner.context_key_sha256,
                candidate=selected_winner.candidate,
                has_physical_tie_conflict=selected_winner.has_physical_tie_conflict or physical_conflict,
                has_typed_tie_conflict=selected_winner.has_typed_tie_conflict or typed_conflict,
            )
            return
        if comparison > 0:
            return
    selected_winner_by_context[winner_context] = _SelectedWinner(
        profile_id=profile.profile_id,
        profile_slot=profile_slot,
        canonical_context_key=canonical_context_key,
        context_key_sha256=context_key_sha256,
        candidate=candidate,
    )


def _winner_tie_conflicts(
    retained_candidate: _NormalizedWinnerCandidate,
    incoming_candidate: _NormalizedWinnerCandidate,
) -> tuple[bool, bool]:
    """Return physical and typed conflicts for two equal selected candidates."""

    return (
        _physical_candidate_identity(retained_candidate.candidate)
        != _physical_candidate_identity(incoming_candidate.candidate),
        retained_candidate.values_by_field != incoming_candidate.values_by_field,
    )


def _raise_selected_winner_tie_conflicts(
    selected_winner_by_context: Mapping[tuple[int, int, str], _SelectedWinner],
) -> None:
    """Reject only ties that remain at the deterministic winning semantic key."""

    if any(winner.has_physical_tie_conflict for winner in selected_winner_by_context.values()):
        raise WinnerMaterializationError("equal semantic winner tie has conflicting physical identity")
    if any(winner.has_typed_tie_conflict for winner in selected_winner_by_context.values()):
        raise WinnerMaterializationError("equal semantic winner tie has conflicting typed values")


def _physical_candidate_identity(candidate: WinnerCandidate) -> tuple[int, int, int, int | None]:
    return (
        candidate.entity_binding_id,
        candidate.family_revision_id,
        candidate.context_collection_slot,
        candidate.context_child_revision_id,
    )


def _validate_candidate_context(candidate: WinnerCandidate, declared_scope_slots: frozenset[int]) -> None:
    slot = candidate.context_collection_slot
    if isinstance(slot, bool) or not isinstance(slot, int) or slot not in declared_scope_slots:
        raise WinnerMaterializationError("winner candidate context is not a declared root or child scope")
    if slot == 0:
        if candidate.context_child_revision_id is not None or candidate.context_child_key_sha256 is not None:
            raise WinnerMaterializationError("root winner contexts cannot carry a child identity")
        return
    _positive(candidate.context_child_revision_id, "context_child_revision_id")
    _sha256(candidate.context_child_key_sha256, "candidate child key digest")


def _context_key(
    profile: SelectionProfile,
    candidate: _NormalizedWinnerCandidate,
    fields_by_id: Mapping[str, Field],
) -> tuple[str, bytes]:
    dimensions: list[dict[str, object]] = []
    for field_id in profile.context_dimensions:
        field = fields_by_id[field_id]
        scalar = candidate.values_by_field.get(field_id)
        if scalar is None and not field.nullable:
            raise WinnerMaterializationError(f"winner candidate is missing required context field {field_id}")
        dimensions.append(
            {
                "field_slot": field.field_slot,
                "field_type": field.value_type,
                "value_state": "missing" if scalar is None else scalar.value_state,
                "value": None if scalar is None else _context_value(scalar),
            }
        )
    canonical = canonical_json(
        {
            "dimensions": dimensions,
            "profile_id": profile.profile_id,
            "scope": "root" if candidate.candidate.context_collection_slot == 0 else "child",
        }
    )
    if len(canonical.encode("utf-8")) > _MAX_CONTEXT_BYTES:
        raise WinnerMaterializationError("canonical winner context exceeds the v1 byte limit")
    return canonical, _context_digest(canonical)


def _context_digest(canonical_context_key: str) -> bytes:
    return hashlib.sha256(_CONTEXT_DIGEST_PREFIX + canonical_context_key.encode("utf-8")).digest()


def _context_value(scalar: TypedScalar) -> object | None:
    value = scalar.logical_value
    if value is None:
        return None
    if scalar.field_type == "decimal":
        assert isinstance(value, Decimal)
        if value.is_zero():
            return "0"
        rendered = format(value, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered
    if scalar.field_type == "date":
        assert isinstance(value, dt.date)
        return value.isoformat()
    if scalar.field_type == "timestamp":
        assert isinstance(value, dt.datetime)
        return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    return value


def _compare_candidates(
    profile: SelectionProfile,
    left: _NormalizedWinnerCandidate,
    right: _NormalizedWinnerCandidate,
    fields_by_id: Mapping[str, Field],
) -> int:
    for term in profile.selection_terms:
        field = fields_by_id[term.field_id]
        comparison = _compare_term(
            _selection_value_for_order(left, field),
            _selection_value_for_order(right, field),
            direction=term.direction,
            nulls=term.nulls,
        )
        if comparison:
            return comparison
    left_key = _semantic_tie_key(left.candidate)
    right_key = _semantic_tie_key(right.candidate)
    return (left_key > right_key) - (left_key < right_key)


def _selection_value_for_order(
    candidate: _NormalizedWinnerCandidate,
    field: Field,
) -> object | None:
    """Return a selection value, ordering nullable absence as an explicit null.

    Context construction retains the missing/null distinction.  Selection has a
    narrower contract: an absent nullable field follows the profile's null
    policy, while an absent required field rejects the candidate.
    """

    scalar = candidate.values_by_field.get(field.field_id)
    if scalar is None:
        if not field.nullable:
            raise WinnerMaterializationError(f"winner candidate is missing required selection field {field.field_id}")
        return None
    return scalar.logical_value


def _validate_selection_values(
    profile: SelectionProfile,
    candidate: _NormalizedWinnerCandidate,
    fields_by_id: Mapping[str, Field],
) -> None:
    """Reject candidates that omit a required profile selection field."""

    for term in profile.selection_terms:
        _selection_value_for_order(candidate, fields_by_id[term.field_id])


def _compare_term(left: object | None, right: object | None, *, direction: str, nulls: str) -> int:
    if left is None or right is None:
        if left is None and right is None:
            return 0
        is_null_before = nulls == "first"
        return -1 if (left is None) == is_null_before else 1
    comparison = (left > right) - (left < right)
    return -comparison if direction == "desc" else comparison


def _semantic_tie_key(candidate: WinnerCandidate) -> tuple[bytes, bytes]:
    return (
        bytes(candidate.family_sha256),
        b"" if candidate.context_child_key_sha256 is None else bytes(candidate.context_child_key_sha256),
    )


def _validate_materialization(value: object) -> None:
    if not isinstance(value, WinnerMaterialization):
        raise WinnerMaterializationError("materialization must be a WinnerMaterialization")
    if not isinstance(value.generation, GenerationIdentity):
        raise WinnerMaterializationError("materialization generation is malformed")
    if (
        isinstance(value.profile_count, bool)
        or not isinstance(value.profile_count, int)
        or not 0 <= value.profile_count <= MAX_SELECTION_PROFILES
    ):
        raise WinnerMaterializationError("materialization profile count exceeds the v1 limit")
    if not isinstance(value.profile_context_slots, tuple) or len(value.profile_context_slots) != value.profile_count:
        raise WinnerMaterializationError("materialization profile scopes are malformed")
    for slot in value.profile_context_slots:
        if isinstance(slot, bool) or not isinstance(slot, int) or slot < 0:
            raise WinnerMaterializationError("materialization profile scopes are malformed")
    _validate_winner_rows(value.generation, value.profile_count, value.profile_context_slots, value.winners)


def _validate_winner_rows(
    generation: GenerationIdentity,
    profile_count: int,
    profile_context_slots: tuple[int, ...],
    winners: tuple[MaterializedWinner, ...],
) -> None:
    keys: set[tuple[int, int, bytes]] = set()
    for winner in winners:
        if not isinstance(winner, MaterializedWinner) or winner.generation != generation:
            raise WinnerMaterializationError("winner generation binding does not match its materialization")
        if not 1 <= winner.profile_slot <= profile_count:
            raise WinnerMaterializationError("winner profile slot is outside the materialization profile range")
        expected_context_slot = profile_context_slots[winner.profile_slot - 1]
        if winner.context_collection_slot != expected_context_slot:
            raise WinnerMaterializationError("winner context collection does not match its selection profile")
        _positive(winner.entity_binding_id, "entity_binding_id")
        _positive(winner.family_revision_id, "family_revision_id")
        _sha256(winner.context_key_sha256, "winner context key")
        _validate_canonical_context_key(winner.canonical_context_key)
        if _context_digest(winner.canonical_context_key) != winner.context_key_sha256:
            raise WinnerMaterializationError("winner context digest does not bind its canonical context")
        if winner.context_collection_slot == 0:
            if winner.context_child_revision_id is not None:
                raise WinnerMaterializationError("root winner contexts cannot carry a child revision")
        else:
            _positive(winner.context_collection_slot, "context_collection_slot")
            _positive(winner.context_child_revision_id, "context_child_revision_id")
        key = (winner.profile_slot, winner.entity_binding_id, winner.context_key_sha256)
        if key in keys:
            raise WinnerMaterializationError("winner materialization cannot repeat a lookup key")
        keys.add(key)


def _validate_canonical_context_key(value: object) -> None:
    if not isinstance(value, str) or not value:
        raise WinnerMaterializationError("winner canonical context is malformed")
    try:
        if len(value.encode("utf-8")) > _MAX_CONTEXT_BYTES:
            raise WinnerMaterializationError("winner canonical context is malformed")
    except UnicodeEncodeError as exc:
        raise WinnerMaterializationError("winner canonical context is malformed") from exc
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise WinnerMaterializationError("winner canonical context is malformed") from exc
    if not isinstance(parsed, Mapping):
        raise WinnerMaterializationError("winner canonical context is malformed")
    try:
        if canonical_json(parsed) != value:
            raise WinnerMaterializationError("winner canonical context is not canonical")
    except (TypeError, ValueError) as exc:
        raise WinnerMaterializationError("winner canonical context is malformed") from exc


def _add_models(session: Any, models: tuple[object, ...], label: str) -> None:
    add_all = getattr(session, "add_all", None)
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(add_all) or not callable(in_transaction):
        raise TypeError(f"{label} persistence requires an AsyncSession-style transaction")
    if not in_transaction():
        raise WinnerMaterializationError(f"{label} persistence requires an active caller transaction")
    add_all(models)


async def _flush(session: Any, label: str) -> None:
    flush = getattr(session, "flush", None)
    if not callable(flush):
        raise TypeError(f"{label} persistence requires an AsyncSession-style flush")
    result = flush()
    if inspect.isawaitable(result):
        await result


def _positive(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _sha256(value: object, label: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)) or len(value) != 32:
        raise WinnerMaterializationError(f"{label} must contain 32 bytes")
    return bytes(value)


def _require_mapping(value: object, label: str, error_type: type[ValueError]) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise error_type(f"{label} must be a mapping")
    return value
