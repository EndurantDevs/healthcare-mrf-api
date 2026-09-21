# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional persistence for immutable ``custom-import/v1`` definitions."""

from __future__ import annotations

import hmac
import re
from collections.abc import Iterable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
)
from process.custom_import.definition import (
    CONTRACT_VERSION,
    ChildCollection,
    CustomImportDefinition,
    DefinitionError,
    Field,
    canonical_json,
    canonical_sha256,
)
from process.custom_import.materialization import DefinitionIdentity, selection_profile_models
from process.custom_import.runner_registry import (
    has_profile_mismatch,
    load_collection_slots,
    load_registry,
    validate_field_rows,
    validate_field_slot_ledger,
)
from process.custom_import.runner_types import CandidateRunnerError, CandidateRunRequest

__all__ = ("DefinitionRegistrationError", "RegisteredDefinition", "register_definition")


_DATASET_KEY = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class DefinitionRegistrationError(ValueError):
    """A definition cannot be safely registered as an immutable revision."""


@dataclass(frozen=True)
class RegisteredDefinition:
    """Compact stable identity returned by definition registration."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    created: bool


@dataclass(frozen=True)
class _LockedDefinitionState:
    dataset: CustomImportDataset
    definition_rows: tuple[CustomImportDefinitionRevision, ...]
    schema_rows: tuple[CustomImportSchemaRevision, ...]
    field_slot_rows: tuple[CustomImportFieldSlot, ...]


def _normalized_dataset_key(value: object) -> str:
    if not isinstance(value, str) or not _DATASET_KEY.fullmatch(value):
        raise DefinitionRegistrationError("dataset_key must be lower_snake_case")
    return value


def _canonical_definition(value: object) -> CustomImportDefinition:
    if not isinstance(value, CustomImportDefinition):
        raise TypeError("definition must be a CustomImportDefinition")
    try:
        canonical_definition = CustomImportDefinition.from_json(value.canonical)
    except (DefinitionError, TypeError, ValueError) as exc:
        raise DefinitionRegistrationError("definition canonical content is invalid") from exc
    if canonical_definition != value:
        raise DefinitionRegistrationError("definition canonical content does not match its fields")
    return canonical_definition


def _require_transaction(session: AsyncSession) -> None:
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise DefinitionRegistrationError("definition registration requires an active caller transaction")


def _require_clean_session(session: AsyncSession) -> None:
    if any(bool(getattr(session, attribute, ())) for attribute in ("new", "dirty", "deleted")):
        raise DefinitionRegistrationError("definition registration requires a clean session")


def _no_autoflush(session: AsyncSession):
    return getattr(session, "no_autoflush", nullcontext())


async def _locked_dataset(session: AsyncSession, dataset_key: str) -> CustomImportDataset:
    await session.execute(
        pg_insert(CustomImportDataset)
        .values(dataset_key=dataset_key)
        .on_conflict_do_nothing(index_elements=(CustomImportDataset.dataset_key,))
    )
    with _no_autoflush(session):
        result = await session.execute(
            select(CustomImportDataset)
            .where(CustomImportDataset.dataset_key == dataset_key)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    dataset = result.scalar_one_or_none()
    if dataset is None or dataset.dataset_key != dataset_key:
        raise DefinitionRegistrationError("registered dataset is unavailable")
    return dataset


async def _locked_dataset_rows(
    session: AsyncSession,
    model: type[Any],
    dataset_id: int,
) -> tuple[Any, ...]:
    with _no_autoflush(session):
        result = await session.execute(
            select(model)
            .where(model.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    return tuple(result.scalars().all())


async def _locked_definition_state(session: AsyncSession, dataset_key: str) -> _LockedDefinitionState:
    dataset = await _locked_dataset(session, dataset_key)
    return _LockedDefinitionState(
        dataset=dataset,
        definition_rows=await _locked_dataset_rows(session, CustomImportDefinitionRevision, dataset.dataset_id),
        schema_rows=await _locked_dataset_rows(session, CustomImportSchemaRevision, dataset.dataset_id),
        field_slot_rows=await _locked_dataset_rows(session, CustomImportFieldSlot, dataset.dataset_id),
    )


def _row_by_revision(
    rows: Iterable[Any],
    revision_number: int,
    label: str,
) -> Any | None:
    matching_rows = tuple(row for row in rows if row.revision_number == revision_number)
    if len(matching_rows) > 1:
        raise DefinitionRegistrationError(f"persisted {label} revision identity is ambiguous")
    return matching_rows[0] if matching_rows else None


def _is_matching_digest(value: object, expected: bytes) -> bool:
    try:
        return hmac.compare_digest(bytes(value), expected)
    except TypeError, ValueError:
        return False


def _row_by_digest(rows: Iterable[Any], attribute: str, expected: bytes, label: str) -> Any | None:
    matching_rows = tuple(row for row in rows if _is_matching_digest(getattr(row, attribute), expected))
    if len(matching_rows) > 1:
        raise DefinitionRegistrationError(f"persisted {label} content identity is ambiguous")
    return matching_rows[0] if matching_rows else None


def _is_matching_schema(
    schema_row: CustomImportSchemaRevision,
    definition: CustomImportDefinition,
) -> bool:
    return (
        schema_row.revision_number == definition.schema_revision
        and schema_row.canonical_schema == definition.schema_canonical
        and _is_matching_digest(schema_row.schema_sha256, bytes.fromhex(definition.schema_digest))
    )


def _is_matching_definition(
    definition_row: CustomImportDefinitionRevision,
    definition: CustomImportDefinition,
    schema_revision_id: int,
) -> bool:
    return (
        definition_row.schema_revision_id == schema_revision_id
        and definition_row.revision_number == definition.definition_revision
        and definition_row.contract_version == CONTRACT_VERSION
        and definition_row.refresh_mode == definition.refresh_mode
        and definition_row.canonical_definition == definition.canonical
        and _is_matching_digest(definition_row.definition_sha256, bytes.fromhex(definition.digest))
    )


def _persisted_definition(
    definition_row: CustomImportDefinitionRevision,
    schema_row: CustomImportSchemaRevision,
) -> CustomImportDefinition:
    try:
        parsed_definition = CustomImportDefinition.from_json(definition_row.canonical_definition)
    except (DefinitionError, TypeError, ValueError) as exc:
        raise DefinitionRegistrationError("persisted definition canonical content is invalid") from exc
    if not _is_matching_schema(schema_row, parsed_definition) or not _is_matching_definition(
        definition_row,
        parsed_definition,
        schema_row.schema_revision_id,
    ):
        raise DefinitionRegistrationError("persisted definition identity is invalid")
    return parsed_definition


def _validate_transition(
    definition: CustomImportDefinition,
    definition_rows: Iterable[CustomImportDefinitionRevision],
    schema_rows: Iterable[CustomImportSchemaRevision],
) -> None:
    previous_row = max(definition_rows, key=lambda row: row.revision_number, default=None)
    if previous_row is None:
        return
    schema_by_id = {schema_row.schema_revision_id: schema_row for schema_row in schema_rows}
    previous_schema = schema_by_id.get(previous_row.schema_revision_id)
    if previous_schema is None:
        raise DefinitionRegistrationError("persisted definition schema is unavailable")
    previous_definition = _persisted_definition(previous_row, previous_schema)
    try:
        CustomImportDefinition.from_json(definition.canonical, previous=previous_definition)
    except (DefinitionError, TypeError, ValueError) as exc:
        raise DefinitionRegistrationError("definition revision transition is invalid") from exc


def _new_field_slots(
    fields: Iterable[Field],
    slot_rows: Iterable[CustomImportFieldSlot],
) -> tuple[Field, ...]:
    field_by_slot = {field_slot.field_slot: field_slot.field_id for field_slot in slot_rows}
    slot_by_field = {field_slot.field_id: field_slot.field_slot for field_slot in slot_rows}
    new_fields: list[Field] = []
    for field in fields:
        bound_field_id = field_by_slot.get(field.field_slot)
        bound_slot = slot_by_field.get(field.field_id)
        if bound_field_id not in {None, field.field_id} or bound_slot not in {None, field.field_slot}:
            raise DefinitionRegistrationError("stable field slot identity is already bound differently")
        if bound_field_id is None:
            new_fields.append(field)
    return tuple(new_fields)


def _collection_slot_by_name(definition: CustomImportDefinition) -> dict[str, int]:
    return {
        collection.name: collection_slot
        for collection_slot, collection in enumerate(definition.child_collections, start=1)
    }


def _key_shape(collection: ChildCollection) -> Mapping[str, object]:
    return {
        "child_key": list(collection.child_key),
        "parent_key": [
            {"child": key_part.child_field, "root": key_part.root_field} for key_part in collection.parent_key
        ],
    }


async def _persist_schema(
    session: AsyncSession,
    dataset_id: int,
    definition: CustomImportDefinition,
    new_field_slots: Iterable[Field],
    collection_slot_by_name: Mapping[str, int],
) -> CustomImportSchemaRevision:
    schema_row = CustomImportSchemaRevision(
        dataset_id=dataset_id,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    session.add(schema_row)
    await session.flush()

    slot_models = tuple(
        CustomImportFieldSlot(
            dataset_id=dataset_id,
            field_slot=field.field_slot,
            field_id=field.field_id,
        )
        for field in new_field_slots
    )
    if slot_models:
        session.add_all(slot_models)
        await session.flush()

    child_collection_models = tuple(
        CustomImportChildCollection(
            schema_revision_id=schema_row.schema_revision_id,
            dataset_id=dataset_id,
            collection_slot=collection_slot_by_name[collection.name],
            collection_name=collection.name,
            canonical_key_shape=canonical_json(_key_shape(collection)),
            key_shape_sha256=bytes.fromhex(canonical_sha256(_key_shape(collection), domain="schema")),
        )
        for collection in definition.child_collections
    )
    field_models = tuple(
        CustomImportField(
            schema_revision_id=schema_row.schema_revision_id,
            dataset_id=dataset_id,
            field_slot=field.field_slot,
            collection_slot=0 if field.collection is None else collection_slot_by_name[field.collection],
            field_name=field.field_id,
            field_type=field.value_type,
            is_nullable=field.nullable,
            projection_slot=field.projection_slot or 0,
        )
        for field in definition.fields
    )
    session.add_all((*child_collection_models, *field_models))
    await session.flush()
    return schema_row


async def _persist_definition(
    session: AsyncSession,
    dataset_id: int,
    schema_row: CustomImportSchemaRevision,
    definition: CustomImportDefinition,
    collection_slot_by_name: Mapping[str, int],
) -> CustomImportDefinitionRevision:
    """Persist one parent definition and its source/profile descendants."""

    definition_row = CustomImportDefinitionRevision(
        dataset_id=dataset_id,
        schema_revision_id=schema_row.schema_revision_id,
        revision_number=definition.definition_revision,
        contract_version=CONTRACT_VERSION,
        refresh_mode=definition.refresh_mode,
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    session.add(definition_row)
    await session.flush()

    stream_slot_by_id = await _persist_source_streams(
        session,
        dataset_id,
        schema_row,
        definition_row,
        definition,
        collection_slot_by_name,
    )
    await _persist_aliases_and_profiles(
        session,
        dataset_id,
        schema_row,
        definition_row,
        definition,
        collection_slot_by_name,
        stream_slot_by_id,
    )
    return definition_row


async def _persist_source_streams(
    session: AsyncSession,
    dataset_id: int,
    schema_row: CustomImportSchemaRevision,
    definition_row: CustomImportDefinitionRevision,
    definition: CustomImportDefinition,
    collection_slot_by_name: Mapping[str, int],
) -> dict[str, int]:
    stream_slot_by_id = {
        stream.stream_id: stream_slot for stream_slot, stream in enumerate(definition.source_streams, start=1)
    }
    stream_models = tuple(
        CustomImportSourceStream(
            definition_revision_id=definition_row.definition_revision_id,
            dataset_id=dataset_id,
            schema_revision_id=schema_row.schema_revision_id,
            stream_slot=stream_slot_by_id[stream.stream_id],
            stream_id=stream.stream_id,
            record_kind=stream.record_kind,
            collection_slot=None
            if stream.child_collection is None
            else collection_slot_by_name[stream.child_collection],
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for stream in definition.source_streams
    )
    session.add_all(stream_models)
    await session.flush()
    return stream_slot_by_id


async def _persist_aliases_and_profiles(
    session: AsyncSession,
    dataset_id: int,
    schema_row: CustomImportSchemaRevision,
    definition_row: CustomImportDefinitionRevision,
    definition: CustomImportDefinition,
    collection_slot_by_name: Mapping[str, int],
    stream_slot_by_id: Mapping[str, int],
) -> None:
    field_slot_by_id = {field.field_id: field.field_slot for field in definition.fields}
    alias_models = tuple(
        CustomImportFieldAlias(
            definition_revision_id=definition_row.definition_revision_id,
            dataset_id=dataset_id,
            schema_revision_id=schema_row.schema_revision_id,
            stream_slot=stream_slot_by_id[alias.stream_id],
            alias_name=alias.source_label,
            field_slot=field_slot_by_id[alias.field_id],
        )
        for alias in definition.aliases
    )
    profile_models = selection_profile_models(
        definition,
        identity=DefinitionIdentity(
            dataset_id=dataset_id,
            definition_revision_id=definition_row.definition_revision_id,
            schema_revision_id=schema_row.schema_revision_id,
        ),
        child_collection_slots=collection_slot_by_name,
    )
    if alias_models or profile_models:
        session.add_all((*alias_models, *profile_models))
        await session.flush()


def _existing_registration(
    state: _LockedDefinitionState,
    definition: CustomImportDefinition,
) -> tuple[RegisteredDefinition | None, CustomImportSchemaRevision | None]:
    definition_by_revision = _row_by_revision(
        state.definition_rows,
        definition.definition_revision,
        "definition",
    )
    definition_by_digest = _row_by_digest(
        state.definition_rows,
        "definition_sha256",
        bytes.fromhex(definition.digest),
        "definition",
    )
    schema_by_revision = _row_by_revision(state.schema_rows, definition.schema_revision, "schema")
    schema_by_digest = _row_by_digest(
        state.schema_rows,
        "schema_sha256",
        bytes.fromhex(definition.schema_digest),
        "schema",
    )
    if definition_by_revision is not None:
        if schema_by_revision is None or not _is_matching_schema(schema_by_revision, definition):
            raise DefinitionRegistrationError("definition revision is bound to a different schema")
        if not _is_matching_definition(definition_by_revision, definition, schema_by_revision.schema_revision_id):
            raise DefinitionRegistrationError("definition revision is already bound to different content")
        return (
            RegisteredDefinition(
                dataset_id=state.dataset.dataset_id,
                definition_revision_id=definition_by_revision.definition_revision_id,
                schema_revision_id=schema_by_revision.schema_revision_id,
                created=False,
            ),
            schema_by_revision,
        )
    if definition_by_digest is not None:
        raise DefinitionRegistrationError("definition content is already bound to another revision")
    if schema_by_revision is not None:
        if not _is_matching_schema(schema_by_revision, definition):
            raise DefinitionRegistrationError("schema revision is already bound to different content")
        return None, schema_by_revision
    if schema_by_digest is not None:
        raise DefinitionRegistrationError("schema content is already bound to another revision")
    return None, None


async def _validate_replay_graph(
    session: AsyncSession,
    registration: RegisteredDefinition,
    definition: CustomImportDefinition,
) -> None:
    request = CandidateRunRequest(
        dataset_id=registration.dataset_id,
        definition_revision_id=registration.definition_revision_id,
        schema_revision_id=registration.schema_revision_id,
        execution_id=1,
        lease_token=b"definition-replay",
        definition=definition,
        roots=(),
        children_by_collection={},
    )
    try:
        registry = await load_registry(session, request)
    except CandidateRunnerError as exc:
        raise DefinitionRegistrationError("persisted definition graph does not match its content") from exc
    expected_profiles = selection_profile_models(
        definition,
        identity=DefinitionIdentity(
            dataset_id=registration.dataset_id,
            definition_revision_id=registration.definition_revision_id,
            schema_revision_id=registration.schema_revision_id,
        ),
        child_collection_slots=registry.child_collection_slots,
    )
    with _no_autoflush(session):
        profile_rows = await session.execute(
            select(CustomImportSelectionProfile)
            .where(
                CustomImportSelectionProfile.dataset_id == registration.dataset_id,
                CustomImportSelectionProfile.definition_revision_id == registration.definition_revision_id,
                CustomImportSelectionProfile.schema_revision_id == registration.schema_revision_id,
            )
            .order_by(CustomImportSelectionProfile.profile_slot)
        )
    persisted_profiles = tuple(profile_rows.scalars().all())
    if len(persisted_profiles) != len(expected_profiles) or any(
        has_profile_mismatch(actual, expected)
        for actual, expected in zip(persisted_profiles, expected_profiles, strict=True)
    ):
        raise DefinitionRegistrationError("persisted definition graph does not match its content")


async def _validate_reused_schema(
    session: AsyncSession,
    dataset_id: int,
    schema_revision_id: int,
    definition: CustomImportDefinition,
) -> Mapping[str, int]:
    request = CandidateRunRequest(
        dataset_id=dataset_id,
        definition_revision_id=1,
        schema_revision_id=schema_revision_id,
        execution_id=1,
        lease_token=b"schema-reuse",
        definition=definition,
        roots=(),
        children_by_collection={},
    )
    try:
        collection_slots = await load_collection_slots(session, request)
        await validate_field_rows(session, request, collection_slots)
        await validate_field_slot_ledger(session, request)
    except CandidateRunnerError as exc:
        raise DefinitionRegistrationError("persisted schema graph does not match its content") from exc
    return collection_slots


async def register_definition(
    session: AsyncSession,
    dataset_key: str,
    definition: CustomImportDefinition,
) -> RegisteredDefinition:
    """Create one immutable definition revision or return its exact replay.

    The caller owns an active transaction.  Existing dataset state is locked
    before it is inspected so a concurrent registration cannot choose the same
    revision independently.  Every drift check happens before definition rows
    are added to that transaction.
    """

    normalized_dataset_key = _normalized_dataset_key(dataset_key)
    canonical_definition = _canonical_definition(definition)
    _require_transaction(session)
    _require_clean_session(session)

    state = await _locked_definition_state(session, normalized_dataset_key)
    replay, schema_row = _existing_registration(state, canonical_definition)
    if replay is not None:
        await _validate_replay_graph(session, replay, canonical_definition)
        return replay

    _validate_transition(canonical_definition, state.definition_rows, state.schema_rows)
    new_field_slots = _new_field_slots(canonical_definition.fields, state.field_slot_rows)
    collection_slot_by_name = _collection_slot_by_name(canonical_definition)
    if schema_row is None:
        schema_row = await _persist_schema(
            session,
            state.dataset.dataset_id,
            canonical_definition,
            new_field_slots,
            collection_slot_by_name,
        )
    else:
        collection_slot_by_name = await _validate_reused_schema(
            session,
            state.dataset.dataset_id,
            schema_row.schema_revision_id,
            canonical_definition,
        )

    definition_row = await _persist_definition(
        session,
        state.dataset.dataset_id,
        schema_row,
        canonical_definition,
        collection_slot_by_name,
    )
    return RegisteredDefinition(
        dataset_id=state.dataset.dataset_id,
        definition_revision_id=definition_row.definition_revision_id,
        schema_revision_id=schema_row.schema_revision_id,
        created=True,
    )
