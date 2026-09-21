# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validated durable definition and attempt state for candidate orchestration."""

from __future__ import annotations

import datetime as dt
import hmac
import time
from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportLease,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
)
from process.custom_import.definition import canonical_json
from process.custom_import.execution import MAX_LEASE_SECONDS, LeaseGrant, lease_token_sha256
from process.custom_import.materialization import (
    DefinitionIdentity,
    persist_selection_profiles,
    selection_profile_models,
)
from process.custom_import.runner_codec import definition_digest
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRegistry,
    CandidateRunnerError,
    CandidateRunRequest,
    CurrentGenerationPointer,
    LeaseAuthorityLost,
)

# A graph transaction owns the dataset, execution, and lease locks, so a
# separate heartbeat would wait behind it.  Give that transaction one bounded
# authoritative window instead.  It may never silently outlive the lifecycle
# module's maximum lease horizon.
_MATERIALIZATION_LEASE_WINDOW_SECONDS = MAX_LEASE_SECONDS
_MATERIALIZATION_WINDOW_KEY = "custom_import_runner_materialization_window"


@dataclass(frozen=True)
class _MaterializationLeaseWindow:
    """One local deadline bound to a fenced lease renewal in this transaction."""

    expires_at: dt.datetime
    monotonic_deadline: float


async def locked_candidate_context(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
) -> tuple[CandidateRegistry, CustomImportExecution, CurrentGenerationPointer | None]:
    """Lock a live attempt in finality order before writing candidate rows."""

    await lock_dataset(session, request.dataset_id)
    execution = await lock_execution(session, request)
    lease = await lock_lease(session, request.execution_id)
    now = await verify_live_attempt(session, request, grant, execution, lease)
    await establish_materialization_authority(session, request, grant, execution, lease, now)
    registry = await load_registry(session, request)
    pointer = await load_current_pointer(session, request.dataset_id)
    return registry, execution, pointer


async def lock_dataset(session: AsyncSession, dataset_id: int) -> None:
    """Acquire the shared dataset serialization parent."""

    dataset = (
        await session.execute(
            select(CustomImportDataset)
            .where(CustomImportDataset.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if dataset is None:
        raise CandidateRunnerError("candidate dataset does not exist")


async def lock_execution(session: AsyncSession, request: CandidateRunRequest) -> CustomImportExecution:
    """Lock and validate the immutable identity of the execution request."""

    execution = (
        await session.execute(
            select(CustomImportExecution)
            .where(CustomImportExecution.execution_id == request.execution_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if execution is None:
        raise CandidateRunnerError("candidate execution does not exist")
    if (
        execution.dataset_id != request.dataset_id
        or execution.definition_revision_id != request.definition_revision_id
        or execution.schema_revision_id != request.schema_revision_id
        or execution.capture_bundle_id is None
    ):
        raise CandidateRunnerError("candidate execution identity does not match the request")
    return execution


async def lock_lease(session: AsyncSession, execution_id: int) -> CustomImportLease | None:
    """Lock the one mutable fence row for an execution."""

    return (
        await session.execute(
            select(CustomImportLease)
            .where(CustomImportLease.execution_id == execution_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def verify_live_attempt(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    execution: CustomImportExecution,
    lease: CustomImportLease | None,
) -> dt.datetime:
    """Reject cancellation or stale fencing before any immutable append."""

    if execution.state == "canceling":
        raise CancellationRequested("candidate execution is canceling")
    now = await database_now(session)
    token_sha256 = lease_token_sha256(request.lease_token)
    if (
        execution.state != "running"
        or lease is None
        or lease.fence != grant.fence
        or lease.token_sha256 is None
        or not hmac.compare_digest(bytes(lease.token_sha256), token_sha256)
        or lease.expires_at is None
        or lease.expires_at <= now
    ):
        raise LeaseAuthorityLost("candidate execution lease is no longer current")
    return now


async def establish_materialization_authority(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    execution: CustomImportExecution,
    lease: CustomImportLease | None,
    now: dt.datetime,
) -> None:
    """Renew the exact fence for one bounded graph transaction.

    Dataset, execution, and lease are already locked in that order.  The
    conditional update remains the database fence even though no outside
    heartbeat can run while this graph transaction owns those locks.  A local
    monotonic deadline and transaction-local statement budgets then ensure the
    graph rolls back rather than committing writes after this lease window.
    """

    if lease is None:
        raise LeaseAuthorityLost("candidate execution lease is no longer current")
    if _MATERIALIZATION_WINDOW_KEY in session.info:
        raise CandidateRunnerError("candidate materialization authority is already bound to this transaction")
    token_sha256 = lease_token_sha256(request.lease_token)
    expires_at = now + dt.timedelta(seconds=_MATERIALIZATION_LEASE_WINDOW_SECONDS)
    with session.no_autoflush:
        renewed = await session.execute(
            update(CustomImportLease)
            .where(
                CustomImportLease.execution_id == execution.execution_id,
                CustomImportLease.fence == grant.fence,
                CustomImportLease.token_sha256 == token_sha256,
                CustomImportLease.expires_at > now,
            )
            .values(heartbeat_at=now, expires_at=expires_at, updated_at=now)
            .returning(CustomImportLease.expires_at)
        )
    if renewed.scalar_one_or_none() is None:
        raise LeaseAuthorityLost("candidate execution lease is no longer current")
    # Keep the locked model aligned without scheduling a second, unfenced ORM
    # update during the transaction context's final flush.
    set_committed_value(lease, "heartbeat_at", now)
    set_committed_value(lease, "expires_at", expires_at)
    session.info[_MATERIALIZATION_WINDOW_KEY] = _MaterializationLeaseWindow(
        expires_at=expires_at,
        monotonic_deadline=time.monotonic() + _MATERIALIZATION_LEASE_WINDOW_SECONDS,
    )


async def prepare_materialization_statement(session: AsyncSession) -> None:
    """Bound the next graph SQL statement to the fenced lease window.

    The graph may renew only while it owns the row locks; this helper is called
    immediately before each flush or materialization batch so an expired local
    window fails the whole transaction before a stale append can commit.
    """

    window = session.info.get(_MATERIALIZATION_WINDOW_KEY)
    if not isinstance(window, _MaterializationLeaseWindow):
        raise CandidateRunnerError("candidate materialization authority is not bound to this transaction")
    remaining_milliseconds = int((window.monotonic_deadline - time.monotonic()) * 1_000)
    if remaining_milliseconds <= 0:
        raise LeaseAuthorityLost("candidate materialization lease window expired")
    with session.no_autoflush:
        await session.execute(select(func.set_config("statement_timeout", str(max(1, remaining_milliseconds)), True)))


def clear_materialization_authority(session: AsyncSession) -> None:
    """Remove the transaction-local graph window after completion or rollback."""

    session.info.pop(_MATERIALIZATION_WINDOW_KEY, None)


async def database_now(session: AsyncSession) -> dt.datetime:
    """Read an aware timestamp from the authoritative PostgreSQL clock."""

    with session.no_autoflush:
        now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, dt.datetime) or now.tzinfo is None:
        raise CandidateRunnerError("database clock did not return an aware timestamp")
    return now


async def load_current_pointer(session: AsyncSession, dataset_id: int) -> CurrentGenerationPointer | None:
    """Load the current pointer after the dataset lock establishes a stable view."""

    pointer_model = (
        await session.execute(
            select(CustomImportCurrentGeneration)
            .where(CustomImportCurrentGeneration.dataset_id == dataset_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if pointer_model is None:
        return None
    return CurrentGenerationPointer(
        generation_id=pointer_model.generation_id,
        definition_revision_id=pointer_model.definition_revision_id,
        schema_revision_id=pointer_model.schema_revision_id,
        version=pointer_model.pointer_version,
    )


async def load_registry(session: AsyncSession, request: CandidateRunRequest) -> CandidateRegistry:
    """Validate the persisted immutable registry against the parsed definition."""

    await validate_revision_identity(session, request)
    collection_slots = await load_collection_slots(session, request)
    await validate_field_rows(session, request, collection_slots)
    await validate_field_slot_ledger(session, request)
    stream_slots, root_stream_slot = await load_stream_slots(session, request, collection_slots)
    await validate_alias_rows(session, request, stream_slots)
    return CandidateRegistry(
        child_collection_slots=collection_slots,
        stream_slots=stream_slots,
        root_stream_slot=root_stream_slot,
    )


async def validate_revision_identity(session: AsyncSession, request: CandidateRunRequest) -> None:
    """Require exact durable definition and schema documents and hashes."""

    definition_model = await session.get(CustomImportDefinitionRevision, request.definition_revision_id)
    schema_model = await session.get(CustomImportSchemaRevision, request.schema_revision_id)
    if (
        definition_model is None
        or schema_model is None
        or definition_model.dataset_id != request.dataset_id
        or schema_model.dataset_id != request.dataset_id
        or definition_model.schema_revision_id != request.schema_revision_id
        or definition_model.canonical_definition != request.definition.canonical
        or bytes(definition_model.definition_sha256) != bytes.fromhex(request.definition.digest)
        or schema_model.canonical_schema != request.definition.schema_canonical
        or bytes(schema_model.schema_sha256) != bytes.fromhex(request.definition.schema_digest)
    ):
        raise CandidateRunnerError("persisted definition or schema does not match the candidate definition")


async def load_collection_slots(session: AsyncSession, request: CandidateRunRequest) -> Mapping[str, int]:
    """Validate definition-owned child collection names, slots, and key shapes."""

    collection_models = list(
        (
            await session.scalars(
                select(CustomImportChildCollection).where(
                    CustomImportChildCollection.dataset_id == request.dataset_id,
                    CustomImportChildCollection.schema_revision_id == request.schema_revision_id,
                )
            )
        ).all()
    )
    collection_by_name = {model.collection_name: model for model in collection_models}
    collection_slots_by_name = {model.collection_name: model.collection_slot for model in collection_models}
    expected_names = {collection.name for collection in request.definition.child_collections}
    if set(collection_slots_by_name) != expected_names or len(set(collection_slots_by_name.values())) != len(
        collection_slots_by_name
    ):
        raise CandidateRunnerError("persisted child collection slots do not match the definition")
    for collection in request.definition.child_collections:
        collection_model = collection_by_name.get(collection.name)
        key_shape_dict = {
            "child_key": list(collection.child_key),
            "parent_key": [{"child": part.child_field, "root": part.root_field} for part in collection.parent_key],
        }
        canonical_key_shape = canonical_json(key_shape_dict)
        if (
            collection_model is None
            or collection_model.canonical_key_shape != canonical_key_shape
            or bytes(collection_model.key_shape_sha256) != definition_digest("schema", canonical_key_shape)
        ):
            raise CandidateRunnerError("persisted child collection keys do not match the definition")
    return collection_slots_by_name


async def validate_field_rows(
    session: AsyncSession,
    request: CandidateRunRequest,
    collection_slots: Mapping[str, int],
) -> None:
    """Require each current schema field to exactly match its parsed contract."""

    field_models = list(
        (
            await session.scalars(
                select(CustomImportField).where(
                    CustomImportField.dataset_id == request.dataset_id,
                    CustomImportField.schema_revision_id == request.schema_revision_id,
                )
            )
        ).all()
    )
    fields_by_slot = {model.field_slot: model for model in field_models}
    if len(fields_by_slot) != len(request.definition.fields):
        raise CandidateRunnerError("persisted field rows do not match the definition")
    for field in request.definition.fields:
        field_model = fields_by_slot.get(field.field_slot)
        expected_collection_slot = 0 if field.collection is None else collection_slots[field.collection]
        expected_projection_slot = field.projection_slot or 0
        if (
            field_model is None
            or field_model.field_name != field.field_id
            or field_model.collection_slot != expected_collection_slot
            or field_model.field_type != field.value_type
            or field_model.is_nullable != field.nullable
            or field_model.projection_slot != expected_projection_slot
        ):
            raise CandidateRunnerError("persisted field rows do not match the definition")


async def validate_field_slot_ledger(session: AsyncSession, request: CandidateRunRequest) -> None:
    """Require current field identities without rejecting later ledger entries."""

    slot_models = list(
        (
            await session.scalars(
                select(CustomImportFieldSlot).where(CustomImportFieldSlot.dataset_id == request.dataset_id)
            )
        ).all()
    )
    slots_by_number = {model.field_slot: model.field_id for model in slot_models}
    slots_by_name = {model.field_id: model.field_slot for model in slot_models}
    if (
        len(slots_by_number) != len(slot_models)
        or len(slots_by_name) != len(slot_models)
        or any(
            slots_by_number.get(field.field_slot) != field.field_id
            or slots_by_name.get(field.field_id) != field.field_slot
            for field in request.definition.fields
        )
    ):
        raise CandidateRunnerError("persisted field slots do not match the definition")


async def load_stream_slots(
    session: AsyncSession,
    request: CandidateRunRequest,
    collection_slots: Mapping[str, int],
) -> tuple[Mapping[str, int], int]:
    """Validate durable source streams and return their current slot registry."""

    stream_models = list(
        (
            await session.scalars(
                select(CustomImportSourceStream).where(
                    CustomImportSourceStream.dataset_id == request.dataset_id,
                    CustomImportSourceStream.definition_revision_id == request.definition_revision_id,
                    CustomImportSourceStream.schema_revision_id == request.schema_revision_id,
                )
            )
        ).all()
    )
    streams_by_id = {model.stream_id: model for model in stream_models}
    if len(streams_by_id) != len(request.definition.source_streams):
        raise CandidateRunnerError("persisted source streams do not match the definition")
    for stream in request.definition.source_streams:
        stream_model = streams_by_id.get(stream.stream_id)
        expected_collection_slot = (
            None if stream.child_collection is None else collection_slots[stream.child_collection]
        )
        if (
            stream_model is None
            or stream_model.record_kind != stream.record_kind
            or stream_model.collection_slot != expected_collection_slot
            or stream_model.decoder != stream.format
            or stream_model.compression != stream.compression
            or stream_model.snapshot_token_selector != stream.snapshot_token
            or stream_model.record_path != stream.record_path
        ):
            raise CandidateRunnerError("persisted source streams do not match the definition")
    if len({model.stream_slot for model in stream_models}) != len(stream_models):
        raise CandidateRunnerError("persisted source stream slots are not unique")
    root_stream_models = [model for model in stream_models if model.record_kind == "root"]
    if len(root_stream_models) != 1:
        raise CandidateRunnerError("persisted definition must have one root stream")
    return (
        {stream_id: model.stream_slot for stream_id, model in streams_by_id.items()},
        root_stream_models[0].stream_slot,
    )


async def validate_alias_rows(
    session: AsyncSession,
    request: CandidateRunRequest,
    stream_slots: Mapping[str, int],
) -> None:
    """Require exact parser-owned source alias to field-slot mappings."""

    alias_models = list(
        (
            await session.scalars(
                select(CustomImportFieldAlias).where(
                    CustomImportFieldAlias.dataset_id == request.dataset_id,
                    CustomImportFieldAlias.definition_revision_id == request.definition_revision_id,
                    CustomImportFieldAlias.schema_revision_id == request.schema_revision_id,
                )
            )
        ).all()
    )
    actual_aliases = {(model.stream_slot, model.alias_name, model.field_slot) for model in alias_models}
    expected_aliases = {
        (
            stream_slots[alias.stream_id],
            alias.source_label,
            request.definition.fields_by_id[alias.field_id].field_slot,
        )
        for alias in request.definition.aliases
    }
    if actual_aliases != expected_aliases:
        raise CandidateRunnerError("persisted field aliases do not match the definition")


async def ensure_selection_profiles(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
) -> None:
    """Persist an absent immutable profile set or validate an exact replay."""

    await prepare_materialization_statement(session)
    identity = DefinitionIdentity(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    expected_profiles = selection_profile_models(
        request.definition,
        identity=identity,
        child_collection_slots=registry.child_collection_slots,
    )
    persisted_profiles = list(
        (
            await session.scalars(
                select(CustomImportSelectionProfile)
                .where(
                    CustomImportSelectionProfile.dataset_id == request.dataset_id,
                    CustomImportSelectionProfile.definition_revision_id == request.definition_revision_id,
                    CustomImportSelectionProfile.schema_revision_id == request.schema_revision_id,
                )
                .order_by(CustomImportSelectionProfile.profile_slot)
            )
        ).all()
    )
    if not persisted_profiles:
        await prepare_materialization_statement(session)
        await persist_selection_profiles(
            session,
            request.definition,
            identity=identity,
            child_collection_slots=registry.child_collection_slots,
        )
        return
    if len(persisted_profiles) != len(expected_profiles) or any(
        has_profile_mismatch(actual_profile, expected_profile)
        for actual_profile, expected_profile in zip(persisted_profiles, expected_profiles, strict=True)
    ):
        raise CandidateRunnerError("persisted selection profiles do not match the definition")


def has_profile_mismatch(
    actual_profile: CustomImportSelectionProfile, expected_profile: CustomImportSelectionProfile
) -> bool:
    """Return whether one persisted profile differs from its immutable plan."""

    return (
        actual_profile.profile_slot != expected_profile.profile_slot
        or actual_profile.profile_id != expected_profile.profile_id
        or actual_profile.context_collection_slot != expected_profile.context_collection_slot
        or actual_profile.canonical_profile != expected_profile.canonical_profile
        or bytes(actual_profile.profile_sha256) != bytes(expected_profile.profile_sha256)
    )
