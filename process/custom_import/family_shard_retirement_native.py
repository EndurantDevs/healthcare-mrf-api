# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Permanent retired custody for file cleanup; no request, lease or writer is minted."""

import hashlib
from dataclasses import dataclass, field
from types import SimpleNamespace

import msgpack
from sqlalchemy import select, text

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCaptureBundle,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportLease,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRetirement
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_handoff as handoff
from process.custom_import.build_source import _set_timeout
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import TERMINAL_STATES, _database_now
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT, ROLE_SPECS, FamilyBlockLayout
from process.custom_import.family_shard_owner_access import require_owner_connection
from process.custom_import.retirement import _IDENTITY_NAMES
from process.custom_import.retirement_history import assert_closed_continuations
from process.custom_import.storage_layout import snapshot_schema

CONTRACT = "custom-import/retired-owner-files/v1"


@dataclass(frozen=True)
class RetiredOwnerScope:
    """Only immutable historical scope; deliberately lacks a writer/read binding."""

    family_id: int
    scope: tuple = field(repr=False)
    head: object
    sha256: str


@dataclass(frozen=True)
class TerminalOwnerScope:
    """Committed producer identity only; this is never physical retirement authority."""

    family_id: int
    scope: tuple = field(repr=False)
    head: object


def _same_producer(descriptor, family, execution, lease, build, capture):
    expected_by_field = {
        "dataset_id": descriptor["dataset_id"],
        "definition_revision_id": descriptor["definition_revision_id"],
        "schema_revision_id": descriptor["schema_revision_id"],
        "execution_id": descriptor["engine_execution_id"],
        "capture_bundle_id": descriptor["capture_bundle_id"],
        "producing_fence": descriptor["fence"],
        "producing_token_sha256": bytes.fromhex(descriptor["bearer_sha256"]),
    }
    if any(getattr(family, name) != expected_value for name, expected_value in expected_by_field.items()) or (
        execution.idempotency_key != descriptor["controller_execution_id"]
        or bytes(execution.request_identity_sha256).hex() != descriptor["request_identity_sha256"]
        or execution.source_binding_revision_id != descriptor["source_binding_revision_id"]
        or lease.fence < family.producing_fence
        or lease.fence == family.producing_fence
        and execution.state not in TERMINAL_STATES
        or build.generation_id != family.generation_id
        or build.base_generation_id != descriptor["base_generation_id"]
        or build.base_pointer_version != descriptor["base_pointer_version"]
        or build.build_deadline_at.isoformat().replace("+00:00", "Z") != descriptor["build_deadline_at"]
        or capture.capture_state != "sealed"
        or bytes(capture.manifest_sha256).hex() != descriptor["capture_manifest_sha256"]
        or capture.source_binding_revision_id != descriptor["source_binding_revision_id"]
        or bytes(capture.source_binding_sha256).hex() != descriptor["source_binding_sha256"]
    ):
        handoff.unavailable()
    for name in (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    ):
        if getattr(build, name) != getattr(family, name):
            handoff.unavailable()
    if bytes(build.request_identity_sha256).hex() != descriptor["request_identity_sha256"]:
        handoff.unavailable()


def _same_parent(descriptor, family, retired, execution, lease, build, capture):
    """Preserve the immutable retirement-to-original-producer identity join."""
    _same_producer(descriptor, family, execution, lease, build, capture)
    if any(getattr(retired, name) != getattr(family, name) for name in _IDENTITY_NAMES):
        handoff.unavailable()


def _retired_relations(rows, owner_oid):
    slots_by_role = {role: slot for role, (slot, _name) in ROLE_SPECS.items()}
    if len(rows) != len(slots_by_role):
        handoff.unavailable()
    oids_by_role = {}
    for row in rows:
        if slots_by_role.pop(row.role_name, None) != row.role_slot or (
            row.table_owner != owner_oid
            or type(row.table_oid) is not int
            or row.table_oid <= 0
            or row.table_oid in oids_by_role.values()
            or len(bytes(row.columns_sha256)) != 32
            or len(bytes(row.constraints_sha256)) != 32
        ):
            handoff.unavailable()
        oids_by_role[row.role_name] = row.table_oid
    if slots_by_role:
        handoff.unavailable()
    return SimpleNamespace(owner_oid=owner_oid, oids_by_role=oids_by_role)


async def _producer_rows(session, descriptor):
    family = (
        await session.scalars(
            select(CustomImportSnapshotFamily).where(
                CustomImportSnapshotFamily.execution_id == descriptor["engine_execution_id"],
                CustomImportSnapshotFamily.producing_fence == descriptor["fence"],
            )
        )
    ).one_or_none()
    if family is None:
        handoff.unavailable()
    execution = await session.get(CustomImportExecution, family.execution_id)
    lease = await session.get(CustomImportLease, family.execution_id)
    capture = await session.get(CustomImportCaptureBundle, family.capture_bundle_id)
    build = (
        await session.scalars(
            select(CustomImportBuildAttempt).where(
                CustomImportBuildAttempt.execution_id == family.execution_id,
                CustomImportBuildAttempt.producing_fence == family.producing_fence,
            )
        )
    ).one_or_none()
    if any(native_row is None for native_row in (execution, lease, capture, build)):
        handoff.unavailable()
    _same_producer(descriptor, family, execution, lease, build, capture)
    return family, execution, lease, build, capture


async def _retired_rows(session, descriptor):
    family, execution, lease, build, capture = await _producer_rows(session, descriptor)
    retired = await session.get(CustomImportSnapshotRetirement, family.family_id)
    if retired is None:
        handoff.unavailable()
    _same_parent(descriptor, family, retired, execution, lease, build, capture)
    if (
        await session.scalar(
            text("SELECT pg_catalog.to_regnamespace(:namespace)"), {"namespace": snapshot_schema(family.family_id)}
        )
        is not None
    ):
        handoff.unavailable()
    await assert_closed_continuations(session, family)
    return family, retired, build, capture


async def _committed_scope(session, family, build, capture):
    """Reconstruct the same immutable layout, producer scope and committed head."""
    metadata = await session.get(CustomImportFamilyBlockLayout, family.family_id)
    revision = await session.get(CustomImportDefinitionRevision, family.definition_revision_id)
    if (
        metadata is None
        or revision is None
        or (
            metadata.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
            or revision.dataset_id != family.dataset_id
            or revision.schema_revision_id != family.schema_revision_id
        )
    ):
        handoff.unavailable()
    definition = CustomImportDefinition.from_json(revision.canonical_definition)
    layout = FamilyBlockLayout(definition, metadata.collection_slots)
    if bytes(revision.definition_sha256) != bytes.fromhex(definition.digest) or (
        bytes(metadata.definition_sha256) != bytes.fromhex(definition.digest)
        or bytes(metadata.layout_sha256) != layout.digest
    ):
        handoff.unavailable()
    relations = (
        await session.scalars(
            select(CustomImportFamilyBlockRelation)
            .where(CustomImportFamilyBlockRelation.family_id == family.family_id)
            .order_by(CustomImportFamilyBlockRelation.role_slot)
        )
    ).all()
    owner_oid = await session.scalar(text("SELECT CURRENT_USER::regrole::oid"))
    relations = _retired_relations(relations, owner_oid)
    identity = SimpleNamespace(
        dataset_id=family.dataset_id,
        definition_revision_id=family.definition_revision_id,
        schema_revision_id=family.schema_revision_id,
        definition=definition,
    )
    scope = checkpoint._scope_cells(build, capture, identity, layout, relations, family.family_id)
    head = checkpoint._head(metadata)
    if head is None:
        handoff.unavailable()
    return scope, head


async def read_retired_scope(factory, parent, profile):
    """Require committed physical retirement before observing historical checkpoints."""
    descriptor = handoff.checked_descriptor(parent["descriptor"])
    async with factory() as session, session.begin():
        await require_owner_connection(session, profile["owner_role"])
        family, retired, build, capture = await _retired_rows(session, descriptor)
        scope, head = await _committed_scope(session, family, build, capture)
        evidence = (
            CONTRACT,
            handoff.identity(parent),
            tuple(getattr(retired, name) for name in _IDENTITY_NAMES),
            retired.engine_id,
            retired.reason,
            retired.retired_at.isoformat(),
            scope,
            head.revision,
            head.sha256,
            descriptor["owner_profile_sha256"],
        )
        digest = hashlib.sha256(msgpack.packb(evidence, use_bin_type=True)).hexdigest()
        return RetiredOwnerScope(family.family_id, scope, head, digest)


async def read_terminal_scope(factory, parent, profile):
    """Read terminal producer custody without minting a lease or retirement evidence."""
    from process.custom_import.family_shard_production import _continued_producer

    descriptor = handoff.checked_descriptor(parent["descriptor"])
    async with factory() as session, session.begin():
        await _set_timeout(session, 2000)
        await require_owner_connection(session, profile["owner_role"])
        family, execution, lease, build, capture = await _producer_rows(session, descriptor)
        if (
            execution.state not in TERMINAL_STATES
            or lease.fence != descriptor["fence"]
            or lease.token_sha256 is None
            or lease.expires_at is None
            or lease.expires_at > await _database_now(session)
        ):
            handoff.unavailable()
        if lease.continuation_id is None:
            if bytes(lease.token_sha256).hex() != descriptor["bearer_sha256"]:
                handoff.unavailable()
        else:
            await _continued_producer(session, descriptor, execution, lease, build)
        scope, head = await _committed_scope(session, family, build, capture)
        return TerminalOwnerScope(family.family_id, scope, head)
