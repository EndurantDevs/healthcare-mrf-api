# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Separate closed audit history from authority that can still consume storage."""

from sqlalchemy import and_, exists, or_, select

from db.models.custom_import import CustomImportBuildAttempt as Build
from db.models.custom_import import (
    CustomImportDefinitionRevision,
)
from db.models.custom_import import (
    CustomImportExecution as Execution,
)
from db.models.custom_import import (
    CustomImportLease as Lease,
)
from db.models.custom_import_continuation import (
    CustomImportBuildContinuation as Continuation,
)
from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import TERMINAL_STATES
from process.custom_import.query_projection_binding import lock_projection_tables


def _continuation_identity_differs(family):
    return or_(
        Build.execution_id != Continuation.execution_id,
        Build.capture_bundle_id != Continuation.capture_bundle_id,
        Build.producing_fence != Continuation.producer_fence,
        Build.producing_token_sha256 != Continuation.producer_token_sha256,
        Build.base_generation_id.is_distinct_from(Continuation.base_generation_id),
        *(
            getattr(Build, name) != getattr(Continuation, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
        ),
        *(
            getattr(Continuation, name) != getattr(family, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
        ),
        and_(
            Continuation.snapshot_family_id == family.family_id,
            or_(
                Continuation.execution_id != family.execution_id,
                Continuation.capture_bundle_id != family.capture_bundle_id,
                Continuation.producer_fence != family.producing_fence,
                Continuation.producer_token_sha256 != family.producing_token_sha256,
            ),
        ),
        *(
            getattr(Execution, name) != getattr(Continuation, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
        ),
    )


async def assert_closed_continuations(session, family):
    """Keep history while refusing any continuation or build with usable authority."""
    from process.custom_import.retirement import SnapshotRetirementRefused

    is_referenced = Continuation.snapshot_family_id == family.family_id
    if family.generation_id is not None:
        is_referenced = or_(is_referenced, Continuation.base_generation_id == family.generation_id)
    closed = Execution.state.in_(TERMINAL_STATES)
    if family.frozen_at is None:
        closed = or_(
            closed,
            and_(
                Continuation.snapshot_family_id == family.family_id,
                Continuation.execution_id == family.execution_id,
                Continuation.producer_fence == family.producing_fence,
                Lease.fence > family.producing_fence,
            ),
        )
    query = (
        select(1)
        .select_from(Continuation)
        .outerjoin(Build, Build.build_id == Continuation.build_id)
        .outerjoin(
            Execution,
            Execution.execution_id == Continuation.execution_id,
        )
        .outerjoin(Lease, Lease.execution_id == Continuation.execution_id)
        .where(
            is_referenced,
            or_(
                Build.build_id.is_(None),
                Execution.execution_id.is_(None),
                Lease.execution_id.is_(None),
                _continuation_identity_differs(family),
                ~closed,
            ),
        )
    )
    if await session.scalar(select(exists(query))):
        raise SnapshotRetirementRefused("snapshot retirement retains active continuation authority")
    await _assert_unused_build_base(session, family)


async def _assert_unused_build_base(session, family):
    from process.custom_import.retirement import SnapshotRetirementRefused

    if family.generation_id is not None:
        pending = (
            select(1)
            .select_from(Build)
            .outerjoin(Execution, Execution.execution_id == Build.execution_id)
            .where(
                Build.base_generation_id == family.generation_id,
                or_(Execution.execution_id.is_(None), ~Execution.state.in_(TERMINAL_STATES)),
            )
        )
        if await session.scalar(select(exists(pending))):
            raise SnapshotRetirementRefused("snapshot retirement target is an active retained build base")


async def lock_historical_projection(session, family):
    """Preserve the manifest, verifying and pinning its exact physical leaves for retirement."""
    from process.custom_import.retirement import SnapshotRetirementRefused

    manifest = await session.scalar(
        select(CustomImportQueryProjection)
        .where(
            CustomImportQueryProjection.family_id == family.family_id,
        )
        .with_for_update()
    )
    if manifest is None:
        return ()
    definition = await session.get(CustomImportDefinitionRevision, family.definition_revision_id)
    if definition is None:
        raise SnapshotRetirementRefused("snapshot retirement projection definition is missing")
    parsed = CustomImportDefinition.from_json(definition.canonical_definition)
    tables = await lock_projection_tables(session, manifest, parsed, require_complete=manifest.phase == "complete")
    return tuple(tables)
