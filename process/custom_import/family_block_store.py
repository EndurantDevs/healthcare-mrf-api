# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only parallel COPY and durable closure for an isolated block candidate.

CPU work and complete-family validation belong outside these transactions. Every
mutation checks the existing dataset/execution/lease authority immediately before
commit. Bulk work holds only candidate relation locks. This module grants no new
writer capability. Only the existing engine owner can use it.
"""

import json
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import BigInteger, exists, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCaptureBundle,
    CustomImportDataset,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRetirement
from process.custom_import.build_source import (
    _WINDOW,
    _assert_build_identity,
    _call,
    _close_contexts,
    _enter_context,
    _flush_page,
    _initial_page_window,
    _is_source_writer_owner,
    _lock_page,
    _page_session,
    _PageWindow,
    _prepare_statement,
)
from process.custom_import.continuation_runtime import processing_deadline, producer_token_sha256
from process.custom_import.execution import LeaseGrant
from process.custom_import.family_block_binding import (
    FamilyBlockUnavailable,
    has_family_block_indexes,
    resolve_family_block_binding,
    validate_family_block_indexes,
)
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import (
    FAMILY_BLOCK_LAYOUT_CONTRACT,
    READ_CAPABILITIES,
    ROLE_SPECS,
    copy_columns,
    index_statements,
    load_statements,
)
from process.custom_import.family_block_validate import CandidateScope, ClosedCandidateIdentity, check_closed_identity
from process.custom_import.runner_registry import lock_execution, lock_lease, verify_live_attempt
from process.custom_import.runner_types import LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_schema


async def _execute(session, statement, parameters=None):
    await _prepare_statement(session)
    return await session.execute(text(statement) if isinstance(statement, str) else statement, parameters or {})


async def _control_schema(session):
    connection = await session.connection()
    model = CustomImportSnapshotFamily.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model, model)
    if not schema:
        raise FamilyBlockUnavailable("family block control schema is missing")
    return connection.dialect.identifier_preparer, schema


async def _owner(session):
    if not await _is_source_writer_owner(session):
        raise FamilyBlockUnavailable("family block writes require the existing engine owner")
    quote, schema = await _control_schema(session)
    names = [
        f"{quote.quote_schema(schema)}.{name}"
        for name in (
            "custom_import_snapshot_family",
            "custom_import_family_block_layout",
            "custom_import_family_block_relation",
        )
    ]
    permitted = (
        await _execute(
            session,
            "SELECT count(*)=3 AND bool_and(pg_catalog.pg_get_userbyid(relowner)=CURRENT_USER) "
            "FROM pg_catalog.pg_class WHERE oid=ANY(CAST(:names AS regclass[]))",
            {"names": names},
        )
    ).scalar_one()
    if permitted is not True:
        raise FamilyBlockUnavailable("family block control custody differs")


def _authority(request):
    return (
        ("bigint", request.execution_id),
        ("bigint", request.fence),
        ("bytea", producer_token_sha256(request)),
    )


def _continuation_build_id(request):
    context = getattr(request, "continuation", None)
    return None if context is None else context.build_id


def _require_candidate_producer(family, request):
    if (
        family is None
        or family.frozen_at is not None
        or (
            family.dataset_id,
            family.definition_revision_id,
            family.schema_revision_id,
            family.execution_id,
            family.producing_fence,
            bytes(family.producing_token_sha256),
        )
        != (
            request.dataset_id,
            request.definition_revision_id,
            request.schema_revision_id,
            request.execution_id,
            request.fence,
            producer_token_sha256(request),
        )
    ):
        raise FamilyBlockUnavailable("family block producer or write closure differs")


async def _require_unsealed_candidate(session, request, family):
    execution_capture = (
        await _execute(
            session,
            select(CustomImportExecution.capture_bundle_id).where(
                CustomImportExecution.execution_id == request.execution_id
            ),
        )
    ).scalar_one()
    if family.capture_bundle_id != execution_capture:
        raise FamilyBlockUnavailable("family block capture binding differs")
    quote, control = await _control_schema(session)
    closed = (
        await _execute(
            session,
            f"SELECT EXISTS (SELECT 1 FROM {quote.quote_schema(control)}.custom_import_generation_seal WHERE generation_id=:generation) "
            f"OR EXISTS (SELECT 1 FROM {quote.quote_schema(control)}.custom_import_no_change_seal "
            "WHERE execution_id=:execution AND dataset_id=:dataset)",
            {"generation": family.generation_id, "execution": family.execution_id, "dataset": family.dataset_id},
        )
    ).scalar_one()
    if closed:
        raise FamilyBlockUnavailable("family block writes are sealed")


async def _candidate(session, request, family_id, layout, phases, *, locked=True):
    """Validate candidate scope and finality before resolving its existing role custody."""
    if layout.definition.digest != request.definition.digest:
        raise FamilyBlockUnavailable("family block requested definition differs")
    await _owner(session)
    if locked:
        # Older owner allocation/create calls already hold control locks. Never
        # wait for a leaf or catalog lookup behind transition DDL in that order.
        # Fixed names are untrusted until the complete binding proof below.
        await _relation_barrier(session, layout.tables(family_id), loading=True, nowait=True)
    statement = (
        select(CustomImportSnapshotFamily)
        .where(CustomImportSnapshotFamily.family_id == family_id)
        .execution_options(populate_existing=True)
    )
    family = (await _execute(session, statement.with_for_update() if locked else statement)).scalar_one_or_none()
    _require_candidate_producer(family, request)
    await _require_unsealed_candidate(session, request, family)
    statement = (
        select(CustomImportFamilyBlockLayout)
        .where(CustomImportFamilyBlockLayout.family_id == family_id)
        .execution_options(populate_existing=True)
    )
    descriptor = (await _execute(session, statement.with_for_update() if locked else statement)).scalar_one_or_none()
    if descriptor is None or descriptor.phase not in phases or bytes(descriptor.layout_sha256) != layout.digest:
        raise FamilyBlockUnavailable("family block phase or descriptor differs")
    if locked and descriptor.phase == "loading":
        resolved = (await _call(session, "lock_custom_import_writable_snapshot", _authority(request))).scalar_one()
        if resolved != family_id:
            raise FamilyBlockUnavailable("family block writer resolved another candidate")
    binding = await resolve_family_block_binding(
        session,
        family_id,
        layout.definition,
        layout.collection_slots,
        persistence="candidate",
        prepare=_prepare_statement,
    )
    if binding is None:
        raise FamilyBlockUnavailable("family block writer resolved a legacy snapshot")
    return descriptor, binding


async def _relation_barrier(session, tables, *, loading, nowait=False):
    # COPY locks coexist. Transitions also exclude each other, avoiding two
    # SHARE holders both upgrading to ACCESS EXCLUSIVE for SET LOGGED.
    quote, _ = await _control_schema(session)
    targets = ",".join(quote.format_table(table) for table in tables.values())
    mode = "ROW EXCLUSIVE" if loading else "SHARE ROW EXCLUSIVE"
    await _execute(session, f"LOCK TABLE {targets} IN {mode} MODE" + (" NOWAIT" if nowait else ""))


async def _observe_attempt(session, request):
    """Fail early without control-row locks; this observation cannot authorize commit."""
    candidate_row = (
        await _execute(
            session,
            select(
                CustomImportExecution,
                CustomImportLease,
                CustomImportCaptureBundle,
                CustomImportBuildAttempt,
                exists().where(
                    CustomImportSnapshotRetirement.execution_id == request.execution_id,
                    CustomImportSnapshotRetirement.producing_fence == request.fence,
                ),
            )
            .select_from(CustomImportExecution)
            .join(CustomImportDataset, CustomImportDataset.dataset_id == CustomImportExecution.dataset_id)
            .outerjoin(CustomImportLease, CustomImportLease.execution_id == CustomImportExecution.execution_id)
            .outerjoin(
                CustomImportCaptureBundle,
                CustomImportCaptureBundle.capture_bundle_id == CustomImportExecution.capture_bundle_id,
            )
            .outerjoin(
                CustomImportBuildAttempt,
                (CustomImportBuildAttempt.execution_id == CustomImportExecution.execution_id)
                & (CustomImportBuildAttempt.producing_fence == request.fence),
            )
            .where(CustomImportExecution.execution_id == request.execution_id)
            .execution_options(populate_existing=True),
        )
    ).one_or_none()
    if candidate_row is None:
        raise FamilyBlockUnavailable("family block execution is missing")
    execution, lease, capture, build, retired = candidate_row
    scope = request.dataset_id, request.definition_revision_id, request.schema_revision_id
    if retired or (execution.dataset_id, execution.definition_revision_id, execution.schema_revision_id) != scope:
        raise FamilyBlockUnavailable("family block execution scope or retirement differs")
    if (
        capture is None
        or capture.capture_state != "sealed"
        or (capture.dataset_id, capture.definition_revision_id, capture.schema_revision_id) != scope
    ):
        raise FamilyBlockUnavailable("family block sealed capture differs")
    now = await verify_live_attempt(
        session,
        request,
        LeaseGrant(request.execution_id, request.fence, request.build_deadline_at, "running"),
        execution,
        lease,
    )
    _check_deadlines(request, now)
    if build is not None:
        _assert_build_identity(build, request, execution)
    elif request.continuation is not None:
        raise FamilyBlockUnavailable("family block continuation build is missing")
    return None if build is None else build.build_id


def _check_deadlines(request, now):
    deadline = processing_deadline(request)
    if deadline is not None and now >= deadline:
        raise LeaseAuthorityLost("build deadline elapsed before block commit")
    if request.authorization_expires_at is not None and now >= request.authorization_expires_at:
        raise LeaseAuthorityLost("writer authorization expired before block commit")


def _descriptor_state(descriptor):
    return deepcopy(
        {column.name: getattr(descriptor, column.name) for column in CustomImportFamilyBlockLayout.__table__.c}
    )


async def _fresh_commit_authority(session, request):
    await _flush_page(session)
    await _prepare_statement(session)
    execution = await lock_execution(session, request)
    await _prepare_statement(session)
    lease = await lock_lease(session, request.execution_id)
    await _prepare_statement(session)
    now = await verify_live_attempt(
        session,
        request,
        LeaseGrant(request.execution_id, request.fence, request.build_deadline_at, "running"),
        execution,
        lease,
    )
    _check_deadlines(request, now)
    await _prepare_statement(session)


@dataclass(frozen=True)
class SourceAllocationTransition:
    """One caller's provisional checkpoint values, bound to its original SOURCE build."""

    build_id: int
    values: dict


def _check_source_transition_build(transition, build_id):
    if isinstance(transition, SourceAllocationTransition) and (
        type(transition.build_id) is not int or transition.build_id <= 0 or transition.build_id != build_id
    ):
        raise FamilyBlockUnavailable("family block expected build differs")


async def _lock_source_transition_build(session, request, build_id, transition, original_window):
    build = await _lock_page(session, replace(request, operation_deadline=original_window.deadline), build_id)
    if isinstance(transition, SourceAllocationTransition) and (
        build is None or (build.build_id, build.phase) != (transition.build_id, "source")
    ):
        raise FamilyBlockUnavailable("family block expected build phase changed before commit")
    renewed = session.info[_WINDOW]
    session.info[_WINDOW] = _PageWindow(
        min(original_window.deadline, renewed.deadline),
        min(original_window.statement_timeout_ms, renewed.statement_timeout_ms),
    )


def _source_transition_values(transition):
    if not isinstance(transition, SourceAllocationTransition):
        return transition
    if set(transition.values) != {"owner_checkpoint_revision", "owner_checkpoint_sha256"}:
        raise FamilyBlockUnavailable("SOURCE checkpoint transition is not completely prepared")
    return transition.values


@asynccontextmanager
async def _block_session(
    session_factory, request, family_id, layout, phase, *, loading=False, transition=None, expected_identity=None
):
    """Hold leaf custody during bulk work, then check all authority before committing.

    Transition updates occur only after the established authority lock order.
    The initial observed lease/operation window never widens during bulk work.
    """
    contexts, primary = [], None
    try:
        session = await _enter_context(contexts, session_factory())
        await _enter_context(contexts, session.begin())
        await _initial_page_window(session, request)
        original_window = session.info[_WINDOW]
        await _owner(session)
        build_id = await _observe_attempt(session, request)
        _check_source_transition_build(transition, build_id)
        # Acquire fixed-name leaf locks before catalog functions can acquire
        # weaker leaf locks and create an upgrade cycle with another transition.
        await _relation_barrier(session, layout.tables(family_id), loading=loading)
        if await _observe_attempt(session, request) != build_id:
            raise FamilyBlockUnavailable("family block build changed while acquiring leaf custody")
        phases = {phase} if isinstance(phase, str) else set(phase)
        descriptor, binding = await _candidate(session, request, family_id, layout, phases, locked=False)
        observed_phase = descriptor.phase
        expected_descriptor = _descriptor_state(descriptor)
        if expected_identity is not None:
            await _validation_identity(session, descriptor, binding, expected_identity)
        yield session, descriptor, binding
        await _prepare_statement(session)
        # _lock_page also bounds its lock acquisition. A concurrent heartbeat
        # cannot extend this transaction's original observed deadline.
        await _lock_source_transition_build(session, request, build_id, transition, original_window)
        if observed_phase != "loading":
            # The writable resolver intentionally refuses closed phases. Retain
            # its native capture/build/continuation/retirement authority checks.
            await _call(session, "lock_custom_import_snapshot_attempt", _authority(request))
        current, current_binding = await _candidate(session, request, family_id, layout, {observed_phase})
        if _descriptor_state(current) != expected_descriptor:
            raise FamilyBlockUnavailable("family block descriptor changed during bulk work")
        if expected_identity is not None:
            await _validation_identity(session, current, current_binding, expected_identity)
        if transition:
            await _execute(
                session,
                update(CustomImportFamilyBlockLayout)
                .where(CustomImportFamilyBlockLayout.family_id == family_id)
                .values(**_source_transition_values(transition)),
            )
        await _fresh_commit_authority(session, request)
    except BaseException as exc:
        primary = exc
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary


async def _revoke_defaults(session, *, kind, identity, oid):
    """The existing protected creator's explicit-ACL cleanup, without new roles."""
    quote, _ = await _control_schema(session)
    await _execute(session, f"REVOKE ALL ON {kind} {identity} FROM PUBLIC")
    catalog, acl, owner, default_kind = (
        ("pg_namespace", "nspacl", "nspowner", "n") if kind == "SCHEMA" else ("pg_class", "relacl", "relowner", "r")
    )
    grantees = (
        (
            await _execute(
                session,
                f"SELECT DISTINCT pg_catalog.pg_get_userbyid(a.grantee) FROM pg_catalog.{catalog} c, "
                f"LATERAL pg_catalog.aclexplode(coalesce(c.{acl},pg_catalog.acldefault('{default_kind}',c.{owner}))) a "
                f"WHERE c.oid=:oid AND a.grantee NOT IN (0,c.{owner})",
                {"oid": oid},
            )
        )
        .scalars()
        .all()
    )
    for grantee in grantees:
        await _execute(session, f"REVOKE ALL ON {kind} {identity} FROM {quote.quote(grantee)}")


async def reader_capability_plan(session, role, *, prepare=None, source_family_id=None):
    """Read-only review material; this does not execute or authorize a GRANT."""
    if role not in READ_CAPABILITIES:
        raise ValueError("unknown family block read role")
    if not READ_CAPABILITIES[role]:
        return ()
    quote, schema = await _control_schema(session)
    if source_family_id is not None:
        schema = snapshot_schema(source_family_id)
    names = [f"{quote.quote_schema(schema)}.{name}" for name in READ_CAPABILITIES[role]]
    # A role must already hold every contributing old table's explicit SELECT.
    # No UNION of capabilities, PUBLIC propagation, write right or grant option.
    if prepare:
        await prepare(session)
    grantees = (
        (
            await session.execute(
                text(
                    "SELECT pg_catalog.pg_get_userbyid(a.grantee) FROM pg_catalog.pg_class c, "
                    "LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a "
                    "WHERE c.oid=ANY(CAST(:names AS regclass[])) AND a.privilege_type='SELECT' AND a.grantee NOT IN (0,c.relowner) "
                    "GROUP BY a.grantee HAVING count(DISTINCT c.oid)=:count",
                ),
                {"names": names, "count": len(names)},
            )
        )
        .scalars()
        .all()
    )
    return tuple(grantees)


async def _register_candidate_roles(session, family_id, layout, quote, schema):
    for statement in load_statements(family_id, layout):
        await _execute(session, statement)
    tables = layout.tables(family_id)
    for role, table in tables.items():
        identity = quote.format_table(table)
        candidate_row = (
            (
                await _execute(
                    session,
                    "SELECT c.oid::bigint AS table_oid,c.relowner::bigint AS table_owner, "
                    f"{quote.quote_schema(schema)}.custom_import_snapshot_columns_sha256(c.oid::bigint) AS columns_sha256, "
                    "(SELECT pg_catalog.sha256(pg_catalog.convert_to(coalesce(pg_catalog.string_agg("
                    "ROW(k.contype,pg_catalog.pg_get_constraintdef(k.oid))::text,E'\\n' ORDER BY k.contype,"
                    "pg_catalog.pg_get_constraintdef(k.oid)),''),'UTF8')) FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid) AS constraints_sha256 "
                    "FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass(:identity)",
                    {"identity": identity},
                )
            )
            .mappings()
            .one()
        )
        await _revoke_defaults(session, kind="TABLE", identity=identity, oid=candidate_row["table_oid"])
        await _execute(
            session,
            insert(CustomImportFamilyBlockRelation).values(
                family_id=family_id, role_slot=ROLE_SPECS[role][0], role_name=role, **candidate_row
            ),
        )
    comment = layout.comment.replace("'", "''")
    await _execute(session, f"COMMENT ON TABLE {quote.format_table(tables['family'])} IS '{comment}'")


async def create_candidate(session_factory, request, layout):
    """Atomically create/register one owner-only UNLOGGED candidate or exact replay.

    Reader ACL propagation requires a separately reviewed existing-capability
    mapping. This storage preparation never executes a new object GRANT.
    """
    if layout.definition.digest != request.definition.digest:
        raise FamilyBlockUnavailable("family block requested definition differs")
    async with _page_session(session_factory, request, _continuation_build_id(request)) as (session, _):
        await _owner(session)
        execution = (await _call(session, "lock_custom_import_snapshot_attempt", _authority(request))).mappings().one()
        old = (
            await _execute(
                session,
                select(CustomImportSnapshotFamily.family_id).where(
                    CustomImportSnapshotFamily.execution_id == request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == request.fence,
                ),
            )
        ).scalar_one_or_none()
        if old is not None:
            await _candidate(session, request, old, layout, {"loading", "closed", "verified", "logged"})
            return old
        family_id = (
            await _execute(
                session,
                insert(CustomImportSnapshotFamily)
                .values(
                    dataset_id=request.dataset_id,
                    definition_revision_id=request.definition_revision_id,
                    schema_revision_id=request.schema_revision_id,
                    execution_id=request.execution_id,
                    capture_bundle_id=execution["capture_bundle_id"],
                    producing_fence=request.fence,
                    producing_token_sha256=producer_token_sha256(request),
                )
                .returning(CustomImportSnapshotFamily.family_id),
            )
        ).scalar_one()
        await _execute(
            session,
            insert(CustomImportFamilyBlockLayout).values(
                family_id=family_id,
                layout_contract=FAMILY_BLOCK_LAYOUT_CONTRACT,
                definition_sha256=bytes.fromhex(layout.definition.digest),
                layout_sha256=layout.digest,
                collection_slots=dict(layout.collection_slots),
                phase="loading",
            ),
        )
        quote, schema = await _control_schema(session)
        namespace = snapshot_schema(family_id)
        await _execute(session, f"CREATE SCHEMA {quote.quote_schema(namespace)}")
        namespace_oid = (
            await _execute(session, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:name", {"name": namespace})
        ).scalar_one()
        await _revoke_defaults(session, kind="SCHEMA", identity=quote.quote_schema(namespace), oid=namespace_oid)
        await _register_candidate_roles(session, family_id, layout, quote, schema)
        await _candidate(session, request, family_id, layout, {"loading"})
        return family_id


def _size(value):
    if value is None:
        return 4
    if isinstance(value, bytes | bytearray | memoryview):
        return 4 + len(value)
    if isinstance(value, str):
        return 4 + len(value.encode("utf-8"))
    if type(value) is bool:
        return 5
    if isinstance(value, int | float | date | datetime | Decimal):
        return 16 + len(str(value))
    if isinstance(value, Mapping | list | tuple):
        return 4 + len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
    raise ValueError("unsupported native COPY cell")


def check_copy_batch(layout, family_id, batches, *, maximum_rows, maximum_bytes):
    """Inspect only an already bounded batch outside locks; budget failure is retryable."""
    if type(maximum_rows) is not int or type(maximum_bytes) is not int or min(maximum_rows, maximum_bytes) <= 0:
        raise ValueError("COPY requires positive caller resource bounds")
    tables = layout.tables(family_id)
    if not isinstance(batches, Mapping) or not batches or set(batches) - set(tables) or "shard_receipt" in batches:
        raise ValueError("COPY requires a nonempty closed-role batch")
    count = size = 0
    for role, rows in batches.items():
        if not isinstance(rows, Sequence):
            raise ValueError("COPY requires prebounded rows")
        for row in rows:
            if len(row) != len(copy_columns(tables[role])):
                raise ValueError("COPY row shape differs")
            count += 1
            size += 2 + sum(_size(value) for value in row)
            if count > maximum_rows or size > maximum_bytes:
                raise FamilyBlockResourceLimit("COPY exceeds the caller's batch budget")
    if not count:
        raise ValueError("COPY requires at least one row")
    return count


async def close_candidate(session_factory, request, family_id, layout):
    """Close candidate writes using the original protected snapshot authority."""
    async with _block_session(
        session_factory,
        request,
        family_id,
        layout,
        "loading",
        transition={"phase": "closed", "closed_at": func.clock_timestamp()},
    ):
        return


async def prepare_indexes(session_factory, request, family_id, layout):
    """Prepare or exactly replay indexes within the unchanged authority window."""
    async with _block_session(session_factory, request, family_id, layout, "closed") as (session, _, _binding):
        if await has_family_block_indexes(session, _binding, prepare=_prepare_statement):
            return
        for statement in index_statements(family_id, layout):
            await _execute(session, statement)
        await validate_family_block_indexes(session, _binding, prepare=_prepare_statement)


def _validation_input(identity, digest):
    if type(identity) is not ClosedCandidateIdentity or type(identity.scope) is not CandidateScope:
        raise ValueError("validation requires its completed original candidate identity")
    if not isinstance(digest, bytes) or len(digest) != 32:
        raise ValueError("closed candidate requires a complete owner validation commitment")
    return identity.scope.family_id


async def _validation_row(session, model, *predicates):
    return (await _execute(session, select(model.__table__).where(*predicates))).mappings().one_or_none()


async def _validation_identity(session, descriptor, binding, expected):
    scope = expected.scope
    snapshot = await _validation_row(
        session, CustomImportSnapshotFamily, CustomImportSnapshotFamily.family_id == scope.family_id
    )
    generation = await _validation_row(
        session, CustomImportGeneration, CustomImportGeneration.generation_id == scope.generation_id
    )
    build = await _validation_row(
        session,
        CustomImportBuildAttempt,
        CustomImportBuildAttempt.execution_id == scope.execution_id,
        CustomImportBuildAttempt.producing_fence == scope.producing_fence,
    )
    expected_by_name = {name: cell for name, cell in scope.parameters().items() if name != "family_id"}
    if snapshot is None or any(
        row is None or any(row[name] != expected_cell for name, expected_cell in expected_by_name.items())
        for row in (generation, build)
    ):
        raise FamilyBlockUnavailable("completed validation generation, build or base scope differs")
    if (
        check_closed_identity(binding, scope, snapshot, _descriptor_state(descriptor), phase=descriptor.phase)
        != expected
    ):
        raise FamilyBlockUnavailable("completed validation candidate identity differs")


def _completed_validation(descriptor, digest, counts):
    if descriptor.validation_sha256 != digest or any(
        getattr(descriptor, name) != count for name, count in counts.items()
    ):
        raise FamilyBlockUnavailable("completed owner validation proof or census differs")


async def _logged_binding(session, binding):
    actual = await resolve_family_block_binding(
        session,
        binding.family_id,
        binding.layout.definition,
        binding.layout.collection_slots,
        persistence="logged",
        prepare=_prepare_statement,
    )
    if actual is None or (actual.owner_oid, dict(actual.oids_by_role), actual.layout.digest) != (
        binding.owner_oid,
        dict(binding.oids_by_role),
        binding.layout.digest,
    ):
        raise FamilyBlockUnavailable("logged candidate custody differs")


async def _log_unlogged_roles(session, binding):
    rows = (
        (
            await _execute(
                session,
                "SELECT oid::bigint AS oid,relpersistence::text AS persistence FROM pg_catalog.pg_class "
                "WHERE oid=ANY(CAST(:oids AS oid[]))",
                {"oids": list(binding.oids_by_role.values())},
            )
        )
        .mappings()
        .all()
    )
    persistence_by_oid = {row["oid"]: row["persistence"] for row in rows}
    if (
        len(rows) != len(binding.oids_by_role)
        or set(persistence_by_oid) != set(binding.oids_by_role.values())
        or any(kind not in ("u", "p") for kind in persistence_by_oid.values())
    ):
        raise FamilyBlockUnavailable("candidate native persistence census differs")
    quote, _ = await _control_schema(session)
    for role, table in binding.tables_by_role.items():
        if persistence_by_oid[binding.oids_by_role[role]] == "u":
            await _execute(session, f"ALTER TABLE {quote.format_table(table)} SET LOGGED")


async def accept_validation(
    session_factory,
    request,
    expected_identity,
    layout,
    *,
    validation_sha256,
    expected_completed_shards,
    source_record_count,
    role_counts,
):
    """Bind the completed original proof and replay only its exact stored commitment.

    The caller must complete all existing authority/scope/lineage/scalar/winner
    guards, full codec/sparse proofs, directory-to-record comparisons, exact native
    revision homes, required indexes and aggregate accounting. This low-level
    storage transition does not implement or substitute for those checks.
    """
    family_id = _validation_input(expected_identity, validation_sha256)
    counts = _checked_census(expected_completed_shards, source_record_count, role_counts)
    transition_by_name = {}
    async with _block_session(
        session_factory,
        request,
        family_id,
        layout,
        ("closed", "verified", "logged"),
        transition=transition_by_name,
        expected_identity=expected_identity,
    ) as (session, descriptor, binding):
        if descriptor.phase == "closed":
            transition_by_name.update(phase="verified", validation_sha256=validation_sha256, **counts)
        else:
            _completed_validation(descriptor, validation_sha256, counts)
        await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
        await _verify_shard_census(session, binding, counts)
        if descriptor.phase == "logged":
            await _logged_binding(session, binding)


async def make_logged(
    session_factory,
    request,
    expected_identity,
    layout,
    *,
    validation_sha256,
    expected_completed_shards,
    source_record_count,
    role_counts,
):
    """Make the exact verified candidate durable; replay checks without repeating DDL."""
    family_id = _validation_input(expected_identity, validation_sha256)
    counts = _checked_census(expected_completed_shards, source_record_count, role_counts)
    transition_by_name = {}
    async with _block_session(
        session_factory,
        request,
        family_id,
        layout,
        ("verified", "logged"),
        transition=transition_by_name,
        expected_identity=expected_identity,
    ) as (session, descriptor, binding):
        _completed_validation(descriptor, validation_sha256, counts)
        await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
        await _verify_shard_census(session, binding, counts)
        if descriptor.phase == "verified":
            await _log_unlogged_roles(session, binding)
            transition_by_name.update(phase="logged")
        await _logged_binding(session, binding)


class ShardOutputProof:
    """Streaming digest of exact COPY cells; chunk boundaries do not change it.

    Workers use this while writing their immutable owned spool. The coordinator
    repeats it over copied cells, not by trusting a worker-declared result hash.
    Row order, role names, actual IDs and scalar representations are committed.
    """

    def __init__(self, layout, shard_id, partition_sha256, source_record_count):
        import hashlib

        from process.custom_import.family_block_records import _integer, _sha256

        _integer(shard_id, "shard identity", zero=True)
        _integer(source_record_count, "shard source count", zero=True)
        _sha256(partition_sha256)
        self.shard_id, self.partition_sha256, self.source_record_count = (
            shard_id,
            bytes(partition_sha256),
            source_record_count,
        )
        self.role_counts = {role: 0 for role in ROLE_SPECS if role != "shard_receipt"}
        self._digest = hashlib.sha256(b"custom-import/family-block-shard-output/v1\x00")
        self._feed((layout.digest, shard_id, self.partition_sha256, source_record_count))

    def _feed(self, value):
        import struct

        import msgpack

        from process.custom_import.family_shard_input_records import _encode_scalar

        packed = msgpack.packb(value, use_bin_type=True, default=_encode_scalar)
        self._digest.update(struct.pack("!Q", len(packed)))
        self._digest.update(packed)

    def add(self, role, rows):
        """Accumulate canonical output bytes and counts for one bounded role batch."""
        if role not in self.role_counts:
            raise ValueError("shard output role differs")
        for row in rows:
            self._feed((role, row))
            self.role_counts[role] += 1

    def receipt(self):
        """Return role counts and digests for the exact candidate snapshot scope."""
        import struct

        import msgpack

        digest = self._digest.copy()
        footer = msgpack.packb(("complete", tuple(self.role_counts.items())), use_bin_type=True)
        digest.update(struct.pack("!Q", len(footer)))
        digest.update(footer)
        return {
            "shard_id": self.shard_id,
            "partition_sha256": self.partition_sha256,
            "result_sha256": digest.digest(),
            "source_record_count": self.source_record_count,
            "role_counts": dict(self.role_counts),
        }


def _checked_receipt(layout, receipt):
    """Validate the exact replay shape without permitting missing count evidence."""
    from process.custom_import.family_block_records import _integer, _sha256

    if not isinstance(receipt, Mapping) or set(receipt) != {
        "shard_id",
        "partition_sha256",
        "result_sha256",
        "source_record_count",
        "role_counts",
    }:
        raise ValueError("shard receipt shape differs")
    proof = ShardOutputProof(layout, receipt["shard_id"], receipt["partition_sha256"], receipt["source_record_count"])
    _sha256(receipt["result_sha256"])
    if not isinstance(receipt["role_counts"], Mapping) or set(receipt["role_counts"]) != set(proof.role_counts):
        raise ValueError("shard receipt role census differs")
    for count in receipt["role_counts"].values():
        _integer(count, "shard role count", zero=True)
    return {
        **receipt,
        "partition_sha256": bytes(receipt["partition_sha256"]),
        "result_sha256": bytes(receipt["result_sha256"]),
        "role_counts": dict(receipt["role_counts"]),
    }


async def copy_shard(
    session_factory, request, family_id, layout, prepared_batches, receipt, *, maximum_rows, maximum_bytes
):
    """COPY bounded views of an immutable spool and receipt in one owner transaction.

    Prepare content, IDs and order before authority locks; exact retries consume no spool.
    Native claims serialize retries until commit; every failure rolls back all rows.
    """
    expected = _checked_receipt(layout, receipt)
    async with _block_session(
        session_factory,
        request,
        family_id,
        layout,
        "loading",
        loading=True,
    ) as (session, _, binding):
        table = binding.tables_by_role["shard_receipt"]
        claimed = (
            await _execute(
                session,
                pg_insert(table)
                .values(**expected)
                .on_conflict_do_nothing(index_elements=[table.c.shard_id])
                .returning(table.c.shard_id),
            )
        ).scalar_one_or_none()
        if claimed is None:
            old = (
                (await _execute(session, select(table).where(table.c.shard_id == expected["shard_id"])))
                .mappings()
                .one()
            )
            actual_by_role = {key: old[key] for key in expected}
            if actual_by_role != expected:
                raise FamilyBlockUnavailable("shard replay input, output or accounting differs")
            return expected
        proof = ShardOutputProof(
            layout, expected["shard_id"], expected["partition_sha256"], expected["source_record_count"]
        )
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        for batches in prepared_batches:
            check_copy_batch(layout, family_id, batches, maximum_rows=maximum_rows, maximum_bytes=maximum_bytes)
            for role, candidate_rows in batches.items():
                if candidate_rows:
                    proof.add(role, candidate_rows)
                    await _prepare_statement(session)
                    candidate_target = binding.tables_by_role[role]
                    await raw.driver_connection.copy_records_to_table(
                        candidate_target.name,
                        schema_name=candidate_target.schema,
                        columns=copy_columns(candidate_target),
                        records=candidate_rows,
                    )
        if proof.receipt() != expected:
            raise FamilyBlockUnavailable("copied shard output differs from completed spool evidence")
        return expected


def _checked_census(expected_completed_shards, source_record_count, role_counts):
    from process.custom_import.family_block_records import _integer

    _integer(expected_completed_shards, "completed shard census")
    _integer(source_record_count, "source census", zero=True)
    if not isinstance(role_counts, Mapping) or set(role_counts) != set(ROLE_SPECS) - {"shard_receipt"}:
        raise ValueError("shard aggregate role census differs")
    for count in role_counts.values():
        _integer(count, "aggregate role count", zero=True)
    return {
        "expected_completed_shards": expected_completed_shards,
        "source_record_count": source_record_count,
        "role_counts": dict(role_counts),
    }


def _compare_shard_census(row, expected):
    count = expected["expected_completed_shards"]
    if (row["shards"], row["first_shard"], row["last_shard"], row["source_record_count"]) != (
        count,
        0,
        count - 1,
        expected["source_record_count"],
    ):
        raise FamilyBlockUnavailable("completed shard census or source accounting differs")
    if any(row[f"role_{index}"] != expected["role_counts"][role] for index, role in enumerate(expected["role_counts"])):
        raise FamilyBlockUnavailable("completed shard output accounting differs")


async def _verify_shard_census(session, binding, expected):
    table = binding.tables_by_role["shard_receipt"]
    statement = select(
        func.count().label("shards"),
        func.min(table.c.shard_id).label("first_shard"),
        func.max(table.c.shard_id).label("last_shard"),
        func.coalesce(func.sum(table.c.source_record_count), 0).label("source_record_count"),
        *(
            func.coalesce(func.sum(table.c.role_counts[role].astext.cast(BigInteger)), 0).label(f"role_{index}")
            for index, role in enumerate(expected["role_counts"])
        ),
    )
    row = (await _execute(session, statement)).mappings().one()
    _compare_shard_census(row, expected)
