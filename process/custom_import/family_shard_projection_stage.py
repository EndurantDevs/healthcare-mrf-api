# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stage provisional projection COPY rows in one bounded read-only snapshot.

Native candidate rows cannot independently prove their own original semantics.
The owner retains the source/retained/member/winner evidence and later closes and
compares the complete candidate. This module writes only its owned local spool.
"""

import asyncio
from dataclasses import dataclass, replace

from sqlalchemy import and_, func, select, text

from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution, CustomImportGeneration
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.build_source import (
    _WINDOW,
    _close_contexts,
    _drain_cleanup,
    _enter_context,
    _initial_page_window,
    _prepare_statement,
    _source_cleanup_failure,
)
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import ShardOutputProof, _candidate, _execute, _observe_attempt
from process.custom_import.family_block_validate import require_readonly_snapshot
from process.custom_import.family_shard_allocate import (
    SourceAllocationLimits,
)
from process.custom_import.family_shard_allocate import (
    _checked_source_provenance as _checked_source,
)
from process.custom_import.family_shard_base_values_sql import census_statement, fetch_rows
from process.custom_import.family_shard_common import SourceCommonContext, _finish
from process.custom_import.family_shard_output import FamilyShardOutput
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_projection_sql import projection_sources
from process.custom_import.family_shard_role_validation import _copy_record
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_sources import child_hash_collision_statement, field_slots
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields, reduction_statement
from process.custom_import.read_identity import _is_same_read_transaction, _read_transaction_identity


class ProjectionStageOutput(FamilyShardOutput):
    """Existing closed COPY replay plus primitive provisional generation custody."""

    def __init__(self, work, path, receipt, proof_inputs):
        super().__init__(work, path, receipt)
        self._proof_inputs = tuple(sorted(proof_inputs.items()))

    @property
    def proof_inputs(self):
        """Detach observations; none is a completed native-input or finality proof."""
        return dict(self._proof_inputs)


def _effective_work(work, limits):
    if type(work) is not PlanWork or type(limits) is not SourceAllocationLimits:
        raise ValueError("projection staging requires explicit owner resource bounds")
    bounded = replace(
        work.limits,
        **{
            name: min(getattr(work.limits, name), getattr(limits.spool, name))
            for name in ("max_spool_bytes", "max_record_bytes", "memory_bytes")
        },
    )
    return replace(work, limits=bounded)


def _generation_state(common, build_id, native):
    request, allocated_source = common.request, common.source
    capture = _checked_source(request, allocated_source.source)
    build, generation, snapshot, source_binding = native
    expected_by_name = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "execution_id": request.execution_id,
        "capture_bundle_id": capture["capture_bundle_id"],
        "producing_fence": request.fence,
        "producing_token_sha256": producer_token_sha256(request),
    }
    if any(
        getattr(producer, name) != expected
        for producer in (build, generation, snapshot)
        for name, expected in expected_by_name.items()
    ):
        raise FamilyBlockUnavailable("projection native producer or capture differs")
    if (
        build.build_id != build_id
        or build.phase != "output"
        or build.graph_frozen_at is None
        or build.source_frozen_at is None
        or build.source_occurrence_count != allocated_source.source.counts[0]
        or build.candidate_error_count != 0
        or build.output_frozen_at is not None
        or build.verified_at is not None
        or snapshot.family_id != allocated_source.snapshot_family_id
        or snapshot.frozen_at is not None
        or build.generation_id != generation.generation_id
        or snapshot.generation_id != generation.generation_id
        or generation.base_generation_id != request.expected_base_generation_id
        or source_binding != capture["source_binding_revision_id"]
    ):
        raise FamilyBlockUnavailable("projection requires the original open output generation")
    return GenerationIdentity(
        generation.generation_id, request.dataset_id, request.definition_revision_id, request.schema_revision_id
    )


def _proof_inputs(common, build_id, binding, generation):
    source, request = common.source, common.request
    capture = _checked_source(request, source.source)
    return dict(
        family_id=source.snapshot_family_id,
        build_id=build_id,
        generation_id=generation.generation_id,
        dataset_id=generation.dataset_id,
        definition_revision_id=generation.definition_revision_id,
        schema_revision_id=generation.schema_revision_id,
        execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request).hex(),
        capture_bundle_id=capture["capture_bundle_id"],
        source_binding_revision_id=capture["source_binding_revision_id"],
        definition_sha256=request.definition.digest,
        layout_sha256=binding.layout.digest.hex(),
        source_plan_sha256=source.source.sha256.hex(),
        allocation_sha256=source.sha256.hex(),
        source_partition_sha256=source.source.partition_sha256,
        original_source_records=source.source.counts[0],
        owner_oid=binding.owner_oid,
        role_oids=tuple(sorted(binding.oids_by_role.items())),
        base_generation_id=request.expected_base_generation_id,
        base_pointer_version=request.expected_pointer_version,
        complete_scope=request.complete_scope,
    )


async def _native_state(session, common, build_id):
    request, source = common.request, common.source
    if await _observe_attempt(session, request) != build_id:
        raise FamilyBlockUnavailable("projection original build changed")
    _descriptor, binding = await _candidate(
        session, request, source.snapshot_family_id, common.layout, {"loading"}, locked=False
    )
    build, generation, snapshot, execution = (
        CustomImportBuildAttempt,
        CustomImportGeneration,
        CustomImportSnapshotFamily,
        CustomImportExecution,
    )
    statement = (
        select(build, generation, snapshot, execution.source_binding_revision_id)
        .select_from(build)
        .join(generation, generation.generation_id == build.generation_id)
        .join(snapshot, snapshot.family_id == source.snapshot_family_id)
        .join(execution, execution.execution_id == build.execution_id)
        .where(build.build_id == build_id)
        .execution_options(populate_existing=True)
    )
    native = (await _execute(session, statement)).one_or_none()
    if native is None:
        raise FamilyBlockUnavailable("projection output generation is missing")
    identity = _generation_state(common, build_id, native)
    return binding, identity, _proof_inputs(common, build_id, binding, identity)


async def _observe(factory, common, build_id):
    contexts, primary, observed = [], None, None
    try:
        session = await _enter_context(contexts, factory())
        if session.in_transaction() or session.new or session.dirty or session.deleted:
            raise FamilyBlockUnavailable("projection observation requires a clean owned session")
        await _enter_context(contexts, session.begin())
        await _initial_page_window(session, common.request)
        observed = (*await _native_state(session, common, build_id), session.info[_WINDOW])
    except BaseException as error:
        primary = error
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary
    return observed


@dataclass
class _Read:
    session: object
    transactions: tuple
    window: object
    work: object
    limits: SourceAllocationLimits

    async def prepare(self):
        """Keep every statement/cursor in the unchanged observed native window."""
        self.work.current()
        if self.session.info.get(_WINDOW) is not self.window or not _is_same_read_transaction(
            self.transactions, await _read_transaction_identity(self.session)
        ):
            raise FamilyBlockUnavailable("projection read transaction or deadline changed")
        await _prepare_statement(self.session)
        if not _is_same_read_transaction(self.transactions, await _read_transaction_identity(self.session)):
            raise FamilyBlockUnavailable("projection read transaction changed during preparation")
        self.work.current()


def _scope_fields(definition, child):
    return tuple(field for field in configured_fields(definition) if (field.collection is not None) == child)


def _reduction(definition, source, values_by_field, child):
    return reduction_statement(
        source, _scope_fields(definition, child), values_by_field=values_by_field, field_slots=field_slots(definition)
    )


def _child_output(binding, reduced):
    dictionary = binding.tables_by_role["key_dictionary"]
    joined = reduced.outerjoin(
        dictionary,
        and_(
            dictionary.c.child_key_sha256 == reduced.c.child_key_sha256,
            dictionary.c.canonical_child_key.collate("C") == reduced.c.canonical_child_key.collate("C"),
        ),
    )
    identities = tuple(
        reduced.c[name] for name in ("entity_binding_id", "selected_value", "canonical_child_key", "child_key_sha256")
    )
    guard = select(*identities).select_from(joined).group_by(*identities).having(func.count(dictionary.c.key_rank) != 1)
    output = select(
        reduced.c.entity_binding_id,
        reduced.c.selected_value,
        dictionary.c.key_rank,
        dictionary.c.key_variant,
        *(column for column in reduced.c if column.name.startswith("field_")),
    ).select_from(joined)
    return guard, output


def _queries(binding, generation):
    definition = binding.layout.definition
    roots, root_values, children, child_values = projection_sources(binding, generation)
    guards, outputs = [ambiguity_statement(roots)], []
    if _scope_fields(definition, False):
        outputs.append(("query_root", _reduction(definition, roots, root_values, False)))
    if children is not None:
        guards.extend((ambiguity_statement(children), child_hash_collision_statement(children)))
        reduced = _reduction(definition, children, child_values, True).cte("block_projection_reduced_children")
        key_guard, output = _child_output(binding, reduced)
        guards.append(key_guard)
        outputs.append(("query_child", output))
    return tuple(guards), tuple(outputs)


def _append_partition(read, target, path, proof, table, role, rows, previous):
    if len(rows) > read.limits.maximum_batch_records:
        raise FamilyBlockResourceLimit("projection native partition exceeds the row budget")
    used = 0
    for native in rows:
        read.work.current()
        key, packed = _copy_record(table, tuple(native), read.work.limits)
        used += len(packed) + 128
        if used > read.limits.maximum_batch_bytes or used > read.work.limits.memory_bytes // 2:
            raise FamilyBlockResourceLimit("projection native partition exceeds the byte reservation")
        if previous is not None and key <= previous:
            raise FamilyBlockUnavailable("projection native output key repeats or is unordered")
        previous = key
        proof.add(role, (tuple(native),))
        read.work.frame(target, (role, packed), path)
    return previous


async def _copy_cursor(read, statement, role, target, path, proof, table):
    await read.prepare()
    census = (await read.session.execute(census_statement(statement))).one()
    count = min(fetch_rows(census, read.work.limits), read.limits.maximum_batch_records)
    if (census.row_count == 0) != (census.row_bytes == 0):
        raise FamilyBlockUnavailable("projection native count or width differs")
    if census.row_bytes > min(read.limits.maximum_batch_bytes, read.work.limits.memory_bytes // 2):
        raise FamilyBlockResourceLimit("projection native row exceeds the byte reservation")
    if census.row_bytes:
        count = min(
            count, max(1, min(read.limits.maximum_batch_bytes, read.work.limits.memory_bytes // 2) // census.row_bytes)
        )
    await read.prepare()
    cursor = await read.session.stream(statement.execution_options(yield_per=count))
    primary, previous, seen = None, None, 0
    try:
        async for rows in cursor.partitions(count):
            await read.prepare()
            previous = await _cpu(read.work, _append_partition, read, target, path, proof, table, role, rows, previous)
            seen += len(rows)
        await read.prepare()
        if seen != census.row_count:
            raise FamilyBlockUnavailable("projection native cursor EOF census differs")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary


async def _read_output(factory, common, build_id, observed, work, proof, limits):
    contexts, primary, path = [], None, work.path()
    try:
        session = await _enter_context(contexts, factory())
        if session.in_transaction() or session.new or session.dirty or session.deleted:
            raise FamilyBlockUnavailable("projection read requires a clean owned session")
        await _enter_context(contexts, session.begin())
        session.info[_WINDOW] = observed[3]
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        read = _Read(session, await _read_transaction_identity(session), observed[3], work, limits)
        await read.prepare()
        await require_readonly_snapshot(session)
        binding, generation, header = await _native_state(session, common, build_id)
        if header != observed[2]:
            raise FamilyBlockUnavailable("projection observed candidate custody changed")
        guards, outputs = _queries(binding, generation)
        for guard in guards:
            await read.prepare()
            if (await session.execute(guard.limit(1))).first() is not None:
                raise FamilyBlockUnavailable("projection complete source or dictionary is ambiguous")
        with path.open("xb") as copy_file:
            for role, statement in outputs:
                table = binding.tables_by_role[role]
                if tuple(statement.selected_columns.keys()) != tuple(table.c.keys()):
                    raise FamilyBlockUnavailable("projection native COPY column order differs")
                statement = statement.order_by(
                    *(statement.selected_columns[name] for name in table.primary_key.columns.keys())
                )
                await _copy_cursor(read, statement, role, copy_file, path, proof, table)
        await read.prepare()
        await require_readonly_snapshot(session)
    except BaseException as error:
        primary = error
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary
    return path


async def stage_shard_query_projection(factory, common, build_id, *, work, limits, shard_id):
    """Stage complete provisional rows; never COPY, mutate metadata or accept proof.

    The explicit shard uses the original SOURCE partition and contributes zero
    source records. The caller retains this closed spool until final validation
    and terminal cleanup. Empty capability still checks current owner authority.
    """
    if type(common) is not SourceCommonContext:
        raise ValueError("projection staging requires its original SOURCE context")
    _integer(build_id, "projection build")
    _integer(shard_id, "projection shard", zero=True)
    work = _effective_work(work, limits)
    observed = await _observe(factory, common, build_id)
    if not configured_fields(common.request.definition):
        return None
    scratch, primary, output = _Scratch(work), None, None
    try:
        async with asyncio.timeout_at(observed[3].deadline):
            proof = ShardOutputProof(common.layout, shard_id, bytes.fromhex(common.source.source.partition_sha256), 0)
            path = await _read_output(factory, common, build_id, observed, scratch, proof, limits)
            if (await _observe(factory, common, build_id))[2] != observed[2]:
                raise FamilyBlockUnavailable("projection final observed candidate custody changed")
            ready = await _cpu(scratch, _finish, scratch, path)
            output = await _cpu(scratch, ProjectionStageOutput, scratch, ready, proof.receipt(), observed[2])
            scratch.current()
    except BaseException as error:
        primary = error
    if primary is not None:
        try:
            scratch.close()
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
        raise primary
    return output
