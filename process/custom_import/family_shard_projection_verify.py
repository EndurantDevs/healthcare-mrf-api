# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare closed derived rows and hash their unchanged logical representation.

The caller separately proves the original families, raw scalars and winners.
This read-only comparison neither accepts those inputs nor seals a generation.
"""

import json
from dataclasses import dataclass

from sqlalchemy import and_, func, select

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.family_block_binding import (
    FamilyBlockBinding,
    resolve_family_block_binding,
    validate_family_block_indexes,
)
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_read_sql import query_projection_views
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_validate import (
    CandidateScope,
    ClosedCandidateIdentity,
    FamilyBlockValidationError,
    check_closed_identity,
    require_readonly_snapshot,
)
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base_values_sql import census_statement, fetch_rows
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_projection_stage import _effective_work, _queries
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_evidence import (
    ProjectionDigest,
    ProjectionDigestState,
    QueryProjectionEvidence,
)
from process.custom_import.query_projection_sources import field_slots
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.read_identity import _is_same_read_transaction, _read_transaction_identity


@dataclass(frozen=True)
class ClosedProjectionEvidence:
    identity: ClosedCandidateIdentity
    evidence: QueryProjectionEvidence | None
    checkpoint: ProjectionDigestState | None


async def _prepare(session, transactions, work, prepare):
    work.current()
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise FamilyBlockValidationError("projection validation read transaction changed")
    if prepare is not None:
        await prepare(session)
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise FamilyBlockValidationError("projection validation preparation changed its transaction")
    work.current()


def _scope(binding, generation, identity):
    if (
        type(binding) is not FamilyBlockBinding
        or type(generation) is not GenerationIdentity
        or type(identity) is not ClosedCandidateIdentity
        or type(identity.scope) is not CandidateScope
        or binding.family_id != identity.scope.family_id
        or binding.layout.digest != identity.layout_sha256
        or binding.owner_oid != identity.owner_oid
        or tuple(sorted(binding.oids_by_role.items())) != identity.role_oids
        or any(
            getattr(generation, name) != getattr(identity.scope, name)
            for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
        )
    ):
        raise FamilyBlockValidationError("projection validation closed scope differs")
    for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id"):
        _integer(getattr(generation, name), name)


async def _closed_identity(session, binding, identity, ready):
    await ready()
    actual = await resolve_family_block_binding(
        session,
        binding.family_id,
        binding.layout.definition,
        binding.layout.collection_slots,
        persistence="candidate",
        prepare=lambda _session: ready(),
    )
    await ready()
    snapshot = (
        (
            await session.execute(
                select(CustomImportSnapshotFamily.__table__).where(
                    CustomImportSnapshotFamily.family_id == binding.family_id
                )
            )
        )
        .mappings()
        .one()
    )
    await ready()
    descriptor = (
        (
            await session.execute(
                select(CustomImportFamilyBlockLayout.__table__).where(
                    CustomImportFamilyBlockLayout.family_id == binding.family_id
                )
            )
        )
        .mappings()
        .one()
    )
    if check_closed_identity(actual, identity.scope, snapshot, descriptor, phase=descriptor.get("phase")) != identity:
        raise FamilyBlockValidationError("projection validation closure or native custody changed")
    await validate_family_block_indexes(session, actual, prepare=lambda _session: ready())
    await ready()
    return actual


async def _require_empty(session, statement, ready):
    await ready()
    if (await session.execute(statement.limit(1))).first() is not None:
        raise FamilyBlockValidationError("closed projection source or complete output differs")


async def _compare_outputs(session, binding, generation, ready):
    definition = binding.layout.definition
    statements_by_role = {}
    if configured_fields(definition):
        guards, statements = _queries(binding, generation)
        for statement in guards:
            await _require_empty(session, statement, ready)
        statements_by_role = dict(statements)
    for role in ("query_root", "query_child"):
        table = binding.tables_by_role[role]
        actual = select(table)
        expected = statements_by_role.get(role)
        if expected is None:
            await _require_empty(session, actual, ready)
        else:
            if tuple(expected.selected_columns.keys()) != tuple(table.c.keys()):
                raise FamilyBlockValidationError("closed projection native columns differ")
            for difference in (expected.except_all(actual), actual.except_all(expected)):
                await _require_empty(session, select(difference.subquery()), ready)


def _semantic_statement(binding, generation, role):
    definition = binding.layout.definition
    table = query_projection_views(binding)[
        "custom_import_derived_child" if role == "query_child" else "custom_import_derived_root"
    ]
    entity = binding.tables_by_role["entity_binding"]
    canonical = entity.c.canonical_value.collate("C").label("canonical_entity")
    joined = table.join(
        entity,
        and_(
            entity.c.entity_binding_id == table.c.entity_binding_id,
            entity.c.dataset_id == generation.dataset_id,
            entity.c.adapter_id == json.loads(definition.schema_canonical)["root"]["entity"]["adapter"],
        ),
    )
    order_columns = [canonical, table.c.selected_value]
    if role == "query_child":
        order_columns.append(table.c.canonical_child_key.collate("C"))
    return select(*table.c, canonical).select_from(joined).order_by(*order_columns)


def _digest_rows(work, digest, definition, role, rows):
    is_child = role == "query_child"
    fields = tuple(field for field in configured_fields(definition) if (field.collection is not None) == is_child)
    slots = field_slots(definition)
    for row in rows:
        work.current()
        values_by_field = {field.field_id: row[f"field_{slots[field.field_id]}"] for field in fields}
        arguments = row["canonical_entity"], row["selected_value"]
        if is_child:
            digest.add_child(*arguments, row["canonical_child_key"], values_by_field)
        else:
            digest.add_root(*arguments, values_by_field)


async def _digest_scope(session, binding, generation, role, digest, work, limits, ready):
    statement = _semantic_statement(binding, generation, role)
    await ready()
    expected_count = (
        await session.execute(select(func.count()).select_from(binding.tables_by_role[role]))
    ).scalar_one()
    await ready()
    census = (await session.execute(census_statement(statement))).one()
    if census.row_count != expected_count or (census.row_count == 0) != (census.row_bytes == 0):
        raise FamilyBlockValidationError("closed projection semantic coverage differs")
    maximum_bytes = min(limits.maximum_batch_bytes, work.limits.memory_bytes // 2)
    if census.row_bytes > maximum_bytes:
        raise FamilyBlockResourceLimit("closed projection semantic row exceeds its operational buffer")
    count = min(fetch_rows(census, work.limits), limits.maximum_batch_records)
    if census.row_bytes:
        count = min(count, max(1, maximum_bytes // census.row_bytes))
    await ready()
    cursor = await session.stream(statement.execution_options(yield_per=count))
    primary, observed = None, 0
    try:
        async for native_rows in cursor.mappings().partitions(count):
            await ready()
            await _cpu(work, _digest_rows, work, digest, binding.layout.definition, role, native_rows)
            observed += len(native_rows)
        await ready()
        if observed != expected_count:
            raise FamilyBlockValidationError("closed projection semantic EOF differs")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    return observed


async def verify_closed_query_projection(session, binding, generation, identity, *, work, limits, prepare=None):
    """Compare every derived row and hash roots then children in one pinned read.

    Complete original-input validation and a fresh authorized owner transition
    remain mandatory before persisting this evidence or accepting the candidate.
    """
    _scope(binding, generation, identity)
    work = _effective_work(work, limits)
    if not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise FamilyBlockValidationError("closed projection requires a clean live read transaction")
    scratch, primary, projection_evidence = _Scratch(work), None, None
    try:
        transactions = await _read_transaction_identity(session)

        async def ready():
            """Keep the native read in its original transaction and work budget."""
            await _prepare(session, transactions, scratch, prepare)

        await ready()
        await require_readonly_snapshot(session)
        actual = await _closed_identity(session, binding, identity, ready)
        await _compare_outputs(session, actual, generation, ready)
        evidence, checkpoint = None, None
        if configured_fields(actual.layout.definition):
            digest = ProjectionDigest(actual.layout.definition)
            for role in ("query_root", "query_child"):
                await _digest_scope(session, actual, generation, role, digest, scratch, limits, ready)
            evidence, checkpoint = digest.finish(), digest.checkpoint()
        await ready()
        await require_readonly_snapshot(session)
        await _closed_identity(session, actual, identity, ready)
        projection_evidence = ClosedProjectionEvidence(identity, evidence, checkpoint)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return projection_evidence
