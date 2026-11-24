# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve stable root, entity and family identities for a compact SOURCE page."""

from sqlalchemy import BigInteger, LargeBinary, Sequence, Text, and_, any_, bindparam, column, func, literal, select
from sqlalchemy.dialects.postgresql import ARRAY, insert

from db.models.custom_import import (
    CustomImportBuildFamily,
    CustomImportEntityBinding,
    CustomImportRootRecord,
)
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import copy_columns
from process.custom_import.family_block_write import _lock_open_layout
from process.custom_import.runner_graph import entity_value_digest
from process.custom_import.storage_layout import snapshot_tables

_ROOT = CustomImportRootRecord.__table__
_ENTITY = CustomImportEntityBinding.__table__
_FAMILY_IDS = Sequence("custom_import_family_revision_family_revision_id_seq", schema=_ROOT.schema)


def _source_identities(definition, roots):
    root_ids = tuple(source.plan.root_record_id for source in roots)
    entities = tuple(source.values[definition.entity_field] for source in roots)
    hashes = tuple(entity_value_digest(entity) for entity in entities)
    return (
        func.unnest(
            bindparam("input_root_ids", root_ids, type_=ARRAY(BigInteger)),
            bindparam("input_entities", entities, type_=ARRAY(Text)),
            bindparam("input_entity_hashes", hashes, type_=ARRAY(LargeBinary)),
        )
        .table_valued(column("root_id", BigInteger), column("entity_value", Text), column("entity_hash", LargeBinary))
        .render_derived(name="source_ids")
    )


async def _intern_entities(session, dataset_id, inputs):
    columns = ("dataset_id", "adapter_id", "canonical_value", "value_sha256")
    rows = select(literal(dataset_id, BigInteger), literal("npi"), inputs.c.entity_value, inputs.c.entity_hash)
    statement = insert(_ENTITY).from_select(columns, rows.distinct(), include_defaults=False)
    await _prepare_statement(session)
    await session.execute(statement.on_conflict_do_nothing(index_elements=columns[:3]))


def _allocated_ids(build, plan, inputs):
    return (
        select(inputs.c.root_id, _FAMILY_IDS.next_value(), _ENTITY.c.entity_binding_id)
        .select_from(inputs)
        .join(
            plan,
            and_(
                plan.c.build_id == build.build_id,
                plan.c.root_record_id == inputs.c.root_id,
                plan.c.selection_kind == "source",
                plan.c.family_revision_id.is_(None),
                plan.c.complete_at.is_(None),
            ),
        )
        .join(
            _ROOT,
            and_(
                _ROOT.c.dataset_id == build.dataset_id,
                _ROOT.c.root_record_id == plan.c.root_record_id,
                _ROOT.c.logical_key_sha256 == plan.c.root_key_sha256,
            ),
        )
        .join(
            _ENTITY,
            and_(
                _ENTITY.c.dataset_id == build.dataset_id,
                _ENTITY.c.adapter_id == "npi",
                _ENTITY.c.canonical_value == inputs.c.entity_value,
                _ENTITY.c.value_sha256 == inputs.c.entity_hash,
            ),
        )
        .order_by(inputs.c.root_id)
    )


async def _copy_identities(session, target, source, ids, dataset_id):
    columns = copy_columns(target)
    primary_key = tuple(target.primary_key.columns)[0]
    selected = bindparam("selected_ids", ids, type_=ARRAY(BigInteger))
    rows = select(*(source.c[name] for name in columns)).where(
        source.c.dataset_id == dataset_id, source.c[primary_key.name] == any_(selected)
    )
    statement = insert(target).from_select(columns, rows, include_defaults=False)
    await _prepare_statement(session)
    await session.execute(statement.on_conflict_do_nothing(index_elements=[primary_key]))


async def _allocate_locked(session, build, binding, roots):
    if build.phase != "graph" or any(source.plan.build_id != build.build_id for source in roots):
        raise FamilyBlockUnavailable("compact SOURCE allocation differs from its build")
    inputs = _source_identities(binding.layout.definition, roots)
    plan = snapshot_tables(binding.family_id)[CustomImportBuildFamily.__tablename__]
    await _intern_entities(session, build.dataset_id, inputs)
    await _prepare_statement(session)
    allocated = (await session.execute(_allocated_ids(build, plan, inputs))).all()
    identities_by_root = {root_id: (family_id, entity_id) for root_id, family_id, entity_id in allocated}
    if len(identities_by_root) != len(roots) or len(allocated) != len(roots):
        raise FamilyBlockUnavailable("compact SOURCE allocation differs from its admitted roots")
    await _copy_identities(
        session, binding.tables_by_role["root_record"], _ROOT, tuple(identities_by_root), build.dataset_id
    )
    await _copy_identities(
        session,
        binding.tables_by_role["entity_binding"],
        _ENTITY,
        tuple({entity_id for _, entity_id in identities_by_root.values()}),
        build.dataset_id,
    )
    return identities_by_root


async def allocate_block_identities(session_factory, request, build_id, binding, roots):
    """Use the canonical interner and sequence once per admitted SOURCE page."""
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        return await _allocate_locked(session, build, binding, roots)
