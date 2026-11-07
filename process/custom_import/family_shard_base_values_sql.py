# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based retained cursors; native origin/lease authority is caller-owned."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import (
    BigInteger,
    Column,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Table,
    Text,
    any_,
    bindparam,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY

from db.models import custom_import as models
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import post_seal_audit
from process.custom_import.build_source import _drain_cleanup
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_base import (
    _base_binding,
    _base_definition,
    _base_identity,
    _verify_leaf_pins,
)
from process.custom_import.query_projection_finality import validate_sealed_projection
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

_IDS = any_(bindparam("retained_ids", type_=ARRAY(BigInteger)))


def tables(base):
    """Only positively resolved native layouts select their exact fixed tables."""
    if base.binding is not None:
        return dict(base.binding.tables_by_role)
    if base.pin.family_id is not None and base.pin.legacy_snapshot is not True:
        raise FamilyBlockUnavailable("retained base physical layout is unresolved")
    physical = None if base.pin.family_id is None else snapshot_tables(base.pin.family_id)
    model_by_role = {
        "family": models.CustomImportFamilyRevision,
        "root": models.CustomImportRootRevision,
        "child": models.CustomImportChildRevision,
        "edge": models.CustomImportFamilyChild,
        "root_scalar": models.CustomImportRootScalar,
        "child_scalar": models.CustomImportChildScalar,
        "pack": models.CustomImportPack,
        "member": models.CustomImportGenerationFamily,
    }
    return {
        role: model.__table__ if physical is None else physical[model.__tablename__]
        for role, model in model_by_role.items()
    }


def _columns(table, prefix):
    return [column.label(prefix + column.name) for column in table.c]


def family_statement(base, native):
    """Read original family and generation membership together."""
    family, member = native["family"], native.get("member", native.get("generation_family"))
    joined = family.outerjoin(
        member,
        (member.c.family_revision_id == family.c.family_revision_id)
        & (member.c.generation_id == base.generation.generation_id),
    )
    return (
        select(*_columns(family, "family_"), *_columns(member, "member_"))
        .select_from(joined)
        .where(family.c.family_revision_id == _IDS)
        .order_by(family.c.family_revision_id)
    )


def legacy_statements(base, native):
    """Every edge/scalar of selected families remains in the independent streams."""
    family_table = native["family"]
    root_table = native["root"]
    child_table = native["child"]
    edge_table = native["edge"]
    root_scalars, child_scalars = native["root_scalar"], native["child_scalar"]
    selected = lambda table: table.c.family_revision_id == _IDS
    root_join = family_table.outerjoin(root_table, root_table.c.root_revision_id == family_table.c.root_revision_id)
    children = edge_table.outerjoin(child_table, child_table.c.child_revision_id == edge_table.c.child_revision_id)
    root_scalar_join = family_table.join(
        root_scalars, root_scalars.c.root_revision_id == family_table.c.root_revision_id
    )
    child_scalar_join = edge_table.join(
        child_scalars, child_scalars.c.child_revision_id == edge_table.c.child_revision_id
    )
    return {
        "root": select(family_table.c.family_revision_id, *_columns(root_table, "record_"))
        .select_from(root_join)
        .where(selected(family_table))
        .order_by(family_table.c.family_revision_id),
        "child": select(
            edge_table.c.family_revision_id, *_columns(edge_table, "edge_"), *_columns(child_table, "record_")
        )
        .select_from(children)
        .where(selected(edge_table))
        .order_by(edge_table.c.family_revision_id, edge_table.c.child_revision_id),
        "root_scalar": select(family_table.c.family_revision_id, *_columns(root_scalars, "scalar_"))
        .select_from(root_scalar_join)
        .where(selected(family_table))
        .order_by(family_table.c.family_revision_id, root_scalars.c.root_revision_id, root_scalars.c.field_slot),
        "child_scalar": select(edge_table.c.family_revision_id, *_columns(child_scalars, "scalar_"))
        .select_from(child_scalar_join)
        .where(selected(edge_table))
        .order_by(edge_table.c.family_revision_id, child_scalars.c.child_revision_id, child_scalars.c.field_slot),
    }


def block_statements(native):
    """Read complete selected fragment and observed-ID directories."""
    return {
        role: select(native[role])
        .where(native[role].c.family_revision_id == _IDS)
        .order_by(*native[role].primary_key.columns)
        for role in ("fragment", "revision_run")
    }


def pack_statement(native):
    """Retain original pack, execution, sealed capture and stream identity."""
    pack = native["pack"]
    execution_table = models.CustomImportExecution.__table__
    capture_table = models.CustomImportCapture.__table__
    stream_table = models.CustomImportSourceStream.__table__
    joined = pack.outerjoin(execution_table, execution_table.c.execution_id == pack.c.execution_id)
    joined = joined.outerjoin(
        capture_table,
        (capture_table.c.capture_bundle_id == pack.c.capture_bundle_id)
        & (capture_table.c.stream_slot == pack.c.stream_slot),
    )
    joined = joined.outerjoin(
        stream_table,
        (stream_table.c.definition_revision_id == pack.c.definition_revision_id)
        & (stream_table.c.stream_slot == pack.c.stream_slot),
    )
    execution_names = (
        "execution_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "capture_bundle_id",
    )
    capture_names = (
        "capture_bundle_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "stream_slot",
        "capture_state",
    )
    stream_names = (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "stream_slot",
        "record_kind",
        "collection_slot",
    )
    return (
        select(
            *_columns(pack, "pack_"),
            *(execution_table.c[name].label("execution_" + name) for name in execution_names),
            *(capture_table.c[name].label("capture_" + name) for name in capture_names),
            *(stream_table.c[name].label("stream_" + name) for name in stream_names),
        )
        .select_from(joined)
        .where(pack.c.pack_id == _IDS)
        .order_by(pack.c.pack_id)
    )


def _row_bytes(subquery):
    byte_count = 1024
    for column in subquery.c:
        if isinstance(column.type, String | Text | LargeBinary):
            byte_count += func.coalesce(func.octet_length(column), 0).cast(BigInteger) * 8
        else:
            byte_count += 128
    return byte_count


def census_statement(statement):
    """Measure only selected native rows before allocating a driver buffer."""
    relation = statement.order_by(None).subquery("retained_selected_rows")
    return select(
        func.count().label("row_count"), func.coalesce(func.max(_row_bytes(relation)), 0).label("row_bytes")
    ).select_from(relation)


def fetch_rows(census, limits):
    """Reserve bounded fetched rows from observed selected byte widths."""
    if any(type(value) is not int or value < 0 for value in (census.row_count, census.row_bytes)):
        raise FamilyBlockUnavailable("retained native cursor census differs")
    if census.row_count == 0:
        return 1
    if census.row_bytes > limits.max_record_bytes or census.row_bytes > limits.memory_bytes:
        raise FamilyBlockResourceLimit("retained native row exceeds its owner resource reservation")
    return min(4096, max(1, limits.memory_bytes // max(1, census.row_bytes)))


@asynccontextmanager
async def rows(base, statement, ids, limits):
    """Bound driver allocation by the actual selected native row width and EOF."""
    await base.check()
    if not 0 < len(ids) <= 4096 or any(type(identity) is not int or not 0 < identity < 2**63 for identity in ids):
        raise FamilyBlockUnavailable("retained native ID batch differs")
    parameters_by_name = {"retained_ids": list(ids)}
    census = (await base.session.execute(census_statement(statement), parameters_by_name)).one()
    count = fetch_rows(census, limits)
    cursor = await base.session.stream(statement.execution_options(yield_per=count), parameters_by_name)
    primary = None
    stream_state = SimpleNamespace(count=0, is_complete=False)

    async def stream():
        """Require native EOF beyond the last counted row."""
        async for row in cursor.mappings():
            stream_state.count += 1
            yield dict(row)
        if stream_state.count != census.row_count:
            raise FamilyBlockUnavailable("retained native cursor complete census differs")
        stream_state.is_complete = True

    iterator = stream()
    try:
        yield iterator
        if not stream_state.is_complete:
            raise FamilyBlockUnavailable("retained native cursor was not fully exhausted")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(iterator.aclose(), primary)
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    await base.check()


async def _control_schema(session):
    connection = await session.connection()
    schemas = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = models.CustomImportGeneration.__table__.schema
    schema = schemas.get(schema, schema)
    if not schema:
        raise FamilyBlockUnavailable("retained origin requires explicit control schema")
    return connection.dialect.identifier_preparer.quote_schema(schema)


async def origin_context(base):
    """An unregistered origin is inapplicable only after explicit native layout proof."""
    await base.check()
    if base.pin.family_id is None:
        return None
    snapshot = await base.session.get(CustomImportSnapshotFamily, base.pin.family_id)
    if snapshot is None or snapshot.family_id != base.pin.family_id or snapshot.frozen_at is None:
        raise FamilyBlockUnavailable("retained origin registry is missing")
    physical_origin = await base.session.scalar(
        text("SELECT pg_catalog.to_regclass(:relation)::oid::bigint"),
        {"relation": '"' + snapshot_schema(base.pin.family_id) + '".legacy_copy_origin'},
    )
    if physical_origin != snapshot.origin_table_oid:
        raise FamilyBlockUnavailable("retained legacy-copy origin registration differs")
    if snapshot.origin_table_oid is None:
        return None
    if base.binding is not None or base.generation.base_generation_id is None:
        raise FamilyBlockUnavailable("retained legacy-copy origin scope differs")
    ancestor = await base.session.get(models.CustomImportGeneration, base.generation.base_generation_id)
    if ancestor is None or (ancestor.dataset_id, ancestor.schema_revision_id) != (
        base.generation.dataset_id,
        base.generation.schema_revision_id,
    ):
        raise FamilyBlockUnavailable("retained legacy-copy ancestor differs")
    # Reuse the read-only base proof; the sealing helper takes control-row locks.
    ancestor, seal = await _base_identity(
        base.session,
        SimpleNamespace(
            expected_base_generation_id=ancestor.generation_id,
            dataset_id=base.generation.dataset_id,
            schema_revision_id=base.generation.schema_revision_id,
        ),
    )
    definition, registry = await _base_definition(base.session, ancestor)
    pin, binding = await _base_binding(base.session, ancestor, definition, registry)
    await validate_sealed_projection(base.session, ancestor, seal, definition, family_block_binding=binding)
    await _verify_leaf_pins(base.session, ancestor, pin, base._transactions)
    if binding is not None or (pin.family_id is not None and not pin.legacy_snapshot):
        raise FamilyBlockUnavailable("legacy-copy ancestor requires its original legacy relations")
    await base.check()
    return SimpleNamespace(
        family=snapshot,
        base_family=None if pin.family_id is None else SimpleNamespace(family_id=pin.family_id),
        quoted=await _control_schema(base.session),
        generation=base.generation,
        ancestor_scope=(ancestor.generation_id, pin.family_id),
    )


def _origin_table(context):
    names = (
        "revision_id",
        "family_revision_id",
        "root_record_id",
        "root_revision_id",
        "base_generation_id",
        "base_family_revision_id",
        "base_root_revision_id",
        "base_child_revision_id",
    )
    return Table(
        "legacy_copy_origin",
        MetaData(),
        Column("kind", String(5)),
        *(Column(name, BigInteger) for name in names),
        Column("collection_slot", SmallInteger),
        schema=snapshot_schema(context.family.family_id),
    )


def _origin_prefix(context):
    schema = '"' + snapshot_schema(context.family.family_id) + '"'
    origin = schema + ".legacy_copy_origin"
    families, edges = schema + ".custom_import_family_revision", schema + ".custom_import_family_child"
    selected = "ANY(CAST(:retained_ids AS bigint[]))"
    # Also retain a corrupt origin claiming another family but naming a selected
    # revision. PK joins keep these branches scoped without scanning every origin.
    return (
        "WITH retained_origins AS MATERIALIZED ("
        "SELECT o.* FROM "
        + origin
        + " o WHERE o.family_revision_id="
        + selected
        + " UNION SELECT o.* FROM "
        + families
        + " f JOIN "
        + origin
        + " o ON o.kind='root' AND o.revision_id=f.root_revision_id WHERE f.family_revision_id="
        + selected
        + " UNION SELECT o.* FROM "
        + edges
        + " e JOIN "
        + origin
        + " o ON o.kind='child' AND o.revision_id=e.child_revision_id WHERE e.family_revision_id="
        + selected
        + ") "
    )


def origin_statement(context, *, check=False):
    """Reuse the four original arm-61 joins over all selected/referencing origins."""
    prefix = _origin_prefix(context)
    if not check:
        columns = tuple(_origin_table(context).c)
        relation = text(prefix + "SELECT * FROM retained_origins").columns(*columns).subquery("selected_origins")
        return select(relation).order_by(relation.c.family_revision_id, relation.c.kind, relation.c.revision_id)
    resource, _digest = post_seal_audit._sql_resource()
    sql = next(arm["sql"] for arm in resource["arms"] if arm["ordinal"] == 61)
    sql = sql.replace("__LEAF__.legacy_copy_origin", "retained_origins")
    return post_seal_audit._compiled(prefix + sql, context)
