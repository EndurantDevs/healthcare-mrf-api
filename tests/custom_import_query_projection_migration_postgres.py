# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native metadata, custody, typed cells and bounded context qualification."""

import hashlib
from decimal import Decimal

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import insert, select, text
from sqlalchemy.exc import DBAPIError

from process.custom_import.query_projection_binding import QueryProjectionUnavailable, projection_table_identity
from process.custom_import.query_projection_pages import cell_insert_statement, context_key_statement
from process.custom_import.query_projection_sources import field_slots
from process.custom_import.query_projection_storage import (
    QUERY_PROJECTION_CONTRACT,
    query_projection_load_statements,
    query_projection_tables,
)
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_query_projection_migration import migration as projection_migration
from tests.test_custom_import_query_projection_storage import _definition
from tests.test_custom_import_query_projection_values import _source

_FAMILY = 2**63 - 1
_SCHEMA = "ci_snapshot_9223372036854775807"


def _run_projection_migration(connection, schema, operation):
    module = projection_migration()
    module._schema = lambda: schema
    module.op = Operations(MigrationContext.configure(connection))
    getattr(module, operation)()


async def _assert_projection_lock_timeout(case):
    with pytest.raises(DBAPIError) as failure:
        async with case.engine.begin() as connection:
            await connection.execute(text("SET LOCAL statement_timeout='5s'"))
            await connection.execute(text("SET LOCAL lock_timeout='0'"))
            await connection.run_sync(_run_projection_migration, case.schema_name, "upgrade")
    assert failure.value.orig.sqlstate == "55P03"


@pytest.mark.asyncio
async def test_projection_migration_lock_failure_rolls_back_and_preserves_stricter_limits():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_run_projection_migration, case.schema_name, "downgrade")
        async with case.engine.connect() as reader:
            await reader.execute(text(f'SELECT 1 FROM "{case.schema_name}".custom_import_generation_seal LIMIT 0'))
            try:
                await _assert_projection_lock_timeout(case)
            finally:
                await reader.rollback()
        async with case.engine.connect() as connection:
            original_limit = await connection.scalar(text("SHOW lock_timeout"))
            await connection.rollback()
            async with connection.begin():
                assert (
                    await connection.scalar(
                        text("SELECT to_regclass(:relation)"),
                        {"relation": f"{case.schema_name}.custom_import_query_projection"},
                    )
                    is None
                )
                assert (
                    await connection.scalar(
                        text(
                            "SELECT count(*) FROM information_schema.columns WHERE table_schema=:schema "
                            "AND table_name='custom_import_generation_seal' AND column_name LIKE 'query_projection_%'"
                        ),
                        {"schema": case.schema_name},
                    )
                    == 0
                )
                await connection.execute(text("SET LOCAL lock_timeout='100ms'"))
                await connection.run_sync(_run_projection_migration, case.schema_name, "upgrade")
                assert await connection.scalar(text("SHOW lock_timeout")) == "100ms"
            assert await connection.scalar(text("SHOW lock_timeout")) == original_limit


async def _check_custody(connection, control_schema, definition):
    oid, owner, columns_sha = await projection_table_identity(
        connection, _FAMILY, "custom_import_derived_root", definition
    )
    assert oid > 0 and owner > 0
    native_sha = await connection.scalar(
        text(f'SELECT "{control_schema}".custom_import_snapshot_columns_sha256(CAST(:oid AS bigint))'), {"oid": oid}
    )
    assert columns_sha == native_sha
    async with connection.begin_nested() as savepoint:
        await connection.execute(text(f"ALTER TABLE {_SCHEMA}.custom_import_derived_root ADD COLUMN unexpected text"))
        with pytest.raises(QueryProjectionUnavailable, match="custody|shape"):
            await projection_table_identity(connection, _FAMILY, "custom_import_derived_root", definition)
        await savepoint.rollback()


async def _check_bigint_contexts(connection, definition):
    table = query_projection_tables(_FAMILY, definition)["custom_import_derived_root"]
    context_rows = [
        dict(
            entity_binding_id=index + 1,
            selected_value=1,
            field_1=Decimal(f"{value}.000000000000"),
        )
        for index, value in enumerate((-(2**63), 2**63 - 1))
    ]
    await connection.execute(insert(table), context_rows)
    actual_values = (
        (await connection.execute(select(table.c.field_1).order_by(table.c.entity_binding_id))).scalars().all()
    )
    assert actual_values == [context["field_1"] for context in context_rows]
    for invalid in (Decimal("NaN"), Decimal("10000000000000000000")):
        with pytest.raises(DBAPIError):
            async with connection.begin_nested():
                await connection.execute(insert(table).values(entity_binding_id=3, selected_value=1, field_1=invalid))
    await connection.execute(insert(table).values(entity_binding_id=3, selected_value=1, field_1=None))
    assert await connection.scalar(select(table.c.field_1).where(table.c.entity_binding_id == 3)) is None


async def _check_context_pages(connection, definition, *, child):
    source_rows = [
        (10, -(2**63), "segment_a", 0, 1),
        (10, -(2**63), "segment_b", Decimal("0.000000000001"), 1),
        (11, 2**63 - 1, "segment_a", 9, 0),
        (11, 2**63 - 1, "segment_b", None, -1),
    ]
    if child:
        key = '["α"]'
        source_rows = [(*context, key, hashlib.sha256(key.encode()).digest()) for context in source_rows]
    source_relation = _source(source_rows, child=child).cte()
    table = query_projection_tables(_FAMILY, definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    fields = tuple(field for field in definition.query.derived_fields if (field.collection is not None) == child)
    metric = fields[0]
    values_by_field = {
        metric.source_field_id: source_relation.c.metric,
        metric.weight_field_id: source_relation.c.weight,
    }
    after_keys = None
    for entity in (10, 11):
        keys = (
            await connection.execute(context_key_statement(source_relation, child=child, after=after_keys, limit=1))
        ).all()
        assert len(keys) == 1 and keys[0].entity_binding_id == entity
        insert_result = await connection.execute(
            cell_insert_statement(
                table,
                source_relation,
                values_by_field=values_by_field,
                fields=fields,
                field_slots=field_slots(definition),
                keys=keys,
            )
        )
        assert insert_result.rowcount == 1
        after_keys = tuple(keys[0][: 3 if child else 2])
    assert not (
        await connection.execute(context_key_statement(source_relation, child=child, after=after_keys, limit=1))
    ).all()
    projected_columns = [table.c.entity_binding_id, table.c.selected_value]
    projected_columns.extend(table.c[f"field_{field_slots(definition)[field.field_id]}"] for field in fields)
    actual = (
        await connection.execute(
            select(*projected_columns).where(table.c.entity_binding_id >= 10).order_by(table.c.entity_binding_id)
        )
    ).all()
    assert [tuple(context) for context in actual] == (
        [(10, -(2**63), Decimal("0.000000000000"), 1), (11, 2**63 - 1, None, 0)]
        if child
        else [(10, -(2**63), Decimal("0.000000000000")), (11, 2**63 - 1, None)]
    )


async def _check_seal_marker_constraint(connection, control_schema):
    constraint_sql = await connection.scalar(
        text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid=to_regclass(:relation) "
            "AND conname='custom_import_generation_seal_query_projection_check'"
        ),
        {"relation": f"{control_schema}.custom_import_generation_seal"},
    )
    assert constraint_sql
    # Isolate the installed check from immutable seal writer guards.
    await connection.execute(
        text(
            "CREATE TEMPORARY TABLE projection_seal_markers (query_projection_contract text, "
            "query_projection_capability_sha256 bytea,query_projection_sha256 bytea, "
            "query_projection_root_count bigint,query_projection_child_count bigint, "
            f"{constraint_sql}) ON COMMIT DROP"
        )
    )
    statement = text("INSERT INTO projection_seal_markers VALUES (:contract,:capability,:projection,:roots,:children)")
    await connection.execute(statement, dict.fromkeys(("contract", "capability", "projection", "roots", "children")))
    markers_by_name = dict(contract=None, capability=b"a" * 32, projection=b"b" * 32, roots=0, children=0)
    with pytest.raises(DBAPIError, match="check constraint"):
        async with connection.begin_nested():
            await connection.execute(statement, markers_by_name)
    await connection.execute(statement, dict(markers_by_name, contract=QUERY_PROJECTION_CONTRACT))
    assert await connection.scalar(text("SELECT count(*) FROM projection_seal_markers")) == 2


@pytest.mark.asyncio
async def test_projection_migration_custody_numeric_boundaries_and_native_context_pages():
    definition = _definition()
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            columns = await connection.execute(
                text(
                    "SELECT column_name,is_nullable FROM information_schema.columns "
                    "WHERE table_schema=:schema AND table_name='custom_import_generation_seal' "
                    "AND column_name LIKE 'query_projection_%'"
                ),
                {"schema": case.schema_name},
            )
            assert len(list(columns)) == 5
            assert (
                await connection.scalar(
                    text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_query_projection')
                )
                == 0
            )
            await _check_seal_marker_constraint(connection, case.schema_name)
            await connection.execute(text(f"CREATE SCHEMA {_SCHEMA}"))
            try:
                for statement in query_projection_load_statements(_FAMILY, definition):
                    await connection.execute(text(statement))
                await _check_custody(connection, case.schema_name, definition)
                await _check_bigint_contexts(connection, definition)
                for child in (False, True):
                    await _check_context_pages(connection, definition, child=child)
            finally:
                await connection.execute(text(f"DROP SCHEMA {_SCHEMA} CASCADE"))
