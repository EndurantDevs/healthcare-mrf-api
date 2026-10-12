# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed operation-owned model heap/index checks without database mutation."""

import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import Column, MetaData, String, Table, text

from process import reference_family_archive as archive


@pytest.fixture
def pair(monkeypatch):
    runtime_spec = archive.ndc_model_family_spec
    metadata = MetaData(schema="synthetic_source")
    product = Table("product", metadata, Column("product_id", String, primary_key=True), Column("product_ndc", String))
    package = Table("package", metadata, Column("package_ndc", String, primary_key=True), Column("product_ndc", String))
    models = tuple(
        SimpleNamespace(
            __tablename__=table.name,
            __table__=table,
            __my_additional_indexes__=({"name": "product_ndc", "index_elements": ["product_ndc"], "using": "gin"},),
        )
        for table in (product, package)
    )
    spec = archive.ReferenceFamilySpec(
        "ndc", models, relationships=((models[1], "product_ndc", models[0], "product_ndc", False, False),)
    )
    monkeypatch.setattr(archive, "ndc_model_family_spec", lambda: spec)
    name_by_table = {table: f"hp_ndc_a_{'a' * 32}_{table}" for table in ("product", "package")}
    oid_by_table = {"product": 101, "package": 102}

    async def fetchval(sql, *args):
        if sql.startswith("SELECT pg_catalog.to_regclass"):
            return next(
                oid_by_table[table] for table in oid_by_table if args[0].endswith('"' + name_by_table[table] + '"')
            )
        assert sql.startswith("SELECT EXISTS")
        return False

    connection = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=AsyncMock(side_effect=fetchval)
    )
    return SimpleNamespace(
        spec=spec, names=name_by_table, oids=oid_by_table, connection=connection, runtime_spec=runtime_spec
    )


@pytest.mark.asyncio
async def test_operation_heaps_defer_indexes_and_finish_nonunique_indexed_edge(pair):
    await archive.precreate_ndc_model_heaps(pair.connection, "synthetic", pair.names)
    statements = [call.args[0] for call in pair.connection.execute.await_args_list]
    assert len(statements) == 2
    assert all(
        "PRIMARY KEY" not in sql and "CREATE INDEX" not in sql and "FOREIGN KEY" not in sql for sql in statements
    )
    assert all(pair.names[table] in statements[index] for index, table in enumerate(pair.names))
    assert all(model.__table__.schema == "synthetic_source" for model in pair.spec.model_types)
    assert tuple(pair.spec.model_types[0].__table__.primary_key.columns.keys()) == ("product_id",)
    pair.connection.execute.reset_mock()
    await archive.complete_ndc_model_heaps(pair.connection, "synthetic", pair.names, pair.oids)
    statements = [call.args[0] for call in pair.connection.execute.await_args_list]
    assert sum("PRIMARY KEY" in sql for sql in statements) == 2
    assert sum("CREATE INDEX" in sql for sql in statements) == 2
    for name in pair.names.values():
        assert sum(f'ON "synthetic"."{name}"' in sql for sql in statements if "CREATE INDEX" in sql) == 1
    assert not any("FOREIGN KEY" in sql for sql in statements)
    relationship = pair.connection.fetchval.await_args_list[-1].args[0]
    assert 'p."product_ndc"=c."product_ndc"' in relationship
    assert "NOT EXISTS" in relationship
    assert pair.names["product"] in relationship and pair.names["package"] in relationship


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "foreign", "cross_attempt", "mixed_kind", "transaction"])
async def test_model_targets_refuse_unowned_or_incomplete_pair_before_ddl(pair, change):
    if change == "missing":
        pair.names.pop("package")
    elif change == "foreign":
        pair.names["package"] = "package"
    elif change == "cross_attempt":
        pair.names["package"] = f"hp_ndc_a_{'b' * 32}_package"
    elif change == "mixed_kind":
        pair.names["package"] = f"hp_ndc_package_s_{'a' * 32}"
    else:
        pair.connection.is_in_transaction = lambda: False
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await archive.precreate_ndc_model_heaps(pair.connection, "synthetic", pair.names)
    pair.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_refuses_changed_oid_before_indexing(pair):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="custody"):
        await archive.complete_ndc_model_heaps(
            pair.connection, "synthetic", pair.names, {"product": 999, "package": 102}
        )
    assert all("CREATE INDEX" not in call.args[0] for call in pair.connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_nonunique_edge_still_refuses_orphans(pair):
    pair.connection.fetchval.side_effect = [101, 102, True]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="relationship differs"):
        await archive.complete_ndc_model_heaps(pair.connection, "synthetic", pair.names, pair.oids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "oids", (None, {"product": 101}, {"product": True, "package": 102}, {"product": 0, "package": 102})
)
async def test_invalid_heap_custody_refuses_before_locks_or_indexes(pair, oids):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="custody"):
        await archive.complete_ndc_model_heaps(pair.connection, "synthetic", pair.names, oids)
    pair.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_statement_keeps_session_path_and_refuses_unowned_native_transaction(pair):
    statement = text("SELECT 1")
    session = SimpleNamespace(execute=AsyncMock(return_value="executed"))
    assert await archive._execute_model_statement(session, statement) == "executed"
    session.execute.assert_awaited_once_with(statement)
    pair.connection.is_in_transaction = lambda: False
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="caller transaction"):
        await archive._execute_model_statement(pair.connection, statement)
    pair.connection.execute.assert_not_awaited()


def test_model_family_uses_the_existing_runtime_models_and_relationships(pair, monkeypatch):
    runtime = SimpleNamespace(
        Product=pair.spec.model_types[0], Package=pair.spec.model_types[1], RELATIONSHIPS=pair.spec.relationships
    )
    monkeypatch.setitem(sys.modules, "drug_snapshot_runtime.ndc", runtime)
    spec = pair.runtime_spec()
    assert spec.importer_id == "ndc"
    assert spec.model_types == pair.spec.model_types
    assert spec.relationships == pair.spec.relationships


@pytest.mark.asyncio
async def test_partial_parent_index_cannot_qualify_relationship(pair):
    pair.spec.model_types[0].__my_additional_indexes__ = (
        {"index_elements": ["product_ndc"], "using": "gin", "where": "product_ndc IS NOT NULL"},
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="column shape"):
        await archive._validate_declared_model_relationships(
            pair.connection, pair.spec, "synthetic", physical_names=pair.names
        )
    pair.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_heap_creation_indexes_the_same_mapped_tables(pair):
    await archive._create_model_heaps(
        pair.connection, pair.spec, "synthetic", ordinary_heaps=True, physical_names=pair.names
    )
    indexes = [call.args[0] for call in pair.connection.execute.await_args_list if "CREATE INDEX" in call.args[0]]
    assert len(indexes) == 2
    for name in pair.names.values():
        assert sum(f'ON "synthetic"."{name}"' in sql for sql in indexes) == 1
