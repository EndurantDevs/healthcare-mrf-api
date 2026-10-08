# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Empty native witnesses retain the real compiler's index names and declarations."""

from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Identity, Index, Integer, MetaData, Table, Text, text
from sqlalchemy.dialects.postgresql import dialect

from process import reference_family_archive as archive


@pytest.mark.parametrize("importer", ("nucc", "claims-pricing", "drug-claims"))
def test_native_witness_uses_each_family_model_index_compiler(importer):
    for model in archive.reference_family_spec(importer).model_types:
        table, statements = archive._model_index_catalog_plan(model, "source_catalog")
        actual_statements = [str(statement.compile(dialect=dialect())) for statement in statements[1:]]
        indexes = tuple(getattr(model, "__my_initial_indexes__", ()) or ()) + tuple(
            getattr(model, "__my_additional_indexes__", ()) or ()
        )
        expected_statements = [
            archive._additional_index_sql(
                "pg_temp", model, index, table_name=table.name, index_schema_name="source_catalog"
            )
            for index in indexes
        ]
        assert actual_statements == expected_statements
        assert table.schema == "pg_temp"
        assert table.dialect_options["postgresql"]["on_commit"] == "DROP"
        assert all(column.identity is None and column.server_default is None for column in table.columns)


def test_native_witness_keeps_initial_unique_include_and_exact_source_names():
    source = Table(
        "native_model", MetaData(), Column("id", Integer, Identity(), primary_key=True), Column("display_name", Text)
    )
    Index("native_partial", source.c.id, unique=True, postgresql_where=text("id IS NOT NULL"))
    model = SimpleNamespace(
        __tablename__=source.name,
        __table__=source,
        __my_initial_indexes__=(
            {"name": "initial", "index_elements": ("id",), "unique": True, "include": ("display_name",)},
        ),
        __my_additional_indexes__=({"name": "value_lower", "index_elements": ("lower(display_name)",)},),
    )
    table, statements = archive._model_index_catalog_plan(model, "source_catalog")
    sql_statements = [str(statement.compile(dialect=dialect())) for statement in statements]
    assert "IDENTITY" not in sql_statements[0] and "SERIAL" not in sql_statements[0]
    assert "CREATE UNIQUE INDEX native_partial" in sql_statements[1]
    assert "WHERE id IS NOT NULL" in sql_statements[1]
    assert (
        archive._additional_index_sql(
            "pg_temp", model, model.__my_initial_indexes__[0], table_name=table.name, index_schema_name="source_catalog"
        )
        == sql_statements[2]
    )
    assert "CREATE UNIQUE INDEX" in sql_statements[2] and "INCLUDE (display_name)" in sql_statements[2]


def test_index_opclass_inputs_come_only_from_trusted_model_declarations():
    model = archive.reference_family_spec("drug-claims").model_types[1]
    inputs = archive._model_index_catalog_inputs(model)
    assert inputs.count(("gin", "gin_trgm_ops", None)) == 4
    assert ("btree", None, "year") in inputs


def test_ptg_index_witness_omits_model_edges_without_weakening_default_factory():
    from process import entity_address_native_publication as publication
    from process.ptg_parts import ptg2_physical_binding as ptg

    for model in ptg.local_data_family_spec().model_types:
        if not any(index.dialect_options["postgresql"].get("where") is not None for index in model.__table__.indexes):
            continue
        source_foreign_keys = set(model.__table__.foreign_keys)
        table, statements = archive._model_index_catalog_plan(model, "source_catalog")
        assert not table.foreign_keys
        assert "FOREIGN KEY" not in str(statements[0].compile(dialect=dialect()))
        assert model.__table__.foreign_keys == source_foreign_keys
        if source_foreign_keys:
            with pytest.raises(RuntimeError, match="model relationships require set validation"):
                publication._model_catalog_table(model)


def test_witness_opclasses_are_qualified_without_moving_ordering_or_function_lookup():
    model = archive.reference_family_spec("drug-claims").model_types[1]
    opclasses_by_input = {key: ("public", key[1] or "text_ops") for key in archive._model_index_catalog_inputs(model)}
    _, statements = archive._model_index_catalog_plan(model, "source_catalog", resolved_opclasses=opclasses_by_input)
    sql_statements = [str(statement.compile(dialect=dialect())) for statement in statements]
    assert any('lower(COALESCE(rx_name, \'\')) "public"."gin_trgm_ops"' in sql for sql in sql_statements)
    assert any('total_drug_cost "public"."text_ops" DESC' in sql for sql in sql_statements)
    assert not any('DESC "public"' in sql for sql in sql_statements)
