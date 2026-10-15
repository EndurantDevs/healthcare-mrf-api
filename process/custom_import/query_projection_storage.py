# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Definition-owned typed context rows outside the legacy snapshot inventory."""

from functools import lru_cache
from types import MappingProxyType

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Index,
    LargeBinary,
    MetaData,
    Numeric,
    Table,
    Text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from db.models.custom_import_query_projection import QUERY_PROJECTION_CONTRACT
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

__all__ = (
    "QUERY_PROJECTION_CONTRACT",
    "QUERY_PROJECTION_BINDING_INDEX",
    "QUERY_PROJECTION_TABLE_NAMES",
    "query_projection_tables",
    "query_projection_load_statements",
    "query_projection_index_statements",
    "query_projection_index_count",
)

QUERY_PROJECTION_BINDING_INDEX = "custom_import_entity_binding_query_projection_order_idx"
QUERY_PROJECTION_TABLE_NAMES = ("custom_import_derived_root", "custom_import_derived_child")
_DIALECT = postgresql.dialect()


def _context_table(metadata, name, field_specs, *, child):
    columns = [
        Column("entity_binding_id", BigInteger, primary_key=True),
        Column("selected_value", BigInteger, primary_key=True),
        CheckConstraint("entity_binding_id > 0", name=f"{name}_identity"),
    ]
    if child:
        columns.extend(
            (
                Column("child_key_sha256", LargeBinary(32), primary_key=True),
                Column("canonical_child_key", Text(collation="C"), nullable=False),
                CheckConstraint(
                    "octet_length(child_key_sha256) = 32 AND octet_length(canonical_child_key) BETWEEN 1 AND 8192",
                    name=f"{name}_child_key",
                ),
            )
        )
    for slot, is_child, value_type in field_specs:
        if is_child == child:
            field_name = f"field_{slot}"
            columns.append(Column(field_name, BigInteger if value_type == "integer" else Numeric(31, 12)))
            if value_type == "decimal":
                columns.append(CheckConstraint(f"{field_name} <> 'NaN'::numeric", name=f"{name}_{field_name}_finite"))
    return Table(name, metadata, *columns)


def query_projection_tables(family_id: int, definition):
    """Cache only checked static column shapes, never a generation or authority."""

    schema_name = snapshot_schema(family_id)
    if not isinstance(definition, CustomImportDefinition):
        raise ValueError("query projection requires a parsed definition")
    enabled_field_ids = {field.field_id for field in configured_fields(definition)}
    field_specs = tuple(
        (slot, field.collection is not None, field.value_type)
        for slot, field in enumerate(definition.query.derived_fields, 1)
        if field.field_id in enabled_field_ids
    )
    if len(definition.query.derived_fields) > 32 or any(
        kind not in {"integer", "decimal"} for _, _, kind in field_specs
    ):
        raise ValueError("query projection numeric layout is invalid")
    return _query_projection_tables(schema_name, field_specs)


@lru_cache(maxsize=32)
def _query_projection_tables(schema_name, field_specs):
    metadata = MetaData(schema=schema_name)
    return MappingProxyType(
        {
            name: _context_table(metadata, name, field_specs, child=index == 1)
            for index, name in enumerate(QUERY_PROJECTION_TABLE_NAMES)
        }
    )


def query_projection_load_statements(family_id: int, definition) -> tuple[str, ...]:
    """Create context rows in the existing candidate schema without row hooks."""

    return tuple(
        str(CreateTable(table).compile(dialect=_DIALECT))
        for table in query_projection_tables(family_id, definition).values()
    )


def query_projection_index_statements(family_id: int, definition) -> tuple[str, ...]:
    """Index the replacement's typed values after loading, before publication."""

    statements = []
    for index, table in enumerate(query_projection_tables(family_id, definition).values()):
        keys = [table.c.selected_value]
        if index == 1:
            keys.append(table.c.child_key_sha256)
        for column in table.c:
            if not column.name.startswith("field_"):
                continue
            serving_index = Index(
                f"{table.name}_{column.name}_idx",
                *keys,
                column,
                table.c.entity_binding_id,
            )
            statements.append(str(CreateIndex(serving_index).compile(dialect=_DIALECT)))
            table.indexes.remove(serving_index)
    if statements:
        statements.append(_binding_index_statement(family_id))
    return tuple(statements)


def _binding_index_statement(family_id):
    table = snapshot_tables(family_id)["custom_import_entity_binding"]
    index = Index(
        QUERY_PROJECTION_BINDING_INDEX,
        table.c.dataset_id,
        table.c.adapter_id,
        table.c.canonical_value.collate("C"),
        table.c.entity_binding_id,
    )
    statement = str(CreateIndex(index).compile(dialect=_DIALECT))
    table.indexes.remove(index)
    return statement


def query_projection_index_count(definition) -> int:
    """Count field indexes and the shared canonical-order index when enabled."""

    if not isinstance(definition, CustomImportDefinition):
        raise ValueError("query projection requires a parsed definition")
    field_count = len(configured_fields(definition))
    return field_count + bool(field_count)
