# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog assertions shared by the PTG V4 PostgreSQL migration proofs."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
)


V4_TABLES = frozenset(
    {
        "ptg2_v4_snapshot_map_root",
        "ptg2_v4_snapshot_map_pack",
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
    }
)


def v3_provider_set_prerequisite_ddl(schema: str) -> str:
    """Return the real V3 provider-set shape referenced by the V4 migration."""

    return f"""
        CREATE TABLE {schema}.ptg2_v3_provider_set (
            snapshot_key bigint NOT NULL,
            provider_set_key integer NOT NULL,
            provider_set_global_id_128 bytea NOT NULL,
            provider_count bigint NOT NULL,
            network_names text[] NOT NULL DEFAULT ARRAY[]::text[],
            CONSTRAINT ptg2_v3_provider_set_pkey
                PRIMARY KEY (snapshot_key, provider_set_key),
            CONSTRAINT ptg2_v3_provider_set_global_id_key
                UNIQUE (snapshot_key, provider_set_global_id_128),
            CONSTRAINT ptg2_v3_provider_set_global_id_check
                CHECK (octet_length(provider_set_global_id_128) = 16),
            CONSTRAINT ptg2_v3_provider_set_provider_count_check
                CHECK (provider_count >= 0),
            CONSTRAINT ptg2_v3_provider_set_key_check
                CHECK (provider_set_key >= 0)
        )
        """


async def _rows(
    engine: AsyncEngine,
    statement: str,
    schema_name: str,
) -> list[Any]:
    async with engine.connect() as connection:
        result = await connection.execute(
            sa.text(statement),
            {"schema_name": schema_name},
        )
        return list(result.all())


async def _assert_v4_tables_and_columns(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    table_rows = await _rows(
        engine,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema_name AND table_name LIKE 'ptg2_v4_%'",
        schema_name,
    )
    assert {str(table_row[0]) for table_row in table_rows} == V4_TABLES
    column_rows = await _rows(
        engine,
        "SELECT table_name, column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = :schema_name AND table_name IN "
        "('ptg2_v4_npi_scope', 'ptg2_v4_snapshot_map_root', "
        "'ptg2_v4_provider_graph_diagnostic') "
        "ORDER BY table_name, ordinal_position",
        schema_name,
    )
    columns_by_table: dict[str, list[tuple[str, str]]] = {}
    for table_name, column_name, is_nullable in column_rows:
        columns_by_table.setdefault(str(table_name), []).append(
            (str(column_name), str(is_nullable))
        )
    assert columns_by_table["ptg2_v4_npi_scope"] == [
        ("snapshot_key", "NO"),
        ("npi_key", "NO"),
        ("npi", "NO"),
    ]
    assert "npi_count" in {
        name for name, _nullable in columns_by_table["ptg2_v4_snapshot_map_root"]
    }
    diagnostic_columns = columns_by_table[
        "ptg2_v4_provider_graph_diagnostic"
    ]
    assert tuple(name for name, _nullable in diagnostic_columns) == (
        "snapshot_key",
        *PTG2_V4_GRAPH_RESOURCE_FIELDS,
        *PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
        "created_at",
    )
    assert {
        name for name, nullable in diagnostic_columns if nullable == "YES"
    } == {
        "worst_provider_set_key",
        "worst_member_digest",
        "worst_online_provider_set_key",
        "worst_online_member_digest",
    }


async def _assert_v4_npi_constraints(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    constraint_rows = await _rows(
        engine,
        "SELECT constraint_record.conname, constraint_record.contype::text, "
        "pg_get_constraintdef(constraint_record.oid) "
        "FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = :schema_name "
        "AND relation.relname = 'ptg2_v4_npi_scope' "
        "AND constraint_record.contype <> 'n'",
        schema_name,
    )
    constraints_by_name = {
        str(constraint_row[0]): (
            str(constraint_row[1]),
            " ".join(str(constraint_row[2]).split()),
        )
        for constraint_row in constraint_rows
    }
    required_constraints = {
        "ptg2_v4_npi_scope_pkey",
        "ptg2_v4_npi_scope_npi_key",
        "ptg2_v4_npi_scope_root_fkey",
        "ptg2_v4_npi_scope_npi_key_check",
        "ptg2_v4_npi_scope_npi_check",
    }
    assert set(constraints_by_name) == required_constraints
    assert constraints_by_name["ptg2_v4_npi_scope_pkey"] == (
        "p",
        "PRIMARY KEY (snapshot_key, npi_key)",
    )
    assert constraints_by_name["ptg2_v4_npi_scope_npi_key"] == (
        "u",
        "UNIQUE (snapshot_key, npi)",
    )
    foreign_key = constraints_by_name["ptg2_v4_npi_scope_root_fkey"]
    assert foreign_key[0] == "f" and foreign_key[1].endswith("ON DELETE CASCADE")
    assert "ptg2_v4_snapshot_map_root(snapshot_key)" in foreign_key[1]
    assert "npi_key >= 0" in constraints_by_name[
        "ptg2_v4_npi_scope_npi_key_check"
    ][1]
    npi_check = constraints_by_name["ptg2_v4_npi_scope_npi_check"]
    assert npi_check[0] == "c" and "1000000000" in npi_check[1]
    assert "9999999999" in npi_check[1]


async def _assert_v4_diagnostic_constraints(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    constraint_rows = await _rows(
        engine,
        "SELECT constraint_record.conname, "
        "pg_get_constraintdef(constraint_record.oid) "
        "FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation "
        "ON relation.oid = constraint_record.conrelid "
        "JOIN pg_namespace AS namespace "
        "ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = :schema_name "
        "AND relation.relname = 'ptg2_v4_provider_graph_diagnostic'",
        schema_name,
    )
    constraints_by_name = {
        str(name): " ".join(str(definition).split())
        for name, definition in constraint_rows
    }
    limits = constraints_by_name[
        "ptg2_v4_provider_graph_diagnostic_limits_check"
    ]
    online = constraints_by_name[
        "ptg2_v4_provider_graph_diagnostic_online_check"
    ]
    assert "max_set_patterns_per_set > 0" in limits
    assert "max_online_group_npi_batches_per_set > 0" in limits
    assert "provider_expansion_rate_page_rows > 0" in limits
    assert "max_online_provider_expansion_graph_batches > 0" in limits
    assert (
        "worst_online_group_npi_byte_work "
        "<= max_online_group_npi_bytes_per_set"
    ) in online


async def _assert_v4_guards(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    trigger_rows = await _rows(
        engine,
        "SELECT relation.relname, trigger_record.tgname, function_record.proname "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid = trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "JOIN pg_proc AS function_record ON function_record.oid = trigger_record.tgfoid "
        "WHERE namespace.nspname = :schema_name AND NOT trigger_record.tgisinternal "
        "AND relation.relname LIKE 'ptg2_v4_%'",
        schema_name,
    )
    triggers = {
        tuple(str(trigger_value) for trigger_value in trigger_row)
        for trigger_row in trigger_rows
    }
    metadata_tables = {
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
    }
    expected_triggers = {
        (table_name, f"{table_name}_guard", "guard_ptg2_v4_snapshot_metadata")
        for table_name in metadata_tables
    }
    expected_triggers |= {
        (
            "ptg2_v4_snapshot_map_root",
            "ptg2_v4_snapshot_map_root_guard",
            "guard_ptg2_v4_snapshot_map_root",
        ),
        (
            "ptg2_v4_snapshot_map_pack",
            "ptg2_v4_snapshot_map_pack_guard",
            "guard_ptg2_v4_snapshot_map_pack",
        ),
    }
    assert triggers == expected_triggers
    function_rows = await _rows(
        engine,
        "SELECT procedure.proname FROM pg_proc AS procedure "
        "JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace "
        "WHERE namespace.nspname = :schema_name "
        "AND procedure.proname LIKE 'guard_ptg2_v4_%'",
        schema_name,
    )
    assert {str(function_row[0]) for function_row in function_rows} == {
        "guard_ptg2_v4_snapshot_metadata",
        "guard_ptg2_v4_snapshot_map_root",
        "guard_ptg2_v4_snapshot_map_pack",
    }


async def assert_v4_catalog(engine: AsyncEngine, schema_name: str) -> None:
    """Assert the installed V4 tables, dictionary, and guards are exact."""

    await _assert_v4_tables_and_columns(engine, schema_name)
    await _assert_v4_npi_constraints(engine, schema_name)
    await _assert_v4_diagnostic_constraints(engine, schema_name)
    await _assert_v4_guards(engine, schema_name)


async def assert_v4_schema_absent(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Assert downgrade removed every additive V4 table and guard function."""

    table_rows = await _rows(
        engine,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema_name AND table_name LIKE 'ptg2_v4_%'",
        schema_name,
    )
    function_rows = await _rows(
        engine,
        "SELECT procedure.proname FROM pg_proc AS procedure "
        "JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace "
        "WHERE namespace.nspname = :schema_name "
        "AND procedure.proname LIKE 'guard_ptg2_v4_%'",
        schema_name,
    )
    assert table_rows == []
    assert function_rows == []
