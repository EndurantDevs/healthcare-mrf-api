# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import uuid

import pytest

from db.connection import db
from process.ptg_parts import ptg2_manifest_publish


def _row_tuple(row) -> tuple:
    mapping = getattr(row, "_mapping", None)
    return tuple(mapping.values()) if mapping is not None else tuple(row)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unlogged_stage", "expected_persistence"),
    ((True, "u"), (False, "p")),
)
async def test_real_postgres_price_atom_rewrite_uses_configured_stage_durability(
    monkeypatch,
    unlogged_stage,
    expected_persistence,
):
    """Honor logged and unlogged price-stage durability in PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    monkeypatch.setenv(
        ptg2_manifest_publish.PTG2_UNLOGGED_STAGE_ENV,
        "true" if unlogged_stage else "false",
    )
    schema_name = f"ptg2_price_rewrite_{uuid.uuid4().hex[:16]}"
    schema = f'"{schema_name}"'
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {schema}")
        await db.execute_ddl(
            f"""
            CREATE UNLOGGED TABLE {schema}.price_atoms (
                price_atom_global_id_128 uuid NOT NULL,
                negotiated_type varchar(64),
                negotiated_rate text,
                expiration_date varchar(32),
                service_code text[] NOT NULL,
                billing_class varchar(64),
                setting varchar(64),
                billing_code_modifier text[] NOT NULL,
                additional_information text
            )
            """
        )
        await db.status(
            f"""
            INSERT INTO {schema}.price_atoms
                (price_atom_global_id_128, negotiated_type, negotiated_rate,
                 expiration_date, service_code, billing_class, setting,
                 billing_code_modifier, additional_information)
            VALUES
                ('11111111-1111-1111-1111-111111111111', 'derived', '10.00',
                 '2026-12-31', ARRAY['11'], 'institutional', 'facility',
                 ARRAY['AA'], 'alpha'),
                ('22222222-2222-2222-2222-222222222222', 'negotiated', '20.00',
                 '2027-12-31', ARRAY['22'], 'professional', 'non-facility',
                 ARRAY['BB'], 'beta')
            """
        )

        manifest = (
            await ptg2_manifest_publish._rewrite_ptg2_manifest_price_atom_table_lean_dict(
                schema_name=schema_name,
                price_atom_table="price_atoms",
                price_atom_dictionary_table="price_attributes",
            )
        )

        persistence_rows = await db.all(
            """
            SELECT relation.relname, relation.relpersistence::text
              FROM pg_class AS relation
              JOIN pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = :schema_name
               AND relation.relname IN ('price_atoms', 'price_attributes')
             ORDER BY relation.relname
            """,
            schema_name=schema_name,
        )
        assert [
            _row_tuple(persistence_row) for persistence_row in persistence_rows
        ] == [
            ("price_atoms", expected_persistence),
            ("price_attributes", expected_persistence),
        ]
        assert manifest["price_atom_constant_keys"] == {}
        assert manifest["price_atom_constant_values"] == {}
        assert manifest["price_atom_dictionary_table"] == (
            f"{schema_name}.price_attributes"
        )
        assert int(
            await db.scalar(f"SELECT count(*) FROM {schema}.price_attributes") or 0
        ) == 14
        lean_rows = await db.all(
            f"""
            SELECT price_atom_global_id_128::text, negotiated_rate,
                   negotiated_type_key, expiration_date_key, service_code_key,
                   billing_class_key, setting_key, billing_code_modifier_key,
                   additional_information_key
              FROM {schema}.price_atoms
             ORDER BY price_atom_global_id_128
            """
        )
        assert [_row_tuple(lean_atom_row) for lean_atom_row in lean_rows] == [
            ("11111111-1111-1111-1111-111111111111", "10.00", 0, 0, 0, 0, 0, 0, 0),
            ("22222222-2222-2222-2222-222222222222", "20.00", 1, 1, 1, 1, 1, 1, 1),
        ]
    finally:
        await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await db.disconnect()
