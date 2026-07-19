# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import uuid

import pytest

from db.connection import db
from process.ptg_parts import ptg2_manifest_publish, ptg2_shared_price


def _row_tuple(row) -> tuple:
    mapping = getattr(row, "_mapping", None)
    return tuple(mapping.values()) if mapping is not None else tuple(row)


@pytest.mark.asyncio
async def test_real_postgres_atom_key_stage_rejects_duplicate_scanner_ids():
    """Keep a database uniqueness backstop behind the single-source fast path."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    schema_name = f"ptg2_atom_key_duplicate_{uuid.uuid4().hex[:16]}"
    schema = f'"{schema_name}"'
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {schema}")
        await db.execute_ddl(
            f"""
            CREATE UNLOGGED TABLE {schema}.price_atoms (
                price_atom_global_id_128 uuid NOT NULL
            )
            """
        )
        await db.status(
            f"""
            INSERT INTO {schema}.price_atoms (price_atom_global_id_128)
            VALUES
                ('11111111-1111-1111-1111-111111111111'),
                ('11111111-1111-1111-1111-111111111111')
            """
        )

        with pytest.raises(
            Exception,
            match="could not create unique index|duplicate key value",
        ):
            await ptg2_shared_price._create_v3_atom_key_stage(
                schema_name=schema_name,
                price_atom_table="price_atoms",
                stage_table="atom_keys",
            )
    finally:
        await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await db.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_cross_source_atom_normalization_removes_duplicates():
    """Retain real database coverage for the multi-source canonicalization path."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    schema_name = f"ptg2_atom_normalize_{uuid.uuid4().hex[:16]}"
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
                ('11111111-1111-1111-1111-111111111111', 'negotiated', '10.00',
                 '2026-12-31', ARRAY['11'], 'professional', 'office',
                 ARRAY[]::text[], NULL),
                ('11111111-1111-1111-1111-111111111111', 'negotiated', '10.00',
                 '2026-12-31', ARRAY['11'], 'professional', 'office',
                 ARRAY[]::text[], NULL)
            """
        )

        metrics = await ptg2_shared_price._normalize_strict_price_atom_stage(
            schema_name=schema_name,
            price_atom_table="price_atoms",
        )

        assert metrics == {
            "rows_before": 2,
            "rows_after": 1,
            "duplicate_rows_removed": 1,
            "conflicting_ids": 0,
        }
        assert int(await db.scalar(f"SELECT COUNT(*) FROM {schema}.price_atoms")) == 1
    finally:
        await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await db.disconnect()


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
                 ARRAY['BB'], 'beta'),
                ('33333333-3333-3333-3333-333333333333', 'shared', '30.00',
                 NULL, ARRAY[]::text[], 'shared', NULL,
                 ARRAY[]::text[], 'shared'),
                ('44444444-4444-4444-4444-444444444444', 'shared', '40.00',
                 NULL, ARRAY[]::text[], 'shared', NULL,
                 ARRAY[]::text[], 'shared'),
                ('55555555-5555-5555-5555-555555555555', NULL, '50.00',
                 '2028-12-31', ARRAY['11', '22'], NULL, 'shared',
                 ARRAY['AA', 'BB'], NULL)
            """
        )
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {schema}.price_atoms_original AS
            TABLE {schema}.price_atoms
            """
        )
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {schema}.price_attributes_legacy AS
            WITH dictionary_source AS (
                SELECT 'negotiated_type'::varchar(64) AS attr_kind,
                       negotiated_type::text AS text_value,
                       NULL::text[] AS text_array
                  FROM {schema}.price_atoms
                 GROUP BY negotiated_type
                UNION ALL
                SELECT 'expiration_date'::varchar(64), expiration_date::text,
                       NULL::text[]
                  FROM {schema}.price_atoms
                 GROUP BY expiration_date
                UNION ALL
                SELECT 'service_code'::varchar(64), NULL::text,
                       service_code::text[]
                  FROM {schema}.price_atoms
                 GROUP BY service_code
                UNION ALL
                SELECT 'billing_class'::varchar(64), billing_class::text,
                       NULL::text[]
                  FROM {schema}.price_atoms
                 GROUP BY billing_class
                UNION ALL
                SELECT 'setting'::varchar(64), setting::text, NULL::text[]
                  FROM {schema}.price_atoms
                 GROUP BY setting
                UNION ALL
                SELECT 'billing_code_modifier'::varchar(64), NULL::text,
                       billing_code_modifier::text[]
                  FROM {schema}.price_atoms
                 GROUP BY billing_code_modifier
                UNION ALL
                SELECT 'additional_information'::varchar(64),
                       additional_information::text, NULL::text[]
                  FROM {schema}.price_atoms
                 GROUP BY additional_information
            )
            SELECT attr_kind,
                   (row_number() OVER (
                       PARTITION BY attr_kind
                       ORDER BY text_value NULLS FIRST, text_array NULLS FIRST
                   ) - 1)::integer AS attr_key,
                   text_value,
                   text_array
              FROM dictionary_source
            """
        )

        manifest = (
            await ptg2_manifest_publish._rewrite_price_atom_lean_dictionary(
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
        ) == 28
        dictionary_difference_count = int(
            await db.scalar(
                f"""
                SELECT count(*)
                  FROM (
                      (SELECT * FROM {schema}.price_attributes
                       EXCEPT ALL
                       SELECT * FROM {schema}.price_attributes_legacy)
                      UNION ALL
                      (SELECT * FROM {schema}.price_attributes_legacy
                       EXCEPT ALL
                       SELECT * FROM {schema}.price_attributes)
                  ) differences
                """
            )
            or 0
        )
        assert dictionary_difference_count == 0
        reconstructed_difference_count = int(
            await db.scalar(
                f"""
                WITH reconstructed AS (
                    SELECT atom.price_atom_global_id_128::text AS atom_id,
                           atom.negotiated_rate::text AS negotiated_rate,
                           negotiated_type.text_value AS negotiated_type,
                           expiration_date.text_value AS expiration_date,
                           service_code.text_array AS service_code,
                           billing_class.text_value AS billing_class,
                           setting.text_value AS setting,
                           billing_code_modifier.text_array AS billing_code_modifier,
                           additional_information.text_value AS additional_information
                      FROM {schema}.price_atoms atom
                      JOIN {schema}.price_attributes negotiated_type
                        ON negotiated_type.attr_kind = 'negotiated_type'
                       AND negotiated_type.attr_key = atom.negotiated_type_key
                      JOIN {schema}.price_attributes expiration_date
                        ON expiration_date.attr_kind = 'expiration_date'
                       AND expiration_date.attr_key = atom.expiration_date_key
                      JOIN {schema}.price_attributes service_code
                        ON service_code.attr_kind = 'service_code'
                       AND service_code.attr_key = atom.service_code_key
                      JOIN {schema}.price_attributes billing_class
                        ON billing_class.attr_kind = 'billing_class'
                       AND billing_class.attr_key = atom.billing_class_key
                      JOIN {schema}.price_attributes setting
                        ON setting.attr_kind = 'setting'
                       AND setting.attr_key = atom.setting_key
                      JOIN {schema}.price_attributes billing_code_modifier
                        ON billing_code_modifier.attr_kind = 'billing_code_modifier'
                       AND billing_code_modifier.attr_key = atom.billing_code_modifier_key
                      JOIN {schema}.price_attributes additional_information
                        ON additional_information.attr_kind = 'additional_information'
                       AND additional_information.attr_key = atom.additional_information_key
                ), original AS (
                    SELECT price_atom_global_id_128::text AS atom_id,
                           negotiated_rate::text AS negotiated_rate,
                           negotiated_type::text AS negotiated_type,
                           expiration_date::text AS expiration_date,
                           service_code::text[] AS service_code,
                           billing_class::text AS billing_class,
                           setting::text AS setting,
                           billing_code_modifier::text[] AS billing_code_modifier,
                           additional_information::text AS additional_information
                      FROM {schema}.price_atoms_original
                )
                SELECT count(*)
                  FROM (
                      (SELECT * FROM reconstructed
                       EXCEPT ALL
                       SELECT * FROM original)
                      UNION ALL
                      (SELECT * FROM original
                       EXCEPT ALL
                       SELECT * FROM reconstructed)
                  ) differences
                """
            )
            or 0
        )
        assert reconstructed_difference_count == 0
        lean_rows = await db.all(
            f"""
            SELECT price_atom_global_id_128::text, negotiated_rate
              FROM {schema}.price_atoms
             ORDER BY price_atom_global_id_128
            """
        )
        assert [_row_tuple(lean_atom_row) for lean_atom_row in lean_rows] == [
            ("11111111-1111-1111-1111-111111111111", "10.00"),
            ("22222222-2222-2222-2222-222222222222", "20.00"),
            ("33333333-3333-3333-3333-333333333333", "30.00"),
            ("44444444-4444-4444-4444-444444444444", "40.00"),
            ("55555555-5555-5555-5555-555555555555", "50.00"),
        ]
    finally:
        await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await db.disconnect()
