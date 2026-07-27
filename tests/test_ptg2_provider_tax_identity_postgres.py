# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL lifecycle proof for shared provider tax identities."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.ptg2_provider_tax_identity_postgres_support import (
    assert_layout_cascade,
    assert_new_v4_requires_sidecar,
    assert_pre_sidecar_v4_completion,
    async_database_url,
    create_prerequisites,
    drop_disposable_schema,
    invalid_source_entries_by_snapshot,
    insert_candidate_sidecar,
    load_migration,
    manifest_insert,
    manifest_parameters,
    quoted,
    run_migration_action,
)


async def _insert_valid_sidecar(
    connection: Any,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        manifest_insert(schema_name),
        manifest_parameters(),
    )
    await connection.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_provider_tax_identity (
                snapshot_key, tin_key, tin_id_128, tin_hmac_sha256
            ) VALUES (
                11,
                0,
                decode(repeat('44', 16), 'hex'),
                decode(repeat('44', 16) || repeat('55', 16), 'hex')
            )
            """
        )
    )
    await connection.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_provider_group_tax_identity (
                snapshot_key,
                provider_group_global_id_128,
                tax_identity_state,
                tin_key,
                source_bitmap
            )
            SELECT
                11,
                provider_group_global_id_128,
                CASE provider_group_key
                    WHEN 1 THEN 'matched_ein'
                    WHEN 2 THEN 'missing'
                    WHEN 3 THEN 'malformed'
                    WHEN 4 THEN 'unsupported_type'
                END,
                CASE WHEN provider_group_key = 1 THEN 0 END,
                decode('01', 'hex')
              FROM {schema}.ptg2_v3_provider_group
             WHERE snapshot_key = 11
            """
        )
    )


async def _assert_sidecar_cardinality(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    async with engine.begin() as connection:
        await _insert_valid_sidecar(connection, schema_name)
        sidecar_count = await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM "
                f"{schema}.ptg2_provider_group_tax_identity"
            )
        )
        matched_count = await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM "
                f"{schema}.ptg2_provider_group_tax_identity "
                "WHERE tax_identity_state = 'matched_ein' "
                "AND tin_key IS NOT NULL"
            )
        )
        unavailable_reverse_count = await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM "
                f"{schema}.ptg2_provider_group_tax_identity "
                "WHERE tax_identity_state <> 'matched_ein' "
                "AND tin_key IS NOT NULL"
            )
        )
        manifest_contract_values = (
            await connection.execute(
                sa.text(
                    f"""
                    SELECT token_policy_id,
                           normalization_contract,
                           hmac_contract
                      FROM {schema}.ptg2_provider_tax_identity_manifest
                     WHERE snapshot_key = 11
                    """
                )
            )
        ).one()
    assert sidecar_count == 4
    assert matched_count == 1
    assert unavailable_reverse_count == 0
    assert tuple(manifest_contract_values) == (
        "ptg-tin-hmac-sha256-v1:2026-07",
        "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_sha256_ptg_tin_v1",
    )


async def _tax_schema_catalog(
    engine: AsyncEngine,
    schema_name: str,
) -> tuple[list[Any], list[Any]]:
    async with engine.begin() as connection:
        index_records = (
            await connection.execute(
                sa.text(
                    """
                    SELECT indexname, indexdef FROM pg_indexes
                     WHERE schemaname = :schema_name
                       AND tablename = ANY(CAST(:table_names AS text[]))
                    """
                ),
                {
                    "schema_name": schema_name,
                    "table_names": [
                        "ptg2_provider_tax_identity",
                        "ptg2_provider_group_tax_identity",
                    ],
                },
            )
        ).fetchall()
        column_records = (
            await connection.execute(
                sa.text(
                    """
                    SELECT table_name, column_name, data_type
                      FROM information_schema.columns
                     WHERE table_schema = :schema_name
                       AND table_name = ANY(CAST(:table_names AS text[]))
                    """
                ),
                {
                    "schema_name": schema_name,
                    "table_names": [
                        "ptg2_provider_tax_identity_legacy_layout",
                        "ptg2_provider_tax_identity_manifest",
                        "ptg2_provider_tax_identity",
                        "ptg2_provider_group_tax_identity",
                    ],
                },
            )
        ).fetchall()
    return list(index_records), list(column_records)


async def _assert_index_and_storage_contract(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    index_records, column_records = await _tax_schema_catalog(engine, schema_name)
    index_definition_by_name = {
        str(index_record.indexname): str(index_record.indexdef)
        for index_record in index_records
    }
    locator = index_definition_by_name["ptg2_provider_tax_identity_locator_idx"]
    reverse = index_definition_by_name[
        "ptg2_provider_group_tax_identity_tin_group_idx"
    ]
    stored_column_names = {
        str(column_record.column_name) for column_record in column_records
    }
    stored_type_by_column = {
        str(column_record.column_name): str(column_record.data_type)
        for column_record in column_records
    }
    created_at_tables = {
        str(column_record.table_name)
        for column_record in column_records
        if str(column_record.column_name) == "created_at"
    }
    assert set(index_definition_by_name) == {
        "ptg2_provider_tax_identity_pkey",
        "ptg2_provider_tax_identity_locator_idx",
        "ptg2_provider_group_tax_identity_pkey",
        "ptg2_provider_group_tax_identity_tin_group_idx",
    }
    assert locator.startswith("CREATE UNIQUE INDEX")
    assert "tin_id_128" in locator and "tin_hmac_sha256" in locator
    assert "tax_identity_state = 'matched_ein'" in reverse
    assert {"tin", "tin_value", "business_name"}.isdisjoint(stored_column_names)
    assert created_at_tables == {"ptg2_provider_tax_identity_manifest"}
    assert stored_type_by_column["tax_identity_state"] == "text"
    assert stored_type_by_column["source_bitmap"] == "bytea"
    assert stored_type_by_column["tin_key"] == "integer"
    assert stored_type_by_column["tin_hmac_sha256"] == "bytea"


async def _assert_token_and_state_constraints(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    invalid_statements = (
        f"""
        INSERT INTO {schema}.ptg2_provider_group_tax_identity (
            snapshot_key, provider_group_global_id_128,
            tax_identity_state, tin_key, source_bitmap
        ) VALUES (
            11, decode(repeat('99', 16), 'hex'),
            'missing', 0, decode('01', 'hex')
        )
        """,
        f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity (
            snapshot_key, tin_key, tin_id_128, tin_hmac_sha256
        ) VALUES (
            11, 2, decode(repeat('66', 16), 'hex'),
            decode(repeat('77', 32), 'hex')
        )
        """,
    )
    for statement, expected_error in zip(
        invalid_statements,
        ("state_check", "token_check"),
        strict=True,
    ):
        with pytest.raises(DBAPIError, match=expected_error):
            async with engine.begin() as connection:
                await connection.execute(sa.text(statement))


async def _assert_manifest_contract_constraints(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    contract_cases = (
        (
            15,
            {"normalization_contract": "ein_ascii_digits_9_v1"},
            "manifest_contract_check",
        ),
        (
            16,
            {"hmac_contract": "hmac_sha256_ptg_tax_identity_v1"},
            "manifest_contract_check",
        ),
        (
            17,
            {"token_policy_id": "ptg-tin-hmac-sha256-v1:Bad"},
            "manifest_policy_check",
        ),
    )
    for snapshot_key, overrides, expected_error in contract_cases:
        with pytest.raises(DBAPIError, match=expected_error):
            async with engine.begin() as connection:
                await connection.execute(
                    manifest_insert(schema_name, snapshot_key=snapshot_key),
                    manifest_parameters(**overrides),
                )


async def _assert_direct_writes_are_immutable(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    direct_statements = [
        f"""
        UPDATE {schema}.ptg2_provider_tax_identity_manifest
           SET content_digest = content_digest WHERE snapshot_key = 11
        """,
        *(
            f"DELETE FROM {schema}.{table_name} WHERE snapshot_key = 11"
            for table_name in (
                "ptg2_provider_group_tax_identity",
                "ptg2_provider_tax_identity",
                "ptg2_provider_tax_identity_manifest",
            )
        ),
    ]
    for statement in direct_statements:
        with pytest.raises(
            DBAPIError,
            match="ptg2_provider_tax_identity_immutable",
        ):
            async with engine.begin() as connection:
                await connection.execute(sa.text(statement))
    with pytest.raises(
        DBAPIError,
        match="ptg2_provider_tax_identity_legacy_layout_immutable",
    ):
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"DELETE FROM "
                    f"{schema}.ptg2_provider_tax_identity_legacy_layout "
                    "WHERE snapshot_key = 19"
                )
            )


async def _assert_completion_guards(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    with pytest.raises(
        DBAPIError,
        match="ptg2_provider_tax_identity_not_building",
    ):
        async with engine.begin() as connection:
            await connection.execute(
                manifest_insert(schema_name, snapshot_key=12),
                manifest_parameters(),
            )
    await insert_candidate_sidecar(
        engine,
        schema_name,
        snapshot_key=13,
        group_limit=3,
        bitmap_hex="01",
    )
    await insert_candidate_sidecar(
        engine,
        schema_name,
        snapshot_key=14,
        group_limit=4,
        bitmap_hex="0100",
    )
    await insert_candidate_sidecar(
        engine,
        schema_name,
        snapshot_key=18,
        group_limit=4,
        bitmap_hex="00",
    )
    for snapshot_key in (13, 14, 18):
        with pytest.raises(
            DBAPIError,
            match="ptg2_provider_tax_identity_summary_mismatch",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                        "SET state = 'complete' "
                        "WHERE snapshot_key = :snapshot_key"
                    ),
                    {"snapshot_key": snapshot_key},
                )
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                "SET state = 'complete' WHERE snapshot_key = 11"
            )
        )


async def _insert_source_provenance_candidates(
    engine: AsyncEngine,
    schema_name: str,
) -> tuple[int, ...]:
    """Insert one high-bit case and every malformed source-map case."""

    nine_source_entries = [
        {"shard_id": f"shard-{ordinal}", "ordinal": ordinal}
        for ordinal in range(9)
    ]
    await insert_candidate_sidecar(
        engine,
        schema_name,
        snapshot_key=20,
        group_limit=4,
        bitmap_hex="0102",
        manifest_overrides={
            "source_map": json.dumps(nine_source_entries),
            "source_shard_count": 9,
        },
    )
    invalid_entries_by_snapshot = invalid_source_entries_by_snapshot()
    for snapshot_key, source_entries in invalid_entries_by_snapshot.items():
        await insert_candidate_sidecar(
            engine,
            schema_name,
            snapshot_key=snapshot_key,
            group_limit=4,
            bitmap_hex="01",
            manifest_overrides={
                "source_map": json.dumps(source_entries),
                "source_shard_count": len(source_entries),
            },
        )
    return (20, *invalid_entries_by_snapshot)


async def _assert_source_provenance_completion_guards(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Malformed source maps and out-of-range bitmap bits cannot seal."""

    schema = quoted(schema_name)
    snapshot_keys = await _insert_source_provenance_candidates(
        engine,
        schema_name,
    )
    for snapshot_key in snapshot_keys:
        with pytest.raises(
            DBAPIError,
            match="ptg2_provider_tax_identity_summary_mismatch",
        ):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                        "SET state = 'complete' "
                        "WHERE snapshot_key = :snapshot_key"
                    ),
                    {"snapshot_key": snapshot_key},
                )


@pytest.mark.asyncio
async def test_provider_tax_identity_postgres_lifecycle(monkeypatch) -> None:
    """Prove exact states, immutable rows, reverse lookup, and cascade."""

    engine = create_async_engine(async_database_url(), pool_size=1, max_overflow=0)
    schema_name = f"ptg2_tax_identity_test_{uuid.uuid4().hex}"
    migration = load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    is_schema_created = False
    try:
        await create_prerequisites(engine, schema_name)
        is_schema_created = True
        await run_migration_action(engine, migration, "upgrade")
        await _assert_sidecar_cardinality(engine, schema_name)
        await _assert_index_and_storage_contract(engine, schema_name)
        await _assert_token_and_state_constraints(engine, schema_name)
        await _assert_manifest_contract_constraints(engine, schema_name)
        await _assert_direct_writes_are_immutable(engine, schema_name)
        await _assert_completion_guards(engine, schema_name)
        await _assert_source_provenance_completion_guards(
            engine,
            schema_name,
        )
        await assert_pre_sidecar_v4_completion(engine, schema_name)
        await assert_new_v4_requires_sidecar(engine, schema_name)
        await assert_layout_cascade(engine, schema_name)
        await run_migration_action(engine, migration, "downgrade")
    finally:
        if is_schema_created:
            await drop_disposable_schema(engine, schema_name)
        await engine.dispose()
