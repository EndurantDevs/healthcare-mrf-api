# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL differential and rollback proof for projection v3."""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as rate_profiles
from api import ptg2_serving as serving
from api.plan_pricing_aggregate_pack import AggregateZipRecord
from api import plan_pricing_projection_contract as projection_contract
from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_v3_types import _BuildState
from tests.test_plan_pricing_projection_postgres import (
    POSTGRES_DSN_ENV,
    TEST_DATABASE_PATTERN,
    _create_import_run_stub,
    _migration_statements,
    _sqlalchemy_async_dsn,
)
from tests.test_plan_pricing_projection_v3_postgres import (
    _factorized_migration_statements,
)


asyncpg = pytest.importorskip("asyncpg")


@dataclass(frozen=True)
class _MigratedDatabase:
    schema: str
    engine: AsyncEngine


@pytest.fixture
async def migrated_v3_database(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")

    schema = f"plan_pricing_v3_diff_{uuid.uuid4().hex[:12]}"
    engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        await _create_import_run_stub(admin, f'"{schema}"')
        for statement in _migration_statements(monkeypatch, schema):
            await admin.execute(statement)
        for statement in _factorized_migration_statements(monkeypatch, schema):
            await admin.execute(statement)
        yield _MigratedDatabase(schema, engine)
    finally:
        await engine.dispose()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


async def _insert_candidate(connection, schema: str, projection_id: str) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO "{schema}".plan_pricing_projection_candidate (
                projection_id, contract_version, binding_manifest_digest,
                binding_manifest, provider_signature, state
            ) VALUES (
                :projection_id, 'plan_pricing_factorized_v3', :digest,
                '[]'::jsonb, :digest, 'building'
            )
            """
        ),
        {"projection_id": projection_id, "digest": projection_id},
    )


def _provider_cell(
    projection_id: str,
    npi: int,
    zip5: str,
    entity_type_code: int,
    taxonomy_code: str,
) -> dict:
    return {
        "projection_id": projection_id,
        "geo_cell": zip5,
        "npi": npi,
        "entity_type_code": entity_type_code,
        "taxonomy_codes": [taxonomy_code],
        "fragment": b"{}",
    }


async def _insert_provider_cells(
    connection,
    schema: str,
    projection_id: str,
) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO "{schema}".plan_pricing_provider_cell (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment
            ) VALUES (
                :projection_id, :geo_cell, :npi, :entity_type_code,
                :taxonomy_codes, :fragment
            )
            """
        ),
        [
            _provider_cell(projection_id, 1000000001, "10001", 1, " 207x00000x "),
            _provider_cell(projection_id, 1000000002, "10001", 1, "208D00000X"),
            _provider_cell(projection_id, 1000000003, "10001", 2, "207X00000X"),
            _provider_cell(projection_id, 1000000004, "10002", 1, "207X00000X"),
        ],
    )


def _membership(binding_ordinal: int, provider_set_key: int, npi: int) -> dict:
    return {
        "binding_ordinal": binding_ordinal,
        "provider_set_key": provider_set_key,
        "npi": npi,
    }


def _occurrence(
    binding_ordinal: int,
    provider_set_key: int,
    price_set_id: str,
    occurrence_count: int = 1,
) -> dict:
    return {
        "binding_ordinal": binding_ordinal,
        "provider_set_key": provider_set_key,
        "price_set_id": price_set_id,
        "occurrence_count": occurrence_count,
    }


def _rate(
    binding_ordinal: int,
    price_set_id: str,
    negotiated_rate: str,
    rate_multiplicity: int = 1,
) -> dict:
    return {
        "binding_ordinal": binding_ordinal,
        "price_set_id": price_set_id,
        "negotiated_rate": Decimal(negotiated_rate),
        "rate_multiplicity": rate_multiplicity,
    }


async def _stage_rows(connection, memberships, occurrences, rates) -> None:
    statements_and_rows = (
        (
            """INSERT INTO plan_pricing_provider_member_stage
               (binding_ordinal, provider_set_key, npi)
               VALUES (:binding_ordinal, :provider_set_key, :npi)""",
            memberships,
        ),
        (
            """INSERT INTO plan_pricing_code_occurrence_stage
               (binding_ordinal, provider_set_key, price_set_id, occurrence_count)
               VALUES (:binding_ordinal, :provider_set_key, :price_set_id,
                       :occurrence_count)""",
            occurrences,
        ),
        (
            """INSERT INTO plan_pricing_price_rate_stage
               (binding_ordinal, price_set_id, negotiated_rate, rate_multiplicity)
               VALUES (:binding_ordinal, :price_set_id, :negotiated_rate,
                       :rate_multiplicity)""",
            rates,
        ),
    )
    for statement, rows_by_field in statements_and_rows:
        await connection.execute(text(statement), rows_by_field)
    await connection.execute(
        text(
            """INSERT INTO plan_pricing_provider_set_stage
               (binding_ordinal, provider_set_key, provider_set_id,
                membership_count)
               SELECT binding_ordinal, provider_set_key,
                      md5(binding_ordinal::text || ':' || provider_set_key::text),
                      COUNT(*)::integer
                 FROM plan_pricing_provider_member_stage
                GROUP BY binding_ordinal, provider_set_key"""
        )
    )


async def _aggregate_records(
    connection,
    monkeypatch,
    schema: str,
    projection_id: str,
    code_identity: tuple[str, str],
    taxonomy_codes: tuple[str, ...] | None,
) -> tuple[AggregateZipRecord, ...]:
    aggregate_sql = projection._AGGREGATE_STATS_SQL.replace(
        table("plan_pricing_provider_cell"),
        f'"{schema}"."plan_pricing_provider_cell"',
    )
    aggregate_work_sql = projection._AGGREGATE_WORK_SQL.replace(
        table("plan_pricing_provider_cell"),
        f'"{schema}"."plan_pricing_provider_cell"',
    )
    with monkeypatch.context() as scoped:
        scoped.setattr(projection, "_AGGREGATE_STATS_SQL", aggregate_sql)
        scoped.setattr(projection, "_AGGREGATE_WORK_SQL", aggregate_work_sql)
        scoped.setattr(
            serving,
            "_inferred_provider_taxonomy_rule",
            lambda _args: (
                SimpleNamespace(taxonomy_codes=taxonomy_codes)
                if taxonomy_codes is not None
                else None
            ),
        )
        return await projection._aggregate_records(
            connection, projection_id, code_identity
        )


async def _stored_rate_profiles(
    connection,
    monkeypatch,
    schema: str,
    projection_id: str,
    code_identity: tuple[str, str],
):
    store_sql = rate_profiles._STORE_RATE_PROFILES_SQL.replace(
        table("plan_pricing_rate_profile"),
        f'"{schema}"."plan_pricing_rate_profile"',
    )
    state = _BuildState(hashlib.sha256())
    with monkeypatch.context() as scoped:
        scoped.setattr(projection_contract, "SCHEMA", schema)
        await rate_profiles._store_rate_profiles(
            connection,
            projection_id,
            code_identity,
            state,
            store_sql=store_sql,
        )
    stored_result = await connection.execute(
        text(
            f"""SELECT binding_ordinal, provider_set_key, membership_count,
                       minimum_negotiated_rate, maximum_negotiated_rate,
                       rate_count, negotiated_rates, rate_multiplicities
                  FROM "{schema}".plan_pricing_rate_profile
                 WHERE projection_id = :projection_id
                 ORDER BY binding_ordinal, provider_set_key"""
        ),
        {"projection_id": projection_id},
    )
    return tuple(stored_result.mappings()), state


def _assert_three_binding_rate_profiles(stored_profiles) -> None:
    assert [
        (
            profile_by_field["binding_ordinal"],
            profile_by_field["provider_set_key"],
            profile_by_field["membership_count"],
            profile_by_field["rate_count"],
            tuple(profile_by_field["negotiated_rates"]),
            tuple(profile_by_field["rate_multiplicities"]),
        )
        for profile_by_field in stored_profiles
    ] == [
        (0, 10, 2, 6, (Decimal("10"), Decimal("20")), (4, 2)),
        (1, 20, 2, 1, (Decimal("30"),), (1,)),
        (2, 30, 2, 2, (Decimal("40"), Decimal("50")), (1, 1)),
    ]


@pytest.mark.asyncio
async def test_v3_sql_preserves_three_binding_multiplicity_and_union(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "a" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await _insert_provider_cells(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _stage_rows(
            connection,
            [
                _membership(0, 10, 1000000001),
                _membership(0, 10, 1000000002),
                _membership(1, 20, 1000000002),
                _membership(1, 20, 1000000003),
                _membership(2, 30, 1000000001),
                _membership(2, 30, 1000000004),
            ],
            [
                _occurrence(0, 10, "a", 2),
                _occurrence(1, 20, "b"),
                _occurrence(2, 30, "c"),
            ],
            [
                _rate(0, "a", "10", 2),
                _rate(0, "a", "20"),
                _rate(1, "b", "30"),
                _rate(2, "c", "40"),
                _rate(2, "c", "50"),
            ],
        )
        stored_profiles, profile_state = await _stored_rate_profiles(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
        )
        actual_records = await _aggregate_records(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
            None,
        )
    assert actual_records == (
        AggregateZipRecord("10001", 3, 9, Decimal("10"), Decimal("20"), Decimal("50")),
        AggregateZipRecord("10002", 1, 2, Decimal("40"), Decimal("45"), Decimal("50")),
    )
    assert profile_state.rate_profile_count == 3
    _assert_three_binding_rate_profiles(stored_profiles)


@pytest.mark.asyncio
async def test_v3_sql_normalizes_ruled_taxonomy_across_two_bindings(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "b" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await _insert_provider_cells(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _stage_rows(
            connection,
            [
                _membership(0, 10, 1000000001),
                _membership(0, 10, 1000000002),
                _membership(1, 20, 1000000001),
                _membership(1, 20, 1000000003),
                _membership(1, 21, 1000000004),
            ],
            [_occurrence(0, 10, "a"), _occurrence(1, 20, "b"), _occurrence(1, 21, "c")],
            [
                _rate(0, "a", "10"),
                _rate(0, "a", "20"),
                _rate(1, "b", "30"),
                _rate(1, "c", "40"),
                _rate(1, "c", "50"),
            ],
        )
        actual_records = await _aggregate_records(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("CPT", "27447"),
            ("207X00000X",),
        )
    assert actual_records == (
        AggregateZipRecord("10001", 1, 3, Decimal("10"), Decimal("20"), Decimal("30")),
        AggregateZipRecord("10002", 1, 2, Decimal("40"), Decimal("45"), Decimal("50")),
    )


async def _cancelled_build(
    engine: AsyncEngine,
    schema: str,
    projection_id: str,
    inserted: asyncio.Event,
) -> None:
    async with engine.begin() as connection:
        await _insert_candidate(connection, schema, projection_id)
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_provider_membership
                    (projection_id, binding_ordinal, provider_set_key, npi)
                    VALUES (:projection_id, 0, 1, 1000000001)"""
            ),
            {"projection_id": projection_id},
        )
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_provider_cell
                    (projection_id, geo_cell, npi, entity_type_code,
                     taxonomy_codes, fragment)
                    VALUES (:projection_id, '10001', 1000000001, 1,
                            ARRAY['207X00000X'], :fragment)"""
            ),
            {"projection_id": projection_id, "fragment": b"{}"},
        )
        await connection.execute(
            text(
                f"""INSERT INTO "{schema}".plan_pricing_rate_profile (
                    projection_id, code_system, code, binding_ordinal,
                    provider_set_key, membership_count,
                    minimum_negotiated_rate, maximum_negotiated_rate,
                    rate_count, negotiated_rates, rate_multiplicities
                ) VALUES (
                    :projection_id, 'CPT', '27447', 0, 1, 1,
                    10, 10, 1, ARRAY[10]::numeric[], ARRAY[1]::bigint[]
                )"""
            ),
            {"projection_id": projection_id},
        )
        inserted.set()
        await asyncio.Future()


@pytest.mark.asyncio
async def test_cancelled_transaction_leaves_no_candidate_or_child_rows(
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "c" * 64
    inserted = asyncio.Event()
    build_task = asyncio.create_task(
        _cancelled_build(database.engine, database.schema, projection_id, inserted)
    )
    await asyncio.wait_for(inserted.wait(), timeout=2)
    build_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(build_task, timeout=2)

    async with database.engine.connect() as connection:
        counts = await connection.execute(
            text(
                f"""SELECT
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_projection_candidate WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_provider_membership WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_provider_cell WHERE projection_id = :id),
                  (SELECT COUNT(*) FROM "{database.schema}".
                    plan_pricing_rate_profile WHERE projection_id = :id)"""
            ),
            {"id": projection_id},
        )
        assert counts.one() == (0, 0, 0, 0)
