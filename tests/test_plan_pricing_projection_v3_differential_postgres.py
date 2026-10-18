# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL differential and rollback proof for projection v3."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from api import plan_pricing_em_distance_build as em_build
from api import plan_pricing_projection_contract as projection_contract
from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as rate_profiles
from api import plan_pricing_projection_v3_work as work_admission
from api import ptg2_serving as serving
from api.plan_pricing_aggregate_pack import AggregateZipRecord
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
    projection_id: str,
) -> None:
    await connection.execute(
        text(
            """
            INSERT INTO plan_pricing_provider_cell_stage (
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
            _provider_cell(projection_id, 1000000001, "10003", 1, " 207x00000x "),
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
    projection_id: str,
    code_identity: tuple[str, str],
) -> tuple[AggregateZipRecord, ...]:
    return await projection._aggregate_records(connection, projection_id, code_identity)


async def _prepare_code_work_under_limits(
    connection,
    monkeypatch,
    schema: str,
    projection_id: str,
    code_identity: tuple[str, str],
    taxonomy_codes: tuple[str, ...] | None,
    **limit_overrides: int,
):
    state = _BuildState(hashlib.sha256())
    with monkeypatch.context() as scoped:
        scoped.setattr(projection_contract, "SCHEMA", schema)
        scoped.setattr(work_admission, "MAX_CODE_MEMBERSHIP_PROBES", 100)
        scoped.setattr(work_admission, "MAX_PROJECTION_MEMBERSHIP_PROBES", 1_000)
        scoped.setattr(work_admission, "MAX_CODE_MEMBER_CELL_WORK_ROWS", 100)
        scoped.setattr(work_admission, "MAX_PROJECTION_MEMBER_CELL_WORK_ROWS", 1_000)
        for field_name, limit_value in limit_overrides.items():
            scoped.setattr(work_admission, field_name, limit_value)
        scoped.setattr(
            serving,
            "_inferred_provider_taxonomy_rule",
            lambda _args: SimpleNamespace(taxonomy_codes=taxonomy_codes) if taxonomy_codes is not None else None,
        )
        prepared_work = await work_admission._prepare_code_work(connection, projection_id, code_identity, state)
    return prepared_work, state


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


async def _stage_three_binding_inputs(connection) -> None:
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


@pytest.mark.asyncio
async def test_v3_sql_preserves_three_binding_multiplicity_and_union(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "a" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _insert_provider_cells(connection, projection_id)
        await _stage_three_binding_inputs(connection)
        prepared_work, work_state = await _prepare_code_work_under_limits(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
            None,
            MAX_CODE_RATE_PROFILE_WORK_ROWS=5,
        )
        profile_join_rows = await connection.scalar(
            text("SELECT SUM(join_row_count) FROM plan_pricing_rate_frequency_stage")
        )
        assert profile_join_rows == 5
        await connection.execute(text("TRUNCATE plan_pricing_code_occurrence_stage, plan_pricing_price_rate_stage"))
        stored_profiles, profile_state = await _stored_rate_profiles(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("HCPCS", "G0439"),
        )
        actual_records = await _aggregate_records(
            connection,
            projection_id,
            ("HCPCS", "G0439"),
        )
    assert actual_records == (
        AggregateZipRecord("10001", 3, 9, Decimal("10"), Decimal("20"), Decimal("50")),
        AggregateZipRecord("10002", 1, 2, Decimal("40"), Decimal("45"), Decimal("50")),
        AggregateZipRecord("10003", 1, 8, Decimal("10"), Decimal("15"), Decimal("50")),
    )
    assert prepared_work == work_admission._CodeWork(6, 8, 6, 5, 11, 9, 6, 2, 19, 9)
    assert (
        work_state.membership_probe_work_rows,
        work_state.member_cell_work_rows,
        work_state.rate_profile_work_rows,
        work_state.aggregate_work_rows,
    ) == (6, 8, 5, 11)
    assert profile_state.rate_profile_count == 3
    _assert_three_binding_rate_profiles(stored_profiles)


async def _assert_ruled_work_caps(
    connection,
    monkeypatch,
    schema: str,
    projection_id: str,
) -> None:
    common_args = (
        connection,
        monkeypatch,
        schema,
        projection_id,
        ("CPT", "27447"),
        ("207X00000X",),
    )
    with pytest.raises(ValueError, match="membership-probe"):
        await _prepare_code_work_under_limits(
            *common_args,
            MAX_CODE_MEMBERSHIP_PROBES=4,
        )
    with pytest.raises(ValueError, match="member-cell"):
        await _prepare_code_work_under_limits(
            *common_args,
            MAX_CODE_MEMBER_CELL_WORK_ROWS=6,
        )


@pytest.mark.asyncio
async def test_v3_sql_normalizes_ruled_taxonomy_across_two_bindings(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "b" * 64
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
        await projection._create_stage_tables(connection)
        await _insert_provider_cells(connection, projection_id)
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
        await _assert_ruled_work_caps(connection, monkeypatch, database.schema, projection_id)
        prepared_work, _state = await _prepare_code_work_under_limits(
            connection,
            monkeypatch,
            database.schema,
            projection_id,
            ("CPT", "27447"),
            ("207X00000X",),
        )
        actual_records = await _aggregate_records(
            connection,
            projection_id,
            ("CPT", "27447"),
        )
    assert actual_records == (
        AggregateZipRecord("10001", 1, 3, Decimal("10"), Decimal("20"), Decimal("30")),
        AggregateZipRecord("10002", 1, 2, Decimal("40"), Decimal("45"), Decimal("50")),
        AggregateZipRecord("10003", 1, 3, Decimal("10"), Decimal("20"), Decimal("30")),
    )
    assert prepared_work == work_admission._CodeWork(5, 7, 5, 5, 8, 5, 2, 2, 8, 3)


def _em_output_relation(name):
    assert name in {"em_actual_rates", "em_legacy_rates"}
    return f'pg_temp."{name}"'


async def _create_em_output(connection, name):
    relation = _em_output_relation(name)
    array_checks = " AND ".join(
        f"cardinality({field})=6 AND array_ndims({field})=1 AND array_lower({field},1)=1"
        for field in ("minimum_rates", "maximum_rates", "rate_counts")
    )
    slot_checks = " AND ".join(
        f"""(((code_mask & (1 << {index - 1})) <> 0) IS NOT DISTINCT FROM (minimum_rates[{index}] IS NOT NULL))
        AND (((code_mask & (1 << {index - 1})) <> 0) IS NOT DISTINCT FROM (maximum_rates[{index}] IS NOT NULL))
        AND (((code_mask & (1 << {index - 1})) <> 0) IS NOT DISTINCT FROM (rate_counts[{index}] IS NOT NULL))
        AND ((code_mask & (1 << {index - 1})) = 0 OR (
            minimum_rates[{index}] >= 0 AND maximum_rates[{index}] >= minimum_rates[{index}]
            AND rate_counts[{index}] > 0
            AND minimum_rates[{index}]::text NOT IN ('NaN','Infinity','-Infinity')
            AND maximum_rates[{index}]::text NOT IN ('NaN','Infinity','-Infinity')))"""
        for index in range(1, 7)
    )
    await connection.execute(
        text(f"""CREATE TEMP TABLE {relation} (
        projection_id varchar(64) NOT NULL CHECK (projection_id ~ '^[0-9a-f]{{64}}$'),
        npi bigint NOT NULL CHECK (npi > 0), code_mask smallint NOT NULL CHECK (code_mask BETWEEN 1 AND 63),
        minimum_rates numeric[] NOT NULL, maximum_rates numeric[] NOT NULL, rate_counts bigint[] NOT NULL,
        PRIMARY KEY (projection_id,npi), CHECK ({array_checks}), CHECK ({slot_checks})
    ) ON COMMIT DROP""")
    )


async def _stage_em_inputs(connection):
    await _stage_rows(
        connection,
        [_membership(3, 10, 101), _membership(3, 11, 101), _membership(17, 30, 101), _membership(17, 30, 102)],
        [_occurrence(3, 10, "a", 2), _occurrence(3, 11, "b", 4), _occurrence(17, 30, "c", 3)],
        [_rate(3, "a", "10.25", 3), _rate(3, "b", "20.5", 2), _rate(17, "c", "5.125", 4)],
    )
    await connection.execute(
        text("""CREATE TEMP TABLE em_legacy_stage (
        npi bigint NOT NULL, code_index smallint NOT NULL CHECK (code_index BETWEEN 0 AND 5),
        minimum_rate numeric NOT NULL CHECK (minimum_rate >= 0),
        maximum_rate numeric NOT NULL CHECK (maximum_rate >= minimum_rate),
        rate_count bigint NOT NULL CHECK (rate_count > 0), PRIMARY KEY(npi,code_index)
    ) ON COMMIT DROP""")
    )


async def _assert_em_native_bounds(connection, monkeypatch):
    work_by_field = (
        (await connection.execute(text(em_build._EM_RATE_WORK_SQL), {"rate_row_probe_limit": 4})).mappings().one()
    )
    assert dict(work_by_field) == {"attributed_rate_rows": 3, "rate_profile_work_rows": Decimal(3)}
    assert await em_build._admit_em_rate_work(connection, 3) == 3
    for constant, limit, message in [
        ("MAX_PROJECTION_PROVIDER_MEMBERSHIPS", 2, "attributed rate row ceiling"),
        ("MAX_EM_CODE_RATE_WORK_ROWS", 2, "rate-profile work bound"),
    ]:
        with monkeypatch.context() as budget_patch:
            budget_patch.setattr(em_build, constant, limit)
            with pytest.raises(ValueError, match=message):
                await em_build._admit_em_rate_work(connection, em_build.MAX_EM_ATTRIBUTED_RATE_ROWS)
        assert await connection.scalar(text("SELECT COUNT(*) FROM plan_pricing_em_distance_rate_stage")) == 0
    with pytest.raises(ValueError, match="attributed rate row ceiling"):
        await em_build._admit_em_rate_work(connection, 2)
    assert await connection.scalar(text("SELECT COUNT(*) FROM plan_pricing_em_distance_rate_stage")) == 0


async def _stage_em_code_pair(connection, code_index):
    if code_index:
        await connection.execute(text("UPDATE plan_pricing_price_rate_stage SET negotiated_rate=negotiated_rate+1"))
    assert await em_build._admit_em_rate_work(connection, em_build.MAX_EM_ATTRIBUTED_RATE_ROWS - 3 * code_index) == 3
    await connection.execute(text(em_build._STAGE_CODE_RATES_SQL), {"code_index": code_index})
    await connection.execute(
        text("""INSERT INTO em_legacy_stage
        (npi,code_index,minimum_rate,maximum_rate,rate_count)
        SELECT member.npi,:code_index,MIN(price.negotiated_rate),MAX(price.negotiated_rate),
               SUM(occurrence.occurrence_count::numeric * price.rate_multiplicity::numeric)::bigint
          FROM plan_pricing_code_occurrence_stage occurrence
          JOIN plan_pricing_price_rate_stage price
            ON price.binding_ordinal=occurrence.binding_ordinal AND price.price_set_id=occurrence.price_set_id
          JOIN plan_pricing_provider_member_stage member
            ON member.binding_ordinal=occurrence.binding_ordinal AND member.provider_set_key=occurrence.provider_set_key
         GROUP BY member.npi"""),
        {"code_index": code_index},
    )


async def _assert_em_attributed_stage(connection):
    staged_rates = (
        await connection.execute(
            text("""SELECT binding_ordinal,npi,code_index,
        minimum_rate,maximum_rate,rate_count FROM plan_pricing_em_distance_rate_stage
        ORDER BY code_index,binding_ordinal,npi""")
        )
    ).all()
    expected_rates = []
    for index in range(6):
        expected_rates.extend(
            [
                (3, 101, index, Decimal("10.25") + index, Decimal("20.5") + index, 14),
                (17, 101, index, Decimal("5.125") + index, Decimal("5.125") + index, 12),
                (17, 102, index, Decimal("5.125") + index, Decimal("5.125") + index, 12),
            ]
        )
    assert staged_rates == expected_rates
    assert await connection.scalar(text("SELECT COUNT(*) FROM em_legacy_stage")) == 12


async def _store_em_output_pair(connection, projection_id):
    storage_token = em_build.table("plan_pricing_em_distance_rate")
    assert em_build._STORE_RATES_SQL.count(storage_token) == 1
    actual_sql = em_build._STORE_RATES_SQL.replace(storage_token, _em_output_relation("em_actual_rates"), 1)
    assert actual_sql.replace(_em_output_relation("em_actual_rates"), storage_token, 1) == em_build._STORE_RATES_SQL
    await connection.execute(text(actual_sql), {"projection_id": projection_id})
    legacy_sql = f"""INSERT INTO {_em_output_relation("em_legacy_rates")}
        (projection_id,npi,code_mask,minimum_rates,maximum_rates,rate_counts)
        SELECT :projection_id,npi,SUM(1 << code_index)::smallint,
               {em_build._rate_array_sql("MIN", "minimum_rate", "numeric")},
               {em_build._rate_array_sql("MAX", "maximum_rate", "numeric")},
               {em_build._rate_array_sql("SUM", "rate_count", "bigint")}
          FROM em_legacy_stage GROUP BY npi ORDER BY npi"""
    await connection.execute(text(legacy_sql), {"projection_id": projection_id})


async def _em_output_semantics(connection, name):
    return tuple(
        (
            await connection.execute(
                text(f"""SELECT npi,code_mask,minimum_rates,maximum_rates,rate_counts
        FROM {_em_output_relation(name)} ORDER BY npi""")
            )
        ).mappings()
    )


def _em_semantic_digest(stored_rates):
    digest = hashlib.sha256()
    for rate_by_field in stored_rates:
        em_build.digest_row(digest, "em-rate", (int(rate_by_field["npi"]),), em_build._rate_fragment(rate_by_field))
    return digest.hexdigest()


async def _assert_em_output_parity(connection):
    actual_rates = await _em_output_semantics(connection, "em_actual_rates")
    legacy_rates = await _em_output_semantics(connection, "em_legacy_rates")
    assert actual_rates == legacy_rates and len(actual_rates) == 2
    assert [rate_by_field["npi"] for rate_by_field in actual_rates] == [101, 102]
    assert [rate_by_field["code_mask"] for rate_by_field in actual_rates] == [63, 63]
    assert actual_rates[0]["minimum_rates"] == [Decimal("5.125") + index for index in range(6)]
    assert actual_rates[0]["maximum_rates"] == [Decimal("20.5") + index for index in range(6)]
    assert actual_rates[0]["rate_counts"] == [26] * 6
    assert actual_rates[1]["minimum_rates"] == actual_rates[1]["maximum_rates"] == actual_rates[0]["minimum_rates"]
    assert actual_rates[1]["rate_counts"] == [12] * 6
    expected_fragment = (
        b'[63,["5.125","6.125","7.125","8.125","9.125","10.125"],'
        b'["20.5","21.5","22.5","23.5","24.5","25.5"],[26,26,26,26,26,26]]'
    )
    assert em_build._rate_fragment(actual_rates[0]) == expected_fragment
    assert _em_semantic_digest(actual_rates) == _em_semantic_digest(legacy_rates)


@pytest.mark.asyncio
async def test_native_em_binding_stage_preserves_v1_prices_and_bounds(migrated_v3_database, monkeypatch):
    async with migrated_v3_database.engine.begin() as connection:
        await projection._create_stage_tables(connection)
        await connection.execute(text(em_build._CREATE_RATE_STAGE_SQL))
        await _stage_em_inputs(connection)
        await _assert_em_native_bounds(connection, monkeypatch)
        for code_index in range(6):
            await _stage_em_code_pair(connection, code_index)
        await _assert_em_attributed_stage(connection)
        for name in ("em_actual_rates", "em_legacy_rates"):
            await _create_em_output(connection, name)
        await _store_em_output_pair(connection, "c" * 64)
        await _assert_em_output_parity(connection)


def _attribution_selection(digest="a"):
    from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding

    return PlanReleaseServingSelection(
        "hpserve_" + "1" * 26,
        "hprelease_" + "2" * 26,
        "hpplan_" + "3" * 26,
        None,
        "2026-01",
        "published",
        digest * 64,
        tuple(
            PlanReleaseSnapshotBinding(ordinal, "snapshot-" + source, source, "plan-a", "", "in_network", True)
            for ordinal, source in ((3, "source-a"), (17, "source-b"))
        ),
    )


@pytest.fixture
async def attribution_native_roles(migrated_v3_database):
    dsn = os.environ[POSTGRES_DSN_ENV]
    admin = await asyncpg.connect(dsn)
    suffix = uuid.uuid4().hex[:12]
    roles = SimpleNamespace(
        owner="em_owner_" + suffix, operator="em_operator_" + suffix, reader="em_reader_" + suffix, schemas=[]
    )
    password = uuid.uuid4().hex
    connections = []
    try:
        database = await admin.fetchval("SELECT quote_ident(current_database())")
        await admin.execute(f'CREATE ROLE "{roles.owner}" NOLOGIN')
        for name in (roles.operator, roles.reader):
            await admin.execute(
                f"CREATE ROLE \"{name}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            )
        await admin.execute(f'GRANT "{roles.owner}" TO "{roles.operator}" WITH INHERIT FALSE, SET TRUE')
        await admin.execute(f'GRANT CREATE ON DATABASE {database} TO "{roles.owner}","{roles.operator}"')
        for name in (roles.operator, roles.reader):
            connections.append(await asyncpg.connect(dsn, user=name, password=password))
        roles.publisher, roles.consumer = connections
        yield roles
    finally:
        for connection in connections:
            await connection.close()
        for schema in roles.schemas:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for name in (roles.operator, roles.reader, roles.owner):
            await admin.execute(f'REVOKE ALL ON DATABASE {database} FROM "{name}"')
        await admin.execute(f'REVOKE "{roles.owner}" FROM "{roles.operator}"')
        for name in (roles.operator, roles.reader, roles.owner):
            await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
        await admin.close()


async def _attribution_stage(connection, *, maximum=False, invalid=None):
    await connection.execute(em_build._CREATE_RATE_STAGE_SQL)
    staged_rates = []
    for index in range(6):
        for ordinal, npi, minimum, maximum_rate, count in (
            (3, 101, Decimal("10.25") + index, Decimal("20.5") + index, 2**63 - 2 if maximum else 14),
            (17, 101, Decimal("5.125") + index, Decimal("5.125") + index, 1 if maximum else 12),
            (17, 102, Decimal("5.125") + index, Decimal("5.125") + index, 12),
        ):
            staged_rates.append((ordinal, npi, index, minimum, maximum_rate, count))
    if invalid == "overflow":
        staged_rates[1] = (*staged_rates[1][:-1], 2)
    elif invalid == "ordinal":
        staged_rates[0] = (999, *staged_rates[0][1:])
    elif invalid == "numeric":
        staged_rates[0] = (*staged_rates[0][:3], Decimal("NaN"), Decimal("NaN"), 14)
    elif invalid == "count":
        staged_rates[0] = (*staged_rates[0][:-1], 0)
    await connection.executemany(
        "INSERT INTO plan_pricing_em_distance_rate_stage VALUES ($1,$2,$3,$4,$5,$6)",
        staged_rates,
    )


async def _attribution_publish(roles, selection, **stage_options):
    from api import plan_pricing_em_attribution as attribution

    identity = attribution._identity(selection)
    roles.schemas.append(identity[2])
    async with roles.publisher.transaction(isolation="repeatable_read"):
        await attribution._check_roles(roles.publisher, attribution._roles(roles.owner, (roles.reader,)))
        await _attribution_stage(roles.publisher, **stage_options)
        receipt = await attribution._publish(roles.publisher, selection, identity, roles.owner, (roles.reader,))
        assert await roles.publisher.fetchval("SELECT current_user") == roles.operator
        assert await attribution._owner_verify(roles.publisher, receipt, identity[0]) == receipt
        assert await roles.publisher.fetchval("SELECT current_user") == roles.operator
    return receipt


async def _attribution_reader_proof(roles, receipt, expected_count):
    from api import plan_pricing_em_attribution as attribution

    async with roles.consumer.transaction(isolation="repeatable_read", readonly=True):
        assert await roles.consumer.fetchval("SHOW transaction_read_only") == "on"
        assert await roles.consumer.fetchval("SHOW transaction_isolation") == "repeatable read"
        assert not await roles.consumer.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
        assert await attribution._verify(roles.consumer, receipt, receipt.identity_json) == receipt
        retained = await attribution._namespace(roles.consumer, receipt.schema_name)
        assert attribution._decode(retained["receipt"]) == receipt
        prices = await roles.consumer.fetch(
            f"SELECT code_index,min(minimum_rate) AS minimum,max(maximum_rate) AS maximum,sum(rate_count)::bigint AS count FROM {receipt.rates_relation} WHERE npi=101 GROUP BY code_index ORDER BY code_index"
        )
        assert len(prices) == 6
        assert [price["minimum"] for price in prices] == [Decimal("5.125") + index for index in range(6)]
        assert [price["maximum"] for price in prices] == [Decimal("20.5") + index for index in range(6)]
        assert [price["count"] for price in prices] == [expected_count] * 6
        assert await roles.consumer.fetchval(f"SELECT count(*) FROM {receipt.rates_relation}") == 18
    async with roles.consumer.transaction(isolation="repeatable_read"):
        for statement in (
            f"UPDATE {receipt.rates_relation} SET rate_count=1",
            f"TRUNCATE {receipt.rates_relation}",
            f'CREATE TABLE "{receipt.schema_name}".forbidden(n integer)',
            f'SET LOCAL ROLE "{roles.owner}"',
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with roles.consumer.transaction():
                    await roles.consumer.execute(statement)


async def _attribution_candidate_refusals(roles, retained):
    from api import plan_pricing_em_attribution as attribution

    for digest, invalid in (("c", "overflow"), ("d", "ordinal"), ("e", "numeric"), ("f", "count")):
        selection = _attribution_selection(digest)
        identity = attribution._identity(selection)
        roles.schemas.append(identity[2])
        async with roles.publisher.transaction(isolation="repeatable_read"):
            with pytest.raises((ValueError, asyncpg.CheckViolationError)):
                async with roles.publisher.transaction():
                    await _attribution_stage(roles.publisher, maximum=True, invalid=invalid)
                    await attribution._publish(roles.publisher, selection, identity, roles.owner, (roles.reader,))
            assert await roles.publisher.fetchval("SELECT current_user") == roles.operator
            assert await attribution._namespace(roles.publisher, identity[2]) is None
            assert await attribution._owner_verify(roles.publisher, retained, retained.identity_json) == retained
        await _attribution_reader_proof(roles, retained, 26)


async def _assert_attribution_constraint_refusal(connection, statement):
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(statement)
    finally:
        await savepoint.rollback()


async def _attribution_table_constraint_refusals(roles, receipt):
    from api import plan_pricing_em_attribution as attribution

    async with roles.publisher.transaction(isolation="repeatable_read"):
        await roles.publisher.execute(f'SET LOCAL ROLE "{roles.owner}"')
        for assignment in (
            "rate_count=0",
            "minimum_rate='NaN',maximum_rate='NaN'",
            "code_index=6",
            "binding_ordinal=-1",
            "npi=0",
        ):
            await _assert_attribution_constraint_refusal(
                roles.publisher, f"UPDATE {receipt.rates_relation} SET {assignment}"
            )
        await roles.publisher.execute(f'SET LOCAL ROLE "{roles.operator}"')
        assert await attribution._owner_verify(roles.publisher, receipt, receipt.identity_json) == receipt


async def _attribution_custody_refusals(roles, receipt):
    from dataclasses import replace

    from api import plan_pricing_em_attribution as attribution

    async with roles.publisher.transaction(isolation="repeatable_read"):
        for statement in (
            f'GRANT UPDATE ON {receipt.rates_relation} TO "{roles.reader}"',
            f'ALTER INDEX "{receipt.schema_name}".rates_lookup RENAME TO changed_lookup',
            f'REINDEX INDEX "{receipt.schema_name}".rates_lookup',
            f"ALTER TABLE {receipt.rates_relation} DROP CONSTRAINT rates_count_ck",
            f"COMMENT ON SCHEMA \"{receipt.schema_name}\" IS '{{}}'",
        ):
            savepoint = roles.publisher.transaction()
            await savepoint.start()
            try:
                await roles.publisher.execute(f'SET LOCAL ROLE "{roles.owner}"')
                await roles.publisher.execute(statement)
                await roles.publisher.execute(f'SET LOCAL ROLE "{roles.operator}"')
                with pytest.raises(ValueError):
                    await attribution._owner_verify(roles.publisher, receipt, receipt.identity_json)
                assert await roles.publisher.fetchval("SELECT current_user") == roles.operator
            finally:
                await savepoint.rollback()
            assert await attribution._owner_verify(roles.publisher, receipt, receipt.identity_json) == receipt
        changed = replace(receipt, table_filenode=receipt.table_filenode + 1)
        changed = replace(changed, receipt_sha256=attribution._receipt_digest(changed))
        with pytest.raises(ValueError):
            await attribution._owner_verify(roles.publisher, changed, changed.identity_json)
        with pytest.raises(ValueError, match="full recipe"):
            await attribution._owner_verify(
                roles.publisher, receipt, attribution._identity(_attribution_selection("b"))[0]
            )


@pytest.mark.asyncio
async def test_native_em_attribution_publication_custody_replay_and_bigint_bounds(attribution_native_roles):
    from api import plan_pricing_em_attribution as attribution

    roles = attribution_native_roles
    receipt = await _attribution_publish(roles, _attribution_selection())
    assert receipt.row_count == 18 and receipt.provider_count == 2 and receipt.binding_ordinals == (3, 17)
    assert receipt.code_accounting == tuple((index, 3, "38") for index in range(6))
    await _attribution_reader_proof(roles, receipt, 26)
    boundary = await _attribution_publish(roles, _attribution_selection("b"), maximum=True)
    await _attribution_reader_proof(roles, boundary, 2**63 - 1)
    await _attribution_candidate_refusals(roles, receipt)
    await _attribution_table_constraint_refusals(roles, receipt)
    await _attribution_custody_refusals(roles, receipt)
    await _attribution_reader_proof(roles, receipt, 26)
    async with roles.consumer.transaction(isolation="repeatable_read", readonly=True):
        assert await attribution._content(roles.consumer, receipt.schema_name, "rates") == (18, receipt.content_sha256)


@pytest.mark.asyncio
async def test_native_managed_em_read_retained_offices_and_attribution(migrated_v3_database, monkeypatch):
    from tests.managed_em_read_postgres_support import test_managed_em_native_read

    await test_managed_em_native_read(migrated_v3_database, monkeypatch)
