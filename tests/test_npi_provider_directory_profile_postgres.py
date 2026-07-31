# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from datetime import datetime

import asyncpg
import pytest

from api.endpoint import npi as npi_module


PROFILE_SERVING_POSTGRES_FIXTURE_SQL = """
    CREATE TEMP TABLE pd_profile (
        npi bigint PRIMARY KEY,
        profile_json jsonb NOT NULL,
        evidence_json jsonb NOT NULL,
        generation_id text NOT NULL,
        published_at timestamp NOT NULL
    ) ON COMMIT DROP;
    CREATE TEMP TABLE pd_evidence (
        evidence_key text PRIMARY KEY
    ) ON COMMIT DROP;
    CREATE TEMP TABLE pd_serving_generation (
        singleton_key text PRIMARY KEY,
        generation_id text,
        published_at timestamp,
        profile_as_of varchar(10),
        status text,
        operation text,
        control_generation bigint,
        profile_target_oid bigint,
        evidence_target_oid bigint
    ) ON COMMIT DROP;
"""


def _profile_serving_asyncpg_query():
    query_template = (
        npi_module.PROVIDER_DIRECTORY_PROFILE_SERVING_QUERY_TEMPLATE
    )
    return query_template.format(
        serving_generation_ref="pg_temp.pd_serving_generation",
        profile_table_ref="pg_temp.pd_profile",
        evidence_select="",
    ).replace(
        ":npis", "$1"
    ).replace(
        ":profile_table_ref", "$2"
    ).replace(
        ":evidence_table_ref", "$3"
    )


async def _create_profile_serving_postgres_fixture(connection):
    await connection.execute(PROFILE_SERVING_POSTGRES_FIXTURE_SQL)
    await connection.execute(
        """
        INSERT INTO pd_profile
        VALUES ($1, $2::jsonb, $3::jsonb, $4, $5);
        """,
        1588616783,
        "{}",
        "{}",
        "pdprofile_11111111111111111111111111111111",
        datetime(2026, 7, 13, 20, 0),
    )
    profile_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        "pg_temp.pd_profile",
    )
    evidence_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        "pg_temp.pd_evidence",
    )
    return profile_oid, evidence_oid


@pytest.mark.asyncio
async def test_profile_serving_query_fences_transition_in_postgresql():
    """Prove fallback, adoption, and OID mismatch against PostgreSQL tableoid."""
    database_dsn = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"
    )
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to run serving truth table")
    connection = await asyncpg.connect(database_dsn)
    transaction = connection.transaction()
    await transaction.start()
    try:
        profile_oid, evidence_oid = (
            await _create_profile_serving_postgres_fixture(connection)
        )
        query = _profile_serving_asyncpg_query()
        query_args = (
            [1588616783],
            "pg_temp.pd_profile",
            "pg_temp.pd_evidence",
        )
        fallback_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            """
            INSERT INTO pd_serving_generation
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
            """,
            "global",
            "pdprofile_11111111111111111111111111111111",
            datetime(2026, 7, 30, 15, 0),
            "2026-07-29",
            "published",
            "publish",
            6,
            profile_oid,
            evidence_oid,
        )
        adopted_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            "UPDATE pd_serving_generation SET profile_as_of = NULL"
        )
        missing_as_of_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            "UPDATE pd_serving_generation "
            "SET profile_as_of = '2026-07-29', "
            "profile_target_oid = profile_target_oid + 1"
        )
        mismatch_rows = await connection.fetch(query, *query_args)
    finally:
        await transaction.rollback()
        await connection.close()

    assert fallback_rows[0]["published_at"] == datetime(2026, 7, 13, 20, 0)
    assert adopted_rows[0]["published_at"] == datetime(2026, 7, 30, 15, 0)
    assert adopted_rows[0]["serving_generation_key"] == "global"
    assert adopted_rows[0]["profile_as_of"] == "2026-07-29"
    assert missing_as_of_rows == []
    assert mismatch_rows == []
