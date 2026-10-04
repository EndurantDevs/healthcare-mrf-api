# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic dependency tables in a guarded, disposable PostgreSQL database."""

import os
import re
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.provider_directory_entities_contract import CURSOR_KEY_ENV

_DSN_ENV = "HLTHPRT_DIRECTORY_ENTITIES_TEST_DSN"
GROUP_A = "00000000-0000-0000-0000-000000000001"
GROUP_B = "00000000-0000-0000-0000-000000000002"
SITE_A = "00000000-0000-0000-0000-000000001001"
SITE_B = "00000000-0000-0000-0000-000000001002"
SITE_C = "00000000-0000-0000-0000-000000001003"
SITE_D = "00000000-0000-0000-0000-000000001004"


def _database_url():
    raw = os.getenv(_DSN_ENV)
    if not raw:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw)
    if (
        url.host not in {"localhost", "127.0.0.1"}
        or url.port is None
        or not 1 <= url.port <= 65535
        or not url.drivername.startswith("postgresql")
        or not re.fullmatch(r"hc_directory_entities_[0-9a-f]{32}", url.database or "")
    ):
        pytest.fail("A UUID-owned local PostgreSQL test database is required")
    return url.set(drivername="postgresql+asyncpg")


async def _create_tables(session):
    statements = [
        "CREATE TABLE doctor_clinician_address (id bigint)",
        "CREATE TABLE cms_doctor_education (id bigint)",
        "CREATE TABLE cms_doctor_group_site (row_number bigint PRIMARY KEY, npi bigint NOT NULL, org_pac_id varchar(64), "
        "adrs_id text, facility_name text, generation_id varchar(64) NOT NULL, observed_at timestamp NOT NULL)",
        "CREATE INDEX ON cms_doctor_group_site (org_pac_id)",
        "CREATE TABLE provider_directory_cms_doctors_group_binding (org_pac_id varchar(64) PRIMARY KEY, "
        "organization_id uuid NOT NULL UNIQUE)",
        "CREATE TABLE provider_directory_cms_doctors_site_binding (adrs_id varchar(256) PRIMARY KEY, "
        "site_id uuid NOT NULL UNIQUE)",
        "CREATE TABLE reference_family_result_generation (importer_id text PRIMARY KEY, local_lineage_id uuid, "
        "local_generation bigint, origin_lineage_id uuid, origin_generation bigint, "
        "published_at timestamptz, relation_oids bigint[])",
    ]
    for statement in statements:
        await session.execute(text(statement))
    await session.execute(
        text("""
        INSERT INTO reference_family_result_generation VALUES
        ('cms-doctors', :lineage, 1, :lineage, 1, now(),
         ARRAY['doctor_clinician_address'::regclass::oid::bigint,
               'cms_doctor_education'::regclass::oid::bigint,
               'cms_doctor_group_site'::regclass::oid::bigint])
    """),
        {"lineage": uuid4()},
    )


async def _seed_groups(session):
    await session.execute(
        text("""
        INSERT INTO provider_directory_cms_doctors_site_binding VALUES
        ('synthetic-address-alpha', :first), ('synthetic-address-beta', :second),
        ('synthetic-address-gamma', :third), ('synthetic-address-delta', :fourth)
    """),
        {"first": SITE_A, "second": SITE_B, "third": SITE_C, "fourth": SITE_D},
    )
    await session.execute(
        text("""
        INSERT INTO provider_directory_cms_doctors_group_binding VALUES
        ('synthetic-pac-alpha', :first), ('synthetic-pac-beta', :second),
        ('synthetic-unpublished-pac', '00000000-0000-0000-0000-000000000003')
    """),
        {"first": GROUP_A, "second": GROUP_B},
    )
    await session.execute(
        text("""
        INSERT INTO cms_doctor_group_site VALUES
        (1, 1234567893, 'synthetic-pac-alpha', 'synthetic-address-alpha', 'Example Group', 'synthetic-release', '2026-01-01'),
        (2, 1234567893, 'synthetic-pac-alpha', 'synthetic-address-beta', 'Another Group Name', 'synthetic-release', '2026-01-01'),
        (3, 1000000004, 'synthetic-pac-beta', 'synthetic-address-gamma', 'Second Group', 'synthetic-release', '2026-01-01'),
        (4, 1234567893, 'synthetic-pac-alpha', NULL, 'Example Group', 'synthetic-release', '2026-01-01'),
        (5, 1234567893, 'synthetic-pac-alpha', 'synthetic-address-delta', 'Example Group', 'synthetic-release', '2026-01-01')
    """)
    )


@asynccontextmanager
async def directory_database(monkeypatch):
    """Own one synthetic schema; require the caller to own and remove its database."""
    schema = "directory_" + uuid4().hex
    engine = create_async_engine(_database_url(), connect_args={"server_settings": {"search_path": schema}})
    sessions = async_sessionmaker(engine)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setenv(CURSOR_KEY_ENV, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    try:
        async with sessions() as session, session.begin():
            await session.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _create_tables(session)
            await _seed_groups(session)
        yield sessions
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
