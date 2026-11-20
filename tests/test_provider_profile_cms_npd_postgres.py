# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider profile reads over a synthetic typed CMS NPD release in real PostgreSQL."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import provider_profile_cms_npd as cms_npd
from api.cms_npd_typed_queries import current_cms_release
from tests.cms_npd_typed_postgres_support import NPI, ROLE_1, typed_cms_database

ENDPOINT = "00000004-0000-4000-8000-000000000001"
_EXTRA = (
    f"""INSERT INTO endpoint (id, connection_type, address, name, status) VALUES
    ('{ENDPOINT}', 'hl7-fhir-rest', 'https://example.test/fhir', 'Example FHIR', 'active')""",
    f"""UPDATE practitioner_role SET specialty_codes = ARRAY['207R00000X'], new_patients = 'newpt',
        endpoint_ids = ARRAY['{ENDPOINT}']::uuid[],
        insurance_plan_ids = ARRAY['00000002-0000-4000-8000-000000000001', '00000002-0000-4000-8000-000000000002']::uuid[]
     WHERE id = '{ROLE_1}'""",
    "UPDATE location SET phones = ARRAY['555-0100'], line1 = '1 Example Way', city = 'Exampletown' "
    "WHERE name = 'Example Clinic'",
    """INSERT INTO practitioner_role (id, practitioner_npi, organization_id, active, specialty_codes, period_end) VALUES
    ('00000003-0000-4000-8000-0000000000a1', 1234567893, 'net-nested', false, ARRAY['inactive-code'], NULL),
    ('00000003-0000-4000-8000-0000000000a2', 1234567893, 'net-nested', true, ARRAY['ended-code'], '2020-01-01')""",
)


async def _seed(sessions):
    async with sessions() as session, session.begin():
        release = await current_cms_release(session, None)
        await session.execute(text(f"SET LOCAL search_path TO {release['schema']}"))
        for statement in _EXTRA:
            await session.execute(text(statement))
        await session.execute(text("ANALYZE practitioner_role"))
    return release


@pytest.mark.asyncio
async def test_profile_read(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        await _seed(sessions)
        async with sessions() as session, session.begin():
            release, rows = await cms_npd.read_cms_npd_roles(session, NPI)
        projection = cms_npd.cms_npd_projection(NPI, release, rows)
        displays_by_category = {
            category: sorted(item["display"] for item in group["items"])
            for category, group in projection["categories"].items()
        }
        assert displays_by_category == {
            "organizations": ["Example Health Organization"],
            "specialties": ["207R00000X"],
            "accepting_patients": ["Accepting new patients — Example Health Organization"],
            "contact": ["Example FHIR — https://example.test/fhir", "Phone: 555-0100 (work)"],
            # The draft plan, the missing plan and the missing site are not listed.
            "network_participation": ["Listed network: Example Network", "Listed plan: Example Plan"],
        }
        assert [record["resource_id"] for record in projection["evidence"]["records"]] == [str(ROLE_1)]


@pytest.mark.asyncio
async def test_npi_index_plan(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        release = await _seed(sessions)
        statement = cms_npd._ROLE_SQL.format(s=release["schema"], n=cms_npd.LIST_LIMIT)
        async with sessions() as session, session.begin():
            # The synthetic tables are tiny; disable sequential scans to see the plan a large release gets.
            await session.execute(text("SET LOCAL enable_seqscan = off"))
            plan = "\n".join(
                (await session.execute(text("EXPLAIN " + statement), {"npi": NPI, "role_limit": 100})).scalars()
            )
        assert "practitioner_role_practitioner_npi_idx" in plan
        assert "Seq Scan" not in plan


@pytest.mark.asyncio
async def test_dropped_release(monkeypatch):
    async with typed_cms_database(monkeypatch) as sessions:
        release = await _seed(sessions)
        async with sessions() as session, session.begin():
            await session.execute(text(f"DROP SCHEMA {release['schema']} CASCADE"))

        async with sessions() as session, session.begin():

            @asynccontextmanager
            async def transaction():
                async with session.begin_nested():
                    yield session

            monkeypatch.setattr(cms_npd, "db", SimpleNamespace(transaction=transaction))
            assert await cms_npd.fetch_cms_npd_projection(NPI) is None
            # The savepoint kept the surrounding snapshot usable.
            assert await session.scalar(text("SELECT 1")) == 1
