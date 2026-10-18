# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary Reader resource closure from actual retained CMS publication."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import provider_profile_cms_resources as resource_reader
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_profile_cms_resources import read_cms_profile_resources
from api.provider_profile_network_facts import pin_cms_profile_scope
from api.provider_profile_scoped_resources import project_scoped_profile_resources
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import published_profile_source as published_profile_source

pytestmark = pytest.mark.asyncio
NPI = "1000000491"


async def _read(fixture, manifest, **options):
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles["reader"]}"'))
        assert await session.scalar(text("SELECT current_user")) == fixture.roles["reader"]
        assert not await session.scalar(
            text("SELECT pg_has_role(current_user,:owner,'SET')"), {"owner": fixture.roles["owner"]}
        )
        scope = await pin_cms_profile_scope(
            session, manifest, NPI, excluded_network_ids=options.pop("excluded_network_ids", ()), **options
        )
        return await read_cms_profile_resources(session, scope, NPI)


async def test_native_direct_plan_and_exact_office_resources(published_profile_source):
    _, fixture, _, manifest, _, sites = published_profile_source
    result = await _read(fixture, manifest)
    assert result["associations"]
    assert {a["plan_id"] for a in result["associations"]} == {None, "plan-1"}
    assert {a["location_resource_id"] for a in result["associations"]} == {"site-1"}
    projected = project_scoped_profile_resources(
        result["resources"],
        NPI,
        result["associations"],
        excluded_network_resource_ids=result["excluded_network_resource_ids"],
    )
    assert projected["independent_resources"]
    assert projected["bundles"]
    assert not (await _read(fixture, manifest, location_id=str(sites[1])))["associations"]


async def test_native_default_exclusion_retains_independent_practitioner(published_profile_source, monkeypatch):
    _, fixture, _, manifest, network, _ = published_profile_source
    result = await _read(fixture, manifest, excluded_network_ids=(network,))
    assert result["associations"] == []
    assert result["excluded_network_resource_ids"] == ["network-1"]
    assert result["resources"] and {row["resource_type"] for row in result["resources"]} <= {
        "Practitioner",
        "PractitionerRole",
    }
    assert any(row["resource_type"] == "Practitioner" for row in result["resources"])

    for bound in ("MAX_ROWS", "MAX_BYTES"):
        with monkeypatch.context() as stricter:
            stricter.setattr(resource_reader, bound, 0)
            with pytest.raises(DirectoryReadError):
                await _read(fixture, manifest)
