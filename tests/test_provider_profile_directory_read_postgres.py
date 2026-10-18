# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed profile composition from real retained CMS evidence under Reader."""

import json
import secrets
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.network_address_scope import network_address_read_scope
from api.provider_profile_network_contract import parse_managed_profile_query
from api.provider_profile_network_read import _directory_profile, read_address_scoped_directory_profile
from db.models import system
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import published_profile_source as published_profile_source

pytestmark = pytest.mark.asyncio
NPI = "1000000491"


@pytest.fixture
async def reader_engine(published_profile_source):
    """Authenticate as the exact disposable Reader, with no administrator pool reuse."""
    _, fixture, _, _, _, _ = published_profile_source
    role = fixture.roles["reader"]
    password = secrets.token_urlsafe(24)
    engine = None
    try:
        await fixture.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        engine = create_async_engine(
            fixture.engine.url.set(username=role, password=password),
            pool_size=1,
            max_overflow=0,
        )
        yield engine
    finally:
        if engine is not None:
            await engine.dispose()
        await fixture.connection.execute(f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')


async def _read(fixture, engine, manifest, exclusions, schema, *, selected=None):
    _, query = parse_managed_profile_query({"npi": NPI, "include_evidence": "true"})
    async with async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        identity = (await session.execute(text("SELECT session_user,current_user"))).one()
        assert identity == (fixture.roles["reader"], fixture.roles["reader"])
        assert not await session.scalar(text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user"))
        assert not await session.scalar(
            text("SELECT pg_has_role(current_user,:owner,'SET')"), {"owner": fixture.roles["owner"]}
        )
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        if selected is not None:
            with network_address_read_scope(manifest, selected):
                return await read_address_scoped_directory_profile(session, int(NPI), include_evidence=True)
        return await _directory_profile(session, driver, manifest, NPI, query, exclusions, schema)


async def test_native_directory_profile_uses_exact_office_and_preserves_independent_facts(
    published_profile_source, reader_engine, tmp_path, monkeypatch
):
    _, fixture, _, manifest, network, _ = published_profile_source
    schema = "profile_types_" + uuid4().hex
    await fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
    try:
        metadata = MetaData(schema=schema)
        for name in (
            "ProviderDirectoryPractitioner",
            "ProviderDirectoryPractitionerRole",
            "ProviderDirectoryOrganization",
            "ProviderDirectoryOrganizationAffiliation",
            "ProviderDirectoryHealthcareService",
            "ProviderDirectoryEndpoint",
        ):
            getattr(system, name).__table__.to_metadata(metadata, schema=schema)
        async with fixture.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.roles["reader"]}"')
        assert not await fixture.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')",
            fixture.roles["reader"],
            schema + ".provider_directory_practitioner_role",
        )
        visible = await _read(fixture, reader_engine, manifest, (), schema)
        denied = await _read(fixture, reader_engine, manifest, (network,), schema)
        monkeypatch.setenv("DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
        selected = await _read(fixture, reader_engine, manifest, (), schema, selected=(network,))
        unselected = await _read(fixture, reader_engine, manifest, (), schema, selected=(network + 1,))
    finally:
        await fixture.connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
    (tmp_path / "profile-result.json").write_text(json.dumps({"visible": visible, "denied": denied}, default=str))
    assert visible and denied
    assert selected == visible and unselected == denied
    visible_json = json.dumps(visible)
    denied_json = json.dumps(denied)
    assert "site-1" in visible_json and "site-2" not in visible_json
    assert "plan-1" in visible_json and "plan-1" not in denied_json
    assert "role-1" not in denied_json
    assert set(denied["profile"]["facts"]) == {"qualification", "qualification_detail", "years_of_practice"}
    for category in denied["profile"]["facts"]:
        assert denied["profile"]["facts"][category] == visible["profile"]["facts"][category]
