# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy from two genuinely acquired, reviewed and retained CMS networks."""

import json
import secrets
from contextlib import aclosing
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.provider_profile_cms_resources import read_cms_profile_resources
from api.provider_profile_network_contract import parse_managed_profile_query
from api.provider_profile_network_facts import pin_cms_profile_scope
from api.provider_profile_network_read import _directory_profile
from db.models import system
from process.network_approved_membership_source import pin_approved_membership_source
from tests import cms_npd_admission_postgres_support as support
from tests import test_network_cms_registry_source_pair_postgres as pairs
from tests.cms_registry_recipe_postgres_support import _reviewed_cms_binding
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import (
    _compose_profile_manifest,
    _grant_profile_reader,
    _publish_profile_recipe,
    _remove_profile_pairs,
)
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_candidate_composition_postgres import _remove_candidates

pytestmark = pytest.mark.asyncio
NPI = "1000000491"
HIDDEN_NAME = "Hidden Network B"
EMPLOYER_NAME = "Unrelated Employer"


def _raw_privacy_resources(original, revision):
    """Declare all references before file hashes, admission and source sealing."""
    resources_by_type = original(revision)
    resources_by_type["Organization"][0].update(name="Permitted Network A", type=[{"text": "ntwk"}])
    resources_by_type["Organization"][1]["name"] = EMPLOYER_NAME
    resources_by_type["Organization"].append({"id": "network-2", "name": HIDDEN_NAME, "type": [{"text": "ntwk"}]})
    resources_by_type["Practitioner"][0]["identifier"] = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": NPI}]
    resources_by_type["Location"][0]["name"] = "Permitted Office A"
    resources_by_type["Location"].append(
        {"id": "site-2", "name": "Hidden Office B", "address": {"line": ["2 Sample Street"]}}
    )
    resources_by_type["InsurancePlan"][0].update(
        name="Permitted Plan A", network=[{"reference": "Organization/network-1"}]
    )
    resources_by_type["Endpoint"][0]["managingOrganization"] = {"reference": "Organization/network-2"}
    resources_by_type["Endpoint"].append(
        {
            "id": "employer-endpoint",
            "status": "active",
            "address": "https://employer.example.test/fhir",
            "managingOrganization": {"reference": "Organization/insurer-1"},
        }
    )
    resources_by_type["HealthcareService"][0]["providedBy"] = {"reference": "Organization/insurer-1"}
    role = resources_by_type["PractitionerRole"][0]
    role.update(
        organization={"reference": "Organization/network-2"},
        network=[{"reference": "Organization/network-1"}],
        insurancePlan=[{"reference": "InsurancePlan/plan-1"}],
        location=[{"reference": "Location/site-1"}],
        endpoint=[{"reference": "Endpoint/endpoint-1"}],
        healthcareService=[{"reference": "HealthcareService/service-1"}],
    )
    resources_by_type["PractitionerRole"].append(
        {
            **role,
            "id": "employer-role",
            "organization": {"reference": "Organization/insurer-1"},
            "endpoint": [{"reference": "Endpoint/employer-endpoint"}],
        }
    )
    resources_by_type["PractitionerRole"].append(
        {
            **role,
            "id": "hidden-role",
            "network": [{"reference": "Organization/network-2"}],
            "insurancePlan": [],
            "location": [{"reference": "Location/site-2"}],
        }
    )
    resources_by_type["OrganizationAffiliation"][0]["active"] = False
    return resources_by_type


async def _review_both_networks(fixture, pair):
    pin, coordinates = pair.recipe.source_pin, pair.recipe.binding_coordinates
    actor, networks, binding_a, _ = await _reviewed_cms_binding(
        fixture.connection, fixture.control_schema, fixture.engine, pin, coordinates
    )
    legacy_b = await fixture.connection.fetchval(
        "SELECT network_id FROM mrf.provider_directory_insurance_network_source_binding WHERE source_id='cms-npd' AND resource_type='Organization' AND resource_id='network-2'"
    )
    assert legacy_b is not None
    binding_b_by_field = {
        **binding_a,
        "binding_id": str(uuid4()),
        "source_key": "network-2",
        "source_scope_json": {
            "organization_id": "network-2",
            "legacy_uuid": str(legacy_b),
            "alias_scope": pin.alias_scope,
        },
        "network_id": networks[1]["record_id"],
        "evidence_id": "reviewed-network-b",
    }
    record_b = (await _reviewed_binding(fixture.connection, fixture.control_schema, actor, [binding_b_by_field]))[
        "records"
    ][0]
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, record_b),
        actor,
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=approval["approved_revision"], control_schema=fixture.control_schema
        )
    sites = [
        await fixture.connection.fetchval(
            f"SELECT site_id FROM \"{pin.read_schema_name}\".provider_directory_entity_source_binding WHERE resource_type='Location' AND resource_id=$1",
            name,
        )
        for name in ("site-1", "site-2")
    ]
    assert all(sites) and sites[0] != sites[1]
    return ((fixture.connection, fixture.control_schema, pin, sites), actor, networks, binding_a, coordinates, approved)


@pytest.fixture
async def privacy_publication(monkeypatch, tmp_path):
    original = support._resource_rows
    monkeypatch.setattr(support, "_resource_rows", lambda revision: _raw_privacy_resources(original, revision))
    directory, acquired = support.retained_release(tmp_path)
    capture_id, copy_targets = uuid4(), []
    async with support.protected_admission_database(monkeypatch) as database:
        with support.release_probe_client(directory) as client:
            initial = await support.run_protected_admission(
                database, monkeypatch, "privacy-admission", directory, acquired, client
            )
        proof = await pairs._publish_pair(database, initial, monkeypatch)
        monkeypatch.setenv(
            "NETWORK_REGISTRY_TEST_DSN",
            database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        )
        async with aclosing(serving_schema.__wrapped__()) as registry:
            async with aclosing(custom_db.__wrapped__(await anext(registry))) as custom:
                fixture = await anext(custom)
                for extension in ("intarray", "btree_gin", "postgis"):
                    await fixture.connection.execute(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
                try:
                    pair = await pairs._capture_bounded_pair(
                        database,
                        proof,
                        capture_id,
                        (fixture.roles["owner"], fixture.roles["reader"], database._source_component.owner),
                    )
                    reviewed = await _review_both_networks(fixture, pair)
                    manifest = await _publish_profile_recipe(fixture, reviewed, copy_targets)
                    manifest = await _compose_profile_manifest(fixture, manifest, reviewed[-1], copy_targets)
                    await _grant_profile_reader(fixture)
                    yield fixture, manifest, reviewed[2], pair
                finally:
                    await _remove_candidates(fixture, copy_targets, uuid4())
                    await _remove_profile_pairs(fixture, capture_id)


@pytest.fixture
async def privacy_reader(privacy_publication):
    fixture, _, _, _ = privacy_publication
    role, password = fixture.roles["reader"], secrets.token_urlsafe(24)
    engine = None
    try:
        await fixture.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        engine = create_async_engine(
            fixture.engine.url.set(username=role, password=password), pool_size=1, max_overflow=0
        )
        yield engine
    finally:
        if engine is not None:
            await engine.dispose()
        await fixture.connection.execute(f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')


@pytest.fixture
async def profile_type_schema(privacy_publication):
    fixture, _, _, _ = privacy_publication
    schema = "profile_privacy_" + uuid4().hex
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
        yield schema
    finally:
        await fixture.connection.execute(f'DROP SCHEMA "{schema}" CASCADE')


async def _read_privacy_profiles(fixture, engine, manifest, networks, schema):
    _, query = parse_managed_profile_query({"npi": NPI, "include_evidence": "true"})
    profiles_by_case = {}
    async with async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        role = fixture.roles["reader"]
        assert (await session.execute(text("SELECT session_user,current_user"))).one() == (role, role)
        assert not await session.scalar(text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user"))
        assert not await session.scalar(
            text("SELECT pg_has_role(current_user,:owner,'SET')"), {"owner": fixture.roles["owner"]}
        )
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        for name, exclusions in (
            ("visible", ()),
            ("excluded", (networks[1]["record_id"],)),
            ("all_excluded", tuple(sorted(network["record_id"] for network in networks))),
        ):
            scope = await pin_cms_profile_scope(session, manifest, NPI, excluded_network_ids=exclusions)
            inputs = await read_cms_profile_resources(session, scope, NPI)
            if name == "excluded":
                assert inputs["excluded_network_resource_ids"] == ["network-2"]
                assert {association["network_resource_id"] for association in inputs["associations"]} == {"network-1"}
                assert {association["location_resource_id"] for association in inputs["associations"]} == {"site-1"}
            profiles_by_case[name] = await _directory_profile(session, driver, manifest, NPI, query, exclusions, schema)
    return profiles_by_case


async def test_native_hidden_same_source_network_dependencies_do_not_leak(
    privacy_publication, privacy_reader, profile_type_schema, tmp_path
):
    fixture, manifest, networks, _ = privacy_publication
    profiles_by_case = await _read_privacy_profiles(fixture, privacy_reader, manifest, networks, profile_type_schema)
    (tmp_path / "privacy-result.json").write_text(json.dumps(profiles_by_case, default=str))
    visible_profile = profiles_by_case["visible"]
    permitted_profile = profiles_by_case["excluded"]
    independent_profile = profiles_by_case["all_excluded"]
    assert visible_profile and permitted_profile and independent_profile
    visible_json, permitted_json = json.dumps(visible_profile), json.dumps(permitted_profile)
    assert HIDDEN_NAME in visible_json and "network-2" in visible_json
    assert HIDDEN_NAME not in permitted_json and "network-2" not in permitted_json
    assert "Hidden Office B" not in permitted_json and "hidden-role" not in permitted_json
    for expected in ("Organization/network-1", "Permitted Plan A", "site-1", EMPLOYER_NAME):
        assert expected in permitted_json
    for category in ("qualification", "qualification_detail", "years_of_practice"):
        assert independent_profile["profile"]["facts"][category] == permitted_profile["profile"]["facts"][category]
