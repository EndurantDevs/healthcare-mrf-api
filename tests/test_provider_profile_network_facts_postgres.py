# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Profile facts from real committed CMS custody and a published canonical head."""

import hashlib
import json
from contextlib import aclosing
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.provider_profile_network_facts import read_cms_profile_plan_inputs
from process import provider_directory_cms_npd as cms
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_fhir_membership_source import read_fhir_membership_batch
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_record_store import RegistryRecordCommand
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests import cms_npd_admission_postgres_support as support
from tests import test_network_cms_registry_source_pair_postgres as pairs
from tests.cms_registry_recipe_postgres_support import _reviewed_cms_binding
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_registry_candidate_composition_postgres import (
    _approve_composition_intent,
    _approve_composition_source_offices,
    _remove_candidates,
    _roles,
)
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


async def _publish_profile_recipe(fixture, reviewed_source, copy_targets, *, public_contacts=False):
    """Retain the actual reviewed office and its raw recipe in a serving manifest."""
    source_fixture, _, _, _, coordinates, approved = reviewed_source
    connection, schema, source_pin, sites = source_fixture
    source_table = await _profile_addresses(fixture, public_contacts=public_contacts)
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    copy_targets.append(copy_target)
    async with connection.transaction(isolation="repeatable_read"):
        batch = await read_fhir_membership_batch(
            connection,
            source_pin,
            registry_schema=schema,
            approved_source=approved,
            binding_coordinates=coordinates,
            approved_only=True,
        )
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={
                "unified_address": fixture.base.generation_id,
                "fhir": batch.generation_id,
                "custom_membership": approved.generation_id,
            },
            source_recipes=(RegistrySourceMembershipRecipe(source_pin, coordinates),),
            approved_custom_revision=approved.approved_revision,
            expected_head=0,
            expected_rows=batch.membership_rows,
            control_schema=schema,
        )
        await connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))''')
        for index, site in enumerate(sites):
            await connection.execute(
                f'''INSERT INTO "{copy_target.schema_name}".provider_location_binding
                SELECT 'npi',entity_id,$1,location_key,entity_type,entity_id FROM {source_table}
                ORDER BY location_key LIMIT 1 OFFSET $2''',
                site,
                index,
            )
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=batch.input_bytes,
            expected_input_sha256=hashlib.sha256(batch.input_bytes).hexdigest(),
            control_schema=schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=schema)
    await prepare_and_publish_network_candidate(
        connection, copy_target, fixture.base, **_roles(fixture), control_schema=schema
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(connection, control_schema=schema)


async def _profile_addresses(fixture, *, public_contacts=False):
    """Use two complete native addresses for the source's two distinct offices."""
    connection = fixture.connection
    _, source_table = await _source_addresses(fixture, 2, "npi")
    await connection.execute(f"UPDATE {source_table} SET entity_id='1000000491',npi=1000000491")
    await connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=1000000491')
    if public_contacts:
        await _public_address_contacts(connection, source_table)
    return source_table


async def _public_address_contacts(connection, source_table):
    """Prepare native identity/precision before publication; mask-zero base facts stay public."""
    import ptg2_address_canon

    fields = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
    addresses = await connection.fetch(f"SELECT * FROM {source_table} ORDER BY location_key")
    assert all(address["row_origin"] == "base" and address["address_source_mask"] == 0 for address in addresses)
    canonical = ptg2_address_canon.canonicalize_batch(
        [tuple(address[field] for field in fields) for address in addresses]
    )
    rows = [
        {
            "location_key": address["location_key"],
            "address_key": proof["address_key"],
            "premise_key": proof["premise_key"],
            "precision": proof["identity_key"].rsplit("|", 1)[-1],
        }
        for address, proof in zip(addresses, canonical, strict=True)
    ]
    await connection.execute(
        f"""UPDATE {source_table} address SET address_key=prepared.address_key,premise_key=prepared.premise_key,
          address_precision=prepared.precision,telephone_number='555-0100',phone_number='5550100'
        FROM jsonb_to_recordset($1::jsonb) AS prepared(location_key text,address_key uuid,premise_key uuid,precision text)
        WHERE address.location_key=prepared.location_key""",
        json.dumps(rows),
    )


async def _compose_profile_manifest(fixture, manifest, approved, copy_targets):
    """Compose and publish exact retained source rows through the real registry pipeline."""
    await fixture.connection.execute(
        "SET TimeZone='UTC'; SET DateStyle='ISO, YMD'; SET IntervalStyle='iso_8601'; SET extra_float_digits=3; SET bytea_output='hex'"
    )
    request_id = uuid4()
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    composed, addresses = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=approved.approved_revision,
        expected_head=manifest.generation_id,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(manifest.schema_name, "entity_address_unified", manifest.manifest_sha256)
        ),
        writer_roles=_roles(fixture),
        source_manifest=manifest,
        control_schema=fixture.control_schema,
    )
    copy_targets.append(composed)
    await prepare_and_publish_network_candidate(
        fixture.connection,
        composed,
        addresses,
        **_roles(fixture),
        control_schema=fixture.control_schema,
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    return manifest


async def _grant_profile_reader(fixture):
    """Close public definer execution and grant only source/registry reads in the owned database."""
    for schema in (fixture.control_schema, "mrf"):
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.roles["reader"]}"')
        await fixture.connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{fixture.roles["reader"]}"'
        )
    for routine in await fixture.connection.fetch(
        "SELECT p.oid::regprocedure::text AS name FROM pg_proc p WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype)"
    ):
        await fixture.connection.execute(f"REVOKE EXECUTE ON FUNCTION {routine['name']} FROM PUBLIC")


async def _remove_profile_office(fixture, manifest, network_id, sites, actor, copy_targets):
    """Approve one exact office REMOVE without editing any published source rows."""
    source_members = await fixture.connection.fetch(
        f'SELECT DISTINCT ON (provider_system,provider_id,location_id) * FROM "{manifest.schema_name}".network_membership_source WHERE network_id=$1 ORDER BY provider_system,provider_id,location_id',
        network_id,
    )
    assert {member["location_id"] for member in source_members} == set(sites)
    await _approve_composition_source_offices(fixture, manifest, copy_targets[-1], source_members, actor)
    office_b = next(member for member in source_members if member["location_id"] == sites[1])
    command = RegistryRecordCommand(
        "membership",
        office_b["network_id"],
        "create",
        0,
        {
            "memberships_json": [
                {
                    "network_id": office_b["network_id"],
                    "provider_system": office_b["provider_system"],
                    "provider_id": office_b["provider_id"],
                    "location_id": str(office_b["location_id"]),
                    "evidence_id": "reviewed-office-removal",
                    "operation": "remove",
                }
            ]
        },
        "Review exact office removal",
        uuid4().hex,
    )
    _, revision = await _approve_composition_intent(fixture, actor, command)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        latest_approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=revision, control_schema=fixture.control_schema
        )
    return await _compose_profile_manifest(fixture, manifest, latest_approved, copy_targets), revision


async def _prepare_profile_source(fixture, pair, copy_targets, *, public_contacts=False):
    """Review the source binding and publish the exact source/effective office inputs."""
    pin = pair.recipe.source_pin
    actor, networks, binding, approved = await _reviewed_cms_binding(
        fixture.connection, fixture.control_schema, fixture.engine, pin, pair.recipe.binding_coordinates
    )
    sites = [
        await fixture.connection.fetchval(
            f"SELECT site_id FROM \"{pin.read_schema_name}\".provider_directory_entity_source_binding WHERE resource_type='Location' AND resource_id=$1",
            resource,
        )
        for resource in ("site-1", "site-2")
    ]
    reviewed = (
        (fixture.connection, fixture.control_schema, pin, sites),
        actor,
        networks,
        binding,
        pair.recipe.binding_coordinates,
        approved,
    )
    manifest = await _publish_profile_recipe(fixture, reviewed, copy_targets, public_contacts=public_contacts)
    manifest = await _compose_profile_manifest(fixture, manifest, approved, copy_targets)
    source_manifest = manifest
    manifest, revision = await _remove_profile_office(
        fixture, manifest, networks[0]["record_id"], sites, actor, copy_targets
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert manifest.approved_custom_revision == revision > source_manifest.approved_custom_revision
        assert (
            await resolve_network_serving_manifest(
                fixture.connection, generation_id=source_manifest.generation_id, control_schema=fixture.control_schema
            )
            == source_manifest
        )
        for retained_manifest in (source_manifest, manifest):
            retained_sites = await fixture.connection.fetch(
                f'SELECT location_id FROM "{retained_manifest.schema_name}".network_membership_source WHERE network_id=$1',
                networks[0]["record_id"],
            )
            assert {member["location_id"] for member in retained_sites} == set(sites)
        current_sites = await fixture.connection.fetch(
            f'SELECT location_id FROM "{manifest.schema_name}".network_membership WHERE network_id=$1',
            networks[0]["record_id"],
        )
        assert {member["location_id"] for member in current_sites} == {sites[0]}
    await _grant_profile_reader(fixture)
    return manifest, networks[0]["record_id"], sites


async def _remove_profile_pairs(fixture, capture_id):
    """Remove only this fixture's exact retained pair schemas."""
    for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
async def published_profile_source(monkeypatch, tmp_path, request):
    """Own actual CMS admission, capture, canonical publication and exact cleanup."""
    pairs._retained_office_resources(monkeypatch)
    source_resources = support._resource_rows

    def resources(revision):
        rows = source_resources(revision)
        rows["OrganizationAffiliation"][0]["active"] = False
        return rows

    monkeypatch.setattr(support, "_resource_rows", resources)
    directory, acquired = support.retained_release(tmp_path)
    capture_id = uuid4()
    copy_targets = []
    async with support.admission_database(monkeypatch) as database:
        with support.release_probe_client(directory) as client:
            initial = await cms._run_acquired({"context": {}}, {}, "profile-admission", directory, acquired, client)
        proof = await pairs._publish_pair(database, initial, monkeypatch)
        monkeypatch.setenv(
            "NETWORK_REGISTRY_TEST_DSN",
            database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        )
        async with aclosing(serving_schema.__wrapped__()) as registry:
            native_registry = await anext(registry)
            async with aclosing(custom_db.__wrapped__(native_registry)) as custom:
                fixture = await anext(custom)
                for extension in ("intarray", "btree_gin", "postgis"):
                    await fixture.connection.execute(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
                try:
                    pair = await pairs._capture_bounded_pair(
                        database, proof, capture_id, (fixture.roles["owner"], fixture.roles["reader"])
                    )
                    manifest, network, sites = await _prepare_profile_source(
                        fixture, pair, copy_targets, public_contacts=getattr(request, "param", False) is True
                    )
                    yield database, fixture, pair, manifest, network, sites
                finally:
                    await _remove_candidates(fixture, copy_targets, uuid4())
                    await _remove_profile_pairs(fixture, capture_id)


async def _read(fixture, manifest, **options):
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles["reader"]}"'))
        assert await session.scalar(text("SELECT current_user")) == fixture.roles["reader"]
        assert not await session.scalar(
            text("SELECT pg_has_role(current_user,:owner,'SET')"), {"owner": fixture.roles["owner"]}
        )
        return await read_cms_profile_plan_inputs(
            session, manifest, "1000000491", excluded_network_ids=options.pop("excluded_network_ids", ()), **options
        )


async def test_native_profile_exact_office_and_default_exclusion(published_profile_source):
    _database, fixture, pair, manifest, network, sites = published_profile_source
    result = await _read(fixture, manifest)
    assert result.dataset_id == pair.recipe.source_pin.dataset_id
    assert result.facts and {fact["plan_id"] for fact in result.facts} == {"plan-1"}
    assert {fact["location_id"] for fact in result.facts} == {str(sites[0])}
    assert not (await _read(fixture, manifest, selected_network_ids=(network + 1,))).facts
    assert not (await _read(fixture, manifest, location_id=str(sites[1]))).facts
    assert not (await _read(fixture, manifest, excluded_network_ids=(network,))).facts
    # A canonical effective office with no raw role/site assertion grants no source fact.
    unrelated_site = uuid4()
    await fixture.connection.execute(
        f'''INSERT INTO "{manifest.schema_name}".network_membership
        SELECT network_id,provider_system,provider_id,$1,evidence_id
        FROM "{manifest.schema_name}".network_membership LIMIT 1''',
        unrelated_site,
    )
    await fixture.connection.execute(
        f'''INSERT INTO "{manifest.schema_name}".provider_location_binding
        SELECT provider_system,provider_id,$1,location_key,entity_type,entity_id
        FROM "{manifest.schema_name}".provider_location_binding LIMIT 1''',
        unrelated_site,
    )
    assert not (await _read(fixture, manifest, location_id=str(unrelated_site))).facts


@pytest.mark.parametrize("corruption", ["plan", "network", "source", "retained-epoch"])
async def test_native_profile_rejects_foreign_or_unproved_facts(published_profile_source, corruption):
    _database, fixture, pair, manifest, _network, _sites = published_profile_source
    pin = pair.recipe.source_pin
    if corruption == "retained-epoch":
        await fixture.connection.execute(f"COMMENT ON SCHEMA \"{pair.address_ownership.schema_name}\" IS 'unproved'")
        with pytest.raises(ValueError):
            await _read(fixture, manifest)
        return
    if corruption == "plan":
        await fixture.connection.execute(
            f'''UPDATE "{pin.read_schema_name}".provider_directory_dataset_resource
            SET payload_json=(payload_json::jsonb||$1::jsonb)
            WHERE resource_type='InsurancePlan' ''',
            '{"network_refs": ["Organization/foreign"]}',
        )
    elif corruption == "source":
        await fixture.connection.execute(
            f'''UPDATE "{pin.read_schema_name}".provider_directory_insurance_network_plan_evidence
            SET source_id='foreign-source' '''
        )
    else:
        await fixture.connection.execute(
            f'''UPDATE "{pin.read_schema_name}".provider_directory_insurance_network_plan_evidence
            SET network_resource_id='foreign-network' '''
        )
    assert not (await _read(fixture, manifest)).facts


@pytest.mark.parametrize("bound", ["MAX_FACTS", "MAX_BYTES"])
async def test_native_profile_accounting_refuses_before_fact_transfer(published_profile_source, monkeypatch, bound):
    from api import provider_profile_network_facts as profile_facts
    from api.provider_directory_entities_contract import DirectoryReadError

    _database, fixture, _pair, manifest, _network, _sites = published_profile_source
    assert (await _read(fixture, manifest)).facts
    # A deliberately stricter test budget exercises both native aggregate guards.
    monkeypatch.setattr(profile_facts, bound, 0)
    with pytest.raises(DirectoryReadError):
        await _read(fixture, manifest)


async def test_native_profile_refuses_open_address_custody(published_profile_source):
    _database, fixture, pair, manifest, _network, _sites = published_profile_source
    assert (await _read(fixture, manifest)).facts
    await fixture.connection.execute(
        f'GRANT UPDATE ON "{pair.address_ownership.schema_name}".entity_address_unified TO "{fixture.roles["reader"]}"'
    )
    with pytest.raises(ValueError, match="Native owner or runtime privileges are not closed"):
        await _read(fixture, manifest)
