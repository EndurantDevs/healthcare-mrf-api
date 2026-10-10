# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual admitted CMS source, native custody and reviewed registry recipe fixtures."""

import json
import os
from contextlib import aclosing
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

from process import provider_directory_cms_serving_coverage as coverage
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_custody import (
    capture_retained_cms_fhir_source_custody,
    require_retained_cms_fhir_source_custody,
)
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import cms_npd_admission_postgres_support as cms_support
from tests.test_cms_npd_candidate_coverage_postgres import _stage as _stage_cms_edition
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_network_serving_schema_postgres import serving_schema as _registry_schema_fixture
from tests.test_provider_directory_cms_resource_batch_postgres import _protect_custody_schema, _remove_custody_roles
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft


@pytest_asyncio.fixture(scope="module", loop_scope="module", autouse=True)
async def cms_recipe_template():
    """Register one exact native migration template and owned database cleanup."""
    configured = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not configured:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select the isolated native database")
    source_url = make_url(configured).set(drivername="postgresql+asyncpg")
    async with cms_support._owned_database(source_url) as (database_url, _admin):
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv(cms_support._DSN_ENV, database_url.render_as_string(hide_password=False))
            request = SimpleNamespace(param=cms_support.LEGACY_MIGRATION_PREFIXES)
            async with aclosing(cms_support.cms_admission_template.__wrapped__(request)) as baseline:
                await anext(baseline)
                yield


@pytest.fixture
async def cms_recipe_database(monkeypatch):
    async with cms_support.admission_database(
        monkeypatch, migration_prefixes=cms_support.LEGACY_MIGRATION_PREFIXES
    ) as database:
        for extension in ("intarray", "btree_gin", "postgis"):
            await database.status(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
        yield database


@pytest.fixture
async def serving_schema(cms_recipe_database, monkeypatch):
    """Place actual registry migrations and retained CMS source in the same native database."""
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN",
        cms_recipe_database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
    )
    async with aclosing(_registry_schema_fixture.__wrapped__()) as registry_fixture:
        yield await anext(registry_fixture)


def _membership_resource_rows(original, revision):
    """Declare source NPI, network and one exact office before all eight files are hashed."""
    resources = original(revision)
    resources["Organization"][0]["type"] = [{"text": "ntwk"}]
    resources["Practitioner"][0]["identifier"] = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000491"}]
    resources["PractitionerRole"][0]["network"] = [{"reference": "Organization/network-1"}]
    resources["PractitionerRole"][0]["insurancePlan"] = [{"reference": "InsurancePlan/plan-1"}]
    resources["InsurancePlan"][0]["network"] = [{"reference": "Organization/network-1"}]
    resources["OrganizationAffiliation"][0]["active"] = False
    resources["Location"].append({"id": "unused-office", "status": "active", "address": {"line": ["2 Sample Street"]}})
    return resources


async def _published_cms_pin(monkeypatch, directory):
    """Run acquired source admission, native publication and real coverage sealing."""
    original = cms_support._resource_rows
    with monkeypatch.context() as source_settings:
        source_settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        source_settings.setenv("DB_SCHEMA", "mrf")
        source_settings.setattr(
            cms_support, "_resource_rows", lambda revision: _membership_resource_rows(original, revision)
        )
        candidate, release, digest = await _stage_cms_edition(source_settings, directory)
        await publish_validated_source_local_dataset(
            cms_support.fhir,
            candidate,
            "cms-npd",
            before_cutover=lambda session: coverage.validate_cms_candidate_coverage(
                session, candidate.dataset_id, release
            ),
            before_cutover_timeout_seconds=60,
            after_promotion=lambda: coverage.seal_cms_candidate_coverage(cms_support.fhir, candidate, release, digest),
        )
        await coverage.prepare_cms_candidate_coverage(cms_support.fhir, candidate, release, digest)
    return candidate, release, digest


async def _closed_cms_pin(connection, candidate, release, digest, role_names):
    """Capture and recheck the actual sealed source owner, native guards and ACLs."""
    source_pin = PinnedFHIRMembershipSource(
        "mrf",
        "cms-npd",
        candidate.endpoint_id,
        candidate.dataset_id,
        digest,
        release,
        await connection.fetchval("SELECT 'mrf.provider_directory_dataset_resource'::regclass::oid"),
        "cms-npd",
        "2026-01-01",
    )
    await _protect_custody_schema(connection, role_names)
    async with connection.transaction(isolation="repeatable_read"):
        custody = await capture_retained_cms_fhir_source_custody(
            connection, source_pin, owner_role=role_names[0], runtime_roles=tuple(sorted(role_names[1:]))
        )
        source_pin = replace(
            source_pin,
            custody_owner_role=custody.owner_role,
            custody_runtime_roles=custody.runtime_roles,
            custody_proof_sha256=custody.proof_sha256,
            custody_catalog_sha256=custody.catalog_sha256,
        )
        await require_retained_cms_fhir_source_custody(connection, replace(custody, source_pin=source_pin))
    return source_pin


async def _cms_binding_scope(connection, source_pin):
    """Read the source's real sealed network-binding descriptor."""
    metadata = json.loads(
        await connection.fetchval(
            "SELECT publication_metadata_summary_json::text FROM mrf.provider_directory_endpoint_dataset WHERE dataset_id=$1",
            source_pin.dataset_id,
        )
    )["network_bindings"]
    coordinates = RegistryNetworkSourceCoordinates(
        **{
            field: metadata[field]
            for field in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        }
    )
    return coordinates


async def _reviewed_cms_binding(connection, schema, engine, source_pin, coordinates):
    """Approve exactly two known networks and one explicit source-network binding."""
    actor = _actor()
    networks = [await _draft(engine, schema, _create("network"), actor) for _ in range(2)]
    legacy_id = await connection.fetchval(
        "SELECT network_id FROM mrf.provider_directory_insurance_network_source_binding WHERE source_id='cms-npd' AND resource_type='Organization' AND resource_id='network-1'"
    )
    binding_dict = {
        "binding_id": str(uuid4()),
        **{
            field: getattr(coordinates, field)
            for field in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
        },
        "source_key": "network-1",
        "source_scope_json": {
            "organization_id": "network-1",
            "legacy_uuid": str(legacy_id),
            "alias_scope": source_pin.alias_scope,
        },
        "network_id": networks[0]["record_id"],
        "evidence_id": "reviewed-cms-network",
        "evidence_sha256": "e" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }
    binding = (await _reviewed_binding(connection, schema, actor, [binding_dict]))["records"][0]
    approval = await _approve(connection, schema, await _command(connection, schema, *networks, binding), actor)
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=schema
        )
    return actor, networks, binding_dict, approved


async def _cms_site_ids(connection):
    """Read the declared and unused site UUIDs in one deterministic native query."""
    site_rows = await connection.fetch(
        "SELECT resource_id,site_id FROM mrf.provider_directory_entity_source_binding "
        "WHERE source_id='cms-npd' AND resource_type='Location' "
        "AND resource_id=ANY($1::text[]) ORDER BY resource_id",
        ["site-1", "unused-office"],
    )
    assert [row["resource_id"] for row in site_rows] == ["site-1", "unused-office"]
    sites = tuple(row["site_id"] for row in site_rows)
    return sites


@pytest.fixture
async def reviewed_cms_source(cms_recipe_database, serving_schema, monkeypatch, tmp_path):
    """Use complete admitted source evidence and exact protected native custody."""
    connection, schema, engine = serving_schema
    candidate, release, digest = await _published_cms_pin(monkeypatch, tmp_path)
    role_names = tuple("recipe_custody_" + uuid4().hex for _ in range(3))
    try:
        source_pin = await _closed_cms_pin(connection, candidate, release, digest, role_names)
        coordinates = await _cms_binding_scope(connection, source_pin)
        actor, networks, binding_fields, approved = await _reviewed_cms_binding(
            connection, schema, engine, source_pin, coordinates
        )
        sites = await _cms_site_ids(connection)
        yield (connection, schema, source_pin, sites), actor, networks, binding_fields, coordinates, approved
    finally:
        await _remove_custody_roles(connection, role_names)
