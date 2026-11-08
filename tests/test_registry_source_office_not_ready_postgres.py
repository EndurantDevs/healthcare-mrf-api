# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only genuine missing targets produce cacheable CMS office readiness failures."""

import json
from contextlib import aclosing

import pytest

from process.network_initial_cms_office_bindings import InitialCMSOfficeBindingError
from process.registry_initial_source_composition import OfficeSummaryRequest, prepare_initial_source_offices
from process.registry_source_office_errors import SourceOfficeNotReady
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests import cms_registry_recipe_postgres_support as support
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.cms_registry_recipe_postgres_support import serving_schema as serving_schema
from tests.registry_compact_source_office_postgres_support import compact_cms_offices
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_initial_source_office_bindings_postgres import cms_full_office_source as cms_full_office_source

pytestmark = pytest.mark.asyncio


@pytest.fixture
def two_cms_sites(cms_full_office_source, monkeypatch):
    original = support._membership_resource_rows

    def resources(source, revision):
        documents = original(source, revision)
        documents["Location"][1]["address"] = {
            **documents["Location"][0]["address"],
            "line": ["123 Example Street", "Suite 3"],
        }
        documents["PractitionerRole"][0]["location"].append({"reference": "Location/unused-office"})
        return documents

    monkeypatch.setattr(support, "_membership_resource_rows", resources)


@pytest.fixture
async def two_site_source(two_cms_sites, cms_recipe_database, serving_schema, monkeypatch, tmp_path):
    async with aclosing(
        support.reviewed_cms_source.__wrapped__(cms_recipe_database, serving_schema, monkeypatch, tmp_path)
    ) as generator:
        yield await anext(generator)


async def _address_payload(connection, schema, mode):
    if mode == "complete":
        return
    await connection.execute(f'DELETE FROM "{schema}".entity_address_unified WHERE location_key=$1', "b" * 64)
    if mode == "tampered_positive":
        await connection.execute(
            f'UPDATE "{schema}".entity_address_unified SET second_line=$1 WHERE location_key=$2', "Suite 9", "a" * 64
        )
    elif mode == "duplicate_positive":
        await connection.execute(
            f'INSERT INTO "{schema}".entity_address_unified SELECT (jsonb_populate_record(NULL::"{schema}".entity_address_unified,to_jsonb(office)||$1::jsonb)).* FROM "{schema}".entity_address_unified office WHERE location_key=$2',
            json.dumps({"location_key": "c" * 64}),
            "a" * 64,
        )


async def _prepare(context):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        return await prepare_initial_source_offices(
            fixture.execution_connection,
            (RegistrySourceMembershipRecipe(context.source_pin, context.reviewed[4]),),
            context.reviewed[-1],
            context.source,
            request=OfficeSummaryRequest(runtime_roles=context.runtime_roles, control_schema=fixture.control_schema),
        )


@pytest.mark.parametrize("mode", ("complete", "missing", "tampered_positive", "duplicate_positive"))
async def test_missing_office_is_typed_only_after_all_positive_matches_verify(
    two_site_source, custom_db, monkeypatch, mode
):
    async def office_setup(connection, schema):
        await _address_payload(connection, schema, mode)

    async with compact_cms_offices(custom_db, two_site_source, monkeypatch, office_setup=office_setup) as context:
        if mode == "complete":
            assert (await _prepare(context))["office_rows"] == 2
        elif mode == "missing":
            with pytest.raises(SourceOfficeNotReady):
                await _prepare(context)
        else:
            with pytest.raises(InitialCMSOfficeBindingError):
                await _prepare(context)
