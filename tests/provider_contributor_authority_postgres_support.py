# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native contributor policy and retained office SQL; no full rate/readiness claim."""

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import text

from api import provider_service_source_policy as policy
from api.network_address_scope import network_address_read_scope, scoped_address_parameters, scoped_address_relation_sql
from api.provider_detail_network_read import managed_provider_network_read_scope
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests import test_provider_service_source_policy_postgres as sources
from tests.managed_billing_sql_postgres_support import (
    _billing_sql_inputs,
    _retained_office_proof,
)
from tests.managed_billing_sql_postgres_support import (
    billing_office_inputs as billing_office_inputs,
)
from tests.test_provider_service_source_policy_postgres import (
    _ALLOWED,
    _EXCLUDED,
    _reader_identity,
    _selection,
)
from tests.test_provider_service_source_policy_postgres import (
    cms_resource_template as cms_resource_template,
)
from tests.test_provider_service_source_policy_postgres import (
    profile_transport as profile_transport,
)
from tests.test_provider_service_source_policy_postgres import (
    published_profile_source as published_profile_source,
)
from tests.test_provider_service_source_policy_postgres import (
    source_admission as source_admission,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def contributor_company_inputs(monkeypatch):
    """Publish a second independent company association through the existing fixture."""
    original_companies, original_sources = sources._seed_companies, sources._seed_sources

    async def companies(held, revision, identities):
        await original_companies(held, revision, identities)
        await sources._approved_row(
            held,
            revision,
            "company_links",
            identities[2],
            {
                "company_id": identities[2],
                "network_ids": [_ALLOWED, held.network],
                "archived": False,
            },
        )

    async def source_units(held, schema, revision, identities):
        reviewed = await original_sources(held, schema, revision, identities)
        for network in (_ALLOWED, held.network):
            entry = sources._review(schema, "other_company", identities[2], network, revision)
            if network == _ALLOWED:
                await held.fixture.connection.execute(
                    f"INSERT INTO \"{schema}\".ptg2_snapshot VALUES($1,'published',$2::jsonb)",
                    entry.mapping["dataset_id"],
                    json.dumps(entry.manifest),
                )
                reviewed["other_company"] = entry
            await sources._store_review(held, entry)
            await sources._approved_row(held, revision, "network_binding", entry.mapping["binding_id"], entry.mapping)
        return reviewed

    monkeypatch.setattr(sources, "_seed_companies", companies)
    monkeypatch.setattr(sources, "_seed_sources", source_units)


def _evidence(name, document):
    if os.getenv("CONTRIBUTOR_NATIVE_EVIDENCE"):
        Path(os.environ["CONTRIBUTOR_NATIVE_EVIDENCE"] + "-" + name + ".json").write_text(
            json.dumps(document, indent=2) + "\n"
        )


@pytest.mark.usefixtures("contributor_company_inputs")
async def test_native_hidden_company_sources_cannot_use_public_shared_office(source_admission):
    admission = source_admission
    held = admission.held
    selection = _selection(
        [admission.reviews_by_name[name] for name in ("allowed", "excluded", "other_company", "public")]
    )
    organization = RegistryOrganizationVisibility((admission.companies[0],), ())
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        pid = await _reader_identity(driver, held)
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(_EXCLUDED,),
            access_scope_sha256="a" * 64,
            organization_policy=organization,
        ):
            admitted = await policy.admitted_plan_release_bindings(session, selection)
            assert admitted == selection.bindings[2:]
            relation = scoped_address_relation_sql("mrf.entity_address_unified")
            offices = (
                await session.execute(
                    text(f"SELECT npi,location_key FROM {relation} ORDER BY location_key"),
                    scoped_address_parameters({}),
                )
            ).all()
            assert len(offices) == 2 and len({office.npi for office in offices}) == 1
            assert await _reader_identity(driver, held) == pid
    assert len(selection.bindings) == 4 and selection._validated_serving_tables == ()
    _evidence(
        "company",
        {
            "admitted_sources": [binding.source_key for binding in admitted],
            "public_same_npi_office_count": len(offices),
            "reader_read_only_repeatable_read": True,
            "full_rates_readiness": False,
        },
    )


@pytest.mark.usefixtures("billing_office_inputs", "contributor_company_inputs")
async def test_native_selected_reviewed_company_source_keeps_exact_retained_office(source_admission, monkeypatch):
    admission = source_admission
    held = admission.held
    offices = await _billing_sql_inputs(admission, monkeypatch)
    selection = _selection([admission.reviews_by_name[name] for name in ("allowed", "other_company", "public")])
    organization = RegistryOrganizationVisibility((admission.companies[0],), ())
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        pid = await _reader_identity(driver, held)
        async with managed_provider_network_read_scope(
            session,
            selector=(held.network,),
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=(),
            access_scope_sha256="a" * 64,
            organization_policy=organization,
        ) as manifest:
            admitted = await policy.admitted_plan_release_bindings(session, selection)
            assert admitted == (selection.bindings[1],)
            evidence_by_field = await _retained_office_proof(session, admission, offices, admitted)
            denied_policy = RegistryOrganizationVisibility(
                tuple(sorted((admission.companies[0], admission.companies[2]))), ()
            )
            with network_address_read_scope(
                manifest, (held.network,), access_scope_sha256="b" * 64, organization_policy=denied_policy
            ):
                assert await policy.admitted_plan_release_bindings(session, selection) == ()
            assert await _reader_identity(driver, held) == pid
    evidence_by_field.update(
        selected_reviewed_company_source=True,
        all_denied_company_scope=True,
        reader_read_only_repeatable_read=True,
        full_rates_readiness=False,
    )
    _evidence("selected", evidence_by_field)
