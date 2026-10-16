# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retained billing SQL; release, graph and selector readiness remain separate."""

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import billing_search_network_policy as policy
from api import ptg2_serving as native
from api.custom_import_provider_detail import parse_managed_provider_policy
from api.network_address_scope import scoped_address_parameters
from api.provider_detail_network_read import managed_provider_network_read_scope
from tests import test_provider_profile_network_facts_postgres as profile_source
from tests.ptg2_serving_address_evidence_postgres_support import _create_tables
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_service_source_policy_postgres import (
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
def billing_office_inputs(monkeypatch):
    """Prepare synthetic protected offices before the real custody/publication pipeline."""
    original = profile_source._profile_addresses

    async def protected_offices(fixture, **options):
        options["public_contacts"] = True
        table = await original(fixture, **options)
        await fixture.connection.execute(f"UPDATE {table} SET row_origin='reviewed_fixture',address_source_mask=1")
        return table

    monkeypatch.setattr(profile_source, "_profile_addresses", protected_offices)


async def _billing_sql_inputs(admission, monkeypatch):
    """Keep current evidence inputs separate from the immutable retained office heap."""
    connection = admission.held.fixture.connection
    database = SimpleNamespace(status=connection.execute)
    await _create_tables(database, admission.schema)
    retained = f'"{admission.manifest.schema_name}".entity_address_unified'
    offices = await connection.fetch(f"SELECT * FROM {retained} ORDER BY cardinality(canonical_network_ids) DESC")
    assert len(offices) == 2 and len({office["npi"] for office in offices}) == 1
    assert all(office["row_origin"] == "reviewed_fixture" for office in offices)
    assert offices[0]["canonical_network_ids"] and not offices[1]["canonical_network_ids"]
    for index, office in enumerate(offices):
        await connection.execute(
            f'INSERT INTO "{admission.schema}".npi_address(npi,address_key,date_added) VALUES($1,$2,current_date)',
            office["npi"],
            office["address_key"],
        )
        await connection.execute(
            f'''INSERT INTO "{admission.schema}".entity_address_evidence
            (evidence_id,location_key,address_key,premise_key,npi,source_id,source_record_key,source_run_id,observed_at)
            VALUES($1,$2,$3,$4,$5,1,$6,'20261016',current_timestamp)''',
            index + 1,
            office["location_key"],
            office["address_key"],
            office["premise_key"],
            office["npi"],
            "nppes:synthetic:" + str(index),
        )
    await connection.execute(
        f'INSERT INTO "{admission.schema}".ptg2_v3_npi_scope VALUES(71,$1),(99,$1)', offices[0]["npi"]
    )
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{admission.schema}" TO "{admission.held.role}"')
    monkeypatch.setattr(native, "PTG2_SCHEMA", admission.schema)
    return offices


def _context(schema, *, denied_location_key=None):
    filters = "npi_scope.snapshot_key = :shared_snapshot_key"
    if denied_location_key is not None:
        filters += " AND addr.location_key = :requested_location_key"
    return native._MembershipLocationQuery(
        address_table=schema + ".entity_address_unified",
        npi_scope_table=schema + ".ptg2_v3_npi_scope",
        filter_sql=filters,
        parameter_map={
            "shared_snapshot_key": 71,
            "limit": 1,
            "offset": 0,
            "requested_location_key": denied_location_key,
        },
        distance_sql="CASE WHEN cardinality(addr.canonical_network_ids)>0 THEN 20 ELSE 1 END::double precision",
        address_assurance_sql="addr.address_key IS NOT NULL",
        knn_order_sql=None,
    )


async def _native_locations(session, schema, *, denied_location_key=None):
    context = _context(schema, denied_location_key=denied_location_key)
    query = policy._retained_location_sql(native, context)
    context = replace(context, parameter_map=scoped_address_parameters(context.parameter_map))
    rows = await native._execute_membership_location_sql(session, context, query, offset=0)
    await policy._retained_lineage(session, native, rows)
    return rows, query


async def _retained_office_proof(session, admission, offices, admitted):
    read_snapshot_ids = []
    for binding in admitted:
        read_snapshot_ids.append(binding.snapshot_id)
        location_rows, query = await _native_locations(session, admission.schema)
    assert [location_row["npi"] for location_row in location_rows] == [offices[0]["npi"]]
    address_by_field = json.loads(location_rows[0]["address_payload"])
    assert address_by_field["location_key"] == offices[0]["location_key"]
    assert address_by_field["address_provenance"][0]["source_record_id"] == "nppes:synthetic:0"
    assert location_rows[0]["distance_miles"] == 20
    denied, _ = await _native_locations(session, admission.schema, denied_location_key=offices[1]["location_key"])
    assert denied == []
    denied_location_rows = [
        {
            "npi": offices[1]["npi"],
            "_geo_evidence_level": "nppes_registry_address",
            "_geo_evidence_source_id": 1,
            "address_payload": json.dumps({"location_key": offices[1]["location_key"]}),
        }
    ]
    await policy._retained_lineage(session, native, denied_location_rows)
    assert denied_location_rows == []
    return {
        "read_snapshot_ids": read_snapshot_ids,
        "result_count_with_limit_one": len(location_rows),
        "same_npi_denied_office": denied,
        "denied_office_lineage": denied_location_rows,
        "allowed_location_key": address_by_field["location_key"],
        "sql": query,
    }


@pytest.mark.usefixtures("billing_office_inputs")
async def test_native_billing_retained_offices_sources_and_lineage(source_admission, monkeypatch):
    """Actual source admission precedes retained ranking/limit and exact stored lineage."""
    admission = source_admission
    offices = await _billing_sql_inputs(admission, monkeypatch)
    selection = _selection([admission.reviews_by_name[name] for name in ("excluded", "allowed")])
    original = selection.bindings
    document = v2_envelope({}, excluded_network_ids=[_EXCLUDED])
    _, exclusions, digest, organization = parse_managed_provider_policy(document)
    held = admission.held
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        pid = await _reader_identity(driver, held)
        async with managed_provider_network_read_scope(
            session,
            selector=None,
            network_generation=admission.manifest.generation_id,
            excluded_network_ids=exclusions,
            access_scope_sha256=digest,
            organization_policy=organization,
        ):
            admitted = await policy.admitted_plan_release_bindings(session, selection)
            assert admitted == (original[1],) and admitted[0] is original[1]
            evidence_by_field = await _retained_office_proof(session, admission, offices, admitted)
            assert await driver.fetchval("SELECT pg_backend_pid()") == pid
            assert await _reader_identity(driver, held) == pid
    assert selection.bindings is original and evidence_by_field["read_snapshot_ids"] == [original[1].snapshot_id]
    evidence_by_field.update(
        native_source_admission=True,
        admitted_ordinals=[binding.binding_ordinal for binding in admitted],
        denied_original_ordinal=original[0].binding_ordinal,
        reader_repeatable_read_read_only=True,
        limits="Real retained publication/source admission and SQL. Protected synthetic office origin prepared before publication. Typed location-query input; no full release, graph/rate, selector, HTTP or readiness qualification.",
    )
    if os.getenv("MANAGED_BILLING_NATIVE_EVIDENCE"):
        Path(os.environ["MANAGED_BILLING_NATIVE_EVIDENCE"]).write_text(json.dumps(evidence_by_field, indent=2) + "\n")
