# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native wide resource fanout, pinned reads and transactional COPY rollback."""

import json
from uuid import uuid4

import pytest

from process.network_fhir_membership_source import copy_fhir_membership_batch
from process.network_membership_candidate_lifecycle import _locked_candidate, create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from tests.cms_npd_admission_postgres_support import fhir
from tests.provider_directory_rooted_synthetic_support import wide_network_membership_resources
from tests.test_network_fhir_membership_source_postgres import (
    _RESOURCE_COPY_COLUMNS,
    _generation,
    _read,
    _resource,
)
from tests.test_network_fhir_membership_source_postgres import (
    fhir_source as fhir_source,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


async def _copy_wide_network_fixture(fhir_source):
    """Seed exact native network bindings set-wise before the bounded resource COPY."""
    connection, schema, _, _ = fhir_source
    await _resource(connection, schema, "Organization", "provider-one", {"npi": 1000000491})
    await connection.execute(f"""
        INSERT INTO "{schema}".provider_directory_organization_identity
            SELECT md5('entity-wide-'||n)::uuid,now() FROM generate_series(0,794) n;
        INSERT INTO "{schema}".provider_directory_entity_source_binding
            SELECT 'source-example','Organization','network-wide-'||n,md5('entity-wide-'||n)::uuid,NULL,now()
            FROM generate_series(0,794) n;
        INSERT INTO "{schema}".provider_directory_entity_release_evidence
            SELECT 'source-example','Organization','network-wide-'||n,'release-one',repeat('a',64),'{{}}',now()
            FROM generate_series(0,794) n;
        INSERT INTO "{schema}".provider_directory_insurance_network_identity
            SELECT md5('network-wide-'||n)::uuid,now() FROM generate_series(0,794) n;
        INSERT INTO "{schema}".provider_directory_insurance_network_source_binding
            SELECT 'source-example','Organization','network-wide-'||n,md5('network-wide-'||n)::uuid,now()
            FROM generate_series(0,794) n;
        INSERT INTO "{schema}".network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE
            SELECT 1000+n,md5('canonical-wide-'||n)::uuid FROM generate_series(0,794) n;
        INSERT INTO "{schema}".network_registry_alias
            SELECT 'fhir','source-example','legacy_fhir_uuid',md5('network-wide-'||n)::uuid::text,'medical',
                1000+n,'reviewed-example',now() FROM generate_series(0,794) n;
    """)
    normalized_records = [
        (
            "edition-one",
            "Organization",
            f"network-wide-{index}",
            "a" * 64,
            json.dumps({"resource_id": f"network-wide-{index}", "active": True}),
            "a" * 64,
        )
        for index in range(795)
    ]
    source_resources_by_kind = wide_network_membership_resources()
    for kind, parser in (
        ("OrganizationAffiliation", fhir._parse_organization_affiliation_resource),
        ("PractitionerRole", fhir._parse_practitioner_role_resource),
    ):
        raw_resource = source_resources_by_kind[kind][0]
        normalized_payload = parser(raw_resource, {"source_id": "source-example", "resource_id": raw_resource["id"]})[1]
        normalized_records.append(
            ("edition-one", kind, raw_resource["id"], "a" * 64, json.dumps(normalized_payload), "a" * 64)
        )
    assert (
        await connection.copy_records_to_table(
            "provider_directory_dataset_resource",
            schema_name=schema,
            columns=_RESOURCE_COPY_COLUMNS,
            records=normalized_records,
        )
        == "COPY 797"
    )


async def test_affiliation_795_networks_and_role_23_extensions_keep_native_fanout(fhir_source):
    await _copy_wide_network_fixture(fhir_source)
    _, _, _, sites = fhir_source
    raw = wide_network_membership_resources()
    assert len(raw["OrganizationAffiliation"][0]["network"]) == 795
    assert len(raw["PractitionerRole"][0]["extension"]) == 23
    batch = await _read(fhir_source)
    assert batch.source_rows == batch.accepted_source_rows == 2
    assert batch.membership_rows == 818
    assert batch.unresolved_rows == batch.unresolved_source_rows == batch.omitted_source_rows == 0
    memberships = json.loads(batch.input_bytes)
    network_ids = {entry["network_id"] for entry in memberships}
    assert network_ids == set(range(1000, 1795))
    assert {entry["location_id"] for entry in memberships} == {str(sites[0])}
    assert str(sites[1]) not in {entry["location_id"] for entry in memberships}
    assert all(
        {(entry["provider_id"], entry["location_id"]) for entry in memberships if entry["network_id"] == network_id}
        == {("1000000491", str(sites[0]))}
        for network_id in network_ids
    )
    assert sum(entry["network_id"] < 1023 for entry in memberships) == 46


async def test_wide_membership_source_pin_keeps_incumbent_through_successor_and_rollback(fhir_source):
    await _copy_wide_network_fixture(fhir_source)
    connection, schema, _, _ = fhir_source
    original = await _read(fhir_source)
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(
            f"UPDATE \"{schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
        )
        await _generation(connection, schema, "edition-two", "b" * 64)
        await _resource(connection, schema, "PractitionerRole", "broken-successor", {}, dataset="edition-two")
        assert await _read(fhir_source) == original
    finally:
        await transaction.rollback()
    assert (
        await connection.fetchval(
            f'SELECT dataset_id FROM "{schema}".provider_directory_endpoint_dataset WHERE is_current'
        )
        == "edition-one"
    )
    assert await _read(fhir_source) == original


async def test_wide_membership_native_copy_rolls_back_complete_818_row_batch(fhir_source):
    await _copy_wide_network_fixture(fhir_source)
    connection, schema, source_pin, _ = fhir_source
    batch = await _read(fhir_source)
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    try:
        async with connection.transaction():
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={"fhir": source_pin.generation_id},
                approved_custom_revision=0,
                expected_head=0,
                expected_rows=818,
                control_schema=schema,
            )

        async def authority(connection, requested):
            candidate = await _locked_candidate(connection, requested, '"' + schema + '"')
            assert candidate["state"] == "open"
            return requested

        transaction = connection.transaction()
        await transaction.start()
        try:
            receipt = await copy_fhir_membership_batch(
                connection, batch, copy_target, require_candidate_authority=authority
            )
            assert receipt.row_count == 818
            assert (
                await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 818
            )
            assert (
                await connection.fetchval(
                    f"SELECT count(DISTINCT(network_id,provider_system,provider_id,location_id)) "
                    f'FROM "{copy_target.schema_name}".network_membership'
                )
                == 795
            )
        finally:
            await transaction.rollback()
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 0
        assert await _read(fhir_source) == batch
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None
