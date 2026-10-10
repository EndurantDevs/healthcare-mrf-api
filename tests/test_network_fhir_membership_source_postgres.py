# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned FHIR extraction through actual retained models and native COPY."""

import asyncio
import hashlib
import importlib.util
import json
from dataclasses import asdict, replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData

from db.models.provider_directory_cms_npd_resource_witness import ProviderDirectoryCMSNPDResourceWitness
from db.models.system import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryDatasetNetworkPlan,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
)
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    FHIRMembershipSourceError,
    PinnedFHIRMembershipSource,
    copy_fhir_membership_batch,
    read_fhir_membership_batch,
)
from process.network_membership_candidate_lifecycle import _locked_candidate, create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_source_recipe_composition import _read_recipe_page
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _CountedConnection, _create, _draft

pytestmark = pytest.mark.asyncio


async def _source_tables(engine, schema):
    metadata = MetaData()
    for model in (
        ProviderDirectoryAPIEndpoint,
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryDatasetResource,
        ProviderDirectoryDatasetNetworkPlan,
    ):
        model.__table__.to_metadata(metadata, schema=schema)
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        for filename in (
            "20260929010000_provider_directory_entity_identity.py",
            "20260929020000_provider_directory_insurance_network_identity.py",
        ):
            path = Path(__file__).parents[1] / "alembic/versions" / filename
            spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)

            def upgrade(sync_connection):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()

            await connection.run_sync(upgrade)


async def _resource(connection, schema, kind, resource_id, payload, *, dataset="edition-one", digest="a" * 64):
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_dataset_resource '
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json,acquired_resource_sha256) VALUES($1,$2,$3,$4,$5::json,$4)",
        dataset,
        kind,
        resource_id,
        digest,
        json.dumps({"resource_id": resource_id, **payload}),
    )


async def _generation(connection, schema, dataset="edition-one", digest="d" * 64):
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_endpoint_dataset '
        "(dataset_id,endpoint_id,dataset_hash,status,is_current,resource_count,published_at,publication_metadata_summary_json) "
        "VALUES($1,'endpoint-example',$2,'published',true,0,now(),'{\"source_ids\":[\"source-example\"]}')",
        dataset,
        digest,
    )


async def _evidence(connection, schema, kind, resource_id, identity):
    identity_table, identity_column = (
        ("provider_directory_site_identity", "site_id")
        if kind == "Location"
        else ("provider_directory_organization_identity", "organization_id")
    )
    await connection.execute(f'INSERT INTO "{schema}".{identity_table} VALUES($1,now())', identity)
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_entity_source_binding '
        f"(source_id,resource_type,resource_id,{identity_column},created_at) VALUES('source-example',$1,$2,$3,now())",
        kind,
        resource_id,
        identity,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_entity_release_evidence VALUES '
        "('source-example',$1,$2,'release-one',$3,'{}',now())",
        kind,
        resource_id,
        "a" * 64,
    )


@pytest.fixture
async def fhir_source(serving_schema):
    connection, schema, engine = serving_schema
    await _source_tables(engine, schema)
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_api_endpoint '
        "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
        "VALUES('endpoint-example','https://directory.example.test/fhir',$1,$1)",
        "e" * 64,
    )
    await _generation(connection, schema)
    sites = (uuid4(), uuid4())
    for resource_id, site_id in zip(("office-one", "office-two"), sites, strict=True):
        await _resource(connection, schema, "Location", resource_id, {"status": "active"})
        await _evidence(connection, schema, "Location", resource_id, site_id)
    await _resource(connection, schema, "Practitioner", "practitioner-one", {"npi": 1000000491})
    await _resource(connection, schema, "Organization", "network-one", {"active": True})
    await _evidence(connection, schema, "Organization", "network-one", uuid4())
    legacy_id = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_insurance_network_identity VALUES($1,now())', legacy_id
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_insurance_network_source_binding '
        "VALUES('source-example','Organization','network-one',$1,now())",
        legacy_id,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE VALUES(42,$1)',
        uuid4(),
    )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES '
        "('fhir','source-example','legacy_fhir_uuid',$1,'medical',42,'reviewed-example',now())",
        str(legacy_id),
    )
    oid = await connection.fetchval("SELECT to_regclass($1)::oid", schema + ".provider_directory_dataset_resource")
    source_pin = PinnedFHIRMembershipSource(
        schema,
        "source-example",
        "endpoint-example",
        "edition-one",
        "d" * 64,
        "release-one",
        oid,
        "medical",
        "2026-10-01",
    )
    yield connection, schema, source_pin, sites


def _role(location="office-one", **changes):
    return {
        "practitioner_ref": "Practitioner/practitioner-one",
        "location_refs": ["Location/" + location],
        "network_refs": ["Organization/network-one"],
        "active": True,
        **changes,
    }


async def _read(fixture, **options):
    connection, schema, source_pin, _ = fixture
    async with connection.transaction():
        return await read_fhir_membership_batch(connection, source_pin, registry_schema=schema, **options)


async def test_exact_office_copy_uses_real_authority_codec_and_caller_rollback(fhir_source):
    connection, schema, source_pin, sites = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    batch = await _read(fhir_source)
    memberships = json.loads(batch.input_bytes)
    assert batch.generation_id == source_pin.generation_id
    assert batch.source_rows == batch.membership_rows == 1 and batch.unresolved_rows == 0
    assert memberships[0]["location_id"] == str(sites[0]) and memberships[0]["network_id"] == 42
    assert memberships[0]["provider_id"] == "1000000491" and memberships[0]["provider_system"] == "npi"
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
                expected_rows=1,
                control_schema=schema,
            )

        async def authority(connection, requested):
            """Use the actual candidate control row and complete ownership coordinates."""
            candidate = await _locked_candidate(connection, requested, '"' + schema + '"')
            assert (
                candidate["state"] == "open"
                and json.loads(candidate["source_generations"])["fhir"] == source_pin.generation_id
            )
            return requested

        transaction = connection.transaction()
        await transaction.start()
        receipt = await copy_fhir_membership_batch(
            connection, batch, copy_target, require_candidate_authority=authority
        )
        assert receipt.row_count == 1
        copied = await connection.fetchrow(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
        assert copied["location_id"] == sites[0] and copied["location_id"] != sites[1]
        with pytest.raises(FHIRMembershipSourceError, match="accounting mismatch"):
            await copy_fhir_membership_batch(
                connection, replace(batch, membership_rows=2), copy_target, require_candidate_authority=authority
            )
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 1
        await transaction.rollback()
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 0
    finally:
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')


@pytest.mark.parametrize(
    "change",
    [
        {"location_refs": ["Location/missing"]},
        {"network_refs": ["Organization/missing"]},
        {"location_refs": []},
        {"network_refs": []},
        {"practitioner_ref": "Practitioner/missing"},
        {"location_refs": ["https://other.example.test/Location/office-one"]},
        {"network_refs": ["https://other.example.test/Organization/network-one"]},
        {"location_refs": "Location/office-one"},
        {"npi": 1000000509},
        {"period_start": "unknown"},
        {"source_id": "foreign-source"},
    ],
)
async def test_unresolved_references_never_copy_or_expand(fhir_source, change):
    connection, schema, _, _ = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role(**change))
    batch = await _read(fhir_source)
    assert batch.membership_rows == 0 and batch.unresolved_rows == 1
    with pytest.raises(FHIRMembershipSourceError, match="unresolved"):
        await copy_fhir_membership_batch(connection, batch, None, require_candidate_authority=None)


async def test_explicit_plan_affiliation_and_two_offices_stay_scoped(fhir_source):
    connection, schema, _, sites = fhir_source
    await _resource(connection, schema, "Organization", "provider-one", {"npi": 1000000491})
    await _resource(connection, schema, "InsurancePlan", "plan-one", {"network_refs": ["Organization/network-one"]})
    await connection.execute(
        f"INSERT INTO \"{schema}\".provider_directory_dataset_network_plan VALUES('edition-one','network-one','plan-one')"
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_insurance_network_plan_evidence VALUES '
        "('source-example','release-one','Organization','network-one','plan-one','[\"Organization/network-one\"]',NULL,NULL,$1,'{}',now())",
        "a" * 64,
    )
    await _resource(
        connection,
        schema,
        "OrganizationAffiliation",
        "affiliation-one",
        {
            "participating_organization_ref": "Organization/provider-one",
            "location_refs": ["Location/office-two"],
            "insurance_plan_refs": ["InsurancePlan/plan-one"],
            "active": True,
        },
    )
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    batch = await _read(fhir_source)
    assert batch.membership_rows == 2 and batch.unresolved_rows == 0
    assert {entry["location_id"] for entry in json.loads(batch.input_bytes)} == {str(site) for site in sites}
    await connection.execute(f'DELETE FROM "{schema}".provider_directory_dataset_network_plan')
    assert await _read(fhir_source) == batch
    await connection.execute(f'DELETE FROM "{schema}".provider_directory_insurance_network_plan_evidence')
    changed = await _read(fhir_source)
    assert changed.membership_rows == 1 and changed.unresolved_rows == 1


@pytest.fixture
async def cms_fhir_source(fhir_source, serving_schema):
    """Retain real CMS witness models with distinct raw and normalized hashes."""
    connection, schema, source_pin, sites = fhir_source
    metadata = MetaData()
    for model in (
        ProviderDirectoryCMSNPDResourceWitness,
        ProviderDirectoryDatasetResource,
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryAPIEndpoint,
    ):
        model.__table__.to_metadata(metadata, schema=schema)
    async with serving_schema[2].begin() as ddl:
        await ddl.run_sync(metadata.create_all)
    await _resource(connection, schema, "InsurancePlan", "plan-one", {"network_refs": ["Organization/network-one"]})
    await _resource(
        connection,
        schema,
        "PractitionerRole",
        "role-one",
        _role(network_refs=[], insurance_plan_refs=["InsurancePlan/plan-one"]),
    )
    release = "e" * 64
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_entity_source_binding '
        f"SELECT 'cms-npd',resource_type,resource_id,organization_id,site_id,created_at "
        f'FROM "{schema}".provider_directory_entity_source_binding',
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_insurance_network_source_binding '
        f"SELECT 'cms-npd',resource_type,resource_id,network_id,created_at "
        f'FROM "{schema}".provider_directory_insurance_network_source_binding',
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_entity_release_evidence '
        f"SELECT 'cms-npd',resource_type,resource_id,$1,$2,payload_json,observed_at "
        f'FROM "{schema}".provider_directory_entity_release_evidence',
        release,
        "b" * 64,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_insurance_network_plan_evidence VALUES '
        "('cms-npd',$1,'Organization','network-one','plan-one','[]',NULL,NULL,$2,'{}',now())",
        release,
        "b" * 64,
    )
    await connection.execute(f"UPDATE \"{schema}\".network_registry_alias SET source_id='cms-npd'")
    await connection.execute(f'UPDATE "{schema}".provider_directory_dataset_resource SET acquired_resource_sha256=NULL')
    await connection.execute(
        f'UPDATE "{schema}".provider_directory_endpoint_dataset '
        'SET publication_metadata_summary_json=\'{"source_ids":["cms-npd"]}\'',
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_cms_npd_resource_witness '
        "SELECT dataset_id,'cms-npd',$1,resource_type,resource_id,$2,payload_hash,'{}'::jsonb "
        f'FROM "{schema}".provider_directory_dataset_resource',
        release,
        "b" * 64,
    )
    source_pin = replace(source_pin, source_id="cms-npd", release_id=release)
    return connection, schema, source_pin, sites


@pytest.mark.parametrize("damage", [None, "missing", "release", "normalized", "raw", "contradiction"])
async def test_cms_exact_membership_requires_release_bound_raw_witness(cms_fhir_source, damage):
    """A NULL acquired digest resolves only through corroborated CMS raw witnesses."""
    connection, schema, _, sites = cms_fhir_source
    complete = await _read(cms_fhir_source)
    assert complete.membership_rows == 1 and complete.unresolved_rows == 0
    assert json.loads(complete.input_bytes)[0]["location_id"] == str(sites[0])
    if damage:
        change = {
            "missing": "DELETE FROM {namespace}.provider_directory_cms_npd_resource_witness",
            "release": "UPDATE {namespace}.provider_directory_cms_npd_resource_witness SET release_id='"
            + "f" * 64
            + "'",
            "normalized": "UPDATE {namespace}.provider_directory_cms_npd_resource_witness SET normalized_payload_hash='"
            + "c" * 64
            + "'",
            "raw": "UPDATE {namespace}.provider_directory_cms_npd_resource_witness SET raw_payload_sha256='"
            + "c" * 64
            + "'",
            "contradiction": "UPDATE {namespace}.provider_directory_dataset_resource SET acquired_resource_sha256='"
            + "c" * 64
            + "'",
        }[damage]
        await connection.execute(change.format(namespace='"' + schema + '"'))
        rejected = await _read(cms_fhir_source)
        assert rejected.membership_rows == 0 and rejected.unresolved_rows > 0


async def test_retained_generation_reread_cursor_and_native_fixed_queries(fhir_source):
    connection, schema, source_pin, sites = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    original = await _read(fhir_source)
    await connection.execute(
        f"UPDATE \"{schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
    )
    await _generation(connection, schema, "edition-two", "b" * 64)
    await _resource(connection, schema, "PractitionerRole", "role-one", _role("office-two"), dataset="edition-two")
    assert await _read(fhir_source) == original
    assert (await _read(fhir_source, after_resource=original.next_resource)).next_resource is None
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_dataset_resource '
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json,acquired_resource_sha256) "
        "SELECT 'edition-one','PractitionerRole','role-'||lpad(n::text,5,'0'),$1,jsonb_build_object("
        "'resource_id','role-'||lpad(n::text,5,'0'),'npi',1000000491,'location_refs',jsonb_build_array('Location/office-one'),"
        "'network_refs',jsonb_build_array('Organization/network-one')),$1 FROM generate_series(1,4999) n",
        "a" * 64,
    )
    logged_queries = []
    connection.add_query_logger(logged_queries.append)
    try:
        counts = []
        async with connection.transaction():
            for limit in (1, 5000):
                await asyncio.sleep(0)
                logged_queries.clear()
                batch = await read_fhir_membership_batch(connection, source_pin, registry_schema=schema, limit=limit)
                await asyncio.sleep(0)
                counts.append(len(logged_queries))
                assert batch.membership_rows == limit and batch.unresolved_rows == 0
                assert all(entry["location_id"] == str(sites[0]) for entry in json.loads(batch.input_bytes))
        assert counts == [2, 2]
    finally:
        connection.remove_query_logger(logged_queries.append)


@pytest.mark.parametrize(
    "change",
    [
        {"dataset_sha256": "b" * 64},
        {"dataset_id": "missing"},
        {"source_id": "another-source"},
        {"endpoint_id": "other-endpoint"},
        {"resource_table_oid": 1},
    ],
)
async def test_pinned_scope_and_physical_identity_reject_drift(fhir_source, change):
    connection, schema, source_pin, _ = fhir_source
    async with connection.transaction():
        with pytest.raises(FHIRMembershipSourceError, match="unavailable"):
            await read_fhir_membership_batch(connection, replace(source_pin, **change), registry_schema=schema)


async def test_release_mismatch_raw_evidence_and_expansion_cap(fhir_source):
    connection, schema, source_pin, _ = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    async with connection.transaction():
        batch = await read_fhir_membership_batch(
            connection, replace(source_pin, release_id="wrong-release"), registry_schema=schema
        )
    assert batch.unresolved_rows == 1 and batch.membership_rows == 0
    await connection.execute(
        f"UPDATE \"{schema}\".provider_directory_entity_release_evidence SET payload_sha256=$1 WHERE resource_type='Location'",
        "b" * 64,
    )
    assert (await _read(fhir_source)).unresolved_rows == 1
    await connection.execute(
        f"UPDATE \"{schema}\".provider_directory_dataset_resource SET payload_json=jsonb_set(payload_json::jsonb,'{{location_refs}}',$1::jsonb) WHERE resource_type='PractitionerRole'",
        json.dumps(["Location/office-one"] * 5001),
    )
    with pytest.raises(FHIRMembershipSourceError, match="bounds"):
        await _read(fhir_source)


async def test_inactive_and_period_scopes_are_explicit(fhir_source):
    connection, schema, _, _ = fhir_source
    for resource_id, change in (
        ("inactive", {"active": False}),
        ("expired", {"period_end": "2025-01-01"}),
        ("future", {"period_start": "2027-01-01"}),
        ("current", {"period_start": "2026-01-01"}),
    ):
        await _resource(connection, schema, "PractitionerRole", resource_id, _role(**change))
    batch = await _read(fhir_source)
    assert batch.source_rows == 4 and batch.membership_rows == 1 and batch.unresolved_rows == 0


async def test_ambiguous_site_binding_and_alias_scope_never_guess(fhir_source):
    connection, schema, source_pin, sites = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    async with connection.transaction():
        batch = await read_fhir_membership_batch(
            connection, replace(source_pin, alias_scope="other-scope"), registry_schema=schema
        )
    assert batch.membership_rows == 0 and batch.unresolved_rows == 1
    await connection.execute(
        f"UPDATE \"{schema}\".provider_directory_entity_source_binding SET site_id=$1 WHERE resource_id='office-two'",
        sites[0],
    )
    batch = await _read(fhir_source)
    assert batch.membership_rows == 0 and batch.unresolved_rows == 1


async def test_relation_pin_holds_native_ddl_lock_and_copy_needs_caller(fhir_source):
    connection, schema, source_pin, _ = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    async with connection.transaction():
        await read_fhir_membership_batch(connection, source_pin, registry_schema=schema)
        assert await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND relation=$1::oid "
            "AND mode='AccessShareLock' AND granted)",
            source_pin.resource_table_oid,
        )
    batch = await _read(fhir_source)
    with pytest.raises(FHIRMembershipSourceError, match="caller transaction"):
        await copy_fhir_membership_batch(connection, batch, None, require_candidate_authority=None)


async def test_native_invalid_npi_rejects_whole_copy(fhir_source):
    connection, schema, _, _ = fhir_source
    await _resource(connection, schema, "PractitionerRole", "role-one", _role(npi=1234567890, practitioner_ref=None))
    batch = await _read(fhir_source)
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )

    async def authority(connection, requested):
        return requested

    async with connection.transaction():
        with pytest.raises(ValueError, match="invalid_npi"):
            await copy_fhir_membership_batch(connection, batch, copy_target, require_candidate_authority=authority)


async def test_bad_cursor_bounds_and_unpublished_source_fail_before_extraction(fhir_source):
    connection, schema, source_pin, _ = fhir_source
    for options_by_name in (
        {"limit": True},
        {"limit": 0},
        {"limit": 5001},
        {"after_resource": ([], "role-one")},
        {"after_resource": ("PractitionerRole", "")},
        {"after_resource": ("Location", "office-one")},
    ):
        async with connection.transaction():
            with pytest.raises(FHIRMembershipSourceError):
                await read_fhir_membership_batch(connection, source_pin, registry_schema=schema, **options_by_name)
    await connection.execute(
        f"UPDATE \"{schema}\".provider_directory_endpoint_dataset SET status='validated',published_at=NULL"
    )
    with pytest.raises(FHIRMembershipSourceError, match="unavailable"):
        await _read(fhir_source)


async def _reviewed_binding(connection, schema, actor, rows):
    command = NetworkSourceBindingBatchCommand(
        json.dumps(rows).encode(), "Reviewed exact FHIR organization", uuid4().hex
    )
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


async def _approved_pin(connection, schema):
    revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
    async with connection.transaction(isolation="repeatable_read"):
        return await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)


async def _admit_binding_coordinates(connection, source, coordinates):
    descriptor_dict = {
        **dict(
            zip(
                ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
                coordinates.sql_parameters,
                strict=True,
            )
        ),
        "alias_scope": source.alias_scope,
        "source_key_kind": "organization_resource_id",
    }
    await connection.execute(
        f'UPDATE "{source.schema_name}".provider_directory_endpoint_dataset '
        "SET publication_metadata_summary_json=jsonb_set(publication_metadata_summary_json::jsonb,"
        "'{network_bindings}',$1::jsonb) WHERE dataset_id=$2",
        json.dumps(descriptor_dict),
        source.dataset_id,
    )


@pytest.fixture
async def reviewed_fhir_source(fhir_source, serving_schema):
    connection, schema, source_pin, sites = fhir_source
    _, _, engine = serving_schema
    actor = _actor()
    networks = [await _draft(engine, schema, _create("network"), actor) for _ in range(2)]
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source_pin.source_id,
        source_pin.schema_name,
        source_pin.dataset_id,
        "producer-one",
        "binding-edition-one",
    )
    await _admit_binding_coordinates(connection, source_pin, coordinates)
    legacy_id = await connection.fetchval(
        f'SELECT network_id FROM "{schema}".provider_directory_insurance_network_source_binding'
    )
    binding_dict = {
        "binding_id": str(uuid4()),
        "source_system": "fhir",
        "source_id": source_pin.source_id,
        "dataset_schema": schema,
        "dataset_id": source_pin.dataset_id,
        "producer_id": coordinates.producer_id,
        "edition_id": coordinates.edition_id,
        "source_key": "network-one",
        "source_scope_json": {
            "organization_id": "network-one",
            "legacy_uuid": str(legacy_id),
            "alias_scope": source_pin.alias_scope,
        },
        "network_id": networks[0]["record_id"],
        "evidence_id": "review-one",
        "evidence_sha256": "e" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }
    await connection.execute(f'DELETE FROM "{schema}".network_registry_alias')
    binding = (await _reviewed_binding(connection, schema, actor, [binding_dict]))["records"][0]
    await _approve(connection, schema, await _command(connection, schema, *networks, binding), actor)
    await _resource(connection, schema, "PractitionerRole", "role-one", _role())
    return fhir_source, actor, networks, binding_dict, coordinates, await _approved_pin(connection, schema)


async def _reviewed_read(fixture, *, pin=None, coordinates=None, **options):
    source_fixture, _, _, _, original_coordinates, original_pin = fixture
    connection, schema, source, _ = source_fixture
    async with connection.transaction(isolation="repeatable_read"):
        return await read_fhir_membership_batch(
            connection,
            source,
            registry_schema=schema,
            approved_source=pin or original_pin,
            binding_coordinates=coordinates or original_coordinates,
            **options,
        )


async def test_native_recipe_pages_split_large_fanout_without_truncation(reviewed_fhir_source):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source_pin, original_sites = fixture
    sites = (*original_sites, *(uuid4() for _ in range(4)))
    location_ids = ["office-one", "office-two", *("extra-office-" + str(number) for number in range(4))]
    for resource_id, site in zip(location_ids[2:], sites[2:], strict=True):
        await _resource(connection, schema, "Location", resource_id, {"status": "active"})
        await _evidence(connection, schema, "Location", resource_id, site)
    await connection.execute(
        f"DELETE FROM \"{schema}\".provider_directory_dataset_resource WHERE resource_type='PractitionerRole'"
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_dataset_resource '
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json,acquired_resource_sha256) "
        "SELECT $1,'PractitionerRole','bulk-role-'||lpad(number::text,4,'0'),$2,"
        "jsonb_set($3::jsonb,'{resource_id}',to_jsonb('bulk-role-'||lpad(number::text,4,'0'))),$2 "
        "FROM generate_series(1,1001) number",
        source_pin.dataset_id,
        "a" * 64,
        json.dumps(_role(location_refs=["Location/" + office_id for office_id in location_ids])),
    )
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    cursor, limit, seen = None, 1000, set()
    source_rows = membership_rows = 0
    async with connection.transaction(isolation="repeatable_read"):
        for _ in range(4):
            batch, next_cursor, limit = await _read_recipe_page(connection, recipe, pin, cursor, schema, limit)
            assert limit == 500 and batch.membership_rows <= 5000 and batch.unresolved_rows == 0
            memberships = json.loads(batch.input_bytes)
            assert {member["location_id"] for member in memberships} <= {str(site) for site in sites}
            seen.update(member["evidence_id"] for member in memberships)
            source_rows += batch.source_rows
            membership_rows += batch.membership_rows
            if not batch.source_rows:
                break
            assert next_cursor != cursor
            cursor = next_cursor
        else:
            pytest.fail("Recipe did not reach its empty terminal page")
    assert source_rows == 1001 and membership_rows == len(seen) == 6006


@pytest.mark.parametrize("error", [FHIRMembershipBatchBoundsError, FHIRMembershipSourceError])
async def test_recipe_split_stops_at_one_and_never_retries_invalid_source(reviewed_fhir_source, monkeypatch, error):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source, _ = fixture
    limits = []

    async def reject(*args, limit, **options):
        limits.append(limit)
        raise error("rejected source page")

    monkeypatch.setattr("process.registry_source_recipe_composition.read_fhir_membership_batch", reject)
    with pytest.raises(error):
        await _read_recipe_page(connection, RegistrySourceMembershipRecipe(source, coordinates), pin, None, schema, 2)
    assert limits == ([2, 1] if error is FHIRMembershipBatchBoundsError else [2])


@pytest.mark.parametrize("approved_only", [None, 0, 1, "true", [], {}])
async def test_selection_policy_requires_exact_boolean(reviewed_fhir_source, approved_only):
    with pytest.raises(FHIRMembershipSourceError, match="source is invalid"):
        await _reviewed_read(reviewed_fhir_source, approved_only=approved_only)


async def test_legacy_source_cannot_select_approved_only(fhir_source):
    with pytest.raises(FHIRMembershipSourceError, match="coordinates are invalid"):
        await _read(fhir_source, approved_only=True)


async def test_approved_only_preserves_strict_hash_and_empty_accounting(reviewed_fhir_source):
    strict = await _reviewed_read(reviewed_fhir_source)
    expected_recipe_dict = {
        "source": strict.source.coordinates,
        "approved_source": asdict(strict.approved_source),
        "binding_coordinates": asdict(strict.binding_coordinates),
    }
    assert (
        strict.generation_id
        == hashlib.sha256(json.dumps(expected_recipe_dict, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    selected = await _reviewed_read(reviewed_fhir_source, approved_only=True)
    assert selected.approved_only and selected.omitted_rows == selected.unresolved_rows == 0
    assert selected.membership_rows == 1 and selected.generation_id != strict.generation_id
    assert selected.input_bytes == strict.input_bytes
    empty = await _reviewed_read(reviewed_fhir_source, approved_only=True, after_resource=selected.next_resource)
    assert (empty.source_rows, empty.membership_rows, empty.omitted_rows, empty.unresolved_rows) == (0, 0, 0, 0)
    assert empty.next_resource is None and empty.input_bytes == b"[]"


async def _approve_fhir_binding_command(connection, schema, actor, binding):
    """Persist and selectively approve one exact reviewed binding transition."""
    receipt = (await _reviewed_binding(connection, schema, actor, [binding]))["records"][0]
    await _approve(connection, schema, await _command(connection, schema, receipt), actor)


async def test_approved_rebind_overrides_observed_alias_and_close_omits(reviewed_fhir_source):
    """Approved rebind and closure leave the observed source alias unchanged."""
    fixture, actor, networks, binding, _, _ = reviewed_fhir_source
    connection, schema, source_pin, sites = fixture
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES '
        "('fhir',$1,'legacy_fhir_uuid',$2,$3,$4,'retained-observation',now())",
        source_pin.source_id,
        binding["source_scope_json"]["legacy_uuid"],
        source_pin.alias_scope,
        networks[0]["record_id"],
    )
    await _approve_fhir_binding_command(
        connection,
        schema,
        actor,
        {
            **binding,
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": binding["network_id"],
            "network_id": networks[1]["record_id"],
        },
    )
    pin = await _approved_pin(connection, schema)
    strict = await _reviewed_read(reviewed_fhir_source, pin=pin)
    assert (strict.membership_rows, strict.unresolved_rows, strict.omitted_rows) == (0, 1, 0)
    selected = await _reviewed_read(reviewed_fhir_source, pin=pin, approved_only=True)
    assert (selected.membership_rows, selected.unresolved_rows, selected.omitted_rows) == (1, 0, 0)
    assert json.loads(selected.input_bytes)[0]["network_id"] == networks[1]["record_id"]
    assert json.loads(selected.input_bytes)[0]["location_id"] == str(sites[0])
    await _approve_fhir_binding_command(
        connection,
        schema,
        actor,
        {
            **binding,
            "operation": "close",
            "expected_revision": 2,
            "expected_network_id": networks[1]["record_id"],
            "network_id": networks[1]["record_id"],
        },
    )
    omitted = await _reviewed_read(
        reviewed_fhir_source, pin=await _approved_pin(connection, schema), approved_only=True
    )
    assert (omitted.source_rows, omitted.membership_rows, omitted.unresolved_rows, omitted.omitted_rows) == (1, 0, 0, 1)
    assert omitted.input_bytes == b"[]"
    assert (
        await connection.fetchval(f'SELECT network_id FROM "{schema}".network_registry_alias')
        == networks[0]["record_id"]
    )


@pytest.mark.parametrize("missing", ["network", "plan", "location", "provider"])
async def test_missing_selected_lineage_is_not_intentional_omission(reviewed_fhir_source, missing):
    connection, schema, _, _ = reviewed_fhir_source[0]
    changes_by_missing = {
        "network": {"network_refs": ["Organization/missing"]},
        "plan": {"network_refs": [], "insurance_plan_refs": ["InsurancePlan/missing"]},
        "location": {"location_refs": ["Location/missing"]},
        "provider": {"practitioner_ref": "Practitioner/missing"},
    }
    await _resource(connection, schema, "PractitionerRole", "zz-last", _role(**changes_by_missing[missing]))
    batch = await _reviewed_read(reviewed_fhir_source, approved_only=True)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows, batch.omitted_rows) == (2, 1, 1, 0)
    with pytest.raises(FHIRMembershipSourceError, match="unresolved"):
        await copy_fhir_membership_batch(connection, batch, None, require_candidate_authority=None)


async def test_reviewed_approval_rebind_and_close_change_only_new_extraction(reviewed_fhir_source):
    fixture, actor, networks, binding_dict, _, original_pin = reviewed_fhir_source
    connection, schema, source_pin, sites = fixture
    original = await _reviewed_read(reviewed_fhir_source)
    assert original.generation_id != source_pin.generation_id
    assert original.approved_source == original_pin
    assert json.loads(original.input_bytes)[0]["location_id"] == str(sites[0])
    assert json.loads(original.input_bytes)[0]["network_id"] == networks[0]["record_id"]
    rebind_dict = {
        **binding_dict,
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": binding_dict["network_id"],
        "network_id": networks[1]["record_id"],
    }
    pending = (await _reviewed_binding(connection, schema, actor, [rebind_dict]))["records"][0]
    assert await _reviewed_read(reviewed_fhir_source) == original
    await _approve(connection, schema, await _command(connection, schema, pending), actor)
    second_pin = await _approved_pin(connection, schema)
    updated = await _reviewed_read(reviewed_fhir_source, pin=second_pin)
    assert updated.generation_id != original.generation_id
    assert json.loads(updated.input_bytes)[0]["network_id"] == networks[1]["record_id"]
    with pytest.raises(FHIRMembershipSourceError, match="bindings are unavailable"):
        await _reviewed_read(reviewed_fhir_source)
    closed = (
        await _reviewed_binding(
            connection,
            schema,
            actor,
            [
                {
                    **rebind_dict,
                    "operation": "close",
                    "expected_revision": 2,
                    "expected_network_id": rebind_dict["network_id"],
                }
            ],
        )
    )["records"][0]
    assert await _reviewed_read(reviewed_fhir_source, pin=second_pin) == updated
    await _approve(connection, schema, await _command(connection, schema, closed), actor)
    # An agreeing legacy alias must not resurrect a closed reviewed binding.
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES '
        "('fhir',$1,'legacy_fhir_uuid',$2,$3,$4,'reviewed-example',now())",
        source_pin.source_id,
        binding_dict["source_scope_json"]["legacy_uuid"],
        source_pin.alias_scope,
        rebind_dict["network_id"],
    )
    closed_batch = await _reviewed_read(reviewed_fhir_source, pin=await _approved_pin(connection, schema))
    assert closed_batch.membership_rows == 0 and closed_batch.unresolved_rows == 1
    assert json.loads(original.input_bytes)[0]["network_id"] == networks[0]["record_id"]


@pytest.mark.parametrize(
    "metadata", [None, {}, {"producer_id": "foreign"}, {"source_key_kind": "other"}, {"extra": True}]
)
async def test_reviewed_metadata_requires_closed_admitted_descriptor(reviewed_fhir_source, metadata):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source, _ = fixture
    descriptor_dict = {
        **dict(
            zip(
                ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
                coordinates.sql_parameters,
                strict=True,
            )
        ),
        "alias_scope": source.alias_scope,
        "source_key_kind": "organization_resource_id",
    }
    value = metadata if metadata is None or not metadata else {**descriptor_dict, **metadata}
    await connection.execute(
        f'UPDATE "{schema}".provider_directory_endpoint_dataset SET publication_metadata_summary_json=$1::json',
        json.dumps({"source_ids": [source.source_id], "network_bindings": value}),
    )
    counted = _CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(FHIRMembershipSourceError, match="unavailable"):
            await read_fhir_membership_batch(
                counted, source, registry_schema=schema, approved_source=pin, binding_coordinates=coordinates
            )
    assert counted.statements == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"source_system": "ptg"},
        {"source_id": "foreign"},
        {"dataset_schema": "foreign"},
        {"dataset_id": "foreign"},
        {"producer_id": "foreign"},
        {"edition_id": "foreign"},
    ],
)
async def test_reviewed_coordinate_drift_never_crossmatches(reviewed_fhir_source, changes):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source, _ = fixture
    counted = _CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(FHIRMembershipSourceError):
            await read_fhir_membership_batch(
                counted,
                source,
                registry_schema=schema,
                approved_source=pin,
                binding_coordinates=replace(coordinates, **changes),
            )
    assert counted.statements == int("producer_id" in changes or "edition_id" in changes)


@pytest.mark.parametrize("side", ["approved_source", "binding_coordinates"])
async def test_reviewed_pins_must_be_paired_before_sql(reviewed_fhir_source, side):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source, _ = fixture
    counted = _CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(FHIRMembershipSourceError, match="coordinates"):
            await read_fhir_membership_batch(
                counted, source, registry_schema=schema, **{side: pin if side == "approved_source" else coordinates}
            )
    assert counted.statements == 0


@pytest.mark.parametrize("isolation", ["read_committed", "repeatable_read"])
async def test_reviewed_reads_require_isolation_and_exact_approved_fingerprint(reviewed_fhir_source, isolation):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source_pin, _ = fixture
    selected_pin = replace(pin, generation_id="f" * 64) if isolation == "repeatable_read" else pin
    counted = _CountedConnection(connection)
    async with connection.transaction(isolation=isolation):
        with pytest.raises(FHIRMembershipSourceError, match="bindings are unavailable"):
            await read_fhir_membership_batch(
                counted,
                source_pin,
                registry_schema=schema,
                approved_source=selected_pin,
                binding_coordinates=coordinates,
            )
    assert counted.statements == 2


async def test_reviewed_alias_conflict_and_missing_mapping_never_fallback(reviewed_fhir_source):
    fixture, _, networks, row, _, _ = reviewed_fhir_source
    connection, schema, source, _ = fixture
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES '
        "('fhir',$1,'legacy_fhir_uuid',$2,$3,$4,'reviewed-example',now())",
        source.source_id,
        row["source_scope_json"]["legacy_uuid"],
        source.alias_scope,
        networks[1]["record_id"],
    )
    conflicted = await _reviewed_read(reviewed_fhir_source)
    assert conflicted.membership_rows == 0 and conflicted.unresolved_rows == 1
    await connection.execute(f'DELETE FROM "{schema}".network_registry_alias')
    await connection.execute(
        f'UPDATE "{schema}".provider_directory_entity_release_evidence SET payload_sha256=$1 '
        "WHERE resource_type='Organization'",
        "b" * 64,
    )
    assert (await _reviewed_read(reviewed_fhir_source)).unresolved_rows == 1


async def test_reviewed_retained_edition_and_keyset_ignore_new_source_head(reviewed_fhir_source):
    fixture, _, _, _, _, _ = reviewed_fhir_source
    connection, schema, source_pin, sites = fixture
    original = await _reviewed_read(reviewed_fhir_source)
    await connection.execute(
        f'UPDATE "{schema}".provider_directory_endpoint_dataset '
        "SET status='superseded',is_current=false WHERE dataset_id=$1",
        source_pin.dataset_id,
    )
    await _generation(connection, schema, "edition-two", "b" * 64)
    await _resource(connection, schema, "PractitionerRole", "role-one", _role("office-two"), dataset="edition-two")
    assert await _reviewed_read(reviewed_fhir_source) == original
    assert json.loads(original.input_bytes)[0]["location_id"] == str(sites[0])
    terminal = await _reviewed_read(reviewed_fhir_source, after_resource=original.next_resource)
    assert terminal.source_rows == terminal.membership_rows == terminal.unresolved_rows == 0
    assert terminal.next_resource is None and terminal.generation_id == original.generation_id


@pytest.mark.parametrize(
    "field", ["source_key", "organization_id", "legacy_uuid", "alias_scope", "producer_id", "edition_id"]
)
async def test_reviewed_exact_source_key_and_full_scope_are_required(reviewed_fhir_source, field):
    fixture, actor, networks, row, coordinates, _ = reviewed_fhir_source
    connection, schema, _, _ = fixture
    foreign_dict = {**row, "binding_id": str(uuid4()), "source_scope_json": dict(row["source_scope_json"])}
    if field in foreign_dict["source_scope_json"]:
        foreign_dict["source_scope_json"][field] = str(uuid4()) if field == "legacy_uuid" else "foreign"
    else:
        foreign_dict[field] = "foreign"
    binding = (await _reviewed_binding(connection, schema, actor, [foreign_dict]))["records"][0]
    closed = (
        await _reviewed_binding(
            connection,
            schema,
            actor,
            [{**row, "operation": "close", "expected_revision": 1, "expected_network_id": row["network_id"]}],
        )
    )["records"][0]
    await _approve(connection, schema, await _command(connection, schema, binding, closed), actor)
    batch = await _reviewed_read(
        reviewed_fhir_source, pin=await _approved_pin(connection, schema), coordinates=coordinates
    )
    assert batch.membership_rows == 0 and batch.unresolved_rows == 1


@pytest.mark.parametrize("approved_only", [False, True])
async def test_reviewed_fixed_query_count_and_last_unresolved_prevents_copy(reviewed_fhir_source, approved_only):
    fixture, _, _, _, coordinates, pin = reviewed_fhir_source
    connection, schema, source_pin, _ = fixture
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_dataset_resource '
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json,acquired_resource_sha256) "
        "SELECT 'edition-one','PractitionerRole','role-'||lpad(n::text,5,'0'),$1,jsonb_build_object("
        "'resource_id','role-'||lpad(n::text,5,'0'),'npi',1000000491,'location_refs',jsonb_build_array('Location/office-one'),"
        "'network_refs',jsonb_build_array('Organization/network-one')),$1 FROM generate_series(1,99) n",
        "a" * 64,
    )
    for limit in (1, 100):
        counted = _CountedConnection(connection)
        async with connection.transaction(isolation="repeatable_read"):
            batch = await read_fhir_membership_batch(
                counted,
                source_pin,
                registry_schema=schema,
                limit=limit,
                approved_source=pin,
                binding_coordinates=coordinates,
                approved_only=approved_only,
            )
        assert counted.statements == 4 and batch.membership_rows == limit and batch.unresolved_rows == 0
    await _resource(connection, schema, "PractitionerRole", "zz-unresolved", _role("missing"))
    batch = await _reviewed_read(reviewed_fhir_source, approved_only=approved_only)
    assert batch.membership_rows == 100 and batch.unresolved_rows == 1
    counted = _CountedConnection(connection)
    with pytest.raises(FHIRMembershipSourceError, match="unresolved"):
        await copy_fhir_membership_batch(counted, batch, None, require_candidate_authority=None, control_schema=schema)
    assert counted.statements == 0


@pytest.mark.parametrize("approved_only", [False, True])
@pytest.mark.parametrize("bad_pin", [None, "approved_revision", "approved_generation", "fhir_generation", "policy"])
async def test_reviewed_copy_requires_actual_candidate_recipe_and_rolls_back(
    reviewed_fhir_source, bad_pin, approved_only
):
    fixture, _, _, _, _, pin = reviewed_fhir_source
    connection, schema, _, sites = fixture
    batch = await _reviewed_read(reviewed_fhir_source, approved_only=approved_only)
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    generation_by_source = {"custom_membership": pin.generation_id, "fhir:source-example": batch.generation_id}
    if bad_pin == "approved_generation":
        generation_by_source["custom_membership"] = "b" * 64
    if bad_pin == "fhir_generation":
        generation_by_source["fhir:source-example"] = batch.source.generation_id
    if bad_pin == "policy":
        generation_by_source["fhir:source-example"] = replace(batch, approved_only=not approved_only).generation_id
    try:
        async with connection.transaction():
            await create_network_candidate(
                connection,
                copy_target,
                source_generations=generation_by_source,
                approved_custom_revision=pin.approved_revision + int(bad_pin == "approved_revision"),
                expected_head=0,
                expected_rows=1,
                control_schema=schema,
            )

        async def authority(connection, requested):
            """Use the actual candidate control lock and ownership check."""
            assert (await _locked_candidate(connection, requested, '"' + schema + '"'))["state"] == "open"
            return requested

        transaction = connection.transaction()
        await transaction.start()
        if bad_pin is not None:
            with pytest.raises(FHIRMembershipSourceError, match="candidate pin"):
                await copy_fhir_membership_batch(
                    connection, batch, copy_target, require_candidate_authority=authority, control_schema=schema
                )
        else:
            receipt = await copy_fhir_membership_batch(
                connection, batch, copy_target, require_candidate_authority=authority, control_schema=schema
            )
            assert receipt.row_count == 1
            assert (
                await connection.fetchval(f'SELECT location_id FROM "{copy_target.schema_name}".network_membership')
                == sites[0]
            )
        await transaction.rollback()
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 0
    finally:
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
