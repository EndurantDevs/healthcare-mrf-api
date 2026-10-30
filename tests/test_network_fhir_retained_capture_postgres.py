# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Reader capture of synthetic legacy FHIR through native PostgreSQL."""

import asyncio
import hashlib
import importlib.util
import json
import os
import secrets
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy import MetaData

from db.models import EntityAddressUnified
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_candidate_custody import (
    NetworkCandidateCustodyError,
    seal_network_candidate_custody,
    verify_network_candidate_custody,
)
from process.network_fhir_membership_source import (
    PinnedFHIRMembershipSource,
    read_retained_fhir_membership_batch,
)
from process.network_fhir_retained_capture import (
    capture_retained_fhir_source,
    pin_retained_fhir_read,
    require_retained_fhir_capture,
)
from process.network_fhir_retained_capture_contract import (
    REFUSAL,
    SEMANTIC_PROFILE,
    RetainedFHIRCaptureError,
    RetainedFHIRInput,
)
from process.network_initial_cms_office_bindings import (
    InitialCMSOfficeBindingError,
    pin_cms_office_address_source,
)
from process.network_membership_pipeline import (
    prepare_and_publish_network_candidate,
    prepare_network_candidate,
)
from process.network_membership_writer_closure import NetworkWriterClosureError
from process.network_serving_read import resolve_network_serving_manifest
from process.network_source_binding_store import (
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_source_office_errors import SourceOfficeNotReady
from process.registry_source_recipe_composition import (
    RegistrySourceCompositionError,
    SourceRecipeReadContext,
    _read_recipe_page,
    _require_recipe_custody,
    stage_registry_source_recipes,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_address_projection_postgres import (
    projection_db as projection_db,
)
from tests.test_network_custom_address_source_postgres import (
    _bootstrap_prepare,
    _bootstrap_specification,
)
from tests.test_network_custom_address_source_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_network_membership_pipeline_postgres import pipeline_db as pipeline_db
from tests.test_network_membership_publication_postgres import _restore_candidate_owners
from tests.test_network_membership_validation_postgres import (
    validation_db as validation_db,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import (
    _actor,
    _approve,
    _command,
    _create,
    _draft,
)

pytestmark = pytest.mark.asyncio

_MODERN_ROLE_CASE_BY_PROFILE = {
    "modern": False,
    "modern_missing": True,
    "modern_mixed": "mixed",
    "modern_null": "null",
    "modern_unknown_plan": "unknown_plan",
}


def _contents(source_id):
    source_by_field = {"source_id": source_id}
    address_by_field = {
        "first_line": "123 Example St",
        "second_line": "Suite 1",
        "city_name": "Chicago",
        "state_name": "IL",
        "state_code": "IL",
        "postal_code": "60601",
        "country_code": "US",
    }
    resources = [
        ("Organization", "network-one", {"name": "Example network", "type_codes": ["ntwk"]}),
        ("Location", "office-one", address_by_field),
        ("Location", "office-two", {**address_by_field, "second_line": "Suite 2"}),
        ("Practitioner", "provider-one", {"npi": "1234567893"}),
        ("InsurancePlan", "plan-one", {"name": "Example plan", "network_refs": ["Organization/network-one"]}),
        (
            "PractitionerRole",
            "role-one",
            {
                "practitioner_ref": "Practitioner/provider-one",
                "npi": "1234567893",
                "location_refs": ["Location/office-one"],
                "network_refs": ["Organization/network-one"],
                "insurance_plan_refs": ["InsurancePlan/plan-one"],
            },
        ),
    ]
    return [
        (kind, identity, {**source_by_field, "resource_id": identity, **normalized_payload})
        for kind, identity, normalized_payload in resources
    ]


def _proof_rows(resources, hash_contract=LEGACY_RESOURCE_HASH_CONTRACT):
    kinds = (
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    )
    roots, counts, root = {kind: hashlib.sha256() for kind in kinds}, dict.fromkeys(kinds, 0), hashlib.sha256()
    records = []
    for kind, identity, payload in sorted(resources):
        payload_hash = resource_payload_sha256_for_contract(payload, hash_contract, resource_type=kind)
        encoded = json.dumps([kind, identity, payload_hash], separators=(",", ":")).encode()
        if records:
            root.update(b"\n")
        if counts[kind]:
            roots[kind].update(b"\n")
        roots[kind].update(encoded)
        root.update(encoded)
        counts[kind] += 1
        records.append(("dataset-one", kind, identity, payload_hash, json.dumps(payload), None))
    return records, root.hexdigest(), counts, {kind: value.hexdigest() for kind, value in roots.items()}


async def _legacy_tables(admin, schema):
    """Install only synthetic legacy source tables, deliberately no endpoint relation."""
    await admin.execute(f'''CREATE TABLE "{schema}".provider_directory_endpoint_dataset(
        dataset_id varchar(96) PRIMARY KEY,endpoint_id varchar(64),acquisition_root_run_id varchar(64),
        dataset_hash varchar(64),status varchar(32),is_current boolean,resource_count bigint,published_at timestamp,
        publication_metadata_json json,publication_metadata_summary_json jsonb,publication_metadata_sha256 varchar(64),
        content_proof_admission_version smallint,content_proof_admission_kind varchar(32),content_proof_admission_sha256 varchar(64),
        content_proof_resource_types varchar(64)[],candidate_storage_json jsonb,completion_proof_required_version integer);
        CREATE TABLE "{schema}".provider_directory_source(source_id varchar(64) PRIMARY KEY,endpoint_id varchar(64));
        CREATE TABLE "{schema}".provider_directory_dataset_resource(dataset_id varchar(96) NOT NULL,
        resource_type varchar(64) NOT NULL,resource_id varchar(256) NOT NULL,payload_hash varchar(64) NOT NULL,
        payload_json json NOT NULL,acquired_resource_sha256 varchar(64),PRIMARY KEY(dataset_id,resource_type,resource_id)) PARTITION BY LIST(dataset_id);
        CREATE TABLE "{schema}".retained_input PARTITION OF "{schema}".provider_directory_dataset_resource FOR VALUES IN ('dataset-one');
        CREATE INDEX ON "{schema}".provider_directory_dataset_resource(dataset_id,resource_type COLLATE "C",resource_id COLLATE "C")''')


async def _legacy_metadata(admin, schema, modern=False, missing_role=False):
    """Publish exact synthetic resources under either supported metadata profile."""
    resources = _modern_resources(missing_role) if modern else _contents("source-one")
    resource_records, dataset_hash, counts, hashes = _proof_rows(
        resources, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT if modern else LEGACY_RESOURCE_HASH_CONTRACT
    )
    summary = _fixture_summary(dataset_hash, counts, hashes, modern, resources)
    document = (
        _modern_metadata(resource_records, summary)
        if modern
        else {
            "source_ids": ["source-one"],
            "source_summary_v1": summary,
            "provider_directory_content_proof_v1": {"proof_sha256": "d" * 64},
        }
    )
    metadata = json.dumps(document)
    await admin.execute(
        f"INSERT INTO \"{schema}\".provider_directory_endpoint_dataset(dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,resource_count,published_at,publication_metadata_json) VALUES($1,$2,$3,$4,'published',true,$5,now(),$6::json)",
        "dataset-one",
        "endpoint-one",
        "run-one",
        dataset_hash,
        len(resource_records),
        metadata,
    )
    if modern:
        await _store_fixture_admission(admin, schema, document)
    await admin.execute(f"INSERT INTO \"{schema}\".provider_directory_source VALUES('source-one','endpoint-one')")
    await admin.copy_records_to_table(
        "provider_directory_dataset_resource", schema_name=schema, records=resource_records
    )
    return metadata, summary


def _modern_metadata(resource_records, summary):
    """Use production shard construction and complete semantic proof validation."""
    from process import provider_directory_proof_store as proofs

    descriptors = _modern_descriptors(resource_records)
    metrics_by_field = {
        name: summary[name]
        for name in ("address_records", "addressed_locations", "distinct_npis", "geocoded_locations")
    }
    scope = sorted(summary["resource_counts"])
    lineage = proofs._ProofLineage("dataset-one", "endpoint-one", "run-one", ["source-one"], scope, scope)
    merged = proofs._MergedDatasetProof(
        summary["dataset_hash"],
        summary["total_resources"],
        summary["resource_hashes"],
        summary["resource_counts"],
        metrics_by_field,
        hashlib.sha256(b"1234567893").hexdigest(),
        [],
        SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        {"added_name_count": 0, "collision_identities": 0, "observation_variants": 0, "union_name_count": 0},
    )
    proof = proofs._sealed_dataset_proof_metadata(lineage, merged, descriptors, "2026-01-01")
    return {
        "source_ids": ["source-one"],
        "dataset_hash": summary["dataset_hash"],
        "resource_count": summary["total_resources"],
        "source_summary_v1": summary,
        "dataset_network_plan": {
            "version": 1,
            "complete": True,
            "dataset_id": "dataset-one",
            "edge_count": 2,
            "unresolved_reference_count": 1,
            "build_run_id": "original-build",
            "replaced_edge_count": 0,
        },
        "expected_resources": scope,
        "selected_resources": scope,
        "proof_resource_scope": scope,
        "resource_hash_contract": SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-01-01",
        "provider_directory_content_proof_v1": proof,
    }


async def _legacy_guards(admin, schema, reader_role):
    """Use the existing authenticated ALWAYS guard, never a test bypass body."""
    path = Path(__file__).parents[1] / "alembic/versions/20260808190000_provider_directory_subset_completion_proof.py"
    spec = importlib.util.spec_from_file_location("retained_native_guard", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    await admin.execute(module._dataset_resource_guard_sql(schema, subset_aware=True))
    for action, old, new in (
        ("INSERT", None, "new_rows"),
        ("UPDATE", "old_rows", "new_rows"),
        ("DELETE", "old_rows", None),
        ("TRUNCATE", None, None),
    ):
        transition = " ".join(("OLD TABLE AS " + old if old else "", "NEW TABLE AS " + new if new else "")).strip()
        trigger = "retained_" + action.lower()
        timing = "BEFORE" if action == "TRUNCATE" else "AFTER"
        await admin.execute(
            f'CREATE TRIGGER {trigger} {timing} {action} ON "{schema}".provider_directory_dataset_resource '
            + ("REFERENCING " + transition + " " if transition else "")
            + f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".guard_tin_npi_connector_dataset_resource()'
        )
        await admin.execute(
            f'ALTER TABLE "{schema}".provider_directory_dataset_resource ENABLE ALWAYS TRIGGER {trigger}'
        )
    await admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader_role}"')
    await admin.execute(
        f'GRANT SELECT ON "{schema}".provider_directory_endpoint_dataset,"{schema}".provider_directory_source,"{schema}".provider_directory_dataset_resource TO "{reader_role}"'
    )


async def _legacy_input(admin, schema, reader_role, metadata, summary, modern=False):
    """Pin independently observed publication fields and their original refusal."""
    origin = RetainedFHIRInput(
        schema,
        "source-one",
        "endpoint-one",
        "dataset-one",
        summary["dataset_hash"],
        "run-one",
        await admin.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}".provider_directory_endpoint_dataset'),
        await admin.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}".provider_directory_dataset_resource'),
        await admin.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}".retained_input'),
        reader_role,
        len(metadata.encode()),
        hashlib.sha256(metadata.encode()).hexdigest(),
        summary["summary_sha256"],
        "d" * 64,
        tuple(sorted(summary["resource_counts"].items())),
        tuple(sorted(summary["resource_hashes"].items())),
        tuple(
            sorted(
                {**summary["resource_counts"], "Practitioner": summary["resource_counts"]["Practitioner"] + 1}.items()
            )
        ),
        REFUSAL,
        preparation_schema="retained_preparation_" + schema.removeprefix("legacy_fhir_test_"),
    )
    if modern:
        return _modern_origin(
            origin,
            metadata,
            await admin.fetchval(
                f'SELECT publication_metadata_sha256 FROM "{schema}".provider_directory_endpoint_dataset'
            ),
        )
    return origin


async def _legacy_candidate_guards(admin, schema, source_owner, captures):
    """Install existing canonical-copy body and parent/leaf statement guards."""
    from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
    from db.migration_provider_directory_dataset_candidates import TABLES as PARENTS

    preparation = "retained_preparation_" + schema.removeprefix("legacy_fhir_test_")
    await admin.execute("RESET ROLE")
    await admin.execute(f'CREATE SCHEMA "{preparation}"')
    captures.append(preparation)
    await admin.execute(
        f'CREATE TABLE "{preparation}".preparation(importer_id varchar,state varchar,handoff jsonb,publication jsonb)'
    )
    await admin.execute(f'GRANT USAGE ON SCHEMA "{preparation}" TO "{source_owner}"')
    await admin.execute(
        f'GRANT SELECT(importer_id,state,handoff,publication) ON "{preparation}".preparation TO "{source_owner}"'
    )
    await admin.execute(f'SET ROLE "{source_owner}"')
    for table in PARENTS[1:4]:
        await admin.execute(f'CREATE TABLE "{schema}"."{table}"(dataset_id varchar(96))')
    body = _canonical_copy_guard_bodies(schema, preparation)[1]
    await admin.execute(
        f'CREATE FUNCTION "{schema}".guard_pd_dataset_candidate_parent() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog AS $fn${body}$fn$'
    )
    for table in (*PARENTS[:4], "retained_input"):
        for operation, image in (("INSERT", "NEW"), ("UPDATE", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")):
            root = PARENTS[0] if table == "retained_input" else table
            await admin.execute(
                f'CREATE TRIGGER a_pd_candidate_{operation.lower()}_{image.lower()} AFTER {operation} ON "{schema}"."{table}" REFERENCING {image} TABLE AS affected_rows FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".guard_pd_dataset_candidate_parent(\'{root}\',\'{image}\')'
            )


async def _cleanup_capture_fixture(admin, schema, role_by_kind, readers, captures):
    """Remove exact fixture-created namespaces and roles after closing all logins."""
    for connection in readers:
        await connection.close()
    await admin.execute("RESET ROLE")
    for name in captures:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    for role in reversed(tuple(role_by_kind.values())):
        await admin.execute(f'DROP OWNED BY "{role}"')
        await admin.execute(f'DROP ROLE "{role}"')
    await admin.close()


@pytest.fixture
async def legacy_capture_db(request):
    """Create exact disposable native roles and register cleanup before setup."""
    is_modern = getattr(request, "param", None) in _MODERN_ROLE_CASE_BY_PROFILE
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    admin = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    schema = "legacy_fhir_test_" + suffix
    role_by_kind = {key: "retained_" + key + "_" + suffix for key in ("source", "owner", "reader", "publisher")}
    password = uuid4().hex + uuid4().hex
    readers, captures = [], []
    try:
        for key, role in role_by_kind.items():
            login = "LOGIN PASSWORD '" + password + "'" if key in {"reader", "publisher"} else "NOLOGIN"
            await admin.execute(
                f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
        database = await admin.fetchval("SELECT quote_ident(current_database())")
        await admin.execute(
            f'GRANT CREATE ON DATABASE {database} TO "{role_by_kind["publisher"]}","{role_by_kind["owner"]}"'
        )
        await admin.execute(
            f'GRANT "{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}" WITH INHERIT FALSE, SET TRUE'
        )
        await admin.execute(
            f'GRANT "{role_by_kind["reader"]}" TO "{role_by_kind["publisher"]}" WITH INHERIT TRUE, SET FALSE'
        )
        await admin.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role_by_kind["source"]}"')
        await admin.execute(f'SET ROLE "{role_by_kind["source"]}"')
        await _legacy_tables(admin, schema)
        metadata, summary = await _legacy_metadata(
            admin,
            schema,
            modern=is_modern,
            missing_role=_MODERN_ROLE_CASE_BY_PROFILE.get(getattr(request, "param", None), False),
        )
        await _legacy_guards(admin, schema, role_by_kind["reader"])
        await _legacy_candidate_guards(admin, schema, role_by_kind["source"], captures)
        await admin.execute("RESET ROLE")
        origin = await _legacy_input(admin, schema, role_by_kind["reader"], metadata, summary, modern=is_modern)

        async def connect(role):
            connection = await asyncpg.connect(
                dsn.replace("postgresql+asyncpg://", "postgresql://"), user=role_by_kind[role], password=password
            )
            readers.append(connection)
            return connection

        yield SimpleNamespace(
            admin=admin,
            schema=schema,
            role_by_kind=role_by_kind,
            origin=origin,
            connect=connect,
            captures=captures,
            metadata=metadata,
        )
    finally:
        await _cleanup_capture_fixture(admin, schema, role_by_kind, readers, captures)


async def _capture(fixture, origin=None, *, runtime_roles=None):
    reader, writer = await fixture.connect("reader"), await fixture.connect("publisher")
    identity = uuid4()
    fixture.captures.append("registry_fhir_capture_" + identity.hex)
    async with (
        reader.transaction(isolation="repeatable_read", readonly=True),
        writer.transaction(isolation="repeatable_read"),
    ):
        value = await capture_retained_fhir_source(
            reader,
            writer,
            origin or fixture.origin,
            capture_id=identity,
            owner_role=fixture.role_by_kind["owner"],
            runtime_roles=runtime_roles or (fixture.role_by_kind["reader"],),
        )
        assert await writer.fetchval("SELECT current_user") == fixture.role_by_kind["publisher"]
    return value


def _observe_capture_creation(monkeypatch):
    """Observe native owners immediately after initial DDL, before COPY and closure."""
    from process import network_fhir_retained_capture as capture_module

    original, observations = capture_module._create, []

    async def observe(writer, reader, origin, schema, owner_role):
        await original(writer, reader, origin, schema, owner_role)
        observations.append(
            await writer.fetchval(
                "SELECT current_user=$2 AND n.nspowner=$2::regrole "
                "AND count(*)=5 AND bool_and(c.relowner=$2::regrole) "
                "FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid "
                "WHERE n.nspname=$1 GROUP BY n.nspowner",
                schema,
                owner_role,
            )
        )

    monkeypatch.setattr(capture_module, "_create", observe)
    return observations


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_capture_owner_needs_no_database_create(legacy_capture_db, monkeypatch):
    """Capture under Publisher CREATE while Owner and every runtime actor lack it."""
    fixture = legacy_capture_db
    roles = fixture.role_by_kind
    roles["execution"] = "retained_execution_" + uuid4().hex
    await fixture.admin.execute(f'CREATE ROLE "{roles["execution"]}" NOLOGIN')
    database = await fixture.admin.fetchval("SELECT quote_ident(current_database())")
    await fixture.admin.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{roles["owner"]}"')
    assert await fixture.admin.fetchval(
        "SELECT has_database_privilege($1,current_database(),'CREATE') "
        "AND NOT has_database_privilege($2,current_database(),'CREATE') "
        "AND pg_has_role($1,$2,'SET') AND NOT pg_has_role($1,$2,'USAGE')",
        roles["publisher"],
        roles["owner"],
    )
    runtime_roles = tuple(sorted((roles["execution"], roles["reader"])))
    owners_at_creation = _observe_capture_creation(monkeypatch)
    capture = await _capture(fixture, runtime_roles=runtime_roles)
    assert owners_at_creation == [True]
    assert capture.owner_role == roles["owner"] and capture.runtime_roles == runtime_roles
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        assert await require_retained_fhir_capture(reader, capture) == capture
    for role in runtime_roles:
        assert await fixture.admin.fetchval(
            "SELECT NOT pg_has_role($1,$2,'MEMBER') AND NOT pg_has_role($1,$2,'USAGE') "
            "AND NOT pg_has_role($1,$2,'SET') "
            "AND NOT has_database_privilege($1,current_database(),'CREATE') "
            "AND has_schema_privilege($1,$3::oid,'USAGE') "
            "AND NOT has_schema_privilege($1,$3::oid,'CREATE')",
            role,
            roles["owner"],
            capture.schema_oid,
        )
    assert await fixture.admin.fetchval(
        "SELECT NOT has_database_privilege($1,current_database(),'CREATE')", roles["owner"]
    )


@pytest.mark.parametrize("damage", ["publisher_create", "publisher_set", "runtime_set"])
async def test_capture_create_for_owner_keeps_native_authority_checks(legacy_capture_db, damage):
    """Creating directly for Owner never permits a missing Publisher right or runtime reach."""
    fixture, roles = legacy_capture_db, legacy_capture_db.role_by_kind
    if damage == "publisher_create":
        database = await fixture.admin.fetchval("SELECT quote_ident(current_database())")
        await fixture.admin.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{roles["publisher"]}"')
    elif damage == "publisher_set":
        await fixture.admin.execute(f'REVOKE "{roles["owner"]}" FROM "{roles["publisher"]}"')
    else:
        await fixture.admin.execute(f'GRANT "{roles["owner"]}" TO "{roles["reader"]}" WITH INHERIT FALSE, SET TRUE')
    with pytest.raises((NetworkWriterClosureError, asyncpg.InsufficientPrivilegeError)):
        await _capture(fixture)
    assert not await fixture.admin.fetchval(
        "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=ANY($1::text[]))",
        [name for name in fixture.captures if name.startswith("registry_fhir_capture_")],
    )


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_genuine_reader_capture_preserves_header_and_uses_no_endpoint(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        assert await require_retained_fhir_capture(reader, capture) == capture
        read = await pin_retained_fhir_read(reader, capture)
        await read.verify(reader, capture)
        assert (
            await reader.fetchval(
                f'SELECT publication_metadata_json::text FROM "{capture.schema_name}".provider_directory_endpoint_dataset'
            )
            == fixture.metadata
        )
        assert (
            await reader.fetchval("SELECT to_regclass($1)", f'"{fixture.schema}".provider_directory_api_endpoint')
            is None
        )
        assert (
            await reader.fetchval(
                f'SELECT count(*) FROM "{capture.schema_name}".provider_directory_entity_source_binding'
            )
            == 2
        )
    assert (
        await fixture.admin.fetchval(
            f'SELECT publication_metadata_json::text FROM "{fixture.schema}".provider_directory_endpoint_dataset'
        )
        == fixture.metadata
    )


@pytest.mark.parametrize("damage", ["body", "root", "count", "retained_count"])
async def test_native_capture_refuses_bad_payload_or_published_roots(legacy_capture_db, damage):
    fixture = legacy_capture_db
    origin = fixture.origin
    if damage in {"body", "retained_count"}:
        await fixture.admin.execute(
            f'ALTER TABLE "{fixture.schema}".provider_directory_dataset_resource DISABLE TRIGGER USER'
        )
        if damage == "body":
            await fixture.admin.execute(
                f'UPDATE "{fixture.schema}".provider_directory_dataset_resource SET payload_json=\'{{"resource_id":"office-one","source_id":"source-one","first_line":"Changed"}}\'::json WHERE resource_type=\'Location\' AND resource_id=\'office-one\''
            )
        else:
            await fixture.admin.execute(
                f"DELETE FROM \"{fixture.schema}\".provider_directory_dataset_resource WHERE resource_type='Location' AND resource_id='office-two'"
            )
        for action in ("insert", "update", "delete", "truncate"):
            await fixture.admin.execute(
                f'ALTER TABLE "{fixture.schema}".provider_directory_dataset_resource ENABLE ALWAYS TRIGGER retained_{action}'
            )
        for name in ("insert_new", "update_new", "update_old", "delete_old"):
            await fixture.admin.execute(
                f'ALTER TABLE "{fixture.schema}".provider_directory_dataset_resource ENABLE TRIGGER a_pd_candidate_{name}'
            )
    elif damage == "root":
        origin = replace(origin, dataset_sha256="f" * 64)
    else:
        origin = replace(
            origin,
            resource_counts=tuple((kind, count + (kind == "Location")) for kind, count in origin.resource_counts),
            descriptor_counts=tuple((kind, count + (kind == "Location")) for kind, count in origin.descriptor_counts),
        )
    with pytest.raises(RetainedFHIRCaptureError):
        await _capture(fixture, origin)
    assert (
        await fixture.admin.fetchval(
            "SELECT count(*) FROM pg_namespace WHERE nspname=ANY($1::text[])",
            [name for name in fixture.captures if name.startswith("registry_fhir_capture_")],
        )
        == 0
    )


def _binding_command(coordinates, network_id, organization, capture_sha):
    """Keep the native whole-batch scope fixture explicit and synthetic."""
    return NetworkSourceBindingBatchCommand(
        json.dumps(
            [
                {
                    "binding_id": str(uuid4()),
                    **dict(
                        zip(
                            ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
                            coordinates.sql_parameters,
                            strict=True,
                        )
                    ),
                    "source_key": organization,
                    "source_scope_json": {
                        "organization_id": organization,
                        "capture_sha256": capture_sha,
                    },
                    "network_id": network_id,
                    "evidence_id": "synthetic-reviewed-organization",
                    "evidence_sha256": "e" * 64,
                    "operation": "bind",
                    "expected_revision": 0,
                    "expected_network_id": None,
                }
            ]
        ).encode(),
        "Reviewed retained Organization",
        uuid4().hex,
    )


async def _approved_capture_recipe(fixture, serving, capture, *, organization="network-one", capture_sha=None):
    """Use actual catalog drafts, native scope encoding and approval snapshots."""
    connection, control, engine = serving
    actor = _actor()
    network = await _draft(engine, control, _create("network"), actor)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        fixture.origin.source_id,
        fixture.origin.schema_name,
        fixture.origin.dataset_id,
        "registry-retained-normalized-fhir-v1",
        capture.capture_id,
    )
    command = _binding_command(coordinates, network["record_id"], organization, capture_sha or capture.capture_sha256)
    async with connection.transaction():
        binding = (await apply_network_source_binding_batch(connection, command, actor, control_schema=control))[
            "records"
        ][0]
    await _approve(connection, control, await _command(connection, control, network, binding), actor)
    async with connection.transaction(isolation="repeatable_read"):
        revision = await connection.fetchval(f'SELECT approved_revision FROM "{control}".registry_revision_control')
        approved = await pin_approved_membership_source(connection, approved_revision=revision, control_schema=control)
    await connection.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{fixture.role_by_kind["reader"]}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control}" TO "{fixture.role_by_kind["reader"]}"')
    source_pin = PinnedFHIRMembershipSource(
        fixture.origin.schema_name,
        fixture.origin.source_id,
        fixture.origin.endpoint_id,
        fixture.origin.dataset_id,
        fixture.origin.dataset_sha256,
        capture.release_id,
        fixture.origin.resource_parent_oid,
        "medical",
        "2026-10-01",
        retained_capture=capture,
    )
    return RegistrySourceMembershipRecipe(source_pin, coordinates), approved, network["record_id"]


async def _retained_addresses(fixture, serving, *, only_sibling=False, ambiguous=False):
    """Retain separate full native EUA rows for two offices of the same NPI."""
    import ptg2_address_canon

    connection, _, engine = serving
    schema = "retained_offices_" + uuid4().hex
    fixture.captures.append(schema)
    addresses = [
        ("123 Example St", "Suite 1", "Chicago", "IL", "60601", "US"),
        ("123 Example St", "Suite 2", "Chicago", "IL", "60601", "US"),
    ]
    canonical = ptg2_address_canon.canonicalize_batch(addresses)
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    async with engine.begin() as setup:
        table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
        await setup.run_sync(lambda native: table.create(native))
    selected = [1] if only_sibling else [0, 1]
    if ambiguous:
        selected.append(0)
    for position, index in enumerate(selected, 1):
        await connection.execute(
            f'''INSERT INTO "{schema}".entity_address_unified
            (location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,
             postal_code,country_code,address_key,address_precision,premise_key)
            VALUES($1,'npi','1234567893',1234567893,$2,'practice',$3,$4,$5,$6,$7,$8,$9,'street',$10)''',
            str(position) * 64,
            position,
            *addresses[index],
            UUID(canonical[index]["address_key"]),
            UUID(canonical[index]["premise_key"]),
        )
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{fixture.role_by_kind["owner"]}"')
    await connection.execute(
        f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{fixture.role_by_kind["owner"]}"'
    )
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.role_by_kind["reader"]}"')
    await connection.execute(f'GRANT SELECT ON "{schema}".entity_address_unified TO "{fixture.role_by_kind["reader"]}"')
    return PinnedAddressSource(schema, "entity_address_unified", "retained-office-edition")


@pytest.mark.parametrize("office_case", ["exact", "sibling", "ambiguous"])
async def test_closed_capture_recipe_selects_only_exact_office(legacy_capture_db, serving_schema, office_case):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, network_id = await _approved_capture_recipe(fixture, serving_schema, capture)
    addresses = await _retained_addresses(
        fixture, serving_schema, only_sibling=office_case == "sibling", ambiguous=office_case == "ambiguous"
    )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        batch, cursor, limit = await _read_recipe_page(reader, recipe, approved, None, serving_schema[1], 1000, read)
        assert batch.source_rows == batch.membership_rows == batch.accepted_source_rows == 1
        assert batch.unresolved_rows == batch.omitted_rows == 0
        membership = json.loads(batch.input_bytes)[0]
        assert membership["network_id"] == network_id and membership["provider_id"] == "1234567893"
        from process.network_fhir_retained_capture_contract import registry_location_id

        assert membership["location_id"] == str(registry_location_id("source-one", "office-one"))
        assert membership["location_id"] != str(registry_location_id("source-one", "office-two"))
        if office_case == "exact":
            pin = await pin_cms_office_address_source(
                reader,
                batch,
                addresses,
                runtime_roles=(fixture.role_by_kind["reader"],),
                control_schema=serving_schema[1],
                retained_read=read,
            )
            assert pin.office_count == 1
        else:
            expected_error, reason = (
                (SourceOfficeNotReady, "registry_source_office_not_ready")
                if office_case == "sibling"
                else (InitialCMSOfficeBindingError, "initial_cms_office_correspondence_unresolved")
            )
            with pytest.raises(expected_error, match=reason):
                await pin_cms_office_address_source(
                    reader,
                    batch,
                    addresses,
                    runtime_roles=(fixture.role_by_kind["reader"],),
                    control_schema=serving_schema[1],
                    retained_read=read,
                )
        empty, _, _ = await _read_recipe_page(reader, recipe, approved, cursor, serving_schema[1], limit, read)
        assert empty.source_rows == empty.membership_rows == 0


@pytest.mark.parametrize("changed", ["organization", "capture"])
async def test_approved_mapping_cannot_claim_another_capture_or_organization(
    legacy_capture_db, serving_schema, changed
):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, _ = await _approved_capture_recipe(
        fixture,
        serving_schema,
        capture,
        organization="other-network" if changed == "organization" else "network-one",
        capture_sha="b" * 64 if changed == "capture" else None,
    )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        batch = await read_retained_fhir_membership_batch(
            reader,
            recipe.source_pin,
            read,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert batch.membership_rows == 0 and batch.omitted_rows == 1 and batch.unresolved_rows == 0


@pytest.mark.parametrize("damage", ["owner", "acl", "payload", "metadata_bytes"])
async def test_capture_native_custody_refuses_drift(legacy_capture_db, damage):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    if damage == "owner":
        await fixture.admin.execute(
            f'ALTER TABLE "{capture.schema_name}".provider_directory_entity_release_evidence OWNER TO "{fixture.role_by_kind["source"]}"'
        )
    elif damage == "acl":
        await fixture.admin.execute(
            f'GRANT UPDATE ON "{capture.schema_name}".provider_directory_dataset_resource TO "{fixture.role_by_kind["reader"]}"'
        )
    elif damage == "payload":
        await fixture.admin.execute(
            f"UPDATE \"{capture.schema_name}\".provider_directory_dataset_resource SET payload_json='{{}}'::json WHERE resource_type='Location'"
        )
    else:
        await fixture.admin.execute(
            f"UPDATE \"{capture.schema_name}\".provider_directory_endpoint_dataset SET publication_metadata_json=(publication_metadata_json::text||' ')::json"
        )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises((RetainedFHIRCaptureError, ValueError)):
            await pin_retained_fhir_read(reader, capture)


@pytest.mark.parametrize("profile", ["publisher_login", "write_transaction", "read_committed"])
async def test_capture_requires_genuine_reader_and_readonly_repeatable_read(legacy_capture_db, profile):
    fixture = legacy_capture_db
    reader = await fixture.connect("publisher" if profile == "publisher_login" else "reader")
    writer = await fixture.connect("publisher")
    async with (
        reader.transaction(
            isolation="read_committed" if profile == "read_committed" else "repeatable_read",
            readonly=profile != "write_transaction",
        ),
        writer.transaction(isolation="repeatable_read"),
    ):
        with pytest.raises(ValueError):
            await capture_retained_fhir_source(
                reader,
                writer,
                fixture.origin,
                capture_id=uuid4(),
                owner_role=fixture.role_by_kind["owner"],
                runtime_roles=(fixture.role_by_kind["reader"],),
            )


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_retained_capture_composes_and_publishes_one_exact_office(legacy_capture_db, serving_schema):
    """Use existing candidate/publication APIs, preserving the sibling NPI office."""
    from process.network_membership_pipeline import prepare_and_publish_network_candidate
    from process.registry_candidate_composition import (
        RegistryCompositionAddressSources,
        compose_registry_membership_candidate,
    )

    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, network_id = await _approved_capture_recipe(fixture, serving_schema, capture)
    addresses = await _retained_addresses(fixture, serving_schema)
    connection, control, _ = serving_schema
    for extension in ("intarray", "btree_gin", "postgis"):
        await connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
    request_id = uuid4()
    fixture.captures.append("network_composition_" + uuid5(request_id, "address:" + approved.generation_id).hex)
    roles_by_group = {
        "owner_role": fixture.role_by_kind["owner"],
        "loader_roles": (),
        "reader_roles": (fixture.role_by_kind["reader"],),
    }
    arguments_by_name = {
        "request_id": request_id,
        "approved_revision": approved.approved_revision,
        "expected_head": 0,
        "address_sources": RegistryCompositionAddressSources(addresses, initial_source_recipes=(recipe,)),
        "writer_roles": roles_by_group,
        "control_schema": control,
    }
    try:
        copy_target, candidate_addresses = await compose_registry_membership_candidate(connection, **arguments_by_name)
        assert await compose_registry_membership_candidate(connection, **arguments_by_name) == (
            copy_target,
            candidate_addresses,
        )
        await prepare_and_publish_network_candidate(
            connection, copy_target, candidate_addresses, **roles_by_group, control_schema=control
        )
        await _assert_published_capture_offices(connection, control, network_id)
    finally:
        fixture.captures.extend(
            await connection.fetchval(
                f'SELECT coalesce(array_agg(schema_name),ARRAY[]::text[]) FROM "{control}".network_membership_candidate WHERE dataset_id=$1',
                uuid5(request_id, "dataset"),
            )
        )


async def _assert_published_capture_offices(connection, control, network_id):
    """Check the exact address array and untouched sibling after native atomic publication."""
    from process.network_serving_read import resolve_network_serving_manifest

    async with connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(connection, control_schema=control)
        members = await connection.fetch(
            f'SELECT network_id,provider_id,location_id FROM "{manifest.schema_name}".network_membership'
        )
        assert (
            len(members) == 1 and members[0]["network_id"] == network_id and members[0]["provider_id"] == "1234567893"
        )
        offices = await connection.fetch(
            f'SELECT second_line,canonical_network_ids FROM "{manifest.schema_name}".entity_address_unified ORDER BY second_line'
        )
        assert [(office["second_line"], office["canonical_network_ids"]) for office in offices] == [
            ("Suite 1", [network_id]),
            ("Suite 2", []),
        ]


@pytest.mark.parametrize("target", ["leaf", "leaf_column", "source"])
async def test_genuine_reader_refuses_any_input_writer_privilege(legacy_capture_db, target):
    """A readonly transaction cannot substitute for closed input actor privileges."""
    fixture = legacy_capture_db
    relation, privilege = {
        "leaf": ("retained_input", "UPDATE"),
        "leaf_column": ("retained_input", "UPDATE(payload_json)"),
        "source": ("provider_directory_source", "UPDATE"),
    }[target]
    await fixture.admin.execute(
        f'GRANT {privilege} ON "{fixture.schema}"."{relation}" TO "{fixture.role_by_kind["reader"]}"'
    )
    with pytest.raises(RetainedFHIRCaptureError):
        await _capture(fixture)


@pytest.mark.parametrize("damage", ["parent_disabled", "leaf_disabled", "leaf_argument", "body"])
async def test_capture_requires_installed_candidate_parent_and_leaf_guards(legacy_capture_db, damage):
    """An unchanged legacy guard alone cannot qualify a changed current guard set."""
    fixture = legacy_capture_db
    if damage in {"parent_disabled", "leaf_disabled"}:
        table = "provider_directory_dataset_resource" if damage == "parent_disabled" else "retained_input"
        await fixture.admin.execute(
            f'ALTER TABLE "{fixture.schema}"."{table}" DISABLE TRIGGER a_pd_candidate_update_old'
        )
    elif damage == "leaf_argument":
        await fixture.admin.execute(f'DROP TRIGGER a_pd_candidate_update_old ON "{fixture.schema}".retained_input')
        await fixture.admin.execute(
            f"CREATE TRIGGER a_pd_candidate_update_old AFTER UPDATE ON \"{fixture.schema}\".retained_input REFERENCING OLD TABLE AS affected_rows FOR EACH STATEMENT EXECUTE FUNCTION \"{fixture.schema}\".guard_pd_dataset_candidate_parent('provider_directory_dataset_resource','NEW')"
        )
    else:
        from db.migration_generic_source_candidates import _canonical_copy_guard_bodies

        body = _canonical_copy_guard_bodies(fixture.schema, fixture.origin.preparation_schema)[1] + "\n-- altered\n"
        await fixture.admin.execute(
            f'CREATE OR REPLACE FUNCTION "{fixture.schema}".guard_pd_dataset_candidate_parent() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog AS $fn${body}$fn$'
        )
    with pytest.raises(RetainedFHIRCaptureError):
        await _capture(fixture)


def _plan_nodes(plan):
    """Inspect eligible plan structure without treating fixture costs as live timing."""
    yield plan
    for child in plan.get("Plans", []):
        yield from _plan_nodes(child)


async def test_closed_target_has_eligible_c_cursor_index_for_both_pages(legacy_capture_db):
    """The target C cursor uses its own index while copied columns/PK stay native."""
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL enable_seqscan=off")
        for cursor in (None, ("Location", "office-one")):
            predicate, arguments = "dataset_id=$1", [fixture.origin.dataset_id]
            if cursor:
                predicate += ' AND (resource_type COLLATE "C",resource_id COLLATE "C")>($2 COLLATE "C",$3 COLLATE "C")'
                arguments.extend(cursor)
            plan = json.loads(
                await reader.fetchval(
                    f'EXPLAIN (FORMAT JSON) SELECT * FROM "{capture.schema_name}".provider_directory_dataset_resource '
                    f'WHERE {predicate} ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT 1000',
                    *arguments,
                )
            )
            nodes = tuple(_plan_nodes(plan[0]["Plan"]))
            assert not any(node["Node Type"] in {"Sort", "Incremental Sort"} for node in nodes)
            assert any(node.get("Index Name") == "retained_lookup_1_0" for node in nodes)
        assert (
            await reader.fetchval(
                "SELECT bool_and(original.attcollation=copied.attcollation) FROM pg_attribute original "
                "JOIN pg_attribute copied ON copied.attname=original.attname "
                "WHERE original.attrelid=$1::oid AND copied.attrelid=$2::oid AND original.attnum>0 AND NOT original.attisdropped",
                fixture.origin.resource_parent_oid,
                dict(capture.relation_oids)["provider_directory_dataset_resource"],
            )
            is True
        )


@pytest.mark.parametrize("damage", ["missing", "collation", "unique", "extra", "opclass"])
async def test_resource_cursor_index_is_part_of_closed_native_capture(legacy_capture_db, damage):
    """A same-column PK cannot stand in for the exact nonunique C lookup."""
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    namespace = '"' + capture.schema_name + '"'
    if damage != "extra":
        await fixture.admin.execute(f"DROP INDEX {namespace}.retained_lookup_1_0")
    if damage in {"collation", "unique", "opclass"}:
        unique = "UNIQUE " if damage == "unique" else ""
        keys = 'dataset_id,resource_type COLLATE "C",resource_id COLLATE "C"'
        if damage == "collation":
            keys = "dataset_id,resource_type,resource_id"
        elif damage == "opclass":
            keys = 'dataset_id,resource_type COLLATE "C" varchar_pattern_ops,resource_id COLLATE "C"'
        await fixture.admin.execute(
            f"CREATE {unique}INDEX retained_lookup_1_0 ON {namespace}.provider_directory_dataset_resource ({keys})"
        )
    elif damage == "extra":
        await fixture.admin.execute(
            f"CREATE INDEX extra_cursor ON {namespace}.provider_directory_dataset_resource (resource_id)"
        )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError):
            await pin_retained_fhir_read(reader, capture)


@pytest.mark.parametrize("repeat_cancel", [False, True])
async def test_interrupted_capture_rolls_back_target_and_drains_reader(legacy_capture_db, monkeypatch, repeat_cancel):
    """Cancellation leaves no target schema or active export on either native backend."""
    from process import network_fhir_retained_capture as capture_module

    fixture, started = legacy_capture_db, asyncio.Event()
    reader, writer = await fixture.connect("reader"), await fixture.connect("publisher")
    identity = uuid4()
    schema = "registry_fhir_capture_" + identity.hex
    fixture.captures.append(schema)

    async def wait_during_export(pair, *_arguments):
        started.set()
        await pair.copy_from_query("SELECT pg_sleep(60) /* retained_capture_cancel */", output=lambda _chunk: None)

    monkeypatch.setattr(capture_module, "_copy_resources", wait_during_export)
    rollback_started, release_rollback = _reader_rollback_barrier(monkeypatch, reader, started, repeat_cancel)
    async with (
        reader.transaction(isolation="repeatable_read", readonly=True),
        writer.transaction(isolation="repeatable_read"),
    ):
        original_vxid = await reader.fetchval(
            "SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid'"
        )
        task = asyncio.create_task(
            capture_retained_fhir_source(
                reader,
                writer,
                fixture.origin,
                capture_id=identity,
                owner_role=fixture.role_by_kind["owner"],
                runtime_roles=(fixture.role_by_kind["reader"],),
            )
        )
        try:
            async with asyncio.timeout(5):
                await started.wait()
                while not await fixture.admin.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1 AND state='active' "
                    "AND query LIKE '%retained_capture_cancel%')",
                    reader.get_server_pid(),
                ):
                    await asyncio.sleep(0.01)
                task.cancel()
                if repeat_cancel:
                    await rollback_started.wait()
                    task.cancel()
                    assert not task.done()
                    release_rollback.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
            await _assert_cancelled_reader(fixture, reader, writer, schema, original_vxid)
        finally:
            release_rollback.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize(
    "damage",
    [
        "missing_proof",
        "wrong_proof",
        "proof_lineage",
        "summary",
        "missing_summary",
        "metadata_seal",
        "admission_version",
        "admission_kind",
        "admission_proof",
        "resource_scope",
        "hash_contract",
        "as_of",
    ],
)
async def test_modern_capture_requires_complete_original_and_current_admission(legacy_capture_db, damage):
    """Re-pinning changed raw bytes cannot bypass proof or existing summary controls."""
    fixture = legacy_capture_db
    metadata = json.loads(fixture.metadata)
    origin = fixture.origin
    changes_by_column = {}
    if damage == "missing_proof":
        metadata.pop("provider_directory_content_proof_v1")
    if damage == "wrong_proof":
        metadata["provider_directory_content_proof_v1"]["shards"] = []
    if damage == "proof_lineage":
        metadata["provider_directory_content_proof_v1"]["source_ids"] = ["other-source"]
    if damage == "hash_contract":
        metadata["resource_hash_contract"] = "transport_bound_v1"
    if damage == "as_of":
        metadata["semantic_projection_as_of"] = "2026-01-02"
    if damage in {"summary", "missing_summary"}:
        stored = json.loads(
            await fixture.admin.fetchval(
                f'SELECT publication_metadata_summary_json::text FROM "{fixture.schema}".provider_directory_endpoint_dataset'
            )
        )
        if damage == "summary":
            stored["source_summary_v1"]["resource_counts"]["Location"] += 1
        changes_by_column["publication_metadata_summary_json"] = (
            None if damage == "missing_summary" else json.dumps(stored)
        )
    if damage in {"metadata_seal", "admission_version", "admission_kind", "admission_proof", "resource_scope"}:
        column, column_value = {
            "metadata_seal": ("publication_metadata_sha256", "1" * 64),
            "admission_version": ("content_proof_admission_version", None),
            "admission_kind": ("content_proof_admission_kind", "other"),
            "admission_proof": ("content_proof_admission_sha256", "1" * 64),
            "resource_scope": ("content_proof_resource_types", ["Location"]),
        }[damage]
        changes_by_column[column] = column_value
    raw = json.dumps(metadata)
    changes_by_column["publication_metadata_json"] = raw
    await _replace_fixture_header(fixture, changes_by_column)
    origin = replace(
        origin, metadata_bytes=len(raw.encode()), metadata_text_sha256=hashlib.sha256(raw.encode()).hexdigest()
    )
    with pytest.raises((RetainedFHIRCaptureError, RuntimeError, ValueError)):
        await _capture(fixture, origin)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_modern_profile_retains_canonical_guard_and_copied_header_controls(legacy_capture_db):
    """An admitted proof does not replace canonical Source or closed target custody."""
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    source_header = await fixture.admin.fetchrow(
        f'SELECT row_to_json(h)::text FROM "{fixture.schema}".provider_directory_endpoint_dataset h'
    )
    target_header = await fixture.admin.fetchrow(
        f'SELECT row_to_json(h)::text FROM "{capture.schema_name}".provider_directory_endpoint_dataset h'
    )
    assert source_header == target_header
    await fixture.admin.execute(
        f'ALTER TABLE "{fixture.schema}".retained_input DISABLE TRIGGER a_pd_candidate_update_old'
    )
    with pytest.raises(RetainedFHIRCaptureError):
        await _capture(fixture)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_modern_capture_retains_and_accounts_unselected_missing_plan_reference(legacy_capture_db):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await require_retained_fhir_capture(reader, capture)
        assert capture.origin.unresolved_plan_network_count == 1
        assert (
            await reader.fetchval(
                f"SELECT count(*) FROM \"{capture.schema_name}\".provider_directory_insurance_network_plan_evidence WHERE network_resource_id='missing-network'"
            )
            == 1
        )
        assert (
            await reader.fetchval(
                f"SELECT count(*) FROM \"{capture.schema_name}\".provider_directory_dataset_resource WHERE resource_type='Organization' AND resource_id='missing-network'"
            )
            == 0
        )
        assert (
            await reader.fetchval(
                f"SELECT count(*) FROM \"{capture.schema_name}\".provider_directory_entity_release_evidence WHERE resource_type='Organization' AND resource_id='missing-network'"
            )
            == 0
        )
        assert json.loads(
            await reader.fetchval(
                f"SELECT payload_json::text FROM \"{capture.schema_name}\".provider_directory_dataset_resource WHERE resource_id='unresolved-plan'"
            )
        )["network_refs"] == ["Organization/missing-network"]


@pytest.mark.parametrize(
    ("legacy_capture_db", "accepted_resources", "omitted_resources"),
    [("modern_missing", 1, 1), ("modern_mixed", 2, 0)],
    indirect=["legacy_capture_db"],
)
async def test_unselected_missing_organization_keeps_complete_read_and_stage_accounting(
    legacy_capture_db, serving_schema, accepted_resources, omitted_resources
):
    from process.network_fhir_membership_source import read_fhir_membership_batch

    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, network_id = await _approved_capture_recipe(fixture, serving_schema, capture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read"):
        read = await _require_recipe_custody(reader, recipe)
        batch = await read_retained_fhir_membership_batch(
            reader,
            recipe.source_pin,
            read,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert batch.source_rows == 2 and batch.next_resource is not None
        assert batch.unresolved_rows == batch.unresolved_source_rows == 0
        assert batch.accepted_source_rows == accepted_resources
        assert batch.omitted_source_rows == omitted_resources and batch.omitted_rows == 1
        assert batch.membership_rows == accepted_resources
        assert all(member["network_id"] == network_id for member in json.loads(batch.input_bytes))
        assert [(outcome.outcome, outcome.reason) for outcome in batch.source_outcomes].count(
            ("omitted", "unmapped_network")
        ) == omitted_resources
        stage = await stage_registry_source_recipes(
            reader,
            (recipe,),
            approved,
            request_id=uuid4(),
            control_schema=serving_schema[1],
            read_context=SourceRecipeReadContext(retained_fhir_reads={capture: read}),
        )
        assert stage.raw_source_rows == 2 and stage.accepted_raw_source_rows == accepted_resources
        assert stage.omitted_raw_source_rows == omitted_resources and stage.omitted_rows == 1
        assert stage.membership_rows == accepted_resources
        assert await reader.fetchval(f'SELECT count(*) FROM pg_temp."{stage.table_name}"') == accepted_resources
        await reader.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
        strict = await read_fhir_membership_batch(
            reader,
            recipe.source_pin,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert strict.unresolved_rows > 0 and strict.unresolved_source_rows > 0
        assert await reader.fetchval(f'SELECT generation_id FROM "{serving_schema[1]}".network_serving_control') is None


@pytest.mark.parametrize(
    ("legacy_capture_db", "organization", "reason"),
    [
        ("modern_missing", "missing-network", "network_unresolved"),
        ("modern_null", "network-one", "malformed_resource"),
        ("modern_unknown_plan", "network-one", "network_unresolved"),
    ],
    indirect=["legacy_capture_db"],
)
async def test_retained_selected_or_invalid_network_refuses_the_complete_stage(
    legacy_capture_db, serving_schema, organization, reason
):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, _ = await _approved_capture_recipe(fixture, serving_schema, capture, organization=organization)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read"):
        read = await _require_recipe_custody(reader, recipe)
        batch = await read_retained_fhir_membership_batch(
            reader,
            recipe.source_pin,
            read,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert batch.source_rows == 2 and batch.unresolved_rows > 0 and batch.unresolved_source_rows == 1
        assert any(outcome.outcome == "unresolved" and outcome.reason == reason for outcome in batch.source_outcomes)
        before_tables = await reader.fetchval("SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema()")
        with pytest.raises(RegistrySourceCompositionError, match="unresolved"):
            await stage_registry_source_recipes(
                reader,
                (recipe,),
                approved,
                request_id=uuid4(),
                control_schema=serving_schema[1],
                read_context=SourceRecipeReadContext(retained_fhir_reads={capture: read}),
            )
        assert (
            await reader.fetchval("SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema()")
            == before_tables
        )
        assert await reader.fetchval(f'SELECT generation_id FROM "{serving_schema[1]}".network_serving_control') is None


@pytest.mark.parametrize("legacy_capture_db", ["modern_missing"], indirect=True)
async def test_selected_missing_organization_refuses_membership_copy(legacy_capture_db, serving_schema):
    from process.network_fhir_membership_source import FHIRMembershipSourceError, copy_fhir_membership_batch

    fixture = legacy_capture_db
    capture = await _capture(fixture)
    recipe, approved, _ = await _approved_capture_recipe(
        fixture, serving_schema, capture, organization="missing-network"
    )
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        batch = await read_retained_fhir_membership_batch(
            reader,
            recipe.source_pin,
            read,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert batch.unresolved_rows > 0 and batch.membership_rows == 0
        with pytest.raises(FHIRMembershipSourceError, match="unresolved"):
            await copy_fhir_membership_batch(reader, batch, None, require_candidate_authority=None)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
@pytest.mark.parametrize(
    "damage", ["wrong_expected", "raw_summary_disagrees", "resolved_organization_missing", "dropped_plan_evidence"]
)
async def test_modern_unresolved_accounting_cannot_hide_missing_or_dropped_evidence(
    legacy_capture_db, monkeypatch, damage
):
    from process import network_fhir_retained_capture as capture_module

    fixture = legacy_capture_db
    origin = fixture.origin
    if damage == "wrong_expected":
        origin = replace(origin, unresolved_plan_network_count=0)
    elif damage == "raw_summary_disagrees":
        document = json.loads(fixture.metadata)
        document["dataset_network_plan"]["unresolved_reference_count"] = 2
        raw = json.dumps(document)
        table = f'"{fixture.schema}".provider_directory_endpoint_dataset'
        await fixture.admin.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
        try:
            await fixture.admin.execute(f"UPDATE {table} SET publication_metadata_json=$1::json", raw)
        finally:
            await fixture.admin.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")
        origin = replace(
            origin, metadata_bytes=len(raw.encode()), metadata_text_sha256=hashlib.sha256(raw.encode()).hexdigest()
        )
    else:
        original = capture_module._copy_resources

        async def incomplete_copy(pair, selected_origin, schema, release_id):
            await original(pair, selected_origin, schema, release_id)
            if damage == "resolved_organization_missing":
                await pair.writer.execute(
                    f"DELETE FROM \"{schema}\".provider_directory_dataset_resource WHERE resource_type='Organization' AND resource_id='network-one'"
                )
            else:
                await pair.writer.execute(
                    f"DELETE FROM \"{schema}\".provider_directory_insurance_network_plan_evidence WHERE network_resource_id='missing-network'"
                )

        monkeypatch.setattr(capture_module, "_copy_resources", incomplete_copy)
    with pytest.raises(RetainedFHIRCaptureError):
        await _capture(fixture, origin)


def _modern_resources(missing_role):
    """Use canonical mapped payloads, preserving one intentionally missing plan reference."""
    from process.provider_directory_organization_hash import canonical_organization_payload
    from process.provider_directory_resource_hash import canonical_practitioner_payload

    resources = _contents("source-one")
    resources.append(
        (
            "InsurancePlan",
            "unresolved-plan",
            {
                "source_id": "source-one",
                "resource_id": "unresolved-plan",
                "name": "Other plan",
                "network_refs": ["Organization/missing-network"],
            },
        )
    )
    if missing_role:
        original = next(resource[2] for resource in resources if resource[0] == "PractitionerRole")
        role_by_field = {
            **original,
            "resource_id": "unresolved-role",
            "network_refs": ["Organization/missing-network"],
            "insurance_plan_refs": ["InsurancePlan/unresolved-plan"],
        }
        if missing_role == "mixed":
            role_by_field["network_refs"].insert(0, "Organization/network-one")
        elif missing_role == "null":
            role_by_field["network_refs"] = [None]
        elif missing_role == "unknown_plan":
            role_by_field["network_refs"] = []
            role_by_field["insurance_plan_refs"] = ["InsurancePlan/unknown-plan"]
        resources.append(("PractitionerRole", "unresolved-role", role_by_field))
    return [
        (
            kind,
            identity,
            canonical_organization_payload(normalized_payload)
            if kind == "Organization"
            else canonical_practitioner_payload({**normalized_payload, "names": []})
            if kind == "Practitioner"
            else normalized_payload,
        )
        for kind, identity, normalized_payload in resources
    ]


def _fixture_summary(dataset_hash, counts, hashes, modern, resources):
    """Bind source summary metrics to the exact synthetic resource census."""
    addressed_locations = sum(
        kind == "Location" and type(document.get("first_line")) is str and bool(document["first_line"].strip())
        for kind, _, document in resources
    )
    distinct_npis = {document["npi"] for _, _, document in resources if document.get("npi")}
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding("dataset-one", "endpoint-one", "run-one", dataset_hash),
        source_ids=["source-one"],
        selected_resources=tuple(counts),
        count_by_resource=counts,
        hash_by_resource=hashes,
        count_by_field={
            "distinct_npis": len(distinct_npis),
            "individual_practitioners": counts.get("Practitioner", 0),
            "organization_resources": counts.get("Organization", 0),
            "address_records": 0 if modern else addressed_locations,
            "addressed_locations": addressed_locations,
            "geocoded_locations": 0,
            "practitioner_role_resources": counts["PractitionerRole"],
            "network_plan_links": counts["InsurancePlan"],
            "organization_affiliation_links": 0,
        },
        count_by_category={"unknown_field_counts": {}, "intentional_drop_counts": {}},
        identity_by_field={"semantic_contract_id": "healthporta.provider-directory.fhir-normalized-resource.v1"},
    )


async def _store_fixture_admission(admin, schema, document):
    """Seal later projection bookkeeping separately from unchanged original proof bytes."""
    from process.provider_directory_admission_seal import admission_seal_from_validated_metadata
    from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256

    seal = admission_seal_from_validated_metadata(document)
    assert seal is not None
    current_summary = json.loads(json.dumps(seal.metadata_summary))
    current_summary["dataset_network_plan"].update(build_run_id="later-build", replaced_edge_count=2)
    seal = replace(seal, metadata_summary=current_summary)
    seal = replace(seal, metadata_sha256=canonical_payload_sha256(seal.digest_envelope()))
    await admin.execute(
        f'UPDATE "{schema}".provider_directory_endpoint_dataset SET publication_metadata_summary_json=$1::jsonb,'
        "publication_metadata_sha256=$2,content_proof_admission_version=$3,content_proof_admission_kind=$4,"
        "content_proof_admission_sha256=$5,content_proof_resource_types=$6 WHERE dataset_id='dataset-one'",
        json.dumps(seal.metadata_summary),
        seal.metadata_sha256,
        seal.admission_version,
        seal.admission_kind,
        seal.proof_sha256,
        list(seal.resource_types),
    )


def _modern_descriptors(resource_records):
    """Build complete production proof shards from the actual mapped payloads."""
    from process import provider_directory_proof_store as proofs

    descriptors = []
    for kind in sorted({resource_record[1] for resource_record in resource_records}):
        resource_rows = [
            dict(
                zip(
                    (
                        "dataset_id",
                        "resource_type",
                        "resource_id",
                        "payload_hash",
                        "payload_json",
                        "acquired_resource_sha256",
                    ),
                    resource_record,
                    strict=True,
                )
            )
            for resource_record in resource_records
            if resource_record[1] == kind
        ]
        for resource_row in resource_rows:
            resource_row["payload_json"] = json.loads(resource_row["payload_json"])
        descriptor, _artifact = proofs.build_dataset_proof_shard(
            resource_rows,
            dataset_id="dataset-one",
            endpoint_id="endpoint-one",
            acquisition_root_run_id="run-one",
            source_ids=["source-one"],
            resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        )
        descriptors.append(descriptor)
    descriptors.sort(key=lambda descriptor: descriptor["shard_id"])
    return descriptors


def _modern_origin(origin, metadata, admission_sha256):
    """Pin exact modern proof/admission fields without changing source rows."""
    document = json.loads(metadata)
    return replace(
        origin,
        admission_profile=SEMANTIC_PROFILE,
        original_admission_refusal=None,
        proof_sha256=document["provider_directory_content_proof_v1"]["proof_sha256"],
        descriptor_counts=origin.resource_counts,
        semantic_projection_as_of="2026-01-01",
        unresolved_plan_network_count=1,
        admission_metadata_sha256=admission_sha256,
    )


def _reader_rollback_barrier(monkeypatch, reader, started, repeat_cancel):
    """Hold the real rollback for a deterministic second cancellation only."""
    from asyncpg.transaction import Transaction

    rollback_started, release_rollback = asyncio.Event(), asyncio.Event()
    native_rollback = Transaction.rollback

    async def held_rollback(transaction):
        if repeat_cancel and transaction._connection is reader and started.is_set():
            rollback_started.set()
            await release_rollback.wait()
        return await native_rollback(transaction)

    monkeypatch.setattr(Transaction, "rollback", held_rollback)
    return rollback_started, release_rollback


async def _assert_cancelled_reader(fixture, reader, writer, schema, original_vxid):
    """Prove immediate native absence and both original transaction identities."""
    assert await writer.fetchval("SELECT to_regnamespace($1)", schema) is None
    assert await writer.fetchval("SELECT current_user") == fixture.role_by_kind["publisher"]
    assert not await fixture.admin.fetchval(
        "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1 AND state='active')",
        reader.get_server_pid(),
    )
    assert await reader.fetchval("SELECT 1") == 1
    assert await reader.fetchval("SHOW transaction_isolation") == "repeatable read"
    assert await reader.fetchval("SHOW transaction_read_only") == "on"
    assert (
        await reader.fetchval(
            "SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid'"
        )
        == original_vxid
    )


async def _replace_fixture_header(fixture, changes_by_column):
    """Change only a disposable fixture row and restore all existing header triggers."""
    table = f'"{fixture.schema}".provider_directory_endpoint_dataset'
    await fixture.admin.execute(f"ALTER TABLE {table} DISABLE TRIGGER USER")
    try:
        for column, column_value in changes_by_column.items():
            cast = (
                "::json"
                if column == "publication_metadata_json"
                else "::jsonb"
                if column.endswith("summary_json")
                else ""
            )
            await fixture.admin.execute(f'UPDATE {table} SET "{column}"=$1{cast}', column_value)
    finally:
        await fixture.admin.execute(f"ALTER TABLE {table} ENABLE TRIGGER USER")


@pytest.fixture
async def retained_address_custody_db(pipeline_db, bootstrap_db):
    """Own exact loading, immutable-base and independent final-custody resources."""
    case = pipeline_db
    connection = case.connection
    for extension in ("intarray", "postgis", "btree_gin"):
        await connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
    roles = tuple(sorted(case.writer_roles.values()))
    original_owner = await connection.fetchval("SELECT quote_ident(current_user)")
    control_owner = "retained_control_" + uuid4().hex
    await connection.execute(f'CREATE ROLE "{control_owner}" NOLOGIN')
    try:
        namespace = f'"{case.control_schema}"'
        await connection.execute(f'ALTER SCHEMA {namespace} OWNER TO "{control_owner}"')
        for table in ("network_membership_candidate", "network_candidate_custody"):
            await connection.execute(f'ALTER TABLE {namespace}.{table} OWNER TO "{control_owner}"')
        for role in roles:
            await connection.execute(f'REVOKE ALL ON {namespace}.network_candidate_custody FROM "{role}"')
            await connection.execute(f'GRANT SELECT ON {namespace}.network_candidate_custody TO "{role}"')
        await connection.execute(
            f"UPDATE {namespace}.registry_revision_control SET draft_revision=2,approved_revision=2"
        )
        for network_id in (7, 42, 88):
            await connection.execute(
                f"INSERT INTO {namespace}.registry_approved_record "
                "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
                "VALUES(2,'network',$1,1,1,$2::jsonb)",
                str(network_id),
                json.dumps(
                    dict(
                        network_id=network_id,
                        archived=False,
                        display_name="Synthetic network",
                        aliases=[],
                    )
                ),
            )
        case.complete_runtime_roles = roles
        yield case
    finally:
        await connection.execute("RESET ROLE")
        await connection.execute(f'ALTER SCHEMA "{case.control_schema}" OWNER TO {original_owner}')
        for table in ("network_membership_candidate", "network_candidate_custody"):
            await connection.execute(f'ALTER TABLE "{case.control_schema}".{table} OWNER TO {original_owner}')
        # The outer candidate fixture still owns exact candidate-schema cleanup.
        await _restore_candidate_owners(case, original_owner)
        await connection.execute(f'DROP OWNED BY "{control_owner}"')
        await connection.execute(f'DROP ROLE "{control_owner}"')


async def _retained_custody_seal(case):
    async def authorized(connection, target):
        return target == case.copy_target and await connection.fetchval("SELECT current_user=session_user")

    return await seal_network_candidate_custody(
        case.connection,
        case.copy_target,
        control_schema=case.control_schema,
        runtime_roles=case.complete_runtime_roles,
        authorize=authorized,
    )


async def _retained_custody_publisher_login(case):
    """Open the genuine publisher with the supported DSN and a generated fixture password."""
    role, password = case.writer_roles["publisher"], secrets.token_hex(24)
    await case.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    return role, await asyncpg.connect(dsn, user=role, password=password)


async def _retained_custody_captured_input(source, roles):
    reader, writer = await source.connect("reader"), await source.connect("publisher")
    identity = uuid4()
    source.captures.append("registry_fhir_capture_" + identity.hex)
    async with (
        reader.transaction(isolation="repeatable_read", readonly=True),
        writer.transaction(isolation="repeatable_read"),
    ):
        return await capture_retained_fhir_source(
            reader,
            writer,
            source.origin,
            capture_id=identity,
            owner_role=source.role_by_kind["owner"],
            runtime_roles=tuple(sorted(set(roles + (source.role_by_kind["reader"],)))),
        )


async def _retained_custody_base(case, source, serving, bootstrap):
    address = await _retained_addresses(source, serving)
    columns = ",".join('"' + name + '"' for name in EntityAddressUnified.__table__.columns.keys())
    await case.connection.execute(f'DELETE FROM "{bootstrap.source_schema}".entity_address_unified')
    await case.connection.execute(
        f'INSERT INTO "{bootstrap.source_schema}".entity_address_unified({columns}) '
        f'SELECT {columns} FROM "{address.schema_name}".entity_address_unified'
    )
    receipt = await _bootstrap_prepare(
        bootstrap,
        await _bootstrap_specification(bootstrap),
        runtime_roles=case.complete_runtime_roles,
    )
    return receipt.address_source


async def _retained_custody_compose(
    case,
    publisher,
    address,
    recipe,
    revision,
    *,
    previous=None,
    request=None,
    head=None,
):
    arguments_by_name = dict(
        request_id=request or uuid4(),
        approved_revision=revision,
        expected_head=(previous.generation_id if head is None and previous is not None else head or 0),
        address_sources=RegistryCompositionAddressSources(
            address, initial_source_recipes=(recipe,) if previous is None else ()
        ),
        source_manifest=previous,
        writer_roles=dict(
            owner_role=case.writer_roles["owner"],
            loader_roles=(case.writer_roles["loader"],),
            reader_roles=(case.writer_roles["reader"],),
        ),
        control_schema=case.control_schema,
    )
    result = await compose_registry_membership_candidate(publisher, **arguments_by_name)
    case.additional_schemas.append(result[0].schema_name)
    return result, arguments_by_name


async def _retained_custody_publish(case, target, address):
    case.copy_target, case.address_source = target, address
    arguments_by_name = dict(
        owner_role=case.writer_roles["owner"],
        loader_roles=(case.writer_roles["loader"],),
        reader_roles=(case.writer_roles["reader"],),
        control_schema=case.control_schema,
    )
    await case.connection.execute(f'SET ROLE "{case.writer_roles["publisher"]}"')
    try:
        prepared = await prepare_network_candidate(case.connection, target, address, **arguments_by_name)
        assert prepared["state"] == "ready"
    finally:
        await case.connection.execute("RESET ROLE")
    await _retained_custody_seal(case)
    await case.connection.execute(f'SET ROLE "{case.writer_roles["publisher"]}"')
    try:
        return await prepare_and_publish_network_candidate(case.connection, target, address, **arguments_by_name)
    finally:
        await case.connection.execute("RESET ROLE")


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_retained_fhir_reuses_independent_address_owner_through_native_custody(
    retained_address_custody_db, legacy_capture_db, serving_schema, bootstrap_db
):
    """Publish both retained profiles through distinct owners and actual native custody."""
    case, source_fixture = retained_address_custody_db, legacy_capture_db
    capture = await _retained_custody_captured_input(source_fixture, case.complete_runtime_roles)
    recipe, approved, network_id = await _approved_capture_recipe(source_fixture, serving_schema, capture)
    base = await _retained_custody_base(case, source_fixture, serving_schema, bootstrap_db)
    case.additional_schemas.append(case.copy_target.schema_name)
    role, connection = await _retained_custody_publisher_login(case)
    try:
        request = uuid4()
        (copy_target, address), args = await _retained_custody_compose(
            case, connection, base, recipe, approved.approved_revision, request=request
        )
        assert address == base
        retained_owner = await _assert_retained_actor_boundary(case, base)
        assert await case.connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL",
            copy_target.schema_name + ".provider_location_binding",
        )
        await _retained_custody_publish(case, copy_target, address)
        await _assert_retained_reach_recheck(case, copy_target, retained_owner)
        async with case.connection.transaction(isolation="repeatable_read", readonly=True):
            pin = await resolve_network_serving_manifest(case.connection, control_schema=case.control_schema)
            office_rows = await case.connection.fetch(
                f'SELECT canonical_network_ids FROM "{pin.schema_name}".entity_address_unified ORDER BY location_key'
            )
            assert [office_row["canonical_network_ids"] for office_row in office_rows] == [[network_id], []]
            assert await case.connection.fetchval(f'SELECT count(*) FROM "{pin.schema_name}".network_membership') == 1
    finally:
        await connection.close()
        await case.connection.execute(f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')


async def _assert_retained_actor_boundary(case, base):
    """Assert independent retained/loading/custody owners and the complete actor union."""
    retained_owner = await case.connection.fetchval(
        "SELECT nspowner FROM pg_namespace WHERE nspname=$1", base.schema_name
    )
    loading_owner = await case.connection.fetchval(
        "SELECT oid FROM pg_roles WHERE rolname=$1", case.writer_roles["owner"]
    )
    custody_owner = await case.connection.fetchval(
        "SELECT relowner FROM pg_class WHERE oid=to_regclass($1)",
        case.control_schema + ".network_candidate_custody",
    )
    assert len({retained_owner, loading_owner, custody_owner}) == 3
    for actor in case.complete_runtime_roles:
        assert not await case.connection.fetchval(
            "SELECT pg_has_role($1::name,$2::oid,'MEMBER') OR pg_has_role($1::name,$2::oid,'USAGE') OR pg_has_role($1::name,$2::oid,'SET')",
            actor,
            retained_owner,
        )
    return retained_owner


async def _assert_retained_reach_recheck(case, target, retained_owner):
    """An ordinary sealed replay must refuse a later SET-only owner path."""
    # A later SET-only owner edge must be refused by ordinary sealed-candidate rechecks.
    retained_name = await case.connection.fetchval("SELECT rolname FROM pg_roles WHERE oid=$1::oid", retained_owner)
    actor = case.writer_roles["owner"]
    await case.connection.execute(f'GRANT "{retained_name}" TO "{actor}" WITH INHERIT FALSE,SET TRUE')
    try:
        async with case.connection.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(NetworkCandidateCustodyError):
                await verify_network_candidate_custody(case.connection, target, control_schema=case.control_schema)
    finally:
        await case.connection.execute(f'REVOKE "{retained_name}" FROM "{actor}"')
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await verify_network_candidate_custody(case.connection, target, control_schema=case.control_schema)


@pytest.fixture
def verified_mapping_resources(monkeypatch):
    """Seal mixed source evidence before deriving native publication roots."""
    import sys

    original = _contents

    def mixed(source_id):
        resources = original(source_id)
        office = next(resource_document for kind, identity, resource_document in resources if identity == "office-one")
        changes_by_location = {
            "office-missing-base": {"second_line": "Suite 3"},
            "office-invalid-address": {"first_line": {"unsupported": True}},
            "office-mismatch": {"second_line": "Suite 4"},
            "office-cross-one": {"second_line": "Suite 5"},
            "office-cross-two": {"second_line": "Suite 5"},
        }
        for identity, changes in changes_by_location.items():
            resources.append(("Location", identity, {**office, **changes, "resource_id": identity}))
        for identity, npi in (("provider-bad", "1234567890"), ("provider-missing", None)):
            resources.append(
                (
                    "Practitioner",
                    identity,
                    {"source_id": source_id, "resource_id": identity, **({"npi": npi} if npi else {})},
                )
            )
        choices = (
            ("invalid-npi", "provider-bad", "office-one", "1234567890"),
            ("missing-npi", "provider-missing", "office-one", None),
            ("missing-base", "provider-one", "office-missing-base", "1234567893"),
            ("invalid-address", "provider-one", "office-invalid-address", "1234567893"),
            ("ambiguous", "provider-one", "office-two", "1234567893"),
            ("mismatch", "provider-one", "office-mismatch", "1234567893"),
            ("cross-one", "provider-one", "office-cross-one", "1234567893"),
            ("cross-two", "provider-one", "office-cross-two", "1234567893"),
            ("valid-repeat", "provider-one", "office-one", "1234567893"),
        )
        for identity, provider, location, npi in choices:
            resources.append(
                (
                    "PractitionerRole",
                    identity,
                    {
                        "source_id": source_id,
                        "resource_id": identity,
                        "practitioner_ref": "Practitioner/" + provider,
                        "location_refs": ["Location/" + location],
                        "network_refs": ["Organization/network-one"],
                        "insurance_plan_refs": [],
                        **({"npi": npi} if npi else {}),
                    },
                )
            )
        return resources

    monkeypatch.setattr(sys.modules[__name__], "_contents", mixed)


@pytest.fixture
async def verified_mapping_source(verified_mapping_resources, request):
    """Use the unchanged genuine capture lifecycle after synthetic source construction."""
    from contextlib import aclosing

    async with aclosing(legacy_capture_db.__wrapped__(request)) as lifecycle:
        yield await anext(lifecycle)


async def _verified_mapping_base(case, source_fixture, serving, bootstrap, all_skipped):
    """Freeze valid, missing and conflicting office correspondence in one native base."""
    import ptg2_address_canon

    address = await _retained_addresses(source_fixture, serving)
    namespace = '"' + address.schema_name + '"'
    canonical = ptg2_address_canon.canonicalize_batch(
        [
            ("123 Example St", "Suite 4", "Chicago", "IL", "60601", "US"),
            ("123 Example St", "Suite 5", "Chicago", "IL", "60601", "US"),
        ]
    )
    changes = [
        dict(location_key="3" * 64, checksum=3, first_line="999 Different Street"),
        dict(
            location_key="4" * 64,
            checksum=4,
            first_line="999 Different Street",
            second_line="Suite 4",
            address_key=canonical[0]["address_key"],
        ),
        dict(location_key="5" * 64, checksum=5, second_line="Suite 5", address_key=canonical[1]["address_key"]),
    ]
    for position, change in enumerate(changes):
        await case.connection.execute(
            f"INSERT INTO {namespace}.entity_address_unified SELECT (jsonb_populate_record(NULL::{namespace}.entity_address_unified,to_jsonb(base)||$1::jsonb)).* FROM {namespace}.entity_address_unified base WHERE location_key=$2",
            json.dumps(change),
            ("2" if position == 0 else "1") * 64,
        )
    if all_skipped:
        await case.connection.execute(f"DELETE FROM {namespace}.entity_address_unified WHERE location_key=$1", "1" * 64)
    columns = ",".join('"' + name + '"' for name in EntityAddressUnified.__table__.columns.keys())
    await case.connection.execute(f'DELETE FROM "{bootstrap.source_schema}".entity_address_unified')
    await case.connection.execute(
        f'INSERT INTO "{bootstrap.source_schema}".entity_address_unified({columns}) SELECT {columns} FROM {namespace}.entity_address_unified'
    )
    receipt = await _bootstrap_prepare(
        bootstrap, await _bootstrap_specification(bootstrap), runtime_roles=case.complete_runtime_roles
    )
    return receipt.address_source


async def _assert_verified_lookup_null_semantics(connection):
    """Keep opaque directory IDs and NULL NPI distinct from every real numeric value."""
    from process import network_initial_cms_office_bindings as offices

    suffix = uuid4().hex
    base_name, input_name = "initial_cms_scalar_base_" + suffix, "initial_cms_scalar_input_" + suffix
    await connection.execute(f"""CREATE TEMP TABLE "{base_name}"(
      entity_type text,entity_id text,npi bigint,address_key uuid,location_key text PRIMARY KEY,
      address_precision text,type text,inferred_npi bigint) ON COMMIT DROP""")
    await connection.execute(f"""CREATE TEMP TABLE "{input_name}"(
      page_key text,ordinal bigint,office jsonb,canonical jsonb) ON COMMIT DROP""")
    addresses = [UUID(int=number) for number in range(1, 7)]
    specifications = [
        ("provider_directory", "opaque-provider", None, addresses[0]),
        ("provider_directory", "opaque-provider", 0, addresses[0]),
        ("provider_directory", "opaque-provider", 0, addresses[1]),
        ("provider_directory", "opaque-provider", 9999999999, addresses[2]),
        ("npi", "1234567893", 1234567893, addresses[3]),
        ("npi", "1234567893", None, addresses[4]),
        ("provider_directory", "opaque-provider", None, addresses[5]),
        ("provider_directory", "opaque-provider", None, addresses[5]),
        ("provider_directory", "opaque-provider", None, None),
    ]
    base_records = [
        (*base_fields, f"{index:064x}", "street", "practice", None)
        for index, base_fields in enumerate(specifications, 1)
    ]
    await connection.copy_records_to_table(base_name, schema_name="pg_temp", records=base_records)
    input_records = [
        (
            str(index),
            1,
            json.dumps({"provider_system": system, "provider_id": identity}),
            json.dumps({"address_key": str(address) if address else None}),
        )
        for index, (system, identity, address) in enumerate(
            [
                ("provider_directory", "opaque-provider", addresses[0]),
                ("provider_directory", "opaque-provider", addresses[1]),
                ("provider_directory", "opaque-provider", addresses[2]),
                ("npi", "1234567893", addresses[3]),
                ("npi", "1234567893", addresses[4]),
                ("provider_directory", "opaque-provider", addresses[5]),
                ("provider_directory", "opaque-provider", None),
            ]
        )
    ]
    await connection.copy_records_to_table(input_name, schema_name="pg_temp", records=input_records)
    winners_name = await offices._verified_office_match_keys(connection, input_name, 'pg_temp."' + base_name + '"')
    actual = await connection.fetch(
        f'SELECT page_key,ordinal,location_key FROM pg_temp."{winners_name}" ORDER BY page_key'
    )
    assert [tuple(winning_row) for winning_row in actual] == [("0", 1, f"{1:064x}"), ("3", 1, f"{5:064x}")]


async def _interrupt_after_verified_match_parity(connection, matches, _copy_target):
    """Check original full-row JSON/SHA bytes before exercising rollback cleanup."""
    from process import network_initial_cms_office_bindings as offices

    table = "pg_temp." + offices._identifier(matches.table_name)
    prefix = f"""WITH offices AS MATERIALIZED (
      SELECT page_key,row_number() OVER(PARTITION BY page_key ORDER BY office::text) AS ordinal,
        office-'canonical'-'location_key'-'address'-'address_row_sha256' AS office,
        office->'canonical' AS canonical FROM {table}
    ), """
    original = prefix + offices._MATCH_BODY.replace("{page_column}", "page_key,")
    original = original.format(
        address=f"{offices._identifier(matches.address_source.schema_name)}.{offices._identifier(matches.address_source.table_name)}"
    )
    summary = "SELECT coalesce(jsonb_agg(jsonb_build_array(page_key,office) ORDER BY page_key,office::text),'[]'::jsonb)::text FROM "
    expected = await connection.fetchval(original + summary + "encoded WHERE invalid IS false")
    actual = await connection.fetchval(summary + table)
    assert actual == expected
    await _assert_verified_lookup_null_semantics(connection)
    raise asyncio.CancelledError


@pytest.mark.parametrize("all_skipped", [False, True])
async def test_verified_retained_mapping_is_one_exact_set_with_rollback_and_replay(
    retained_address_custody_db, verified_mapping_source, serving_schema, bootstrap_db, monkeypatch, all_skipped
):
    """Keep only fitting rows, reuse one match set, and preserve strict source replay."""
    from process import network_initial_cms_office_bindings as offices
    from process import registry_source_recipe_composition as source_pages
    from process.registry_source_recipe_composition import stage_registry_source_recipes

    case, source_fixture = retained_address_custody_db, verified_mapping_source
    capture = await _retained_custody_captured_input(source_fixture, case.complete_runtime_roles)
    recipe, approved, _ = await _approved_capture_recipe(source_fixture, serving_schema, capture)
    base = await _verified_mapping_base(case, source_fixture, serving_schema, bootstrap_db, all_skipped)
    role, publisher = await _retained_custody_publisher_login(case)
    original_page = source_pages._read_recipe_page
    original_copy = offices.copy_verified_retained_offices
    calls = []

    async def one_row_page(*arguments, **options):
        changed_arguments = list(arguments)
        changed_arguments[5] = 1
        calls.append(1)
        return await original_page(*changed_arguments, **options)

    monkeypatch.setattr(source_pages, "_read_recipe_page", one_row_page)
    try:
        async with publisher.transaction(isolation="repeatable_read"):
            with pytest.raises(ValueError, match="(?i)npi|unresolved"):
                await stage_registry_source_recipes(
                    publisher, (recipe,), approved, request_id=uuid4(), control_schema=case.control_schema
                )
        request = uuid4()
        monkeypatch.setattr(offices, "copy_verified_retained_offices", _interrupt_after_verified_match_parity)
        with pytest.raises(asyncio.CancelledError):
            await _retained_custody_compose(case, publisher, base, recipe, approved.approved_revision, request=request)
        assert not await publisher.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND (relname LIKE 'initial_cms_%' OR relname LIKE 'registry_recipe_%' OR relname LIKE 'registry_source_page_%'))"
        )
        monkeypatch.setattr(offices, "copy_verified_retained_offices", original_copy)
        calls.clear()
        (copy_target, address), arguments = await _retained_custody_compose(
            case, publisher, base, recipe, approved.approved_revision, request=request
        )
        assert len(calls) == dict(capture.origin.resource_counts)["PractitionerRole"] + 1
        assert await publisher.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == (
            0 if all_skipped else 2
        )
        bindings = await publisher.fetch(
            f'SELECT provider_id,location_key FROM "{copy_target.schema_name}".provider_location_binding'
        )
        assert [tuple(binding) for binding in bindings] == ([] if all_skipped else [("1234567893", "1" * 64)])
        assert await compose_registry_membership_candidate(publisher, **arguments) == (copy_target, address)
        await _retained_custody_publish(case, copy_target, address)
    finally:
        await publisher.close()
        await case.connection.execute(f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')
