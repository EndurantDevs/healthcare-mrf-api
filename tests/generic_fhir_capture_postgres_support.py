# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine normalized FHIR fixtures with original publication and native custody."""

import hashlib
import importlib.util
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import EntityAddressUnified
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_retained_capture import capture_retained_fhir_source
from process.network_fhir_retained_capture_contract import REFUSAL, SEMANTIC_PROFILE, RetainedFHIRInput
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.provider_directory_source_summary import ProviderDirectorySourceSummaryBinding, build_source_summary
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

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


async def _legacy_metadata(admin, schema, modern=False, missing_role=False, source_id="source-one"):
    """Publish exact synthetic resources under either supported metadata profile."""
    resources = _modern_resources(missing_role) if modern else _contents(source_id)
    resource_records, dataset_hash, counts, hashes = _proof_rows(
        resources, SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT if modern else LEGACY_RESOURCE_HASH_CONTRACT
    )
    summary = _fixture_summary(dataset_hash, counts, hashes, modern, resources, source_id=source_id)
    document = (
        _modern_metadata(resource_records, summary)
        if modern
        else {
            "source_ids": [source_id],
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
    await admin.execute(f"INSERT INTO \"{schema}\".provider_directory_source VALUES($1,'endpoint-one')", source_id)
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


async def _legacy_input(admin, schema, reader_role, metadata, summary, modern=False, source_id="source-one"):
    """Pin independently observed publication fields and their original refusal."""
    origin = RetainedFHIRInput(
        schema,
        source_id,
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


def _fixture_connections(dsn, role_by_kind, password, readers, engines):
    async def connect(role):
        connection = await asyncpg.connect(
            dsn.replace("postgresql+asyncpg://", "postgresql://"), user=role_by_kind[role], password=password
        )
        readers.append(connection)
        return connection

    def session_factory(role):
        url = make_url(dsn).set(drivername="postgresql+asyncpg", username=role_by_kind[role], password=password)
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        engines.append(engine)
        return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

    return connect, session_factory


@pytest.fixture
async def legacy_capture_db(request):
    """Create exact disposable native roles and register cleanup before setup."""
    is_modern = getattr(request, "param", None) in _MODERN_ROLE_CASE_BY_PROFILE
    source_id = getattr(request, "source_id", "source-one")
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    admin = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    schema = "legacy_fhir_test_" + suffix
    role_by_kind = {key: "retained_" + key + "_" + suffix for key in ("source", "owner", "reader", "publisher")}
    password = uuid4().hex + uuid4().hex
    readers, captures, engines = [], [], []
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
            source_id=source_id,
        )
        await _legacy_guards(admin, schema, role_by_kind["reader"])
        await _legacy_candidate_guards(admin, schema, role_by_kind["source"], captures)
        await admin.execute("RESET ROLE")
        origin = await _legacy_input(admin, schema, role_by_kind["reader"], metadata, summary, is_modern, source_id)
        connect, session_factory = _fixture_connections(dsn, role_by_kind, password, readers, engines)
        yield SimpleNamespace(
            admin=admin,
            schema=schema,
            role_by_kind=role_by_kind,
            origin=origin,
            connect=connect,
            captures=captures,
            metadata=metadata,
            session_factory=session_factory,
        )
    finally:
        for engine in engines:
            await engine.dispose()
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


def _fixture_summary(dataset_hash, counts, hashes, modern, resources, *, source_id="source-one"):
    """Bind source summary metrics to the exact synthetic resource census."""
    addressed_locations = sum(
        kind == "Location" and type(document.get("first_line")) is str and bool(document["first_line"].strip())
        for kind, _, document in resources
    )
    distinct_npis = {document["npi"] for _, _, document in resources if document.get("npi")}
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding("dataset-one", "endpoint-one", "run-one", dataset_hash),
        source_ids=[source_id],
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
