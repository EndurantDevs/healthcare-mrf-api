# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine observation Readers accept historical facts without Source admission."""

import json
import os
import secrets
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.registry_fhir_catalog_extraction import DEFAULT_BYTES
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_observed_source import (
    FHIRCatalogObservedSourceError,
    collect_fhir_catalog_observation,
)
from process.registry_fhir_catalog_reference_rollup import (
    MAX_PAGE_REFERENCES,
    fhir_catalog_reference_workspace,
    staged_fhir_catalog_references,
)
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile, validate_fhir_catalog_source_descriptor

pytestmark = pytest.mark.asyncio


async def _tables(admin, schema, layout):
    partition_layout = "LIST" if layout == "SCOPED" else layout
    await admin.execute(f'''CREATE TABLE "{schema}".provider_directory_endpoint_dataset(
      dataset_id varchar(96) PRIMARY KEY,endpoint_id varchar(64),acquisition_root_run_id varchar(64),
      dataset_hash varchar(64),status text,is_current boolean,published_at timestamp,resource_count bigint,
      previous_dataset_id varchar(96),publication_metadata_json json,publication_metadata_sha256 varchar(64),
      content_proof_admission_version integer,content_proof_admission_kind text,content_proof_admission_sha256 varchar(64),
      candidate_storage_json jsonb);
      CREATE TABLE "{schema}".provider_directory_dataset_resource(
      dataset_id varchar(96),resource_type varchar(64),resource_id varchar(64),payload_hash varchar(64),payload_json jsonb)
      {"" if layout == "heap" else "PARTITION BY " + partition_layout + "(dataset_id)"}''')
    if layout == "HASH":
        for remainder in range(2):
            await admin.execute(
                f'CREATE TABLE "{schema}".resources_{remainder} PARTITION OF "{schema}".provider_directory_dataset_resource '
                f"FOR VALUES WITH (MODULUS 2,REMAINDER {remainder})"
            )
    elif layout == "LIST":
        await admin.execute(
            f'CREATE TABLE "{schema}".resources_one PARTITION OF "{schema}".provider_directory_dataset_resource '
            "FOR VALUES IN ('dataset-one')"
        )
    elif layout == "SCOPED":
        await admin.execute(
            f'CREATE TABLE "{schema}".resources_one PARTITION OF "{schema}".provider_directory_dataset_resource '
            "FOR VALUES IN ('dataset-one') PARTITION BY RANGE(payload_hash);"
            f'CREATE TABLE "{schema}".resources_low PARTITION OF "{schema}".resources_one '
            "FOR VALUES FROM (MINVALUE) TO ('8');"
            f'CREATE TABLE "{schema}".resources_high PARTITION OF "{schema}".resources_one '
            "FOR VALUES FROM ('8') TO (MAXVALUE);"
            f'CREATE TABLE "{schema}".resources_acquiring '
            f'(LIKE "{schema}".provider_directory_dataset_resource INCLUDING ALL);'
            f'CREATE TABLE "{schema}".resources_detached '
            f'(LIKE "{schema}".provider_directory_dataset_resource INCLUDING ALL)'
        )


async def _seed(fixture, version):
    metadata = None if version is None else json.dumps({"resource_hash_contract": f"historical-v{version}"})
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".provider_directory_endpoint_dataset VALUES '
        "('dataset-one','endpoint-one','publication-root',$1,'published',true,'2026-01-01',7,'previous-one',$2::json,$3,$4,$5,$6,$7::jsonb)",
        "a" * 64,
        metadata,
        None if version is None else "b" * 64,
        version,
        None if version is None else "generic",
        None if version is None else "c" * 64,
        None if fixture.catalog == fixture.schema else json.dumps(_resource_map(fixture, "dataset-one")),
    )
    resources = [
        ("InsurancePlan", "plan", {"name": "Example", "network_refs": ["Organization/network"]}),
        ("Organization", "network", {"name": "Example Network", "type_codes": ["ntwk"]}),
        ("OrganizationAffiliation", "affiliation", {"network_refs": ["Organization/network"]}),
        ("PractitionerRole", "role", {"network_refs": ["Organization/network"]}),
    ]
    await fixture.admin.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name=fixture.schema,
        records=[
            ("dataset-one", kind, identity, "d" * 64, json.dumps(normalized_payload))
            for kind, identity, normalized_payload in resources
        ],
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_api_endpoint VALUES '
        "('endpoint-one','https://example.test/fhir')"
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_source VALUES '
        "('alias-b','endpoint-one'),('alias-a','endpoint-one')"
    )


async def _cleanup(fixture):
    for reader in fixture.readers:
        await reader.close()
    for schema in dict.fromkeys((fixture.catalog, fixture.schema)):
        await fixture.admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await fixture.admin.fetchval("SELECT to_regnamespace($1)", schema) is None
    for role in reversed(fixture.created_roles):
        if not await fixture.admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
            continue
        await fixture.admin.execute(f'DROP OWNED BY "{role}"')
        await fixture.admin.execute(f'DROP ROLE "{role}"')
        assert not await fixture.admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
    await fixture.admin.close()


async def _source_permissions(fixture):
    """Apply native read and write grants to the exact fixture relation names."""
    roles = fixture.roles_by_kind
    for schema in dict.fromkeys((fixture.schema, fixture.catalog)):
        await fixture.admin.execute(
            f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles["reader"]}","{roles["writer"]}";'
            f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "{schema}" TO "{roles["writer"]}"'
        )
    await fixture.admin.execute(
        f'GRANT SELECT(source_id,endpoint_id) ON "{fixture.catalog}".provider_directory_source TO "{roles["reader"]}";'
        f'GRANT SELECT(endpoint_id,canonical_api_base) ON "{fixture.catalog}".provider_directory_api_endpoint TO "{roles["reader"]}";'
        f"GRANT SELECT(dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,published_at,"
        f"resource_count,previous_dataset_id,publication_metadata_json,candidate_storage_json) "
        f'ON "{fixture.header_schema}".provider_directory_endpoint_dataset TO "{roles["reader"]}";'
        f'GRANT SELECT ON "{fixture.schema}".provider_directory_dataset_resource TO "{roles["reader"]}"'
    )
    if fixture.layout == "SCOPED":
        await fixture.admin.execute(
            f"GRANT SELECT(source_id,resource_id,network_id) ON "
            f'"{fixture.schema}".provider_directory_insurance_network_source_binding TO "{roles["reader"]}"'
        )
    await refresh_published_profile(fixture)


async def refresh_published_profile(fixture):
    """Select the fixture's real catalog and payload namespaces without physical policy."""
    fixture.profile = FHIRCatalogSourceReadProfile(fixture.schema, fixture.header_schema)


def _resource_map(fixture, dataset_id, name="provider_directory_dataset_resource"):
    """Locate observation data by name; this seed does not represent SDK admission."""
    return {
        "identity": {"dataset_id": dataset_id},
        "storage": {"catalog_schema": fixture.header_schema, "storage_schema": fixture.schema},
        "tables": {"provider_directory_dataset_resource": {"schema": fixture.schema, "name": name}},
    }


async def _scoped_metadata(fixture):
    """Keep private metadata columns and unpublished payloads outside the scoped grants."""
    await fixture.admin.execute(
        f'ALTER TABLE "{fixture.schema}".provider_directory_endpoint_dataset ADD COLUMN private_note text;'
        f'ALTER TABLE "{fixture.catalog}".provider_directory_source ADD COLUMN private_note text;'
        f'ALTER TABLE "{fixture.catalog}".provider_directory_api_endpoint ADD COLUMN private_note text;'
        f'CREATE TABLE "{fixture.schema}".provider_directory_insurance_network_source_binding('
        f"source_id varchar(64) NOT NULL,resource_id varchar(256) NOT NULL,network_id uuid NOT NULL,"
        f"private_note text,PRIMARY KEY(source_id,resource_id));"
        f'INSERT INTO "{fixture.schema}".provider_directory_insurance_network_source_binding '
        f"VALUES('alias-a','network','11111111-1111-4111-8111-111111111111',NULL);"
        f'INSERT INTO "{fixture.schema}".resources_acquiring '
        f"VALUES('dataset-acquiring','Organization','private',repeat('e',64),'{{}}');"
        f'INSERT INTO "{fixture.schema}".resources_detached '
        f"VALUES('dataset-detached','Organization','private',repeat('e',64),'{{}}')"
    )


@pytest.fixture
async def observed_source_db(request):
    """Create only task-unique Source identities and verify exact cleanup."""
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    layout, version = getattr(request, "param", ("HASH", 2))
    suffix, password = uuid4().hex, secrets.token_urlsafe(24)
    roles_by_kind = {kind: "fc_obs_" + kind + "_" + suffix for kind in ("owner", "reader", "writer")}
    fixture = SimpleNamespace(
        admin=await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://")),
        schema="fc_obs_payload_" + suffix,
        catalog=("fc_obs_payload_" if layout == "heap" else "fc_obs_catalog_") + suffix,
        header_schema=("fc_obs_payload_" if layout == "heap" else "fc_obs_catalog_") + suffix,
        roles_by_kind=roles_by_kind,
        readers=[],
        created_roles=[],
        layout=layout,
    )
    try:
        for kind, role in roles_by_kind.items():
            login = "LOGIN PASSWORD '" + password + "'" if kind == "reader" else "NOLOGIN"
            assert not await fixture.admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
            fixture.created_roles.append(role)
            await fixture.admin.execute(
                f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
        for schema in dict.fromkeys((fixture.schema, fixture.catalog)):
            await fixture.admin.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{roles_by_kind["owner"]}"')
        await _tables(fixture.admin, fixture.schema, layout)
        await fixture.admin.execute(f'''CREATE TABLE "{fixture.catalog}".provider_directory_source(
          source_id varchar(64),endpoint_id varchar(64));
          CREATE TABLE "{fixture.catalog}".provider_directory_api_endpoint(endpoint_id varchar(64),canonical_api_base text)''')
        await _seed(fixture, version)
        if layout == "SCOPED":
            await _scoped_metadata(fixture)
        if fixture.header_schema != fixture.schema:
            await fixture.admin.execute(
                f'ALTER TABLE "{fixture.schema}".provider_directory_endpoint_dataset SET SCHEMA "{fixture.header_schema}"'
            )
        await _source_permissions(fixture)

        async def connect():
            reader = await asyncpg.connect(
                dsn.replace("postgresql+asyncpg://", "postgresql://"),
                user=roles_by_kind["reader"],
                password=password,
            )
            fixture.readers.append(reader)
            if layout == "SCOPED":
                await reader.execute("SET search_path=pg_catalog,pg_temp,public")
            return reader

        fixture.connect = connect
        yield fixture
    finally:
        await _cleanup(fixture)


async def _observe(fixture, **changes):
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        return await collect_fhir_catalog_observation(
            reader,
            fixture.profile,
            endpoint_id="endpoint-one",
            dataset_id="dataset-one",
            **changes,
        )


@pytest.mark.parametrize("observed_source_db", [("HASH", 2), ("LIST", 3), ("heap", None)], indirect=True)
async def test_historical_proofs_are_provenance_not_admission_requirements(observed_source_db):
    fixture = observed_source_db
    first, replay = await _observe(fixture), await _observe(fixture)
    descriptor = first.descriptor
    assert first.status == "accepted" and first.reason is None
    assert first.accepted_at.utcoffset().total_seconds() == 0
    assert first.accepted_at == replay.accepted_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert descriptor == replay.descriptor and len(descriptor) == 13
    assert validate_fhir_catalog_source_descriptor(descriptor) == descriptor
    assert (
        descriptor["accepted_receipt_id"] == CATALOG_OBSERVATION_CONTRACT + ":" + descriptor["accepted_receipt_sha256"]
    )
    assert descriptor["source_aliases"] == ["alias-a", "alias-b"] and descriptor["complete"] is True
    assert descriptor["previous_dataset_id"] == "previous-one"
    admission = descriptor["admission"]
    assert all(family["rows"] == 1 for family in admission["families"].values())
    assert admission["publication"]["claimed_resource_count"] == 7
    assert admission["publication"]["published_at"] == "2026-01-01T00:00:00+00:00"
    assert descriptor["tables"] == {
        "header": {"schema": fixture.header_schema, "name": "provider_directory_endpoint_dataset"},
        "resource": {"schema": fixture.schema, "name": "provider_directory_dataset_resource"},
    }
    assert set(admission["publication"]) == {
        "root_run_id",
        "published_at",
        "resource_hash_contract",
        "claimed_resource_count",
    }
    assert admission["publication"]["resource_hash_contract"] == (
        None if fixture.layout == "heap" else "historical-v" + str(2 if fixture.layout == "HASH" else 3)
    )


async def test_empty_families_remain_explicit(observed_source_db):
    fixture = observed_source_db
    await fixture.admin.execute(f'DELETE FROM "{fixture.schema}".provider_directory_dataset_resource')
    await fixture.admin.execute(
        f'UPDATE "{fixture.header_schema}".provider_directory_endpoint_dataset SET previous_dataset_id=NULL'
    )
    observed = await _observe(fixture)
    assert observed.descriptor["previous_dataset_id"] is None
    assert len(observed.descriptor["admission"]["families"]) == 4
    assert all(family["rows"] == 0 for family in observed.descriptor["admission"]["families"].values())


@pytest.mark.parametrize("observed_source_db", [("HASH", 2), ("SCOPED", 2)], indirect=True)
async def test_repeatable_snapshot_pins_facts_while_other_authorized_writer_changes_them(observed_source_db):
    fixture = observed_source_db
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        first = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        await fixture.admin.execute(f'SET ROLE "{fixture.roles_by_kind["writer"]}"')
        try:
            await fixture.admin.execute(
                f'UPDATE "{fixture.schema}".provider_directory_dataset_resource SET payload_json=payload_json||\'{{"name":"Changed"}}\'::jsonb '
                "WHERE resource_type='Organization'"
            )
        finally:
            await fixture.admin.execute("RESET ROLE")
        pinned = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert pinned.descriptor == first.descriptor
    changed = await _observe(fixture)
    assert changed.descriptor["dataset_sha256"] == first.descriptor["dataset_sha256"]
    assert changed.descriptor["tables"] == first.descriptor["tables"]
    assert changed.descriptor["accepted_receipt_sha256"] != first.descriptor["accepted_receipt_sha256"]


async def test_superseded_source_can_be_replayed_without_requiring_current_head(observed_source_db):
    fixture = observed_source_db
    first = await _observe(fixture)
    await fixture.admin.execute(
        f"UPDATE \"{fixture.header_schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.header_schema}".provider_directory_endpoint_dataset '
        "SELECT 'dataset-two',endpoint_id,'next-root',dataset_hash,'published',true,published_at,resource_count,"
        "'dataset-one',publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,"
        "content_proof_admission_kind,content_proof_admission_sha256,"
        f"jsonb_set(candidate_storage_json,'{{identity,dataset_id}}','\"dataset-two\"') "
        f'FROM "{fixture.header_schema}".provider_directory_endpoint_dataset'
    )
    with pytest.raises(FHIRCatalogObservedSourceError, match="publication_invalid"):
        await _observe(fixture)
    historical = await _observe(fixture, require_current=False)
    assert historical.descriptor == first.descriptor
    assert historical.accepted_at == first.accepted_at


@pytest.mark.parametrize(
    "transaction",
    [None, ("read_committed", True), ("repeatable_read", False)],
    ids=["inactive", "read_committed", "writable"],
)
async def test_observation_requires_an_active_repeatable_snapshot(observed_source_db, transaction):
    fixture = observed_source_db
    reader = await fixture.connect()
    if transaction is None:
        with pytest.raises(ValueError, match="transaction_required"):
            await collect_fhir_catalog_observation(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
    else:
        async with reader.transaction(isolation=transaction[0], readonly=transaction[1]):
            if transaction[0] == "read_committed":
                with pytest.raises(ValueError, match="transaction_required"):
                    await collect_fhir_catalog_observation(
                        reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                    )
            else:
                observed = await collect_fhir_catalog_observation(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
                assert observed.status == "accepted"


@pytest.mark.parametrize("damage", ["resource_columns", "registration", "publication", "duplicate_alias"])
async def test_missing_source_data_and_invalid_registration_are_refused(observed_source_db, damage):
    fixture = observed_source_db
    statements_by_damage = {
        "resource_columns": [
            f'DROP TABLE "{fixture.schema}".provider_directory_dataset_resource CASCADE',
            f'CREATE VIEW "{fixture.schema}".provider_directory_dataset_resource AS SELECT 1 AS invalid',
            f'GRANT SELECT ON "{fixture.schema}".provider_directory_dataset_resource TO "{fixture.roles_by_kind["reader"]}"',
        ],
        "publication": [f'DELETE FROM "{fixture.header_schema}".provider_directory_endpoint_dataset'],
        "duplicate_alias": [
            f"INSERT INTO \"{fixture.catalog}\".provider_directory_source VALUES ('alias-a','endpoint-one')"
        ],
        "registration": [f'DELETE FROM "{fixture.catalog}".provider_directory_source'],
    }
    for statement in statements_by_damage[damage]:
        await fixture.admin.execute(statement)
    with pytest.raises(FHIRCatalogObservedSourceError):
        await _observe(fixture)


@pytest.mark.parametrize("observed_source_db", [("heap", 2)], indirect=True)
async def test_duplicate_key_at_page_boundary_cannot_hide_from_stream_digest(observed_source_db):
    fixture = observed_source_db
    await fixture.admin.execute(f'DELETE FROM "{fixture.schema}".provider_directory_dataset_resource')
    records = [("dataset-one", "Organization", f"{index:04}", "d" * 64, '{"name":"Example"}') for index in range(1000)]
    records.append(records[-1])
    await fixture.admin.copy_records_to_table(
        "provider_directory_dataset_resource", schema_name=fixture.schema, records=records
    )
    with pytest.raises(FHIRCatalogObservedSourceError, match="resource_count_changed"):
        await _observe(fixture)


async def test_ddl_busy_is_a_bounded_unavailable_read(observed_source_db):
    fixture = observed_source_db
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL lock_timeout='200ms'")
        async with fixture.admin.transaction():
            await fixture.admin.execute(
                f'LOCK TABLE "{fixture.schema}".provider_directory_dataset_resource IN ACCESS EXCLUSIVE MODE'
            )
            with pytest.raises(FHIRCatalogObservedSourceError, match="read_unavailable"):
                await collect_fhir_catalog_observation(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
        assert await reader.fetchval("SELECT 1") == 1
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.status == "accepted"


async def test_column_only_catalog_reader_pins_relations_until_outer_transaction_ends(observed_source_db):
    fixture = observed_source_db
    reader = await fixture.connect()
    catalog_relation = '"' + fixture.catalog + '".provider_directory_source'
    assert await reader.fetchval("SELECT count(source_id) FROM " + catalog_relation) == 2
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL lock_timeout='200ms'")
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.status == "accepted"
        assert await reader.fetchval("SELECT current_setting('lock_timeout')") == "200ms"
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with fixture.admin.transaction():
                await fixture.admin.execute("LOCK TABLE " + catalog_relation + " IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.parametrize("table", ["provider_directory_source", "provider_directory_api_endpoint"])
async def test_catalog_ddl_timeout_restores_reader_setting_and_snapshot(observed_source_db, table):
    fixture = observed_source_db
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL lock_timeout='200ms'")
        async with fixture.admin.transaction():
            await fixture.admin.execute(f'LOCK TABLE "{fixture.catalog}"."{table}" IN ACCESS EXCLUSIVE MODE')
            with pytest.raises(FHIRCatalogObservedSourceError, match="read_unavailable"):
                await collect_fhir_catalog_observation(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
        assert await reader.fetchval("SELECT current_setting('lock_timeout')") == "200ms"
        assert await reader.fetchval("SELECT 1") == 1
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.status == "accepted"


async def test_repeated_role_references_use_native_rollup_instead_of_expanding_edge_limit(observed_source_db):
    fixture = observed_source_db
    await fixture.admin.execute(f'DELETE FROM "{fixture.schema}".provider_directory_dataset_resource')
    role_payload = json.dumps({"network_refs": ["Organization/network"] * 11})
    await fixture.admin.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name=fixture.schema,
        records=[
            ("dataset-one", "PractitionerRole", f"role-{index:04}", "d" * 64, role_payload) for index in range(1000)
        ],
    )
    reader = await fixture.connect()
    async with (
        fhir_catalog_reference_workspace(reader) as workspace,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        await reader.execute("SET LOCAL search_path=pg_catalog,pg_temp,public")
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.descriptor["admission"]["families"]["PractitionerRole"]["rows"] == 1000
        async with staged_fhir_catalog_references(
            reader,
            schema_name=fixture.schema,
            dataset_id="dataset-one",
            source_uri="https://example.test/fhir",
            workspace=workspace,
        ) as stage:
            batches = [batch async for batch in stage.iter_batches()]
            assert stage.accounting.source_rows == 1000 and stage.accounting.reference_rows == 11000
            assert stage.accounting.source_bytes < DEFAULT_BYTES
            assert stage.accounting.distinct_references == 1
            assert len(batches) == len(batches[0]) == 1
            assert batches[0][0].reference_rows == 11000
            assert batches[0][0].edge.network_resource_id == "network"


@pytest.mark.parametrize(
    "references",
    [[{"reference": "Organization/network"}], ["Organization/network"] * (MAX_PAGE_REFERENCES + 1)],
    ids=["malformed-element", "reference-limit"],
)
async def test_consumed_role_reference_values_and_bounds_are_still_validated(observed_source_db, references):
    fixture = observed_source_db
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".provider_directory_dataset_resource SET payload_json=$1::jsonb '
        "WHERE resource_type='PractitionerRole'",
        json.dumps({"network_refs": references}),
    )
    with pytest.raises(ValueError):
        await _observe(fixture)


async def _assert_metadata_columns_only(reader, fixture):
    """Leave private metadata columns outside the native reader's selected projection."""
    for schema, name in (
        (fixture.header_schema, "provider_directory_endpoint_dataset"),
        (fixture.catalog, "provider_directory_source"),
        (fixture.catalog, "provider_directory_api_endpoint"),
        (fixture.schema, "provider_directory_insurance_network_source_binding"),
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with reader.transaction():
                await reader.fetch(f'SELECT * FROM ONLY "{schema}"."{name}"')


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_named_reader_reads_published_facts_and_denies_private_data_and_writes(observed_source_db):
    """Native grants keep separate unpublished payloads and private columns unreadable."""
    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert all(family["rows"] == 1 for family in observed.descriptor["admission"]["families"].values())
        await _assert_metadata_columns_only(reader, fixture)
        for name in ("resources_acquiring", "resources_detached"):
            relation = f'"{fixture.schema}"."{name}"'
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with reader.transaction():
                    await reader.fetch("SELECT * FROM " + relation)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with reader.transaction():
                    await reader.copy_from_query("SELECT * FROM " + relation, output=lambda value: None)
        with pytest.raises((asyncpg.InsufficientPrivilegeError, asyncpg.ReadOnlySQLTransactionError)):
            async with reader.transaction():
                await reader.execute(
                    f'UPDATE "{fixture.schema}".provider_directory_dataset_resource SET payload_hash=payload_hash'
                )
        async with fixture.admin.transaction():
            await fixture.admin.execute(
                f'LOCK TABLE "{fixture.schema}".resources_acquiring IN ACCESS EXCLUSIVE MODE NOWAIT'
            )
            assert (
                await fixture.admin.execute(
                    f'UPDATE "{fixture.schema}".resources_acquiring SET payload_json=$${{"progress":true}}$$'
                )
                == "UPDATE 1"
            )

    relation = f'"{fixture.schema}".provider_directory_dataset_resource'
    for statement in (
        f"INSERT INTO {relation} SELECT * FROM {relation}",
        f"UPDATE {relation} SET payload_hash=payload_hash",
        f"DELETE FROM {relation}",
        f"TRUNCATE {relation}",
        f'CREATE TABLE "{fixture.schema}".unapproved(id integer)',
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with reader.transaction():
                await reader.execute(statement)


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("damage", ["header", "alias"])
async def test_logical_publication_drift_refuses_the_original_descriptor(observed_source_db, damage):
    """Current-source revalidation compares complete publication and registration data."""
    from process.registry_fhir_catalog_sources import require_current_fhir_catalog_sources

    fixture = observed_source_db
    first = await _observe(fixture)
    if damage == "header":
        await fixture.admin.execute(
            f'UPDATE "{fixture.header_schema}".provider_directory_endpoint_dataset SET dataset_hash=repeat($$e$$,64)'
        )
    else:
        await fixture.admin.execute(
            f'INSERT INTO "{fixture.catalog}".provider_directory_source(source_id,endpoint_id) VALUES($$new-alias$$,$$endpoint-one$$)'
        )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError, match="source_changed"):
            await require_current_fhir_catalog_sources(reader, (first.descriptor,))


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_named_resource_read_blocks_ddl_until_snapshot_ends(observed_source_db):
    """Ordinary SELECT locks keep the selected relation usable for the pinned reader."""
    fixture, reader = observed_source_db, await observed_source_db.connect()
    relation = f'"{fixture.schema}".provider_directory_dataset_resource'
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.status == "accepted"
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with fixture.admin.transaction():
                await fixture.admin.execute("LOCK TABLE " + relation + " IN ACCESS EXCLUSIVE MODE NOWAIT")
    async with fixture.admin.transaction():
        await fixture.admin.execute("LOCK TABLE " + relation + " IN ACCESS EXCLUSIVE MODE NOWAIT")


async def _inventory(fixture, reader):
    """Consume the maintained bounded source inventory in the caller's snapshot."""
    from process.registry_fhir_catalog_sources import iter_fhir_catalog_sources

    return [
        item
        async for page in iter_fhir_catalog_sources(
            reader, fixture.profile, page_size=1, observation_contract=CATALOG_OBSERVATION_CONTRACT
        )
        for item in page
    ]


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_named_inventory_uses_column_projections_and_observation_data(observed_source_db):
    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await _assert_metadata_columns_only(reader, fixture)
        dispositions = await _inventory(fixture, reader)
    assert len(dispositions) == 1 and dispositions[0].status == "accepted"
    assert dispositions[0].descriptor["admission"]["kind"] == "catalog_observation"
    assert dispositions[0].descriptor["tables"]["resource"]["schema"] == fixture.schema


async def _add_second_scope_entry(fixture, *, published):
    """Seed an independent endpoint and actual named data relation for inventory tests."""
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.header_schema}".provider_directory_endpoint_dataset '
        "(dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,published_at,resource_count,"
        "previous_dataset_id,publication_metadata_json,candidate_storage_json) "
        "SELECT 'dataset-acquiring','endpoint-two','other-root',repeat('e',64),$1,$2,published_at,1,NULL,"
        f'publication_metadata_json,$3::jsonb FROM "{fixture.header_schema}".provider_directory_endpoint_dataset '
        "WHERE dataset_id='dataset-one'",
        "published" if published else "acquiring",
        published,
        json.dumps(_resource_map(fixture, "dataset-acquiring", "resources_acquiring")),
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_api_endpoint(endpoint_id,canonical_api_base) '
        "VALUES('endpoint-two','https://example.test/other');"
        f'INSERT INTO "{fixture.catalog}".provider_directory_source(source_id,endpoint_id) VALUES($$alias-c$$,$$endpoint-two$$)'
    )
    if published:
        await fixture.admin.execute(
            f'GRANT SELECT ON "{fixture.schema}".resources_acquiring TO "{fixture.roles_by_kind["reader"]}"'
        )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_inventory_reports_unpublished_second_endpoint_without_accepting_it(observed_source_db):
    fixture, reader = observed_source_db, await observed_source_db.connect()
    await _add_second_scope_entry(fixture, published=False)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        dispositions = await _inventory(fixture, reader)
    assert [(item.endpoint_id, item.status) for item in dispositions] == [
        ("endpoint-one", "accepted"),
        ("endpoint-two", "unavailable"),
    ]
    with pytest.raises(FHIRCatalogObservedSourceError, match="publication_invalid"):
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await collect_fhir_catalog_observation(
                reader, fixture.profile, endpoint_id="endpoint-two", dataset_id="dataset-acquiring"
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_distinct_dataset_observations_keep_exact_content_identity(observed_source_db):
    fixture, reader = observed_source_db, await observed_source_db.connect()
    await _add_second_scope_entry(fixture, published=True)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        first = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        second = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-two", dataset_id="dataset-acquiring"
        )
    assert first.descriptor["native_read_profile"] == second.descriptor["native_read_profile"]
    assert first.descriptor["accepted_receipt_sha256"] != second.descriptor["accepted_receipt_sha256"]
    assert second.descriptor["coordinates"]["dataset_id"] == "dataset-acquiring"
    assert second.descriptor["tables"]["resource"]["name"] == "resources_acquiring"


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_bad_facts_do_not_poison_the_next_inventory_endpoint(observed_source_db):
    fixture = observed_source_db
    await _add_second_scope_entry(fixture, published=True)
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".resources_one SET payload_json=$${{"name":42}}$$ '
        "WHERE resource_type='Organization' AND resource_id='network'"
    )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        dispositions = await _inventory(fixture, reader)
    assert [(item.endpoint_id, item.status) for item in dispositions] == [
        ("endpoint-one", "unavailable"),
        ("endpoint-two", "accepted"),
    ]
    assert dispositions[0].descriptor is None and dispositions[1].descriptor is not None


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("revocation", ["resource_select", "header_column", "catalog_usage"])
async def test_named_reads_require_actual_projection_privileges(observed_source_db, revocation):
    """Prior successful reads cannot replace PostgreSQL's current positive access checks."""
    fixture = observed_source_db
    await _observe(fixture)
    reader_role = fixture.roles_by_kind["reader"]
    statements_by_revocation = {
        "resource_select": f'REVOKE SELECT ON "{fixture.schema}".provider_directory_dataset_resource FROM "{reader_role}"',
        "header_column": f'REVOKE SELECT(publication_metadata_json) ON "{fixture.header_schema}".provider_directory_endpoint_dataset FROM "{reader_role}"',
        "catalog_usage": f'REVOKE USAGE ON SCHEMA "{fixture.catalog}" FROM "{reader_role}"',
    }
    await fixture.admin.execute(statements_by_revocation[revocation])
    with pytest.raises(FHIRCatalogObservedSourceError, match="read_unavailable"):
        await _observe(fixture)


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("namespace", ["source", "catalog", "both"])
async def test_named_observation_accepts_quoted_namespace_names(observed_source_db, namespace):
    """Resolve actual names after namespace relocation without stored physical coordinates."""
    fixture = observed_source_db
    for attribute in ("schema", "catalog"):
        if namespace != "both" and attribute != ("schema" if namespace == "source" else "catalog"):
            continue
        previous = getattr(fixture, attribute)
        selected = ("ObservedPayload_" if attribute == "schema" else "ObservedCatalog_") + uuid4().hex
        await fixture.admin.execute(f'ALTER SCHEMA "{previous}" RENAME TO "{selected}"')
        setattr(fixture, attribute, selected)
    fixture.header_schema = fixture.catalog
    await fixture.admin.execute(
        f'UPDATE "{fixture.header_schema}".provider_directory_endpoint_dataset SET candidate_storage_json=$1::jsonb',
        json.dumps(_resource_map(fixture, "dataset-one")),
    )
    await refresh_published_profile(fixture)
    observed = await _observe(fixture)
    assert observed.status == "accepted"
    assert observed.descriptor["native_read_profile"] == fixture.profile.document()
    assert observed.descriptor["tables"]["header"]["schema"] == fixture.catalog
    assert observed.descriptor["tables"]["resource"]["schema"] == fixture.schema
