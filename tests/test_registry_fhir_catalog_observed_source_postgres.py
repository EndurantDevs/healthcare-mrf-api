# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine observation Readers accept historical facts without Source admission."""

import asyncio
import hashlib
import json
import os
import secrets
from collections import Counter
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, fhir_catalog_json_bytes
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
      content_proof_admission_version integer,content_proof_admission_kind text,content_proof_admission_sha256 varchar(64));
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
            f'CREATE TABLE "{schema}".resources_acquiring PARTITION OF "{schema}".provider_directory_dataset_resource '
            "FOR VALUES IN ('dataset-acquiring');"
            f'CREATE TABLE "{schema}".resources_detached '
            f'(LIKE "{schema}".provider_directory_dataset_resource INCLUDING ALL)'
        )


async def _seed(fixture, version):
    metadata = None if version is None else json.dumps({"resource_hash_contract": f"historical-v{version}"})
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".provider_directory_endpoint_dataset VALUES '
        "('dataset-one','endpoint-one','publication-root',$1,'published',true,'2026-01-01',7,'previous-one',$2::json,$3,$4,$5,$6)",
        "a" * 64,
        metadata,
        None if version is None else "b" * 64,
        version,
        None if version is None else "generic",
        None if version is None else "c" * 64,
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
    for schema in (fixture.catalog, fixture.schema):
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
    roles_by_kind = fixture.roles_by_kind
    for relation in await fixture.admin.fetch(
        "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=ANY($1::text[]) AND c.relkind IN ('r','p')",
        [fixture.schema, fixture.catalog],
    ):
        await fixture.admin.execute(
            f'ALTER TABLE "{relation["nspname"]}"."{relation["relname"]}" OWNER TO "{roles_by_kind["owner"]}"'
        )
    for schema in (fixture.schema, fixture.catalog):
        await fixture.admin.execute(
            f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles_by_kind["reader"]}","{roles_by_kind["writer"]}"'
        )
        if schema == fixture.schema and fixture.layout != "SCOPED":
            await fixture.admin.execute(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles_by_kind["reader"]}"'
            )
        await fixture.admin.execute(
            f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "{schema}" TO "{roles_by_kind["writer"]}"'
        )
    await fixture.admin.execute(
        f'GRANT SELECT(tableoid,source_id,endpoint_id) ON "{fixture.catalog}".provider_directory_source '
        f'TO "{roles_by_kind["reader"]}"'
    )
    await fixture.admin.execute(
        f'GRANT SELECT(tableoid,endpoint_id,canonical_api_base) ON "{fixture.catalog}".provider_directory_api_endpoint '
        f'TO "{roles_by_kind["reader"]}"'
    )
    fixture.profile = FHIRCatalogSourceReadProfile(
        fixture.schema,
        roles_by_kind["owner"],
        (roles_by_kind.get("runtime", roles_by_kind["reader"]),),
        fixture.catalog,
    )
    if fixture.layout == "SCOPED":
        await _published_permissions(fixture)
        await refresh_published_profile(fixture)


async def _published_permissions(fixture):
    """Grant a separate login only the exact published leaves and metadata columns."""
    reader, runtime = fixture.roles_by_kind["reader"], fixture.roles_by_kind["runtime"]
    await fixture.admin.execute(
        f'GRANT USAGE ON SCHEMA "{fixture.schema}","{fixture.catalog}" TO "{runtime}";'
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.schema}","{fixture.catalog}" TO "{runtime}";'
        f'GRANT SELECT ON "{fixture.schema}".resources_low,"{fixture.schema}".resources_high TO "{reader}";'
        f"GRANT SELECT(tableoid,dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,"
        f"published_at,resource_count,previous_dataset_id,publication_metadata_json,publication_metadata_sha256,"
        f"content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256) "
        f'ON "{fixture.schema}".provider_directory_endpoint_dataset TO "{reader}";'
        f"GRANT SELECT(tableoid,source_id,resource_id,network_id) "
        f'ON "{fixture.schema}".provider_directory_insurance_network_source_binding TO "{reader}";'
        f'GRANT EXECUTE ON FUNCTION pg_control_system() TO "{reader}"'
    )


async def _published_publication(fixture, dataset_id="dataset-one"):
    """Read the fixture's actual publication fields for the exact scope digest."""
    publication_by_field = dict(
        await fixture.admin.fetchrow(
            f"SELECT published_at,acquisition_root_run_id AS root_run_id,"
            f"publication_metadata_json->>'resource_hash_contract' AS resource_hash_contract,"
            f"content_proof_admission_version AS original_admission_version,"
            f"content_proof_admission_kind AS original_admission_kind,"
            f"content_proof_admission_sha256 AS original_admission_sha256,"
            f"publication_metadata_sha256 AS original_metadata_sha256,resource_count AS claimed_resource_count "
            f'FROM "{fixture.schema}".provider_directory_endpoint_dataset WHERE dataset_id=$1',
            dataset_id,
        )
    )
    publication_by_field["published_at"] = publication_by_field["published_at"].replace(tzinfo=timezone.utc).isoformat()
    return publication_by_field


async def refresh_published_profile(fixture):
    """Issue a synthetic exact scope from this fixture's current native identities."""
    from process.registry_fhir_catalog_published_read import parse_fhir_catalog_published_read_scope

    async def relation(schema, name):
        """Pin an exact fixture relation without deriving admission from its ACL."""
        return {
            "schema_name": schema,
            "relation_name": name,
            "relation_oid": await fixture.admin.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}"."{name}"'),
        }

    publication_by_field = await _published_publication(fixture)
    leaves = [await relation(fixture.schema, name) for name in ("resources_low", "resources_high")]
    scope_by_field = {
        "version": 1,
        "reader_role": fixture.roles_by_kind["reader"],
        "reader_oid": await fixture.admin.fetchval(
            "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", fixture.roles_by_kind["reader"]
        ),
        "database_oid": await fixture.admin.fetchval(
            "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
        ),
        "database_system_identifier": str(
            await fixture.admin.fetchval("SELECT system_identifier FROM pg_control_system()")
        ),
        "header": {
            "relation": await relation(fixture.schema, "provider_directory_endpoint_dataset"),
            "projection": "catalog_observation_v1",
        },
        "source_registration": await relation(fixture.catalog, "provider_directory_source"),
        "endpoint_registration": await relation(fixture.catalog, "provider_directory_api_endpoint"),
        "resource_root": await relation(fixture.schema, "provider_directory_dataset_resource"),
        "bindings": {
            "relation": await relation(fixture.schema, "provider_directory_insurance_network_source_binding"),
            "source_aliases": ["alias-a", "alias-b"],
        },
        "datasets": [
            {
                "endpoint_id": "endpoint-one",
                "dataset_id": "dataset-one",
                "dataset_sha256": "a" * 64,
                "source_aliases": ["alias-a", "alias-b"],
                "resource_leaves": sorted(leaves, key=lambda leaf: leaf["relation_oid"]),
                "publication_sha256": hashlib.sha256(fhir_catalog_json_bytes(publication_by_field)).hexdigest(),
            }
        ],
    }
    fixture.profile = FHIRCatalogSourceReadProfile(
        fixture.schema,
        fixture.roles_by_kind["owner"],
        (fixture.roles_by_kind["runtime"],),
        fixture.catalog,
        published_read_scope=parse_fhir_catalog_published_read_scope(scope_by_field),
    )


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
    if layout == "SCOPED":
        roles_by_kind["runtime"] = "fc_obs_runtime_" + suffix
    fixture = SimpleNamespace(
        admin=await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://")),
        schema="fc_obs_payload_" + suffix,
        catalog="fc_obs_catalog_" + suffix,
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
        for schema in (fixture.schema, fixture.catalog):
            await fixture.admin.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{roles_by_kind["owner"]}"')
        await _tables(fixture.admin, fixture.schema, layout)
        await fixture.admin.execute(f'''CREATE TABLE "{fixture.catalog}".provider_directory_source(
          source_id varchar(64),endpoint_id varchar(64));
          CREATE TABLE "{fixture.catalog}".provider_directory_api_endpoint(endpoint_id varchar(64),canonical_api_base text)''')
        await _seed(fixture, version)
        if layout == "SCOPED":
            await _scoped_metadata(fixture)
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
    assert descriptor == replay.descriptor and len(descriptor) == 17
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
    expected_version = await fixture.admin.fetchval(
        f'SELECT content_proof_admission_version FROM "{fixture.schema}".provider_directory_endpoint_dataset'
    )
    assert admission["publication"]["original_admission_version"] == expected_version
    if expected_version is None:
        assert all(
            admission["publication"][field] is None
            for field in (
                "resource_hash_contract",
                "original_admission_version",
                "original_admission_kind",
                "original_admission_sha256",
                "original_metadata_sha256",
            )
        )
    leaf_oids = await fixture.admin.fetch(
        "SELECT relid::oid::bigint AS oid FROM pg_partition_tree($1::regclass) WHERE isleaf",
        f'"{fixture.schema}".provider_directory_dataset_resource',
    )
    expected_leaves = (
        [descriptor["resource_table_oid"]]
        if fixture.layout == "heap"
        else sorted(leaf_row["oid"] for leaf_row in leaf_oids)
    )
    assert descriptor["resource_leaf_oids"] == expected_leaves


async def test_empty_families_remain_explicit(observed_source_db):
    fixture = observed_source_db
    await fixture.admin.execute(f'DELETE FROM "{fixture.schema}".provider_directory_dataset_resource')
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".provider_directory_endpoint_dataset SET previous_dataset_id=NULL'
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
    assert changed.descriptor["native_catalog_sha256"] == first.descriptor["native_catalog_sha256"]
    assert changed.descriptor["accepted_receipt_sha256"] != first.descriptor["accepted_receipt_sha256"]


async def test_superseded_source_can_be_replayed_without_requiring_current_head(observed_source_db):
    fixture = observed_source_db
    first = await _observe(fixture)
    await fixture.admin.execute(
        f"UPDATE \"{fixture.schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".provider_directory_endpoint_dataset '
        "SELECT 'dataset-two',endpoint_id,'next-root',dataset_hash,'published',true,published_at,resource_count,"
        "'dataset-one',publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,"
        f'content_proof_admission_kind,content_proof_admission_sha256 FROM "{fixture.schema}".provider_directory_endpoint_dataset'
    )
    with pytest.raises(FHIRCatalogObservedSourceError, match="publication_invalid"):
        await _observe(fixture)
    historical = await _observe(fixture, require_current=False)
    assert historical.descriptor == first.descriptor
    assert historical.accepted_at == first.accepted_at


@pytest.mark.parametrize(
    "unsafe", ["writable", "read_committed", "owner_set", "writer_set", "schema_create", "catalog_update"]
)
async def test_genuine_reader_rejects_transaction_and_reachable_write_paths(observed_source_db, unsafe):
    fixture = observed_source_db
    reader_role = fixture.roles_by_kind["reader"]
    if unsafe in {"owner_set", "writer_set"}:
        target = fixture.roles_by_kind["owner" if unsafe == "owner_set" else "writer"]
        await fixture.admin.execute(f'GRANT "{target}" TO "{reader_role}" WITH INHERIT FALSE,SET TRUE')
    elif unsafe == "schema_create":
        await fixture.admin.execute(f'GRANT CREATE ON SCHEMA "{fixture.catalog}" TO "{reader_role}"')
    elif unsafe == "catalog_update":
        await fixture.admin.execute(
            f'GRANT UPDATE(source_id) ON "{fixture.catalog}".provider_directory_source TO "{reader_role}"'
        )
    reader = await fixture.connect()
    async with reader.transaction(
        isolation="read_committed" if unsafe == "read_committed" else "repeatable_read", readonly=unsafe != "writable"
    ):
        with pytest.raises(FHIRCatalogObservedSourceError, match="reader_unsafe|reader_write_path"):
            await collect_fhir_catalog_observation(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.parametrize(
    "damage",
    [
        "resource_owner",
        "row_security",
        "unlogged",
        "resource_view",
        "registration",
        "publication",
        "no_resource_leaves",
        "duplicate_alias",
    ],
)
async def test_physical_or_registration_changes_are_not_accepted(observed_source_db, damage):
    fixture = observed_source_db
    statements_by_damage = {
        "resource_owner": [f'ALTER TABLE "{fixture.schema}".resources_0 OWNER TO "{fixture.roles_by_kind["writer"]}"'],
        "row_security": [
            f'ALTER TABLE "{fixture.schema}".provider_directory_endpoint_dataset ENABLE ROW LEVEL SECURITY'
        ],
        "unlogged": [f'ALTER TABLE "{fixture.schema}".provider_directory_endpoint_dataset SET UNLOGGED'],
        "resource_view": [
            f'DROP TABLE "{fixture.schema}".provider_directory_dataset_resource CASCADE',
            f'CREATE VIEW "{fixture.schema}".provider_directory_dataset_resource AS SELECT 1 AS invalid',
            f'GRANT SELECT ON "{fixture.schema}".provider_directory_dataset_resource TO "{fixture.roles_by_kind["reader"]}"',
        ],
        "publication": [f'DELETE FROM "{fixture.schema}".provider_directory_endpoint_dataset'],
        "no_resource_leaves": [f'DROP TABLE "{fixture.schema}".resources_{remainder}' for remainder in range(2)],
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
    assert not await reader.fetchval("SELECT has_table_privilege(current_user,$1,'SELECT')", catalog_relation)
    assert await reader.fetchval("SELECT has_column_privilege(current_user,$1,'tableoid','SELECT')", catalog_relation)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL lock_timeout='7s'")
        observed = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        assert observed.status == "accepted"
        assert await reader.fetchval("SELECT current_setting('lock_timeout')") == "7s"
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with fixture.admin.transaction():
                await fixture.admin.execute("LOCK TABLE " + catalog_relation + " IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.parametrize("table", ["provider_directory_source", "provider_directory_api_endpoint"])
async def test_catalog_ddl_timeout_restores_reader_setting_and_snapshot(observed_source_db, table):
    fixture = observed_source_db
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.execute("SET LOCAL lock_timeout='7s'")
        async with fixture.admin.transaction():
            await fixture.admin.execute(f'LOCK TABLE "{fixture.catalog}"."{table}" IN ACCESS EXCLUSIVE MODE')
            with pytest.raises(FHIRCatalogObservedSourceError, match="read_unavailable"):
                await collect_fhir_catalog_observation(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
        assert await reader.fetchval("SELECT current_setting('lock_timeout')") == "7s"
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
    """Keep unprojected columns inaccessible even when all required metadata is readable."""
    for schema, name in (
        (fixture.schema, "provider_directory_endpoint_dataset"),
        (fixture.catalog, "provider_directory_source"),
        (fixture.catalog, "provider_directory_api_endpoint"),
        (fixture.schema, "provider_directory_insurance_network_source_binding"),
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with reader.transaction():
                await reader.fetch(f'SELECT * FROM ONLY "{schema}"."{name}"')


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_leaf_login_reads_without_parent_or_private_candidate_access(observed_source_db):
    """Read all four families while acquiring and detached payloads stay inaccessible."""
    from process.registry_fhir_catalog_grant import _reader_identity
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture, reader = observed_source_db, await observed_source_db.connect()
    identity = await reader.fetchrow("SELECT session_user,current_user")
    assert identity["session_user"] == identity["current_user"] == fixture.roles_by_kind["reader"]
    assert fixture.roles_by_kind["reader"] not in fixture.profile.runtime_roles
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        published = await pin_fhir_catalog_published_read(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        resources = await reader.fetch("SELECT * FROM (" + published.resource_sql() + ") AS catalog_resources")
        assert {resource["resource_type"] for resource in resources} == {
            "Organization",
            "InsurancePlan",
            "OrganizationAffiliation",
            "PractitionerRole",
        }
        assert {resource["source_table_oid"] for resource in resources} <= set(published.native["resource_leaf_oids"])
        for name in (
            "provider_directory_dataset_resource",
            "resources_one",
            "resources_acquiring",
            "resources_detached",
        ):
            relation = f'"{fixture.schema}"."{name}"'
            assert not await reader.fetchval("SELECT has_table_privilege(current_user,$1,'SELECT')", relation)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with reader.transaction():
                    await reader.fetch("SELECT * FROM ONLY " + relation)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with reader.transaction():
                    await reader.copy_from_query("SELECT * FROM ONLY " + relation, output=lambda value: None)
        await _assert_metadata_columns_only(reader, fixture)
        async with fixture.admin.transaction():
            await fixture.admin.execute(
                f'LOCK TABLE ONLY "{fixture.schema}".resources_acquiring IN ACCESS EXCLUSIVE MODE NOWAIT'
            )
            await fixture.admin.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["writer"]}"')
            await fixture.admin.execute("SET LOCAL lock_timeout='200ms'")
            assert (
                await fixture.admin.execute(
                    f'UPDATE "{fixture.schema}".resources_acquiring SET payload_json=\'{{"progress":true}}\''
                )
                == "UPDATE 1"
            )
        await published.verify(reader)
    observed = await _observe(fixture)
    assert observed.status == "accepted"
    assert all(family["rows"] == 1 for family in observed.descriptor["admission"]["families"].values())
    await _reader_identity(reader, fixture.admin, (observed.descriptor,))


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize(
    "damage",
    [
        "binding_column",
        "binding_alias",
        "binding_owner",
        "parent_select",
        "private_select",
        "owner_set",
        "grant_option",
        "schema_create",
        "persistent_update",
        "private_metadata",
        "createdb",
        "dangerous_role",
        "parameter_set",
    ],
)
async def test_published_scope_rejects_disclosure_or_reachable_authority(observed_source_db, damage):
    """Native grants never replace the explicit binding and reader custody contract."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    reader_role = fixture.roles_by_kind["reader"]
    binding = f'"{fixture.schema}".provider_directory_insurance_network_source_binding'
    statements_by_damage = {
        "binding_column": f'REVOKE SELECT(network_id) ON {binding} FROM "{reader_role}"',
        "binding_alias": f"INSERT INTO {binding} VALUES('unadmitted-alias','private','22222222-2222-4222-8222-222222222222',NULL)",
        "binding_owner": f'ALTER TABLE {binding} OWNER TO "{fixture.roles_by_kind["writer"]}"',
        "parent_select": f'GRANT SELECT ON "{fixture.schema}".provider_directory_dataset_resource TO "{reader_role}"',
        "private_select": f'GRANT SELECT ON "{fixture.schema}".resources_detached TO "{reader_role}"',
        "owner_set": f'GRANT "{fixture.roles_by_kind["owner"]}" TO "{reader_role}" WITH INHERIT FALSE,SET TRUE',
        "grant_option": f'GRANT SELECT ON "{fixture.schema}".resources_low TO "{reader_role}" WITH GRANT OPTION',
        "schema_create": f'GRANT CREATE ON SCHEMA "{fixture.catalog}" TO "{reader_role}"',
        "persistent_update": f'GRANT UPDATE ON "{fixture.schema}".resources_low TO "{reader_role}"',
        "private_metadata": f'GRANT SELECT(private_note) ON "{fixture.catalog}".provider_directory_source TO "{reader_role}"',
        "createdb": f'ALTER ROLE "{reader_role}" CREATEDB',
        "dangerous_role": f'GRANT pg_read_all_data TO "{reader_role}"',
        "parameter_set": f'GRANT SET ON PARAMETER session_replication_role TO "{reader_role}"',
    }
    original = await fixture.admin.fetchval(f"SELECT network_id FROM {binding} WHERE source_id='alias-a'")
    await fixture.admin.execute(statements_by_damage[damage])
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
    assert await fixture.admin.fetchval(f"SELECT network_id FROM {binding} WHERE source_id='alias-a'") == original


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("damage", ["rename", "replace_oid", "detach", "attach", "header", "alias", "missing_leaf"])
async def test_published_scope_rejects_changed_pins_and_leaf_coverage(observed_source_db, damage):
    """A stale exact scope cannot follow renamed, replaced or newly attached objects."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    namespace = '"' + fixture.schema + '"'
    statements_by_damage = {
        "rename": f"ALTER TABLE {namespace}.resources_low RENAME TO resources_renamed",
        "replace_oid": f"DROP TABLE {namespace}.resources_low;CREATE TABLE {namespace}.resources_low PARTITION OF {namespace}.resources_one FOR VALUES FROM (MINVALUE) TO ('8')",
        "detach": f"ALTER TABLE {namespace}.resources_one DETACH PARTITION {namespace}.resources_low",
        "attach": f"ALTER TABLE {namespace}.resources_one DETACH PARTITION {namespace}.resources_low;"
        f"CREATE TABLE {namespace}.resources_added PARTITION OF {namespace}.resources_one FOR VALUES FROM (MINVALUE) TO ('4');"
        f"ALTER TABLE {namespace}.resources_one ATTACH PARTITION {namespace}.resources_low FOR VALUES FROM ('4') TO ('8')",
        "header": f"UPDATE {namespace}.provider_directory_endpoint_dataset SET dataset_hash=repeat('e',64)",
        "alias": f"INSERT INTO \"{fixture.catalog}\".provider_directory_source(source_id,endpoint_id) VALUES('new-alias','endpoint-one')",
        "missing_leaf": f"DROP TABLE {namespace}.resources_low",
    }
    await fixture.admin.execute(statements_by_damage[damage])
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_leaf_locks_pin_identity_until_reader_transaction_ends(observed_source_db):
    """Hold only admitted leaf identity while the caller owns its readonly snapshot."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        published = await pin_fhir_catalog_published_read(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with fixture.admin.transaction():
                await fixture.admin.execute(
                    f'LOCK TABLE ONLY "{fixture.schema}".resources_low IN ACCESS EXCLUSIVE MODE NOWAIT'
                )
        await published.verify(reader)
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE ONLY "{fixture.schema}".resources_low IN ACCESS EXCLUSIVE MODE NOWAIT')
    with pytest.raises(ValueError):
        await published.verify(reader)


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_refuses_mixed_hash_leaves_despite_dataset_filter(observed_source_db):
    """An exact dataset predicate cannot authorize a heap shared with acquiring rows."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    namespace = '"' + fixture.schema + '"'
    await fixture.admin.execute(
        f"DROP TABLE {namespace}.provider_directory_dataset_resource CASCADE;"
        f"CREATE TABLE {namespace}.provider_directory_dataset_resource("
        f"dataset_id varchar(96),resource_type varchar(64),resource_id varchar(64),payload_hash varchar(64),payload_json jsonb) "
        f"PARTITION BY HASH(resource_type);"
        f"CREATE TABLE {namespace}.resources_low PARTITION OF {namespace}.provider_directory_dataset_resource "
        f"FOR VALUES WITH(MODULUS 2,REMAINDER 0);"
        f"CREATE TABLE {namespace}.resources_high PARTITION OF {namespace}.provider_directory_dataset_resource "
        f"FOR VALUES WITH(MODULUS 2,REMAINDER 1);"
        f"INSERT INTO {namespace}.provider_directory_dataset_resource "
        f"SELECT dataset,'Organization','network',repeat('d',64),'{{}}' FROM "
        f"unnest(ARRAY['dataset-one','dataset-acquiring','dataset-prepared']) dataset"
    )
    await _source_permissions(fixture)
    reader = await fixture.connect()
    assert (
        await fixture.admin.fetchval(
            f"SELECT count(*) FROM {namespace}.provider_directory_dataset_resource "
            f"WHERE dataset_id<>'dataset-one' AND tableoid=(SELECT tableoid FROM "
            f"{namespace}.provider_directory_dataset_resource WHERE dataset_id='dataset-one')"
        )
        == 2
    )
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_handle_cannot_survive_rollback_of_its_projection_locks(observed_source_db):
    """Rollback a savepoint while preserving the surrounding readonly transaction."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        savepoint = reader.transaction()
        await savepoint.start()
        published = await pin_fhir_catalog_published_read(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        scope = fixture.profile.published_read_scope
        projected_oids = [
            scope.header.relation.relation_oid,
            scope.source_registration.relation_oid,
            scope.endpoint_registration.relation_oid,
            scope.bindings.relation.relation_oid,
            *published.native["resource_leaf_oids"],
        ]
        await savepoint.rollback()
        assert (
            await reader.fetchval(
                "SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND relation=ANY($1::oid[]) "
                "AND mode='AccessShareLock'",
                projected_oids,
            )
            == 0
        )
        with pytest.raises(ValueError, match="native_projection_locks_changed"):
            await published.verify(reader)
        assert reader.is_in_transaction()


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_reader_rejects_executable_security_definer_escape(observed_source_db):
    """An inert fixture function still exposes a forbidden owner-execution route."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await fixture.admin.execute(
        f'CREATE FUNCTION "{fixture.schema}".reader_escape() RETURNS integer LANGUAGE sql SECURITY DEFINER '
        f'AS \'SELECT 1\';ALTER FUNCTION "{fixture.schema}".reader_escape() OWNER TO "{fixture.roles_by_kind["owner"]}"'
    )
    reader = await fixture.connect()
    assert await reader.fetchval(f'SELECT "{fixture.schema}".reader_escape()') == 1
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError, match="reader_scope_unsafe"):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_inventory_uses_column_projections_and_explicit_observation_contract(observed_source_db):
    """Run native inventory without whole-header grants or semantic-admission fallback."""
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceError, iter_fhir_catalog_sources

    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await _assert_metadata_columns_only(reader, fixture)
        dispositions = [
            item
            async for page in iter_fhir_catalog_sources(
                reader,
                fixture.profile,
                page_size=1,
                observation_contract=CATALOG_OBSERVATION_CONTRACT,
            )
            for item in page
        ]
        assert len(dispositions) == 1 and dispositions[0].status == "accepted"
        assert dispositions[0].descriptor["admission"]["kind"] == "catalog_observation"
        with pytest.raises(FHIRCatalogSourceError, match="scoped_semantic_compatibility_required"):
            async for _ in iter_fhir_catalog_sources(reader, fixture.profile):
                pytest.fail("observation facts became semantic admission")


async def _add_second_scope_entry(fixture, *, published):
    """Declare a second synthetic physical unit without deriving its publication authority."""
    from dataclasses import replace

    from process.registry_fhir_catalog_published_read import parse_fhir_catalog_published_read_scope

    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".provider_directory_endpoint_dataset '
        f"(dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,published_at,resource_count,"
        f"previous_dataset_id,publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,"
        f"content_proof_admission_kind,content_proof_admission_sha256) "
        f"SELECT 'dataset-acquiring','endpoint-two','other-root',repeat('e',64),$1,$2,published_at,1,NULL,"
        f"publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,"
        f"content_proof_admission_kind,content_proof_admission_sha256 "
        f"FROM \"{fixture.schema}\".provider_directory_endpoint_dataset WHERE dataset_id='dataset-one'",
        "published" if published else "acquiring",
        published,
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.catalog}".provider_directory_api_endpoint(endpoint_id,canonical_api_base) '
        f"VALUES('endpoint-two','https://example.test/other');"
        f"INSERT INTO \"{fixture.catalog}\".provider_directory_source(source_id,endpoint_id) VALUES('alias-c','endpoint-two');"
        f'GRANT SELECT ON "{fixture.schema}".resources_acquiring TO "{fixture.roles_by_kind["reader"]}"'
    )
    scope_by_field = fixture.profile.published_read_scope.document()
    scope_by_field["bindings"]["source_aliases"].append("alias-c")
    scope_by_field["datasets"].append(
        {
            "endpoint_id": "endpoint-two",
            "dataset_id": "dataset-acquiring",
            "dataset_sha256": "e" * 64,
            "source_aliases": ["alias-c"],
            "resource_leaves": [
                {
                    "schema_name": fixture.schema,
                    "relation_name": "resources_acquiring",
                    "relation_oid": await fixture.admin.fetchval(
                        "SELECT to_regclass($1)::oid::bigint", f'"{fixture.schema}".resources_acquiring'
                    ),
                }
            ],
            "publication_sha256": hashlib.sha256(
                fhir_catalog_json_bytes(await _published_publication(fixture, "dataset-acquiring"))
            ).hexdigest(),
        }
    )
    fixture.profile = replace(
        fixture.profile, published_read_scope=parse_fhir_catalog_published_read_scope(scope_by_field)
    )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_rejects_an_acquiring_second_declared_dataset(observed_source_db):
    """Every readable unit must be published even when the requested unit is valid."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await _add_second_scope_entry(fixture, published=False)
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_handle_rejects_another_admitted_dataset_descriptor(observed_source_db):
    """A shared profile fingerprint cannot transfer a handle to a second dataset."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await _add_second_scope_entry(fixture, published=True)
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        first = await pin_fhir_catalog_published_read(
            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
        )
        second = await collect_fhir_catalog_observation(
            reader, fixture.profile, endpoint_id="endpoint-two", dataset_id="dataset-acquiring"
        )
        assert second.status == "accepted"
        assert first.native["native_catalog_sha256"] == second.descriptor["native_catalog_sha256"]
        with pytest.raises(ValueError):
            await first.verify(reader, descriptor=second.descriptor)


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("constraint_case", ["validated", "missing", "permissive", "not_valid"])
async def test_published_binding_resource_type_requires_validated_organization_only_check(
    observed_source_db, constraint_case
):
    """Prove typed binding semantics through native checks without another column grant."""
    fixture = observed_source_db
    binding = f'"{fixture.schema}".provider_directory_insurance_network_source_binding'
    await fixture.admin.execute(
        f"ALTER TABLE {binding} ADD COLUMN resource_type varchar(16) NOT NULL DEFAULT 'Organization'"
    )
    constraints_by_case = {
        "validated": "CHECK(resource_type='Organization')",
        "missing": None,
        "permissive": "CHECK(resource_type IN ('Organization','InsurancePlan'))",
        "not_valid": "CHECK(resource_type='Organization') NOT VALID",
    }
    constraint = constraints_by_case[constraint_case]
    if constraint is not None:
        await fixture.admin.execute(f"ALTER TABLE {binding} ADD CONSTRAINT binding_resource_kind " + constraint)
    await refresh_published_profile(fixture)
    reader = await fixture.connect()
    assert not await reader.fetchval("SELECT has_column_privilege(current_user,$1,'resource_type','SELECT')", binding)
    if constraint_case == "validated":
        observed = await _observe(fixture)
        assert observed.status == "accepted"
    else:
        with pytest.raises(ValueError, match="binding_shape_invalid"):
            await _observe(fixture)


@contextmanager
def published_metadata_counts(connection):
    """Count fixed metadata query classes without retaining arguments or connection fields."""
    counts = Counter()

    def observe(record):
        """Ignore native guards and count only complete publication/registration reads."""
        query = record.query
        if "SELECT NOT EXISTS(SELECT FROM ONLY " in query and " WHERE source_id IS NULL" in query:
            counts["bindings"] += 1
        elif "left(dataset_hash,65) AS dataset_hash" in query and " AS current_count " in query:
            counts["headers"] += 1
        elif "left(canonical_api_base,2049) AS source_uri FROM ONLY " in query:
            counts["endpoints"] += 1
        elif "left(source_id,65) AS source_id FROM ONLY " in query:
            counts["sources"] += 1

    with connection.query_logger(observe):
        yield counts


async def _read_published_scope(fixture, reader, mode):
    """Exercise genuine direct pins or the existing native inventory consumer."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read
    from process.registry_fhir_catalog_sources import iter_fhir_catalog_sources

    if mode == "inventory":
        dispositions = [
            item
            async for page in iter_fhir_catalog_sources(
                reader,
                fixture.profile,
                page_size=1,
                observation_contract=CATALOG_OBSERVATION_CONTRACT,
            )
            for item in page
        ]
        assert len(dispositions) == 2 and all(item.status == "accepted" for item in dispositions)
    else:
        for entry in fixture.profile.published_read_scope.datasets:
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id=entry.endpoint_id, dataset_id=entry.dataset_id
            )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("mode", ["pins", "inventory", "uncached"])
async def test_published_scope_reuses_one_linear_metadata_proof(observed_source_db, mode):
    """Reuse a two-entry scope across repeated pins and nested native inventory."""
    from process.registry_fhir_catalog_published_read import fhir_catalog_published_reads

    fixture = observed_source_db
    await _add_second_scope_entry(fixture, published=True)
    reader = await fixture.connect()
    with published_metadata_counts(reader) as counts:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with fhir_catalog_published_reads(reader) if mode != "uncached" else nullcontext():
                for _ in range(2):
                    await _read_published_scope(fixture, reader, mode)
        await asyncio.sleep(0)
    proofs = 4 if mode == "uncached" else 1
    assert counts == {"headers": 2 * proofs, "sources": 2 * proofs, "endpoints": 2 * proofs, "bindings": proofs}


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cache_cannot_cross_native_transactions(observed_source_db):
    """An outer context must perform a fresh proof after a real RR transaction changes."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    with published_metadata_counts(reader) as counts, ExitStack() as contexts:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            contexts.enter_context(fhir_catalog_published_reads(reader))
            previous = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            with pytest.raises(ValueError):
                await previous.verify(reader)
        await asyncio.sleep(0)
    assert counts == {"headers": 2, "sources": 2, "endpoints": 2, "bindings": 2}


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cache_cannot_cross_connections(observed_source_db):
    """A second genuine login cannot borrow the first connection's proof."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture = observed_source_db
    first_reader, second_reader = await fixture.connect(), await fixture.connect()
    with (
        published_metadata_counts(first_reader) as first_counts,
        published_metadata_counts(second_reader) as second_counts,
    ):
        async with (
            first_reader.transaction(isolation="repeatable_read", readonly=True),
            second_reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            with fhir_catalog_published_reads(first_reader):
                first = await pin_fhir_catalog_published_read(
                    first_reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
                with pytest.raises(ValueError):
                    await first.verify(second_reader)
                await pin_fhir_catalog_published_read(
                    second_reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
        await asyncio.sleep(0)
    assert first_counts == second_counts == {"headers": 1, "sources": 1, "endpoints": 1, "bindings": 1}


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("explicit_context", [False, True])
async def test_published_scope_cache_cannot_cross_tasks(observed_source_db, explicit_context):
    """A child task revalidates even when it inherits its parent's context variable."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    with published_metadata_counts(reader) as counts:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with fhir_catalog_published_reads(reader):
                first = await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )

                async def child_pin():
                    """Use the same idle connection from a distinct native read task."""
                    with pytest.raises(ValueError):
                        await first.verify(reader)
                    with fhir_catalog_published_reads(reader) if explicit_context else nullcontext():
                        await pin_fhir_catalog_published_read(
                            reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                        )

                await asyncio.create_task(child_pin())
        await asyncio.sleep(0)
    assert counts == {"headers": 2, "sources": 2, "endpoints": 2, "bindings": 2}


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cached_pin_refuses_rolled_back_projection_locks(observed_source_db):
    """A cached new pin cannot silently reacquire authority lost with its savepoint."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    with published_metadata_counts(reader) as counts:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with fhir_catalog_published_reads(reader):
                savepoint = reader.transaction()
                await savepoint.start()
                await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
                await savepoint.rollback()
                with pytest.raises(ValueError, match="native_projection_locks_changed"):
                    await pin_fhir_catalog_published_read(
                        reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                    )
        await asyncio.sleep(0)
    assert counts == {"headers": 1, "sources": 1, "endpoints": 1, "bindings": 1}


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cached_history_cannot_authorize_current_head(observed_source_db):
    """A false current requirement never widens a cached superseded header."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    await fixture.admin.execute(
        f"UPDATE \"{fixture.schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
    )
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            historical = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one", require_current=False
            )
            assert historical.header["status"] == "superseded"
            with pytest.raises(ValueError, match="publication_invalid"):
                await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one", require_current=True
                )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cached_pin_rechecks_effective_acl(observed_source_db):
    """New private payload authority invalidates a cache hit in the existing snapshot."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            await fixture.admin.execute(
                f'GRANT SELECT ON "{fixture.schema}".resources_detached TO "{fixture.roles_by_kind["reader"]}"'
            )
            with pytest.raises(ValueError, match="reader_scope_unsafe"):
                await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_cached_acl_guard_stays_fresh_across_repeated_grants(observed_source_db):
    """Prime the production guard, then reject three new ACL grants in the same RR."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    private_relation = f'"{fixture.schema}".resources_detached'
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            for _ in range(3):
                await fixture.admin.execute(
                    f'GRANT SELECT ON {private_relation} TO "{fixture.roles_by_kind["reader"]}"'
                )
                with pytest.raises(ValueError, match="reader_scope_unsafe"):
                    await pin_fhir_catalog_published_read(
                        reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                    )
                await fixture.admin.execute(
                    f'REVOKE SELECT ON {private_relation} FROM "{fixture.roles_by_kind["reader"]}"'
                )
                restored = await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
                assert restored.header["status"] == "published"


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_bad_facts_do_not_poison_the_next_inventory_endpoint(observed_source_db):
    """A rolled-back first collector cannot leave a stale proof for a valid sibling."""
    from process.registry_fhir_catalog_published_read import fhir_catalog_published_reads
    from process.registry_fhir_catalog_sources import iter_fhir_catalog_sources

    fixture = observed_source_db
    await _add_second_scope_entry(fixture, published=True)
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".resources_one SET payload_json=\'{{"name":42}}\' '
        f"WHERE resource_type='Organization' AND resource_id='network'"
    )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            dispositions = [
                item
                async for page in iter_fhir_catalog_sources(
                    reader,
                    fixture.profile,
                    page_size=1,
                    observation_contract=CATALOG_OBSERVATION_CONTRACT,
                )
                for item in page
            ]
    assert [(item.endpoint_id, item.status) for item in dispositions] == [
        ("endpoint-one", "unavailable"),
        ("endpoint-two", "accepted"),
    ]
    assert dispositions[0].descriptor is None and dispositions[1].descriptor is not None


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("revocation", ["leaf_select", "header_column", "catalog_usage"])
async def test_published_scope_cached_pin_requires_every_projection_privilege(observed_source_db, revocation):
    """A warm proof cannot replace current positive native read privileges."""
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )

    fixture, reader = observed_source_db, await observed_source_db.connect()
    reader_role = fixture.roles_by_kind["reader"]
    statements_by_revocation = {
        "leaf_select": f'REVOKE SELECT ON "{fixture.schema}".resources_low FROM "{reader_role}"',
        "header_column": f'REVOKE SELECT(publication_metadata_json) ON "{fixture.schema}".provider_directory_endpoint_dataset FROM "{reader_role}"',
        "catalog_usage": f'REVOKE USAGE ON SCHEMA "{fixture.catalog}" FROM "{reader_role}"',
    }
    scope = fixture.profile.published_read_scope
    payload_relations = [
        scope.resource_root.sql,
        *(pin.sql for entry in scope.datasets for pin in entry.resource_leaves),
    ]
    payload_queries = []

    def observe(record):
        """Retain only whether the rejected hit attempted a resource SQL path."""
        if any(relation in record.query for relation in payload_relations):
            payload_queries.append(True)

    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            await fixture.admin.execute(statements_by_revocation[revocation])
            with reader.query_logger(observe), published_metadata_counts(reader) as counts:
                with pytest.raises(ValueError):
                    await pin_fhir_catalog_published_read(
                        reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                    )
                await asyncio.sleep(0)
    assert not payload_queries and not counts


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("namespace", ["source", "catalog", "both"])
async def test_published_scope_cache_accepts_quoted_catalog_namespace(observed_source_db, namespace):
    """Preserve exact OIDs and ACLs when an admitted catalog uses a mixed-case name."""
    from process.registry_fhir_catalog_grant import _reader_identity
    from process.registry_fhir_catalog_published_read import (
        fhir_catalog_published_reads,
        pin_fhir_catalog_published_read,
    )
    from process.registry_fhir_catalog_sources import iter_fhir_catalog_sources

    fixture = observed_source_db
    original_scope = fixture.profile.published_read_scope
    original_acl_by_attribute = {}
    for attribute in ("schema", "catalog"):
        if namespace != "both" and attribute != ("schema" if namespace == "source" else "catalog"):
            continue
        original_namespace = getattr(fixture, attribute)
        original_acl_by_attribute[attribute] = await fixture.admin.fetchval(
            "SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", original_namespace
        )
        mixed_case = "ScopedSource_" + uuid4().hex if attribute == "schema" else "ScopedCatalog_" + uuid4().hex
        await fixture.admin.execute(f'ALTER SCHEMA "{original_namespace}" RENAME TO "{mixed_case}"')
        setattr(fixture, attribute, mixed_case)
    await refresh_published_profile(fixture)
    scope = fixture.profile.published_read_scope
    assert scope.source_registration.relation_oid == original_scope.source_registration.relation_oid
    assert scope.endpoint_registration.relation_oid == original_scope.endpoint_registration.relation_oid
    for attribute, original_acl in original_acl_by_attribute.items():
        assert (
            await fixture.admin.fetchval(
                "SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", getattr(fixture, attribute)
            )
            == original_acl
        )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with fhir_catalog_published_reads(reader):
            first = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            repeated = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            assert repeated.native == first.native
            dispositions = [
                disposition
                async for page in iter_fhir_catalog_sources(
                    reader, fixture.profile, observation_contract=CATALOG_OBSERVATION_CONTRACT
                )
                for disposition in page
            ]
            assert len(dispositions) == 1 and dispositions[0].status == "accepted"
            descriptor = dispositions[0].descriptor
            assert descriptor["native_read_profile"] == fixture.profile.document()
            await _reader_identity(reader, fixture.admin, (descriptor,))
