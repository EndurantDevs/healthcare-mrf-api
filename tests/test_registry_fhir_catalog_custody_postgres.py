# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated metadata sealing and native runtime privilege denial."""

import asyncio
import copy
import hashlib
import json
import os
import re
from contextlib import suppress
from dataclasses import asdict
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.registry_approval import RegistryApprovedRecord
from db.registry_fhir_catalog import PROJECTOR_VERSION, table_sql
from process import registry_fhir_catalog_validation as validation
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_writer_closure import writer_role_names
from process.registry_fhir_catalog_custody import (
    PHYSICAL_TABLES,
    FHIRCatalogTarget,
    freeze_fhir_catalog_snapshot,
    verify_fhir_catalog_snapshot,
)
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichmentContext, enrich_fhir_catalog_edges
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_projection import (
    BINDING_COLUMNS,
    NETWORK_COLUMNS,
    RELATIONSHIP_COLUMNS,
    SourceCatalogContext,
    format_fhir_catalog_projection,
)
from process.registry_fhir_catalog_validation import canonical_json, metadata_generation
from tests.test_registry_fhir_catalog_read import COMPANY, relationship_document


async def _connect_metadata_actors(fixture, role_password):
    """Keep every role-specific connection under the fixture's registered cleanup."""
    dsn = fixture.dsn.replace("postgresql+asyncpg://", "postgresql://")
    fixture.sealer_pool = await asyncpg.create_pool(
        dsn, user=fixture.roles["publisher"], password=role_password, min_size=0, max_size=2
    )
    for kind in ("loader", "reader", "publisher"):
        observer = await asyncpg.connect(dsn, user=fixture.roles[kind], password=role_password)
        fixture.observers.append(observer)
        setattr(fixture, kind, observer)


def _approved_record_table(schema):
    return RegistryApprovedRecord.__table__.to_metadata(MetaData(), schema=schema)


def test_custody_approval_columns_match_sealer_predicates():
    table = _approved_record_table("catalog_control_fixture")
    referenced_columns = set(re.findall(r"\bproduct\.([a-z_]+)\b", validation._SET_SQL))
    assert "record_revision" in referenced_columns
    assert referenced_columns <= set(table.columns.keys())
    assert not table.c.record_revision.nullable and not table.c.custom_revision.nullable
    assert "registry_approved_revision" in {constraint.name for constraint in table.constraints}


async def _create_metadata_roles(fixture, created_roles):
    """Provision exact fixture roles inside the caller's already registered cleanup."""
    connection, role_by_kind, role_password = fixture.connection, fixture.roles, fixture.role_password
    password_literal = await connection.fetchval("SELECT quote_literal($1::text)", role_password)
    for kind, name in role_by_kind.items():
        credential = "NOLOGIN" if kind == "owner" else "LOGIN PASSWORD " + password_literal
        await connection.execute(f'CREATE ROLE "{name}" ' + credential)
        created_roles.append(name)
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(
        f'GRANT CREATE ON DATABASE {database} TO "{role_by_kind["owner"]}","{role_by_kind["publisher"]}"'
    )
    await connection.execute(
        f'GRANT "{role_by_kind["loader"]}","{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}"'
    )


@pytest.fixture
async def metadata_custody():
    """Keep native metadata roles, control schema and connections under exact cleanup."""
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    role_by_kind = {kind: "fc_" + kind + "_" + suffix for kind in ("owner", "loader", "reader", "publisher")}
    role_password = uuid4().hex
    created_roles, schemas, observers = [], [], []
    fixture = SimpleNamespace(
        connection=connection,
        roles=role_by_kind,
        role_password=role_password,
        schemas=schemas,
        observers=observers,
        dsn=dsn,
    )
    try:
        await _create_metadata_roles(fixture, created_roles)
        await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        fixture.control_schema = "fhir_custody_control_" + uuid4().hex
        schemas.append(fixture.control_schema)
        await connection.execute(f'CREATE SCHEMA "{fixture.control_schema}"')
        await connection.execute(
            f'CREATE TABLE "{fixture.control_schema}".network_registry_identity(network_id integer PRIMARY KEY)'
        )
        approved_table = _approved_record_table(fixture.control_schema)
        await connection.execute(str(CreateTable(approved_table).compile(dialect=postgresql.dialect())))
        await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{role_by_kind["publisher"]}"')
        await connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{role_by_kind["publisher"]}"'
        )
        fixture.target = await create_metadata_target(fixture)
        await _connect_metadata_actors(fixture, role_password)
        yield fixture
    finally:
        if hasattr(fixture, "sealer_pool"):
            await fixture.sealer_pool.close()
        for observer in observers:
            await observer.close()
        for schema in reversed(schemas):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        for name in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{name}"')
            await connection.execute(f'DROP ROLE "{name}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )
        await connection.close()


async def create_metadata_target(fixture):
    candidate_id = uuid4()
    target = FHIRCatalogTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "fhir_catalog_" + candidate_id.hex
    )
    fixture.schemas.append(target.schema_name)
    namespace = '"' + target.schema_name + '"'
    await fixture.connection.execute(f'CREATE SCHEMA {namespace} AUTHORIZATION "{fixture.roles["loader"]}"')
    for table in sorted(PHYSICAL_TABLES):
        for statement in table_sql(namespace, table):
            await fixture.connection.execute(statement)
        await fixture.connection.execute(f'ALTER TABLE {namespace}.{table} OWNER TO "{fixture.roles["loader"]}"')
    return target


def expected_snapshot(fixture, target=None):
    snapshot_by_field = {
        "schema_name": (target or fixture.target).schema_name,
        "network_generation": 9,
        "manifest_sha256": "f" * 64,
        "metadata_approved_revision": 6,
        "membership_mapping_sha256": hashlib.sha256(b"").hexdigest(),
        "mapping_sha256": hashlib.sha256(b"").hexdigest(),
        "standing_grant_sha256": "d" * 64,
        "source_vector_json": [],
        "projector_version": PROJECTOR_VERSION,
        "as_of": date(2026, 1, 1),
    }
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    return snapshot_by_field


async def freeze_metadata(fixture, target=None, snapshot=None):
    target = target or fixture.target
    snapshot = snapshot or expected_snapshot(fixture, target)
    async with fixture.publisher.transaction():
        receipt = await freeze_fhir_catalog_snapshot(
            fixture.publisher,
            target,
            writer_roles=writer_role_names(
                fixture.roles["owner"], (fixture.roles["loader"],), (fixture.roles["reader"],)
            ),
            expected_snapshot=snapshot,
            control_schema=fixture.control_schema,
        )
    return {**snapshot, "custody_json": receipt, "accounting_json": receipt["validation"]["tables"]}


@pytest.mark.asyncio
async def test_runtime_writes_are_closed(metadata_custody):
    fixture = metadata_custody
    snapshot_by_field = await freeze_metadata(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    assert set(snapshot_by_field["custody_json"]["relation_oids"]) == PHYSICAL_TABLES
    async with fixture.reader.transaction(readonly=True):
        await verify_fhir_catalog_snapshot(fixture.reader, snapshot_by_field)
        assert await fixture.reader.fetchval(f"SELECT count(*) FROM {namespace}.registry_fhir_catalog_network") == 0
    statements = (
        f"INSERT INTO {namespace}.registry_fhir_catalog_network SELECT * FROM {namespace}.registry_fhir_catalog_network",
        f"UPDATE {namespace}.registry_fhir_catalog_network SET archived=true",
        f"DELETE FROM {namespace}.registry_fhir_catalog_network",
        f"TRUNCATE {namespace}.registry_fhir_catalog_network",
        f"CREATE TABLE {namespace}.untracked(id integer)",
        f"ALTER TABLE {namespace}.registry_fhir_catalog_network ADD COLUMN untracked integer",
        f'SET ROLE "{fixture.roles["owner"]}"',
    )
    for observer in (fixture.loader, fixture.reader):
        for statement in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await observer.execute(statement)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await observer.copy_records_to_table(
                "registry_fhir_catalog_network",
                schema_name=fixture.target.schema_name,
                records=[("a" * 64, 7, "Example Network", "[]", False, 1, None, "example network")],
            )


@pytest.mark.asyncio
async def test_native_identity_change_refuses(metadata_custody):
    fixture = metadata_custody
    snapshot_by_field = await freeze_metadata(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    await fixture.connection.execute(f"DROP TABLE {namespace}.registry_fhir_catalog_network")
    for statement in table_sql(namespace, "registry_fhir_catalog_network"):
        await fixture.connection.execute(statement)
    await fixture.connection.execute(
        f'ALTER TABLE {namespace}.registry_fhir_catalog_network OWNER TO "{fixture.roles["owner"]}"'
    )
    async with fixture.connection.transaction(readonly=True):
        with pytest.raises(ValueError, match="object identity changed"):
            await verify_fhir_catalog_snapshot(fixture.connection, snapshot_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["grant", "set_path"])
async def test_native_authority_drift_refuses(metadata_custody, drift):
    fixture = metadata_custody
    snapshot_by_field = await freeze_metadata(fixture)
    if drift == "grant":
        await fixture.connection.execute(
            f'GRANT UPDATE ON "{fixture.target.schema_name}".registry_fhir_catalog_network TO "{fixture.roles["reader"]}"'
        )
    else:
        await fixture.connection.execute(f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["reader"]}"')
    async with fixture.connection.transaction(readonly=True):
        with pytest.raises(ValueError, match="not closed"):
            await verify_fhir_catalog_snapshot(fixture.connection, snapshot_by_field)


def source_descriptor():
    return {
        "coordinates": {
            "source_system": "fhir",
            "source_id": "sample-fhir",
            "dataset_schema": "sample_source",
            "dataset_id": "dataset-one",
            "producer_id": "sample-producer",
            "edition_id": "edition-one",
        },
        "profile": "published_normalized",
        "endpoint_id": "sample-endpoint",
        "dataset_sha256": "e" * 64,
        "resource_table_oid": 123,
        "source_uri": "https://example.test/fhir/",
        "accepted_receipt_id": "receipt-one",
        "accepted_receipt_sha256": "f" * 64,
        "complete": True,
        "previous_dataset_id": None,
    }


async def seed_observations(fixture, count, *, expected_count=None):
    descriptor = source_descriptor()
    source_generation = hashlib.sha256(canonical_json(descriptor).encode()).hexdigest()
    snapshot_by_field = expected_snapshot(fixture)
    snapshot_by_field["source_vector_json"] = [
        {"source_generation": source_generation, "source_id": "sample-fhir", "status": "current"}
    ]
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    namespace = '"' + fixture.target.schema_name + '"'
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_source "
        "(source_generation,source_id,descriptor_json,profile,source_rows,observation_rows,accepted_at) "
        "VALUES($1,'sample-fhir',$2::jsonb,'published_normalized',$3,$4,$5)",
        source_generation,
        canonical_json(descriptor),
        count,
        count if expected_count is None else expected_count,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    observations = []
    for ordinal in range(count):
        resource_id = f"organization-{ordinal:04d}"
        document_by_field = {
            "resource_id": resource_id,
            "name": resource_id,
            "aliases": [],
            "declared_network": False,
            "status": "active",
            "identifiers": [],
            "source_hash": hashlib.sha256(resource_id.encode()).hexdigest(),
        }
        observations.append(
            (source_generation, "Organization", resource_id, "organization", 0, canonical_json(document_by_field))
        )
    await fixture.connection.copy_records_to_table(
        "registry_fhir_catalog_observation",
        schema_name=fixture.target.schema_name,
        columns=("source_generation", "resource_type", "resource_id", "observation_kind", "ordinal", "document_json"),
        records=observations,
    )
    return snapshot_by_field


class BatchCounter:
    def __init__(self, connection):
        self.connection = connection
        self.pages = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetch(self, query, *parameters):
        if query.startswith("WITH page AS MATERIALIZED"):
            self.pages += 1
        return await self.connection.fetch(query, *parameters)


@pytest.mark.asyncio
async def test_validation_batches_and_stable_digest(metadata_custody, monkeypatch):
    fixture = metadata_custody
    snapshot_by_field = await seed_observations(fixture, 1000)
    await fixture.connection.execute(f'ANALYZE "{fixture.target.schema_name}".registry_fhir_catalog_observation')
    source_generation = snapshot_by_field["source_vector_json"][0]["source_generation"]
    sql, parameters = validation._page_query(
        '"' + fixture.target.schema_name + '"',
        "registry_fhir_catalog_observation",
        (source_generation, "Organization", "organization-0000", "organization", 0),
    )
    explained = json.loads(await fixture.connection.fetchval("EXPLAIN (ANALYZE,FORMAT JSON) " + sql, *parameters))
    assert "registry_fhir_catalog_observation_pkey" in json.dumps(explained)
    counted = BatchCounter(fixture.publisher)
    async with fixture.publisher.transaction():
        receipt = await freeze_fhir_catalog_snapshot(
            counted,
            fixture.target,
            writer_roles=writer_role_names(
                fixture.roles["owner"], (fixture.roles["loader"],), (fixture.roles["reader"],)
            ),
            expected_snapshot=snapshot_by_field,
            control_schema=fixture.control_schema,
        )
    tables = receipt["validation"]["tables"]
    assert tables["registry_fhir_catalog_observation"]["rows"] == 1000
    assert tables["registry_fhir_catalog_observation"]["batches"] == 1
    assert tables["registry_fhir_catalog_observation"]["bytes"] > 0
    assert counted.pages == 8
    repeated = await freeze_metadata(fixture, snapshot=snapshot_by_field)
    assert repeated["custody_json"]["validation"]["content_sha256"] == receipt["validation"]["content_sha256"]
    monkeypatch.setattr(
        validation, "_table_digest", AsyncMock(side_effect=AssertionError("retained read rescanned metadata"))
    )
    async with fixture.reader.transaction(readonly=True):
        await verify_fhir_catalog_snapshot(fixture.reader, repeated)


@pytest.mark.asyncio
async def test_source_count_and_input_tamper_refuse(metadata_custody):
    fixture = metadata_custody
    snapshot_by_field = await seed_observations(fixture, 1, expected_count=2)
    with pytest.raises(ValueError, match="set_validation_failed"):
        await freeze_metadata(fixture, snapshot=snapshot_by_field)
    await fixture.connection.execute(
        f'UPDATE "{fixture.target.schema_name}".registry_fhir_catalog_source SET observation_rows=1'
    )
    sealed = await freeze_metadata(fixture, snapshot=snapshot_by_field)
    tampered = copy.deepcopy(sealed)
    tampered["as_of"] = date(2026, 1, 2)
    tampered["metadata_generation"] = metadata_generation(tampered)
    async with fixture.reader.transaction(readonly=True):
        with pytest.raises(ValueError, match="inputs_changed"):
            await verify_fhir_catalog_snapshot(fixture.reader, tampered)


@pytest.mark.asyncio
async def test_committed_writer_is_validated_after_fence(metadata_custody):
    fixture = metadata_custody
    snapshot_by_field = expected_snapshot(fixture)
    pid = await fixture.publisher.fetchval("SELECT pg_backend_pid()")
    task = None
    try:
        async with fixture.loader.transaction():
            await fixture.loader.execute(
                f'INSERT INTO "{fixture.target.schema_name}".registry_fhir_catalog_network '
                "(metadata_generation,network_id,display_name,aliases,archived,revision,benefit_codes,search_name) "
                "VALUES($1,999,'Raced Network','[]'::jsonb,false,1,NULL,'raced network')",
                snapshot_by_field["metadata_generation"],
            )
            task = asyncio.create_task(freeze_metadata(fixture, snapshot=snapshot_by_field))
            for _ in range(100):
                if await fixture.connection.fetchval(
                    "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=$1", pid
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("sealer did not wait on the active metadata writer")
        with pytest.raises(ValueError, match="set_validation_failed"):
            await task
        assert (
            await fixture.connection.fetchval(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1", fixture.target.schema_name
            )
            == fixture.roles["loader"]
        )
    finally:
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_stale_transaction_cannot_seal(metadata_custody):
    fixture = metadata_custody
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        with pytest.raises(ValueError, match="fresh_snapshot_required"):
            await freeze_metadata(fixture)


def _projection_resources(organization_id):
    return [
        {
            "resource_type": "Organization",
            "resource_id": organization_id,
            "payload_hash": hashlib.sha256(organization_id.encode()).hexdigest(),
            "payload_json": {"resource_id": organization_id, "name": organization_id, "active": True},
        },
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-one",
            "payload_hash": "1" * 64,
            "payload_json": {
                "resource_id": "plan-one",
                "name": "Gold Plan",
                "network_refs": ["Organization/" + organization_id],
            },
        },
    ]


async def seed_source_projection(fixture):
    snapshot_by_field = await seed_observations(fixture, 1)
    organization_id = "organization-0000"
    extracted = extract_fhir_catalog_batch(_projection_resources(organization_id), profile="retained_normalized")
    context = FHIRCatalogEnrichmentContext(
        "sample-fhir", date(2026, 1, 1), (), (), "sample-endpoint", source_uri="https://example.test/fhir/"
    )
    relationships = enrich_fhir_catalog_edges(extracted.organizations, extracted.edges, context)
    source_generation = snapshot_by_field["source_vector_json"][0]["source_generation"]
    await fixture.connection.execute(
        f'UPDATE "{fixture.target.schema_name}".registry_fhir_catalog_source SET source_rows=2,observation_rows=2'
    )
    await fixture.connection.copy_records_to_table(
        "registry_fhir_catalog_observation",
        schema_name=fixture.target.schema_name,
        columns=("source_generation", "resource_type", "resource_id", "observation_kind", "ordinal", "document_json"),
        records=[
            (source_generation, "InsurancePlan", "plan-one", "edge", 0, canonical_json(asdict(extracted.edges[0])))
        ],
    )
    await fixture.connection.execute(f'INSERT INTO "{fixture.control_schema}".network_registry_identity VALUES(7)')
    projection_context = SourceCatalogContext(
        RegistryNetworkSourceCoordinates(**source_descriptor()["coordinates"]),
        source_generation,
        snapshot_by_field["metadata_generation"],
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        "raw_fhir",
        legacy_aliases=((organization_id, str(uuid4()), "synthetic-scope"),),
    )
    projection = format_fhir_catalog_projection(
        extracted.organizations, relationships, ((organization_id, 7),), projection_context
    )
    for table, columns, batch_rows in (
        ("registry_fhir_catalog_network", NETWORK_COLUMNS, projection.network_rows),
        ("registry_fhir_catalog_binding", BINDING_COLUMNS, projection.binding_rows),
        ("registry_fhir_catalog_relationship", RELATIONSHIP_COLUMNS, projection.relationship_rows),
    ):
        await fixture.connection.copy_records_to_table(
            table, schema_name=fixture.target.schema_name, columns=columns, records=batch_rows
        )
    return snapshot_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("injected_plan", [False, True])
async def test_source_plan_provenance_is_required(metadata_custody, injected_plan):
    fixture = metadata_custody
    snapshot_by_field = await seed_source_projection(fixture)
    if injected_plan:
        await fixture.loader.execute(
            f'UPDATE "{fixture.target.schema_name}".registry_fhir_catalog_relationship '
            "SET document_json=jsonb_set(document_json,'{plan,display_name}','\"Injected Plan\"'::jsonb)"
        )
        with pytest.raises(ValueError, match="set_validation_failed"):
            await freeze_metadata(fixture, snapshot=snapshot_by_field)
    else:
        sealed = await freeze_metadata(fixture, snapshot=snapshot_by_field)
        assert sealed["accounting_json"]["registry_fhir_catalog_relationship"]["rows"] == 1


@pytest.mark.asyncio
async def test_manual_uuid_relationship_is_validated(metadata_custody):
    fixture = metadata_custody
    await fixture.connection.execute(f'INSERT INTO "{fixture.control_schema}".network_registry_identity VALUES(7)')
    approved_rows = [
        (6, "company", COMPANY, 1, 1, canonical_json({"display_name": "Example Insurance", "archived": False})),
        (6, "network", "7", 1, 1, canonical_json({"display_name": "Example Network", "archived": False})),
        (6, "company_links", COMPANY, 1, 1, canonical_json({"network_ids": [7], "archived": False})),
    ]
    await fixture.connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=fixture.control_schema,
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
        records=approved_rows,
    )
    snapshot_by_field = expected_snapshot(fixture)
    async with fixture.connection.transaction():
        fingerprints = await approved_fhir_catalog_fingerprints(
            fixture.connection, (6,), control_schema=fixture.control_schema
        )
    snapshot_by_field.update(
        mapping_sha256=fingerprints[6]["metadata"], membership_mapping_sha256=fingerprints[6]["membership"]
    )
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    document = relationship_document()
    document.update(source_id="registry-manual", plan=None, organization_reference=None, group=None)
    await fixture.connection.copy_records_to_table(
        "registry_fhir_catalog_network",
        schema_name=fixture.target.schema_name,
        records=[
            (snapshot_by_field["metadata_generation"], 7, "Example Network", "[]", False, 1, None, "example network")
        ],
    )
    await fixture.connection.copy_records_to_table(
        "registry_fhir_catalog_relationship",
        schema_name=fixture.target.schema_name,
        columns=RELATIONSHIP_COLUMNS,
        records=[
            (
                snapshot_by_field["metadata_generation"],
                document["relationship_id"],
                7,
                "registry-manual",
                snapshot_by_field["mapping_sha256"],
                None,
                UUID(COMPANY),
                None,
                "example insurance",
                "",
                canonical_json(document),
            )
        ],
    )
    sealed = await freeze_metadata(fixture, snapshot=snapshot_by_field)
    assert sealed["accounting_json"]["registry_fhir_catalog_relationship"]["rows"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("table", sorted(validation._KEYS))
async def test_digest_accepts_each_expected_native_key_before_paging(table):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=canonical_json(validation._KEYS[table])),
        fetch=AsyncMock(return_value=[]),
    )
    report = await validation._table_digest(connection, '"catalog_fixture"', table)
    assert report == {"rows": 0, "sha256": hashlib.sha256(b"").hexdigest(), "batches": 0, "bytes": 0}
    connection.fetchval.assert_awaited_once()
    assert connection.fetchval.await_args.args[1] == '"catalog_fixture".' + table
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native_keys",
    [
        None,
        [],
        [["metadata_generation", "text"]],
        [["metadata_generation", "text"], ["display_name", "text"]],
        [["network_id", "integer"], ["metadata_generation", "text"]],
        [["metadata_generation", "text"], ["network_id", "text"]],
        [["metadata_generation", "text"], ["network_id", "integer"], ["display_name", "text"]],
    ],
    ids=["missing", "empty", "incomplete", "unrelated", "reordered", "wrong-type", "extra-column"],
)
async def test_digest_refuses_missing_or_changed_native_key_without_paging(native_keys):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=None if native_keys is None else canonical_json(native_keys)),
        fetch=AsyncMock(),
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_key_invalid"):
        await validation._table_digest(connection, '"catalog_fixture"', "registry_fhir_catalog_network")
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("unrelated_unique", [False, True])
async def test_duplicate_paging_keys_refuse_after_writer_closure(metadata_custody, unrelated_unique):
    fixture = metadata_custody
    snapshot_by_field = expected_snapshot(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    await fixture.connection.execute(f'INSERT INTO "{fixture.control_schema}".network_registry_identity VALUES(7)')
    await fixture.loader.execute(
        f"ALTER TABLE {namespace}.registry_fhir_catalog_network DROP CONSTRAINT registry_fhir_catalog_network_pkey"
    )
    if unrelated_unique:
        await fixture.loader.execute(
            f"CREATE UNIQUE INDEX unrelated_unique ON {namespace}.registry_fhir_catalog_network "
            "(metadata_generation,network_id,display_name)"
        )
    await fixture.loader.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_network "
        "(metadata_generation,network_id,display_name,aliases,archived,revision,benefit_codes,search_name) "
        "SELECT $1,7,'Example Network '||ordinal,'[]'::jsonb,false,1,NULL,'example network '||ordinal "
        "FROM generate_series(1,$2::integer) ordinal",
        snapshot_by_field["metadata_generation"],
        validation.PAGE_ROWS + 1,
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_validation_key_invalid"):
        await freeze_metadata(fixture, snapshot=snapshot_by_field)
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_fhir_catalog_network") == 1001
    assert (
        await fixture.connection.fetchval(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1", fixture.target.schema_name
        )
        == fixture.roles["loader"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["schema_missing", "table_missing", "rls"])
async def test_sealed_catalog_requires_its_complete_original_unfiltered_heaps(metadata_custody, change):
    fixture = metadata_custody
    selected = await freeze_metadata(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    statements_by_change = {
        "schema_missing": f"DROP SCHEMA {namespace} CASCADE",
        "table_missing": f"DROP TABLE {namespace}.registry_fhir_catalog_network",
        "rls": f"ALTER TABLE {namespace}.registry_fhir_catalog_network ENABLE ROW LEVEL SECURITY",
    }
    await fixture.connection.execute(statements_by_change[change])
    async with fixture.connection.transaction(readonly=True):
        with pytest.raises(ValueError, match="custody_invalid"):
            await verify_fhir_catalog_snapshot(fixture.connection, selected)
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_registry_identity')
        == 0
    )
