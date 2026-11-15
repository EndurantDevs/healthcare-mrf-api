# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual named source-factory intake, checkpoint and reset transaction proof."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from importlib import import_module
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable

from db.connection import Database
from db.models.system import (
    ProviderDirectoryDatasetAffiliationOrganization,
    ProviderDirectoryDatasetInsurancePlan,
    ProviderDirectoryDatasetNetworkPlan,
    ProviderDirectoryPaginationCheckpoint,
)
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_source_checkpoint_control import source_checkpoint_transaction
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.test_provider_directory_named_checkpoint_dataflow_postgres import _native_factory

directory = import_module("process.provider_directory_fhir")


def _metadata():
    return {
        "source_ids": [directory._stable_source_id(directory.anthem.seed_rows()[0])],
        "selected_resources": list(directory.anthem.RESOURCES),
        "expected_resources": list(directory.anthem.RESOURCES),
        "resource_hash_contract": "contract",
    }


async def _templates(connection, metadata):
    await connection.execute("CREATE SCHEMA catalog")
    await connection.execute("CREATE SCHEMA source_schema")
    await connection.execute(
        "CREATE TABLE source_schema.provider_directory_endpoint_dataset("
        "dataset_id varchar(96) PRIMARY KEY,endpoint_id text,acquisition_root_run_id text,status text,"
        "is_current boolean,published_at timestamp,publication_metadata_json jsonb,candidate_storage_json jsonb)"
    )
    await connection.execute(
        "INSERT INTO source_schema.provider_directory_endpoint_dataset VALUES "
        "('dataset','endpoint','root','acquiring',false,NULL,$1,NULL)",
        json.dumps(metadata),
    )
    await connection.execute(
        "CREATE TABLE source_schema.provider_directory_dataset_resource("
        "dataset_id varchar(96),resource_type varchar(64),resource_id varchar(256),payload_hash varchar(64),"
        "payload_json json,acquired_resource_sha256 varchar(64))"
    )
    await connection.execute(
        "CREATE TABLE source_schema.provider_directory_cms_npd_resource_witness("
        "dataset_id text,source_id text,release_id text,resource_type text,resource_id text,raw_payload_sha256 text,"
        "normalized_payload_hash text,raw_payload_json json)"
    )
    for model in (
        ProviderDirectoryDatasetInsurancePlan,
        ProviderDirectoryDatasetNetworkPlan,
        ProviderDirectoryDatasetAffiliationOrganization,
    ):
        table = model.__table__.to_metadata(MetaData(), schema="source_schema")
        await connection.execute(str(CreateTable(table).compile(dialect=dialect())))
    await connection.execute(
        "CREATE TABLE source_schema.provider_directory_cms_npd_relationship (dataset_id varchar(96) NOT NULL,"
        "source_id varchar(64) NOT NULL,release_id varchar(256) NOT NULL,resource_type varchar(64) NOT NULL,"
        "resource_id varchar(256) NOT NULL,source_payload_hash varchar(64) NOT NULL,raw_payload_sha256 varchar(64) NOT NULL,"
        "reference_field varchar(64) NOT NULL,parent_ordinal integer NOT NULL,reference_ordinal integer NOT NULL,"
        "target_type varchar(64) NOT NULL,target_reference text,target_resource_id varchar(256),"
        "resolution_status varchar(16) NOT NULL,period_start text,period_end text)"
    )
    checkpoint = ProviderDirectoryPaginationCheckpoint.__table__.to_metadata(MetaData(), schema="source_schema")
    for constraint in tuple(checkpoint.foreign_key_constraints):
        checkpoint.constraints.remove(constraint)
    await connection.execute(str(CreateTable(checkpoint).compile(dialect=dialect())))
    for resource_type in directory.anthem.RESOURCES:
        await connection.execute(
            "INSERT INTO source_schema.provider_directory_pagination_checkpoint "
            "(canonical_api_base,resource_type,source_scope_hash,dataset_id,source_ids,acquisition_root_run_id,"
            "owner_run_id,start_url_hash,state,pages_processed,rows_processed,recent_cursor_hashes,completeness_json,"
            "created_at,updated_at) VALUES ('https://example.invalid/fhir',$1,'scope','dataset',$2,'root','run',"
            "'hash','active',0,0,'[]','{}',now(),now())",
            resource_type,
            json.dumps(metadata["source_ids"]),
        )


@asynccontextmanager
async def _test_roles(admin):
    roles_by_kind = {
        key: "compact_" + key + "_" + uuid4().hex[:16] for key in ("owner", "publisher", "reader", "other")
    }
    registered_names = []
    try:
        for key, name in roles_by_kind.items():
            registered_names.append(name)
            await admin.execute(
                f"CREATE ROLE {storage_identifier(name)} " + ("LOGIN" if key == "publisher" else "NOLOGIN")
            )
        await admin.execute(
            f"GRANT {storage_identifier(roles_by_kind['owner'])} TO {storage_identifier(roles_by_kind['publisher'])}"
        )
        yield roles_by_kind
    finally:
        for name in reversed(registered_names):
            await admin.execute(f"DROP ROLE IF EXISTS {storage_identifier(name)}")
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", registered_names
        )


@asynccontextmanager
async def _intake_runtime(owned, roles_by_kind, metadata, connection, monkeypatch):
    engine = create_async_engine(
        owned.set(username=roles_by_kind["publisher"]),
        connect_args={"server_settings": {"statement_timeout": "10000", "lock_timeout": "5000"}},
    )
    database = Database()
    database.engine = engine
    configured = source.GenericSourcePolicy(
        "source_schema", roles_by_kind["owner"], roles_by_kind["publisher"], (roles_by_kind["reader"],)
    )
    factory = _native_factory(engine, configured)
    candidate = SimpleNamespace(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        selected_resources=directory.anthem.RESOURCES,
        reused_from_checkpoint=False,
    )
    adapter = SimpleNamespace(
        db=database,
        _schema=lambda: "catalog",
        _endpoint_dataset_candidate_metadata=lambda _candidate: metadata,
        RESOURCE_HASH_CONTRACT_METADATA_KEY="resource_hash_contract",
        RESOURCE_HASH_CONTRACTS=("contract",),
        SEMANTIC_PROJECTION_AS_OF_METADATA_KEY="semantic_projection_as_of",
    )
    monkeypatch.setattr(directory, "db", database)
    monkeypatch.setattr(directory, "_schema", lambda: "catalog")
    try:
        async with source.bind_generic_source_transaction_factory(factory):
            yield SimpleNamespace(
                database=database,
                factory=factory,
                candidate=candidate,
                directory=adapter,
                connection=connection,
                metadata=metadata,
                roles_by_kind=roles_by_kind,
            )
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def intake_case(monkeypatch):
    """Reuse the normal named factory with UUID-owned database and standard test roles."""
    url = _database_url().set(database="postgres")
    async with AsyncExitStack() as stack:
        admin = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        stack.push_async_callback(admin.close)
        roles_by_kind = await stack.enter_async_context(_test_roles(admin))
        owned, _database_admin = await stack.enter_async_context(_owned_database(url))
        connection = await asyncpg.connect(owned.set(drivername="postgresql").render_as_string(hide_password=False))
        stack.push_async_callback(connection.close)
        await connection.execute(
            f"GRANT CREATE ON DATABASE {storage_identifier(owned.database)} TO {storage_identifier(roles_by_kind['owner'])}"
        )
        await connection.execute(f"SET ROLE {storage_identifier(roles_by_kind['owner'])}")
        metadata = _metadata()
        await _templates(connection, metadata)
        yield await stack.enter_async_context(_intake_runtime(owned, roles_by_kind, metadata, connection, monkeypatch))


def _rows(case, resource_type, count=1):
    rows = []
    for ordinal in range(count):
        payload = {
            "resource_id": "same",
            "observation": ordinal,
            "network_refs": ["Organization/network"],
            "location_refs": ["Location/office"],
            "healthcare_service_refs": ["HealthcareService/" + "f" * 32],
        }
        rows.append(
            {
                "dataset_id": case.candidate.dataset_id,
                "resource_type": resource_type,
                "resource_id": "same",
                "payload_hash": hashlib.sha256(compact._json(payload).encode()).hexdigest(),
                "payload_json": payload,
                "acquired_resource_sha256": "b" * 64,
            }
        )
    return rows


async def _prepare(case, **kwargs):
    async with source.source_transaction(case.database):
        return await storage.prepare_generic_source_candidate(case.database, case.directory, case.candidate, **kwargs)


async def _append_checkpoint(case, resource_type, rows, *, fail=False):
    async with source_checkpoint_transaction(
        case.database, schema="catalog", dataset_ids=("dataset",), root_run_ids=("root",)
    ) as session:
        await storage.append_generic_source_batch(case.database, case.directory, case.candidate, rows)
        updated = await directory._update_last_updated_partition_checkpoint(
            session,
            dict(
                canonical_api_base="https://example.invalid/fhir",
                resource_type=resource_type,
                source_scope_hash="scope",
                dataset_id="dataset",
                acquisition_root_run_id="root",
                owner_run_id="run",
                source_ids=json.dumps(case.metadata["source_ids"]),
                checkpoint_payload="synthetic-issued-next",
                state="active",
                pages_processed=None if fail else 1,
                rows_processed=len(rows),
                is_complete=False,
            ),
        )
        assert updated == 1


async def _binding(case):
    return json.loads(
        await case.connection.fetchval(
            "SELECT candidate_storage_json::text "
            "FROM source_schema.provider_directory_endpoint_dataset WHERE dataset_id='dataset'"
        )
    )


async def _assert_other_role_denied(case, statements):
    await case.connection.execute(f"SET ROLE {storage_identifier(case.roles_by_kind['other'])}")
    try:
        for statement, arguments in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with case.connection.transaction():
                    await case.connection.execute(statement, *arguments)
    finally:
        await case.connection.execute(f"SET ROLE {storage_identifier(case.roles_by_kind['owner'])}")


@pytest.mark.asyncio
async def test_compact_intake_closes_default_table_column_and_sequence_grants(intake_case, monkeypatch):
    case = intake_case
    principal = storage_identifier(case.roles_by_kind["other"])
    await case.connection.execute(f"GRANT USAGE ON SCHEMA source_schema TO {principal}")
    for kind in ("TABLES", "SEQUENCES"):
        await case.connection.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA source_schema GRANT ALL ON {kind} TO {principal}"
        )
    original_ddl = compact.compact_payload_ddl

    def with_column_grant(tables):
        return (
            *original_ddl(tables),
            f"GRANT SELECT(payload_hash),UPDATE(payload_hash) ON TABLE {tables.relation('aliases')} TO {principal}",
        )

    monkeypatch.setattr(compact, "compact_payload_ddl", with_column_grant)
    binding_by_field = await _prepare(case, payload_format=compact.FORMAT)
    aliases = storage_relation("source_schema", binding_by_field["tables"]["resource_aliases"]["name"])
    view = storage_relation("source_schema", binding_by_field["tables"]["resource_input"]["name"])
    sequence = await case.connection.fetchval("SELECT pg_get_serial_sequence($1,'batch_ordinal')", aliases)
    await _assert_other_role_denied(
        case,
        [
            (f"SELECT * FROM {view}", ()),
            (f"SELECT * FROM {aliases}", ()),
            (f"SELECT payload_hash FROM {aliases}", ()),
            (f"UPDATE {aliases} SET payload_hash=payload_hash", ()),
            ("SELECT nextval($1::regclass)", (sequence,)),
        ],
    )
    await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"))
    assert await case.connection.fetchval(f"SELECT count(*) FROM {view}") == 1
    assert (await _binding(case))["ordinal"] == 1
    assert all(set(table) == {"schema", "name"} for table in binding_by_field["tables"].values())
    assert (
        {table["schema"] for table in binding_by_field["tables"].values()}
        == {binding_by_field["storage"]["storage_schema"]}
        == {"source_schema"}
    )


@pytest.mark.asyncio
async def test_compact_intake_checkpoint_atomicity_and_disabled_handoff(intake_case):
    case = intake_case
    binding = await _prepare(case, payload_format=compact.FORMAT)
    view = storage_relation("source_schema", binding["tables"]["resource_input"]["name"])
    original_rows = _rows(case, "PractitionerRole", 3)
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError) as failure:
        await _append_checkpoint(case, "PractitionerRole", original_rows, fail=True)
    assert failure.value.orig.sqlstate == "23502"
    assert await case.connection.fetchval(f"SELECT count(*) FROM {view}") == 0
    assert (await _binding(case))["ordinal"] == 0
    assert (
        await case.connection.fetchval(
            "SELECT rows_processed FROM source_schema.provider_directory_pagination_checkpoint "
            "WHERE resource_type='PractitionerRole'"
        )
        == 0
    )
    await _append_checkpoint(case, "PractitionerRole", original_rows)
    stored = await case.connection.fetch(f"SELECT * FROM {view} ORDER BY batch_ordinal,row_ordinal")
    assert len(stored) == 3 and len({stored_row["resource_id"] for stored_row in stored}) == 1
    assert len({stored_row["payload_hash"] for stored_row in stored}) == 3
    for actual, original in zip(stored, original_rows, strict=True):
        assert json.loads(actual["payload_json"]) == original["payload_json"]
        assert actual["payload_hash"] == original["payload_hash"]
    before = await _binding(case)
    async with source.source_transaction(case.database):
        assert await storage.prepare_generic_source_candidate(case.database, case.directory, case.candidate) == before
        with pytest.raises(ValueError, match="original_storage_publisher_required"):
            await completion.prepare_generic_source_handoff(None, before)
    assert await _binding(case) == before and await case.connection.fetchval(f"SELECT count(*) FROM {view}") == 3


@pytest.mark.asyncio
async def test_compact_concurrent_families_keep_unique_ordinals(intake_case, monkeypatch):
    case = intake_case
    binding = await _prepare(case, payload_format=compact.FORMAT)
    reached, ready = set(), asyncio.Event()
    original = storage._lock_resource_family

    async def lock_family(connection, policy, dataset_id, resource_type):
        await original(connection, policy, dataset_id, resource_type)
        reached.add(resource_type)
        if len(reached) == 2:
            ready.set()
        await asyncio.wait_for(ready.wait(), 3)

    monkeypatch.setattr(storage, "_lock_resource_family", lock_family)
    await asyncio.gather(
        *(
            _append_checkpoint(case, family, _rows(case, family))
            for family in ("PractitionerRole", "OrganizationAffiliation")
        )
    )
    view = storage_relation("source_schema", binding["tables"]["resource_input"]["name"])
    stored = await case.connection.fetch(f"SELECT resource_type,batch_ordinal FROM {view}")
    assert len(stored) == 2 and len({row["batch_ordinal"] for row in stored}) == 2
    assert (await _binding(case))["ordinal"] == max(row["batch_ordinal"] for row in stored)
    sets = storage_relation("source_schema", binding["tables"]["resource_reference_sets"]["name"])
    assert await case.connection.fetchval(f"SELECT count(*) FROM {sets}") == 1


@pytest.mark.asyncio
async def test_compact_all_families_reset_and_exact_disposal(intake_case):
    case = intake_case
    binding = await _prepare(case, payload_format=compact.FORMAT)
    for family in directory.anthem.RESOURCES:
        await _append_checkpoint(case, family, _rows(case, family))
    view = storage_relation("source_schema", binding["tables"]["resource_input"]["name"])
    assert await case.connection.fetchval(f"SELECT count(*) FROM {view}") == 7
    stored = await case.connection.fetch(f"SELECT * FROM {view} ORDER BY batch_ordinal,row_ordinal")
    for actual, family in zip(stored, directory.anthem.RESOURCES, strict=True):
        expected = _rows(case, family)[0]
        assert actual["resource_type"] == family and actual["resource_id"] == expected["resource_id"]
        assert json.loads(actual["payload_json"]) == expected["payload_json"]
        assert actual["payload_hash"] == expected["payload_hash"]
        assert actual["acquired_resource_sha256"] == expected["acquired_resource_sha256"]
    async with source.source_transaction(case.database):
        resolved = await storage.resolve_existing_source_candidate(case.database, case.directory, "dataset")
        await storage.reset_generic_source_family(case.database, case.directory, resolved, "PractitionerRole")
    reset = await _binding(case)
    cutoff = reset["resets"]["PractitionerRole"]
    assert cutoff == reset["ordinal"]
    await _append_checkpoint(case, "PractitionerRole", _rows(case, "PractitionerRole"))
    assert await case.connection.fetchval(f"SELECT count(*) FROM {view} WHERE resource_type='PractitionerRole'") == 2
    assert (
        await case.connection.fetchval(
            f"SELECT count(*) FROM {view} WHERE resource_type='PractitionerRole' AND batch_ordinal>$1", cutoff
        )
        == 1
    )
    async with source.source_transaction(case.database):
        async with source.source_owner_transaction(case.database):
            await directory.db.status("UPDATE source_schema.provider_directory_endpoint_dataset SET status='failed'")
        await storage.discard_generic_source_inputs(case.database, case.directory, case.candidate, None)
    assert (await _binding(case))["phase"] == "disposed"
    for table in binding["tables"].values():
        assert await case.connection.fetchval(
            "SELECT to_regclass($1) IS NULL", storage_relation("source_schema", table["name"])
        )


@pytest.mark.asyncio
async def test_existing_named_intake_stays_plaintext_and_explicit_switch_refuses(intake_case):
    case = intake_case
    binding = await _prepare(case)
    assert binding["contract"] == storage.NAMED_ISOLATED_CONTRACT and "payload_format" not in binding
    await _append_checkpoint(case, "Organization", _rows(case, "Organization"))
    before = await _binding(case)
    with pytest.raises(ValueError, match="payload_format_changed"):
        await _prepare(case, payload_format=compact.FORMAT)
    assert await _binding(case) == before
