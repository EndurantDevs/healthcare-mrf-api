# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native pin mechanics with genuine component roles, not a runtime manifest."""

import asyncio
import importlib
import importlib.util
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from db.connection import Database
from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryCMSNPDResourceWitness,
    ProviderDirectoryDatasetAffiliationOrganization,
    ProviderDirectoryDatasetInsurancePlan,
    ProviderDirectoryDatasetNetworkPlan,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
    ProviderDirectorySource,
)
from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_rehydrate_pin as pin
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, RehydrationRequest
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_transaction,
    source_transaction_available,
)
from tests.provider_directory_source_postgres_support import native_source_component

fhir = importlib.import_module("process.provider_directory_fhir")
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.getenv("HP_REHYDRATION_PROJECTION_NATIVE") != "1", reason="requires explicit disposable pin fixture"
    ),
]
_MODELS = (
    ProviderDirectoryDatasetResource,
    ProviderDirectoryDatasetInsurancePlan,
    ProviderDirectoryDatasetNetworkPlan,
    ProviderDirectoryDatasetAffiliationOrganization,
    ProviderDirectoryCMSNPDResourceWitness,
)


def _relationship_layout(schema):
    """Reuse original columns/native checks; no legacy functions, hooks or FKs."""
    path = Path(__file__).parents[1] / "alembic/versions/20260930050000_cms_npd_relationship.py"
    spec = importlib.util.spec_from_file_location("pin_relationship_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    statements = []
    module.op = SimpleNamespace(execute=statements.append)
    module._schema = lambda: '"' + schema + '"'
    module.upgrade()
    return (
        re.sub(r"        FOREIGN KEY .*?ON DELETE CASCADE,\n", "", statements[0], flags=re.S)
        + " PARTITION BY LIST(dataset_id)"
    )


async def _layout(case):
    schema, database = case.policy.schema, case.database
    for model in (ProviderDirectoryAPIEndpoint, ProviderDirectorySource, ProviderDirectoryEndpointDataset, *_MODELS):
        query = fhir._provider_directory_artifact_scope_table_sql(model, schema, model.__tablename__)
        query = query.replace("CREATE UNLOGGED TABLE", "CREATE TABLE", 1).removesuffix(";")
        if model in _MODELS:
            query += " PARTITION BY LIST(dataset_id)"
        await database.status(query)
        keys = ",".join('"' + column.name + '"' for column in model.__table__.primary_key.columns)
        await database.status(f'ALTER TABLE "{schema}"."{model.__tablename__}" ADD PRIMARY KEY ({keys})')
    await database.status(_relationship_layout(schema))
    connection = bound_publisher().connection
    names_by_root = {}
    for root, suffix in zip(storage.RELATIONS, ("r", "ip", "np", "ao", "w", "rl")):
        name = "pdgs_" + case.token + "_" + suffix
        await connection.execute(
            f"CREATE TABLE {storage_relation(schema, name)} PARTITION OF {storage_relation(schema, root)} FOR VALUES IN ('dataset')"
        )
        await storage._close_grants(connection, case.policy, name, readable=True)
        names_by_root[root] = name
    readers = ",".join('"' + role + '"' for role in case.policy.runtime_roles)
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO {readers}')
    captured = await storage._capture(connection, case.policy, list(names_by_root.values()), private=False)
    return {root: captured[name] for root, name in names_by_root.items()}


async def _seed(case, tables):
    database, schema = case.database, case.policy.schema
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_api_endpoint (endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) VALUES ('endpoint','https://fixture.test/fhir',repeat('c',64),repeat('d',64))"
    )
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_source (source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) VALUES ('source','Fixture','endpoint','https://fixture.test/fhir',false,false)"
    )
    metadata = dict(selected_resources=["Location"], source_ids=["source"])
    identity_by_field = dict(
        dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root", metadata=metadata
    )
    binding_by_field = dict(
        contract=storage.CONTRACT,
        identity=identity_by_field,
        phase="published",
        ordinal=0,
        resets={},
        tables=tables,
        auxiliary=None,
        relationship_sha256=None,
    )
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_endpoint_dataset (dataset_id,endpoint_id,acquisition_root_run_id,status,is_current,resource_count,dataset_hash,published_at,publication_metadata_json,candidate_storage_json) VALUES ('dataset','endpoint','root','published',true,1,repeat('a',64),timestamp '2026-07-20 12:30:00',CAST(:metadata AS jsonb),CAST(:binding AS jsonb))",
        metadata=storage._encoded(metadata),
        binding=storage._encoded(binding_by_field),
    )
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_dataset_resource (dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES ('dataset','Location','location',repeat('b',64),'{{\"resource_id\":\"location\"}}')"
    )
    return binding_by_field


@asynccontextmanager
async def _case(monkeypatch):
    admin = Database()
    schema = "rehydrate_pin_" + uuid4().hex[:12]
    await admin.connect()
    assert "test" in str(await admin.scalar("SELECT current_database()"))
    await admin.status(f'CREATE SCHEMA "{schema}"')
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    try:
        async with native_source_component(admin.engine, schema, monkeypatch) as case:
            case.token, case.schema, case.admin = uuid4().hex, schema, admin
            monkeypatch.setenv("HLTHPRT_DB_READER_USER", case.reader)
            monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", "")
            monkeypatch.setattr(fhir, "db", case.database)
            async with source_transaction(case.database, isolation="repeatable read"):
                tables = await _layout(case)
                case.binding = await _seed(case, tables)
            case.runtime = SimpleNamespace(database=case.database, schema=schema)
            case.request = RehydrationRequest("source", "dataset", "root", "repair")
            yield case
    finally:
        await admin.status(f'DROP SCHEMA "{schema}" CASCADE')
        assert await admin.scalar("SELECT to_regnamespace(:name)", name=schema) is None
        await admin.disconnect()


async def _drop_probe(case, should_lock):
    table = case.binding["tables"][storage.RESOURCE]["name"]
    async with case.admin.engine.connect() as connection, connection.begin():
        if should_lock:
            with pytest.raises(Exception, match="could not obtain lock"):
                await connection.execute(text(f'LOCK TABLE "{case.schema}"."{table}" IN ACCESS EXCLUSIVE MODE NOWAIT'))
        else:
            await connection.execute(text(f'LOCK TABLE "{case.schema}"."{table}" IN ACCESS EXCLUSIVE MODE NOWAIT'))


async def _assert_native_reader_denials(pinned):
    """Native read-only transactions and grants reject actual source DML and DDL."""
    relation = pinned.source_relation(fhir)
    for statement in (f"DELETE FROM {relation} WHERE FALSE", f"ALTER TABLE {relation} ADD forbidden_column integer"):
        with pytest.raises(DBAPIError, match="read-only|permission denied|must be owner"):
            async with pinned.reader._session.begin_nested():
                await pinned.reader.scalar(statement)


async def test_native_original_reader_pins_without_binding_factory(monkeypatch):
    async with _case(monkeypatch) as case:
        async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            assert await pinned.reader.scalar("SHOW transaction_read_only") == "on"
            assert await pinned.reader.scalar("SHOW transaction_isolation") == "repeatable read"
            assert not case.database.has_reader_session() and case.database._transaction_binding() is None
            assert source_transaction_available() is None
            relation = pinned.reader.relation(storage.RESOURCE)
            assert await pinned.reader.scalar(f"SELECT count(*) FROM {relation}") == 1
            async with source_transaction(case.database, isolation="read committed"):
                await case.database.status(
                    f"UPDATE \"{case.schema}\".provider_directory_endpoint_dataset SET resource_count=resource_count WHERE dataset_id='dataset'"
                )
            await _assert_native_reader_denials(pinned)
            await _drop_probe(case, True)
        await _drop_probe(case, False)
        with pytest.raises(DatasetRehydrationError, match="reader_transaction_ended"):
            await pinned.reader.scalar("SELECT 1")


@pytest.mark.parametrize("mutation", ["table_name", "column", "select_right", "incomplete", "source", "root", "hash"])
async def test_native_pin_rejects_invalid_names_data_access_or_source(monkeypatch, mutation):
    async with _case(monkeypatch) as case:
        binding = case.binding
        table = binding["tables"][storage.RESOURCE]["name"]
        if mutation == "incomplete":
            encoded_binding = storage._encoded({})
        elif mutation == "table_name":
            binding["tables"][storage.RESOURCE]["name"] = "missing_resource"
            encoded_binding = storage._encoded(binding)
        else:
            encoded_binding = storage._encoded(binding)
        async with case.admin.engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET candidate_storage_json=CAST(:v AS jsonb)'
                ),
                {"v": encoded_binding},
            )
            if mutation in {"root", "hash"}:
                column, changed = (
                    ("acquisition_root_run_id", "wrong") if mutation == "root" else ("dataset_hash", "invalid")
                )
                await connection.execute(
                    text(f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET "{column}"=:v'),
                    {"v": changed},
                )
            if mutation == "source":
                await connection.execute(
                    text(
                        f"UPDATE \"{case.schema}\".provider_directory_source SET canonical_api_base='https://other.test/fhir'"
                    )
                )
            if mutation == "column":
                await connection.execute(
                    text(
                        f'ALTER TABLE "{case.schema}"."{storage.RESOURCE}" RENAME COLUMN payload_json TO incompatible_payload'
                    )
                )
            if mutation == "select_right":
                await connection.execute(text(f'REVOKE SELECT ON "{case.schema}"."{table}" FROM "{case.reader}"'))
        with pytest.raises((DatasetRehydrationError, GenericCandidateError, DBAPIError)):
            async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
                await pinned.reader.all(f"SELECT payload_json FROM {pinned.source_relation(fhir)}")
                pytest.fail("unsafe source admitted")
        # The deliberately missing name is a document error, not the real retained table.
        binding["tables"][storage.RESOURCE]["name"] = table
        await _drop_probe(case, False)


async def test_native_pin_child_task_and_caught_error_release(monkeypatch):
    async with _case(monkeypatch) as case:
        with pytest.raises(RuntimeError, match="late failure"):
            async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
                assert await asyncio.create_task(pinned.reader.scalar("SELECT 1")) == 1
                raise RuntimeError("late failure")
        await _drop_probe(case, False)


async def test_native_pin_cancellation_releases_original_reader(monkeypatch):
    async with _case(monkeypatch) as case:
        entered = asyncio.Event()

        async def run():
            async with pin.pinned_current_source(case.runtime, case.request, directory=fhir):
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(run())
        await entered.wait()
        await _drop_probe(case, True)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _drop_probe(case, False)


async def _assert_configured_pin(case, mode):
    """Permit native read rights while refusing modes that break immutable snapshots."""
    if mode == "alternate_principal":
        async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            assert await pinned.reader.scalar(f"SELECT count(*) FROM {pinned.source_relation(fhir)}") == 1
            await _assert_native_reader_denials(pinned)
            await _drop_probe(case, True)
    else:
        with pytest.raises(DatasetRehydrationError, match="reader_transaction_changed"):
            async with pin.pinned_current_source(case.runtime, case.request, directory=fhir):
                pytest.fail("unstable writable snapshot admitted")
    await _drop_probe(case, False)


@pytest.mark.parametrize("mode", ["alternate_principal", "read_committed", "read_write"])
async def test_native_pin_preserves_read_modes_with_native_principals(monkeypatch, mode):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async with _case(monkeypatch) as case:
        user = case.publisher if mode == "alternate_principal" else case.reader
        engine = create_async_engine(
            case.admin.engine.url.set(username=user, password=None),
            isolation_level="READ COMMITTED" if mode == "read_committed" else "REPEATABLE READ",
            execution_options={"postgresql_readonly": mode != "read_write"},
        )
        supplied = Database(engine=engine, session_factory=async_sessionmaker(engine))
        supplied._reader_login = (case.reader, "")

        async def substitute():
            return supplied

        monkeypatch.setattr(case.database, "_connect_reader", substitute)
        try:
            await _assert_configured_pin(case, mode)
        finally:
            await engine.dispose()


async def test_native_pin_accepts_benign_index_and_opaque_legacy_metadata(monkeypatch):
    async with _case(monkeypatch) as case:
        original = pin._capture_source
        saved_binding_by_field = {**case.binding, "database_oid": None}
        saved_binding_by_field["tables"] = {
            root: {**table, "oid": False, "owner": "opaque", "layout_sha256": None}
            for root, table in case.binding["tables"].items()
        }
        async with case.admin.engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET candidate_storage_json=CAST(:binding AS jsonb)'
                ),
                {"binding": storage._encoded(saved_binding_by_field)},
            )

        async def add_index(runtime, request):
            captured = await original(runtime, request)
            table = case.binding["tables"][storage.RESOURCE]["name"]
            async with case.admin.engine.begin() as connection:
                await connection.execute(
                    text(f'CREATE INDEX additional_lookup ON "{case.schema}"."{table}" (payload_hash)')
                )
            return captured

        monkeypatch.setattr(pin, "_capture_source", add_index)
        async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            assert await pinned.reader.all(f"SELECT resource_id,payload_json FROM {pinned.source_relation(fhir)}") == [
                ("location", {"resource_id": "location"})
            ]
            await _drop_probe(case, True)
        assert (
            await case.admin.scalar(
                f'SELECT candidate_storage_json FROM "{case.schema}".provider_directory_endpoint_dataset'
            )
            == saved_binding_by_field
        )
        await _drop_probe(case, False)


async def test_native_pin_keeps_scope_while_fresh_factory_observes_change(monkeypatch):
    async with _case(monkeypatch) as case:
        async with pin.pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            async with source_transaction(case.database, isolation="read committed"):
                await case.database.status(
                    f"UPDATE \"{case.schema}\".provider_directory_endpoint_dataset SET dataset_hash=repeat('c',64)"
                )
                fresh = await pin._load_dataset_scope(case.runtime, case.request, lock=True)
            assert fresh.fence_identity != pinned.scope.fence_identity
            assert (
                await pinned.reader.scalar(
                    f'SELECT dataset_hash FROM "{case.schema}".provider_directory_endpoint_dataset'
                )
                == "a" * 64
            )
        await _drop_probe(case, False)
