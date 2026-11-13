# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named COPY and checkpoint writes share the existing native transaction."""

import asyncio
from contextlib import asynccontextmanager
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.schema import CreateTable

from db.connection import Database
from db.models.system import ProviderDirectoryPaginationCheckpoint
from db.plan_release_storage import storage_identifier
from process import provider_directory_generic_source_policy as source
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_proof_store import delete_dataset_proof_shards
from process.provider_directory_source_checkpoint_control import (
    require_checkpoint_executor,
    source_checkpoint_transaction,
)
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage
from tests.test_provider_directory_isolated_source_provision import native_bootstrap as native_bootstrap


def _native_factory(engine, configured):
    """Use real sessions and the existing named Source transaction contexts."""

    @asynccontextmanager
    async def transaction(database, *, isolation):
        async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
            connection = await _copy_driver(session)
            async with (
                database.bind_existing_session(session),
                source.bind_generic_source_publisher(
                    connection, configured, verify_role_profile=AsyncMock(), transaction_isolation=isolation
                ),
            ):
                yield session

    return SimpleNamespace(
        transaction=transaction,
        policy=configured,
        storage_binding=NamedFHIRSourceStorage("catalog", "source_schema"),
        producer_identity=("a" * 40, "sha256:" + "b" * 64),
        _require_current_policy=lambda: None,
    )


async def _write_checkpoint_batch(database, unrelated, cursor, failure):
    directory = import_module("process.provider_directory_fhir")
    async with source_checkpoint_transaction(database, schema="catalog"):
        await require_checkpoint_executor(unrelated)
        executor = await directory._require_source_native_connection(unrelated)
        connection = await _copy_driver(source.source_copy_session())
        await connection.copy_records_to_table(
            "payload", schema_name="source_schema", columns=("resource_id",), records=[("synthetic",)]
        )
        await delete_dataset_proof_shards(unrelated, "catalog", "synthetic")
        checkpoint_by_field = dict(
            canonical_api_base="https://example.invalid/fhir",
            resource_type="Practitioner",
            source_scope_hash="scope",
            dataset_id="synthetic",
            source_ids='["synthetic-source"]',
            acquisition_root_run_id="root",
            owner_run_id="run",
            checkpoint_payload=cursor,
            state="active",
            pages_processed=None if failure == "checkpoint-sql" else 1,
            rows_processed=1,
            is_complete=False,
        )
        await executor.status(
            "INSERT INTO source_schema.provider_directory_pagination_checkpoint "
            "(canonical_api_base,resource_type,source_scope_hash,dataset_id,source_ids,acquisition_root_run_id,"
            "owner_run_id,start_url_hash,state,pages_processed,rows_processed,recent_cursor_hashes,"
            "completeness_json,created_at,updated_at) VALUES (:canonical_api_base,:resource_type,:source_scope_hash,"
            ":dataset_id,CAST(:source_ids AS json),:acquisition_root_run_id,:owner_run_id,'hash','active',0,0,"
            "'[]','{}',now(),now())",
            **checkpoint_by_field,
        )
        assert await directory._update_last_updated_partition_checkpoint(unrelated, checkpoint_by_field) == 1
        if failure == "checkpoint-sql":
            raise AssertionError("required checkpoint column accepted null")
        if failure is not None:
            raise failure


async def _prepare_named_atomicity_storage(case):
    """Create only isolated native data, checkpoint and transient proof fixtures."""
    owner = storage_identifier(case.roles["owner"])
    await case.connection.execute(f"CREATE SCHEMA source_schema AUTHORIZATION {owner}")
    await case.connection.execute("CREATE TABLE source_schema.payload (resource_id text PRIMARY KEY)")
    checkpoint = ProviderDirectoryPaginationCheckpoint.__table__.to_metadata(MetaData(), schema="source_schema")
    for constraint in tuple(checkpoint.foreign_key_constraints):
        checkpoint.constraints.remove(constraint)
    await case.connection.execute(str(CreateTable(checkpoint).compile(dialect=dialect())))
    await case.connection.execute(f"ALTER TABLE source_schema.payload OWNER TO {owner}")
    await case.connection.execute(
        f"ALTER TABLE source_schema.provider_directory_pagination_checkpoint OWNER TO {owner}"
    )
    await case.connection.execute(
        "CREATE TABLE source_schema.provider_directory_endpoint_dataset (dataset_id text PRIMARY KEY,"
        "endpoint_id text NOT NULL,acquisition_root_run_id text NOT NULL,status text NOT NULL,"
        "is_current boolean NOT NULL,publication_metadata_json jsonb,candidate_storage_json jsonb)"
    )
    await case.connection.execute(
        "INSERT INTO source_schema.provider_directory_endpoint_dataset VALUES "
        "('synthetic','endpoint','root','validated',false,'{}',NULL)"
    )
    await case.connection.execute(
        "CREATE TABLE source_schema.provider_directory_dataset_proof_shard (dataset_id text NOT NULL,"
        "shard_id text NOT NULL,resource_counts_json jsonb NOT NULL,PRIMARY KEY(dataset_id,shard_id))"
    )
    await case.connection.execute(
        "INSERT INTO source_schema.provider_directory_dataset_proof_shard VALUES "
        "('synthetic','existing','{\"Practitioner\":1}')"
    )
    for name in ("provider_directory_endpoint_dataset", "provider_directory_dataset_proof_shard"):
        await case.connection.execute(f"ALTER TABLE source_schema.{name} OWNER TO {owner}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, RuntimeError("checkpoint refused"), asyncio.CancelledError(), "checkpoint-sql"]
)
async def test_named_copy_and_checkpoint_commit_or_roll_back_together(native_bootstrap, monkeypatch, failure):
    case = native_bootstrap
    await _prepare_named_atomicity_storage(case)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", case.url.database)
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    database = Database()
    database._database_override = "synthetic_ambient_control"
    engine = create_async_engine(case.url)
    configured = source.GenericSourcePolicy(
        "source_schema", case.roles["owner"], case.roles["publisher"], (case.roles["reader"],)
    )
    factory = _native_factory(engine, configured)
    cursor = "https://example.invalid/fhir/Practitioner?_getpages=opaque%2Bcursor%2Ftoken%3D"
    unrelated = SimpleNamespace(status=AsyncMock(side_effect=AssertionError("unrelated executor used")))
    directory = import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "_schema", lambda: "catalog")
    try:
        async with source.bind_generic_source_transaction_factory(factory):
            try:
                await _write_checkpoint_batch(database, unrelated, cursor, failure)
            except DBAPIError as caught:
                assert failure == "checkpoint-sql"
                assert caught.orig.sqlstate == "23502" and "pages_processed" in str(caught)
            except (RuntimeError, asyncio.CancelledError) as caught:
                assert caught is failure
        expected = 1 if failure is None else 0
        assert await case.connection.fetchval("SELECT count(*) FROM source_schema.payload") == expected
        assert (
            await case.connection.fetchval(
                "SELECT count(*) FROM source_schema.provider_directory_pagination_checkpoint"
            )
            == expected
        )
        assert (
            await case.connection.fetchval("SELECT count(*) FROM source_schema.provider_directory_dataset_proof_shard")
        ) == 1 - expected
        if failure is None:
            assert (
                await case.connection.fetchval(
                    "SELECT next_url FROM source_schema.provider_directory_pagination_checkpoint"
                )
                == cursor
            )
        unrelated.status.assert_not_awaited()
        assert database._transaction_binding() is None
        assert source._OWNER.get() is None and source._PUBLISHER.get() is None
    finally:
        await engine.dispose()
