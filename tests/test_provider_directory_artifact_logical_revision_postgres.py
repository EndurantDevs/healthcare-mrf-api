# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native logical artifact revisions and candidate closure in disposable storage."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_artifact_bundle_preparation as artifacts
from process.entity_address_cutover_contract import postgres_sqlstate
from tests.provider_directory_entities_postgres_support import _database_url

fhir = importlib.import_module("process.provider_directory_fhir")


def _database(url):
    engine = create_async_engine(url, pool_size=1, max_overflow=0)
    return Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))


async def _create_accounts(administrator, schema, account_by_purpose):
    async with administrator.begin() as connection:
        for role, credential, _database in account_by_purpose.values():
            await connection.execute(
                text(f"CREATE ROLE \"{role}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{credential}'")
            )
        owner = account_by_purpose["owner"][0]
        await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"'))
        for role, _password, _database in account_by_purpose.values():
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))


async def _remove_accounts(administrator, schema, account_by_purpose):
    for _role, _password, database in account_by_purpose.values():
        await database.disconnect()
    roles = [role for role, _password, _database in account_by_purpose.values()]
    async with administrator.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        for role in roles:
            await connection.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))"), {"roles": roles}
        )
        assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None


@asynccontextmanager
async def _artifact_case(monkeypatch):
    url = _database_url()
    token = uuid4().hex
    schema = "artifact_revision_" + token
    administrator = create_async_engine(url)
    account_by_purpose = {}
    for purpose in ("owner", "writer", "reader"):
        role, credential = "artifact_" + purpose + "_" + token[:16], uuid4().hex
        account_by_purpose[purpose] = (role, credential, _database(url.set(username=role, password=credential)))
    try:
        await _create_accounts(administrator, schema, account_by_purpose)
        database = account_by_purpose["owner"][2]
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _create_revision_state(database, schema)
        yield SimpleNamespace(schema=schema, database=database, accounts=account_by_purpose)
    finally:
        try:
            await _remove_accounts(administrator, schema, account_by_purpose)
        finally:
            await administrator.dispose()


async def _create_revision_state(database, schema):
    await database.status(
        f'CREATE TABLE "{schema}".address_alias_artifact_state_v1 '
        "(artifact_name text PRIMARY KEY,generation bigint NOT NULL,updated_at timestamptz NOT NULL)"
    )
    await database.status(
        f'CREATE TABLE "{schema}".address_alias_state_v1 '
        "(singleton boolean PRIMARY KEY,schema_version smallint,active_ruleset_version smallint,generation bigint)"
    )
    await database.status(
        f'INSERT INTO "{schema}".address_alias_state_v1 VALUES (true,:schema_version,:ruleset,0)',
        schema_version=fhir.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION,
        ruleset=fhir.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
    )


async def _no_indexes(_schema, _stage):
    return None


async def _stage(case, target, alias_generation=None):
    publication = await artifacts.artifact_publication(fhir, case.schema, target)
    return fhir.ProviderDirectoryPreparedArtifactStage(
        schema=case.schema,
        stage_table="candidate",
        target_relation=target,
        rename_stage_indexes=_no_indexes,
        build_fence=fhir.ProviderDirectoryArtifactBuildFence(
            target_name=(case.schema, target), target_publication=publication, alias_generation=alias_generation
        ),
    )


async def _create_target(case, target, alias_generation=None):
    for name, marker in ((target, "original"), ("candidate", "replacement")):
        await case.database.status(f'CREATE TABLE "{case.schema}"."{name}" (marker text PRIMARY KEY)')
        await case.database.status(f'INSERT INTO "{case.schema}"."{name}" VALUES (:marker)', marker=marker)
    stage = await _stage(case, target, alias_generation)
    async with case.database.transaction():
        await fhir._record_address_alias_artifact_generation(stage)
    return await _stage(case, target, alias_generation)


async def _publish(case, stage):
    await artifacts.assert_artifact_build_fence(fhir, stage)
    await case.database.status(f'ALTER TABLE "{case.schema}"."{stage.target_relation}" RENAME TO previous')
    await case.database.status(f'ALTER TABLE "{case.schema}".candidate RENAME TO "{stage.target_relation}"')
    await fhir._record_address_alias_artifact_generation(stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_generation", [0, 7])
async def test_same_name_same_alias_republication_rejects_stale_fence(monkeypatch, alias_generation):
    async with _artifact_case(monkeypatch) as case:
        await case.database.status(
            f'UPDATE "{case.schema}".address_alias_state_v1 SET generation=:generation', generation=alias_generation
        )
        target = fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
        stage = await _create_target(case, target, alias_generation)
        before = await artifacts.address_overlay_publication(fhir, case.schema)
        async with case.database.transaction():
            await _publish(case, stage)
        after = await artifacts.address_overlay_publication(fhir, case.schema)
        assert before[0] == after[0] == alias_generation and after[1] > before[1]
        assert await case.database.scalar(f'SELECT marker FROM "{case.schema}"."{target}"') == "replacement"
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale):
            async with case.database.transaction():
                await artifacts.assert_artifact_build_fence(fhir, stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["alias", "published_overlay", "scratch_overlay"])
async def test_corroboration_refuses_changed_overlay_inputs_and_keeps_serving(monkeypatch, change):
    async with _artifact_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        target_relation = fhir.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
        stage = await _create_target(case, target_relation, 0)
        overlay = fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
        dependency = "prepared_overlay" if change == "scratch_overlay" else overlay
        await database.status(f'CREATE TABLE "{schema}"."{dependency}" (marker text)')
        if change != "scratch_overlay":
            async with database.transaction():
                await fhir._record_address_alias_artifact_generation(await _stage(case, overlay, 0))
        token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({overlay: dependency})
        try:
            build_fence = await artifacts.capture_address_corroboration_fence(
                fhir, schema, target_relation, ["source_example"], stage.build_fence
            )
        finally:
            fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)
        stage = replace(stage, build_fence=build_fence)
        async with database.transaction():
            await artifacts.assert_artifact_build_fence(fhir, stage)
        if change == "alias":
            await database.status(f'UPDATE "{schema}".address_alias_state_v1 SET generation=1')
            expected = "address_alias_generation"
        elif change == "published_overlay":
            async with database.transaction():
                await fhir._record_address_alias_artifact_generation(await _stage(case, overlay, 0))
            expected = "dependency_relation"
        else:
            await database.status(f'DROP TABLE "{schema}"."{dependency}" RESTRICT')
            expected = "dependency_relation"
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match=expected):
            async with database.transaction():
                await artifacts.assert_artifact_build_fence(fhir, stage)
        assert await database.scalar(f'SELECT marker FROM "{schema}"."{target_relation}"') == "original"
        assert await database.scalar(f'SELECT marker FROM "{schema}".candidate') == "replacement"


@pytest.mark.asyncio
async def test_rollback_restores_publication_revision_and_prior_fence(monkeypatch):
    async with _artifact_case(monkeypatch) as case:
        stage = await _create_target(case, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, 0)
        before = stage.build_fence.target_publication
        with pytest.raises(RuntimeError, match="rollback"):
            async with case.database.transaction():
                await _publish(case, stage)
                assert await artifacts.artifact_publication(fhir, case.schema, stage.target_relation) != before
                raise RuntimeError("rollback")
        assert await artifacts.artifact_publication(fhir, case.schema, stage.target_relation) == before
        assert await case.database.scalar(f'SELECT marker FROM "{case.schema}"."{stage.target_relation}"') == "original"
        async with case.database.transaction():
            await artifacts.assert_artifact_build_fence(fhir, stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_generation", [None, 0])
async def test_nonaligned_artifact_publication_advances_monotonic_revision(monkeypatch, alias_generation):
    async with _artifact_case(monkeypatch) as case:
        stage = await _create_target(case, "provider_directory_network", alias_generation)
        await case.database.status(
            f"UPDATE \"{case.schema}\".address_alias_artifact_state_v1 SET updated_at='2100-01-01 00:00:00+00'"
        )
        before = await artifacts.artifact_publication(fhir, case.schema, stage.target_relation)
        stage = replace(stage, build_fence=replace(stage.build_fence, target_publication=before))
        async with case.database.transaction():
            await _publish(case, stage)
        after = await artifacts.artifact_publication(fhir, case.schema, stage.target_relation)
        assert before[0] is after[0] is True
        assert after[1][0] == before[1][0] == "0" and after[1][1] > before[1][1]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [fhir.profile_artifact.PROFILE_TABLE, fhir.profile_artifact.PROFILE_EVIDENCE_TABLE])
async def test_profile_publication_uses_profile_generation_without_alias_revision(monkeypatch, target):
    async with _artifact_case(monkeypatch) as case:
        await case.database.status(f'CREATE TABLE "{case.schema}"."{target}" (marker text)')
        state = fhir._provider_directory_profile_serving_generation_ref(case.schema)
        await case.database.status(
            f"CREATE TABLE {state} (singleton_key text PRIMARY KEY,generation_id text NOT NULL,updated_at timestamptz)"
        )
        await case.database.status(
            f"INSERT INTO {state} VALUES ('global',:generation,clock_timestamp())", generation="a" * 64
        )
        stage = await _stage(case, target)
        async with case.database.transaction():
            await fhir._record_address_alias_artifact_generation(stage)
            await artifacts.assert_artifact_build_fence(fhir, stage)
        assert not await case.database.scalar(
            f'SELECT EXISTS(SELECT FROM "{case.schema}".address_alias_artifact_state_v1)'
        )
        await case.database.status(f"UPDATE {state} SET generation_id=:generation", generation="b" * 64)
        current = await artifacts.artifact_publication(fhir, case.schema, target)
        assert current[1][0] == "b" * 64
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale):
            async with case.database.transaction():
                await artifacts.assert_artifact_build_fence(fhir, stage)


async def _denied(database, statement):
    with pytest.raises(DBAPIError) as failure:
        async with database.transaction():
            await database.status(statement)
    assert postgres_sqlstate(failure.value) == "42501"


@pytest.mark.asyncio
async def test_frozen_candidate_denies_native_writes_while_live_reader_stays_pinned(monkeypatch):
    async with _artifact_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        writer_role, _password, writer = case.accounts["writer"]
        reader_role, _password, reader = case.accounts["reader"]
        await _create_target(case, "provider_directory_network")
        await database.status(f'GRANT SELECT ON "{schema}".provider_directory_network TO "{reader_role}"')
        await database.status(f'GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON "{schema}".candidate TO "{writer_role}"')
        await writer.status(f"UPDATE \"{schema}\".candidate SET marker='prepared'")
        async with reader.transaction():
            await reader.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"
            async with asyncio.timeout(2):
                await artifacts.freeze_artifact_stage(fhir, schema, "candidate")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"
        for account in (database, writer):
            for statement in (
                f"INSERT INTO \"{schema}\".candidate VALUES ('late')",
                f"UPDATE \"{schema}\".candidate SET marker='late'",
                f'DELETE FROM "{schema}".candidate',
                f'TRUNCATE "{schema}".candidate',
            ):
                await _denied(account, statement)
        assert await database.scalar(f'SELECT marker FROM "{schema}".candidate') == "prepared"


async def _require_pending_writer_refusal(case):
    database, schema = case.database, case.schema
    async with asyncio.timeout(1):
        with pytest.raises(DBAPIError) as failure:
            await artifacts.freeze_artifact_stage(fhir, schema, "candidate")
    assert postgres_sqlstate(failure.value) == "55P03"
    await database.status(f"INSERT INTO \"{schema}\".candidate VALUES ('not_frozen')")
    await database.status(f"DELETE FROM \"{schema}\".candidate WHERE marker='not_frozen'")


@pytest.mark.asyncio
async def test_pending_writer_refuses_freeze_without_partial_revocation_then_rolls_back(monkeypatch):
    async with _artifact_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        writer_role, _credential, writer = case.accounts["writer"]
        reader_role, _credential, reader = case.accounts["reader"]
        await _create_target(case, "provider_directory_network")
        await database.status(f'GRANT SELECT ON "{schema}".provider_directory_network TO "{reader_role}"')
        await database.status(f'GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON "{schema}".candidate TO "{writer_role}"')
        async with reader.transaction():
            await reader.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"
            with pytest.raises(RuntimeError, match="writer rollback"):
                async with writer.transaction():
                    await writer.status(f"UPDATE \"{schema}\".candidate SET marker='pending'")
                    await _require_pending_writer_refusal(case)
                    await writer.status(f"INSERT INTO \"{schema}\".candidate VALUES ('still_writable')")
                    await writer.status(f"DELETE FROM \"{schema}\".candidate WHERE marker='still_writable'")
                    await writer.status(f'TRUNCATE "{schema}".candidate')
                    raise RuntimeError("writer rollback")
            assert await database.scalar(f'SELECT marker FROM "{schema}".candidate') == "replacement"
            await artifacts.freeze_artifact_stage(fhir, schema, "candidate")
            await _denied(database, f"UPDATE \"{schema}\".candidate SET marker='late'")
            await _denied(writer, f"INSERT INTO \"{schema}\".candidate VALUES ('late')")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"


@pytest.mark.asyncio
async def test_native_owner_can_lock_and_publish_frozen_candidate_without_reopening_writes(monkeypatch):
    async with _artifact_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        reader_role, _credential, reader = case.accounts["reader"]
        stage = await _create_target(case, "provider_directory_network")
        await database.status(f'GRANT SELECT ON "{schema}".provider_directory_network TO "{reader_role}"')
        async with reader.transaction():
            await reader.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"
            await artifacts.freeze_artifact_stage(fhir, schema, "candidate")
            assert await reader.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "original"
        await _denied(database, f"INSERT INTO \"{schema}\".candidate VALUES ('late')")
        await _denied(database, f"UPDATE \"{schema}\".candidate SET marker='late'")
        async with database.transaction():
            await database.status(f'LOCK TABLE "{schema}".candidate IN ACCESS EXCLUSIVE MODE NOWAIT')
            await _publish(case, stage)
            assert await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_network') == "replacement"
        assert await database.scalar(f'SELECT marker FROM "{schema}".previous') == "original"
        await _denied(database, f"UPDATE \"{schema}\".provider_directory_network SET marker='late'")
