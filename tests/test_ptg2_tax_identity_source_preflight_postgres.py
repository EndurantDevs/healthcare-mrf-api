# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial PostgreSQL proof for source-local publication preflights."""

from __future__ import annotations

from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_tax_identity_source_artifact as artifact
from process.ptg_parts import ptg2_tax_identity_source_preflight as preflight
from process.ptg_parts import ptg2_tax_identity_source_projection as projection
from process.ptg_parts import ptg2_tax_identity_source_publish as source_publish
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from process.ptg_parts import ptg2_tax_identity_source_target_preflight as target
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_provider_tax_identity_postgres_support import (
    create_prerequisites,
    drop_disposable_schema,
    load_migration as load_parent_migration,
    quoted,
    run_migration_action,
)
from tests.ptg2_tax_identity_source_postgres_targets import (
    SOURCE_SNAPSHOT_ID,
    insert_source_projection_targets,
)
from tests.ptg2_tax_identity_source_projection_fixture import (
    POLICY,
    ordinal_digest,
    write_sidecar,
)
from tests.test_ptg2_tax_identity_source_projection_postgres import (
    _assert_source_tables_empty,
    _configure_projector_database,
    _database_url,
    _fresh_projection,
    _load_source_migration,
    _prepare_source_projection,
)


@asynccontextmanager
async def _prepared_projection_database(monkeypatch, tmp_path: Path):
    engine = create_async_engine(_database_url(), pool_size=2, max_overflow=0)
    schema_name = f"ptg2_tax_identity_test_{uuid.uuid4().hex}"
    database = Database(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )
    _configure_projector_database(monkeypatch, database, schema_name)
    has_created_schema = False
    try:
        await create_prerequisites(engine, schema_name)
        has_created_schema = True
        await run_migration_action(engine, load_parent_migration(), "upgrade")
        await insert_source_projection_targets(engine, schema_name)
        await run_migration_action(engine, _load_source_migration(), "upgrade")
        yield database, schema_name, partial(_prepare_source_projection, tmp_path)
    finally:
        if has_created_schema:
            await drop_disposable_schema(engine, schema_name)
        await engine.dispose()


async def _assert_guarded_mutation_rejected(
    session,
    *,
    schema_name: str,
    prepared,
    operation: str,
    use_seal: bool,
) -> None:
    staged = await source_stage.stage_tax_identity_source_projection(
        session,
        prepared,
    )
    relation_name = staged.seal_table_name if use_seal else staged.table_name
    relation = f'{quoted("pg_temp")}.{quoted(relation_name)}'
    update_assignment = (
        "stage_table = stage_table"
        if use_seal
        else "source_ordinal = source_ordinal + 1"
    )
    statement_by_operation = {
        "insert": f"INSERT INTO {relation} SELECT * FROM {relation} LIMIT 1",
        "update": f"UPDATE {relation} SET {update_assignment}",
        "delete": f"DELETE FROM {relation}",
        "truncate": f"TRUNCATE TABLE {relation}",
    }
    with pytest.raises(sa.exc.DBAPIError, match="PTG2_TAX_SOURCE_STAGE_SEALED"):
        async with session.begin_nested():
            await session.execute(sa.text(statement_by_operation[operation]))
    await _assert_source_tables_empty(session, quoted(schema_name))


async def _assert_guarded_mutation_attempt(
    database: Database,
    *,
    schema_name: str,
    prepare_projection,
    operation: str,
    use_seal: bool,
) -> None:
    with _fresh_projection(prepare_projection) as prepared:
        async with database.transaction() as session:
            await _assert_guarded_mutation_rejected(
                session,
                schema_name=schema_name,
                prepared=prepared,
                operation=operation,
                use_seal=use_seal,
            )


async def _assert_guard_bypass_detected(
    database: Database,
    *,
    schema_name: str,
    prepare_projection,
) -> None:
    with _fresh_projection(prepare_projection) as prepared:
        async with database.transaction() as session:
            staged = await source_stage.stage_tax_identity_source_projection(
                session,
                prepared=prepared,
            )
            stage = f'{quoted("pg_temp")}.{quoted(staged.table_name)}'
            trigger = quoted(f"{staged.table_name}_guard")
            await session.execute(
                sa.text(f"ALTER TABLE {stage} DISABLE TRIGGER {trigger}")
            )
            await session.execute(sa.text(f"""
                UPDATE {stage}
                   SET source_key = source_key + 10,
                       source_ordinal = source_ordinal + 10
                """))
            await session.execute(sa.text(f"""
                UPDATE {stage}
                   SET source_key = CASE source_key WHEN 10 THEN 1 ELSE 0 END,
                       source_ordinal =
                           CASE source_ordinal WHEN 10 THEN 1 ELSE 0 END
                """))
            await session.execute(
                sa.text(f"ALTER TABLE {stage} ENABLE ALWAYS TRIGGER {trigger}")
            )
            with pytest.raises(
                projection.TaxIdentitySourceProjectionError,
                match="ptg2_tax_identity_source_projection_invalid",
            ):
                await source_publish.publish_staged_tax_identity_source_projection(
                    session,
                    schema_name=schema_name,
                    logical_snapshot_id=SOURCE_SNAPSHOT_ID,
                    snapshot_key=18,
                    staged=staged,
                    prepared=prepared,
                )
            await _assert_source_tables_empty(session, quoted(schema_name))


@pytest.mark.asyncio
async def test_sealed_stage_blocks_mutation_and_detects_guard_bypass(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove the stage is immutable and attribution is rechecked."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, schema_name, prepare_projection = prepared_db
        for use_seal in (False, True):
            for operation in ("insert", "update", "delete", "truncate"):
                await _assert_guarded_mutation_attempt(
                    database,
                    schema_name=schema_name,
                    prepare_projection=prepare_projection,
                    operation=operation,
                    use_seal=use_seal,
                )
        await _assert_guard_bypass_detected(
            database,
            schema_name=schema_name,
            prepare_projection=prepare_projection,
        )


@pytest.mark.asyncio
async def test_seal_self_oid_is_revalidated_after_guard_bypass(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove a restored guard cannot hide a modified self-seal."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, schema_name, prepare_projection = prepared_db
        with _fresh_projection(prepare_projection) as prepared:
            async with database.transaction() as session:
                staged = await source_stage.stage_tax_identity_source_projection(
                    session,
                    prepared=prepared,
                )
                seal = f'{quoted("pg_temp")}.{quoted(staged.seal_table_name)}'
                trigger = quoted(f"{staged.seal_table_name}_guard")
                await session.execute(
                    sa.text(f"ALTER TABLE {seal} DISABLE TRIGGER {trigger}")
                )
                await session.execute(
                    sa.text(f"UPDATE {seal} SET seal_oid = stage_oid")
                )
                await session.execute(
                    sa.text(f"ALTER TABLE {seal} ENABLE ALWAYS TRIGGER {trigger}")
                )
                with pytest.raises(
                    projection.TaxIdentitySourceProjectionError,
                    match="ptg2_tax_identity_source_projection_invalid",
                ):
                    await source_publish.publish_staged_tax_identity_source_projection(
                        session,
                        schema_name=schema_name,
                        logical_snapshot_id=SOURCE_SNAPSHOT_ID,
                        snapshot_key=18,
                        staged=staged,
                        prepared=prepared,
                    )
                await _assert_source_tables_empty(session, quoted(schema_name))


@pytest.mark.asyncio
async def test_stage_preflight_accepts_an_empty_physical_source(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove a real source binding may validly contribute zero observations."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, _schema_name, _prepare_projection = prepared_db
        empty_source_projection = artifact.prepare_tax_identity_source_projection(
            (
                write_sidecar(
                    tmp_path,
                    source_key=0,
                    shard_id="shard-a",
                    identity_digit="1",
                    state_codes=(1, 2, 3, 4),
                    matched_hmac=bytes.fromhex("44" * 16 + "55" * 16),
                ),
                write_sidecar(
                    tmp_path,
                    source_key=1,
                    shard_id="shard-b",
                    identity_digit="2",
                    state_codes=(),
                    matched_hmac=bytes.fromhex("44" * 16 + "66" * 16),
                ),
            ),
            scratch_parent=tmp_path,
            token_policy_id=POLICY,
            token_policy_descriptor_sha256=bytes.fromhex(
                token_policy_descriptor_sha256(POLICY)
            ),
            source_ordinal_map=(
                {"shard_id": "shard-a", "ordinal": 0},
                {"shard_id": "shard-b", "ordinal": 1},
            ),
            source_ordinal_map_digest=ordinal_digest(("shard-a", "shard-b")),
            aggregate_tax_content_digest=bytes.fromhex("33" * 32),
        )
        try:
            assert empty_source_projection.bindings[1].provider_group_count == 0
            async with database.transaction() as session:
                staged = await source_stage.stage_tax_identity_source_projection(
                    session,
                    empty_source_projection,
                )
                _stage, provider_group_count = (
                    await preflight.validate_staged_tax_identity_source_projection(
                        session,
                        staged=staged,
                        prepared=empty_source_projection,
                    )
                )
                assert provider_group_count == 4
                await source_stage._drop_staged_tax_identity_source_projection(
                    session,
                    staged,
                )
        finally:
            empty_source_projection.cleanup()


async def _assert_recreated_relation_rejected(
    session,
    *,
    schema_name: str,
    prepared,
    replace_stage: bool,
) -> None:
    staged = await source_stage.stage_tax_identity_source_projection(
        session,
        prepared,
    )
    relation_name = staged.table_name if replace_stage else staged.seal_table_name
    relation = f'{quoted("pg_temp")}.{quoted(relation_name)}'
    await session.execute(sa.text(f"DROP TABLE {relation}"))
    await session.execute(
        sa.text(f"CREATE TEMP TABLE {relation} (replacement integer) ON COMMIT DROP")
    )
    with pytest.raises(
        projection.TaxIdentitySourceProjectionError,
        match="ptg2_tax_identity_source_projection_invalid",
    ):
        await source_publish.publish_staged_tax_identity_source_projection(
            session,
            schema_name=schema_name,
            logical_snapshot_id=SOURCE_SNAPSHOT_ID,
            snapshot_key=18,
            staged=staged,
            prepared=prepared,
        )
    await _assert_source_tables_empty(session, quoted(schema_name))


@pytest.mark.asyncio
async def test_preflight_rejects_recreated_stage_and_seal_relations(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove relation names cannot substitute for sealed relation identity."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, schema_name, prepare_projection = prepared_db
        for replace_stage in (True, False):
            with _fresh_projection(prepare_projection) as prepared:
                async with database.transaction() as session:
                    await _assert_recreated_relation_rejected(
                        session,
                        schema_name=schema_name,
                        prepared=prepared,
                        replace_stage=replace_stage,
                    )


async def _assert_contender_lock_rejected(
    contender_database: Database,
    *,
    schema_name: str,
    prepared,
) -> None:
    with pytest.raises(sa.exc.DBAPIError, match="could not obtain lock"):
        async with contender_database.transaction() as contender:
            await target.lock_tax_identity_source_target_vector(
                contender,
                schema_name=schema_name,
                logical_snapshot_id=SOURCE_SNAPSHOT_ID,
                prepared=prepared,
            )


async def _assert_target_vector_locking(
    database: Database,
    *,
    schema_name: str,
    prepare_projection,
) -> None:
    contender_database = Database(
        engine=database.engine,
        session_factory=async_sessionmaker(
            database.engine,
            expire_on_commit=False,
        ),
    )
    with _fresh_projection(prepare_projection) as prepared:
        async with database.transaction() as lock_owner:
            await target.lock_tax_identity_source_target_vector(
                lock_owner,
                schema_name=schema_name,
                logical_snapshot_id=SOURCE_SNAPSHOT_ID,
                prepared=prepared,
            )
            await _assert_contender_lock_rejected(
                contender_database,
                schema_name=schema_name,
                prepared=prepared,
            )
            await _assert_tail_fence_mutations_rejected(
                contender_database,
                schema_name=schema_name,
            )
        async with database.transaction() as released_contender:
            assert (
                await target.lock_tax_identity_source_target_vector(
                    released_contender,
                    schema_name=schema_name,
                    logical_snapshot_id=SOURCE_SNAPSHOT_ID,
                    prepared=prepared,
                )
                == SOURCE_SNAPSHOT_ID
            )


@pytest.mark.asyncio
async def test_target_vector_tail_fence_rejects_concurrent_mutation(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove the NOWAIT tail fence blocks vector phantoms and mutations."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, schema_name, prepare_projection = prepared_db
        await _assert_target_vector_locking(
            database,
            schema_name=schema_name,
            prepare_projection=prepare_projection,
        )


async def _assert_tail_fence_mutations_rejected(
    contender_database: Database,
    *,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    mutation_statements = (
        f"""
        UPDATE {schema}.ptg2_snapshot
           SET status = 'complete'
         WHERE snapshot_id = :snapshot_id
        """,
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_source
            (snapshot_id, source_key, source_type, identity_kind,
             identity_sha256)
        VALUES
            (:snapshot_id, 2, 'in_network',
             'logical_json_sha256_v1', :identity_sha256)
        """,
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_source
           SET identity_sha256 = :identity_sha256
         WHERE snapshot_id = :snapshot_id AND source_key = 0
        """,
        f"""
        DELETE FROM {schema}.ptg2_v3_snapshot_source
         WHERE snapshot_id = :snapshot_id AND source_key = 0
        """,
    )
    for mutation_statement in mutation_statements:
        with pytest.raises(sa.exc.DBAPIError, match="lock timeout"):
            async with contender_database.transaction() as contender:
                await contender.execute(sa.text("SET LOCAL lock_timeout = '100ms'"))
                await contender.execute(
                    sa.text(mutation_statement),
                    {
                        "snapshot_id": SOURCE_SNAPSHOT_ID,
                        "identity_sha256": "8" * 64,
                    },
                )
