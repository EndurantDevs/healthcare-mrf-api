# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in real PostgreSQL proof for the source-local projector."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from functools import partial
import importlib.util
import os
from pathlib import Path
import re
from unittest.mock import Mock
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_tax_identity_source_artifact as artifact
from process.ptg_parts import ptg2_tax_identity_source_observations as observations
from process.ptg_parts import ptg2_tax_identity_source_preflight as preflight
from process.ptg_parts import ptg2_tax_identity_source_publish as source_publish
from process.ptg_parts import ptg2_tax_identity_source_projection as projection
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from process.ptg_parts import ptg2_tax_identity_source_target_preflight as target
from process.ptg_parts import ptg2_tax_identity_source_validation as validation
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_provider_tax_identity_postgres_support import (
    create_prerequisites,
    drop_disposable_schema,
    load_migration as load_parent_migration,
    quoted,
    run_migration_action,
)
from tests.ptg2_tax_identity_source_projection_fixture import (
    POLICY as _POLICY,
    ordinal_digest as _ordinal_digest,
    write_sidecar as _sidecar,
)
from tests.ptg2_tax_identity_source_resolver_postgres_support import (
    assert_source_resolver,
)
from tests.ptg2_tax_identity_source_postgres_targets import (
    CONFLICTING_SOURCE_SNAPSHOT_ID as _CONFLICTING_SNAPSHOT_ID,
    SOURCE_SNAPSHOT_ID as _SNAPSHOT_ID,
    insert_source_projection_targets as _insert_aggregate,
)
from tests.ptg2_tax_identity_source_reuse_support import (
    assert_sealed_reuse_proofs,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260806100000_ptg2_tax_identity_source.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG2_TAX_IDENTITY_SOURCE_POSTGRES_DSN"
_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)


def _load_source_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_tax_source_projection_postgres_proof",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    url = make_url(raw_dsn)
    database_name = str(url.database or "")
    if (
        not url.drivername.startswith("postgresql")
        or not database_name
        or _DATABASE_RE.search(database_name) is None
    ):
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return url.set(drivername="postgresql+asyncpg")


async def _publish(
    database: Database,
    prepared,
    schema_name: str,
    *,
    heartbeat_callback=None,
):
    async with database.transaction() as session:
        staged = await source_stage.stage_tax_identity_source_projection(
            session,
            prepared,
        )
        await _assert_stage_is_query_ready(
            session,
            stage_table=staged.table_name,
            expected_row_count=prepared.provider_group_occurrence_count,
        )
        return await source_publish.publish_staged_tax_identity_source_projection(
            session,
            schema_name=schema_name,
            logical_snapshot_id=_SNAPSHOT_ID,
            snapshot_key=18,
            staged=staged,
            prepared=prepared,
            heartbeat_callback=heartbeat_callback,
        )


async def _prove_post_source_rollback(
    database: Database,
    prepare_projection,
    schema_name: str,
) -> None:
    with _fresh_projection(prepare_projection) as prepared:
        with pytest.raises(RuntimeError, match="post-source publication failure"):
            async with database.transaction() as session:
                staged = await source_stage.stage_tax_identity_source_projection(
                    session,
                    prepared,
                )
                await source_publish.publish_staged_tax_identity_source_projection(
                    session,
                    schema_name=schema_name,
                    logical_snapshot_id=_SNAPSHOT_ID,
                    snapshot_key=18,
                    staged=staged,
                    prepared=prepared,
                )
                raise RuntimeError("post-source publication failure")
    schema = quoted(schema_name)
    async with database.transaction() as session:
        stored_count = await session.scalar(sa.text(f"""
                SELECT COUNT(*)::bigint
                  FROM {schema}.ptg2_provider_group_tax_identity_source
                 WHERE snapshot_key = 18
                """))
    assert int(stored_count or 0) == 0


async def _assert_source_tables_empty(session, schema: str) -> None:
    stored_counts = (await session.execute(sa.text(f"""
            SELECT
              (SELECT COUNT(*)::bigint FROM {schema}.
                  ptg2_provider_tax_identity_source_manifest),
              (SELECT COUNT(*)::bigint FROM {schema}.
                  ptg2_provider_tax_identity_source_binding),
              (SELECT COUNT(*)::bigint FROM {schema}.
                  ptg2_provider_group_tax_identity_source)
            """))).one()
    assert tuple(int(count) for count in stored_counts) == (0, 0, 0)


async def _assert_preinsert_rejection(
    session,
    *,
    schema_name: str,
    staged_prepared,
    published_prepared=None,
    logical_snapshot_id: str = _SNAPSHOT_ID,
    snapshot_key: int = 18,
) -> None:
    staged = await source_stage.stage_tax_identity_source_projection(
        session,
        staged_prepared,
    )
    with pytest.raises(
        projection.TaxIdentitySourceProjectionError,
        match="ptg2_tax_identity_source_projection_invalid",
    ):
        await source_publish.publish_staged_tax_identity_source_projection(
            session,
            schema_name=schema_name,
            logical_snapshot_id=logical_snapshot_id,
            snapshot_key=snapshot_key,
            staged=staged,
            prepared=published_prepared or staged_prepared,
        )
    await _assert_source_tables_empty(session, quoted(schema_name))


async def _prove_preinsert_guards(
    database: Database,
    prepare_projection,
    schema_name: str,
) -> None:
    with _fresh_projection(prepare_projection) as prepared:
        async with database.transaction() as session:
            await _assert_preinsert_rejection(
                session,
                schema_name=schema_name,
                staged_prepared=prepared,
                published_prepared=replace(prepared, content_digest=b"z" * 32),
            )
    for mismatch_by_field in (
        {"aggregate_tax_content_digest": b"x" * 32},
        {"source_ordinal_map_digest": b"y" * 32},
        {"bindings": "truncated"},
    ):
        with _fresh_projection(prepare_projection) as prepared:
            replacement_by_field = dict(mismatch_by_field)
            if replacement_by_field.get("bindings") == "truncated":
                replacement_by_field["bindings"] = prepared.bindings[:1]
            mismatched = replace(prepared, **replacement_by_field)
            async with database.transaction() as session:
                await _assert_preinsert_rejection(
                    session,
                    schema_name=schema_name,
                    staged_prepared=mismatched,
                )
    for snapshot_key in (19, 20):
        with _fresh_projection(prepare_projection) as prepared:
            async with database.transaction() as session:
                await _assert_preinsert_rejection(
                    session,
                    schema_name=schema_name,
                    staged_prepared=prepared,
                    snapshot_key=snapshot_key,
                )
    with _fresh_projection(prepare_projection) as prepared:
        async with database.transaction() as session:
            await _assert_preinsert_rejection(
                session,
                schema_name=schema_name,
                staged_prepared=prepared,
                logical_snapshot_id=_CONFLICTING_SNAPSHOT_ID,
            )


async def _assert_stage_is_query_ready(
    session,
    *,
    stage_table: str,
    expected_row_count: int,
) -> None:
    index_definitions = tuple(
        (
            await session.execute(
                sa.text("""
                    SELECT pg_get_indexdef(indexes.indexrelid)
                      FROM pg_index AS indexes
                      JOIN pg_class AS stage
                        ON stage.oid = indexes.indrelid
                     WHERE stage.relnamespace = pg_my_temp_schema()
                       AND stage.relname = :stage_table
                     ORDER BY indexes.indexrelid
                    """),
                {"stage_table": stage_table},
            )
        )
        .scalars()
        .all()
    )
    assert len(index_definitions) == 2
    assert any(
        "UNIQUE" in definition and "(source_key, source_record_ordinal)" in definition
        for definition in index_definitions
    )
    assert any(
        "(provider_group_global_id_128)" in definition
        for definition in index_definitions
    )
    analyzed_row_count = await session.scalar(
        sa.text("""
            SELECT stage.reltuples::bigint
              FROM pg_class AS stage
             WHERE stage.relnamespace = pg_my_temp_schema()
               AND stage.relname = :stage_table
            """),
        {"stage_table": stage_table},
    )
    assert int(analyzed_row_count) == expected_row_count


def _prepare_source_projection(tmp_path: Path):
    scratch_parent = tmp_path / "projection-scratch"
    scratch_parent.mkdir(exist_ok=True)
    return artifact.prepare_tax_identity_source_projection(
        (
            _sidecar(
                tmp_path,
                source_key=0,
                shard_id="shard-a",
                identity_digit="1",
                state_codes=(1, 2, 3, 4),
                matched_hmac=bytes.fromhex("44" * 16 + "55" * 16),
            ),
            _sidecar(
                tmp_path,
                source_key=1,
                shard_id="shard-b",
                identity_digit="2",
                state_codes=(2, 1, 3, 4),
                matched_hmac=bytes.fromhex("44" * 16 + "66" * 16),
            ),
        ),
        scratch_parent=scratch_parent,
        token_policy_id=_POLICY,
        token_policy_descriptor_sha256=bytes.fromhex(
            token_policy_descriptor_sha256(_POLICY)
        ),
        source_ordinal_map=(
            {"shard_id": "shard-a", "ordinal": 0},
            {"shard_id": "shard-b", "ordinal": 1},
        ),
        source_ordinal_map_digest=_ordinal_digest(("shard-a", "shard-b")),
        aggregate_tax_content_digest=bytes.fromhex("33" * 32),
    )


@contextmanager
def _fresh_projection(prepare_projection):
    prepared = prepare_projection()
    try:
        yield prepared
    finally:
        prepared.cleanup()


async def _verify_replay_and_conflict(
    database: Database,
    prepare_projection,
    schema_name: str,
):
    await _prove_preinsert_guards(database, prepare_projection, schema_name)
    await _prove_post_source_rollback(database, prepare_projection, schema_name)
    heartbeat = Mock()

    with _fresh_projection(prepare_projection) as prepared:
        published = await _publish(
            database,
            prepared,
            schema_name,
            heartbeat_callback=heartbeat,
        )
    with _fresh_projection(prepare_projection) as prepared:
        replayed = await _publish(
            database,
            prepared,
            schema_name,
            heartbeat_callback=heartbeat,
        )
    assert published == replayed
    assert published.provider_group_occurrence_count == 8
    assert heartbeat.call_count == 12
    with _fresh_projection(prepare_projection) as prepared:
        with pytest.raises(
            projection.TaxIdentitySourceProjectionError,
            match="ptg2_tax_identity_source_projection_invalid",
        ):
            await _publish(
                database,
                replace(prepared, content_digest=b"x" * 32),
                schema_name,
            )
    return published


async def _seal_and_verify_stored_projection(
    engine,
    schema_name: str,
    source_publication,
) -> None:
    schema = quoted(schema_name)
    async with engine.begin() as connection:
        stored_counts = tuple(
            int(stored_count) for stored_count in (await connection.execute(sa.text(f"""
                    SELECT
                      (SELECT COUNT(*) FROM {schema}.
                          ptg2_provider_tax_identity_source_manifest),
                      (SELECT COUNT(*) FROM {schema}.
                          ptg2_provider_tax_identity_source_binding),
                      (SELECT COUNT(*) FROM {schema}.
                          ptg2_provider_group_tax_identity_source)
                    """))).one()
        )
        assert stored_counts == (1, 2, 8)
        matched_tin_keys = tuple((await connection.execute(sa.text(f"""
                        SELECT tin_key
                          FROM {schema}.ptg2_provider_group_tax_identity_source
                         WHERE tax_identity_state = 'matched_ein'
                         ORDER BY provider_group_global_id_128
                        """))).scalars().all())
        assert matched_tin_keys == (0, 1)
        await connection.execute(
            sa.text(
                f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                "SET state = 'complete' WHERE snapshot_key = 18"
            )
        )
        await connection.execute(
            sa.text(
                f"UPDATE {schema}.ptg2_v3_snapshot_layout "
                "SET state = 'sealed' WHERE snapshot_key = 18"
            )
        )
        await assert_source_resolver(
            connection,
            schema_name,
            source_publication,
        )


def _configure_projector_database(monkeypatch, database, schema_name: str) -> None:
    """Bind projector modules to one disposable database and tiny batches."""

    for module in (
        source_publish,
        source_stage,
        preflight,
        target,
        observations,
        validation,
    ):
        monkeypatch.setattr(module, "db", database)
    monkeypatch.setattr(observations, "_OBSERVATION_BATCH_ROWS", 2)
    monkeypatch.setattr(validation, "_VALIDATION_BATCH_ROWS", 2)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)


@pytest.mark.asyncio
async def test_projector_publishes_replays_rolls_back_and_validates_reuse(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove fresh publish, replay, rollback, seal, and durable reuse."""

    engine = create_async_engine(_database_url(), pool_size=1, max_overflow=0)
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
        await _insert_aggregate(engine, schema_name)
        await run_migration_action(engine, _load_source_migration(), "upgrade")
        prepare_projection = partial(_prepare_source_projection, tmp_path)
        published = await _verify_replay_and_conflict(
            database,
            prepare_projection,
            schema_name,
        )
        await _seal_and_verify_stored_projection(
            engine,
            schema_name,
            published,
        )
        await assert_sealed_reuse_proofs(
            engine,
            schema_name=schema_name,
            published=published,
        )
    finally:
        if has_created_schema:
            await drop_disposable_schema(engine, schema_name)
        await engine.dispose()
