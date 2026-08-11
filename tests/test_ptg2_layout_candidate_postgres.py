"""PostgreSQL proof for private PTG layout candidates and live adoption."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts import ptg2_shared_blocks, ptg2_shared_gc
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_DENSE_LAYOUT_TABLES,
    SharedMappingDigestSummary,
    reserve_shared_layout,
    seal_shared_layout,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    _defer_duplicate_v4_cleanup,
)
from tests.ptg2_layout_candidate_postgres_support import (
    assert_candidate_migration_downgrade as _assert_candidate_migration_downgrade,
    assert_migration_candidate_cleanup as _assert_migration_candidate_cleanup,
    create_layout_tables as _create_layout_tables,
    install_legacy_layout_candidate_fixture as _install_legacy_layout_candidate_fixture,
    verify_v4_cleanup_replay as _verify_v4_cleanup_replay,
    seed_v4_duplicate_cleanup_pair as _seed_v4_duplicate_cleanup_pair,
    upgrade_and_assert_live_candidate_adoption as _upgrade_and_assert_live_candidate_adoption,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
    asyncpg,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260810120000_ptg2_layout_build_candidates.py"
)


def _sqlalchemy_dsn() -> str:
    return _dsn().replace("postgresql://", "postgresql+asyncpg://", 1)


class _SessionExecutor:
    def __init__(self, session) -> None:
        self.session = session

    async def all(self, statement, **params):
        result = await self.session.execute(text(statement), params)
        return result.all()

    async def status(self, statement, **params):
        result = await self.session.execute(text(statement), params)
        return result.rowcount


def _install_empty_layout_summary(monkeypatch) -> SharedMappingDigestSummary:
    expected = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=0,
        unique_block_count=0,
        entry_count=0,
        logical_byte_count=0,
        canonical_byte_count=0,
        object_kinds=(),
    )

    async def summary(*_args, **_kwargs):
        return expected

    monkeypatch.setattr(
        ptg2_shared_blocks,
        "summarize_shared_snapshot_mappings",
        summary,
    )
    return expected


async def _reserve_candidate(sessions, schema_name: str, build_token: str):
    async with sessions.begin() as session:
        return await reserve_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=b"f" * 32,
            build_token=build_token,
        )


async def _seal_candidate(
    sessions,
    schema_name: str,
    expected: SharedMappingDigestSummary,
    snapshot_key: int,
    build_token: str,
):
    async with sessions.begin() as session:
        return await seal_shared_layout(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=build_token,
            expected_summary=expected,
            support_digest=b"s" * 32,
            layout_manifest={"contract": "private-candidate-v1"},
        )


async def _reserve_private_candidate_pair(sessions, schema_name: str):
    slow, peer = await asyncio.gather(
        _reserve_candidate(sessions, schema_name, "source-0"),
        _reserve_candidate(sessions, schema_name, "source-1"),
    )
    assert slow.snapshot_key != peer.snapshot_key
    return slow, peer


async def _assert_peer_candidate_seals(
    sessions,
    schema_name: str,
    schema: str,
    expected: SharedMappingDigestSummary,
    slow,
    peer,
) -> None:
    async with sessions.begin() as holder:
        await holder.execute(
            text(
                f"SELECT snapshot_key FROM {schema}.ptg2_layout_build_candidate "
                "WHERE snapshot_key = :snapshot_key FOR UPDATE"
            ),
            {"snapshot_key": slow.snapshot_key},
        )
        peer_sealed = await asyncio.wait_for(
            _seal_candidate(
                sessions,
                schema_name,
                expected,
                peer.snapshot_key,
                "source-1",
            ),
            timeout=2.0,
        )
        assert (peer_sealed.snapshot_key, peer_sealed.reused) == (
            peer.snapshot_key,
            False,
        )


async def _seal_candidate_under_dense_lock(
    sessions,
    schema_name: str,
    schema: str,
    expected: SharedMappingDigestSummary,
    slow,
):
    dense_table = PTG2_V3_DENSE_LAYOUT_TABLES[0]
    async with sessions.begin() as dense_holder:
        await dense_holder.execute(
            text(
                f'LOCK TABLE {schema}."{dense_table}" '
                "IN ACCESS EXCLUSIVE MODE"
            )
        )
        return await asyncio.wait_for(
            _seal_candidate(
                sessions,
                schema_name,
                expected,
                slow.snapshot_key,
                "source-0",
            ),
            timeout=2.0,
        )


async def _assert_pending_candidate_cleanup(
    sessions,
    schema: str,
    slow,
    peer,
) -> None:
    async with sessions() as session:
        assert await session.scalar(
            text(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_layout_fingerprint")
        ) == 1
        cleanup = (
            await session.execute(
                text(
                    f"SELECT snapshot_key, canonical_snapshot_key, "
                    f"cleanup_pending_at IS NOT NULL AS cleanup_pending "
                    f"FROM {schema}.ptg2_layout_build_candidate"
                )
            )
        ).mappings().one()
        assert dict(cleanup) == {
            "snapshot_key": slow.snapshot_key,
            "canonical_snapshot_key": peer.snapshot_key,
            "cleanup_pending": True,
        }


async def _release_and_assert_candidate_cleanup(
    sessions,
    schema_name: str,
    schema: str,
) -> None:
    async with sessions.begin() as session:
        executor = _SessionExecutor(session)
        release_kwargs_by_name = {
            "schema_name": schema_name,
            "building_max_age_seconds": 21_600,
            "grace_seconds": 0,
            "max_layouts": 10,
            "layout_keys": None,
        }
        first_gc = await ptg2_shared_gc._release_layouts_ready(
            executor,
            **release_kwargs_by_name,
        )
        replay_gc = await ptg2_shared_gc._release_layouts_ready(
            executor,
            **release_kwargs_by_name,
        )
    assert first_gc.logical_layout_count == 1
    assert replay_gc.logical_layout_count == 0
    async with sessions() as session:
        assert await session.scalar(
            text(
                f"SELECT COUNT(*) FROM {schema}.ptg2_layout_build_candidate"
            )
        ) == 0


async def _mark_v4_cleanup(
    sessions,
    schema_name: str,
    schema: str,
    loser_key: int,
    winner_key: int,
) -> None:
    dense_table = PTG2_V3_DENSE_LAYOUT_TABLES[0]
    async with sessions.begin() as dense_holder:
        await dense_holder.execute(
            text(f'LOCK TABLE {schema}."{dense_table}" IN ACCESS EXCLUSIVE MODE')
        )
        async with sessions.begin() as marker_session:
            await asyncio.wait_for(
                _defer_duplicate_v4_cleanup(
                    marker_session,
                    schema_name=schema_name,
                    snapshot_key=loser_key,
                    canonical_snapshot_key=winner_key,
                ),
                timeout=2.0,
            )
        async with sessions() as observer:
            cleanup_target = await observer.scalar(
                text(
                    f"SELECT canonical_snapshot_key FROM "
                    f"{schema}.ptg2_layout_build_candidate "
                    "WHERE snapshot_key = :snapshot_key "
                    "AND cleanup_pending_at IS NOT NULL"
                ),
                {"snapshot_key": loser_key},
            )
        assert cleanup_target == winner_key


@pytest.mark.asyncio
async def test_v4_duplicate_cleanup_never_blocks_seal_on_dense_tables() -> None:
    """V4 reuse commits a durable cleanup marker before dense GC can run."""

    schema_name = f"ptg2_v4_cleanup_{uuid.uuid4().hex}"
    schema = _quote(schema_name)
    engine = create_async_engine(_sqlalchemy_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            await _create_layout_tables(connection, schema)
        loser_key, winner_key = await _seed_v4_duplicate_cleanup_pair(
            sessions,
            schema,
            generation=PTG2_V4_SHARED_GENERATION,
        )
        await _mark_v4_cleanup(
            sessions,
            schema_name,
            schema,
            loser_key,
            winner_key,
        )
        await _verify_v4_cleanup_replay(
            sessions,
            schema_name,
            schema,
            loser_key,
            winner_key,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_identical_layout_builds_are_private_until_first_seal(
    monkeypatch,
) -> None:
    """A locked slow candidate cannot delay a peer with the same fingerprint."""

    schema_name = f"ptg2_layout_candidate_{uuid.uuid4().hex}"
    schema = _quote(schema_name)
    engine = create_async_engine(_sqlalchemy_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    expected = _install_empty_layout_summary(monkeypatch)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            await _create_layout_tables(connection, schema)

        slow, peer = await _reserve_private_candidate_pair(
            sessions,
            schema_name,
        )
        await _assert_peer_candidate_seals(
            sessions,
            schema_name,
            schema,
            expected,
            slow,
            peer,
        )
        slow_sealed = await _seal_candidate_under_dense_lock(
            sessions,
            schema_name,
            schema,
            expected,
            slow,
        )
        assert (slow_sealed.snapshot_key, slow_sealed.reused) == (
            peer.snapshot_key,
            True,
        )
        await _assert_pending_candidate_cleanup(sessions, schema, slow, peer)
        await _release_and_assert_candidate_cleanup(
            sessions,
            schema_name,
            schema,
        )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


async def _seal_mixed_version_candidates(
    sessions,
    schema_name: str,
    expected: SharedMappingDigestSummary,
):
    async with sessions.begin() as session:
        new = await reserve_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=bytes.fromhex("41" * 32),
            build_token="new-source",
        )
    assert new.snapshot_key != 41
    seal_kwargs_by_name = {
        "schema_name": schema_name,
        "expected_summary": expected,
        "support_digest": b"s" * 32,
    }
    async with sessions.begin() as session:
        new_sealed = await seal_shared_layout(
            session,
            snapshot_key=new.snapshot_key,
            build_token="new-source",
            layout_manifest={"contract": "mixed-version-new"},
            **seal_kwargs_by_name,
        )
    assert (new_sealed.snapshot_key, new_sealed.reused) == (
        new.snapshot_key,
        False,
    )
    for manifest_contract in (
        "mixed-version-old",
        "mixed-version-old-replay",
    ):
        async with sessions.begin() as session:
            old_sealed = await seal_shared_layout(
                session,
                snapshot_key=41,
                build_token="old-source",
                layout_manifest={"contract": manifest_contract},
                **seal_kwargs_by_name,
            )
        assert (old_sealed.snapshot_key, old_sealed.reused) == (
            new.snapshot_key,
            True,
        )
    return new


@pytest.mark.asyncio
async def test_candidate_migration_adopts_live_building_fingerprint(
    monkeypatch,
) -> None:
    """The migration copies one active legacy reservation without rewriting it."""

    schema_name = f"ptg2_layout_upgrade_{uuid.uuid4().hex}"
    schema = _quote(schema_name)
    connection = await asyncpg.connect(_dsn())
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    try:
        await connection.execute(f"CREATE SCHEMA {schema}")
        await _install_legacy_layout_candidate_fixture(connection, schema)
        await _upgrade_and_assert_live_candidate_adoption(
            connection,
            schema,
            migration,
            statements,
        )
        expected = _install_empty_layout_summary(monkeypatch)
        engine = create_async_engine(_sqlalchemy_dsn())
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            new = await _seal_mixed_version_candidates(
                sessions,
                schema_name,
                expected,
            )
            await _assert_migration_candidate_cleanup(
                connection,
                schema,
                new.snapshot_key,
            )
        finally:
            await engine.dispose()
        await _assert_candidate_migration_downgrade(
            connection,
            schema_name,
            migration,
            statements,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await connection.close()


def test_candidate_migration_is_the_only_head_and_contains_live_adoption(
    monkeypatch,
) -> None:
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg2_layout_candidate_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    sql = "\n".join(statements)
    assert migration.down_revision == "20260810110000_ptg_wave_receipt_authority"
    assert "ptg2_layout_build_candidate" in sql
    assert "layout.state = 'building'" in sql
    assert "COUNT(fingerprint.semantic_fingerprint) <> 1" in sql
    assert "ptg2_v3_layout_fingerprint" in sql
    assert "cleanup_pending_at" in sql
    assert "cleanup_pending_idx" in sql
    assert "ptg2_capture_building_layout_fingerprint" in sql
    assert "AFTER INSERT OR UPDATE" in sql
