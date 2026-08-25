"""Real-PostgreSQL isolation and recovery proof for packed finalizer stages."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import asyncpg
import pytest

from api.ptg2_shared_blocks import fetch_shared_blocks
from db.connection import db
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_v4_finalizer_publish as finalizer_publish
from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_SHARED_GENERATION
from scripts.research import ptg2_packed_finalizer_abba_artifacts as artifact_factory
from scripts.research.ptg2_packed_finalizer_abba_contract import BenchmarkArtifacts
from scripts.research.ptg2_packed_finalizer_abba_lifecycle import (
    ArmRequest,
    inspect_arm_state,
    is_arm_schema_removed,
    prepare_arm_schema,
    run_packed_failure_probe,
    run_production_arm,
)
from tests.ptg2_v4_stale_metadata_postgres_support import postgres_dsn
from tests.test_ptg2_packed_finalizer_wrapper_postgres import (
    _configure_test_database,
    _tiny_shape,
)


_TARGET_DIGEST_DOMAIN = b"PTG2V4FINALIZERTARGETS\x01"


@dataclass(frozen=True)
class _IsolationFixture:
    dsn: str
    schema_name: str
    snapshot_key: int
    owner_token: str
    stale_token: str
    owner_work: Path
    stale_work: Path
    owner_artifacts: BenchmarkArtifacts
    stale_artifacts: BenchmarkArtifacts

    def request(self, *, stale: bool = False) -> ArmRequest:
        return ArmRequest(
            "b2" if stale else "b1",
            True,
            self.schema_name,
            self.snapshot_key,
            self.stale_token if stale else self.owner_token,
            self.stale_work if stale else self.owner_work,
            self.stale_artifacts if stale else self.owner_artifacts,
        )


def _different_artifacts(directory: Path) -> BenchmarkArtifacts:
    target_payload = artifact_factory._target_payload

    def stale_payload(object_kind: str, target_number: int) -> bytes:
        return hashlib.sha256(
            b"stale-attempt\x00" + target_payload(object_kind, target_number)
        ).digest()

    with patch.object(artifact_factory, "_target_payload", stale_payload):
        return artifact_factory.generate_artifacts(directory, _tiny_shape())


async def _seed_prior_pointer(dsn: str, schema_name: str) -> None:
    schema = _quote_ident(schema_name)
    connection = await asyncpg.connect(dsn)
    try:
        prior_key = await connection.fetchval(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (build_token, generation, state, mapping_digest, support_digest)
            VALUES ('prior-token', $1, 'sealed', $2, $3)
            RETURNING snapshot_key
            """,
            PTG2_V4_SHARED_GENERATION,
            b"p" * 32,
            b"q" * 32,
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id, status) "
            "VALUES ('prior-snapshot', 'published')"
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_binding "
            "(snapshot_id, snapshot_key) VALUES ('prior-snapshot', $1)",
            prior_key,
        )
        await connection.execute(
            f"INSERT INTO {schema}.ptg2_current_snapshot "
            "(slot, snapshot_id) VALUES ('active', 'prior-snapshot')"
        )
    finally:
        await connection.close()


@asynccontextmanager
async def _isolation_fixture(monkeypatch, tmp_path):
    dsn = postgres_dsn()
    _configure_test_database(monkeypatch, dsn)
    token = uuid.uuid4().hex[:12]
    schema_name = f"ptg_packed_abba_{token}_b1"
    owner_work, stale_work = tmp_path / "owner-work", tmp_path / "stale-work"
    owner_work.mkdir()
    stale_work.mkdir()
    owner_artifacts = artifact_factory.generate_artifacts(
        tmp_path / "owner-artifacts", _tiny_shape()
    )
    stale_artifacts = _different_artifacts(tmp_path / "stale-artifacts")
    try:
        await db.disconnect()
        await db.connect()
        snapshot_key = await prepare_arm_schema(
            dsn,
            schema_name=schema_name,
            build_token=f"owner-{token}",
            shape_sha256=owner_artifacts.shape.sha256(),
        )
        await _seed_prior_pointer(dsn, schema_name)
        yield _IsolationFixture(
            dsn,
            schema_name,
            snapshot_key,
            f"owner-{token}",
            f"stale-{token}",
            owner_work,
            stale_work,
            owner_artifacts,
            stale_artifacts,
        )
    finally:
        is_schema_removed = True
        if db.engine is not None:
            is_schema_removed = await is_arm_schema_removed(schema_name)
        await db.disconnect()
        owner_artifacts.cleanup()
        stale_artifacts.cleanup()
        assert is_schema_removed
        assert not any(owner_work.iterdir())
        assert not any(stale_work.iterdir())
        owner_work.rmdir()
        stale_work.rmdir()


async def _stage_fingerprint(
    connection: asyncpg.Connection,
    schema_name: str,
    table_name: str,
    hash_column: str,
) -> tuple[int, int, str]:
    table = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    column = _quote_ident(hash_column)
    record = await connection.fetchrow(
        f"""
        SELECT to_regclass($1)::oid::bigint AS table_oid,
               COUNT(*)::bigint AS row_count,
               md5(string_agg(encode({column}, 'hex'), ''
                   ORDER BY encode({column}, 'hex'))) AS content_digest
          FROM {table}
        """,
        f"{schema_name}.{table_name}",
    )
    return int(record["table_oid"]), int(record["row_count"]), record["content_digest"]


async def _stage_pair(
    connection: asyncpg.Connection,
    fixture: _IsolationFixture,
    build_token: str,
) -> tuple[tuple[int, int, str], tuple[int, int, str]]:
    stage = snapshot_publish._finalizer_block_stage_name(
        fixture.snapshot_key, build_token
    )
    return (
        await _stage_fingerprint(connection, fixture.schema_name, stage, "block_hash"),
        await _stage_fingerprint(
            connection,
            fixture.schema_name,
            finalizer_publish._pack_stage_name(stage),
            "map_block_hash",
        ),
    )


async def _assert_stale_attempt_absent(
    connection: asyncpg.Connection,
    fixture: _IsolationFixture,
) -> None:
    stage = snapshot_publish._finalizer_block_stage_name(
        fixture.snapshot_key, fixture.stale_token
    )
    assert await connection.fetchrow(
        "SELECT to_regclass($1), to_regclass($2)",
        f"{fixture.schema_name}.{stage}",
        f"{fixture.schema_name}.{finalizer_publish._pack_stage_name(stage)}",
    ) == (None, None)
    schema = _quote_ident(fixture.schema_name)
    counts = await connection.fetchrow(
        f"""
        SELECT (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_root),
               (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_pack),
               (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_target),
               (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block),
               (SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin),
               (SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate),
               (SELECT COUNT(*) FROM {schema}.ptg2_v3_block)
        """
    )
    assert tuple(counts) == (0, 0, 0, 2, 0, 0, 2)


async def _assert_persisted_owner(fixture: _IsolationFixture, arm) -> None:
    connection = await asyncpg.connect(fixture.dsn)
    schema = _quote_ident(fixture.schema_name)
    try:
        root = await connection.fetchrow(
            f"SELECT map_digest, canonical_mapping_digest, target_identity_digest "
            f"FROM {schema}.ptg2_v4_finalizer_map_root "
            "WHERE snapshot_key = $1",
            fixture.snapshot_key,
        )
        target_hashes = await connection.fetch(
            f"SELECT block_hash FROM {schema}.ptg2_v4_finalizer_map_target "
            "WHERE snapshot_key = $1 ORDER BY block_hash",
            fixture.snapshot_key,
        )
        target_digest = hashlib.sha256(_TARGET_DIGEST_DOMAIN)
        for target_record in target_hashes:
            target_digest.update(bytes(target_record["block_hash"]))
        publication = arm["finalizer_publication"]
        assert bytes(root["map_digest"]).hex() == publication["map_digest"]
        assert bytes(root["canonical_mapping_digest"]).hex() == (
            fixture.owner_artifacts.expected_summary["packed_mapping_digest"]
        )
        assert bytes(root["target_identity_digest"]) == target_digest.digest()
    finally:
        await connection.close()
    assert arm["summary"]["mapping_digest"] == (
        fixture.owner_artifacts.expected_summary["mapping_digest"]
    )


async def _assert_api_reads_all_packed_kinds(fixture: _IsolationFixture) -> None:
    async with db.transaction() as session:
        for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS:
            blocks = await fetch_shared_blocks(
                session,
                schema_name=fixture.schema_name,
                snapshot_key=fixture.snapshot_key,
                object_kind=object_kind,
                block_keys=(0,),
                require_all=True,
            )
            assert tuple(blocks) == (0,)
            assert len(blocks[0]) == 1


async def _attach_finalizer_manifest(fixture: _IsolationFixture, arm) -> None:
    publication = arm["finalizer_publication"]
    manifest_by_field = finalizer_publish.V4FinalizerMapPublication(
        object_kinds=tuple(publication["object_kinds"]),
        mapping_count=publication["mapping_count"],
        unique_block_count=publication["unique_block_count"],
        entry_count=publication["entry_count"],
        logical_byte_count=publication["logical_byte_count"],
        stored_byte_count=publication["stored_byte_count"],
        map_pack_count=publication["map_pack_count"],
        stored_map_byte_count=publication["stored_map_byte_count"],
        map_digest=bytes.fromhex(publication["map_digest"]),
        canonical_mapping_digest=bytes.fromhex(
            publication["canonical_mapping_digest"]
        ),
        canonical_byte_count=publication["canonical_byte_count"],
        target_identity_digest=bytes.fromhex(
            publication["target_identity_digest"]
        ),
        contract=publication["contract"],
    ).manifest()
    schema = _quote_ident(fixture.schema_name)
    await db.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'sealed',
               layout_manifest = jsonb_set(
               layout_manifest,
               '{{serving_index}}',
               COALESCE(layout_manifest->'serving_index', '{{}}'::jsonb)
                 || jsonb_build_object('finalizer_mapping', CAST(:manifest AS jsonb))
           )
         WHERE snapshot_key = :snapshot_key
        """,
        manifest=json.dumps(manifest_by_field, sort_keys=True),
        snapshot_key=fixture.snapshot_key,
    )


async def _assert_recovery_preserves_prior_pointer(fixture: _IsolationFixture) -> None:
    stats = await shared_gc.abandon_owned_v4_layout(
        schema_name=fixture.schema_name,
        snapshot_key=fixture.snapshot_key,
        build_token=fixture.owner_token,
        grace_seconds=60,
        options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=4),
    )
    assert stats.logical_layout_count == 1
    assert stats.candidate_hash_count == 14
    assert await inspect_arm_state(fixture.request()) == {
        "root_rows": 0,
        "pack_rows": 0,
        "target_rows": 0,
        "relational_rows": 0,
        "pin_rows": 0,
        "gc_rows": 14,
        "cas_rows": 14,
        "stage_tables_present": 0,
    }
    schema = _quote_ident(fixture.schema_name)
    pointer = await db.first(
        f"SELECT current.snapshot_id, binding.snapshot_key "
        f"FROM {schema}.ptg2_current_snapshot AS current "
        f"JOIN {schema}.ptg2_v3_snapshot_binding AS binding "
        "ON binding.snapshot_id = current.snapshot_id WHERE current.slot = 'active'"
    )
    assert pointer[0] == "prior-snapshot"
    assert int(pointer[1]) != fixture.snapshot_key


@pytest.mark.asyncio
async def test_stale_attempt_cannot_replace_owner_stage_or_receipts(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove stale equal-shape data cannot substitute for the owner attempt."""

    async with _isolation_fixture(monkeypatch, tmp_path) as fixture:
        owner_staged, release_owner = asyncio.Event(), asyncio.Event()
        real_publish = finalizer_publish._publish_atomic_map

        async def pause_owner(publication, **kwargs):
            if kwargs["build_token"] == fixture.owner_token:
                owner_staged.set()
                await release_owner.wait()
            return await real_publish(publication, **kwargs)

        monkeypatch.setattr(finalizer_publish, "_publish_atomic_map", pause_owner)
        owner_task = asyncio.create_task(run_production_arm(fixture.request()))
        try:
            await asyncio.wait_for(owner_staged.wait(), timeout=10)
            connection = await asyncpg.connect(fixture.dsn)
            try:
                owner_stage = await _stage_pair(
                    connection, fixture, fixture.owner_token
                )
                with pytest.raises(RuntimeError, match="lost build ownership"):
                    await run_packed_failure_probe(fixture.request(stale=True), None)
                assert await _stage_pair(
                    connection, fixture, fixture.owner_token
                ) == owner_stage
                await _assert_stale_attempt_absent(connection, fixture)
            finally:
                await connection.close()
            release_owner.set()
            arm = await asyncio.wait_for(owner_task, timeout=20)
        finally:
            release_owner.set()
            if not owner_task.done():
                owner_task.cancel()
                await asyncio.gather(owner_task, return_exceptions=True)
        await _assert_persisted_owner(fixture, arm)
        await _attach_finalizer_manifest(fixture, arm)
        await _assert_api_reads_all_packed_kinds(fixture)


@pytest.mark.asyncio
async def test_committed_root_recovery_preserves_prior_pointer(
    monkeypatch,
    tmp_path,
) -> None:
    """Recover one post-finalizer, pre-seal failure without pointer drift."""

    async with _isolation_fixture(monkeypatch, tmp_path) as fixture:
        arm = await run_production_arm(fixture.request())
        await _assert_persisted_owner(fixture, arm)
        await _assert_recovery_preserves_prior_pointer(fixture)


@pytest.mark.asyncio
async def test_stage_guard_serializes_and_releases_after_cancel(
    monkeypatch,
) -> None:
    """Prove one canceled waiter cannot leak or bypass the attempt guard."""

    dsn = postgres_dsn()
    _configure_test_database(monkeypatch, dsn)
    first_entered, release_first = asyncio.Event(), asyncio.Event()

    async def guarded(entry_event: asyncio.Event, hold_event=None):
        async with snapshot_publish._finalizer_block_stage_guard(
            schema_name="mrf", snapshot_key=17, build_token="same-token"
        ):
            entry_event.set()
            if hold_event is not None:
                await hold_event.wait()

    await db.disconnect()
    await db.connect()
    first = asyncio.create_task(guarded(first_entered, release_first))
    await asyncio.wait_for(first_entered.wait(), timeout=5)
    cancelled_entered = asyncio.Event()
    cancelled = asyncio.create_task(guarded(cancelled_entered))
    await asyncio.sleep(0.05)
    assert not cancelled_entered.is_set()
    cancelled.cancel()
    await asyncio.gather(cancelled, return_exceptions=True)
    successor_entered = asyncio.Event()
    successor = asyncio.create_task(guarded(successor_entered))
    await asyncio.sleep(0.05)
    assert not successor_entered.is_set()
    release_first.set()
    await asyncio.wait_for(asyncio.gather(first, successor), timeout=5)
    assert successor_entered.is_set()
    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction():
            assert await connection.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                "ptg2-v4-finalizer-stage:mrf:17:same-token",
            )
    finally:
        await connection.close()
        await db.disconnect()


@pytest.mark.asyncio
async def test_repeated_cancel_drains_real_finalizer_stage_cleanup(
    monkeypatch,
    tmp_path,
) -> None:
    """A second cancellation cannot strand the exact PostgreSQL stage."""
    async with _isolation_fixture(monkeypatch, tmp_path) as fixture:
        publication_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        cleanup_release = asyncio.Event()
        real_drop = snapshot_publish._drop_finalizer_block_stage
        async def pause_publication(*_args, **_kwargs):
            publication_started.set()
            await asyncio.Event().wait()
        async def delayed_drop(request):
            cleanup_started.set()
            await cleanup_release.wait()
            await real_drop(request)

        monkeypatch.setattr(
            finalizer_publish,
            "publish_v4_finalizer_maps",
            pause_publication,
        )
        monkeypatch.setattr(
            snapshot_publish,
            "_drop_finalizer_block_stage",
            delayed_drop,
        )
        task = asyncio.create_task(
            run_packed_failure_probe(fixture.request(), None)
        )

        await asyncio.wait_for(publication_started.wait(), timeout=10)
        task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=10)
        task.cancel()
        cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)

        stage = snapshot_publish._finalizer_block_stage_name(
            fixture.snapshot_key,
            fixture.owner_token,
        )
        connection = await asyncpg.connect(fixture.dsn)
        try:
            assert await connection.fetchrow(
                "SELECT to_regclass($1), to_regclass($2)",
                f"{fixture.schema_name}.{stage}",
                f"{fixture.schema_name}.{finalizer_publish._pack_stage_name(stage)}",
            ) == (None, None)
        finally:
            await connection.close()
