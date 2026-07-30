# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for reviewed PTG activation replay and rollback pins."""

from __future__ import annotations

import datetime
import os
import uuid

import pytest

from db.connection import db
from process.ptg_parts import source_pointer_reviewed_activation as reviewed
from tests.ptg2_attestation_compat_test_support import quoted_identifier


def _require_postgres() -> None:
    if os.getenv("HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_ATTESTATION_COMPAT_POSTGRES_TEST=1 for the "
            "isolated PostgreSQL test"
        )


async def _create_cutover_tables(schema: str) -> None:
    statements = (
        f"""CREATE TABLE {schema}.ptg2_snapshot (
                snapshot_id text PRIMARY KEY,
                status text NOT NULL,
                published_at timestamptz,
                previous_snapshot_id text,
                manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
                snapshot_id text PRIMARY KEY,
                snapshot_key bigint NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                snapshot_key bigint PRIMARY KEY,
                generation text NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
                snapshot_id text PRIMARY KEY
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_candidate_audit_attestation (
                snapshot_id text PRIMARY KEY,
                activation_intent text NOT NULL,
                attestation_digest bytea NOT NULL,
                activated_at timestamptz
            )""",
        f"""CREATE TABLE {schema}.ptg2_current_source_snapshot (
                source_key text PRIMARY KEY,
                snapshot_id text NOT NULL,
                previous_snapshot_id text
            )""",
        f"""CREATE TABLE {schema}.ptg2_current_plan_source (
                plan_id text NOT NULL,
                source_key text NOT NULL,
                snapshot_id text NOT NULL,
                previous_snapshot_id text
            )""",
        f"""CREATE TABLE {schema}.ptg2_snapshot_pin (
                owner_type text NOT NULL,
                owner_id text NOT NULL,
                snapshot_id text NOT NULL,
                reason text NOT NULL,
                created_at timestamptz NOT NULL,
                PRIMARY KEY (owner_type, owner_id, snapshot_id)
            )""",
    )
    for statement in statements:
        await db.execute_ddl(statement)


async def _seed_reviewed_cutover(schema: str, digest: bytes) -> None:
    await db.status(
        f"""INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, status, published_at, previous_snapshot_id, manifest)
            VALUES
                ('snap-old', 'published', now() - interval '1 day', NULL, '{{}}'),
                (
                    'snap-new', 'published', now(), 'snap-old',
                    '{{"activation": {{
                        "state": "activated",
                        "mode": "reviewed_audit_only_control",
                        "source_key": "source-a"
                    }}}}'
                )"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key)
            VALUES ('snap-new', 17)"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, generation)
            VALUES (17, 'shared_blocks_v4')"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_scope (snapshot_id)
            VALUES ('snap-new')"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_candidate_audit_attestation
                (
                    snapshot_id, activation_intent,
                    attestation_digest, activated_at
                )
            VALUES ('snap-new', 'audit_only', :digest, now())""",
        digest=digest,
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_current_source_snapshot
                (source_key, snapshot_id, previous_snapshot_id)
            VALUES ('source-a', 'snap-new', 'snap-old')"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_current_plan_source
                (plan_id, source_key, snapshot_id, previous_snapshot_id)
            VALUES ('plan-a', 'source-a', 'snap-new', 'snap-old')"""
    )


async def _pin_and_replay(schema_name: str, digest: bytes) -> dict[str, object]:
    async with db.transaction() as session:
        await reviewed.pin_reviewed_activation_predecessor(
            session,
            schema_name=schema_name,
            activation_by_field={"previous_snapshot_id": "snap-old"},
            activated_at=datetime.datetime.now(datetime.timezone.utc),
            rollback_owner_id="reviewed-op",
            is_reviewed_audit_only=True,
        )
    async with db.transaction() as session:
        return await reviewed.completed_reviewed_activation(
            session,
            schema_name=schema_name,
            source_key="source-a",
            snapshot_id="snap-new",
            expected_current_snapshot_id="snap-old",
            expected_audit_only_attestation_digest=digest,
            rollback_owner_id="reviewed-op",
        )


@pytest.mark.asyncio
async def test_postgres_reviewed_cutover_replay_requires_exact_route_and_pin():
    """Replay succeeds only while the exact pointer and rollback pin survive."""

    _require_postgres()
    schema_name = f"ptg_reviewed_cutover_{uuid.uuid4().hex[:16]}"
    schema = quoted_identifier(schema_name)
    digest = b"a" * 32
    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {schema}")
        await _create_cutover_tables(schema)
        await _seed_reviewed_cutover(schema, digest)
        replay = await _pin_and_replay(schema_name, digest)
        assert replay["status"] == "already_promoted"
        assert replay["storage_generation"] == "shared_blocks_v4"
        assert replay["rollback_owner_id"] == "reviewed-op"

        await db.status(
            f"""UPDATE {schema}.ptg2_current_source_snapshot
                   SET snapshot_id = 'snap-old',
                       previous_snapshot_id = NULL
                 WHERE source_key = 'source-a'"""
        )
        async with db.transaction() as session:
            with pytest.raises(
                reviewed.PTG2SourcePointerConflict,
                match="does not match the exact reviewed activation",
            ):
                await reviewed.completed_reviewed_activation(
                    session,
                    schema_name=schema_name,
                    source_key="source-a",
                    snapshot_id="snap-new",
                    expected_current_snapshot_id="snap-old",
                    expected_audit_only_attestation_digest=digest,
                    rollback_owner_id="reviewed-op",
                )
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await db.disconnect()
