"""Disposable PostgreSQL fixture for exact snapshot rollback."""

from __future__ import annotations

import datetime
import json

from db.connection import db
from process.ptg_parts.source_pointers import _plan_pointer_entry


SCHEMA_NAME = "mrf"
SOURCE_KEY = "rollback-source"
PLAN_ID = "synthetic-plan"
TARGET_SNAPSHOT_ID = "rollback-snapshot-a"
CURRENT_SNAPSHOT_ID = "rollback-snapshot-b"
TARGET_OWNER_ID = "rollback-reference-a"
CURRENT_OWNER_ID = "rollback-reference-b"
IMPORT_MONTH = datetime.date(2026, 7, 1)
SNAPSHOT_KEY = 9_001


def _serving_manifest(snapshot_id: str) -> str:
    return json.dumps(
        {
            "snapshot_id": snapshot_id,
            "serving_index": {
                "source_key": SOURCE_KEY,
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            },
        },
        sort_keys=True,
    )


async def _insert_snapshot(
    snapshot_id: str,
    previous_snapshot_id: str | None,
) -> None:
    timestamp = datetime.datetime(2026, 7, 23, 12, 0)
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_snapshot
            (snapshot_id, import_run_id, import_month, status, created_at,
             validated_at, published_at, previous_snapshot_id, manifest)
        VALUES
            (:snapshot_id, :import_run_id, :import_month, 'published',
             :created_at, :created_at, :created_at, :previous_snapshot_id,
             CAST(:manifest AS json))
        """,
        snapshot_id=snapshot_id,
        import_run_id=f"run-{snapshot_id}",
        import_month=IMPORT_MONTH,
        created_at=timestamp,
        previous_snapshot_id=previous_snapshot_id,
        manifest=_serving_manifest(snapshot_id),
    )


async def insert_snapshot_scope(snapshot_id: str) -> None:
    """Insert the resolver scope, logical mapping, and activated audit row."""

    await _insert_primary_snapshot_scope(snapshot_id)
    await _insert_plan_scope(snapshot_id)
    await _insert_activated_attestation(snapshot_id)


async def _insert_primary_snapshot_scope(snapshot_id: str) -> None:
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_v3_snapshot_scope
            (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
        VALUES (:snapshot_id, :plan_id, 'group', :coverage_scope_id)
        """,
        snapshot_id=snapshot_id,
        plan_id=PLAN_ID,
        coverage_scope_id=b"c" * 32,
    )


async def _insert_plan_scope(snapshot_id: str) -> None:
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_v3_snapshot_plan_scope
            (snapshot_id, plan_id, plan_market_type)
        VALUES (:snapshot_id, :plan_id, 'group')
        """,
        snapshot_id=snapshot_id,
        plan_id=PLAN_ID,
    )


async def _insert_activated_attestation(snapshot_id: str) -> None:
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_v3_candidate_audit_attestation
            (snapshot_id, snapshot_key, source_key, plan_id, plan_market_type,
             coverage_scope_id, source_set_digest, audit_sample_digest,
             contract, tool_name, tool_version, report_digest, report,
             attested_at, expires_at, activated_at)
        VALUES
            (:snapshot_id, :snapshot_key, :source_key, :plan_id, 'group',
             :coverage_scope_id, :source_set_digest, :audit_sample_digest,
             'ptg2_v3_release_audit_attestation_v3', 'synthetic-audit', '1',
             :report_digest, '{{}}'::jsonb, :attested_at, :expires_at,
             :attested_at)
        """,
        snapshot_id=snapshot_id,
        snapshot_key=SNAPSHOT_KEY,
        source_key=SOURCE_KEY,
        plan_id=PLAN_ID,
        coverage_scope_id=b"c" * 32,
        source_set_digest=b"d" * 32,
        audit_sample_digest=b"a" * 32,
        report_digest=b"r" * 32,
        attested_at=datetime.datetime(
            2026,
            7,
            23,
            12,
            0,
            tzinfo=datetime.timezone.utc,
        ),
        expires_at=datetime.datetime(
            2026,
            8,
            23,
            12,
            0,
            tzinfo=datetime.timezone.utc,
        ),
    )


async def _seed_rollback_pins() -> None:
    for owner_id, snapshot_id in (
        (TARGET_OWNER_ID, TARGET_SNAPSHOT_ID),
        (CURRENT_OWNER_ID, CURRENT_SNAPSHOT_ID),
    ):
        await db.status(
            f"""
            INSERT INTO "{SCHEMA_NAME}".ptg2_snapshot_pin
                (owner_type, owner_id, snapshot_id, reason)
            VALUES ('ptg_v4_rollback', :owner_id, :snapshot_id,
                    'retained exact rollback reference')
            """,
            owner_id=owner_id,
            snapshot_id=snapshot_id,
        )


async def _seed_source_pointer(timestamp: datetime.datetime) -> None:
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_current_source_snapshot
            (source_key, snapshot_id, previous_snapshot_id, import_month,
             updated_at)
        VALUES (:source_key, :snapshot_id, :previous_snapshot_id,
                :import_month, :updated_at)
        """,
        source_key=SOURCE_KEY,
        snapshot_id=CURRENT_SNAPSHOT_ID,
        previous_snapshot_id=TARGET_SNAPSHOT_ID,
        import_month=IMPORT_MONTH,
        updated_at=timestamp,
    )


async def _seed_plan_pointer(timestamp: datetime.datetime) -> None:
    plan_pointer = _plan_pointer_entry(
        plan_id=PLAN_ID,
        plan_market_type="group",
        import_month=IMPORT_MONTH,
        source_key=SOURCE_KEY,
        snapshot_id=CURRENT_SNAPSHOT_ID,
        previous_snapshot_id=TARGET_SNAPSHOT_ID,
        updated_at=timestamp,
    )
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_current_plan_source
            (plan_source_key, plan_id, plan_market_type, import_month,
             source_key, snapshot_id, previous_snapshot_id, updated_at)
        VALUES
            (:plan_source_key, :plan_id, :plan_market_type, :import_month,
             :source_key, :snapshot_id, :previous_snapshot_id, :updated_at)
        """,
        **plan_pointer,
    )


async def _seed_global_pointer(timestamp: datetime.datetime) -> None:
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_current_snapshot
            (slot, snapshot_id, previous_snapshot_id, updated_at)
        VALUES ('current', :snapshot_id, :previous_snapshot_id, :updated_at)
        """,
        snapshot_id=CURRENT_SNAPSHOT_ID,
        previous_snapshot_id=TARGET_SNAPSHOT_ID,
        updated_at=timestamp,
    )


async def seed_rollback_pair() -> None:
    """Insert two complete published snapshots and their forward pointers."""

    await _insert_snapshot(TARGET_SNAPSHOT_ID, None)
    await _insert_snapshot(CURRENT_SNAPSHOT_ID, TARGET_SNAPSHOT_ID)
    await db.status(
        f"""
        INSERT INTO "{SCHEMA_NAME}".ptg2_v3_snapshot_layout
            (snapshot_key, build_token, generation, state, mapping_digest,
             support_digest, logical_byte_count, published_at)
        VALUES
            (:snapshot_key, 'rollback-layout', 'shared_blocks_v3', 'sealed',
             :mapping_digest, :support_digest, 1, now())
        """,
        snapshot_key=SNAPSHOT_KEY,
        mapping_digest=b"m" * 32,
        support_digest=b"s" * 32,
    )
    for snapshot_id in (TARGET_SNAPSHOT_ID, CURRENT_SNAPSHOT_ID):
        await db.status(
            f"""
            INSERT INTO "{SCHEMA_NAME}".ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key)
            VALUES (:snapshot_id, :snapshot_key)
            """,
            snapshot_id=snapshot_id,
            snapshot_key=SNAPSHOT_KEY,
        )
        await insert_snapshot_scope(snapshot_id)
    await _seed_rollback_pins()
    timestamp = datetime.datetime(2026, 7, 23, 12, 5)
    await _seed_source_pointer(timestamp)
    await _seed_plan_pointer(timestamp)
    await _seed_global_pointer(timestamp)
