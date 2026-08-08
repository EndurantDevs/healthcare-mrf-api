"""PostgreSQL proof for atomic V4 retirement and permanent fencing."""

from __future__ import annotations

import json

import pytest

from tests.ptg_wave_supersession_fixtures import admission_rollback_proof
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _evidence,
    _insert_successor,
    _insert_supersession,
    _install_migration,
    _quote,
    asyncpg,
)


def _v4_cohort(successor_wave_id: str) -> tuple[dict, dict, bytes]:
    supersession, supersession_canonical = _evidence(successor_wave_id)
    rollback = admission_rollback_proof(
        successor_wave_id=successor_wave_id,
        intent_count=17,
    )
    cohort_map = {
        "schema_version": "healthporta.ptg-import-wave-attestation.v4",
        "wave_id": successor_wave_id,
        "supersession": supersession,
        "admission_rollback_supersession": rollback,
    }
    return cohort_map, rollback, supersession_canonical


async def _insert_admission_rollback(
    connection,
    schema: str,
    proof: dict[str, object],
) -> None:
    predecessor_map = proof["predecessor"]
    unsigned_proof_map = {
        name: proof_field_value
        for name, proof_field_value in proof.items()
        if name != "proof_digest"
    }
    canonical = json.dumps(
        unsigned_proof_map,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave_admission_rollback (
            predecessor_wave_id, predecessor_idempotency_key,
            predecessor_request_digest, predecessor_wave_digest,
            predecessor_release_queue, predecessor_intent_count,
            successor_wave_id, recovery_basis, recovery_evidence,
            recovery_evidence_canonical, recovery_evidence_sha256, created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'admission_rollback_absent',
            $8::jsonb, $9, $10, clock_timestamp()
        )
        """,
        predecessor_map["wave_id"],
        predecessor_map["idempotency_key"],
        predecessor_map["request_digest"],
        predecessor_map["wave_digest"],
        predecessor_map["release_queue"],
        predecessor_map["intent_count"],
        proof["successor_wave_id"],
        json.dumps(proof),
        canonical,
        proof["proof_digest"],
    )


async def _assert_incomplete_pair_rolls_back(
    connection,
    schema: str,
    *,
    include_supersession: bool,
) -> None:
    suffix = "v7-only" if include_supersession else "v9-only"
    successor_wave_id = f"v4-{suffix}"
    cohort_map, rollback, supersession_canonical = _v4_cohort(
        successor_wave_id
    )
    with pytest.raises(asyncpg.PostgresError, match="DUAL_RETIREMENT"):
        async with connection.transaction():
            if include_supersession:
                await _insert_supersession(
                    connection,
                    schema,
                    successor_wave_id,
                    cohort_map["supersession"],
                    supersession_canonical,
                )
            else:
                await _insert_admission_rollback(
                    connection,
                    schema,
                    rollback,
                )
            await _insert_successor(
                connection,
                schema,
                successor_wave_id,
                "admitted",
                cohort_map,
            )
            await connection.execute(
                f"SET LOCAL search_path TO {_quote(schema)}"
            )
            await connection.execute(
                'SET CONSTRAINTS '
                '"ptg_wave_v4_dual_retirement_binding_guard" IMMEDIATE'
            )
    quoted = _quote(schema)
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted}.ptg_import_wave "
        "WHERE wave_id = $1",
        successor_wave_id,
    ) == 0
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession "
        "WHERE successor_wave_id = $1",
        successor_wave_id,
    ) == 0
    assert await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{quoted}.ptg_import_wave_admission_rollback "
        "WHERE successor_wave_id = $1",
        successor_wave_id,
    ) == 0


async def _assert_retired_wave_insert_rejected(
    connection,
    quoted: str,
    predecessor: dict,
) -> None:
    with pytest.raises(asyncpg.PostgresError, match="ADMISSION_RETIRED"):
        await connection.execute(
            f"""
            INSERT INTO {quoted}.ptg_import_wave (
                wave_id, idempotency_key, request_digest, state,
                intent_count, wave_digest, manifest_digest, jobs_digest,
                release_queue, queue, worker_class, resource_class,
                worker_limit, cohort_attestation
            ) VALUES (
                $1, $2, $3, 'admitted', $4, $5, $6, $6,
                $7, 'arq:PTGSmall', 'process.PTGSmall', 'small', 12,
                '{{}}'::jsonb
            )
            """,
            predecessor["wave_id"],
            predecessor["idempotency_key"],
            predecessor["request_digest"],
            predecessor["intent_count"],
            predecessor["wave_digest"],
            "d" * 64,
            predecessor["release_queue"],
        )


async def _assert_retired_run_tags_rejected(
    connection,
    quoted: str,
    predecessor: dict,
) -> None:
    retired_tags = (
        (
            "retired-metrics-wave",
            {"_wave_id": "unrelated-wave"},
            {"wave_id": predecessor["wave_id"]},
        ),
        (
            "retired-metrics-digest",
            {"_wave_digest": "0" * 64},
            {"wave_digest": predecessor["wave_digest"]},
        ),
    )
    for run_id, params, metrics in retired_tags:
        with pytest.raises(asyncpg.PostgresError, match="ADMISSION_RETIRED"):
            await connection.execute(
                f"INSERT INTO {quoted}.import_run "
                "(run_id, importer, status, params, metrics) "
                "VALUES ($1, 'ptg', 'queued', $2::jsonb, $3::jsonb)",
                run_id,
                json.dumps(params),
                json.dumps(metrics),
            )

    await connection.execute(
        f"INSERT INTO {quoted}.import_run "
        "(run_id, importer, status, params, metrics) "
        "VALUES ('retired-update', 'ptg', 'queued', "
        "'{\"_wave_id\":\"unrelated-wave\"}'::jsonb, '{}'::jsonb)"
    )
    with pytest.raises(asyncpg.PostgresError, match="ADMISSION_RETIRED"):
        await connection.execute(
            f"UPDATE {quoted}.import_run SET metrics = $1::jsonb "
            "WHERE run_id = 'retired-update'",
            json.dumps({"wave_id": predecessor["wave_id"]}),
        )
    stored_metrics = await connection.fetchval(
        f"SELECT metrics FROM {quoted}.import_run "
        "WHERE run_id = 'retired-update'"
    )
    assert json.loads(stored_metrics) == {}


async def _persist_complete_pair(connection, schema: str) -> dict:
    successor_wave_id = "v4-successor"
    cohort_map, rollback, supersession_canonical = _v4_cohort(
        successor_wave_id
    )
    async with connection.transaction():
        await _insert_supersession(
            connection,
            schema,
            successor_wave_id,
            cohort_map["supersession"],
            supersession_canonical,
        )
        await _insert_admission_rollback(connection, schema, rollback)
        await _insert_successor(
            connection,
            schema,
            successor_wave_id,
            "admitted",
            cohort_map,
        )
    quoted = _quote(schema)
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession "
        "WHERE successor_wave_id = $1",
        successor_wave_id,
    ) == 1
    assert await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{quoted}.ptg_import_wave_admission_rollback "
        "WHERE successor_wave_id = $1",
        successor_wave_id,
    ) == 1
    return rollback


@pytest.mark.asyncio
async def test_v4_requires_both_retirement_records(monkeypatch):
    dsn = _dsn()
    schema = "wave_recovery_dual_rollback"
    quoted = _quote(schema)
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        await _assert_incomplete_pair_rolls_back(
            connection,
            schema,
            include_supersession=True,
        )
        await _assert_incomplete_pair_rolls_back(
            connection,
            schema,
            include_supersession=False,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_v4_complete_pair_is_permanently_fenced(monkeypatch):
    dsn = _dsn()
    schema = "wave_recovery_admission_rollback"
    quoted = _quote(schema)
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        rollback = await _persist_complete_pair(connection, schema)

        successor_wave_id = "v4-successor"
        predecessor = rollback["predecessor"]
        await _assert_retired_wave_insert_rejected(
            connection,
            quoted,
            predecessor,
        )
        await _assert_retired_run_tags_rejected(
            connection,
            quoted,
            predecessor,
        )
        with pytest.raises(asyncpg.PostgresError, match="IMMUTABLE"):
            await connection.execute(
                f"UPDATE {quoted}.ptg_import_wave_admission_rollback "
                "SET created_at = clock_timestamp() "
                "WHERE successor_wave_id = $1",
                successor_wave_id,
            )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.close()
