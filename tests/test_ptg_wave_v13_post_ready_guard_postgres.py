"""PostgreSQL enforcement regressions for the closed V13 abandonment guard."""

from __future__ import annotations

import datetime as dt
import json

import pytest

from process.ptg_wave_receipt_authority import ABANDONMENT_RECEIPT_SCHEMA
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment import abandonment_receipt_payload
from tests.ptg_wave_v12_pristine_abandonment_support import (
    boundary as _v6_boundary,
    keyring as _receipt_keyring,
)
from tests.ptg_wave_v13_post_ready_guard_support import (
    JSON_NULL_GUARD_MIGRATION_PATH,
    MIGRATION_PATH,
    add_v13_head_prerequisites,
    v13_proof,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _assert_python_admission_capacity,
    _fixture,
    _install_receipt_migration,
    _seed_pristine_intents_and_runs,
    _seed_v6_wave,
)
from tests.test_ptg_wave_recovery_storage_postgres import _load_migration
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote, asyncpg


async def _install_v13_guard(connection, monkeypatch, schema: str) -> None:
    await _install_receipt_migration(connection, monkeypatch, schema)
    await add_v13_head_prerequisites(connection, _quote(schema))
    sql_statements: list[str] = []
    for migration_path in (MIGRATION_PATH, JSON_NULL_GUARD_MIGRATION_PATH):
        migration = _load_migration(migration_path)
        monkeypatch.setattr(migration.op, "execute", sql_statements.append)
        migration.upgrade()
    async with connection.transaction():
        for statement in sql_statements:
            await connection.execute(statement)


def _job_receipt_by_field(admission: dict, job_uid: str) -> dict:
    return {
        "wave_digest": admission["wave_digest"],
        "job_uid": job_uid,
        "manifest_identity": "1" * 64,
        "config_identity": "2" * 64,
        "pinned_image_reference": "registry.invalid/ptg@sha256:" + "3" * 64,
        "pinned_image_digest": "3" * 64,
        "runtime_image_identity": "sha256:" + "4" * 64,
    }


async def _seed_v13_wave(
    connection,
    schema: str,
    admission: dict,
    job_receipt_by_field: dict,
) -> str:
    await _seed_v6_wave(
        connection,
        schema,
        admission,
        state="slots_waiting",
        materialized={
            "kubernetes": {
                "job_uid": job_receipt_by_field["job_uid"],
                "job_receipt_digest": sha256_digest(
                    canonical_json(job_receipt_by_field)
                ),
            }
        },
    )
    await _seed_pristine_intents_and_runs(connection, schema, admission)
    quoted_schema = _quote(schema)
    await connection.execute(
        f"UPDATE {quoted_schema}.import_run SET error = 'null'::json"
    )
    await connection.execute(
        f"UPDATE {quoted_schema}.ptg_import_wave SET "
        "kubernetes_manifest = '{}'::json, "
        "kubernetes_manifest_bytes = convert_to('{}', 'UTF8'), "
        "kubernetes_manifest_sha256 = encode(sha256(convert_to('{}', 'UTF8')), 'hex') "
        "WHERE wave_id = $1",
        admission["wave_id"],
    )
    return quoted_schema


async def _assert_unbound_quarantine_rejected(
    connection,
    quoted_schema: str,
    admission: dict,
) -> None:
    with pytest.raises(asyncpg.CheckViolationError) as excinfo:
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg_import_wave_quarantine (
                predecessor_wave_id, reason
            ) VALUES ($1, 'v13_post_ready_unreleased_failure_cutover')
            """,
            admission["wave_id"],
        )
    assert (
        excinfo.value.constraint_name
        == "ptg_import_wave_quarantine_abandonment_evidence_check"
    )
    assert await _quarantine_count(connection, quoted_schema, admission) == 0


async def _assert_empty_proof_rejected(
    connection,
    quoted_schema: str,
    admission: dict,
) -> None:
    with pytest.raises(asyncpg.PostgresError, match="V13_ABANDONMENT_PROOF_INVALID"):
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg_import_wave_quarantine (
                predecessor_wave_id, reason, successor_wave_id,
                recovery_basis, recovery_evidence,
                recovery_evidence_canonical, recovery_evidence_sha256,
                receipt_key_id
            ) VALUES (
                $1, 'v13_post_ready_unreleased_failure_cutover', $2,
                'v13_post_ready_unreleased_failure_cutover',
                $3::jsonb, $4::bytea, $5, $6
            )
            """,
            admission["wave_id"],
            ordinary_cutover_id(admission["wave_id"]),
            json.dumps({}),
            b"{}",
            "0" * 64,
            admission["receipt_key_id"],
        )
    assert await _quarantine_count(connection, quoted_schema, admission) == 0


async def _quarantine_count(connection, quoted_schema: str, admission: dict) -> int:
    return await connection.fetchval(
        f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_quarantine "
        "WHERE predecessor_wave_id = $1",
        admission["wave_id"],
    )


async def _assert_legacy_quarantine_allowed(
    connection,
    quoted_schema: str,
    admission: dict,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.ptg_import_wave_quarantine (
            predecessor_wave_id, reason
        ) VALUES ($1, 'materialized_preclaim_failure')
        """,
        admission["wave_id"],
    )
    assert await connection.fetchval(
        f"SELECT recovery_basis FROM {quoted_schema}.ptg_import_wave_quarantine "
        "WHERE predecessor_wave_id = $1",
        admission["wave_id"],
    ) is None


@pytest.mark.asyncio
async def test_postgres_v13_guard_rejects_an_unbound_quarantine(monkeypatch):
    """The new basis cannot bypass proof or receipt validation by direct SQL."""

    schema = "v13_post_ready_guard_reject"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_v13_guard(connection, monkeypatch, schema)
        admission = _fixture()["abandonment"]["proof"]["admission"]
        job_receipt_by_field = _job_receipt_by_field(admission, "v13-synthetic-job")
        quoted_schema = await _seed_v13_wave(
            connection,
            schema,
            admission,
            job_receipt_by_field,
        )
        await _assert_unbound_quarantine_rejected(connection, quoted_schema, admission)
        await _assert_empty_proof_rejected(connection, quoted_schema, admission)
        await _assert_legacy_quarantine_allowed(connection, quoted_schema, admission)
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


async def _insert_signed_quarantine(
    connection,
    quoted_schema: str,
    admission: dict,
    proof: dict,
    signer,
) -> None:
    receipt = signer.sign_receipt(
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id=admission["receipt_key_id"],
        issued_at=dt.datetime(2026, 8, 17, 0, 7, tzinfo=dt.UTC),
        receipt_payload=abandonment_receipt_payload(proof),
    )
    unsigned_proof_by_field = {
        name: field_value
        for name, field_value in proof.items()
        if name != "proof_digest"
    }
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.ptg_import_wave_quarantine (
            predecessor_wave_id, reason, successor_wave_id,
            recovery_basis, recovery_evidence, recovery_evidence_canonical,
            recovery_evidence_sha256, receipt_key_id, abandonment_receipt,
            abandonment_receipt_payload_digest, abandonment_receipt_issued_at,
            created_at
        ) VALUES (
            $1, 'v13_post_ready_unreleased_failure_cutover', $2,
            'v13_post_ready_unreleased_failure_cutover', $3::jsonb, $4::bytea,
            $5, $6, $7::jsonb, $8, $9::text::timestamptz,
            $9::text::timestamptz
        )
        """,
        admission["wave_id"],
        proof["cutover_id"],
        json.dumps(proof),
        canonical_json(unsigned_proof_by_field),
        proof["proof_digest"],
        admission["receipt_key_id"],
        json.dumps(receipt),
        receipt["payload_digest"],
        receipt["issued_at"],
    )


async def _assert_v13_work_is_frozen(
    connection,
    quoted_schema: str,
    admission: dict,
) -> None:
    unsafe_sql_statements = (
        f"UPDATE {quoted_schema}.import_run SET status = 'running' "
        "WHERE run_id = 'fixture-run-0'",
        f"INSERT INTO {quoted_schema}.ptg_import_wave_claim (wave_id) VALUES ($1)",
        f"INSERT INTO {quoted_schema}.ptg_source_attempt_event "
        "(outer_run_id, event_kind) VALUES ('fixture-run-0', 'worker_start_admitted')",
    )
    for unsafe_sql in unsafe_sql_statements:
        with pytest.raises(asyncpg.PostgresError, match="V13_ABANDONED_IMMUTABLE"):
            if "$1" in unsafe_sql:
                await connection.execute(unsafe_sql, admission["wave_id"])
            else:
                await connection.execute(unsafe_sql)


async def _assert_non_null_error_rejected(
    connection,
    quoted_schema: str,
    admission: dict,
    proof: dict,
    signer,
) -> None:
    await connection.execute(
        f"UPDATE {quoted_schema}.import_run "
        "SET error = '{\"kind\":\"synthetic\"}'::json "
        "WHERE run_id = 'fixture-run-0'"
    )
    with pytest.raises(
        asyncpg.PostgresError,
        match="PTG_IMPORT_WAVE_V13_ABANDONMENT_NOT_PRISTINE",
    ):
        await _insert_signed_quarantine(
            connection,
            quoted_schema,
            admission,
            proof,
            signer,
        )
    await connection.execute(
        f"UPDATE {quoted_schema}.import_run SET error = 'null'::json "
        "WHERE run_id = 'fixture-run-0'"
    )


@pytest.mark.asyncio
async def test_postgres_v13_signed_quarantine_releases_capacity_and_freezes_work(
    monkeypatch,
):
    """A valid V13 row is the sole write and fences every old work surface."""

    schema = "v13_post_ready_guard_signed"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_v13_guard(connection, monkeypatch, schema)
        _wave, _intents, _runs, admission = _v6_boundary()
        signer = _receipt_keyring(monkeypatch)
        assert admission["receipt_key_id"] == "receipt-active"
        job_receipt_by_field = _job_receipt_by_field(admission, "v13-signed-job")
        quoted_schema = await _seed_v13_wave(
            connection,
            schema,
            admission,
            job_receipt_by_field,
        )
        proof = v13_proof(admission, job_receipt_by_field)
        await _assert_non_null_error_rejected(
            connection,
            quoted_schema,
            admission,
            proof,
            signer,
        )
        await _insert_signed_quarantine(
            connection,
            quoted_schema,
            admission,
            proof,
            signer,
        )
        assert await connection.fetchval(
            f"SELECT recovery_evidence_sha256 FROM {quoted_schema}.ptg_import_wave_quarantine "
            "WHERE predecessor_wave_id = $1",
            admission["wave_id"],
        ) == proof["proof_digest"]
        await _assert_python_admission_capacity(
            monkeypatch,
            schema,
            wave_id=admission["wave_id"],
            released=True,
        )
        await _assert_v13_work_is_frozen(connection, quoted_schema, admission)
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
