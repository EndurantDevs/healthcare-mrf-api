"""Disposable PostgreSQL proof for exact-wave recovery storage guards."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlsplit

import pytest

asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260807120000_ptg_import_wave_recovery_storage.py"
)
JSON_NULL_PATCH_PATH = ROOT / "alembic" / "versions" / (
    "20260808140000_ptg_import_wave_json_null_preclaim.py"
)
ADMISSION_ROLLBACK_PATH = ROOT / "alembic" / "versions" / (
    "20260808150000_ptg_import_wave_admission_rollback.py"
)
MATERIALIZED_PRECLAIM_PATH = ROOT / "alembic" / "versions" / (
    "20260808180000_ptg_import_wave_materialized_preclaim.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG_IMPORT_WAVE_RECOVERY_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"^ptg_import_wave_recovery_test_[a-z0-9][a-z0-9_]{7,}$"
)


class _Operations:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration(path: Path = MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(
        f"ptg_import_wave_recovery_storage_{path.stem}", path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _dsn() -> str:
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is not configured")
    database = urlsplit(dsn).path.lstrip("/")
    if not _DISPOSABLE_DATABASE_RE.fullmatch(database):
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit disposable database")
    return dsn


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _create_wave_table(connection, quoted: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE {quoted}.ptg_import_wave (
            wave_id varchar(64) PRIMARY KEY,
            idempotency_key varchar(160) NOT NULL UNIQUE,
            request_digest varchar(64) NOT NULL,
            cohort_attestation_digest varchar(64),
            state text NOT NULL,
            uncertainty_resume_state text,
            k8s_post_ticket text,
            k8s_post_started_at timestamptz,
            kubernetes_job_uid text,
            kubernetes_job_receipt jsonb,
            kubernetes_job_receipt_digest text,
            kubernetes_ready_attestation jsonb,
            kubernetes_ready_attestation_digest text,
            redis_release_ticket text,
            redis_release_started_at timestamptz,
            redis_release_attestation jsonb,
            redis_release_attestation_digest text,
            failure_receipt jsonb,
            failure_receipt_digest text,
            outcomes_digest text,
            linkage_ack jsonb,
            linkage_ack_digest text,
            terminal_evidence_digest text,
            terminal_summary jsonb,
            redis_cleanup_ticket text,
            redis_cleanup_started_at timestamptz,
            redis_cleanup_evidence jsonb,
            redis_cleanup_evidence_digest text,
            kubernetes_delete_ticket text,
            kubernetes_delete_started_at timestamptz,
            kubernetes_delete_evidence jsonb,
            kubernetes_delete_evidence_digest text,
            cleanup_evidence_digest text,
            cleanup_summary jsonb,
            resolved_at timestamptz,
            intent_count integer NOT NULL,
            wave_digest text NOT NULL,
            manifest_digest text NOT NULL,
            jobs_digest text NOT NULL,
            release_queue text NOT NULL,
            queue text NOT NULL,
            worker_class text NOT NULL,
            resource_class text NOT NULL,
            worker_limit integer NOT NULL,
            protocol_identity text,
            kubernetes_manifest_identity text,
            kubernetes_config_identity text,
            pinned_image_reference text,
            pinned_image_digest text,
            runtime_image_identity text,
            cohort_attestation jsonb NOT NULL DEFAULT '{{}}'::jsonb
        )
        """
    )


async def _create_support_tables(connection, quoted: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE {quoted}.import_run (
            run_id text PRIMARY KEY, node_id text, importer text, status text,
            source_file_import_id text, import_id text, phase_detail text,
            started_at timestamptz, finished_at timestamptz, snapshot_id text,
            error jsonb, progress jsonb, params jsonb, metrics jsonb
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted}.ptg_import_wave_intent (
            wave_id text, run_id text, source_file_import_id text,
            job_id text, ordinal integer
        )
        """
    )
    await connection.execute(
        f"CREATE TABLE {quoted}.ptg_import_wave_claim (wave_id text)"
    )
    await connection.execute(
        f"CREATE TABLE {quoted}.ptg_import_wave_outcome (wave_id text)"
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted}.ptg_source_attempt_event (
            outer_run_id text, event_kind text
        )
        """
    )


async def _seed_predecessor(connection, quoted: str) -> None:
    digest = "a" * 64
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave (
            wave_id, idempotency_key, request_digest, state,
            uncertainty_resume_state, k8s_post_ticket,
            k8s_post_started_at, intent_count, wave_digest, manifest_digest,
            jobs_digest, release_queue, queue, worker_class, resource_class,
            worker_limit
        ) VALUES (
            'predecessor-wave', 'predecessor-wave', $1::text, 'uncertain',
            'slots_waiting', 'post-ticket',
            clock_timestamp(), 1, $1::text, $1::text, $1::text,
            'arq:PTGSmall:wave:' || $1::text,
            'arq:PTGSmall', 'process.PTGSmall', 'small', 12
        )
        """,
        digest,
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted}.import_run (
            run_id, importer, status, source_file_import_id, import_id,
            phase_detail, error, progress, metrics
        ) VALUES (
            'run-1', 'ptg', 'queued', 'source-1', 'source-1',
            'wave admitted; controller materialization pending',
            'null'::jsonb,
            '{{"unit":"run","total":1,"done":0,"pct":0,"message":"wave admitted; controller materialization pending"}}',
            jsonb_build_object(
                'wave_id', 'predecessor-wave', 'queue', 'arq:PTGSmall:wave:' || $1,
                'base_queue', 'arq:PTGSmall', 'worker_class', 'process.PTGSmall',
                'resource_class', 'small', 'worker_limit', 12, 'job_id', 'job-1',
                'ordinal', 0, 'wave_digest', $1
            )
        )
        """,
        digest,
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave_intent
            (wave_id, run_id, source_file_import_id, job_id, ordinal)
        VALUES ('predecessor-wave', 'run-1', 'source-1', 'job-1', 0)
        """
    )


async def _create_prerequisites(connection, schema: str) -> None:
    """Create the isolated storage contract and one pristine predecessor."""

    quoted = _quote(schema)
    await connection.execute(f'CREATE SCHEMA {quoted}')
    await _create_wave_table(connection, quoted)
    await _create_support_tables(connection, quoted)
    await _seed_predecessor(connection, quoted)


async def _install_migration(connection, monkeypatch, schema: str) -> object:
    await _create_prerequisites(connection, schema)
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", operations.execute)
    migration.upgrade()
    async with connection.transaction():
        for statement in operations.statements:
            await connection.execute(statement)
    patch = _load_migration(JSON_NULL_PATCH_PATH)
    patch_statements: list[str] = []
    monkeypatch.setattr(patch.op, "execute", patch_statements.append)
    patch.upgrade()
    async with connection.transaction():
        for statement in patch_statements:
            await connection.execute(statement)
    rollback = _load_migration(ADMISSION_ROLLBACK_PATH)
    rollback_statements: list[str] = []
    monkeypatch.setattr(rollback.op, "execute", rollback_statements.append)
    rollback.upgrade()
    async with connection.transaction():
        for statement in rollback_statements:
            await connection.execute(statement)
    materialized = _load_migration(MATERIALIZED_PRECLAIM_PATH)
    materialized_statements: list[str] = []
    monkeypatch.setattr(
        materialized.op,
        "execute",
        materialized_statements.append,
    )
    materialized.upgrade()
    async with connection.transaction():
        for statement in materialized_statements:
            await connection.execute(statement)
    return migration


def _signed_evidence(
    unsigned_evidence_map: dict[str, object],
) -> tuple[dict[str, object], bytes]:
    canonical = json.dumps(
        unsigned_evidence_map,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return {
        **unsigned_evidence_map,
        "proof_digest": hashlib.sha256(canonical).hexdigest(),
    }, canonical


def _evidence(successor_wave_id: str) -> tuple[dict[str, object], bytes]:
    digest = "a" * 64
    unsigned_evidence_map = {
        "schema_version": "healthporta.ptg-wave.logical-preclaim-supersession.v1",
        "recovery_basis": "logical_preclaim_failure",
        "predecessor": {
            "wave_id": "predecessor-wave", "wave_digest": digest,
            "manifest_digest": digest, "jobs_digest": digest, "intent_count": 1,
        },
        "successor_wave_id": successor_wave_id,
        "database": {
            "pristine_run_count": 1, "claim_count": 0, "outcome_count": 0,
            "worker_start_event_count": 0,
        },
        "kubernetes": {
            "job_name": "hpw-ptg-wave-" + "a" * 40, "job_uid": "job-uid",
            "completion_mode": "Indexed", "completions": 12,
            "parallelism": 12, "backoff_limit": 0, "failed": 12,
            "active": 0, "succeeded": 0, "ready": 0, "terminating": 0,
            "failed_condition": True, "complete_condition": False,
        },
        "redis": {
            "unclaimed_attestation_digest": "b" * 64,
            "ready_slot_count": 0, "release_present": False,
            "queued_ordinal_count": 0, "job_ordinal_count": 0,
            "result_ordinal_count": 0, "retry_ordinal_count": 0,
            "in_progress_ordinal_count": 0, "health_check_present": False,
        },
    }
    return _signed_evidence(unsigned_evidence_map)


async def _insert_successor(connection, schema: str, wave_id: str, state: str, cohort: dict) -> None:
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave (
            wave_id, idempotency_key, request_digest, state, intent_count,
            wave_digest, manifest_digest,
            jobs_digest, release_queue, queue, worker_class, resource_class,
            worker_limit, cohort_attestation
        ) VALUES ($1, $1, $3, $2, 1, $4, $4, $4,
                  'arq:PTGSmall:wave:' || $4,
                  'arq:PTGSmall', 'process.PTGSmall', 'small', 12, $5::jsonb)
        """,
        wave_id, state, "c" * 64, "b" * 64, json.dumps(cohort),
    )


async def _insert_supersession(connection, schema: str, wave_id: str, evidence: dict, canonical: bytes) -> None:
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave_supersession (
            predecessor_wave_id, successor_wave_id, recovery_basis,
            recovery_evidence, recovery_evidence_canonical,
            recovery_evidence_sha256
        ) VALUES (
            'predecessor-wave', $1, 'logical_preclaim_failure', $2::jsonb,
            $3, $4
        )
        """,
        wave_id, json.dumps(evidence), canonical, evidence["proof_digest"],
    )


async def _assert_predecessor_immutable(connection, quoted: str) -> None:
    with pytest.raises(asyncpg.PostgresError, match="QUARANTINED_IMMUTABLE"):
        await connection.execute(
            f"UPDATE {quoted}.ptg_import_wave SET state = 'materializing' "
            "WHERE wave_id = 'predecessor-wave'"
        )


async def _assert_successor_variants_rejected(connection, schema: str) -> None:
    quoted = _quote(schema)
    for wave_id, state, successor_cohort_map in (
        ("terminal-successor", "failed", {}),
        ("wrong-schema-successor", "admitted", {
            "schema_version": "healthporta.ptg-import-wave-attestation.v2",
        }),
        ("wrong-envelope-successor", "admitted", {
            "schema_version": "healthporta.ptg-import-wave-attestation.v3",
            "wave_id": "wrong-envelope-successor", "supersession": {},
        }),
    ):
        evidence, canonical = _evidence(wave_id)
        if not successor_cohort_map:
            successor_cohort_map = {
                "schema_version": "healthporta.ptg-import-wave-attestation.v3",
                "wave_id": wave_id, "supersession": evidence,
            }
        with pytest.raises(asyncpg.PostgresError, match="SUCCESSOR_BINDING_INVALID"):
            async with connection.transaction():
                await _insert_successor(
                    connection, schema, wave_id, state, successor_cohort_map,
                )
                await _insert_supersession(connection, schema, wave_id, evidence, canonical)
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession"
        ) == 0
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave WHERE wave_id = $1",
            wave_id,
        ) == 0


async def _assert_invalid_evidence_rejected(connection, schema: str) -> None:
    quoted = _quote(schema)
    for wave_id, mutate in (
        ("missing-kubernetes", lambda evidence: evidence.pop("kubernetes")),
        ("decimal-completions", lambda evidence: evidence["kubernetes"].update(
            completions=12.0,
        )),
        ("string-release-present", lambda evidence: evidence["redis"].update(
            release_present="false",
        )),
        ("numeric-job-uid", lambda evidence: evidence["kubernetes"].update(job_uid=7)),
        ("wrong-job-name", lambda evidence: evidence["kubernetes"].update(
            job_name="other-job",
        )),
    ):
        evidence, _canonical = _evidence(wave_id)
        unsigned_evidence_map = deepcopy(evidence)
        unsigned_evidence_map.pop("proof_digest")
        mutate(unsigned_evidence_map)
        evidence, canonical = _signed_evidence(unsigned_evidence_map)
        successor_cohort_map = {
            "schema_version": "healthporta.ptg-import-wave-attestation.v3",
            "wave_id": wave_id, "supersession": evidence,
        }
        with pytest.raises(asyncpg.PostgresError, match="EVIDENCE_INVALID"):
            async with connection.transaction():
                await _insert_successor(
                    connection, schema, wave_id, "admitted", successor_cohort_map,
                )
                await _insert_supersession(
                    connection, schema, wave_id, evidence, canonical,
                )
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession"
        ) == 0


@pytest.mark.asyncio
async def test_successor_binding_rejects_terminal_or_unrelated_rows_and_rolls_back(monkeypatch):
    dsn = _dsn()
    schema = "wave_recovery_successor_rejection"
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        await _assert_predecessor_immutable(connection, _quote(schema))
        await _assert_successor_variants_rejected(connection, schema)
        await _assert_invalid_evidence_rejected(connection, schema)
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()

@pytest.mark.asyncio
async def test_successor_binding_accepts_atomic_admission_and_canonical_evidence(monkeypatch):
    dsn = _dsn()
    schema = "wave_recovery_successor_acceptance"
    connection = await asyncpg.connect(dsn)
    try:
        await _install_migration(connection, monkeypatch, schema)
        wave_id = "admitted-successor"
        evidence, canonical = _evidence(wave_id)
        successor_cohort_map = {
            "schema_version": "healthporta.ptg-import-wave-attestation.v3",
            "wave_id": wave_id, "supersession": evidence,
        }
        async with connection.transaction():
            await _insert_successor(
                connection,
                schema,
                wave_id,
                "admitted",
                successor_cohort_map,
            )
            await _insert_supersession(connection, schema, wave_id, evidence, canonical)
        quoted = _quote(schema)
        await connection.execute(
            f"UPDATE {quoted}.ptg_import_wave SET state = 'materializing' "
            "WHERE wave_id = 'admitted-successor'"
        )
        assert await connection.fetchval(
            f"SELECT state FROM {quoted}.ptg_import_wave WHERE wave_id = $1",
            wave_id,
        ) == "materializing"
        bad_evidence, bad_canonical = _evidence("canonical-mismatch")
        with pytest.raises(asyncpg.PostgresError):
            async with connection.transaction():
                await _insert_successor(
                    connection, schema, "canonical-mismatch", "admitted", {
                        "schema_version": "healthporta.ptg-import-wave-attestation.v3",
                        "wave_id": "canonical-mismatch",
                        "supersession": bad_evidence,
                    },
                )
                await _insert_supersession(
                    connection, schema, "canonical-mismatch", bad_evidence,
                    bad_canonical + b" ",
                )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
