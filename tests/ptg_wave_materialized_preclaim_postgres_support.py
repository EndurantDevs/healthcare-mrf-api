"""Disposable-PostgreSQL fixtures for V5 materialized recovery."""

from __future__ import annotations

import json

from api.ptg_wave_kubernetes import _job_name
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    wave_queue_name,
)
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    build_admission_rollback_supersession_proof,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    build_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from tests.test_ptg_wave_recovery_storage_postgres import (
    _evidence,
    _insert_successor,
    _insert_supersession,
    _quote,
)


_PREDECESSOR_PROOF_FIELDS = (
    "wave_id",
    "idempotency_key",
    "request_digest",
    "cohort_attestation_digest",
    "wave_digest",
    "release_queue",
    "manifest_digest",
    "jobs_digest",
    "intent_count",
    "worker_limit",
    "kubernetes_manifest_identity",
    "kubernetes_config_identity",
    "pinned_image_reference",
    "pinned_image_digest",
    "runtime_image_identity",
    "kubernetes_job_uid",
    "kubernetes_job_receipt_digest",
)


def _canonical_proof(proof: dict) -> bytes:
    return canonical_json({
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "proof_digest"
    })


def _rollback_evidence(
    successor_wave_id: str,
) -> tuple[dict, bytes, dict]:
    request_digest = "5" * 64
    wave_digest = sha256_digest(
        (
            PTG_SMALL_WAVE_PROTOCOL_IDENTITY
            + "\0"
            + request_digest
        ).encode()
    )
    predecessor_map = {
        "wave_id": "rollback-predecessor",
        "idempotency_key": "rollback-predecessor",
        "request_digest": request_digest,
        "wave_digest": wave_digest,
        "release_queue": wave_queue_name(wave_digest),
        "intent_count": 1,
    }
    proof = build_admission_rollback_supersession_proof(
        predecessor_map,
        successor_wave_id,
        database={field_name: 0 for field_name in DATABASE_FIELDS},
        kubernetes={
            "job_name": _job_name(wave_digest),
            "job_present": False,
            "pod_count": 0,
        },
        redis={
            "queue_name": predecessor_map["release_queue"],
            "queued_entry_count": 0,
            "ready_slot_count": 0,
            "release_present": False,
            "health_check_present": False,
        },
    )
    return proof, _canonical_proof(proof), predecessor_map


def _materialized_descriptor() -> dict:
    request_digest = "6" * 64
    wave_digest = sha256_digest(
        (
            PTG_SMALL_WAVE_PROTOCOL_IDENTITY
            + "\0"
            + request_digest
        ).encode()
    )
    pinned_digest = "d" * 64
    receipt_map = {
        "wave_digest": wave_digest,
        "job_uid": "materialized-job-uid",
        "manifest_identity": "8" * 64,
        "config_identity": "9" * 64,
        "pinned_image_reference": (
            "registry.example/worker@sha256:" + pinned_digest
        ),
        "pinned_image_digest": pinned_digest,
        "runtime_image_identity": "sha256:" + "e" * 64,
    }
    return {
        "wave_id": "materialized-wave",
        "idempotency_key": "materialized-wave",
        "request_digest": request_digest,
        "cohort_attestation_digest": "7" * 64,
        "wave_digest": wave_digest,
        "release_queue": wave_queue_name(wave_digest),
        "manifest_digest": "a" * 64,
        "jobs_digest": "b" * 64,
        "intent_count": 1,
        "worker_limit": 12,
        "kubernetes_manifest_identity": "8" * 64,
        "kubernetes_config_identity": "9" * 64,
        "pinned_image_reference": receipt_map["pinned_image_reference"],
        "pinned_image_digest": pinned_digest,
        "runtime_image_identity": receipt_map["runtime_image_identity"],
        "kubernetes_job_uid": receipt_map["job_uid"],
        "kubernetes_job_receipt": receipt_map,
        "kubernetes_job_receipt_digest": sha256_digest(
            canonical_json(receipt_map)
        ),
    }


async def _insert_rollback(
    connection,
    schema: str,
    successor_wave_id: str,
    proof: dict,
    canonical: bytes,
    predecessor: dict,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave_admission_rollback (
            predecessor_wave_id, predecessor_idempotency_key,
            predecessor_request_digest, predecessor_wave_digest,
            predecessor_release_queue, predecessor_intent_count,
            successor_wave_id, recovery_basis, recovery_evidence,
            recovery_evidence_canonical, recovery_evidence_sha256,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            'admission_rollback_absent', $8::jsonb, $9, $10,
            clock_timestamp()
        )
        """,
        predecessor["wave_id"],
        predecessor["idempotency_key"],
        predecessor["request_digest"],
        predecessor["wave_digest"],
        predecessor["release_queue"],
        predecessor["intent_count"],
        successor_wave_id,
        json.dumps(proof),
        canonical,
        proof["proof_digest"],
    )


async def _insert_prior_v4_admission(
    connection,
    schema: str,
    descriptor: dict,
) -> tuple[dict, dict, dict]:
    logical_proof, logical_canonical = _evidence(descriptor["wave_id"])
    rollback_proof, rollback_canonical, rollback_predecessor = (
        _rollback_evidence(descriptor["wave_id"])
    )
    cohort_map = {
        "schema_version": "healthporta.ptg-import-wave-attestation.v4",
        "wave_id": descriptor["wave_id"],
        "supersession": logical_proof,
        "admission_rollback_supersession": rollback_proof,
    }
    async with connection.transaction():
        await _insert_successor(
            connection,
            schema,
            descriptor["wave_id"],
            "admitted",
            cohort_map,
        )
        await _insert_supersession(
            connection,
            schema,
            descriptor["wave_id"],
            logical_proof,
            logical_canonical,
        )
        await _insert_rollback(
            connection,
            schema,
            descriptor["wave_id"],
            rollback_proof,
            rollback_canonical,
            rollback_predecessor,
        )
    return logical_proof, rollback_proof, rollback_predecessor


async def _persist_materialized_boundary(
    connection,
    schema: str,
    descriptor: dict,
) -> None:
    quoted = _quote(schema)
    await connection.execute(
        f"""
        UPDATE {quoted}.ptg_import_wave SET
            request_digest = $1, cohort_attestation_digest = $2,
            wave_digest = $3, manifest_digest = $4, jobs_digest = $5,
            release_queue = $6, state = 'slots_waiting',
            k8s_post_ticket = 'post-ticket-v10',
            k8s_post_started_at = clock_timestamp(),
            kubernetes_job_uid = $7,
            kubernetes_job_receipt = $8::jsonb,
            kubernetes_job_receipt_digest = $9,
            protocol_identity = $10,
            kubernetes_manifest_identity = $11,
            kubernetes_config_identity = $12,
            pinned_image_reference = $13, pinned_image_digest = $14,
            runtime_image_identity = $15
        WHERE wave_id = $16
        """,
        descriptor["request_digest"],
        descriptor["cohort_attestation_digest"],
        descriptor["wave_digest"],
        descriptor["manifest_digest"],
        descriptor["jobs_digest"],
        descriptor["release_queue"],
        descriptor["kubernetes_job_uid"],
        json.dumps(descriptor["kubernetes_job_receipt"]),
        descriptor["kubernetes_job_receipt_digest"],
        PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
        descriptor["kubernetes_manifest_identity"],
        descriptor["kubernetes_config_identity"],
        descriptor["pinned_image_reference"],
        descriptor["pinned_image_digest"],
        descriptor["runtime_image_identity"],
        descriptor["wave_id"],
    )


async def _persist_pristine_run(connection, schema: str, descriptor: dict) -> None:
    quoted = _quote(schema)
    await connection.execute(
        f"""
        INSERT INTO {quoted}.import_run (
            run_id, importer, status, source_file_import_id, import_id,
            phase_detail, error, progress, metrics
        ) VALUES (
            'materialized-run', 'ptg', 'queued', 'materialized-source',
            'materialized-source',
            'wave admitted; controller materialization pending',
            'null'::jsonb,
            '{{"unit":"run","total":1,"done":0,"pct":0,
              "message":"wave admitted; controller materialization pending"}}',
            jsonb_build_object(
                'wave_id', $1::text, 'queue', $2::text,
                'base_queue', 'arq:PTGSmall',
                'worker_class', 'process.PTGSmall',
                'resource_class', 'small', 'worker_limit', 12,
                'job_id', 'materialized-job-0', 'ordinal', 0,
                'wave_digest', $3::text
            )
        )
        """,
        descriptor["wave_id"],
        descriptor["release_queue"],
        descriptor["wave_digest"],
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave_intent (
            wave_id, run_id, source_file_import_id, job_id, ordinal
        ) VALUES (
            $1, 'materialized-run', 'materialized-source',
            'materialized-job-0', 0
        )
        """,
        descriptor["wave_id"],
    )


async def seed_materialized_predecessor(connection, schema: str) -> dict:
    """Seed one V4 successor at the exact durable Job-only boundary."""

    descriptor = _materialized_descriptor()
    logical_proof, rollback_proof, rollback_predecessor = (
        await _insert_prior_v4_admission(connection, schema, descriptor)
    )
    await _persist_materialized_boundary(connection, schema, descriptor)
    await _persist_pristine_run(connection, schema, descriptor)
    return {
        **descriptor,
        "logical_proof_digest": logical_proof["proof_digest"],
        "rollback_proof_digest": rollback_proof["proof_digest"],
        "logical_predecessor_wave_id": "predecessor-wave",
        "rollback_predecessor_wave_id": rollback_predecessor["wave_id"],
    }


def materialized_evidence(
    descriptor: dict,
    successor_wave_id: str,
) -> tuple[dict, bytes]:
    """Build one exact successor-bound V5 storage witness."""

    proof = build_materialized_preclaim_supersession_proof(
        predecessor={
            field_name: descriptor[field_name]
            for field_name in _PREDECESSOR_PROOF_FIELDS
        },
        successor_wave_id=successor_wave_id,
        prior_recovery={
            "logical_preclaim_predecessor_wave_id": descriptor[
                "logical_predecessor_wave_id"
            ],
            "logical_preclaim_proof_digest": descriptor[
                "logical_proof_digest"
            ],
            "admission_rollback_predecessor_wave_id": descriptor[
                "rollback_predecessor_wave_id"
            ],
            "admission_rollback_proof_digest": descriptor[
                "rollback_proof_digest"
            ],
        },
        kubernetes={
            "job_name": _job_name(descriptor["wave_digest"]),
            "job_uid": descriptor["kubernetes_job_uid"],
            "job_receipt_digest": descriptor[
                "kubernetes_job_receipt_digest"
            ],
            "completion_mode": "Indexed",
            "completions": 12,
            "parallelism": 12,
            "backoff_limit": 0,
            "failed": 12,
            "active": 0,
            "succeeded": 0,
            "ready": 0,
            "terminating": 0,
            "failed_condition": True,
            "complete_condition": False,
        },
        redis={
            "unclaimed_attestation_digest": "f" * 64,
            "ready_slot_count": 0,
            "release_present": False,
            "queued_ordinal_count": 0,
            "job_ordinal_count": 0,
            "result_ordinal_count": 0,
            "retry_ordinal_count": 0,
            "in_progress_ordinal_count": 0,
            "health_check_present": False,
        },
    )
    return proof, _canonical_proof(proof)


async def insert_materialized_supersession(
    connection,
    schema: str,
    predecessor_wave_id: str,
    successor_wave_id: str,
    evidence: dict,
    canonical: bytes,
) -> None:
    """Insert the quarantine and immutable V5 handoff in one transaction."""

    quoted = _quote(schema)
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave_quarantine (
            predecessor_wave_id, reason
        ) VALUES ($1, 'materialized_preclaim_failure')
        """,
        predecessor_wave_id,
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted}.ptg_import_wave_supersession (
            predecessor_wave_id, successor_wave_id, recovery_basis,
            recovery_evidence, recovery_evidence_canonical,
            recovery_evidence_sha256
        ) VALUES (
            $1, $2, 'materialized_preclaim_failure', $3::jsonb,
            $4, $5
        )
        """,
        predecessor_wave_id,
        successor_wave_id,
        json.dumps(evidence),
        canonical,
        evidence["proof_digest"],
    )


def successor_cohort(successor_wave_id: str, evidence: dict) -> dict:
    """Return the minimal V5 envelope enforced by deferred binding."""

    return {
        "schema_version": "healthporta.ptg-import-wave-attestation.v5",
        "wave_id": successor_wave_id,
        "materialized_preclaim_supersession": evidence,
    }


__all__ = [
    "insert_materialized_supersession",
    "materialized_evidence",
    "seed_materialized_predecessor",
    "successor_cohort",
]
