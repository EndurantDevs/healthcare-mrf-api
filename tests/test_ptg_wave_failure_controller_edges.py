

"""Focused failure, fence, receipt, and terminal-edge contracts."""


from __future__ import annotations


import types


from unittest.mock import AsyncMock, Mock


import pytest


from sanic.exceptions import BadRequest, NotFound


from api import control_wave_routes as routes


from api.ptg_wave_kubernetes import PTGWaveContractError, queue_for_wave


from process import ptg_control


from process import ptg_wave_failure as failure


from process import ptg_wave_failure_kubernetes as failure_kubernetes


from process import ptg_wave_failure_persistence as failure_persistence


from process import ptg_wave_failure_receipts as failure_receipts


from process import ptg_wave_failure_snapshots as failure_snapshots


from process import ptg_wave_failure_terminal as failure_terminal


from process import ptg_wave_failure_types as failure_types


from process import ptg_wave_failure_validation as failure_validation


from process import ptg_wave_controller_isolation as isolation


from process import ptg_wave_controller_receipts as receipts


from process import ptg_wave_outcome_contract as outcomes


from process.ptg_parts import ptg_wave_admission_fence as fence


from process.ptg_wave_barrier import PTGWaveWorkerIdentity, run_after_wave_release


from process.ptg_wave_state import PTGWaveStateConflict, canonical_json, sha256_digest


from process.ptg_wave_terminal_state import derive_terminal_state


_WAVE_DIGEST = "1" * 64


_MANIFEST_DIGEST = "2" * 64


_JOBS_DIGEST = "3" * 64


_CONFIG_DIGEST = "4" * 64


_MANIFEST_IDENTITY = "5" * 64


_IMAGE_DIGEST = "6" * 64


_RUNTIME_IDENTITY = "sha256:" + "7" * 64


_IMAGE = f"registry.example/synthetic@sha256:{_IMAGE_DIGEST}"


_LINKAGE_KEY = "synthetic-linkage-key"


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-synthetic",
        "wave_digest": _WAVE_DIGEST,
        "intent_count": 2,
        "state": "executing",
        "queue": "arq:PTGSmall",
        "release_queue": queue_for_wave(_WAVE_DIGEST),
        "worker_class": "process.PTGSmall",
        "resource_class": "small",
        "worker_limit": 12,
        "protocol_identity": "protocol-v1",
        "serializer_identity": "serializer-v1",
        "manifest_digest": _MANIFEST_DIGEST,
        "jobs_digest": _JOBS_DIGEST,
        "kubernetes_config_identity": _CONFIG_DIGEST,
        "kubernetes_manifest_identity": _MANIFEST_IDENTITY,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-synthetic"}},
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": _IMAGE_DIGEST,
        "runtime_image_identity": _RUNTIME_IDENTITY,
        "kubernetes_job_uid": "job-synthetic",
        "kubernetes_delete_ticket": "delete-ticket",
        "kubernetes_delete_evidence": None,
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_job_receipt_digest": "8" * 64,
        "kubernetes_ready_attestation": {
            "slots": [
                {
                    "slot": slot,
                    "pod_uid": f"pod-synthetic-{slot}",
                    "runtime_image_identity": _RUNTIME_IDENTITY,
                }
                for slot in range(12)
            ]
        },
        "redis_release_attestation": None,
        "redis_release_attestation_digest": None,
        "redis_release_ticket": None,
        "redis_cleanup_ticket": None,
        "redis_cleanup_evidence_digest": None,
        "k8s_post_ticket": "post-ticket",
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "outcomes_digest": "9" * 64,
        "linkage_ack": None,
        "linkage_ack_digest": None,
        "terminal_summary": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _intent(ordinal: int):
    return types.SimpleNamespace(
        ordinal=ordinal,
        run_id=f"run-synthetic-{ordinal}",
        job_id=f"job-synthetic-{ordinal}",
        source_file_import_id=f"source-synthetic-{ordinal}",
        content_version="v1",
    )


def _claim(wave, intent, *, slot: int = 0, **overrides):
    fields_by_field = {
        "ordinal": intent.ordinal,
        "wave_id": wave.wave_id,
        "run_id": intent.run_id,
        "job_id": intent.job_id,
        "claim_status": "started",
        "failure_code": None,
        "kubernetes_job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
        "config_identity": wave.kubernetes_config_identity,
        "slot": slot,
        "pod_uid": f"pod-synthetic-{slot}",
        "claim_attempt_token": "a" * 32,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _preclaim_evidence(wave):
    failed_slots = [
        {
            "slot": slot["slot"],
            "pod_uid": slot["pod_uid"],
            "phase": "Failed",
            "runtime_image_identity": wave.runtime_image_identity,
        }
        for slot in wave.kubernetes_ready_attestation["slots"]
    ]
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave.kubernetes-preclaim-failure.v1",
        "wave_digest": wave.wave_digest,
        "queue": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "config_identity": wave.kubernetes_config_identity,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": wave.runtime_image_identity,
        "job_name": "ptg-wave-synthetic",
        "job_uid": wave.kubernetes_job_uid,
        "backoff_limit": 0,
        "job_active": 0,
        "job_failed": 12,
        "job_succeeded": 0,
        "job_failure_condition": {"type": "Failed", "status": "True"},
        "failed_slots": failed_slots,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": sha256_digest(canonical_json(unsigned_by_field)),
    }


def _outcome(intent, *, status="failed"):
    return types.SimpleNamespace(
        ordinal=intent.ordinal,
        run_id=intent.run_id,
        job_id=intent.job_id,
        source_file_import_id=intent.source_file_import_id,
        content_version=intent.content_version,
        status=status,
        snapshot_id=None,
        import_id=None,
    )


class _Pipeline:
    def __init__(self, values):
        self.values = values
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def zcard(self, key):
        self.calls.append(("zcard", key))

    def get(self, key):
        self.calls.append(("get", key))

    async def execute(self):
        return self.values


class _Redis:
    def __init__(self, values):
        self.values = values

    def pipeline(self, *, transaction):
        assert transaction is True
        return _Pipeline(self.values)


class _Request:
    def __init__(self, *, json=None, args=None):
        self.json = json
        self.args = {} if args is None else args


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class _SequenceSession:
    def __init__(self, results):
        self.results = list(results)
        self.added = []

    async def execute(self, *_args, **_kwargs):
        return _Rows(self.results.pop(0))

    def add(self, value):
        self.added.append(value)


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return False


def _unclaimed_receipt(wave, *, reason, evidence, origin_state, operation, ticket):
    return {
        "schema_version": failure_types.FAILURE_SCHEMA,
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "origin_state": origin_state,
        "reason": reason,
        "operation": operation,
        "operation_ticket": ticket,
        "evidence": evidence,
        "evidence_digest": sha256_digest(canonical_json(evidence)),
        "unclaimed_ordinals_digest": failure_types._unclaimed_ordinals_digest(wave),
    }


def _absence_evidence(wave):
    unsigned = failure_kubernetes._expected_kubernetes_absence(wave)
    return {
        **unsigned,
        "observation_digest": sha256_digest(canonical_json(unsigned)),
    }


def _redis_failure_receipt(wave, **overrides):
    fields_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "target_key_count": 4 + (4 * wave.intent_count),
        "ready_slots": [],
        "ready_slots_digest": sha256_digest(canonical_json([])),
        "release_present": False,
        "release_digest": None,
        "release_receipt": None,
        "queued_ordinals": [],
        "job_ordinals": [],
        "result_ordinals": [],
        "retry_ordinals": [],
        "in_progress_ordinals": [],
        "health_check_present": False,
    }
    fields_by_field.update(overrides)
    fields_by_field["attestation_digest"] = sha256_digest(canonical_json(fields_by_field))
    return fields_by_field


def _claimed_receipt(wave, *, claimed_ordinals, origin_state="executing"):
    kubernetes_evidence_by_field = {"kubernetes": "synthetic"}
    redis_evidence_by_field = {"redis": "synthetic"}
    return {
        "schema_version": failure_types.CLAIMED_PRESTART_FAILURE_SCHEMA,
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "origin_state": origin_state,
        "reason": failure_types.CLAIMED_PRESTART_FAILURE_REASON,
        "operation": "worker_start",
        "operation_ticket": None,
        "claimed_ordinals": claimed_ordinals,
        "claimed_ordinals_digest": failure_types._claimed_ordinals_digest(
            wave, claimed_ordinals
        ),
        "kubernetes_evidence": kubernetes_evidence_by_field,
        "kubernetes_evidence_digest": sha256_digest(
            canonical_json(kubernetes_evidence_by_field)
        ),
        "redis_evidence": redis_evidence_by_field,
        "redis_evidence_digest": sha256_digest(canonical_json(redis_evidence_by_field)),
    }
