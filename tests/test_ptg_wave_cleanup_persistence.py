

"""Direct terminal-proof and cleanup persistence contracts."""


from __future__ import annotations


import copy


import types


from unittest.mock import AsyncMock, Mock


import pytest


from process import ptg_wave_cleanup as cleanup


_WAVE = "1" * 64


_MANIFEST = "2" * 64


_JOBS = "3" * 64


_IMAGE = "registry.example/engine@sha256:" + "4" * 64


class _Result:
    def __init__(self, *, rows=()):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.flush_count = 0

    async def execute(self, _statement):
        assert self.results, "unexpected database execute"
        return self.results.pop(0)

    async def flush(self):
        self.flush_count += 1


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": _WAVE,
        "state": "cleaning",
        "intent_count": 2,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "manifest_digest": _MANIFEST,
        "jobs_digest": _JOBS,
        "pinned_image_reference": _IMAGE,
        "redis_release_attestation": {"release_digest": "5" * 64},
        "outcomes_digest": "6" * 64,
        "linkage_ack_digest": "7" * 64,
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "terminal_evidence_digest": "8" * 64,
        "terminal_summary": None,
        "redis_cleanup_ticket": None,
        "redis_cleanup_started_at": None,
        "redis_cleanup_evidence": None,
        "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_ticket": None,
        "kubernetes_delete_started_at": None,
        "kubernetes_delete_evidence": None,
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_job_uid": "job-uid",
        "kubernetes_job_receipt_digest": "9" * 64,
        "k8s_post_started_at": object(),
        "kubernetes_manifest_identity": "a" * 64,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _install_wave(monkeypatch, wave, session=None):
    session = session or _Session()
    monkeypatch.setattr(cleanup.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(cleanup, "_locked_wave", AsyncMock(return_value=wave))
    return session


def _pre_cleanup(wave):
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference,
        "release_digest": wave.redis_release_attestation["release_digest"],
        "target_key_count": 4 + 4 * wave.intent_count,
        "queue_entry_count": 0,
        "job_payload_count": 0,
        "result_count": wave.intent_count,
        "retry_count": 0,
        "in_progress_count": 0,
        "health_check_count": 1,
        "result_presence_digest": "b" * 64,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }


def _post_cleanup(wave):
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "absent_target_count": 4 + 4 * wave.intent_count,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }


def _cleanup_operation(wave, pre):
    return {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "deleted_key_count": 3,
        "pre_cleanup_attestation_digest": pre["attestation_digest"],
        "pre_cleanup": pre,
    }


def _cleanup_evidence(wave, *, mode="executed"):
    pre = _pre_cleanup(wave)
    wave.terminal_summary = {"redis_pre_cleanup": pre}
    return {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": wave.redis_cleanup_ticket,
        "mode": mode,
        "pre_cleanup": pre,
        "operation_receipt": None if mode == "get_only_reconciled" else _cleanup_operation(wave, pre),
        "post_cleanup": _post_cleanup(wave),
    }


def _kubernetes_evidence(wave):
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave.kubernetes-absence.v1",
        "operation_ticket": wave.kubernetes_delete_ticket,
        "wave_digest": wave.wave_digest,
        "job_name": wave.kubernetes_manifest["metadata"]["name"],
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "delete_permitted": wave.kubernetes_job_uid is not None,
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    return {
        **unsigned_by_field,
        "observation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }
