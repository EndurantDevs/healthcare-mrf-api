"""Crash-boundary contracts for the claimed-before-ImportRun-start PTG path."""

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

import process.ptg_wave_failure as failure
from process.ptg_wave_outcomes import linkage_mapping_digest, sign_linkage_ack
from process.ptg_wave_state import (
    canonical_json,
    sha256_digest,
)


_PIN_DIGEST = "1" * 64
_PIN = f"registry/unit@sha256:{_PIN_DIGEST}"
_RUNTIME = "sha256:" + "2" * 64
_CONFIG = "3" * 64
_MANIFEST = "4" * 64
_WAVE = "5" * 64
_JOBS = "6" * 64
_KEY = "claimed-prestart-linkage-key"


def _wave(*, state: str = "executing", intent_count: int = 3):
    ready_receipt_map = {
        "slots": [
            {
                "slot": slot,
                "pod_uid": f"pod-{slot}",
                "runtime_image_identity": _RUNTIME,
            }
            for slot in range(12)
        ]
    }
    return types.SimpleNamespace(
        wave_id="wave-claimed-prestart",
        wave_digest=_WAVE,
        intent_count=intent_count,
        state=state,
        queue="arq:PTGSmall",
        release_queue=f"arq:PTGSmall:wave:{_WAVE}",
        worker_class="process.PTGSmall",
        resource_class="small",
        worker_limit=12,
        jobs_digest=_JOBS,
        manifest_digest="a" * 64,
        protocol_identity="healthporta.ptg-small.exact-wave.v1",
        serializer_identity="arq-0.28.process-msgpack.v1",
        kubernetes_manifest={"metadata": {"name": "ptg-wave-claimed-prestart"}},
        kubernetes_manifest_identity=_MANIFEST,
        kubernetes_config_identity=_CONFIG,
        kubernetes_job_uid="job-uid-original",
        kubernetes_job_receipt_digest="b" * 64,
        kubernetes_ready_attestation=ready_receipt_map,
        kubernetes_ready_attestation_digest="c" * 64,
        pinned_image_reference=_PIN,
        pinned_image_digest=_PIN_DIGEST,
        runtime_image_identity=_RUNTIME,
        redis_release_attestation=None,
        redis_release_attestation_digest=None,
        outcomes_digest=None,
        failure_receipt=None,
        failure_receipt_digest=None,
        linkage_ack=None,
        linkage_ack_digest=None,
        redis_cleanup_ticket="redis-cleanup-ticket",
    )


def _intents(count: int = 3):
    return [
        types.SimpleNamespace(
            wave_id="wave-claimed-prestart",
            ordinal=ordinal,
            run_id=f"run-{ordinal}",
            job_id=f"job-{ordinal}",
            source_file_import_id=f"source-{ordinal}",
            content_version="v1",
        )
        for ordinal in range(count)
    ]


def _claims(wave, intents, ordinals=(0, 2)):
    return [
        types.SimpleNamespace(
            wave_id=wave.wave_id,
            ordinal=ordinal,
            run_id=intents[ordinal].run_id,
            job_id=intents[ordinal].job_id,
            slot=ordinal,
            pod_uid=f"pod-{ordinal}",
            kubernetes_job_uid=wave.kubernetes_job_uid,
            pinned_image_reference=wave.pinned_image_reference,
            pinned_image_digest=wave.pinned_image_digest,
            runtime_image_identity=wave.runtime_image_identity,
            config_identity=wave.kubernetes_config_identity,
            manifest_identity=wave.kubernetes_manifest_identity,
            claim_status="started",
            failure_code=None,
            claim_attempt_token=f"{ordinal + 1:032x}",
        )
        for ordinal in ordinals
    ]


def _runs(wave, intents):
    return [
        types.SimpleNamespace(
            run_id=intent.run_id,
            importer="ptg",
            source_file_import_id=intent.source_file_import_id,
            import_id=intent.source_file_import_id,
            status="queued",
            phase_detail="wave admitted; controller materialization pending",
            started_at=None,
            finished_at=None,
            heartbeat_at=None,
            snapshot_id=None,
            error=None,
            progress={
                "unit": "run",
                "total": 1,
                "done": 0,
                "pct": 0,
                "message": "wave admitted; controller materialization pending",
            },
            metrics={
                "wave_id": wave.wave_id,
                "queue": wave.release_queue,
                "base_queue": wave.queue,
                "worker_class": wave.worker_class,
                "resource_class": wave.resource_class,
                "worker_limit": wave.worker_limit,
                "job_id": intent.job_id,
                "ordinal": intent.ordinal,
                "wave_digest": wave.wave_digest,
            },
        )
        for intent in intents
    ]


def _kubernetes_failure(wave):
    failure_evidence_map = {
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
        "job_name": wave.kubernetes_manifest["metadata"]["name"],
        "job_uid": wave.kubernetes_job_uid,
        "backoff_limit": 0,
        "job_active": 0,
        "job_failed": 12,
        "job_succeeded": 0,
        "job_failure_condition": {"type": "Failed", "status": "True"},
        "failed_slots": [
            {
                "slot": slot["slot"],
                "pod_uid": slot["pod_uid"],
                "phase": "Failed",
                "runtime_image_identity": wave.runtime_image_identity,
            }
            for slot in wave.kubernetes_ready_attestation["slots"]
        ],
    }
    return {
        **failure_evidence_map,
        "attestation_digest": sha256_digest(
            canonical_json(failure_evidence_map)
        ),
    }


def _redis_idle(wave):
    ready_slots = failure._expected_redis_ready_slots(wave)
    release_map = failure._expected_redis_release_mapping(wave, ready_slots)
    release_digest = sha256_digest(canonical_json(release_map))
    wave.redis_release_attestation = {"release_digest": release_digest}
    wave.redis_release_attestation_digest = sha256_digest(
        canonical_json(wave.redis_release_attestation)
    )
    idle_evidence_map = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "target_key_count": 4 + (4 * wave.intent_count),
        "ready_slots": ready_slots,
        "ready_slots_digest": sha256_digest(canonical_json(ready_slots)),
        "release_present": True,
        "release_digest": release_digest,
        "release_receipt": release_map,
        "queued_ordinals": list(range(wave.intent_count)),
        "job_ordinals": list(range(wave.intent_count)),
        "result_ordinals": [],
        "retry_ordinals": [],
        "in_progress_ordinals": [],
        "health_check_present": False,
    }
    return {
        **idle_evidence_map,
        "attestation_digest": sha256_digest(canonical_json(idle_evidence_map)),
    }


def _outcomes(intents):
    return [
        types.SimpleNamespace(
            ordinal=intent.ordinal,
            run_id=intent.run_id,
            job_id=intent.job_id,
            source_file_import_id=intent.source_file_import_id,
            content_version=intent.content_version,
            status="dead_letter",
            snapshot_id=None,
            import_id=None,
        )
        for intent in intents
    ]


def _link(wave, outcomes):
    records = [
        {
            "ordinal": outcome.ordinal,
            "run_id": outcome.run_id,
            "job_id": outcome.job_id,
            "source_file_import_id": outcome.source_file_import_id,
            "content_version": outcome.content_version,
            "status": outcome.status,
            "snapshot_id": outcome.snapshot_id,
            "import_id": outcome.import_id,
        }
        for outcome in outcomes
    ]
    wave.outcomes_digest = failure._outcomes_digest(records)
    unsigned_ack_map = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": linkage_mapping_digest(outcomes),
        "outcomes_digest": wave.outcomes_digest,
    }
    wave.linkage_ack = {
        **unsigned_ack_map,
        "signature": sign_linkage_ack(unsigned_ack_map, key=_KEY),
    }
    wave.linkage_ack_digest = sha256_digest(canonical_json(wave.linkage_ack))


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, result_rows):
        self._result_rows = list(result_rows)
        self.added = []
        self.statements = []

    async def execute(self, statement, _parameters=None):
        self.statements.append(str(statement))
        if not self._result_rows:
            raise AssertionError(f"unexpected SQL: {statement}")
        return _Result(self._result_rows.pop(0))

    def add(self, value):
        self.added.append(value)


@asynccontextmanager
async def _transaction(session):
    yield session


async def _run_snapshot(
    monkeypatch,
    *,
    wave,
    intents,
    claims,
    runs,
    event_rows=(),
):
    session = _Session([intents, claims, runs, event_rows, []])
    transition = AsyncMock()
    monkeypatch.setattr(failure.db, "transaction", lambda: _transaction(session))
    monkeypatch.setattr(failure, "_locked_wave", AsyncMock(return_value=wave))
    monkeypatch.setattr(failure, "_transition", transition)
    digest = await failure.snapshot_claimed_prestart_dead_letter_outcomes(
        wave.wave_id,
        kubernetes_evidence=_kubernetes_failure(wave),
        redis_evidence=_redis_idle(wave),
    )
    return digest, session, transition


@pytest.mark.asyncio
async def test_claim_commit_then_pod_death_dead_letters_all_n_atomically(monkeypatch):
    wave = _wave()
    intents = _intents()
    claims = _claims(wave, intents)
    runs = _runs(wave, intents)

    digest, session, transition = await _run_snapshot(
        monkeypatch,
        wave=wave,
        intents=intents,
        claims=claims,
        runs=runs,
    )

    assert digest == transition.await_args.kwargs["values"]["outcomes_digest"]
    receipt = transition.await_args.kwargs["values"]["failure_receipt"]
    assert receipt["schema_version"] == (
        "healthporta.ptg-wave.claimed-prestart-failure.v1"
    )
    assert receipt["claimed_ordinals"] == [0, 2]
    assert receipt["claimed_ordinals_digest"] == failure._claimed_ordinals_digest(
        wave,
        [0, 2],
    )
    assert receipt["kubernetes_evidence"]["job_failed"] == 12
    assert receipt["redis_evidence"]["result_ordinals"] == []
    assert len(session.added) == wave.intent_count
    assert {run.status for run in runs} == {"dead_letter"}
    assert all(run.error["retryable"] is False for run in runs)
    assert any("ptg_source_attempt_event" in sql for sql in session.statements)


@pytest.mark.asyncio
async def test_released_zero_claim_variant_uses_the_same_strict_prestart_receipt(
    monkeypatch,
):
    wave = _wave(state="released")
    intents = _intents()
    runs = _runs(wave, intents)

    _digest, _session, transition = await _run_snapshot(
        monkeypatch,
        wave=wave,
        intents=intents,
        claims=[],
        runs=runs,
    )

    receipt = transition.await_args.kwargs["values"]["failure_receipt"]
    assert receipt["origin_state"] == "released"
    assert receipt["claimed_ordinals"] == []
    assert receipt["claimed_ordinals_digest"] == failure._claimed_ordinals_digest(
        wave,
        [],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    ["running", "succeeded", "attempt_progress", "execution_event"],
)
async def test_claimed_prestart_holds_before_any_db_change_on_execution_evidence(
    monkeypatch,
    boundary,
):
    wave = _wave()
    intents = _intents()
    claims = _claims(wave, intents)
    runs = _runs(wave, intents)
    event_rows = []
    if boundary == "running":
        runs[0].status = "running"
        runs[0].started_at = object()
    elif boundary == "succeeded":
        runs[0].status = "succeeded"
        runs[0].started_at = object()
        runs[0].finished_at = object()
        runs[0].snapshot_id = "snapshot-0"
    elif boundary == "attempt_progress":
        runs[0].progress["attempt_id"] = "run-0:attempt"
    else:
        event_rows = [("run-0",)]
    session = _Session([intents, claims, runs, event_rows])
    transition = AsyncMock()
    monkeypatch.setattr(failure.db, "transaction", lambda: _transaction(session))
    monkeypatch.setattr(failure, "_locked_wave", AsyncMock(return_value=wave))
    monkeypatch.setattr(failure, "_transition", transition)

    with pytest.raises(
        failure.PTGWaveFailureConflict,
        match="started, progressed|execution marker",
    ):
        await failure.snapshot_claimed_prestart_dead_letter_outcomes(
            wave.wave_id,
            kubernetes_evidence=_kubernetes_failure(wave),
            redis_evidence=_redis_idle(wave),
        )

    assert session.added == []
    assert transition.await_count == 0
    assert runs[1].status == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_change", ["rejected", "replaced_pod"])
async def test_claimed_prestart_holds_on_nonstarted_or_replaced_claim(
    monkeypatch,
    claim_change,
):
    wave = _wave()
    intents = _intents()
    claims = _claims(wave, intents)
    if claim_change == "rejected":
        claims[0].claim_status = "rejected"
        claims[0].failure_code = "ptg_exact_wave_claim_rejected"
    else:
        claims[0].pod_uid = "replacement-pod"
    runs = _runs(wave, intents)
    session = _Session([intents, claims])
    transition = AsyncMock()
    monkeypatch.setattr(failure.db, "transaction", lambda: _transaction(session))
    monkeypatch.setattr(failure, "_locked_wave", AsyncMock(return_value=wave))
    monkeypatch.setattr(failure, "_transition", transition)

    with pytest.raises(failure.PTGWaveFailureConflict, match="execution identity"):
        await failure.snapshot_claimed_prestart_dead_letter_outcomes(
            wave.wave_id,
            kubernetes_evidence=_kubernetes_failure(wave),
            redis_evidence=_redis_idle(wave),
        )

    assert {run.status for run in runs} == {"queued"}
    assert transition.await_count == 0
