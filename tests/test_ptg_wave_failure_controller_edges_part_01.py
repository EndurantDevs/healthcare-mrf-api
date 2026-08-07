# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave failure and controller edge contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_edges import (
    AsyncMock,
    Mock,
    PTGWaveContractError,
    PTGWaveStateConflict,
    PTGWaveWorkerIdentity,
    _CONFIG_DIGEST,
    _IMAGE,
    _JOBS_DIGEST,
    _MANIFEST_DIGEST,
    _MANIFEST_IDENTITY,
    _RUNTIME_IDENTITY,
    _Redis,
    _WAVE_DIGEST,
    _claim,
    _intent,
    _preclaim_evidence,
    _wave,
    canonical_json,
    derive_terminal_state,
    failure_kubernetes,
    failure_receipts,
    failure_snapshots,
    failure_validation,
    fence,
    isolation,
    pytest,
    queue_for_wave,
    receipts,
    run_after_wave_release,
    sha256_digest,
    types,
)


def test_failure_kubernetes_rejects_drifted_preclaim_evidence():
    wave = _wave()
    evidence = _preclaim_evidence(wave)

    assert failure_kubernetes._verify_preclaim_kubernetes_failure(
        wave, evidence
    ) == evidence

    evidence["job_failed"] = 11
    with pytest.raises(
        failure_kubernetes.PTGWaveFailureConflict,
        match="pre-claim Job failure evidence is not exact",
    ):
        failure_kubernetes._verify_preclaim_kubernetes_failure(wave, evidence)

def test_failure_kubernetes_requires_first_preclaim_receipt_to_match():
    wave = _wave()
    evidence = _preclaim_evidence(wave)
    failure_by_field = {"reason": "pre_claim_failure", "evidence": evidence}

    assert failure_kubernetes._verify_failure_kubernetes(
        wave, failure_by_field, evidence
    ) == evidence

    with pytest.raises(
        failure_kubernetes.PTGWaveFailureConflict,
        match="differs from its first attestation",
    ):
        failure_kubernetes._verify_failure_kubernetes(
            wave, failure_by_field, {**evidence, "job_name": "foreign-job"}
        )

@pytest.mark.parametrize(
    ("value", "count", "label"),
    [
        ([0, 1], 2, "queued"),
        ([], 2, "job"),
    ],
)
def test_failure_redis_ordinal_sets_accept_canonical_exact_values(
    value, count, label
):
    assert failure_validation._ordinal_set(value, count, label) == set(value)

@pytest.mark.parametrize(
    "value",
    [
        [1, 0],
        [0, 0],
        [True],
        [2],
        "0",
    ],
)
def test_failure_redis_ordinal_sets_reject_noncanonical_values(value):
    with pytest.raises(
        failure_validation.PTGWaveFailureConflict,
        match="failure Redis queued ordinals are invalid",
    ):
        failure_validation._ordinal_set(value, 2, "queued")

def test_failure_redis_ready_slots_are_empty_before_kubernetes_readiness():
    wave = _wave(kubernetes_ready_attestation=None)

    assert failure_validation._expected_redis_ready_slots(wave) == []

def test_failure_recovery_prefers_get_only_operations_and_rejects_corruption():
    wave = _wave(
        state="cleaning",
        kubernetes_delete_ticket="delete-ticket",
        redis_cleanup_ticket="cleanup-ticket",
    )
    plan = failure_receipts.read_only_recovery_plan(wave)

    assert plan.operation == "kubernetes_delete"
    assert plan.mutation_permitted is False

    wave.kubernetes_delete_evidence_digest = "a" * 64
    assert failure_receipts.read_only_recovery_plan(wave).operation == "redis_cleanup"

    wave.failure_receipt = {}
    wave.failure_receipt_digest = "b" * 64
    with pytest.raises(failure_receipts.PTGWaveFailureConflict):
        failure_receipts._confirmed_failure_reason(wave)

def test_claimed_prestart_snapshot_requires_exact_started_claim_identity():
    wave = _wave()
    intents = [_intent(0), _intent(1)]
    claims = [_claim(wave, intents[0], slot=0)]

    assert failure_snapshots._started_claim_ordinals(wave, intents, claims) == [0]

    claims[0].pod_uid = "foreign-pod"
    with pytest.raises(
        failure_snapshots.PTGWaveFailureConflict,
        match="differs from its admitted execution identity",
    ):
        failure_snapshots._started_claim_ordinals(wave, intents, claims)

def test_claimed_prestart_snapshot_rejects_duplicate_or_invalid_ordinals():
    wave = _wave()
    intents = [_intent(0), _intent(1)]
    first = _claim(wave, intents[0], slot=0)
    duplicate = _claim(wave, intents[0], slot=1)

    with pytest.raises(
        failure_snapshots.PTGWaveFailureConflict,
        match="canonical admitted subset",
    ):
        failure_snapshots._started_claim_ordinals(
            wave, intents, [first, duplicate]
        )

def test_claimed_prestart_run_must_remain_admission_pristine():
    wave = _wave()
    intent = _intent(0)
    run = types.SimpleNamespace(
        run_id=intent.run_id,
        importer="ptg",
        source_file_import_id=intent.source_file_import_id,
        import_id=intent.source_file_import_id,
        status="queued",
        phase_detail="wave admitted; controller materialization pending",
        started_at=None,
        finished_at=None,
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

    assert failure_snapshots._is_prestart_run_pristine(wave, intent, run) is True

    run.started_at = object()
    assert failure_snapshots._is_prestart_run_pristine(wave, intent, run) is False

@pytest.mark.asyncio
async def test_admission_fence_rejects_other_work_and_allows_its_owner(monkeypatch):
    executor = object()
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("wave-synthetic", "executing")]),
    )
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[]))

    with pytest.raises(fence.PTGWaveCapacityConflict, match="reserved"):
        await fence.require_no_capacity_owning_wave(executor)

    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[("run-owned",)]))
    await fence.require_no_capacity_owning_wave(
        executor, owner_run_id="run-owned"
    )

@pytest.mark.asyncio
async def test_admission_fence_rejects_active_nonwave_work(monkeypatch):
    monkeypatch.setattr(
        fence, "_capacity_owning_waves", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[("run-live",)]))

    with pytest.raises(
        fence.PTGWaveCapacityConflict, match="active PTG work prevents"
    ):
        await fence.require_wave_admission_capacity(object())

@pytest.mark.asyncio
async def test_admission_fence_short_circuits_legacy_and_blank_inputs():
    assert await fence.is_ptg_wave_owned_run(object(), "") is False
    assert await fence.is_ptg_wave_owned_run(object(), "   ") is False

@pytest.mark.asyncio
async def test_controller_isolation_rejects_generic_redis_work():
    controller = types.SimpleNamespace(
        _PTG_BASE_QUEUES=("arq:PTGSmall",),
        health_check_key_suffix=":health-check",
        PTGWaveControllerHold=RuntimeError,
    )

    with pytest.raises(RuntimeError, match="queue is not empty"):
        await isolation._require_generic_redis_idle(
            controller, _Redis([1, None])
        )

    with pytest.raises(RuntimeError, match="health key is present"):
        await isolation._require_generic_redis_idle(
            controller, _Redis([0, "healthy"])
        )

    await isolation._require_generic_redis_idle(controller, _Redis([0, None]))

@pytest.mark.parametrize(
    ("job", "expected"),
    [
        ({}, True),
        ({"status": {"succeeded": 1}}, False),
        ({"status": {"failed": 1}}, False),
        ({"status": {"active": 1}}, True),
        ({"metadata": {"deletionTimestamp": "now"}}, True),
    ],
)
def test_controller_isolation_identifies_generic_capacity_ownership(
    job, expected
):
    assert isolation.is_generic_job_nonterminal(job) is expected

def test_controller_receipts_bind_cleanup_and_absence_to_persisted_wave():
    wave = _wave(
        terminal_summary={"redis_pre_cleanup": {"queue_entry_count": 0}},
        redis_cleanup_ticket="cleanup-ticket",
    )
    absence = receipts.kubernetes_absence_receipt(
        wave,
        {"job_absent": True, "pod_count": 0, "pods_absent": True},
    )
    cleanup = receipts.redis_cleanup_receipt(
        wave,
        {"owner": False, "operation_ticket": "reconcile-ticket"},
        None,
        types.SimpleNamespace(as_mapping=lambda: {"queue_entry_count": 0}),
    )

    assert absence["operation_ticket"] == "delete-ticket"
    assert len(absence["observation_digest"]) == 64
    assert cleanup["mode"] == "get_only_reconciled"
    assert cleanup["operation_ticket"] == "reconcile-ticket"
    assert receipts.operation_ticket("cleanup").startswith("cleanup:")

def test_unclaimed_failure_receipt_binds_all_ordinals_and_evidence():
    wave = _wave()
    evidence_by_field = {"job_absent": True}
    receipt = receipts.unclaimed_failure_receipt(
        wave,
        origin_state="slots_waiting",
        reason="kubernetes_post_absent",
        operation="kubernetes_post",
        operation_ticket="post-ticket",
        evidence=evidence_by_field,
    )

    assert receipt["wave_id"] == wave.wave_id
    assert receipt["evidence_digest"] == sha256_digest(canonical_json(evidence_by_field))
    assert len(receipt["unclaimed_ordinals_digest"]) == 64

@pytest.mark.asyncio
async def test_barrier_starts_only_after_exact_release():
    identity = PTGWaveWorkerIdentity(
        wave_digest=_WAVE_DIGEST,
        queue=queue_for_wave(_WAVE_DIGEST),
        worker_class="process.PTGSmall",
        slot_index=0,
        pod_uid="pod-synthetic",
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        job_count=2,
        config_identity=_CONFIG_DIGEST,
        manifest_identity=_MANIFEST_IDENTITY,
        image_identity=_IMAGE,
        runtime_image_identity=_RUNTIME_IDENTITY,
    )
    release_by_field = {
        "released": True,
        "wave_digest": identity.wave_digest,
        "queue": identity.queue,
        "worker_class": identity.worker_class,
        "manifest_digest": identity.manifest_digest,
        "jobs_digest": identity.jobs_digest,
        "job_count": identity.job_count,
        "config_identity": identity.config_identity,
        "manifest_identity": identity.manifest_identity,
        "image_identity": identity.image_identity,
        "runtime_image_identity": identity.runtime_image_identity,
    }
    barrier = types.SimpleNamespace(
        register_ready=AsyncMock(),
        wait_for_release=AsyncMock(return_value=release_by_field),
    )
    start = Mock(return_value="started")

    assert await run_after_wave_release(identity, barrier, start) == "started"
    barrier.register_ready.assert_awaited_once_with(identity)
    start.assert_called_once_with()

    release_by_field["queue"] = "arq:foreign"
    with pytest.raises(PTGWaveContractError, match="queue"):
        await run_after_wave_release(identity, barrier, start)

@pytest.mark.parametrize(
    ("statuses", "failure_digest", "expected"),
    [
        (["succeeded", "succeeded"], None, "succeeded"),
        (["failed", "failed"], None, "failed"),
        (["canceled", "failed"], None, "canceled"),
        (["dead_letter", "failed"], None, "dead_letter"),
        (["dead_letter", "dead_letter"], "a" * 64, "dead_letter"),
    ],
)
def test_terminal_state_reduces_only_exact_terminal_outcomes(
    statuses, failure_digest, expected
):
    wave = _wave(
        failure_receipt_digest=failure_digest,
        intent_count=len(statuses),
    )
    terminal_values = [
        types.SimpleNamespace(ordinal=ordinal, status=status)
        for ordinal, status in enumerate(statuses)
    ]

    assert derive_terminal_state(wave, terminal_values) == expected

def test_terminal_state_rejects_missing_or_nonterminal_outcomes():
    wave = _wave(intent_count=2)

    with pytest.raises(PTGWaveStateConflict, match="every exact outcome"):
        derive_terminal_state(
            wave, [types.SimpleNamespace(ordinal=1, status="failed")]
        )

    with pytest.raises(PTGWaveStateConflict, match="only terminal"):
        derive_terminal_state(
            wave,
            [
                types.SimpleNamespace(ordinal=0, status="running"),
                types.SimpleNamespace(ordinal=1, status="failed"),
            ],
        )
