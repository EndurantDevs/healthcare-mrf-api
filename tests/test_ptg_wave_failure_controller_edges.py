

"""Focused failure, fence, receipt, and terminal-edge contracts."""


from __future__ import annotations


import types


from unittest.mock import AsyncMock, Mock


import pytest


from sanic.exceptions import BadRequest, NotFound


from api import control_wave_linkage_route as linkage_routes


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
    def __init__(self, *, json=None, args=None, headers=None, body=b""):
        self.json = json
        self.args = {} if args is None else args
        self.headers = {} if headers is None else headers
        self.body = body


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

def test_outcome_contract_rejects_bad_linkage_and_claim_disposition():
    intent = _intent(0)
    successful_run = types.SimpleNamespace(
        status="succeeded",
        snapshot_id="snapshot-synthetic",
        import_id=intent.source_file_import_id,
    )
    assert outcomes._outcome_record(intent, successful_run)["status"] == "succeeded"

    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict, match="lacks snapshot evidence"
    ):
        outcomes._outcome_record(
            intent,
            types.SimpleNamespace(
                status="succeeded",
                snapshot_id=None,
                import_id=intent.source_file_import_id,
            ),
        )

    claim = types.SimpleNamespace(
        ordinal=0, claim_status="rejected", failure_code="synthetic_failure"
    )
    assert outcomes._validate_claim_outcomes(
        [claim], [{"ordinal": 0, "status": "failed"}]
    ) == [0]

    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="disposition does not match",
    ):
        outcomes._validate_claim_outcomes(
            [claim], [{"ordinal": 0, "status": "succeeded"}]
        )

def test_outcome_contract_validates_exact_signed_linkage_ack():
    wave = _wave(intent_count=1, outcomes_digest="a" * 64)
    outcome = _outcome(_intent(0))
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": outcomes.linkage_mapping_digest([outcome]),
        "outcomes_digest": wave.outcomes_digest,
    }
    ack_by_field = {
        **unsigned_by_field,
        "signature": outcomes.sign_linkage_ack(unsigned_by_field, key=_LINKAGE_KEY),
    }

    _, digest = outcomes._validate_linkage_ack(
        wave, [outcome], ack_by_field, _LINKAGE_KEY
    )
    assert len(digest) == 64

    ack_by_field["mapping_digest"] = "b" * 64
    ack_by_field["signature"] = outcomes.sign_linkage_ack(
        {name: field_value for name, field_value in ack_by_field.items() if name != "signature"},
        key=_LINKAGE_KEY,
    )
    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="does not cover every exact outcome",
    ):
        outcomes._validate_linkage_ack(wave, [outcome], ack_by_field, _LINKAGE_KEY)

@pytest.mark.asyncio
async def test_control_wave_routes_translate_admission_and_lookup(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", Mock())
    monkeypatch.setattr(
        routes,
        "admit_import_wave",
        AsyncMock(return_value=({"wave_id": "wave-synthetic"}, True)),
    )
    response = await routes.control_admit_import_wave(
        _Request(json={"wave": "synthetic"})
    )
    assert response.status == 201

    monkeypatch.setattr(routes, "get_import_wave", AsyncMock(return_value=None))
    with pytest.raises(NotFound):
        await routes.control_get_import_wave(_Request(), "missing-wave")

    monkeypatch.setattr(
        routes,
        "get_import_wave",
        AsyncMock(return_value={"wave_id": "wave-synthetic"}),
    )
    response = await routes.control_get_import_wave(
        _Request(), "wave-synthetic"
    )
    assert response.status == 200

@pytest.mark.asyncio
async def test_control_wave_routes_require_exact_linkage_payload(monkeypatch):
    monkeypatch.setattr(linkage_routes, "require_control_auth", Mock())

    with pytest.raises(BadRequest, match="only linkage_ack"):
        await linkage_routes.control_record_import_wave_linkage(
            _Request(json={"linkage_ack": {}, "extra": True}), "wave-synthetic"
        )

    monkeypatch.setattr(
        linkage_routes, "record_linkage_ack", AsyncMock(return_value="a" * 64)
    )
    response = await linkage_routes.control_record_import_wave_linkage(
        _Request(json={"linkage_ack": {"synthetic": True}}), "wave-synthetic"
    )
    assert response.status == 200

def test_control_helpers_validate_exact_payload_lane_and_rebuild_scope(monkeypatch):
    assert ptg_control._is_complete_exact_wave_payload(
        {
            "_wave_id": "wave-synthetic",
            "_wave_digest": _WAVE_DIGEST,
            "_wave_job_id": "job-synthetic",
        }
    )
    assert not ptg_control._is_complete_exact_wave_payload({})

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "arq:expected")
    ptg_control._assert_expected_lane({"_expected_queue": "arq:expected"})
    with pytest.raises(RuntimeError, match="expected"):
        ptg_control._assert_expected_lane({"_expected_queue": "arq:foreign"})

    assert ptg_control._full_rebuild_scope_digest({}) is None
    assert ptg_control._full_rebuild_scope_digest(
        {"_full_rebuild_scope_digest": "a" * 64}
    ) == "a" * 64
    with pytest.raises(ValueError, match="scope digest"):
        ptg_control._full_rebuild_scope_digest(
            {"_full_rebuild_scope_digest": "not-a-digest"}
        )
    with pytest.raises(ValueError, match="only an internal"):
        ptg_control._full_rebuild_scope_digest({"_full_rebuild_token": "opaque"})

    assert ptg_control._full_rebuild_proof_metrics_by_name(None) == {}
    assert ptg_control._full_rebuild_proof_metrics_by_name("a" * 64) == {
        "full_rebuild_requested": True,
        "raw_artifact_reuse_forced_off": True,
        "partial_artifact_retention_forced_off": True,
    }

def test_failure_kubernetes_covers_claimed_post_absence_and_delete_paths():
    wave = _wave()
    preclaim = _preclaim_evidence(wave)
    claimed_by_field = {
        "schema_version": failure_types.CLAIMED_PRESTART_FAILURE_SCHEMA,
        "kubernetes_evidence": preclaim,
    }
    assert failure_kubernetes._verify_failure_kubernetes(
        wave, claimed_by_field, preclaim
    ) == preclaim
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="claimed-prestart"):
        failure_kubernetes._verify_failure_kubernetes(
            wave, claimed_by_field, {**preclaim, "job_name": "foreign"}
        )

    wave = _wave(kubernetes_job_uid=None, kubernetes_job_receipt_digest=None)
    post_evidence_by_field = {
        "wave_digest": wave.wave_digest,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "job_name": "ptg-wave-synthetic",
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    post_failure_by_field = {"reason": "kubernetes_post_absent", "evidence": post_evidence_by_field}
    assert failure_kubernetes._verify_failure_kubernetes(
        wave, post_failure_by_field, post_evidence_by_field
    ) == post_evidence_by_field
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="GET receipt"):
        failure_kubernetes._verify_failure_kubernetes(
            wave, post_failure_by_field, {"job_absent": True}
        )

    wave = _wave()
    absence = _absence_evidence(wave)
    wave.kubernetes_delete_evidence = absence
    assert failure_kubernetes._verify_failure_kubernetes(
        wave, {"reason": "redis_release_absent"}, absence
    ) == absence
    absence["pod_count"] = 1
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not exact"):
        failure_kubernetes._verify_failure_kubernetes(
            wave, {"reason": "redis_release_absent"}, absence
        )

def test_failure_kubernetes_absence_requires_exact_digest_bound_mapping():
    wave = _wave()
    evidence = _absence_evidence(wave)
    assert failure_kubernetes._verify_kubernetes_absence(wave, evidence) == evidence

    evidence["observation_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not exact"):
        failure_kubernetes._verify_kubernetes_absence(wave, evidence)

def test_failure_recovery_plan_requires_next_read_only_step():
    wave = _wave(
        kubernetes_delete_evidence_digest="a" * 64,
        redis_cleanup_evidence_digest="b" * 64,
        redis_release_ticket="release-ticket",
        kubernetes_job_receipt_digest=None,
    )
    assert failure_receipts.read_only_recovery_plan(wave).operation == "redis_release"
    wave.redis_release_attestation_digest = "c" * 64
    assert failure_receipts.read_only_recovery_plan(wave).operation == "kubernetes_post"
    wave.kubernetes_job_receipt_digest = "d" * 64
    assert failure_receipts.read_only_recovery_plan(wave) is None


def test_unclaimed_post_receipt_binds_exact_wave():
    post_wave = _wave(
        state="slots_waiting",
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
    )
    post_evidence_by_field = {
        "wave_digest": post_wave.wave_digest,
        "manifest_identity": post_wave.kubernetes_manifest_identity,
        "job_name": "ptg-wave-synthetic",
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    post = _unclaimed_receipt(
        post_wave,
        reason="kubernetes_post_absent",
        evidence=post_evidence_by_field,
        origin_state="slots_waiting",
        operation="kubernetes_post",
        ticket=post_wave.k8s_post_ticket,
    )
    assert failure_receipts._require_unclaimed_failure_receipt(
        post_wave, post, require_origin_state=True
    ) == post
    post["wave_id"] = "foreign-wave"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not bind"):
        failure_receipts._require_unclaimed_failure_receipt(
            post_wave, post, require_origin_state=False
        )


def test_unclaimed_release_receipt_requires_absence(monkeypatch):
    redis_wave = _wave(
        state="redis_releasing",
        redis_release_ticket="release-ticket",
    )
    redis_evidence_by_field = {"redis": "observed"}
    redis = _unclaimed_receipt(
        redis_wave,
        reason="redis_release_absent",
        evidence=redis_evidence_by_field,
        origin_state="redis_releasing",
        operation="redis_release",
        ticket="release-ticket",
    )
    verify_redis = Mock()
    monkeypatch.setattr(failure_receipts, "_verify_failure_redis", verify_redis)
    assert failure_receipts._require_unclaimed_failure_receipt(
        redis_wave, redis, require_origin_state=True
    ) == redis
    verify_redis.assert_called_once_with(
        redis_wave, redis, redis_evidence_by_field, require_release_absent=True
    )


def test_unclaimed_preclaim_receipt_requires_digest():
    preclaim_wave = _wave(state="executing")
    preclaim = _unclaimed_receipt(
        preclaim_wave,
        reason="pre_claim_failure",
        evidence=_preclaim_evidence(preclaim_wave),
        origin_state="executing",
        operation="worker_start",
        ticket=None,
    )
    assert failure_receipts._require_unclaimed_failure_receipt(
        preclaim_wave, preclaim, require_origin_state=True
    ) == preclaim
    preclaim["evidence_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is invalid"):
        failure_receipts._require_unclaimed_failure_receipt(
            preclaim_wave, preclaim, require_origin_state=False
        )

def test_claimed_failure_receipts_validate_dispatch_ordinals_and_evidence(monkeypatch):
    wave = _wave(state="executing")
    receipt = _claimed_receipt(wave, claimed_ordinals=[0])
    monkeypatch.setattr(failure_receipts, "_verify_preclaim_kubernetes_failure", Mock())
    monkeypatch.setattr(failure_receipts, "_verify_failure_redis", Mock())

    assert failure_receipts._require_claimed_prestart_failure_receipt(
        wave, receipt, require_origin_state=True
    ) == receipt
    assert failure_receipts._require_failure_receipt(
        wave, receipt, require_origin_state=True
    ) == receipt

    wrong_state = _claimed_receipt(
        wave, claimed_ordinals=[0], origin_state="released"
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflicts"):
        failure_receipts._require_claimed_prestart_failure_receipt(
            wave, wrong_state, require_origin_state=False
        )

    invalid_ordinals = _claimed_receipt(wave, claimed_ordinals=[1, 0])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="invalid claimed"):
        failure_receipts._require_claimed_prestart_failure_receipt(
            wave, invalid_ordinals, require_origin_state=False
        )

    bad_evidence = _claimed_receipt(wave, claimed_ordinals=[0])
    bad_evidence["redis_evidence_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="evidence digest"):
        failure_receipts._validated_claimed_evidence(bad_evidence)

def test_failure_validation_verifies_signed_linkage_ack():
    wave = _wave(intent_count=1, outcomes_digest="a" * 64)
    terminal_outcome = _outcome(_intent(0), status="dead_letter")
    unsigned_ack_by_field = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": outcomes.linkage_mapping_digest([terminal_outcome]),
        "outcomes_digest": wave.outcomes_digest,
    }
    wave.linkage_ack = {
        **unsigned_ack_by_field,
        "signature": outcomes.sign_linkage_ack(unsigned_ack_by_field, key=_LINKAGE_KEY),
    }
    wave.linkage_ack_digest = sha256_digest(canonical_json(wave.linkage_ack))
    failure_validation._verify_linkage(
        wave, [terminal_outcome], key=_LINKAGE_KEY
    )
    wave.linkage_ack_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not cover"):
        failure_validation._verify_linkage(
            wave, [terminal_outcome], key=_LINKAGE_KEY
        )
    wave.linkage_ack = None
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="requires linkage"):
        failure_validation._verify_linkage(
            wave, [terminal_outcome], key=_LINKAGE_KEY
        )


def test_failure_validation_rejects_bad_redis_envelope():
    wave = _wave(kubernetes_ready_attestation=None)
    receipt = _redis_failure_receipt(wave)
    assert failure_validation._validate_redis_receipt_envelope(
        wave, {"reason": "kubernetes_post_absent"}, receipt
    ) == (receipt, False)
    receipt["health_check_present"] = 1
    receipt["attestation_digest"] = sha256_digest(canonical_json({
        name: field_value
        for name, field_value in receipt.items()
        if name != "attestation_digest"
    }))
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="health-check"):
        failure_validation._validate_redis_receipt_envelope(
            wave, {"reason": "kubernetes_post_absent"}, receipt
        )


def test_failure_validation_requires_canonical_ready_membership():
    ready_wave = _wave()
    expected_ready_slots = failure_validation._expected_redis_ready_slots(ready_wave)
    failure_validation._validate_partial_ready_membership(
        expected_ready_slots, expected_ready_slots[:1]
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="membership is invalid"):
        failure_validation._validate_partial_ready_membership(expected_ready_slots, "invalid")
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="canonical Kubernetes"):
        failure_validation._validate_partial_ready_membership(
            expected_ready_slots, list(reversed(expected_ready_slots[:2]))
        )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="lacks exact"):
        failure_validation._expected_redis_ready_slots(
            _wave(kubernetes_ready_attestation={"slots": []})
        )


def test_failure_validation_rejects_release_presence():
    wave = _wave(kubernetes_ready_attestation=None)
    lifecycle = failure_validation.FailureRedisOrdinals(
        queued=set(),
        jobs=set(),
        results=set(),
        retries=set(),
        in_progress=set(),
    )
    failure_validation._validate_redis_release(
        wave,
        {"reason": "kubernetes_post_absent"},
        _redis_failure_receipt(wave),
        [],
        lifecycle,
        require_release_absent=True,
    )
    invalid_release = _redis_failure_receipt(wave, release_present=True)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="presence conflicts"):
        failure_validation._validate_redis_release(
            wave,
            {"reason": "kubernetes_post_absent"},
            invalid_release,
            [],
            lifecycle,
            require_release_absent=True,
        )

@pytest.mark.asyncio
async def test_failure_snapshot_unclaimed_paths_and_row_guards(monkeypatch):
    wave = _wave(state="slots_waiting")
    intents = [_intent(0), _intent(1)]
    session = _SequenceSession([[], intents, []])
    runs = [types.SimpleNamespace(status="failed"), types.SimpleNamespace(status="failed")]
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=runs)
    )
    assert await failure_snapshots._unclaimed_snapshot_rows(
        session, wave, wave.wave_id
    ) == (intents, runs)

    claimed_session = _SequenceSession([[0]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="claimed wave"):
        await failure_snapshots._unclaimed_snapshot_rows(
            claimed_session, wave, wave.wave_id
        )

    missing_intents = _SequenceSession([[], [_intent(0)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        await failure_snapshots._unclaimed_snapshot_rows(
            missing_intents, wave, wave.wave_id
        )

    complete_intents = [_intent(0), _intent(1)]
    short_runs = _SequenceSession([[], complete_intents])
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=[object()])
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted ImportRun"):
        await failure_snapshots._unclaimed_snapshot_rows(
            short_runs, wave, wave.wave_id
        )

    succeeded_runs = [
        types.SimpleNamespace(status="succeeded"),
        types.SimpleNamespace(status="failed"),
    ]
    succeeded = _SequenceSession([[], complete_intents])
    monkeypatch.setattr(
        failure_snapshots, "_locked_wave_runs", AsyncMock(return_value=succeeded_runs)
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="successful"):
        await failure_snapshots._unclaimed_snapshot_rows(
            succeeded, wave, wave.wave_id
        )

    existing = _SequenceSession([[0]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="already exist"):
        await failure_snapshots._require_no_existing_outcomes(existing, wave.wave_id)

@pytest.mark.asyncio
async def test_failure_snapshot_wrappers_event_markers_and_claimed_guards(monkeypatch):
    receipt_by_field = {"receipt": "synthetic"}
    wave = _wave(
        state="awaiting_linkage",
        failure_receipt_digest=sha256_digest(canonical_json(receipt_by_field)),
        outcomes_digest="a" * 64,
    )
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    assert await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
        facade, wave.wave_id, failure_receipt=receipt_by_field
    ) == wave.outcomes_digest

    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflicts"):
        await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
            facade, wave.wave_id, failure_receipt=receipt_by_field
        )

    wave.state = "foreign"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not expected"):
        await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
            facade, wave.wave_id, failure_receipt=receipt_by_field
        )

    marker_session = _SequenceSession([
        [types.SimpleNamespace(_mapping={"outer_run_id": "run-synthetic-1"})]
    ])
    assert await failure_snapshots._worker_start_event_ordinals(
        marker_session, [_intent(0), _intent(1)]
    ) == [1]
    invalid_marker_session = _SequenceSession([[("foreign-run",)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="observation is invalid"):
        await failure_snapshots._worker_start_event_ordinals(
            invalid_marker_session, [_intent(0)]
        )

    claimed_wave = _wave(state="released")
    claimed_session = _SequenceSession([[_intent(0)]])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        await failure_snapshots._claimed_snapshot_rows(
            claimed_session, claimed_wave, claimed_wave.wave_id
        )

    intents = [_intent(0), _intent(1)]
    claimed_rows = _SequenceSession([intents, [_claim(claimed_wave, intents[0])]])
    monkeypatch.setattr(
        failure_snapshots,
        "_locked_wave_runs",
        AsyncMock(return_value=[object(), object()]),
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflict"):
        await failure_snapshots._claimed_snapshot_rows(
            claimed_rows, claimed_wave, claimed_wave.wave_id
        )

@pytest.mark.asyncio
async def test_failure_facade_and_persistence_write_exact_dead_letter_state(monkeypatch):
    monkeypatch.setattr(failure, "_snapshot_unclaimed", AsyncMock(return_value="digest"))
    assert await failure.snapshot_unclaimed_dead_letter_outcomes(
        "wave-synthetic", failure_receipt={"synthetic": True}
    ) == "digest"

    monkeypatch.setattr(
        failure_persistence,
        "PTGImportWaveOutcome",
        lambda **kwargs: types.SimpleNamespace(**kwargs),
    )
    transition = AsyncMock()
    facade = types.SimpleNamespace(_transition=transition)
    wave = _wave()
    intents = [_intent(0), _intent(1)]
    runs = [types.SimpleNamespace(), types.SimpleNamespace()]
    snapshot = failure_persistence.DeadLetterSnapshot(
        session=_SequenceSession([]),
        wave=wave,
        wave_id=wave.wave_id,
        intents=intents,
        runs=runs,
        receipt={"failure": "synthetic"},
        receipt_digest="a" * 64,
        is_claimed_prestart=False,
    )
    digest = await failure_persistence.persist_dead_letter_snapshot(facade, snapshot)
    assert len(digest) == 64
    assert all(run.status == "dead_letter" for run in runs)
    assert all("worker claim" in run.phase_detail for run in runs)
    transition.assert_awaited_once()

    claimed_run = types.SimpleNamespace()
    failure_persistence._dead_letter_runs(
        [claimed_run], object(), is_claimed_prestart=True
    )
    assert claimed_run.error["code"] == "ptg_exact_wave_claimed_prestart_failure"
    assert claimed_run.progress["message"] == "dead letter"

def test_failure_terminal_and_type_helpers_reject_any_nonexact_evidence(monkeypatch):
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="SHA-256"):
        failure_types._digest("invalid", "synthetic")
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="contiguous"):
        failure_types._rows_by_ordinal([types.SimpleNamespace(ordinal=1)])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="object"):
        failure_types._require_mapping([], "synthetic")

    wave = _wave(intent_count=1)
    intent = _intent(0)
    outcome = _outcome(intent, status="dead_letter")
    dead_letter_records = failure_terminal._dead_letter_records(
        [intent], [outcome], "not dead letter"
    )
    wave.outcomes_digest = failure_types._outcomes_digest(dead_letter_records)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="every admitted"):
        failure_terminal._require_exact_coverage(wave, [], [], "every admitted")
    outcome.status = "failed"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not dead letter"):
        failure_terminal._dead_letter_records(
            [intent], [outcome], "not dead letter"
        )

    receipt_by_field = {"claimed_ordinals": [0]}
    wave.failure_receipt = receipt_by_field
    wave.failure_receipt_digest = sha256_digest(canonical_json(receipt_by_field))
    monkeypatch.setattr(
        failure_terminal,
        "_require_claimed_prestart_failure_receipt",
        Mock(return_value=receipt_by_field),
    )
    assert failure_terminal._claimed_failure_receipt(wave, [0])[0] == receipt_by_field
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="differ"):
        failure_terminal._claimed_failure_receipt(wave, [])
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is corrupt"):
        failure_terminal._claimed_failure_receipt(wave, [0])

    monkeypatch.setattr(
        failure_terminal, "_verify_failure_kubernetes", Mock(return_value={"k": 1})
    )
    monkeypatch.setattr(
        failure_terminal, "_verify_failure_redis", Mock(return_value={"r": 1})
    )
    assert failure_terminal._terminal_receipt_evidence(
        wave,
        receipt_by_field,
        {"kubernetes": {}, "redis": {}},
        receipt_name="synthetic",
        fields_error="exact fields",
    ) == ({"k": 1}, {"r": 1})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="exact fields"):
        failure_terminal._terminal_receipt_evidence(
            wave,
            receipt_by_field,
            {"kubernetes": {}},
            receipt_name="synthetic",
            fields_error="exact fields",
        )

@pytest.mark.asyncio
async def test_unclaimed_failure_snapshot_replays_and_rejects_invalid_slots(monkeypatch):
    receipt_by_field = {"receipt": "synthetic"}
    wave = _wave(state="slots_waiting")
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    monkeypatch.setattr(
        failure_snapshots,
        "_require_unclaimed_failure_receipt",
        Mock(return_value=receipt_by_field),
    )
    monkeypatch.setattr(
        failure_snapshots, "_unclaimed_snapshot_rows", AsyncMock(return_value=([], []))
    )
    persist = AsyncMock(return_value="digest")
    monkeypatch.setattr(failure_snapshots, "persist_dead_letter_snapshot", persist)
    assert await failure_snapshots.snapshot_unclaimed_dead_letter_outcomes(
        facade, wave.wave_id, failure_receipt=receipt_by_field
    ) == "digest"
    persist.assert_awaited_once()

    with pytest.raises(failure_types.PTGWaveFailureConflict, match="12-slot"):
        failure_snapshots._ready_slots_by_number(
            _wave(kubernetes_ready_attestation={"slots": []})
        )
    malformed = _wave()
    malformed.kubernetes_ready_attestation["slots"][0]["slot"] = 12
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="12-slot"):
        failure_snapshots._ready_slots_by_number(malformed)


@pytest.mark.asyncio
async def test_claimed_failure_snapshot_binds_evidence_and_state(monkeypatch):
    wave = _wave(state="slots_waiting")
    session = object()
    facade = types.SimpleNamespace(
        db=types.SimpleNamespace(transaction=lambda: _Transaction(session)),
        _locked_wave=AsyncMock(return_value=wave),
    )
    kubernetes_evidence, redis_evidence = {"k": 1}, {"r": 1}
    claimed_receipt_by_field = {
        "kubernetes_evidence": kubernetes_evidence,
        "redis_evidence": redis_evidence,
    }
    wave.state = "awaiting_linkage"
    wave.outcomes_digest = "a" * 64
    monkeypatch.setattr(
        failure_snapshots,
        "_require_claimed_prestart_failure_receipt",
        Mock(return_value=claimed_receipt_by_field),
    )
    assert failure_snapshots._existing_claimed_outcomes_digest(
        wave, kubernetes_evidence, redis_evidence
    ) == wave.outcomes_digest
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="conflicts"):
        failure_snapshots._existing_claimed_outcomes_digest(
            wave, {"k": "foreign"}, redis_evidence
        )
    monkeypatch.setattr(
        failure_snapshots, "_existing_claimed_outcomes_digest", Mock(return_value="digest")
    )
    assert await failure_snapshots.snapshot_claimed_prestart_dead_letter_outcomes(
        facade,
        wave.wave_id,
        kubernetes_evidence=kubernetes_evidence,
        redis_evidence=redis_evidence,
    ) == "digest"
    wave.state = "slots_waiting"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="not expected"):
        await failure_snapshots.snapshot_claimed_prestart_dead_letter_outcomes(
            facade,
            wave.wave_id,
            kubernetes_evidence=kubernetes_evidence,
            redis_evidence=redis_evidence,
        )


def test_failure_receipt_confirmation_requires_valid_digest(monkeypatch):
    wave = _wave()
    confirmed_by_field = {"reason": "pre_claim_failure"}
    wave.failure_receipt = confirmed_by_field
    wave.failure_receipt_digest = sha256_digest(canonical_json(confirmed_by_field))
    monkeypatch.setattr(
        failure_receipts, "_require_failure_receipt", Mock(return_value=confirmed_by_field)
    )
    assert failure_receipts._confirmed_failure_reason(wave) == "pre_claim_failure"
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is corrupt"):
        failure_receipts._confirmed_failure_reason(wave)


def test_unclaimed_receipt_rejects_invalid_post_absence():
    wave = _wave()
    envelope = _unclaimed_receipt(
        wave,
        reason="pre_claim_failure",
        evidence=_preclaim_evidence(wave),
        origin_state="released",
        operation="worker_start",
        ticket=None,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="origin state"):
        failure_receipts._validate_unclaimed_receipt_envelope(
            wave, envelope, require_origin_state=True
        )

    post_wave = _wave(
        state="slots_waiting", kubernetes_job_uid=None, kubernetes_job_receipt_digest=None
    )
    post = _unclaimed_receipt(
        post_wave,
        reason="kubernetes_post_absent",
        evidence={},
        origin_state="slots_waiting",
        operation="kubernetes_post",
        ticket=post_wave.k8s_post_ticket,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="POST absence"):
        failure_receipts._validate_kubernetes_post_absence(post_wave, post, {})


def test_failure_receipt_rejects_invalid_release_and_preclaim_absence():
    wave = _wave()
    redis_wave = _wave(state="redis_releasing", redis_release_ticket="release-ticket")
    redis = _unclaimed_receipt(
        redis_wave,
        reason="redis_release_absent",
        evidence={},
        origin_state="foreign",
        operation="redis_release",
        ticket="release-ticket",
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="Redis release"):
        failure_receipts._validate_redis_release_absence(redis_wave, redis, {})

    preclaim = _unclaimed_receipt(
        wave,
        reason="pre_claim_failure",
        evidence=_preclaim_evidence(wave),
        origin_state="executing",
        operation="foreign",
        ticket=None,
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="pre-claim"):
        failure_receipts._validate_preclaim_failure(wave, preclaim, preclaim["evidence"])


def test_claimed_failure_receipt_requires_bound_operation_and_state():
    wave = _wave()
    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    claimed["operation"] = "foreign"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not bind"):
        failure_receipts._validate_claimed_receipt_envelope(
            wave, claimed, require_origin_state=False
        )
    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    wave.state = "released"
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="origin state"):
        failure_receipts._validate_claimed_receipt_envelope(
            wave, claimed, require_origin_state=True
        )

def test_failure_validation_residual_fail_closed_branches(monkeypatch):
    wave = _wave(linkage_ack={}, linkage_ack_digest="a" * 64)
    monkeypatch.setattr(
        failure_validation,
        "_validate_linkage_ack",
        Mock(side_effect=failure_validation.PTGWaveStateConflict("invalid")),
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not cover"):
        failure_validation._verify_linkage(wave, [], key=_LINKAGE_KEY)

    claimed = _claimed_receipt(wave, claimed_ordinals=[0])
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="differs"):
        failure_validation._validate_redis_receipt_envelope(
            wave, claimed, _redis_failure_receipt(wave)
        )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="fields are not exact"):
        failure_validation._validate_redis_receipt_envelope(wave, {}, {})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="does not bind"):
        failure_validation._validate_redis_receipt_envelope(
            wave,
            {},
            _redis_failure_receipt(wave, wave_id="foreign"),
        )
    corrupt = _redis_failure_receipt(wave)
    corrupt["attestation_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="digest is invalid"):
        failure_validation._validate_redis_receipt_envelope(wave, {}, corrupt)

    no_ready_wave = _wave(kubernetes_ready_attestation=None)
    corrupt_ready = _redis_failure_receipt(no_ready_wave)
    corrupt_ready["ready_slots_digest"] = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="membership digest"):
        failure_validation._validate_redis_ready_membership(
            no_ready_wave,
            {"reason": "redis_release_absent", "origin_state": "redis_releasing"},
            corrupt_ready,
            False,
        )

def test_failure_terminal_and_kubernetes_residual_fail_closed_branches(monkeypatch):
    intent = _intent(0)
    outcome = _outcome(intent, status="dead_letter")
    receipt_by_field = {"failure": "synthetic"}
    wave = _wave(intent_count=1, outcomes_digest="0" * 64)
    monkeypatch.setattr(
        failure_terminal, "_require_unclaimed_failure_receipt", Mock(return_value=receipt_by_field)
    )
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="outcomes digest"):
        failure_terminal.verify_unclaimed_dead_letter_terminal_eligibility(
            wave, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )
    dead_letter_records = failure_terminal._dead_letter_records([intent], [outcome], "invalid")
    wave.outcomes_digest = failure_types._outcomes_digest(dead_letter_records)
    wave.failure_receipt_digest = "0" * 64
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="receipt digest"):
        failure_terminal.verify_unclaimed_dead_letter_terminal_eligibility(
            wave, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )

    claimed = _wave(intent_count=1, outcomes_digest="0" * 64)
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="outcomes digest"):
        failure_terminal.verify_claimed_prestart_terminal_eligibility(
            claimed, [intent], [], [outcome], {}, key=_LINKAGE_KEY
        )

    delete_wave = _wave(kubernetes_delete_evidence={"observed": True})
    with pytest.raises(failure_types.PTGWaveFailureConflict, match="persisted receipt"):
        failure_kubernetes._verify_failure_kubernetes(
            delete_wave, {"reason": "redis_release_absent"}, {"observed": False}
        )
