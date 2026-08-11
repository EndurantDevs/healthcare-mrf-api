# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave failure and controller edge contracts."""

from __future__ import annotations

from api import control_wave_linkage_route as linkage_routes
from tests.test_ptg_wave_failure_controller_edges import (
    AsyncMock,
    BadRequest,
    Mock,
    NotFound,
    _LINKAGE_KEY,
    _Request,
    _WAVE_DIGEST,
    _absence_evidence,
    _claimed_receipt,
    _intent,
    _outcome,
    _preclaim_evidence,
    _unclaimed_receipt,
    _wave,
    failure_kubernetes,
    failure_receipts,
    failure_types,
    outcomes,
    ptg_control,
    pytest,
    routes,
    types,
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
