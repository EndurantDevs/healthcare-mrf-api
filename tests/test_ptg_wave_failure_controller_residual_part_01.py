# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave residual fail-closed contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_residual import (
    AsyncMock,
    BadRequest,
    Mock,
    NotFound,
    SanicException,
    _LINKAGE_KEY,
    _Request,
    _claim,
    _identity,
    _identity_environment,
    _intent,
    _outcome,
    _terminal_kubernetes_receipt,
    _terminal_redis_receipt,
    _wave,
    attestation,
    barrier,
    control_imports,
    outcome_contract,
    pytest,
    routes,
    terminal_validation,
    types,
)


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({"HLTHPRT_PTG_WAVE_SLOT_INDEX": "bad"}, "slot index must be an integer"),
        ({"HLTHPRT_PTG_WAVE_SLOT_INDEX": "00"}, "slot index must be canonical"),
        ({"HLTHPRT_PTG_WAVE_JOB_COUNT": "bad"}, "job count must be an integer"),
        ({"HLTHPRT_PTG_WAVE_JOB_COUNT": "02"}, "job count must be canonical"),
    ],
)
def test_worker_identity_environment_rejects_noncanonical_numbers(environment, message):
    values = _identity_environment(**environment)
    with pytest.raises(barrier.PTGWaveContractError, match=message):
        barrier.PTGWaveWorkerIdentity.from_environment(values)

@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"slot_index": 12}, "between zero and eleven"),
        ({"worker_class": "process.FHIR"}, "process.PTGSmall"),
        ({"manifest_digest": "G" * 64}, "lowercase"),
        ({"job_count": True}, "1 through 4096"),
        ({"image_identity": "registry.example/synthetic:latest"}, "pinned"),
        ({"runtime_image_identity": "sha256:" + "G" * 64}, "canonical"),
        ({"pod_uid": " "}, "non-empty trimmed"),
    ],
)
def test_worker_identity_validate_rejects_each_fail_closed_edge(overrides, message):
    with pytest.raises(barrier.PTGWaveContractError, match=message):
        _identity(**overrides).validate()

@pytest.mark.asyncio
async def test_barrier_awaits_async_start_and_rejects_unreleased_receipts():
    identity = _identity()
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
    gate = types.SimpleNamespace(
        register_ready=AsyncMock(),
        wait_for_release=AsyncMock(return_value=release_by_field),
    )

    async def start():
        return "started"

    assert await barrier.run_after_wave_release(identity, gate, start) == "started"
    release_by_field["released"] = False
    with pytest.raises(barrier.PTGWaveContractError, match="not released"):
        await barrier.run_after_wave_release(identity, gate, start)

def test_barrier_required_rejects_missing_values():
    with pytest.raises(barrier.PTGWaveContractError, match="missing or invalid"):
        barrier._required({}, "HLTHPRT_PTG_WAVE_DIGEST")

def test_outcome_primitives_and_claim_guards(monkeypatch):
    intent = _intent(0)
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="SHA-256"):
        outcome_contract._digest("G" * 64, "digest")

    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="key is required"):
        outcome_contract._linkage_key(None)

    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="terminal"):
        outcome_contract._outcome_record(
            intent,
            types.SimpleNamespace(status="running", snapshot_id=None, import_id=None),
        )
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="source evidence"):
        outcome_contract._outcome_record(
            intent,
            types.SimpleNamespace(
                status="succeeded",
                snapshot_id="snapshot-synthetic",
                import_id="other-source",
            ),
        )
    assert outcome_contract._rows_by_ordinal([], require_zero_based=False) == []
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="invalid ordinals"):
        outcome_contract._validate_claim_outcomes(
            [types.SimpleNamespace(ordinal=0, claim_status="started", failure_code=None)],
            [{"ordinal": "bad", "status": "failed"}],
        )
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="different ordinals"):
        outcome_contract._validate_claim_outcomes(
            [types.SimpleNamespace(ordinal=0, claim_status="started", failure_code=None)],
            [{"ordinal": 1, "status": "failed"}],
        )
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="must be an object"):
        outcome_contract._validate_linkage_ack(_wave(), [], [], _LINKAGE_KEY)


def test_linkage_ack_shape_and_binding_guards():
    wave = _wave(intent_count=1, outcomes_digest="a" * 64)
    outcome = _outcome(_intent(0))
    unsigned_by_field = {
        "schema_version": "wrong-version",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": outcome_contract.linkage_mapping_digest([outcome]),
        "outcomes_digest": wave.outcomes_digest,
    }
    unsigned_by_field["signature"] = outcome_contract.sign_linkage_ack(
        unsigned_by_field, key=_LINKAGE_KEY
    )
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="fields are not exact"):
        outcome_contract._validate_linkage_ack(
            wave, [outcome], unsigned_by_field, _LINKAGE_KEY
        )

    unsigned_by_field["schema_version"] = "healthporta.ptg-wave-linkage-ack.v1"
    unsigned_by_field["wave_id"] = "other-wave"
    unsigned_by_field["signature"] = outcome_contract.sign_linkage_ack(
        {name: candidate_value for name, candidate_value in unsigned_by_field.items() if name != "signature"},
        key=_LINKAGE_KEY,
    )
    with pytest.raises(outcome_contract.PTGWaveOutcomeConflict, match="does not bind"):
        outcome_contract._validate_linkage_ack(
            wave, [outcome], unsigned_by_field, _LINKAGE_KEY
        )


def test_terminal_ready_slots_and_row_binding():
    wave = _wave()
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="12-slot"):
        terminal_validation._ready_slots_by_number(
            _wave(kubernetes_ready_attestation={"slots": []})
        )
    duplicate = _wave()
    duplicate.kubernetes_ready_attestation["slots"][1]["slot"] = 0
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="12-slot"):
        terminal_validation._ready_slots_by_number(duplicate)

    intent = _intent(0)
    bound_wave = _wave(intent_count=1)
    claim = _claim(bound_wave, intent)
    outcome = _outcome(intent)
    ready_by_field = {0: bound_wave.kubernetes_ready_attestation["slots"][0]}
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="every admitted"):
        terminal_validation._validate_rows(bound_wave, [intent], [], [], ready_by_field)
    outcome.content_version = "v2"
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="differs"):
        terminal_validation._validate_rows(
            bound_wave,
            [intent],
            [claim],
            [outcome],
            ready_by_field,
        )
    outcome.content_version = intent.content_version
    claim.pod_uid = "foreign"
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="does not bind"):
        terminal_validation._validate_rows(
            bound_wave,
            [intent],
            [claim],
            [outcome],
            ready_by_field,
        )


def test_terminal_outcome_and_linkage_digest_guards(monkeypatch):
    wave = _wave()
    outcome = _outcome(_intent(0))
    monkeypatch.setattr(terminal_validation, "_collection_digest", Mock(return_value="bad"))
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="outcomes digest"):
        terminal_validation._validate_outcome_and_linkage_digests(
            wave, [outcome], key=_LINKAGE_KEY
        )
    monkeypatch.setattr(
        terminal_validation, "_collection_digest", Mock(return_value=wave.outcomes_digest)
    )
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="linkage acknowledgement"):
        terminal_validation._validate_outcome_and_linkage_digests(
            wave, [outcome], key=_LINKAGE_KEY
        )
    wave.linkage_ack = {}
    wave.linkage_ack_digest = "a" * 64
    monkeypatch.setattr(
        terminal_validation, "_validate_linkage_ack", Mock(return_value=({}, "b" * 64))
    )
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="acknowledgement digest"):
        terminal_validation._validate_outcome_and_linkage_digests(
            wave, [outcome], key=_LINKAGE_KEY
        )


def test_terminal_kubernetes_receipt_guards():
    wave = _wave()
    receipt = _terminal_kubernetes_receipt(
        wave,
        wave.kubernetes_ready_attestation["slots"],
    )
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="fields are not exact"):
        terminal_validation._validate_kubernetes_receipt(wave, {}, [])
    mismatched_by_field = dict(receipt, queue="arq:foreign")
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="differs"):
        terminal_validation._validate_kubernetes_receipt(
            wave, mismatched_by_field, wave.kubernetes_ready_attestation["slots"]
        )
    corrupt_by_field = dict(receipt, attestation_digest="0" * 64)
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="digest is invalid"):
        terminal_validation._validate_kubernetes_receipt(
            wave, corrupt_by_field, wave.kubernetes_ready_attestation["slots"]
        )


def test_terminal_redis_receipt_and_entrypoint_guards(monkeypatch):
    wave = _wave()
    redis = _terminal_redis_receipt(wave)
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="fields are not exact"):
        terminal_validation._validate_redis_receipt(wave, {})
    nonidle_by_field = dict(redis, queue_entry_count=1)
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="idleness"):
        terminal_validation._validate_redis_receipt(wave, nonidle_by_field)

    monkeypatch.setattr(
        terminal_validation,
        "_ready_slots_by_number",
        Mock(return_value=([], {})),
    )
    monkeypatch.setattr(terminal_validation, "_validate_rows", Mock(return_value=[]))
    monkeypatch.setattr(
        terminal_validation,
        "_validate_outcome_and_linkage_digests",
        Mock(),
    )
    with pytest.raises(terminal_validation.PTGWaveOutcomeConflict, match="fields are not exact"):
        terminal_validation.verify_terminal_eligibility(wave, [], [], [], {})


def test_import_attestation_canonical_identifier_and_envelope_guards():
    with pytest.raises(ValueError, match="canonical JSON"):
        attestation._canonical({"bad": object()})
    for candidate_value, message in (
        (None, "must be a string"),
        (" ", "trimmed"),
        ("bad/", "worker-wave-compatible"),
    ):
        with pytest.raises(ValueError, match=message):
            attestation._identifier(candidate_value, "identifier", 8)
    with pytest.raises(ValueError, match="key is required"):
        attestation._attestation_key(object())
    with pytest.raises(ValueError, match="must be an object"):
        attestation._verify_attestation([], attestation_key="test")
    with pytest.raises(ValueError, match="fields are not exact"):
        attestation._verify_attestation({}, attestation_key="test")
    unsupported_by_field = {
        "schema_version": "unsupported",
        "wave_id": "wave",
        "idempotency_key": "key",
        "snapshot": {},
        "partition": {},
        "intents": [],
    }
    signed_unsupported_by_field = {
        **unsupported_by_field,
        "signature": "a" * 64,
    }
    with pytest.raises(ValueError, match="schema_version is unsupported"):
        attestation._verify_attestation(signed_unsupported_by_field, attestation_key="test")


def test_import_attestation_snapshot_guards():
    with pytest.raises(ValueError, match="snapshot fields"):
        attestation._validate_snapshot({})
    valid_snapshot_by_field = {
        name: "a" * 64
        for name in (
            "snapshot_digest",
            "membership_digest",
            "inventory_digest",
            "subscription_coverage_digest",
            "entitlement_coverage_digest",
            "catalog_generation",
        )
    }
    valid_snapshot_by_field["authorization_basis"] = (
        attestation.AUTHORIZATION_BASIS
    )
    valid_snapshot_by_field["authorization_digest"] = "b" * 64
    valid_snapshot_by_field["entitlement_coverage_count"] = True
    with pytest.raises(ValueError, match="invalid"):
        attestation._validate_snapshot(valid_snapshot_by_field)


def test_import_attestation_partition_guards():
    with pytest.raises(ValueError, match="partition fields"):
        attestation._validate_partition({})
    partition_by_field = {
        "complete": True,
        "physical_coordinate_count": 1,
        "physical_coordinate_digest": "a" * 64,
        "imported_coordinate_count": 1,
        "imported_coordinate_digest": "b" * 64,
        "reused_coordinate_count": 0,
        "reused_coordinate_digest": "c" * 64,
        "partition_digest": "d" * 64,
    }
    partition_by_field["physical_coordinate_count"] = 0
    with pytest.raises(ValueError, match="physical_coordinate_count"):
        attestation._validate_partition(partition_by_field)
    partition_by_field["physical_coordinate_count"] = 2
    with pytest.raises(ValueError, match="imported \\+ reused"):
        attestation._validate_partition(partition_by_field)

@pytest.mark.asyncio
async def test_control_wave_route_error_translation_and_outcomes(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", Mock())
    monkeypatch.setattr(
        routes,
        "admit_import_wave",
        AsyncMock(side_effect=routes.ImportWaveConflict("conflict")),
    )
    with pytest.raises(SanicException, match="conflict"):
        await routes.control_admit_import_wave(_Request(json={}))
    monkeypatch.setattr(
        routes, "admit_import_wave", AsyncMock(side_effect=ValueError("invalid"))
    )
    with pytest.raises(BadRequest, match="invalid"):
        await routes.control_admit_import_wave(_Request(json={}))
    monkeypatch.setattr(
        routes, "get_import_wave", AsyncMock(side_effect=ValueError("invalid"))
    )
    with pytest.raises(BadRequest, match="invalid"):
        await routes.control_get_import_wave(_Request(), "wave")
    monkeypatch.setattr(
        routes, "get_wave_outcomes_page", AsyncMock(side_effect=ValueError("bad"))
    )
    with pytest.raises(BadRequest, match="bad"):
        await routes.control_get_import_wave_outcomes(
            _Request(args={"after_ordinal": "bad"}), "wave"
        )
    monkeypatch.setattr(
        routes,
        "record_linkage_ack",
        AsyncMock(side_effect=routes.PTGWaveOutcomeConflict("conflict")),
    )
    with pytest.raises(SanicException, match="conflict"):
        await routes.control_record_import_wave_linkage(
            _Request(json={"linkage_ack": {}}), "wave"
        )
    monkeypatch.setattr(routes, "get_wave_receipts", AsyncMock(return_value=None))
    with pytest.raises(NotFound):
        await routes.control_get_import_wave_proof(_Request(), "wave")

def test_exact_wave_internal_params_are_rejected_at_both_import_boundaries():
    internal_by_field = {"_wave_id": "wave-synthetic"}
    with pytest.raises(ValueError, match="exact-wave identity"):
        control_imports._import_param_views("ptg", internal_by_field, run_id="run")
    with pytest.raises(ValueError, match="exact-wave identity"):
        control_imports._assert_ptg_rebuild_request_params("ptg", internal_by_field)
