

"""Residual fail-closed branch coverage for exact-wave controller contracts."""


from __future__ import annotations


import asyncio


import runpy


import types


from unittest.mock import AsyncMock, Mock


import pytest


from sanic.exceptions import BadRequest, NotFound, SanicException


from api import control as control_api


from api import control_import_wave_attestation as attestation


from api import control_imports


from api import control_wave_linkage_route as linkage_routes


from api import control_wave_routes as routes


from api import control_workers


from api import mrf_discovery_catalog_manifest as catalog_manifest


from api import provider_specialty_filters


from api import ptg2_candidate_audit_reverse as reverse_scope


from api import ptg2_candidate_audit_v4 as v4_scope


from api.mrf_discovery_catalog_paging import bounded_file_windows


from api.provider_profile_display import display_value


from process import ptg_control


from process import ptg_wave_barrier as barrier


from process import ptg_wave_controller_isolation as isolation


from process import ptg_wave_controller_receipts as receipts


from process import ptg_wave_outcome_contract as outcome_contract


from process import ptg_wave_outcome_terminal_validation as terminal_validation


from process import ptg_wave_receipt_projection as receipt_projection


from process import ptg_wave_worker as wave_worker


from process.ptg_parts import frozen_rate_binding_store as bindings


from process.ptg_parts import ptg_wave_admission_fence as fence


from process.ptg_wave_terminal_state import derive_terminal_state


from tests.test_ptg_wave_failure_controller_edges import (
    _CONFIG_DIGEST,
    _IMAGE,
    _JOBS_DIGEST,
    _LINKAGE_KEY,
    _MANIFEST_DIGEST,
    _MANIFEST_IDENTITY,
    _RUNTIME_IDENTITY,
    _WAVE_DIGEST,
    _Request,
    _claim,
    _intent,
    _outcome,
    _wave,
)


def _identity(**overrides):
    values_by_field = {
        "wave_digest": _WAVE_DIGEST,
        "queue": barrier.queue_for_wave(_WAVE_DIGEST),
        "worker_class": "process.PTGSmall",
        "slot_index": 0,
        "pod_uid": "pod-synthetic",
        "manifest_digest": _MANIFEST_DIGEST,
        "jobs_digest": _JOBS_DIGEST,
        "job_count": 2,
        "config_identity": _CONFIG_DIGEST,
        "manifest_identity": _MANIFEST_IDENTITY,
        "image_identity": _IMAGE,
        "runtime_image_identity": _RUNTIME_IDENTITY,
    }
    values_by_field.update(overrides)
    return barrier.PTGWaveWorkerIdentity(**values_by_field)


def _identity_environment(**overrides):
    values_by_field = {
        "HLTHPRT_PTG_WAVE_DIGEST": _WAVE_DIGEST,
        "HLTHPRT_ACTIVE_WORKER_QUEUE": barrier.queue_for_wave(_WAVE_DIGEST),
        "HLTHPRT_ACTIVE_WORKER_CLASS": "process.PTGSmall",
        "HLTHPRT_PTG_WAVE_SLOT_INDEX": "0",
        "HLTHPRT_PTG_WAVE_POD_UID": "pod-synthetic",
        "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST": _MANIFEST_DIGEST,
        "HLTHPRT_PTG_WAVE_JOBS_DIGEST": _JOBS_DIGEST,
        "HLTHPRT_PTG_WAVE_JOB_COUNT": "2",
        "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY": _CONFIG_DIGEST,
        "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY": _MANIFEST_IDENTITY,
        "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY": _IMAGE,
        "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY": _RUNTIME_IDENTITY,
    }
    values_by_field.update(overrides)
    return values_by_field


def _terminal_kubernetes_receipt(wave, ready_slots):
    expected = terminal_validation._expected_kubernetes_receipt(wave, ready_slots)
    return {
        **expected,
        "attestation_digest": terminal_validation.sha256_digest(
            terminal_validation.canonical_json(expected)
        ),
    }


def _terminal_redis_receipt(wave):
    wave.redis_release_attestation = {"release_digest": "a" * 64}
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference,
        "release_digest": "a" * 64,
        "target_key_count": 4 + (4 * wave.intent_count),
        "queue_entry_count": 0,
        "job_payload_count": 0,
        "result_count": 0,
        "retry_count": 0,
        "in_progress_count": 0,
        "health_check_count": 0,
        "result_presence_digest": "b" * 64,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": terminal_validation.sha256_digest(
            terminal_validation.canonical_json(unsigned_by_field)
        ),
    }


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _Column:
    def in_(self, _values):
        return self

    def __eq__(self, _other):
        return self


class _Query:
    def where(self, *_args):
        return self

    def limit(self, *_args):
        return self

    def join(self, *_args):
        return self

    def order_by(self, *_args):
        return self


def _isolation_controller(rows, generic_jobs):
    result = types.SimpleNamespace(all=lambda: rows)
    return types.SimpleNamespace(
        exists=lambda _query: False,
        select=lambda *_args: _Query(),
        PTGImportWaveIntent=types.SimpleNamespace(
            run_id=_Column(),
            wave_id=_Column(),
            ordinal=_Column(),
        ),
        ImportRun=types.SimpleNamespace(
            run_id=_Column(),
            importer=_Column(),
            status=_Column(),
        ),
        PTG_WAVE_FENCED_IMPORTERS=("ptg",),
        PTG_ACTIVE_RUN_STATES=("queued", "running"),
        db=types.SimpleNamespace(execute=AsyncMock(return_value=result)),
        PTGWaveControllerHold=RuntimeError,
        PTGWaveStateConflict=ValueError,
        list_generic_ptg_jobs=lambda: generic_jobs,
        _generic_job_nonterminal=isolation.is_generic_job_nonterminal,
    )


def _synthetic_catalog_metadata() -> dict[str, object]:
    accumulator = catalog_manifest._SourceManifestAccumulator.create(
        "synthetic-source"
    )
    accumulator.add_file("synthetic-file", 0)
    return {
        "discovery_run_id": "synthetic-run",
        catalog_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY: accumulator.manifest(
            "synthetic-run"
        ),
    }


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
    monkeypatch.setattr(linkage_routes, "require_control_auth", Mock())
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
        linkage_routes,
        "record_linkage_ack",
        AsyncMock(side_effect=routes.PTGWaveOutcomeConflict("conflict")),
    )
    with pytest.raises(SanicException, match="conflict"):
        await linkage_routes.control_record_import_wave_linkage(
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

def _install_admission_replay_mocks(monkeypatch):
    connection = object()
    monkeypatch.setattr(control_imports.db, "acquire", lambda: _Acquire(connection))
    for name in (
        "require_source_attempt_capabilities",
        "guard_source_attempt",
        "acquire_ptg_admission_lock",
        "require_no_capacity_owning_wave",
        "insert_or_compare_frozen_binding",
    ):
        monkeypatch.setattr(control_imports, name, AsyncMock())
    monkeypatch.setattr(
        control_imports,
        "source_file_import_id_from_payload",
        Mock(return_value="source"),
    )
    monkeypatch.setattr(
        control_imports,
        "_locked_ptg_source_replay",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_is_parallel_active_importer_run_allowed",
        Mock(return_value=False),
    )


@pytest.mark.asyncio
async def test_ptg_source_admission_replay_does_not_insert(monkeypatch):
    _install_admission_replay_mocks(monkeypatch)
    existing_by_field = {"run_id": "run-existing"}
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        AsyncMock(return_value=[existing_by_field]),
    )
    assert await control_imports._admit_ptg_source_file_run(
        {"params": {}, "idempotency_key": None}
    ) == existing_by_field


@pytest.mark.asyncio
async def test_ptg_fenced_admission_replays_do_not_insert(monkeypatch):
    _install_admission_replay_mocks(monkeypatch)
    existing_by_field = {"run_id": "run-existing"}
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        AsyncMock(return_value=[existing_by_field]),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": None}
    ) == existing_by_field

@pytest.mark.asyncio
async def test_claim_exception_never_guesses_after_ambiguous_reconciliation(monkeypatch):
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(side_effect=RuntimeError()),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    ) is None
    fields_by_field = {"run_id": "run"}
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value=fields_by_field),
    )
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(side_effect=RuntimeError()),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    ) is None
    flushed = AsyncMock()
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(return_value=types.SimpleNamespace(status="rejected")),
    )
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", flushed)
    assert (await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    )).status == "rejected"
    flushed.assert_awaited_once()
    marked = AsyncMock()
    monkeypatch.setattr(ptg_control, "mark_control_run", marked)
    await ptg_control._mark_exact_wave_preexecution_failure(
        "run", reason="reason", error=RuntimeError("detail")
    )
    marked.assert_awaited_once()

def test_terminal_state_rejects_non_dead_letter_failure_receipt():
    with pytest.raises(Exception, match="all dead letter"):
        derive_terminal_state(
            _wave(failure_receipt_digest="a" * 64, intent_count=1),
            [types.SimpleNamespace(ordinal=0, status="failed")],
        )

@pytest.mark.asyncio
async def test_fence_fallbacks_and_conflicts_are_fail_closed(monkeypatch):
    class ExecuteOnly:
        async def execute(self, *_args):
            return types.SimpleNamespace(
                all=lambda: [("row",)],
                scalar=lambda: "value",
            )

    assert await fence._all(ExecuteOnly(), object()) == [("row",)]
    assert await fence._scalar(ExecuteOnly(), object(), {}) == "value"
    assert await fence._capacity_owning_waves(object()) == []
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("wave", "executing"), ("other", "executing")]),
    )
    with pytest.raises(fence.PTGWaveCapacityConflict, match="ambiguous"):
        await fence.require_no_capacity_owning_wave(object())
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("wave", "executing")]),
    )
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[]))
    with pytest.raises(fence.PTGWaveCapacityConflict, match="reserved"):
        await fence.require_no_capacity_owning_wave(
            object(),
            owner_run_id="not-owned",
        )
    with pytest.raises(fence.PTGWaveCapacityConflict, match="already reserved"):
        await fence.require_wave_admission_capacity(object())
    original_is_ptg_wave_owned_run = fence.is_ptg_wave_owned_run
    monkeypatch.setattr(
        fence,
        "is_ptg_wave_owned_run",
        AsyncMock(return_value=True),
    )
    with pytest.raises(fence.PTGWaveOwnershipConflict):
        await fence.require_not_wave_owned_run(object(), "run")
    monkeypatch.setattr(
        fence,
        "is_ptg_wave_owned_run",
        original_is_ptg_wave_owned_run,
    )
    assert await fence.is_ptg_wave_owned_run(object(), "run") is False

def test_kubernetes_receipt_and_membership_projection(monkeypatch):
    attested = types.SimpleNamespace(
        wave_digest=_WAVE_DIGEST,
        job_uid="job",
        manifest_identity=_MANIFEST_IDENTITY,
        config_identity=_CONFIG_DIGEST,
        image_identity=_IMAGE,
        runtime_image_identity=_RUNTIME_IDENTITY,
        pod_uid_by_slot={slot: f"pod-{slot}" for slot in range(12)},
    )
    monkeypatch.setattr(
        receipts,
        "attest_existing_ptg_wave_job",
        Mock(return_value=attested),
    )
    projected = receipts.kubernetes_job_receipt({}, {})
    assert projected["pinned_image_digest"] == _IMAGE.rsplit("@sha256:", 1)[1]
    monkeypatch.setattr(
        receipts,
        "validate_ptg_wave_job_manifest",
        Mock(
            return_value=types.SimpleNamespace(
                image=_IMAGE,
                runtime_image_identity=_RUNTIME_IDENTITY,
            )
        ),
    )
    assert receipts.kubernetes_ready_receipt({}, attested)["slots"][0]["slot"] == 0
    with pytest.raises(Exception, match="membership"):
        receipts.assert_slot_membership(
            attested,
            [types.SimpleNamespace(slot=0, pod_uid="foreign")],
        )


def test_redis_and_terminal_receipt_projection(monkeypatch):
    monkeypatch.setattr(
        receipts,
        "validate_ptg_wave_job_manifest",
        Mock(
            return_value=types.SimpleNamespace(
                image=_IMAGE,
                runtime_image_identity=_RUNTIME_IDENTITY,
            )
        ),
    )
    release = types.SimpleNamespace(
        wave_id=_WAVE_DIGEST,
        queue_name="queue",
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        job_count=2,
        protocol_identity="p",
        serializer_identity="s",
        kubernetes_manifest_identity=_MANIFEST_IDENTITY,
        config_identity=_CONFIG_DIGEST,
        image_identity=_IMAGE,
        runtime_image_identity=_RUNTIME_IDENTITY,
        runtime_identity_digest="a" * 64,
        ready_slots=(),
        ready_slots_digest="b" * 64,
        release_digest="c" * 64,
    )
    assert receipts.redis_release_receipt(release)["wave_digest"] == _WAVE_DIGEST
    wave = _wave()
    initial = receipts.initial_kubernetes_attestation(wave)
    assert initial.job_uid == wave.kubernetes_job_uid
    assert receipts.kubernetes_terminal_receipt(
        wave,
        types.SimpleNamespace(as_mapping=lambda: {"ok": True}),
    ) == {"ok": True}
    assert receipts.redis_terminal_receipt(
        types.SimpleNamespace(as_mapping=lambda: {"ok": True})
    ) == {"ok": True}

@pytest.mark.asyncio
async def test_worker_admission_and_binding_replay_short_circuit(monkeypatch):
    connection = object()
    monkeypatch.setattr(control_workers.db, "acquire", lambda: _Acquire(connection))
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", AsyncMock())
    action_lock = AsyncMock()
    monkeypatch.setattr(
        control_workers,
        "acquire_control_run_worker_action_lock",
        action_lock,
    )
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", AsyncMock())
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", AsyncMock())
    failed_by_field = {"status": "failed"}
    monkeypatch.setattr(
        control_workers,
        "_admit_worker_ensure",
        AsyncMock(return_value=failed_by_field),
    )
    assert await control_workers._guarded_ptg_family_ensure(
        {}, run_id="run", importer="ptg", selected_specs=[]
    ) is failed_by_field
    action_lock.assert_awaited_once_with(connection, "run")
    expected_by_field = {"digest": "a" * 64}
    monkeypatch.setattr(
        bindings,
        "protected_frozen_tuple_presence",
        Mock(return_value=("frozen_rate_file_set_contract",)),
    )
    monkeypatch.setattr(
        bindings,
        "frozen_rate_binding_from_params",
        Mock(return_value=expected_by_field),
    )
    monkeypatch.setattr(
        bindings,
        "source_file_import_id_from_params",
        Mock(return_value=None),
    )
    assert await bindings.recheck_frozen_binding_on_connection(connection, {}) == expected_by_field

@pytest.mark.asyncio
async def test_control_api_translation_edges(monkeypatch):
    monkeypatch.setattr(control_api, "_require_control_auth", Mock())
    monkeypatch.setattr(control_api, "page_limit", Mock(side_effect=ValueError("bad")))
    with pytest.raises(BadRequest):
        await control_api.control_mrf_discovery_sources(_Request())
    with pytest.raises(BadRequest):
        await control_api.control_mrf_discovery_source_files(_Request(), "source")
    monkeypatch.setattr(control_api, "_ptg_import_file_payload", Mock(return_value={}))
    monkeypatch.setattr(
        control_api,
        "create_import_run",
        AsyncMock(return_value=({"run_id": "run"}, False)),
    )
    assert (await control_api.control_ptg_import_file(_Request(json={}))).status == 409
    monkeypatch.setattr(
        control_api,
        "promote_ptg2_source_snapshot",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        control_api,
        "_is_ptg_snapshot_refresh_requested",
        Mock(return_value=True),
    )
    monkeypatch.setattr(
        control_api,
        "_ptg_source_snapshot_refresh_payload",
        Mock(side_effect=ValueError("bad")),
    )
    with pytest.raises(BadRequest):
        await control_api.control_ptg_source_snapshot_promote(
            _Request(json={"source_key": "source", "snapshot_id": "snapshot"})
        )

def test_worker_module_main_entrypoint_calls_asyncio_run(monkeypatch):
    seen_values = []

    def capture(coroutine):
        seen_values.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(asyncio, "run", capture)
    with pytest.warns(RuntimeWarning, match="found in sys.modules"):
        runpy.run_module("process.ptg_wave_worker", run_name="__main__")
    assert len(seen_values) == 1

@pytest.mark.asyncio
async def test_worker_barrier_factory_resolution_covers_async_and_failure_edges(
    monkeypatch,
):
    identity = _identity()
    ready_barrier = types.SimpleNamespace(
        register_ready=AsyncMock(),
        wait_for_release=AsyncMock(),
    )

    async def async_factory(_identity):
        return ready_barrier

    assert await wave_worker._resolve_barrier(identity, async_factory) is ready_barrier
    with pytest.raises(barrier.PTGWaveContractError, match="ready/release barrier"):
        await wave_worker._resolve_barrier(identity, lambda _identity: object())

    monkeypatch.delenv("HLTHPRT_PTG_WAVE_BARRIER_FACTORY", raising=False)
    with pytest.raises(barrier.PTGWaveContractError, match="is required"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "missing.module.factory",
    )
    with pytest.raises(barrier.PTGWaveContractError, match="unavailable"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "process.ptg_wave_worker.PTG_WAVE_SLOT_COUNT",
    )
    with pytest.raises(barrier.PTGWaveContractError, match="not callable"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "process.ptg_wave_worker._drain_wave_queue",
    )
    assert wave_worker._factory_from_environment() is wave_worker._drain_wave_queue

    monkeypatch.setenv("HLTHPRT_PTG_WAVE_WORKER_SETTINGS", "process.FHIR")
    with pytest.raises(barrier.PTGWaveContractError, match="process.PTGSmall"):
        await wave_worker._drain_wave_queue(identity)

@pytest.mark.asyncio
async def test_controller_isolation_preflight_and_terminal_run_edges(monkeypatch):
    monkeypatch.setattr(isolation, "_require_generic_redis_idle", AsyncMock())
    bundle = types.SimpleNamespace(
        wave=types.SimpleNamespace(wave_id="wave", intent_count=1),
        intents=[object()],
    )

    with pytest.raises(RuntimeError, match="database work"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([("run",)], []),
            bundle,
            object(),
        )
    with pytest.raises(RuntimeError, match="Kubernetes Job"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([], [{"status": {"active": 1}}]),
            bundle,
            object(),
        )
    short_bundle = types.SimpleNamespace(
        wave=types.SimpleNamespace(wave_id="wave", intent_count=2),
        intents=[object()],
    )
    with pytest.raises(ValueError, match="lost an admitted intent"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([], []),
            short_bundle,
            object(),
        )
    await isolation.require_ptg_only_idle(
        _isolation_controller([], []),
        bundle,
        object(),
    )

    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([("succeeded",)], []),
        bundle,
    ) is True
    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([], []),
        bundle,
    ) is False
    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([("running",)], []),
        bundle,
    ) is False

@pytest.mark.asyncio
async def test_claim_reconciliation_none_and_nonrejected_paths(monkeypatch):
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value=None),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(),
        {},
        run_id="run",
        claim_attempt_token="token",
    ) is None

    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value={"run_id": "run"}),
    )
    resolution = types.SimpleNamespace(status="claimed")
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(return_value=resolution),
    )
    flushed = AsyncMock()
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", flushed)
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(),
        {},
        run_id="run",
        claim_attempt_token="token",
    ) is resolution
    flushed.assert_not_awaited()

    marked = AsyncMock()
    monkeypatch.setattr(ptg_control, "mark_control_run", marked)
    await ptg_control._mark_exact_wave_preexecution_failure(
        "run",
        reason="",
        error=None,
    )
    assert marked.await_args.kwargs["error"]["message"] == "worker start failed"

@pytest.mark.asyncio
async def test_incomplete_claim_payload_is_never_reconciled(monkeypatch):
    reconcile = AsyncMock()
    monkeypatch.setattr(
        ptg_control,
        "_claim_exact_wave_worker_start",
        AsyncMock(side_effect=RuntimeError("claim")),
    )
    monkeypatch.setattr(
        ptg_control,
        "_reconcile_exact_wave_claim_exception",
        reconcile,
    )

    with pytest.raises(RuntimeError, match="claim"):
        await ptg_control.ptg_control_start(
            {}, {"run_id": "run", "params": {"_wave_id": "wave"}}
        )
    reconcile.assert_not_awaited()

@pytest.mark.asyncio
async def test_control_without_run_id_never_installs_live_context(monkeypatch):
    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", AsyncMock())
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", AsyncMock(return_value=None))
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", AsyncMock(return_value=None))
    monkeypatch.setattr(
        ptg_control,
        "validated_worker_rate_params",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(ptg_control, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(ptg_control, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_control, "ptg_main", AsyncMock(return_value={}))
    reset = Mock()
    monkeypatch.setattr(ptg_control, "reset_live_progress_context", reset)

    result = await ptg_control.ptg_control_start({}, {"params": {}})

    assert result == {"status": "succeeded", "run_id": ""}
    reset.assert_not_called()

@pytest.mark.asyncio
async def test_fence_scalar_keyword_fallback_and_idle_admission(monkeypatch):
    class KeywordOnlyScalar:
        async def scalar(self, _statement, **parameters):
            return parameters["relation"]

    assert await fence._scalar(
        KeywordOnlyScalar(), object(), {"relation": "mrf.ptg_import_wave"}
    ) == "mrf.ptg_import_wave"

    monkeypatch.setattr(fence, "_capacity_owning_waves", AsyncMock(return_value=[]))
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[]))
    await fence.require_wave_admission_capacity(object())

@pytest.mark.asyncio
async def test_successful_wave_and_discovery_get_routes(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", Mock())
    outcomes_page = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(routes, "get_wave_outcomes_page", outcomes_page)
    outcome_response = await routes.control_get_import_wave_outcomes(
        _Request(args={"after_ordinal": "1", "limit": "2"}), "wave"
    )
    assert outcome_response.status == 200
    outcomes_page.assert_awaited_once_with("wave", after_ordinal=1, limit=2)

    proof = AsyncMock(return_value={"wave_id": "wave"})
    monkeypatch.setattr(routes, "get_wave_receipts", proof)
    assert (await routes.control_get_import_wave_proof(_Request(), "wave")).status == 200

    monkeypatch.setattr(control_api, "_require_control_auth", Mock())
    monkeypatch.setattr(control_api, "page_limit", Mock(return_value=2))
    source_files = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(control_api, "list_discovery_source_files_page", source_files)
    response = await control_api.control_mrf_discovery_source_files(
        _Request(args={"cursor": "cursor", "limit": "2"}), "source"
    )
    assert response.status == 200
    source_files.assert_awaited_once_with("source", cursor="cursor", limit=2)

def test_matching_slot_membership_and_receipt_projection():
    attested = types.SimpleNamespace(
        pod_uid_by_slot={slot: f"pod-{slot}" for slot in range(12)}
    )
    receipts.assert_slot_membership(
        attested,
        [types.SimpleNamespace(slot=slot, pod_uid=f"pod-{slot}") for slot in range(12)],
    )

    wave = _wave()
    vars(wave).update(
        {
            "state_version": 1,
            "physical_coordinate_count": 1,
            "imported_coordinate_count": 1,
            "reused_coordinate_count": 0,
            "physical_coordinate_digest": "a",
            "imported_coordinate_digest": "b",
            "reused_coordinate_digest": "c",
            "partition_digest": "d",
            "k8s_post_started_at": None,
            "kubernetes_job_receipt": None,
            "kubernetes_ready_attestation_digest": None,
            "redis_release_started_at": None,
            "terminal_evidence_digest": None,
            "redis_cleanup_evidence": None,
            "cleanup_evidence_digest": None,
            "cleanup_summary": None,
            "resolved_at": None,
        }
    )
    assert receipt_projection.wave_receipt_mapping(wave)["capacity_owning"] is True

def test_coverage_margin_pager_stops_before_a_second_plan_dense_row():
    windows, cursor = bounded_file_windows(
        [
            {
                "mrf_file_id": "synthetic-file-one",
                "metadata_json": {"plan_info": [{}]},
            },
            {
                "mrf_file_id": "synthetic-file-two",
                "metadata_json": {"plan_info": [{}]},
            },
        ],
        limit=2,
        cursor_plan_offset=0,
        plan_reference_limit=1,
    )

    assert [window.file_data["mrf_file_id"] for window in windows] == [
        "synthetic-file-one"
    ]
    assert cursor == "synthetic-file-one"

def test_coverage_margin_manifest_rejects_an_unsupported_requested_page_size():
    assert (
        catalog_manifest.catalog_paging_manifest_for_file_page(
            _synthetic_catalog_metadata(),
            page_limit=99,
        )
        is None
    )

@pytest.mark.parametrize(
    "page_totals",
    (
        "not-a-mapping",
        {"100": 1, "250": 1},
        {"100": 1, "250": 0, "500": 1},
    ),
)
def test_coverage_margin_manifest_rejects_malformed_cached_totals(
    page_totals: object,
):
    metadata = _synthetic_catalog_metadata()
    manifest = metadata[catalog_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY]
    assert isinstance(manifest, dict)
    manifest["page_totals"] = page_totals

    assert (
        catalog_manifest.catalog_paging_manifest_for_file_page(
            metadata,
            page_limit=100,
        )
        is None
    )

@pytest.mark.parametrize("counts", ((True, 0), (0, -1)))
def test_v4_bound_rejects_invalid_counts(
    counts: tuple[object, object],
):
    with pytest.raises(ValueError, match="V4 candidate proof counts"):
        v4_scope.v4_candidate_proof_memory_bound(*counts)

def test_v4_bound_skips_zero_candidate_graph():
    assert v4_scope.v4_candidate_proof_memory_bound(2, 0) == 992

@pytest.mark.parametrize(
    ("counts", "message"),
    (
        ((True, 0), "candidate source code count"),
        ((0, True), "candidate source membership count"),
    ),
)
def test_coverage_margin_source_projection_rejects_non_integer_counts(
    counts: tuple[object, object],
    message: str,
):
    with pytest.raises(ValueError, match=message):
        reverse_scope.source_key_projection_retention_upper_bound(*counts)

def test_coverage_margin_provider_projection_rejects_boolean_count():
    with pytest.raises(ValueError, match="candidate provider projection counts"):
        reverse_scope.provider_candidate_projection_retention_upper_bound(
            0, 0, False, 0
        )

def test_coverage_margin_display_omits_duplicate_casefolded_code():
    assert display_value(
        "synthetic_detail",
        {"display": "Synthetic label", "code": "synthetic label"},
    ) == "Synthetic label"

def test_coverage_margin_specialty_exists_sql_excludes_subspecialties():
    params_by_field: dict[str, object] = {}
    specialty_filter = provider_specialty_filters.ProviderSpecialtyFilter(
        classification="Synthetic Class",
        include_subspecialties=False,
        primary_only=False,
    )

    sql = provider_specialty_filters.provider_specialty_taxonomy_exists_sql(
        "p.npi",
        params_by_field,
        "synthetic",
        specialty_filter,
        schema="mrf",
    )

    assert "NULLIF(BTRIM(COALESCE(synthetic_nucc.specialization, '')), '') IS NULL" in sql
    assert params_by_field == {"synthetic_classification": "Synthetic Class"}
