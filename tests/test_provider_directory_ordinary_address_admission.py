# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary input custody and signed-purpose refusals on the existing producer."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_bundle_preparation as coordinator
from process import provider_directory_cms_nonprofile_capacity as shared
from process import provider_directory_ordinary_address_admission as admission
from process import provider_directory_profile_selection_contract as selection
from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _captured_case(monkeypatch):
    case = serving_case()
    database = SimpleNamespace(_transaction_binding=lambda: None)
    directory = SimpleNamespace(db=database)

    class CapturedServingInputs:
        def __init__(self):
            self.database = database
            self.scope_payload = deepcopy(case.plan.payload["ordinary_scope"])
            self.recheck = AsyncMock()

    # The coordinator owns the actual native capture implementation. This host
    # double tests admission's exact-type, identity and recheck boundary only.
    monkeypatch.setattr(coordinator, "CapturedServingInputs", CapturedServingInputs, raising=False)
    return case, directory, CapturedServingInputs()


@pytest.mark.parametrize("replacement", ["json", "database", "subclass", "scope"])
async def test_worker_data_or_detached_capture_cannot_admit_address(monkeypatch, replacement):
    case, directory, capture = _captured_case(monkeypatch)
    if replacement == "json":
        with pytest.raises(ValueError):
            await admission.recheck_capture(
                directory, case.execution, case.plan, {"scope_payload": capture.scope_payload}
            )
        return
    if replacement == "database":
        capture.database = SimpleNamespace()
    elif replacement == "subclass":
        capture = type("DerivedCapture", (type(capture),), {})()
    else:
        capture.scope_payload["owner_oid"] += 1
        capture.scope_payload["input_sha256"] = contract.input_sha256(
            {name: capture.scope_payload[name] for name in contract.INPUT_FIELDS}
        )
    with pytest.raises(ValueError):
        await admission.recheck_capture(directory, case.execution, case.plan, capture)


async def test_capture_recheck_failure_precedes_authority_transport(monkeypatch):
    case, directory, capture = _captured_case(monkeypatch)
    capture.recheck.side_effect = RuntimeError("native_source_changed")
    transport = AsyncMock()
    with pytest.raises(RuntimeError, match="native_source_changed"):
        await admission.produce_ordinary_address_admission(
            directory,
            replace(case.execution, serving_nonprofile_capacity_attestation=case.envelope),
            case.fence,
            run_id="run_" + "a" * 32,
            signed_plan=case.plan,
            captured_inputs=capture,
            fresh_storage_envelope=transport,
        )
    transport.assert_not_awaited()


async def test_existing_cms_selection_failure_keeps_its_original_runtime_error():
    from tests.test_provider_directory_cms_nonprofile_capacity import _producer

    producer = _producer()
    attestation = replace(
        producer.execution.attestation, payload={**producer.execution.attestation.payload, "desired_cms_dataset": None}
    )
    with pytest.raises(RuntimeError, match="provider_directory_nonprofile_capacity_selection_changed"):
        await shared._require_admission_inputs(
            producer.fhir, replace(producer.execution, attestation=attestation), producer.fence, producer.plan, None
        )


@pytest.mark.parametrize("field", sorted(contract.INPUT_FIELDS - {"retention_kind"}))
def test_input_digest_binds_every_scope_identity(field):
    scope = serving_case().plan.payload["ordinary_scope"]
    scope[field] = scope[field] + 1 if type(scope[field]) is int else "99" * 32
    with pytest.raises(ValueError, match="input_digest"):
        contract.validate_scope(scope)


def test_recomputed_foreign_source_vector_still_refuses_the_profile_pair():
    case = serving_case()
    scope = case.plan.payload["ordinary_scope"]
    scope["source_vector_sha256"] = "99" * 32
    scope["input_sha256"] = contract.input_sha256({name: scope[name] for name in contract.INPUT_FIELDS})
    plan = replace(case.plan, ordinary_scope_json=contract.canonical(scope))
    with pytest.raises(ValueError, match="source_vector_changed"):
        contract.require_plan_selection(plan.payload, case.execution.attestation.payload, scope)


@pytest.mark.parametrize("initial_mode", [False, True])
def test_runtime_task_keeps_the_exact_separately_signed_pair(initial_mode):
    case = serving_case(initial_mode=initial_mode)
    task_by_field = {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": case.execution.generation,
        "provider_directory_profile_selection_attestation": case.execution.attestation.payload,
        "provider_directory_profile_capacity_attestation": case.pair,
        contract.EXECUTION_FIELD: case.envelope,
    }
    execution = selection.validated_profile_execution(task_by_field)
    assert execution.capacity_attestation == case.pair
    assert execution.serving_nonprofile_capacity_attestation == case.envelope
    assert task_by_field["publish_artifacts_targets"] == ["profile"]
    task_by_field[contract.EXECUTION_FIELD] = case.pair
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection.validated_profile_execution(task_by_field)


async def test_common_cut_keeps_original_binding_without_rechecking_its_old_predecessor(monkeypatch):
    case, directory, capture = _captured_case(monkeypatch)
    lease = verify_serving_envelope(case.envelope)
    producer = shared._CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        case.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=capture,
    )
    producer._cutover_active = True
    producer._cutover_request = NonprofileAdmissionCheck("cutover", lease, case.plan, ())
    producer._cutover_witness = SimpleNamespace(expires_at=lease.expires_at)
    producer._cutover_observation = {}
    producer._assert_consumption = AsyncMock()
    producer._assert_physical = AsyncMock()
    producer._assert_wal_budget = AsyncMock()
    original_binding = SimpleNamespace(session=object())
    directory.db._transaction_binding = lambda: original_binding
    directory.db.scalar = AsyncMock(return_value=VALIDATION_TIME)
    await producer.assert_cutover(())
    capture.recheck.assert_awaited_once_with(directory)
    capture.recheck.side_effect = AssertionError("predecessor already replaced by this transaction")
    await producer.assert_cutover(None)
    assert capture.recheck.await_count == 1
    producer._assert_wal_budget.assert_awaited_once()
    directory.db._transaction_binding = lambda: SimpleNamespace(session=original_binding.session)
    with pytest.raises(RuntimeError, match="cutover_operation_changed"):
        await producer.assert_cutover(None)


async def test_mutated_capture_cannot_finish_an_already_admitted_cut(monkeypatch):
    case, directory, capture = _captured_case(monkeypatch)
    lease = verify_serving_envelope(case.envelope)
    producer = shared._CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        case.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=capture,
    )
    await producer._check_captured_inputs()
    capture.scope_payload["max_retention_data_bytes"] += 1
    with pytest.raises(ValueError, match="capture_changed"):
        await producer._check_captured_inputs(semantic=False)


def _recipe_scope(monkeypatch):
    from process import entity_address_preparation_admission as gate

    case, directory, capture = _captured_case(monkeypatch)
    capture.address_task = {
        "refresh_mode": "provider-directory-partial",
        "serving_only_refresh": True,
        "test_mode": False,
        "provider_directory_source_ids": ["synthetic-directory"],
        "provider_directory_dataset_id": "dataset-fixture",
        "provider_directory_run_id": "run_" + "a" * 32,
        "provider_directory_partial_scope": "latest-run",
    }
    monkeypatch.setattr(gate, "_fhir", lambda: directory)
    monkeypatch.setattr(gate._native(), "db", directory.db)
    scope = SimpleNamespace(admission=SimpleNamespace(plan=case.plan, captured_inputs=capture))
    return gate, scope, {**capture.address_task, "publish": True, "skip_publish": False}


def test_ordinary_recipe_is_exact_and_cms_full_recipe_remains_closed(monkeypatch):
    gate, scope, task = _recipe_scope(monkeypatch)
    gate.assert_full_recipe({"context": {}}, task, scope)
    with pytest.raises(RuntimeError, match="full fresh build"):
        gate.assert_full_recipe({"context": {}}, task)
    gate.assert_full_recipe({"context": {}}, {"refresh_mode": "full", "serving_only_refresh": False})


@pytest.mark.parametrize("changed", ["source", "dataset", "run", "full", "limit", "extra", "prepared", "environment"])
def test_ordinary_recipe_cannot_expand_or_reuse_captured_scope(monkeypatch, changed):
    gate, scope, task = _recipe_scope(monkeypatch)
    context_by_field = {}
    replacements_by_case = {
        "source": {"provider_directory_source_ids": ["different-directory"]},
        "dataset": {"provider_directory_dataset_id": "other-dataset"},
        "run": {"provider_directory_run_id": "run_" + "b" * 32},
        "full": {"refresh_mode": "full"},
        "limit": {"source_limit": 1},
        "extra": {"unreviewed_option": True},
    }
    task.update(replacements_by_case.get(changed, {}))
    if changed == "prepared":
        context_by_field["stage_prepared"] = True
    if changed == "environment":
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", "true")
    with pytest.raises(RuntimeError):
        gate.assert_full_recipe({"context": context_by_field}, task, scope)


def test_ordinary_retention_budget_is_measured_before_profile_resumes(monkeypatch):
    case, directory, capture = _captured_case(monkeypatch)
    producer = shared._CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        verify_serving_envelope(case.envelope),
        case.profile,
        case.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=capture,
    )
    limit = case.plan.logging_wal_upper_bound_bytes
    assert producer._remaining_logging_wal("pre_logging", limit) == 0
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        producer._remaining_logging_wal("readiness", limit + 1)
    producer.preparation_wal_end_bytes = limit
    assert producer._remaining_logging_wal("cutover", limit + 1) == 0
    with pytest.raises(RuntimeError, match="logging_phase_closed"):
        producer._remaining_logging_wal("pre_logging", limit + 1)


def test_ordinary_continuation_signs_its_own_purpose_and_rejects_cms_substitution():
    import datetime

    from process import provider_directory_cms_storage_continuation as continuation
    from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
    from tests.test_provider_directory_cms_storage_continuation import _envelope

    case = serving_case()
    lease = verify_serving_envelope(case.envelope)
    request = continuation.request_storage_continuation(
        lease,
        run_id="run_" + "a" * 32,
        phase="pre_logging",
        requested_at=VALIDATION_TIME,
    )
    assert request.payload["admission_purpose"] == contract.PURPOSE
    storage = deepcopy(lease.signing_preflight_guard["control_plane_request"]["storage_observation"])
    storage.update(observed_at=continuation._utc(VALIDATION_TIME), issued_at=continuation._utc(VALIDATION_TIME))
    witness_by_field = {
        **request.binding_by_field,
        "issued_at": continuation._utc(VALIDATION_TIME),
        "expires_at": continuation._utc(VALIDATION_TIME + datetime.timedelta(seconds=30)),
        "storage_observation": storage,
    }
    arguments_by_name = dict(
        request=request, lease=lease, trust=capacity_trust_from_envelope(case.envelope), now=VALIDATION_TIME
    )
    verified = continuation.verify_storage_continuation(_envelope(witness_by_field), **arguments_by_name)
    assert verified.request_nonce == request.payload["request_nonce"]
    witness_by_field["admission_purpose"] = "cms_nonprofile"
    with pytest.raises(RuntimeError, match="request_binding_changed"):
        continuation.verify_storage_continuation(_envelope(witness_by_field), **arguments_by_name)


@pytest.mark.parametrize("enabled", [False, True])
def test_cms_inference_uses_its_existing_option_with_a_captured_input(monkeypatch, enabled):
    from process import entity_address_preparation_admission as gate
    from tests.test_provider_directory_cms_nonprofile_capacity import _producer

    scope = SimpleNamespace(admission=SimpleNamespace(plan=_producer().plan, captured_inputs=object()))
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", str(enabled))
    token = gate._ADMISSION.set(scope)
    try:
        assert gate.inference_enabled(False) is enabled
    finally:
        gate._ADMISSION.reset(token)


@pytest.mark.parametrize("changed", [None, "option", "test_mode", "missing_capture"])
def test_ordinary_inference_still_requires_its_exact_captured_recipe(monkeypatch, changed):
    gate, scope, _task = _recipe_scope(monkeypatch)
    scope.admission.captured_inputs.affected_scope = {"inference_enabled": True}
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "false" if changed == "option" else "true")
    if changed == "missing_capture":
        scope.admission.captured_inputs = None
    token = gate._ADMISSION.set(scope)
    try:
        if changed is None:
            assert gate.inference_enabled(False) is True
        else:
            with pytest.raises((RuntimeError, ValueError)):
                gate.inference_enabled(changed == "test_mode")
    finally:
        gate._ADMISSION.reset(token)
