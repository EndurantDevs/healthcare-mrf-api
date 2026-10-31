# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the ordinary receipt producer with real pair signatures and observed_by_field inputs."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_artifact_bundle_preparation as coordinator
from process import provider_directory_cms_capacity_contract as ledger
from process import provider_directory_cms_preflight as producer
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_runtime_observation as runtime
from process import provider_directory_serving_capacity_contract as ordinary
from tests.provider_directory_cms_capacity_test_support import rehash_guard
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_serving_capacity_signing_test_support import (
    serving_case,
    sign_serving_guard,
    verify_serving_envelope,
)
from tests.test_provider_directory_cms_preflight import (
    _database,
    _database_observation,
    _read_inputs_observations,
    fhir,
)
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _captured_inputs(monkeypatch, case):
    """Double only native capture; all request, geometry and signature codecs remain real."""

    class CapturedServingInputs:
        def __init__(self):
            self.database = fhir.db
            self.scope_payload = deepcopy(case.plan.payload["ordinary_scope"])
            self.overlay_fence = SimpleNamespace(datasets=case.fence.datasets[:1])
            self.recheck = AsyncMock()

    monkeypatch.setattr(coordinator, "CapturedServingInputs", CapturedServingInputs, raising=False)
    captured = CapturedServingInputs()
    receipt = case.guard["healthcare_receipt"]
    projection = SimpleNamespace(**receipt["artifact_scope_projection"])
    plan = producer._monitored_plan(
        case.request,
        case.profile,
        case.fence,
        projection,
        SimpleNamespace(native_targets=ordinary.NATIVE_ADDRESS_TARGETS, input_hash=case.plan.native_address_input_hash),
        {"address_overlay"},
        frozenset(case.plan.resource_types),
    )
    assert plan == case.plan
    observed_by_field = {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        **{
            name: getattr(case.profile.runtime_witness, name)
            for name in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
        },
    }
    inputs = producer._PreflightInputs(
        plan,
        case.fence,
        projection,
        None,
        None,
        observed_by_field,
        receipt["database_binding"],
        SimpleNamespace(
            payload=receipt["serving_generation_preflight"],
            payload_sha256=receipt["serving_generation_preflight_sha256"],
        ),
        captured,
    )
    return inputs, captured


def _pair_authority(monkeypatch, case):
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
    monkeypatch.setattr(
        producer.capacity_runtime, "configured_capacity_lease_trust", lambda: capacity_trust_from_envelope(case.pair)
    )


@pytest.mark.parametrize("initial_mode", [False, True])
@pytest.mark.parametrize("empty_scope", [False, True])
async def test_ordinary_projection_uses_original_read_snapshot_and_server_capture(
    monkeypatch, initial_mode, empty_scope
):
    case = serving_case(initial_mode=initial_mode)
    database, session, events = _database(monkeypatch)
    inputs, captured = _captured_inputs(monkeypatch, case)
    if empty_scope:
        captured.overlay_fence = SimpleNamespace(datasets=())
    _read_inputs_observations(monkeypatch, inputs, session, events)
    project = AsyncMock(wraps=fhir._provider_directory_artifact_scope_exact_projection)
    monkeypatch.setattr(fhir, "_provider_directory_artifact_scope_exact_projection", project)
    monkeypatch.setattr(fhir, "_attested_profile_publication_fence", AsyncMock(return_value=case.fence))
    monkeypatch.setattr(fhir, "_assert_profile_selection_matches_artifact_fence", Mock())

    async def capture(directory, execution, fence, *, retention_limits, refresh_source_ids):
        assert directory is fhir and execution is case.request.execution and fence is case.fence
        assert refresh_source_ids == case.plan.payload["ordinary_scope"]["requested_source_ids"]
        assert retention_limits == {
            name: case.plan.payload["ordinary_scope"][name] for name in ordinary.RETENTION_LIMIT_FIELDS
        }
        assert database._transaction_binding().session is session
        assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in events
        assert "projection" not in events
        events.append("capture")
        return captured

    monkeypatch.setattr(coordinator, "capture_serving_inputs", capture, raising=False)
    monkeypatch.setattr(
        producer, "cms_address_preparation", Mock(side_effect=AssertionError("CMS authority is unrelated"))
    )
    monkeypatch.setattr(
        producer, "_database_observation", AsyncMock(return_value=_database_observation(inputs, case.profile))
    )
    monkeypatch.setattr(producer, "observe_profile_runtime", AsyncMock(return_value=inputs.runtime))
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_serving", AsyncMock(return_value=inputs.serving))
    assert await producer._read_inputs(fhir, case.request, case.profile) == inputs
    assert events[-3:] == ["capture", "projection", "end"]
    assert project.await_args.args[1] is captured.overlay_fence
    assert project.await_args.kwargs["source_fence"] is captured.overlay_fence
    assert project.await_args.args[2] == frozenset(case.plan.resource_types)
    assert len(captured.overlay_fence.datasets) < len(case.fence.datasets)
    assert inputs.plan.desired_fence_hash == producer.desired_fence_hash(case.fence)
    assert producer._projection_fields(case.request, inputs)["capacity_geometry"] == case.plan.payload
    database.status.assert_not_awaited()


@pytest.mark.parametrize("changed", ["scope", "owner", "json"])
async def test_reviewed_scope_cannot_replace_server_capture(monkeypatch, changed):
    case = serving_case()
    _database(monkeypatch)
    _inputs, captured = _captured_inputs(monkeypatch, case)
    if changed == "scope":
        captured.scope_payload["max_retention_data_bytes"] += 1
    elif changed == "owner":
        captured.database = object()
    returned_capture = {"scope_payload": captured.scope_payload} if changed == "json" else captured
    monkeypatch.setattr(coordinator, "capture_serving_inputs", AsyncMock(return_value=returned_capture), raising=False)
    with pytest.raises(ValueError, match="captur"):
        await producer._address_observation(fhir, case.request, case.fence, object())


@pytest.mark.parametrize("initial_mode", [False, True])
async def test_real_produced_ordinary_receipt_signs_and_round_trips_existing_ledger(monkeypatch, initial_mode):
    case = serving_case(initial_mode=initial_mode)
    database, _session, _events = _database(monkeypatch)
    inputs, captured = _captured_inputs(monkeypatch, case)
    _pair_authority(monkeypatch, case)
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})
    monkeypatch.setattr(fhir, "assert_profile_selection_current_in_transaction", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", AsyncMock())
    monkeypatch.setattr(
        producer, "_database_observation", AsyncMock(return_value=_database_observation(inputs, case.profile))
    )
    monkeypatch.setattr(producer, "observe_profile_runtime", AsyncMock(return_value=inputs.runtime))
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_serving", AsyncMock(return_value=inputs.serving))
    monkeypatch.setattr(producer, "_paired_preflight", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_existing_receipt", AsyncMock(return_value=None))
    receipt_seed = case.guard["healthcare_receipt"]
    monkeypatch.setattr(
        producer, "_quiescence", AsyncMock(return_value=(receipt_seed["quiescence"], receipt_seed["quiescence_sha256"]))
    )
    monkeypatch.setattr(
        fhir,
        "_profile_capacity_preflight_receipt_layout",
        AsyncMock(return_value=receipt_seed["preflight_receipt_storage"]),
    )
    receipt = await producer._issue_receipt(fhir, case.request, inputs, case.profile)
    captured.recheck.assert_awaited_once_with(fhir)
    assert receipt["request_nonce"] != case.profile.signing_preflight_guard["healthcare_receipt"]["request_nonce"]
    case.guard["healthcare_receipt"] = receipt
    rehash_guard(case.guard)
    verified = verify_serving_envelope(sign_serving_guard(case.guard, case.pair))
    expected = ledger.capacity_preflight_receipt_row_values(case.request, receipt, issued_at=VALIDATION_TIME)
    assert database.status.await_args.kwargs == expected
    assert ledger.read_capacity_preflight_receipt(expected, verified) == receipt


async def test_changed_capture_refuses_before_receipt_or_ledger_write(monkeypatch):
    case = serving_case()
    database, _session, _events = _database(monkeypatch)
    inputs, captured = _captured_inputs(monkeypatch, case)
    captured.recheck.side_effect = RuntimeError("captured_predecessor_changed")
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})
    monkeypatch.setattr(fhir, "assert_profile_selection_current_in_transaction", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", AsyncMock())
    with pytest.raises(RuntimeError, match="captured_predecessor_changed"):
        await producer._issue_receipt(fhir, case.request, inputs, case.profile)
    database.status.assert_not_awaited()


@pytest.mark.parametrize("projection", [False, True])
async def test_existing_public_capacity_entry_dispatches_ordinary_purpose(monkeypatch, projection):
    case = serving_case()
    raw = deepcopy(case.request.request_payload)
    if projection:
        raw.update(contract_id=ordinary.PROJECTION_REQUEST)
        raw.pop("signing_guard")
    parser = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    request = parser(raw)
    method = "capacity_authority_projection" if projection else "capacity_preflight"
    original = AsyncMock(return_value={"closed": "ordinary"})
    monkeypatch.setattr(producer, method, original)
    entry = getattr(fhir, "provider_directory_profile_" + method)
    assert await entry(request) == {"closed": "ordinary"}
    original.assert_awaited_once_with(fhir, request)


async def test_invalid_real_paired_signature_precedes_projection_reads(monkeypatch):
    case = serving_case()
    database, _session, events = _database(monkeypatch)
    _pair_authority(monkeypatch, case)
    admission = deepcopy(case.request.serving_nonprofile_admission)
    signature = admission["paired_profile_lease"]["signature"]
    admission["paired_profile_lease"]["signature"] = ("A" if signature[0] != "A" else "B") + signature[1:]
    request = replace(case.request, serving_nonprofile_admission=admission)
    with pytest.raises(ValueError):
        await producer.capacity_preflight(fhir, request)
    assert events == []
    database.status.assert_not_awaited()


async def test_original_consumer_records_and_rechecks_distinct_ordinary_purpose(monkeypatch):
    from process import provider_directory_cms_nonprofile_capacity as shared

    case = serving_case()
    database, _session, _events = _database(monkeypatch)
    inputs, captured = _captured_inputs(monkeypatch, case)
    lease = verify_serving_envelope(case.envelope)
    owner = shared._CapacityProducer(
        fhir,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        case.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=captured,
    )
    owner._consume_preflight = AsyncMock()
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})
    monkeypatch.setattr(fhir, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_provider_directory_profile_capacity_control_run", AsyncMock())
    monkeypatch.setattr(fhir, "assert_profile_selection_current_in_transaction", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", AsyncMock())
    monkeypatch.setattr(
        fhir, "_provider_directory_profile_capacity_acceptance_time", AsyncMock(return_value=VALIDATION_TIME)
    )
    monkeypatch.setattr(shared, "observe_profile_runtime", AsyncMock(return_value=inputs.runtime))
    await owner._consume(inputs.runtime)
    row_by_field = dict(database.status.await_args.kwargs)
    assert row_by_field["admission_purpose"] == ordinary.PURPOSE
    assert row_by_field["lease_digest"] == lease.lease_digest
    database.all = AsyncMock(return_value=[row_by_field])
    await owner._assert_consumption()
    assert database.all.await_args.kwargs == {"run_id": owner.run_id, "purpose": ordinary.PURPOSE}
    row_by_field["lease_digest"] = "99" * 32
    with pytest.raises(RuntimeError, match="consumption_changed"):
        await owner._assert_consumption()
