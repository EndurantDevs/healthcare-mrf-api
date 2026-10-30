# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain actual execution identity, prepare hashes before cut and clean only unpublished data."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_ordinary_serving as ordinary


@dataclass(frozen=True)
class ParsedRequest:
    """Observe an in-process execution replacement without rewriting its signed wire bytes."""

    execution: object
    request_payload: dict


@pytest.mark.parametrize("changed", [None, "paired_digest", "selection", "generation"])
async def test_verify_pair_before_retaining_actual_execution_and_initial_mode(monkeypatch, changed):
    attestation = SimpleNamespace(payload={"proof": "selected"})
    execution = SimpleNamespace(
        attestation=attestation,
        generation=1,
        capacity_attestation={"original": "initial_pair"},
        serving_nonprofile_capacity_attestation={"original": "serving_pair"},
    )
    parsed_execution = SimpleNamespace(
        attestation=SimpleNamespace(payload={"proof": "other"} if changed == "selection" else attestation.payload),
        generation=2 if changed == "generation" else 1,
        capacity_attestation={},
    )
    request = ParsedRequest(parsed_execution, {"profile_execution": "original_canonical_wire"})
    envelope_by_field = {
        "lease": {
            "signing_preflight_guard": {"healthcare_request": {}, "healthcare_receipt": {"capacity_geometry": {}}}
        }
    }
    plan = SimpleNamespace(capacity_geometry_hash="geometry", paired_profile_lease_digest="paired")
    trust = SimpleNamespace(database_system_identifier="system", database_oid=17, database_name="fixture")
    fhir = SimpleNamespace(_profile_capacity_preflight_clock=AsyncMock(return_value="verified_now"))
    signature = Mock()
    pair = AsyncMock(return_value=SimpleNamespace(lease_digest="other" if changed == "paired_digest" else "paired"))
    monkeypatch.setattr(ordinary, "validated_serving_execution_capacity", Mock(return_value=envelope_by_field))
    monkeypatch.setattr(ordinary, "validated_serving_preflight_request", Mock(return_value=request))
    monkeypatch.setattr(ordinary, "validated_serving_capacity_geometry", Mock(return_value=plan))
    monkeypatch.setattr(ordinary, "assert_serving_geometry_matches_request", Mock())
    monkeypatch.setattr(ordinary, "configured_capacity_lease_trust", lambda: trust)
    monkeypatch.setattr(ordinary, "verify_database_capacity_lease", signature)
    monkeypatch.setattr(ordinary.preflight, "_verified_pair", pair)
    if changed is None:
        normalized, returned_plan, _lease = await ordinary._verified_request(fhir, execution)
        assert normalized.execution is execution and returned_plan is plan
        assert normalized.request_payload is request.request_payload
    else:
        with pytest.raises(
            RuntimeError, match="paired_lease_changed" if changed == "paired_digest" else "execution_changed"
        ):
            await ordinary._verified_request(fhir, execution)
    signature.assert_called_once_with(
        envelope_by_field,
        trust=trust,
        now="verified_now",
        expected_capacity_geometry_hash="geometry",
        expected_database_system_identifier="system",
        expected_database_oid=17,
        expected_database_name="fixture",
    )
    pair.assert_awaited_once_with(fhir, request)
    assert request.execution is parsed_execution and request.execution.capacity_attestation == {}


@pytest.fixture
def case(monkeypatch):
    plan = SimpleNamespace(payload={"scope": "signed"})
    capture, fence = object(), object()
    admission = object()
    execution = SimpleNamespace(attestation=SimpleNamespace(payload={"proof": "selected"}), generation=1)
    fhir = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: None),
        _validated_admission_run_id=Mock(return_value="run_" + "1" * 32),
        _PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION=ContextVar("test_profile_capacity", default=None),
    )
    expected_case_by_family = {"profile": {"rows": 1, "data_sha256": "ab" * 32}}
    snapshot = SimpleNamespace(plan=plan, fence=fence, captured_inputs=capture)
    monkeypatch.setattr(ordinary, "_verified_request", AsyncMock(return_value=(object(), plan, object())))
    monkeypatch.setattr(ordinary.preflight, "_read_inputs", AsyncMock(return_value=snapshot))
    monkeypatch.setattr(ordinary, "produce_ordinary_address_admission", AsyncMock(return_value=admission))
    monkeypatch.setattr(ordinary, "configured_storage_continuation", lambda: "original_authority")
    monkeypatch.setattr(ordinary.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    discard = AsyncMock()
    monkeypatch.setattr(ordinary.retention, "discard_unpublished_retention", discard)
    event_names = []

    async def retain(prepared, expected_by_family):
        expected_by_family.update(expected_case_by_family)
        prepared.retained_generation = SimpleNamespace(cutover_actor=None)
        event_names.append("retained_and_hashed")
        return prepared.retained_generation

    @asynccontextmanager
    async def prepare(prepared, **options):
        assert prepared.execution is execution and prepared.fence is fence
        assert options["address_preparation"].keywords == {"run_id": "run_" + "1" * 32}
        await options["retention_preparation"](prepared)
        try:
            yield prepared
        finally:
            event_names.append("builders_closed")

    monkeypatch.setattr(ordinary, "_retain_with_postimages", retain)
    monkeypatch.setattr(ordinary, "prepare_ordinary_serving_artifacts", prepare)
    return SimpleNamespace(
        fhir=fhir,
        execution=execution,
        plan=plan,
        snapshot=snapshot,
        admission=admission,
        capture=capture,
        fence=fence,
        expected=expected_case_by_family,
        discard=discard,
        trace=event_names,
    )


@pytest.mark.parametrize("outcome", ["uncut", "attempted", "caller_failure"])
async def test_shared_builders_finish_before_cut_and_unpublished_retention_is_cleaned(case, outcome):
    try:
        async with ordinary.prepare_current_ordinary_serving(
            case.fhir,
            execution=case.execution,
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
        ) as (prepared, hashes):
            assert hashes == case.expected and case.trace == ["retained_and_hashed"]
            assert prepared.nonprofile_admission is case.admission
            if outcome == "attempted":
                prepared.retained_generation.cutover_actor = {"xid": "actual"}
            if outcome == "caller_failure":
                raise RuntimeError("caller_failed")
    except RuntimeError as error:
        assert outcome == "caller_failure" and str(error) == "caller_failed"
    else:
        assert outcome != "caller_failure"
    assert case.trace == ["retained_and_hashed", "builders_closed"]
    if outcome == "attempted":
        case.discard.assert_not_awaited()
    else:
        case.discard.assert_awaited_once_with(prepared)
    assert case.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None


async def test_changed_capture_is_rejected_before_admission_and_builders(case):
    case.snapshot.plan = SimpleNamespace(payload={"scope": "changed"})
    with pytest.raises(RuntimeError, match="capture_geometry_changed"):
        async with ordinary.prepare_current_ordinary_serving(
            case.fhir,
            execution=case.execution,
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
        ):
            pytest.fail("changed signed scope accepted")
    ordinary.produce_ordinary_address_admission.assert_not_awaited()
    assert case.trace == []


async def test_bound_transaction_is_rejected_before_any_authority_read(case):
    case.fhir.db._transaction_binding = lambda: object()
    with pytest.raises(RuntimeError, match="requires_own_transaction"):
        async with ordinary.prepare_current_ordinary_serving(
            case.fhir,
            execution=case.execution,
            run_id="run_" + "1" * 32,
            control_run_id="run_" + "1" * 32,
            metrics={},
        ):
            pytest.fail("borrowed transaction accepted")
    ordinary._verified_request.assert_not_awaited()
    ordinary.preflight._read_inputs.assert_not_awaited()
