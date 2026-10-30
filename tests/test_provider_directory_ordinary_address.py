# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the shared candidate builder and its failure cleanup on the verified scope."""

import importlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_ordinary_address as address


@pytest.fixture
def case(monkeypatch):
    database = SimpleNamespace(_transaction_binding=lambda: None)
    fhir = importlib.import_module("process.provider_directory_fhir")
    native = importlib.import_module("process.entity_address_unified")
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(fhir, "_schema", lambda: "fixture")
    fence = object()
    capture = SimpleNamespace(
        _fence=fence,
        address_task={"refresh_mode": "provider-directory-partial", "source_ids": ["example"]},
        predecessor={
            "native": {
                "observed": {"alias_state": {"generation": 8}},
                "current_dependency_bindings": {"fixture.npi": {"relation_oid": 17}},
            }
        },
    )
    plan = SimpleNamespace(desired_profile_as_of="2026-01-01", native_address_input_hash="ab" * 32)
    admission = SimpleNamespace(plan=plan, captured_inputs=capture, assert_ready=AsyncMock())
    prepared, inputs, context, overlay = object(), object(), {"control_run_id": "run_example"}, object()
    preparation_inputs = Mock(return_value=inputs)
    build, cleanup = AsyncMock(return_value=prepared), AsyncMock()
    verified = Mock()
    monkeypatch.setattr(address, "require_capture", Mock(return_value=capture))
    monkeypatch.setattr(address, "_preparation_inputs", preparation_inputs)
    monkeypatch.setattr(address, "_new_context", AsyncMock(return_value=context))
    monkeypatch.setattr(address.ordinary, "require_prepared", verified)
    monkeypatch.setattr(address.candidates, "prepare_provider_directory_entity_address", build)
    monkeypatch.setattr(address.candidates, "cleanup_prepared_entity_address_generation", cleanup)
    return SimpleNamespace(
        fhir=fhir,
        native=native,
        fence=fence,
        capture=capture,
        admission=admission,
        prepared=prepared,
        inputs=inputs,
        context=context,
        overlay=overlay,
        preparation_inputs=preparation_inputs,
        build=build,
        cleanup=cleanup,
        verified=verified,
    )


@pytest.mark.parametrize("failure", [None, "caller", "after_build"])
async def test_original_candidate_inputs_lifetime_and_failure_cleanup(case, failure):
    original_task = deepcopy(case.capture.address_task)
    if failure == "after_build":
        case.admission.assert_ready.side_effect = [None, RuntimeError("changed")]
    try:
        async with address.prepare_ordinary_address(
            case.fence, {"resource": "owned_stage"}, case.overlay, case.admission, run_id="run_example"
        ) as prepared:
            assert prepared is case.prepared
            case.cleanup.assert_not_awaited()
            if failure == "caller":
                raise RuntimeError("caller")
    except RuntimeError as error:
        assert str(error) == ("changed" if failure == "after_build" else "caller")
        assert failure is not None
    else:
        assert failure is None
    case.cleanup.assert_awaited_once_with(case.prepared)
    case.verified.assert_called_once_with(case.prepared, case.admission)
    case.preparation_inputs.assert_called_once_with(
        case.fence, {"resource": "owned_stage"}, case.overlay, {"alias_generation": 8}, "2026-01-01"
    )
    case.build.assert_awaited_once_with(
        case.context,
        original_task,
        preparation_input=case.inputs,
        admission=case.admission,
        native_input_hash="ab" * 32,
        dependency_bindings={"fixture.npi": {"relation_oid": 17}},
    )
    assert case.capture.address_task == original_task


@pytest.mark.parametrize("changed", ["database", "transaction", "fence", "empty_scope"])
async def test_refuse_detached_owner_or_scope_before_candidate_creation(case, changed, monkeypatch):
    if changed == "database":
        monkeypatch.setattr(case.native, "db", object())
    elif changed == "transaction":
        monkeypatch.setattr(case.fhir.db, "_transaction_binding", lambda: object())
    elif changed == "fence":
        case.capture._fence = object()
    else:
        case.capture.address_task = None
    with pytest.raises(RuntimeError, match="provider_directory_ordinary_address_"):
        async with address.prepare_ordinary_address(case.fence, {}, case.overlay, case.admission, run_id="run_example"):
            pytest.fail("detached preparation was accepted")
    case.build.assert_not_awaited()
    case.cleanup.assert_not_awaited()
