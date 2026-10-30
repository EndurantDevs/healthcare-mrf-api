# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial reactivation preserves historical facts and requires the exact recovered cut."""

import hashlib
from contextvars import ContextVar
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_bundle_preparation as capture
from process import provider_directory_cms_preflight as producer
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_initial_recovery as recovery
from process import provider_directory_serving_recovery as journal
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME
from tests.test_provider_directory_profile_initial import _target
from tests.test_provider_directory_serving_input_capture import _fixture


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _witness(original):
    return {
        "initial_targets": deepcopy(original),
        "initial_receipt": {
            "build_id": "pdpb_" + "c" * 32,
            "generation_id": "pdprofile_" + "d" * 32,
            "run_id": "run_" + "e" * 32,
            "payload_sha256": "ef" * 32,
        },
        "recovery_id": "2cfaed08-62e5-4641-a011-d3491f1cb48f",
        "recovered_event_sha256": "ab" * 32,
    }


async def _recovered_fixture(monkeypatch, *, history=True):
    _case, directory, _fence, state, roots, _limits = _fixture(monkeypatch, initial=True, history=history)
    original = (await initial.capture_targets(directory, "fixture")).payload
    witness = _witness(original)
    scalar = directory.db.scalar.side_effect
    first = directory.db.first.side_effect
    original_terminal = deepcopy(state.history)

    async def recovered_scalar(query, **parameters):
        if contract.RECEIPT_TABLE in query:
            return True
        return await scalar(query, **parameters)

    async def recovered_first(query, **parameters):
        if "WHERE run_id=:run" in query:
            assert parameters["run"] == original_terminal["run_id"]
            return deepcopy(original_terminal)
        return await first(query, **parameters)

    directory.db.scalar.side_effect = recovered_scalar
    directory.db.first.side_effect = recovered_first
    directory.db.first.reset_mock()
    provider = AsyncMock(return_value=witness)
    monkeypatch.setattr(journal, "recovered_initial_targets", provider, raising=False)
    return directory, state, roots, witness, provider


@pytest.mark.parametrize("history", [False, True])
async def test_reactivation_keeps_original_capture_and_current_storage_sizes(monkeypatch, history):
    directory, state, roots, witness, provider = await _recovered_fixture(monkeypatch, history=history)
    original = deepcopy(witness["initial_targets"])
    roots["provider_directory_profile"]["total_bytes"] += 8192
    if history:
        state.history["run_id"] = "run_" + "f" * 32
        state.history["result"]["profile_generation_id"] = "pdprofile_" + "f" * 32
    captured = (await initial.capture_targets(directory, "fixture")).payload
    assert captured["contract_id"] == contract.RECOVERED_TARGET_CONTRACT
    assert captured["initial_commit_receipt_absent"] is False
    assert captured["serving_singleton_absent"] is True
    assert captured["profile_target_bytes"] == original["profile_target_bytes"] + 8192
    assert captured["recovered_initial_publication"]["initial_targets"] == original
    assert captured["historical_publication"] == original["historical_publication"]
    assert all("ORDER BY finished_at" not in call.args[0] for call in directory.db.first.call_args_list)
    if history:
        assert captured["historical_publication"]["profile_as_of"] is None
        assert "profile_as_of" not in captured["historical_publication"]["result"]
    provider.assert_awaited_once_with(directory, "fixture")
    directory.db.status.assert_not_awaited()
    assert witness["initial_targets"] == original


@pytest.mark.parametrize("changed", ["missing_witness", "old_oid", "old_layout", "history", "mixed_generation"])
async def test_reactivation_refuses_unproven_or_different_restored_targets(monkeypatch, changed):
    directory, _state, _roots, witness, provider = await _recovered_fixture(monkeypatch)
    if changed == "missing_witness":
        provider.return_value = None
    if changed == "old_oid":
        directory._locked_profile_adoption_target_oids.return_value = (20003, 20001)
    if changed == "old_layout":
        directory._provider_directory_profile_relation_storage_fingerprint.return_value.exact_fingerprint = "cd" * 32
    if changed == "history":
        witness["initial_targets"]["historical_publication"]["run_id"] = "run_" + "f" * 32
        original_first = directory.db.first.side_effect

        async def missing_history(query, **parameters):
            return None if "WHERE run_id=:run" in query else await original_first(query, **parameters)

        directory.db.first.side_effect = missing_history
    if changed == "mixed_generation":
        original_scalar = directory.db.scalar.side_effect

        async def mixed_scalar(query, **parameters):
            return True if "generation_id <>" in query else await original_scalar(query, **parameters)

        directory.db.scalar.side_effect = mixed_scalar
    with pytest.raises(RuntimeError, match="initial_(recovery_changed|receipt_exists)"):
        await initial.capture_targets(directory, "fixture")


@pytest.mark.parametrize("changed", ["head", "cms_history"])
async def test_existing_active_publication_rejections_are_preserved(monkeypatch, changed):
    directory, state, _roots, _witness_value, provider = await _recovered_fixture(monkeypatch)
    if changed == "head":
        state.serving = object()
    else:
        original_scalar = directory.db.scalar.side_effect

        async def cms_scalar(query, **parameters):
            if "to_regclass(:ref)" in query or "provider_directory_cms_serving_receipt" in query:
                return True
            return await original_scalar(query, **parameters)

        directory.db.scalar.side_effect = cms_scalar
    with pytest.raises(RuntimeError, match="initial_(serving_exists|common_history_exists)"):
        await initial.capture_targets(directory, "fixture")
    provider.assert_not_awaited()


@pytest.mark.parametrize(
    "changed",
    [
        "receipt_absence",
        "extra",
        "nested_v2",
        "original_hash",
        "current_oid",
        "current_rows",
        "recovery_id",
        "receipt_id",
        "receipt_hash",
    ],
)
def test_recovered_target_codec_rejects_changed_history_or_open_fields(changed):
    target = recovery.recovered_target_state(_target(), _witness(_target()))
    reference = target["recovered_initial_publication"]
    if changed == "receipt_absence":
        target["initial_commit_receipt_absent"] = True
    if changed == "extra":
        reference["unchecked"] = True
    if changed == "nested_v2":
        reference["initial_targets"] = deepcopy(target)
    if changed == "original_hash":
        reference["original_target_state_sha256"] = "00" * 32
    if changed == "current_oid":
        target["profile_target_oid"] += 1
    if changed == "current_rows":
        target["profile_rows"] += 1
    if changed == "recovery_id":
        reference["recovery_id"] = "NOT-A-UUID"
    if changed == "receipt_id":
        reference["initial_receipt"]["build_id"] = "foreign"
    if changed == "receipt_hash":
        reference["initial_receipt"]["payload_sha256"] = "A" * 64
    with pytest.raises(ValueError):
        contract.validated_target_state(target)


def test_cutover_requires_same_recovered_event_and_preserves_v1_hash_domain():
    original = _target()
    historical_digest = hashlib.sha256(
        contract.TARGET_CONTRACT.encode("ascii") + b"\0" + contract.canonical_json(original).encode("ascii")
    ).hexdigest()
    assert contract.target_state_sha256(original) == historical_digest
    recovery.assert_recovered_binding(original, None)
    witness = _witness(original)
    target = recovery.recovered_target_state(original, witness)
    recovery.assert_recovered_binding(target, witness)
    assert contract.target_state_sha256(target) != historical_digest
    witness["recovered_event_sha256"] = "cd" * 32
    with pytest.raises(RuntimeError, match="recovery_changed"):
        recovery.assert_recovered_binding(target, witness)
    with pytest.raises(RuntimeError, match="recovery_changed"):
        recovery.assert_recovered_binding(original, witness)


async def test_initial_preflight_uses_verified_pair_without_changing_signed_request(monkeypatch):
    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch, initial=True, history=True)
    request = case.request
    original_payload = deepcopy(request.request_payload)
    assert request.execution.capacity_attestation == {}
    directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES = ContextVar("preflight_overrides", default={})
    directory._profile_capacity_preflight_clock = AsyncMock(return_value=VALIDATION_TIME)
    monkeypatch.setattr(
        producer.capacity_runtime, "configured_capacity_lease_trust", lambda: capacity_trust_from_envelope(case.pair)
    )
    bound = directory.db._transaction_binding()
    directory.db._transaction_binding = lambda: None

    async def observe(original_directory, paired_request, verified_pair):
        assert original_directory is directory
        assert verified_pair.lease_digest == case.profile.lease_digest
        assert paired_request.execution.capacity_attestation == case.pair
        assert directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is paired_request.execution
        assert paired_request.request_payload == original_payload
        directory.db._transaction_binding = lambda: bound
        return await capture.capture_serving_inputs(directory, paired_request.execution, fence, retention_limits=limits)

    monkeypatch.setattr(producer, "_read_inputs", observe)
    monkeypatch.setattr(producer, "_projection_fields", lambda _request, observed: {"scope": observed.scope_payload})
    projected = await producer.capacity_authority_projection(directory, request)
    assert projected["scope"]["retention_kind"] == "snapshot"
    assert request.request_payload == original_payload and request.execution.capacity_attestation == {}
    assert directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None
