# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact CMS preflight inputs, monitored budgets and original Profile signatures."""

import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import provider_directory_cms_address as cms_address
from process import provider_directory_cms_capacity_contract as contract
from process import provider_directory_cms_preflight as producer
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_runtime_observation as runtime
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from tests.provider_directory_cms_capacity_test_support import cms_guard, cms_plan, rehash_guard, sign_guard
from tests.test_provider_directory_cms_address import address_build_case as address_build_case
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust, _verify

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _fresh_guard(plan=None):
    """Give the second admission its own replay nonce while retaining the original Profile pair."""
    guard = cms_guard(plan)
    nonce = "39" * 32
    guard["control_plane_request"]["signing_intent"]["request_nonce"] = nonce
    guard["control_plane_receipt"]["request_nonce"] = nonce
    guard["healthcare_request"]["signing_guard"]["request_nonce"] = nonce
    guard["healthcare_receipt"]["request_nonce"] = nonce
    rehash_guard(guard)
    return guard


def _request(*, projection=False, limits=None, retention_policy=None):
    raw = _fresh_guard()["healthcare_request"]
    if projection:
        raw["contract_id"] = contract.CMS_PROJECTION_REQUEST_CONTRACT
        raw.pop("signing_guard")
    raw[contract.CMS_ADMISSION_FIELD]["limits"].update(limits or {})
    if retention_policy is not None:
        raw[contract.CMS_ADMISSION_FIELD]["registry_source_retention"] = retention_policy
    validate = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    return validate(raw)


def _retention_policy(execution):
    """Use the real request DTO for the exact selected synthetic CMS dataset."""
    cms = execution.attestation.desired_cms_dataset
    request = RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(
            "fixture",
            cms["source_id"],
            cms["endpoint_id"],
            cms["dataset_id"],
            cms["dataset_hash"],
            "release-1",
            123,
            "cms-npd",
            execution.attestation.desired_profile_as_of,
        ),
        RegistryNetworkSourceCoordinates("fhir", cms["source_id"], "fixture", cms["dataset_id"], "producer", "edition"),
        execution.attestation.proof_id,
        "b" * 64,
        "c" * 64,
        4096,
        8192,
    )
    return request.policy(UUID("00000000-0000-0000-0000-000000000001"), "retained_owner", ("reader", "writer"))


def _inputs(request):
    template_plan, fence, profile_lease = cms_plan()
    projection = SimpleNamespace(
        projection_hash=template_plan.artifact_scope_projection_hash, projected_rows=21, projected_logical_bytes=4500
    )
    address = SimpleNamespace(
        native_targets=template_plan.native_address_targets, input_hash=template_plan.native_address_input_hash
    )
    plan = producer._monitored_plan(
        request, profile_lease, fence, projection, address, template_plan.publish_targets, template_plan.resource_types
    )
    receipt = cms_guard(plan)["healthcare_receipt"]
    serving = SimpleNamespace(
        payload=receipt["serving_generation_preflight"], payload_sha256=receipt["serving_generation_preflight_sha256"]
    )
    observed_runtime_by_field = {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        **{
            name: getattr(profile_lease.runtime_witness, name)
            for name in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
        },
    }
    return producer._PreflightInputs(
        plan,
        fence,
        projection,
        {"accepted": 1},
        {"revision": 3},
        observed_runtime_by_field,
        receipt["database_binding"],
        serving,
    ), profile_lease


def _database_observation(inputs, profile_lease):
    data_tablespace = next(entry for entry in profile_lease.tablespaces if entry.usage == "data")
    temp_tablespace = next(entry for entry in profile_lease.tablespaces if entry.usage == "temp")
    return {
        **{
            name: getattr(profile_lease, name)
            for name in ("database_system_identifier", "database_oid", "database_name")
        },
        "data_tablespace_oid": data_tablespace.tablespace_oid,
        "data_tablespace_name": data_tablespace.tablespace_name,
        "temp_tablespace_oid": temp_tablespace.tablespace_oid,
        "temp_tablespace_name": temp_tablespace.tablespace_name,
        "query_parallel_workers": 0,
        "maintenance_parallel_workers": 0,
        "temp_limit_bytes": inputs.plan.temp_file_limit_bytes_per_backend,
    }


def _database(monkeypatch):
    events, active = [], []
    session = SimpleNamespace(execute=AsyncMock(side_effect=lambda statement: events.append(str(statement))))

    @asynccontextmanager
    async def transaction():
        assert not active
        active.append(session)
        events.append("begin")
        try:
            yield session
        finally:
            active.pop()
            events.append("end")

    database = SimpleNamespace(
        transaction=transaction,
        _transaction_binding=lambda: SimpleNamespace(session=session) if active else None,
        status=AsyncMock(return_value=1),
    )
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(producer, "apply_temp_file_limit", AsyncMock())
    return database, session, events


def test_reviewed_ceilings_are_reserved_with_deterministic_phase_budgets():
    request = _request(limits={"max_wal_bytes": 100_001})
    inputs, profile_lease = _inputs(request)
    assert dict(inputs.plan.reservation_bytes) == {"data": 100_000, "temp": 100_000, "wal": 100_001}
    assert inputs.plan.logging_wal_upper_bound_bytes == 50_000
    assert inputs.plan.cutover_wal_upper_bound_bytes == 50_001
    assert inputs.plan.paired_profile_lease_digest == profile_lease.lease_digest
    assert inputs.plan.native_address_input_hash == "ef" * 32
    assert inputs.plan.artifact_scope_projection_hash == "cd" * 32
    assert "capacity_complete" not in inputs.plan.payload


def test_insufficient_wal_ceiling_fails_without_inventing_phase_budget():
    with pytest.raises(RuntimeError, match="phase_wal_budget_too_small"):
        _inputs(_request(limits={"max_wal_bytes": 1}))


@pytest.mark.asyncio
async def test_invalid_profile_signature_precedes_every_database_mutation(monkeypatch):
    request = _request()
    admission = deepcopy(request.cms_nonprofile_admission)
    signature = admission["paired_profile_lease"]["signature"]
    admission["paired_profile_lease"]["signature"] = ("A" if signature[0] != "A" else "B") + signature[1:]
    request = replace(request, cms_nonprofile_admission=admission)
    database, _session, events = _database(monkeypatch)
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
    monkeypatch.setattr(producer.capacity_runtime, "configured_capacity_lease_trust", _trust)
    with pytest.raises(ValueError, match="invalid_signature"):
        await producer.capacity_preflight(fhir, request)
    assert events == []
    database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_registration_commits_before_read_snapshot_and_restores_context(monkeypatch):
    request = _request(projection=True)
    inputs, profile_lease = _inputs(request)
    _database_value, _session, events = _database(monkeypatch)
    monkeypatch.setattr(producer, "_verified_pair", AsyncMock(return_value=profile_lease))

    async def register(session, schema):
        assert fhir.db._transaction_binding().session is session
        events.append("register")

    async def read_inputs(*_args):
        assert fhir.db._transaction_binding() is None
        assert events[-1] == "end" and "register" in events
        assert fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is request.execution
        events.append("read")
        return inputs

    monkeypatch.setattr(producer, "register_native_address_inputs", register)
    monkeypatch.setattr(producer, "_read_inputs", read_inputs)
    prior = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    result = await producer.capacity_authority_projection(fhir, request)
    assert result["contract_id"] == contract.CMS_PROJECTION_CONTRACT
    assert result["required_reservation_bytes_by_storage_class"] == dict(inputs.plan.reservation_bytes)
    assert result["database_binding"] == inputs.database_binding
    assert result["capacity_geometry"] == inputs.plan.payload
    assert fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is prior


def _retention_preflight_inputs(monkeypatch, request, inputs, address_build_case, policy):
    """Bind the actual address factory before computing its expected signed geometry."""
    monkeypatch.setattr(fhir, "_schema", lambda: "fixture")
    inputs = replace(inputs, native_dependencies=address_build_case[3], native_input_fence=address_build_case[4])
    bare_factory = cms_address.cms_address_preparation(
        fhir,
        request.execution,
        inputs.fence,
        inputs.native_dependencies,
        native_input_fence=inputs.native_input_fence,
        run_id="run_" + "2" * 32,
        worker_count=inputs.plan.worker_count,
        temp_file_limit_bytes_per_backend=inputs.plan.temp_file_limit_bytes_per_backend,
    )
    retained_factory = bare_factory.with_registry_source_retention(policy)
    inputs = replace(
        inputs,
        plan=replace(
            inputs.plan,
            native_address_targets=retained_factory.native_targets,
            native_address_input_hash=retained_factory.input_hash,
        ),
    )
    return inputs, bare_factory, retained_factory


def _read_inputs_observations(monkeypatch, inputs, session, events):
    """Retain all source/projection observations in the same synthetic read snapshot."""
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})
    monkeypatch.setattr(fhir, "assert_profile_selection_current_in_transaction", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_provider_directory_artifact_resource_types",
        lambda *_args, **_kwargs: frozenset(inputs.plan.resource_types),
    )
    monkeypatch.setattr(
        fhir, "PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS", (*inputs.plan.publish_targets, "profile", "corroboration")
    )
    monkeypatch.setattr(fhir, "_assert_provider_directory_artifact_scope_exact_capacity", Mock())
    monkeypatch.setattr(producer, "_paired_preflight", AsyncMock())

    def in_snapshot(label, result):
        async def observe(*_args, **_kwargs):
            assert fhir.db._transaction_binding().session is session
            assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in events
            events.append(label)
            return result

        return observe

    monkeypatch.setattr(producer, "prepare_desired_fence", in_snapshot("full-proof", inputs.fence))
    monkeypatch.setattr(
        fhir,
        "_provider_directory_artifact_scope_exact_projection",
        in_snapshot("projection", inputs.artifact_projection),
    )
    monkeypatch.setattr(
        producer.receipts, "capture_native_dependencies", in_snapshot("dependencies", inputs.native_dependencies)
    )
    monkeypatch.setattr(
        producer, "capture_native_address_input_fence", in_snapshot("revisions", inputs.native_input_fence)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("retention", [False, True])
async def test_full_proof_projection_and_revisions_share_one_read_snapshot(monkeypatch, address_build_case, retention):
    request = _request()
    policy = _retention_policy(request.execution) if retention else None
    request = _request(retention_policy=policy)
    inputs, profile_lease = _inputs(request)
    _database_value, session, events = _database(monkeypatch)
    if retention:
        inputs, bare_factory, retained_factory = _retention_preflight_inputs(
            monkeypatch, request, inputs, address_build_case, policy
        )
    _read_inputs_observations(monkeypatch, inputs, session, events)
    factory = Mock(
        return_value=SimpleNamespace(
            native_targets=inputs.plan.native_address_targets, input_hash=inputs.plan.native_address_input_hash
        )
    )
    if retention:
        factory = Mock(wraps=cms_address.cms_address_preparation)
    monkeypatch.setattr(producer, "cms_address_preparation", factory)
    monkeypatch.setattr(
        producer, "_database_observation", AsyncMock(return_value=_database_observation(inputs, profile_lease))
    )
    monkeypatch.setattr(producer, "observe_profile_runtime", AsyncMock(return_value=inputs.runtime))
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_serving", AsyncMock(return_value=inputs.serving))
    actual = await producer._read_inputs(fhir, request, profile_lease)
    assert actual == inputs
    assert events.index("full-proof") < events.index("revisions") < events.index("end")
    producer.apply_temp_file_limit.assert_awaited_once_with(fhir.db, 1024)
    assert "SET LOCAL max_parallel_workers_per_gather=0" in events
    assert "SET LOCAL max_parallel_maintenance_workers=0" in events
    assert factory.call_args.kwargs["native_input_fence"] is inputs.native_input_fence
    if retention:
        assert actual.plan.native_address_input_hash == retained_factory.input_hash
        assert actual.plan.native_address_input_hash != bare_factory.input_hash


def _issue_stubs(monkeypatch, request, inputs, profile_lease):
    _database_value, _session, events = _database(monkeypatch)
    monkeypatch.setattr(producer, "_assert_current_inputs", AsyncMock())
    monkeypatch.setattr(producer, "_verified_pair", AsyncMock(return_value=profile_lease))
    monkeypatch.setattr(producer, "_paired_preflight", AsyncMock())
    monkeypatch.setattr(fhir, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_existing_receipt", AsyncMock(return_value=None))
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", AsyncMock(return_value=VALIDATION_TIME))
    seed = cms_guard(inputs.plan)["healthcare_receipt"]
    monkeypatch.setattr(
        producer, "_quiescence", AsyncMock(return_value=(seed["quiescence"], seed["quiescence_sha256"]))
    )
    monkeypatch.setattr(
        fhir, "_profile_capacity_preflight_receipt_layout", AsyncMock(return_value=seed["preflight_receipt_storage"])
    )
    return events


@pytest.mark.asyncio
async def test_produced_receipt_passes_real_signature_and_exact_row_mapper(monkeypatch):
    request = _request()
    inputs, profile_lease = _inputs(request)
    _issue_stubs(monkeypatch, request, inputs, profile_lease)
    receipt = await producer._issue_receipt(fhir, request, inputs, profile_lease)
    inserted = fhir.db.status.await_args.kwargs
    expected = contract.capacity_preflight_receipt_row_values(request, receipt, issued_at=VALIDATION_TIME)
    assert inserted == expected
    guard = _fresh_guard(inputs.plan)
    guard["healthcare_receipt"] = receipt
    rehash_guard(guard)
    verified = _verify(sign_guard(guard, cms=True), expected_capacity_geometry_hash=inputs.plan.capacity_geometry_hash)
    assert verified.signing_preflight_guard["healthcare_receipt"] == receipt
    assert contract.read_capacity_preflight_receipt(expected, verified) == receipt
    assert profile_lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"] != inputs.plan.payload


@pytest.mark.asyncio
async def test_drift_rejects_before_any_receipt_is_written(monkeypatch):
    request = _request()
    inputs, profile_lease = _inputs(request)
    _issue_stubs(monkeypatch, request, inputs, profile_lease)
    monkeypatch.setattr(
        producer, "_assert_current_inputs", AsyncMock(side_effect=RuntimeError("native_inputs_changed"))
    )
    with pytest.raises(RuntimeError, match="native_inputs_changed"):
        await producer._issue_receipt(fhir, request, inputs, profile_lease)
    fhir.db.status.assert_not_awaited()
    producer._paired_preflight.assert_not_awaited()


@pytest.mark.asyncio
async def test_preflight_may_not_outlive_profile_pair(monkeypatch):
    request = _request()
    inputs, profile_lease = _inputs(request)
    _issue_stubs(monkeypatch, request, inputs, profile_lease)
    from datetime import timedelta

    request = replace(request, expires_at=profile_lease.expires_at + timedelta(seconds=1))
    with pytest.raises(RuntimeError, match="paired_profile_expiry_too_short"):
        await producer._issue_receipt(fhir, request, inputs, profile_lease)
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "native", "runtime", "serving", "tablespace"])
async def test_fresh_bounded_checks_keep_endpoint_first_and_detect_drift(monkeypatch, changed):
    request = _request()
    inputs, profile_lease = _inputs(request)
    _database_value, session, events = _database(monkeypatch)
    monkeypatch.setattr(fhir, "assert_profile_selection_current_in_transaction", AsyncMock())
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})

    async def endpoint(_fence):
        events.append("endpoint")

    async def native(_session, _schema, expected):
        assert events[-1] == "endpoint" and expected == inputs.native_input_fence
        events.append("native")
        if changed == "native":
            raise RuntimeError("native_inputs_changed")

    monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", endpoint)
    monkeypatch.setattr(producer, "assert_native_address_input_fence", native)
    monkeypatch.setattr(producer.receipts, "assert_native_dependencies", AsyncMock())
    observation = _database_observation(inputs, profile_lease)
    if changed == "tablespace":
        observation["data_tablespace_oid"] += 1
    monkeypatch.setattr(producer, "_database_observation", AsyncMock(return_value=observation))
    actual_runtime_by_field = {
        **inputs.runtime,
        **({"healthcare_source_commit": "ba" * 20} if changed == "runtime" else {}),
    }
    monkeypatch.setattr(producer, "observe_profile_runtime", AsyncMock(return_value=actual_runtime_by_field))
    serving = (
        SimpleNamespace(payload={"changed": True}, payload_sha256="00" * 32) if changed == "serving" else inputs.serving
    )
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_serving", AsyncMock(return_value=serving))
    if changed:
        with pytest.raises(RuntimeError, match="changed"):
            await producer._assert_current_inputs(fhir, request, inputs, profile_lease, session)
    else:
        await producer._assert_current_inputs(fhir, request, inputs, profile_lease, session)
    if changed != "native":
        producer._database_observation.assert_awaited_once_with(fhir, inputs.plan)
    assert events[:2] == ["endpoint", "native"]
