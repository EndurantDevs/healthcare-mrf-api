# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh standalone purpose, historical linkage and measured-boundary regressions."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_serving_inverse_capacity as capacity
from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_recovery as recovery
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_serving_inverse_capacity_support import signed_envelope
from tests.test_provider_directory_serving_inverse_capacity_signing import verify

RUN = "run_" + "a" * 32


def task():
    envelope = signed_envelope()
    execution = envelope["lease"]["signing_preflight_guard"]["healthcare_request"]["recovery_execution"]
    return {
        recovery.OPERATION_PARAM: "recover",
        recovery.GENERATION_PARAM: execution["generation"],
        "source_ids": execution["source_ids"],
        contract.EVENT_FIELD: execution["published_event_sha256"],
        contract.EXECUTION_FIELD: envelope,
        "run_id": RUN,
    }


@pytest.mark.parametrize("damage", ["event", "generation", "source", "acquisition", "forward"])
def test_request_refuses_operation_substitution_or_forward_flags(damage):
    request = task()
    if damage == "event":
        request[contract.EVENT_FIELD] = "0" * 64
    elif damage == "generation":
        request[recovery.GENERATION_PARAM] = "00000000-0000-4000-8000-000000000099"
    elif damage == "source":
        request["source_ids"] = ["other"]
    elif damage == "acquisition":
        request["import_resources"] = False
    else:
        request["provider_directory_profile_capacity_attestation"] = {}
    with pytest.raises(ValueError):
        capacity.task_execution(request)


def test_existing_public_adapter_retains_closed_inverse_params_and_global_writer_conflict():
    from api import control_imports as api

    request = task()
    params_by_field = {key: value for key, value in request.items() if key != "run_id"}
    assert recovery.is_recovery_request("provider-directory-fhir", params_by_field)
    kind = api._provider_directory_operation(params_by_field)
    assert kind == (api._PROVIDER_DIRECTORY_GLOBAL_PROFILE, frozenset(), None)
    assert api._has_provider_directory_operation_conflict(
        kind, (api._PROVIDER_DIRECTORY_SCOPED_ARTIFACT, frozenset({"other"}), None)
    )
    assert not api._has_provider_directory_operation_conflict(
        kind, (api._PROVIDER_DIRECTORY_ACQUISITION, frozenset({"other"}), "endpoint")
    )


@pytest.mark.asyncio
async def test_real_signed_consumption_is_historical_evidence_without_fresh_authority(monkeypatch):
    import importlib

    fhir = importlib.import_module("process.provider_directory_fhir")
    request = task()
    envelope = request[contract.EXECUTION_FIELD]
    lease = verify(envelope)
    plan = lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]
    consumption_by_field = {
        "admission_purpose": contract.PURPOSE,
        **capacity_lease_consumption_values(
            lease, capacity._binding(RUN, plan), accepted_at=contract.utc_second(envelope["lease"]["issued_at"])
        ),
    }
    db = SimpleNamespace(all=AsyncMock(return_value=[consumption_by_field]))
    actual = SimpleNamespace(
        db=db,
        _schema=lambda: "unit",
        _unscoped_qt=fhir._unscoped_qt,
        ProviderDirectoryProfileCapacityLeaseConsumption=fhir.ProviderDirectoryProfileCapacityLeaseConsumption,
        _pagination_checkpoint_row_mapping=lambda value: value,
        _provider_directory_profile_capacity_consumption_identity=fhir._provider_directory_profile_capacity_consumption_identity,
    )
    monkeypatch.setattr(capacity, "configured_capacity_lease_trust", lambda: capacity_trust_from_envelope(envelope))
    operation = capacity.task_execution(request).recovery_execution
    inverse_by_field = dict(
        run_id=RUN, operation_sha256=contract.operation_sha256(operation), lease_digest=lease.lease_digest
    )
    assert (
        await capacity.consumed_inverse_lease(
            actual,
            inverse_by_field,
            generation=operation["generation"],
            published_event_sha256=operation["published_event_sha256"],
        )
        == lease
    )
    consumption_by_field["executable_plan_hash"] = "0" * 64
    with pytest.raises(ValueError, match="inverse_consumption_changed"):
        await capacity.consumed_inverse_lease(
            actual,
            inverse_by_field,
            generation=operation["generation"],
            published_event_sha256=operation["published_event_sha256"],
        )
    db.all.return_value = []
    with pytest.raises(ValueError, match="inverse_consumption_missing"):
        await capacity.consumed_inverse_lease(
            actual,
            inverse_by_field,
            generation=operation["generation"],
            published_event_sha256=operation["published_event_sha256"],
        )


async def _boundary_inputs(monkeypatch):
    """Build synthetic signed values while keeping the tested owner on this same task."""
    import importlib

    fhir = importlib.import_module("process.provider_directory_fhir")
    request = task()
    lease = verify(request[contract.EXECUTION_FIELD])
    plan = deepcopy(lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"])
    binding = object()
    consumption_by_field = {
        "admission_purpose": contract.PURPOSE,
        **capacity_lease_consumption_values(lease, capacity._binding(RUN, plan), accepted_at=lease.issued_at),
    }
    db = SimpleNamespace(
        _transaction_binding=lambda: binding,
        all=AsyncMock(return_value=[consumption_by_field]),
        scalar=AsyncMock(return_value=0),
    )
    actual = SimpleNamespace(
        db=db,
        _schema=lambda: "unit",
        _unscoped_qt=fhir._unscoped_qt,
        ProviderDirectoryProfileCapacityLeaseConsumption=fhir.ProviderDirectoryProfileCapacityLeaseConsumption,
        _pagination_checkpoint_row_mapping=lambda value: value,
        _provider_directory_profile_capacity_consumption_identity=fhir._provider_directory_profile_capacity_consumption_identity,
        _profile_capacity_preflight_clock=AsyncMock(return_value=lease.issued_at),
    )
    admitted = capacity.InverseAdmission(actual, capacity.task_execution(request), RUN, lease, plan, None, None, "0/1")
    actor_by_field = dict(
        session="builder",
        current="builder",
        owner=plan["scope"]["owner_oid"],
        backend=7,
        xid="8",
        database_oid=plan["scope"]["database_oid"],
    )
    admitted.binding = binding
    admitted.actor_json = capacity.encoded(actor_by_field)
    admitted.consumption = consumption_by_field
    admitted.witness = SimpleNamespace(expires_at=lease.expires_at)
    monkeypatch.setattr(capacity.receipts, "ordinary", AsyncMock(return_value=(None, None, actor_by_field)))
    observed_by_field = {
        name: getattr(lease, name) for name in ("database_system_identifier", "database_oid", "database_name")
    }
    for entry in lease.tablespaces:
        observed_by_field[entry.usage + "_tablespace_oid"] = entry.tablespace_oid
        observed_by_field[entry.usage + "_tablespace_name"] = entry.tablespace_name
    observed_by_field.update(
        query_parallel_workers=0,
        maintenance_parallel_workers=0,
        temp_limit_bytes=plan["temp_file_limit_bytes_per_backend"],
    )
    monkeypatch.setattr(capacity, "_read_database_observation", AsyncMock(return_value=observed_by_field))
    inventory_by_field = {
        entry["oid"]: {"total_bytes": entry["baseline_total_bytes"]} for entry in plan["scope"]["relations"]
    }
    monkeypatch.setattr(capacity.inputs, "assert_scope_native", AsyncMock(return_value=inventory_by_field))
    return actual, admitted, inventory_by_field, db, plan, lease


@pytest.mark.asyncio
async def test_resident_growth_has_no_delete_credit_and_is_checked_before_commit(monkeypatch):
    """Reject missing native headroom/WAL/deadline on the original admitted owner."""
    actual, admitted, inventory_by_field, db, plan, lease = await _boundary_inputs(monkeypatch)
    oids = sorted(inventory_by_field)
    inventory_by_field[oids[0]]["total_bytes"] += 8192
    inventory_by_field[oids[1]]["total_bytes"] = 0
    assert await admitted.boundary() == 0
    assert admitted.peak_data_bytes == 8192
    inventory_by_field[oids[0]]["total_bytes"] = 0
    await admitted.boundary()
    assert admitted.peak_data_bytes == 8192
    inventory_by_field[oids[2]]["total_bytes"] += dict(plan["reservation_bytes"])["data"] + 1
    with pytest.raises(ValueError, match="data_budget"):
        await admitted.boundary()
    inventory_by_field[oids[2]]["total_bytes"] = 0
    admitted.peak_data_bytes = 0
    db.scalar.return_value = plan["preparation_wal_upper_bound_bytes"] + 1
    with pytest.raises(ValueError, match="preparation_wal_budget"):
        await admitted.boundary()
    admitted.cutover_start_wal = 0
    db.scalar.return_value = plan["cutover_wal_upper_bound_bytes"] + 1
    with pytest.raises(ValueError, match="cutover_wal_budget"):
        await admitted.boundary()
    db.scalar.return_value = 0
    actual._profile_capacity_preflight_clock.return_value = lease.max_build_deadline
    with pytest.raises(ValueError, match="deadline"):
        await admitted.boundary()


@pytest.mark.parametrize("damage", ["absent", "reuse", "operation", "override"])
def test_inverse_retry_never_reuses_capacity_or_changes_operation(damage):
    request = task()
    replacement = deepcopy(request[contract.EXECUTION_FIELD])
    replacement["lease"].update(attestation_id="b" * 64, reservation_id="fresh-reservation", nonce="c" * 64)
    overrides_by_field = {contract.EXECUTION_FIELD: replacement}
    if damage == "absent":
        overrides_by_field = {}
    elif damage == "reuse":
        overrides_by_field = {contract.EXECUTION_FIELD: request[contract.EXECUTION_FIELD]}
    elif damage == "operation":
        replacement["lease"]["signing_preflight_guard"]["healthcare_request"]["recovery_execution"]["source_ids"] = [
            "other"
        ]
    else:
        overrides_by_field["source_ids"] = ["other"]
    with pytest.raises(ValueError):
        capacity.validated_inverse_retry_params(request, overrides_by_field, "run_" + "d" * 32)


def test_existing_core_retry_rotates_only_fresh_inverse_envelope():
    from api import control_imports as api

    request = task()
    replacement = deepcopy(request[contract.EXECUTION_FIELD])
    replacement["lease"].update(attestation_id="b" * 64, reservation_id="fresh-reservation", nonce="c" * 64)
    refreshed = api._retry_child_params(
        {"importer": "provider-directory-fhir", "params": request},
        "run_" + "d" * 32,
        {contract.EXECUTION_FIELD: replacement},
    )
    assert refreshed["run_id"] == "run_" + "d" * 32
    assert refreshed[contract.EVENT_FIELD] == request[contract.EVENT_FIELD]
    assert refreshed[contract.EXECUTION_FIELD] is replacement
