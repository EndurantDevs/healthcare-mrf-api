# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure branch coverage for UHC/Profile importer boundary helpers."""

from __future__ import annotations

import datetime
import json
import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_capacity_preflight_contract as contract
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    synthetic_profile_execution,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    EXPIRES_AT,
    ISSUED_AT,
    _geometry,
    _quiescence,
    _receipt,
    _receipt_storage,
    _request,
    _runtime_observation,
    _serving_state,
    _transaction,
    importer,
)


@pytest.fixture(autouse=True)
def _import_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _workload():
    request = _request()
    return types.SimpleNamespace(
        limits_payload=request.limits_payload,
        limits_sha256=request.limits_sha256,
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="8" * 64,
        ),
    )


def _lease(receipt):
    return types.SimpleNamespace(
        nonce=receipt["receipt_sha256"],
        expires_at=EXPIRES_AT,
        observed_at=ISSUED_AT,
        issued_at=ISSUED_AT,
        attestation_id="attestation-1",
        signing_preflight_guard={
            "healthcare_receipt": dict(receipt),
            "healthcare_receipt_sha256": receipt["receipt_sha256"],
            "healthcare_request_sha256": receipt["request_sha256"],
            "control_plane_receipt_sha256": (receipt["control_plane_receipt_sha256"]),
            "capacity_limits_sha256": receipt["capacity_limits_sha256"],
        },
    )


def _receipt_row(receipt):
    values = importer._profile_capacity_preflight_receipt_values(
        _request(),
        receipt,
        issued_at=ISSUED_AT,
    )
    return {
        **values,
        "receipt_json": receipt,
        "consumed_at": None,
    }


def test_flex_dispatch_and_trigger_shape_guards(monkeypatch):
    monkeypatch.setattr(importer, "is_uhc_flex_profile_source", lambda _source: True)
    monkeypatch.setattr(
        importer,
        "_artifact_flex_retained_resources",
        lambda **_kwargs: ("Practitioner",),
    )
    assert importer._artifact_dataset_retained_resources(
        source_id="synthetic-flex",
        endpoint_id="endpoint",
        dataset_id="dataset",
        evidence_run_id="run",
        selected_resources=("Practitioner",),
        publication_metadata={},
        dataset_scoped_ready=True,
    ) == ("Practitioner",)

    triggers = [
        {
            "tgtype": 10,
            "trigger_enabled": "A",
            "trigger_function_oid": index,
            "trigger_function_source": "ignored",
        }
        for index in range(3)
    ]
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="trigger_shape_changed",
    ):
        importer._assert_profile_capacity_trigger_shape(
            triggers,
            3,
            "unexpected-error",
            expected_single_use_receipt=True,
        )


@pytest.mark.asyncio
async def test_profile_control_layout_missing_and_unbudgeted(monkeypatch):
    layouts = tuple(
        types.SimpleNamespace(
            exact_fingerprint=str(index),
            effective_tablespace_oids=(1663,),
        )
        for index in range(4)
    )
    identity = types.SimpleNamespace(
        build_checkpoint_storage_fingerprint="0",
        import_run_storage_fingerprint="1",
        capacity_consumption_storage_fingerprint="2",
        serving_generation_storage_fingerprint="3",
        tablespace_oid=1663,
    )
    monkeypatch.setattr(
        importer,
        "_profile_primary_control_layouts",
        AsyncMock(return_value=layouts),
    )
    relation_oid = AsyncMock(return_value=None)
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", relation_oid)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="preflight_relation_missing",
    ):
        await importer._profile_control_layouts(identity)

    relation_oid.return_value = 20_008
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(
            return_value=types.SimpleNamespace(
                effective_tablespace_oids=(9999,),
            )
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="preflight_layout_unbudgeted",
    ):
        await importer._profile_control_layouts(identity)


def test_stored_receipt_shape_digest_and_execution_identity():
    receipt_by_field = _receipt()
    capacity_lease = _lease(receipt_by_field)
    receipt_row_by_field = _receipt_row(receipt_by_field)
    receipt_row_by_field["receipt_json"] = json.dumps(receipt_by_field)
    assert (
        importer._profile_capacity_preflight_stored_receipt(
            receipt_row_by_field,
            capacity_lease,
        )
        == receipt_by_field
    )

    invalid_shape_by_field = dict(receipt_by_field)
    invalid_shape_by_field.pop("quiescence")
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_invalid",
    ):
        importer._profile_capacity_preflight_stored_receipt(
            {**receipt_row_by_field, "receipt_json": invalid_shape_by_field},
            capacity_lease,
        )

    invalid_digest_by_field = {
        **receipt_by_field,
        "quiescence_sha256": "0" * 64,
    }
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_invalid",
    ):
        importer._profile_capacity_preflight_stored_receipt(
            {**receipt_row_by_field, "receipt_json": invalid_digest_by_field},
            capacity_lease,
        )

    expected_execution_identity = (
        importer._profile_capacity_expected_execution_identity(
            synthetic_profile_execution()
        )
    )
    assert expected_execution_identity["materialization_mode"] == "source_delta"
    assert expected_execution_identity["profile_strategy_version"] == (
        synthetic_profile_execution().attestation.profile_strategy_version
    )


def test_receipt_body_binding_guard():
    """Reject a preflight receipt whose closed body changes."""
    receipt = _receipt()
    row_by_field = _receipt_row(receipt)
    lease = _lease(receipt)
    execution = synthetic_profile_execution()
    workload = _workload()
    geometry = _geometry()

    importer._assert_profile_capacity_receipt_body_binding(
        row_by_field, receipt, execution, workload, geometry, lease
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="binding_changed",
    ):
        importer._assert_profile_capacity_receipt_body_binding(
            row_by_field,
            {**receipt, "contract_id": "changed"},
            execution,
            workload,
            geometry,
            lease,
        )


def test_receipt_static_binding_guard():
    """Reject static ledger fields that drift from the signed receipt."""
    receipt = _receipt()
    row_by_field = _receipt_row(receipt)
    lease = _lease(receipt)
    execution = synthetic_profile_execution()
    workload = _workload()
    geometry = _geometry()
    importer._assert_profile_capacity_preflight_static_binding(
        row_by_field, receipt, execution, workload, geometry, lease
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="binding_changed",
    ):
        importer._assert_profile_capacity_preflight_static_binding(
            {**row_by_field, "limits_sha256": "0" * 64},
            receipt,
            execution,
            workload,
            geometry,
            lease,
        )


def test_receipt_open_guard():
    """Reject consumed receipts at the serialized admission clock."""
    receipt = _receipt()
    row_by_field = _receipt_row(receipt)
    lease = _lease(receipt)
    importer._assert_profile_capacity_receipt_open(
        row_by_field,
        lease,
        ISSUED_AT + datetime.timedelta(seconds=1),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_expired",
    ):
        importer._assert_profile_capacity_receipt_open(
            {**row_by_field, "consumed_at": ISSUED_AT},
            lease,
            ISSUED_AT + datetime.timedelta(seconds=1),
        )


def test_receipt_serving_guard():
    """Reject a receipt that no longer matches serving generation state."""
    receipt = _receipt()
    identity = types.SimpleNamespace(serving_state=_serving_state())
    importer._assert_profile_capacity_receipt_serving(receipt, identity)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_changed",
    ):
        importer._assert_profile_capacity_receipt_serving(
            {**receipt, "serving_generation_preflight": None}, identity
        )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_changed",
    ):
        importer._assert_profile_capacity_receipt_serving(
            {**receipt, "serving_generation_preflight_sha256": "0" * 64},
            identity,
        )


@pytest.mark.asyncio
async def test_receipt_storage_state_and_single_use_consume_guards(monkeypatch):
    receipt = _receipt()
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_receipt_layout",
        AsyncMock(return_value=receipt["preflight_receipt_storage"]),
    )
    await importer._assert_profile_capacity_receipt_storage("mrf", receipt)

    quiescence, quiescence_sha256 = _quiescence()
    monkeypatch.setattr(
        importer,
        "_profile_capacity_quiescence",
        AsyncMock(return_value=(quiescence, quiescence_sha256)),
    )
    await importer._assert_profile_capacity_receipt_state(
        "mrf",
        "run-1",
        receipt,
        receipt["runtime_observation"],
        ISSUED_AT,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="state_changed",
    ):
        await importer._assert_profile_capacity_receipt_state(
            "mrf", "run-1", receipt, {"changed": True}, ISSUED_AT
        )

    status = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(importer.db, "status", status)
    await importer._mark_profile_capacity_receipt_consumed(
        "mrf", "run-1", _lease(receipt), ISSUED_AT
    )
    status.return_value = "UPDATE 0"
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="consume_lost",
    ):
        await importer._mark_profile_capacity_receipt_consumed(
            "mrf", "run-1", _lease(receipt), ISSUED_AT
        )

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_missing",
    ):
        await importer._consume_profile_capacity_preflight_receipt(
            schema="mrf",
            run_id="run-1",
            execution=synthetic_profile_execution(),
            identity=types.SimpleNamespace(),
            workload=types.SimpleNamespace(),
            geometry=_geometry(),
            lease=_lease(receipt),
            runtime_observation={},
        )


@pytest.mark.asyncio
async def test_locked_serving_adoption_paths(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    bad_lease = types.SimpleNamespace(
        signing_preflight_guard={
            "healthcare_receipt": {
                "serving_generation_preflight": {"resolution": "existing"}
            }
        }
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="adoption_not_signed",
    ):
        await importer._locked_profile_admission_serving_state(bad_lease)

    adopted = _serving_state()
    monkeypatch.setattr(
        importer,
        "_adopt_provider_directory_profile_serving_generation",
        AsyncMock(return_value=adopted),
    )
    good_lease = types.SimpleNamespace(
        signing_preflight_guard={
            "healthcare_receipt": {
                "serving_generation_preflight": {"resolution": "legacy_adoption"}
            }
        }
    )
    assert await importer._locked_profile_admission_serving_state(good_lease) == adopted

    with pytest.raises(RuntimeError, match="serving_resolution_invalid"):
        importer._profile_capacity_preflight_serving_payload(
            adopted,
            resolution="invalid",
        )
