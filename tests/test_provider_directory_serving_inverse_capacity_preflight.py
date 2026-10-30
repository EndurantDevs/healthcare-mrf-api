# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact preflight provenance and native baseline comparisons without database authority."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_inputs as inputs
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from tests.provider_directory_serving_inverse_capacity_support import execution, geometry, limits, scope, signing_guard


def _scope_changed(**fields):
    value = scope()
    value.update(fields)
    value["input_sha256"] = contract.digest(
        contract.INPUT_DOMAIN, {key: item for key, item in value.items() if key != "input_sha256"}
    )
    return value


def test_capture_preserves_resident_baseline_without_crediting_byte_growth():
    original = scope()
    grown = deepcopy(original)
    grown["relations"][0]["baseline_total_bytes"] += 8192
    grown["input_sha256"] = contract.digest(
        contract.INPUT_DOMAIN, {key: item for key, item in grown.items() if key != "input_sha256"}
    )
    assert inputs.has_same_captured_inputs(original, grown)
    assert original["relations"][0]["baseline_total_bytes"] == 8192
    grown["relations"][0]["native_sha256"] = "0" * 64
    grown["input_sha256"] = contract.digest(
        contract.INPUT_DOMAIN, {key: item for key, item in grown.items() if key != "input_sha256"}
    )
    with pytest.raises(ValueError, match="family_native"):
        inputs.has_same_captured_inputs(original, grown)


@pytest.mark.parametrize("field", ["forward_state_sha256", "retention_payload_sha256", "published_event_sha256"])
def test_native_capture_refuses_changed_event_state_or_scope(field):
    assert not inputs.has_same_captured_inputs(scope(), _scope_changed(**{field: "0" * 64}))


def test_engine_geometry_uses_exact_native_tablespace_names_and_finite_limits():
    request = contract.validated_inverse_projection_request(
        dict(contract_id=contract.PROJECTION_REQUEST, recovery_execution=execution(), limits=limits())
    )
    observation_by_field = dict(
        data_tablespace_oid=1663,
        data_tablespace_name="pg_default",
        temp_tablespace_oid=1663,
        temp_tablespace_name="pg_default",
    )
    assert (
        preflight.geometry(request, scope(), observation_by_field, dict(block_size=8192, wal_segment_size=16 * 1024**2))
        == geometry()
    )
    observation_by_field["data_tablespace_name"] = "other_volume"
    assert (
        preflight.geometry(
            request, scope(), observation_by_field, dict(block_size=8192, wal_segment_size=16 * 1024**2)
        )["data_tablespace_name"]
        == "other_volume"
    )
    with pytest.raises(ValueError):
        contract.validated_inverse_limits({**limits(), "temp_file_limit_bytes_per_backend": 1025})


def test_inverse_receipt_reuses_original_profile_provenance_without_forward_mode():
    guard = signing_guard()
    receipt = guard["healthcare_receipt"]
    request = contract.validated_inverse_preflight_request(guard["healthcare_request"])
    row = preflight.receipt_row_values(request, receipt, issued_at=contract.utc_second(receipt["issued_at"]))
    assert row["materialization_mode"] == "serving_inverse"
    assert row["selection_proof_id"] == scope()["profile_provenance"]["selection_proof_id"]
    assert row["serving_preflight_sha256"] == receipt["capacity_geometry"]["scope"]["input_sha256"]
    lease = SimpleNamespace(signing_preflight_guard=guard, nonce=receipt["receipt_sha256"])
    assert preflight.read_inverse_receipt_row(row, lease) == receipt
    row["control_generation"] += 1
    with pytest.raises(ValueError, match="stored_provenance"):
        preflight.read_inverse_receipt_row(row, lease)
