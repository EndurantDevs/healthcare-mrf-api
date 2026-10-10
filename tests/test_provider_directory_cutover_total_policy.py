# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep bounded cutover WAL acceptance tied to the complete signed reservation."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_cutover as cutover
from tests.test_provider_directory_profile_bounded_capacity import _bounded_cutover_receipt
from tests.test_provider_directory_profile_bounded_capacity import admitted_window as admitted_window

fhir = importlib.import_module("process.provider_directory_fhir")


def test_replay_allows_global_increment_above_window_forecast_with_signed_headroom():
    geometry, receipt, run_id = _bounded_cutover_receipt(0)
    forecast, actual = receipt["cutover_forecast_json"], receipt["cutover_actual_json"]
    totals = actual["target_windows"]["evidence_target"]
    totals["projected_wal_bytes"] = 1
    actual["cutover_wal_bytes"] = (
        sum(
            cap.max_wal_bytes
            for cap in geometry.relation_byte_caps
            if cap.relation_name in {"evidence_target", "profile_target"}
        )
        + 1
    )
    observed = cutover._lsn_bytes(actual["target_wal_start_lsn"]) + actual["cutover_wal_bytes"]
    actual["wal_observed_lsn"] = f"{observed >> 32:X}/{observed & 0xFFFFFFFF:X}"
    validation_args_by_name = {
        name: receipt[name]
        for name in ("build_id", "evidence_inserted", "evidence_deleted", "profile_inserted", "profile_deleted")
    }
    validation_args_by_name.update(run_id=run_id, forecast_hash=receipt["cutover_forecast_hash"])
    capacity.validate_profile_delta_cutover_evidence(geometry, forecast, actual, **validation_args_by_name)
    actual["cutover_wal_bytes"] = geometry.reservation_bytes_by_storage_class["wal"] + 1
    observed = cutover._lsn_bytes(actual["target_wal_start_lsn"]) + actual["cutover_wal_bytes"]
    actual["wal_observed_lsn"] = f"{observed >> 32:X}/{observed & 0xFFFFFFFF:X}"
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="total_wal_projection_exceeded"):
        capacity.validate_profile_delta_cutover_evidence(geometry, forecast, actual, **validation_args_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("overrun", [False, True])
async def test_final_metadata_uses_complete_signed_total_not_local_interval(admitted_window, monkeypatch, overrun):
    admission, state = admitted_window
    forecast = SimpleNamespace(
        target_projection=SimpleNamespace(wal_bytes=1),
        metadata_projection=SimpleNamespace(wal_bytes=80, commit_envelope_bytes=20),
        wal_start_lsn="0/1",
    )
    admission.wal_tracker.pending_metadata_wal_bytes = 100
    admission.wal_tracker.accounted_metadata_wal_bytes = 100
    monkeypatch.setattr(fhir, "_profile_delta_cutover_wal_bytes", AsyncMock(return_value=309744))
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_wal_budget", AsyncMock())
    state.wal = admission.geometry.reservation_bytes_by_storage_class["wal"] - (19 if overrun else 20)
    if overrun:
        with pytest.raises(RuntimeError, match="final_wal_exceeded"):
            await fhir._validate_profile_delta_final_wal(admission, forecast)
        assert admission.wal_tracker.pending_metadata_wal_bytes == 100
        assert admission.wal_tracker.unresolved_window
    else:
        await fhir._validate_profile_delta_final_wal(admission, forecast)
        assert admission.wal_tracker.pending_metadata_wal_bytes == 20
