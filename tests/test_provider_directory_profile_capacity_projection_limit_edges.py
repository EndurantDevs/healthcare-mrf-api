# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Aggregate projection limit edge coverage."""

from __future__ import annotations

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_physical as physical
from process import provider_directory_profile_capacity_target as target
from tests.test_provider_directory_profile_capacity_projection import (
    _projection_geometry,
    _projection_inputs,
)


def test_scratch_projection_enforces_growth_cap():
    relation_cap = next(
        cap
        for cap in _projection_geometry().relation_byte_caps
        if cap.relation_name == "evidence_stage"
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="scratch_projection_growth_exceeded",
    ):
        physical._assert_scratch_projection_caps(
            relation_cap,
            growth_bytes=relation_cap.max_scratch_bytes + 1,
            wal_bytes=0,
        )


def test_delta_projection_enforces_aggregate_wal_reservation(monkeypatch):
    geometry_state = _projection_geometry()
    reserved_wal = geometry_state.reservation_bytes_by_storage_class["wal"]

    def projected_target(_geometry, delta_input):
        return capacity.ProviderDirectoryProfileTargetDeltaProjection(
            relation_name=delta_input.relation_name,
            target_growth_bytes=0,
            deleted_logical_bytes=0,
            wal_bytes=reserved_wal,
        )

    monkeypatch.setattr(
        target,
        "_target_delta_projection",
        projected_target,
    )
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="delta_projection_total_wal_exceeded",
    ):
        target.project_profile_delta_capacity(
            geometry_state,
            _projection_inputs(),
        )
