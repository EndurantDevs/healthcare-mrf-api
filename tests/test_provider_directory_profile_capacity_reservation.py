# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Physical reservation gates for signed Profile capacity leases."""

from __future__ import annotations

import datetime

import pytest

from process import provider_directory_profile_capacity_attestation as lease
from tests.test_provider_directory_profile_capacity_attestation import UTC, _verify


def _assert_capacity_reservation_refused(
    verified_lease,
    *,
    required_bytes_by_storage_class: dict[str, int],
    minimum_remaining_bytes: int,
    required_build_seconds: int,
    reason: str,
) -> None:
    """Assert one exact physical-capacity or deadline refusal."""

    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match=reason):
        lease.assert_database_capacity_lease_reservation(
            verified_lease,
            required_bytes_by_storage_class=required_bytes_by_storage_class,
            minimum_remaining_bytes=minimum_remaining_bytes,
            required_build_seconds=required_build_seconds,
        )


def test_reservation_gate_checks_every_class_and_physical_remaining_space():
    """Require every signed storage reservation to fit its physical volume."""

    verified = _verify()
    lease.assert_database_capacity_lease_reservation(
        verified,
        required_bytes_by_storage_class={
            "data": 175_000_000_000,
            "temp": 20_000_000_000,
            "wal": 140_000_000_000,
        },
        minimum_remaining_bytes=250_000_000_000,
        required_build_seconds=3_000,
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={
            "data": 180_000_000_001,
            "temp": 20_000_000_000,
            "wal": 140_000_000_000,
        },
        minimum_remaining_bytes=1,
        required_build_seconds=3_000,
        reason="reservation_too_small",
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=300_000_000_001,
        required_build_seconds=3_000,
        reason="remaining_capacity_too_small",
    )
    _assert_capacity_reservation_refused(
        verified,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=1,
        required_build_seconds=3_301,
        reason="deadline_too_short",
    )
    skewed_lease = _verify(
        now=datetime.datetime(2026, 7, 30, 11, 59, 58, tzinfo=UTC)
    )
    _assert_capacity_reservation_refused(
        skewed_lease,
        required_bytes_by_storage_class={"data": 1, "temp": 1, "wal": 1},
        minimum_remaining_bytes=1,
        required_build_seconds=3_300,
        reason="deadline_too_short",
    )
