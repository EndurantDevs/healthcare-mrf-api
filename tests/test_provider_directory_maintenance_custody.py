# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Long-lived maintenance is consumed only at its actual terminal boundary."""

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_control_maintenance as maintenance
from tests.test_provider_directory_retained_maintenance_coupled import close_healthy, coupled_fixture

pytestmark = pytest.mark.asyncio


async def test_original_maintenance_custody_spans_parents_without_early_authority():
    fhir, connection, _, admission, _ = await coupled_fixture()
    try:
        async with maintenance.control_maintenance_fence(fhir, connection):
            held = admission.wal_tracker.owned_control_maintenance_custody
            original = held["original_outcome"]
            identity_group = held["identity_check_group"]
            identity_owner = identity_group["original_outcome"]
            assert identity_group is held["original_identity_check_group"]
            assert identity_group["consumed"] and identity_owner.is_committed
            assert identity_owner is not original and identity_owner.connection is not connection
            assert held["task"] is asyncio.current_task() and held["connection"] is connection
            assert held["admission"] is admission and held["lease"].owned_outcome is original
            assert held["status"] == "incomplete" and not held["consumed"]
            assert original.measurement is None and not original.is_committed
            async with fhir._profile_capacity_mutation_window("evidence_stage"):
                admission.wal_tracker.owned_control_transaction_groups.clear()
                assert fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get() is not held["acquisition_window"]
                assert held["status"] == "incomplete" and original.measurement is None
                assert held["identity_check_group"] is identity_group
                assert held["ordinary_body_physical_wal_upper_bytes"] is None
            await maintenance.release_before_cutover(fhir)
            assert held["outcome"] is original is admission.wal_tracker.owned_control_maintenance_outcome
            assert held["terminal_window"] is not held["acquisition_window"]
            assert held["consumed"] and held["status"] == "complete_long_lived_mixed_unclassified"
            assert original.status == "committed_measured" and original.cleanup_complete
            assert not held["accounting_authority"] and not held["reservation_refund"]
            assert held["ordinary_body_native_outcomes"] == (original, identity_owner)
            assert held["ordinary_body_physical_wal_upper_bytes"] == 618
            assert held["whole_owner_wal_readiness"] == "incomplete"
            assert held["unobserved_owner_phases"] == ("preparation", "restoration", "sampling_tail")
        assert connection.events.count("begin") == connection.events.count("commit") == 1
    finally:
        await close_healthy(connection)


@pytest.mark.parametrize(
    "field",
    [
        "lease",
        "task",
        "connection",
        "admission",
        "acquisition_window",
        "original_outcome",
        "retained_driver",
        "retained_pid",
        "terminal_window",
        "consumed",
    ],
)
async def test_misbound_terminal_custody_retains_authentic_committed_outcome(monkeypatch, field):
    fhir, connection, _, admission, _ = await coupled_fixture()
    consume = maintenance._consume_owned_maintenance_custody

    def corrupt(lease, outcome):
        lease.owned_custody[field] = object()
        consume(lease, outcome)

    monkeypatch.setattr(maintenance, "_consume_owned_maintenance_custody", corrupt)
    try:
        with pytest.raises(RuntimeError, match="maintenance_custody_incomplete"):
            async with maintenance.control_maintenance_fence(fhir, connection):
                await maintenance.release_before_cutover(fhir)
        held = admission.wal_tracker.owned_control_maintenance_custody
        outcome = admission.wal_tracker.owned_control_maintenance_outcome
        assert outcome.status == "committed_measured" and outcome.cleanup_complete and outcome.is_committed
        assert held["status"] == "incomplete" and held["consumed"] is not True
        assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)


async def test_terminal_maintenance_custody_cannot_be_consumed_twice():
    fhir, connection, _, admission, _ = await coupled_fixture()
    try:
        async with maintenance.control_maintenance_fence(fhir, connection):
            await maintenance.release_before_cutover(fhir)
            held = admission.wal_tracker.owned_control_maintenance_custody
            with pytest.raises(RuntimeError, match="maintenance_custody_incomplete"):
                maintenance._consume_owned_maintenance_custody(held["lease"], held["outcome"])
            assert held["consumed"] and not held["accounting_authority"]
        assert connection.events.count("commit") == 1
    finally:
        await close_healthy(connection)


async def test_rollback_keeps_original_failure_and_incomplete_custody():
    fhir, connection, _, admission, _ = await coupled_fixture()
    primary = OSError("synthetic primary failure")
    with pytest.raises(OSError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            raise primary
    held = admission.wal_tracker.owned_control_maintenance_custody
    outcome = admission.wal_tracker.owned_control_maintenance_outcome
    assert caught.value is primary and held["outcome"] is outcome is held["original_outcome"]
    assert outcome.commit_state == "rolled_back" and outcome.cleanup_complete
    assert held["status"] == "incomplete" and not held["consumed"]
    assert not held["accounting_authority"] and not held["reservation_refund"]
    assert connection.closed and connection.state.checkedout == 0


def _replace_identity_group(held, group, change):
    """Corrupt one original binding without creating a replacement native owner."""
    if change == "group":
        held["identity_check_group"] = dict(group)
        return
    if change == "outcome":
        group["outcome"] = held["original_outcome"]
        return
    if change == "one_owner":
        group["outcome"] = group["original_outcome"] = held["original_outcome"]
        return
    if change == "commit":
        group["original_outcome"].commit_state = "uncertain"
        return
    if change == "measurement":
        group["original_outcome"].measurement = None
        return
    group[change] = object()


@pytest.mark.parametrize(
    "change",
    ["group", "outcome", "one_owner", "identity", "driver", "pid", "window", "consumed", "commit", "measurement"],
)
async def test_identity_callback_replacement_never_completes_family(change):
    fhir, connection, _, admission, _ = await coupled_fixture()
    try:
        with pytest.raises(RuntimeError, match="maintenance_identity_custody_incomplete"):
            async with maintenance.control_maintenance_fence(fhir, connection):
                held = admission.wal_tracker.owned_control_maintenance_custody
                group = held["identity_check_group"]
                _replace_identity_group(held, group, change)
                await maintenance.release_before_cutover(fhir)
        assert held["original_outcome"].is_committed and not held["consumed"]
        assert held["ordinary_body_physical_wal_upper_bytes"] is None
        assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)


@pytest.mark.parametrize(
    "record_bytes,records,expected",
    [(Decimal(0), 0, 0), (Decimal(25), 1, 112), (Decimal(128), 3, 309), (Decimal(8153), 1, 8280)],
)
@pytest.mark.parametrize("server_version", (180002, 180006))
async def test_terminal_receipt_consumes_checked_body_upper(record_bytes, records, expected, server_version):
    fhir, connection, _, admission, _ = await coupled_fixture()
    admission.geometry = replace(admission.geometry, postgres_server_version_num=server_version)
    admission.control_wal_projection = capacity.project_profile_control_wal_capacity(
        admission.geometry, admission.control_wal_projection.plan_input
    )
    try:
        async with maintenance.control_maintenance_fence(fhir, connection):
            held = admission.wal_tracker.owned_control_maintenance_custody
            group = held["identity_check_group"]
            outcome = group["original_outcome"]
            outcome.measurement = replace(outcome.measurement, wal_bytes_delta=record_bytes, wal_records_delta=records)
            await maintenance.release_before_cutover(fhir)
            assert held["ordinary_body_physical_wal_upper_bytes"] == 309 + expected
            assert held["whole_owner_wal_readiness"] == "incomplete"
            assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)


@pytest.mark.parametrize(
    "record_bytes,records,error",
    [
        (Decimal("NaN"), 1, "ordinary_wal_invalid"),
        (Decimal("Infinity"), 1, "ordinary_wal_invalid"),
        (Decimal("25.5"), 1, "ordinary_wal_invalid"),
        (Decimal(-1), 0, "ordinary_wal_invalid"),
        (Decimal(1), 0, "ordinary_wal_invalid"),
        (Decimal(24), 1, "ordinary_wal_invalid"),
        (25, 1, "ordinary_wal_invalid"),
        (Decimal(25), True, "ordinary_wal_invalid"),
        (Decimal(2**63 - 1), 1, "ordinary_wal_overflow"),
        (Decimal(100000), 1, "body_wal_overrun"),
    ],
)
@pytest.mark.parametrize("server_version", (180002, 180006))
async def test_bad_observation_preserves_commit_without_family_consumption(
    record_bytes, records, error, server_version
):
    fhir, connection, _, admission, _ = await coupled_fixture()
    admission.geometry = replace(admission.geometry, postgres_server_version_num=server_version)
    admission.control_wal_projection = capacity.project_profile_control_wal_capacity(
        admission.geometry, admission.control_wal_projection.plan_input
    )
    try:
        with pytest.raises(RuntimeError, match=error):
            async with maintenance.control_maintenance_fence(fhir, connection):
                held = admission.wal_tracker.owned_control_maintenance_custody
                callback = held["identity_check_group"]["original_outcome"]
                callback.measurement = replace(
                    callback.measurement, wal_bytes_delta=record_bytes, wal_records_delta=records
                )
                await maintenance.release_before_cutover(fhir)
        assert held["original_outcome"].is_committed and not held["consumed"]
        assert held["ordinary_body_physical_wal_upper_bytes"] is None
        assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)


async def test_callback_failure_preserves_original_exception_and_both_exposures():
    fhir, connection, _, admission, _ = await coupled_fixture()
    primary = OSError("synthetic identity callback failure")

    async def fail(*args):
        raise primary

    fhir._admission_database_guard = fail
    with pytest.raises(OSError) as caught:
        async with maintenance.control_maintenance_fence(fhir, connection):
            pytest.fail("preparation entered after identity callback failure")
    held = admission.wal_tracker.owned_control_maintenance_custody
    group = held["identity_check_group"]
    assert caught.value is primary and group["failure"] is primary
    assert group["original_outcome"].commit_state == "rolled_back" and not group["consumed"]
    assert held["original_outcome"].commit_state == "rolled_back" and not held["consumed"]
    assert held["ordinary_body_physical_wal_upper_bytes"] is None
    assert fhir.callback_state.checkedout == connection.state.checkedout == 0
    assert not held["accounting_authority"] and not held["reservation_refund"]


@pytest.mark.parametrize(
    "field,new_value",
    [
        ("postgres_server_version_num", 180003),
        ("postgres_server_version_num", 180005),
        ("postgres_server_version_num", 180007),
        ("postgres_server_version_num", 190000),
        ("postgres_block_size_bytes", 4096),
        ("postgres_wal_block_size_bytes", 4096),
    ],
)
async def test_unpinned_geometry_preserves_commit_without_consumption(field, new_value, monkeypatch):
    fhir, connection, _, admission, _ = await coupled_fixture()
    bound = maintenance._ordinary_body_physical_wal_upper

    def changed_geometry(measurement, geometry):
        return bound(measurement, replace(geometry, **{field: new_value}))

    monkeypatch.setattr(maintenance, "_ordinary_body_physical_wal_upper", changed_geometry)
    try:
        with pytest.raises(RuntimeError, match="ordinary_wal_invalid"):
            async with maintenance.control_maintenance_fence(fhir, connection):
                await maintenance.release_before_cutover(fhir)
        held = admission.wal_tracker.owned_control_maintenance_custody
        assert held["original_outcome"].is_committed and not held["consumed"]
        assert held["ordinary_body_physical_wal_upper_bytes"] is None
        assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)


async def test_combined_signed_overflow_preserves_both_original_owners(monkeypatch):
    fhir, connection, _, admission, _ = await coupled_fixture()
    consume = maintenance._consume_owned_maintenance_custody

    def oversized_pair(lease, outcome):
        callback = lease.owned_custody["identity_check_group"]["original_outcome"]
        for observed in (outcome, callback):
            observed.measurement = replace(observed.measurement, wal_bytes_delta=Decimal(2**62), wal_records_delta=1)
        consume(lease, outcome)

    monkeypatch.setattr(maintenance, "_consume_owned_maintenance_custody", oversized_pair)
    try:
        with pytest.raises(RuntimeError, match="ordinary_wal_overflow"):
            async with maintenance.control_maintenance_fence(fhir, connection):
                await maintenance.release_before_cutover(fhir)
        held = admission.wal_tracker.owned_control_maintenance_custody
        assert held["original_outcome"].is_committed and held["identity_check_group"]["original_outcome"].is_committed
        assert held["ordinary_body_physical_wal_upper_bytes"] is None and not held["consumed"]
        assert not held["accounting_authority"] and not held["reservation_refund"]
    finally:
        await close_healthy(connection)
