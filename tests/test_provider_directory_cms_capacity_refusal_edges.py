# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refuse foreign prepared layouts and unsafe capacity reservations."""

import contextvars
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_profile_capacity_control_operations as operations
from process import provider_directory_profile_capacity_control_projection as projection
from tests.test_provider_directory_cms_preparation import _manifest_boundary, _manifest_fixture


@pytest.mark.parametrize("mode", ["initial", "delta"])
@pytest.mark.parametrize("corruption", ["selection", "native-target", "columns", "primary-key"])
async def test_prepared_layout_refusals_close_transaction_before_publication(monkeypatch, capsys, mode, corruption):
    fixture = _manifest_fixture(monkeypatch, mode)
    if corruption == "selection":
        build = (
            fixture.profile_bundle.profile_delta
            if mode == "delta"
            else fixture.profile_bundle.stages[0].profile_initial_build
        )
        build.selection_proof_id = "other-proof"
        reason = "prepared_manifest_profile_changed"
    elif corruption == "native-target":
        target, name, oid = fixture.address.stage_oids[0]
        fixture.address.stage_oids = ((target + "_foreign", name, oid), *fixture.address.stage_oids[1:])
        reason = "prepared_manifest_native_changed"
    else:
        oid = fixture.heaps["profile_stage"]["oid"]
        if corruption == "columns":
            fixture.attributes[:] = [row for row in fixture.attributes if row["relation_oid"] != oid]
        else:
            primary_index = next(row for row in fixture.indexes if row["relation_oid"] == oid and row["indisprimary"])
            primary_index["indisprimary"] = False
        reason = "prepared_manifest_indexes_incomplete"
    with pytest.raises(RuntimeError, match=reason):
        async with _manifest_boundary(fixture):
            pytest.fail("invalid layout reached publication")
    assert capsys.readouterr().out == ""
    assert fixture.fhir.db._transaction_binding() is None
    assert fixture.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None


@pytest.mark.parametrize("relations", [[], [{"oid": 7}, {"oid": 7}], [{"oid": n} for n in range(65)]])
async def test_manifest_inventory_refused_before_catalog_reads(relations):
    fhir = SimpleNamespace(_artifact_scope_relation_identities=AsyncMock())
    with pytest.raises(RuntimeError, match="prepared_manifest_inventory_invalid"):
        await preparation._prepared_manifest_catalog(fhir, relations)
    fhir._artifact_scope_relation_identities.assert_not_awaited()


@pytest.mark.parametrize("corruption", ["missing", "changed-oid", "claimed-role"])
def test_owned_manifest_relation_cannot_be_replaced_or_claimed_twice(corruption):
    relation_by_field = {"oid": 7}
    relations_by_identity = {("test_schema", "owned_stage"): relation_by_field}
    if corruption == "missing":
        relations_by_identity.clear()
    elif corruption == "changed-oid":
        relation_by_field["oid"] = 8
    else:
        relation_by_field["role"] = "profile"
    original_by_field = dict(relation_by_field)
    with pytest.raises(RuntimeError, match="prepared_manifest_ownership_changed"):
        preparation._manifest_relation(
            relations_by_identity, "test_schema", "owned_stage", target="profile", role="profile", oid=7
        )
    assert relation_by_field == original_by_field


async def test_manifest_cannot_observe_inside_an_existing_transaction(monkeypatch, capsys):
    fixture = _manifest_fixture(monkeypatch, "initial")
    fixture.fhir.db.transaction_active = True
    with pytest.raises(RuntimeError, match="prepared_manifest_requires_no_transaction"):
        await preparation._emit_prepared_manifest(fixture.prepared, "run-a", "run-a")
    assert capsys.readouterr().out == ""
    assert fixture.settings_by_name == fixture.initial_settings


@pytest.mark.parametrize("operation", ["metadata", "guards"])
async def test_checkpoint_admission_refused_before_stage_mutation(operation):
    fhir = SimpleNamespace(
        _checked_serialized_metadata_payload_bytes=Mock(side_effect=RuntimeError("metadata too large")),
        _reserve_provider_directory_profile_wal_budget=AsyncMock(),
        _assert_provider_directory_profile_capacity_build=Mock(side_effect=RuntimeError("foreign build")),
        _assert_provider_directory_profile_capacity_consumption=AsyncMock(),
        _apply_provider_directory_profile_capacity_settings=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="metadata too large|foreign build"):
        if operation == "metadata":
            await operations._reserve_checkpoint_claim(fhir, object(), {"selection": "test"})
        else:
            await operations._checkpoint_claim_guards(fhir, object(), object(), object(), object())
    fhir._reserve_provider_directory_profile_wal_budget.assert_not_awaited()
    fhir._assert_provider_directory_profile_capacity_consumption.assert_not_awaited()
    fhir._apply_provider_directory_profile_capacity_settings.assert_not_awaited()


async def test_checkpoint_reserves_metadata_before_revalidating_and_applying_limits():
    events = []
    fhir = SimpleNamespace(
        _checked_serialized_metadata_payload_bytes=Mock(side_effect=lambda *args, **kwargs: events.append("metadata")),
        _reserve_provider_directory_profile_wal_budget=AsyncMock(
            side_effect=lambda *args, **kwargs: events.append("reserve")
        ),
        _assert_provider_directory_profile_capacity_build=Mock(side_effect=lambda *args: events.append("build")),
        _assert_provider_directory_profile_capacity_consumption=AsyncMock(
            side_effect=lambda *args: events.append("consumption")
        ),
        _apply_provider_directory_profile_capacity_settings=AsyncMock(
            side_effect=lambda *args: events.append("limits")
        ),
    )
    admission, build, evidence, profile = object(), object(), object(), object()
    await operations._reserve_checkpoint_claim(fhir, admission, {"selection": "test"})
    await operations._checkpoint_claim_guards(fhir, admission, build, evidence, profile)
    assert events == ["metadata", "reserve", "build", "consumption", "limits"]
    fhir._reserve_provider_directory_profile_wal_budget.assert_awaited_once_with(
        admission,
        control_operation_counts={"profile_stage_reinitialize": 1, "profile_stage_initialize": 1},
    )


async def test_ordinary_checkpoint_never_enters_capacity_admission():
    await operations._reserve_checkpoint_claim(SimpleNamespace(), None, {})
    await operations._checkpoint_claim_guards(SimpleNamespace(), None, object(), object(), object())


def test_pending_control_wal_cannot_exceed_the_signed_relation_window():
    tracker = SimpleNamespace(pending_control_wal_bytes={"owner": 3})
    admission = SimpleNamespace(geometry=SimpleNamespace(bounded_admission=True), wal_tracker=tracker)
    fhir = SimpleNamespace(
        _provider_directory_profile_capacity_relation_cap=Mock(return_value=SimpleNamespace(max_wal_bytes=10))
    )
    with pytest.raises(RuntimeError, match="capacity_window_wal_projected"):
        projection._pending_reservation_wal(fhir, admission, ("owner", "profile"), {"profile": 6}, 2)
    assert tracker.pending_control_wal_bytes == {"owner": 3}


@pytest.mark.parametrize("growth", [True, -1, 1.5, "1"])
async def test_growth_reservation_rejects_invalid_counts_before_observation(growth):
    tracker = SimpleNamespace(completed_relation_classes=set())
    fhir = SimpleNamespace(_provider_directory_profile_capacity_relation_bytes=AsyncMock())
    with pytest.raises(RuntimeError, match="growth_reservation_invalid"):
        await projection.reserve_growth(fhir, SimpleNamespace(wal_tracker=tracker), "profile", growth)
    fhir._provider_directory_profile_capacity_relation_bytes.assert_not_awaited()


@pytest.mark.parametrize(
    "window,unresolved", [(None, False), ((object(), "other"), False), ((object(), "profile"), True)]
)
async def test_growth_reservation_requires_the_matching_resolved_window(window, unresolved):
    tracker = SimpleNamespace(unresolved_window=unresolved, completed_relation_classes=set())
    fhir = SimpleNamespace(
        _PROFILE_CAPACITY_MUTATION_WINDOW=contextvars.ContextVar("test_window", default=window),
        _provider_directory_profile_capacity_relation_bytes=AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="relation_window_required"):
        await projection.reserve_growth(fhir, SimpleNamespace(wal_tracker=tracker), "profile", 0)
    fhir._provider_directory_profile_capacity_relation_bytes.assert_not_awaited()
