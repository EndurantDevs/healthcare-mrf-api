# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recheck funded metadata growth without replacing any signed capacity identity."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_control_projection as control
from tests import test_provider_directory_cms_capacity_ledger as ledger
from tests.test_provider_directory_cms_capacity_ledger import _resume_geometry
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input

MUTATIONS = (
    "build_checkpoint_insert",
    "build_checkpoint_update",
    "import_run_update",
    "capacity_consumption_insert",
)


def _funded_state(initial_mode=False):
    geometry, _projection = _resume_geometry(initial_mode)
    plan = _control_wal_plan_input(
        evidence_batch_count=115,
        compact_batch_count=400,
        admission_row_lock_count=ledger.fhir.PROVIDER_DIRECTORY_PROFILE_ADMISSION_ROW_LOCK_COUNT,
    )
    mutation_by_name = {
        name: replace(
            getattr(plan, name),
            payload_upper_bytes=65536,
            main_index_pages=(2,) * count,
            toast_index_pages=(2,),
        )
        for name, count in zip(MUTATIONS, (3, 3, 8, 4), strict=True)
    }
    plan = replace(plan, **mutation_by_name)
    geometry = replace(geometry, control_wal_plan_input_hash=capacity.profile_control_wal_plan_input_hash(plan))
    projection = capacity.project_profile_control_wal_capacity(geometry, plan)
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    return geometry, capacity.project_profile_control_wal_capacity(geometry, plan)


def _grown(plan):
    return replace(
        plan,
        build_checkpoint_insert=replace(plan.build_checkpoint_insert, main_index_pages=(6, 2, 2)),
        build_checkpoint_update=replace(plan.build_checkpoint_update, main_index_pages=(6, 2, 2)),
        import_run_update=replace(plan.import_run_update, main_index_pages=(4, 2, 2, 2, 2, 2, 2, 2)),
    )


@pytest.mark.parametrize("initial_mode", [False, True])
def test_funded_index_growth_preserves_original_plan_and_projection(initial_mode):
    geometry, projection = _funded_state(initial_mode)
    observed = _grown(projection.plan_input)
    original_bytes = capacity.canonical_profile_control_wal_projection_json(projection)
    assert capacity.profile_control_wal_plan_input_hash(observed) != geometry.control_wal_plan_input_hash
    control.assert_profile_control_metadata_growth(geometry, projection, observed)
    assert capacity.canonical_profile_control_wal_projection_json(projection) == original_bytes
    assert capacity.revalidate_profile_control_wal_projection(geometry, projection) is projection
    with pytest.raises(ValueError, match="control_wal_plan_input_hash_mismatch"):
        capacity.project_profile_control_wal_capacity(geometry, observed)


@pytest.mark.parametrize("name", MUTATIONS)
@pytest.mark.parametrize("field", ["main_index_pages", "toast_index_pages"])
def test_unfunded_growth_refuses_for_every_metadata_budget(name, field):
    geometry, projection = _funded_state()
    mutation = getattr(projection.plan_input, name)
    pages = getattr(mutation, field)
    observed = replace(projection.plan_input, **{name: replace(mutation, **{field: (2**40, *pages[1:])})})
    with pytest.raises(ValueError, match="control_metadata_resume_growth_unfunded"):
        control.assert_profile_control_metadata_growth(geometry, projection, observed)


@pytest.mark.parametrize(
    "field", ["payload_upper_bytes", "deleted_toast_chunks", "main_index_pages", "toast_index_pages"]
)
def test_metadata_identity_or_index_count_change_refuses(field):
    geometry, projection = _funded_state()
    mutation = projection.plan_input.build_checkpoint_update
    changed = (*getattr(mutation, field), 2) if field.endswith("pages") else getattr(mutation, field) - 1
    if field == "deleted_toast_chunks":
        changed = mutation.deleted_toast_chunks + 1
    observed = replace(projection.plan_input, build_checkpoint_update=replace(mutation, **{field: changed}))
    with pytest.raises(ValueError, match="control_metadata_resume_identity_changed"):
        control.assert_profile_control_metadata_growth(geometry, projection, observed)


@pytest.mark.parametrize(
    "field", ["evidence_batch_count", "compact_batch_count", "affected_source_count", "cutover_row_lock_count"]
)
def test_control_work_count_change_refuses(field):
    geometry, projection = _funded_state()
    observed = replace(projection.plan_input, **{field: getattr(projection.plan_input, field) + 1})
    with pytest.raises(ValueError, match="control_metadata_resume_identity_changed"):
        control.assert_profile_control_metadata_growth(geometry, projection, observed)


def test_retained_projection_is_independently_revalidated():
    geometry, projection = _funded_state()
    damaged = replace(projection, total_control_wal_bytes=projection.total_control_wal_bytes + 1)
    with pytest.raises(ValueError):
        control.assert_profile_control_metadata_growth(geometry, damaged, _grown(projection.plan_input))


@pytest.mark.parametrize("initial_mode", [False, True])
@pytest.mark.parametrize("unfunded", [False, True])
async def test_signed_resume_retains_authority_and_original_tracker(monkeypatch, initial_mode, unfunded):
    monkeypatch.setattr(ledger, "_resume_geometry", _funded_state)
    paused, execution = await ledger._admitted_resume_fixture(monkeypatch, initial_mode)
    original = paused.admission
    observed = _grown(original.control_wal_projection.plan_input)
    if unfunded:
        observed = replace(
            observed, import_run_update=replace(observed.import_run_update, main_index_pages=(2**40, *(2,) * 7))
        )
    workload = ledger.fhir._profile_admission_workload.return_value._replace(control_wal_plan_input=observed)
    ledger.fhir._profile_admission_workload.return_value = workload

    def same_geometry(actual_workload, _inputs):
        assert actual_workload.control_wal_plan_input is original.control_wal_projection.plan_input
        assert actual_workload._replace(control_wal_plan_input=observed) == workload
        return SimpleNamespace(geometry=original.geometry, control_wal_projection=original.control_wal_projection)

    monkeypatch.setattr(ledger.fhir, "_profile_admission_geometry", same_geometry)
    arguments = ledger.fhir, paused, execution, "fence", "resource-fence", frozenset({"Practitioner"})
    if unfunded:
        with pytest.raises(ValueError, match="control_metadata_resume_growth_unfunded"):
            await ledger.capacity.resume_profile_capacity(*arguments)
        ledger.fhir._profile_admission_runtime_state.assert_not_awaited()
        assert original.initial_wal_offset_bytes == 0
        return
    resumed = await ledger.capacity.resume_profile_capacity(*arguments)
    assert resumed.geometry is original.geometry and resumed.control_wal_projection is original.control_wal_projection
    assert resumed.lease is original.lease and resumed.wal_tracker is original.wal_tracker
    assert resumed.initial_wal_offset_bytes == 73 and resumed.initial_wal_lsn == "0/9000"
    ledger.fhir._assert_provider_directory_profile_wal_budget.assert_awaited_once_with(resumed)
