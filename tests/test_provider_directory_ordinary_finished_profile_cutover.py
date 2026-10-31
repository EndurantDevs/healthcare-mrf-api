# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the finished Profile pair only inside its original ordinary common transaction."""

import asyncio
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_ordinary_address_admission as ordinary
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_serving_recovery_receipts as receipts
from tests.test_provider_directory_ordinary_terminal_fence import ordinary_case as ordinary_case


def _profile_pair(case):
    directory, producer = case.directory, case.producer
    profile = SimpleNamespace(
        run_id=producer.run_id, lease=producer.profile_lease, wal_tracker=SimpleNamespace(unresolved_window=False)
    )
    directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION = ContextVar("finished_profile", default=profile)
    directory._PROFILE_CAPACITY_MUTATION_WINDOW = ContextVar("finished_window", default=None)
    directory._assert_provider_directory_profile_wal_budget = AsyncMock()
    directory._schema = lambda: "fixture"
    directory._unscoped_qt = lambda schema, table: schema + "." + table
    directory._pagination_checkpoint_row_mapping = lambda row: row
    directory.ProviderDirectoryProfileCapacityLeaseConsumption = SimpleNamespace(__tablename__="consumption")
    directory.db.first = AsyncMock(
        return_value={
            "attestation_id": producer.profile_lease.attestation_id,
            "lease_digest": producer.profile_lease.lease_digest,
            "selection_proof_id": producer.plan.selection_proof_id,
        }
    )
    directory.db.scalar = AsyncMock(return_value=100)
    producer.resumed_profile = profile
    producer._cutover_binding = case.binding
    producer.cutover_wal_start_bytes = producer.preparation_wal_end_bytes = 0
    return profile


@pytest.fixture
def cut(ordinary_case, monkeypatch):
    case = ordinary_case
    profile = _profile_pair(case)
    driver = object()
    owner = owned.OwnedWalTransaction(session=case.binding.session, retained_driver=driver, retained_pid=77)
    actor_by_field = dict(session="builder", current="builder", owner=44, backend=77, xid="91", database_oid=42)
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(case.binding.session, driver, actor_by_field)))
    tokens = (
        (
            artifacts._TERMINAL,
            artifacts._TERMINAL.set((case.admission, case.binding.session, case.capture, case.factory)),
        ),
        (owned._ACTIVE_OWNED_TRANSACTION, owned._ACTIVE_OWNED_TRANSACTION.set(None)),
        (preparation._ACTIVE, preparation._ACTIVE.set(None)),
    )
    prepared = preparation.PreparedServingArtifacts(
        case.directory, case.fence, case.producer.execution, case.admission, None, None, {}, {}, None
    )
    yield SimpleNamespace(case=case, prepared=prepared, profile=profile, owner=owner, actor=actor_by_field)
    for variable, token in reversed(tokens):
        variable.reset(token)


async def _seal_profile(cut):
    owned._ACTIVE_OWNED_TRANSACTION.set((cut.case.directory.db, asyncio.current_task(), cut.owner))
    await ordinary.finish_profile_cutover(cut.prepared)


async def _address_interval(cut):
    await _seal_profile(cut)
    cut.case.directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    preparation._ACTIVE.set(cut.case.admission)


async def test_finished_profile_pair_remains_durable_without_reactivating_its_meter(cut):
    await _address_interval(cut)
    producer = cut.case.producer
    await producer._assert_wal_budget(producer._cutover_request)
    assert cut.case.directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
    cut.case.directory._assert_provider_directory_profile_wal_budget.assert_awaited_once_with(cut.profile)
    assert cut.case.directory.db.first.await_count == 2
    assert "admission_purpose='profile'" in cut.case.directory.db.first.await_args.args[0]


@pytest.mark.parametrize(
    "fault",
    [
        "unsealed",
        "wrong_profile",
        "wrong_admission",
        "readiness",
        "cms",
        "closed_cut",
        "binding",
        "owner",
        "driver",
        "xid",
        "backend",
    ],
)
async def test_finished_interval_cannot_escape_its_original_binding(cut, fault):
    await _address_interval(cut)
    case, producer = cut.case, cut.case.producer
    request = producer._cutover_request
    match fault:
        case "unsealed":
            producer._ordinary_finished_profile = None
        case "wrong_profile":
            case.directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object())
        case "wrong_admission":
            preparation._ACTIVE.set(object())
        case "readiness":
            request = replace(request, phase="readiness")
        case "cms":
            producer.plan = object()
        case "closed_cut":
            producer._cutover_active = False
        case "binding":
            case.directory.db._transaction_binding = lambda: SimpleNamespace(session=case.binding.session)
        case "owner":
            owned._ACTIVE_OWNED_TRANSACTION.set((case.directory.db, asyncio.current_task(), replace(cut.owner)))
        case "driver":
            receipts.ordinary.return_value = (case.binding.session, object(), cut.actor)
        case _:
            cut.actor["xid" if fault == "xid" else "backend"] = "changed"
    with pytest.raises((RuntimeError, ValueError)):
        await producer._assert_wal_budget(request)


async def test_other_task_cannot_borrow_a_finished_profile_interval(cut):
    await _address_interval(cut)
    producer = cut.case.producer
    with pytest.raises((RuntimeError, ValueError)):
        await asyncio.create_task(producer._assert_wal_budget(producer._cutover_request))


@pytest.mark.parametrize("fault", ["wal", "consumption"])
async def test_finished_interval_preserves_budget_and_consumed_pair_checks(cut, fault):
    await _address_interval(cut)
    producer = cut.case.producer
    if fault == "wal":
        cut.case.directory.db.scalar.return_value = producer.plan.cutover_wal_upper_bound_bytes + 1
    else:
        cut.case.directory.db.first.return_value = None
    with pytest.raises(
        RuntimeError, match="wal_budget_exceeded" if fault == "wal" else "active_profile_consumption_changed"
    ):
        await producer._assert_wal_budget(producer._cutover_request)


@pytest.mark.parametrize("fault", ["unresolved", "repeat"])
async def test_only_a_finished_original_profile_can_be_sealed_once(cut, fault):
    if fault == "unresolved":
        cut.profile.wal_tracker.unresolved_window = True
    else:
        await _seal_profile(cut)
    with pytest.raises(ValueError, match="finished_profile_admission"):
        await _seal_profile(cut)


async def test_failed_final_profile_budget_never_opens_finished_interval(cut):
    cut.case.directory._assert_provider_directory_profile_wal_budget.side_effect = RuntimeError("profile_budget")
    with pytest.raises(RuntimeError, match="profile_budget"):
        await _seal_profile(cut)
    assert cut.case.producer._ordinary_finished_profile is None


@pytest.mark.parametrize("fails", [False, True])
async def test_existing_cutover_owner_always_forgets_finished_interval(cut, fails):
    producer = cut.case.producer
    producer._cutover_active = False
    cut.case.directory.db._transaction_binding = lambda: None
    try:
        async with producer.cutover_operation():
            producer._ordinary_finished_profile = object()
            if fails:
                raise RuntimeError("cut_failed")
    except RuntimeError as error:
        assert fails and str(error) == "cut_failed"
    assert producer._ordinary_finished_profile is None
