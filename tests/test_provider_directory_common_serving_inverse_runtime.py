# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current public dispatch and commit-settlement ordering; no native authority is mocked green."""

import asyncio
import importlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_common_serving_recovery as common
from process import provider_directory_profile_capacity_preflight_contract as requests
from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from process.provider_directory_common_serving_inverse import InitialRestore
from tests.provider_directory_serving_inverse_capacity_support import execution, limits, signing_guard


@pytest.mark.asyncio
async def test_current_public_preflight_parsers_and_fhir_dispatch_keep_exact_inverse_dto(monkeypatch):
    raw = signing_guard()["healthcare_request"]
    request = requests.validated_capacity_preflight_request(raw)
    projection = requests.validated_capacity_authority_projection_request(
        dict(contract_id=contract.PROJECTION_REQUEST, recovery_execution=execution(), limits=limits())
    )
    assert type(request) is type(projection) is contract.InverseCapacityPreflightRequest
    fhir = importlib.import_module("process.provider_directory_fhir")
    issued_by_field = {"inverse": "receipt"}
    observed_by_field = {"inverse": "projection"}
    issue = AsyncMock(return_value=issued_by_field)
    observe = AsyncMock(return_value=observed_by_field)
    monkeypatch.setattr(preflight, "inverse_capacity_preflight", issue)
    monkeypatch.setattr(preflight, "inverse_capacity_authority_projection", observe)
    assert await fhir.provider_directory_profile_capacity_preflight(request) is issued_by_field
    assert await fhir.provider_directory_profile_capacity_authority_projection(projection) is observed_by_field
    issue.assert_awaited_once_with(fhir, request)
    observe.assert_awaited_once_with(fhir, projection)
    raw["limits"]["max_cutover_seconds"] = 3
    with pytest.raises(requests.ProviderDirectoryProfileCapacityPreflightError):
        requests.validated_capacity_preflight_request(raw)


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["preflight", "projection"])
async def test_native_drift_and_prepare_timeout_use_existing_stale_api_error(monkeypatch, surface):
    class Stale(RuntimeError):
        pass

    fhir = SimpleNamespace(ProviderDirectoryArtifactBuildStale=Stale)
    private = "_inverse_capacity_" + ("preflight" if surface == "preflight" else "authority_projection")
    public = getattr(preflight, private[1:])
    monkeypatch.setattr(preflight, private, AsyncMock(side_effect=ValueError("native changed")))
    with pytest.raises(Stale, match="changed"):
        await public(fhir, object())
    monkeypatch.setattr(preflight, private, AsyncMock(side_effect=TimeoutError()))
    with pytest.raises(Stale, match="changed"):
        await public(fhir, object())


@pytest.mark.asyncio
async def test_uncertain_commit_requires_terminal_readback_not_preconsumption(monkeypatch):
    failure = RuntimeError("acknowledgment lost")
    monkeypatch.setattr(common, "_recover_once", AsyncMock(side_effect=failure))
    read = AsyncMock(return_value={"serving_recovery": {"outcome": "recovered"}})
    monkeypatch.setattr(common, "_readback", read)
    result = await common._recover(object(), {}, "run")
    assert result["serving_recovery"]["outcome"] == "recovered" and read.await_count == 1
    read.return_value = None
    with pytest.raises(RuntimeError) as caught:
        await common._recover(object(), {}, "run")
    assert caught.value is failure


@pytest.mark.asyncio
async def test_cancellation_drains_readback_and_preserves_original_cancellation(monkeypatch):
    failure = asyncio.CancelledError("cancelled during commit")
    monkeypatch.setattr(common, "_recover_once", AsyncMock(side_effect=failure))
    drained_entries = []

    async def read(*_args):
        await asyncio.sleep(0)
        drained_entries.append(True)
        return {"serving_recovery": {"outcome": "recovered"}}

    monkeypatch.setattr(common, "_readback", read)
    with pytest.raises(asyncio.CancelledError) as caught:
        await common._recover(object(), {}, "run")
    assert caught.value is failure and drained_entries == [True]


@pytest.mark.asyncio
async def test_affected_restore_counts_are_exact_and_each_family_is_measured(monkeypatch):
    events = []

    async def boundary():
        events.append("measure")

    async def status(sql, **_params):
        events.append(sql)
        return "DELETE 2" if sql == "delete" else "INSERT 0 1"

    fhir = SimpleNamespace(db=SimpleNamespace(status=status), _coerce_rowcount=lambda value: int(value.split()[-1]))
    admission = SimpleNamespace(
        boundary=boundary, geometry={"scope": {"families": [dict(family="unified", live_rows=2, insert_rows=1)]}}
    )
    observed = SimpleNamespace(
        row_restores=[SimpleNamespace(family="unified", delete_sql="delete", insert_sql="insert", parameters=())]
    )
    await common._restore_rows(fhir, admission, observed)
    assert events == ["measure", "delete", "insert", "measure"]
    admission.geometry["scope"]["families"][0]["live_rows"] = 3
    with pytest.raises(ValueError, match="restore_rows_changed"):
        await common._restore_rows(fhir, admission, observed)


def _application_case(monkeypatch, initial_restores=()):
    document_by_field = dict(
        source={"source_id": "source_one"},
        before={"initial_targets": None},
        after={"roots": {}},
        preparation={"retention": {}},
    )
    fhir = SimpleNamespace(db=SimpleNamespace(status=AsyncMock()), _schema=lambda: "serving")
    admission = SimpleNamespace(
        captured=SimpleNamespace(document=document_by_field),
        execution=dict(generation="generation", source_ids=["source_one"], published_event_sha256="a" * 64),
        inverse={"run_id": "inverse_run"},
        boundary=AsyncMock(),
        refresh=AsyncMock(),
        begin_cutover=AsyncMock(),
    )
    observed = SimpleNamespace(
        document=deepcopy(document_by_field),
        generation="generation",
        event_sha256="a" * 64,
        initial_restores=initial_restores,
    )
    steps = SimpleNamespace(
        freeze=AsyncMock(return_value={"all_live_roots", "all_retained_copies", "all_scopes"}),
        lock=AsyncMock(),
        observe=AsyncMock(return_value=observed),
        recheck=AsyncMock(),
        rows=AsyncMock(),
        initial=AsyncMock(),
        metadata=AsyncMock(),
        capture=AsyncMock(return_value={"restored": True}),
        append=AsyncMock(),
    )
    for name in ("rows", "initial", "metadata"):
        monkeypatch.setattr(common, "_restore_" + name, getattr(steps, name))
    monkeypatch.setattr(common, "_lock_inputs", steps.freeze)
    monkeypatch.setattr(common.receipts, "lock", steps.lock)
    monkeypatch.setattr(common.observation, "observe_locked_inverse", steps.observe)
    monkeypatch.setattr(common.observation, "recheck_locked_inverse", steps.recheck)
    monkeypatch.setattr(common.journal, "capture_state", steps.capture)
    monkeypatch.setattr(common.recovery, "_append_document", steps.append)
    return fhir, admission, observed, steps


@pytest.mark.asyncio
async def test_inverse_cut_locks_only_renamed_heaps(monkeypatch):
    plans = tuple(
        InitialRestore(family, common.journal.documents.FAMILIES[family], oid, "pdsrv_r_" + format(oid, "x"), ())
        for family, oid in (("profile", 31), ("evidence", 47))
    )
    fhir, admission, observed, steps = _application_case(monkeypatch, plans)
    wall = object()
    actions = []
    for name, action in (("cut", admission.begin_cutover), ("lock", steps.lock), ("rows", steps.rows)):
        action.side_effect = lambda *_args, step=name, **_kwargs: actions.append(step)
    await common._apply(fhir, admission, wall)
    assert actions == ["cut", "lock", "rows"]
    steps.freeze.assert_awaited_once_with(fhir, admission.captured.document)
    steps.lock.assert_awaited_once_with(
        fhir.db,
        "serving",
        {"provider_directory_profile", "provider_directory_profile_evidence", "pdsrv_r_1f", "pdsrv_r_2f"},
        wait=True,
    )
    admission.refresh.assert_awaited_once_with("cutover")
    admission.begin_cutover.assert_awaited_once_with(wall)
    steps.recheck.assert_awaited_once_with(observed)
    for action in (steps.rows, steps.initial, steps.metadata):
        action.assert_awaited_once_with(fhir, admission, observed)
    steps.append.assert_awaited_once()
    fhir.db.status.assert_awaited_once_with("SET CONSTRAINTS ALL IMMEDIATE")


@pytest.mark.asyncio
async def test_delta_inverse_preserves_reader_lock_mode(monkeypatch):
    fhir, admission, observed, steps = _application_case(monkeypatch)
    actions = []
    for name, action in (
        ("freeze", steps.freeze),
        ("observe", steps.observe),
        ("refresh", admission.refresh),
        ("cut", admission.begin_cutover),
        ("recheck", steps.recheck),
        ("rows", steps.rows),
        ("initial", steps.initial),
        ("metadata", steps.metadata),
        ("append", steps.append),
    ):
        action.side_effect = lambda *_args, step=name, stub=action, **_kwargs: (
            actions.append(step),
            stub.return_value,
        )[1]
    await common._apply(fhir, admission, object())
    assert actions == ["freeze", "observe", "refresh", "cut", "recheck", "rows", "initial", "metadata", "append"]
    steps.lock.assert_not_awaited()
    steps.recheck.assert_awaited_once_with(observed)
    assert admission.boundary.await_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["event", "recheck", "cut"])
async def test_inverse_lock_change_preserves_refusals(monkeypatch, failure):
    fhir, admission, observed, steps = _application_case(monkeypatch)
    if failure == "event":
        observed.event_sha256 = "b" * 64
    elif failure == "recheck":
        steps.recheck.side_effect = ValueError("native_or_owner_changed")
    else:
        admission.begin_cutover.side_effect = TimeoutError("signed_cutover_expired")
    with pytest.raises((ValueError, TimeoutError)):
        await common._apply(fhir, admission, object())
    for action in (steps.rows, steps.initial, steps.metadata, steps.append):
        action.assert_not_awaited()
    steps.lock.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_lock_timeout_prevents_restore_writes(monkeypatch):
    plan = InitialRestore("profile", "provider_directory_profile", 31, "pdsrv_r_1f", ())
    fhir, admission, _observed, steps = _application_case(monkeypatch, (plan,))
    failure = TimeoutError("signed_lock_deadline")
    steps.lock.side_effect = failure
    with pytest.raises(TimeoutError) as caught:
        await common._apply(fhir, admission, object())
    assert caught.value is failure
    admission.begin_cutover.assert_awaited_once()
    for action in (steps.recheck, steps.rows, steps.initial, steps.metadata, steps.append):
        action.assert_not_awaited()
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("options,suffix", [({}, " NOWAIT"), ({"mode": "SHARE"}, " NOWAIT"), ({"wait": True}, "")])
async def test_receipt_lock_wait_is_explicit(monkeypatch, options, suffix):
    driver = SimpleNamespace(execute=AsyncMock())
    database = object()
    owner = AsyncMock(return_value=(object(), driver, {"owner": "original"}))
    monkeypatch.setattr(common.receipts, "ordinary", owner)
    await common.receipts.lock(database, "serving", ("z", "a", "z"), **options)
    mode = options.get("mode", "ACCESS EXCLUSIVE")
    owner.assert_awaited_once_with(database)
    driver.execute.assert_awaited_once_with(f'LOCK TABLE "serving"."a","serving"."z" IN {mode} MODE{suffix}')


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"wait": 1}, {"wait": None}, {"mode": "SHARE", "wait": True}])
async def test_receipt_lock_rejects_invalid_wait(monkeypatch, options):
    owner = AsyncMock()
    monkeypatch.setattr(common.receipts, "ordinary", owner)
    with pytest.raises(ValueError, match="lock_wait_invalid"):
        await common.receipts.lock(object(), "serving", ("a",), **options)
    owner.assert_not_awaited()
