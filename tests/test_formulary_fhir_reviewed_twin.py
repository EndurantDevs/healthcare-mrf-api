# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts for fixed library-only formulary twin verification."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import process.formulary_fhir.reviewed_source as reviewed_module
import process.formulary_fhir.reviewed_twin as twin_module
from process.formulary_fhir.manual_lock import ManualSourceLockError
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from process.formulary_fhir.synchronizer import SynchronizationResult


CUTOFF = dt.datetime(2026, 8, 8, tzinfo=dt.UTC)
BASELINE_ID = "ffd_" + "1" * 48
CANDIDATE_ID = "ffd_" + "2" * 48


def _result(dataset_id: str) -> SynchronizationResult:
    return SynchronizationResult(
        dataset_id=dataset_id,
        acquisition_contract_hash="a" * 64,
        list_count=1,
        alias_count=2,
        medication_membership_count=3,
        coverage_hash="b" * 64,
        membership_hash="c" * 64,
        full_aliases=2,
        reused_aliases=0,
        resumed_aliases=0,
        request_count=9,
        transient_retry_count=0,
        throttle_count=0,
    )


def _dataset(
    dataset_id: str,
    run_id: str,
    *,
    intent: str,
) -> DatasetRef:
    manifest = twin_module.reviewed_source_manifest()
    return DatasetRef(
        manifest.source_id,
        dataset_id,
        run_id,
        None,
        CUTOFF,
        "a" * 64,
        intent,
        "verified",
    )


@asynccontextmanager
async def _transaction():
    yield


class _StateDatabase:
    def __init__(self, *, requested: bool, pointer: str | None = None) -> None:
        manifest = twin_module.reviewed_source_manifest()
        self.source_rows = [reviewed_module._source_values(manifest)]
        self.requested = requested
        self.pointer = pointer
        self.statements: list[str] = []

    transaction = staticmethod(_transaction)

    async def status(self, statement: str, **_params: object):
        self.statements.append(statement)
        return None

    async def all(self, statement: str, **_params: object):
        self.statements.append(statement)
        return self.source_rows

    async def first(self, statement: str, **_params: object):
        self.statements.append(statement)
        if "fhir_formulary_current" in statement:
            return {"dataset_id": self.pointer} if self.pointer else None
        return {
            "status": "verified",
            "publish_requested": self.requested,
            "seed_eligible": False,
        }


def test_twin_request_requires_distinct_exact_run_ids():
    baseline, candidate, cutoff_at = twin_module._twin_request(
        "baseline-run",
        "candidate-run",
        CUTOFF,
    )
    assert (baseline, candidate, cutoff_at) == (
        "baseline-run",
        "candidate-run",
        CUTOFF,
    )

    with pytest.raises(ReviewedSourceError) as caught:
        twin_module._twin_request("same-run", "same-run", CUTOFF)

    assert caught.value.code == "invalid_request"


@pytest.mark.asyncio
@pytest.mark.parametrize("publish_requested", [False, True])
async def test_synchronize_candidate_uses_fresh_client_and_fixed_intent(
    monkeypatch,
    publish_requested,
):
    events: list[str] = []
    binding = SimpleNamespace(config=object())
    repository = object()

    async def unchanged(*_args, **_kwargs):
        events.append("source")

    @asynccontextmanager
    async def client_manager():
        events.append("client-enter")
        yield "client"
        events.append("client-exit")

    def client_factory(config):
        assert config is binding.config
        return client_manager()

    async def run_sync(**kwargs):
        events.append("sync")
        assert kwargs["client"] == "client"
        assert kwargs["repository"] is repository
        assert kwargs["intent"] == (
            "requested" if publish_requested else "none"
        )
        return _result(CANDIDATE_ID)

    monkeypatch.setattr(twin_module, "require_source_unchanged", unchanged)
    monkeypatch.setattr(twin_module, "_run_verified_sync", run_sync)

    synchronization = await twin_module._synchronize_candidate(
        object(),
        binding,
        repository,
        client_factory,
        "candidate-run",
        CUTOFF,
        publish_requested=publish_requested,
    )

    assert synchronization.dataset_id == CANDIDATE_ID
    assert events == ["source", "client-enter", "sync", "client-exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("publish_requested", [False, True])
async def test_candidate_state_accepts_only_exact_flags(publish_requested):
    manifest = twin_module.reviewed_source_manifest()
    database = _StateDatabase(requested=publish_requested)

    await twin_module._require_candidate_state(
        database,
        manifest,
        _result(CANDIDATE_ID),
        None,
        publish_requested=publish_requested,
    )

    assert any(statement.startswith("LOCK TABLE") for statement in database.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_name", ["catalog", "dataset", "pointer"])
async def test_candidate_state_rejects_catalog_dataset_and_pointer_drift(
    failure_name,
):
    manifest = twin_module.reviewed_source_manifest()
    database = _StateDatabase(requested=True)
    expected_code = "source"
    if failure_name == "catalog":
        database.source_rows.clear()
        expected_code = "catalog"
    elif failure_name == "dataset":
        database.requested = False
    else:
        database.pointer = "ffd_" + "3" * 48

    with pytest.raises(ReviewedSourceError) as caught:
        await twin_module._require_candidate_state(
            database,
            manifest,
            _result(CANDIDATE_ID),
            None,
            publish_requested=True,
        )

    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_verified_dataset_ref_binds_result_and_intent():
    exact_dataset = _dataset(
        CANDIDATE_ID,
        "candidate-run",
        intent="requested",
    )
    repository = SimpleNamespace(begin_dataset=AsyncMock(return_value=exact_dataset))

    stored_dataset = await twin_module._verified_dataset_ref(
        repository,
        _result(CANDIDATE_ID),
        "candidate-run",
        CUTOFF,
        publish_requested=True,
    )

    assert stored_dataset == exact_dataset
    assert repository.begin_dataset.await_args.kwargs["intent"] == "requested"


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["dataset", "status"])
async def test_verified_dataset_ref_rejects_mismatch(mismatch):
    dataset_id = BASELINE_ID if mismatch == "dataset" else CANDIDATE_ID
    status = "building" if mismatch == "status" else "verified"
    stored_dataset = _dataset(
        dataset_id,
        "candidate-run",
        intent="requested",
    )
    if status != "verified":
        stored_dataset = DatasetRef(
            stored_dataset.source_id,
            stored_dataset.dataset_id,
            stored_dataset.run_id,
            stored_dataset.previous_dataset_id,
            stored_dataset.cutoff_at,
            stored_dataset.acquisition_contract_hash,
            stored_dataset.intent,
            status,
        )
    repository = SimpleNamespace(
        begin_dataset=AsyncMock(return_value=stored_dataset)
    )

    with pytest.raises(ReviewedSourceError) as caught:
        await twin_module._verified_dataset_ref(
            repository,
            _result(CANDIDATE_ID),
            "candidate-run",
            CUTOFF,
            publish_requested=True,
        )

    assert caught.value.code == "source"


@pytest.mark.asyncio
async def test_verified_candidate_runs_state_check_before_dataset_reload(
    monkeypatch,
):
    binding = object()
    repository = object()
    synchronization = _result(CANDIDATE_ID)
    candidate = _dataset(CANDIDATE_ID, "candidate-run", intent="requested")
    events: list[str] = []
    context = twin_module._TwinContext(
        database="database",
        binding=binding,
        repository=repository,
        client_factory="factory",
        manifest=twin_module.reviewed_source_manifest(),
        previous_pointer=None,
    )

    async def synchronize(*_args, **_kwargs):
        events.append("synchronize")
        return synchronization

    async def require_state(*_args, **_kwargs):
        events.append("state")

    async def dataset_ref(*_args, **_kwargs):
        events.append("dataset")
        return candidate

    monkeypatch.setattr(twin_module, "_synchronize_candidate", synchronize)
    monkeypatch.setattr(twin_module, "_require_candidate_state", require_state)
    monkeypatch.setattr(twin_module, "_verified_dataset_ref", dataset_ref)

    observed = await twin_module._verified_candidate(
        context,
        "candidate-run",
        CUTOFF,
        publish_requested=True,
    )

    assert observed == (synchronization, candidate)
    assert events == ["synchronize", "state", "dataset"]


@pytest.mark.asyncio
async def test_verify_twins_orders_roles_admission_and_postflight(monkeypatch):
    manifest = twin_module.reviewed_source_manifest()
    binding = object()
    admission = object()
    baseline = _dataset(BASELINE_ID, "baseline-run", intent="none")
    candidate = _dataset(CANDIDATE_ID, "candidate-run", intent="requested")
    events: list[object] = []

    monkeypatch.setattr(twin_module, "_register_manifest", AsyncMock(return_value=binding))
    monkeypatch.setattr(twin_module, "_current_pointer", AsyncMock(return_value=None))
    monkeypatch.setattr(twin_module, "FHIRFormularyRepository", Mock(return_value="repository"))

    async def verified(context, run_id, _cutoff, *, publish_requested):
        events.append((run_id, publish_requested, context.repository))
        if publish_requested:
            return _result(CANDIDATE_ID), candidate
        return _result(BASELINE_ID), baseline

    async def admit(**kwargs):
        events.append("admit")
        assert kwargs["binding"] is binding
        assert kwargs["baseline"] == baseline
        assert kwargs["candidate"] == candidate
        return admission

    async def postflight(*_args, **kwargs):
        events.append(("postflight", kwargs["publish_requested"]))

    monkeypatch.setattr(twin_module, "_verified_candidate", verified)
    monkeypatch.setattr(twin_module, "admit_verified_twins", admit)
    monkeypatch.setattr(twin_module, "_require_candidate_state", postflight)

    observed = await twin_module._verify_twins(
        object(),
        object(),
        "baseline-run",
        "candidate-run",
        CUTOFF,
    )

    assert observed is admission
    assert events == [
        ("baseline-run", False, "repository"),
        ("candidate-run", True, "repository"),
        "admit",
        ("postflight", True),
    ]
    assert manifest.source_id


@pytest.mark.asyncio
async def test_public_twin_verifier_holds_one_source_lease(monkeypatch):
    admission = object()
    lease_events: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        lease_events.append("enter")
        yield
        lease_events.append("exit")

    verify_twins = AsyncMock(return_value=admission)
    monkeypatch.setattr(twin_module.manual_lock, "manual_source_lease", source_lease)
    monkeypatch.setattr(twin_module, "_verify_twins", verify_twins)

    observed = await twin_module.verify_reviewed_source_twins(
        baseline_run_id="baseline-run",
        candidate_run_id="candidate-run",
        cutoff=CUTOFF,
        database="database",
        client_factory="client-factory",
    )

    assert observed is admission
    assert lease_events == ["enter", "exit"]
    assert verify_twins.await_args.args[0:2] == ("database", "client-factory")


@pytest.mark.asyncio
async def test_public_twin_verifier_sanitizes_lock_failure(monkeypatch):
    @asynccontextmanager
    async def unavailable_lease(*_args, **_kwargs):
        raise ManualSourceLockError("busy")
        yield

    monkeypatch.setattr(
        twin_module.manual_lock,
        "manual_source_lease",
        unavailable_lease,
    )

    with pytest.raises(ReviewedSourceError) as caught:
        await twin_module.verify_reviewed_source_twins(
            baseline_run_id="baseline-run",
            candidate_run_id="candidate-run",
            cutoff=CUTOFF,
            database=object(),
        )

    assert caught.value.code == "busy"
