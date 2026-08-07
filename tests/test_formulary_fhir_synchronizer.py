# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Orchestration and dormancy tests for verify-only formulary sync."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from copy import deepcopy

import pytest

import process.formulary_fhir.synchronizer as sync_module
from process.formulary_fhir.continuation import FHIRTransportError
from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.continuation import medication_search_contract
from process.formulary_fhir.parser import parse_coverage_plan
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.repository import AliasCompletionFence
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import CompletedAliasCheckpoint
from process.formulary_fhir.repository import CoveragePlanWriteResult
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.synchronizer import synchronize_verified_dataset
from process.formulary_fhir.types import CurrentVersionCensus


FIXTURES = __import__("pathlib").Path(__file__).parent / "fixtures" / "formulary_fhir"
CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)
RUNTIME_CONFIG = {
    "timeout_seconds": 30,
    "max_attempts": 2,
    "page_size": 50,
    "max_pages": 100,
    "max_total_resources": 5_000,
    "max_response_bytes": 1_048_576,
}


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _coverage_resource(alias: str = "SYNTH-A") -> dict[str, object]:
    resource = _fixture("coverage_plan.json")
    resource["extension"] = [
        extension
        for extension in resource["extension"]
        if extension.get("valueString") == alias
        or "PlanID-extension" not in str(extension.get("url"))
    ]
    return resource


def _medication_resource(alias: str = "SYNTH-A") -> dict[str, object]:
    resource = _fixture("medication_a.json")
    for extension in resource["extension"]:
        if "PlanID-extension" in str(extension.get("url")):
            extension["valueString"] = alias
    return resource


def _source_row() -> dict[str, object]:
    return {
        "source_id": "source-alpha",
        "canonical_base": "https://synthetic.invalid/fhir",
        "enabled": True,
        "runtime_config_json": deepcopy(RUNTIME_CONFIG),
        "metadata_json": {"mode": "manual"},
    }


class _SourceDatabase:
    def __init__(
        self,
        events: list[str],
        *,
        drift_on_read: int | None = None,
    ) -> None:
        self.events = events
        self.drift_on_read = drift_on_read
        self.read_count = 0

    async def first(self, _statement: str, **_params: object):
        self.read_count += 1
        self.events.append("source-read")
        source_by_field = _source_row()
        if self.drift_on_read == self.read_count:
            source_by_field["metadata_json"] = {"mode": "changed"}
        return source_by_field


class _Client:
    def __init__(
        self,
        config,
        events: list[str],
        *,
        medication_error: BaseException | None = None,
    ) -> None:
        self.config = config
        self.events = events
        self.medication_error = medication_error
        self.request_count = 0
        self.transient_retry_count = 0
        self.throttle_count = 0
        self.medication_calls = 0

    async def __aenter__(self):
        self.events.append("client-enter")
        return self

    async def __aexit__(self, *_error) -> None:
        self.events.append("client-exit")

    async def coverage_plan_current_census(self, *, cutoff):
        self.events.append("coverage-http")
        self.request_count += 1
        resource = _coverage_resource()
        contract_hash = coverage_plan_search_contract(
            self.config,
            cutoff,
        ).contract_hash
        return CurrentVersionCensus("List", cutoff, 1, (resource,), contract_hash)

    async def medication_current_census(self, alias, *, cutoff):
        self.events.append("medication-http")
        self.medication_calls += 1
        self.request_count += 1
        if self.medication_error is not None:
            raise self.medication_error
        resource = _medication_resource(alias)
        contract_hash = medication_search_contract(
            self.config,
            alias,
            cutoff,
        ).contract_hash
        return CurrentVersionCensus(
            "MedicationKnowledge",
            cutoff,
            1,
            (resource,),
            contract_hash,
        )


class _Repository:
    def __init__(
        self,
        events: list[str],
        *,
        dataset_status: str = "building",
        checkpoint_mode: str | None = None,
        current: CurrentSnapshot | None = None,
        lifecycle_error: BaseException | None = None,
        next_fence_token: int = 1,
    ) -> None:
        self.events = events
        self.dataset_status = dataset_status
        self.checkpoint_mode = checkpoint_mode
        self.current = current or CurrentSnapshot(None, {})
        self.lifecycle_error = lifecycle_error
        self.next_fence_token = next_fence_token
        self.dataset: DatasetRef | None = None
        self.alias: AliasRef | None = None
        self.alias_count = 1
        self.medication_count = 1
        self.failed_with: BaseException | None = None
        self.interrupted_with: BaseException | None = None

    async def begin_dataset(self, **values):
        self.events.append("begin")
        assert values["intent"] == "none"
        previous_dataset_id = (
            self.current.dataset.dataset_id if self.current.dataset else None
        )
        self.dataset = DatasetRef(
            source_id="source-alpha",
            dataset_id="ffd_" + "1" * 48,
            run_id=values["run_id"],
            previous_dataset_id=previous_dataset_id,
            cutoff_at=values["cutoff_at"],
            acquisition_contract_hash=values["acquisition_contract_hash"],
            intent="none",
            status=self.dataset_status,
        )
        return self.dataset

    async def current_snapshot(self):
        self.events.append("current")
        return self.current

    async def put_coverage_plan(self, *, dataset, plan):
        self.events.append("put-plan")
        self.alias = AliasRef(
            "source-alpha",
            plan.public_id,
            "ffa_" + "2" * 48,
            plan.source_plan_identifiers[0],
        )
        return CoveragePlanWriteResult(
            dataset,
            "ffcv_" + "3" * 48,
            (self.alias,),
        )

    async def completed_alias_checkpoint(self, *, dataset, alias):
        self.events.append("checkpoint")
        if self.checkpoint_mode is None:
            return None
        return CompletedAliasCheckpoint(
            source_id="source-alpha",
            dataset_id=dataset.dataset_id,
            alias_id=alias.alias_id,
            alias_version_id="ffav_" + "4" * 48,
            expected_count=1,
            membership_hash="a" * 64,
            acquisition_mode=self.checkpoint_mode,
        )

    async def next_alias_completion_fence(self, *, dataset, alias):
        self.events.append("fence")
        assert dataset is self.dataset and alias is self.alias
        prior_mode = "full" if self.next_fence_token > 1 else None
        return AliasCompletionFence(self.next_fence_token, prior_mode)

    async def put_alias_version(self, write):
        self.events.append("put-full")
        assert write.fence_token == self.next_fence_token
        variants_by_id = {
            medication.upstream_medication_id: medication_variant_hash(medication)
            for medication in write.medications
        }
        return AliasVersionResult(
            "source-alpha",
            write.dataset.dataset_id,
            write.alias.alias_id,
            "ffav_" + "4" * 48,
            write.expected_count,
            membership_hash(variants_by_id),
            "full",
        )

    async def link_reused_alias(self, *, dataset, alias, prior, fence_token):
        self.events.append("put-reuse")
        assert fence_token == self.next_fence_token
        return AliasVersionResult(
            "source-alpha",
            dataset.dataset_id,
            alias.alias_id,
            prior.alias_version_id,
            prior.expected_count,
            prior.membership_hash,
            "reuse",
        )

    async def verify_dataset(self, *, dataset):
        self.events.append("verify")
        return DatasetVerification(
            "source-alpha",
            dataset.dataset_id,
            1,
            self.alias_count,
            self.medication_count,
            "b" * 64,
            "c" * 64,
        )

    async def fail_dataset(self, _dataset, error):
        self.events.append("fail")
        self.failed_with = error
        if self.lifecycle_error is not None:
            raise self.lifecycle_error

    async def interrupt_dataset(self, _dataset, error):
        self.events.append("interrupt")
        self.interrupted_with = error
        if self.lifecycle_error is not None:
            raise self.lifecycle_error


def _published_snapshot(*, membership_hash_value: str) -> CurrentSnapshot:
    plan = parse_coverage_plan(
        _coverage_resource(),
        canonical_base="https://synthetic.invalid/fhir",
    )
    published_dataset = DatasetRef(
        "source-alpha",
        "ffd_" + "9" * 48,
        "published-run",
        None,
        CUTOFF - dt.timedelta(days=1),
        "d" * 64,
        "seed",
        "published",
    )
    prior = PriorAliasState(
        "source-alpha",
        plan.public_id,
        "ffa_" + "2" * 48,
        "SYNTH-A",
        "ffav_" + "8" * 48,
        1,
        published_dataset.cutoff_at,
        {},
        membership_hash_value,
    )
    return CurrentSnapshot(published_dataset, {(plan.public_id, "SYNTH-A"): prior})


async def _run(
    monkeypatch,
    repository: _Repository,
    database: _SourceDatabase,
    client: _Client | None = None,
):
    client_by_role: dict[str, _Client] = {}

    def client_factory(config):
        selected_client = client or _Client(config, repository.events)
        selected_client.config = config
        client_by_role["selected"] = selected_client
        return selected_client

    monkeypatch.setattr(
        sync_module,
        "FHIRFormularyRepository",
        lambda **_values: repository,
    )
    synchronization_result = await synchronize_verified_dataset(
        source_id="source-alpha",
        run_id="synthetic-run-1",
        cutoff=CUTOFF,
        database=database,
        client_factory=client_factory,
    )
    return synchronization_result, client_by_role["selected"]


@pytest.mark.asyncio
async def test_sync_is_serial_verify_only_and_source_rechecked(monkeypatch):
    events: list[str] = []
    repository = _Repository(events)
    synchronization_result, client = await _run(
        monkeypatch,
        repository,
        _SourceDatabase(events),
    )

    assert synchronization_result.full_aliases == 1
    assert (
        synchronization_result.reused_aliases
        == synchronization_result.resumed_aliases
        == 0
    )
    assert synchronization_result.request_count == 2
    assert client.medication_calls == 1
    assert events == [
        "source-read",
        "client-enter",
        "coverage-http",
        "source-read",
        "begin",
        "current",
        "put-plan",
        "checkpoint",
        "fence",
        "source-read",
        "medication-http",
        "put-full",
        "source-read",
        "verify",
        "client-exit",
    ]


@pytest.mark.asyncio
async def test_completed_checkpoint_skips_alias_http(monkeypatch):
    events: list[str] = []
    repository = _Repository(events, checkpoint_mode="full")
    result, client = await _run(
        monkeypatch,
        repository,
        _SourceDatabase(events),
    )

    assert result.full_aliases == result.resumed_aliases == 1
    assert client.medication_calls == 0
    assert "medication-http" not in events


@pytest.mark.asyncio
async def test_exact_published_predecessor_is_reused(monkeypatch):
    medication = parse_medication_knowledge(_medication_resource())
    prior_hash = membership_hash(
        {medication.upstream_medication_id: medication_variant_hash(medication)}
    )
    events: list[str] = []
    repository = _Repository(
        events,
        current=_published_snapshot(membership_hash_value=prior_hash),
    )
    result, _client = await _run(
        monkeypatch,
        repository,
        _SourceDatabase(events),
    )

    assert result.reused_aliases == 1
    assert "put-reuse" in events
    assert "put-full" not in events


@pytest.mark.asyncio
async def test_verified_replay_skips_predecessor_and_alias_work(monkeypatch):
    events: list[str] = []
    repository = _Repository(events, dataset_status="verified")
    result, client = await _run(
        monkeypatch,
        repository,
        _SourceDatabase(events),
    )

    assert result.resumed_aliases == 1
    assert result.full_aliases == result.reused_aliases == 0
    assert client.medication_calls == 0
    assert "current" not in events and "put-plan" not in events


@pytest.mark.asyncio
async def test_predecessor_race_and_final_source_drift_fail_candidate(monkeypatch):
    events: list[str] = []
    repository = _Repository(events)
    repository.current = _published_snapshot(membership_hash_value="a" * 64)
    original_begin = repository.begin_dataset

    async def begin_without_predecessor(**values):
        dataset = await original_begin(**values)
        repository.dataset = DatasetRef(
            dataset.source_id,
            dataset.dataset_id,
            dataset.run_id,
            None,
            dataset.cutoff_at,
            dataset.acquisition_contract_hash,
            dataset.intent,
            dataset.status,
        )
        return repository.dataset

    repository.begin_dataset = begin_without_predecessor
    with pytest.raises(RuntimeError, match="predecessor"):
        await _run(monkeypatch, repository, _SourceDatabase(events))
    assert isinstance(repository.failed_with, RuntimeError)

    drift_events: list[str] = []
    drift_repository = _Repository(drift_events)
    with pytest.raises(Exception, match="changed"):
        await _run(
            monkeypatch,
            drift_repository,
            _SourceDatabase(drift_events, drift_on_read=4),
        )
    assert drift_repository.failed_with is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "lifecycle"),
    [
        (asyncio.CancelledError(), "interrupt"),
        (TimeoutError("timeout"), "interrupt"),
        (FHIRTransportError("transient", is_transient=True), "interrupt"),
        (FHIRTransportError("terminal"), "fail"),
        (ValueError("invalid"), "fail"),
    ],
)
async def test_failure_classification_preserves_original(monkeypatch, error, lifecycle):
    events: list[str] = []
    repository = _Repository(
        events,
        lifecycle_error=RuntimeError("secondary lifecycle failure"),
    )
    database = _SourceDatabase(events)
    client = _Client(None, events, medication_error=error)

    with pytest.raises(type(error)) as caught:
        await _run(monkeypatch, repository, database, client)

    assert caught.value is error
    assert lifecycle in events
    assert (repository.interrupted_with is error) == (lifecycle == "interrupt")
