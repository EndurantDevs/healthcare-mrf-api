# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import json
from dataclasses import replace
from pathlib import Path

import pytest

from process.formulary_fhir.identity import public_formulary_id
from process.formulary_fhir.client import FHIRTransportError
from process.formulary_fhir.parser import parse_coverage_plan
from process.formulary_fhir.repository import (
    CompletedAliasCheckpoint,
    CurrentSnapshot,
    PriorAliasState,
)
from process.formulary_fhir.synchronizer import (
    AliasWork,
    _is_california_plan,
    _run_alias_wave,
    synchronize,
)

FIXTURES = Path(__file__).parent / "fixtures" / "formulary_fhir"
BASE = "https://fhir.example.invalid/r4"
CUTOFF = dt.datetime(2026, 8, 6, 12, tzinfo=dt.UTC)


def _fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class _Client:
    base_url = BASE
    throttle_count = 0

    def __init__(self, *, deltas=None, full=None, counts=None):
        self.coverage = [_fixture("coverage_plan.json")]
        self.deltas = deltas or {}
        self.full = full or {
            "SYNTH-NCAL-A": [_fixture("medication_a.json")],
            "SYNTH-NCAL-B": [_fixture("medication_b.json")],
        }
        self.counts = counts or {
            alias: len(resources) for alias, resources in self.full.items()
        }
        self.active_by_alias = {}
        self.maximum_active_by_alias = {}

    async def coverage_plans(self, *, cutoff):
        assert cutoff == CUTOFF
        for item in self.coverage:
            yield item

    async def coverage_plan_count(self, *, cutoff):
        assert cutoff == CUTOFF
        return len(self.coverage)

    async def alias_count(self, alias, *, cutoff):
        assert cutoff == CUTOFF
        return self.counts[alias]

    async def medications(self, alias, *, cutoff, updated_since=None):
        self.active_by_alias[alias] = self.active_by_alias.get(alias, 0) + 1
        self.maximum_active_by_alias[alias] = max(
            self.maximum_active_by_alias.get(alias, 0),
            self.active_by_alias[alias],
        )
        try:
            resources = (
                self.deltas.get(alias, []) if updated_since else self.full[alias]
            )
            for item in resources:
                yield item
        finally:
            self.active_by_alias[alias] -= 1


class _DriftingCountClient(_Client):
    def __init__(self):
        super().__init__()
        self.count_calls = {}

    async def alias_count(self, alias, *, cutoff):
        self.count_calls[alias] = self.count_calls.get(alias, 0) + 1
        baseline = await super().alias_count(alias, cutoff=cutoff)
        return baseline if self.count_calls[alias] == 1 else baseline + 1


class _DriftingListCountClient(_Client):
    def __init__(self):
        super().__init__()
        self.list_count_calls = 0

    async def coverage_plan_count(self, *, cutoff):
        self.list_count_calls += 1
        baseline = await super().coverage_plan_count(cutoff=cutoff)
        return baseline if self.list_count_calls == 1 else baseline + 1


class _Repository:
    def __init__(
        self,
        current=None,
        *,
        verify_error=None,
        completed=None,
        loaded_variants=None,
    ):
        self.current = current or CurrentSnapshot(None, None, {})
        self.verify_error = verify_error
        self.coverage = []
        self.alias_versions = []
        self.reused = []
        self.checkpoints = []
        self.failed = None
        self.interrupted = None
        self.published = False
        self.pointer = "previous-dataset"
        self.completed = completed or {}
        self.loaded_prior_aliases = []
        self.begin_requests = []
        self.loaded_variants = loaded_variants or {}

    async def begin_dataset(self, **kwargs):
        self.begin_requests.append(kwargs)
        return "candidate-dataset"

    async def current_snapshot(self):
        return self.current

    async def load_prior_alias_state(self, prior):
        self.loaded_prior_aliases.append(prior.alias_id)
        return replace(
            prior,
            variants_by_medication_id=self.loaded_variants.get(
                prior.alias_id,
                prior.variants_by_medication_id,
            ),
        )

    async def put_coverage_plan(self, *, dataset_id, plan):
        assert dataset_id == "candidate-dataset"
        self.coverage.append(plan)
        return {
            alias: f"alias-{index}"
            for index, alias in enumerate(plan.source_plan_identifiers)
        }

    async def link_reused_alias(self, *, dataset_id, prior):
        self.reused.append((dataset_id, prior.alias_version_id))

    async def put_alias_version(self, write):
        write_by_field = {
            **vars(write),
            "records": write.medications,
        }
        self.alias_versions.append(write_by_field)
        return f"version-{write.alias_id}"

    async def save_checkpoint(self, checkpoint):
        self.checkpoints.append(vars(checkpoint))

    async def completed_alias_checkpoint(self, **kwargs):
        return self.completed.get(kwargs["alias_id"])

    async def verify_dataset(self, dataset_id):
        if self.verify_error:
            raise self.verify_error
        return {
            "list_count": len(self.coverage),
            "alias_count": len(self.alias_versions) + len(self.reused),
            "medication_membership_count": sum(
                item["expected_count"] for item in self.alias_versions
            ),
            "coverage_hash": "c" * 64,
            "membership_hash": "m" * 64,
        }

    async def publish_dataset(self, dataset_id):
        self.published = True
        self.pointer = dataset_id
        return 2

    async def fail_dataset(self, dataset_id, exc):
        self.failed = (dataset_id, exc)

    async def interrupt_dataset(self, dataset_id, exc):
        self.interrupted = (dataset_id, exc)


@pytest.mark.asyncio
async def test_equal_total_aliases_are_crawled_independently_and_keep_different_members():
    client = _Client()
    repository = _Repository()

    result = await synchronize(
        client=client,
        repository=repository,
        run_id="synthetic-run",
        cutoff=CUTOFF,
        publish=False,
        seed_eligible=True,
        alias_concurrency=2,
    )

    assert result["alias_modes"] == {"reuse": 0, "delta": 0, "full": 2}
    records_by_alias = {
        item["alias_id"]: {record.upstream_medication_id for record in item["records"]}
        for item in repository.alias_versions
    }
    assert records_by_alias["alias-0"] == {"MI-synthetic-drug-a"}
    assert records_by_alias["alias-1"] == {"MI-synthetic-drug-b"}
    assert all(value == 1 for value in client.maximum_active_by_alias.values())
    assert repository.published is False
    assert repository.begin_requests[0]["seed_eligible"] is True


@pytest.mark.asyncio
async def test_empty_delta_reuses_prior_alias_version(monkeypatch):
    monkeypatch.setattr(
        "process.formulary_fhir.synchronizer.is_rolling_reconciliation_due",
        lambda *_args, **_kwargs: False,
    )
    public_id = public_formulary_id(BASE, "synthetic-coverage-a")
    prior_state_by_key = {
        (public_id, "SYNTH-NCAL-A"): PriorAliasState(
            alias_id="prior-a",
            alias_version_id="prior-version-a",
            expected_count=1,
            cutoff_at=CUTOFF - dt.timedelta(days=1),
            variants_by_medication_id={},
            membership_hash_value="a" * 64,
        ),
        (public_id, "SYNTH-NCAL-B"): PriorAliasState(
            alias_id="prior-b",
            alias_version_id="prior-version-b",
            expected_count=1,
            cutoff_at=CUTOFF - dt.timedelta(days=1),
            variants_by_medication_id={},
            membership_hash_value="b" * 64,
        ),
    }
    repository = _Repository(
        CurrentSnapshot(
            "previous-dataset",
            CUTOFF - dt.timedelta(days=1),
            prior_state_by_key,
        )
    )

    sync_result_by_field = await synchronize(
        client=_Client(),
        repository=repository,
        run_id="synthetic-reuse-run",
        cutoff=CUTOFF,
    )

    assert sync_result_by_field["alias_modes"] == {
        "reuse": 2,
        "delta": 0,
        "full": 0,
    }
    assert len(repository.reused) == 2
    assert repository.loaded_prior_aliases == []


@pytest.mark.asyncio
async def test_nonempty_equal_count_delta_loads_only_the_changed_prior_alias(
    monkeypatch,
):
    monkeypatch.setattr(
        "process.formulary_fhir.synchronizer.is_rolling_reconciliation_due",
        lambda *_args, **_kwargs: False,
    )
    public_id = public_formulary_id(BASE, "synthetic-coverage-a")
    prior_by_key = {
        (public_id, "SYNTH-NCAL-A"): PriorAliasState(
            "prior-a",
            "prior-version-a",
            1,
            CUTOFF - dt.timedelta(days=1),
            {},
            "a" * 64,
        ),
        (public_id, "SYNTH-NCAL-B"): PriorAliasState(
            "prior-b",
            "prior-version-b",
            1,
            CUTOFF - dt.timedelta(days=1),
            {},
            "b" * 64,
        ),
    }
    repository = _Repository(
        CurrentSnapshot(
            "previous-dataset",
            CUTOFF - dt.timedelta(days=1),
            prior_by_key,
        ),
        loaded_variants={
            "prior-a": {"MI-synthetic-drug-a": "a" * 64},
        },
    )
    client = _Client(
        deltas={"SYNTH-NCAL-A": [_fixture("medication_a.json")]},
    )

    sync_result_by_field = await synchronize(
        client=client,
        repository=repository,
        run_id="synthetic-delta-run",
        cutoff=CUTOFF,
    )

    assert sync_result_by_field["alias_modes"] == {
        "reuse": 1,
        "delta": 1,
        "full": 0,
    }
    assert repository.loaded_prior_aliases == ["prior-a"]


@pytest.mark.asyncio
async def test_failed_verification_preserves_previous_pointer():
    repository = _Repository(verify_error=RuntimeError("injected verification failure"))

    with pytest.raises(RuntimeError, match="injected verification failure"):
        await synchronize(
            client=_Client(),
            repository=repository,
            run_id="synthetic-failed-run",
            cutoff=CUTOFF,
            publish=True,
        )

    assert repository.pointer == "previous-dataset"
    assert repository.published is False
    assert repository.failed[0] == "candidate-dataset"


@pytest.mark.asyncio
async def test_resume_reuses_completed_candidate_aliases_without_refetching():
    repository = _Repository(
        completed={
            "alias-0": CompletedAliasCheckpoint(
                alias_version_id="candidate-version-a",
                expected_count=1,
                membership_hash="a" * 64,
                acquisition_mode="full",
            ),
            "alias-1": CompletedAliasCheckpoint(
                alias_version_id="candidate-version-b",
                expected_count=1,
                membership_hash="b" * 64,
                acquisition_mode="delta",
            ),
        }
    )
    client = _Client()

    sync_result_by_field = await synchronize(
        client=client,
        repository=repository,
        run_id="synthetic-resume-run",
        cutoff=CUTOFF,
    )

    assert sync_result_by_field["resumed_aliases"] == 2
    assert sync_result_by_field["alias_modes"] == {
        "reuse": 0,
        "delta": 1,
        "full": 1,
    }
    assert repository.alias_versions == []
    assert repository.checkpoints == []
    assert client.active_by_alias == {}


@pytest.mark.asyncio
async def test_count_drift_fails_candidate_without_switching_pointer():
    repository = _Repository()

    with pytest.raises(RuntimeError, match="count drifted"):
        await synchronize(
            client=_DriftingCountClient(),
            repository=repository,
            run_id="synthetic-drift-run",
            cutoff=CUTOFF,
            publish=True,
        )

    assert repository.pointer == "previous-dataset"
    assert repository.published is False
    assert repository.failed[0] == "candidate-dataset"


@pytest.mark.asyncio
async def test_list_census_drift_fails_before_alias_acquisition():
    repository = _Repository()

    with pytest.raises(RuntimeError, match="List census"):
        await synchronize(
            client=_DriftingListCountClient(),
            repository=repository,
            run_id="synthetic-list-drift-run",
            cutoff=CUTOFF,
            publish=True,
        )

    assert repository.alias_versions == []
    assert repository.pointer == "previous-dataset"
    assert repository.failed[0] == "candidate-dataset"


@pytest.mark.asyncio
async def test_retryable_transport_interruption_preserves_candidate_for_resume():
    class _InterruptedClient(_Client):
        async def alias_count(self, alias, *, cutoff):
            raise FHIRTransportError("synthetic timeout", retryable=True)

    repository = _Repository()

    with pytest.raises(FHIRTransportError, match="synthetic timeout"):
        await synchronize(
            client=_InterruptedClient(),
            repository=repository,
            run_id="synthetic-interrupted-run",
            cutoff=CUTOFF,
            publish=True,
        )

    assert repository.pointer == "previous-dataset"
    assert repository.published is False
    assert repository.failed is None
    assert repository.interrupted[0] == "candidate-dataset"


def test_mi_alias_is_explicit_california_correction_evidence():
    plan = replace(
        parse_coverage_plan(_fixture("coverage_plan.json"), canonical_base=BASE),
        title=None,
        name=None,
    )

    assert _is_california_plan(AliasWork(plan, "MI-SYNTHETIC-PLAN", "alias"))
    assert not _is_california_plan(AliasWork(plan, "SYNTHETIC-OTHER-PLAN", "alias"))


@pytest.mark.asyncio
async def test_failed_alias_wave_cancels_and_drains_siblings():
    started = asyncio.Event()
    cancelled = asyncio.Event()
    never = asyncio.Event()

    async def slow_alias():
        started.set()
        try:
            await never.wait()
        finally:
            cancelled.set()

    async def failed_alias():
        await started.wait()
        raise RuntimeError("synthetic alias failure")

    with pytest.raises(RuntimeError, match="synthetic alias failure"):
        await _run_alias_wave((slow_alias(), failed_alias()))

    assert cancelled.is_set()
