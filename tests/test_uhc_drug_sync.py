# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_parser as parser
import process.formulary_fhir.uhc_drug_spool as spool
import process.formulary_fhir.uhc_drug_sync as sync
from process.formulary_fhir.repository import AliasCompletionFence
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import CompletedAliasCheckpoint
from process.formulary_fhir.repository import CoveragePlanWriteResult
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository_proof import source_medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import LIBRARY_ONLY_LAUNCH_MODE
from process.formulary_fhir.uhc_drug_parser import load_spooled_uhc_plan
from process.formulary_fhir.uhc_drug_parser import spooled_uhc_plan_keys
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_membership_proof,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_sync_contract_hash,
)
from process.formulary_fhir.uhc_source import uhc_formulary_source_manifest
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import install_artifact_reader


CUTOFF = dt.datetime(2026, 8, 10, 12, tzinfo=dt.UTC)


def _binding() -> EnabledSourceBinding:
    definition = uhc_formulary_source_manifest().definition
    return EnabledSourceBinding(
        source_id=definition.source_id,
        config=definition.config,
        configuration_hash="d" * 64,
        alternative_correction=None,
        launch_mode=LIBRARY_ONLY_LAUNCH_MODE,
    )


def _prepared_source(monkeypatch, tmp_path):
    artifacts, bodies_by_name = artifact_set()
    install_artifact_reader(monkeypatch, spool, bodies_by_name)
    spool_path = tmp_path / "uhc-drugs.sqlite"
    evidence = spool.materialize_uhc_drug_spool(
        artifacts,
        spool_path=spool_path,
    )
    return _binding(), artifacts, spool_path, evidence


class _Repository:
    def __init__(
        self,
        evidence,
        *,
        initial_status: str = "building",
        current_dataset: DatasetRef | None = None,
        checkpoints_by_alias: dict[str, CompletedAliasCheckpoint] | None = None,
    ) -> None:
        self.evidence = evidence
        self.status = initial_status
        self.current = CurrentSnapshot(current_dataset, {})
        self.checkpoints_by_alias = checkpoints_by_alias or {}
        self.events: list[str] = []
        self.dataset: DatasetRef | None = None
        self.failed_with: BaseException | None = None
        self.interrupted_with: BaseException | None = None

    async def begin_dataset(self, **values):
        self.events.append("begin")
        previous_dataset_id = (
            self.current.dataset.dataset_id if self.current.dataset else None
        )
        self.dataset = DatasetRef(
            source_id=_binding().source_id,
            dataset_id="ffd_" + "1" * 48,
            run_id=values["run_id"],
            previous_dataset_id=previous_dataset_id,
            cutoff_at=values["cutoff_at"],
            acquisition_contract_hash=values["acquisition_contract_hash"],
            intent=values["intent"],
            status=self.status,
        )
        return self.dataset

    async def current_snapshot(self):
        self.events.append("current")
        return self.current

    async def put_coverage_plan(self, *, dataset, plan):
        self.events.append("put-plan")
        alias = AliasRef(
            source_id=dataset.source_id,
            public_id=plan.public_id,
            alias_id=stable_id(
                "ffa_",
                dataset.source_id,
                plan.public_id,
                plan.source_plan_identifiers[0],
            ),
            source_plan_identifier=plan.source_plan_identifiers[0],
        )
        return CoveragePlanWriteResult(
            dataset=dataset,
            coverage_version_id=stable_id(
                "ffcv_",
                dataset.source_id,
                plan.public_id,
                plan.content_hash,
            ),
            aliases=(alias,),
        )

    async def completed_alias_checkpoint(self, *, dataset, alias):
        self.events.append("checkpoint")
        return self.checkpoints_by_alias.get(alias.alias_id)

    async def next_alias_completion_fence(self, *, dataset, alias):
        self.events.append("fence")
        return AliasCompletionFence(1, None)

    async def put_alias_version(self, write):
        self.events.append("put-full")
        variants_by_medication_id = {
            medication.upstream_medication_id: source_medication_variant_hash(
                medication,
                None,
            )
            for medication in write.medications
        }
        membership_sha256 = membership_hash(variants_by_medication_id)
        alias_version_id = stable_id(
            "ffav_",
            write.dataset.source_id,
            write.alias.alias_id,
            membership_sha256,
        )
        checkpoint = CompletedAliasCheckpoint(
            source_id=write.dataset.source_id,
            dataset_id=write.dataset.dataset_id,
            alias_id=write.alias.alias_id,
            alias_version_id=alias_version_id,
            expected_count=write.expected_count,
            membership_hash=membership_sha256,
            acquisition_mode="full",
        )
        self.checkpoints_by_alias[write.alias.alias_id] = checkpoint
        return AliasVersionResult(
            source_id=write.dataset.source_id,
            dataset_id=write.dataset.dataset_id,
            alias_id=write.alias.alias_id,
            alias_version_id=alias_version_id,
            membership_count=write.expected_count,
            membership_hash=membership_sha256,
            acquisition_mode="full",
        )

    async def verify_dataset(self, *, dataset):
        self.events.append("verify")
        self.status = "verified"
        return DatasetVerification(
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            list_count=self.evidence.plan_count,
            alias_count=self.evidence.plan_count,
            medication_membership_count=(
                self.evidence.medication_membership_count
            ),
            coverage_hash="a" * 64,
            membership_hash="b" * 64,
        )

    async def fail_dataset(self, _dataset, error):
        self.events.append("fail")
        self.failed_with = error

    async def interrupt_dataset(self, _dataset, error):
        self.events.append("interrupt")
        self.interrupted_with = error

    async def link_reused_alias(self, **_values):
        raise AssertionError("UHC sync must not reuse predecessor aliases")

    async def publish_dataset(self, **_values):
        raise AssertionError("UHC sync must not publish")


def _install_sync_fences(monkeypatch) -> None:
    monkeypatch.setattr(sync, "require_source_unchanged", AsyncMock())
    monkeypatch.setattr(sync, "require_full_checkpoints", AsyncMock())


@pytest.mark.asyncio
async def test_sync_writes_only_full_aliases_and_verifies(monkeypatch, tmp_path):
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    repository = _Repository(evidence)
    _install_sync_fences(monkeypatch)

    synchronization_result = await sync.synchronize_uhc_drug_dataset(
        binding=binding,
        artifacts=artifacts,
        spool_path=spool_path,
        evidence=evidence,
        run_id="uhc-synthetic-sync",
        cutoff_at=CUTOFF,
        database=object(),
        repository=repository,
    )

    assert synchronization_result.dataset.status == "verified"
    assert synchronization_result.full_alias_count == evidence.plan_count == 2
    assert synchronization_result.resumed_alias_count == 0
    assert repository.events.count("put-full") == 2
    assert repository.events[-2:] == ["verify", "begin"]
    assert "fail" not in repository.events


@pytest.mark.asyncio
async def test_verified_replay_revalidates_without_plan_writes(
    monkeypatch,
    tmp_path,
) -> None:
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    repository = _Repository(evidence, initial_status="verified")
    _install_sync_fences(monkeypatch)

    synchronization_result = await sync.synchronize_uhc_drug_dataset(
        binding=binding,
        artifacts=artifacts,
        spool_path=spool_path,
        evidence=evidence,
        run_id="uhc-synthetic-replay",
        cutoff_at=CUTOFF,
        database=object(),
        repository=repository,
    )

    assert synchronization_result.resumed_alias_count == evidence.plan_count
    assert "current" not in repository.events
    assert "put-plan" not in repository.events
    assert repository.events == ["begin", "verify", "begin"]


@pytest.mark.asyncio
async def test_wrong_predecessor_fails_candidate(monkeypatch, tmp_path) -> None:
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    repository = _Repository(evidence)
    _install_sync_fences(monkeypatch)
    original_current_snapshot = repository.current_snapshot

    async def changed_current_snapshot():
        await original_current_snapshot()
        published = DatasetRef(
            source_id=binding.source_id,
            dataset_id="ffd_" + "9" * 48,
            run_id="published-run",
            previous_dataset_id=None,
            cutoff_at=CUTOFF - dt.timedelta(days=1),
            acquisition_contract_hash="e" * 64,
            intent="seed",
            status="published",
        )
        return CurrentSnapshot(published, {})

    repository.current_snapshot = changed_current_snapshot

    with pytest.raises(RuntimeError, match="predecessor changed"):
        await sync.synchronize_uhc_drug_dataset(
            binding=binding,
            artifacts=artifacts,
            spool_path=spool_path,
            evidence=evidence,
            run_id="uhc-predecessor-race",
            cutoff_at=CUTOFF,
            database=object(),
            repository=repository,
        )

    assert isinstance(repository.failed_with, RuntimeError)


@pytest.mark.asyncio
async def test_timeout_interrupts_without_masking_original(monkeypatch, tmp_path):
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    repository = _Repository(evidence)
    _install_sync_fences(monkeypatch)
    timeout_error = TimeoutError("synthetic loader timeout")
    monkeypatch.setattr(
        sync,
        "_materialized_plan",
        AsyncMock(side_effect=timeout_error),
    )

    with pytest.raises(TimeoutError) as caught:
        await sync.synchronize_uhc_drug_dataset(
            binding=binding,
            artifacts=artifacts,
            spool_path=spool_path,
            evidence=evidence,
            run_id="uhc-timeout",
            cutoff_at=CUTOFF,
            database=object(),
            repository=repository,
        )

    assert caught.value is timeout_error
    assert repository.interrupted_with is timeout_error


@pytest.mark.asyncio
async def test_sync_rejects_naive_cutoff_before_repository_mutation(
    monkeypatch,
    tmp_path,
) -> None:
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    repository = _Repository(evidence)
    _install_sync_fences(monkeypatch)

    with pytest.raises(ValueError, match="cutoff is invalid"):
        await sync.synchronize_uhc_drug_dataset(
            binding=binding,
            artifacts=artifacts,
            spool_path=spool_path,
            evidence=evidence,
            run_id="uhc-naive-cutoff",
            cutoff_at=CUTOFF.replace(tzinfo=None),
            database=object(),
            repository=repository,
        )

    assert repository.events == []


def test_sync_hash_binds_spool_and_cutoff_but_not_run_identity(
    monkeypatch,
    tmp_path,
) -> None:
    binding, artifacts, _spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )

    first_hash = uhc_drug_sync_contract_hash(
        binding,
        artifacts,
        evidence,
        CUTOFF,
    )
    changed_evidence = replace(
        evidence,
        duplicate_count=evidence.duplicate_count + 1,
    )

    assert first_hash == uhc_drug_sync_contract_hash(
        binding,
        artifacts,
        evidence,
        CUTOFF,
    )
    assert first_hash != uhc_drug_sync_contract_hash(
        binding,
        artifacts,
        changed_evidence,
        CUTOFF,
    )
    assert first_hash != uhc_drug_sync_contract_hash(
        binding,
        artifacts,
        evidence,
        CUTOFF + dt.timedelta(days=1),
    )


def test_sync_rejects_artifact_verified_after_cutoff(monkeypatch, tmp_path) -> None:
    binding, artifacts, _spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    changed_artifact = replace(
        artifacts.artifacts[0],
        verified_at=CUTOFF + dt.timedelta(seconds=1),
    )
    changed_artifacts = replace(
        artifacts,
        artifacts=(changed_artifact, *artifacts.artifacts[1:]),
    )

    with pytest.raises(ValueError, match="synchronization input"):
        uhc_drug_sync_contract_hash(
            binding,
            changed_artifacts,
            evidence,
            CUTOFF,
        )


def test_membership_proof_matches_materialized_plan(monkeypatch, tmp_path) -> None:
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        plan_key = spooled_uhc_plan_keys(spool_snapshot)[0]
        materialized_plan = load_spooled_uhc_plan(
            spool_snapshot,
            plan_key,
            source_id=binding.source_id,
            canonical_base=binding.config.canonical_base,
            evidence=evidence,
        )

    membership_proof = uhc_drug_membership_proof(materialized_plan)

    assert membership_proof.medication_count == len(materialized_plan.medications)
    assert len(membership_proof.membership_sha256) == 64
