# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path coverage for dormant formulary repository primitives."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_batch
from process.formulary_fhir import repository_checkpoint
from process.formulary_fhir import repository_coverage
from process.formulary_fhir import repository_publish
from process.formulary_fhir import repository_verify
from process.formulary_fhir import repository_write
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import lock_source
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


def _dataset(**overrides) -> DatasetRef:
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": "ffd_" + "a" * 48,
        "run_id": "run-a",
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "acquisition_contract_hash": "c" * 64,
        "intent": "none",
        "status": "building",
    }
    values_by_field.update(overrides)
    return DatasetRef(**values_by_field)


def _alias(**overrides) -> AliasRef:
    values_by_field = {
        "source_id": "source-a",
        "public_id": "fhir_" + "a" * 26,
        "alias_id": "ffa_" + "a" * 48,
        "source_plan_identifier": "SYNTHETIC-PLAN",
    }
    values_by_field.update(overrides)
    return AliasRef(**values_by_field)


def _dataset_row(**overrides):
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": "ffd_" + "a" * 48,
        "run_id": "run-a",
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "status": "building",
        "publish_requested": False,
        "seed_eligible": False,
        "summary_json": {"acquisition_contract_hash": "c" * 64},
        "list_count": 0,
        "alias_count": 0,
        "medication_count": 0,
        "coverage_hash": None,
        "membership_hash": None,
        "published_at": None,
    }
    values_by_field.update(overrides)
    return values_by_field


def _medication(index: int = 1, **overrides) -> MedicationRecord:
    values_by_field = {
        "upstream_medication_id": f"med-{index}",
        "upstream_version_id": "1",
        "upstream_last_updated": CUTOFF,
        "status": "active",
        "drug_name": "Synthetic medication",
        "rxnorm_id": str(index),
        "ndc11": None,
        "codings": (),
        "raw_extensions": (),
        "source_plan_identifiers": ("SYNTHETIC-PLAN",),
        "drug_tier": "preferred",
        "prior_authorization": False,
        "step_therapy": False,
        "quantity_limit": False,
        "alternative_references": (),
        "content_hash": f"{index:064x}",
    }
    values_by_field.update(overrides)
    return MedicationRecord(**values_by_field)


def _plan(**overrides) -> CoveragePlanRecord:
    values_by_field = {
        "upstream_list_id": "list-a",
        "public_id": "fhir_" + "a" * 26,
        "canonical_identity": "https://a.example.invalid/fhir/List/list-a",
        "upstream_version_id": "1",
        "upstream_last_updated": CUTOFF,
        "status": "current",
        "title": "Synthetic plan",
        "name": "Synthetic",
        "upstream_date": CUTOFF,
        "period_start": None,
        "period_end": None,
        "source_plan_identifiers": ("SYNTHETIC-PLAN",),
        "raw_identifiers": (),
        "raw_extensions": (),
        "content_hash": "a" * 64,
    }
    values_by_field.update(overrides)
    return CoveragePlanRecord(**values_by_field)


@pytest.mark.asyncio
async def test_source_and_dataset_locks_fail_closed():
    source_database = SimpleNamespace(first=AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="not registered"):
        await lock_source(source_database, "source-a")
    source_database.first.return_value = {"source_id": "source-a"}
    assert await lock_source(source_database, "source-a") == {
        "source_id": "source-a"
    }

    dataset_database = SimpleNamespace(first=AsyncMock(return_value=_dataset_row()))
    assert (
        await lock_dataset(
            dataset_database,
            "source-a",
            _dataset(),
            allowed_statuses={"building"},
        )
    )["dataset_id"].startswith("ffd_")
    with pytest.raises(RuntimeError, match="source is inconsistent"):
        await lock_dataset(
            dataset_database,
            "source-b",
            _dataset(),
            allowed_statuses={"building"},
        )
    dataset_database.first.return_value = _dataset_row(run_id="changed")
    with pytest.raises(RuntimeError, match="reference is inconsistent"):
        await lock_dataset(
            dataset_database,
            "source-a",
            _dataset(),
            allowed_statuses={"building"},
        )
    dataset_database.first.return_value = _dataset_row(status="verified")
    with pytest.raises(RuntimeError, match="lifecycle state"):
        await lock_dataset(
            dataset_database,
            "source-a",
            _dataset(),
            allowed_statuses={"building"},
        )


@pytest.mark.asyncio
async def test_batch_readback_rejects_medication_and_alternative_mismatch():
    medication = _medication()
    database = SimpleNamespace(all=AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match="medication collision"):
        await repository_batch._assert_medication_batch(
            database,
            "source-a",
            (medication,),
        )
    evidence_medication = replace(
        medication,
        upstream_medication_id="med-0",
        alternative_references=("MedicationKnowledge/med-1",),
    )
    evidence_rows = repository_batch._alternative_rows(
        (evidence_medication, medication),
        {"med-0", "med-1"},
    )
    assert evidence_rows[0][1].is_resolved is True
    with pytest.raises(RuntimeError, match="alternative evidence"):
        await repository_batch._assert_alternatives(
            database,
            "source-a",
            "alias-version",
            evidence_rows,
        )


@pytest.mark.asyncio
async def test_checkpoint_write_paths_reject_stale_or_inconsistent_rows(monkeypatch):
    checkpoint = CheckpointWrite(
        _dataset(),
        _alias(),
        1,
        "full",
        1,
        0,
        None,
        False,
    )
    database = SimpleNamespace()
    monkeypatch.setattr(
        repository_checkpoint,
        "_is_checkpoint_updated",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        repository_checkpoint,
        "_is_checkpoint_inserted",
        AsyncMock(return_value=False),
    )
    with pytest.raises(RuntimeError, match="fence was rejected"):
        await repository_checkpoint.save_checkpoint_row(
            database,
            "source-a",
            checkpoint,
        )
    params = repository_checkpoint._checkpoint_params("source-a", checkpoint)
    mismatch_database = SimpleNamespace(first=AsyncMock(return_value={}))
    with pytest.raises(RuntimeError, match="write is inconsistent"):
        await repository_checkpoint._assert_checkpoint_write(
            mismatch_database,
            params,
        )


def test_completed_checkpoint_row_requires_exact_count_link_and_hash():
    dataset = _dataset()
    alias = _alias()
    valid_row_by_field = {
        "expected_count": 1,
        "processed_count": 1,
        "membership_count": 1,
        "membership_hash": "a" * 64,
        "alias_membership_hash": "a" * 64,
        "alias_version_id": "ffav_" + "a" * 48,
        "acquisition_mode": "full",
    }
    completed = repository_checkpoint._validated_completed_row(
        "source-a",
        dataset,
        alias,
        valid_row_by_field,
    )
    assert completed.expected_count == 1
    for field_name, invalid_value in (
        ("processed_count", 0),
        ("membership_count", 0),
        ("alias_membership_hash", "b" * 64),
        ("alias_version_id", None),
        ("acquisition_mode", "delta"),
    ):
        with pytest.raises(RuntimeError, match="checkpoint is inconsistent"):
            repository_checkpoint._validated_completed_row(
                "source-a",
                dataset,
                alias,
                {**valid_row_by_field, field_name: invalid_value},
            )


@pytest.mark.asyncio
async def test_completed_checkpoint_recomputes_persisted_membership(monkeypatch):
    database = SimpleNamespace(first=AsyncMock(return_value=None))
    completed_alias = await repository_checkpoint.completed_checkpoint(
        database,
        "source-a",
        _dataset(),
        _alias(),
    )
    assert completed_alias is None
    database.first.return_value = {
        "expected_count": 1,
        "processed_count": 1,
        "membership_count": 1,
        "membership_hash": "a" * 64,
        "alias_membership_hash": "a" * 64,
        "alias_version_id": "version-a",
        "acquisition_mode": "reuse",
    }
    monkeypatch.setattr(
        repository_checkpoint,
        "persisted_membership_proof",
        AsyncMock(return_value=(0, membership_hash({}), {})),
    )
    with pytest.raises(RuntimeError, match="membership is inconsistent"):
        await repository_checkpoint.completed_checkpoint(
            database,
            "source-a",
            _dataset(),
            _alias(),
        )


@pytest.mark.asyncio
async def test_coverage_identity_version_and_link_collisions_fail_closed():
    database = SimpleNamespace(
        status=AsyncMock(return_value=0),
        first=AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="public identity collision"):
        await repository_coverage._insert_identity(database, "source-a", _plan())
    with pytest.raises(RuntimeError, match="coverage version collision"):
        await repository_coverage._insert_version(
            database,
            "source-a",
            "version-a",
            _plan(),
        )
    with pytest.raises(RuntimeError, match="coverage link is inconsistent"):
        await repository_coverage._link_version(
            database,
            "source-a",
            _dataset(),
            _plan(),
            "version-a",
        )


def test_alias_preparation_rejects_duplicates_count_and_bad_hash():
    write = AliasVersionWrite(_dataset(), _alias(), 1, (_medication(),), 1)
    prepared = repository_write._prepare_alias_version("source-a", write)
    assert prepared.membership_hash == membership_hash(prepared.variants_by_id)
    duplicate_write = AliasVersionWrite(
        _dataset(),
        _alias(),
        2,
        (_medication(), _medication()),
        1,
    )
    with pytest.raises(RuntimeError, match="duplicate medication"):
        repository_write._prepare_alias_version("source-a", duplicate_write)
    count_write = AliasVersionWrite(_dataset(), _alias(), 2, (_medication(),), 1)
    with pytest.raises(RuntimeError, match="exact alias count"):
        repository_write._prepare_alias_version("source-a", count_write)
    bad_hash_write = AliasVersionWrite(
        _dataset(),
        _alias(),
        1,
        (_medication(content_hash="bad"),),
        1,
    )
    with pytest.raises(ValueError, match="content hash"):
        repository_write._prepare_alias_version("source-a", bad_hash_write)
    crossed_plan_write = AliasVersionWrite(
        _dataset(),
        _alias(),
        1,
        (_medication(source_plan_identifiers=("SYNTHETIC-OTHER",)),),
        1,
    )
    with pytest.raises(RuntimeError, match="source plan"):
        repository_write._prepare_alias_version("source-a", crossed_plan_write)


@pytest.mark.asyncio
async def test_alias_version_and_dataset_link_readbacks_fail_closed():
    write = AliasVersionWrite(_dataset(), _alias(), 1, (_medication(),), 1)
    prepared = repository_write._prepare_alias_version("source-a", write)
    database = SimpleNamespace(
        status=AsyncMock(return_value=0),
        first=AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="alias version collision"):
        await repository_write._insert_alias_version(
            database,
            "source-a",
            write,
            prepared,
        )
    with pytest.raises(RuntimeError, match="alias link is inconsistent"):
        await repository_write._link_alias_version(
            database,
            "source-a",
            _dataset(),
            _alias(),
            "version-a",
        )


def test_publication_policy_rejects_invalid_state_intent_lineage_and_cutoff():
    requested = _dataset(intent="requested", status="verified")
    verified_row = _dataset_row(
        status="verified",
        publish_requested=True,
    )
    cases = (
        (_dataset_row(status="building"), {}, False, "not verified"),
        (verified_row, {}, True, "not publishable"),
        (
            verified_row,
            {"dataset_id": "other"},
            False,
            "predecessor is stale",
        ),
        (
            verified_row,
            {"cutoff_at": CUTOFF + dt.timedelta(days=1)},
            False,
            "candidate cutoff is stale",
        ),
    )
    for dataset_row, current_row, seed_proof, message in cases:
        with pytest.raises(RuntimeError, match=message):
            repository_publish._validate_publication_policy(
                requested,
                dataset_row,
                current_row,
                seed_proof=seed_proof,
            )
    seed = _dataset(intent="seed", status="verified")
    with pytest.raises(RuntimeError, match="requires no pointer"):
        repository_publish._validate_publication_policy(
            seed,
            _dataset_row(status="verified", seed_eligible=True),
            {"dataset_id": "existing"},
            seed_proof=True,
        )


def test_idempotent_publication_requires_a_valid_current_pointer():
    dataset = _dataset(status="published")
    assert (
        repository_publish._idempotent_result(
            "source-a",
            dataset,
            _dataset_row(status="verified"),
            {},
        )
        is None
    )
    with pytest.raises(RuntimeError, match="not current"):
        repository_publish._idempotent_result(
            "source-a",
            dataset,
            _dataset_row(status="published"),
            {"dataset_id": "other"},
        )
    with pytest.raises(RuntimeError, match="pointer is invalid"):
        repository_publish._idempotent_result(
            "source-a",
            dataset,
            _dataset_row(status="published"),
            {"dataset_id": dataset.dataset_id},
        )


def test_coverage_proof_rejects_missing_duplicate_and_repeated_plans():
    base_row_by_field = {
        "public_id": "fhir_" + "a" * 26,
        "canonical_identity": "identity-a",
        "coverage_version_id": "version-a",
        "content_hash": "a" * 64,
        "metadata_json": {"source_plan_identifiers": ["SYNTHETIC-PLAN"]},
    }
    expected, proof_rows = repository_verify._coverage_proof(
        "source-a",
        [base_row_by_field],
    )
    assert expected[base_row_by_field["public_id"]] == {"SYNTHETIC-PLAN"}
    assert len(proof_rows) == 1
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        repository_verify._coverage_proof(
            "source-a",
            [{**base_row_by_field, "metadata_json": {}}],
        )
    with pytest.raises(RuntimeError, match="has duplicates"):
        repository_verify._coverage_proof(
            "source-a",
            [
                {
                    **base_row_by_field,
                    "metadata_json": {
                        "source_plan_identifiers": ["PLAN", "PLAN"]
                    },
                }
            ],
        )
    with pytest.raises(RuntimeError, match="duplicate plans"):
        repository_verify._coverage_proof(
            "source-a",
            [base_row_by_field, base_row_by_field],
        )
