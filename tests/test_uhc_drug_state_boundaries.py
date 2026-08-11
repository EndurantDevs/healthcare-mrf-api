# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-boundary coverage for retained UHC formulary state helpers."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import datetime as dt
import os
from pathlib import Path
import stat
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

import process.formulary_fhir.async_safety as async_safety
import process.formulary_fhir.sync_lifecycle as lifecycle
import process.formulary_fhir.source as source
import process.formulary_fhir.uhc_drug_acquisition as acquisition
import process.formulary_fhir.uhc_drug_repository_writer as writer
import process.formulary_fhir.uhc_drug_sync as sync
import process.formulary_fhir.uhc_drug_twin as twin
import process.provider_directory_retained_blob_staging as staging
import process.provider_directory_retained_blob_store as blob_store
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.source_artifact_contract import artifact_sort_key
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_receipt_test_support import admitted_twin
from tests.uhc_drug_state_boundary_test_support import (
    install_postflight_drift_mocks,
)
from tests.test_uhc_drug_sync import _binding
from tests.test_uhc_drug_sync import CUTOFF


def _directory_state(*, owner: int, mode: int) -> SimpleNamespace:
    return SimpleNamespace(
        st_dev=1,
        st_ino=2,
        st_mode=stat.S_IFDIR | mode,
        st_uid=owner,
    )


@pytest.mark.asyncio
async def test_cooperative_thread_error_without_outer_cancel_is_preserved() -> None:
    """A worker-originated cooperative stop remains its own error."""

    def worker(*, cancel_check) -> None:
        del cancel_check
        raise async_safety.CooperativeThreadCancellation("synthetic stop")

    with pytest.raises(async_safety.CooperativeThreadCancellation):
        await async_safety.cancellable_to_thread(worker)


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_fails", [False, True])
async def test_outer_cancel_wins_after_worker_finishes(worker_fails: bool) -> None:
    """Cancellation wins over both a late worker result and a late error."""

    started = threading.Event()
    release = threading.Event()

    def worker(*, cancel_check):
        del cancel_check
        started.set()
        release.wait(timeout=5)
        if worker_fails:
            raise RuntimeError("late worker failure")
        return "late result"

    operation = asyncio.create_task(async_safety.cancellable_to_thread(worker))
    await asyncio.to_thread(started.wait, 5)
    operation.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await operation


def test_retryable_marker_is_resumable() -> None:
    """Explicit retryable transport-style failures are resumable."""

    error = RuntimeError("synthetic retry")
    error.retryable = True
    assert lifecycle.is_resumable_synchronization_error(error)


@pytest.mark.asyncio
async def test_sync_cancel_callback_and_exact_source_repr() -> None:
    """Synchronous cancellation polling and redacted source repr are covered."""

    await acquisition._invoke_cancel(lambda: None)
    definition = source.ExactSourceDefinition(
        source_id="synthetic-source",
        display_name="Synthetic source",
        config=_binding().config,
        metadata={},
    )
    assert repr(definition) == "ExactSourceDefinition(source_id='synthetic-source')"


def test_staging_directory_descriptor_failures_are_normalized(monkeypatch) -> None:
    """Ownership, mode, and descriptor errors all fail closed."""

    wrong_owner = _directory_state(owner=os.geteuid() + 1, mode=0o700)
    monkeypatch.setattr(staging.os, "fstat", lambda _descriptor: wrong_owner)
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        staging._require_private_owned_directory(9)

    broad_mode = _directory_state(owner=os.geteuid(), mode=0o755)
    monkeypatch.setattr(staging.os, "fstat", lambda _descriptor: broad_mode)
    monkeypatch.setattr(staging.os, "fchmod", lambda *_arguments: None)
    monkeypatch.setattr(staging.install_io, "_sync_directory", lambda *_args: None)
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        staging._require_private_owned_directory(9)

    def fail_fstat(_descriptor):
        raise OSError("synthetic fstat failure")

    monkeypatch.setattr(staging.os, "fstat", fail_fstat)
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        staging._require_private_owned_directory(9)


@pytest.mark.parametrize("name", ["", "../escape", "slash/name", object()])
def test_staging_directory_rejects_invalid_basename(name) -> None:
    """Only one bounded safe staging basename is accepted."""

    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        staging.prepare_retained_artifact_staging_directory(name)


def test_blob_reader_rejects_invalid_size_and_incomplete_read() -> None:
    """Sequential retained reads reject invalid requests and empty chunks."""

    opened_blob = SimpleNamespace(read_at=lambda *_arguments: b"")
    reader = blob_store._RetainedArtifactBlobReader(
        opened_blob,
        "a" * 64,
        1,
    )
    with pytest.raises(ValueError, match="read size"):
        reader.read(-2)
    with pytest.raises(RetainedArtifactError, match="read_incomplete"):
        reader.read()


def _artifact_result(exact_artifacts: VerifiedSourceArtifactSet, **changes):
    result_by_field = {
        "source_id": exact_artifacts.source_id,
        "source_observation_sha256": "c" * 64,
        "source_file_set_sha256": exact_artifacts.source_file_set_sha256,
        "artifact_set_sha256": exact_artifacts.artifact_set_sha256,
        "file_count": 48,
        "downloaded_file_count": 0,
        "reused_file_count": 48,
        "downloaded_byte_count": 0,
        "artifacts": exact_artifacts,
    }
    result_by_field.update(changes)
    return acquisition.UHCDrugArtifactAcquisitionResult(**result_by_field)


def test_acquisition_result_rejects_census_and_family_drift() -> None:
    """Aggregate and exact 24 plus 24 family invariants are independent."""

    exact_artifacts, _bodies = artifact_set()
    with pytest.raises(ValueError, match="result is invalid"):
        _artifact_result(exact_artifacts, reused_file_count=47)

    changed_first = replace(
        exact_artifacts.artifacts[0],
        identity=replace(exact_artifacts.artifacts[0].identity, family="ifp"),
    )
    changed_rows = tuple(
        sorted(
            (changed_first, *exact_artifacts.artifacts[1:]),
            key=artifact_sort_key,
        )
    )
    changed_artifacts = VerifiedSourceArtifactSet(
        source_id=exact_artifacts.source_id,
        source_file_set_sha256=exact_artifacts.source_file_set_sha256,
        raw_listing_projection_sha256=(exact_artifacts.raw_listing_projection_sha256),
        artifacts=changed_rows,
        artifact_set_sha256=artifact_set_sha256(changed_rows),
    )
    with pytest.raises(ValueError, match="result is invalid"):
        _artifact_result(changed_artifacts)

    assert "downloaded_file_count=0" in repr(_artifact_result(exact_artifacts))


@pytest.mark.asyncio
async def test_acquisition_rejects_source_binding_postflight_drift(monkeypatch) -> None:
    """A source binding change after the retained scan blocks success."""

    exact_artifacts, _bodies = artifact_set()
    binding = _binding()
    changed_binding = replace(binding, configuration_hash="e" * 64)
    registration = SimpleNamespace(
        identities=tuple(artifact.identity for artifact in exact_artifacts.artifacts),
        source_observation_sha256="c" * 64,
    )
    install_postflight_drift_mocks(
        monkeypatch,
        binding=binding,
        changed_binding=changed_binding,
        registration=registration,
        exact_artifacts=exact_artifacts,
    )

    with pytest.raises(
        acquisition.UHCDrugArtifactAcquisitionError,
        match="source changed",
    ):
        await acquisition.acquire_uhc_drug_artifacts(
            object(),
            database=object(),
        )


def test_repository_dataset_and_fence_fail_closed(monkeypatch) -> None:
    """Malformed root and completion-fence values never write an alias."""

    with pytest.raises(RuntimeError, match="dataset is inconsistent"):
        writer.require_exact_uhc_dataset(
            SimpleNamespace(
                source_id=_binding().source_id,
                run_id="synthetic-run",
                cutoff_at=CUTOFF,
                acquisition_contract_hash="a" * 64,
                intent="none",
                status="building",
            ),
            binding=_binding(),
            run_id="synthetic-run",
            cutoff_at=CUTOFF,
            contract_hash="a" * 64,
            intent="none",
        )


@pytest.mark.asyncio
async def test_repository_writer_resumes_and_rejects_bad_fence(monkeypatch) -> None:
    """Completed checkpoints resume, while a malformed fence is terminal."""

    proof = SimpleNamespace(medication_count=1)
    alias = SimpleNamespace(alias_id="ffa_synthetic")
    dataset = SimpleNamespace()
    materialized = SimpleNamespace(coverage_plan=object(), medications=(object(),))
    repository = SimpleNamespace(
        put_coverage_plan=AsyncMock(return_value=object()),
        completed_alias_checkpoint=AsyncMock(return_value=object()),
        next_alias_completion_fence=AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(writer, "uhc_drug_membership_proof", lambda _plan: proof)
    monkeypatch.setattr(
        writer,
        "require_exact_coverage_write",
        lambda *_arguments: alias,
    )
    completed_guard = Mock()
    monkeypatch.setattr(writer, "require_exact_completed_checkpoint", completed_guard)

    assert (
        await writer.write_or_resume_uhc_plan(repository, dataset, materialized)
        == "resumed"
    )
    completed_guard.assert_called_once()

    repository.completed_alias_checkpoint.return_value = None
    with pytest.raises(RuntimeError, match="completion fence is invalid"):
        await writer.write_or_resume_uhc_plan(repository, dataset, materialized)


@pytest.mark.asyncio
async def test_sync_private_guards_reject_drift(monkeypatch) -> None:
    """Spool, repository, and write-census drift each fail independently."""

    _twin_result, _artifacts = admitted_twin()
    evidence = _twin_result.baseline.evidence
    with pytest.raises(ValueError, match="snapshot is invalid"):
        await sync._spool_keys(object(), evidence)

    class Snapshot:
        pass

    monkeypatch.setattr(sync, "_VerifiedUHCDrugSpool", Snapshot)
    monkeypatch.setattr(sync, "_drained_to_thread", AsyncMock(return_value=()))
    with pytest.raises(RuntimeError, match="plan census"):
        await sync._spool_keys(Snapshot(), evidence)

    building_dataset = replace(_twin_result.baseline.dataset, status="building")
    monkeypatch.setattr(
        sync,
        "_begin_exact_dataset",
        AsyncMock(return_value=building_dataset),
    )
    with pytest.raises(RuntimeError, match="verification did not persist"):
        await sync._reload_verified_dataset(SimpleNamespace())

    invalid_context = SimpleNamespace(
        repository=SimpleNamespace(current_snapshot=AsyncMock(return_value=object())),
        evidence=evidence,
    )
    with pytest.raises(RuntimeError, match="current snapshot is invalid"):
        await sync._build_and_finish_dataset(
            invalid_context,
            building_dataset,
            (),
            Snapshot(),
        )

    empty_context = SimpleNamespace(
        repository=SimpleNamespace(
            current_snapshot=AsyncMock(return_value=CurrentSnapshot(None, {}))
        ),
        evidence=evidence,
    )
    monkeypatch.setattr(sync, "require_exact_predecessor", lambda *_arguments: None)
    with pytest.raises(RuntimeError, match="write census"):
        await sync._build_and_finish_dataset(
            empty_context,
            building_dataset,
            (),
            Snapshot(),
        )


@pytest.mark.asyncio
async def test_sync_final_spool_drift_blocks_verified_result(monkeypatch) -> None:
    """A different terminal key vector blocks the verified result."""

    twin_result, artifacts = admitted_twin()
    evidence = twin_result.baseline.evidence
    dataset = twin_result.baseline.dataset
    context = SimpleNamespace(
        database=object(),
        evidence=evidence,
        repository=SimpleNamespace(
            verify_dataset=AsyncMock(return_value=twin_result.baseline.verification)
        ),
        artifacts=artifacts,
        binding=_binding(),
    )
    monkeypatch.setattr(sync, "require_full_checkpoints", AsyncMock())
    monkeypatch.setattr(sync, "require_exact_verification", lambda *_args: None)
    monkeypatch.setattr(sync, "_reverified_spool_keys", AsyncMock(return_value=("b",)))

    with pytest.raises(RuntimeError, match="spool changed"):
        await sync._finish_verified_dataset(
            context,
            dataset,
            object(),
            ("a",),
            resumed_alias_count=0,
        )


def test_twin_result_and_work_directory_reject_invalid_values(tmp_path: Path) -> None:
    """Twin aggregates and work paths enforce exact constructor contracts."""

    twin_result, _artifacts = admitted_twin()
    with pytest.raises(ValueError, match="twin result is inconsistent"):
        replace(twin_result, candidate=twin_result.baseline)
    with pytest.raises(ValueError, match="work directory is invalid"):
        twin._validated_work_directory(tmp_path / "missing")


@pytest.mark.asyncio
async def test_twin_artifact_and_contract_drift_fail_closed(monkeypatch) -> None:
    """Retained-set and normalization-contract changes block admission."""

    twin_result, artifacts = admitted_twin()
    request = SimpleNamespace(artifacts=artifacts, database=object(), cutoff_at=CUTOFF)
    monkeypatch.setattr(
        twin,
        "load_complete_source_artifact_set",
        AsyncMock(
            return_value=replace(
                artifacts,
                artifacts=(
                    replace(
                        artifacts.artifacts[0],
                        verified_at=(
                            artifacts.artifacts[0].verified_at + dt.timedelta(seconds=1)
                        ),
                    ),
                    *artifacts.artifacts[1:],
                ),
            )
        ),
    )
    with pytest.raises(RuntimeError, match="retained artifacts changed"):
        await twin._require_artifacts_unchanged(request)

    monkeypatch.setattr(
        twin,
        "uhc_drug_sync_contract_hash",
        Mock(side_effect=["a" * 64, "b" * 64]),
    )
    with pytest.raises(RuntimeError, match="contracts do not match"):
        twin._require_matching_contract_hashes(
            object(),
            request,
            twin_result.baseline.evidence,
            twin_result.candidate.evidence,
        )


@pytest.mark.asyncio
async def test_twin_independent_spool_and_temporary_directory_guards(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Different normalization evidence fails before verification or admission."""

    baseline_path = tmp_path / "baseline.sqlite"
    candidate_path = tmp_path / "candidate.sqlite"
    baseline_path.touch()
    candidate_path.touch()
    twin_result, artifacts = admitted_twin()
    different_evidence = replace(
        twin_result.baseline.evidence,
        duplicate_count=twin_result.baseline.evidence.duplicate_count + 1,
    )
    monkeypatch.setattr(
        twin,
        "cancellable_to_thread",
        AsyncMock(side_effect=[twin_result.baseline.evidence, different_evidence]),
    )
    with pytest.raises(RuntimeError, match="normalizations do not match"):
        await twin._materialize_independent_spools(artifacts, tmp_path)

    expected = object()
    monkeypatch.setattr(
        twin,
        "_build_and_admit_twins",
        AsyncMock(return_value=expected),
    )
    request = SimpleNamespace(work_directory=tmp_path)
    assert await twin._verify_twins_under_lease(request) is expected
