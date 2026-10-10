"""Synthetic admission and publication fences for the CMS bulk source."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import sys
from compression import zstd
from contextlib import asynccontextmanager, contextmanager, nullcontext
from functools import partial
from itertools import count
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.pool import AsyncAdaptedQueuePool

from api.control_imports import importer_registry
from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_observation as cms_observation
from process.cms_npd_source import RESOURCE_FILES, CmsNpdSourceError
from process.provider_directory_profile_source_spec_contract import (
    validated_profile_source_spec,
)
from process.provider_directory_source_local_publication import (
    publish_validated_source_local_dataset,
)
from tests.test_cms_npd_source import _client, _source

fhir = importlib.import_module("process.provider_directory_fhir")


def _dispatch_retained_release(tmp_path):
    """Acquire the same synthetic thousand-row release for all eight families."""
    resource_bytes_by_name = {
        name: b"".join(f'{{"resourceType":"{kind}","id":"synthetic-{index}"}}\n'.encode() for index in range(1000))
        for name, kind in RESOURCE_FILES
    }
    manifest, payloads = _source(resource_bytes_by_name)
    client, _calls = _client(manifest, payloads)
    with client:
        return cms.source.acquire_release(tmp_path, client=client)


def _dispatch_file_tracking(monkeypatch):
    """Track open readers separately from active source writers."""
    opened_files = set()
    writing_types = set()
    peaks_by_phase = {"opened": 0, "writing": 0}
    original_open = cms.zstd.open

    @contextmanager
    def tracked_open(path, mode):
        with original_open(path, mode) as reader:
            opened_files.add(Path(path).name)
            peaks_by_phase["opened"] = max(peaks_by_phase["opened"], len(opened_files))
            try:
                yield reader
            finally:
                opened_files.remove(Path(path).name)

    monkeypatch.setattr(cms.zstd, "open", tracked_open)
    return SimpleNamespace(opened=opened_files, writing=writing_types, peaks=peaks_by_phase, events=[])


def _dispatch_validation_fences(monkeypatch, fixture):
    """Require drained readers and writers before every finalization step."""

    async def checked_step(phase, *_args):
        assert not fixture.opened and not fixture.writing, "validation started before source workers drained"
        fixture.events.append(phase)
        return {kind: 1000 for _, kind in RESOURCE_FILES} if phase == "counts" else {"validated": True}

    for name, phase in (
        ("_assert_counts", "counts"),
        ("_assert_witness_counts", "witnesses"),
        ("_materialize_identity_evidence", "identity"),
        ("_validate_candidate", "validate"),
    ):
        monkeypatch.setattr(cms, name, partial(checked_step, phase))
    monkeypatch.setattr(cms.relationships, "materialize", partial(checked_step, "relationships"))
    monkeypatch.setattr(cms.recovery, "dispose_changed_vector", AsyncMock())
    fixture.publisher = AsyncMock()
    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        fixture.publisher,
    )


def _source_dispatch_fixture(monkeypatch, tmp_path, *, configured=5, size=2, overflow=1):
    """Install the connected pool geometry and retained source-stage fences."""
    directory, receipt = _dispatch_retained_release(tmp_path)
    models_by_type = {kind: object() for _, kind in RESOURCE_FILES}
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", str(configured))
    pool = AsyncAdaptedQueuePool(
        lambda: pytest.fail("source dispatch connected to a database"), pool_size=size, max_overflow=overflow
    )
    fixture = _dispatch_file_tracking(monkeypatch)
    fixture.fhir = SimpleNamespace(
        db=SimpleNamespace(engine=SimpleNamespace(pool=pool)),
        RESOURCE_MODELS_BY_TYPE=models_by_type,
        _provider_directory_database_pool_capacity=fhir._provider_directory_database_pool_capacity,
        _gather_provider_directory_profile_tasks=fhir._gather_provider_directory_profile_tasks,
        _raise_if_resource_import_cancelled=AsyncMock(),
    )
    fixture.candidate = SimpleNamespace(
        dataset_id="source-dispatch-synthetic", already_validated=False, already_published=False
    )
    fixture.directory = directory
    fixture.receipt = receipt
    fixture.identity = cms.release_identity(receipt)
    monkeypatch.setattr(
        cms,
        "_parse_batch_row",
        lambda _fhir, resource, _candidate: (models_by_type[resource["resourceType"]], {"resource_id": resource["id"]}),
    )
    _dispatch_validation_fences(monkeypatch, fixture)
    return fixture


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configured,size,overflow,expected", [(5, 2, 1, 2), (2, 1, 1, 1), (5, 1, 1, 1), (2, 5, 0, 1), (9, 7, 0, 2)]
)
async def test_source_dispatch_overlaps_only_connected_admitted_file_slots(
    monkeypatch, tmp_path, configured, size, overflow, expected
):
    fixture = _source_dispatch_fixture(monkeypatch, tmp_path, configured=configured, size=size, overflow=overflow)
    started = asyncio.Event()
    release = asyncio.Event()
    staged_types = []

    async def persist(_fhir, _model, rows, raw, _candidate, kind):
        assert len(rows) == len(raw) == 1000
        fixture.writing.add(kind)
        fixture.peaks["writing"] = max(fixture.peaks["writing"], len(fixture.writing))
        assert len(fixture.opened) <= expected and len(fixture.writing) <= expected
        if len(fixture.writing) == expected:
            started.set()
        try:
            await release.wait()
            staged_types.append(kind)
        finally:
            fixture.writing.remove(kind)

    monkeypatch.setattr(cms, "_persist_source_batch", persist)
    staging = asyncio.create_task(
        cms._stage_and_validate_candidate(
            fixture.fhir, fixture.directory, fixture.candidate, fixture.identity, fixture.receipt, {}, {}
        )
    )
    try:
        async with asyncio.timeout(2):
            await started.wait()
        assert len(fixture.opened) == expected
        assert fixture.events == []
        release.set()
        assert await staging == {"validated": True}
    finally:
        release.set()
        if not staging.done():
            staging.cancel()
        await asyncio.gather(staging, return_exceptions=True)
    assert sorted(staged_types) == sorted(cms.RESOURCE_TYPES)
    assert fixture.peaks == {"opened": expected, "writing": expected}
    assert fixture.events == ["counts", "witnesses", "identity", "relationships", "validate"]
    fixture.publisher.assert_not_awaited()


def _dispatch_failure_writer(fixture, failure_mode, both_started, drain, failure):
    """Hold both active writers until the test releases their cancellation cleanup."""
    first_kind = RESOURCE_FILES[0][1]

    async def persist(_fhir, _model, _rows, _raw, _candidate, kind):
        fixture.writing.add(kind)
        if len(fixture.writing) == 2:
            both_started.set()
        try:
            await both_started.wait()
            if kind == first_kind and failure_mode not in {"caller_cancel", "repeated_cancel"}:
                if failure_mode in {"first_failure", "cleanup_failure"}:
                    raise failure
                return
            await asyncio.Event().wait()
        finally:
            await drain.wait()
            fixture.writing.remove(kind)
            if failure_mode == "cleanup_failure" and kind != first_kind:
                raise RuntimeError("synthetic-peer-cleanup-failed")

    return persist


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode", ["first_failure", "cleanup_failure", "caller_cancel", "repeated_cancel", "row_count"]
)
async def test_source_dispatch_drains_every_reader_and_writer_before_failure_returns(
    monkeypatch, tmp_path, failure_mode
):
    """Preserve the first failure while every reader and writer drains before validation."""
    fixture = _source_dispatch_fixture(monkeypatch, tmp_path)
    both_started = asyncio.Event()
    drain = asyncio.Event()
    failure = RuntimeError("synthetic-source-write-failed")
    if failure_mode == "row_count":
        fixture.identity["files"][RESOURCE_FILES[0][0]]["row_count"] = 999

    persist = _dispatch_failure_writer(fixture, failure_mode, both_started, drain, failure)

    monkeypatch.setattr(cms, "_persist_source_batch", persist)
    staging = asyncio.create_task(
        cms._stage_and_validate_candidate(
            fixture.fhir, fixture.directory, fixture.candidate, fixture.identity, fixture.receipt, {}, {}
        )
    )
    try:
        async with asyncio.timeout(2):
            await both_started.wait()
        if failure_mode in {"caller_cancel", "repeated_cancel"}:
            staging.cancel()
        await asyncio.sleep(0)
        assert not staging.done() and fixture.writing and fixture.opened
        assert fixture.events == []
        if failure_mode == "repeated_cancel":
            for _ in range(4):
                staging.cancel()
                await asyncio.sleep(0)
                assert not staging.done() and len(fixture.writing) == len(fixture.opened) == 2
        drain.set()
        if failure_mode in {"caller_cancel", "repeated_cancel"}:
            with pytest.raises(asyncio.CancelledError):
                await staging
        else:
            with pytest.raises(RuntimeError) as caught:
                await staging
            if failure_mode in {"first_failure", "cleanup_failure"}:
                assert caught.value is failure
            else:
                assert str(caught.value) == "cms_npd_file_row_count_changed"
    finally:
        drain.set()
        if not staging.done():
            staging.cancel()
        await asyncio.gather(staging, return_exceptions=True)
    assert not fixture.opened and not fixture.writing and fixture.events == []
    fixture.publisher.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("validated,published", [(True, False), (False, True)])
async def test_source_dispatch_replay_verifies_retained_bytes_without_source_writes(
    monkeypatch, tmp_path, validated, published
):
    fixture = _source_dispatch_fixture(monkeypatch, tmp_path)
    fixture.candidate.already_validated = validated
    fixture.candidate.already_published = published
    writer = AsyncMock(side_effect=AssertionError("finalized candidate attempted a source write"))
    monkeypatch.setattr(cms, "_persist_source_batch", writer)
    assert await cms._stage_and_validate_candidate(
        fixture.fhir, fixture.directory, fixture.candidate, fixture.identity, fixture.receipt, {}, {}
    ) == {"validated": True}
    writer.assert_not_awaited()
    (fixture.directory / f"{RESOURCE_FILES[0][0]}.zst").unlink()
    with pytest.raises(CmsNpdSourceError, match="retained_file_missing"):
        await cms._stage_and_validate_candidate(
            fixture.fhir, fixture.directory, fixture.candidate, fixture.identity, fixture.receipt, {}, {}
        )
    writer.assert_not_awaited()
    fixture.publisher.assert_not_awaited()


def test_source_dispatch_uses_serial_unknown_pools_and_rejects_no_writer_capacity():
    capacity = Mock(return_value=5)
    for pool in (None, object(), AsyncAdaptedQueuePool(lambda: None, pool_size=2, max_overflow=-1)):
        fake = SimpleNamespace(
            db=SimpleNamespace(engine=SimpleNamespace(pool=pool)), _provider_directory_database_pool_capacity=capacity
        )
        assert cms._source_stage_workers(fake) == 1
    capacity.assert_not_called()
    pool = AsyncAdaptedQueuePool(lambda: None, pool_size=1, max_overflow=0)
    with pytest.raises(RuntimeError, match="cms_npd_intake_pool_capacity_exceeded"):
        cms._source_stage_workers(
            SimpleNamespace(
                db=SimpleNamespace(engine=SimpleNamespace(pool=pool)),
                _provider_directory_database_pool_capacity=capacity,
            )
        )


@pytest.mark.asyncio
async def test_cms_pre_cutover_deadline_rolls_back_and_releases_transaction() -> None:
    candidate = SimpleNamespace(
        dataset_id="dataset-synthetic",
        endpoint_id="endpoint-synthetic",
        acquisition_root_run_id="run-synthetic",
    )
    dataset = SimpleNamespace(
        source_id="cms-npd",
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        evidence_run_id=candidate.acquisition_root_run_id,
    )
    fence = SimpleNamespace(datasets=[dataset], promotion_datasets=[dataset])
    transaction_state = SimpleNamespace(is_held=False, has_rolled_back=False)

    @asynccontextmanager
    async def transaction():
        transaction_state.is_held = True
        try:
            yield object()
        except BaseException:
            transaction_state.has_rolled_back = True
            raise
        finally:
            transaction_state.is_held = False

    async def stall(_session):
        assert transaction_state.is_held
        await asyncio.Event().wait()

    promote = AsyncMock()
    fake_fhir = SimpleNamespace(
        _resolve_provider_directory_artifact_datasets=AsyncMock(return_value=fence),
        _provider_directory_artifact_transaction_timeout_seconds=lambda _: 2.0,
        _is_provider_directory_dataset_cutover_committed=AsyncMock(return_value=False),
        _promote_provider_directory_artifact_datasets=promote,
        db=SimpleNamespace(transaction=transaction, status=AsyncMock()),
        LOGGER=Mock(),
    )
    with pytest.raises(TimeoutError):
        await publish_validated_source_local_dataset(
            fake_fhir,
            candidate,
            "cms-npd",
            before_cutover=stall,
            before_cutover_timeout_seconds=0.1,
        )
    assert transaction_state.has_rolled_back and not transaction_state.is_held
    promote.assert_not_awaited()
    fake_fhir._is_provider_directory_dataset_cutover_committed.assert_awaited_once_with(fence)


@pytest.fixture(autouse=True)
def _synthetic_recovery(monkeypatch):
    @asynccontextmanager
    async def intake_guard(_fhir, _endpoint_id):
        yield

    monkeypatch.setattr(cms, "_intake_guard", intake_guard)
    monkeypatch.setattr(cms_observation, "enqueue_live_progress", Mock())
    monkeypatch.setattr(cms.recovery, "resume_pending_cleanup", AsyncMock())
    monkeypatch.setattr(cms.recovery, "dispose_prior_vectors", AsyncMock())
    monkeypatch.setattr(cms.recovery, "is_disposed", AsyncMock(return_value=False))


def _receipt() -> dict:
    return {
        "source_id": "cms-npd",
        "generated_at": "2026-09-24",
        "manifest_sha256": "a" * 64,
        "vector_sha256": "b" * 64,
        "files": {
            name: {
                "sha256": "c" * 64,
                "compressed_bytes": 12,
                "original_bytes": 31,
                "row_count": 1,
                "distinct_count": 1,
                "etag": '"synthetic"',
            }
            for name, _ in RESOURCE_FILES
        },
    }


def _observation():
    manifest = cms.source.Manifest(
        "2026-09-24",
        "a" * 64,
        tuple(cms.source.ManifestFile(name, kind, 12, 31) for name, kind in RESOURCE_FILES),
        b"synthetic",
    )
    return cms.source.ObservedRelease(
        manifest,
        tuple(cms.source.FileProbe('"synthetic"', 12) for _ in RESOURCE_FILES),
        "b" * 64,
    )


@pytest.mark.asyncio
async def test_current_observation_requires_exact_covered_publication(monkeypatch):
    generation_by_field = {
        "dataset_id": "dataset-synthetic",
        "dataset_hash": "c" * 64,
        "release_id": "b" * 64,
        "observed_at": "published-synthetic",
    }
    state_by_field = {
        "dataset_id": "dataset-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "dataset_hash": "c" * 64,
        "published_at": "published-synthetic",
        "status": fhir.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "acquisition_root_run_id": "root-synthetic",
        "publication_metadata_json": {"source_release": cms.release_identity(_receipt())},
    }

    @asynccontextmanager
    async def session():
        yield object()

    monkeypatch.setattr(fhir.db, "session", session)
    has_witnesses = AsyncMock(return_value=True)
    monkeypatch.setattr(fhir.db, "scalar", has_witnesses)
    monkeypatch.setattr(cms.relationships, "completed_receipt_count", AsyncMock(return_value=0))
    monkeypatch.setattr(fhir, "_schema", lambda: "synthetic")
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", AsyncMock(return_value=state_by_field))
    accepted = importlib.import_module("api.provider_directory_cms_generation")
    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    monkeypatch.setattr(accepted, "accepted_cms_generation", AsyncMock(return_value=generation_by_field))
    covered = AsyncMock()
    monkeypatch.setattr(coverage, "require_cms_coverage", covered)
    assert await cms._current_observed_publication(_observation()) == state_by_field
    covered.assert_awaited_once()
    has_witnesses.return_value = False
    assert await cms._current_observed_publication(_observation()) is None
    has_witnesses.return_value = True
    state_by_field["publication_metadata_json"]["source_release"]["manifest_sha256"] = "d" * 64
    assert await cms._current_observed_publication(_observation()) is None


@pytest.mark.asyncio
async def test_unchanged_run_skips_acquisition_and_preserves_required_followup(monkeypatch):
    reporter = Mock()
    monkeypatch.setattr(cms_observation, "enqueue_live_progress", reporter)
    observation = _observation()
    state_by_field = {
        "dataset_id": "dataset-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "dataset_hash": "c" * 64,
        "resource_count": 8,
        "acquisition_root_run_id": "root-synthetic",
    }
    descriptor_by_field = {
        "status": "required",
        "dataset_id": state_by_field["dataset_id"],
        "dataset_hash": state_by_field["dataset_hash"],
        "endpoint_id": state_by_field["endpoint_id"],
    }
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))
    monkeypatch.setattr(cms.source, "observe_release", Mock(return_value=observation))
    recheck = Mock()
    monkeypatch.setattr(cms.source, "assert_observed_release_unchanged", recheck)
    monkeypatch.setattr(cms, "_current_observed_publication", AsyncMock(return_value=state_by_field))
    monkeypatch.setattr(
        cms,
        "_completed_tax_candidate_status",
        AsyncMock(return_value={"status": "complete", "retryable": False}),
    )
    acquire = Mock(side_effect=AssertionError("unchanged release must not be acquired"))
    monkeypatch.setattr(cms.source, "acquire_release", acquire)
    monkeypatch.setattr(cms, "_run_acquired", AsyncMock(side_effect=AssertionError("no DB rebuild")))
    followup = AsyncMock(return_value=descriptor_by_field)
    monkeypatch.setattr(cms, "_prepare_current_serving_candidate", followup)
    context_by_field = {"context": {}}
    admission_result = await cms.run(
        context_by_field, {"import_resources": True, "full_refresh": True}, "run-synthetic"
    )
    assert admission_result["cms_serving_candidate"] == descriptor_by_field
    assert admission_result["cms_npd_check"]["outcome"] == "unchanged_current_publication"
    assert admission_result["cms_npd_check"]["vector_sha256"] == observation.vector_sha256
    assert admission_result["cms_npd_check"]["checked_at"]
    assert context_by_field["context"]["audit"] == admission_result
    followup.assert_awaited_once_with(fhir, state_by_field, observation.vector_sha256)
    recheck.assert_called_once()
    acquire.assert_not_called()
    assert [call.kwargs["phase"] for call in reporter.call_args_list] == [
        "cms-npd-source_probe",
        "cms-npd-release_validation",
        "cms-npd-intake_guard",
        "cms-npd-coverage",
        "cms-npd-complete",
    ]


@pytest.mark.asyncio
async def test_unchanged_run_fails_closed_when_final_probe_changes(monkeypatch):
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))
    monkeypatch.setattr(cms.source, "observe_release", Mock(return_value=_observation()))
    monkeypatch.setattr(
        cms, "_current_observed_publication", AsyncMock(return_value={"acquisition_root_run_id": "root"})
    )
    monkeypatch.setattr(
        cms,
        "_completed_tax_candidate_status",
        AsyncMock(return_value={"status": "complete", "retryable": False}),
    )
    monkeypatch.setattr(
        cms.source,
        "assert_observed_release_unchanged",
        Mock(side_effect=CmsNpdSourceError("cms_npd_source_vector_changed")),
    )
    acquire = Mock(side_effect=AssertionError("changed probe must not acquire"))
    monkeypatch.setattr(cms.source, "acquire_release", acquire)
    followup = AsyncMock()
    monkeypatch.setattr(cms, "_prepare_current_serving_candidate", followup)
    context_by_field = {"context": {}}
    with pytest.raises(CmsNpdSourceError, match="source_vector_changed"):
        await cms.run(context_by_field, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    assert context_by_field["context"]["audit"]["cms_npd_check"]["outcome"] == "source_vector_changed"
    followup.assert_not_awaited()
    acquire.assert_not_called()


@pytest.mark.asyncio
async def test_incomplete_initial_probe_records_failed_check_without_acquisition(monkeypatch):
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))
    monkeypatch.setattr(
        cms.source,
        "observe_release",
        Mock(side_effect=CmsNpdSourceError("cms_npd_file_validator_invalid")),
    )
    acquire = Mock(side_effect=AssertionError("invalid probe must not acquire"))
    monkeypatch.setattr(cms.source, "acquire_release", acquire)
    context_by_field = {"context": {}}
    with pytest.raises(CmsNpdSourceError, match="file_validator_invalid"):
        await cms.run(context_by_field, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    check_by_field = context_by_field["context"]["audit"]["cms_npd_check"]
    assert check_by_field["outcome"] == "probe_failed"
    assert check_by_field["vector_sha256"] is None
    assert check_by_field["checked_at"]
    acquire.assert_not_called()


@pytest.mark.asyncio
async def test_unchanged_tax_followup_replays_exact_release(monkeypatch):
    observation = _observation()
    directory = Path("/synthetic/artifacts/cms-npd/releases") / observation.vector_sha256
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: directory.parents[2])
    monkeypatch.setattr(cms.source, "observe_release", Mock(return_value=observation))
    monkeypatch.setattr(
        cms, "_current_observed_publication", AsyncMock(return_value={"dataset_id": "dataset-synthetic"})
    )
    monkeypatch.setattr(cms.importlib.util, "find_spec", Mock(return_value=object()))
    acquire = Mock(return_value=(directory, _receipt()))
    monkeypatch.setattr(cms.source, "acquire_release", acquire)
    monkeypatch.setattr(cms.source, "_release_lock", lambda unused: nullcontext())
    replay = AsyncMock(return_value={"dataset_id": "dataset-synthetic", "tax_candidates": {"retryable": False}})
    monkeypatch.setattr(cms, "_run_acquired", replay)
    admission_result = await cms.run({"context": {}}, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    assert admission_result["cms_npd_check"]["outcome"] == "followup_replay_required"
    assert admission_result["tax_candidates"]["retryable"] is False
    acquire.assert_called_once()
    replay.assert_awaited_once()


@pytest.mark.asyncio
async def test_completed_tax_followup_keeps_unchanged_fast_path(monkeypatch):
    observation = _observation()
    state_by_field = {
        "dataset_id": "dataset-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "dataset_hash": "c" * 64,
        "resource_count": 8,
        "acquisition_root_run_id": "root-synthetic",
    }
    descriptor_by_field = {
        "dataset_id": "dataset-synthetic",
        "dataset_hash": "c" * 64,
        "endpoint_id": "endpoint-synthetic",
    }
    tax_status_by_field = {"status": "complete", "retryable": False, "report_sha256": "d" * 64}
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))
    monkeypatch.setattr(cms.source, "observe_release", Mock(return_value=observation))
    monkeypatch.setattr(cms.source, "assert_observed_release_unchanged", Mock())
    monkeypatch.setattr(cms, "_current_observed_publication", AsyncMock(return_value=state_by_field))
    monkeypatch.setattr(cms.importlib.util, "find_spec", Mock(return_value=object()))
    completed = AsyncMock(return_value=tax_status_by_field)
    monkeypatch.setattr(cms, "_completed_tax_candidate_status", completed)
    monkeypatch.setattr(cms.source, "acquire_release", Mock(side_effect=AssertionError("no acquisition")))
    monkeypatch.setattr(cms, "_prepare_current_serving_candidate", AsyncMock(return_value=descriptor_by_field))
    admission_result = await cms.run({"context": {}}, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    assert admission_result["tax_candidates"] == tax_status_by_field
    assert admission_result["cms_npd_check"]["outcome"] == "unchanged_current_publication"
    completed.assert_awaited_once()


@pytest.mark.asyncio
async def test_tax_completion_probe_requires_exact_complete_result(monkeypatch):
    module = ModuleType("process.cms_npd_tax_candidate_followup")
    completed = AsyncMock(return_value={"status": "complete", "retryable": False})
    module.completed_cms_tax_candidate_report = completed
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(cms.source, "retained_release_directory", lambda root, vector: Path("/synthetic/release"))
    observation = _observation()
    assert await cms._completed_tax_candidate_status(
        observation, {"dataset_id": "dataset-synthetic"}, Path("/synthetic/artifacts")
    ) == {"status": "complete", "retryable": False}
    completed.assert_awaited_once_with(
        fhir,
        release_directory=Path("/synthetic/release"),
        dataset_id="dataset-synthetic",
        vector_sha256=observation.vector_sha256,
        generated_at=observation.manifest.generated_at,
    )
    completed.return_value = {"status": "unavailable", "retryable": True}
    assert (
        await cms._completed_tax_candidate_status(
            observation, {"dataset_id": "dataset-synthetic"}, Path("/synthetic/artifacts")
        )
        is None
    )
    completed.side_effect = ValueError("invalid report")
    assert (
        await cms._completed_tax_candidate_status(
            observation, {"dataset_id": "dataset-synthetic"}, Path("/synthetic/artifacts")
        )
        is None
    )


def test_cms_source_is_selectable_for_proof_bound_profile_publication():
    entry = next(item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == "cms-npd")
    assert entry["source_ids"] == ["cms-npd"]
    assert entry["runnable"] is True
    assert entry["profile_enabled"] is True


def test_cms_artifact_root_rejects_var_tmp(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "/var/tmp")
    with pytest.raises(ValueError, match="cms_npd_artifact_root_unsafe"):
        cms.durable_artifact_root()
    for temporary_root in ("/dev/shm", "/run"):
        monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", temporary_root)
        with pytest.raises(ValueError, match="cms_npd_artifact_root_(unsafe|unavailable)"):
            cms.durable_artifact_root()


def test_rollback_controls_are_available_on_provider_directory_importer():
    registry_entry = next(item for item in importer_registry() if item["name"] == "provider-directory-fhir")
    parameter_names = {parameter["name"] for parameter in registry_entry["params_schema"]}
    assert {"cms_npd_rollback_vector_sha256", "cms_npd_rollback_root_run_id"} <= parameter_names


def test_profile_contract_allows_only_exact_reviewed_cms_exception():
    source_spec_by_field = {
        "schema_version": 1,
        "source_ids": ["cms-npd"],
        "entry_ids": ["cms-npd"],
        "retained_entry_ids": [],
        "dataset_scoped_entry_ids": [],
        "dataset_scoped_variant_groups": [],
        "authority_ids_by_source_id": {},
        "dataset_scoped_endpoint_ids_by_source_id": {},
        "verification_matrix": {"sources": []},
    }
    assert validated_profile_source_spec(source_spec_by_field) == source_spec_by_field
    source_spec_by_field["source_ids"] = ["cms-npd-other"]
    with pytest.raises(RuntimeError, match="source_spec_invalid"):
        validated_profile_source_spec(source_spec_by_field)


def test_complete_scope_and_exact_release_vector_are_required():
    task_by_field = {"import_resources": True, "full_refresh": True, "source_ids": ["cms-npd"]}
    cms.validate_task(task_by_field, "run-synthetic")
    for parameter_override_by_field in ({"resource_limit": 1}, {"test_mode": True}, {"publish_artifacts": True}):
        with pytest.raises(ValueError, match="cms_npd_import_parameters_invalid"):
            cms.validate_task({**task_by_field, **parameter_override_by_field}, "run-synthetic")
    with pytest.raises(ValueError, match="cms_npd_complete_import_required"):
        cms.validate_task({**task_by_field, "full_refresh": False}, "run-synthetic")
    identity = cms.release_identity(_receipt())
    assert set(identity["files"]) == {name for name, _ in RESOURCE_FILES}
    assert all("etag" not in file_by_field for file_by_field in identity["files"].values())
    incomplete = _receipt()
    incomplete["files"].pop("08-OrganizationAffiliation.ndjson")
    with pytest.raises(CmsNpdSourceError, match="receipt_invalid"):
        cms.release_identity(incomplete)


@pytest.mark.asyncio
async def test_empty_witness_group_is_zero_but_unknown_group_is_rejected():
    """A complete eight-file receipt may contain one empty resource family."""

    identity = cms.release_identity(_receipt())
    identity["files"]["03-Endpoint.ndjson"]["distinct_count"] = 0
    projected_counts = [(resource_type, 1, 1) for resource_type in cms.RESOURCE_TYPES if resource_type != "Endpoint"]
    database = SimpleNamespace(all=AsyncMock(return_value=projected_counts))
    fake_fhir = SimpleNamespace(
        _schema=lambda: "mrf",
        _qt=lambda schema, name: f"{schema}.{name}",
        ProviderDirectoryDatasetResource=SimpleNamespace(__tablename__="provider_directory_dataset_resource"),
        db=database,
    )
    candidate = SimpleNamespace(dataset_id="dataset-synthetic")
    await cms._assert_witness_counts(fake_fhir, candidate, identity)
    database.all.return_value = projected_counts + [("UnknownResource", 1, 1)]
    with pytest.raises(RuntimeError, match="cms_npd_witness_counts_incomplete"):
        await cms._assert_witness_counts(fake_fhir, candidate, identity)


@pytest.mark.asyncio
async def test_witness_replay_reads_hashes_without_raw_payload(monkeypatch):
    """A changed raw payload fails replay without fetching large source JSON."""

    witness_by_field = {
        "dataset_id": "dataset-synthetic",
        "source_id": "cms-npd",
        "release_id": "a" * 64,
        "resource_type": "Location",
        "resource_id": "site-1",
        "raw_payload_sha256": "b" * 64,
        "normalized_payload_hash": "c" * 64,
        "raw_payload_json": {"resourceType": "Location", "id": "site-1", "text": "x" * 100_000},
    }
    stored_by_field = {
        field: field_value for field, field_value in witness_by_field.items() if field != "raw_payload_json"
    }
    query_response = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [stored_by_field]))
    copy_rows = AsyncMock()
    monkeypatch.setattr(fhir, "_copy_upsert_rows", copy_rows)
    session = SimpleNamespace(execute=AsyncMock(return_value=query_response))
    await cms._insert_verified_witnesses(session, {witness_by_field["resource_id"]: witness_by_field})
    assert copy_rows.call_args.kwargs == {
        "skip_unchanged": False,
        "transaction_session": session,
        "ignore_conflicts": True,
    }
    selected_columns = set(session.execute.call_args_list[0].args[0].selected_columns.keys())
    assert selected_columns == set(stored_by_field)
    witness_by_field["raw_payload_sha256"] = "d" * 64
    with pytest.raises(RuntimeError, match="cms_npd_witness_payload_conflict"):
        await cms._insert_verified_witnesses(session, {witness_by_field["resource_id"]: witness_by_field})


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_payload", [{"value": "bad\x00value"}, {"bad\x00key": "value"}])
async def test_raw_witness_copy_rejects_lossy_normalization(monkeypatch, raw_payload):
    """Never store sanitized JSON under the original source hash."""
    copy_rows = AsyncMock()
    monkeypatch.setattr(fhir, "_copy_upsert_rows", copy_rows)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(CmsNpdSourceError, match="cms_npd_resource_invalid"):
        await cms._insert_verified_witnesses(session, {"site-1": {"raw_payload_json": raw_payload}})
    copy_rows.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ignore_conflicts", [False, True])
async def test_copy_conflict_policy_is_explicit_and_transaction_bound(monkeypatch, ignore_conflicts):
    """Preserve ordinary update behavior while immutable witnesses opt into DO NOTHING."""
    session = object()
    connection = SimpleNamespace(
        status=AsyncMock(),
        raw_connection=SimpleNamespace(driver_connection=SimpleNamespace(copy_records_to_table=AsyncMock())),
    )

    @asynccontextmanager
    async def bound_connection(transaction_session):
        assert transaction_session is session
        yield connection

    monkeypatch.setattr(fhir, "_copy_upsert_connection", bound_connection)
    options = {"ignore_conflicts": True} if ignore_conflicts else {}
    await fhir._copy_upsert_rows(
        cms.ProviderDirectoryCMSNPDResourceWitness,
        [{"dataset_id": "synthetic", "resource_id": "site-1", "resource_type": "Location", "raw_payload_json": {}}],
        ["dataset_id", "resource_type", "resource_id", "raw_payload_json"],
        ["dataset_id", "resource_type", "resource_id"],
        skip_unchanged=False,
        transaction_session=session,
        **options,
    )
    sql = connection.status.call_args.args[0]
    assert ("DO NOTHING" in sql) is ignore_conflicts
    assert ("DO UPDATE SET" in sql) is not ignore_conflicts


@pytest.mark.asyncio
async def test_rollback_parameters_require_exact_cms_source_before_database_access(monkeypatch):
    database_setup = AsyncMock()
    monkeypatch.setattr(fhir, "ensure_database", database_setup)
    with pytest.raises(ValueError, match="cms_npd_rollback_requires_exclusive_source_scope"):
        await fhir.process_provider_directory_fhir_data(
            {"context": {}},
            {"source_ids": ["other-source"], "cms_npd_rollback_vector_sha256": "b" * 64},
        )
    database_setup.assert_not_awaited()


@pytest.mark.asyncio
async def test_inherited_test_mode_is_rejected_before_acquisition(monkeypatch):
    root = Mock(side_effect=AssertionError("artifact access preceded parameter validation"))
    monkeypatch.setattr(cms, "durable_artifact_root", root)
    with pytest.raises(ValueError, match="cms_npd_import_parameters_invalid"):
        await cms.run(
            {"context": {"test_mode": True}},
            {"import_resources": True, "full_refresh": True},
            "run-synthetic",
        )
    root.assert_not_called()


@pytest.mark.asyncio
async def test_followup_only_reaches_existing_source_local_handler(monkeypatch):
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_source_local_dataset_followup_if_current",
        AsyncMock(
            return_value={"status": "required"},
        ),
    )
    context_by_field = {"context": {"provider_directory_tables_ready": True}}
    result = await fhir.process_provider_directory_fhir_data(
        context_by_field,
        {"source_ids": ["cms-npd"], "dataset_followup_only": True},
    )
    assert result["dataset_followup_only"] is True
    assert result["source_ids"] == ["cms-npd"]


def test_candidate_metadata_keeps_exact_source_release_through_validation():
    identity = cms.release_identity(_receipt())
    candidate = fhir.EndpointDatasetCandidate(
        endpoint_id="e" * 64,
        dataset_id="dataset-synthetic",
        acquisition_root_run_id="run-synthetic",
        source_ids=("cms-npd",),
        selected_resources=tuple(sorted(cms.RESOURCE_SET)),
        import_run_id="run-synthetic",
        previous_dataset_id=None,
        source_release=identity,
    )
    assert fhir._endpoint_dataset_candidate_metadata(candidate)["source_release"] == identity
    assert fhir._endpoint_dataset_publication_metadata(candidate, {})["source_release"] == identity


def test_cms_organization_pseudo_tax_identifier_is_not_promoted_to_tin():
    candidate = SimpleNamespace(
        semantic_projection_as_of="2026-09-24",
        acquisition_root_run_id="run-synthetic",
    )
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization-synthetic",
        "name": "Example Clinic",
        "identifier": [{"system": "urn:cms:npd:pseudo-ein", "value": "00-0000000"}],
    }
    model, row_by_field = cms._parse_batch_row(fhir, resource_by_field, candidate)
    assert model is fhir.ProviderDirectoryOrganization
    assert row_by_field["tax_id"] is None
    assert resource_by_field["identifier"][0]["value"] in str(row_by_field)


def test_cms_npi_uses_source_parser_without_overwriting_valid_identifier():
    candidate = SimpleNamespace(
        semantic_projection_as_of="2026-09-24",
        acquisition_root_run_id="run-synthetic",
    )
    resource_by_field = {"resourceType": "Practitioner", "id": "1234567893"}
    _, row = cms._parse_batch_row(fhir, resource_by_field, candidate)
    assert row["npi"] is None
    resource_by_field["identifier"] = [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"},
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
    ]
    _, row = cms._parse_batch_row(fhir, resource_by_field, candidate)
    assert row["npi"] == 1234567893

    resource_by_field["identifier"] = []
    _, row = cms._parse_batch_row(fhir, resource_by_field, candidate)
    assert row["npi"] is None


@pytest.mark.asyncio
async def test_identity_batches_delegate_complete_source_facts_to_bulk_writer():
    """Native extraction and set validation receive every bounded source row."""
    from sqlalchemy.ext.asyncio import AsyncSession

    writer_calls = []

    @asynccontextmanager
    async def session():
        async with AsyncSession() as writer_session:
            yield writer_session

    async def capture(kind, _session, **kwargs):
        assert kind != "network" or kwargs["resource_type"] != "InsurancePlan" or _session.in_transaction()
        writer_calls.append((kind, kwargs))

    fake_fhir = SimpleNamespace(
        db=SimpleNamespace(session=session, all=AsyncMock(return_value=[("network-1",)])),
        _schema=lambda: "mrf",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        _insurance_plan_network_references=lambda resource: [
            entry["reference"] for entry in resource.get("network", [])
        ],
    )
    entity_writer = SimpleNamespace(bind_entity_batch=partial(capture, "entity"))
    network_writer = SimpleNamespace(record_insurance_network_batch=partial(capture, "network"))
    resource_writer = SimpleNamespace(bind_resource_identity_batch=partial(capture, "resource"))
    identity = cms.release_identity(_receipt())
    organization_resources = [
        {"resourceType": "Organization", "id": "network-1"},
        {"resourceType": "Organization", "id": "network-2", "name": "Network", "type": [{"text": "ntwk"}]},
        {"resourceType": "Organization", "id": "insurer-1", "name": "Network"},
    ]
    plan_resources = [
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "network": [
                {"reference": "Organization/network-1"},
                {"reference": "Organization/unresolved"},
                {"reference": "https://example.test/Organization/other"},
            ],
        }
    ]
    for resource_type, resources in (
        ("Organization", organization_resources),
        ("InsurancePlan", plan_resources),
        ("PractitionerRole", [{"resourceType": "PractitionerRole", "id": "role-1"}]),
    ):
        await cms._write_identity_batch(
            fake_fhir, entity_writer, network_writer, resource_writer, resource_type, resources, identity
        )
    assert [kind for kind, _ in writer_calls] == ["entity", "network", "resource", "network", "resource"]
    assert writer_calls[0][1]["resources"] == organization_resources
    assert writer_calls[1][1]["resources"] == organization_resources
    assert writer_calls[1][1]["resource_type"] == "Organization"
    assert writer_calls[2][1]["resource_ids"] == ["plan-1"]
    assert writer_calls[3][1]["resources"] == plan_resources
    assert writer_calls[3][1]["resource_type"] == "InsurancePlan"
    assert writer_calls[4][1]["resource_ids"] == ["role-1"]
    fake_fhir.db.all.assert_not_awaited()


@pytest.mark.asyncio
async def test_identity_batch_uses_one_thousand_row_writer_call():
    sessions = []
    sizes = []
    network_sizes = []

    @asynccontextmanager
    async def session():
        sessions.append(object())
        yield sessions[-1]

    async def bind(_session, **kwargs):
        sizes.append(len(kwargs["resources"]))

    async def bind_network(_session, **kwargs):
        assert _session is sessions[-1]
        network_sizes.append(len(kwargs["resources"]))

    resources = [{"resourceType": "Organization", "id": f"org-{index}"} for index in range(1_000)]
    await cms._write_identity_batch(
        SimpleNamespace(db=SimpleNamespace(session=session)),
        SimpleNamespace(bind_entity_batch=bind),
        SimpleNamespace(record_insurance_network_batch=bind_network),
        None,
        "Organization",
        resources,
        cms.release_identity(_receipt()),
    )
    assert sizes == [1_000]
    assert network_sizes == [1_000]
    assert len(sessions) == 1


def test_parsed_plan_preserves_unresolved_network_reference():
    _, plan_row_by_field = cms._parse_batch_row(
        fhir,
        {
            "resourceType": "InsurancePlan",
            "id": "plan-1",
            "network": [
                {"reference": "Organization/unresolved"},
            ],
        },
        SimpleNamespace(semantic_projection_as_of="2026-09-24", acquisition_root_run_id="run-synthetic"),
    )
    assert plan_row_by_field["network_refs"] == ["Organization/unresolved"]


@pytest.mark.asyncio
async def test_missing_network_evidence_rejects_complete_entity_counts():
    fake_fhir = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(side_effect=[1, 1, 1, 1, 1, 0])),
        _schema=lambda: "mrf",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        ProviderDirectoryDatasetResource=SimpleNamespace(__tablename__="provider_directory_dataset_resource"),
    )
    with pytest.raises(RuntimeError, match="identity_evidence_incomplete"):
        await cms._assert_identity_evidence(
            fake_fhir, SimpleNamespace(dataset_id="candidate"), cms.release_identity(_receipt())
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("validated,published", ((True, False), (False, True)))
async def test_finalized_replay_checks_identity_without_rewriting_entire_release(
    monkeypatch, tmp_path: Path, validated: bool, published: bool
):
    check = AsyncMock()
    backfill = AsyncMock()
    refresh = AsyncMock()
    monkeypatch.setattr(cms, "_assert_identity_evidence", check)
    monkeypatch.setattr(cms, "_backfill_network_roles", backfill)
    monkeypatch.setattr(cms, "_refresh_identity_statistics", refresh)
    candidate = SimpleNamespace(dataset_id="finalized", already_validated=validated, already_published=published)
    identity = cms.release_identity(_receipt())
    fake_fhir = object()
    ctx_by_field = {}
    await cms._materialize_identity_evidence(fake_fhir, tmp_path, candidate, identity, ctx_by_field, {})
    backfill.assert_awaited_once_with(fake_fhir, identity, ctx_by_field, {})
    check.assert_awaited_once_with(fake_fhir, candidate, identity)
    refresh.assert_not_awaited()
    assert ctx_by_field["context"]["audit"]["cms_intake"]["phase"] == "identity_validation"


def _identity_statistics_fixture(monkeypatch, directory: Path, failure=None):
    _, compressed_by_file = _source()
    for name, compressed in compressed_by_file.items():
        (directory / f"{name}.zst").write_bytes(compressed)
    events = []
    refresh_calls = count(1)

    def refresh_table(statement):
        events.append(("analyze", statement))
        if next(refresh_calls) == 2 and failure is not None:
            raise failure

    check = AsyncMock(side_effect=lambda *_args: events.append(("validate", None)))
    monkeypatch.setattr(cms, "_assert_identity_evidence", check)
    monkeypatch.setattr(
        cms, "_write_identity_batch", AsyncMock(side_effect=lambda *args: events.append(("write", args[4])))
    )
    fake_fhir = SimpleNamespace(
        db=SimpleNamespace(status=AsyncMock(side_effect=refresh_table)),
        _schema=lambda: "synthetic",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        _raise_if_resource_import_cancelled=AsyncMock(side_effect=lambda *_args: events.append(("cancel", None))),
    )
    return SimpleNamespace(
        fhir=fake_fhir,
        candidate=SimpleNamespace(dataset_id="synthetic", already_validated=False, already_published=False),
        identity=cms.release_identity(_receipt()),
        check=check,
        events=events,
    )


@pytest.mark.asyncio
async def test_fresh_identity_refreshes_before_validation(monkeypatch, tmp_path: Path):
    fixture = _identity_statistics_fixture(monkeypatch, tmp_path)
    await cms._materialize_identity_evidence(fixture.fhir, tmp_path, fixture.candidate, fixture.identity, {}, {})
    assert fixture.events == [
        ("write", "Organization"),
        ("write", "Location"),
        ("write", "InsurancePlan"),
        ("write", "PractitionerRole"),
        ("cancel", None),
        ("analyze", 'ANALYZE "synthetic"."provider_directory_resource_identity";'),
        ("cancel", None),
        ("analyze", 'ANALYZE "synthetic"."provider_directory_entity_source_binding";'),
        ("cancel", None),
        ("analyze", 'ANALYZE "synthetic"."provider_directory_entity_release_evidence";'),
        ("cancel", None),
        ("validate", None),
    ]
    fixture.check.assert_awaited_once_with(fixture.fhir, fixture.candidate, fixture.identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_cancelled", (False, True))
async def test_identity_refresh_failure_preserves_writes(monkeypatch, tmp_path: Path, is_cancelled: bool):
    failure = asyncio.CancelledError() if is_cancelled else RuntimeError("synthetic_refresh_failure")
    fixture = _identity_statistics_fixture(monkeypatch, tmp_path, failure)
    with pytest.raises(type(failure)) as caught:
        await cms._materialize_identity_evidence(fixture.fhir, tmp_path, fixture.candidate, fixture.identity, {}, {})
    assert caught.value is failure
    assert fixture.events == [
        ("write", "Organization"),
        ("write", "Location"),
        ("write", "InsurancePlan"),
        ("write", "PractitionerRole"),
        ("cancel", None),
        ("analyze", 'ANALYZE "synthetic"."provider_directory_resource_identity";'),
        ("cancel", None),
        ("analyze", 'ANALYZE "synthetic"."provider_directory_entity_source_binding";'),
    ]
    assert fixture.fhir.db.status.await_count == 2
    fixture.check.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_identity_evidence_blocks_validation_and_publication(monkeypatch, tmp_path: Path):
    candidate = SimpleNamespace(
        dataset_id="dataset-synthetic",
        already_validated=False,
        already_published=False,
    )
    monkeypatch.setattr(cms, "_register_source", AsyncMock(return_value="endpoint-synthetic"))
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", AsyncMock(return_value=None))
    monkeypatch.setattr(cms, "_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(cms, "_stream_file", AsyncMock(return_value=1))
    monkeypatch.setattr(cms, "_assert_counts", AsyncMock(return_value={name: 1 for _, name in RESOURCE_FILES}))
    monkeypatch.setattr(cms, "_assert_witness_counts", AsyncMock())
    monkeypatch.setattr(cms.source, "verify_release", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cms.source, "verify_retained_release", lambda *_args: None)
    monkeypatch.setattr(
        cms,
        "_materialize_identity_evidence",
        AsyncMock(
            side_effect=RuntimeError("cms_npd_identity_evidence_incomplete"),
        ),
    )
    finalizer = AsyncMock()
    publisher = AsyncMock()
    monkeypatch.setattr(fhir, "_finalize_endpoint_dataset_candidate", finalizer)
    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        publisher,
    )
    with pytest.raises(RuntimeError, match="identity_evidence_incomplete"):
        await cms._run_acquired(
            {"context": {}},
            {},
            "run-synthetic",
            tmp_path,
            _receipt(),
            object(),
        )
    finalizer.assert_not_awaited()
    publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_registration_keeps_configured_endpoint_binding():
    upsert = AsyncMock()
    fake_fhir = SimpleNamespace(
        _admit_provider_directory_endpoint_components=lambda **_kwargs: {"endpoint_id": "endpoint-synthetic"},
        _now=lambda: "2026-09-29T00:00:00Z",
        _upsert_rows=upsert,
        ProviderDirectoryAPIEndpoint=object(),
        ProviderDirectorySource=object(),
        PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY="provider_directory_configured_endpoint_id",
    )
    assert await cms._register_source(fake_fhir) == "endpoint-synthetic"
    source_row = upsert.await_args_list[1].args[1][0]
    assert source_row["endpoint_id"] == "endpoint-synthetic"
    assert source_row["metadata_json"]["provider_directory_configured_endpoint_id"] == "endpoint-synthetic"


@pytest.mark.asyncio
async def test_generic_unscoped_publication_rejects_cms_candidate_before_artifacts(monkeypatch):
    fence = SimpleNamespace(promotion_datasets=[SimpleNamespace(source_id="cms-npd")])
    monkeypatch.setattr(fhir, "_resolve_provider_directory_artifact_datasets", AsyncMock(return_value=fence))
    relation_builder = AsyncMock()
    monkeypatch.setattr(fhir, "_publish_current_dataset_relation_artifacts", relation_builder)
    with pytest.raises(RuntimeError, match="cms_npd_verified_publication_required"):
        await fhir._prepare_artifact_publication_fence(
            None,
            run_id="run-synthetic",
            metrics={},
            publish_artifacts_targets=None,
            publish_corroboration=False,
            should_select_validated_candidates=True,
        )
    relation_builder.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_ids,source_id,publish_targets,is_rejected",
    [
        (None, "cms-npd", None, True),
        (["cms-npd"], "cms-npd", {"location_archive"}, True),
        (None, "cms-npd", {"profile"}, False),
        (None, "source-other", None, False),
        (["source-other"], "source-other", {"location_archive"}, False),
    ],
)
async def test_generic_current_cms_archive_requires_admitted_path_before_preparation(
    monkeypatch, source_ids, source_id, publish_targets, is_rejected
):
    dataset = fhir.ProviderDirectoryArtifactDataset(
        source_id, "endpoint-synthetic", "dataset-synthetic", "run-synthetic"
    )
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    monkeypatch.setattr(fhir, "_resolve_provider_directory_artifact_datasets", AsyncMock(return_value=fence))
    dependency_check = Mock()
    relation_builder = AsyncMock()
    bundle_builder = AsyncMock(return_value={"published": True})
    monkeypatch.setattr(fhir, "_assert_provider_directory_artifact_target_dependencies", dependency_check)
    monkeypatch.setattr(fhir, "_publish_current_dataset_relation_artifacts", relation_builder)
    monkeypatch.setattr(fhir, "_publish_artifact_bundle_from_fence", bundle_builder)
    with (
        pytest.raises(RuntimeError, match="cms_archive_admitted_publication_required") if is_rejected else nullcontext()
    ):
        await fhir._publish_provider_directory_dataset_artifacts(
            run_id="run-synthetic",
            metrics={},
            source_ids=source_ids,
            publish_corroboration=False,
            publish_artifacts_targets=publish_targets,
        )
    if is_rejected:
        dependency_check.assert_not_called()
        relation_builder.assert_not_awaited()
        bundle_builder.assert_not_awaited()
    else:
        dependency_check.assert_called_once()
        relation_builder.assert_awaited_once()
        bundle_builder.assert_awaited_once()


@pytest.mark.asyncio
async def test_rollback_requires_previous_superseded_publication():
    identity = cms.release_identity(_receipt())
    state_by_field = {
        "publication_metadata_json": {"source_release": identity},
        "status": fhir.ENDPOINT_DATASET_SUPERSEDED,
        "is_current": False,
    }
    fake_fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=("reacquired-dataset",))),
        _qt=lambda _schema, table: f"mrf.{table}",
        _schema=lambda: "mrf",
        _endpoint_dataset_state=AsyncMock(return_value=state_by_field),
        ENDPOINT_DATASET_SUPERSEDED=fhir.ENDPOINT_DATASET_SUPERSEDED,
    )
    await cms._assert_rollback_predecessor(fake_fhir, "endpoint-synthetic", identity)
    fake_fhir._endpoint_dataset_state.assert_awaited_with("reacquired-dataset")
    fake_fhir.db.first.return_value = None
    with pytest.raises(RuntimeError, match="prior_publication_missing"):
        await cms._assert_rollback_predecessor(fake_fhir, "endpoint-synthetic", identity)


@pytest.mark.asyncio
async def test_stream_flushes_on_decoded_byte_bound(monkeypatch, tmp_path: Path):
    path = tmp_path / "organization.ndjson.zst"
    with zstd.open(path, "wb") as encoded:
        for index in range(3):
            encoded.write(f'{{"resourceType":"Organization","id":"org-{index}"}}\n'.encode())
    model = object()
    persist = AsyncMock()
    fake_fhir = SimpleNamespace(
        RESOURCE_MODELS_BY_TYPE={"Organization": model},
        _raise_if_resource_import_cancelled=AsyncMock(),
    )
    monkeypatch.setattr(cms, "_persist_source_batch", persist)
    monkeypatch.setattr(cms, "BATCH_MAX_DECODED_BYTES", 1)
    monkeypatch.setattr(cms, "_parse_batch_row", lambda *_args: (model, {"id": "synthetic"}))
    candidate = SimpleNamespace(
        dataset_id="dataset-synthetic",
        resource_hash_contract="synthetic",
        semantic_projection_as_of="2026-09-24",
    )
    assert await cms._stream_file(fake_fhir, path, candidate, "Organization", {}, {}) == 3
    assert persist.await_count == 3

    failure = RuntimeError("synthetic-write-failed")
    persist.reset_mock()
    persist.side_effect = [None, failure]
    ctx_by_field = {"context": {}}
    with pytest.raises(RuntimeError) as caught:
        await cms._stream_file(fake_fhir, path, candidate, "Organization", ctx_by_field, {})
    assert caught.value is failure and persist.await_count == 2
    observed = ctx_by_field["context"]["audit"]["cms_intake"]
    assert observed["family"] == "Organization" and observed["completed_input_rows"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["cms_npd_source_vector_changed", "cms_npd_retained_file_missing"])
@pytest.mark.parametrize("failed_check_index", [1, 2])
async def test_retained_verification_failure_never_reaches_validation(
    monkeypatch, tmp_path: Path, error_code: str, failed_check_index: int
):
    identity = cms.release_identity(_receipt())
    candidate = SimpleNamespace(
        dataset_id="dataset-synthetic",
        already_validated=False,
        already_published=False,
    )
    monkeypatch.setattr(cms, "_register_source", AsyncMock(return_value="endpoint-synthetic"))
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", AsyncMock(return_value=None))
    monkeypatch.setattr(cms, "_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(cms, "_stream_file", AsyncMock(return_value=1))
    monkeypatch.setattr(cms, "_assert_counts", AsyncMock(return_value={name: 1 for _, name in RESOURCE_FILES}))
    monkeypatch.setattr(cms, "_assert_witness_counts", AsyncMock())
    monkeypatch.setattr(cms, "_materialize_identity_evidence", AsyncMock())
    monkeypatch.setattr(cms.relationships, "materialize", AsyncMock())
    finalizer = AsyncMock(return_value={"validated": True})
    publisher = AsyncMock()
    monkeypatch.setattr(fhir, "_finalize_endpoint_dataset_candidate", finalizer)
    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        publisher,
    )

    monkeypatch.setattr(cms.source, "verify_release", Mock())
    local_checks = count(1)

    def reject_local_release(*_args):
        if next(local_checks) == failed_check_index:
            raise CmsNpdSourceError(error_code)

    monkeypatch.setattr(cms.source, "verify_retained_release", reject_local_release)
    dispose = AsyncMock()
    monkeypatch.setattr(cms.recovery, "dispose_changed_vector", dispose)
    with pytest.raises(CmsNpdSourceError, match=error_code):
        await cms._run_acquired(
            {"context": {}},
            {},
            "run-synthetic",
            tmp_path,
            _receipt(),
            object(),
        )
    assert cms.release_identity(_receipt()) == identity
    finalizer.assert_not_awaited()
    publisher.assert_not_awaited()
    dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_retained_failure_preserves_published_candidate(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        cms.source,
        "verify_retained_release",
        Mock(side_effect=CmsNpdSourceError("cms_npd_retained_file_missing")),
    )
    dispose = AsyncMock()
    monkeypatch.setattr(cms.recovery, "dispose_changed_vector", dispose)
    with pytest.raises(CmsNpdSourceError, match="retained_file_missing"):
        await cms._verify_or_dispose(object(), SimpleNamespace(already_published=True), {}, tmp_path, {}, None)
    dispose.assert_not_awaited()


@pytest.mark.asyncio
async def test_replaced_release_after_validation_never_reaches_cutover(monkeypatch, tmp_path: Path):
    candidate = SimpleNamespace(
        dataset_id="dataset-synthetic",
        already_validated=False,
        already_published=False,
    )
    monkeypatch.setattr(cms, "_register_source", AsyncMock(return_value="endpoint-synthetic"))
    identity = cms.release_identity(_receipt())
    validated_state_by_field = {"publication_metadata_json": {"source_release": identity}, "dataset_hash": "a" * 64}
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", AsyncMock(side_effect=[None, validated_state_by_field]))
    monkeypatch.setattr(cms, "_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(cms, "_stream_file", AsyncMock(return_value=1))
    monkeypatch.setattr(cms, "_assert_counts", AsyncMock(return_value={name: 1 for _, name in RESOURCE_FILES}))
    monkeypatch.setattr(cms, "_assert_witness_counts", AsyncMock())
    monkeypatch.setattr(cms, "_materialize_identity_evidence", AsyncMock())
    monkeypatch.setattr(cms.relationships, "materialize", AsyncMock())
    finalizer = AsyncMock(return_value={"validated": True})

    async def run_preflight(*_args, **kwargs):
        await kwargs["before_cutover"](object())

    publisher = AsyncMock(side_effect=run_preflight)
    monkeypatch.setattr(fhir, "_finalize_endpoint_dataset_candidate", finalizer)
    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        publisher,
    )
    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", AsyncMock())
    verification_phases = []

    def reject_changed_vector(*_args, **_kwargs):
        verification_phases.append("full")
        if verification_phases.count("full") == 2:
            raise CmsNpdSourceError("cms_npd_source_vector_changed")

    monkeypatch.setattr(cms.source, "verify_release", reject_changed_vector)
    monkeypatch.setattr(cms.source, "verify_retained_release", lambda *_args: verification_phases.append("local"))
    dispose = AsyncMock()
    monkeypatch.setattr(cms.recovery, "dispose_changed_vector", dispose)
    with pytest.raises(CmsNpdSourceError, match="source_vector_changed"):
        await cms._run_acquired(
            {"context": {}},
            {},
            "run-synthetic",
            tmp_path,
            _receipt(),
            object(),
        )
    assert verification_phases == ["full", "local", "local", "full"]
    finalizer.assert_awaited_once()
    publisher.assert_not_awaited()
    dispose.assert_awaited_once()


def _stub_rollback_publication(monkeypatch, identity):
    """Provide a validated candidate while keeping retained-byte verification observable."""
    candidate = SimpleNamespace(
        dataset_id="rollback-dataset",
        endpoint_id="endpoint-synthetic",
        acquisition_root_run_id="run-synthetic",
        already_validated=False,
        already_published=False,
    )
    candidate_factory = AsyncMock(return_value=candidate)
    monkeypatch.setattr(cms, "_register_source", AsyncMock(return_value="endpoint-synthetic"))
    monkeypatch.setattr(cms, "_assert_rollback_predecessor", AsyncMock())
    monkeypatch.setattr(fhir, "_current_endpoint_dataset_id", AsyncMock(return_value=None))
    monkeypatch.setattr(cms.recovery, "reusable_vector_candidate", AsyncMock(return_value=None))
    monkeypatch.setattr(cms, "_candidate", candidate_factory)
    monkeypatch.setattr(cms, "_stream_file", AsyncMock(return_value=1))
    monkeypatch.setattr(cms, "_assert_counts", AsyncMock(return_value={name: 1 for _, name in RESOURCE_FILES}))
    monkeypatch.setattr(cms, "_assert_witness_counts", AsyncMock())
    monkeypatch.setattr(cms, "_materialize_identity_evidence", AsyncMock())
    monkeypatch.setattr(cms.relationships, "materialize", AsyncMock())
    local_rechecks = []
    monkeypatch.setattr(cms.source, "verify_retained_release", lambda *_args: local_rechecks.append(True))
    upstream_verify = Mock(side_effect=AssertionError("retained rollback reached upstream verification"))
    monkeypatch.setattr(cms.source, "verify_release", upstream_verify)
    monkeypatch.setattr(fhir, "_finalize_endpoint_dataset_candidate", AsyncMock(return_value={"validated": True}))
    _stub_validated_rollback_state(monkeypatch, identity)
    tax_followup = AsyncMock(return_value={"status": "complete", "retryable": False})
    monkeypatch.setattr(
        importlib.import_module("process.cms_npd_tax_candidate_followup"),
        "cms_npd_tax_candidate_followup",
        tax_followup,
    )
    return candidate_factory, local_rechecks, upstream_verify, tax_followup


def _stub_validated_rollback_state(monkeypatch, identity):
    """Keep complete admitted state and its scalar coverage identity together."""
    validated_state_by_field = {
        "dataset_id": "rollback-dataset",
        "endpoint_id": "endpoint-synthetic",
        "acquisition_root_run_id": "run-synthetic",
        "publication_metadata_json": {"source_release": identity, "source_ids": ["cms-npd"]},
        "dataset_hash": "a" * 64,
        "status": fhir.ENDPOINT_DATASET_VALIDATED,
        "is_current": False,
        "validated_at": "2026-01-01",
        "published_at": None,
        "resource_count": 8,
    }
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", AsyncMock(return_value=validated_state_by_field))
    monkeypatch.setattr(
        fhir, "_source_local_dataset_followup_if_current", AsyncMock(return_value={"status": "required"})
    )
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", AsyncMock())
    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    monkeypatch.setattr(
        coverage,
        "prepare_cms_candidate_coverage",
        AsyncMock(
            return_value={
                "dataset_id": "rollback-dataset",
                "endpoint_id": "endpoint-synthetic",
                "dataset_hash": "a" * 64,
                "release_id": identity["vector_sha256"],
                "proof_version": 2,
            }
        ),
    )


@pytest.mark.asyncio
async def test_new_publication_defers_optional_tax_until_same_byte_replay(monkeypatch, tmp_path: Path):
    receipt_by_field = _receipt()
    identity = cms.release_identity(receipt_by_field)
    _, _, _, tax_followup = _stub_rollback_publication(monkeypatch, identity)

    async def run_preflight(*_args, **kwargs):
        await kwargs["before_cutover"](object())

    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        AsyncMock(side_effect=run_preflight),
    )
    result = await cms._run_acquired(
        {"context": {}},
        {"import_resources": True, "full_refresh": True},
        "run-synthetic",
        tmp_path,
        receipt_by_field,
        None,
    )
    assert "dataset_followup" not in result
    assert result["status"] == "validated"
    assert result["cms_serving_candidate"]["desired_cms_dataset"]["is_current"] is False
    assert result["tax_candidates"] == {"status": "pending", "retryable": True, "retry_via": "same_byte_import"}
    tax_followup.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_replays_fresh_candidate_without_upstream_recheck(monkeypatch, tmp_path: Path):
    """Reopen retained bytes without consulting the upstream CMS endpoint."""
    receipt_by_field = _receipt()
    identity = cms.release_identity(receipt_by_field)
    candidate_factory, local_rechecks, upstream_verify, tax_followup = _stub_rollback_publication(monkeypatch, identity)

    async def run_preflight(*_args, **kwargs):
        await kwargs["before_cutover"](object())

    publisher = AsyncMock(side_effect=run_preflight)
    monkeypatch.setattr(
        importlib.import_module("process.provider_directory_source_local_publication"),
        "publish_validated_source_local_dataset",
        publisher,
    )
    task_by_field = {"cms_npd_rollback_vector_sha256": identity["vector_sha256"]}
    admission_summary = await cms._run_acquired(
        {"context": {}},
        task_by_field,
        "run-synthetic",
        tmp_path,
        receipt_by_field,
        None,
    )
    candidate_key = candidate_factory.await_args.kwargs["candidate_key"]
    assert candidate_key != identity["vector_sha256"]
    assert candidate_key.startswith("cms-npd-rollback:")
    assert len(local_rechecks) == 4
    assert admission_summary["dataset_id"] == "rollback-dataset"
    assert admission_summary["tax_candidates"] == {
        "status": "pending",
        "retryable": True,
        "retry_via": "same_byte_import",
    }
    tax_followup.assert_not_awaited()
    publisher.assert_not_awaited()
    cms.recovery.dispose_prior_vectors.assert_awaited_once_with(fhir, "endpoint-synthetic", identity["vector_sha256"])
    upstream_verify.assert_not_called()


@pytest.mark.asyncio
async def test_rollback_replay_reuses_current_vector_without_original_run_id(monkeypatch):
    identity = cms.release_identity(_receipt())
    candidate_factory, _, _, _ = _stub_rollback_publication(monkeypatch, identity)
    fhir._current_endpoint_dataset_id.return_value = "rollback-dataset"
    fhir._endpoint_dataset_state.return_value.update(status="published", is_current=True, published_at="2026-01-01")
    await cms._admission_candidate(
        fhir,
        "endpoint-synthetic",
        "later-run",
        identity,
        {"cms_npd_rollback_vector_sha256": identity["vector_sha256"]},
    )
    assert candidate_factory.await_args.kwargs["existing_dataset_id"] == "rollback-dataset"
    cms.recovery.reusable_vector_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_retry_only_reuses_candidate_for_current_predecessor(monkeypatch):
    identity = cms.release_identity(_receipt())
    candidate_factory, _, _, _ = _stub_rollback_publication(monkeypatch, identity)
    fhir._current_endpoint_dataset_id.return_value = "different-current-dataset"
    fhir._endpoint_dataset_state.return_value = {
        "status": fhir.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "publication_metadata_json": {"source_release": {"vector_sha256": "different"}},
    }
    await cms._admission_candidate(
        fhir,
        "endpoint-synthetic",
        "later-run",
        identity,
        {"cms_npd_rollback_vector_sha256": identity["vector_sha256"]},
    )
    assert candidate_factory.await_args.kwargs["existing_dataset_id"] is None
    cms.recovery.reusable_vector_candidate.assert_awaited_once_with(
        fhir, "endpoint-synthetic", identity, previous_dataset_id="different-current-dataset"
    )


@pytest.mark.asyncio
async def test_rollback_run_never_opens_an_upstream_client(monkeypatch, tmp_path: Path):
    vector = "b" * 64
    directory = tmp_path / vector
    directory.mkdir()
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: tmp_path)
    monkeypatch.setattr(cms.source, "retained_release_directory", lambda *_args: directory)
    monkeypatch.setattr(cms.source, "_release_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(cms.source, "load_retained_release", lambda *_args: (directory, _receipt()))
    admission = AsyncMock(return_value={"status": "published"})
    monkeypatch.setattr(cms, "_run_acquired", admission)
    monkeypatch.setattr(cms.httpx, "Client", lambda **_kwargs: pytest.fail("upstream client opened"))
    task_by_field = {
        "import_resources": True,
        "full_refresh": True,
        "cms_npd_rollback_vector_sha256": vector,
    }
    result = await cms.run({"context": {}}, task_by_field, "run-synthetic")
    assert result == {"status": "published"}
    assert admission.await_args.args[-1] is None


def _retained_task(directory, receipt, operation="baseline"):
    return {
        "source_ids": ["cms-npd"],
        "import_resources": True,
        "full_refresh": True,
        "cms_npd_retained_operation": operation,
        "cms_npd_retained_vector_sha256": receipt["vector_sha256"],
        "cms_npd_retained_receipt_sha256": hashlib.sha256((directory / "receipt.json").read_bytes()).hexdigest(),
    }


def _dispatch_task(task, generation=0):
    return {
        **task,
        "provider_directory_dispatch_id": "pdd_" + "a" * 32,
        "provider_directory_dispatch_request_id": "11111111-1111-4111-8111-111111111111",
        "provider_directory_dispatch_request_fingerprint": "b" * 64,
        "provider_directory_dispatch_catalog_digest": "c" * 64,
        "provider_directory_dispatch_contract_version": 2,
        "provider_directory_dispatch_generation": generation,
        **({"provider_directory_repair_id": "22222222-2222-4222-8222-222222222222"} if generation else {}),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"provider_directory_dispatch_generation": True},
        {"provider_directory_dispatch_generation": 0},
        {"provider_directory_dispatch_contract_version": True},
        {"provider_directory_dispatch_id": "other"},
        {"provider_directory_dispatch_request_id": None},
        {"provider_directory_repair_id": "invalid"},
        {"provider_directory_dispatch_request_fingerprint": "A" * 64},
        {"cms_npd_retained_operation": "baseline"},
        {"cms_npd_retained_vector_sha256": "a" * 64},
    ],
)
def test_repair_identity_rejects_malformed_or_partial_controls(overrides):
    with pytest.raises(RuntimeError, match="cms_npd_repair_identity_invalid"):
        cms.recovery._dispatch_identity({**_dispatch_task({}, 1), **overrides}, is_repair=True)


def _lineage_run(run_id, parent=None):
    return {
        "run_id": run_id,
        "retry_of_run_id": parent,
        "engine": "healthcare-mrf-api",
        "node_id": "test-node",
        "importer": "provider-directory-fhir",
        "status": "failed",
        "finished_at": "finished",
        "params": {
            "source_ids": ["cms-npd"],
            "import_resources": True,
            "full_refresh": True,
            **({"provider_directory_pagination_root_run_id": "run-0"} if parent else {}),
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", ["missing", "ambiguous", "active", "unfinished", "foreign_node", "foreign_source", "claimed_root"]
)
async def test_repair_lineage_rejects_unproved_retry_chains(monkeypatch, corruption):
    root = _lineage_run("run-0")
    child = _lineage_run("run-1", "run-0")
    child.update(
        {
            "active": {"status": "running", "finished_at": None},
            "unfinished": {"finished_at": None},
            "foreign_node": {"node_id": "another-node"},
        }.get(corruption, {})
    )
    child["params"].update(
        {
            "foreign_source": {"source_ids": ["another-source"]},
            "claimed_root": {"provider_directory_pagination_root_run_id": "run-1"},
        }.get(corruption, {})
    )

    async def records(_fhir, *, run_id=None, parent_id=None):
        if run_id is not None:
            return [] if corruption == "missing" else [root]
        if parent_id == "run-0":
            return [child, _lineage_run("branch", "run-0")] if corruption == "ambiguous" else [child]
        return []

    monkeypatch.setattr(cms.recovery, "_run_records", records)
    with pytest.raises(RuntimeError, match="cms_npd_acquisition_lineage_invalid"):
        await cms.recovery._run_lineage(object(), "run-0")


@pytest.mark.asyncio
async def test_repair_lineage_is_bounded_and_partial_repair_cannot_fall_back(monkeypatch):
    async def records(_fhir, *, run_id=None, parent_id=None):
        if run_id is not None:
            return [_lineage_run(run_id)]
        index = int(parent_id.split("-")[1]) + 1
        return [_lineage_run(f"run-{index}", parent_id)]

    observed = AsyncMock(side_effect=records)
    monkeypatch.setattr(cms.recovery, "_run_records", observed)
    with pytest.raises(RuntimeError, match="cms_npd_acquisition_lineage_invalid"):
        await cms.recovery._run_lineage(object(), "run-0")
    assert observed.await_count == 257
    with pytest.raises(RuntimeError, match="cms_npd_repair_identity_invalid"):
        await cms.recovery.repaired_candidate_selection(
            object(), "endpoint", {}, {"provider_directory_dispatch_generation": 1}, "run-0"
        )


def _sealed_input(root):
    manifest, payloads = _source()
    client, _ = _client(manifest, payloads)
    with client:
        return cms.source.acquire_release(root, client=client)


def test_retained_controls_leave_unselected_defaults_empty():
    registry = next(row for row in importer_registry() if row["name"] == "provider-directory-fhir")
    retained_fields = {
        "cms_npd_retained_operation",
        "cms_npd_retained_vector_sha256",
        "cms_npd_retained_receipt_sha256",
    }
    assert retained_fields <= {row["name"] for row in registry["params_schema"]}
    assert cms.retained_input_selection({}) is None
    assert cms.retained_input_selection(dict.fromkeys(retained_fields)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"cms_npd_retained_operation": None},
        {"cms_npd_retained_operation": "replace"},
        {"cms_npd_retained_vector_sha256": None},
        {"cms_npd_retained_receipt_sha256": "A" * 64},
        {"cms_npd_retained_receipt_sha256": False},
        {"cms_npd_rollback_vector_sha256": "b" * 64},
        {"cms_npd_rollback_root_run_id": "root"},
        {"provider_directory_pagination_root_run_id": " root "},
        {"source_ids": ["other-source"]},
        {"source_ids": ["cms-npd", "other-source"]},
        {"resource_limit": 1},
        {"page_limit": 1},
        {"source_query": "bounded"},
        {"full_refresh": False},
        {"test": True},
        {"dataset_rehydrate_only": True},
        {"dataset_followup_only": True},
        {"publish_artifacts_only": True},
        {"canonical_backfill_only": True},
    ],
)
async def test_invalid_retained_selector_rejects_before_database(monkeypatch, overrides):
    database = AsyncMock(side_effect=AssertionError("invalid retained selection reached database"))
    monkeypatch.setattr(fhir, "ensure_database", database)
    task_by_field = {
        "source_ids": ["cms-npd"],
        "run_id": "run-synthetic",
        "import_resources": True,
        "full_refresh": True,
        "cms_npd_retained_operation": "baseline",
        "cms_npd_retained_vector_sha256": "a" * 64,
        "cms_npd_retained_receipt_sha256": "b" * 64,
    }
    with pytest.raises(ValueError):
        await fhir.process_provider_directory_fhir_data({"context": {}}, {**task_by_field, **overrides})
    database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["baseline", "rollback"])
async def test_selected_retained_run_verifies_real_seal_and_emits_closed_proof(monkeypatch, tmp_path, operation):
    directory, receipt = _sealed_input(tmp_path)
    task = _retained_task(directory, receipt, operation)
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: tmp_path)
    monkeypatch.setattr(cms.httpx, "Client", lambda **_kwargs: pytest.fail("retained input reached upstream"))
    admission = AsyncMock(return_value={"status": "validated", "vector_sha256": receipt["vector_sha256"]})
    monkeypatch.setattr(cms, "_run_acquired", admission)
    result = await cms.run({"context": {}}, task, "run-synthetic")
    assert result["cms_retained_input"] == {
        "operation": operation,
        "vector_sha256": receipt["vector_sha256"],
        "receipt_sha256": task["cms_npd_retained_receipt_sha256"],
    }
    assert admission.await_args.args[-1] is None
    assert admission.await_args.args[-2] == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["receipt", "file", "missing_file"])
async def test_selected_retained_run_rejects_changed_bytes_before_admission(monkeypatch, tmp_path, corruption):
    directory, receipt = _sealed_input(tmp_path)
    task = _retained_task(directory, receipt)
    if corruption == "receipt":
        path = directory / "receipt.json"
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        path = directory / (RESOURCE_FILES[0][0] + ".zst")
        path.unlink() if corruption == "missing_file" else path.write_bytes(b"changed")
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: tmp_path)
    admission = AsyncMock(side_effect=AssertionError("changed seal reached admission"))
    monkeypatch.setattr(cms, "_run_acquired", admission)
    with pytest.raises(CmsNpdSourceError):
        await cms.run({"context": {}}, task, "run-synthetic")
    admission.assert_not_awaited()


@pytest.mark.asyncio
async def test_selected_receipt_is_rechecked_after_verification(monkeypatch, tmp_path):
    directory, receipt = _sealed_input(tmp_path)
    task = _retained_task(directory, receipt)
    original = cms.source.verify_retained_release

    def change_receipt(*args):
        original(*args)
        path = directory / "receipt.json"
        path.write_bytes(path.read_bytes() + b"\n")

    monkeypatch.setattr(cms.source, "verify_retained_release", change_receipt)
    with pytest.raises(CmsNpdSourceError, match="retained_receipt_changed"):
        await cms._verify_release(directory, receipt, None, task)


@pytest.mark.asyncio
async def test_baseline_history_requires_same_proved_publication_and_root(monkeypatch):
    identity = cms.release_identity(_receipt())
    database = SimpleNamespace(all=AsyncMock(return_value=[]))
    fake_fhir = SimpleNamespace(db=database, _qt=lambda _schema, name: name, _schema=lambda: "synthetic")
    current = AsyncMock(return_value=None)
    monkeypatch.setattr(cms, "_current_release_publication", current)
    same_lineage = AsyncMock(return_value=True)
    monkeypatch.setattr(cms.recovery, "is_same_acquisition_replay", same_lineage)
    await cms._assert_baseline_history(fake_fhir, "endpoint", identity, {}, "first-root")
    current.assert_not_awaited()
    database.all.return_value = [("dataset",)]
    with pytest.raises(RuntimeError, match="baseline_prior_publication_exists"):
        await cms._assert_baseline_history(fake_fhir, "endpoint", identity, {}, "first-root")
    current.return_value = {
        "dataset_id": "dataset",
        "endpoint_id": "endpoint",
        "acquisition_root_run_id": "first-root",
        "publication_metadata_json": {"source_release": identity},
    }
    await cms._assert_baseline_history(fake_fhir, "endpoint", identity, {}, "first-root")
    same_lineage.return_value = False
    with pytest.raises(RuntimeError, match="baseline_prior_publication_exists"):
        await cms._assert_baseline_history(fake_fhir, "endpoint", identity, {}, "unrelated-run")
    same_lineage.return_value = True
    await cms._assert_baseline_history(
        fake_fhir, "endpoint", identity, {"provider_directory_pagination_root_run_id": "first-root"}, "retry"
    )
    current.return_value["acquisition_root_run_id"] = "first-writing-retry"
    await cms._assert_baseline_history(
        fake_fhir, "endpoint", identity, {"provider_directory_pagination_root_run_id": "first-root"}, "later-retry"
    )
    assert same_lineage.await_args.args[1] == "first-writing-retry"
    database.all.return_value.append(("previous-publication",))
    with pytest.raises(RuntimeError, match="baseline_prior_publication_exists"):
        await cms._assert_baseline_history(fake_fhir, "endpoint", identity, {}, "first-root")


@pytest.mark.asyncio
async def test_retained_rollback_preserves_existing_predecessor_guard_and_retry_root(monkeypatch):
    identity = cms.release_identity(_receipt())
    candidate_factory, _, _, _ = _stub_rollback_publication(monkeypatch, identity)
    task_by_field = {
        "cms_npd_retained_operation": "rollback",
        "cms_npd_retained_vector_sha256": identity["vector_sha256"],
        "cms_npd_retained_receipt_sha256": "c" * 64,
        "provider_directory_pagination_root_run_id": "original-root",
    }
    await cms._admission_candidate(fhir, "endpoint", "retry-leaf", identity, task_by_field)
    cms._assert_rollback_predecessor.assert_awaited_once_with(fhir, "endpoint", identity)
    assert candidate_factory.await_args.kwargs["candidate_key"] == (
        f"cms-npd-rollback:{identity['vector_sha256']}:original-root"
    )
    cms._assert_rollback_predecessor.side_effect = RuntimeError("cms_npd_rollback_prior_publication_missing")
    candidate_factory.reset_mock()
    with pytest.raises(RuntimeError, match="prior_publication_missing"):
        await cms._admission_candidate(fhir, "endpoint", "retry-leaf", identity, task_by_field)
    candidate_factory.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reporting_fails", [False, True])
async def test_intake_failure_preserves_terminal_guard_cause_without_exception_text(monkeypatch, reporting_fails):
    from process.control_lifecycle import _control_failure_error, _terminal_metrics_from_context

    class ConnectionLostError(RuntimeError):
        sqlstate = "08006"

    cause = ConnectionLostError("synthetic-private-text")
    guard = RuntimeError("cms_npd_intake_guard_lost")
    guard.__cause__ = cause
    ctx_by_field = {"context": {"audit": {"existing_observation": True}}}
    reporter = Mock(side_effect=RuntimeError("synthetic-private-text") if reporting_fails else None)
    monkeypatch.setattr(cms_observation, "enqueue_live_progress", reporter)
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))

    async def fail(ctx, *_args):
        cms_observation.observe_intake(ctx, phase="identity", family="InsurancePlan", completed_rows=123)
        raise guard

    monkeypatch.setattr(cms, "_run_upstream_check", fail)
    with pytest.raises(RuntimeError) as caught:
        await cms.run(ctx_by_field, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    assert caught.value is guard and caught.value.__cause__ is cause
    metrics = _terminal_metrics_from_context(ctx_by_field["context"])
    observed = metrics["audit"]["cms_intake"]
    assert metrics["audit"]["existing_observation"] is True
    assert observed["phase"] == "identity" and observed["family"] == "InsurancePlan"
    assert observed["completed_input_rows"] == 123
    assert observed["exception_chain"] == [
        {"class": "RuntimeError"},
        {"class": "ConnectionLostError", "sqlstate": "08006"},
    ]
    assert "synthetic-private-text" not in str(observed)
    assert _control_failure_error(guard) == {"code": "import_failed", "message": "cms_npd_intake_guard_lost"}


def test_intake_chain_redacts_malformed_class_and_sqlstate_and_bounds_cycles():
    error = type("Invalid/private-text", (Exception,), {})("synthetic-private-text")
    error.sqlstate = "08006 synthetic-private-text"
    error.orig = error
    assert cms_observation.intake_exception_chain(error) == [{"class": "Exception"}]
    previous = error
    for _ in range(12):
        current = RuntimeError("synthetic-private-text")
        current.__cause__ = previous
        previous = current
    observed = cms_observation.intake_exception_chain(previous)
    assert len(observed) == 6 and observed == [{"class": "RuntimeError"}] * 6


def test_intake_progress_reports_only_real_bounded_safe_observations(monkeypatch):
    clock = iter([0.0, 1.0, 2.0, 15.0, 15.1])
    monkeypatch.setattr(cms_observation, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    events = []
    monkeypatch.setattr(cms_observation, "enqueue_live_progress", lambda **payload: events.append(payload))
    ctx_by_field = {"context": {}}
    cms_observation.observe_intake(ctx_by_field, phase="staging", family="Organization", completed_rows=0)
    cms_observation.observe_intake(ctx_by_field, phase="staging", family="Organization", completed_rows=1_000)
    cms_observation.observe_intake(ctx_by_field, phase="staging", family="Organization", completed_rows=2_000)
    assert len(events) == 1
    assert ctx_by_field["context"]["audit"]["cms_intake"]["completed_input_rows"] == 2_000
    cms_observation.observe_intake(ctx_by_field, phase="staging", family="Organization", completed_rows=3_000)
    assert len(events) == 2
    cms_observation.observe_intake(
        ctx_by_field, phase="staging", family="Organization", completed_rows=3_000, force=True
    )
    assert len(events) == 3
    assert events[-1]["counters"] == {"completed_input_rows": 3_000}
    assert events[-1]["elapsed_seconds"] == 15.1
    assert events[-1]["done"] == 0 and events[-1]["total"] == 1
    cms_observation.observe_intake(ctx_by_field, phase="synthetic-private-text", family="Organization")
    cms_observation.observe_intake(ctx_by_field, phase="staging", family="synthetic-private-text")
    assert len(events) == 3 and "synthetic-private-text" not in str(ctx_by_field)


@pytest.mark.asyncio
async def test_intake_cancellation_is_not_reclassified_or_consumed(monkeypatch):
    cancellation = asyncio.CancelledError()
    ctx_by_field = {"context": {}}
    monkeypatch.setattr(cms, "durable_artifact_root", lambda: Path("/synthetic/artifacts"))

    async def cancel(ctx, *_args):
        cms_observation.observe_intake(ctx, phase="relationships")
        raise cancellation

    monkeypatch.setattr(cms, "_run_upstream_check", cancel)
    with pytest.raises(asyncio.CancelledError) as caught:
        await cms.run(ctx_by_field, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    assert caught.value is cancellation
    observed = ctx_by_field["context"]["audit"]["cms_intake"]
    assert observed["phase"] == "relationships" and "exception_chain" not in observed
    assert "run" not in ctx_by_field["context"]


@pytest.mark.parametrize("context", [None, {}, {"context": None}])
def test_intake_observation_tolerates_missing_context_and_reporting_failure(monkeypatch, context):
    monkeypatch.setattr(
        cms_observation, "enqueue_live_progress", Mock(side_effect=RuntimeError("synthetic-private-text"))
    )
    cms_observation.observe_intake(context, phase="staging", family="Location", completed_rows=1)


def test_intake_observation_does_not_raise_when_context_reporting_is_broken(monkeypatch):
    class Context(dict):
        def __setitem__(self, name, value):
            if name == "audit":
                raise RuntimeError("synthetic-private-text")
            return super().__setitem__(name, value)

    reporter = Mock()
    monkeypatch.setattr(cms_observation, "enqueue_live_progress", reporter)
    cms_observation.observe_intake({"context": Context()}, phase="staging", family="Location", completed_rows=1)
    reporter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("probe_times_out", [False, True])
async def test_intake_chain_distinguishes_implicit_poll_timeout_from_probe_cause(probe_times_out):
    polling_timeout = TimeoutError("synthetic-poll-text")
    probe_timeout = TimeoutError("synthetic-probe-text")
    connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=probe_timeout) if probe_times_out else AsyncMock(return_value=False),
        commit=AsyncMock(),
    )
    # The watcher invokes this actual guard probe while handling its routine polling timeout.
    try:
        raise polling_timeout
    except TimeoutError:
        with pytest.raises(RuntimeError, match="cms_npd_intake_guard_lost") as caught:
            await cms._assert_intake_guard(connection, "synthetic-lock", 1)
    guard_error = caught.value
    if probe_times_out:
        assert guard_error.__cause__ is probe_timeout
        expected_chain_entries = [{"class": "RuntimeError"}, {"class": "TimeoutError"}]
    else:
        assert guard_error.__cause__ is None and guard_error.__context__ is polling_timeout
        expected_chain_entries = [{"class": "RuntimeError"}]
    assert cms_observation.intake_exception_chain(guard_error) == expected_chain_entries


@pytest.mark.asyncio
async def test_unchanged_publication_helper_keeps_three_argument_call_compatible(monkeypatch):
    observation = _observation()
    state_by_field = {"endpoint_id": "endpoint-synthetic", "dataset_id": "dataset-synthetic", "resource_count": 8}
    descriptor_by_field = {"status": "required"}
    monkeypatch.setattr(cms.source, "assert_observed_release_unchanged", Mock())
    monkeypatch.setattr(cms, "_prepare_current_serving_candidate", AsyncMock(return_value=descriptor_by_field))
    result = await cms._unchanged_publication_result(observation, state_by_field, object())
    assert result["cms_serving_candidate"] == descriptor_by_field and result["replayed"] is True
    cms_observation.enqueue_live_progress.assert_not_called()
