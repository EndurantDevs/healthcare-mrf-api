# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real PostgreSQL proof for the fixed reviewed formulary operator."""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
import process.formulary_fhir.repository_publish as repository_publish
import process.formulary_fhir.synchronizer as synchronizer_module
from process.formulary_fhir.reviewed_acquisition import ReviewedAcquisitionResult
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.reviewed_acquisition import acquire_reviewed_twins
from process.formulary_fhir.reviewed_operation import ACQUISITION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import PUBLICATION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import reviewed_run_identities
from process.formulary_fhir.reviewed_publication import publish_reviewed_candidate
from process.formulary_fhir.reviewed_source import reviewed_source_manifest
from tests.test_formulary_fhir_reviewed_source_postgres import _ClientFactory
from tests.test_formulary_fhir_reviewed_source_postgres import (
    _ReviewedCensusClient,
)
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_twin_repository_postgres import _prepare_schema


FIRST_CUTOFF = dt.datetime(2026, 8, 7, 1, tzinfo=dt.UTC)
SECOND_CUTOFF = dt.datetime(2026, 8, 7, 2, tzinfo=dt.UTC)
CANCEL_CUTOFF = dt.datetime(2026, 8, 7, 3, tzinfo=dt.UTC)
PUBLICATION_CANCEL_CUTOFF = dt.datetime(2026, 8, 7, 4, tzinfo=dt.UTC)
POST_VERIFY_CANCEL_CUTOFF = dt.datetime(2026, 8, 7, 5, tzinfo=dt.UTC)


def _enable_acquisition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ACQUISITION_ENABLED_ENV, "true")
    monkeypatch.delenv(PUBLICATION_ENABLED_ENV, raising=False)


def _enable_publication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ACQUISITION_ENABLED_ENV, raising=False)
    monkeypatch.setenv(PUBLICATION_ENABLED_ENV, "true")


async def _pointer() -> dict[str, object]:
    pointer_row = await db.first(
        f"SELECT dataset_id, generation, published_at FROM "
        f"{table_name('fhir_formulary_current')};"
    )
    return dict(pointer_row._mapping) if pointer_row is not None else {}


async def _assert_full_checkpoints(
    acquisition: ReviewedAcquisitionResult,
) -> None:
    rows = await db.all(
        f"SELECT dataset_id, run_id, cutoff_at, acquisition_mode, completed, "
        f"expected_count, processed_count FROM "
        f"{table_name('fhir_formulary_checkpoint')} WHERE "
        "dataset_id = ANY(:dataset_ids) ORDER BY dataset_id, alias_id;",
        dataset_ids=[
            acquisition.baseline_dataset_id,
            acquisition.candidate_dataset_id,
        ],
    )
    run_by_dataset = {
        acquisition.baseline_dataset_id: acquisition.baseline_run_id,
        acquisition.candidate_dataset_id: acquisition.candidate_run_id,
    }
    assert len(rows) == 4
    assert {row.dataset_id for row in rows} == set(run_by_dataset)
    for checkpoint in rows:
        assert checkpoint.run_id == run_by_dataset[checkpoint.dataset_id]
        assert checkpoint.cutoff_at == acquisition.cutoff_at
        assert checkpoint.acquisition_mode == "full"
        assert checkpoint.completed is True
        assert checkpoint.expected_count == checkpoint.processed_count == 2


def _assert_fresh_clients(
    client_factory: _ClientFactory,
    *,
    expected_client_count: int,
) -> None:
    assert len(client_factory.clients) == expected_client_count
    for census_client in client_factory.clients:
        assert census_client.request_count in {1, 3}
        if census_client.request_count == 3:
            assert census_client.medication_aliases == ["SYNTH-A", "SYNTH-B"]
        else:
            assert census_client.medication_aliases == []


async def _run_two_generations(monkeypatch: pytest.MonkeyPatch) -> None:
    client_factory = _ClientFactory()
    _enable_acquisition(monkeypatch)
    first_admission = await acquire_reviewed_twins(
        cutoff=FIRST_CUTOFF,
        client_factory=client_factory,
    )
    assert await _pointer() == {}
    await _assert_full_checkpoints(first_admission)
    _assert_fresh_clients(client_factory, expected_client_count=2)
    first_replay = await acquire_reviewed_twins(
        cutoff=FIRST_CUTOFF,
        client_factory=client_factory,
    )
    assert first_replay == first_admission
    assert await _pointer() == {}
    _assert_fresh_clients(client_factory, expected_client_count=4)

    _enable_publication(monkeypatch)
    first_publication = await publish_reviewed_candidate(cutoff=FIRST_CUTOFF)
    assert first_publication.generation == 1
    assert (await _pointer())["dataset_id"] == first_admission.candidate_dataset_id
    assert await publish_reviewed_candidate(cutoff=FIRST_CUTOFF) == (
        first_publication
    )

    _enable_acquisition(monkeypatch)
    second_admission = await acquire_reviewed_twins(
        cutoff=SECOND_CUTOFF,
        client_factory=client_factory,
    )
    assert (await _pointer())["dataset_id"] == first_admission.candidate_dataset_id
    await _assert_full_checkpoints(second_admission)
    _assert_fresh_clients(client_factory, expected_client_count=6)

    _enable_publication(monkeypatch)
    second_publication = await publish_reviewed_candidate(cutoff=SECOND_CUTOFF)
    assert second_publication.generation == 2
    assert second_publication.predecessor_dataset_id == (
        first_admission.candidate_dataset_id
    )
    assert (await _pointer())["dataset_id"] == second_admission.candidate_dataset_id
    assert await publish_reviewed_candidate(cutoff=SECOND_CUTOFF) == (
        second_publication
    )


class _BlockingCensusClient(_ReviewedCensusClient):
    def __init__(self, config, entered: asyncio.Event) -> None:
        super().__init__(config)
        self.entered = entered

    async def medication_current_census(self, alias, *, cutoff):
        self.entered.set()
        await asyncio.Future()


class _BlockingClientFactory:
    def __init__(self, entered: asyncio.Event) -> None:
        self.entered = entered

    def __call__(self, config) -> _BlockingCensusClient:
        return _BlockingCensusClient(config, self.entered)


async def _assert_cancel_and_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_acquisition(monkeypatch)
    entered = asyncio.Event()
    acquisition_task = asyncio.create_task(
        acquire_reviewed_twins(
            cutoff=CANCEL_CUTOFF,
            client_factory=_BlockingClientFactory(entered),
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    source_id = reviewed_source_manifest().source_id
    with pytest.raises(manual_lock.ManualSourceLockError) as busy:
        async with manual_lock.manual_source_lease(
            db,
            source_id,
            wait_seconds=0.05,
            retry_seconds=0.01,
        ):
            pytest.fail("competing source lease unexpectedly acquired")
    assert busy.value.code == "busy"
    acquisition_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquisition_task
    assert await _pointer() == {}
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_twin_admission')};"
    ) == 0
    async with manual_lock.manual_source_lease(
        db,
        source_id,
        wait_seconds=0.5,
        retry_seconds=0.01,
    ):
        assert await db.scalar("SELECT 1;") == 1

    resumed = await acquire_reviewed_twins(
        cutoff=CANCEL_CUTOFF,
        client_factory=_ClientFactory(),
    )
    await _assert_full_checkpoints(resumed)
    assert await _pointer() == {}

    _enable_publication(monkeypatch)
    publication = await publish_reviewed_candidate(cutoff=CANCEL_CUTOFF)
    assert publication.generation == 1
    assert (await _pointer())["dataset_id"] == resumed.candidate_dataset_id


async def _assert_publication_cancel_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_acquisition(monkeypatch)
    admission = await acquire_reviewed_twins(
        cutoff=PUBLICATION_CANCEL_CUTOFF,
        client_factory=_ClientFactory(),
    )
    _enable_publication(monkeypatch)
    entered = asyncio.Event()
    original_mark_published = repository_publish._mark_published

    async def blocking_mark_published(*_args, **_kwargs) -> None:
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(
        repository_publish,
        "_mark_published",
        blocking_mark_published,
    )
    publication_task = asyncio.create_task(
        publish_reviewed_candidate(cutoff=PUBLICATION_CANCEL_CUTOFF)
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    publication_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publication_task
    assert await _pointer() == {}
    candidate_status = await db.scalar(
        f"SELECT status FROM {table_name('fhir_formulary_dataset')} WHERE "
        "dataset_id = :dataset_id;",
        dataset_id=admission.candidate_dataset_id,
    )
    assert candidate_status == "verified"

    monkeypatch.setattr(
        repository_publish,
        "_mark_published",
        original_mark_published,
    )
    publication = await publish_reviewed_candidate(
        cutoff=PUBLICATION_CANCEL_CUTOFF
    )
    assert publication.generation == 1
    pointer = await _pointer()
    assert pointer == {
        "dataset_id": admission.candidate_dataset_id,
        "generation": publication.generation,
        "published_at": publication.published_at,
    }


async def _dataset_lifecycle(
    source_id: str,
    dataset_id: str,
) -> dict[str, object]:
    """Load the exact resumable fields for one deterministic dataset."""

    dataset_record = await db.first(
        f"SELECT status, error_json FROM "
        f"{table_name('fhir_formulary_dataset')} WHERE "
        "source_id = :source_id AND dataset_id = :dataset_id;",
        source_id=source_id,
        dataset_id=dataset_id,
    )
    assert dataset_record is not None
    return dict(dataset_record._mapping)


async def _assert_post_verify_cancel_resumes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove a verified interruption resumes the same full root."""

    _enable_acquisition(monkeypatch)
    original_result = synchronizer_module._result

    def cancel_after_verification(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(
        synchronizer_module,
        "_result",
        cancel_after_verification,
    )
    with pytest.raises(asyncio.CancelledError):
        await acquire_reviewed_twins(
            cutoff=POST_VERIFY_CANCEL_CUTOFF,
            client_factory=_ClientFactory(),
        )

    manifest = reviewed_source_manifest()
    identities = reviewed_run_identities(POST_VERIFY_CANCEL_CUTOFF)
    baseline_dataset_id = stable_id(
        "ffd_",
        manifest.source_id,
        identities.baseline_run_id,
    )
    assert await _dataset_lifecycle(
        manifest.source_id,
        baseline_dataset_id,
    ) == {
        "status": "verified",
        "error_json": {"resumable": True, "type": "CancelledError"},
    }
    assert await _pointer() == {}
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_twin_admission')};"
    ) == 0

    monkeypatch.setattr(synchronizer_module, "_result", original_result)
    resumed = await acquire_reviewed_twins(
        cutoff=POST_VERIFY_CANCEL_CUTOFF,
        client_factory=_ClientFactory(),
    )
    assert resumed.baseline_dataset_id == baseline_dataset_id
    await _assert_full_checkpoints(resumed)
    assert (
        await _dataset_lifecycle(manifest.source_id, baseline_dataset_id)
    )["error_json"] is None

    _enable_publication(monkeypatch)
    publication = await publish_reviewed_candidate(
        cutoff=POST_VERIFY_CANCEL_CUTOFF
    )
    assert publication.generation == 1
    assert (await _pointer())["dataset_id"] == resumed.candidate_dataset_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "two-generations",
        "cancel-resume",
        "publication-cancel",
        "post-verify-cancel",
    ],
)
async def test_reviewed_operator_real_postgres(
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
) -> None:
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        if scenario == "two-generations":
            await _run_two_generations(monkeypatch)
        elif scenario == "cancel-resume":
            await _assert_cancel_and_resume(monkeypatch)
        elif scenario == "publication-cancel":
            await _assert_publication_cancel_rolls_back(monkeypatch)
        else:
            await _assert_post_verify_cancel_resumes(monkeypatch)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
