# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_publication as publication
from process import uhc_flex_practitioner_publication_store as store
from process.provider_directory_dataset_scoped_publication import (
    ProviderDirectoryDatasetScopedPublicationError,
)
from tests.test_provider_directory_dataset_scoped_publication_contract import (
    _legacy_current,
)
from tests.test_uhc_flex_practitioner_publication import (
    _admission,
    _single_root_admission,
)
from tests.test_uhc_flex_practitioner_publication_contract_boundaries import (
    _readiness,
)


def _identity_and_admission(resource_count: int = 1):
    admission = _admission(resource_count=resource_count)
    identity = publication.build_uhc_flex_practitioner_dataset_identity(admission)
    return identity, admission


def _readiness_row(resource_count: int = 1) -> dict[str, object]:
    readiness_fields = asdict(_readiness(resource_count=resource_count))
    readiness_fields["semantic_projection_as_of"] = date(2026, 8, 10)
    return readiness_fields


@pytest.mark.asyncio
async def test_readiness_rows_and_loaders_are_bounded() -> None:
    readiness = store._readiness_from_row(_readiness_row())
    assert readiness.semantic_projection_as_of == "2026-08-10"
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="state is invalid",
    ):
        store._readiness_from_row({})

    sql = store._readiness_select_sql("header.dataset_id = :dataset_id")
    assert "status = 'published'" in sql
    assert "is_current IS TRUE" in sql
    assert "dataset_ready" in sql
    assert "candidate.error_count AS retry_exhausted_count" in sql

    database = SimpleNamespace(
        first=AsyncMock(side_effect=[None, _readiness_row(), None, _readiness_row()])
    )
    assert (
        await store.load_dataset_readiness(
            readiness.dataset_id,
            database=database,
        )
        is None
    )
    assert (
        await store.load_dataset_readiness(
            readiness.dataset_id,
            database=database,
        )
    ).dataset_id == readiness.dataset_id
    assert await store.load_current_readiness(database=database) is None
    assert (
        await store.load_current_readiness(database=database)
    ).dataset_id == readiness.dataset_id


@pytest.mark.asyncio
async def test_existing_and_orphan_parent_locks_fail_closed() -> None:
    identity, _admitted = _identity_and_admission()
    database = SimpleNamespace(
        first=AsyncMock(return_value={"dataset_id": identity.dataset_id})
    )
    assert await store._locked_existing_dataset(database, identity) == {
        "dataset_id": identity.dataset_id
    }

    for parent_count, expected_code in ((2, "state"), (1, "source_drift")):
        count_database = SimpleNamespace(scalar=AsyncMock(return_value=parent_count))
        with pytest.raises(
            publication.UHCFlexPractitionerPublicationError
        ) as error_info:
            await store._assert_no_orphan_parent(count_database, identity)
        assert error_info.value.code == expected_code
    await store._assert_no_orphan_parent(
        SimpleNamespace(scalar=AsyncMock(return_value=0)),
        identity,
    )


@pytest.mark.asyncio
async def test_current_lock_rejects_orphan_dedicated_header(monkeypatch) -> None:
    identity, _admitted = _identity_and_admission()
    current_loader = AsyncMock(return_value=None)
    monkeypatch.setattr(store, "lock_exact_current_dataset", current_loader)
    empty_database = object()
    assert (
        await store._locked_current_dataset(
            empty_database,
            identity,
        )
        is None
    )
    current_loader.assert_awaited_once_with(
        empty_database,
        pair=store.exact_uhc_dataset_pair(),
        require_ready=False,
    )

    current_loader.side_effect = ProviderDirectoryDatasetScopedPublicationError(
        "both_current"
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="state is invalid",
    ):
        await store._locked_current_dataset(
            object(),
            identity,
        )


@pytest.mark.asyncio
async def test_current_lock_rejects_foreign_and_returns_ready_parent(
    monkeypatch,
) -> None:
    identity, _admitted = _identity_and_admission()
    current_loader = AsyncMock(
        side_effect=ProviderDirectoryDatasetScopedPublicationError("foreign_current")
    )
    monkeypatch.setattr(store, "lock_exact_current_dataset", current_loader)
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="unrelated current dataset",
    ):
        await store._locked_current_dataset(object(), identity)

    previous = _legacy_current()
    current_loader.side_effect = None
    current_loader.return_value = previous
    assert (
        await store._locked_current_dataset(
            object(),
            identity,
        )
        == previous
    )


@pytest.mark.asyncio
async def test_header_inserts_require_one_row_and_compose_metadata(
    monkeypatch,
) -> None:
    identity, admission = _identity_and_admission()
    failed_database = SimpleNamespace(status=AsyncMock(return_value=0))
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        await store._insert_parent_header(
            failed_database,
            identity,
            admission,
            None,
            "{}",
        )
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        await store._insert_dedicated_header(
            failed_database,
            identity,
            admission,
            None,
        )

    complete_database = SimpleNamespace(status=AsyncMock(return_value=1))
    await store._insert_parent_header(
        complete_database,
        identity,
        admission,
        None,
        "{}",
    )
    await store._insert_dedicated_header(
        complete_database,
        identity,
        admission,
        None,
    )

    parent_insert = AsyncMock()
    dedicated_insert = AsyncMock()
    monkeypatch.setattr(store, "_insert_parent_header", parent_insert)
    monkeypatch.setattr(store, "_insert_dedicated_header", dedicated_insert)
    database = object()
    admission = _single_root_admission(error_count=1)
    identity = publication.build_uhc_flex_practitioner_dataset_identity(admission)
    await store._insert_building_headers(
        database,
        identity,
        admission,
        "pdufpd_" + "8" * 48,
        1,
    )
    assert '"cohort_complete":false' in parent_insert.await_args.args[4]
    assert '"retry_exhausted_count":1' in parent_insert.await_args.args[4]
    dedicated_insert.assert_awaited_once_with(
        database, identity, admission, "pdufpd_" + "8" * 48, 1
    )


@pytest.mark.asyncio
async def test_supersede_and_publish_require_exact_atomic_updates(
    monkeypatch,
) -> None:
    previous = _legacy_current()
    superseder = AsyncMock(
        side_effect=ProviderDirectoryDatasetScopedPublicationError("foreign_current")
    )
    monkeypatch.setattr(store, "supersede_exact_current_dataset", superseder)
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="unrelated current dataset",
    ):
        await store._supersede_previous(object(), previous)

    for update_counts in ((0, 1), (1, 0)):
        publish_database = SimpleNamespace(status=AsyncMock(side_effect=update_counts))
        with pytest.raises(publication.UHCFlexPractitionerPublicationError):
            await store._publish_candidate(
                publish_database,
                previous.dataset_id,
            )

    superseder.side_effect = None
    superseder.return_value = None
    supersede_database = object()
    await store._supersede_previous(supersede_database, previous)
    superseder.assert_awaited_with(supersede_database, previous)
    complete_database = SimpleNamespace(
        status=AsyncMock(side_effect=[1, 1]),
    )
    await store._publish_candidate(
        complete_database,
        previous.dataset_id,
    )


@pytest.mark.asyncio
async def test_replay_requires_current_readiness(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    monkeypatch.setattr(
        store,
        "_locked_existing_dataset",
        AsyncMock(return_value={"dataset_id": identity.dataset_id}),
    )
    readiness_loader = AsyncMock(return_value=None)
    monkeypatch.setattr(store, "load_dataset_readiness", readiness_loader)
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="replay is not current",
    ):
        await store._publish_admitted_dataset(
            object(),
            admission,
            identity.endpoint_id,
            10,
        )

    readiness_loader.return_value = _readiness()
    result = await store._publish_admitted_dataset(
        object(),
        admission,
        identity.endpoint_id,
        10,
    )
    assert result.replayed is True
    assert result.readiness.dataset_id == identity.dataset_id


@pytest.mark.asyncio
async def test_new_publication_runs_full_sequence_before_readiness(
    monkeypatch,
) -> None:
    identity, admission = _identity_and_admission()
    previous_dataset = _legacy_current()
    database = object()
    monkeypatch.setattr(
        store,
        "_locked_existing_dataset",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(store, "_assert_no_orphan_parent", AsyncMock())
    monkeypatch.setattr(
        store,
        "_locked_current_dataset",
        AsyncMock(return_value=previous_dataset),
    )
    for function_name in (
        "_insert_building_headers",
        "_materialize_candidate",
        "_validate_candidate",
        "_supersede_previous",
        "_publish_candidate",
    ):
        monkeypatch.setattr(store, function_name, AsyncMock())
    readiness_loader = AsyncMock(return_value=None)
    monkeypatch.setattr(store, "load_dataset_readiness", readiness_loader)

    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="state is invalid",
    ):
        await store._publish_admitted_dataset(
            database,
            admission,
            identity.endpoint_id,
            10,
        )

    readiness_loader.return_value = _readiness()
    publication_result = await store._publish_admitted_dataset(
        database,
        admission,
        identity.endpoint_id,
        10,
    )
    assert publication_result.replayed is False
    store._supersede_previous.assert_awaited_with(
        database,
        previous_dataset,
    )
    store._publish_candidate.assert_awaited_with(
        database,
        identity.dataset_id,
    )


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_public_store_wraps_lock_and_publication_in_one_transaction(
    monkeypatch,
) -> None:
    identity, admission = _identity_and_admission()
    readiness = _readiness()
    expected = publication.UHCFlexPractitionerPublicationResult(
        readiness,
        replayed=False,
    )
    admission_lock = AsyncMock(return_value=(admission, 0))
    admitted_publisher = AsyncMock(return_value=expected)
    monkeypatch.setattr(store, "_lock_admission", admission_lock)
    monkeypatch.setattr(
        store,
        "_publish_admitted_dataset",
        admitted_publisher,
    )
    database = SimpleNamespace(transaction=lambda: _transaction())
    publication_result = await store.publish_registered_uhc_flex_dataset(
        admission.candidate_acquisition_id,
        identity.endpoint_id,
        11,
        database=database,
    )
    assert publication_result is expected
    admission_lock.assert_awaited_once_with(
        database,
        admission.candidate_acquisition_id,
        identity.endpoint_id,
    )
    admitted_publisher.assert_awaited_once_with(
        database,
        admission,
        identity.endpoint_id,
        11,
        0,
    )
