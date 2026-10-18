# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission-lock boundaries for complete and partial Flex publication."""

from __future__ import annotations

import asyncio
import importlib
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict, fields, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_adapters as source_adapters
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_source_acquisition_custody as custody
from process import uhc_flex_official_cohort_snapshot as snapshot
from process import uhc_flex_official_cohort_store as cohort_store
from process import uhc_flex_practitioner_publication as publication
from process import uhc_flex_practitioner_publication_store as store
from process.uhc_flex_practitioner_single_root_contract import build_single_root_admission
from process.uhc_flex_practitioner_twin_store_contract import UHCFlexPractitionerTwinStoreError
from tests.test_uhc_flex_practitioner_publication import _single_root_admission
from tests.test_uhc_flex_practitioner_publication_contract_boundaries import _readiness
from tests.test_uhc_flex_practitioner_publication_store_boundaries import (
    _identity_and_admission,
)
from tests.test_uhc_flex_practitioner_twin_boundaries import _sealed_database_row, _single_root_lock_row
from tests.test_uhc_flex_practitioner_twin_store_contract import OPERATION_KEY, PROJECTION_DATE, TIMESTAMP


@pytest.mark.asyncio
async def test_admission_identity_and_source_fail_closed(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    assert store._is_expected_admission(
        admission,
        admission.candidate_acquisition_id,
    )
    assert not store._is_expected_admission(
        object(),
        admission.candidate_acquisition_id,
    )
    assert not store._is_expected_admission(admission, "pdufpa_" + "0" * 48)

    missing_source_database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(return_value=None),
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="source has drifted",
    ):
        await store._lock_admission(
            missing_source_database,
            admission.candidate_acquisition_id,
            identity.endpoint_id,
        )

    source_database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(side_effect=[{"source_id": admission.source_id}]),
    )
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        AsyncMock(return_value=object()),
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="admission is invalid",
    ):
        await store._lock_admission(
            source_database,
            admission.candidate_acquisition_id,
            identity.endpoint_id,
        )


@pytest.mark.asyncio
async def test_admission_lock_accepts_complete_and_partial_roots(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    source_by_field = {"source_id": admission.source_id}
    database = SimpleNamespace(scalar=AsyncMock(), first=AsyncMock())
    admission_loader = AsyncMock(return_value=admission)
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        admission_loader,
    )
    database.first.side_effect = [
        source_by_field,
        {
            "status": "sealed",
            "cohort_complete": True,
            "error_count": 0,
            "terminal_set_sha256": admission.terminal_set_sha256,
            "resource_count": admission.resource_count,
        },
    ]
    assert await store._lock_admission(
        database,
        admission.candidate_acquisition_id,
        identity.endpoint_id,
    ) == (admission, 0)

    partial = _single_root_admission(error_count=1)
    admission_loader.return_value = partial
    database.first.side_effect = [
        source_by_field,
        {
            "status": "sealed",
            "cohort_complete": False,
            "error_count": 1,
            "terminal_set_sha256": partial.terminal_set_sha256,
            "resource_count": partial.resource_count,
        },
    ]
    assert await store._lock_admission(
        database,
        partial.candidate_acquisition_id,
        identity.endpoint_id,
    ) == (partial, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    (
        {"status": "building"},
        {"cohort_complete": True},
        {"error_count": -1},
        {"terminal_set_sha256": "d" * 64},
        {"resource_count": 2},
    ),
)
async def test_admission_lock_rejects_candidate_state_drift(
    monkeypatch,
    drift,
) -> None:
    admission = _single_root_admission(error_count=1)
    candidate_by_field = {
        "status": "sealed",
        "cohort_complete": False,
        "error_count": 1,
        "terminal_set_sha256": admission.terminal_set_sha256,
        "resource_count": admission.resource_count,
        **drift,
    }
    database = SimpleNamespace(
        scalar=AsyncMock(),
        first=AsyncMock(side_effect=[{"source_id": admission.source_id}, candidate_by_field]),
    )
    monkeypatch.setattr(
        store,
        "require_uhc_flex_practitioner_admission",
        AsyncMock(return_value=admission),
    )

    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="admission is invalid",
    ):
        await store._lock_admission(
            database,
            admission.candidate_acquisition_id,
            publication.uhc_flex_practitioner_endpoint_identity().endpoint_id,
        )


class _FreshPublicationDatabase:
    """Record database boundaries while genuine cohort preparation/replay stays active."""

    def __init__(self, admission, candidate, cohort):
        self.admission, self.candidate, self.cohort = admission, candidate, cohort
        self.binding = None
        self.owner = False
        self.events = []

    def _transaction_binding(self):
        return self.binding

    @asynccontextmanager
    async def transaction(self):
        previous = self.binding
        if previous is None:
            self.binding = SimpleNamespace(session=object())
        try:
            yield
        finally:
            self.binding = previous

    async def scalar(self, _sql, **_parameters):
        return TIMESTAMP

    async def first(self, sql, **_parameters):
        if "provider_directory_uhc_flex_practitioner_twin_admission" in sql:
            return asdict(self.admission)
        if "provider_directory_uhc_flex_practitioner_acquisition" in sql:
            return _sealed_database_row(self.candidate)
        if "provider_directory_uhc_flex_npi_cohort" in sql:
            return asdict(self.cohort)
        assert "FOR SHARE OF source, endpoint" in sql
        return {"source_id": self.admission.source_id}


@asynccontextmanager
async def _fresh_source_transaction(database, *, isolation="read committed"):
    if database.binding is not None:
        yield
        return
    database.events.append(("source", isolation))
    database.binding = SimpleNamespace(session=object())
    database.owner = True
    try:
        yield
    finally:
        database.owner = False
        database.binding = None


@pytest.fixture
def fresh_publication(monkeypatch):
    """Keep real same-task issuance/replay over recorded broker, catalog and publication I/O."""
    candidate, _row, cohort_row, official = _single_root_lock_row()
    cohort = cohort_store._cohort_from_row(cohort_row)
    admission = build_single_root_admission(
        candidate, semantic_projection_as_of=PROJECTION_DATE, operation_key=OPERATION_KEY, admitted_at=TIMESTAMP
    )
    database = _FreshPublicationDatabase(admission, candidate, cohort)
    policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", control_runtime_roles=("builder",))
    driver = SimpleNamespace(is_in_transaction=lambda: database.binding is not None, get_server_pid=lambda: 7)
    driver.fetchval = AsyncMock(return_value=41)
    driver.fetchrow = AsyncMock(
        side_effect=lambda _sql: dict(login="publisher", actor="owner", database_oid=41, backend=7, xid="23")
    )
    publisher = SimpleNamespace(connection=driver, policy=policy, transaction_isolation="repeatable read")
    directory = importlib.import_module("process.provider_directory_fhir")
    source_root = Path(__file__).resolve().parents[1]
    assert Path(directory.__file__).resolve() == source_root / "process/provider_directory_fhir.py"
    monkeypatch.setattr(sys.modules["process"], "provider_directory_fhir", directory)
    monkeypatch.setattr(directory, "db", database)
    monkeypatch.setattr(snapshot, "source_transaction", _fresh_source_transaction)
    monkeypatch.setattr(source_policy, "source_transaction", _fresh_source_transaction)
    monkeypatch.setattr(source_policy, "source_owner_transaction", lambda _db: _empty_source_owner())
    monkeypatch.setattr(snapshot, "source_transaction_available", lambda: publisher if database.owner else None)
    monkeypatch.setattr(source_adapters, "source_transaction_available", lambda: publisher if database.owner else None)
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(snapshot, "source_copy_session", lambda: database.binding.session)
    monkeypatch.setattr(snapshot, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(snapshot, "pinned_current_source", lambda *_args, **_kwargs: _empty_source_owner())
    custody_by_field = {"namespace_roles": ("migration", "namespace_owner")}
    monkeypatch.setattr(snapshot, "_custody", AsyncMock(return_value=custody_by_field))
    monkeypatch.setattr(custody, "capture_cohort_snapshot_custody", AsyncMock(return_value=custody_by_field))
    monkeypatch.setattr(cohort_store, "_current_official_snapshot", AsyncMock(return_value=official))
    monkeypatch.setattr(cohort_store, "_stored_cohort_for_dataset", AsyncMock(return_value=cohort))
    identity = publication.build_uhc_flex_practitioner_dataset_identity(admission)
    readiness_names = {field.name for field in fields(publication.UHCFlexPractitionerDatasetReadiness)}
    readiness = replace(
        _readiness(), **{name: field_value for name, field_value in asdict(identity).items() if name in readiness_names}
    )
    monkeypatch.setattr(store, "load_dataset_readiness", AsyncMock(return_value=readiness))
    monkeypatch.setattr(store, "_locked_existing_dataset", AsyncMock(return_value=None))
    monkeypatch.setattr(store, "_locked_current_dataset", AsyncMock(return_value=None))
    for name in (
        "_assert_no_orphan_parent",
        "_insert_building_headers",
        "_materialize_candidate",
        "_validate_candidate",
        "_supersede_previous",
        "_publish_candidate",
    ):
        monkeypatch.setattr(store, name, AsyncMock())
    monkeypatch.setattr(store, "prepare_dataset_candidate", lambda *_args, **_kwargs: _empty_source_owner())
    return SimpleNamespace(
        database=database,
        admission=admission,
        identity=identity,
        driver=driver,
        publisher=publisher,
    )


@asynccontextmanager
async def _empty_source_owner():
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_fresh_tasks_prepare_real_cohort_before_publication_and_published_replay(fresh_publication, published):
    case = fresh_publication
    if published:
        store._locked_existing_dataset.return_value = {"dataset_id": case.identity.dataset_id}

    async def publish():
        assert asyncio.current_task() not in snapshot._ISSUED
        try:
            result = await store.publish_registered_uhc_flex_dataset(
                case.admission.candidate_acquisition_id, case.identity.endpoint_id, 10, database=case.database
            )
            assert snapshot._ISSUED[asyncio.current_task()][case.admission.cohort_id].cohort == case.database.cohort
            return result
        finally:
            snapshot._ISSUED.pop(asyncio.current_task(), None)

    first = await asyncio.create_task(publish())
    assert first.replayed is published
    assert store._publish_candidate.await_count == (0 if published else 1)
    store._locked_existing_dataset.return_value = {"dataset_id": case.identity.dataset_id}
    replay = await asyncio.create_task(publish())
    assert replay.replayed is True and replay.readiness == first.readiness
    assert case.database.binding is None
    assert (
        case.database.events
        == [("source", "repeatable read"), ("source", "read committed"), ("source", "repeatable read")] * 2
    )
    assert custody.capture_cohort_snapshot_custody.await_count == 2
    assert store._publish_candidate.await_count == (0 if published else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
@pytest.mark.parametrize(
    "damage",
    ["snapshot", "custody", "actor", "database", "backend", "xid", "binding", "connection", "policy", "session"],
)
async def test_fresh_publication_and_replay_refuse_changed_cohort_authority(
    fresh_publication, monkeypatch, published, damage
):
    case = fresh_publication
    if published:
        store._locked_existing_dataset.return_value = {"dataset_id": case.identity.dataset_id}
    original_capture = custody.capture_cohort_snapshot_custody

    async def replace_binding(*args, **kwargs):
        case.database.binding = SimpleNamespace(session=object())
        return await original_capture(*args, **kwargs)

    actor_by_field = dict(login="publisher", actor="owner", database_oid=41, backend=7, xid="23")
    changed_actor_by_field = {**actor_by_field, **{damage: "changed" if damage in {"actor", "xid"} else 99}}
    if damage in {"connection", "policy", "session"}:
        publisher = SimpleNamespace(connection=case.driver, policy=case.publisher.policy)
        if damage == "connection":
            publisher.connection = object()
        elif damage == "policy":
            publisher.policy = SimpleNamespace(**vars(case.publisher.policy))
        else:
            monkeypatch.setattr(snapshot, "source_copy_session", lambda: object())
        monkeypatch.setattr(
            snapshot, "source_transaction_available", lambda: publisher if case.database.owner else None
        )
    if damage == "snapshot":
        official = cohort_store._current_official_snapshot.return_value
        cohort_store._current_official_snapshot.side_effect = [official] * 2 + [object()]
    if damage == "custody":
        custody.capture_cohort_snapshot_custody.return_value = {"namespace_roles": ("foreign", "namespace_owner")}
    if damage == "binding":
        monkeypatch.setattr(custody, "capture_cohort_snapshot_custody", replace_binding)
    if damage in {"actor", "database", "backend", "xid"}:
        if damage == "database":
            changed_actor_by_field = {**actor_by_field, "database_oid": 99}
        case.driver.fetchrow.side_effect = (
            [actor_by_field, changed_actor_by_field] if damage == "xid" else [changed_actor_by_field]
        )

    async def publish():
        try:
            return await store.publish_registered_uhc_flex_dataset(
                case.admission.candidate_acquisition_id, case.identity.endpoint_id, 10, database=case.database
            )
        finally:
            snapshot._ISSUED.pop(asyncio.current_task(), None)

    with pytest.raises(UHCFlexPractitionerTwinStoreError):
        await asyncio.create_task(publish())
    store._publish_candidate.assert_not_awaited()
    store._locked_existing_dataset.assert_not_awaited()
    assert case.database.binding is None


@pytest.mark.asyncio
async def test_fresh_twin_publication_keeps_its_existing_admission_without_cohort_preparation(
    fresh_publication, monkeypatch
):
    case = fresh_publication
    identity, admission = _identity_and_admission()
    case.database.admission = admission
    monkeypatch.setattr(store, "_lock_admission", AsyncMock(return_value=(admission, 0)))
    marker = object()
    monkeypatch.setattr(store, "_publish_admitted_dataset", AsyncMock(return_value=marker))
    assert (
        await store.publish_registered_uhc_flex_dataset(
            admission.candidate_acquisition_id, identity.endpoint_id, 10, database=case.database
        )
        is marker
    )
    assert case.database.events == [("source", "repeatable read")]
    cohort_store._current_official_snapshot.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_already_issued_owner_publication_borrows_original_transaction(fresh_publication, published):
    case = fresh_publication
    if published:
        store._locked_existing_dataset.return_value = {"dataset_id": case.identity.dataset_id}

    async def publish():
        try:
            await cohort_store.sync_uhc_flex_official_cohort(database=case.database)
            async with _fresh_source_transaction(case.database, isolation="repeatable read"):
                original = case.database.binding
                result = await store.publish_registered_uhc_flex_dataset(
                    case.admission.candidate_acquisition_id, case.identity.endpoint_id, 10, database=case.database
                )
                assert case.database.binding is original
                assert result.replayed is published
        finally:
            snapshot._ISSUED.pop(asyncio.current_task(), None)

    await asyncio.create_task(publish())
    assert case.database.events == [
        ("source", "repeatable read"),
        ("source", "read committed"),
        ("source", "repeatable read"),
    ]
    assert case.database.binding is None
