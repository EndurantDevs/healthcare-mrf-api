# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public orchestration checks with an explicit synthetic Source authority seam."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_policy as source
from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process import provider_directory_rooted_graph_publication_store as store
from process import provider_directory_rooted_graph_source_runtime as adapter
from process import provider_directory_rooted_graph_store_support as support
from process.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphPublicationError
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    exact_current,
    readiness,
    resource_counts,
    twin_admission,
)


class _Runtime:
    """Record physical transaction boundaries without supplying private authority."""

    def __init__(self):
        self.connection = SimpleNamespace(fetchval=AsyncMock(return_value=None))
        self.session = object()
        self.candidate_schema = "private_candidate"
        self.catalog_schema = "catalog"
        self.require_bound_factory = Mock()
        self.relation_coordinates = Mock(return_value=("private_candidate", "resource_heap"))
        self.transactions = []
        self.exits = []
        self.events = []

    @asynccontextmanager
    async def transaction(self, database, **options):
        self.transactions.append((database, options))
        self.events.append("transaction_enter")
        try:
            yield SimpleNamespace(connection=self.connection, session=self.session)
        except BaseException as error:
            self.exits.append(error)
            raise
        else:
            self.exits.append(None)
        finally:
            self.events.append("transaction_exit")


@pytest.fixture
def issued_scope():
    runtime = _Runtime()
    factory = SimpleNamespace(rooted_graph_runtime=runtime)
    token = source._FACTORY.set(factory)
    try:
        yield SimpleNamespace(
            runtime=runtime, factory=factory, database=SimpleNamespace(scalar=AsyncMock(return_value=True))
        )
    finally:
        source._FACTORY.reset(token)


@pytest.mark.parametrize("factory", [None, SimpleNamespace()])
def test_unissued_and_generic_factories_do_not_override_relations(factory):
    token = source._FACTORY.set(factory)
    try:
        assert adapter.current_rooted_graph_runtime(object()) is None
        assert adapter.rooted_graph_relation_coordinates("mrf", "resource", "resource_scoped") is None
    finally:
        source._FACTORY.reset(token)


def test_issued_adapter_forwards_original_factory_database_and_relation_scope(issued_scope):
    scope = issued_scope
    assert adapter.current_rooted_graph_runtime(scope.database) is scope.runtime
    scope.runtime.require_bound_factory.assert_called_once_with(scope.factory, scope.database)
    assert adapter.rooted_graph_relation_coordinates("mrf", "resource", "resource_scoped") == (
        "private_candidate",
        "resource_heap",
    )
    scope.runtime.require_bound_factory.assert_called_with(scope.factory, None)
    scope.runtime.relation_coordinates.assert_called_once_with("mrf", "resource", "resource_scoped")


def test_binding_rejection_prevents_relation_override_and_propagates(issued_scope):
    error = RuntimeError("synthetic issued factory rejected")
    issued_scope.runtime.require_bound_factory.side_effect = error
    with pytest.raises(RuntimeError) as rejected:
        adapter.rooted_graph_relation_coordinates("mrf", "resource", "resource_scoped")
    assert rejected.value is error
    issued_scope.runtime.relation_coordinates.assert_not_called()
    with pytest.raises(RuntimeError) as rejected:
        adapter.current_rooted_graph_runtime(issued_scope.database)
    assert rejected.value is error
    issued_scope.runtime.require_bound_factory.assert_called_with(issued_scope.factory, issued_scope.database)


@pytest.mark.asyncio
async def test_issued_transaction_uses_canonical_session_and_does_not_swallow_cancellation(issued_scope):
    error = asyncio.CancelledError()
    issued_scope.database.transaction = Mock(side_effect=AssertionError("generic transaction used"))
    with pytest.raises(asyncio.CancelledError) as rejected:
        async with support.rooted_graph_transaction(issued_scope.database) as session:
            assert session is issued_scope.runtime.session
            raise error
    assert rejected.value is error
    assert issued_scope.runtime.transactions == [(issued_scope.database, {})]
    assert issued_scope.runtime.exits == [error]
    issued_scope.database.transaction.assert_not_called()


@pytest.fixture
def preparation(issued_scope, monkeypatch):
    scope = issued_scope
    scope.handoff = SimpleNamespace(document={"preparation_id": "11111111-1111-4111-8111-111111111111"})
    scope.retained = AsyncMock(return_value=None)
    scope.proof = SimpleNamespace(current=exact_current())

    async def validate_current(database):
        assert database is scope.database
        scope.runtime.events.append("validate_current")
        return scope.proof

    scope.validate = AsyncMock(side_effect=validate_current)
    scope.prepare = AsyncMock(return_value=scope.handoff)
    monkeypatch.setattr(candidate, "retained_rooted_handoff", scope.retained)
    monkeypatch.setattr(preflight, "prepare_current_root", scope.validate)
    monkeypatch.setattr(store, "_prepare_rooted_candidate", scope.prepare)
    return scope


@pytest.mark.asyncio
async def test_new_preparation_validates_after_read_transaction_before_candidate(preparation):
    scope = preparation
    acquisition = twin_admission().publication_acquisition_id
    assert (
        await store.prepare_admitted_rooted_graph_dataset(acquisition, database=scope.database, batch_size=7)
        is scope.handoff
    )
    assert scope.runtime.events == ["transaction_enter", "transaction_exit", "validate_current"]
    scope.retained.assert_awaited_once_with(
        scope.runtime, scope.runtime.connection, None, publication_acquisition_id=acquisition
    )
    scope.prepare.assert_awaited_once_with(scope.database, scope.runtime, scope.proof, acquisition, 7)
    assert scope.runtime.transactions == [(scope.database, {"readonly": True})]


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_retained_preparation_reuse_distinguishes_frozen_and_committed(preparation, published):
    scope = preparation
    scope.retained.return_value = scope.handoff
    scope.runtime.connection.fetchval.side_effect = [dataset_identity().dataset_id, published]
    result = await store.prepare_admitted_rooted_graph_dataset("synthetic-acquisition", database=scope.database)
    assert result is scope.handoff
    scope.prepare.assert_not_awaited()
    assert scope.validate.await_count == int(not published)
    assert scope.runtime.exits == [None]
    assert scope.runtime.connection.fetchval.await_args.args[1] == scope.handoff.document["preparation_id"]


@pytest.mark.asyncio
async def test_frozen_resume_propagates_failed_current_fence_without_rebuilding(preparation):
    scope = preparation
    scope.retained.return_value = scope.handoff
    scope.runtime.connection.fetchval.side_effect = [dataset_identity().dataset_id, False]
    error = ProviderDirectoryRootedGraphPublicationError("foreign_current")
    scope.validate.side_effect = error
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as rejected:
        await store.prepare_admitted_rooted_graph_dataset("synthetic-acquisition", database=scope.database)
    assert rejected.value is error
    scope.prepare.assert_not_awaited()
    assert scope.runtime.exits == [None]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "batch_size", [False, 0, store.PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS + 1]
)
async def test_preparation_invalid_batch_refuses_before_native_read(preparation, batch_size):
    with pytest.raises(ValueError, match="batch_size_invalid"):
        await store.prepare_admitted_rooted_graph_dataset(
            "synthetic-acquisition", database=preparation.database, batch_size=batch_size
        )
    preparation.runtime.connection.fetchval.assert_not_awaited()
    preparation.validate.assert_not_awaited()
    preparation.prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_requires_bound_runtime_before_sql():
    token = source._FACTORY.set(None)
    database = SimpleNamespace(transaction=Mock(), scalar=AsyncMock())
    try:
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError, match="state"):
            await store.prepare_admitted_rooted_graph_dataset("synthetic-acquisition", database=database)
    finally:
        source._FACTORY.reset(token)
    database.transaction.assert_not_called()
    database.scalar.assert_not_awaited()


@pytest.fixture
def materialization(issued_scope, monkeypatch):
    scope = issued_scope
    scope.runtime.connection.fetchval.return_value = {"closed": True, "valid": True, "rows": 0, "last_query_id": None}
    scope.proof = SimpleNamespace(current=exact_current())
    scope.admission = twin_admission()
    scope.identity = dataset_identity()
    scope.counts = resource_counts()
    scope.relations = {"provider_directory_dataset_resource": "resource_heap"}
    scope.retained = AsyncMock(return_value=None)
    scope.seal = AsyncMock(return_value=object())
    monkeypatch.setattr(candidate, "retained_rooted_handoff", scope.retained)
    monkeypatch.setattr(candidate, "seal_rooted_handoff", scope.seal)
    monkeypatch.setattr(
        store, "require_provider_directory_rooted_graph_admission", AsyncMock(return_value=scope.admission)
    )
    monkeypatch.setattr(store, "_preflight_counts", AsyncMock(return_value=scope.counts))
    monkeypatch.setattr(store, "_insert_headers", AsyncMock())
    monkeypatch.setattr(
        store,
        "materialize_provider_directory_rooted_graph_dataset",
        AsyncMock(return_value=SimpleNamespace(resource_counts=scope.counts)),
    )
    monkeypatch.setattr(store, "build_provider_directory_dataset_serving_relations", AsyncMock())
    monkeypatch.setattr(store, "_dataset_hash", AsyncMock(return_value="a" * 64))
    scope.calls = Mock()
    for name, callback in (
        ("lock", scope.database.scalar),
        ("retained", scope.retained),
        ("counts", store._preflight_counts),
        ("headers", store._insert_headers),
        ("materialize", store.materialize_provider_directory_rooted_graph_dataset),
        ("serving", store.build_provider_directory_dataset_serving_relations),
        ("hash", store._dataset_hash),
        ("seal", scope.seal),
    ):
        scope.calls.attach_mock(callback, name)

    @asynccontextmanager
    async def candidate_storage(database, dataset_id, **options):
        assert database is scope.database and dataset_id == scope.identity.dataset_id
        assert options["kind"] == "rooted" and options["detached"] is True
        scope.runtime.events.append("candidate_enter")
        yield scope.relations
        scope.runtime.events.append("resource_indexes_ready")
        await options["finish"]()
        scope.runtime.events.append("candidate_exit")

    monkeypatch.setattr(store, "prepare_dataset_candidate", candidate_storage)
    return scope


async def _materialize(scope):
    return await store._prepare_rooted_candidate(
        scope.database, scope.runtime, scope.proof, scope.admission.publication_acquisition_id, 7
    )


@pytest.mark.asyncio
async def test_detached_candidate_materializes_finishes_hashes_then_seals(materialization):
    scope = materialization
    sealed_handoff = await _materialize(scope)
    assert sealed_handoff is scope.seal.return_value
    assert scope.runtime.transactions == [(scope.database, {"isolation": "repeatable read"})]
    assert [call[0] for call in scope.calls.mock_calls] == [
        "lock",
        "retained",
        "counts",
        "headers",
        "materialize",
        "serving",
        "hash",
        "seal",
    ]
    assert scope.runtime.events == [
        "transaction_enter",
        "candidate_enter",
        "resource_indexes_ready",
        "candidate_exit",
        "transaction_exit",
    ]
    store._insert_headers.assert_awaited_once_with(scope.database, scope.identity, scope.admission, scope.counts)
    store.materialize_provider_directory_rooted_graph_dataset.assert_awaited_once_with(
        scope.database, scope.identity, publication_run_id=scope.admission.publication_run_id, batch_size=7
    )
    store.build_provider_directory_dataset_serving_relations.assert_awaited_once_with(
        scope.database,
        scope.identity.dataset_id,
        build_run_id=scope.admission.publication_run_id,
        expected_acquisition_root_run_id=scope.identity.acquisition_root_run_id,
    )
    store._dataset_hash.assert_awaited_once_with(
        scope.database, scope.identity.dataset_id, resource_relation='"private_candidate"."resource_heap"'
    )
    assert scope.seal.await_args.args[0] is scope.runtime
    assert scope.seal.await_args.args[2:] == (
        scope.identity,
        scope.admission,
        scope.relations,
        scope.counts,
        "a" * 64,
        scope.proof,
    )


@pytest.mark.asyncio
async def test_candidate_rechecks_retained_identity_inside_its_lock(materialization):
    scope = materialization
    retained = object()
    scope.retained.return_value = retained
    assert await _materialize(scope) is retained
    scope.database.scalar.assert_awaited_once()
    assert scope.database.scalar.await_args.kwargs == {"identity": "rooted_candidate:" + scope.identity.dataset_id}
    store._preflight_counts.assert_not_awaited()
    store._insert_headers.assert_not_awaited()
    scope.seal.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["foreign_current", "lock", "counts", "cancel", "hash", "seal"])
async def test_failed_candidate_does_not_escape_transaction_or_seal_partial_data(materialization, failure):
    scope = materialization
    expected = ProviderDirectoryRootedGraphPublicationError
    if failure == "foreign_current":
        scope.proof.current = replace(scope.proof.current, dataset_hash="f" * 64)
    elif failure == "lock":
        scope.database.scalar.return_value = False
    elif failure == "counts":
        store.materialize_provider_directory_rooted_graph_dataset.return_value.resource_counts = {}
    elif failure == "cancel":
        expected = asyncio.CancelledError
        store.materialize_provider_directory_rooted_graph_dataset.side_effect = expected()
    else:
        expected = RuntimeError
        target = store._dataset_hash if failure == "hash" else scope.seal
        target.side_effect = RuntimeError("synthetic failed " + failure)
    with pytest.raises(expected) as rejected:
        await _materialize(scope)
    assert scope.runtime.exits == [rejected.value]
    if failure != "seal":
        scope.seal.assert_not_awaited()
    if failure in {"foreign_current", "lock", "counts", "cancel"}:
        store._dataset_hash.assert_not_awaited()
        store.build_provider_directory_dataset_serving_relations.assert_not_awaited()
    if failure in {"foreign_current", "lock"}:
        store._insert_headers.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_committed_readback_recovers_exact_projection_without_materialization(monkeypatch, encoded):
    ready = readiness()
    document_by_field = {"identity": {"dataset_id": ready.dataset_id}, "dataset_hash": ready.dataset_hash}
    row_by_field = {"state": "published", "handoff": json.dumps(document_by_field) if encoded else document_by_field}
    database = object()
    load = AsyncMock(return_value=ready)
    monkeypatch.setattr(store, "load_dataset_readiness", load)
    result = await store.read_prepared_rooted_publication(row_by_field, database=database)
    assert result.readiness is ready and result.replayed is True
    load.assert_awaited_once_with(ready.dataset_id, database=database)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unpublished", "missing", "hash_drift"])
async def test_readback_refuses_unpublished_missing_or_changed_projection(monkeypatch, failure):
    ready = readiness()
    row_by_field = {
        "state": "frozen" if failure == "unpublished" else "published",
        "handoff": {
            "identity": {"dataset_id": ready.dataset_id},
            "dataset_hash": "f" * 64 if failure == "hash_drift" else ready.dataset_hash,
        },
    }
    load = AsyncMock(return_value=None if failure == "missing" else ready)
    monkeypatch.setattr(store, "load_dataset_readiness", load)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError, match="state"):
        await store.read_prepared_rooted_publication(row_by_field, database=object())
    assert load.await_count == int(failure != "unpublished")


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", [0, 1, 2])
async def test_readback_uses_exact_canonical_connection_and_rejects_ambiguous_rows(monkeypatch, row_count):
    ready = readiness()
    prepared = SimpleNamespace(get_attributes=lambda: [], fetch=AsyncMock(return_value=[asdict(ready)] * row_count))
    connection = SimpleNamespace(prepare=AsyncMock(return_value=prepared))
    fallback = AsyncMock(side_effect=AssertionError("ordinary reader used"))
    monkeypatch.setattr(store, "load_dataset_readiness", fallback)
    row_by_field = {
        "state": "published",
        "handoff": {"identity": {"dataset_id": ready.dataset_id}, "dataset_hash": ready.dataset_hash},
    }
    if row_count == 1:
        publication = await store.read_prepared_rooted_publication(row_by_field, connection=connection)
        assert publication.readiness == ready and publication.replayed is True
    else:
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError, match="state"):
            await store.read_prepared_rooted_publication(row_by_field, connection=connection)
    fallback.assert_not_awaited()
    connection.prepare.assert_awaited_once()
    prepared.fetch.assert_awaited_once_with(ready.dataset_id)
