# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import fields
from datetime import date
import json
from types import SimpleNamespace

import pytest

from process import (
    provider_directory_rooted_graph_publication_readiness_store as readiness_store,
)
from process import provider_directory_rooted_graph_publication_store as store
from process import provider_directory_rooted_graph_publication_store_support as support
from process.provider_directory_dataset_scoped_publication import (
    ProviderDirectoryDatasetScopedPublicationError,
)
from process.provider_directory_rooted_graph_publication import (
    ProviderDirectoryRootedGraphPublicationError,
    ProviderDirectoryRootedGraphPublicationResult,
)
from process.provider_directory_rooted_graph_publication_materialization import (
    ProviderDirectoryRootedGraphMaterialization,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    exact_current,
    readiness,
    resource_counts,
    twin_admission,
)


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error) -> bool:
        return False


class _ScriptedDatabase:
    def __init__(
        self,
        *,
        scalars=(),
        statuses=(),
        rows=(),
        first_rows=(),
    ) -> None:
        self.scalars = list(scalars)
        self.statuses = list(statuses)
        self.rows = list(rows)
        self.first_rows = list(first_rows)
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def scalar(self, statement: str, **parameters: object):
        self.calls.append(("scalar", statement, parameters))
        return self.scalars.pop(0)

    async def status(self, statement: str, **parameters: object) -> int:
        self.calls.append(("status", statement, parameters))
        return self.statuses.pop(0)

    async def all(self, statement: str, **parameters: object):
        self.calls.append(("all", statement, parameters))
        return self.rows.pop(0)

    async def first(self, statement: str, **parameters: object):
        self.calls.append(("first", statement, parameters))
        return self.first_rows.pop(0)


def _readiness_row(*, counts_as_json: bool = False) -> dict[str, object]:
    ready = readiness()
    row_by_field = {field.name: getattr(ready, field.name) for field in fields(ready)}
    if counts_as_json:
        row_by_field["resource_counts"] = json.dumps(row_by_field["resource_counts"])
    return row_by_field


def test_publication_store_support_rejects_schema_and_row_drift(monkeypatch) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    assert support.publication_table("dataset") == '"mrf"."dataset"'
    assert support.publication_row_fields(None) == {}
    assert support.publication_row_fields(SimpleNamespace(_mapping={"id": 1})) == {
        "id": 1
    }
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        support.publication_row_fields(object())
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime")
    monkeypatch.setenv("DB_SCHEMA", "legacy")
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        support.publication_table("dataset")
    monkeypatch.setenv("DB_SCHEMA", "runtime")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        support.publication_table("dataset")


def test_readiness_row_revalidates_json_and_projection_boundaries(monkeypatch) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    ready = readiness_store._readiness_from_row(_readiness_row(counts_as_json=True))
    assert ready == readiness()
    row = _readiness_row()
    row["semantic_projection_as_of"] = date.fromisoformat("2026-08-10")
    assert readiness_store._readiness_from_row(row) == readiness()
    for invalid_row in (
        object(),
        {**_readiness_row(), "resource_counts": "not-json"},
        {**_readiness_row(), "semantic_projection_as_of": object()},
        {**_readiness_row(), "dataset_id": "bad"},
    ):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            readiness_store._readiness_from_row(invalid_row)
    assert "jsonb_build_object" in readiness_store._readiness_select("true")


@pytest.mark.asyncio
async def test_readiness_loaders_return_none_or_exact_rows() -> None:
    ready = readiness()
    database = _ScriptedDatabase(first_rows=(None, _readiness_row(), _readiness_row()))
    assert (
        await readiness_store.load_dataset_readiness(
            ready.dataset_id,
            database=database,
        )
        is None
    )
    assert (
        await readiness_store.load_dataset_readiness(
            ready.dataset_id,
            database=database,
        )
        == ready
    )
    assert (
        await readiness_store.load_replay_readiness(
            database,
            ready.publication_acquisition_id,
        )
        == ready
    )


@pytest.mark.asyncio
async def test_preflight_counts_reject_conflict_and_unexpected_counts() -> None:
    admission = twin_admission()
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as conflict:
        await store._preflight_counts(
            _ScriptedDatabase(scalars=(True,)),
            admission,
        )
    assert conflict.value.code == "content"

    database = _ScriptedDatabase(
        scalars=(False,),
        rows=([{"resource_type": "Organization", "count": 2}],),
    )
    counts = await store._preflight_counts(database, admission)
    assert counts["Practitioner"] == 1
    assert counts["Organization"] == 2
    for count_row in (
        {"resource_type": "Practitioner", "count": 1},
        {"resource_type": "Organization", "count": -1},
        {"resource_type": "foreign", "count": 1},
    ):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            await store._preflight_counts(
                _ScriptedDatabase(scalars=(False,), rows=([count_row],)),
                admission,
            )


@pytest.mark.asyncio
async def test_orphan_and_header_insert_boundaries() -> None:
    identity = dataset_identity()
    admission = twin_admission()
    counts = resource_counts()
    await store._assert_no_orphan_parent(
        _ScriptedDatabase(scalars=(0,)),
        identity,
    )
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as orphan:
        await store._assert_no_orphan_parent(
            _ScriptedDatabase(scalars=(1,)),
            identity,
        )
    assert orphan.value.code == "source_drift"

    identity_fields = store._identity_header_by_field(identity)
    admission_fields = store._admission_header_by_field(admission)
    count_fields = store._count_header_by_field(counts)
    assert identity_fields["previous_dataset_id"] == identity.root_dataset_id
    assert admission_fields["graph_resource_count"] == admission.resource_count
    assert count_fields["resource_count"] == 1

    database = _ScriptedDatabase(statuses=(1, 1))
    await store._insert_headers(database, identity, admission, counts)
    assert len(database.calls) == 2
    for statuses in ((0, 1), (1, 0)):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            await store._insert_headers(
                _ScriptedDatabase(statuses=statuses),
                identity,
                admission,
                counts,
            )


@pytest.mark.asyncio
async def test_dataset_hash_and_atomic_status_transitions(monkeypatch) -> None:
    identity = dataset_identity()
    assert (
        await store._dataset_hash(
            _ScriptedDatabase(scalars=("a" * 64,)),
            identity.dataset_id,
        )
        == "a" * 64
    )
    for invalid_hash in (None, "bad"):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            await store._dataset_hash(
                _ScriptedDatabase(scalars=(invalid_hash,)),
                identity.dataset_id,
            )

    superseded_currents: list[object] = []

    async def supersede(_database, previous):
        superseded_currents.append(previous)

    monkeypatch.setattr(store, "supersede_exact_current_dataset", supersede)
    current = exact_current()
    await store._validate_and_publish(
        _ScriptedDatabase(
            scalars=("b" * 64,),
            statuses=(1, 1, 1, 1),
        ),
        identity,
        current,
    )
    assert superseded_currents == [current]
    for statuses in ((0, 1), (1, 0), (1, 1, 0, 1), (1, 1, 1, 0)):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            await store._validate_and_publish(
                _ScriptedDatabase(scalars=("b" * 64,), statuses=statuses),
                identity,
                current,
            )


async def _no_op(*_arguments, **_keywords) -> None:
    return None


@pytest.mark.asyncio
async def test_materialize_publish_pipeline_proves_counts_and_readiness(
    monkeypatch,
) -> None:
    """The atomic pipeline returns only its exact proved readiness."""
    identity = dataset_identity()
    admission = twin_admission()
    current = exact_current()
    counts_by_resource_type = resource_counts()
    ready = readiness()
    monkeypatch.setattr(store, "_assert_no_orphan_parent", _no_op)
    monkeypatch.setattr(store, "_insert_headers", _no_op)
    monkeypatch.setattr(
        store, "build_provider_directory_dataset_serving_relations", _no_op
    )
    monkeypatch.setattr(store, "_validate_and_publish", _no_op)

    async def preflight(*_arguments):
        return counts_by_resource_type

    async def materialize(*_arguments, **_keywords):
        return ProviderDirectoryRootedGraphMaterialization(counts_by_resource_type)

    async def load_ready(*_arguments, **_keywords):
        return ready

    monkeypatch.setattr(store, "_preflight_counts", preflight)
    monkeypatch.setattr(
        store,
        "materialize_provider_directory_rooted_graph_dataset",
        materialize,
    )
    monkeypatch.setattr(store, "load_dataset_readiness", load_ready)
    publication_result = await store._materialize_and_publish(
        object(),
        admission,
        current,
        64,
    )
    assert publication_result == ProviderDirectoryRootedGraphPublicationResult(
        ready, False
    )


@pytest.mark.asyncio
async def test_materialize_publish_pipeline_rejects_unproved_outputs(
    monkeypatch,
) -> None:
    """Count drift and absent readiness both fail the atomic pipeline."""
    admission = twin_admission()
    current = exact_current()
    counts_by_resource_type = resource_counts()
    monkeypatch.setattr(store, "_assert_no_orphan_parent", _no_op)
    monkeypatch.setattr(store, "_insert_headers", _no_op)
    monkeypatch.setattr(
        store, "build_provider_directory_dataset_serving_relations", _no_op
    )
    monkeypatch.setattr(store, "_validate_and_publish", _no_op)

    async def preflight(*_arguments):
        return counts_by_resource_type

    async def mismatched(*_arguments, **_keywords):
        return ProviderDirectoryRootedGraphMaterialization(
            {**counts_by_resource_type, "Organization": 1}
        )

    monkeypatch.setattr(store, "_preflight_counts", preflight)
    monkeypatch.setattr(
        store,
        "materialize_provider_directory_rooted_graph_dataset",
        mismatched,
    )
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await store._materialize_and_publish(object(), admission, current, 64)

    async def materialize(*_arguments, **_keywords):
        return ProviderDirectoryRootedGraphMaterialization(counts_by_resource_type)

    monkeypatch.setattr(
        store,
        "materialize_provider_directory_rooted_graph_dataset",
        materialize,
    )

    async def no_readiness(*_arguments, **_keywords):
        return None

    monkeypatch.setattr(store, "load_dataset_readiness", no_readiness)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await store._materialize_and_publish(object(), admission, current, 64)


@pytest.mark.asyncio
async def test_locked_result_handles_replay_drift_and_new_publication(
    monkeypatch,
) -> None:
    current = exact_current()
    admission = twin_admission()
    ready = readiness()
    expected = ProviderDirectoryRootedGraphPublicationResult(ready, False)

    async def lock_current(*_arguments, **_keywords):
        return current

    async def require_admission(*_arguments, **_keywords):
        return admission

    async def replay(*_arguments, **_keywords):
        return ready

    monkeypatch.setattr(store, "lock_exact_current_dataset", lock_current)
    monkeypatch.setattr(
        store,
        "require_provider_directory_rooted_graph_admission",
        require_admission,
    )
    monkeypatch.setattr(store, "load_replay_readiness", replay)
    replayed = await store._locked_publication_result(object(), "acquisition", 64)
    assert replayed.replayed is True

    async def no_replay(*_arguments, **_keywords):
        return None

    monkeypatch.setattr(store, "load_replay_readiness", no_replay)
    monkeypatch.setattr(store, "exact_current_matches_root", lambda *_args: False)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as drift:
        await store._locked_publication_result(object(), "acquisition", 64)
    assert drift.value.code == "foreign_current"

    monkeypatch.setattr(store, "exact_current_matches_root", lambda *_args: True)

    async def publish(*_arguments, **_keywords):
        return expected

    monkeypatch.setattr(store, "_materialize_and_publish", publish)
    assert (
        await store._locked_publication_result(object(), "acquisition", 64) == expected
    )


@pytest.mark.asyncio
async def test_public_store_validates_batch_and_maps_shared_errors(monkeypatch) -> None:
    for invalid_batch in (False, 0, 4097):
        with pytest.raises(ValueError, match="batch_size_invalid"):
            await store.publish_admitted_rooted_graph_dataset(
                "acquisition",
                database=object(),
                batch_size=invalid_batch,
            )

    ready = readiness()
    expected = ProviderDirectoryRootedGraphPublicationResult(ready, True)

    async def publish(*_arguments, **_keywords):
        return expected

    monkeypatch.setattr(store, "_locked_publication_result", publish)
    database = _ScriptedDatabase(scalars=(None,))
    assert (
        await store.publish_admitted_rooted_graph_dataset(
            "acquisition",
            database=database,
            batch_size=64,
        )
        == expected
    )
    assert "pg_advisory_xact_lock" in database.calls[0][1]

    for shared_code, expected_code in (
        ("foreign_current", "foreign_current"),
        ("source_drift", "source_drift"),
        ("both_current", "state"),
    ):

        async def fail(*_arguments, **_keywords):
            raise ProviderDirectoryDatasetScopedPublicationError(shared_code)

        monkeypatch.setattr(store, "_locked_publication_result", fail)
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as error:
            await store.publish_admitted_rooted_graph_dataset(
                "acquisition",
                database=_ScriptedDatabase(scalars=(None,)),
                batch_size=64,
            )
        assert error.value.code == expected_code
