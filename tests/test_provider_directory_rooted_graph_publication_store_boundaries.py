# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from datetime import date
from types import SimpleNamespace

import pytest

from process import (
    provider_directory_rooted_graph_publication_readiness_store as readiness_store,
)
from process import provider_directory_rooted_graph_publication_store as store
from process import provider_directory_rooted_graph_publication_store_support as support
from process.provider_directory_rooted_graph_publication import (
    ProviderDirectoryRootedGraphPublicationError,
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
    assert support.publication_row_fields(SimpleNamespace(_mapping={"id": 1})) == {"id": 1}
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
async def test_dataset_hash_streams_the_existing_canonical_bytes_and_default_order() -> None:
    identity = dataset_identity()
    first_by_field = {"resource_type": "Practitioner", "resource_id": "id-1", "payload_hash": "a" * 64}
    second_by_field = {"resource_type": "Practitioner", "resource_id": "id.2", "payload_hash": "b" * 64}
    expected_bytes = b"\n".join(
        json.dumps(tuple(row.values()), separators=(",", ":")).encode() for row in (first_by_field, second_by_field)
    )
    database = _ScriptedDatabase(rows=([first_by_field], [second_by_field], []))
    assert await store._dataset_hash(database, identity.dataset_id) == hashlib.sha256(expected_bytes).hexdigest()
    assert all("COLLATE" not in statement and "string_agg" not in statement for _, statement, _ in database.calls)
    assert database.calls[0][2] == {"dataset_id": identity.dataset_id, "batch_size": 4096}
    assert database.calls[1][2]["cursor_id"] == "id-1"
    assert database.calls[2][2]["cursor_id"] == "id.2"
    assert await store._dataset_hash(_ScriptedDatabase(rows=([],)), identity.dataset_id) == hashlib.sha256().hexdigest()
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await store._dataset_hash(_ScriptedDatabase(rows=([{"resource_type": None}],)), identity.dataset_id)


@pytest.mark.asyncio
async def test_ambient_publication_requires_a_prepared_named_generation():
    database = _ScriptedDatabase()
    with pytest.raises(RuntimeError, match="prepared named generation through the Source operator"):
        await store.publish_admitted_rooted_graph_dataset(
            "pdrga_" + "1" * 48,
            database=database,
            batch_size=64,
        )
    assert database.calls == []
