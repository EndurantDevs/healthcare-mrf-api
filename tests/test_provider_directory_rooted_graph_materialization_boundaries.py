# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_candidate as candidate
from process import (
    provider_directory_rooted_graph_publication_materialization as materialization,
)
from process.provider_directory_rooted_graph_publication import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    ProviderDirectoryRootedGraphPublicationError,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    resource_counts,
    twin_admission,
)

directory = import_module("process.provider_directory_fhir")


class _ScriptedDatabase:
    def __init__(self, *, statuses=(), pages=()) -> None:
        self.statuses = list(statuses)
        self.pages = list(pages)
        self.status_calls: list[tuple[str, dict[str, object]]] = []
        self.all_calls: list[tuple[str, dict[str, object]]] = []

    async def status(self, statement: str, **parameters: object) -> int:
        self.status_calls.append((statement, parameters))
        return self.statuses.pop(0)

    async def all(self, statement: str, **parameters: object) -> list[object]:
        self.all_calls.append((statement, parameters))
        return self.pages.pop(0)


def test_candidate_copy_bounds_encoded_bytes_and_preserves_order(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 49)
    records = [("é" * 4,), ("second",), ("third",)]
    assert list(candidate._bounded_copy_batches(records)) == [records[:2], records[2:]]
    with pytest.raises(RuntimeError, match="candidate_record_too_large"):
        list(candidate._bounded_copy_batches([("é" * 17,)]))


def test_candidate_copy_reserves_binary_framing_and_jsonb_version(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 32)
    assert list(candidate._bounded_copy_batches([("éé",)])) == [[("éé",)]]
    with pytest.raises(RuntimeError, match="candidate_record_too_large"):
        list(candidate._bounded_copy_batches([("ééx",)]))


@asynccontextmanager
async def _candidate_catalog(monkeypatch, database, catalog):
    """Supply only native catalog data while retaining the real Source owner guards."""
    from process import provider_directory_generic_source_kind_candidate as native
    from process.provider_directory_generic_source_storage import CATALOG_SQL
    from tests.test_provider_directory_generic_source_policy import synthetic_source_publisher
    from tests.test_provider_directory_generic_source_storage import _catalog_row

    async with synthetic_source_publisher(monkeypatch, database, schema=directory._schema()) as (connection, _):
        fetch, fetchval = connection.fetch, connection.fetchval.side_effect

        async def catalog_rows(query, *args):
            if query == CATALOG_SQL:
                if catalog != "complete":
                    return catalog
                return [
                    _catalog_row() | {"relname": name.split('"')[3], "nspname": directory._schema(), "oid": index + 1}
                    for index, name in enumerate(args[0])
                ]
            return await fetch(query, *args)

        async def header_or_actor(query, *args):
            if query.startswith("SELECT status,is_current"):
                return {"status": "building", "is_current": False}
            return connection.fetchrow.return_value

        connection.fetch = AsyncMock(side_effect=catalog_rows)
        connection.fetchrow.side_effect = header_or_actor
        connection.fetchval.side_effect = lambda query, *args: (
            100 if query.startswith("SELECT oid::bigint FROM pg_roles") else fetchval(query, *args)
        )
        monkeypatch.setattr(native, "_close_grants", AsyncMock())
        yield connection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind, binding, message",
    (
        ("unsupported", object(), "kind_invalid"),
        ("rooted", None, "transaction_required"),
    ),
)
async def test_candidate_prepare_rejects_missing_authority_before_sql(kind, binding, message):
    """An invalid caller cannot allocate a candidate or enter materialization."""
    database = SimpleNamespace(_transaction_binding=lambda: binding, scalar=AsyncMock())
    expected = message if kind == "unsupported" else "publisher_connection_required"
    with pytest.raises((ValueError, RuntimeError), match=expected):
        async with candidate.prepare_dataset_candidate(database, "dataset", kind=kind):
            pytest.fail("invalid candidate preparation entered materialization")
    database.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("relations", (None, [], {}, "[]", "{}"))
async def test_candidate_prepare_rejects_malformed_mapping_without_finish(monkeypatch, relations):
    """Malformed native catalog data cannot route writes or attach a partition."""
    database = SimpleNamespace(scalar=AsyncMock())
    original = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    async with _candidate_catalog(monkeypatch, database, relations) as connection:
        with pytest.raises((ValueError, RuntimeError, TypeError), match="storage_missing|object of type"):
            async with candidate.prepare_dataset_candidate(database, "dataset", kind="rooted"):
                pytest.fail("invalid relation response entered materialization")
        assert sum(call.args[0].startswith("CREATE TABLE") for call in connection.execute.await_args_list) == 5
        assert not any("ATTACH PARTITION" in call.args[0] for call in connection.execute.await_args_list)
        connection.copy_records_to_table.assert_not_awaited()
    database.scalar.assert_not_awaited()
    assert directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == original


@pytest.mark.asyncio
async def test_candidate_short_copy_restores_scope_without_finish(monkeypatch):
    """A short native COPY aborts the prepared family before finish can run."""
    database = SimpleNamespace(scalar=AsyncMock())
    original = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    async with _candidate_catalog(monkeypatch, database, "complete") as connection:
        connection.copy_records_to_table.return_value = "COPY 0"
        with pytest.raises((ValueError, RuntimeError), match="copy_incomplete"):
            async with candidate.prepare_dataset_candidate(database, "dataset", kind="rooted"):
                await candidate.copy_dataset_candidate_rows(
                    database, "provider_directory_dataset_resource", ("id",), [("one",)]
                )
        connection.copy_records_to_table.assert_awaited_once()
        assert not any("ATTACH PARTITION" in call.args[0] for call in connection.execute.await_args_list)
    database.scalar.assert_not_awaited()
    assert directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "has_transaction, has_relation, count",
    (
        (False, True, 1),
        (True, False, 1),
        (True, True, 0),
        (True, True, 4097),
    ),
)
async def test_candidate_copy_rejects_invalid_context_and_bounds(monkeypatch, has_transaction, has_relation, count):
    """Context and row bounds are rejected before resolving a native connection."""
    root = "provider_directory_dataset_resource"
    database = SimpleNamespace()
    copier = AsyncMock()
    async with _candidate_catalog(monkeypatch, database, "complete") as connection:
        with pytest.raises(RuntimeError, match="^provider_directory_candidate_copy_invalid$"):
            async with candidate.prepare_dataset_candidate(database, "dataset", kind="rooted") as prepared:
                assert root in prepared and directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()[root]
                overrides = {} if has_relation else {root: None}
                with monkeypatch.context() as changed, directory._provider_directory_artifact_relation_scope(overrides):
                    binding = database._transaction_binding if has_transaction else lambda: None
                    changed.setattr(database, "_transaction_binding", binding)
                    changed.setattr(candidate, "_copy_driver", copier)
                    await candidate.copy_dataset_candidate_rows(database, root, ("id",), [("one",)] * count)
        copier.assert_not_awaited()
        connection.copy_records_to_table.assert_not_awaited()
        assert not any("ATTACH PARTITION" in call.args[0] for call in connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_candidate_copy_uses_the_prepared_native_binding(monkeypatch):
    """A valid row reaches COPY in the same physical Source session and exact private heap."""
    root = "provider_directory_dataset_resource"
    database = SimpleNamespace()
    columns, records = ("dataset_id", "resource_type", "resource_id"), [("dataset", "Organization", "org")]
    async with _candidate_catalog(monkeypatch, database, "complete") as connection:
        connection.copy_records_to_table.return_value = "COPY 1"
        with pytest.raises(RuntimeError, match="^unit_stop_after_copy$"):
            async with candidate.prepare_dataset_candidate(database, "dataset", kind="rooted") as prepared:
                assert await candidate.copy_dataset_candidate_rows(database, root, columns, records) == 1
                connection.copy_records_to_table.assert_awaited_once_with(
                    prepared[root], schema_name=directory._schema(), columns=columns, records=records
                )
                raise RuntimeError("unit_stop_after_copy")
        assert not any("ATTACH PARTITION" in call.args[0] for call in connection.execute.await_args_list)


def _raw_row(
    resource_type: str = "Organization",
    resource_id: str = "org.synthetic-1",
    payload_sha256: str = "a" * 64,
) -> dict[str, object]:
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_sha256": payload_sha256,
        "payload_json_text": json.dumps({"resourceType": resource_type, "id": resource_id}),
        "query_id": "pdrgq_" + "b" * 48,
        "attempt": 1,
        "closure_scope": "root",
    }


def _materialized_pair(fields, identity, _publication_run_id):
    resource_by_field = {
        "dataset_id": identity.dataset_id,
        "resource_type": fields["resource_type"],
        "resource_id": fields["resource_id"],
        "payload_hash": "c" * 64,
        "payload_json": {
            "resourceType": fields["resource_type"],
            "id": fields["resource_id"],
        },
    }
    evidence_by_field = {
        "dataset_id": identity.dataset_id,
        "resource_type": fields["resource_type"],
        "resource_id": fields["resource_id"],
        "origin_kind": "acquired_graph",
        "root_dataset_id": identity.root_dataset_id,
        "publication_acquisition_id": identity.publication_acquisition_id,
        "query_id": fields["query_id"],
        "attempt": fields["attempt"],
        "closure_scope": fields["closure_scope"],
        "source_payload_sha256": fields["payload_sha256"],
        "published_payload_hash": "c" * 64,
    }
    return resource_by_field, evidence_by_field


def test_materialization_result_and_schema_boundaries(monkeypatch) -> None:
    counts = resource_counts()
    materialized = materialization.ProviderDirectoryRootedGraphMaterialization(counts)
    assert materialized.resource_count == sum(counts.values())
    with pytest.raises(ValueError, match="materialization_invalid"):
        materialization.ProviderDirectoryRootedGraphMaterialization({})
    with pytest.raises(ValueError, match="materialization_invalid"):
        materialization.ProviderDirectoryRootedGraphMaterialization({**counts, "Practitioner": 0})

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime")
    monkeypatch.setenv("DB_SCHEMA", "legacy")
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._schema()
    monkeypatch.setenv("DB_SCHEMA", "runtime")
    assert materialization._table("resource") == '"runtime"."resource"'
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._schema()
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._row_fields(object())
    assert materialization._row_fields(SimpleNamespace(_mapping={"id": 1})) == {"id": 1}


@pytest.mark.asyncio
async def test_bounded_bulk_insert_counts_are_exact(monkeypatch) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    pair = _materialized_pair(_raw_row(), dataset_identity(), "unused")
    copied_batches = []

    async def copy_rows(_database, table, columns, records):
        copied_batches.append((table, dict(zip(columns, records[0], strict=True))))
        return len(records)

    monkeypatch.setattr(candidate, "copy_dataset_candidate_rows", copy_rows)
    await materialization._insert_rows(object(), [])
    await materialization._insert_rows(object(), [pair])
    assert len(copied_batches) == 2
    assert copied_batches[0][1]["acquired_resource_sha256"] is None
    assert json.loads(copied_batches[0][1]["payload_json"]) == pair[0]["payload_json"]
    assert copied_batches[1][1] == pair[1]
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._require_insert_counts([pair], 0, 1)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._require_insert_counts([pair], 1, 0)


@pytest.mark.asyncio
async def test_root_practitioner_copy_requires_both_exact_counts(monkeypatch) -> None:
    identity = dataset_identity()
    batches = []

    async def insert_rows(_database, pairs):
        batches.append(pairs)

    monkeypatch.setattr(materialization, "_insert_rows", insert_rows)
    resource_by_field = {
        "resource_id": "synthetic-root",
        "payload_hash": "a" * 64,
        "payload_json": '{"resourceType":"Practitioner"}',
    }
    database = _ScriptedDatabase(pages=([resource_by_field], []))
    assert await materialization._copy_root_practitioners(database, identity) == 1
    assert batches[0][0][1]["root_dataset_id"] == identity.root_dataset_id
    assert batches[0][0][1]["published_payload_hash"] == "a" * 64
    assert database.all_calls[0][1]["byte_limit"] == 32 * 1024 * 1024
    assert database.all_calls[1][1]["after_id"] == "synthetic-root"
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await materialization._copy_root_practitioners(_ScriptedDatabase(pages=([],)), identity)


@pytest.mark.asyncio
async def test_graph_page_query_binds_row_and_byte_caps(monkeypatch) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    database = _ScriptedDatabase(pages=([_raw_row()],))
    identity = dataset_identity()

    rows = await materialization._load_graph_page(
        database,
        identity,
        cursor=("", "", "", "", 0),
        batch_size=17,
    )

    assert len(rows) == 1
    statement, parameters = database.all_calls[0]
    assert "cumulative_payload_bytes" in statement
    assert "SELECT DISTINCT ON (resource_type, resource_id)" in statement
    assert parameters["batch_size"] == 17
    assert parameters["batch_payload_bytes"] == 32 * 1024 * 1024


def test_graph_pair_preserves_canonical_transform_and_source_evidence(monkeypatch) -> None:
    identity = dataset_identity()
    fields = _raw_row()

    def normalize(**_arguments):
        return {
            "resource_type": "Organization",
            "resource_id": "org.synthetic-1",
            "payload_hash": "c" * 64,
            "payload_json": {"resourceType": "Organization"},
        }

    monkeypatch.setattr(
        materialization,
        "materialize_provider_directory_dataset_fhir_resource",
        normalize,
    )
    resource_by_field, evidence_by_field = materialization._materialized_graph_pair(
        fields,
        identity,
        twin_admission().publication_run_id,
    )
    assert evidence_by_field["published_payload_hash"] == "c" * 64
    assert resource_by_field["resource_id"] == "org.synthetic-1"


@pytest.mark.asyncio
async def test_graph_materialization_streams_distinct_source_pages(
    monkeypatch,
) -> None:
    identity = dataset_identity()
    repeated = _raw_row()
    endpoint = _raw_row("Endpoint", "endpoint.synthetic-1", "d" * 64)
    pages = iter(([repeated], [endpoint], []))
    inserted_pages: list[list[object]] = []

    async def load_page(*_arguments, **_keywords):
        return next(pages)

    async def insert_rows(_database, pairs):
        inserted_pages.append(pairs)

    monkeypatch.setattr(materialization, "_load_graph_page", load_page)
    monkeypatch.setattr(materialization, "_insert_rows", insert_rows)
    monkeypatch.setattr(materialization, "_materialized_graph_pair", _materialized_pair)
    counts = await materialization._materialize_graph_rows(
        object(),
        identity,
        publication_run_id=twin_admission().publication_run_id,
        batch_size=8,
    )
    assert counts["Organization"] == 1
    assert counts["Endpoint"] == 1
    assert sum(len(page) for page in inserted_pages) == 2


@pytest.mark.asyncio
async def test_public_materializer_validates_inputs_and_combines_counts(
    monkeypatch,
) -> None:
    identity = dataset_identity()
    run_id = twin_admission().publication_run_id
    for invalid_case in (
        (object(), run_id, 1),
        (identity, "bad", 1),
        (identity, run_id, 0),
        (identity, run_id, 4097),
    ):
        with pytest.raises(ValueError, match="materialization_invalid"):
            await materialization.materialize_provider_directory_rooted_graph_dataset(
                object(),
                invalid_case[0],
                publication_run_id=invalid_case[1],
                batch_size=invalid_case[2],
            )

    async def copy_root(_database, _identity):
        return 1

    async def materialize_graph(*_arguments, **_keywords):
        return {
            resource_type: 0
            for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
            if resource_type != "Practitioner"
        }

    monkeypatch.setattr(materialization, "_copy_root_practitioners", copy_root)
    monkeypatch.setattr(materialization, "_materialize_graph_rows", materialize_graph)
    materialized = await materialization.materialize_provider_directory_rooted_graph_dataset(
        object(),
        identity,
        publication_run_id=run_id,
        batch_size=4096,
    )
    assert materialized.resource_counts == resource_counts()
