# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from process import (
    provider_directory_rooted_graph_publication_materialization as materialization,
)
from process.provider_directory_rooted_graph_publication import (
    ProviderDirectoryRootedGraphPublicationError,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    resource_counts,
    twin_admission,
)


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


def _raw_row(
    resource_type: str = "Organization",
    resource_id: str = "org.synthetic-1",
    payload_sha256: str = "a" * 64,
) -> dict[str, object]:
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_sha256": payload_sha256,
        "payload_json_text": json.dumps(
            {"resourceType": resource_type, "id": resource_id}
        ),
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
    }
    return resource_by_field, evidence_by_field


def test_materialization_result_and_schema_boundaries(monkeypatch) -> None:
    counts = resource_counts()
    materialized = materialization.ProviderDirectoryRootedGraphMaterialization(counts)
    assert materialized.resource_count == sum(counts.values())
    with pytest.raises(ValueError, match="materialization_invalid"):
        materialization.ProviderDirectoryRootedGraphMaterialization({})
    with pytest.raises(ValueError, match="materialization_invalid"):
        materialization.ProviderDirectoryRootedGraphMaterialization(
            {**counts, "Practitioner": 0}
        )

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
    database = _ScriptedDatabase(statuses=(1, 1))

    await materialization._insert_rows(database, [])
    await materialization._insert_rows(database, [pair])

    assert len(database.status_calls) == 2
    assert "jsonb_to_recordset" in database.status_calls[0][0]
    bad_database = _ScriptedDatabase(statuses=(1, 0))
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await materialization._insert_rows(bad_database, [pair])
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._require_insert_counts([pair], 0, 1)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("resource_type", "PractitionerX"),
        ("resource_id", "bad/id"),
        ("payload_hash", "bad"),
        ("payload_json", []),
    ),
)
def test_resource_record_rejects_malformed_normalization(field_name, value) -> None:
    normalized_by_field = {
        "resource_type": "Organization",
        "resource_id": "org.synthetic-1",
        "payload_hash": "a" * 64,
        "payload_json": {"resourceType": "Organization", "id": "org.synthetic-1"},
    }
    normalized_by_field[field_name] = value
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._resource_record(normalized_by_field, "dataset")


@pytest.mark.asyncio
async def test_root_practitioner_copy_requires_both_exact_counts() -> None:
    identity = dataset_identity()
    copied = await materialization._copy_root_practitioners(
        _ScriptedDatabase(statuses=(1, 1)),
        identity,
    )
    assert copied == 1
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await materialization._copy_root_practitioners(
            _ScriptedDatabase(statuses=(0,)),
            identity,
        )
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await materialization._copy_root_practitioners(
            _ScriptedDatabase(statuses=(1, 0)),
            identity,
        )


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
    assert parameters["batch_size"] == 17
    assert parameters["batch_payload_bytes"] == 32 * 1024 * 1024


def test_raw_and_normalized_graph_pair_bind_exact_key(monkeypatch) -> None:
    identity = dataset_identity()
    fields = _raw_row()
    assert (
        materialization._raw_graph_resource(
            fields,
            ("Organization", "org.synthetic-1"),
        )["id"]
        == "org.synthetic-1"
    )
    for invalid_text in ("not-json", "[]"):
        with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
            materialization._raw_graph_resource(
                {**fields, "payload_json_text": invalid_text},
                ("Organization", "org.synthetic-1"),
            )
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._raw_graph_resource(
            fields,
            ("Organization", "different"),
        )

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
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        materialization._materialized_graph_pair(
            {**fields, "resource_id": "different"},
            identity,
            twin_admission().publication_run_id,
        )


@pytest.mark.asyncio
async def test_graph_materialization_deduplicates_only_identical_raw_hashes(
    monkeypatch,
) -> None:
    identity = dataset_identity()
    repeated = _raw_row()
    endpoint = _raw_row("Endpoint", "endpoint.synthetic-1", "d" * 64)
    pages = iter(([repeated, repeated], [endpoint], []))
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

    conflicting_pages = iter(([repeated, {**repeated, "payload_sha256": "e" * 64}],))

    async def load_conflict(*_arguments, **_keywords):
        return next(conflicting_pages)

    monkeypatch.setattr(materialization, "_load_graph_page", load_conflict)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        await materialization._materialize_graph_rows(
            object(),
            identity,
            publication_run_id=twin_admission().publication_run_id,
            batch_size=8,
        )


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
    materialized = (
        await materialization.materialize_provider_directory_rooted_graph_dataset(
            object(),
            identity,
            publication_run_id=run_id,
            batch_size=4096,
        )
    )
    assert materialized.resource_counts == resource_counts()
