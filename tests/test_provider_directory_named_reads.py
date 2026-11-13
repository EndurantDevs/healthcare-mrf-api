# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read published directory payloads through portable logical edition names."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control
from api import provider_directory_control_catalog as catalog
from api import provider_directory_source_outcomes as outcomes
from api.endpoint import npi
from api.provider_list_sql import _address_phone_candidates_cte
from process import provider_directory_named_edition as named
from tests.test_provider_directory_source_outcomes import (
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    SOURCE_IDS,
    _catalog,
    _dataset_row,
    _expected_outcome_summary,
    _identity_proof,
    _MappingResult,
    _metadata,
    _relation_metadata,
)


def _connection():
    header = _dataset_row()
    header["previous_dataset_id"] = None
    header["publication_metadata_json"] = _metadata(
        outcome_resource_counts_v1=_identity_proof(), **_relation_metadata()
    )
    pointer_by_field = dict(
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        schema_name="directory_edition_example",
        previous_dataset_id=None,
        previous_schema_name=None,
        aliases=list(SOURCE_IDS),
    )
    return SimpleNamespace(
        copy_to_table=True,
        header=header,
        pointer=pointer_by_field,
        fetchval=AsyncMock(return_value="provider_directory_editions.current_edition"),
        fetch=AsyncMock(side_effect=lambda *_args: [deepcopy(pointer_by_field)]),
        fetchrow=AsyncMock(side_effect=lambda *_args: deepcopy(header)),
        execute=AsyncMock(),
    )


async def test_absent_named_pointer_preserves_legacy_read():
    connection = _connection()
    connection.fetchval.return_value = None
    assert await named.named_current_editions(connection, "mrf") == ()
    connection.fetch.assert_not_awaited()
    assert named.source_relation_sql("mrf", named.HEADER, columns=("dataset_id",)) == '"mrf"."' + named.HEADER + '"'


async def test_logical_read_is_portable_and_keeps_the_pinned_predecessor():
    original, restored = _connection(), _connection()
    editions = await named.named_current_editions(original, "mrf", SOURCE_IDS)
    assert await named.named_current_editions(restored, "mrf", SOURCE_IDS) == editions
    original.pointer.update(dataset_id="new-dataset", schema_name="new_edition")
    sql = named.source_relation_sql(
        "mrf", "provider_directory_dataset_resource", editions, columns=("dataset_id", "resource_id", "payload_json")
    )
    assert 'FROM "directory_edition_example".' in sql and "new_edition" not in sql
    assert "WHERE dataset_id='" + DATASET_ID + "'" in sql
    assert editions[0].header["acquisition_root_run_id"] == ROOT_RUN_ID
    assert all("oid" not in call.args[0].lower() for call in original.fetchrow.await_args_list)


@pytest.mark.parametrize("damage", ["missing", "dataset", "endpoint", "status", "sources", "schema"])
async def test_malformed_named_edition_fails_closed(damage):
    connection = _connection()
    if damage == "missing":
        connection.fetchrow.side_effect = None
        connection.fetchrow.return_value = None
    elif damage == "schema":
        connection.pointer["schema_name"] = "pg_catalog"
    elif damage == "sources":
        connection.header["publication_metadata_json"]["source_ids"] = ["unregistered"]
    else:
        connection.header[{"dataset": "dataset_id", "endpoint": "endpoint_id", "status": "status"}[damage]] = "other"
    with pytest.raises(RuntimeError, match="named_edition_invalid"):
        await named.named_current_editions(connection, "mrf")


async def test_nonempty_rollback_preserves_the_original_edition_header():
    connection = _connection()
    connection.pointer.update(
        dataset_id="edition-b",
        schema_name="edition_b",
        previous_dataset_id=DATASET_ID,
        previous_schema_name="edition_a",
    )
    connection.header.update(dataset_id="edition-b", previous_dataset_id=DATASET_ID)
    pinned = await named.named_current_editions(connection, "mrf")
    connection.pointer.update(
        dataset_id=DATASET_ID,
        schema_name="edition_a",
        previous_dataset_id="edition-b",
        previous_schema_name="edition_b",
    )
    connection.header.update(dataset_id=DATASET_ID, previous_dataset_id=None)
    restored = await named.named_current_editions(connection, "mrf")
    assert restored[0].schema_name == "edition_a" and restored[0].header["previous_dataset_id"] is None
    assert pinned[0].schema_name == "edition_b" and pinned[0].header["previous_dataset_id"] == DATASET_ID
    assert 'FROM "edition_b".' in named.source_relation_sql(
        "mrf", "provider_directory_dataset_resource", pinned, columns=("dataset_id", "resource_id")
    )


async def test_legacy_catalog_predecessor_restores_its_header_and_payload_once():
    connection = _connection()
    connection.pointer.update(
        dataset_id="edition-b",
        schema_name="edition_b",
        previous_dataset_id=DATASET_ID,
        previous_schema_name="mrf",
    )
    connection.header.update(dataset_id="edition-b", previous_dataset_id=DATASET_ID)
    pinned = await named.named_current_editions(connection, "mrf")
    connection.pointer.update(
        dataset_id=DATASET_ID,
        schema_name="mrf",
        previous_dataset_id="edition-b",
        previous_schema_name="edition_b",
    )
    connection.header.update(dataset_id=DATASET_ID, previous_dataset_id=None)
    restored = await named.named_current_editions(connection, "mrf")
    assert restored[0].schema_name == "mrf"
    assert restored[0].header["previous_dataset_id"] is None
    for table_name in (named.HEADER, "provider_directory_dataset_resource"):
        relation = named.source_relation_sql("mrf", table_name, restored, columns=("dataset_id",))
        assert relation.count("WHERE dataset_id='" + DATASET_ID + "'") == 1
        assert "dataset_id NOT IN ('" + DATASET_ID + "')" in relation
        assert "edition_b" not in relation
    registration = named.source_registration_sql("mrf", restored)
    assert "source_id IN ('source-a','source-b')" in registration
    assert 'FROM "mrf"."provider_directory_source"' in registration
    assert 'FROM "edition_b".' in named.source_relation_sql(
        "mrf", "provider_directory_dataset_resource", pinned, columns=("dataset_id",)
    )


async def test_authenticated_source_catalog_reads_named_header(monkeypatch):
    connection = _connection()

    @asynccontextmanager
    async def reader():
        yield connection

    auth_calls = []
    monkeypatch.setattr(control, "_require_control_auth", lambda request: auth_calls.append(request))
    monkeypatch.setattr(control.db, "transaction", reader)
    monkeypatch.setattr(catalog, "provider_directory_source_catalog", _catalog)
    monkeypatch.setattr(catalog, "current_profile_selection_request", AsyncMock(return_value=None))
    monkeypatch.setattr(catalog, "cms_serving_candidate", AsyncMock(return_value=None))
    monkeypatch.setattr(outcomes.db, "execute", AsyncMock(return_value=_MappingResult([])))
    request = SimpleNamespace()
    result = await control.control_provider_directory_sources(request)
    payload = json.loads(result.body)
    assert auth_calls == [request]
    assert payload["items"][0]["current_outcome_summary"] == {
        **_expected_outcome_summary(),
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": "a" * 64,
    }
    assert connection.fetch.await_count == 1
    assert str(connection.execute.await_args.args[0]) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"


async def test_source_catalog_authentication_precedes_named_reads(monkeypatch):
    def refuse(_request):
        raise PermissionError("authentication required")

    reader = AsyncMock(side_effect=AssertionError("unauthorized database read"))
    monkeypatch.setattr(control, "_require_control_auth", refuse)
    monkeypatch.setattr(control.db, "transaction", reader)
    with pytest.raises(PermissionError):
        await control.control_provider_directory_sources(SimpleNamespace())
    reader.assert_not_called()


async def test_payload_phone_and_overlay_queries_use_the_same_named_edition():
    editions = await named.named_current_editions(_connection(), "mrf")
    queries = (
        npi._named_role_evidence_sql("mrf", {}, editions=editions),
        npi._provider_directory_affiliation_evidence_sql("mrf", True, editions=editions),
        npi._provider_directory_overlay_query_sql(set(), editions=editions),
        str(npi._location_status_query("mrf", "mrf.provider_directory_address_overlay", editions=editions)),
        _address_phone_candidates_cte("mrf.entity_address_unified", editions=editions),
    )
    for query in queries:
        assert 'FROM "directory_edition_example"."provider_directory_endpoint_dataset"' in query
        assert 'FROM "directory_edition_example"."provider_directory_dataset_resource"' in query
        assert 'FROM "mrf"."provider_directory_source"' in query
        assert "WHERE dataset_id='" + DATASET_ID + "'" in query


@pytest.mark.parametrize("affiliation", [False, True])
async def test_evidence_loader_passes_the_named_relations_to_its_query(monkeypatch, affiliation):
    connection = _connection()
    flags = dict.fromkeys(
        (
            "provider_directory_network_catalog",
            "provider_directory_organization_affiliation",
            "provider_directory_dataset_network_plan",
            "provider_directory_dataset_affiliation_organization",
            "provider_directory_dataset_insurance_plan",
        ),
        True,
    )
    monkeypatch.setattr(npi, "_provider_directory_evidence_tables", AsyncMock(return_value=flags))
    monkeypatch.setattr(npi, "_disable_provider_directory_evidence_jit", AsyncMock())
    execute = AsyncMock(return_value=_MappingResult([]))
    monkeypatch.setattr(npi, "_execute_stmt", execute)
    loader = (
        npi._fetch_provider_directory_affiliation_evidence_map
        if affiliation
        else npi._fetch_provider_directory_role_evidence_map
    )
    assert await loader([(SOURCE_IDS[0], "role-1")], session=connection) == {}
    query = str(execute.await_args.args[0])
    assert 'FROM "directory_edition_example"."provider_directory_dataset_network_plan"' in query
    assert 'FROM "directory_edition_example"."provider_directory_dataset_insurance_plan"' in query
    assert 'FROM "directory_edition_example"."provider_directory_dataset_resource"' in query
    assert connection.fetch.await_args.args[-1] == [SOURCE_IDS[0]]


async def test_named_catalog_keeps_a_newer_validated_candidate(monkeypatch):
    import datetime as dt

    editions = await named.named_current_editions(_connection(), "mrf")
    candidate = _dataset_row(
        dataset_id="candidate-new",
        status="validated",
        is_current=False,
        validated_at=dt.datetime(2035, 1, 1),
        published_at=None,
    )
    monkeypatch.setattr(outcomes.db, "execute", AsyncMock(return_value=_MappingResult([candidate])))
    result = await outcomes.enrich_provider_directory_source_catalog(_catalog(), editions=editions)
    assert result["items"][0]["outcome_summary"]["dataset_id"] == "candidate-new"
    assert result["items"][0]["current_outcome_summary"]["dataset_id"] == DATASET_ID


async def test_named_payload_keeps_unselected_catalog_aliases_out_of_its_scope():
    connection = _connection()
    connection.pointer["aliases"].append("unselected-alias")
    editions = await named.named_current_editions(connection, "mrf")
    registration = named.source_registration_sql("mrf", editions)
    assert "unselected-alias" not in registration
    assert "source_id IN ('source-a','source-b')" in registration
    assert "endpoint_id IS NULL OR endpoint_id NOT IN" in registration


@pytest.mark.parametrize("source_id", ["source-a", "unselected-alias"])
async def test_profile_lookup_uses_only_the_named_editions_selected_aliases(monkeypatch, source_id):
    import importlib

    directory = importlib.import_module("process.provider_directory_fhir")
    connection = _connection()
    connection.pointer["aliases"].append("unselected-alias")
    editions = await named.named_current_editions(connection, "mrf", [source_id])

    @asynccontextmanager
    async def selection(*_arguments):
        yield editions

    async def first(query, **parameters):
        assert parameters["source_id"] == source_id
        assert named.source_registration_sql("mrf", editions) + " AS source" in query
        assert "source_id IN ('source-a','source-b')" in query
        assert "unselected-alias" not in query
        return {"dataset_id": DATASET_ID} if source_id == "source-a" else None

    monkeypatch.setattr(named, "artifact_selection_snapshot", selection)
    monkeypatch.setattr(directory.db, "first", first)
    monkeypatch.setattr(directory, "_schema", lambda: "mrf")
    assert await named.current_profile_dataset_map(directory, source_id) == (
        {"dataset_id": DATASET_ID} if source_id == "source-a" else {}
    )
