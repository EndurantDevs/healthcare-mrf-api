# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import NotFound, SanicException

from process import uhc_provider_file_catalog as catalog_module
from process import uhc_provider_file_catalog_store as catalog_store
from process import uhc_provider_file_catalog_types as catalog_types


def _entry(family: str, section: str, index: int) -> dict:
    prefix = "Providers" if section == "providers" else "Plans"
    name = f"JSON_{prefix}_{family.upper()}_{index:03d}.json"
    return {
        "name": name,
        "date": "2026-07-20T00:00:00Z",
        "size": 1_000 + index,
        "blobPath": f"ui/{family}/{section}/{name}",
    }


def _live_payloads() -> dict:
    return {
        "cs": {
            "providers": [_entry("cs", "providers", index) for index in range(53)],
        },
        "ifp": {
            "providers": [_entry("ifp", "providers", index) for index in range(25)],
            "plans": [_entry("ifp", "plans", index) for index in range(24)],
        },
    }


class _MappedResult:
    def __init__(self, database_records=()):
        self.database_records = list(database_records)

    def mappings(self):
        return self

    def first(self):
        return self.database_records[0] if self.database_records else None

    def all(self):
        return list(self.database_records)


class _PersistenceSession:
    def __init__(self, *, mutate_file=False, mutate_semantic=False, mutate_raw=False):
        self.mutate_file = mutate_file
        self.mutate_semantic = mutate_semantic
        self.mutate_raw = mutate_raw
        self.statements = []
        self.semantic_parameters_by_field = None
        self.file_parameters_by_field = None
        self.raw_parameters_by_field = None

    def _semantic_result(self):
        semantic_fields_by_name = dict(self.semantic_parameters_by_field)
        semantic_fields_by_name["families_json"] = json.loads(
            semantic_fields_by_name["families_json"]
        )
        semantic_fields_by_name["collection_summary_json"] = json.loads(
            semantic_fields_by_name["collection_summary_json"]
        )
        if self.mutate_semantic:
            semantic_fields_by_name["file_count"] += 1
        return _MappedResult([semantic_fields_by_name])

    def _file_result(self):
        file_records = [dict(fields) for fields in self.file_parameters_by_field]
        if self.mutate_file:
            file_records[0]["source_url"] = catalog_types.CATALOG_URLS["cs"]
        return _MappedResult(file_records)

    def _raw_result(self):
        raw_fields_by_name = dict(self.raw_parameters_by_field)
        raw_fields_by_name["raw_documents_json"] = json.loads(
            raw_fields_by_name["raw_documents_json"]
        )
        if self.mutate_raw:
            raw_fields_by_name["catalog_set_sha256"] = "f" * 64
        return _MappedResult([raw_fields_by_name])

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        if "INSERT INTO" in sql and catalog_store.CATALOG_SET_TABLE in sql:
            self.semantic_parameters_by_field = dict(parameters)
            return _MappedResult()
        if "SELECT catalog_set_sha256, schema_version" in sql:
            return self._semantic_result()
        if "INSERT INTO" in sql and catalog_store.CATALOG_FILE_TABLE in sql:
            self.file_parameters_by_field = [dict(fields) for fields in parameters]
            return _MappedResult()
        if "SELECT catalog_set_sha256, file_id" in sql:
            return self._file_result()
        if "INSERT INTO" in sql and catalog_store.RAW_OBSERVATION_TABLE in sql:
            self.raw_parameters_by_field = dict(parameters)
            return _MappedResult()
        if "SELECT raw_set_sha256" in sql:
            return self._raw_result()
        return _MappedResult()


def _fake_transaction(monkeypatch, database_session, catalog):
    @asynccontextmanager
    async def transaction():
        yield database_session

    monkeypatch.setattr(catalog_store.db, "transaction", transaction)
    monkeypatch.setattr(
        catalog_store,
        "validate_retained_catalog_proof",
        lambda raw_proof: (raw_proof, catalog),
    )


@pytest.mark.asyncio
async def test_persistence_recomputes_and_compares_all_immutable_fields(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    proof_by_field = {"raw_set_sha256": "a" * 64, "documents": []}
    database_session = _PersistenceSession()
    _fake_transaction(monkeypatch, database_session, catalog)

    await catalog_store.record_catalog_observation(catalog, proof_by_field)

    semantic_insert = next(
        statement
        for statement in database_session.statements
        if "INSERT INTO" in statement and catalog_store.CATALOG_SET_TABLE in statement
    )
    raw_insert = next(
        statement
        for statement in database_session.statements
        if "INSERT INTO" in statement
        and catalog_store.RAW_OBSERVATION_TABLE in statement
    )
    file_insert = next(
        statement
        for statement in database_session.statements
        if "INSERT INTO" in statement
        and catalog_store.CATALOG_FILE_TABLE in statement
    )
    assert "last_observed_at=now()" in semantic_insert
    assert "collection_summary_json=EXCLUDED" not in semantic_insert
    assert "raw_documents_json=EXCLUDED" not in raw_insert
    assert "DO NOTHING" in file_insert
    assert "DO UPDATE" not in file_insert


@pytest.mark.asyncio
async def test_persistence_rejects_existing_file_field_mutation(monkeypatch):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    database_session = _PersistenceSession(mutate_file=True)
    _fake_transaction(monkeypatch, database_session, catalog)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="file set"):
        await catalog_store.record_catalog_observation(
            catalog,
            {"raw_set_sha256": "a" * 64, "documents": []},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("session_options", "message"),
    [
        ({"mutate_semantic": True}, "semantic catalog"),
        ({"mutate_raw": True}, "raw catalog identity"),
    ],
)
async def test_persistence_rejects_existing_set_or_raw_mutation(
    monkeypatch,
    session_options,
    message,
):
    catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    database_session = _PersistenceSession(**session_options)
    _fake_transaction(monkeypatch, database_session, catalog)

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        await catalog_store.record_catalog_observation(
            catalog,
            {"raw_set_sha256": "a" * 64, "documents": []},
        )


@pytest.mark.asyncio
async def test_refresh_only_fetches_listing_snapshot_and_returns_its_hash(monkeypatch):
    observed_catalog = catalog_types.observed_catalog_from_payloads(_live_payloads())
    snapshot = SimpleNamespace(
        payloads_by_family=_live_payloads(),
        raw_set_sha256="d" * 64,
    )
    proof_by_field = {"raw_set_sha256": snapshot.raw_set_sha256, "documents": []}

    class _ClientSession:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(catalog_module.aiohttp, "ClientSession", _ClientSession)
    monkeypatch.setattr(
        catalog_module,
        "fetch_catalog_snapshot",
        AsyncMock(return_value=snapshot),
    )
    monkeypatch.setattr(
        catalog_module,
        "retain_catalog_snapshot",
        lambda _snapshot: proof_by_field,
    )
    monkeypatch.setattr(
        catalog_module,
        "observed_catalog_from_payloads",
        lambda _payloads: observed_catalog,
    )
    record_observation = AsyncMock()
    read_catalog = AsyncMock(return_value={"catalog_observed": True})
    monkeypatch.setattr(catalog_module, "record_catalog_observation", record_observation)
    monkeypatch.setattr(catalog_module, "uhc_provider_file_catalog", read_catalog)

    assert await catalog_module.refresh_uhc_provider_file_catalog() == {
        "catalog_observed": True
    }
    record_observation.assert_awaited_once_with(observed_catalog, proof_by_field)
    read_catalog.assert_awaited_once_with(raw_set_sha256=snapshot.raw_set_sha256)


@pytest.mark.asyncio
async def test_catalog_routes_authenticate_and_reject_run_history(monkeypatch):
    from api import uhc_provider_file_catalog as catalog_api

    auth = Mock()
    read_handler = catalog_api.read_uhc_provider_file_catalog
    read_catalog = AsyncMock(return_value={"catalog_observed": True})
    monkeypatch.setattr(catalog_api, "require_control_auth", auth)
    monkeypatch.setattr(catalog_api, "read_uhc_provider_file_catalog", read_catalog)
    request = SimpleNamespace(args={})

    api_response = await catalog_api.control_uhc_provider_directory_files(request)
    assert api_response.status == 200
    assert json.loads(api_response.body)["catalog_observed"] is True
    auth.assert_called_once_with(request)
    with pytest.raises(NotFound):
        await read_handler(
            {"root_run_id": "run_unsupported"}
        )


@pytest.mark.asyncio
async def test_catalog_routes_map_history_and_refresh_failures(monkeypatch):
    from api import uhc_provider_file_catalog as catalog_api

    monkeypatch.setattr(
        catalog_api,
        "uhc_provider_file_catalog",
        AsyncMock(side_effect=catalog_types.UHCFileCatalogNotFound("missing")),
    )
    with pytest.raises(NotFound):
        await catalog_api.read_uhc_provider_file_catalog(
            {"catalog_set_sha256": "e" * 64}
        )
    monkeypatch.setattr(
        catalog_api,
        "refresh_uhc_provider_file_catalog",
        AsyncMock(side_effect=catalog_types.UHCFileCatalogError("sensitive raw")),
    )
    with pytest.raises(SanicException) as error:
        await catalog_api.refresh_uhc_provider_file_catalog_listing()
    assert error.value.status_code == 503
    assert "sensitive raw" not in str(error.value)


@pytest.mark.asyncio
async def test_catalog_read_route_maps_bad_selector_and_proof_failures(monkeypatch):
    from api import uhc_provider_file_catalog as catalog_api

    monkeypatch.setattr(
        catalog_api,
        "uhc_provider_file_catalog",
        AsyncMock(side_effect=ValueError("bad selector")),
    )
    with pytest.raises(SanicException) as bad_request:
        await catalog_api.read_uhc_provider_file_catalog({})
    assert bad_request.value.status_code == 400

    monkeypatch.setattr(
        catalog_api,
        "uhc_provider_file_catalog",
        AsyncMock(side_effect=catalog_types.UHCFileCatalogError("sensitive proof")),
    )
    with pytest.raises(SanicException) as unavailable:
        await catalog_api.read_uhc_provider_file_catalog({})
    assert unavailable.value.status_code == 503
    assert "sensitive proof" not in str(unavailable.value)


def test_store_helpers_reject_invalid_schema_and_persisted_json(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(catalog_types.UHCFileCatalogError, match="schema"):
        catalog_store._schema()
    with pytest.raises(catalog_types.UHCFileCatalogError, match="JSON"):
        catalog_store._json_document("{bad")
    with pytest.raises(catalog_types.UHCFileCatalogError, match="proof mismatch"):
        catalog_store._assert_equal_json([], {}, "proof mismatch")


def test_store_query_result_helpers_support_non_mapping_results():
    class _PlainResult:
        def first(self):
            return {"value": 1}

        def all(self):
            return [{"value": 1}, SimpleNamespace(_mapping={"value": 2})]

    query_result = _PlainResult()
    assert catalog_store._record_fields(None) == {}
    assert catalog_store._first_query_fields(query_result) == {"value": 1}
    assert catalog_store._all_query_fields(query_result) == [
        {"value": 1},
        {"value": 2},
    ]
