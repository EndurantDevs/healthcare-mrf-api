# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only operation selectors share the worker's complete root identity."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest
from sanic.exceptions import BadRequest, Forbidden

from api import control
from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_worker as worker
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.uhc_flex_practitioner_registration import uhc_flex_practitioner_endpoint_identity
from tests.test_provider_directory_rooted_graph_worker import current_root, operation


@pytest.fixture
def operation_database():
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    database = SimpleNamespace(transaction=transaction, status=AsyncMock())
    return database, events


@pytest.fixture
def preparation_database(monkeypatch, operation_database):
    database, events = operation_database
    metadata = AsyncMock(return_value=current_root())
    monkeypatch.setattr(preflight, "_current_metadata", metadata)
    return database, metadata, events


def _current_registry_rows():
    pair = preflight.scoped.exact_uhc_dataset_pair()
    return [
        dict(
            source_id=source_id,
            endpoint_id=endpoint_id,
            source_api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
            endpoint_api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
            source_authority_id="synthetic-authority",
            endpoint_authority_id="synthetic-authority",
            endpoint_signature_sha256=signature,
        )
        for source_id, endpoint_id, signature in (
            (
                pair.legacy_source_id,
                pair.legacy_endpoint_id,
                uhc_flex_practitioner_endpoint_identity().endpoint_signature_hash,
            ),
            (pair.rooted_source_id, pair.rooted_endpoint_id, PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ["missing", "inconsistent", "database"])
async def test_current_metadata_selection_errors(operation_database, selection):
    database, events = operation_database
    database.all = AsyncMock(side_effect=[_current_registry_rows(), []])
    database.first = AsyncMock(return_value=None)
    error, message = ValueError, "provider_directory_rooted_graph_worker_root_changed"
    if selection == "inconsistent":
        database.first.side_effect = [{"dataset_id": "synthetic-edition"}, None]
        error, message = RuntimeError, "provider_directory_rooted_graph_predecessor_invalid"
    if selection == "database":
        database.first.side_effect = RuntimeError("synthetic database failure")
        error, message = RuntimeError, "synthetic database failure"
    request_by_field = {name: operation()[name] for name in ("operation_key", *worker._ROOT_FIELDS)}
    with pytest.raises(error, match=f"^{message}$") as caught:
        await worker.prepare_rooted_graph_operation(database, request_by_field)
    if selection == "missing":
        assert isinstance(caught.value.__cause__, preflight.CurrentRootMissingError)
        assert database.first.await_count == 2
    assert database.all.await_count == 2
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_preparation_derives_the_worker_selector_in_a_readonly_snapshot(preparation_database):
    database, metadata, events = preparation_database
    request_by_field = {name: operation()[name] for name in ("operation_key", *worker._ROOT_FIELDS)}
    assert await worker.prepare_rooted_graph_operation(database, request_by_field) == operation()
    metadata.assert_awaited_once_with(database, lock_rows=False)
    assert database.status.await_args_list == [
        call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"),
        call("SET LOCAL statement_timeout = '5s'"),
        call("SET LOCAL lock_timeout = '1s'"),
    ]
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing", "drift", "extra", "key", "type", "database"])
async def test_preparation_refuses_drift_and_preserves_database_errors(preparation_database, changed):
    database, metadata, _events = preparation_database
    request_by_field = {name: operation()[name] for name in ("operation_key", *worker._ROOT_FIELDS)}
    if changed == "missing":
        metadata.return_value = None
    if changed == "drift":
        request_by_field["root_dataset_hash"] = "0" * 64
    if changed == "extra":
        request_by_field["publication_acquisition_id"] = operation()["publication_acquisition_id"]
    if changed == "key":
        request_by_field["operation_key"] = "invalid"
    if changed == "type":
        request_by_field = None
    if changed == "database":
        metadata.side_effect = RuntimeError("synthetic database failure")
    with pytest.raises(RuntimeError if changed == "database" else ValueError):
        await worker.prepare_rooted_graph_operation(database, request_by_field)
    if changed in {"extra", "key", "type"}:
        metadata.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "auth", "request", "database"])
async def test_control_preparation_preserves_authentication_and_errors(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    prepare = AsyncMock(return_value=operation())
    monkeypatch.setattr(worker, "prepare_rooted_graph_operation", prepare)
    request = SimpleNamespace(headers={"Authorization": "Bearer synthetic-token"}, json={"operation_key": "a" * 64})
    if failure == "auth":
        request.headers = {}
    elif failure == "request":
        prepare.side_effect = ValueError("synthetic invalid request")
    elif failure == "database":
        prepare.side_effect = RuntimeError("synthetic database failure")
    if failure:
        error = {"auth": Forbidden, "request": BadRequest, "database": RuntimeError}[failure]
        with pytest.raises(error):
            await control.control_provider_directory_rooted_graph_operation(request)
    else:
        response = await control.control_provider_directory_rooted_graph_operation(request)
        assert json.loads(response.body) == operation()
    if failure == "auth":
        prepare.assert_not_awaited()
    else:
        assert prepare.await_args.args[1] == request.json
