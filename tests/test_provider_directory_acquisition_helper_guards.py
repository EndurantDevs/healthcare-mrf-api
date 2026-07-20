# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, Mock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ((503, {}, None, 0.1), ([], None, "http_503")),
        ((200, None, None, 0.1), ([], None, "invalid_json")),
        (
            (200, {"errors": [{"message": "denied"}]}, None, 0.1),
            ([], None, 'graphql_errors:[{"message": "denied"}]'),
        ),
        (
            (200, {"data": {}}, None, 0.1),
            ([], None, "missing_graphql_result:providers"),
        ),
        (
            (
                200,
                {
                    "data": {
                        "providers": {
                            "items": [{"id": "p1"}, "invalid"],
                            "nextToken": " next-page ",
                        }
                    }
                },
                None,
                0.1,
            ),
            ([{"id": "p1"}], "next-page", None),
        ),
    ],
)
async def test_alohr_graphql_page_response_guards(
    monkeypatch,
    response,
    expected,
):
    thread_call = AsyncMock(return_value=response)
    monkeypatch.setattr(importer.asyncio, "to_thread", thread_call)

    result = await importer._fetch_alohr_graphql_page(
        "query Providers { providers { items { id } } }",
        "providers",
        "items",
        next_token="cursor",
        timeout=15,
    )

    assert result == expected
    request_payload = thread_call.await_args.args[2]
    assert request_payload["variables"]["nextToken"] == "cursor"


@pytest.mark.asyncio
async def test_upsert_rows_empty_location_and_copy_paths(monkeypatch):
    assert await importer._upsert_rows(importer.ProviderDirectorySource, []) == 0

    attached_rows = [{"source_id": "source_a", "resource_id": "location_a"}]
    attach_contacts = Mock(return_value=attached_rows)
    monkeypatch.setattr(
        importer,
        "_has_missing_location_contact_fields",
        lambda _resource_rows: True,
    )
    monkeypatch.setattr(importer, "_attach_location_contact_fields", attach_contacts)
    monkeypatch.setattr(
        importer,
        "_dedupe_rows_by_primary_key",
        lambda _primary_keys, _resource_rows: [],
    )

    assert await importer._upsert_rows(
        importer.ProviderDirectoryLocation,
        [{"source_id": "source_a", "resource_id": "location_a"}],
    ) == 0
    attach_contacts.assert_called_once()

    monkeypatch.setattr(
        importer,
        "_dedupe_rows_by_primary_key",
        lambda _primary_keys, resource_rows: resource_rows,
    )
    monkeypatch.setattr(importer, "_bound_upsert_session", lambda: None)
    monkeypatch.setattr(
        importer,
        "_transaction_session_options",
        lambda _session: {},
    )
    monkeypatch.setattr(importer, "_is_copy_upsert_enabled", lambda: True)
    monkeypatch.setattr(importer, "_copy_upsert_min_rows", lambda: 1)
    copy_upsert = AsyncMock(return_value=1)
    monkeypatch.setattr(importer, "_copy_upsert_rows", copy_upsert)

    assert await importer._upsert_rows(
        importer.ProviderDirectorySource,
        [{"source_id": "source_a"}],
    ) == 1
    copy_upsert.assert_awaited_once()
