# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Network readers resolve logical publications inside the caller's transaction."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_published_api_pin as published_pin
from process import network_serving_read as serving
from tests.test_network_published_api_pin import published_entry as published_entry


@pytest.fixture
def published_connection(published_entry):
    """Expose the same named publication to the real resolver query."""
    return SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=published_entry))


@pytest.mark.parametrize("api_read", [False, True])
async def test_readers_resolve_the_requested_published_generation(published_connection, api_read):
    manifest = await serving.resolve_network_serving_manifest(
        published_connection, generation_id=7, control_schema="registry", api_read=api_read
    )
    assert manifest.generation_id == 7 and manifest.approved_custom_revision == 4
    assert manifest.address_relation.schema_name == "published_addresses"
    published_connection.fetchrow.assert_awaited_once()
    query, generation, *options = published_connection.fetchrow.await_args.args
    assert generation == 7 and options == ([] if api_read else [False])
    assert '"registry".network_serving_manifest' in query
    assert "manifest.eligible" in query and "candidate.state='published'" in query
    if api_read:
        assert "NULL::bigint AS approved_rows" in query and "registry_approved_record" not in query


async def test_api_read_serves_the_published_pin_without_other_verification(monkeypatch):
    manifest = object()
    published = AsyncMock(return_value=(manifest, None, None))
    direct = AsyncMock(side_effect=AssertionError("API reads use the manifest-only published query"))
    monkeypatch.setattr(published_pin, "read_published_api_pin", published)
    monkeypatch.setattr(serving, "_read_manifest", direct)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    assert (
        await serving.resolve_network_serving_manifest(
            connection, generation_id=7, control_schema="registry", api_read=True
        )
        is manifest
    )
    published.assert_awaited_once_with(connection, 7, "registry", approved=False)
    direct.assert_not_awaited()


@pytest.mark.parametrize("failure", [serving.NetworkServingReadUnavailable("missing"), KeyError("candidate")])
async def test_api_read_failure_never_falls_back(monkeypatch, failure):
    published = AsyncMock(side_effect=failure)
    direct = AsyncMock(side_effect=AssertionError("a failed API pin never selects other publication metadata"))
    monkeypatch.setattr(published_pin, "read_published_api_pin", published)
    monkeypatch.setattr(serving, "_read_manifest", direct)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    with pytest.raises(serving.NetworkServingReadUnavailable) as caught:
        await serving.resolve_network_serving_manifest(connection, control_schema="registry", api_read=True)
    assert (
        caught.value is failure
        if isinstance(failure, serving.NetworkServingReadUnavailable)
        else caught.value.__cause__ is failure
    )
    published.assert_awaited_once_with(connection, None, "registry", approved=False)
    direct.assert_not_awaited()


async def test_other_reads_never_use_the_api_pin(monkeypatch, published_connection):
    published = AsyncMock(side_effect=AssertionError("other reads resolve their own selected publication"))
    monkeypatch.setattr(published_pin, "read_published_api_pin", published)
    manifest = await serving.resolve_network_serving_manifest(
        published_connection, generation_id=7, control_schema="registry"
    )
    assert manifest.generation_id == 7
    published.assert_not_awaited()


@pytest.mark.parametrize("entry", [None, {}, {"candidate_json": "{}", "manifest_json": "{}"}])
async def test_missing_or_invalid_publication_is_unavailable(published_connection, entry):
    published_connection.fetchrow.return_value = entry
    with pytest.raises(serving.NetworkServingReadUnavailable):
        await serving.resolve_network_serving_manifest(published_connection, control_schema="registry", api_read=True)
    published_connection.fetchrow.assert_awaited_once()


async def test_database_failure_keeps_the_original_cause(published_connection):
    failure = asyncpg.UndefinedTableError("missing published relation")
    published_connection.fetchrow.side_effect = failure
    with pytest.raises(serving.NetworkServingReadUnavailable) as caught:
        await serving.resolve_network_serving_manifest(published_connection, control_schema="registry", api_read=True)
    assert caught.value.__cause__ is failure
    published_connection.fetchrow.assert_awaited_once()


async def test_cancellation_reaches_the_transaction_owner(published_connection):
    failure = asyncio.CancelledError()
    published_connection.fetchrow.side_effect = failure
    with pytest.raises(asyncio.CancelledError) as caught:
        await serving.resolve_network_serving_manifest(published_connection, control_schema="registry", api_read=True)
    assert caught.value is failure


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        ({"api_read": 1}, "read mode is invalid"),
        ({"generation_id": 0}, "generation is invalid"),
        ({"generation_id": True}, "generation is invalid"),
        ({"generation_id": 2**63}, "generation is invalid"),
    ],
)
async def test_invalid_selector_is_refused_before_the_connection_is_used(selector, message):
    with pytest.raises(serving.NetworkServingReadUnavailable, match=message):
        await serving.resolve_network_serving_manifest(object(), control_schema="registry", **selector)


async def test_read_outside_a_transaction_is_refused(published_connection):
    published_connection.is_in_transaction = lambda: False
    with pytest.raises(serving.NetworkServingReadUnavailable, match="active transaction"):
        await serving.resolve_network_serving_manifest(published_connection, control_schema="registry", api_read=True)
    published_connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("api_read", [False, True])
@pytest.mark.parametrize("generation", [None, 7])
async def test_metadata_is_bounded_in_sql_before_decode(published_connection, monkeypatch, api_read, generation):
    def unexpected_decode(value):
        pytest.fail("Unavailable metadata must not reach the JSON decoder")

    monkeypatch.setattr(serving.json, "loads", unexpected_decode)
    published_connection.fetchrow.return_value = None
    with pytest.raises(serving.NetworkServingReadUnavailable, match="generation is unavailable"):
        await serving.resolve_network_serving_manifest(
            published_connection, generation_id=generation, control_schema="registry", api_read=api_read
        )
    query = published_connection.fetchrow.await_args.args[0]
    assert "coalesce(octet_length(candidate.validation_json::text),0)<=1048576" in query
    assert "octet_length(candidate.source_generations::text)<=1048576" in query
    assert "octet_length(manifest.source_generations::text)<=1048576" in query
    assert "to_jsonb(candidate)" not in query and "to_jsonb(manifest)" not in query
    assert "source_recipes_json" not in query and "source_inventory_json" not in query
