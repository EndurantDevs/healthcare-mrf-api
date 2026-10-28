# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Negotiation preserves legacy callers and exposes exact immutable metadata pins."""

from unittest.mock import AsyncMock

import pytest

from api.endpoint import network_catalog as endpoint
from api.network_catalog_fhir_query import parse_fhir_catalog_query
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_network_catalog_routes import context as context
from tests.test_network_catalog_routes import envelope, send, v2_envelope
from tests.test_registry_fhir_catalog_read import network_document
from tests.test_registry_network_catalog_read import batch_boundary as batch_boundary


def _page():
    return {
        "generation": "9",
        "approved_custom_revision": "4",
        "metadata_approved_revision": "6",
        "metadata_generation": "a" * 64,
        "continuation_scope_sha256": "b" * 64,
        "view": "fhir-plans-v1",
        "total": 1,
        "limit": 25,
        "offset": 0,
        "next_offset": None,
        "items": [network_document()],
    }


@pytest.mark.asyncio
async def test_opt_in_dispatches_authorized_reader_and_keeps_last_good_serving(context, monkeypatch):
    page = _page()
    page["items"][0]["source_bindings"]["items"][0]["status"] = "last_good"
    reader = AsyncMock(return_value=page)
    monkeypatch.setattr(endpoint, "read_fhir_catalog", reader)
    document = v2_envelope({"view": "fhir-plans-v1", "plan_search": "  Gold  "})
    reply = await send(context, document)
    assert reply.status == 200
    assert reply.json["items"][0]["network_id"] == 7
    assert reply.json["items"][0]["directory_available"] is True
    assert reply.json["metadata_approved_revision"] == "6"
    assert reply.headers["X-Network-Generation"] == "9"
    assert reader.call_args.args[1].plan_search == "Gold"
    assert reader.call_args.kwargs["organization_policy"].excluded_company_ids == ()


@pytest.mark.asyncio
async def test_unattached_enriched_page_has_metadata_pins_and_no_directory_header(context, monkeypatch):
    page = _page()
    page.update(generation=None, approved_custom_revision=None)
    page["items"][0]["directory_available"] = False
    monkeypatch.setattr(endpoint, "read_fhir_catalog", AsyncMock(return_value=page))
    reply = await send(context, v2_envelope({"view": "fhir-plans-v1"}))
    assert reply.status == 200
    assert reply.json["generation"] is None and reply.json["approved_custom_revision"] is None
    assert reply.json["metadata_generation"] == "a" * 64
    assert reply.json["items"][0]["directory_available"] is False
    assert "X-Network-Generation" not in reply.headers
    assert reply.headers["X-Network-Access-Scope"]


@pytest.mark.asyncio
async def test_null_directory_generation_is_never_allowed_for_default_catalog(context, monkeypatch):
    page = _page()
    page.update(generation=None, approved_custom_revision=None)
    monkeypatch.setattr(endpoint, "read_registry_network_catalog", AsyncMock(return_value=page))
    reply = await send(context, envelope())
    assert reply.status == 503


@pytest.mark.asyncio
async def test_enrichment_requires_full_policy_before_any_database_work(context):
    reply = await send(context, envelope({"view": "fhir-plans-v1"}))
    assert reply.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_stale_metadata_returns_explicit_restart_without_legacy_fallback(context, monkeypatch):
    reader = AsyncMock(side_effect=RegistryNetworkCatalogError("registry_network_catalog_source_changed"))
    monkeypatch.setattr(endpoint, "read_fhir_catalog", reader)
    reply = await send(context, v2_envelope({"view": "fhir-plans-v1"}))
    assert reply.status == 409
    assert reply.json == {"error": {"code": "network_catalog_source_changed"}}
    assert context.driver.calls == []


def test_detail_uses_one_item_page_and_both_continuation_pins():
    selector, generation = parse_fhir_catalog_query(
        {
            "view": "fhir-plans-v1",
            "network_id": 7,
            "network_generation": "9",
            "metadata_generation": "a" * 64,
            "continuation_scope_sha256": "b" * 64,
            "relationship_offset": 5,
        },
        "detail",
    )
    assert selector.network_id == 7 and selector.query.limit == 1 and generation == 9
    assert selector.query.archived is False and selector.query.relationship_offset == 5
    with pytest.raises(ValueError):
        parse_fhir_catalog_query({"view": "fhir-plans-v1", "network_id": 7, "relationship_offset": 5}, "detail")


def test_metadata_only_detail_continuation_does_not_need_directory_generation():
    selector, generation = parse_fhir_catalog_query(
        {
            "view": "fhir-plans-v1",
            "network_id": 7,
            "metadata_generation": "a" * 64,
            "continuation_scope_sha256": "b" * 64,
            "relationship_offset": 5,
        },
        "detail",
    )
    assert generation is None and selector.query.generation_id is None
    assert selector.query.relationship_offset == 5
