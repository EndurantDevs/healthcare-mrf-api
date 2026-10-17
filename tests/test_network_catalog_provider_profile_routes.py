# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed profile wire admission precedes the independent Reader snapshot."""

import json

import pytest
from sanic import response

from api.endpoint import network_catalog as endpoint
from api.provider_directory_entities_contract import DirectoryReadError
from tests.test_network_catalog_routes import batch_boundary as batch_boundary
from tests.test_network_catalog_routes import context as context
from tests.test_network_catalog_routes import envelope, send, v2_envelope

_PATH = "/api/v1/registry/serving/providers/profile"


@pytest.fixture
def profile_context(context, monkeypatch):
    context.profile_calls = []
    context.profile_page = {
        "generation": "9",
        "approved_custom_revision": "4",
        "npi": 1000000491,
        "provider_profile": {"npi": 1000000491, "generation_id": "a" * 64},
    }

    async def read(selector, **options):
        context.profile_calls.append((selector, options))
        return context.profile_page

    monkeypatch.setattr(endpoint, "read_managed_provider_profile", read)
    return context


@pytest.mark.asyncio
async def test_profile_full_policy_and_normalized_query_use_no_request_writer(profile_context):
    document = v2_envelope(
        {"npi": "1000000491", "include_evidence": "true", "category": "languages", "limit": "1"},
        excluded_network_ids=[84],
        policy_revision="12",
    )
    result = await send(profile_context, document, path=_PATH)
    assert result.status == 200
    assert result.json == {"client_id": "sample-client", "policy_revision": "12"} | profile_context.profile_page
    assert result.headers["X-Network-Generation"] == "9"
    assert result.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    (npi, query), options = profile_context.profile_calls[0]
    assert npi == "1000000491" and query.include_evidence
    assert (query.page_category, query.page_limit) == ("languages", 1)
    assert options == {"excluded_network_ids": (84,), "access_scope_sha256": document["access_scope_sha256"]}
    assert profile_context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["v1", "digest", "query-string", "duplicate", "typed", "decimal-generation"])
async def test_profile_refuses_untrusted_wire_before_reader(profile_context, kind):
    document, path, raw = v2_envelope({"npi": "1000000491"}), _PATH, None
    if kind == "v1":
        document = envelope({"npi": "1000000491"})
    if kind == "digest":
        document["access_scope_sha256"] = "0" * 64
    if kind == "query-string":
        path += "?category=languages"
    if kind == "duplicate":
        raw = json.dumps(document).replace('"query": {', '"query": {"npi":"1000000491",').encode()
    if kind == "typed":
        document["query"]["include_evidence"] = True
    if kind == "decimal-generation":
        document["query"]["network_generation"] = "9"
    result = await send(profile_context, document, path=path, raw=raw)
    assert result.status == 400 and profile_context.profile_calls == [] and profile_context.session.events == []


@pytest.mark.asyncio
async def test_profile_authentication_precedes_invalid_body(profile_context):
    result = await send(profile_context, path=_PATH, headers={}, raw=b"\xff")
    assert result.status == 403 and profile_context.profile_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 409, 429])
async def test_profile_public_errors_preserve_status_and_private_scope(profile_context, monkeypatch, status):
    async def read(*args, **kwargs):
        return response.json(
            {"message": "The provider profile changed; restart category pagination."},
            status=status,
            headers={"Retry-After": "17"},
        )

    monkeypatch.setattr(endpoint, "read_managed_provider_profile", read)
    document = v2_envelope({"npi": "1000000491"})
    result = await send(profile_context, document, path=_PATH)
    assert result.status == status and result.headers["Retry-After"] == "17"
    assert result.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in result.headers


@pytest.mark.asyncio
async def test_profile_source_failure_is_terminal_without_writer_fallback(profile_context, monkeypatch):
    async def read(*args, **kwargs):
        raise DirectoryReadError(503)

    monkeypatch.setattr(endpoint, "read_managed_provider_profile", read)
    result = await send(profile_context, v2_envelope({"npi": "1000000491"}), path=_PATH)
    assert result.status == 503 and profile_context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("trailing", ["", "/"])
async def test_profile_framework_refusal_is_private(profile_context, trailing):
    _, result = await profile_context.app.asgi_client.get(_PATH + trailing)
    assert result.status == 405 and result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_profile_complete_body_bound_refuses_without_generation(profile_context):
    profile_context.profile_page["provider_profile"]["value"] = "x" * endpoint.MAX_RESPONSE_BYTES
    result = await send(profile_context, v2_envelope({"npi": "1000000491"}), path=_PATH)
    assert result.status == 503 and "X-Network-Generation" not in result.headers
