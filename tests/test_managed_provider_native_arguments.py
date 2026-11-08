# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalized native arguments require a trusted bound network read scope."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import npi as provider
from api.network_address_scope import network_address_read_scope
from process.network_serving_read import PinnedNetworkServingManifest


class ReachedNativeReader(Exception):
    """Stop at session selection, before any database work."""


@pytest.fixture
def native_boundary(monkeypatch):
    candidate = uuid4()
    manifest = PinnedNetworkServingManifest(
        9, str(candidate), "network_candidate_" + candidate.hex, 1, {}, 4, "b" * 64, 123
    )
    request = SimpleNamespace(args={}, query_string="", ctx=SimpleNamespace())

    def reader(_request):
        assert _request is request
        raise ReachedNativeReader

    monkeypatch.setattr(provider, "_request_session", reader)
    return request, manifest


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", [provider.list_providers, provider.get_near_npi])
async def test_normalized_native_arguments_only_reach_reader_in_bound_scope(native_boundary, handler):
    request, manifest = native_boundary
    with pytest.raises(InvalidUsage, match="arguments are invalid"):
        await handler(request, native_args={})
    with network_address_read_scope(manifest, (42,)):
        with pytest.raises(ReachedNativeReader):
            await handler(request, native_args={})


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", [provider.list_providers, provider.get_near_npi])
async def test_scope_does_not_allow_import_context_without_native_arguments(native_boundary, handler):
    request, manifest = native_boundary
    with network_address_read_scope(manifest, (42,)):
        with pytest.raises(InvalidUsage, match="arguments are invalid"):
            await handler(request, import_context=object())
