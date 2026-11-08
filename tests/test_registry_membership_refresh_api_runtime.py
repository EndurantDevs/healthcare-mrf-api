# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Default lifecycle registration cannot enable refresh or overwrite another owner."""

import asyncio
from types import SimpleNamespace

import pytest

from process import registry_membership_refresh_api_runtime as runtime


@pytest.mark.asyncio
async def test_unconfigured_lifecycle_preserves_preexisting_capabilities():
    anchor = object()
    app = SimpleNamespace(config={}, ctx=SimpleNamespace(registry_refresh_anchor_service=anchor))
    await runtime._start_runtime(app)
    await runtime._stop_runtime(app)
    assert app.ctx.registry_refresh_anchor_service is anchor
    assert not hasattr(app.ctx, "registry_membership_refresh_cleanup")


def test_registration_is_once_and_does_not_create_capabilities():
    listeners_by_name = {}

    def register(name):
        def registered(callback):
            listeners_by_name[name] = callback

        return registered

    app = SimpleNamespace(ctx=SimpleNamespace(), listener=register)
    runtime.register_registry_membership_refresh_runtime(app)
    assert set(listeners_by_name) == {"before_server_start", "after_server_stop"}
    assert not hasattr(app.ctx, "registry_membership_refresh_authority")
    with pytest.raises(ValueError, match="already_registered"):
        runtime.register_registry_membership_refresh_runtime(app)


@pytest.mark.asyncio
async def test_owned_runtime_cleanup_preserves_replaced_capabilities(monkeypatch):
    from process import registry_membership_refresh_runtime as builder

    closed_resources = []

    async def close():
        closed_resources.append(True)

    bundle = SimpleNamespace(
        runtime=SimpleNamespace(authorize=object(), address_profile=object()),
        inputs_service=object(),
        anchors_service=object(),
    )

    async def build(_configured, cleanup):
        cleanup.push_async_callback(close)
        return bundle

    monkeypatch.setattr(builder, "build_registry_membership_refresh_runtime", build)
    app = SimpleNamespace(config={"REGISTRY_MEMBERSHIP_REFRESH_RUNTIME": {}}, ctx=SimpleNamespace())
    await runtime._start_runtime(app)
    assert app.ctx.registry_membership_refresh_authority is bundle.runtime.authorize
    replacement = object()
    app.ctx.registry_refresh_anchor_service = replacement
    await runtime._stop_runtime(app)
    await runtime._stop_runtime(app)
    assert closed_resources == [True]
    assert app.ctx.registry_membership_refresh_authority is None
    assert app.ctx.registry_refresh_anchor_service is replacement


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("unavailable"), asyncio.CancelledError()])
async def test_failed_start_closes_owned_resources_and_preserves_failure(monkeypatch, failure):
    from process import registry_membership_refresh_runtime as builder

    closed_resources = []

    async def close():
        closed_resources.append(True)

    async def build(_configured, cleanup):
        cleanup.push_async_callback(close)
        raise failure

    monkeypatch.setattr(builder, "build_registry_membership_refresh_runtime", build)
    app = SimpleNamespace(config={"REGISTRY_MEMBERSHIP_REFRESH_RUNTIME": {}}, ctx=SimpleNamespace())
    with pytest.raises(type(failure)) as refused:
        await runtime._start_runtime(app)
    assert refused.value is failure and closed_resources == [True]
    assert getattr(app.ctx, "registry_membership_refresh_cleanup", None) is None
