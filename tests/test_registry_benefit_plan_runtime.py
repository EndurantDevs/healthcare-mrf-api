# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured clients belong to the API or worker, never to request bodies."""

import json
from types import SimpleNamespace

import pytest
from sanic import Sanic

from process import registry_benefit_plan_runtime as runtime
from process import registry_publication_worker as worker
from process.registry_benefit_plan_metadata import ORIGIN_CONFIG, TOKEN_CONFIG
from tests.test_registry_benefit_plan_metadata import _client, _document, _reply
from tests.test_registry_publication_worker import configuration, resources


def _app(config):
    listeners_by_event = {}
    app = SimpleNamespace(config=config, ctx=SimpleNamespace())
    app.listener = lambda event: lambda callback: listeners_by_event.setdefault(event, callback)
    runtime.register_registry_benefit_plan_runtime(app)
    return app, listeners_by_event


@pytest.mark.asyncio
async def test_api_client_uses_prefixed_server_configuration_and_closes(monkeypatch):
    monkeypatch.setenv(ORIGIN_CONFIG, "https://metadata.example")
    monkeypatch.setenv(TOKEN_CONFIG, "synthetic-selected-plan-token")
    configured = Sanic("metadata_configuration", env_prefix="HLTHPRT_").config
    app, listeners_by_event = _app(configured)
    await listeners_by_event["before_server_start"](app)
    client = app.ctx.registry_plan_metadata_client
    assert client is not None
    with pytest.raises(ValueError):
        await listeners_by_event["before_server_start"](app)
    await listeners_by_event["after_server_stop"](app)
    assert app.ctx.registry_plan_metadata_client is None and client._client.is_closed
    await listeners_by_event["after_server_stop"](app)
    with pytest.raises(ValueError):
        runtime.register_registry_benefit_plan_runtime(app)


@pytest.mark.asyncio
async def test_missing_configuration_is_optional_but_partial_configuration_refuses(monkeypatch):
    monkeypatch.delenv(ORIGIN_CONFIG, raising=False)
    monkeypatch.delenv(TOKEN_CONFIG, raising=False)
    app, listeners_by_event = _app({})
    await listeners_by_event["before_server_start"](app)
    assert app.ctx.registry_plan_metadata_client is None
    await listeners_by_event["after_server_stop"](app)
    app, listeners_by_event = _app({ORIGIN_CONFIG: "https://metadata.example"})
    with pytest.raises(ValueError):
        await listeners_by_event["before_server_start"](app)
    assert getattr(app.ctx, "registry_plan_metadata_client", None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_actual_worker_passes_its_owned_client_and_closes_on_every_exit(monkeypatch, failure):
    resources(monkeypatch)
    client = _client(lambda request: _reply(_document(json.loads(request.content)["healthporta_plan_ids"])))
    seen_clients = []
    monkeypatch.setattr(worker.RegistryBenefitPlanMetadataClient, "from_config", lambda config: client)

    async def consume(**arguments):
        seen_clients.append(arguments["plan_metadata_client"])
        assert seen_clients == [client] and not client._client.is_closed
        if failure:
            raise RuntimeError("synthetic failure")
        return None

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    configured = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    if failure:
        with pytest.raises(RuntimeError):
            await worker.run_registry_publication_worker(configured)
    else:
        assert await worker.run_registry_publication_worker(configured) == {"state": "no_work"}
    assert seen_clients == [client] and client._client.is_closed
