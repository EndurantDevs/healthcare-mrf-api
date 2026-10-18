# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker resource ownership; mocked connections do not qualify native custody."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from process import registry_aca_source_worker as worker
from tests.test_registry_aca_source_worker import configuration


def _pool_factory(config, events, outcome):
    @asynccontextmanager
    async def transaction(**kwargs):
        assert kwargs == {"isolation": "repeatable_read", "readonly": True}
        yield

    async def create_pool(**options):
        role = options["user"]
        assert options["min_size"] == options["max_size"] == 1
        if role == config.reader_role and outcome == "reader_startup":
            raise RuntimeError("reader startup failed")
        identity_by_field = dict(
            safe_login=True,
            current_role=role,
            session_role=role,
            control_oid=1,
            inbox_oid=2,
        )
        connection = SimpleNamespace(
            fetchrow=AsyncMock(return_value=identity_by_field),
            is_in_transaction=lambda: False,
            transaction=transaction,
        )

        @asynccontextmanager
        async def acquire(*, timeout):
            assert timeout == config.sql_limits.connect_timeout_seconds
            try:
                yield connection
            finally:
                events.append((role, "release"))

        async def close():
            events.append((role, "close"))

        return SimpleNamespace(acquire=acquire, close=close)

    return create_pool


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "cancel", "malformed", "reader_startup"])
async def test_consumer_drains_before_both_owned_pools_close(monkeypatch, outcome):
    config = worker.parse_registry_aca_source_worker_config(json.dumps(configuration()))
    events = []
    composer = SimpleNamespace(qualify=AsyncMock())
    consumer = SimpleNamespace(seal_candidate=object(), composition_runtime=composer)
    monkeypatch.setattr(worker.source_policy, "source_transaction_available", lambda: None)
    monkeypatch.setattr(worker.asyncpg, "create_pool", _pool_factory(config, events, outcome))
    monkeypatch.setattr(worker, "_source_config", AsyncMock(return_value=config))
    monkeypatch.setattr(worker, "_ptg_runtime", lambda *_: consumer)
    monkeypatch.setattr(worker, "_custody_runtime", AsyncMock(return_value=consumer))
    qualify = AsyncMock(return_value="qualified-inbox")
    monkeypatch.setattr(worker.RegistryACASourceNotificationStore, "qualify", qualify)

    async def consume(**kwargs):
        assert kwargs["config"] is consumer and kwargs["event"] == config.event
        assert kwargs["identity"] == "qualified-inbox"
        composer.qualify.assert_awaited_once()
        try:
            if outcome == "cancel":
                raise asyncio.CancelledError
            return {"state": "completed", **({"unexpected": True} if outcome == "malformed" else {})}
        finally:
            await asyncio.sleep(0)
            events.append(("consumer", "drained"))

    monkeypatch.setattr(worker, "consume_registry_aca_source_notification_once", consume)
    if outcome == "completed":
        assert await worker.run_registry_aca_source_worker(config) == {"state": "completed"}
    else:
        with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
            await worker.run_registry_aca_source_worker(config)
    assert events[-1] == (config.consumer.execution_role, "close")
    if outcome == "reader_startup":
        assert events == [(config.consumer.execution_role, "close")]
        qualify.assert_not_awaited()
    else:
        assert events.index(("consumer", "drained")) < events.index((config.consumer.execution_role, "release"))
        assert events[-2] == (config.reader_role, "close")


@pytest.mark.parametrize(
    "outcome,code,error",
    [
        ("completed", 0, None),
        ("configuration", 2, "invalid_configuration"),
        ("failure", 1, "worker_failed"),
        ("cancel", 130, "canceled"),
        ("arguments", 2, "invalid_arguments"),
    ],
)
def test_command_has_stable_exit_status_and_sanitized_output(monkeypatch, outcome, code, error):
    monkeypatch.setenv(worker.CONFIG_ENV, "null" if outcome == "configuration" else json.dumps(configuration()))
    run = AsyncMock(return_value={"state": "completed"})
    if outcome in {"failure", "cancel"}:
        run.side_effect = (
            RuntimeError("example protected diagnostic") if outcome == "failure" else asyncio.CancelledError
        )
    monkeypatch.setattr(worker, "run_registry_aca_source_worker", run)
    result = CliRunner().invoke(worker.registry_aca_source_worker_command, ["extra"] if outcome == "arguments" else [])
    assert result.exit_code == code
    assert json.loads(result.output) == ({"state": "error", "code": error} if error else {"state": "completed"})
    if outcome in {"configuration", "arguments"}:
        run.assert_not_awaited()
    else:
        run.assert_awaited_once()
