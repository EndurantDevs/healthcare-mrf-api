# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Mocked child cleanup without importing the native installed proof."""

import ast
import asyncio
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest


@pytest.fixture
def communicate_with_cleanup():
    source_path = Path(__file__).with_name("test_custom_import_installed_operator_postgres.py")
    tree = ast.parse(source_path.read_text())
    helper_node = next(node for node in tree.body if getattr(node, "name", None) == "_communicate_with_cleanup")
    namespace_by_name = {"asyncio": asyncio, "suppress": suppress}
    exec(compile(ast.Module(body=[helper_node], type_ignores=[]), str(source_path), "exec"), namespace_by_name)
    return namespace_by_name[helper_node.name]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "primary,cleanup_error,kill_error",
    [
        (TimeoutError("original"), None, None),
        (asyncio.CancelledError("original"), None, ProcessLookupError()),
        (RuntimeError("original"), TimeoutError("cleanup"), None),
        (asyncio.CancelledError("original"), asyncio.CancelledError("cleanup"), RuntimeError("kill")),
    ],
)
async def test_cleanup_preserves_primary_failure(
    communicate_with_cleanup, monkeypatch, primary, cleanup_error, kill_error
):
    process = SimpleNamespace(
        returncode=None,
        kill=Mock(side_effect=kill_error),
        communicate=AsyncMock(side_effect=[primary, cleanup_error if cleanup_error else (b"", b"")]),
    )
    wait_for = AsyncMock(wraps=asyncio.wait_for)
    monkeypatch.setattr(asyncio, "wait_for", wait_for)
    with pytest.raises(type(primary)) as caught:
        await communicate_with_cleanup(process, b"input")
    assert caught.value is primary
    process.kill.assert_called_once_with()
    assert process.communicate.await_args_list == [call(b"input"), call()]
    assert [entry.kwargs["timeout"] for entry in wait_for.await_args_list] == [30, 5]


@pytest.mark.asyncio
async def test_blocked_drain_is_cancelled_and_original_failure_survives(communicate_with_cleanup, monkeypatch):
    primary = TimeoutError("original")
    cancelled = asyncio.Event()

    async def communicate(*arguments):
        if process.communicate.await_count == 1:
            raise primary
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    real_wait_for = asyncio.wait_for

    async def wait_for(awaitable, *, timeout):
        return await real_wait_for(awaitable, timeout=0.01 if timeout == 5 else timeout)

    process = SimpleNamespace(returncode=None, kill=Mock(), communicate=AsyncMock(side_effect=communicate))
    monkeypatch.setattr(asyncio, "wait_for", wait_for)
    with pytest.raises(TimeoutError) as caught:
        await communicate_with_cleanup(process, b"input")
    assert caught.value is primary and cancelled.is_set()
    process.kill.assert_called_once_with()
