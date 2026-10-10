# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared projection protocol checks; typed row parity still requires PostgreSQL."""

import asyncio
from tempfile import TemporaryFile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import reference_family_archive as archive


def _session(driver):
    return SimpleNamespace(
        in_transaction=lambda: True,
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [301, 86400])
@pytest.mark.parametrize("raw_driver", [False, True])
async def test_projection_preserves_bound_arguments_native_bytes_and_one_deadline(monkeypatch, timeout, raw_driver):
    spool = TemporaryFile(mode="w+b")
    monkeypatch.setattr(archive, "TemporaryFile", lambda **_kwargs: spool)
    wire_bytes = b"\x00native wire bytes\xff"
    query = "SELECT payload FROM source_rows WHERE key=ANY($1::bytea[]) AND label=$2::text"
    arguments = ([b"\x00\xff", b"\x01"], "literal ' quoted $1")
    timeouts = []

    async def capture(actual_query, *actual_args, output, **options):
        assert actual_query == query and actual_args == arguments
        assert actual_args[0] is arguments[0]
        assert options["format"] == "binary"
        timeouts.append(options["timeout"])
        await output(wire_bytes)
        return "COPY 1"

    async def restore(table, *, source, **options):
        assert table == "rows" and source is spool and source.read() == wire_bytes
        assert options["schema_name"] == "candidate" and options["columns"] == ("payload",)
        assert options["format"] == "binary"
        timeouts.append(options["timeout"])
        return "COPY 1"

    driver = SimpleNamespace(is_in_transaction=lambda: True, copy_from_query=capture, copy_to_table=restore)
    copied = await archive.native_copy_projection(
        driver if raw_driver else _session(driver),
        query,
        *arguments,
        schema_name="candidate",
        table_name="rows",
        columns=("payload",),
        max_bytes=len(wire_bytes),
        timeout=timeout,
    )
    assert copied == len(wire_bytes) and spool.closed
    assert 0 < timeouts[1] <= timeouts[0] <= timeout


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_driver", [False, True])
@pytest.mark.parametrize("missing", ["transaction", "copy_from_query", "copy_to_table"])
async def test_projection_refuses_unowned_or_incomplete_native_driver(raw_driver, missing):
    driver = SimpleNamespace(
        is_in_transaction=lambda: missing != "transaction",
        copy_from_query=AsyncMock(),
        copy_to_table=AsyncMock(),
    )
    if missing != "transaction":
        setattr(driver, missing, None)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="COPY is unavailable"):
        await archive.native_copy_projection(
            driver if raw_driver else _session(driver),
            "SELECT 1",
            schema_name="candidate",
            table_name="rows",
            columns=("value",),
            max_bytes=1024,
            timeout=30,
        )
    for method in (driver.copy_from_query, driver.copy_to_table):
        if method is not None:
            method.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_driver", [False, True])
@pytest.mark.parametrize(
    "override",
    [
        {"max_bytes": True},
        {"max_bytes": -1},
        {"max_bytes": 2**63},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"timeout": 86401},
        {"columns": ("value", "value")},
        {"schema_name": "invalid-name"},
        {"table_name": "invalid-name"},
        {"query": "DELETE FROM rows"},
    ],
)
async def test_projection_checks_identical_bounds_before_either_driver_path(raw_driver, override):
    driver = SimpleNamespace(is_in_transaction=lambda: True, copy_from_query=AsyncMock(), copy_to_table=AsyncMock())
    session = _session(driver)
    options_by_name = dict(
        query="SELECT 1", schema_name="candidate", table_name="rows", columns=("value",), max_bytes=1, timeout=30
    )
    options_by_name.update(override)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="bounds differ"):
        await archive.native_copy_projection(driver if raw_driver else session, **options_by_name)
    session.connection.assert_not_awaited()
    driver.copy_from_query.assert_not_awaited()
    driver.copy_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_requires_sqlalchemy_transaction_before_driver_extraction():
    session = _session(SimpleNamespace())
    session.in_transaction = lambda: False
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="caller transaction"):
        await archive.native_copy_projection(
            session, "SELECT 1", schema_name="candidate", table_name="rows", columns=("value",), max_bytes=1, timeout=30
        )
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_drains_spool_failure_then_closes_without_loading(monkeypatch):
    failure = OSError("synthetic spool failure")
    spool = Mock()
    spool.__enter__ = Mock(return_value=spool)
    spool.__exit__ = Mock(return_value=False)
    spool.write.side_effect = failure
    monkeypatch.setattr(archive, "TemporaryFile", lambda **_kwargs: spool)
    drained_chunks = []

    async def capture(_query, *, output, **_options):
        for chunk in (b"first", b"second", b"last"):
            await output(chunk)
            drained_chunks.append(chunk)
        return "COPY 3"

    driver = SimpleNamespace(copy_from_query=capture, copy_to_table=AsyncMock(), terminate=Mock())
    with pytest.raises(OSError) as caught:
        await archive._copy_native_projection(driver, "SELECT 1", "candidate", "rows", ("value",), 20, 30)
    assert caught.value is failure and drained_chunks == [b"first", b"second", b"last"]
    spool.write.assert_called_once_with(b"first")
    spool.__exit__.assert_called_once()
    driver.copy_to_table.assert_not_awaited()
    driver.terminate.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["out", "in"])
@pytest.mark.parametrize("failure", ["cancel", "deadline"])
@pytest.mark.parametrize("raw_driver", [False, True])
async def test_projection_interrupt_terminates_actual_driver_before_spool_cleanup(
    monkeypatch, phase, failure, raw_driver
):
    spool = TemporaryFile(mode="w+b")
    monkeypatch.setattr(archive, "TemporaryFile", lambda **_kwargs: spool)
    events = []

    async def interrupt():
        if failure == "cancel":
            raise asyncio.CancelledError
        await asyncio.Future()

    async def capture(_query, *, output, **_options):
        if phase == "out":
            await interrupt()
        await output(b"native bytes")
        return "COPY 1"

    async def restore(_table, **_options):
        events.append("restore")
        await interrupt()

    def terminate():
        assert not spool.closed
        events.append("terminate")

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_from_query=capture,
        copy_to_table=restore,
        terminate=terminate,
    )
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else TimeoutError):
        await archive.native_copy_projection(
            driver if raw_driver else _session(driver),
            "SELECT 1",
            schema_name="candidate",
            table_name="rows",
            columns=("value",),
            max_bytes=1024,
            timeout=0.01,
        )
    assert spool.closed
    assert events == (["terminate"] if phase == "out" else ["restore", "terminate"])
