# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Checkpoint fixture DDL keeps transaction order and native attestation."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import provider_directory_terminal_root_retirement_pg_support as support


@asynccontextmanager
async def _transaction(events, label):
    events.append(label + "_open")
    try:
        yield
    finally:
        events.append(label + "_close")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_checkpoint_ddl_is_one_ordered_transaction_before_attestation(monkeypatch, failure):
    events = []
    execute = AsyncMock(side_effect=failure("synthetic interruption") if failure else None)
    connection = SimpleNamespace(execute=execute, transaction=lambda: _transaction(events, "ddl"))
    signatures = lambda schema: (f'"{schema}".first()', f'"{schema}".second(text)')
    scenario = SimpleNamespace(
        connection=connection,
        schema='"synthetic"',
        schema_name="synthetic",
        migration=SimpleNamespace(_function_signatures=signatures),
    )
    component = SimpleNamespace(
        owner="owner",
        publisher="publisher",
        reader="reader",
        policy=SimpleNamespace(runtime_roles=("reader",)),
        database=object(),
    )
    publisher = SimpleNamespace(policy=component.policy, connection=object())
    monkeypatch.setattr(support, "RELATION_ORDERS", {"table_one": "id", "table_two": "id"})
    monkeypatch.setattr(support, "CHECKPOINT_LAYOUTS", {"checkpoint": {}})
    monkeypatch.setattr(support, "source_transaction", lambda database, isolation: _transaction(events, "attest"))
    monkeypatch.setattr(support, "bound_publisher", lambda: publisher)
    capture = AsyncMock()
    monkeypatch.setattr(support, "capture_checkpoint_controls", capture)
    if failure:
        with pytest.raises(failure, match="synthetic interruption"):
            await support._adopt_checkpoint_fixture(scenario, component)
        assert events == ["ddl_open", "ddl_close"]
        capture.assert_not_awaited()
        return
    await support._adopt_checkpoint_fixture(scenario, component)
    execute.assert_awaited_once()
    assert execute.await_args.args[0].split(";\n") == [
        'ALTER TABLE "synthetic"."table_one" OWNER TO "owner"',
        'ALTER TABLE "synthetic"."table_two" OWNER TO "owner"',
        'ALTER FUNCTION "synthetic".first() OWNER TO "owner"',
        'REVOKE ALL ON FUNCTION "synthetic".first() FROM PUBLIC',
        'ALTER FUNCTION "synthetic".second(text) OWNER TO "owner"',
        'REVOKE ALL ON FUNCTION "synthetic".second(text) FROM PUBLIC',
        'ALTER FUNCTION "synthetic"."guard_provider_directory_subset_abandonment_checkpoint"() OWNER TO "owner"',
        'REVOKE ALL ON FUNCTION "synthetic"."guard_provider_directory_subset_abandonment_checkpoint"() FROM PUBLIC',
        'REVOKE ALL ON TABLE "synthetic"."checkpoint" FROM PUBLIC, "publisher","reader"',
        'GRANT SELECT ON TABLE "synthetic"."checkpoint" TO "reader"',
    ]
    assert events == ["ddl_open", "ddl_close", "attest_open", "attest_close"]
    capture.assert_awaited_once_with(publisher.connection, component.policy)
