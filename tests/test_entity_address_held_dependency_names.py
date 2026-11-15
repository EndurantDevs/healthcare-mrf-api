# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Held publication inputs use qualified names from named captured inventories."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_dependency_bindings as bindings


@pytest.mark.asyncio
async def test_named_captured_inventory_is_held_and_relocked_by_name():
    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(return_value=77),
        in_transaction=lambda: True,
    )
    database = SimpleNamespace()
    captured_by_name = {"zcta5": {"schema_name": "tiger_capture", "relation_name": "zcta5"}}
    bindings_by_name = {
        "mrf.geo": {"schema_name": "mrf", "table_name": "geo", "relation_oid": 5, "relfilenode": 6},
    }
    async with bindings._hold_publication_dependencies(database, "mrf", session, captured_by_name, bindings_by_name):
        held = bindings._HELD_PUBLICATION_DEPENDENCIES.get()
        assert held.relation_names == ('"mrf"."geo"', '"tiger_capture"."zcta5"')
        cutover = SimpleNamespace(execute=AsyncMock())
        database._transaction_binding = lambda: SimpleNamespace(session=cutover)
        await bindings.lock_selected_publication_dependencies(database, "mrf")
        session.scalar.assert_not_awaited()
        statement = str(cutover.execute.await_args.args[0])
        assert statement == 'LOCK TABLE ONLY "mrf"."geo", ONLY "tiger_capture"."zcta5" IN ACCESS SHARE MODE NOWAIT'
    assert bindings._HELD_PUBLICATION_DEPENDENCIES.get() is None


@pytest.mark.asyncio
async def test_named_dependency_context_releases_after_cancellation():
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(KeyboardInterrupt):
        async with bindings._hold_publication_dependencies(
            object(), "mrf", session, {}, {"mrf.geo": {"schema_name": "mrf", "table_name": "geo"}}
        ):
            raise KeyboardInterrupt
    assert bindings._HELD_PUBLICATION_DEPENDENCIES.get() is None
    session.scalar.assert_not_awaited()
