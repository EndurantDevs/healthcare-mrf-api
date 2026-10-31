# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected attachment inventory linkage and exact comparison, with replaced IO."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_attachment_objects as objects


@pytest.fixture
def retained():
    tables_by_root = {"witness": {"oid": 10}, "relationship": {"oid": 20}}
    snapshot_by_field = {
        "storage": [dict(oid=oid, kind=kind, filenode=oid + 100) for oid, kind in ((10, "r"), (11, "i"), (20, "r"))],
        "objects": [[1259, 10], [1259, 11], [1259, 20]],
        "dependencies": [],
        "shared_dependencies": [],
    }
    return SimpleNamespace(
        tables=tables_by_root,
        expected=snapshot_by_field,
        connection=SimpleNamespace(
            fetchval=AsyncMock(return_value=deepcopy(snapshot_by_field)),
            is_in_transaction=lambda: True,
            is_closed=lambda: False,
        ),
    )


async def test_exact_retained_inventory_uses_only_protected_payload_oids(retained):
    state = retained
    before = deepcopy(state.expected)
    await objects.require_retained_attachment_objects(state.connection, state.tables, state.expected)
    state.connection.fetchval.assert_awaited_once_with(objects._OBJECTS_SQL, [10, 20])
    assert state.expected == before


async def test_retained_input_change_during_query_refuses(retained):
    state = retained
    original = deepcopy(state.expected)

    async def change_input(*args):
        state.expected["dependencies"].append({"changed": True})
        return original

    state.connection.fetchval.side_effect = change_input
    with pytest.raises(ValueError, match="inputs_changed"):
        await objects.require_retained_attachment_objects(state.connection, state.tables, state.expected)


async def test_outside_transaction_refuses_before_query(retained):
    state = retained
    state.connection.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="transaction_required"):
        await objects.require_retained_attachment_objects(state.connection, state.tables, state.expected)
    state.connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("field", ["storage", "objects", "dependencies", "shared_dependencies"])
async def test_changed_native_field_refuses(retained, field):
    state = retained
    state.connection.fetchval.return_value[field].append({"changed": True})
    with pytest.raises(ValueError, match="postimage_changed"):
        await objects.require_retained_attachment_objects(state.connection, state.tables, state.expected)


@pytest.mark.parametrize("change", ["missing", "wrong_heap", "duplicate", "partitioned", "filenode"])
async def test_invalid_retained_identity_refuses_before_sql(retained, change):
    state = retained
    if change == "missing":
        del state.expected["dependencies"]
    elif change == "wrong_heap":
        state.expected["storage"][0]["oid"] = 999
    elif change == "duplicate":
        state.expected["storage"].append(deepcopy(state.expected["storage"][1]))
    elif change == "partitioned":
        state.expected["storage"][0]["kind"] = "p"
    else:
        state.expected["storage"][0]["filenode"] = None
    with pytest.raises(ValueError, match="retained_postimage_required|retained_storage_changed"):
        await objects.require_retained_attachment_objects(state.connection, state.tables, state.expected)
    state.connection.fetchval.assert_not_awaited()
