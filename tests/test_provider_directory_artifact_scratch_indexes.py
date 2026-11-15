# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Index projection and original-task refusals; native SQL is not simulated."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_artifact_scratch_indexes as indexes
from process.provider_directory_output_snapshot_catalog import encoded
from tests.test_provider_directory_artifact_scratch_inputs import _scope, directory
from tests.test_provider_directory_output_snapshot_custody import relation


def _native_index(name, oid):
    """Use synthetic native catalog facts solely to exercise their projection."""
    return dict(name=name, oid=oid, valid=True, ready=True, live=True)


def _prepared_scope():
    """Build detached formatter inputs; these are deliberately never issued."""
    source_by_field = dict(relname="pdsi_synthetic", oid=100, indexes=[_native_index("native_control_key", 101)])
    receipts = []
    for number, root in enumerate(indexes.inputs.TYPED_ROOTS):
        successor = replace(
            relation("pdout_synthetic_" + str(number), 200 + number),
            target=root,
            oid=200 + number,
            indexes=(encoded(_native_index("native_typed_key_" + str(number), 300 + number)),),
        )
        receipts.append(SimpleNamespace(successor=successor))
    return SimpleNamespace(
        source_copy_json=encoded(dict(name=source_by_field["relname"], copy=source_by_field)), prepared=tuple(receipts)
    )


@pytest.mark.parametrize("flag", ["valid", "ready", "live"])
def test_unfinished_native_index_is_refused(flag):
    native_index = _native_index("native_key", 101)
    native_index[flag] = False
    with pytest.raises(RuntimeError, match="scratch_index_inventory_changed"):
        indexes._relation_indexes("synthetic", "pdout_synthetic", [native_index])


@pytest.mark.parametrize("changed", ["duplicate_name", "invalid_name", "empty"])
def test_index_inventory_refuses_ambiguous_or_missing_identity(changed):
    native_indexes = [_native_index("native_a", 101), _native_index("native_b", 102)]
    if changed == "duplicate_name":
        native_indexes[1]["name"] = native_indexes[0]["name"]
    elif changed == "invalid_name":
        native_indexes[1]["name"] = None
    else:
        native_indexes = []
    with pytest.raises(RuntimeError, match="scratch_index_inventory_changed"):
        indexes._relation_indexes("synthetic", "pdout_synthetic", native_indexes)


@pytest.mark.asyncio
async def test_unissued_descriptor_and_raw_maps_cannot_obtain_native_index_metadata():
    for scope in (_scope(), {}, SimpleNamespace(relation_by_table={})):
        with pytest.raises(RuntimeError, match="scratch_scope_required"):
            await indexes.scratch_index_metadata(directory, scope)


@pytest.mark.asyncio
async def test_inherited_issued_scope_refuses_before_opening_a_native_transaction():
    scope = _scope()
    indexes.inputs._ISSUED[scope] = (
        directory,
        scope.admission,
        asyncio.current_task(),
        "{}",
        indexes.inputs._scope_state(scope),
    )
    try:
        task = asyncio.create_task(indexes.scratch_index_metadata(directory, scope))
        with pytest.raises(RuntimeError, match="scratch_scope_required"):
            await task
    finally:
        del indexes.inputs._ISSUED[scope]


@pytest.mark.asyncio
async def test_exact_nine_native_index_identities_use_verified_boundary(monkeypatch):
    scope = _prepared_scope()
    events = []

    @asynccontextmanager
    async def transaction(selected_directory, selected_scope):
        # Exercise boundary ordering and formatting, without representing native authority.
        assert selected_directory is directory and selected_scope is scope
        events.append("verified_boundary_entered")
        rows = iter(
            [
                [_native_index("native_control_key", 101)],
                *[
                    [_native_index("native_typed_key_" + str(number), 300 + number)]
                    for number in range(len(indexes.inputs.TYPED_ROOTS))
                ],
            ]
            * 2
        )
        from unittest.mock import AsyncMock

        yield SimpleNamespace(
            policy=SimpleNamespace(schema="synthetic"),
            connection=SimpleNamespace(fetch=AsyncMock(side_effect=lambda *args: next(rows))),
        )
        events.append("verified_boundary_closed")

    monkeypatch.setattr(indexes.inputs, "scratch_input_transaction", transaction)
    observed = await indexes.scratch_index_metadata(directory, scope)
    assert events == ["verified_boundary_entered", "verified_boundary_closed"]
    assert set(observed) == {indexes.inputs.SOURCE, *indexes.inputs.TYPED_ROOTS}
    assert observed[indexes.inputs.SOURCE] == dict(
        schema="synthetic", relation="pdsi_synthetic", indexes=("native_control_key",)
    )
    for number, root in enumerate(indexes.inputs.TYPED_ROOTS):
        assert observed[root]["indexes"] == ("native_typed_key_" + str(number),)
        assert set(observed[root]) == {"schema", "relation", "indexes"}
    observed[indexes.inputs.SOURCE]["indexes"] = ()
    assert (await indexes.scratch_index_metadata(directory, scope))[indexes.inputs.SOURCE]["indexes"]
