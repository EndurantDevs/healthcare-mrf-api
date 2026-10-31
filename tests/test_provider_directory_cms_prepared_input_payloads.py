# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resource/witness copies keep exact payloads, bounded paging and rollback."""

import asyncio
import json
from collections import Counter
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_derived as derived
from tests.test_provider_directory_cms_prepared_context import case as case
from tests.test_provider_directory_cms_prepared_relationships import graph as graph

_RESOURCE = derived.storage.RESOURCE
_WITNESS = derived.storage.WITNESS
_COLUMNS = {
    _RESOURCE: (
        "dataset_id",
        "resource_type",
        "resource_id",
        "payload_hash",
        "payload_json",
        "acquired_resource_sha256",
    ),
    _WITNESS: (
        "dataset_id",
        "source_id",
        "release_id",
        "resource_type",
        "resource_id",
        "raw_payload_sha256",
        "normalized_payload_hash",
        "raw_payload_json",
    ),
}


def _input_rows(identity, resource_ids=("org-1", "org-10", "org-2")):
    resources, witnesses = [], []
    for number, resource_id in enumerate(resource_ids):
        common_by_field = dict(dataset_id=identity["dataset_id"], resource_type="Organization", resource_id=resource_id)
        resources.append(
            dict(
                **common_by_field,
                payload_hash="a" * 64,
                payload_json=json.dumps(dict(resourceType="Organization", id=resource_id, name="Example é")),
                acquired_resource_sha256=None if number == 2 else "b" * 64,
            )
        )
        witnesses.append(
            dict(
                **common_by_field,
                source_id="cms-npd",
                release_id=identity["metadata"]["source_release"]["vector_sha256"],
                raw_payload_sha256="c" * 64,
                normalized_payload_hash="a" * 64,
                raw_payload_json=json.dumps(
                    dict(resourceType="Organization", id=resource_id, name="Example é", meta=dict(tag=["original"]))
                ),
            )
        )
    return {_RESOURCE: resources, _WITNESS: witnesses}


class _PayloadSavepoint:
    def __init__(self, state):
        self.state = state

    async def start(self):
        self.before_outputs = deepcopy(self.state.output)

    async def commit(self):
        return None

    async def rollback(self):
        self.state.output = self.before_outputs
        self.state.rollbacks += 1


def _page_with_fault(state, root, page_rows):
    fault = state.page_fault
    if not page_rows or root != state.fault_root or fault is None:
        return page_rows
    if fault == "oversize":
        return [dict.fromkeys(_COLUMNS[root]) | {"row_bytes": 4096 - 20}]
    if fault == "total":
        assert len(page_rows) == 2
        return [{**payload_by_field, "row_bytes": 2048} for payload_by_field in page_rows]
    if fault == "row_count":
        return [*page_rows, dict(page_rows[-1])]
    if fault == "missing":
        page_rows[0].pop("row_bytes")
        return page_rows
    page_rows[0]["row_bytes"] = {
        "null": None,
        "true": True,
        "false": False,
        "zero": 0,
        "negative": -1,
        "float": 20.0,
    }[fault]
    return page_rows


def _typed_page(root, payload_rows):
    return [
        dict(
            payload_by_field,
            row_bytes=2
            + sum(
                4 + (len(str(payload_by_field[column]).encode()) + 1 if payload_by_field[column] is not None else 0)
                for column in _COLUMNS[root]
            ),
        )
        for payload_by_field in payload_rows[:2]
    ]


async def _fetch_page(state, query, *parameters):
    root = next(root for root in _COLUMNS if state.handoff.document["tables"][root]["name"] in query)
    assert parameters[-2:] == (2, 4096 - 21)
    assert "COLLATE" not in query and "LIMIT $4" in query and "page_bytes<=$5" in query
    assert "coalesce(pg_catalog.octet_length" in query
    json_column = "payload_json" if root == _RESOURCE else "raw_payload_json"
    assert f'"{json_column}"::text AS "{json_column}"' in query
    assert all(f'CASE WHEN row_bytes<=$5 THEN "{column}" END AS "{column}"' in query for column in _COLUMNS[root])
    assert ",row_bytes FROM bounded" in query
    keys = ("dataset_id", "resource_type", "resource_id")
    payload_rows = state.input[root]
    if not state.stalled and parameters[:-2] != ("", "", ""):
        position = next(
            number
            for number, payload_by_field in enumerate(payload_rows)
            if tuple(payload_by_field[key] for key in keys) == parameters[:-2]
        )
        payload_rows = payload_rows[position + 1 :]
    return _page_with_fault(state, root, _typed_page(root, payload_rows))


async def _copy_page(state, root, columns, copy_rows):
    assert columns == _COLUMNS[root]
    assert 0 < len(copy_rows) <= 2
    state.output[root].extend(copy_rows)
    if state.fail == root:
        raise RuntimeError("payload copy failed")
    if state.fail == "cancel" and root == _WITNESS:
        raise asyncio.CancelledError()
    if state.fail == "coordinates" and root == _WITNESS:
        state.drift = True
    return len(copy_rows) - int(state.short)


async def _output_exists(state, query):
    root = next(root for root in _COLUMNS if state.workspace.receipts[root]["name"] in query)
    assert query.startswith("SELECT EXISTS")
    return bool(state.output[root])


async def _compare_rows(state, query):
    root = next(root for root in _COLUMNS if state.workspace.receipts[root]["name"] in query)
    assert query.count("EXCEPT ALL") == 2
    selected = ",".join(f'"{column}"' + ("::jsonb" if column.endswith("_json") else "") for column in _COLUMNS[root])
    assert query.count("SELECT " + selected + " FROM ") == 4
    expected_rows = [
        tuple(payload_by_field[column] for column in _COLUMNS[root]) for payload_by_field in state.input[root]
    ]
    return dict(changed=Counter(expected_rows) != Counter(state.output[root]), count=len(state.output[root]))


@pytest.fixture
def payloads(graph, monkeypatch):
    """Retain typed input/output rows inside the real helper's mock savepoint."""
    current, connection = graph.workspace, graph.connection
    state = SimpleNamespace(
        workspace=current,
        handoff=graph.handoff,
        connection=connection,
        input=_input_rows(current.identity),
        output={root: [] for root in _COLUMNS},
        fail=None,
        short=False,
        stalled=False,
        drift=False,
        page_fault=None,
        fault_root=_RESOURCE,
        rollbacks=0,
    )
    coordinates = deepcopy(current._inputs["native_coordinates"])
    monkeypatch.setattr(derived, "COPY_MAX_ROWS", 2)
    monkeypatch.setattr(derived, "COPY_MAX_BYTES", 4096)
    monkeypatch.setattr(
        derived.context,
        "_observe",
        AsyncMock(side_effect=lambda *_args: {} if state.drift else deepcopy(coordinates)),
    )
    connection.transaction = partial(_PayloadSavepoint, state)
    connection.fetch = AsyncMock(side_effect=partial(_fetch_page, state))
    connection.fetchval = AsyncMock(side_effect=partial(_output_exists, state))
    connection.fetchrow = AsyncMock(side_effect=partial(_compare_rows, state))
    current.copy_rows = AsyncMock(side_effect=partial(_copy_page, state))
    return state


async def test_copies_both_complete_typed_payloads_in_bounded_pages_then_verifies(payloads):
    original = deepcopy(payloads.handoff.document)
    result = await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert result == dict(
        contract="cms-prepared-input-payloads-v1",
        state="copied",
        dataset_id=payloads.workspace.identity["dataset_id"],
        handoff_sha256=payloads.handoff.document_sha256,
        preparation_id=payloads.workspace.binding["preparation_id"],
        row_counts={_RESOURCE: 3, _WITNESS: 3},
    )
    for root, rows in payloads.input.items():
        assert payloads.output[root] == [tuple(row[column] for column in _COLUMNS[root]) for row in rows]
        calls = [call for call in payloads.workspace.copy_rows.await_args_list if call.args[0] == root]
        assert [len(call.args[2]) for call in calls] == [2, 1]
    assert payloads.connection.fetch.await_count == 6
    payloads.workspace._indexed = True
    assert await derived.verify_prepared_cms_input_payloads(payloads.workspace, payloads.handoff) == {
        **result,
        "state": "verified",
    }
    assert payloads.connection.fetchrow.await_count == 2
    assert payloads.handoff.document == original


async def test_empty_inputs_keep_both_zero_counts_without_copy(payloads):
    payloads.input = {root: [] for root in _COLUMNS}
    result = await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert result["row_counts"] == {_RESOURCE: 0, _WITNESS: 0}
    payloads.workspace.copy_rows.assert_not_awaited()
    payloads.workspace._indexed = True
    verified = await derived.verify_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert verified["state"] == "verified" and verified["row_counts"] == result["row_counts"]


async def test_native_sql_order_may_advance_opposite_to_python_lexical_order(payloads):
    resource_ids = ("org-a", "org-b", "org-C")
    assert resource_ids[2] < resource_ids[1]
    payloads.input = _input_rows(payloads.workspace.identity, resource_ids)
    result = await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert result["row_counts"] == {_RESOURCE: 3, _WITNESS: 3}
    for root in _COLUMNS:
        id_column = _COLUMNS[root].index("resource_id")
        assert tuple(row[id_column] for row in payloads.output[root]) == resource_ids
    payloads.workspace._indexed = True
    assert (await derived.verify_prepared_cms_input_payloads(payloads.workspace, payloads.handoff))[
        "state"
    ] == "verified"


@pytest.mark.parametrize("root", (_RESOURCE, _WITNESS))
@pytest.mark.parametrize(
    "fault", ("oversize", "missing", "null", "true", "false", "zero", "negative", "float", "row_count", "total")
)
async def test_invalid_page_metadata_refuses_before_copy_and_rolls_back(payloads, root, fault):
    payloads.fault_root, payloads.page_fault = root, fault
    with pytest.raises(ValueError, match="copy_page_unbounded"):
        await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert not any(call.args[0] == root for call in payloads.workspace.copy_rows.await_args_list)
    assert not any(payloads.output.values()) and payloads.rollbacks == 1
    payloads.page_fault = None
    assert (await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff))["row_counts"] == {
        _RESOURCE: 3,
        _WITNESS: 3,
    }


@pytest.mark.parametrize("root", (_RESOURCE, _WITNESS))
async def test_occupied_outputs_refuse_before_any_payload_read_or_copy(payloads, root):
    payloads.output[root] = [("existing",)]
    before = deepcopy(payloads.output)
    with pytest.raises(ValueError, match="output_not_empty"):
        await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert payloads.output == before
    payloads.workspace.copy_rows.assert_not_awaited()
    payloads.connection.fetch.assert_not_awaited()


@pytest.mark.parametrize("change", ("handoff", "identity", "resource_alias", "witness_alias", "frozen", "indexed"))
async def test_wrong_identity_or_loading_scope_refuses_before_payload_io(payloads, change):
    if change == "handoff":
        payloads.workspace._binding["handoff_sha256"] = "0" * 64
    elif change == "identity":
        payloads.workspace._binding["identity"]["dataset_id"] = "another-dataset"
    elif change.endswith("_alias"):
        root = _RESOURCE if change == "resource_alias" else _WITNESS
        payloads.workspace._receipts[root]["oid"] = payloads.workspace._inputs["frozen_inventory"]["relations"][0][
            "relation_oid"
        ]
    elif change == "frozen":
        payloads.drift = True
    else:
        payloads.workspace._indexed = True
    with pytest.raises(ValueError):
        await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    payloads.workspace.copy_rows.assert_not_awaited()
    payloads.connection.fetch.assert_not_awaited()


@pytest.mark.parametrize("failure", (_RESOURCE, _WITNESS, "short", "cancel", "coordinates", "stalled"))
async def test_partial_failure_rolls_back_both_outputs_and_allows_exact_retry(payloads, failure):
    payloads.fail = failure
    payloads.short, payloads.stalled = failure == "short", failure == "stalled"
    error = asyncio.CancelledError if failure == "cancel" else (RuntimeError, ValueError)
    with pytest.raises(error):
        await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert not any(payloads.output.values()) and payloads.rollbacks == 1
    payloads.fail, payloads.short, payloads.stalled, payloads.drift = None, False, False, False
    result = await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert result["row_counts"] == {_RESOURCE: 3, _WITNESS: 3}


@pytest.mark.parametrize(
    "root,change",
    (
        (_RESOURCE, "missing"),
        (_WITNESS, "duplicate"),
        (_RESOURCE, "acquired_resource_sha256"),
        (_RESOURCE, "payload_json"),
        (_WITNESS, "raw_payload_json"),
        (_WITNESS, "raw_payload_sha256"),
        (_WITNESS, "normalized_payload_hash"),
    ),
)
async def test_verification_rejects_multiset_and_exact_payload_field_changes(payloads, root, change):
    await derived.copy_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    payloads.workspace._indexed = True
    if change == "missing":
        payloads.output[root].pop()
    elif change == "duplicate":
        payloads.output[root].append(payloads.output[root][0])
    else:
        record_values = list(payloads.output[root][0])
        record_values[_COLUMNS[root].index(change)] = "{}" if change.endswith("_json") else "f" * 64
        payloads.output[root][0] = tuple(record_values)
    before = deepcopy(payloads.output)
    with pytest.raises(ValueError, match="projection_changed"):
        await derived.verify_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    assert payloads.output == before


async def test_verification_requires_indexes_before_comparing_payloads(payloads):
    with pytest.raises(ValueError, match="indexes_required"):
        await derived.verify_prepared_cms_input_payloads(payloads.workspace, payloads.handoff)
    payloads.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("operation", ("copy_prepared_cms_input_payloads", "verify_prepared_cms_input_payloads"))
async def test_unrelated_workspace_is_rejected_without_io(operation):
    with pytest.raises(ValueError, match="scope_required"):
        await getattr(derived, operation)(object(), object())
