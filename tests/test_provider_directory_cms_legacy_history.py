# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Invalid history requests fail before a database session or payload is accessed."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_legacy_history as history
from tests.test_provider_directory_cms_history_bounds import _expectations


def _catalog():
    return {
        "database_oid": 51,
        "schema_oid": 52,
        "roots": {root: {"oid": index + 1, "owner": 42} for index, root in enumerate(history.ROOTS)},
    }


@pytest.mark.parametrize("fault", ["missing", "extra", "alias", "owner", "bool_oid", "fields"])
async def test_invalid_history_coordinates_refuse_before_io(fault):
    expected_by_field = _catalog()
    expected_by_root = expected_by_field["roots"]
    if fault == "missing":
        del expected_by_root[history.WITNESS]
    if fault == "extra":
        expected_by_root["unrelated"] = {"oid": 43, "owner": 42}
    if fault == "alias":
        expected_by_root[history.WITNESS] = deepcopy(expected_by_root[history.RESOURCE])
    if fault == "owner":
        expected_by_root[history.WITNESS]["owner"] = 0
    if fault == "bool_oid":
        expected_by_root[history.WITNESS]["oid"] = True
    if fault == "fields":
        expected_by_root[history.WITNESS]["name"] = "unrelated"
    with pytest.raises(ValueError, match="roots_required"):
        await history.read_cms_legacy_history(
            None, None, None, catalog_schema="example", expected_catalog=expected_by_field, dataset_ids=[]
        )


@pytest.mark.parametrize("datasets", [("one",), [""], ["a" * 97], ["one\x00"], [None], ["b", "a"], ["a", "a"]])
async def test_invalid_history_bounds_refuse_before_io(datasets):
    with pytest.raises(ValueError, match="datasets_required"):
        await history.read_cms_legacy_history(
            None, None, None, catalog_schema="example", expected_catalog=_catalog(), dataset_ids=datasets
        )


@pytest.mark.parametrize("field,value", [("database_oid", True), ("database_oid", 0), ("schema_oid", "52")])
async def test_invalid_catalog_coordinates_refuse_before_io(field, value):
    expected_by_field = _catalog()
    expected_by_field[field] = value
    with pytest.raises(ValueError, match="catalog_required"):
        await history.read_cms_legacy_history(
            None, None, None, catalog_schema="example", expected_catalog=expected_by_field, dataset_ids=[]
        )


def test_request_detaches_native_coordinates_and_bounds():
    expected_by_field = _catalog()
    datasets = ["one"]
    native, bounds = history._request("example", expected_by_field, datasets)
    native["roots"][history.WITNESS]["owner"] += 1
    bounds.append("two")
    assert expected_by_field["roots"][history.WITNESS]["owner"] == 42 and datasets == ["one"]


async def test_shared_scan_preserves_full_content_counts_and_both_layout_checks(monkeypatch):
    expected = _catalog()
    connection, session = object(), object()
    roots = AsyncMock()
    counts = AsyncMock(return_value={"one": 1})
    copies = AsyncMock(return_value={"rows": 1, "bytes": 50, "sha256": "a" * 64})
    relationships = AsyncMock()
    for name, callback in {
        "_roots": roots,
        "_counts": counts,
        "_copy_native_content": copies,
        "_relationships": relationships,
    }.items():
        monkeypatch.setattr(history, name, callback)
    observed = await history._scan_history(connection, session, schema="example", expected=expected, datasets=["one"])
    assert observed["catalog"] == expected
    assert set(observed["dataset_rows"]) == set(observed["content"]) == {history.WITNESS, history.RELATIONSHIP}
    assert roots.await_count == 2 and relationships.await_count == 1 and copies.await_count == 2
    assert [call.kwargs["receipt"]["oid"] for call in copies.await_args_list] == [2, 3]


async def test_shared_scan_refuses_incomplete_copy_before_relationships(monkeypatch):
    monkeypatch.setattr(history, "_roots", AsyncMock())
    monkeypatch.setattr(history, "_counts", AsyncMock(return_value={"one": 2}))
    monkeypatch.setattr(history, "_copy_native_content", AsyncMock(return_value={"rows": 1}))
    relationships = AsyncMock()
    monkeypatch.setattr(history, "_relationships", relationships)
    with pytest.raises(ValueError, match="row_count_changed"):
        await history._scan_history(None, None, schema="example", expected=_catalog(), datasets=["one"])
    relationships.assert_not_awaited()


@pytest.mark.parametrize("fault", ["oid", "datasets", "partial"])
async def test_explicit_history_expectation_mismatch_refuses_before_session_io(fault):
    expected = _catalog()
    checked = _expectations()
    for root, entry in checked.items():
        entry["old_oid"] = expected["roots"][root]["oid"]
    if fault == "oid":
        checked[history.WITNESS]["old_oid"] = 999
    elif fault == "datasets":
        checked[history.WITNESS]["dataset_ids"] = []
        checked[history.RELATIONSHIP]["dataset_ids"] = []
        for entry in checked.values():
            entry["native"]["constraint"]["columns"] = []
    else:
        checked.pop(history.RELATIONSHIP)
    with pytest.raises(ValueError, match="history_bounds_changed|expectations_required"):
        await history.read_cms_legacy_history(
            None,
            None,
            None,
            catalog_schema="example",
            expected_catalog=expected,
            dataset_ids=["one"],
            expected_history_bounds=checked,
        )


async def test_explicit_history_scan_preserves_each_sealed_bound_and_both_layout_checks(monkeypatch):
    expected = _catalog()
    checked = _expectations()
    for root, entry in checked.items():
        entry["old_oid"] = expected["roots"][root]["oid"]
    checked[history.RELATIONSHIP]["dataset_ids"] = ["two"]
    roots = AsyncMock()
    scanner = AsyncMock(return_value={"dataset_rows": {}, "content": {}})
    monkeypatch.setattr(history, "_roots", roots)
    monkeypatch.setattr(history, "_scan_relations", scanner)
    connection, session = object(), object()
    await history._scan_history(
        connection,
        session,
        schema="example",
        expected=expected,
        datasets=["one", "two"],
        expected_history_bounds=checked,
    )
    assert roots.await_count == 2
    assert all(call.kwargs == {"expected_history_bounds": checked} for call in roots.await_args_list)
    assert scanner.await_args.kwargs["datasets_by_root"] == {history.WITNESS: ["one"], history.RELATIONSHIP: ["two"]}


async def test_root_layout_forwards_explicit_expectations_without_replacing_native_identity(monkeypatch):
    from types import SimpleNamespace

    expected = _catalog()
    checked = _expectations()
    for root, entry in checked.items():
        entry["old_oid"] = expected["roots"][root]["oid"]
    rows = [
        dict(
            name=root,
            nspname="example",
            schema_oid=52,
            relkind="p" if root == history.RESOURCE else "r",
            leaf=True,
            **proof,
        )
        for root, proof in expected["roots"].items()
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    authenticate = AsyncMock()
    monkeypatch.setattr(history.layout, "require_cms_canonical_layout", authenticate)
    await history._roots(connection, "example", expected, expected_history_bounds=checked)
    authenticate.assert_awaited_once_with(
        connection, catalog_schema="example", storage_class=history.layout.CONTRACT, expected_history_bounds=checked
    )
