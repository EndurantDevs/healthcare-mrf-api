# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named history mechanics preserve complete content and explicit rollback inputs."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_history_capture as capture
from process import provider_directory_cms_history_record as records
from tests.test_provider_directory_cms_history_bounds import _expectations
from tests.test_provider_directory_cms_history_conversion import _request


@pytest.fixture
def history_case():
    request = _request(same_schema=True)
    record_by_field = {
        "request": request,
        "dataset_rows": {root: {"retained-one": 2, "retained-two": 0} for root in capture.history.ROOTS},
        "content": {root: {"rows": 2, "bytes": 64, "sha256": "a" * 64} for root in capture.history.ROOTS},
    }
    return SimpleNamespace(
        record=record_by_field,
        storage={"catalog_schema": "catalog"},
        family=[{"schema": "catalog", "name": root} for root in capture.scans.ROOTS],
        parents={
            root: {key: heap["parent"][key] for key in ("schema", "name")} for root, heap in request["heaps"].items()
        },
        history_names={root: heap["history_name"] for root, heap in request["heaps"].items()},
    )


@pytest.mark.parametrize("root", capture.scans.ROOTS)
@pytest.mark.parametrize("fault", ["missing", "duplicate"])
def test_all_history_roots_are_declared_once(history_case, root, fault):
    entries = history_case.family
    entries = (
        [entry for entry in entries if entry["name"] != root] if fault == "missing" else entries + [{"name": root}]
    )
    with pytest.raises(ValueError, match="roots_required"):
        capture._old_roots(entries)


async def test_capture_scans_complete_data_and_reads_declared_plan_names(history_case, monkeypatch):
    case = history_case
    scanned_by_field = {key: deepcopy(case.record[key]) for key in ("dataset_rows", "content")}
    reader = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_history", reader)
    identities = [heap[key] for heap in case.record["request"]["heaps"].values() for key in ("old", "parent")]
    identity = AsyncMock(side_effect=deepcopy(identities))
    monkeypatch.setattr(capture, "_identity", identity)
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"dataset_id": dataset_id} for dataset_id in ("retained-two", "retained-one")])
    )
    record_by_field = await capture.capture_cms_history_data(
        connection,
        object(),
        storage=case.storage,
        family=case.family,
        parents=case.parents,
        history_names=case.history_names,
    )
    assert record_by_field == case.record
    assert connection.fetch.await_args.args[0].count("SELECT DISTINCT dataset_id FROM ONLY") == 2
    assert reader.await_args.kwargs["expected"] == {
        "schema": "catalog",
        "roots": {root: {"schema": "catalog", "name": root} for root in capture.scans.ROOTS},
    }
    assert [call.args[1] for call in identity.await_args_list] == [
        {key: entry[key] for key in ("schema", "name")} for entry in identities
    ]
    record_by_field["content"][capture.history.ROOTS[0]]["rows"] = 99
    assert scanned_by_field["content"] == case.record["content"]


async def test_explicit_bounds_prevent_unbounded_dataset_discovery(history_case, monkeypatch):
    case = history_case
    expected = _expectations(["retained-one", "retained-two"])
    reader = AsyncMock(return_value={key: case.record[key] for key in ("dataset_rows", "content")})
    monkeypatch.setattr(capture.scans, "_scan_history", reader)
    connection = SimpleNamespace(fetch=AsyncMock())
    observed = await capture.scan_cms_history_prework_data(
        connection, object(), storage=case.storage, family=case.family, expected_history_bounds=expected
    )
    connection.fetch.assert_not_awaited()
    assert observed["history_dataset_ids"] == {root: entry["dataset_ids"] for root, entry in expected.items()}
    assert reader.await_args.kwargs["expected_history_bounds"] == expected


@pytest.mark.parametrize("fault", ["name", "bounds", "count", "digest", "parent", "storage"])
def test_record_retains_scope_names_and_content_accounting(history_case, fault):
    case = history_case
    record, parents, storage = deepcopy((case.record, case.parents, case.storage))
    root = capture.history.ROOTS[0]
    changes_by_fault = {
        "name": (record["request"]["heaps"][root]["old"], "name", "another_heap"),
        "bounds": (record["dataset_rows"][root], "another_dataset", 1),
        "count": (record["content"][root], "rows", 3),
        "digest": (record["content"][root], "sha256", "invalid"),
        "parent": (parents[root], "name", "another_parent"),
        "storage": (storage, "catalog_schema", "another_catalog"),
    }
    target, key, replacement = changes_by_fault[fault]
    target[key] = replacement
    with pytest.raises(ValueError):
        records.require_cms_history_record(record, storage=storage, parents=parents)


@pytest.mark.parametrize("fault", [None, "plan", "counts", "hash"])
async def test_converted_scan_uses_declared_retained_names_and_complete_content(history_case, monkeypatch, fault):
    case = history_case
    plan = capture.history.plan_cms_history_conversion(case.record["request"])
    scanned_by_field = {key: deepcopy(case.record[key]) for key in ("dataset_rows", "content")}
    if fault == "plan":
        plan["critical"]["attach"] = ()
    elif fault == "counts":
        scanned_by_field["dataset_rows"][capture.history.ROOTS[0]]["retained-one"] += 1
    elif fault == "hash":
        scanned_by_field["content"][capture.history.ROOTS[0]]["sha256"] = "b" * 64
    reader = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_relations", reader)
    if fault:
        with pytest.raises(ValueError, match="plan_changed|retained_content_changed"):
            await capture.read_converted_cms_history_data(None, None, record=case.record, plan=plan)
        if fault == "plan":
            reader.assert_not_awaited()
    else:
        assert (
            await capture.read_converted_cms_history_data(None, None, record=case.record, plan=plan) == scanned_by_field
        )
        assert reader.await_args.kwargs["receipts"] == {
            root: {"schema": "catalog", "name": case.history_names[root]} for root in capture.history.ROOTS
        }


async def test_retained_content_uses_catalog_names_after_parent_namespace_move(history_case, monkeypatch):
    case = history_case
    record_by_field = deepcopy(case.record)
    record_by_field["request"]["future_schema"] = "future"
    for heap in record_by_field["request"]["heaps"].values():
        heap["parent"]["schema"] = "future"
    plan = capture.history.plan_cms_history_conversion(record_by_field["request"])
    scanned_by_field = {key: record_by_field[key] for key in ("dataset_rows", "content")}
    scanner = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_relations", scanner)
    assert (
        await capture.read_converted_cms_history_data(None, None, record=record_by_field, plan=plan) == scanned_by_field
    )
    assert scanner.await_args.kwargs["schema"] == "catalog"
    assert all(receipt["schema"] == "catalog" for receipt in scanner.await_args.kwargs["receipts"].values())
