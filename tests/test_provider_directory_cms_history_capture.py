# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""History data mechanics with replaced IO; no native custody or conversion proof."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_history_capture as capture
from tests.test_provider_directory_cms_history_conversion import _request


def _native(identity):
    return dict(
        oid=identity["oid"],
        relname=identity["name"],
        relowner=identity["owner_oid"],
        relkind=identity["kind"],
        native=dict(
            namespace=[identity["schema_oid"]],
            parents=[],
            indexes=[{"class": [oid]} for oid in identity["indexes"].values()],
            constraints=[
                [dict(conname=name, oid=str(proof["oid"]), contype="f", conparentid="0"), proof["definition"]]
                for name, proof in identity["foreign_keys"].items()
            ],
            triggers=[
                dict(
                    tgname=name,
                    oid=str(proof["oid"]),
                    tgfoid=str(proof["function_oid"]),
                    tgisinternal=False,
                    tgtype=proof["tgtype"],
                    tgenabled=proof["enabled"],
                    tgoldtable=proof["old_table"],
                    tgnewtable=proof["new_table"],
                )
                for name, proof in identity["hooks"].items()
            ],
        ),
    )


@pytest.fixture
def history_case():
    request_by_field = _request(same_schema=True)
    family_entries = [_native(heap["old"]) for heap in request_by_field["heaps"].values()]
    family_entries.append(dict(oid=900, relname=capture.scans.RESOURCE, relowner=4, native={"parents": []}))
    parents_by_root = {root: _native(heap["parent"]) for root, heap in request_by_field["heaps"].items()}
    index_names_by_oid = {
        oid: name
        for heap in request_by_field["heaps"].values()
        for identity in (heap["old"], heap["parent"])
        for name, oid in identity["indexes"].items()
    }
    record_by_field = dict(
        transaction="501",
        request=request_by_field,
        dataset_rows={root: {"retained-one": 2, "retained-two": 0} for root in capture.history.ROOTS},
        content={root: dict(rows=2, bytes=64, sha256="a" * 64) for root in capture.history.ROOTS},
    )
    return SimpleNamespace(
        record=record_by_field,
        storage=dict(database_oid=1, catalog_schema="catalog", catalog_schema_oid=2, namespace_owner_oid=4),
        family=family_entries,
        parents=parents_by_root,
        index_names=index_names_by_oid,
    )


@pytest.mark.parametrize("value, expected", [(1, 1), (2**32 - 1, 2**32 - 1), ("1", 1), ("4294967295", 2**32 - 1)])
def test_native_oid_accepts_only_bounded_integers_or_canonical_ascii_decimals(value, expected):
    assert capture._oid(value) == expected


@pytest.mark.parametrize("value", [True, False, None, 0, -1, 2**32, 1.0, "", "0", "01", "+1", " 1", "١", "4294967296"])
def test_native_oid_rejects_coercion_and_out_of_range_values(value):
    with pytest.raises(ValueError, match="cms_history_capture_native_oid_required"):
        capture._oid(value)


@pytest.mark.parametrize("root", capture.scans.ROOTS)
@pytest.mark.parametrize("change", ["missing", "parent"])
def test_fixed_history_roots_must_be_present_and_unattached(history_case, root, change):
    family_entries = history_case.family
    if change == "missing":
        family_entries = [entry for entry in family_entries if entry["relname"] != root]
    else:
        next(entry for entry in family_entries if entry["relname"] == root)["native"]["parents"] = [999]
    with pytest.raises(ValueError, match="cms_history_capture_roots_(required|changed)"):
        capture._old_roots(family_entries)


def test_native_identity_keeps_fixed_dependencies_and_ignores_internal_or_inherited_entries(history_case):
    case = history_case
    root = capture.history.ROOTS[0]
    entry = case.family[0]
    entry["native"]["triggers"].append({"tgisinternal": True})
    entry["native"]["constraints"].extend(
        [
            ({"contype": "p"}, "PRIMARY KEY (dataset_id)"),
            ({"contype": "f", "conparentid": "999"}, "inherited foreign key"),
        ]
    )
    before = deepcopy(entry)
    assert capture._identity(entry, case.index_names, "catalog", root) == case.record["request"]["heaps"][root]["old"]
    assert entry == before
    entry["native"]["triggers"].append({"tgisinternal": False, "tgname": "unrelated_callback"})
    with pytest.raises(ValueError, match="cms_history_capture_hook_changed"):
        capture._identity(entry, case.index_names, "catalog", root)


@pytest.mark.parametrize("change", ["missing", "extra"])
async def test_index_lookup_rejects_incomplete_or_foreign_catalog_results(history_case, change):
    case = history_case
    entries = [*case.family[:2], *case.parents.values()]
    names = [dict(oid=oid, name=name) for oid, name in case.index_names.items()]
    names = names[:-1] if change == "missing" else [*names, dict(oid=999, name="unrelated_index")]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=names))
    with pytest.raises(ValueError, match="cms_history_capture_indexes_changed"):
        await capture._index_names(connection, entries + entries)
    assert connection.fetch.await_args.args[1] == sorted(case.index_names)


async def test_capture_preserves_two_heap_identity_and_data(history_case, monkeypatch):
    case = history_case
    scanned_by_field = {key: deepcopy(case.record[key]) for key in ("dataset_rows", "content")}
    reader = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_history", reader)
    connection = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=[
                [{"dataset_id": key} for key in ("retained-two", "retained-one", "retained-two")],
                [dict(oid=oid, name=name) for oid, name in reversed(case.index_names.items())],
            ]
        ),
        fetchval=AsyncMock(return_value="501"),
    )
    session = object()
    family_entries = [*case.family, {"relname": "unrelated_relation"}]
    before = deepcopy((case.storage, family_entries, case.parents, scanned_by_field))
    actual = await capture.capture_cms_history_data(
        connection, session, storage=case.storage, family=family_entries, parents=case.parents
    )
    assert actual == case.record
    reader.assert_awaited_once_with(
        connection,
        session,
        schema="catalog",
        expected=dict(
            database_oid=1,
            schema_oid=2,
            roots={entry["relname"]: dict(oid=entry["oid"], owner=entry["relowner"]) for entry in case.family},
        ),
        datasets=["retained-one", "retained-two"],
    )
    bounds_sql = connection.fetch.await_args_list[0].args[0]
    assert bounds_sql.count("SELECT DISTINCT dataset_id FROM ONLY") == 2
    assert all(root in bounds_sql for root in capture.history.ROOTS)
    assert capture.scans.RESOURCE not in bounds_sql
    connection.fetchval.assert_awaited_once_with("SELECT pg_catalog.pg_current_xact_id()::text")
    actual["dataset_rows"][capture.history.ROOTS[0]]["retained-one"] = 99
    actual["request"]["heaps"][capture.history.ROOTS[0]]["dataset_ids"].append("later")
    assert (case.storage, family_entries, case.parents, scanned_by_field) == before


async def test_converted_read_uses_planned_heap_identities_and_copies_accepted_content(history_case, monkeypatch):
    case = history_case
    plan = capture.history.plan_cms_history_conversion(case.record["request"])
    scanned_by_field = {key: deepcopy(case.record[key]) for key in ("dataset_rows", "content")}
    reader = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_relations", reader)
    connection, session = object(), object()
    actual = await capture.read_converted_cms_history_data(connection, session, record=case.record, plan=plan)
    assert actual == scanned_by_field
    reader.assert_awaited_once_with(
        connection,
        session,
        schema="catalog",
        receipts={
            capture.history.ROOTS[0]: dict(name="cms_history_100", oid=100),
            capture.history.ROOTS[1]: dict(name="cms_history_200", oid=200),
        },
        datasets_by_root={root: ["retained-one", "retained-two"] for root in capture.history.ROOTS},
        relations_by_root={
            capture.scans.RESOURCE: '"catalog"."provider_directory_dataset_resource"',
            capture.history.ROOTS[0]: '"catalog"."cms_history_100"',
            capture.history.ROOTS[1]: '"catalog"."cms_history_200"',
        },
    )
    actual["content"][capture.history.ROOTS[0]]["sha256"] = "b" * 64
    assert scanned_by_field["content"] == case.record["content"]


@pytest.mark.parametrize("change", ["plan", "counts", "hash"])
async def test_converted_read_refuses_plan_or_retained_content_drift(history_case, monkeypatch, change):
    case = history_case
    plan = capture.history.plan_cms_history_conversion(case.record["request"])
    scanned_by_field = {key: deepcopy(case.record[key]) for key in ("dataset_rows", "content")}
    if change == "plan":
        plan["critical"]["attach"] = ()
    elif change == "counts":
        scanned_by_field["dataset_rows"][capture.history.ROOTS[0]]["retained-one"] += 1
    else:
        scanned_by_field["content"][capture.history.ROOTS[1]]["sha256"] = "b" * 64
    reader = AsyncMock(return_value=scanned_by_field)
    monkeypatch.setattr(capture.scans, "_scan_relations", reader)
    reason = "plan_changed" if change == "plan" else "retained_content_changed"
    with pytest.raises(ValueError, match="cms_history_capture_" + reason):
        await capture.read_converted_cms_history_data(object(), object(), record=case.record, plan=plan)
    if change == "plan":
        reader.assert_not_awaited()
