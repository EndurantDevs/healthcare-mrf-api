# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dependency evidence pins parents while preserving ordinary mutable storage."""

import importlib.util
import json
import re
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_dependency_policy as dependencies
from tests.test_provider_directory_source_acquisition_custody import _plans


def _parent(oid, name):
    kind = "p" if name in dependencies.acquisition.BULK_ROOTS else "r"
    return dict(
        oid=oid,
        nspname="catalog",
        relname=name,
        relowner=50,
        relkind=kind,
        relpersistence="p",
        native={
            "parents": [],
            "relation": [oid, name, kind, "p", 50, False],
            "constraints": [[{"oid": 99, "conparentid": 7, "confrelid": 1234}, "raw FK clone"]],
        },
    )


def _indexes(model):
    indexes = [
        dict(
            name=str(constraint.name),
            method="btree",
            unique=True,
            primary=isinstance(constraint, PrimaryKeyConstraint),
            ready=True,
            expression_free=True,
            plain=True,
            columns=[column.name for column in constraint.columns],
        )
        for constraint in model.__table__.constraints
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint))
    ]
    partial_indexes = []
    for specification in getattr(model, "__my_additional_indexes__", ()):
        name = specification.get("name") or "_".join(specification["index_elements"])
        index_by_field = dict(
            name=name,
            method=specification.get("using", "btree"),
            unique=bool(specification.get("unique")),
            primary=False,
            ready=True,
            expression_free=True,
            plain="where" not in specification,
            columns=list(specification["index_elements"]),
        )
        indexes.append(index_by_field)
        if "where" in specification:
            predicate = specification["where"]
            if model.__tablename__ == "provider_directory_rooted_graph_work":
                predicate = "kind::text = 'full_insurance_plan_census'::text"
            partial_indexes.append({**index_by_field, "predicate": predicate})
    return indexes, partial_indexes


def _layout(parent, model):
    columns = [
        dict(name=name, type=kind, required=required, identity="", generated="")
        for name, (kind, required) in dependencies.controls._model_columns(model).items()
    ]
    is_partitioned = parent["relkind"] == "p"
    position = next((number for number, column in enumerate(columns, 1) if column["name"] == "acquisition_id"), None)
    parent_by_field = dict(
        oid=parent["oid"],
        kind=parent["relkind"],
        persistence="p",
        rls=False,
        options=None,
        strategy="l" if is_partitioned else None,
        partition_columns=[position] if is_partitioned else None,
        expressions=None,
    )
    indexes, partial = _indexes(model)
    return dict(parent=[parent_by_field], columns=columns, indexes=indexes, partial=partial)


def _physical_rows():
    return {
        oid: {
            "rowtypes": [],
            "toast_oid": 0,
            "table_acl": [[50, 50, "SELECT", False], [60, 50, "SELECT", True]],
            "column_acl": [[2, 60, 50, "UPDATE", False]],
        }
        for oid in [1, *range(100, 122)]
    }


def _v2_sections():
    return dict(
        canonical={"relations": [{"oid": 1}], "migration_plan": {"complete": True}},
        private=[{"oid": 2}],
        routines=[{"oid": 5000, "native": {"body": "fixed"}}],
    )


def _routine_rows(original):
    return [
        {
            **entry,
            "native": {
                **entry["native"],
                "acl": [[50, 50, "EXECUTE", False], [60, 50, "EXECUTE", False]],
                "namespace_identifier": "catalog",
            },
        }
        for entry in [
            *original,
            {"oid": 5001, "native": {"body": "dependency"}},
            {"oid": 6000, "native": {"body": "retirement"}},
            {"oid": 7000, "native": {"body": "callback"}},
        ]
    ]


def _reverse_fixture(monkeypatch):
    histories = [_parent(number, name) for number, name in enumerate(dependencies.HISTORY_ROOTS, 120)]
    plan_rows = [{"singleton": True, "work_oid": 120, "resource_oid": 121}]
    for method in ("_control_layouts", "_history_layouts", "_history_checks", "_history_scopes"):
        monkeypatch.setattr(dependencies, method, AsyncMock())
    for method in ("_proof_foreign", "_control_hooks", "_history_hooks"):
        monkeypatch.setattr(dependencies, method, lambda *arguments: None)
    monkeypatch.setattr(dependencies, "_migration_plan", AsyncMock(return_value=plan_rows))
    monkeypatch.setattr(dependencies, "_read_migration_plan", AsyncMock(return_value=plan_rows))
    monkeypatch.setattr(dependencies, "_history_relations", AsyncMock(return_value=histories))
    return histories, plan_rows


@pytest.fixture
def dependency_case(monkeypatch):
    parents = [_parent(number, name) for number, name in enumerate(dependencies.ROOTS, 100)]
    layout_by_name = {
        storage_relation("catalog", parent["relname"]): _layout(parent, dependencies._models()[parent["relname"]])
        for parent in parents
        if parent["relname"] in dependencies._models()
    }
    histories, plan = _reverse_fixture(monkeypatch)
    v2_by_section = _v2_sections()
    collect = AsyncMock(side_effect=lambda *arguments: deepcopy(v2_by_section))
    monkeypatch.setattr(dependencies.policy_checks, "_collect_objects", collect)
    frontier = AsyncMock(return_value=[{"oid": 5000}, {"oid": 5001}])
    monkeypatch.setattr(dependencies.custody, "_callee_frontier", frontier)
    retirement = AsyncMock(return_value=[{"oid": 6000}])
    monkeypatch.setattr(dependencies, "_retirement_frontier", retirement)
    callbacks = AsyncMock(return_value=[{"oid": 7000}])
    monkeypatch.setattr(dependencies, "_callback_frontier", callbacks)
    physical_by_oid = _physical_rows()
    routine_rows = _routine_rows(v2_by_section["routines"])

    async def fetch(query, *arguments):
        if query == dependencies._PARENTS_SQL:
            assert arguments[0] == "catalog" and arguments[1] in (
                list(dependencies.ROOTS),
                list(dependencies.HISTORY_ROOTS),
            )
            selected = histories if arguments[1] == list(dependencies.HISTORY_ROOTS) else parents
            return [{**parent, "native": json.dumps(parent["native"])} for parent in selected]
        if query == dependencies._LAYOUT_SQL:
            return [dict(name=name, layout=json.dumps(layout_by_name[name])) for name in arguments[0]]
        if query == dependencies._PHYSICAL_SQL:
            return [dict(oid=oid, native=json.dumps(physical_by_oid[oid])) for oid in arguments[0]]
        assert query == dependencies._ROUTINES_SQL
        return [
            {**entry, "native": deepcopy(entry["native"]) if entry["oid"] == 5000 else json.dumps(entry["native"])}
            for entry in routine_rows
        ]

    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch), fetchval=AsyncMock(return_value='{"v3":true}'), execute=AsyncMock()
    )
    return SimpleNamespace(
        parents=parents,
        histories=histories,
        plan=plan,
        layouts=layout_by_name,
        observed=v2_by_section,
        collect=collect,
        frontier=frontier,
        retirement=retirement,
        callbacks=callbacks,
        physical=physical_by_oid,
        routines=routine_rows,
        connection=connection,
        storage=SimpleNamespace(catalog_schema="catalog", storage_schema="private"),
        policy=SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("reader",)),
    )


async def _collect(case):
    return await dependencies._collect(case.connection, case.storage, case.policy, [77])


@pytest.mark.asyncio
async def test_complete_dependency_extension_preserves_v2_roots_and_raw_constraints(dependency_case):
    case = dependency_case
    before = deepcopy(case.observed)
    observed = await _collect(case)
    case.collect.assert_awaited_once_with(case.connection, case.storage, case.policy, [77])
    assert case.observed == before
    assert {
        name: contents
        for name, contents in observed["canonical"].items()
        if name not in {"dependency_relations", "dependency_histories", "dependency_migration_plan", "physical"}
    } == before["canonical"]
    assert observed["private"] == before["private"]
    assert observed["canonical"]["dependency_relations"] == case.parents
    assert observed["canonical"]["physical"] == [
        dict(oid=oid, native=case.physical[oid]) for oid in [1, *range(100, 122)]
    ]
    assert observed["canonical"]["dependency_histories"] == case.histories
    assert observed["canonical"]["dependency_migration_plan"] == {dependencies.PROVENANCE_ROOT: case.plan}
    assert observed["routines"] == case.routines
    assert observed["roles"] == {"v3": True}
    assert case.frontier.await_count == 2
    assert case.retirement.await_count == 2
    assert case.callbacks.await_count == 2
    case.connection.fetchval.assert_awaited_once_with(
        dependencies.custody._ROLES_SQL,
        [1, 2, *range(100, 122)],
        ["catalog", "private"],
        [5000, 5001, 6000, 7000],
        ["owner", "publisher", "reader"],
    )
    routine_call = next(
        call for call in case.connection.fetch.await_args_list if call.args[0] == dependencies._ROUTINES_SQL
    )
    assert routine_call.args[1:] == ([1, 2, *range(100, 122)], [5000, 5001, 6000, 7000])


@pytest.mark.asyncio
async def test_v3_collects_expanded_roles_once_without_discarded_base_scan(dependency_case, monkeypatch):
    case = dependency_case
    unused = AsyncMock(side_effect=AssertionError("v3 must not collect discarded base roles"))
    monkeypatch.setattr(dependencies.policy_checks, "_collect", unused)
    before = deepcopy(case.observed)
    observed = await _collect(case)
    unused.assert_not_awaited()
    case.collect.assert_awaited_once_with(case.connection, case.storage, case.policy, [77])
    case.connection.fetchval.assert_awaited_once()
    expected_by_kind = dict(
        canonical={
            **before["canonical"],
            "dependency_relations": case.parents,
            "dependency_histories": case.histories,
            "dependency_migration_plan": {dependencies.PROVENANCE_ROOT: case.plan},
            "physical": [dict(oid=oid, native=case.physical[oid]) for oid in [1, *range(100, 122)]],
        },
        private=before["private"],
        routines=case.routines,
        roles={"v3": True},
    )
    assert observed == expected_by_kind and case.observed == before
    assert dependencies.custody._evidence_digests(observed) == dependencies.custody._evidence_digests(expected_by_kind)


@pytest.mark.asyncio
async def test_native_namespace_identifier_is_pinned_without_changing_v2(dependency_case):
    case = dependency_case
    original = deepcopy(case.observed)
    observed = await _collect(case)
    assert case.observed == original and observed["routines"] == case.routines
    assert " 'namespace_identifier',pg_catalog.quote_ident(n.nspname)" in dependencies._ROUTINES_SQL
    assert "namespace_identifier" not in dependencies.custody._ROUTINES_SQL
    before = dependencies.custody._evidence_digests(observed)
    observed["routines"][0]["native"]["namespace_identifier"] = '"catalog"'
    after = dependencies.custody._evidence_digests(observed)
    assert {key for key in before if before[key] != after[key]} == {"routines_sha256"}


@pytest.mark.asyncio
async def test_parent_scope_is_exact_and_locking_never_recurses(dependency_case):
    case = dependency_case
    await _collect(case)
    expected = ",".join("ONLY " + storage_relation("catalog", parent["relname"]) for parent in case.parents)
    assert case.connection.execute.await_args_list[0].args == (
        "LOCK TABLE " + expected + " IN ACCESS SHARE MODE NOWAIT",
    )
    assert case.connection.execute.await_count == 2
    assert dependencies._PARENTS_SQL == dependencies.custody._CATALOG_SQL.replace(dependencies._CHILD_UNION, "")
    assert dependencies.custody._CATALOG_SQL.count(dependencies._CHILD_UNION) == 1
    assert "JOIN family parent" not in dependencies._PARENTS_SQL
    assert "pg_catalog.pg_constraint k WHERE k.conrelid=c.oid" in dependencies._PARENTS_SQL
    assert case.connection.fetch.await_count == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda case: case.parents.pop(), id="missing"),
        pytest.param(lambda case: case.parents.append(_parent(200, "mutable_child")), id="extra"),
        pytest.param(lambda case: case.parents[0].update(oid=case.parents[1]["oid"]), id="reused_oid"),
        pytest.param(lambda case: case.parents[0].update(relname="unissued"), id="name"),
        pytest.param(lambda case: case.parents[0].update(nspname="other"), id="namespace"),
        pytest.param(lambda case: case.parents[0].update(relkind="v"), id="view"),
        pytest.param(lambda case: case.parents[0].update(relpersistence="t"), id="temporary"),
        pytest.param(lambda case: case.parents[0]["native"].update(parents=[1]), id="child"),
        pytest.param(lambda case: case.parents[0]["native"]["relation"].__setitem__(5, True), id="partition_child"),
        pytest.param(lambda case: case.parents[0].update(oid=True), id="oid_type"),
    ],
)
async def test_unknown_or_replaced_parent_refuses_before_lock(dependency_case, mutate):
    case = dependency_case
    mutate(case)
    with pytest.raises(ValueError, match="cms_dependency_policy_"):
        await _collect(case)
    case.connection.execute.assert_not_awaited()
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("oid", 999), ("rls", True), ("options", ["fillfactor=50"]), ("kind", "p"), ("strategy", "h")]
)
async def test_source_or_endpoint_native_shape_refuses(dependency_case, field, value):
    case = dependency_case
    case.layouts[storage_relation("catalog", dependencies.ROOTS[0])]["parent"][0][field] = value
    with pytest.raises(ValueError, match="cms_dependency_policy_"):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,mutate",
    [
        pytest.param(dependencies.ROOTS[3], lambda layout: layout["columns"].pop(), id="missing_column"),
        pytest.param(
            dependencies.ROOTS[3], lambda layout: layout["columns"][0].update(type="integer"), id="column_type"
        ),
        pytest.param(
            dependencies.ROOTS[3], lambda layout: layout["columns"][0].update(required=False), id="column_null"
        ),
        pytest.param(dependencies.ROOTS[3], lambda layout: layout["columns"][0].update(generated="s"), id="generated"),
        pytest.param(dependencies.ROOTS[3], lambda layout: layout.update(indexes=[]), id="missing_key"),
        pytest.param(
            dependencies.ROOTS[3], lambda layout: layout["partial"][0].update(predicate="true"), id="partial_predicate"
        ),
        pytest.param(
            "provider_directory_rooted_graph_work",
            lambda layout: layout["parent"][0].update(partition_columns=[0]),
            id="partition_key",
        ),
        pytest.param(
            "provider_directory_rooted_graph_work",
            lambda layout: layout["parent"][0].update(strategy="h"),
            id="partition_strategy",
        ),
    ],
)
async def test_authentic_source_layout_is_required(dependency_case, name, mutate):
    case = dependency_case
    mutate(case.layouts[storage_relation("catalog", name)])
    with pytest.raises(ValueError):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expression", "missing", "unexpected", "duplicate", "not_ready"])
async def test_header_partial_index_cannot_hide_unsupported_shape(dependency_case, change):
    case = dependency_case
    layout = case.layouts[storage_relation("catalog", dependencies.ROOTS[3])]
    if change == "expression":
        for index in layout["indexes"]:
            if index["name"] == layout["partial"][0]["name"]:
                index["expression_free"] = False
    elif change == "missing":
        layout["partial"] = []
    elif change == "unexpected":
        layout["partial"][0]["name"] = "unissued"
    elif change == "duplicate":
        layout["partial"].append(deepcopy(layout["partial"][0]))
    else:
        layout["partial"][0]["ready"] = False
    with pytest.raises(ValueError):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "reordered"])
async def test_batched_layout_must_bind_every_exact_parent(dependency_case, change):
    case = dependency_case
    original_fetch = case.connection.fetch.side_effect

    async def mutate(query, *arguments):
        entries = await original_fetch(query, *arguments)
        if query == dependencies._LAYOUT_SQL:
            return entries[:-1] if change == "missing" else list(reversed(entries))
        return entries

    case.connection.fetch.side_effect = mutate
    with pytest.raises(ValueError, match="layouts_changed"):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_refusal_propagates_without_mutation_or_followup_sql(dependency_case):
    case = dependency_case
    failure = PermissionError("parent lock denied")
    case.connection.execute.side_effect = failure
    with pytest.raises(PermissionError) as caught:
        await _collect(case)
    assert caught.value is failure
    assert case.connection.fetch.await_count == 1
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_parent_reader_sorts_raw_rows_without_changing_constraint_content(dependency_case):
    case = dependency_case
    expected = deepcopy(case.parents)
    case.parents.reverse()
    assert await dependencies._parents(case.connection, "catalog") == expected
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_change_during_lock_or_layout_refuses(dependency_case, monkeypatch):
    case = dependency_case
    layouts = dependencies._layouts

    async def mutate(*arguments):
        await layouts(*arguments)
        case.parents[0]["native"]["constraints"][0][0]["confrelid"] = 999

    monkeypatch.setattr(dependencies, "_layouts", mutate)
    with pytest.raises(ValueError, match="parents_changed"):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "body",
        "missing_fixed",
        "frontier",
        "duplicate",
        "missing_retirement",
        "retirement_changed",
        "missing_callback",
        "callback_changed",
    ],
)
async def test_extended_routines_cannot_replace_fixed_v2_evidence(dependency_case, change):
    case = dependency_case
    original_fetch = case.connection.fetch.side_effect

    async def mutate(query, *arguments):
        entries = await original_fetch(query, *arguments)
        if query != dependencies._ROUTINES_SQL:
            return entries
        if change == "body":
            entries[0]["native"]["body"] = "replaced"
        elif change == "missing_fixed":
            entries.pop(0)
        elif change == "duplicate":
            entries.append(deepcopy(entries[0]))
        elif change in {"missing_retirement", "missing_callback"}:
            excluded = 6000 if change == "missing_retirement" else 7000
            entries = [entry for entry in entries if entry["oid"] != excluded]
        return entries

    case.connection.fetch.side_effect = mutate
    if change == "frontier":
        case.frontier.side_effect = [[{"oid": 5000}, {"oid": 5001}], [{"oid": 5999}]]
    elif change == "retirement_changed":
        case.retirement.side_effect = [[{"oid": 6000}], [{"oid": 6999}]]
    elif change == "callback_changed":
        case.callbacks.side_effect = [[{"oid": 7000}], [{"oid": 7999}]]
    with pytest.raises(ValueError, match="routines_changed"):
        await _collect(case)
    case.connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "missing", "oid", "duplicate", "signature", "overload", "kind"])
async def test_retirement_identity_roots_are_source_fixed_and_unambiguous(fault):
    signatures = [
        storage_relation("catalog", name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        for name, arguments in dependencies._RETIREMENT_CALLEES.items()
    ]
    rows = [
        dict(signature=signature, oid=number, function=True, overloads=1)
        for number, signature in enumerate(signatures, 7000)
    ]
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows[1]["oid"] = rows[0]["oid"]
    elif fault:
        key, value = {
            "oid": ("oid", None),
            "signature": ("signature", "unissued()"),
            "overload": ("overloads", 2),
            "kind": ("function", False),
        }[fault]
        rows[0][key] = value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    if fault:
        with pytest.raises(ValueError, match="retirement_callees_missing_or_ambiguous"):
            await dependencies._retirement_frontier(connection, "catalog")
    else:
        assert await dependencies._retirement_frontier(connection, "catalog") == rows
    connection.fetch.assert_awaited_once_with(dependencies.custody._CALLEES_SQL, signatures)


def _callback_rows():
    rows = []
    for oid, (name, arguments) in enumerate(dependencies._CALLBACK_CALLEES.items(), 8000):
        has_text_result = name.endswith("sha256") or name.startswith("ptg_wave_")
        result_type = "3802" if name.endswith("request_failure_coverage") else "25" if has_text_result else "16"
        attributes = ["f", "3802" if arguments == ("jsonb",) else "25", result_type, 1, 0, True, "0", True, True, False]
        shapes = [attributes]
        if name == "ptg_wave_canonical_json_ascii_v1":
            shapes.insert(0, ["f", "114", "25", 1, 0, True, "0", True, True, False])
        signature = storage_relation("catalog", name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        rows.append(dict(signature=signature, oid=oid, function=True, overloads=shapes))
    return rows


@pytest.mark.asyncio
async def test_callback_frontier_selects_jsonb_and_preserves_exact_json_sibling():
    rows = _callback_rows()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert len(rows) == 14 and sum(len(row["overloads"]) for row in rows) == 15
    assert await dependencies._callback_frontier(connection, "catalog") == rows
    connection.fetch.assert_awaited_once_with(dependencies._CALLBACK_CALLEES_SQL, [row["signature"] for row in rows])
    assert '"ptg_wave_canonical_json_ascii_v1"(pg_catalog.jsonb)' in rows[11]["signature"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "oid",
        "duplicate",
        "signature",
        "kind",
        "varchar",
        "default",
        "variadic",
        "procedure",
        "json_missing",
        "json_default",
        "json_variadic",
        "same_count",
    ],
)
async def test_callback_frontier_refuses_unsupported_identity_and_overloads(fault):
    frontier_rows = _callback_rows()
    json_row = next(entry for entry in frontier_rows if "ptg_wave_canonical_json_ascii_v1" in entry["signature"])
    match fault:
        case "missing":
            frontier_rows.pop()
        case "oid":
            frontier_rows[0]["oid"] = None
        case "duplicate":
            frontier_rows[1]["oid"] = frontier_rows[0]["oid"]
        case "signature":
            frontier_rows[0]["signature"] = "unbound(text)"
        case "kind":
            frontier_rows[0]["function"] = False
        case "varchar" | "default" | "variadic" | "procedure":
            shape = deepcopy(frontier_rows[0]["overloads"][0])
            if fault == "varchar":
                shape[1] = "1043"
            elif fault == "default":
                shape[1], shape[3] = "25 23", 2
                shape[4:6] = [1, False]
            elif fault == "variadic":
                shape[1] = "1009"
                shape[6] = "25"
            else:
                shape[:3] = ["p", "23", "2278"]
            frontier_rows[0]["overloads"].append(shape)
        case "json_missing":
            json_row["overloads"].pop(0)
        case "json_default":
            json_row["overloads"][0][4:6] = [1, False]
        case "json_variadic":
            json_row["overloads"][0][6] = "114"
        case "same_count":
            json_row["overloads"][0][1] = "1043"
    connection = SimpleNamespace(fetch=AsyncMock(return_value=frontier_rows))
    with pytest.raises(ValueError, match="callback_callees_missing_or_ambiguous|callback_overloads_changed"):
        await dependencies._callback_frontier(connection, "catalog")


@pytest.mark.asyncio
async def test_callback_census_uses_native_json_decoding():
    rows = _callback_rows()
    for row in rows:
        row["overloads"] = json.dumps(row["overloads"])
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert await dependencies._callback_frontier(connection, "catalog") == rows


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [None, (1,), [True], [0], [2**32], [2, 1], [1, 1]])
async def test_physical_identity_input_refuses_before_io(oids):
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="physical_identity_required"):
        await dependencies._physical(connection, oids)
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[{"oid": 9, "native": "{}"}], [], [{"oid": 1, "native": "{}"}] * 2])
async def test_physical_identity_output_must_match_the_exact_roots(rows):
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    with pytest.raises(ValueError, match="physical_relations_changed"):
        await dependencies._physical(connection, [1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,row,position,value",
    [("table_acl", 1, 2, "UPDATE"), ("table_acl", 1, 3, False), ("column_acl", 0, 0, 3)],
)
async def test_expanded_acl_drift_changes_only_the_canonical_digest(dependency_case, field, row, position, value):
    case = dependency_case
    before = await _collect(case)
    case.physical[1][field][row][position] = value
    after = await _collect(case)
    old_digests, new_digests = map(dependencies.custody._evidence_digests, (before, after))
    assert old_digests.pop("canonical_sha256") != new_digests.pop("canonical_sha256")
    assert old_digests == new_digests
    assert after["canonical"]["physical"][0]["native"][field][row][position] == value


def test_acl_expansion_uses_root_native_defaults_and_stable_edge_order():
    acl_sql = dependencies._PHYSICAL_SQL.split("'table_acl',", 1)[1].split("'rowtypes',", 1)[0]
    assert "pg_catalog.aclexplode(COALESCE(r.relacl,pg_catalog.acldefault('r',r.relowner)))" in acl_sql
    assert "pg_catalog.aclexplode(a.attacl)" in acl_sql
    assert "WHERE a.attrelid=r.oid AND a.attnum>0" in acl_sql
    assert "ORDER BY acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable" in acl_sql
    assert "ORDER BY a.attnum,acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable" in acl_sql
    assert "reltoastrelid" not in acl_sql and acl_sql.count("'[]'::jsonb") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("position,value", [(0, 61), (1, 51), (2, "USAGE"), (3, True)])
async def test_routine_acl_drift_changes_only_the_routine_digest(dependency_case, position, value):
    case = dependency_case
    before = await _collect(case)
    case.routines[0]["native"]["acl"][1][position] = value
    after = await _collect(case)
    old_digests, new_digests = map(dependencies.custody._evidence_digests, (before, after))
    assert old_digests.pop("routines_sha256") != new_digests.pop("routines_sha256")
    assert old_digests == new_digests
    assert after["routines"][0]["native"]["acl"][1][position] == value


def test_routine_acl_query_preserves_legacy_sql_and_uses_native_defaults():
    additions = dependencies._ROUTINE_ACL_SQL + " 'namespace_identifier',pg_catalog.quote_ident(n.nspname),\n"
    assert dependencies._ROUTINES_SQL.replace(additions, "", 1) == dependencies.custody._ROUTINES_SQL
    assert dependencies.custody._ROUTINES_SQL.count(" 'body',") == 1
    assert (
        "pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner)))"
        in dependencies._ROUTINE_ACL_SQL
    )
    assert "ORDER BY acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable" in dependencies._ROUTINE_ACL_SQL


def _migration(filename):
    path = Path(__file__).parents[1] / "alembic" / "versions" / filename
    specification = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_both_fixed_validators_have_all_literal_relation_reads_pinned():
    rooted = _migration("20260811020000_provider_directory_rooted_graph_acquisition.py")
    practitioner = _migration("20260810080000_provider_directory_uhc_flex_practitioner_publication.py")
    bodies = rooted._rooted_intrinsic_valid_function_sql("sample") + practitioner._valid_function_sql("sample")
    reads = set(re.findall(r'\b(?:FROM|JOIN)\s+"sample"\."([a-z_]+)"', bodies, re.IGNORECASE))
    assert len(reads) == 21
    assert reads <= set(dependencies.policy_checks.ROOTS) | set(dependencies.ROOTS)
    read_roots = set(dependencies.ROOTS) - {*dependencies.CONTROL_ROOTS, dependencies.PROVENANCE_ROOT}
    assert reads - set(dependencies.policy_checks.ROOTS) == read_roots
    assert len(read_roots) == 14 and len(dependencies.ROOTS) == 20


def _history_case(*, attached=False):
    plans, roots = _plans()
    parents = [
        dict(oid=entry["oid"], relname=name) for name, entry in roots.items() if name not in dependencies.HISTORY_ROOTS
    ]
    scope = "pdufpa_" + "1" * 48
    histories = []
    for name, root in zip(dependencies.HISTORY_ROOTS, dependencies.acquisition.BULK_ROOTS[:2], strict=True):
        oid = roots[name]["oid"]
        histories.append(
            dict(
                oid=oid,
                relname=name,
                nspname="catalog",
                relkind="r",
                relpersistence="p",
                relowner=50,
                native=dict(
                    parents=[roots[root]["oid"]] if attached else [],
                    rules=[],
                    columns=[],
                    constraints=[],
                    triggers=[],
                    namespace=[42, "catalog", 50, None],
                    relation=[
                        oid,
                        name,
                        "r",
                        "p",
                        50,
                        attached,
                        False,
                        False,
                        0,
                        None,
                        None,
                        "FOR VALUES IN ('" + scope + "')" if attached else None,
                    ],
                ),
            )
        )
    case = SimpleNamespace(
        parents=parents, histories=histories, plan=plans[dependencies.PROVENANCE_ROOT], scope=scope, present=True
    )

    async def fetch(query, *arguments):
        if query == dependencies._PARENTS_SQL:
            assert arguments == ("catalog", list(dependencies.HISTORY_ROOTS))
            return deepcopy(case.histories)
        assert query.startswith("SELECT acquisition_id FROM")
        return [dict(acquisition_id=case.scope)] if case.present else []

    case.connection = SimpleNamespace(fetch=AsyncMock(side_effect=fetch), fetchval=AsyncMock(return_value=True))
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("attached", [False, True])
async def test_exact_uhc_history_oids_come_only_from_original_migration_plan(attached):
    case = _history_case(attached=attached)
    actual = await dependencies._history_relations(case.connection, "catalog", case.parents, case.plan)
    await dependencies._history_scopes(case.connection, "catalog", actual)
    assert actual == sorted(case.histories, key=lambda row: row["oid"])
    assert not any("ACCESS EXCLUSIVE" in call.args[0] for call in case.connection.fetch.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "work_oid",
        "resource_member",
        "namespace",
        "parent",
        "rls",
        "kind",
        "duplicate",
        "missing",
        "children",
        "nonempty",
        "scope",
    ],
)
async def test_history_identity_scope_and_detached_emptiness_fail_closed(fault):
    case = _history_case(attached=fault == "scope")
    match fault:
        case "work_oid":
            case.plan[0]["work_oid"] += 1
        case "resource_member":
            case.plan[0]["resource_oid"] = 999999
        case "namespace":
            case.histories[0]["nspname"] = "unbound"
        case "parent":
            case.histories[0]["native"]["parents"] = [999999]
        case "rls":
            case.histories[0]["native"]["relation"][6] = True
        case "kind":
            case.histories[0]["relkind"] = "p"
        case "duplicate":
            case.histories[1]["oid"] = case.histories[0]["oid"]
        case "missing":
            case.histories.pop()
        case "children":
            case.connection.fetchval.return_value = False
        case "nonempty":
            case.connection.fetchval.side_effect = [True, False]
        case "scope":
            case.present = False
    with pytest.raises(
        ValueError, match="practitioner_migration_changed|histories_changed|history_|detached_history_nonempty"
    ):
        actual = await dependencies._history_relations(case.connection, "catalog", case.parents, case.plan)
        await dependencies._history_scopes(case.connection, "catalog", actual)


def _history_check_rows(case):
    rows = []
    for offset, history in enumerate(case.histories):
        expected = dependencies._HISTORY_CHECKS[history["relname"].removesuffix("_legacy")]
        checks = [(name, expression) for name, expression in expected.items()]
        checks.append(("pd_uhc_flex_legacy_scope", "CHECK (false)"))
        history["native"]["constraints"] = [
            [
                dict(
                    oid=str(4000 + offset * 10 + number), conname=name, contype="c", convalidated=True, conenforced=True
                ),
                expression,
            ]
            for number, (name, expression) in enumerate(checks)
        ]
        rows.append(
            dict(
                oid=history["oid"],
                checks=[
                    [4000 + offset * 10 + number, name, expression] for number, (name, expression) in enumerate(checks)
                ],
            )
        )
    return rows


@pytest.mark.asyncio
async def test_three_source_native_checks_and_scope_are_required_exactly():
    case = _history_case()
    rows = _history_check_rows(case)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert await dependencies._history_checks(connection, case.histories) is None
    connection.fetch.assert_awaited_once_with(
        dependencies._HISTORY_CHECKS_SQL, sorted(row["oid"] for row in case.histories)
    )
    assert sum(len(checks) for checks in dependencies._HISTORY_CHECKS.values()) == 3


def test_history_model_changes_require_native_check_regeneration():
    expected_by_root = {
        "provider_directory_uhc_flex_practitioner_work": "5388760268f26ff08d2db5cde93f7128a7641a12416155f46438639b08cd6bfb",
        "provider_directory_uhc_flex_practitioner_resource": "05d8c8507228613d8d1ebb7b806890510688277b7656ee1671114f10bf7a4155",
    }
    for root, fingerprint in expected_by_root.items():
        model = dependencies.acquisition._models()[root]
        schema = str(model.__table__.schema)
        statement = dependencies.acquisition._expression_table_sql(model, "expected_check", schema)
        statement = statement.replace(storage_identifier(schema) + ".", '"canonical_schema".')
        # SQLAlchemy stores CHECK constraints in an unordered set.
        columns, *checks = statement.removesuffix(") ON COMMIT DROP").split(",CONSTRAINT ")
        statement = ",CONSTRAINT ".join([columns, *sorted(checks)]) + ") ON COMMIT DROP"
        assert sha256(statement.encode()).hexdigest() == fingerprint


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["grouping", "cast", "predicate", "missing", "extra", "oid", "unvalidated", "scope"])
async def test_history_checks_reject_semantic_and_identity_tampering(fault):
    case = _history_case()
    rows = _history_check_rows(case)
    match fault:
        case "grouping":
            expression = rows[0]["checks"][0][2].replace(" AND ", " AND (", 1)
            rows[0]["checks"][0][2] = expression[:-1] + "))"
        case "cast":
            rows[0]["checks"][0][2] = rows[0]["checks"][0][2].replace("::text", "::varchar", 1)
        case "predicate":
            rows[0]["checks"][0][2] = "CHECK (true)"
        case "missing":
            rows[0]["checks"].pop(0)
        case "extra":
            rows[0]["checks"].append([9999, "unbound", "CHECK (true)"])
        case "oid":
            rows[0]["checks"][0][0] = 9999
        case "unvalidated":
            case.histories[0]["native"]["constraints"][0][0]["convalidated"] = False
        case "scope":
            rows[0]["checks"][-1][2] = "CHECK (true)"
    with pytest.raises(ValueError, match="history_checks_changed|scope_expression_changed"):
        await dependencies._history_checks(SimpleNamespace(fetch=AsyncMock(return_value=rows)), case.histories)


def _control_hook_rows(root):
    from process import provider_directory_source_checkpoint_catalog as checkpoints
    from process import provider_directory_source_proof_control as proofs

    expected = proofs.PROOF_HOOKS if root == proofs.PROOF_ROOT else checkpoints._GUARDS[root]
    contracts = dependencies._control_hook_contracts(root, set(expected))
    hooks = []
    routines = []
    for oid, (name, contract) in enumerate(contracts.items(), 8000):
        event, deferred, initial, old_table, new_table, function = contract
        hooks.append(
            dict(
                tgname=name,
                tgisinternal=False,
                tgfoid=str(oid),
                tgtype=event,
                tgdeferrable=deferred,
                tginitdeferred=initial,
                tgoldtable=old_table,
                tgnewtable=new_table,
                tgenabled="A",
                tgnargs=0,
                tgargs="\\x",
                tgattr=[],
                tgqual=None,
                tgparentid="0",
            )
        )
        routines.append(
            dict(
                oid=oid,
                native=dict(namespace=[42, "catalog", 50, None], definition=dict(proname=function, oid=str(oid))),
            )
        )
    parent_by_field = dict(relname=root, native=dict(namespace=[42, "catalog", 50, None], triggers=hooks))
    return parent_by_field, routines


@pytest.mark.parametrize("root", dependencies.CONTROL_ROOTS)
@pytest.mark.parametrize("attribute_encoding", [[], ""])
def test_canonical_control_hooks_use_exact_source_transition_and_deferred_shapes(root, attribute_encoding):
    parent, routines = _control_hook_rows(root)
    for hook in parent["native"]["triggers"]:
        hook["tgattr"] = attribute_encoding
    assert dependencies._control_hooks([parent], routines) is None


@pytest.mark.parametrize("root", dependencies.CONTROL_ROOTS)
@pytest.mark.parametrize(
    "field",
    [
        "tgtype",
        "tgdeferrable",
        "tgoldtable",
        "tgnewtable",
        "tgargs",
        "tgattr",
        "tgqual",
        "tgfoid",
        "tgenabled",
        "tgparentid",
    ],
)
def test_canonical_control_hook_changes_cannot_be_new_expected_bindings(root, field):
    parent, routines = _control_hook_rows(root)
    hook = parent["native"]["triggers"][0]
    hook[field] = not hook[field] if field == "tgdeferrable" else 0 if field == "tgtype" else "changed"
    with pytest.raises((ValueError, TypeError), match="control_|invalid literal"):
        dependencies._control_hooks([parent], routines)


def _control_catalog(root, oid):
    from process import provider_directory_source_checkpoint_catalog as checkpoints
    from process import provider_directory_source_proof_control as proofs

    if root == proofs.PROOF_ROOT:
        constraints = [
            dict(
                name=root + "_pkey",
                type="p",
                definition="PRIMARY KEY (dataset_id, shard_id)",
                columns=[1, 2],
                valid=True,
            )
        ]
        constraints.extend(
            dict(name=name, type="c", definition=expression, valid=True) for name, expression in proofs._CHECKS.items()
        )
        constraints.append(dict(name=root + "_dataset_fkey", type="f", definition="fixed header FK", valid=True))
        indexes = [
            dict(valid=True, native=True, method="btree", predicate=None, expression=None, keys=keys)
            for keys in (["dataset_id", "shard_id"], ["dataset_id", "acquisition_root_run_id", "shard_id"])
        ]
    else:
        expected = checkpoints.CHECKPOINT_LAYOUTS[root]
        constraints = [
            dict(type=kind, definition=definition.replace("mrf.", "catalog."), valid=True)
            for kind, definition in expected["constraints"]
        ]
        indexes = [
            dict(
                primary=primary,
                unique=unique,
                method=method,
                predicate=predicate,
                expression=expression,
                keys=keys,
                valid=True,
            )
            for primary, unique, method, predicate, expression, keys in expected["indexes"]
        ]
    return dict(
        oid=oid,
        relname=root,
        nspname="catalog",
        relkind="r",
        relpersistence="p",
        relispartition=False,
        parents=[],
        relrowsecurity=False,
        relforcerowsecurity=False,
        native_checks=True,
        columns=deepcopy(dependencies._control_columns(root)),
        constraints=constraints,
        indexes=indexes,
    )


def _control_case():
    from process.plan_release_storage_catalog import CATALOG_SQL

    parents = [dict(oid=oid, relname=root, relowner=50) for oid, root in enumerate(dependencies.CONTROL_ROOTS, 9000)]
    catalogs = [_control_catalog(row["relname"], row["oid"]) for row in parents]
    native_rows = [dict(oid=row["oid"], valid=True) for row in parents]

    async def fetch(query, *arguments):
        if query == CATALOG_SQL:
            assert arguments == ([storage_relation("catalog", name) for name in dependencies.CONTROL_ROOTS], 50, [])
            return catalogs
        assert query.startswith("SELECT target.oid,(") and "FROM pg_trigger" in query
        assert arguments == ([row["oid"] for row in parents],)
        return native_rows

    return SimpleNamespace(
        parents=parents,
        catalogs=catalogs,
        native=native_rows,
        connection=SimpleNamespace(fetch=AsyncMock(side_effect=fetch)),
    )


@pytest.mark.asyncio
async def test_canonical_control_layouts_reuse_exact_source_contracts_without_private_roles():
    case = _control_case()
    assert await dependencies._control_layouts(case.connection, "catalog", case.parents) is None
    assert case.connection.fetch.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["column", "check", "index", "rls", "namespace", "identity", "duplicate", "child", "native"]
)
async def test_canonical_control_layout_drift_refuses(fault):
    case = _control_case()
    match fault:
        case "column":
            case.catalogs[0]["columns"][0][1] = "text"
        case "check":
            case.catalogs[0]["constraints"][1]["definition"] = "CHECK (true)"
        case "index":
            case.catalogs[0]["indexes"].clear()
        case "rls":
            case.catalogs[0]["relrowsecurity"] = True
        case "namespace":
            case.catalogs[0]["nspname"] = "private"
        case "identity":
            case.catalogs[0]["oid"] = 999999
        case "duplicate":
            case.catalogs[-1] = deepcopy(case.catalogs[0])
        case "child":
            case.catalogs.append(deepcopy(case.catalogs[0]))
        case "native":
            case.native[0]["valid"] = False
    with pytest.raises(ValueError, match="control_|proof_|storage_native_changed"):
        await dependencies._control_layouts(case.connection, "catalog", case.parents)


def _plan_case():
    oid = 7000
    columns = dependencies._control_columns(dependencies.PROVENANCE_ROOT)
    native_columns = [
        dict(attribute=dict(attname=name, attnum=number, attrelid=str(oid)), default=None)
        for number, (name, _kind, _required, _default) in enumerate(columns, 1)
    ]
    constraints = [
        [
            dict(contype="p", conkey=[1], condeferrable=False, condeferred=False, convalidated=True),
            "PRIMARY KEY (singleton)",
        ],
        [dict(contype="c", convalidated=True), "CHECK (singleton)"],
    ]
    parent_by_field = dict(
        oid=oid,
        relname=dependencies.PROVENANCE_ROOT,
        native=dict(rules=[], columns=native_columns, constraints=constraints),
    )
    layout_by_field = dict(
        parent=[dict(oid=oid, kind="r", persistence="p", rls=False, options=None)],
        columns=[
            dict(name=name, type=kind, required=required, identity="", generated="")
            for name, kind, required, _default in columns
        ],
    )
    plans, _roots = _plans()
    case = SimpleNamespace(parent=parent_by_field, layout=layout_by_field, plan=plans[dependencies.PROVENANCE_ROOT])

    async def fetch(query, *arguments):
        if query == dependencies._LAYOUT_SQL:
            assert arguments == ([storage_relation("catalog", dependencies.PROVENANCE_ROOT)],)
            return [dict(name=arguments[0][0], layout=json.dumps(case.layout))]
        if query.startswith("SELECT target.oid,("):
            return [dict(oid=oid, valid=True)]
        assert query.startswith("SELECT to_jsonb(r)||jsonb_build_object(")
        return [dict(body=json.dumps(row)) for row in case.plan]

    case.connection = SimpleNamespace(fetch=AsyncMock(side_effect=fetch))
    return case


@pytest.mark.asyncio
async def test_practitioner_plan_is_read_only_after_native_source_shape_check():
    case = _plan_case()
    assert await dependencies._migration_plan(case.connection, "catalog", [case.parent]) == case.plan
    assert case.connection.fetch.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["type", "rls", "rule", "check", "default", "unvalidated", "identity"])
async def test_practitioner_plan_shape_is_checked_before_data_read(fault):
    case = _plan_case()
    match fault:
        case "type":
            case.layout["columns"][1]["type"] = "text"
        case "rls":
            case.layout["parent"][0]["rls"] = True
        case "rule":
            case.parent["native"]["rules"].append("unbound")
        case "check":
            case.parent["native"]["constraints"][1][1] = "CHECK (true)"
        case "default":
            case.parent["native"]["columns"][0]["default"] = {"unbound": True}
        case "unvalidated":
            case.parent["native"]["constraints"][1][0]["convalidated"] = False
        case "identity":
            case.layout["parent"][0]["oid"] += 1
    with pytest.raises(ValueError, match="plan_|metadata_"):
        await dependencies._migration_plan(case.connection, "catalog", [case.parent])
    assert case.connection.fetch.await_count == 1


def _native_hooks(contracts):
    hooks = []
    routines = []
    for name, contract in contracts.items():
        function, mask, arguments, transition, selected, *extra = contract
        oid = 10000 + int.from_bytes(sha256(function.encode()).digest()[:3], "big")
        enabled, old_table, predicate = extra or ("A", None, None)
        assert not selected and predicate is None
        hooks.append(
            dict(
                tgname=name,
                tgisinternal=False,
                tgfoid=str(oid),
                tgtype=mask,
                tgenabled=enabled,
                tgdeferrable=False,
                tginitdeferred=False,
                tgqual=None,
                tgnargs=len(arguments),
                tgargs="\\x" + "".join(value.encode().hex() + "00" for value in arguments),
                tgattr=[],
                tgparentid="0",
                tgoldtable=old_table,
                tgnewtable=transition,
            )
        )
        routines.append(dict(oid=oid, native=dict(definition=dict(proname=function, oid=str(oid)))))
    return hooks, routines


@pytest.mark.parametrize("attached", [False, True])
@pytest.mark.parametrize("attribute_encoding", [[], ""])
def test_history_and_plan_hooks_reuse_existing_source_invocations(attached, attribute_encoding):
    case = _history_case(attached=attached)
    routines = []
    for history in case.histories:
        root = history["relname"].removesuffix("_legacy")
        hooks, functions = _native_hooks(
            dependencies.acquisition._member_hook_contracts(root, attached=attached, history=True)
        )
        history["native"]["triggers"] = hooks
        routines.extend(functions)
    hooks, functions = _native_hooks(dependencies.acquisition._root_hook_contracts()[dependencies.PROVENANCE_ROOT])
    plan_by_field = dict(
        relname=dependencies.PROVENANCE_ROOT,
        oid=7000,
        native=dict(parents=[], columns=[], constraints=[], triggers=hooks),
    )
    routines.extend(functions)
    for relation in [*case.histories, plan_by_field]:
        for hook in relation["native"]["triggers"]:
            hook["tgattr"] = attribute_encoding
    assert dependencies._history_hooks(case.histories, [plan_by_field], routines) is None
    case.histories[0]["native"]["triggers"][0]["tgargs"] = "\\x"
    with pytest.raises(ValueError, match="source_acquisition_hooks_changed"):
        dependencies._history_hooks(case.histories, [plan_by_field], routines)


def _history_layout_case():
    case = _history_case()
    layouts_by_name = {}
    defaults_by_oid = {}
    for history in case.histories:
        model = dependencies.acquisition._models()[history["relname"].removesuffix("_legacy")]
        layouts_by_name[storage_relation("catalog", history["relname"])] = _layout(history, model)
        defaults_by_oid[history["oid"]] = [
            dict(attname=column.name, expression=str(column.server_default.arg) if column.server_default else None)
            for column in model.__table__.columns
        ]
        history["native"]["columns"] = [
            dict(attribute=dict(attname=column.name, attnum=number, attisdropped=False), default=None)
            for number, column in enumerate(model.__table__.columns, 1)
        ]

    async def fetch(query, *arguments):
        if query == dependencies._LAYOUT_SQL:
            return [dict(name=name, layout=json.dumps(layouts_by_name[name])) for name in arguments[0]]
        if query.startswith("SELECT target.oid,("):
            return [dict(oid=oid, valid=True) for oid in arguments[0]]
        assert query.startswith("SELECT a.attname,pg_catalog.pg_get_expr(")
        return defaults_by_oid[arguments[0]]

    case.connection.fetch.side_effect = fetch
    case.layouts = layouts_by_name
    case.defaults = defaults_by_oid
    return case


@pytest.mark.asyncio
async def test_history_columns_defaults_and_indexes_reuse_source_models():
    case = _history_layout_case()
    assert await dependencies._history_layouts(case.connection, "catalog", case.histories) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["column", "index", "default", "identity"])
async def test_history_structural_drift_refuses(fault):
    case = _history_layout_case()
    layout = next(iter(case.layouts.values()))
    match fault:
        case "column":
            layout["columns"][0]["type"] = "text"
        case "index":
            layout["indexes"].clear()
        case "default":
            case.defaults[case.histories[0]["oid"]][0]["expression"] = "unbound()"
        case "identity":
            layout["parent"][0]["oid"] += 1
    with pytest.raises(ValueError, match="history_layout_changed|administrator_|defaults_changed"):
        await dependencies._history_layouts(case.connection, "catalog", case.histories)


def _proof_foreign_case():
    header_by_field = dict(relname="provider_directory_endpoint_dataset", oid=100)
    foreign_by_field = dict(
        contype="f",
        confrelid="100",
        conkey=[1],
        confkey=[1],
        convalidated=True,
        condeferrable=False,
        condeferred=False,
        confupdtype="a",
        confdeltype="c",
        confmatchtype="s",
    )
    proof_by_field = dict(
        relname=dependencies.CONTROL_ROOTS[0], native=dict(constraints=[[foreign_by_field, "fixed FK"]])
    )
    return header_by_field, proof_by_field, foreign_by_field


def test_proof_fk_targets_only_the_protected_canonical_header():
    header, proof, _foreign = _proof_foreign_case()
    assert dependencies._proof_foreign([proof], [header]) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("confrelid", "200"),
        ("conkey", [2]),
        ("confkey", [2]),
        ("convalidated", False),
        ("condeferrable", True),
        ("confdeltype", "a"),
    ],
)
def test_proof_fk_cannot_borrow_a_private_or_different_header(field, value):
    header, proof, foreign = _proof_foreign_case()
    foreign[field] = value
    with pytest.raises(ValueError, match="proof_foreign_changed"):
        dependencies._proof_foreign([proof], [header])


@pytest.mark.asyncio
async def test_new_plan_and_history_remain_in_canonical_digest(dependency_case):
    case = dependency_case
    original = await _collect(case)
    modified = deepcopy(original)
    modified["canonical"]["dependency_migration_plan"][dependencies.PROVENANCE_ROOT][0]["work_oid"] += 1
    before, after = map(dependencies.custody._evidence_digests, (original, modified))
    assert {field for field in before if before[field] != after[field]} == {"canonical_sha256"}
    modified = deepcopy(original)
    modified["canonical"]["dependency_histories"][0]["native"]["constraints"] = ["changed"]
    after = dependencies.custody._evidence_digests(modified)
    assert before["canonical_sha256"] != after["canonical_sha256"]
    assert len(dependencies.custody._CALLEE_FRONTIER) == 32


def test_batched_layout_sql_preserves_existing_queries_and_serialization():
    for statement in (
        dependencies.acquisition._ROOT_SQL,
        dependencies.controls._COLUMN_SQL,
        dependencies.acquisition._PARTIAL_INDEX_SQL,
    ):
        assert statement.replace(":relation", "target.name") in dependencies._LAYOUT_SQL
    extended_indexes = dependencies.controls._INDEX_SQL.replace(
        'i.indisunique AS "unique",', 'i.indexprs IS NULL AS expression_free,i.indisunique AS "unique",'
    )
    assert extended_indexes.replace(":relation", "target.name") in dependencies._LAYOUT_SQL
    assert "unnest($1::text[])" in dependencies._LAYOUT_SQL
    assert "pg_inherits" not in dependencies._LAYOUT_SQL
