# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current acquisition shape stays read-only and rejects ambiguous storage."""

import json
import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_control_custody as controls
from process import provider_directory_source_acquisition_custody as custody
from tests.test_provider_directory_cms_control_custody import _columns, _indexes


def _native(root):
    model = custody._models()[root]
    columns = list(model.__table__.columns)
    is_partitioned = root in custody.BULK_ROOTS or root == custody.ACQUISITION_ROOTS[1]
    return dict(
        oid=100,
        kind="p" if is_partitioned else "r",
        persistence="p",
        rls=False,
        options=None,
        strategy="l" if is_partitioned else None,
        partition_columns=[
            next(
                i
                for i, column in enumerate(columns, 1)
                if column.name == ("cohort_id" if root == custody.ACQUISITION_ROOTS[1] else "acquisition_id")
            )
        ]
        if is_partitioned
        else None,
        expressions=None,
    )


def _partial(root):
    return (
        [
            dict(
                name="pdrg_work_parent_plan_census_key",
                unique=True,
                ready=True,
                columns=["acquisition_id", "kind"],
                predicate="kind::text = 'full_insurance_plan_census'::text",
            )
        ]
        if root == custody.ACQUISITION_ROOTS[8]
        else []
    )


@pytest.mark.parametrize("root", custody.ACQUISITION_ROOTS)
async def test_current_managed_model_check_has_only_native_reads(root):
    model = custody._models()[root]
    database = SimpleNamespace(
        all=AsyncMock(side_effect=[_columns(model), _indexes(model), _partial(root)]),
        first=AsyncMock(return_value=_native(root)),
        status=AsyncMock(),
        create_table=AsyncMock(),
    )
    await controls.require_cms_control_model_layout(database, model, "snapshot_schema")
    assert database.all.await_count == 3 and database.first.await_count == 1
    assert all(call.args[0].lstrip().startswith("SELECT") for call in database.all.await_args_list)
    database.status.assert_not_awaited()
    database.create_table.assert_not_awaited()
    assert model.__runtime_schema_sync__ is False
    assert not model.__table__.info.get("managed_immutable_storage")


async def test_model_name_does_not_admit_an_untrusted_model():
    model = SimpleNamespace(__tablename__=custody.ACQUISITION_ROOTS[0])
    database = SimpleNamespace(all=AsyncMock())
    with pytest.raises(ValueError, match="model_required"):
        await custody.require_acquisition_model_layout(database, model, "snapshot_schema")
    database.all.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "r"),
        ("persistence", "u"),
        ("rls", True),
        ("options", ["fillfactor=70"]),
        ("strategy", "h"),
        ("partition_columns", [2]),
        ("expressions", "arbitrary"),
    ],
)
def test_bulk_parent_shape_drift_requires_an_administrator(field, value):
    root = custody.BULK_ROOTS[0]
    native = _native(root)
    native[field] = value
    with pytest.raises(ValueError, match="administrator_"):
        custody._require_parent(root, native, 1)


def test_rooted_parent_does_not_require_retired_live_claim_index():
    root = custody.ACQUISITION_ROOTS[8]
    model = custody._models()[root]
    indexes = [index for index in _indexes(model) if index["unique"]]
    custody._require_parent_indexes(root, model, indexes, _partial(root))
    for predicate in ("true", "kind::text <> 'full_insurance_plan_census'::text"):
        partial = _partial(root)
        partial[0]["predicate"] = predicate
        with pytest.raises(ValueError, match="administrator_index_required"):
            custody._require_parent_indexes(root, model, indexes, partial)


def test_practitioner_parent_still_requires_the_native_claim_index():
    root = custody.ACQUISITION_ROOTS[3]
    model = custody._models()[root]
    indexes = [index for index in _indexes(model) if index["unique"]]
    with pytest.raises(ValueError, match="administrator_index_required"):
        custody._require_parent_indexes(root, model, indexes, [])


def _plans():
    native_by_root = {
        root: {"oid": position}
        for position, root in enumerate(
            (*custody.ACQUISITION_ROOTS, *custody.HISTORY_ROOTS, *custody.METADATA_ROOTS), 100
        )
    }
    plans_by_root = {
        custody.METADATA_ROOTS[0]: [
            dict(
                singleton=True,
                acquisition_oid=native_by_root[custody.ACQUISITION_ROOTS[2]]["oid"],
                work_oid=native_by_root[custody.HISTORY_ROOTS[0]]["oid"],
                resource_oid=native_by_root[custody.HISTORY_ROOTS[1]]["oid"],
                parent_oid=native_by_root[custody.BULK_ROOTS[1]]["oid"],
                work_parent_oid=native_by_root[custody.BULK_ROOTS[0]]["oid"],
            )
        ],
        custody.METADATA_ROOTS[1]: [dict(singleton=True, complete=True)],
    }
    return plans_by_root, native_by_root


@pytest.mark.parametrize("root", custody.ACQUISITION_ROOTS)
def test_expected_expression_table_has_no_native_defaults_hooks_or_data(root):
    model = custody._models()[root]
    sql = custody._expression_table_sql(model, "expected_checks", str(model.__table__.schema))
    assert sql.startswith('CREATE TEMPORARY TABLE "expected_checks"(')
    assert sql.endswith("ON COMMIT DROP")
    assert not any(keyword in sql for keyword in (" DEFAULT ", " LIKE ", " TRIGGER ", "INSERT INTO", " AS SELECT "))


def test_expected_expression_table_rejects_a_caller_supplied_model():
    model = SimpleNamespace(__tablename__=custody.ACQUISITION_ROOTS[0])
    with pytest.raises(ValueError, match="model_required"):
        custody._expression_table_sql(model, "expected_checks", "snapshot_schema")


@pytest.mark.parametrize("values", [[], ["pdrga_" + "1" * 48], ["pdrga_" + "1" * 48, "pdrga_" + "2" * 48]])
def test_native_history_scope_requires_complete_exact_safe_bound(values):
    expression = (
        "false"
        if not values
        else (
            f"acquisition_id::text = '{values[0]}'::text"
            if len(values) == 1
            else "acquisition_id::text = ANY (ARRAY[" + ",".join("'" + value + "'::text" for value in values) + "])"
        )
    )
    from db.migration_expression_adoption import _normalized_expression

    custody._require_acquisition_scope(_normalized_expression("CHECK (" + expression + ")"), values)
    with pytest.raises(ValueError, match="scope_expression_changed"):
        custody._require_acquisition_scope("checktrue", values)


_PRAC_WORK = "pd_uhc_flex_practitioner_work_guard"
_PRAC_TRUNCATE = "pd_uhc_flex_practitioner_work_truncate"
_FROZEN_INSERT = "pd_uhc_flex_candidate_insert"
_PRAC_IMMUTABLE = "pd_uhc_flex_resource_immutable"
_ROOT_WORK = "provider_directory_rooted_graph_work_row_guard"
_ROOT_TRUNCATE = "rooted_graph_work_truncate"
_COPY_OWNER = "rooted_graph_copy_owner"
_ROOT_IMMUTABLE = "rooted_graph_witness_immutable"
_BUDGET = "rooted_graph_witness_budget"


@pytest.mark.parametrize(
    "root,names",
    [
        (2, {"pd_uhc_flex_practitioner_acquisition_guard", "pd_uhc_flex_practitioner_acquisition_truncate"}),
        (3, {_PRAC_WORK, _PRAC_TRUNCATE, _FROZEN_INSERT}),
        (4, {_PRAC_IMMUTABLE, _FROZEN_INSERT}),
        (5, {"pd_uhc_flex_practitioner_twin_attempt_" + suffix for suffix in ("insert", "immutable", "truncate")}),
        (
            6,
            {"pd_uhc_flex_practitioner_admission_" + suffix for suffix in ("insert", "immutable", "truncate")}
            | {
                "pd_uhc_flex_practitioner_single_root_admission_insert",
                "pd_dataset_bulk_parent_delete",
                "pd_dataset_parent_key_update",
            },
        ),
        (
            7,
            {"provider_directory_rooted_graph_acquisition_" + suffix for suffix in ("row_guard", "truncate_guard")}
            | {"rooted_graph_storage_seal", "pd_dataset_bulk_parent_delete", "pd_dataset_parent_key_update"},
        ),
        (8, {_ROOT_WORK, _ROOT_TRUNCATE, _COPY_OWNER}),
        (9, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (10, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (
            11,
            {
                ("provider_directory_rooted_graph_twin_attempt_" + suffix)[:63]
                for suffix in ("row_guard", "truncate_guard")
            },
        ),
        (
            12,
            {
                ("provider_directory_rooted_graph_twin_admission_" + suffix)[:63]
                for suffix in ("row_guard", "truncate_guard", "legacy_insert_guard", "single_root_insert_guard")
            },
        ),
    ],
)
def test_fixed_hook_names_match_complete_migrations(root, names):
    assert custody._root_hook_contracts()[custody.ACQUISITION_ROOTS[root]].keys() == names


@pytest.mark.parametrize(
    "root,attached,history,empty,names",
    [
        (0, False, True, True, {_FROZEN_INSERT, _PRAC_TRUNCATE}),
        (0, True, True, False, {_FROZEN_INSERT, _PRAC_TRUNCATE, _PRAC_WORK}),
        (0, False, False, False, {_FROZEN_INSERT}),
        (0, True, False, False, {_FROZEN_INSERT, _PRAC_WORK}),
        (1, False, True, True, {_FROZEN_INSERT, _PRAC_IMMUTABLE}),
        (1, False, False, False, {_FROZEN_INSERT, _PRAC_IMMUTABLE}),
        (2, False, True, True, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (2, True, True, False, {_COPY_OWNER, _ROOT_IMMUTABLE, _ROOT_WORK}),
        (2, False, False, False, {_COPY_OWNER, _ROOT_TRUNCATE, _BUDGET}),
        (2, True, False, False, {_COPY_OWNER, _ROOT_TRUNCATE, _BUDGET, _ROOT_WORK}),
        (3, False, True, True, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (3, True, True, False, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (3, False, False, False, {_COPY_OWNER, _ROOT_IMMUTABLE, _BUDGET}),
        (3, True, False, False, {_COPY_OWNER, _ROOT_IMMUTABLE, _BUDGET}),
        (4, False, True, True, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (4, True, True, False, {_COPY_OWNER, _ROOT_IMMUTABLE}),
        (4, False, False, False, {_COPY_OWNER, _ROOT_IMMUTABLE, _BUDGET}),
        (4, True, False, False, {_COPY_OWNER, _ROOT_IMMUTABLE, _BUDGET}),
    ],
)
def test_member_hooks_follow_actual_storage_phases(root, attached, history, empty, names):
    contracts = custody._member_hook_contracts(
        custody.BULK_ROOTS[root], attached=attached, history=history, empty=empty
    )
    assert contracts.keys() == names
    for name, contract in contracts.items():
        if contract[1] & 1:
            assert name in {_PRAC_WORK, _ROOT_WORK} and contract[1] == 27 and attached
    if _FROZEN_INSERT in contracts:
        assert contracts[_FROZEN_INSERT][2] == (("frozen",) if root == 0 or history else ("admission",))
    if _BUDGET in contracts:
        assert contracts[_BUDGET][3] == "inserted_" + ("work", "resource", "edge")[root - 2]


@pytest.mark.parametrize("change", ["keys", "predicate", "duplicate", "not_unique", "unready"])
def test_census_index_requires_exact_two_keys(change):
    root = custody.ACQUISITION_ROOTS[8]
    partial = _partial(root)
    if change == "keys":
        partial[0]["columns"] = ["acquisition_id"]
    elif change == "predicate":
        partial[0]["predicate"] = "true"
    elif change == "duplicate":
        partial.append(deepcopy(partial[0]))
    else:
        partial[0]["unique" if change == "not_unique" else "ready"] = False
    with pytest.raises(ValueError, match="administrator_index_required"):
        custody._require_parent_indexes(root, custody._models()[root], _indexes(custody._models()[root]), partial)


def test_admission_hooks_match_authentic_specialization(monkeypatch):
    from tests.test_provider_directory_specialized_single_root_admission_migration import _capture

    _, statements, _ = _capture(monkeypatch, "upgrade")
    contracts = custody._root_hook_contracts()
    expected_by_name = {}
    for statement in statements:
        if statement.startswith("CREATE TRIGGER") and " WHEN " in statement:
            name = statement.split()[2].strip('"')[:63]
            expected_by_name[name] = re.search(r"NEW.admission_contract_id = '([^']+)'", statement)[1]
    actual_by_name = {
        name: contract[-1]
        for root in (custody.ACQUISITION_ROOTS[6], custody.ACQUISITION_ROOTS[12])
        for name, contract in contracts[root].items()
        if len(contract) == 8 and contract[-1] is not None
    }
    assert len(expected_by_name) == 4 and actual_by_name == expected_by_name


def _hook_catalog(root):
    contracts = custody._root_hook_contracts()[root]
    number_by_column = {
        column.name: position for position, column in enumerate(custody._models()[root].__table__.columns, 1)
    }
    catalog_by_field = dict(
        triggers=[], attributes=[dict(attname=name, attnum=position) for name, position in number_by_column.items()]
    )
    functions_by_name = {}
    for position, (name, contract) in enumerate(contracts.items(), 100):
        function, mask, arguments, transition, selected, *extra = contract
        enabled, old_table, predicate = extra or ("A", None, None)
        functions_by_name.setdefault(function, dict(definition=dict(oid=str(500 + position))))
        hook_by_field = dict(
            oid=str(position),
            tgrelid="71",
            tgname=name,
            tgfoid=functions_by_name[function]["definition"]["oid"],
            tgtype=mask,
            tgenabled=enabled,
            tgisinternal=False,
            tgdeferrable=False,
            tginitdeferred=False,
            tgqual="native predicate" if predicate else None,
            tgnargs=len(arguments),
            tgargs="\\x" + "".join(argument.encode().hex() + "00" for argument in arguments),
            tgattr=[number_by_column[column] for column in selected],
            tgparentid="0",
            tgoldtable=old_table,
            tgnewtable=transition,
        )
        catalog_by_field["triggers"].append(hook_by_field)
        if predicate:
            catalog_by_field.setdefault("hook_predicates", {})[name] = dict(
                oid=position,
                definition="CREATE TRIGGER synthetic BEFORE INSERT ON snapshot_schema.synthetic FOR EACH ROW WHEN (((new.admission_contract_id)::text = '"
                + predicate
                + "'::text)) EXECUTE FUNCTION snapshot_schema.synthetic()",
                dependencies=[
                    ("function", int(hook_by_field["tgfoid"]), 0, "n"),
                    ("relation", 71, 0, "a"),
                    ("relation", 71, number_by_column["admission_contract_id"], "n"),
                ],
            )
    return catalog_by_field, contracts, functions_by_name


@pytest.mark.parametrize("position", [6, 7, 12])
def test_closed_guard_shapes_preserve_exact_modes(position):
    catalog, contracts, functions = _hook_catalog(custody.ACQUISITION_ROOTS[position])
    custody._require_hooks(catalog, contracts, functions)
    for hook in catalog["triggers"]:
        hook["tgenabled"] = "O" if hook["tgenabled"] == "A" else "A"
        with pytest.raises(ValueError, match="hooks_changed"):
            custody._require_hooks(catalog, contracts, functions)
        hook["tgenabled"] = "O" if hook["tgenabled"] == "A" else "A"


@pytest.mark.parametrize("position", [6, 12])
@pytest.mark.parametrize(
    "change",
    [
        "parentheses",
        "cast",
        "case",
        "column",
        "operator",
        "extra_boolean",
        "missing",
        "oid",
        "extra_edge",
        "duplicate_edge",
        "wrong_function",
        "wrong_relation",
        "wrong_column",
        "wrong_class",
        "wrong_kind",
    ],
)
def test_predicate_corruption_is_refused(position, change):
    catalog, contracts, functions = _hook_catalog(custody.ACQUISITION_ROOTS[position])
    custody._require_hooks(catalog, contracts, functions)
    proof = next(iter(catalog["hook_predicates"].values()))
    predicate = next(contract[-1] for contract in contracts.values() if len(contract) == 8 and contract[-1])
    replacement_by_change = {
        "parentheses": (predicate, predicate + "()"),
        "cast": (predicate, predicate + "::text"),
        "case": (predicate, predicate.upper()),
        "column": ("admission_contract_id", "other_contract_id"),
        "operator": (" = ", " <> "),
        "extra_boolean": ("::text)) EXECUTE", "::text) OR true) EXECUTE"),
    }
    if change in replacement_by_change:
        proof["definition"] = proof["definition"].replace(*replacement_by_change[change])
    elif change == "missing":
        catalog["hook_predicates"].clear()
    elif change == "oid":
        proof["oid"] += 1
    elif change in {"extra_edge", "duplicate_edge"}:
        proof["dependencies"].append(
            proof["dependencies"][0] if change == "duplicate_edge" else ("operator", 999, 0, "n")
        )
    else:
        index, field, changed_field = {
            "wrong_function": (0, 1, 999),
            "wrong_relation": (1, 1, 999),
            "wrong_column": (2, 2, 999),
            "wrong_class": (0, 0, "operator"),
            "wrong_kind": (1, 3, "n"),
        }[change]
        dependency_fields = list(proof["dependencies"][index])
        dependency_fields[field] = changed_field
        proof["dependencies"][index] = tuple(dependency_fields)
    with pytest.raises(ValueError, match="hooks_changed"):
        custody._require_hooks(catalog, contracts, functions)


@pytest.mark.asyncio
async def test_predicate_capture_is_one_closed_catalog_batch():
    root = custody.ACQUISITION_ROOTS[6]
    catalog, contracts, functions = _hook_catalog(root)
    proofs = catalog.pop("hook_predicates")
    native_rows = [
        dict(
            name=name,
            oid=proof["oid"],
            definition=proof["definition"],
            reference_kind=kind,
            reference_oid=oid,
            reference_column=column,
            dependency_type=dependency_type,
        )
        for name, proof in proofs.items()
        for kind, oid, column, dependency_type in proof["dependencies"]
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=native_rows))
    await custody._capture_hook_predicates(connection, catalog, dict(oid="71"), contracts)
    custody._require_hooks(catalog, contracts, functions)
    query, relation_oid, names = connection.fetch.await_args.args
    assert connection.fetch.await_count == 1 and relation_oid == 71 and set(names) == proofs.keys()
    assert "pg_catalog.pg_get_triggerdef(t.oid,false)" in query
    assert "d.classid='pg_catalog.pg_trigger'::regclass" in query
    assert "d.objid=t.oid AND d.objsubid=0" in query
    assert "d.refobjid::bigint" in query and "d.deptype::text" in query


@pytest.mark.parametrize(
    "root,name,function,arguments",
    [
        (0, "pd_practitioner_plan_write", "pd_practitioner_migration_fence", ("metadata",)),
        (1, "pdrg_plan_owner", "guard_provider_directory_rooted_graph_copy_owner", ()),
    ],
)
def test_metadata_hooks_keep_exact_fences(root, name, function, arguments):
    assert custody._root_hook_contracts()[custody.METADATA_ROOTS[root]] == {name: (function, 62, arguments, None, ())}


@pytest.mark.parametrize(
    "field,changed_field",
    [
        ("tgtype", 31),
        ("tgfoid", "999"),
        ("tgargs", "\\x01"),
        ("tgnargs", 1),
        ("tgattr", [99]),
        ("tgqual", None),
        ("tgdeferrable", True),
        ("tginitdeferred", True),
        ("tgparentid", "99"),
        ("tgoldtable", "other"),
        ("tgnewtable", "other"),
    ],
)
def test_other_native_hook_drift_refuses(field, changed_field):
    catalog, contracts, functions = _hook_catalog(custody.ACQUISITION_ROOTS[6])
    catalog["triggers"][0][field] = changed_field
    with pytest.raises(ValueError, match="hooks_changed"):
        custody._require_hooks(catalog, contracts, functions)


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_unexpected_hook_names_still_refuse(change):
    catalog, contracts, functions = _hook_catalog(custody.ACQUISITION_ROOTS[6])
    if change == "missing":
        catalog["triggers"].pop()
    else:
        catalog["triggers"].append(dict(catalog["triggers"][0], tgname="unrelated_hook"))
    with pytest.raises(ValueError, match="hooks_changed"):
        custody._require_hooks(catalog, contracts, functions)


@pytest.mark.parametrize("damage", [None, "schema", "missing", "duplicate"])
def test_dataset_plan_requires_complete_logical_relationships(damage):
    from db.migration_provider_dataset_online import _expected_bulk_relationships

    plan_by_field = {
        "relationships": [
            dict(
                source_table=source_table,
                target_table=target_table,
                target_schema="snapshot_schema",
                source_columns=list(source_columns),
                target_columns=list(target_columns),
                delete_action=action,
            )
            for source_table, target_table, source_columns, target_columns, action in sorted(
                _expected_bulk_relationships()
            )
        ]
    }
    if damage == "schema":
        plan_by_field["relationships"][0]["target_schema"] = "unselected_schema"
    elif damage == "missing":
        plan_by_field["relationships"].pop()
    elif damage == "duplicate":
        plan_by_field["relationships"].append(dict(plan_by_field["relationships"][0]))
    if damage:
        with pytest.raises(ValueError, match="dataset_relationships_changed"):
            custody._require_dataset_relationships(plan_by_field, "snapshot_schema")
    else:
        custody._require_dataset_relationships(plan_by_field, "snapshot_schema")
