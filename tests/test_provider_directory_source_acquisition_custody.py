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


@pytest.mark.parametrize("field", ["acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid"])
def test_practitioner_plan_requires_each_original_or_current_oid(field):
    plans, roots = _plans()
    custody._require_migration_plans(plans, roots)
    custody._require_practitioner_migration_plan(plans[custody.METADATA_ROOTS[0]], roots)
    plans[custody.METADATA_ROOTS[0]][0][field] += 1
    with pytest.raises(ValueError, match="practitioner_migration_changed"):
        custody._require_migration_plans(plans, roots)
    with pytest.raises(ValueError, match="practitioner_migration_changed"):
        custody._require_practitioner_migration_plan(plans[custody.METADATA_ROOTS[0]], roots)


@pytest.mark.parametrize("plan", custody.METADATA_ROOTS)
def test_migration_metadata_requires_the_exact_completed_singleton(plan):
    plans, roots = _plans()
    plans[plan].append(deepcopy(plans[plan][0]))
    with pytest.raises(ValueError, match="migration_"):
        custody._require_migration_plans(plans, roots)


def _plan_capture():
    plans, roots = _plans()
    for history in custody.HISTORY_ROOTS:
        roots[history].update(partition_bound=None, parent_oid=None)

    async def fetch(query):
        root = next(root for root in custody.METADATA_ROOTS if root in query)
        return [{"body": json.dumps(plan)} for plan in plans[root]]

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=fetch), fetchval=AsyncMock(return_value=True))
    arguments = (
        connection,
        SimpleNamespace(schema="snapshot_schema"),
        roots,
        {root: roots[root]["oid"] for root in custody.BULK_ROOTS},
        {root: {} for root in custody.BULK_ROOTS},
    )
    return arguments, plans


@pytest.mark.asyncio
async def test_capture_casts_only_practitioner_oids():
    arguments, plans = _plan_capture()
    assert await custody._capture_history_plans(*arguments) == plans
    queries = [call.args[0] for call in arguments[0].fetch.await_args_list]
    practitioner, rooted = queries
    assert practitioner.startswith("SELECT to_jsonb(r) || jsonb_build_object(")
    for field in ("acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid"):
        assert practitioner.count("'" + field + "',r." + field + "::bigint") == 1
    assert practitioner.count("::bigint") == 5
    assert rooted == 'SELECT to_jsonb(r) AS body FROM "snapshot_schema"."pdrg_storage_migration" r'


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["extra", "changed_oid", "incomplete"])
async def test_capture_keeps_strict_plan_refusals(change):
    arguments, plans = _plan_capture()
    practitioner = plans[custody.METADATA_ROOTS[0]][0]
    if change == "extra":
        practitioner["unexpected"] = True
    elif change == "changed_oid":
        practitioner["parent_oid"] += 1
    else:
        practitioner["work_parent_oid"] = None
    with pytest.raises(ValueError, match="practitioner_migration_changed"):
        await custody._capture_history_plans(*arguments)


@pytest.mark.parametrize("field", ["acquisition_oid", "work_oid", "resource_oid", "parent_oid", "work_parent_oid"])
def test_plan_gate_requires_native_integers(field):
    plans, roots = _plans()
    practitioner = plans[custody.METADATA_ROOTS[0]][0]
    practitioner[field] = str(practitioner[field])
    with pytest.raises(ValueError, match="practitioner_migration_changed"):
        custody._require_migration_plans(plans, roots)


def _native_member(root, acquisition, oid=500):
    return dict(
        root=root,
        name=custody._candidate_name(root, acquisition),
        oid=oid,
        acquisition_id=acquisition,
        parent_oid=None,
        bounds=None,
        phase="loading",
        kind="r",
        persistence="p",
    )


def _members(root, acquisition, roots):
    group = custody.BULK_ROOTS[:2] if root in custody.BULK_ROOTS[:2] else custody.BULK_ROOTS[2:]
    members = [_native_member(parent, acquisition, oid=500 + position) for position, parent in enumerate(group)]
    for member in members:
        member.update(parent_oid=roots[member["root"]]["oid"], bounds=[acquisition], phase="attached")
    return members


@pytest.mark.parametrize("root", custody.BULK_ROOTS)
def test_private_candidate_name_binds_exact_protocol_identity(root):
    _, roots = _plans()
    acquisition = ("pdufpa_" if root in custody.BULK_ROOTS[:2] else "pdrga_") + "1" * 48
    members = _members(root, acquisition, roots)
    member = next(member for member in members if member["root"] == root)
    acquisitions_by_root = {parent: {acquisition: "building"} for parent in custody.BULK_ROOTS}
    custody._require_family_members(roots, members, acquisitions_by_root)
    member["name"] = custody._candidate_name(root, acquisition[:-1] + "2")
    with pytest.raises(ValueError, match="family_member_changed"):
        custody._require_family_members(roots, members, acquisitions_by_root)


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("oid", 100, "family_parent_changed"),
        ("parent_oid", 999, "family_parent_changed"),
        ("bounds", ["other"], "family_phase_changed"),
        ("kind", "f", "family_member_changed"),
        ("persistence", "t", "family_member_changed"),
    ],
)
def test_candidate_substitution_never_becomes_family_membership(field, value, code):
    _, roots = _plans()
    root = custody.BULK_ROOTS[0]
    acquisition = "pdufpa_" + "1" * 48
    member = _native_member(root, acquisition)
    member[field] = value
    with pytest.raises(ValueError, match=code):
        custody._require_family_members(roots, [member], {root: {acquisition: "building"}})


def test_duplicate_candidate_scope_or_oid_is_rejected():
    _, roots = _plans()
    root, acquisition = custody.BULK_ROOTS[0], "pdufpa_" + "1" * 48
    first = _native_member(root, acquisition)
    second = _native_member(root, acquisition, oid=501)
    with pytest.raises(ValueError, match="family_scope_reused"):
        custody._require_family_members(roots, [first, second], {root: {acquisition: "building"}})


def test_loading_work_and_attached_rooted_group_have_distinct_protocol_shapes():
    _, roots = _plans()
    acquisition = "pdufpa_" + "1" * 48
    work = _native_member(custody.BULK_ROOTS[0], acquisition)
    custody._require_family_members(roots, [work], {custody.BULK_ROOTS[0]: {acquisition: "building"}})
    work["phase"] = "attached"
    with pytest.raises(ValueError, match="family_phase_changed"):
        custody._require_family_members(roots, [work], {custody.BULK_ROOTS[0]: {acquisition: "building"}})
    acquisition = "pdrga_" + "1" * 48
    members = _members(custody.BULK_ROOTS[2], acquisition, roots)
    acquisitions_by_root = {parent: {acquisition: "building"} for parent in custody.BULK_ROOTS[2:]}
    with pytest.raises(ValueError, match="family_group_incomplete"):
        custody._require_family_members(roots, members[:-1], acquisitions_by_root)


def _foreign_graph():
    contracts = custody._foreign_contracts()
    native_by_root = {}
    for contract in contracts.values():
        for field, names in (("child", contract["columns"]), ("parent", contract["parent_columns"])):
            native = native_by_root.setdefault(
                contract[field], dict(oid=100 + len(native_by_root), owner=17, columns=[])
            )
            native["columns"].extend(name for name in names if name not in native["columns"])
    edges = []
    for name, contract in contracts.items():
        child, parent = native_by_root[contract["child"]], native_by_root[contract["parent"]]
        edges.append(
            dict(
                child=[child["oid"], "snapshot_schema", contract["child"], 17],
                parent=[parent["oid"], "snapshot_schema", contract["parent"], 17],
                constraint=dict(
                    conname=name,
                    conrelid=str(child["oid"]),
                    confrelid=str(parent["oid"]),
                    conkey=[child["columns"].index(column) + 1 for column in contract["columns"]],
                    confkey=[parent["columns"].index(column) + 1 for column in contract["parent_columns"]],
                    confmatchtype="s",
                    confupdtype="a",
                    confdeltype="a",
                    condeferrable=contract["deferred"],
                    condeferred=contract["initially_deferred"],
                    convalidated=True,
                    conenforced=True,
                    conparentid="0",
                ),
            )
        )
    return native_by_root, edges


def test_control_fk_graph_preserves_api_and_native_keys_without_bulk_ri():
    roots, edges = _foreign_graph()
    custody._require_foreign_graph(edges, roots, "snapshot_schema", owner=17, builder=18)
    assert "provider_directory_api_endpoint" in roots
    assert not any(edge["child"][2] in custody.BULK_ROOTS for edge in edges)
    assert len(edges) == len(custody._foreign_contracts())


@pytest.mark.parametrize(
    "field,value",
    [
        ("confkey", [999]),
        ("confdeltype", "c"),
        ("convalidated", False),
        ("conenforced", False),
        ("conparentid", "41"),
        ("condeferrable", "false"),
    ],
)
def test_foreign_contract_drift_refuses_before_adoption(field, value):
    roots, edges = _foreign_graph()
    edges[0]["constraint"][field] = value
    with pytest.raises(ValueError, match="foreign_contract_changed"):
        custody._require_foreign_graph(edges, roots, "snapshot_schema", owner=17, builder=18)


def test_unknown_incoming_fk_cannot_become_a_protected_owner_allowlist():
    roots, edges = _foreign_graph()
    unknown = deepcopy(edges[0])
    unknown["constraint"]["conname"] = "unrelated_incoming_constraint"
    unknown["child"] = [999, "snapshot_schema", "unrelated_table", 17]
    with pytest.raises(ValueError, match="foreign_scope_changed"):
        custody._require_foreign_graph([*edges, unknown], roots, "snapshot_schema", owner=17, builder=18)


def test_known_external_parent_must_already_have_protected_custody():
    roots, edges = _foreign_graph()
    external = "provider_directory_endpoint_dataset"
    roots[external]["owner"] = 18
    for edge in edges:
        for field in ("child", "parent"):
            if edge[field][2] == external:
                edge[field][3] = 18
    with pytest.raises(ValueError, match="foreign_identity_changed"):
        custody._require_foreign_graph(edges, roots, "snapshot_schema", owner=17, builder=18, legacy=True)


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


@pytest.mark.asyncio
async def test_external_fk_on_private_child_refuses_before_any_native_allowance():
    private_name = "pdrgw_" + "1" * 48
    proof_by_field = dict(catalog=dict(classes=[dict(relname=private_name, oid="71")]))
    unknown_by_field = dict(constraint=dict(oid="99"))
    native = SimpleNamespace(fetch=AsyncMock(return_value=[unknown_by_field]), fetchval=AsyncMock())
    family_by_field = dict(relations={private_name: proof_by_field}, foreign=[], cohort=dict(relations={}))
    with pytest.raises(ValueError, match="external_foreign_authority_changed"):
        await controls._capture_complete_foreign(native, {}, family_by_field, [])
    assert native.fetch.await_args.args[1] == [71]
    native.fetchval.assert_not_awaited()


def test_equal_check_text_cannot_mask_a_substituted_function_oid():
    root = custody.ACQUISITION_ROOTS[2]
    entry_by_field = dict(
        name=root,
        root=root,
        catalog=dict(constraints=[dict(oid="41", conname="fixed", contype="c", constraint_definition="CHECK (true)")]),
        dependencies=[dict(object_kind="pg_constraint", objid=41, refclassid=1255, refobjid=51)],
    )
    expected_by_root = {
        root: {"fixed": "check(true)"},
        "_dependencies": {root: [dict(conname="fixed", refclassid=1255, refobjid=51)]},
    }
    custody._require_relation_checks(entry_by_field, expected_by_root)
    entry_by_field["dependencies"][0]["refobjid"] = 52
    with pytest.raises(ValueError, match="check_dependency_changed"):
        custody._require_relation_checks(entry_by_field, expected_by_root)


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


@pytest.mark.asyncio
async def test_loading_work_captures_no_hooks(monkeypatch):
    root, name = custody.BULK_ROOTS[0], "pd_uhc_flex_pw_" + "1" * 40
    native_by_field = dict(relname=name, oid="71", relowner="17", relispartition=False)
    catalog_by_field = dict(classes=[native_by_field], triggers=None, attributes=[])
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=json.dumps(catalog_by_field)),
        fetch=AsyncMock(return_value=_columns(custody._models()[root])),
    )
    for function in ("_require_catalog_shape", "_require_defaults", "_require_relation_access"):
        monkeypatch.setattr(custody, function, lambda *args, **kwargs: None)
    monkeypatch.setattr(custody, "_require_family_indexes", AsyncMock())
    monkeypatch.setattr(custody, "_capture_expression_type", AsyncMock(return_value=([], {})))
    captured = await custody._capture_family_relation(
        connection,
        SimpleNamespace(schema="snapshot_schema"),
        name,
        root,
        dict(roles=dict(owner=17, builder=18), functions={}),
        legacy=True,
        member=dict(phase="loading"),
    )
    assert captured["loading"] is True and captured["catalog"]["triggers"] is None
    custody._require_family_indexes.assert_awaited_once()
    assert custody._require_family_indexes.await_args.args[-1] is True
    assert connection.fetch.await_count == 1


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


def _joint_adoption_family(monkeypatch, parent="provider_directory_source", schema="snapshot_schema"):
    from process import provider_directory_source_cohort_custody as cohort

    monkeypatch.setattr(cohort, "require_cohort_adoption_delta", lambda *_args: None)
    root = "provider_directory_rooted_graph_acquisition"
    before_by_field = dict(
        schema="snapshot_schema",
        roots={root: {"owner": 17}},
        members=[],
        relations={},
        cohort={"relations": {}},
        foreign=[
            dict(
                child=[101, "snapshot_schema", root, 17],
                parent=[102, schema, parent, 17],
                constraint={"oid": "103", "conname": "reference_fkey"},
            )
        ],
    )
    after_by_field = deepcopy(before_by_field)
    after_by_field["roots"][root]["owner"] = 23
    after_by_field["foreign"][0]["child"][3] = after_by_field["foreign"][0]["parent"][3] = 23
    return before_by_field, after_by_field


@pytest.mark.parametrize("parent", ["provider_directory_source", "provider_directory_api_endpoint"])
def test_joint_cut_preserves_control_parent_identity_with_new_owner(monkeypatch, parent):
    before, after = _joint_adoption_family(monkeypatch, parent)
    custody.require_source_acquisition_delta(before, after, {"owner": 23}, {})


@pytest.mark.parametrize(
    "change", ["foreign_schema", "unrelated_parent", "parent_oid", "child_oid", "constraint", "wrong_owner"]
)
def test_joint_cut_refuses_unrelated_foreign_parent_changes(monkeypatch, change):
    parent = "unrelated_parent" if change == "unrelated_parent" else "provider_directory_source"
    schema = "other_schema" if change == "foreign_schema" else "snapshot_schema"
    before, after = _joint_adoption_family(monkeypatch, parent, schema)
    edge = after["foreign"][0]
    if change in {"parent_oid", "child_oid"}:
        edge[change.removesuffix("_oid")][0] = 999
    elif change == "constraint":
        edge["constraint"]["conname"] = "other_fkey"
    elif change == "wrong_owner":
        edge["parent"][3] = 29
    with pytest.raises(ValueError, match="adoption_family_changed"):
        custody.require_source_acquisition_delta(before, after, {"owner": 23}, {})
