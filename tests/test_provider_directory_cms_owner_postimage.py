# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Whole-inventory structure and independent raw-ACL expansion refusals, without SQL."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_owner_postimage as checks
from tests.test_provider_directory_cms_owner_permissions import _plan
from tests.test_provider_directory_cms_owner_permissions import owner_case as owner_case

_LETTERS = {
    "SELECT": "r",
    "INSERT": "a",
    "UPDATE": "w",
    "DELETE": "d",
    "TRUNCATE": "D",
    "REFERENCES": "x",
    "TRIGGER": "t",
    "MAINTAIN": "m",
    "EXECUTE": "X",
}


def _raw_acl(edges, *, table=False):
    rights_by_actor = {}
    for actor, grantor, right, grantable in edges:
        rights_by_actor.setdefault((actor, grantor), []).append(_LETTERS[right] + ("*" if grantable else ""))
    entries = [
        ("" if actor == 0 else "role_" + str(actor)) + "=" + "".join(rights) + "/role_" + str(grantor)
        for (actor, grantor), rights in sorted(rights_by_actor.items())
    ]
    return "{" + ",".join(entries) + "}" if table else entries


def _complete_relation(relation_row, physical):
    oid, owner = relation_row["oid"], relation_row["relowner"]
    native = relation_row["native"]
    native["relation"] = [
        oid,
        relation_row["relname"],
        relation_row["relkind"],
        relation_row["relpersistence"],
        owner,
        bool(native["parents"]),
        False,
        False,
        0,
        None,
        _raw_acl(physical["table_acl"], table=True),
        None,
        None,
    ]
    for column in native["columns"]:
        attribute = column["attribute"]
        attribute.update(
            attrelid=str(oid),
            attacl=_raw_acl([edge[1:] for edge in physical["column_acl"] if edge[0] == attribute["attnum"]]),
        )
        column.update(default=None, type={"oid": "25", "typowner": "10"}, collation=None)
    native.update(
        constraints=[],
        indexes=[
            dict(
                index={"indrelid": str(oid), "indisvalid": True},
                **{"class": [oid + 100000, owner, None, None]},
                definition="fixed index",
                opclasses=[],
            )
        ],
        triggers=[],
        rules=[],
        dependencies=[],
    )
    physical.update(
        relfilenode=oid * 2,
        rowtypes=[
            dict(oid=str(oid + offset), typowner=str(owner), typacl=None, typname="fixed_type")
            for offset in (300000, 400000)
        ],
        toast_oid=oid + 200000,
        toast_classes=[
            dict(oid=str(oid + offset), relowner=str(owner), relacl=None, relname="fixed_toast")
            for offset in (200000, 500000)
        ],
        toast_attributes=[],
        toast_indexes=[],
        toast_constraints=[],
        toast_triggers=[],
        toast_rules=[],
        toast_policies=[],
    )


def _complete_routines(before, plan):
    template = deepcopy(before["routines"][0])
    for oid in range(900000, 900000 + 119 - len(before["routines"])):
        row = deepcopy(template)
        row["oid"] = oid
        row["native"]["definition"].update(oid=str(oid), proname="fixture_routine_" + str(oid))
        before["routines"].append(row)
        plan["routines"].append(
            dict(
                oid=oid,
                nspname="catalog",
                proname=row["native"]["definition"]["proname"],
                owner_oid=plan["owner_oid"],
                routine_acl=[[plan["owner_oid"], plan["owner_oid"], "EXECUTE", False]],
            )
        )
    native = before["routines"][-1]
    native["native"]["namespace"] = [11, "pg_catalog", 10, None]
    native["native"]["definition"].update(pronamespace="11", proowner="10", proname="native_fixture", prosecdef=False)
    native["native"]["acl"] = [[0, 10, "EXECUTE", False], [10, 10, "EXECUTE", False]]
    plan["routines"][-1].update(
        nspname="pg_catalog", proname="native_fixture", owner_oid=10, routine_acl=deepcopy(native["native"]["acl"])
    )
    for row in before["routines"]:
        data = row["native"]
        data["definition"]["proacl"] = None if data["namespace"][1] == "pg_catalog" else _raw_acl(data["acl"])
        data.update(namespace_identifier=data["namespace"][1], body="fixed constructor body", dependencies=[])


def _expected_after(before, plan):
    after = deepcopy(before)
    tables_by_oid = {catalog_row["oid"]: catalog_row for catalog_row in plan["relations"]}
    physical_by_oid = {catalog_row["oid"]: catalog_row["native"] for catalog_row in after["canonical"]["physical"]}
    for field in ("relations", "dependency_relations", "dependency_histories"):
        for catalog_row in after["canonical"][field]:
            permission = tables_by_oid[catalog_row["oid"]]
            catalog_row["relowner"] = catalog_row["native"]["relation"][4] = permission["owner_oid"]
            catalog_row["native"]["relation"][10] = _raw_acl(permission["table_acl"], table=True)
            for column in catalog_row["native"]["columns"]:
                number = column["attribute"]["attnum"]
                column["attribute"]["attacl"] = _raw_acl(
                    [edge[1:] for edge in permission["column_acl"] if edge[0] == number]
                )
            for index in catalog_row["native"]["indexes"]:
                index["class"][1] = permission["owner_oid"]
            physical_by_oid[catalog_row["oid"]]["table_acl"] = deepcopy(permission["table_acl"])
            physical_by_oid[catalog_row["oid"]]["column_acl"] = deepcopy(permission["column_acl"])
            for key, owner_field in (("rowtypes", "typowner"), ("toast_classes", "relowner")):
                for native in physical_by_oid[catalog_row["oid"]][key]:
                    native[owner_field] = str(permission["owner_oid"])
    routines_by_oid = {catalog_row["oid"]: catalog_row for catalog_row in plan["routines"]}
    for catalog_row in after["routines"]:
        if catalog_row["native"]["namespace"][1] == "pg_catalog":
            continue
        permission = routines_by_oid[catalog_row["oid"]]
        catalog_row["native"]["definition"]["proowner"] = str(permission["owner_oid"])
        catalog_row["native"]["definition"]["proacl"] = _raw_acl(permission["routine_acl"])
        catalog_row["native"]["acl"] = deepcopy(permission["routine_acl"])
    after["roles"] = {"independently_verified_by_caller": True}
    return after


@pytest.fixture
def structure_case(owner_case):
    # The existing planner fixture supplies fixed roots, retained leaves and caps.
    plan = _plan(owner_case)
    before = deepcopy(owner_case.inventory)
    physical_by_oid = {row["oid"]: row["native"] for row in before["canonical"]["physical"]}
    for field in ("relations", "dependency_relations", "dependency_histories"):
        for row in before["canonical"][field]:
            _complete_relation(row, physical_by_oid[row["oid"]])
    _complete_routines(before, plan)
    private = deepcopy(before["canonical"]["relations"][0])
    private.update(oid=7000, relname="private_root", nspname="private_source")
    private["native"]["relation"][:2] = [7000, "private_root"]
    private["native"]["namespace"][:2] = [43, "private_source"]
    for column in private["native"]["columns"]:
        column["attribute"]["attrelid"] = "7000"
    before["private"] = [private]
    return SimpleNamespace(before=before, after=_expected_after(before, plan), plan=plan)


def _expansions(before, after):
    expansions = []
    for phase, inventory in (("before", before), ("after", after)):
        physical_by_oid = {row["oid"]: row["native"] for row in inventory["canonical"]["physical"]}
        routines_by_oid = {row["oid"]: row["native"]["acl"] for row in inventory["routines"]}
        for site in checks.cms_owner_acl_inputs(inventory, phase):
            if site["kind"] == "routine":
                edges = routines_by_oid[site["oid"]]
            elif site["kind"] == "table":
                edges = physical_by_oid[site["oid"]]["table_acl"]
            else:
                edges = [row[1:] for row in physical_by_oid[site["oid"]]["column_acl"] if row[0] == site["attnum"]]
            expansions.append({**site, "edges": deepcopy(edges)})
    return expansions


def _verify(case, expansions=None):
    selected = _expansions(case.before, case.after) if expansions is None else expansions
    return checks.require_cms_owner_structure(case.before, case.after, case.plan, acl_expansions=selected)


def test_full_family_and_119_routines_preserve_inputs(structure_case):
    case = structure_case
    before = deepcopy(vars(case))
    assert len(case.plan["relations"]) >= 48 and len(case.plan["routines"]) == 119
    assert _verify(case) is None
    assert vars(case) == before


def test_projection_is_detached_and_every_expansion_is_mandatory(structure_case):
    case = structure_case
    sites = checks.cms_owner_acl_inputs(case.before, "before")
    assert sites and all(set(row) == checks._SITE_FIELDS for row in sites)
    sites[0]["raw_acl"] = "changed"
    assert sites != checks.cms_owner_acl_inputs(case.before, "before")
    with pytest.raises(TypeError):
        checks.require_cms_owner_structure(case.before, case.after, case.plan)


@pytest.mark.parametrize("phase", [None, "", "BEFORE", [], 1])
def test_projection_refuses_unknown_phase(structure_case, phase):
    with pytest.raises(ValueError, match="acl_phase_required"):
        checks.cms_owner_acl_inputs(structure_case.before, phase)


@pytest.mark.parametrize("kind", ["table", "column", "routine"])
@pytest.mark.parametrize("field", ["phase", "oid", "attnum", "owner_oid", "raw_acl", "edges"])
def test_raw_acl_query_result_must_bind_exact_site_and_semantics(structure_case, kind, field):
    case = structure_case
    expansions = _expansions(case.before, case.after)
    row = next(row for row in expansions if row["kind"] == kind and row["phase"] == "after")
    if field == "raw_acl":
        row[field] = "wrong" if kind == "table" else ["wrong"]
    elif field == "edges":
        row[field] = [[999, row["owner_oid"], "EXECUTE" if kind == "routine" else "SELECT", False]]
    elif field == "phase":
        row[field] = "unknown"
    else:
        row[field] += 1
    with pytest.raises(ValueError):
        _verify(case, expansions)


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param(lambda rows: rows.pop(), id="missing"),
        pytest.param(lambda rows: rows.extend([deepcopy(rows[0])]), id="extra"),
        pytest.param(lambda rows: rows[0].update(rows[-1]), id="duplicate"),
        pytest.param(lambda rows: rows[0].update(additional=True), id="fields"),
        pytest.param(lambda rows: rows[0].update(kind=[]), id="unhashable"),
        pytest.param(lambda rows: rows[0]["edges"].append(deepcopy(rows[0]["edges"][0])), id="duplicate_edge"),
        pytest.param(lambda rows: rows[0].update(edges=[[101, 101, "SELECT", 0]]), id="grantable_type"),
    ],
)
def test_malformed_or_incomplete_acl_expansion_refuses(structure_case, mutation):
    case = structure_case
    expansions = _expansions(case.before, case.after)
    mutation(expansions)
    with pytest.raises(ValueError):
        _verify(case, expansions)


@pytest.mark.parametrize(
    "collection", ["relations", "dependency_relations", "dependency_histories", "physical", "routines"]
)
@pytest.mark.parametrize("fault", ["missing", "extra", "duplicate", "malformed"])
def test_changed_inventory_identity_scope_refuses(structure_case, collection, fault):
    case = structure_case
    rows = case.after["routines"] if collection == "routines" else case.after["canonical"][collection]
    if fault == "missing":
        rows.pop()
    elif fault in {"extra", "duplicate"}:
        row = deepcopy(rows[0])
        if fault == "extra":
            row["oid"] += 10000000
        rows.append(row)
    else:
        rows[0]["oid"] = True
    with pytest.raises(ValueError):
        _verify(case)


@pytest.mark.parametrize("collection", ["relations", "routines"])
@pytest.mark.parametrize("fault", ["missing", "extra", "duplicate", "malformed", "name", "owner"])
def test_changed_planner_identity_scope_refuses(structure_case, collection, fault):
    case = structure_case
    rows = case.plan[collection]
    if fault == "missing":
        rows.pop()
    elif fault in {"extra", "duplicate"}:
        row = deepcopy(rows[0])
        if fault == "extra":
            row["oid"] += 10000000
        rows.append(row)
    elif fault == "malformed":
        rows[0]["unexpected"] = True
    elif fault == "name":
        rows[0]["relname" if collection == "relations" else "proname"] = "wrong"
    else:
        rows[0]["owner_oid"] += 1
    with pytest.raises(ValueError):
        _verify(case)


def _set_path(document, path, replacement):
    selected = document
    for part in path[:-1]:
        selected = selected[part]
    selected[path[-1]] = replacement


_RELATION_DRIFT = [
    (("relname",), "changed"),
    (("native", "relation", 11), "changed bound"),
    (("native", "namespace", 2), 999),
    (("native", "columns", 0, "attribute", "attname"), "changed"),
    (("native", "columns", 0, "default"), {"changed": True}),
    (("native", "columns", 0, "type", "typowner"), "999"),
    (("native", "constraints"), [{"changed": True}]),
    (("native", "indexes", 0, "class", 0), 999),
    (("native", "indexes", 0, "class", 1), 999),
    (("native", "indexes", 0, "class", 2), "changed"),
    (("native", "triggers"), [{"changed": True}]),
    (("native", "rules"), [{"changed": True}]),
    (("native", "dependencies"), [{"changed": True}]),
]


@pytest.mark.parametrize("path,replacement", _RELATION_DRIFT)
def test_whole_relation_structure_remains_exact(structure_case, path, replacement):
    case = structure_case
    _set_path(case.after["canonical"]["relations"][0], path, replacement)
    with pytest.raises(ValueError):
        _verify(case)


_PHYSICAL_DRIFT = [
    (("relfilenode",), 999),
    (("toast_oid",), 999),
    (("rowtypes", 0, "oid"), "999"),
    (("rowtypes", 0, "typowner"), "999"),
    (("rowtypes", 0, "typacl"), ["unbound=X/role_104"]),
    (("toast_classes", 0, "oid"), "999"),
    (("toast_classes", 0, "relowner"), "999"),
    (("toast_classes", 0, "relacl"), "changed"),
    (("toast_indexes",), [{"changed": True}]),
    (("toast_constraints",), [{"changed": True}]),
    (("toast_triggers",), [{"changed": True}]),
    (("toast_rules",), [{"changed": True}]),
    (("toast_attributes",), [{"changed": True}]),
    (("toast_policies",), [{"changed": True}]),
]


@pytest.mark.parametrize("path,replacement", _PHYSICAL_DRIFT)
def test_rowtype_toast_and_physical_storage_remain_exact(structure_case, path, replacement):
    case = structure_case
    _set_path(case.after["canonical"]["physical"][0]["native"], path, replacement)
    with pytest.raises(ValueError):
        _verify(case)


@pytest.mark.parametrize("field", ["prosrc", "proargtypes", "prorettype", "proconfig", "prosecdef", "provolatile"])
def test_every_application_routine_definition_field_is_unchanged(structure_case, field):
    case = structure_case
    case.after["routines"][0]["native"]["definition"][field] = "changed"
    with pytest.raises(ValueError, match="routine.*changed"):
        _verify(case)


@pytest.mark.parametrize("field", ["body", "language", "namespace_identifier", "dependencies"])
def test_routine_constructor_language_namespace_and_dependencies_are_preserved(structure_case, field):
    case = structure_case
    case.after["routines"][0]["native"][field] = "changed"
    with pytest.raises(ValueError, match="routines_changed"):
        _verify(case)


def test_native_routines_must_stay_completely_unchanged(structure_case):
    case = structure_case
    case.after["routines"][-1]["native"]["definition"]["prosrc"] = "changed"
    with pytest.raises(ValueError, match="routines_changed"):
        _verify(case)


@pytest.mark.parametrize("field", ["migration_plan", "dependency_migration_plan"])
def test_original_history_and_provenance_commitments_never_change(structure_case, field):
    case = structure_case
    case.after["canonical"][field] = {"changed": True}
    with pytest.raises(ValueError, match="canonical_changed"):
        _verify(case)


def test_private_storage_never_changes(structure_case):
    case = structure_case
    case.after["private"][0]["relowner"] += 1
    case.after["private"][0]["native"]["relation"][4] += 1
    with pytest.raises(ValueError, match="private_changed"):
        _verify(case)


@pytest.mark.parametrize("level", ["inventory", "canonical", "plan"])
def test_unknown_top_level_fields_refuse(structure_case, level):
    case = structure_case
    selected = case.after if level == "inventory" else case.after["canonical"] if level == "canonical" else case.plan
    selected["unreviewed"] = True
    with pytest.raises(ValueError):
        _verify(case)


@pytest.mark.parametrize("phase", ["before", "after"])
def test_nonnull_dropped_column_acl_is_not_ignored(structure_case, phase):
    inventory = getattr(structure_case, phase)
    attribute = inventory["canonical"]["relations"][0]["native"]["columns"][0]["attribute"]
    attribute.update(attisdropped=True, attacl=["unbound=r/role_104"])
    with pytest.raises(ValueError, match="dropped_column_acl"):
        checks.cms_owner_acl_inputs(inventory, phase)


def test_equivalent_new_acl_spelling_is_allowed_only_after_independent_expansion(structure_case):
    case = structure_case
    row = case.after["canonical"]["relations"][0]
    raw = row["native"]["relation"][10]
    row["native"]["relation"][10] = "{" + ",".join('"' + item + '"' for item in raw[1:-1].split(",")) + "}"
    assert _verify(case) is None


def test_default_null_acl_keeps_native_function_exact(structure_case):
    case = structure_case
    assert case.before["routines"][-1]["native"]["definition"]["proacl"] is None
    assert _verify(case) is None


def test_null_dropped_attribute_still_has_a_bound_query_site(structure_case):
    inventory = structure_case.before
    relation = inventory["canonical"]["relations"][0]
    attribute = relation["native"]["columns"][0]["attribute"]
    attribute.update(attisdropped=True, attacl=None)
    sites = [
        row
        for row in checks.cms_owner_acl_inputs(inventory, "before")
        if row["kind"] == "column" and row["oid"] == relation["oid"] and row["attnum"] == attribute["attnum"]
    ]
    assert len(sites) == 1 and sites[0]["raw_acl"] is None


def test_duplicate_routine_signature_with_new_oid_refuses(structure_case):
    case = structure_case
    row = case.before["routines"][1]
    original = case.before["routines"][0]["native"]
    row["native"]["definition"].update(
        proname=original["definition"]["proname"], proargtypes=original["definition"]["proargtypes"]
    )
    with pytest.raises(ValueError, match="routine_signature_changed"):
        checks.cms_owner_acl_inputs(case.before, "before")


@pytest.mark.parametrize("field", ["relfilenode", "rowtypes", "toast_oid", "table_acl"])
def test_missing_physical_fields_are_not_a_supported_preimage(structure_case, field):
    case = structure_case
    case.before["canonical"]["physical"][0]["native"].pop(field)
    with pytest.raises(ValueError, match="physical_native_required"):
        _verify(case)


@pytest.mark.parametrize("field", ["table_acl", "column_acl", "routine_acl"])
def test_expanded_acl_must_equal_the_plan_not_only_raw_spelling(structure_case, field):
    case = structure_case
    rows = case.plan["routines"] if field == "routine_acl" else case.plan["relations"]
    rows[0][field] = []
    with pytest.raises(ValueError):
        _verify(case)


@pytest.mark.parametrize("fault", ["duplicate_column", "missing_column", "duplicate_rowtype", "duplicate_toast"])
def test_malformed_native_subobject_identities_refuse(structure_case, fault):
    case = structure_case
    if fault in {"duplicate_column", "missing_column"}:
        columns = case.before["canonical"]["relations"][0]["native"]["columns"]
        if fault == "duplicate_column":
            columns.append(deepcopy(columns[0]))
        else:
            columns[0]["attribute"].pop("attnum")
    else:
        physical = case.before["canonical"]["physical"][0]["native"]
        rows = physical["rowtypes" if fault == "duplicate_rowtype" else "toast_classes"]
        rows.append(deepcopy(rows[0]))
    with pytest.raises(ValueError):
        _verify(case)
