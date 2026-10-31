# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure structural checks for a genuinely planned common ownership transition.

The caller authenticates the original inventory and planner output, holds native
custody and supplies independent PostgreSQL expansions of every supplied raw ACL.
Those query results are data, never authority. Role/effective-rights verification
and complete raw dynamic-family stability remain mandatory caller checks before
promoting successor hashes. This module executes no SQL and issues no records.
"""

from copy import deepcopy

from process.provider_directory_cms_control_custody import _RIGHTS

_INVENTORY = {"canonical", "private", "routines", "roles"}
_CANONICAL = {
    "relations",
    "migration_plan",
    "dependency_relations",
    "dependency_histories",
    "dependency_migration_plan",
    "physical",
}
_RELATION_FIELDS = {"oid", "nspname", "relname", "relowner", "relkind", "relpersistence", "native"}
_SITE_FIELDS = {"phase", "kind", "oid", "attnum", "owner_oid", "raw_acl"}
_COLUMN_RIGHTS = {"SELECT", "INSERT", "UPDATE", "REFERENCES"}
_PHYSICAL_FIELDS = {
    "relfilenode",
    "table_acl",
    "column_acl",
    "rowtypes",
    "toast_oid",
    "toast_classes",
    "toast_attributes",
    "toast_indexes",
    "toast_constraints",
    "toast_triggers",
    "toast_rules",
    "toast_policies",
}


def _require(condition, code):
    if not condition:
        raise ValueError("cms_owner_structure_" + code)


def _oid(value):
    _require(type(value) is int or type(value) is str and value.isascii() and value.isdecimal(), "identity_required")
    selected = int(value)
    _require(0 < selected < 2**32, "identity_required")
    return selected


def _same(left, right):
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(_same(value, right[key]) for key, value in left.items())
    if type(left) is list:
        return len(left) == len(right) and all(_same(old, new) for old, new in zip(left, right, strict=True))
    return left == right


def _by_oid(rows, fields, code):
    _require(type(rows) is list, code)
    rows_by_oid = {}
    for row in rows:
        _require(type(row) is dict and set(row) == fields and type(row["oid"]) is int, code)
        oid = _oid(row["oid"])
        _require(oid not in rows_by_oid, code)
        rows_by_oid[oid] = row
    return rows_by_oid


def _parts(inventory):
    _require(type(inventory) is dict and set(inventory) == _INVENTORY, "inventory_fields_changed")
    canonical = inventory["canonical"]
    _require(type(canonical) is dict and set(canonical) == _CANONICAL, "canonical_fields_changed")
    _require(
        type(canonical["migration_plan"]) is dict and type(canonical["dependency_migration_plan"]) is dict,
        "canonical_plan_required",
    )
    _require(type(inventory["roles"]) is dict, "roles_required")
    relation_rows = []
    for field in ("relations", "dependency_relations", "dependency_histories"):
        _require(type(canonical[field]) is list, "relations_required")
        relation_rows.extend(canonical[field])
    relations = _by_oid(relation_rows, _RELATION_FIELDS, "relation_identity_changed")
    private = _by_oid(inventory["private"], _RELATION_FIELDS, "private_identity_changed")
    _require(
        all(
            type(relation_row["nspname"]) is str
            and bool(relation_row["nspname"])
            and type(relation_row["relname"]) is str
            and bool(relation_row["relname"])
            and type(relation_row["relowner"]) is int
            and _oid(relation_row["relowner"])
            for relation_row in (*relations.values(), *private.values())
        ),
        "relation_identity_changed",
    )
    _require(not relations.keys() & private.keys(), "private_identity_changed")
    physical = _by_oid(canonical["physical"], {"oid", "native"}, "physical_identity_changed")
    _require(
        all(
            type(physical_row["native"]) is dict and set(physical_row["native"]) == _PHYSICAL_FIELDS
            for physical_row in physical.values()
        ),
        "physical_native_required",
    )
    routines = _by_oid(inventory["routines"], {"oid", "native"}, "routine_identity_changed")
    _require(bool(relations) and physical.keys() == relations.keys() and bool(routines), "inventory_scope_changed")
    _require(
        len({(relation_row["nspname"], relation_row["relname"]) for relation_row in relations.values()})
        == len(relations),
        "relation_name_changed",
    )
    for private_row in private.values():
        _relation_sites(private_row, "before")
    return relations, physical, routines


def _edges(rows, kind):
    _require(type(rows) is list, "acl_edges_required")
    allowed = _COLUMN_RIGHTS if kind == "column" else {"EXECUTE"} if kind == "routine" else _RIGHTS
    edges = set()
    for edge in rows:
        _require(type(edge) is list and len(edge) == 4, "acl_edges_required")
        grantee, grantor, right, grantable = edge
        _require(
            type(grantee) is int
            and 0 <= grantee < 2**32
            and type(grantor) is int
            and 0 < grantor < 2**32
            and type(right) is str
            and right in allowed
            and type(grantable) is bool,
            "acl_edges_required",
        )
        edge_values = tuple(edge)
        _require(edge_values not in edges, "duplicate_acl_edge")
        edges.add(edge_values)
    return edges


def _site(phase, kind, oid, attnum, owner, raw):
    _oid(owner)
    _require(type(owner) is int, "acl_owner_required")
    if kind == "table":
        _require(raw is None or type(raw) is str, "raw_acl_required")
    else:
        _require(raw is None or type(raw) is list and all(type(item) is str for item in raw), "raw_acl_required")
    return dict(phase=phase, kind=kind, oid=oid, attnum=attnum, owner_oid=owner, raw_acl=deepcopy(raw))


def _relation_sites(relation_row, phase):
    native, oid = relation_row["native"], relation_row["oid"]
    fields = {
        "namespace",
        "relation",
        "parents",
        "columns",
        "constraints",
        "indexes",
        "triggers",
        "rules",
        "dependencies",
    }
    _require(
        type(native) is dict
        and set(native) == fields
        and all(type(native_part) is list for native_part in native.values()),
        "relation_native_required",
    )
    relation = native["relation"]
    _require(
        len(relation) == 13
        and _same(
            relation[:5],
            [
                oid,
                relation_row["relname"],
                relation_row["relkind"],
                relation_row["relpersistence"],
                relation_row["relowner"],
            ],
        )
        and len(native["namespace"]) == 4
        and native["namespace"][1] == relation_row["nspname"],
        "relation_native_changed",
    )
    return [
        _site(phase, "table", oid, 0, relation_row["relowner"], relation[10]),
        *_column_sites(native["columns"], phase, oid, relation_row["relowner"]),
    ]


def _column_sites(columns, phase, oid, owner):
    _require(type(columns) is list, "columns_required")
    sites = []
    numbers, names = set(), set()
    for column in columns:
        _require(
            type(column) is dict
            and set(column) == {"attribute", "default", "type", "collation"}
            and type(column["attribute"]) is dict,
            "column_identity_changed",
        )
        attribute = column["attribute"]
        number, name = attribute.get("attnum"), attribute.get("attname")
        _require(
            type(number) is int
            and number > 0
            and number not in numbers
            and type(name) is str
            and bool(name)
            and name not in names
            and type(attribute.get("attisdropped")) is bool
            and "attacl" in attribute
            and _oid(attribute.get("attrelid")) == oid,
            "column_identity_changed",
        )
        _require(not attribute["attisdropped"] or attribute["attacl"] is None, "dropped_column_acl")
        numbers.add(number)
        names.add(name)
        sites.append(_site(phase, "column", oid, number, owner, attribute["attacl"]))
    return sites


def _routine_site(row, phase):
    native = row["native"]
    _require(
        type(native) is dict
        and set(native)
        == {"definition", "language", "namespace", "body", "dependencies", "acl", "namespace_identifier"}
        and type(native["definition"]) is dict,
        "routine_native_required",
    )
    definition, namespace = native["definition"], native.get("namespace")
    _require(
        _oid(definition.get("oid")) == row["oid"]
        and type(definition.get("proname")) is str
        and bool(definition["proname"])
        and type(namespace) is list
        and len(namespace) == 4
        and _oid(definition.get("pronamespace")) == _oid(namespace[0])
        and type(namespace[1]) is str
        and type(definition.get("proargtypes")) is list
        and all(_oid(argument) for argument in definition["proargtypes"])
        and "proacl" in definition
        and type(native.get("acl")) is list,
        "routine_identity_changed",
    )
    return _site(phase, "routine", row["oid"], 0, _oid(definition.get("proowner")), definition["proacl"])


def cms_owner_acl_inputs(inventory, phase):
    """Project exact native-query inputs; callers independently expand these raw ACLs."""
    _require(type(phase) is str and phase in {"before", "after"}, "acl_phase_required")
    relations, _physical, routines = _parts(inventory)
    sites = [site for oid in sorted(relations) for site in _relation_sites(relations[oid], phase)]
    sites.extend(_routine_site(routines[oid], phase) for oid in sorted(routines))
    _require(
        len(
            {
                (
                    row["native"]["namespace"][0],
                    row["native"]["definition"]["proname"],
                    tuple(_oid(argument) for argument in row["native"]["definition"]["proargtypes"]),
                )
                for row in routines.values()
            }
        )
        == len(routines),
        "routine_signature_changed",
    )
    return sites


def _expanded_acls(before, after, expansions):
    inputs = cms_owner_acl_inputs(before, "before") + cms_owner_acl_inputs(after, "after")
    inputs_by_site = {(row["phase"], row["kind"], row["oid"], row["attnum"]): row for row in inputs}
    _require(type(expansions) is list and len(expansions) == len(inputs_by_site), "acl_expansion_scope_changed")
    edges_by_site = {}
    for row in expansions:
        _require(type(row) is dict and set(row) == _SITE_FIELDS | {"edges"}, "acl_expansion_fields_changed")
        _require(
            type(row["phase"]) is str
            and type(row["kind"]) is str
            and type(row["oid"]) is int
            and type(row["attnum"]) is int,
            "acl_expansion_identity_changed",
        )
        key = row["phase"], row["kind"], row["oid"], row["attnum"]
        _require(key in inputs_by_site and key not in edges_by_site, "acl_expansion_identity_changed")
        _require(_same({field: row[field] for field in _SITE_FIELDS}, inputs_by_site[key]), "raw_acl_binding_changed")
        edges_by_site[key] = _edges(row["edges"], row["kind"])
    _require(edges_by_site.keys() == inputs_by_site.keys(), "acl_expansion_scope_changed")
    return edges_by_site


def _physical_acls(relations, physical, routines, expanded, phase):
    for oid, relation in relations.items():
        native = physical[oid]["native"]
        _require(type(native) is dict and set(native) == _PHYSICAL_FIELDS, "physical_native_required")
        _require(expanded[phase, "table", oid, 0] == _edges(native.get("table_acl"), "table"), "table_acl_changed")
        columns = {
            (number, *edge)
            for (selected_phase, kind, selected_oid, number), edges in expanded.items()
            if (selected_phase, kind, selected_oid) == (phase, "column", oid)
            for edge in edges
        }
        raw = native.get("column_acl")
        _require(type(raw) is list, "column_acl_changed")
        column_edges = set()
        for row in raw:
            _require(type(row) is list and len(row) == 5 and type(row[0]) is int, "column_acl_changed")
            edge = (row[0], *next(iter(_edges([row[1:]], "column"))))
            _require(edge not in column_edges, "duplicate_acl_edge")
            column_edges.add(edge)
        _require(columns == column_edges, "column_acl_changed")
    for oid, routine in routines.items():
        _require(
            expanded[phase, "routine", oid, 0] == _edges(routine["native"]["acl"], "routine"), "routine_acl_changed"
        )


def _plans(plan, relations, routines):
    _require(
        type(plan) is dict and set(plan) == {"owner_oid", "ordinary_writer_oid", "relations", "routines"},
        "plan_fields_changed",
    )
    owner, writer = plan["owner_oid"], plan["ordinary_writer_oid"]
    _require(type(owner) is int and type(writer) is int and _oid(owner) != _oid(writer), "plan_owner_changed")
    tables = _by_oid(
        plan["relations"],
        {"oid", "nspname", "relname", "owner_oid", "table_acl", "column_acl"},
        "plan_relation_changed",
    )
    functions = _by_oid(
        plan["routines"], {"oid", "nspname", "proname", "owner_oid", "routine_acl"}, "plan_routine_changed"
    )
    _require(tables.keys() == relations.keys() and functions.keys() == routines.keys(), "plan_scope_changed")
    for oid, planned_entry in tables.items():
        original = relations[oid]
        _require(
            (planned_entry["nspname"], planned_entry["relname"], planned_entry["owner_oid"])
            == (original["nspname"], original["relname"], owner),
            "plan_relation_changed",
        )
    for oid, planned_entry in functions.items():
        native = routines[oid]["native"]
        expected_owner = _oid(native["definition"]["proowner"]) if native["namespace"][1] == "pg_catalog" else owner
        _require(
            (planned_entry["nspname"], planned_entry["proname"], planned_entry["owner_oid"])
            == (native["namespace"][1], native["definition"]["proname"], expected_owner),
            "plan_routine_changed",
        )
    return tables, functions


def _owner_value(before, target):
    _oid(before)
    return str(target) if type(before) is str else target


def _relation_after(before, after, plan):
    expected = deepcopy(before)
    expected["relowner"] = expected["native"]["relation"][4] = plan["owner_oid"]
    expected["native"]["relation"][10] = after["native"]["relation"][10]
    _require(len(expected["native"]["columns"]) == len(after["native"]["columns"]), "columns_changed")
    for column, current in zip(expected["native"]["columns"], after["native"]["columns"], strict=True):
        column["attribute"]["attacl"] = current["attribute"]["attacl"]
    for index in expected["native"]["indexes"]:
        _require(
            type(index) is dict and type(index.get("class")) is list and len(index["class"]) == 4,
            "index_identity_changed",
        )
        _require(_oid(index["class"][1]) == before["relowner"], "index_owner_changed")
        index["class"][1] = plan["owner_oid"]
    return expected


def _physical_after(before, plan):
    expected = deepcopy(before)
    native = expected["native"]
    native["table_acl"], native["column_acl"] = deepcopy((plan["table_acl"], plan["column_acl"]))
    for field, owner_field in (("rowtypes", "typowner"), ("toast_classes", "relowner")):
        rows = native.get(field)
        _require(type(rows) is list, "physical_native_required")
        object_oids = set()
        for row in rows:
            _require(type(row) is dict and _oid(row.get("oid")) not in object_oids, "physical_object_changed")
            object_oids.add(_oid(row["oid"]))
            row[owner_field] = _owner_value(row.get(owner_field), plan["owner_oid"])
    return expected


def _routine_after(before, after, plan):
    if before["native"]["namespace"][1] == "pg_catalog":
        _require(_same(before["native"]["acl"], plan["routine_acl"]), "native_routine_plan_changed")
        return before
    expected = deepcopy(before)
    definition = expected["native"]["definition"]
    definition["proowner"] = _owner_value(definition["proowner"], plan["owner_oid"])
    definition["proacl"] = deepcopy(after["native"]["definition"]["proacl"])
    expected["native"]["acl"] = deepcopy(plan["routine_acl"])
    return expected


def require_cms_owner_structure(before, after, plan, *, acl_expansions):
    """Require exact planned structure; roles, effective access and dynamic census are caller gates."""
    original, physical, routines = _parts(before)
    current, observed_physical, observed_routines = _parts(after)
    _require(
        original.keys() == current.keys() and routines.keys() == observed_routines.keys(), "inventory_scope_changed"
    )
    _require(_same(before["private"], after["private"]), "private_changed")
    expanded = _expanded_acls(before, after, acl_expansions)
    _physical_acls(original, physical, routines, expanded, "before")
    _physical_acls(current, observed_physical, observed_routines, expanded, "after")
    tables, functions = _plans(plan, original, routines)
    canonical_by_kind = dict(before["canonical"])
    for field in ("relations", "dependency_relations", "dependency_histories"):
        canonical_by_kind[field] = [
            _relation_after(row, current[row["oid"]], tables[row["oid"]]) for row in canonical_by_kind[field]
        ]
    canonical_by_kind["physical"] = [_physical_after(row, tables[row["oid"]]) for row in canonical_by_kind["physical"]]
    _require(_same(canonical_by_kind, after["canonical"]), "canonical_changed")
    expected_routines = [
        _routine_after(row, observed_routines[row["oid"]], functions[row["oid"]]) for row in before["routines"]
    ]
    _require(_same(expected_routines, after["routines"]), "routines_changed")
