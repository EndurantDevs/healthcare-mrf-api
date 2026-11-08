# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expected effective rights from authenticated ACLs and locked native role powers.

This returns comparison data, not authority. The caller authenticates the before
inventory and plan and retains role, DDL and current-database-row custody through
the transition. Native capabilities must reproduce every protected before vector
before they may project the planned ACLs. Private vectors stay unchanged.
"""

import json
from copy import deepcopy

_TABLE = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN")
_COLUMN = ("SELECT", "INSERT", "UPDATE", "REFERENCES")
_ROLE_FIELDS = {"roles", "memberships", "reach", "relation_rights", "namespace_rights", "routine_rights"}
_CAPABILITIES_SQL = """WITH actors AS (
 SELECT oid,rolname,rolsuper FROM pg_catalog.pg_roles WHERE oid=ANY($1::oid[])
), grantees AS (
 SELECT oid FROM pg_catalog.pg_roles WHERE oid=ANY($2::oid[])
)
SELECT actor.oid::bigint AS oid,actor.rolname,actor.rolsuper,
 pg_catalog.pg_has_role(actor.oid,'pg_read_all_data'::pg_catalog.regrole::oid,'USAGE') AS read_all,
 pg_catalog.pg_has_role(actor.oid,'pg_write_all_data'::pg_catalog.regrole::oid,'USAGE') AS write_all,
 pg_catalog.pg_has_role(actor.oid,'pg_maintain'::pg_catalog.regrole::oid,'USAGE') AS maintain,
 COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(grantee.oid::bigint,
 pg_catalog.pg_has_role(actor.oid,grantee.oid,'USAGE')) ORDER BY grantee.oid)
 FROM grantees grantee),'[]'::jsonb) AS usage
FROM actors actor ORDER BY actor.oid"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_owner_role_rights_" + code)


def _oid(identity):
    _require(
        type(identity) is int or type(identity) is str and identity.isascii() and identity.isdecimal(),
        "oid_required",
    )
    selected = int(identity)
    _require(0 < selected < 2**32, "oid_required")
    return selected


def _indexed(entries, code):
    _require(type(entries) is list and bool(entries), code)
    entries_by_oid = {}
    for entry in entries:
        _require(type(entry) is dict and "oid" in entry and type(entry["oid"]) is int, code)
        identity = _oid(entry["oid"])
        _require(identity not in entries_by_oid, code)
        entries_by_oid[identity] = entry
    return entries_by_oid


def _roles(inventory):
    protected = inventory.get("roles")
    _require(type(protected) is dict and set(protected) == _ROLE_FIELDS, "role_fields_changed")
    _require(type(protected["roles"]) is list and bool(protected["roles"]), "roles_required")
    roles_by_oid = {}
    for role in protected["roles"]:
        _require(
            type(role) is dict
            and {"oid", "rolname", "rolsuper"} <= role.keys()
            and type(role["rolname"]) is str
            and bool(role["rolname"])
            and type(role["rolsuper"]) is bool,
            "role_identity_changed",
        )
        identity = _oid(role["oid"])
        _require(identity not in roles_by_oid, "role_identity_changed")
        roles_by_oid[identity] = role
    _require(
        list(roles_by_oid) == sorted(roles_by_oid)
        and len({role["rolname"] for role in roles_by_oid.values()}) == len(roles_by_oid),
        "role_order_changed",
    )
    for field in ("memberships", "reach", "namespace_rights"):
        _require(protected[field] is None or type(protected[field]) is list, "role_fields_changed")
    return roles_by_oid


def _columns(relation):
    native = relation.get("native")
    _require(type(native) is dict and type(native.get("columns")) is list, "columns_required")
    names, numbers, visible = set(), set(), []
    for column in native["columns"]:
        _require(type(column) is dict and type(column.get("attribute")) is dict, "columns_changed")
        attribute = column["attribute"]
        number, name = attribute.get("attnum"), attribute.get("attname")
        _require(
            type(number) is int
            and 0 < number <= 32767
            and number not in numbers
            and type(name) is str
            and name not in names
            and type(attribute.get("attisdropped")) is bool,
            "columns_changed",
        )
        numbers.add(number)
        names.add(name)
        if not attribute["attisdropped"]:
            visible.append(number)
    _require(visible == sorted(visible), "column_order_changed")
    return visible


def _scopes(inventory, plan):
    _require(type(inventory) is dict and type(plan) is dict, "inputs_required")
    canonical = inventory.get("canonical")
    _require(type(canonical) is dict, "canonical_required")
    relations = []
    for field in ("relations", "dependency_relations", "dependency_histories"):
        _require(type(canonical.get(field)) is list, "relations_required")
        relations.extend(canonical[field])
    relations_by_oid = _indexed(relations, "relation_scope_changed")
    private = inventory.get("private")
    _require(type(private) is list, "private_scope_changed")
    private_by_oid = _indexed(private, "private_scope_changed") if private else {}
    _require(not relations_by_oid.keys() & private_by_oid.keys(), "private_scope_changed")
    physical_by_oid = _indexed(canonical.get("physical"), "physical_scope_changed")
    routines_by_oid = _indexed(inventory.get("routines"), "routine_scope_changed")
    relation_plans_by_oid = _indexed(plan.get("relations"), "plan_relation_scope_changed")
    routine_plans_by_oid = _indexed(plan.get("routines"), "plan_routine_scope_changed")
    _require(
        relations_by_oid.keys() == physical_by_oid.keys() == relation_plans_by_oid.keys()
        and routines_by_oid.keys() == routine_plans_by_oid.keys(),
        "plan_scope_changed",
    )
    return (
        relations_by_oid,
        private_by_oid,
        physical_by_oid,
        routines_by_oid,
        relation_plans_by_oid,
        routine_plans_by_oid,
    )


def _acl(entries, columns, roles_by_oid, *, is_column=False, is_routine=False):
    _require(type(entries) is list, "acl_required")
    edges = []
    for entry in entries:
        _require(type(entry) is list and len(entry) == (5 if is_column else 4), "acl_changed")
        number, edge = (entry[0], entry[1:]) if is_column else (None, entry)
        grantee, grantor, privilege, has_option = edge
        _require(
            type(grantee) is int
            and (grantee == 0 or grantee in roles_by_oid)
            and type(grantor) is int
            and _oid(grantor)
            and type(has_option) is bool
            and type(privilege) is str
            and privilege in (("EXECUTE",) if is_routine else _COLUMN if is_column else _TABLE),
            "acl_identity_changed",
        )
        _require(not is_column or type(number) is int and number in columns, "column_acl_changed")
        edges.append(tuple(entry))
    _require(len(set(edges)) == len(edges), "acl_duplicate")
    return edges


def _relation_acl(relation, before, after, roles_by_oid):
    columns = _columns(relation)
    _require(type(before) is dict and type(after) is dict, "relation_acl_required")
    _require(_oid(relation.get("relowner")) in roles_by_oid, "relation_owner_unbound")
    _require(_oid(after.get("owner_oid")) in roles_by_oid, "relation_owner_unbound")
    return (
        columns,
        (
            _acl(before.get("table_acl"), columns, roles_by_oid),
            _acl(before.get("column_acl"), columns, roles_by_oid, is_column=True),
        ),
        (
            _acl(after.get("table_acl"), columns, roles_by_oid),
            _acl(after.get("column_acl"), columns, roles_by_oid, is_column=True),
        ),
    )


def _routine_acl(routine, planned, roles_by_oid):
    native = routine.get("native")
    _require(type(native) is dict and type(native.get("definition")) is dict, "routine_required")
    definition = native["definition"]
    _require(
        _oid(definition.get("oid")) == routine["oid"]
        and _oid(definition.get("proowner")) in roles_by_oid
        and _oid(planned.get("owner_oid")) in roles_by_oid,
        "routine_owner_unbound",
    )
    return (
        _acl(native.get("acl"), (), roles_by_oid, is_routine=True),
        _acl(planned.get("routine_acl"), (), roles_by_oid, is_routine=True),
    )


def _acl_inputs(inventory, plan, roles_by_oid):
    relations, private, physical, routines, relation_plans, routine_plans = _scopes(inventory, plan)
    relation_acls_by_oid = {
        identity: _relation_acl(relation, physical[identity].get("native"), relation_plans[identity], roles_by_oid)
        for identity, relation in relations.items()
    }
    routine_acls_by_oid = {
        identity: _routine_acl(routine, routine_plans[identity], roles_by_oid) for identity, routine in routines.items()
    }
    columns_by_oid = {identity: item[0] for identity, item in relation_acls_by_oid.items()}
    columns_by_oid.update({identity: _columns(relation) for identity, relation in private.items()})
    return relation_acls_by_oid, routine_acls_by_oid, columns_by_oid


def _grantees(relation_acls_by_oid, routine_acls_by_oid):
    grantee_oids = set()
    for _, before, after in relation_acls_by_oid.values():
        for tables, columns in (before, after):
            grantee_oids.update(edge[0] for edge in tables if edge[0])
            grantee_oids.update(edge[1] for edge in columns if edge[1])
    for before, after in routine_acls_by_oid.values():
        grantee_oids.update(edge[0] for edge in (*before, *after) if edge[0])
    return sorted(grantee_oids)


async def _capabilities(connection, roles_by_oid, grantees):
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    captured = await connection.fetch(_CAPABILITIES_SQL, list(roles_by_oid), grantees)
    capabilities_by_oid = {}
    for native_capability in captured:
        capability_by_field = dict(native_capability)
        _require(
            set(capability_by_field) == {"oid", "rolname", "rolsuper", "read_all", "write_all", "maintain", "usage"},
            "native_shape_changed",
        )
        identity = capability_by_field["oid"]
        _require(
            type(identity) is int and identity in roles_by_oid and identity not in capabilities_by_oid,
            "native_identity_changed",
        )
        role = roles_by_oid[identity]
        _require(
            all(type(capability_by_field[field]) is bool for field in ("rolsuper", "read_all", "write_all", "maintain"))
            and capability_by_field["rolname"] == role["rolname"]
            and capability_by_field["rolsuper"] is role["rolsuper"],
            "native_role_changed",
        )
        usage = capability_by_field["usage"]
        usage = json.loads(usage) if type(usage) is str else usage
        _require(
            type(usage) is list
            and all(
                type(edge) is list and len(edge) == 2 and type(edge[0]) is int and type(edge[1]) is bool
                for edge in usage
            )
            and [edge[0] for edge in usage] == grantees,
            "native_usage_changed",
        )
        capabilities_by_oid[identity] = {
            **capability_by_field,
            "usage": {grantee for grantee, has_usage in usage if has_usage},
        }
    _require(list(capabilities_by_oid) == list(roles_by_oid), "native_roster_changed")
    return capabilities_by_oid


def _effective_relation(capability, columns, acls):
    tables, column_edges = acls
    grantees = {0, *capability["usage"]}
    rights = {privilege for grantee, _, privilege, _ in tables if grantee in grantees}
    if capability["read_all"]:
        rights.add("SELECT")
    if capability["write_all"]:
        rights.update(("INSERT", "UPDATE", "DELETE"))
    if capability["maintain"]:
        rights.add("MAINTAIN")
    if capability["rolsuper"]:
        rights.update(_TABLE)
    selected_edges = {(number, privilege) for number, grantee, _, privilege, _ in column_edges if grantee in grantees}
    selected_edges.update((number, privilege) for number in columns for privilege in _COLUMN if privilege in rights)
    return [privilege for privilege in _TABLE if privilege in rights], [list(edge) for edge in sorted(selected_edges)]


def _effective_routine(capability, acl):
    return capability["rolsuper"] or any(grantee == 0 or grantee in capability["usage"] for grantee, *_ in acl)


def _relation_vectors(protected, columns_by_oid, role_oids):
    supplied = protected.get("relation_rights")
    _require(type(supplied) is list, "relation_vectors_required")
    identities = []
    for entry in supplied:
        _require(type(entry) is list and len(entry) == 4, "relation_vector_changed")
        actor, relation, tables, columns = entry
        _require(
            type(actor) is int and actor in role_oids and type(relation) is int and relation in columns_by_oid,
            "relation_vector_identity_changed",
        )
        _require(
            type(tables) is list and tables == [privilege for privilege in _TABLE if privilege in tables],
            "table_vector_changed",
        )
        _require(
            type(columns) is list
            and all(
                type(edge) is list
                and len(edge) == 2
                and type(edge[0]) is int
                and edge[0] in columns_by_oid[relation]
                and type(edge[1]) is str
                and edge[1] in _COLUMN
                for edge in columns
            ),
            "column_vector_changed",
        )
        _require(
            columns == [list(edge) for edge in sorted({tuple(edge) for edge in columns})], "column_vector_order_changed"
        )
        identities.append((actor, relation))
    _require(
        identities == [(actor, relation) for actor in role_oids for relation in sorted(columns_by_oid)],
        "relation_vector_scope_changed",
    )
    return supplied


def _routine_vectors(protected, routine_oids, role_oids):
    supplied = protected.get("routine_rights")
    _require(type(supplied) is list, "routine_vectors_required")
    _require(
        all(
            type(entry) is list
            and len(entry) == 3
            and type(entry[0]) is int
            and type(entry[1]) is int
            and type(entry[2]) is bool
            for entry in supplied
        )
        and [entry[:2] for entry in supplied]
        == [[actor, routine] for actor in role_oids for routine in sorted(routine_oids)],
        "routine_vector_scope_changed",
    )
    return supplied


def _project(protected, relation_acls_by_oid, routine_acls_by_oid, columns_by_oid, capabilities_by_oid):
    expected = deepcopy(protected)
    relations = _relation_vectors(protected, columns_by_oid, list(capabilities_by_oid))
    for position, (actor, relation, tables, columns) in enumerate(relations):
        if relation in relation_acls_by_oid:
            numbers, before, after = relation_acls_by_oid[relation]
            capability = capabilities_by_oid[actor]
            _require(_effective_relation(capability, numbers, before) == (tables, columns), "before_relation_mismatch")
            after_tables, after_columns = _effective_relation(capability, numbers, after)
            expected["relation_rights"][position] = [actor, relation, after_tables, after_columns]
    for position, (actor, routine, has_execute) in enumerate(
        _routine_vectors(protected, routine_acls_by_oid, list(capabilities_by_oid))
    ):
        before, after = routine_acls_by_oid[routine]
        capability = capabilities_by_oid[actor]
        _require(_effective_routine(capability, before) is has_execute, "before_routine_mismatch")
        expected["routine_rights"][position] = [actor, routine, _effective_routine(capability, after)]
    return expected


async def expected_cms_owner_roles(connection, inventory, plan):
    """Project exact effective postimage vectors after proving the protected before.

    No changes or extra authority are issued. All role/namespace/reach data and
    private vectors are copied exactly; unsupported or mismatched scope refuses.
    """
    _require(type(inventory) is dict, "inventory_required")
    roles_by_oid = _roles(inventory)
    relations, routines, columns = _acl_inputs(inventory, plan, roles_by_oid)
    capabilities = await _capabilities(connection, roles_by_oid, _grantees(relations, routines))
    return _project(inventory["roles"], relations, routines, columns, capabilities)
