# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only exact native-counter catalog proof shared with administrative custody."""

from db.plan_release_storage import storage_identifier, storage_relation

ROOT = "cms_native_input_revision"
FUNCTIONS = ("cms_native_input_advance", "cms_native_input_revision_guard")
_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
_GUARD = """
BEGIN
    IF TG_OP='UPDATE' AND pg_trigger_depth()=2
       AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name
       AND NEW.table_name=OLD.table_name AND NEW.revision=OLD.revision+1
       AND NEW.mutation_xid=pg_current_xact_id() THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'cms_native_input_revision_immutable';
END;
"""
_CENSUS_SQL = """SELECT (SELECT c.oid::bigint FROM pg_class c
 WHERE c.relnamespace=$1::oid AND c.relname='cms_native_input_revision') AS relation_oid,
 ARRAY(SELECT p.oid::bigint FROM pg_proc p WHERE p.pronamespace=$1::oid
 AND p.proname=ANY($2::text[]) ORDER BY p.oid) AS function_oids"""
_ACCESS_SQL = """SELECT p.oid::bigint AS function_oid,r.oid::bigint AS role,r.rolsuper,
 pg_has_role(r.oid,p.proowner,'USAGE') AS owner_usage,
 pg_has_role(r.oid,$2::oid,'USAGE') AS builder_usage,
 has_function_privilege(r.oid,p.oid,'EXECUTE') AS can_execute
 FROM pg_proc p CROSS JOIN pg_roles r WHERE p.oid=ANY($1::oid[]) ORDER BY p.oid,r.oid"""


class NativeCounterCatalogError(RuntimeError):
    """The optional installed counter does not match its exact catalog contract."""


def _equal(condition):
    if not condition:
        raise NativeCounterCatalogError("native_input_counter_catalog_changed")


def _normalized(expression):
    return "".join(expression.split()).replace("(", "").replace(")", "").replace("'4294967295'::bigint", "4294967295")


def _root(catalog):
    roots = [entry for entry in catalog["classes"] if entry["relname"] == ROOT]
    _equal(len(roots) == 1)
    return roots[0]


def _require_layout(catalog):
    root = _root(catalog)
    oid = int(root["oid"])
    _equal(root["relkind"] == "r" and root["relpersistence"] == "p" and root["relam"] == "2")
    _equal(root["reloptions"] is None and root["relreplident"] == "d")
    _equal(not any(root[key] for key in ("relispartition", "relrowsecurity", "relforcerowsecurity")))
    _equal(all(catalog[key] is None for key in ("rules", "policies", "inheritance")))
    _equal(all(entry["relowner"] == root["relowner"] for entry in catalog["classes"]))
    _equal(all(entry["relacl"] is None for entry in catalog["classes"] if entry is not root))
    _require_columns(catalog, oid)
    _require_constraints(catalog)
    from process.provider_directory_cms_control_custody import _require_native_indexes

    _require_native_indexes(catalog)
    indexes = catalog["indexes"] or []
    root_indexes = [i for i in indexes if int(i["indrelid"]) == oid]
    _equal(
        len(root_indexes) == 1
        and root_indexes[0]["indisprimary"]
        and root_indexes[0]["indisunique"]
        and root_indexes[0]["indkey"] in ([1], "1")
    )
    _equal(
        all(
            i["native"]
            and i["method"] == "btree"
            and i["indisvalid"]
            and i["indisready"]
            and i["indislive"]
            and not i["index_expressions"]
            and not i["index_predicate"]
            for i in indexes
        )
    )
    return root


def _require_constraints(catalog):
    """Require the migration's PK, two checks and four native PostgreSQL 18 NOT NULL constraints."""
    constraints = catalog["constraints"] or []
    _equal(
        len(constraints) == 7
        and all(
            constraint["convalidated"]
            and constraint["conenforced"]
            and not constraint["condeferrable"]
            and not constraint["condeferred"]
            for constraint in constraints
        )
    )
    _equal(
        sorted((constraint["contype"], tuple(constraint["conkey"])) for constraint in constraints)
        == [("c", (1,)), ("c", (4,)), ("n", (1,)), ("n", (2,)), ("n", (3,)), ("n", (4,)), ("p", (1,))]
    )
    _equal(
        {_normalized(constraint["constraint_definition"]) for constraint in constraints if constraint["contype"] == "c"}
        == {"CHECKrelation_oid>=1ANDrelation_oid<=4294967295", "CHECKrevision>=0"}
    )
    not_null_constraints = [constraint for constraint in constraints if constraint["contype"] == "n"]
    _equal(
        all(
            constraint["constraint_definition"] == f"NOT NULL {name}"
            and constraint["conislocal"] is True
            and constraint["coninhcount"] == 0
            and int(constraint["conparentid"]) == 0
            and constraint["connoinherit"] is False
            for constraint, name in zip(
                sorted(not_null_constraints, key=lambda item: item["conkey"]),
                ("relation_oid", "schema_name", "table_name", "revision"),
                strict=True,
            )
        )
    )


def _require_columns(catalog, oid):
    """Require the five builtin columns, including the nullable transaction stamp."""
    columns = [entry for entry in catalog["attributes"] if int(entry["attrelid"]) == oid]
    _equal(
        [(attribute["attnum"], attribute["attname"], int(attribute["atttypid"])) for attribute in columns]
        == [
            (1, "relation_oid", 20),
            (2, "schema_name", 19),
            (3, "table_name", 19),
            (4, "revision", 20),
            (5, "mutation_xid", 5069),
        ]
    )
    _equal(
        all(
            not attribute["attisdropped"] and not attribute["attidentity"] and not attribute["attgenerated"]
            for attribute in columns
        )
    )
    _equal([attribute["attnotnull"] for attribute in columns] == [True, True, True, True, False])
    _equal([int(attribute["attcollation"]) for attribute in columns] == [0, 950, 950, 0, 0])
    _equal([attribute["default_expression"] for attribute in columns] == ["", "", "", "0", ""])
    _equal(columns[-1]["atthasmissing"] is False and columns[-1]["attmissingval"] is None)
    _equal(all(attribute["attacl"] is None for attribute in catalog["attributes"] if int(attribute["attrelid"]) != oid))


def _require_functions(snapshot_by_field, language_oid):
    from process.provider_directory_cms_native_inputs import _advance_body

    prior = int(_root(snapshot_by_field["ledger"]["catalog"])["relowner"])
    for name, function in snapshot_by_field["functions"].items():
        definition = function["definition"]
        _equal(int(definition["proowner"]) == prior and int(definition["prolang"]) == language_oid)
        _equal(int(definition["pronamespace"]) == int(snapshot_by_field["namespace"]["oid"]))
        _equal(definition["proname"] == name and definition["pronargs"] == 0 and int(definition["prorettype"]) == 2279)
        _equal(definition["prokind"] == "f" and definition["proconfig"] == ["search_path=pg_catalog"])
        _equal(definition["prosecdef"] is (name == FUNCTIONS[0]))
        _equal(
            " ".join(definition["prosrc"].split())
            == " ".join((_advance_body(snapshot_by_field["schema"]) if name == FUNCTIONS[0] else _GUARD).split())
        )
        expected_edges = {(prior, prior, "EXECUTE", False)}
        if name == FUNCTIONS[1]:
            expected_edges.add((0, prior, "EXECUTE", False))
        elif prior != snapshot_by_field["builder"]:
            expected_edges.add((snapshot_by_field["builder"], prior, "EXECUTE", False))
        _equal(set(map(tuple, function["acl"])) == expected_edges)
    guards = snapshot_by_field["ledger"]["catalog"]["triggers"] or []
    expected_by_name = {"cms_native_input_revision_immutable": 27, "cms_native_input_revision_no_truncate": 34}
    _equal(len(guards) == 2 and {guard["tgname"] for guard in guards} == set(expected_by_name))
    function_oid = int(snapshot_by_field["functions"][FUNCTIONS[1]]["definition"]["oid"])
    _equal(
        all(
            guard["tgtype"] == expected_by_name[guard["tgname"]]
            and guard["tgenabled"] == "A"
            and not guard["tgisinternal"]
            and int(guard["tgfoid"]) == function_oid
            and guard["tgnargs"] == 0
            and guard["tgqual"] is None
            and int(guard["tgconstraint"]) == 0
            and not guard["tgdeferrable"]
            and not guard["tginitdeferred"]
            for guard in guards
        )
    )


def _require_acl(snapshot_by_field):
    catalog = snapshot_by_field["ledger"]["catalog"]
    prior, builder = int(_root(catalog)["relowner"]), snapshot_by_field["builder"]
    tables = {(prior, prior, privilege, False) for privilege in _RIGHTS}
    columns = set()
    if prior != builder:
        tables.add((builder, prior, "SELECT", False))
        columns = {(number, builder, prior, "INSERT", False) for number in (1, 2, 3)}
    _equal(set(map(tuple, catalog["table_acl"])) == tables)
    _equal(set(map(tuple, catalog["column_acl"])) == columns)


def _require_effective_access(snapshot, role_ids, rows):
    catalog = snapshot["ledger"]["catalog"]
    _equal(len(catalog["access"]) == len(role_ids) and {row["role"] for row in catalog["access"]} == role_ids)
    prior = int(_root(catalog)["relowner"])
    for row in catalog["access"]:
        _equal(all(type(row[key]) is bool for key in ("superuser", "builtin", "owner_usage", "builder_usage")))
        if row["superuser"] or row["builtin"]:
            continue
        owner_usage = row["owner_usage"] if prior == snapshot["owner"] else row["builder_usage"]
        rights = _RIGHTS if owner_usage else {"SELECT"} if row["builder_usage"] else set()
        _equal(set(row["table"]) == rights)
        _equal([number for number, _ in row["columns"]] == [1, 2, 3, 4, 5])
        for number, privileges in row["columns"]:
            columns = rights & {"INSERT", "SELECT", "UPDATE", "REFERENCES"}
            if row["builder_usage"] and number <= 3:
                columns = columns | {"INSERT"}
            _equal(set(privileges) == columns)
    functions = snapshot["functions"]
    expected_pairs = {
        (int(function["definition"]["oid"]), role) for function in functions.values() for role in role_ids
    }
    _equal(len(rows) == len(expected_pairs) and {(row["function_oid"], row["role"]) for row in rows} == expected_pairs)
    invoker = int(functions[FUNCTIONS[1]]["definition"]["oid"])
    for row in rows:
        _equal(all(type(row[key]) is bool for key in ("rolsuper", "owner_usage", "builder_usage", "can_execute")))
        _equal(
            row["can_execute"]
            is (row["function_oid"] == invoker or row["rolsuper"] or row["owner_usage"] or row["builder_usage"])
        )


def _require_function_shapes(function_rows, policy, namespace):
    from process import provider_directory_source_callable_custody as source
    from process.provider_directory_cms_native_inputs import _advance_body

    for function_row in function_rows:
        function_by_field = dict(function_row, definition=source._decoded(function_row["definition"]))
        definition = function_by_field["definition"]
        spec = (
            (),
            "trigger",
            (),
            "plpgsql",
            "v",
            False,
            "u",
            definition["proname"] == FUNCTIONS[0],
            source._body_digest(
                _advance_body(policy.schema) if definition["proname"] == FUNCTIONS[0] else _GUARD, policy.schema
            ),
        )
        source._require_definition(
            function_by_field, spec, policy.schema, namespace["oid"], source._TYPE_OIDS, namespace["identifier"]
        )


async def _capture_counter_catalog(connection, policy, roles, namespace, census):
    from process import provider_directory_source_callable_custody as source
    from process.provider_directory_cms_control_custody import _NATIVE_DEPENDENCIES_SQL, _TYPE_SQL
    from process.provider_directory_cms_receipt_custody import _CATALOG_SQL

    table = storage_relation(policy.schema, ROOT)
    catalog_sql = _CATALOG_SQL.replace(
        "'builder_reach',", "'builder_usage',pg_has_role(p.oid,$3::oid,'USAGE'),'builder_reach',"
    )
    catalog = source._decoded(await connection.fetchval(catalog_sql, table, roles["owner"], roles["builder"]))
    root = _require_layout(catalog)
    _equal(int(root["oid"]) == census["relation_oid"] and int(root["relnamespace"]) == namespace["oid"])
    _equal(int(root["relowner"]) in {roles["owner"], roles["builder"]})
    _equal(await connection.fetchval(_NATIVE_DEPENDENCIES_SQL, int(root["oid"])) is True)
    composite = source._decoded(await connection.fetchval(_TYPE_SQL, int(root["oid"])))
    _equal(composite["typtype"] == "c" and composite["typowner"] == root["relowner"])
    _equal(composite["oid"] == root["reltype"])
    _equal(int(composite["typrelid"]) == int(root["oid"]) and int(composite["typnamespace"]) == namespace["oid"])
    signatures = [storage_relation(policy.schema, name) + "()" for name in FUNCTIONS]
    function_rows = await connection.fetch(
        source._FUNCTION_SQL, signatures, policy.schema, list(FUNCTIONS), list(policy.runtime_roles)
    )
    _equal(len(function_rows) == 2 and {function_row["signature"] for function_row in function_rows} == set(signatures))
    functions_by_name = {
        source._decoded(function_row["definition"])["proname"]: dict(
            definition=source._decoded(function_row["definition"]), acl=source._decoded(function_row["acl"])
        )
        for function_row in function_rows
    }
    _equal(set(functions_by_name) == set(FUNCTIONS))
    _equal(
        sorted(int(function["definition"]["oid"]) for function in functions_by_name.values()) == census["function_oids"]
    )
    snapshot_by_field = dict(
        schema=policy.schema,
        owner=roles["owner"],
        builder=roles["builder"],
        namespace=dict(oid=namespace["oid"]),
        ledger=dict(catalog=catalog, composite=composite),
        functions=functions_by_name,
    )
    language = await connection.fetchval("SELECT oid FROM pg_language WHERE lanname='plpgsql'")
    _require_functions(snapshot_by_field, language)
    _require_acl(snapshot_by_field)
    _require_function_shapes(function_rows, policy, namespace)
    return snapshot_by_field


async def _counter_callable_capability(connection, policy, roles, namespace, role_graph):
    from process.provider_directory_cms_native_registration import (
        _native_registration_function,
        _native_registration_ledger,
    )

    _equal(connection.is_in_transaction() is True)
    census_by_field = dict(await connection.fetchrow(_CENSUS_SQL, namespace["oid"], list(FUNCTIONS)))
    if census_by_field["relation_oid"] is None and not census_by_field["function_oids"]:
        allowed = ()
    else:
        _equal(
            type(census_by_field["relation_oid"]) is int
            and 0 < census_by_field["relation_oid"] < 2**32
            and len(census_by_field["function_oids"]) == 2
            and len(set(census_by_field["function_oids"])) == 2
            and all(type(oid) is int and 0 < oid < 2**32 for oid in census_by_field["function_oids"])
        )
        snapshot = await _capture_counter_catalog(connection, policy, roles, namespace, census_by_field)
        ledger = await _native_registration_ledger(connection, policy.schema)
        function = await _native_registration_function(connection, policy.schema, ledger)
        advance = snapshot["functions"][FUNCTIONS[0]]["definition"]
        _equal(
            ledger["ledger_oid"] == census_by_field["relation_oid"] and ledger["owner_oid"] == int(advance["proowner"])
        )
        _equal(function["function_oid"] == int(advance["oid"]))
        access = await connection.fetch(_ACCESS_SQL, census_by_field["function_oids"], roles["builder"])
        role_ids = {int(role["oid"]) for role in role_graph["roles"]}
        _require_effective_access(snapshot, role_ids, access)
        allowed = (int(advance["oid"]),) if ledger["owner_oid"] == roles["owner"] else ()
        _equal(dict(await connection.fetchrow(_CENSUS_SQL, namespace["oid"], list(FUNCTIONS))) == census_by_field)
    _equal(connection.is_in_transaction() is True)
    return allowed
