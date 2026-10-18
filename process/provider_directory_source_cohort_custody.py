# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody for the separate immutable cohort-member snapshot family."""

import asyncio
import json
import re
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_callable_custody import _admin_identity, _decoded
from process.provider_directory_generic_source_policy import GenericSourcePolicy

HEADER = "provider_directory_uhc_flex_npi_cohort"
MEMBER = "provider_directory_uhc_flex_npi_member"
HISTORY = MEMBER + "_legacy"
_COHORT = re.compile(r"pdufc_[0-9a-f]{48}\Z")
_BOUND = "pd_uhc_flex_npi_legacy_scope"
_HEADER_GUARD = "guard_provider_directory_uhc_flex_npi_cohort"
_MEMBER_GUARD = "guard_provider_directory_uhc_flex_npi_member"
_INSERT_GUARD = "validate_provider_directory_uhc_flex_npi_members"
_RETIREMENT_GUARD = "guard_provider_directory_terminal_root_retirement_child"
_IDENTITY = "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid"


def _require(condition, code):
    if not condition:
        raise ValueError("source_cohort_" + code)


def _child_name(cohort_id):
    _require(type(cohort_id) is str and _COHORT.fullmatch(cohort_id) is not None, "identity_changed")
    return "pdufcm_" + cohort_id[6:]


def _header_hooks(legacy):
    return {
        "pd_uhc_flex_npi_cohort_guard": (_HEADER_GUARD, 31 if legacy else 27, (), None, ()),
        "pd_uhc_flex_npi_cohort_guard_truncate": (_HEADER_GUARD, 34, (), None, ()),
        "pd_trr_flex_cohort_row": (_RETIREMENT_GUARD, 31, (), None, ()),
        "pd_trr_flex_cohort_truncate": (_RETIREMENT_GUARD, 34, (), None, ()),
    }


def _history_hooks(legacy):
    return (
        {
            "pd_uhc_flex_npi_member_guard": (_MEMBER_GUARD, 27, (), None, ()),
            "pd_uhc_flex_npi_member_guard_truncate": (_MEMBER_GUARD, 34, (), None, ()),
            "pd_uhc_flex_npi_member_insert_guard": (_INSERT_GUARD, 4, (), "new_rows", ()),
        }
        if legacy
        else {}
    )


def _require_member_columns(catalog, native):
    columns = [row for row in catalog["attributes"] if row["attrelid"] == native["oid"]]
    _require(
        len(columns) == 3
        and [row["attname"] for row in columns] == ["cohort_id", "npi", "created_at"]
        and [row["atttypid"] for row in columns] == ["1043", "20", "1184"]
        and [row["atttypmod"] for row in columns] == [58, -1, -1]
        and [row["attcollation"] for row in columns] == ["100", "0", "0"]
        and all(
            row["attnotnull"] and not row["attisdropped"] and not row["attidentity"] and not row["attgenerated"]
            for row in columns
        )
        and [row["default_expression"] for row in columns] == ["", "", "transaction_timestamp()"],
        "columns_changed",
    )


def _require_member_indexes(catalog, native, name):
    indexes = [row for row in catalog["indexes"] or () if row["indrelid"] == native["oid"]]
    names = (
        ("pd_uhc_flex_npi_snapshot_pkey", "pd_uhc_flex_npi_snapshot_npi_idx")
        if name == MEMBER
        else (
            ("pd_uhc_flex_npi_member_pkey", "pd_uhc_flex_npi_member_npi_idx")
            if name == HISTORY
            else (name + "_pk", name + "_npi")
        )
    )
    by_name = {row["index_name"]: row for row in indexes}
    _require(len(indexes) == 2 and set(by_name) == set(names), "indexes_changed")
    for index_name, keys, unique in ((names[0], [1, 2], True), (names[1], [2, 1], False)):
        index = by_name[index_name]
        _require(
            index["indkey"] == keys
            and index["indisunique"] is unique
            and index["indisprimary"] is unique
            and index["native"]
            and index["method"] == "btree"
            and all(index[key] for key in ("indisvalid", "indisready", "indislive", "indimmediate"))
            and index["indnatts"] == index["indnkeyatts"] == 2
            and index["indoption"] == [0, 0]
            and index["index_expressions"] == index["index_predicate"] == ""
            and list(map(int, index["indcollation"])) == ([100, 0] if unique else [0, 100]),
            "indexes_changed",
        )


def _require_member_constraints(catalog, native, name, header_oid, *, legacy):
    from db.migration_expression_adoption import _normalized_expression

    constraints = [
        constraint_row for constraint_row in catalog["constraints"] or () if constraint_row["conrelid"] == native["oid"]
    ]
    _require(
        all(
            constraint_row["convalidated"]
            and constraint_row["conenforced"]
            and constraint_row["contype"] in {"p", "c", "n", "f"}
            for constraint_row in constraints
        ),
        "constraints_changed",
    )
    foreign_rows = [constraint_row for constraint_row in constraints if constraint_row["contype"] == "f"]
    if name == HISTORY and legacy:
        _require(
            len(foreign_rows) == 1
            and foreign_rows[0]["conname"] == "pd_uhc_flex_npi_member_cohort_fkey"
            and int(foreign_rows[0]["confrelid"]) == header_oid
            and foreign_rows[0]["conkey"] == foreign_rows[0]["confkey"] == [1]
            and foreign_rows[0]["condeferrable"]
            and foreign_rows[0]["condeferred"]
            and foreign_rows[0]["confdeltype"] == foreign_rows[0]["confupdtype"] == "a",
            "history_fk_changed",
        )
    else:
        _require(not foreign_rows, "member_foreign_key")
    check_by_name = {
        constraint_row["conname"]: _normalized_expression(constraint_row["constraint_definition"])
        for constraint_row in constraints
        if constraint_row["contype"] == "c"
    }
    expression = check_by_name.pop("pd_uhc_flex_npi_member_npi_check", "")
    _require(_simple_check(expression) == "checknpi>=1000000000andnpi<=2999999999", "range_check_changed")
    scope_name = _BOUND if name == HISTORY else name + "_scope" if name != MEMBER else None
    _require(set(check_by_name) == ({scope_name} if scope_name else set()), "scope_check_changed")


def _simple_check(expression):
    """Ignore only grouping and explicit built-in scalar casts in the fixed small CHECK grammar."""
    expression = re.sub(r"::(?:text|bigint|charactervarying)(?:\[\])?", "", expression)
    expression = re.sub(r"'([0-9]+)'", r"\1", expression)
    return expression.replace("(", "").replace(")", "")


def _require_scope_check(entry, cohorts):
    from db.migration_expression_adoption import _normalized_expression

    name = entry["name"]
    if name == MEMBER:
        return
    check_name = _BOUND if name == HISTORY else name + "_scope"
    actual = next(
        check["constraint_definition"] for check in entry["catalog"]["constraints"] if check["conname"] == check_name
    )
    actual = _simple_check(_normalized_expression(actual))
    if name != HISTORY:
        _require(len(cohorts) == 1 and actual == "checkcohort_id='" + cohorts[0] + "'", "scope_check_changed")
    elif not cohorts:
        _require(actual == "checkfalse", "history_scope_changed")
    else:
        comparison = (
            "cohort_id='" + cohorts[0] + "'"
            if len(cohorts) == 1
            else "cohort_id=anyarray[" + ",".join("'" + value + "'" for value in cohorts) + "]"
        )
        expected = "checkcohort_idisnotnulland" + comparison
        _require(actual == expected, "history_scope_changed")


async def _capture_member(connection, policy, name, roles, functions, header_oid, *, legacy):
    from process.provider_directory_cms_control_custody import _TYPE_SQL
    from process.provider_directory_cms_receipt_custody import _CATALOG_SQL
    from process.provider_directory_source_acquisition_custody import (
        _require_catalog_shape,
        _require_hooks,
        _require_relation_access,
    )

    catalog = _decoded(
        await connection.fetchval(_CATALOG_SQL, storage_relation(policy.schema, name), roles["owner"], roles["builder"])
    )
    _require(catalog and catalog["classes"], "member_missing")
    native = next(catalog_row for catalog_row in catalog["classes"] if catalog_row["relname"] == name)
    _require_catalog_shape(catalog, name, native, roles, legacy=legacy)
    _require(native["relkind"] == ("p" if name == MEMBER else "r"), "member_shape_changed")
    _require_member_columns(catalog, native)
    _require_member_indexes(catalog, native, name)
    _require_member_constraints(catalog, native, name, header_oid, legacy=legacy)
    _require_hooks(catalog, _history_hooks(legacy) if name == HISTORY else {}, functions)
    _require_relation_access(catalog, native, roles, legacy=legacy, root=MEMBER, history=True)
    from process.provider_directory_source_acquisition_custody import _EXPRESSION_DEPENDENCY_SQL

    dependencies = [
        dict(catalog_row) for catalog_row in await connection.fetch(_EXPRESSION_DEPENDENCY_SQL, int(native["oid"]))
    ]
    _require(
        all(
            catalog_row["operator_schema"] in {None, "pg_catalog"}
            and catalog_row["function_schema"] in {None, "pg_catalog"}
            for catalog_row in dependencies
        ),
        "expression_dependency_changed",
    )
    composite = _decoded(await connection.fetchval(_TYPE_SQL, int(native["oid"])))
    _require(composite["typowner"] == native["relowner"] and composite["typtype"] == "c", "rowtype_changed")
    return dict(name=name, root=MEMBER, catalog=catalog, composite=composite, dependencies=dependencies)


async def capture_cohort_family(connection, policy, callable_receipt, *, legacy=False):
    """Authenticate the separate cohort partition family and its exact immutable phase."""
    from process.provider_directory_source_acquisition_custody import (
        ACQUISITION_ROOTS,
        _capture_family_relation,
    )

    header = await _capture_family_relation(connection, policy, HEADER, HEADER, callable_receipt, legacy=legacy)
    native_header = next(
        catalog_row for catalog_row in header["catalog"]["classes"] if catalog_row["relname"] == HEADER
    )
    function_by_name = {entry["definition"]["proname"]: entry for entry in callable_receipt["functions"].values()}
    roles = callable_receipt["roles"]
    legacy = legacy and int(native_header["relowner"]) == roles["builder"]
    ids = [
        catalog_row["cohort_id"]
        for catalog_row in await connection.fetch(
            "SELECT cohort_id FROM " + storage_relation(policy.schema, HEADER) + ' ORDER BY cohort_id COLLATE "C"'
        )
    ]
    _require(all(_COHORT.fullmatch(cohort_id) for cohort_id in ids), "identity_changed")
    relation_by_name = {HEADER: header}
    for name in (MEMBER, HISTORY):
        relation_by_name[name] = await _capture_member(
            connection, policy, name, roles, function_by_name, int(native_header["oid"]), legacy=legacy
        )
        _require(
            next(
                catalog_row["relowner"]
                for catalog_row in relation_by_name[name]["catalog"]["classes"]
                if catalog_row["relname"] == name
            )
            == native_header["relowner"],
            "mixed_adoption_phase",
        )
    parent = next(
        catalog_row
        for catalog_row in relation_by_name[MEMBER]["catalog"]["classes"]
        if catalog_row["relname"] == MEMBER
    )
    partition = await connection.fetchrow(
        "SELECT partstrat::text AS kind,partattrs::smallint[] AS columns,partexprs::text AS expression "
        "FROM pg_partitioned_table WHERE partrelid=$1::oid",
        int(parent["oid"]),
    )
    _require(
        partition is not None and dict(partition) == dict(kind="l", columns=[1], expression=None), "partition_changed"
    )
    children = await _capture_cohort_children(connection, policy, callable_receipt, relation_by_name, ids, legacy)
    return dict(
        relations=relation_by_name,
        cohort_ids=ids,
        children=children,
        member_root=ACQUISITION_ROOTS[1],
        legacy=legacy,
    )


async def _capture_cohort_children(connection, policy, callable_receipt, relation_by_name, ids, legacy):
    from process.provider_directory_source_acquisition_custody import _partition_values

    parent = next(
        native_row for native_row in relation_by_name[MEMBER]["catalog"]["classes"] if native_row["relname"] == MEMBER
    )
    header = next(
        native_row for native_row in relation_by_name[HEADER]["catalog"]["classes"] if native_row["relname"] == HEADER
    )
    function_by_name = {entry["definition"]["proname"]: entry for entry in callable_receipt["functions"].values()}
    children = await connection.fetch(
        "SELECT c.relname AS name,c.oid::bigint AS oid,pg_get_expr(c.relpartbound,c.oid,true) AS bound "
        "FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent=$1::oid ORDER BY c.oid",
        int(parent["oid"]),
    )
    covered_ids = set()
    history_ids = []
    for child in children:
        scope_ids = _partition_values(child["bound"])
        _require(
            scope_ids is not None and set(scope_ids) <= set(ids) and not covered_ids.intersection(scope_ids),
            "partition_changed",
        )
        covered_ids.update(scope_ids)
        if child["name"] == HISTORY:
            history_ids = scope_ids
        if child["name"] != HISTORY:
            _require(len(scope_ids) == 1 and child["name"] == _child_name(scope_ids[0]), "child_changed")
            relation_by_name[child["name"]] = await _capture_member(
                connection,
                policy,
                child["name"],
                callable_receipt["roles"],
                function_by_name,
                int(header["oid"]),
                legacy=legacy,
            )
            _require_scope_check(relation_by_name[child["name"]], scope_ids)
    _require(covered_ids == set(ids), "historical_coverage_changed")
    if not ids:
        _require(
            await connection.fetchval(
                "SELECT NOT EXISTS(SELECT FROM " + storage_relation(policy.schema, HISTORY) + ")"
            ),
            "history_nonempty",
        )
    _require_scope_check(relation_by_name[HISTORY], history_ids)
    return [dict(catalog_row) for catalog_row in children]


async def retire_cohort_intake_guards(connection, policy, before):
    """Retire only the replaced bulk checks after native custody and complete parity have closed writes."""
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    _require(
        await connection.fetchval(
            """SELECT count(*)=3 AND bool_and(c.relowner=to_regrole($3))
        AND NOT EXISTS(SELECT FROM pg_class heap CROSS JOIN pg_roles ordinary
          WHERE heap.relnamespace=to_regnamespace($1) AND heap.relname=ANY($2::text[])
           AND ordinary.rolname=ANY($4::text[]) AND
           (has_table_privilege(ordinary.oid,heap.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
            OR EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=heap.oid AND a.attnum>0
                AND NOT a.attisdropped AND has_column_privilege(ordinary.oid,heap.oid,a.attnum,'INSERT,UPDATE,REFERENCES'))))
        FROM pg_class c WHERE c.relnamespace=to_regnamespace($1) AND c.relname=ANY($2::text[])""",
            policy.schema,
            [HEADER, MEMBER, HISTORY],
            policy.owner_role,
            list(policy.runtime_roles),
        )
        is True,
        "closed_custody_required",
    )
    await _require_cohort_parity(connection, policy)
    if not before["legacy"]:
        return
    table = storage_relation(policy.schema, HISTORY)
    for name in _history_hooks(True):
        await connection.execute("DROP TRIGGER " + storage_identifier(name) + " ON " + table)
    await connection.execute("ALTER TABLE " + table + " DROP CONSTRAINT pd_uhc_flex_npi_member_cohort_fkey")
    header = storage_relation(policy.schema, HEADER)
    await connection.execute("DROP TRIGGER pd_uhc_flex_npi_cohort_guard ON " + header)
    await connection.execute(
        "CREATE TRIGGER pd_uhc_flex_npi_cohort_guard BEFORE UPDATE OR DELETE ON "
        + header
        + " FOR EACH ROW EXECUTE FUNCTION "
        + storage_relation(policy.schema, _HEADER_GUARD)
        + "()"
    )
    await connection.execute("ALTER TABLE " + header + " ENABLE ALWAYS TRIGGER pd_uhc_flex_npi_cohort_guard")
    _require(
        asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
        "admin_transaction_changed",
    )


def require_cohort_adoption_delta(before, after, roles):
    """Permit only authenticated owner/ACL changes and the fixed obsolete bulk guard retirement."""
    from process.provider_directory_source_acquisition_custody import _require_family_relation_delta

    _require(
        before.keys() == after.keys() and before["relations"].keys() == after["relations"].keys(),
        "adoption_scope_changed",
    )
    expected = deepcopy(before)
    expected["legacy"] = False
    foreign_rows = [
        check for check in before["relations"][HISTORY]["catalog"]["constraints"] or () if check["contype"] == "f"
    ]
    foreign_oid = foreign_rows[0]["oid"] if foreign_rows else None
    for name, previous in expected["relations"].items():
        current = after["relations"][name]
        if name == HISTORY:
            previous["catalog"]["constraints"] = [
                check for check in previous["catalog"]["constraints"] if check["oid"] != foreign_oid
            ]
            previous["dependencies"] = [
                dependency for dependency in previous["dependencies"] if str(dependency["objid"]) != str(foreign_oid)
            ]
        if name in {HEADER, HISTORY}:
            previous["catalog"]["triggers"] = [
                hook
                for hook in previous["catalog"]["triggers"] or ()
                if hook["tgconstraint"] != foreign_oid and not (name == HISTORY and not hook["tgisinternal"])
            ] or None
        if name == HEADER:
            for hook in previous["catalog"]["triggers"]:
                if hook["tgname"] == "pd_uhc_flex_npi_cohort_guard":
                    actual = next(
                        hook_row for hook_row in current["catalog"]["triggers"] if hook_row["tgname"] == hook["tgname"]
                    )
                    hook["oid"], hook["tgtype"] = actual["oid"], 27
            previous["catalog"]["triggers"].sort(key=lambda hook: int(hook["oid"]))
        _require_family_relation_delta(previous, current, roles)
    expected["relations"] = after["relations"]
    _require(expected == after, "adoption_family_changed")


async def _require_cohort_parity(connection, policy):
    header_relation = storage_relation(policy.schema, HEADER)
    member_relation = storage_relation(policy.schema, MEMBER)
    source_relation = storage_relation(policy.schema, "provider_directory_dataset_resource")
    statement = f"""WITH expected AS MATERIALIZED(
        SELECT h.cohort_id,h.npi_count,h.practitioner_resource_count,count(r.*)::bigint AS resources,
         count(DISTINCT (r.payload_json::jsonb->>'npi')::bigint)::bigint AS npis
        FROM {header_relation} h LEFT JOIN {source_relation} r ON r.dataset_id=h.official_dataset_id
         AND r.resource_type='Practitioner' GROUP BY h.cohort_id,h.npi_count,h.practitioner_resource_count),
        actual AS MATERIALIZED(SELECT cohort_id,count(*)::bigint AS npis FROM {member_relation} GROUP BY cohort_id)
        SELECT NOT EXISTS(SELECT FROM expected e LEFT JOIN actual a USING(cohort_id)
          WHERE e.npi_count IS DISTINCT FROM a.npis OR e.npi_count IS DISTINCT FROM e.npis
             OR e.practitioner_resource_count IS DISTINCT FROM e.resources)
         AND NOT EXISTS(SELECT FROM {member_relation} m LEFT JOIN {header_relation} h USING(cohort_id) WHERE h.cohort_id IS NULL)
         AND NOT EXISTS(SELECT m.cohort_id,m.npi FROM {member_relation} m EXCEPT
             SELECT h.cohort_id,(r.payload_json::jsonb->>'npi')::bigint FROM {header_relation} h JOIN {source_relation} r
              ON r.dataset_id=h.official_dataset_id AND r.resource_type='Practitioner')
         AND NOT EXISTS(SELECT h.cohort_id,(r.payload_json::jsonb->>'npi')::bigint FROM {header_relation} h JOIN {source_relation} r
              ON r.dataset_id=h.official_dataset_id AND r.resource_type='Practitioner'
             EXCEPT SELECT m.cohort_id,m.npi FROM {member_relation} m)"""
    _require(await connection.fetchval(statement) is True, "historical_parity_changed")


async def capture_cohort_snapshot_custody(connection, policy, *, migration_role, namespace_owner_role, cohort_id=None):
    """Read-only native proof; the caller additionally binds its issued original task and producer intent."""
    from process.provider_directory_source_callable_custody import capture_source_callable_custody

    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction(), "policy_required")
    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_IDENTITY))
    callable_receipt = await capture_source_callable_custody(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role
    )
    family = await capture_cohort_family(connection, policy, callable_receipt)
    if cohort_id is not None:
        from process.provider_directory_source_acquisition_custody import _partition_values

        selected_child_rows = [
            native_row for native_row in family["children"] if cohort_id in _partition_values(native_row["bound"])
        ]
        _require(cohort_id in family["cohort_ids"] and len(selected_child_rows) == 1, "selected_child_missing")
        selected = selected_child_rows[0]
        await connection.execute(
            "LOCK TABLE "
            + storage_relation(policy.schema, MEMBER)
            + ","
            + storage_relation(policy.schema, selected["name"])
            + " IN ACCESS SHARE MODE NOWAIT"
        )
        oid = await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", storage_relation(policy.schema, selected["name"])
        )
        _require(
            oid == selected["oid"]
            and await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_locks "
                "WHERE pid=pg_backend_pid() AND locktype='relation' AND relation=$1::oid AND mode='AccessShareLock' AND granted)",
                oid,
            ),
            "selected_lock_changed",
        )
        family["cohort_ids"], family["children"] = [cohort_id], selected_child_rows
        family["relations"] = {
            name: proof
            for name, proof in family["relations"].items()
            if name in {HEADER, MEMBER, HISTORY, selected["name"]}
        }
        allowed_oids = {
            native_row["oid"] for proof in family["relations"].values() for native_row in proof["catalog"]["classes"]
        }
        for proof in family["relations"].values():
            if proof["catalog"]["inheritance"] is not None:
                proof["catalog"]["inheritance"] = [
                    native_row
                    for native_row in proof["catalog"]["inheritance"]
                    if native_row["inhrelid"] in allowed_oids and native_row["inhparent"] in allowed_oids
                ]
    _require(
        asyncio.current_task() is task and dict(await connection.fetchrow(_IDENTITY)) == identity, "transaction_changed"
    )
    return dict(
        namespace_roles=(migration_role, namespace_owner_role),
        proof=json.dumps(family, sort_keys=True, separators=(",", ":")),
    )
