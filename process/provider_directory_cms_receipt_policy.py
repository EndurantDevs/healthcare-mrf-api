# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native session-bound INSERT authority for one low-volume publication receipt."""

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_identifier

READ_POLICY = "cms_serving_receipt_read"
INSERT_POLICY = "cms_serving_receipt_legacy_insert"
POLICY_SQL = """SELECT jsonb_agg(jsonb_build_object('policy',to_jsonb(p),
 'using_expression',pg_get_expr(p.polqual,p.polrelid,true),
 'check_expression',pg_get_expr(p.polwithcheck,p.polrelid,true),
 'native_dependencies',(SELECT count(*)=1 AND bool_and(
 d.refclassid='pg_class'::regclass AND d.refobjid=p.polrelid AND d.refobjsubid=0
 AND d.objsubid=0 AND d.deptype='a') FROM pg_depend d
 WHERE d.classid='pg_policy'::regclass AND d.objid=p.oid)
 AND NOT EXISTS(SELECT FROM pg_shdepend d WHERE d.classid='pg_policy'::regclass AND d.objid=p.oid
 AND d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database())))
 ORDER BY p.oid) FROM pg_policy p WHERE p.polrelid=$1::oid"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_receipt_" + code)


def insert_expression(builder):
    """Use native session identities, never a role-membership permission test."""
    storage_identifier(builder)
    return f"SESSION_USER::text = '{builder}'::text AND CURRENT_USER::text = '{builder}'::text"


def policy_statements(table, builder):
    """Publish both exact policies in the caller's already-locked admin transaction."""
    return (
        f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
        f"CREATE POLICY {READ_POLICY} ON {table} FOR SELECT TO PUBLIC USING (true)",
        f"CREATE POLICY {INSERT_POLICY} ON {table} FOR INSERT TO PUBLIC WITH CHECK ({insert_expression(builder)})",
    )


def require_receipt_policies(catalog, root, builder):
    """Bind raw policy identity, deparsed expressions and pinned native dependencies."""
    proofs = catalog.get("receipt_policies")
    policies = catalog["policies"]
    _require(type(proofs) is list and type(policies) is list and len(proofs) == len(policies) == 2, "policies_changed")
    _require([proof["policy"] for proof in proofs] == policies, "policy_catalog_changed")
    _require({policy["polname"] for policy in policies} == {READ_POLICY, INSERT_POLICY}, "policies_changed")
    _require(len({policy["oid"] for policy in policies}) == 2, "policies_changed")
    for proof in proofs:
        _require_policy(proof, root, builder)


def _require_policy(proof, root, builder):
    policy = proof["policy"]
    is_read = policy["polname"] == READ_POLICY
    _require(
        set(proof) == {"policy", "using_expression", "check_expression", "native_dependencies"}
        and set(policy)
        == {"oid", "polname", "polrelid", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck"}
        and int(policy["oid"]) > 0
        and policy["polrelid"] == root["oid"]
        and policy["polcmd"] == ("r" if is_read else "a")
        and policy["polpermissive"] is True
        and policy["polroles"] == ["0"]
        and (policy["polqual"] is None) is (not is_read)
        and (policy["polwithcheck"] is None) is is_read
        and proof["native_dependencies"] is True
        and _normalized_expression(proof["using_expression"]) == ("true" if is_read else "")
        and _normalized_expression(proof["check_expression"])
        == ("" if is_read else _normalized_expression(insert_expression(builder))),
        "policy_changed",
    )


def require_bound_access(access_entries, principals, role_graph, *, legacy_builder=False):
    """Permit inherited intake rights only behind the exact original-session policy."""
    owner, publisher, builder, migration = principals
    migration_access = [entry for entry in access_entries if entry["role"] == migration]
    _require(
        len(migration_access) == 1
        and all(migration_access[0][key] is True for key in ("owner_reach", "owner_usage", "owner_set")),
        "migration_authority_changed",
    )
    roles_by_oid = {int(role["oid"]): role for role in role_graph["roles"]}
    builder_access = next(entry for entry in access_entries if entry["role"] == builder)
    _require_builder_access(builder_access, legacy_builder)
    for access in access_entries:
        if access["superuser"] or access["role"] in {owner, publisher, migration}:
            continue
        _require(not access["owner_reach"], "owner_authority")
        if access["builtin"]:
            continue
        if access["role"] == builder or access["builder_reach"]:
            _require_inherited_access(access, builder_access, roles_by_oid)
        else:
            _require(
                set(access["table"]) <= {"SELECT"}
                and all(set(rights) <= {"SELECT"} for _column, rights in access["columns"]),
                "inherited_writer",
            )


def _require_builder_access(access, legacy):
    if legacy:
        _require(
            "SELECT" in access["table"]
            and bool(set(access["table"]) & {"MAINTAIN", "UPDATE", "DELETE", "TRUNCATE"})
            and all("INSERT" in rights for column, rights in access["columns"] if column in {1, 2}),
            "legacy_writer_missing",
        )
    else:
        _require(
            set(access["table"]) == {"SELECT", "MAINTAIN"}
            and all(
                set(rights) == ({"SELECT", "INSERT"} if column in {1, 2} else {"SELECT"})
                for column, rights in access["columns"]
            ),
            "legacy_writer_changed",
        )


def _require_inherited_access(access, builder_access, roles_by_oid):
    builder_rights_by_column = {column: set(rights) for column, rights in builder_access["columns"]}
    has_insert = any("INSERT" in rights for _column, rights in access["columns"])
    _require(
        set(access["table"]) <= set(builder_access["table"])
        and all(set(rights) <= builder_rights_by_column[column] for column, rights in access["columns"])
        and (not has_insert or roles_by_oid[access["role"]]["rolbypassrls"] is False),
        "inherited_writer",
    )
