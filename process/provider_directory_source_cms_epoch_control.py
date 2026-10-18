# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody for the two fixed low-volume CMS epoch controls."""

from copy import deepcopy

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _close_relation_grants
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import HEADER, _require
from process.provider_directory_source_coverage_control import _GUARD_SQL, _NATIVE_SQL, _decoded, _digest

RELATIONSHIP_ROOT = "provider_directory_cms_npd_relationship_receipt"
SERVED_ROOT = "provider_directory_cms_serving_coverage"
EPOCH_CONTROL_ROOTS = (RELATIONSHIP_ROOT, SERVED_ROOT)
EPOCH_GUARD_FUNCTIONS = (
    "guard_cms_npd_relationship_receipt_insert",
    "guard_cms_npd_relationship_receipt_mutation",
    "guard_cms_npd_relationship_truncate",
    "guard_cms_npd_serving_coverage",
    "guard_cms_npd_covered_truncate",
)
_LAYOUTS = {
    RELATIONSHIP_ROOT: (
        ("dataset_id", "character varying(96)", True, None),
        ("release_id", "character varying(64)", True, None),
        ("projection_contract", "character varying(64)", True, None),
        ("relationship_count", "bigint", True, None),
    ),
    SERVED_ROOT: (
        ("dataset_id", "character varying(96)", True, None),
        ("release_id", "character varying(256)", True, None),
        ("dataset_hash", "character varying(64)", True, None),
        ("published_at", "timestamp without time zone", True, None),
        ("created_at", "timestamp with time zone", True, None),
        ("proof_version", "smallint", True, "1"),
    ),
}
_CHECKS = {
    RELATIONSHIP_ROOT: {
        "CHECK (((release_id)::text ~ '^[0-9a-f]{64}$'::text))": True,
        "CHECK (((projection_contract)::text = 'cms-npd-reference-ledger-v1'::text))": True,
        "CHECK ((relationship_count >= 0))": True,
    },
    SERVED_ROOT: {
        "CHECK ((proof_version = ANY (ARRAY[1, 2])))": True,
        "CHECK ((proof_version = 2)) NOT VALID": False,
    },
}
_KEYS = {RELATIONSHIP_ROOT: ("dataset_id",), SERVED_ROOT: ("dataset_id", "release_id", "proof_version")}
_ACL_SQL = """SELECT jsonb_build_object('table',COALESCE((SELECT jsonb_agg(
 jsonb_build_array(g.grantee::bigint,g.grantor::bigint,g.privilege_type,g.is_grantable)
 ORDER BY g.grantee,g.grantor,g.privilege_type,g.is_grantable)
 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) g),'[]'::jsonb),
 'columns',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attnum,g.grantee::bigint,
 g.grantor::bigint,g.privilege_type,g.is_grantable) ORDER BY a.attnum,g.grantee,g.grantor,g.privilege_type)
 FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) g WHERE a.attrelid=c.oid),'[]'::jsonb),
 'index_owners',(SELECT jsonb_agg(jsonb_build_array(i.indexrelid::bigint,idx.relowner::bigint)
 ORDER BY i.indexrelid) FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid=c.oid))
 FROM pg_class c WHERE c.oid=$1::oid"""
_OWNER_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}


def _require_layout(relation):
    root = relation["relname"]
    _require(
        relation["relkind"] == "r"
        and relation["relpersistence"] == "p"
        and not any(relation[key] for key in ("relispartition", "parents", "relrowsecurity", "relforcerowsecurity"))
        and relation["native_checks"]
        and _decoded(relation["columns"]) == [list(column) for column in _LAYOUTS[root]],
        "cms_epoch_control_layout_changed",
    )
    constraints = _decoded(relation["constraints"])
    checks_by_expression = {
        _normalized_expression(entry["definition"]): entry["valid"] for entry in constraints if entry["type"] == "c"
    }
    _require(
        len(constraints) == len(_CHECKS[root]) + 2
        and sorted(entry["type"] for entry in constraints) == sorted([*(["c"] * len(_CHECKS[root])), "f", "p"])
        and all(entry["valid"] for entry in constraints if entry["type"] != "c")
        and checks_by_expression == {_normalized_expression(sql): valid for sql, valid in _CHECKS[root].items()},
        "cms_epoch_control_constraints_changed",
    )
    indexes = _decoded(relation["indexes"])
    _require(
        len(indexes) == 1
        and indexes[0]["valid"]
        and indexes[0]["primary"]
        and indexes[0]["unique"]
        and indexes[0]["method"] == "btree"
        and indexes[0]["predicate"] is None
        and indexes[0]["expression"] is None
        and tuple(indexes[0]["keys"]) == _KEYS[root],
        "cms_epoch_control_indexes_changed",
    )


async def _capture_control(connection, policy, relation_by_field, owner, *, legacy):
    """Authenticate native structure and privileges before issuing a transport-stable catalog."""
    from process.network_fhir_source_custody import _GUARDS, _require_guard

    relation_by_field = dict(relation_by_field)
    for field in ("columns", "constraints", "indexes"):
        relation_by_field[field] = _decoded(relation_by_field[field])
    root = relation_by_field["relname"]
    _require_layout(relation_by_field)
    if legacy:
        permitted = await connection.fetchval(
            "SELECT $1::oid=ANY(ARRAY(SELECT oid FROM pg_roles WHERE rolname=ANY($2::text[])))",
            relation_by_field["relowner"],
            [policy.owner_role, *policy.control_runtime_roles],
        )
        _require(permitted is True, "cms_epoch_control_legacy_owner_changed")
    else:
        _require(
            relation_by_field["relowner"] == owner
            and relation_by_field["direct_acl_closed"]
            and relation_by_field["readers_closed"],
            "cms_epoch_control_custody_changed",
        )
    native_sql = _NATIVE_SQL.replace("k.confdeltype='a'", "k.confdeltype::text=$3::text")
    _require(
        await connection.fetchval(
            native_sql,
            relation_by_field["oid"],
            storage_relation(policy.schema, HEADER),
            "r" if root == SERVED_ROOT else "a",
        )
        is True,
        "cms_epoch_control_native_contract_changed",
    )
    guards = [dict(guard_record) for guard_record in await connection.fetch(_GUARD_SQL, relation_by_field["oid"])]
    _require(
        len(guards) == len(_GUARDS[root]) and {guard["tgname"] for guard in guards} == set(_GUARDS[root]),
        "cms_epoch_control_hooks_changed",
    )
    for guard in guards:
        allowed = (policy.owner_role, *policy.control_runtime_roles) if legacy else (policy.owner_role,)
        _require(
            guard["function_owner"] in allowed and guard["provolatile"] == "v",
            "cms_epoch_control_function_owner_changed",
        )
        _require_guard(guard, policy.schema, guard["proowner"], guard["tgname"], _GUARDS[root][guard["tgname"]])
    # Native FK identity/actions were checked above; deparser qualification varies with search_path.
    definition = f"FOREIGN KEY (dataset_id) REFERENCES {policy.schema}.{HEADER}(dataset_id)"
    if root == SERVED_ROOT:
        definition += " ON DELETE RESTRICT"
    relation_by_field["constraints"] = [
        dict(entry, definition=definition) if entry["type"] == "f" else entry
        for entry in relation_by_field["constraints"]
    ]
    return {
        "relation": dict(relation_by_field),
        "guards": guards,
        "acl": _decoded(await connection.fetchval(_ACL_SQL, relation_by_field["oid"])),
    }


async def capture_source_cms_epoch_controls(connection, policy, *, legacy=False):
    """Authenticate both controls before reading them; return native, not name-only, receipts."""
    _require(type(policy) is GenericSourcePolicy, "cms_epoch_control_policy_required")
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    rows = await connection.fetch(
        CATALOG_SQL, [storage_relation(policy.schema, root) for root in EPOCH_CONTROL_ROOTS], owner, readers
    )
    _require(
        len(rows) == 2
        and {row["relname"] for row in rows} == set(EPOCH_CONTROL_ROOTS)
        and all(row["nspname"] == policy.schema for row in rows),
        "cms_epoch_controls_missing",
    )
    return {row["relname"]: await _capture_control(connection, policy, row, owner, legacy=legacy) for row in rows}


def cms_epoch_control_receipts(controls):
    """Pin exact identities and catalog/guard/ACL digests in an issued input receipt."""
    return {
        root: {
            "oid": entry["relation"]["oid"],
            "owner": entry["relation"]["relowner"],
            "catalog": _digest(entry["relation"]),
            "guards": _digest(entry["guards"]),
            "acl": _digest(entry["acl"]),
        }
        for root, entry in controls.items()
    }


def require_cms_epoch_adoption_delta(before, after, owner, readers, owner_role):
    """Only fixed owner/ACL changes are admitted; no source rows or native structures change."""
    _require(before.keys() == after.keys() == set(EPOCH_CONTROL_ROOTS), "cms_epoch_adoption_roots_changed")
    for root, previous in before.items():
        current = after[root]
        expected = deepcopy(previous["relation"])
        expected.update(relowner=owner, direct_acl_closed=True, readers_closed=True)
        _require(current["relation"] == expected, "cms_epoch_adoption_catalog_changed")
        expected_guards = [dict(guard, proowner=owner, function_owner=owner_role) for guard in previous["guards"]]
        _require(current["guards"] == expected_guards, "cms_epoch_adoption_guards_changed")
        acl = current["acl"]
        expected_acl_entries = [[owner, owner, right, False] for right in _OWNER_RIGHTS]
        expected_acl_entries.extend([reader, owner, "SELECT", False] for reader in readers)
        _require(
            sorted(acl["table"]) == sorted(expected_acl_entries)
            and acl["columns"] == []
            and acl["index_owners"] == [[oid, owner] for oid, _ in previous["acl"]["index_owners"]],
            "cms_epoch_adoption_acl_changed",
        )


async def provision_source_cms_epoch_controls(connection, policy, *, verify_role_profile, verify_writer_closure):
    """Adopt the fixed controls and existing guard functions in one genuine administrator cut."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile)
        and callable(verify_writer_closure),
        "cms_epoch_explicit_admin_required",
    )
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
    )
    _require(
        actor["native_admin"] is True
        and actor["session"] == actor["current"]
        and actor["current"] not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "cms_epoch_independent_admin_required",
    )
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        await connection.execute("SET LOCAL lock_timeout='5s'")
        await connection.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(policy.schema, root) for root in EPOCH_CONTROL_ROOTS)
            + " IN ACCESS EXCLUSIVE MODE"
        )
        before = await capture_source_cms_epoch_controls(connection, policy, legacy=True)
        for root, entry in before.items():
            await _close_relation_grants(
                connection,
                {"nspname": policy.schema, "relname": root, "oid": entry["relation"]["oid"]},
                policy.owner_role,
                policy.runtime_roles,
            )
            columns = await connection.fetch(
                "SELECT DISTINCT a.attname,g.grantee,r.rolname FROM pg_attribute a "
                "CROSS JOIN LATERAL aclexplode(a.attacl) g LEFT JOIN pg_roles r ON r.oid=g.grantee "
                "WHERE a.attrelid=$1::oid ORDER BY a.attname,g.grantee",
                entry["relation"]["oid"],
            )
            for column in columns:
                principal = "PUBLIC" if column["grantee"] == 0 else storage_identifier(column["rolname"])
                await connection.execute(
                    f"REVOKE ALL PRIVILEGES ({storage_identifier(column['attname'])}) "
                    f"ON TABLE {storage_relation(policy.schema, root)} FROM {principal} CASCADE"
                )
        for name in EPOCH_GUARD_FUNCTIONS:
            await connection.execute(
                f"ALTER FUNCTION {storage_relation(policy.schema, name)}() "
                f"OWNER TO {storage_identifier(policy.owner_role)}"
            )
        owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
        after = await capture_source_cms_epoch_controls(connection, policy)
        require_cms_epoch_adoption_delta(before, after, owner, readers, policy.owner_role)
        await verify_writer_closure(connection, policy)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        return cms_epoch_control_receipts(after)
