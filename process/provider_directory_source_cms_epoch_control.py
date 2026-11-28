# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody for the two fixed low-volume CMS epoch controls."""

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import HEADER, _require
from process.provider_directory_source_coverage_control import _GUARD_SQL, _NATIVE_SQL, _decoded

RELATIONSHIP_ROOT = "provider_directory_cms_npd_relationship_receipt"
SERVED_ROOT = "provider_directory_cms_serving_coverage"
EPOCH_CONTROL_ROOTS = (RELATIONSHIP_ROOT, SERVED_ROOT)
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
        _require_guard(guard, policy.schema, guard["tgname"], _GUARDS[root][guard["tgname"]])
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
