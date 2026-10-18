# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody for one immutable, low-volume candidate coverage receipt."""

import hashlib
import json

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import HEADER, _require

COVERAGE_ROOT = "provider_directory_cms_candidate_coverage"
COVERAGE_COLUMNS = (
    ("dataset_id", "character varying(96)", True, None),
    ("endpoint_id", "character varying(96)", True, None),
    ("release_id", "character varying(64)", True, None),
    ("dataset_hash", "character varying(64)", True, None),
    ("proof_version", "smallint", True, None),
    ("admission_sha256", "character varying(64)", True, None),
    ("metadata_sha256", "character varying(64)", True, None),
    ("relationship_count", "bigint", True, None),
    ("created_at", "timestamp with time zone", True, None),
)
_CHECKS = {
    "CHECK (((release_id)::text ~ '^[0-9a-f]{64}$'::text))",
    "CHECK (((dataset_hash)::text ~ '^[0-9a-f]{64}$'::text))",
    "CHECK ((proof_version = 2))",
    "CHECK ((relationship_count >= 0))",
}
_GUARDS = {"cms_npd_candidate_coverage_insert": 7, "cms_npd_candidate_coverage_immutable": 58}
_GUARD_SQL = """
SELECT t.tgname,t.tgtype,t.tgenabled::text,t.tgnargs,t.tgqual IS NULL AS unconditional,
 t.tgoldtable,t.tgnewtable,t.tgargs,t.tgdeferrable,t.tginitdeferred,
 ARRAY(SELECT a.attname::text FROM unnest(t.tgattr::smallint[]) WITH ORDINALITY watched(attnum,ordinal)
 JOIN pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=watched.attnum
 WHERE NOT a.attisdropped ORDER BY watched.ordinal) AS update_columns,
 t.tgfoid::bigint,p.proname,p.proowner::bigint,r.rolname AS function_owner,p.prosecdef,p.proconfig,p.prosrc,
 p.pronargs,p.provolatile::text,n.nspname,l.lanname,p.prorettype='trigger'::regtype AS returns_trigger
FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_roles r ON r.oid=p.proowner
JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
WHERE t.tgrelid=$1::oid AND NOT t.tgisinternal ORDER BY t.tgname
"""
_NATIVE_SQL = """
SELECT NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=$1::oid)
 AND NOT EXISTS(SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
 WHERE a.attrelid=$1::oid AND a.attnum>0 AND NOT a.attisdropped
 AND (a.attgenerated<>'' OR a.attidentity<>'' OR a.attcollation<>t.typcollation))
 AND (SELECT count(*)=1 AND bool_and(k.confrelid=to_regclass($2) AND k.conkey=ARRAY[1]::smallint[]
 AND k.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=k.confrelid AND attname='dataset_id')]::smallint[]
 AND k.convalidated AND NOT k.condeferrable AND NOT k.condeferred
 AND k.confupdtype='a' AND k.confdeltype='a' AND k.confmatchtype='s')
 FROM pg_constraint k WHERE k.conrelid=$1::oid AND k.contype='f')
 AND (SELECT count(*)=2 AND bool_and(t.tgconstraint=k.oid AND t.tgenabled='O'
 AND p.pronamespace='pg_catalog'::regnamespace AND p.proname IN('RI_FKey_check_ins','RI_FKey_check_upd'))
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 JOIN pg_constraint k ON k.oid=t.tgconstraint WHERE t.tgrelid=$1::oid AND t.tgisinternal)
 AND NOT EXISTS(SELECT FROM pg_index i CROSS JOIN LATERAL
 unnest(i.indkey::smallint[],i.indclass::oid[],i.indcollation::oid[],i.indoption::smallint[])
 x(attnum,opclass_oid,collation_oid,flags) LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=x.attnum
 LEFT JOIN pg_opclass op ON op.oid=x.opclass_oid
 WHERE i.indrelid=$1::oid AND (NOT i.indimmediate OR i.indnkeyatts<>i.indnatts OR i.indisexclusion
 OR a.attnum IS NULL OR op.opcnamespace<>'pg_catalog'::regnamespace
 OR op.opcname<>CASE a.atttypid WHEN 'varchar'::regtype THEN 'text_ops'
 WHEN 'int2'::regtype THEN 'int2_ops' ELSE '' END OR x.flags<>0 OR x.collation_oid<>a.attcollation))
"""


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _require_layout(relation):
    _require(
        relation["relkind"] == "r"
        and relation["relpersistence"] == "p"
        and not relation["relispartition"]
        and not relation["parents"]
        and not relation["relrowsecurity"]
        and not relation["relforcerowsecurity"]
        and relation["native_checks"]
        and _decoded(relation["columns"]) == [list(column) for column in COVERAGE_COLUMNS],
        "coverage_layout_changed",
    )
    constraints = _decoded(relation["constraints"])
    _require(
        len(constraints) == 6
        and all(constraint["valid"] for constraint in constraints)
        and sorted(constraint["type"] for constraint in constraints) == ["c", "c", "c", "c", "f", "p"]
        and {
            _normalized_expression(constraint["definition"]) for constraint in constraints if constraint["type"] == "c"
        }
        == {_normalized_expression(definition) for definition in _CHECKS},
        "coverage_constraints_changed",
    )
    indexes = _decoded(relation["indexes"])
    _require(
        len(indexes) == 2
        and all(
            index["valid"] and index["method"] == "btree" and index["predicate"] is None and index["expression"] is None
            for index in indexes
        )
        and {(index["primary"], index["unique"], tuple(index["keys"])) for index in indexes}
        == {(True, True, ("dataset_id", "release_id", "proof_version")), (False, False, ("release_id",))},
        "coverage_indexes_changed",
    )


async def capture_source_coverage_control(connection, policy, *, legacy=False):
    """Attest the fixed receipt before any row read, lock, insert or admin adoption."""
    _require(type(policy) is GenericSourcePolicy, "coverage_policy_required")
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    qualified = storage_relation(policy.schema, COVERAGE_ROOT)
    relation_rows = await connection.fetch(CATALOG_SQL, [qualified], owner, readers)
    _require(
        len(relation_rows) == 1
        and relation_rows[0]["nspname"] == policy.schema
        and relation_rows[0]["relname"] == COVERAGE_ROOT,
        "coverage_missing",
    )
    relation = relation_rows[0]
    _require_layout(relation)
    if legacy:
        permitted = await connection.fetchval(
            "SELECT $1::oid=ANY(ARRAY(SELECT oid FROM pg_roles WHERE rolname=ANY($2::text[])))",
            relation["relowner"],
            [policy.owner_role, *policy.control_runtime_roles],
        )
        _require(permitted is True, "coverage_legacy_owner_changed")
    else:
        _require(
            relation["relowner"] == owner and relation["direct_acl_closed"] and relation["readers_closed"],
            "coverage_custody_changed",
        )
    _require(
        await connection.fetchval(_NATIVE_SQL, relation["oid"], storage_relation(policy.schema, HEADER)) is True,
        "coverage_native_contract_changed",
    )
    guards = await _guards(connection, policy, relation["oid"])
    acl = await connection.fetchval(
        "SELECT jsonb_build_object('table',c.relacl::text,'columns',"
        "(SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attacl::text) ORDER BY a.attnum) FROM pg_attribute a "
        "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)) FROM pg_class c WHERE c.oid=$1::oid",
        relation["oid"],
    )
    return {
        "oid": relation["oid"],
        "owner": relation["relowner"],
        "layout": _digest(_layout(relation)),
        "guards": _digest(guards),
        "acl": _digest(_decoded(acl)),
    }


def _layout(relation):
    return {field: _decoded(relation[field]) for field in ("columns", "constraints", "indexes")}


async def _guards(connection, policy, oid):
    from process.network_fhir_source_custody import _require_guard

    guards = await connection.fetch(_GUARD_SQL, oid)
    _require(len(guards) == 2 and {guard["tgname"] for guard in guards} == set(_GUARDS), "coverage_hooks_changed")
    for guard in guards:
        _require(
            guard["function_owner"] in (policy.owner_role, *policy.control_runtime_roles)
            and guard["provolatile"] == "v",
            "coverage_function_owner_changed",
        )
        _require_guard(guard, policy.schema, guard["proowner"], guard["tgname"], _GUARDS[guard["tgname"]])
    return [dict(guard) for guard in guards]
