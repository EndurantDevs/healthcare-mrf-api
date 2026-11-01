# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare native CMS preparation evidence; this module issues no authority.

The expected profile must eventually be loaded from a separately protected CMS
issuer record. Shared FHIR enrollment, task input and a detached handoff cannot
supply that provenance. Equality here is evidence, never a write capability.
"""

import asyncio
import hashlib
import json
import math
import re
from contextlib import asynccontextmanager
from dataclasses import asdict

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_provision import ISOLATED_FHIR_ROOTS
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

CONTRACT = "cms-preparation-custody-evidence-v1"
CANONICAL_ROOTS = (
    "provider_directory_endpoint_dataset",
    *TABLES[:4],
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_resource_identity",
    "provider_directory_organization_identity",
    "provider_directory_site_identity",
    "provider_directory_insurance_network_identity",
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
    "provider_directory_cms_npd_relationship_receipt",
    "provider_directory_cms_candidate_coverage",
)
_DIGEST_FIELDS = ("canonical_sha256", "private_sha256", "routines_sha256", "roles_sha256")
# Fixed source-reviewed PL/pgSQL callees; pg_depend does not record these calls.
# Keep serving callees until a separately enforced noncurrent boundary exists.
_CALLEE_FRONTIER = {
    "cms_serving_archive_result_matches": ("jsonb",),
    "cms_serving_native_matches": ("jsonb",),
    "cms_serving_native_snapshot": (),
    "cms_serving_oids": ("text[]",),
    "cms_serving_receipt_matches": ("jsonb",),
    "cms_serving_require_fresh": ("text", "jsonb"),
    "guard_pd_dataset_candidate_parent": (),
    "pd_dataset_candidate_tables": ("text",),
    "provider_directory_endpoint_dataset_admission_metadata_sha256": ("jsonb", "int2", "text", "text", "varchar[]"),
    "provider_directory_rooted_graph_dataset_intrinsic_valid": ("text",),
    "provider_directory_subset_abandonment_valid": ("text",),
    "provider_directory_subset_canonical_sha256": ("jsonb",),
    "provider_directory_subset_completion_canonical_json": ("jsonb",),
    "provider_directory_subset_completion_proof_pair_valid": ("jsonb", "text"),
    "provider_directory_subset_completion_proof_shape_valid": ("jsonb", "text", "int8"),
    "provider_directory_subset_content_proof_valid": (
        "jsonb",
        "text",
        "text",
        "text",
        "jsonb",
        "jsonb",
        "text",
        "int8",
        "jsonb",
        "jsonb",
    ),
    "provider_directory_subset_coverage_shape_valid": ("jsonb", "jsonb", "text", "text"),
    "provider_directory_subset_payload_canonical_json": ("jsonb",),
    "provider_directory_subset_payload_sha256": ("jsonb",),
    "provider_directory_subset_replay_evidence_shape_valid": ("jsonb", "text", "jsonb", "text"),
    "provider_directory_subset_terminal_disposition_v4_valid": ("text",),
    "provider_directory_subset_terminal_disposition_v5_valid": ("text",),
    "provider_directory_subset_terminal_disposition_valid": ("text",),
    "provider_directory_terminal_root_retirement_eligible": ("text", "int4"),
    "provider_directory_terminal_root_retirement_evidence": ("text",),
    "provider_directory_terminal_root_retirement_marker_valid": ("text", "jsonb"),
    "provider_directory_terminal_root_retirement_relation_evidence": ("text", "text"),
    "provider_directory_terminal_root_retirement_v2_eligible": ("text", "int4"),
    "provider_directory_terminal_root_retirement_v2_evidence": ("text",),
    "provider_directory_terminal_root_retirement_v2_marker_valid": ("text", "jsonb"),
    "provider_directory_uhc_flex_practitioner_dataset_valid": ("text",),
    "validate_pd_dataset_bulk_relationships": ("text", "text"),
}
_CALLEES_SQL = """SELECT expected.signature,p.oid::bigint,p.prokind='f' AS function,
 (SELECT count(*) FROM pg_catalog.pg_proc overloaded
  WHERE overloaded.pronamespace=p.pronamespace AND overloaded.proname=p.proname) AS overloads
 FROM unnest($1::text[]) WITH ORDINALITY expected(signature,position)
 LEFT JOIN pg_catalog.pg_proc p ON p.oid=pg_catalog.to_regprocedure(expected.signature)
 ORDER BY expected.position"""
_ACTOR_SQL = """SELECT session_user::text AS session,current_user::text AS current,
 pg_catalog.pg_backend_pid() AS backend,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 pg_catalog.current_setting('session_replication_role') AS replication,
 (SELECT virtualtransaction FROM pg_catalog.pg_locks
 WHERE pid=pg_catalog.pg_backend_pid() AND locktype='virtualxid' AND granted) AS transaction"""
_COORDINATES_SQL = """SELECT jsonb_build_object(
 'database_oid',(SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()),
 'catalog_schema_oid',c.oid::bigint,'storage_schema_oid',s.oid::bigint,'namespace_owner_oid',s.nspowner::bigint,
 'owner_oid',o.oid::bigint,'owner_login',o.rolcanlogin,'publisher_login',p.rolcanlogin)
 FROM pg_catalog.pg_namespace c,pg_catalog.pg_namespace s,pg_catalog.pg_roles o,pg_catalog.pg_roles p
 WHERE c.nspname=$1 AND s.nspname=$2 AND o.rolname=$3 AND p.rolname=$4"""
_CATALOG_SQL = """WITH RECURSIVE family(oid) AS (
 SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=ANY($2::text[])
 UNION SELECT child.inhrelid FROM pg_catalog.pg_inherits child JOIN family parent ON parent.oid=child.inhparent
)
SELECT c.oid::bigint,n.nspname,c.relname,c.relowner::bigint,c.relkind::text,c.relpersistence::text,
 jsonb_build_object(
 'namespace',jsonb_build_array(n.oid::bigint,n.nspname,n.nspowner::bigint,n.nspacl::text),
 'relation',jsonb_build_array(c.oid::bigint,c.relname,c.relkind::text,c.relpersistence::text,
  c.relowner::bigint,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,c.reloftype::bigint,
  c.reloptions,c.relacl::text,pg_catalog.pg_get_expr(c.relpartbound,c.oid),pg_catalog.pg_get_partkeydef(c.oid)),
 'parents',ARRAY(SELECT inhparent::bigint FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent),
 'columns',COALESCE((SELECT jsonb_agg(jsonb_build_object('attribute',to_jsonb(a)-'attstattarget',
  'default',to_jsonb(d),'type',to_jsonb(t),'collation',to_jsonb(coll)) ORDER BY a.attnum)
  FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
  LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=a.attcollation
  WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb),
 'constraints',COALESCE((SELECT jsonb_agg(jsonb_build_array(to_jsonb(k),pg_catalog.pg_get_constraintdef(k.oid))
  ORDER BY k.oid) FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid),'[]'::jsonb),
 'indexes',COALESCE((SELECT jsonb_agg(jsonb_build_object('index',to_jsonb(i),
  'class',jsonb_build_array(idx.oid::bigint,idx.relowner::bigint,idx.relacl::text,idx.reloptions),
  'definition',pg_catalog.pg_get_indexdef(i.indexrelid),
  'opclasses',ARRAY(SELECT to_jsonb(op) FROM unnest(i.indclass::oid[]) WITH ORDINALITY key(oid,n)
    JOIN pg_catalog.pg_opclass op ON op.oid=key.oid ORDER BY key.n)) ORDER BY i.indexrelid)
  FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
  WHERE i.indrelid=c.oid),'[]'::jsonb),
 'triggers',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_trigger t
  WHERE t.tgrelid=c.oid),'[]'::jsonb),
 'rules',COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.oid) FROM pg_catalog.pg_rewrite r
  WHERE r.ev_class=c.oid),'[]'::jsonb),
 'dependencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,
  d.refclassid,d.refobjid,d.refobjsubid,d.deptype) FROM pg_catalog.pg_depend d
  WHERE d.classid='pg_catalog.pg_class'::regclass AND d.objid=c.oid),'[]'::jsonb)
 ) AS native
FROM family f JOIN pg_catalog.pg_class c ON c.oid=f.oid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid"""
_ROUTINES_SQL = """WITH RECURSIVE objects(classid,objid) AS (
 SELECT 'pg_catalog.pg_trigger'::regclass,t.oid FROM pg_catalog.pg_trigger t WHERE t.tgrelid=ANY($1::oid[])
 UNION SELECT 'pg_catalog.pg_attrdef'::regclass,d.oid FROM pg_catalog.pg_attrdef d WHERE d.adrelid=ANY($1::oid[])
 UNION SELECT 'pg_catalog.pg_constraint'::regclass,k.oid FROM pg_catalog.pg_constraint k WHERE k.conrelid=ANY($1::oid[])
 UNION SELECT 'pg_catalog.pg_class'::regclass,i.indexrelid FROM pg_catalog.pg_index i WHERE i.indrelid=ANY($1::oid[])
), routines(oid) AS (
 SELECT t.tgfoid FROM pg_catalog.pg_trigger t WHERE t.tgrelid=ANY($1::oid[])
 UNION SELECT unnest($2::oid[])
 UNION SELECT d.refobjid FROM objects o JOIN pg_catalog.pg_depend d ON d.classid=o.classid AND d.objid=o.objid
 WHERE d.refclassid='pg_catalog.pg_proc'::regclass
 UNION SELECT d.refobjid FROM routines r JOIN pg_catalog.pg_depend d
 ON d.classid='pg_catalog.pg_proc'::regclass AND d.objid=r.oid
 WHERE d.refclassid='pg_catalog.pg_proc'::regclass
)
SELECT p.oid::bigint,jsonb_build_object('definition',to_jsonb(p),'language',to_jsonb(l),
 'namespace',jsonb_build_array(n.oid::bigint,n.nspname,n.nspowner::bigint,n.nspacl::text),
 'body',pg_catalog.pg_get_functiondef(p.oid),
 'dependencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.refclassid,d.refobjid,d.refobjsubid,d.deptype)
 FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_proc'::regclass AND d.objid=p.oid),'[]'::jsonb)) AS native
FROM routines r JOIN pg_catalog.pg_proc p ON p.oid=r.oid
JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace JOIN pg_catalog.pg_language l ON l.oid=p.prolang ORDER BY p.oid"""
_ROLES_SQL = """WITH acl_targets(oid) AS (
 SELECT c.relowner FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])
 UNION SELECT acl.grantee FROM pg_catalog.pg_class c,
 LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl
 WHERE c.oid=ANY($1::oid[]) AND acl.grantee<>0
 UNION SELECT acl.grantee FROM pg_catalog.pg_attribute a,
 LATERAL pg_catalog.aclexplode(a.attacl) acl WHERE a.attrelid=ANY($1::oid[]) AND acl.grantee<>0
 UNION SELECT n.nspowner FROM pg_catalog.pg_namespace n WHERE n.nspname=ANY($2::text[])
 UNION SELECT acl.grantee FROM pg_catalog.pg_namespace n,
 LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) acl
 WHERE n.nspname=ANY($2::text[]) AND acl.grantee<>0
 UNION SELECT p.proowner FROM pg_catalog.pg_proc p WHERE p.oid=ANY($3::oid[])
 UNION SELECT acl.grantee FROM pg_catalog.pg_proc p,
 LATERAL pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) acl
 WHERE p.oid=ANY($3::oid[]) AND acl.grantee<>0
), targets AS (
 SELECT oid FROM acl_targets UNION SELECT oid FROM pg_catalog.pg_roles WHERE rolname=ANY($4::text[])
 OR EXISTS(SELECT FROM pg_catalog.pg_class c,
 unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']) privilege
 WHERE c.oid=ANY($1::oid[]) AND pg_catalog.has_table_privilege(pg_roles.oid,c.oid,privilege)
 AND NOT EXISTS(SELECT FROM pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) acl
 WHERE acl.grantee=0 AND acl.privilege_type=privilege))
), connected AS (
 SELECT DISTINCT actor.oid FROM pg_catalog.pg_roles actor,targets target
 WHERE (actor.rolcanlogin OR actor.rolname=ANY($4::text[]))
 AND (pg_catalog.pg_has_role(actor.oid,target.oid,'SET') OR pg_catalog.pg_has_role(actor.oid,target.oid,'USAGE'))
), related AS (
 SELECT oid FROM targets UNION SELECT oid FROM connected
 UNION SELECT r.oid FROM pg_catalog.pg_roles r
 WHERE EXISTS(SELECT FROM connected actor,targets target
 WHERE (pg_catalog.pg_has_role(actor.oid,r.oid,'SET') OR pg_catalog.pg_has_role(actor.oid,r.oid,'USAGE'))
 AND (pg_catalog.pg_has_role(r.oid,target.oid,'SET') OR pg_catalog.pg_has_role(r.oid,target.oid,'USAGE')))
)
SELECT jsonb_build_object(
 'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY r.oid)
 FROM pg_catalog.pg_roles r JOIN related ON related.oid=r.oid),
 'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY m.roleid,m.member) FROM pg_catalog.pg_auth_members m
 WHERE m.roleid IN(SELECT oid FROM related) AND m.member IN(SELECT oid FROM related)),
 'reach',(SELECT jsonb_agg(jsonb_build_array(actor.oid::bigint,target.oid::bigint,
 pg_catalog.pg_has_role(actor.oid,target.oid,'SET'),pg_catalog.pg_has_role(actor.oid,target.oid,'USAGE'))
 ORDER BY actor.oid,target.oid) FROM connected actor,targets target),
 'relation_rights',(SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,c.oid::bigint,
 ARRAY(SELECT privilege FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']) privilege
 WHERE pg_catalog.has_table_privilege(r.oid,c.oid,privilege)),
 ARRAY(SELECT jsonb_build_array(a.attnum,privilege) FROM pg_catalog.pg_attribute a,
 unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) privilege WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 AND pg_catalog.has_column_privilege(r.oid,c.oid,a.attnum,privilege) ORDER BY a.attnum,privilege)) ORDER BY r.oid,c.oid)
 FROM related r,unnest($1::oid[]) c(oid)),
 'namespace_rights',(SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,n.name,
 pg_catalog.has_schema_privilege(r.oid,n.name,'USAGE'),pg_catalog.has_schema_privilege(r.oid,n.name,'CREATE'))
 ORDER BY r.oid,n.name) FROM related r,unnest($2::text[]) n(name)),
 'routine_rights',(SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,p.oid::bigint,
 pg_catalog.has_function_privilege(r.oid,p.oid,'EXECUTE')) ORDER BY r.oid,p.oid)
 FROM related r,unnest($3::oid[]) p(oid)))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_preparation_custody_" + code)


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _profile(profile, storage_binding, policy):
    """Validate before any catalog IO; an ordinary manifest is not a CMS profile."""
    fields = {"contract", "source_ids", "storage", "policy", *_DIGEST_FIELDS}
    _require(type(profile) is dict and set(profile) == fields, "cms_profile_required")
    _require(profile["contract"] == CONTRACT and profile["source_ids"] == ["cms-npd"], "cms_profile_required")
    _require(
        type(storage_binding) is FHIRSourceStorageBinding and type(policy) is GenericSourcePolicy,
        "explicit_coordinates_required",
    )
    _require(profile["storage"] == asdict(storage_binding), "storage_changed")
    expected_policy = json.loads(json.dumps(asdict(policy)))
    _require(profile["policy"] == expected_policy and policy.schema == storage_binding.storage_schema, "policy_changed")
    _require(
        all(type(profile[field]) is str and re.fullmatch(r"[0-9a-f]{64}", profile[field]) for field in _DIGEST_FIELDS),
        "digest_required",
    )
    return json.loads(json.dumps(profile, allow_nan=False))


async def _inventory(connection, schema, roots):
    rows = [dict(row) for row in await connection.fetch(_CATALOG_SQL, schema, list(roots))]
    _require({row["relname"] for row in rows if row["relname"] in roots} == set(roots), "roots_missing")
    _require(
        all(row["nspname"] == schema and row["relkind"] in {"r", "p"} and row["relpersistence"] == "p" for row in rows),
        "native_relation_changed",
    )
    return [{**row, "native": _decoded(row["native"])} for row in rows]


async def _callee_frontier(connection, schema):
    """Resolve only the fixed qualified overloads; this is no callable attestation."""
    signatures = [
        storage_relation(schema, name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        for name, arguments in _CALLEE_FRONTIER.items()
    ]
    rows = [dict(row) for row in await connection.fetch(_CALLEES_SQL, signatures)]
    _require(
        len(rows) == len(signatures)
        and [row["signature"] for row in rows] == signatures
        and all(row["oid"] is not None and row["function"] is True and row["overloads"] == 1 for row in rows)
        and len({row["oid"] for row in rows}) == len(signatures),
        "callees_missing_or_ambiguous",
    )
    return rows


_CALLABLE_OWNER_SQL = """SELECT count(*)=cardinality($1::oid[]) AND COALESCE(bool_and(
 ((p.pronamespace='pg_catalog'::regnamespace AND p.oid<16384 AND p.proowner=10 AND owner.rolsuper)
 OR (NOT (owner.rolcanlogin OR owner.rolsuper OR owner.rolcreatedb OR owner.rolcreaterole
      OR owner.rolreplication OR owner.rolbypassrls)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles elevated
     WHERE (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
            OR elevated.rolreplication OR elevated.rolbypassrls)
       AND pg_catalog.pg_has_role(owner.oid,elevated.oid,'MEMBER'))))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles actor
     WHERE actor.rolcanlogin AND NOT actor.rolsuper AND actor.rolname<>$2
       AND pg_catalog.pg_has_role(actor.oid,p.proowner,'MEMBER'))
 ),false)
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_roles owner ON owner.oid=p.proowner
 WHERE p.oid=ANY($1::oid[])"""

_ALLOCATION_CATALOGS = (
    "pg_proc",
    "pg_authid",
    "pg_auth_members",
    "pg_db_role_setting",
    "pg_event_trigger",
    "pg_language",
)
_ALLOCATION_ACTOR_SQL = _ACTOR_SQL.replace(
    " AS transaction",
    " AS transaction,(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) AS administrator,"
    "(SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=current_user) AS role_oid,"
    "current_setting('transaction_isolation') AS isolation,current_setting('transaction_read_only') AS readonly,"
    "current_setting('search_path') AS search_path",
)
_ALLOCATION_NAMESPACES_SQL = """SELECT oid::bigint,nspname,nspowner::bigint,nspacl::text
 FROM pg_catalog.pg_namespace WHERE oid=ANY($1::oid[]) ORDER BY oid FOR SHARE NOWAIT"""
_ALLOCATION_ADMISSION_SQL = """SELECT c.oid::bigint FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.oid"""
# Allocation reads/row-locks admission rows; only their DML triggers are outside its executed closure.
_ALLOCATION_ROUTINES_SQL = _ROUTINES_SQL.replace(
    "t.tgrelid=ANY($1::oid[])", "t.tgrelid=ANY($1::oid[]) AND t.tgrelid<>ALL($3::oid[])"
)
_ALLOCATION_ROOTS_SQL = """SELECT count(*)=5 AND bool_and(c.relpersistence='p'
 AND c.relkind=CASE WHEN c.relname='provider_directory_endpoint_dataset' THEN 'r'::"char" ELSE 'p'::"char" END)
 AND (SELECT count(*)=1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
 WHERE t.tgrelid=pg_catalog.to_regclass(pg_catalog.format('%I.provider_directory_endpoint_dataset',$2::text))
 AND p.pronamespace=$2::regnamespace AND p.proname='prepare_pd_generic_dataset_storage'
 AND t.tgname='pd_dataset_storage' AND t.tgtype=5 AND t.tgenabled IN ('O','A')
 AND t.tgnargs=0 AND t.tgqual IS NULL AND NOT t.tgisinternal AND t.tgattr=''::pg_catalog.int2vector)
 FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[]) AND c.relnamespace=$2::regnamespace
 AND c.relname=ANY($3::text[])"""
_ALLOCATION_LOCKS_SQL = """SELECT count(*)=cardinality($1::oid[]) AND bool_and(EXISTS(
 SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid() AND l.locktype='relation'
 AND l.database=CASE WHEN c.relisshared THEN 0::oid ELSE
 (SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()) END
 AND l.relation=c.oid AND l.granted AND l.mode=ANY($2::text[]))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
 AND l.locktype='relation' AND l.relation=c.oid AND l.granted
 AND l.mode IN ('ExclusiveLock','AccessExclusiveLock')))
 FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])"""
_ALLOCATION_DEPENDENCIES_SQL = """WITH objects(classid,objid) AS (
 SELECT 'pg_catalog.pg_proc'::regclass,unnest($1::oid[])
 UNION SELECT 'pg_catalog.pg_class'::regclass,unnest($2::oid[])
 UNION SELECT 'pg_catalog.pg_trigger'::regclass,oid FROM pg_catalog.pg_trigger
 WHERE tgrelid=ANY($2::oid[]) AND tgrelid<>ALL($4::oid[])
 UNION SELECT 'pg_catalog.pg_attrdef'::regclass,oid FROM pg_catalog.pg_attrdef WHERE adrelid=ANY($2::oid[])
 UNION SELECT 'pg_catalog.pg_constraint'::regclass,oid FROM pg_catalog.pg_constraint WHERE conrelid=ANY($2::oid[])
 UNION SELECT 'pg_catalog.pg_class'::regclass,indexrelid FROM pg_catalog.pg_index WHERE indrelid=ANY($2::oid[])
), dependencies AS (
 SELECT d.* FROM pg_catalog.pg_depend d JOIN objects o ON d.classid=o.classid AND d.objid=o.objid
)
SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_event_trigger WHERE evtenabled<>'D')
 AND EXISTS(SELECT FROM pg_catalog.pg_roles WHERE oid=10 AND rolsuper)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolcanlogin AND NOT rolsuper
 AND pg_catalog.pg_has_role(oid,10::oid,'MEMBER'))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE NOT rolsuper
 AND pg_catalog.has_schema_privilege(oid,11::oid,'CREATE'))
 AND NOT EXISTS(SELECT FROM dependencies d WHERE NOT (
 (d.refclassid='pg_catalog.pg_proc'::regclass AND d.refobjid=ANY($1::oid[]))
 OR (d.refclassid='pg_catalog.pg_class'::regclass AND (d.refobjid=ANY($2::oid[])
 OR EXISTS(SELECT FROM pg_catalog.pg_index WHERE indexrelid=d.refobjid AND indrelid=ANY($2::oid[]))))
 OR (d.refclassid='pg_catalog.pg_constraint'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_constraint k
 WHERE k.oid=d.refobjid AND k.conrelid=ANY($2::oid[]) AND k.contype IN ('f','p','u','c','n')))
 OR (d.classid='pg_catalog.pg_constraint'::regclass AND d.refclassid='pg_catalog.pg_trigger'::regclass
 AND d.deptype='i' AND d.objsubid=0 AND d.refobjsubid=0
 AND EXISTS(SELECT FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_trigger t ON t.tgconstraint=k.oid
 WHERE k.oid=d.objid AND t.oid=d.refobjid AND k.contype='t' AND k.conrelid=t.tgrelid
 AND t.tgrelid=ANY($2::oid[]) AND (t.tgrelid=ANY($4::oid[]) OR t.tgfoid=ANY($1::oid[]))))
 OR (d.classid='pg_catalog.pg_trigger'::regclass AND d.refclassid='pg_catalog.pg_constraint'::regclass
 AND d.deptype='i' AND d.objsubid=0 AND d.refobjsubid=0
 AND EXISTS(SELECT FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_constraint k ON k.oid=t.tgconstraint
 JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
 WHERE t.oid=d.objid AND k.oid=d.refobjid AND k.contype='f' AND k.confrelid=t.tgrelid
 AND t.tgrelid=ANY($2::oid[]) AND t.tgconstrrelid=k.conrelid AND t.tgconstrindid=k.conindid
 AND t.tgisinternal AND t.tgtype IN (9,17) AND t.tgenabled IN ('O','A')
 AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgattr=''::pg_catalog.int2vector
 AND t.tgfoid=ANY($1::oid[]) AND p.pronamespace=11))
 OR (d.refclassid='pg_catalog.pg_namespace'::regclass AND d.refobjid=ANY($3::oid[]))
 OR (d.refclassid='pg_catalog.pg_language'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_language l
 WHERE l.oid=d.refobjid AND l.lanowner=10 AND l.lanname IN ('sql','plpgsql') AND l.lanpltrusted))
 OR (d.refclassid='pg_catalog.pg_type'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_type t
 WHERE t.oid=d.refobjid AND t.oid<16384 AND t.typowner=10 AND t.typnamespace=11 AND t.typtype IN ('b','p')))
 OR (d.refclassid='pg_catalog.pg_collation'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_collation c
 WHERE c.oid=d.refobjid AND c.collowner=10 AND c.collnamespace=11))
 OR (d.refclassid='pg_catalog.pg_opclass'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_opclass o
 WHERE o.oid=d.refobjid AND o.oid<16384 AND o.opcowner=10 AND o.opcnamespace=11))
 OR (d.refclassid='pg_catalog.pg_operator'::regclass AND EXISTS(SELECT FROM pg_catalog.pg_operator o
 WHERE o.oid=d.refobjid AND o.oid<16384 AND o.oprowner=10 AND o.oprnamespace=11))
 ))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 WHERE p.oid=ANY($1::oid[]) AND NOT (l.lanowner=10 AND
 ((l.lanpltrusted AND l.lanname IN ('sql','plpgsql')) OR
 (p.oid<16384 AND p.proowner=10 AND p.pronamespace=11 AND l.lanname='internal'))))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_type t WHERE t.oid IN (
 SELECT unnest(p.proargtypes::oid[]) FROM pg_catalog.pg_proc p WHERE p.oid=ANY($1::oid[])
 UNION SELECT p.prorettype FROM pg_catalog.pg_proc p WHERE p.oid=ANY($1::oid[])
 UNION SELECT a.atttypid FROM pg_catalog.pg_attribute a WHERE a.attrelid=ANY($2::oid[]) AND a.attnum>0)
 AND (t.oid>=16384 OR t.typowner<>10 OR t.typnamespace<>11 OR t.typtype NOT IN ('b','p')))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_partitioned_table WHERE partrelid=ANY($2::oid[]) AND partdefid<>0)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_language l WHERE l.oid IN
 (SELECT prolang FROM pg_catalog.pg_proc WHERE oid=ANY($1::oid[])) AND NOT (
 (l.lanname IN ('sql','internal') AND NOT l.lanispl AND l.lanplcallfoid=0 AND l.laninline=0
 AND l.lanvalidator=CASE l.lanname WHEN 'sql' THEN 2248 ELSE 2246 END)
 OR (l.lanname='plpgsql' AND l.lanispl AND NOT EXISTS(
 SELECT FROM (VALUES(l.lanplcallfoid,'plpgsql_call_handler'),(l.laninline,'plpgsql_inline_handler'),
 (l.lanvalidator,'plpgsql_validator')) h(oid,name) LEFT JOIN pg_catalog.pg_proc p ON p.oid=h.oid
 WHERE p.oid IS NULL OR p.oid>=16384 OR p.proowner<>10 OR p.pronamespace<>11 OR p.prolang<>13
 OR p.prosrc<>h.name OR p.probin IS DISTINCT FROM '$libdir/plpgsql'))))
"""


def _allocation_coordinates(expected_pins, source_metadata, catalog_schema, preparation_schema, relation_oids):
    _require(
        type(expected_pins) is dict
        and set(expected_pins) == {"functions", "namespaces", "roles"}
        and type(expected_pins["functions"]) is dict
        and type(expected_pins["namespaces"]) is list
        and type(expected_pins["roles"]) is dict,
        "allocation_pins_required",
    )
    _require(
        set(expected_pins["roles"]) == {"roles", "memberships"}
        and type(expected_pins["roles"]["roles"]) is list
        and type(expected_pins["roles"]["memberships"]) is list,
        "allocation_related_roles_required",
    )
    pins_by_kind = json.loads(json.dumps({**expected_pins, "metadata": source_metadata}, allow_nan=False))
    _require(
        "prepare_pd_generic_dataset_storage" in pins_by_kind["functions"], "allocation_original_allocator_required"
    )
    relations = list(relation_oids)
    _require(
        relations
        and all(type(oid) is int and 0 < oid < 2**32 for oid in relations)
        and len(set(relations)) == len(relations),
        "allocation_relations_required",
    )
    _require(
        type(pins_by_kind["namespaces"]) is list
        and {namespace["nspname"] for namespace in pins_by_kind["namespaces"]}
        == {"pg_catalog", catalog_schema, preparation_schema},
        "allocation_namespaces_required",
    )
    namespace_oids = [namespace["oid"] for namespace in pins_by_kind["namespaces"]]
    catalog_oid = next(
        namespace["oid"] for namespace in pins_by_kind["namespaces"] if namespace["nspname"] == catalog_schema
    )
    return pins_by_kind, relations, namespace_oids, catalog_oid


def _require_allocation_actor(connection, actor_by_field, pins_by_kind):
    _require(
        actor_by_field["administrator"] is True
        and actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["role_oid"]
        not in {int(entry["definition"]["proowner"]) for entry in pins_by_kind["metadata"].values()}
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["transaction"]
        and actor_by_field["replication"] == "origin"
        and actor_by_field["isolation"] == "read committed"
        and actor_by_field["readonly"] == "off"
        and actor_by_field["search_path"] == "pg_catalog, pg_temp",
        "allocation_original_admin_required",
    )


async def _verify_allocation_relations(connection, actor_by_field, task, catalog_oids, relations, catalog_schema):
    _require(
        asyncio.current_task() is task
        and connection.is_in_transaction() is True
        and not connection.is_closed()
        and dict(await connection.fetchrow(_ALLOCATION_ACTOR_SQL)) == actor_by_field,
        "allocation_original_admin_changed",
    )
    _require(
        await connection.fetchval(_ALLOCATION_LOCKS_SQL, catalog_oids, ["ShareLock"]) is True
        and await connection.fetchval(
            _ALLOCATION_LOCKS_SQL, relations, ["ShareUpdateExclusiveLock", "ShareRowExclusiveLock"]
        )
        is True,
        "allocation_native_locks_required",
    )
    _require(
        await connection.fetchval(
            _ALLOCATION_ROOTS_SQL,
            relations,
            catalog_schema,
            ["provider_directory_endpoint_dataset", *TABLES[:4]],
        )
        is True,
        "allocation_original_roots_required",
    )


async def _allocation_role_snapshot(connection, expected_by_kind, actor_by_field):
    from process import provider_directory_cms_callable_custody as callables

    snapshot_by_kind = _decoded(await connection.fetchval(callables._ROLE_GRAPH_SQL))
    related_oids = {role["oid"] for role in expected_by_kind["roles"]}
    _require(
        [role for role in snapshot_by_kind["roles"] if role["oid"] in related_oids] == expected_by_kind["roles"]
        and [
            membership
            for membership in snapshot_by_kind["memberships"] or []
            if membership["roleid"] in related_oids and membership["member"] in related_oids
        ]
        == expected_by_kind["memberships"],
        "allocation_roles_changed",
    )
    snapshot_by_kind["settings"] = [
        dict(setting)
        for setting in await connection.fetch(
            "SELECT setdatabase::bigint,setrole::bigint,setconfig FROM pg_catalog.pg_db_role_setting ORDER BY setdatabase,setrole"
        )
    ]
    _require(
        not any(
            setting["setdatabase"] in {0, actor_by_field["database_oid"]}
            and setting["setrole"] in {0, actor_by_field["role_oid"], *related_oids}
            for setting in snapshot_by_kind["settings"]
        ),
        "allocation_applicable_role_settings_unbound",
    )
    return snapshot_by_kind


async def _verify_allocation_namespaces(connection, pins_by_kind, namespace_oids):
    namespaces = [dict(namespace) for namespace in await connection.fetch(_ALLOCATION_NAMESPACES_SQL, namespace_oids)]
    _require(
        namespaces == sorted(pins_by_kind["namespaces"], key=lambda namespace: namespace["oid"]),
        "allocation_namespace_changed",
    )


async def _require_allocation_resolution(connection):
    # SHOW resolves no caller-controlled functions, operators, types or relations.
    # An omitted pg_temp searches temp relations BEFORE pg_catalog implicitly.
    _require(await connection.fetchval("SHOW search_path") == "pg_catalog, pg_temp", "allocation_resolution_required")


async def _lock_allocation_catalogs(connection):
    await _require_allocation_resolution(connection)
    await connection.execute(
        "LOCK TABLE " + ",".join("pg_catalog." + name for name in _ALLOCATION_CATALOGS) + " IN SHARE MODE NOWAIT"
    )
    return await connection.fetchval(
        "SELECT array_agg(oid ORDER BY oid) FROM pg_catalog.pg_class WHERE relnamespace=11 AND relname=ANY($1::text[])",
        list(_ALLOCATION_CATALOGS),
    )


async def _allocation_routines(connection, catalog_schema, relations):
    from process.provider_directory_source_storage_binding import _CATALOG_ROOTS

    admission_rows = await connection.fetch(_ALLOCATION_ADMISSION_SQL, catalog_schema, sorted(_CATALOG_ROOTS))
    admission_oids = [row["oid"] for row in admission_rows]
    _require(
        len(admission_oids) == len(set(admission_oids)) == len(_CATALOG_ROOTS)
        and set(admission_oids) <= set(relations),
        "allocation_admission_required",
    )
    frontier = await _callee_frontier(connection, catalog_schema)
    routines = await connection.fetch(
        _ALLOCATION_ROUTINES_SQL, relations, [callee["oid"] for callee in frontier], admission_oids
    )
    return routines, admission_oids


async def _verify_allocation_callees(
    connection, catalog_schema, preparation_schema, relations, namespace_oids, catalog_oid, pins_by_kind
):
    from process import provider_directory_cms_callable_custody as callables

    signatures = list(callables.cms_callee_signatures(catalog_schema))
    routines, admission_oids = await _allocation_routines(connection, catalog_schema, relations)
    oids = [routine["oid"] for routine in routines]
    native_oids = {
        routine["oid"] for routine in routines if int(_decoded(routine["native"])["definition"]["pronamespace"]) == 11
    }
    if native_oids:
        from process.provider_directory_cms_control_custody import _require_native_foreign_functions

        await _require_native_foreign_functions(connection, native_oids)
    _require(
        oids
        and await connection.fetchval(_ALLOCATION_DEPENDENCIES_SQL, oids, relations, namespace_oids, admission_oids)
        is True,
        "allocation_dependencies_unaccounted",
    )
    function_rows = await connection.fetch(
        callables._FUNCTION_SQL, signatures, catalog_schema, list(pins_by_kind["functions"]), []
    )
    verified = callables.require_original_cms_allocation_callees(
        function_rows,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_oid,
        preparation_schema=preparation_schema,
        expected_functions=pins_by_kind["functions"],
        source_metadata=pins_by_kind["metadata"],
    )
    _require(
        {int(function["definition"]["oid"]) for function in verified.values()} == set(oids) - native_oids,
        "allocation_callable_closure_changed",
    )
    return verified


@asynccontextmanager
async def freeze_cms_allocation_callables(
    connection, *, catalog_schema, preparation_schema, relation_oids, expected_pins, source_metadata, deadline
):
    """Freeze authenticated original callables for one short empty allocation.

    The caller authenticates callable/related-role pins and qualifies namespace
    identities through existing protected records. Unrelated roles/settings are
    internal stability observations only. The caller holds relation/claim custody
    and owns one outer deadline through COMMIT or ROLLBACK. This context
    never commits, releases a claim, issues a finisher, or releases transaction
    locks on exit. Never enter it in a transaction retained for payload COPY.
    Source admission tables may only be read or row-locked throughout the caller's transaction.
    Existing canonical rows cannot be updated/deleted or have pending deferred events:
    external parent-side RI hooks are metadata only in this fixed INSERT-only path.
    """
    loop = asyncio.get_running_loop()
    _require(
        type(deadline) in {float, int} and math.isfinite(deadline) and 0 < deadline - loop.time() <= 3,
        "allocation_deadline_required",
    )
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    pins_by_kind, relations, namespace_oids, catalog_oid = _allocation_coordinates(
        expected_pins, source_metadata, catalog_schema, preparation_schema, relation_oids
    )
    task = asyncio.current_task()
    async with asyncio.timeout_at(deadline):
        await _require_allocation_resolution(connection)
        actor_by_field = dict(await connection.fetchrow(_ALLOCATION_ACTOR_SQL))
        _require_allocation_actor(connection, actor_by_field, pins_by_kind)
        catalog_oids = await _lock_allocation_catalogs(connection)
        stability_by_kind = await _allocation_role_snapshot(connection, pins_by_kind["roles"], actor_by_field)

        async def _verify():
            await _require_allocation_resolution(connection)
            await _verify_allocation_relations(
                connection, actor_by_field, task, catalog_oids, relations, catalog_schema
            )
            await _verify_allocation_namespaces(connection, pins_by_kind, namespace_oids)
            _require(
                await _allocation_role_snapshot(connection, pins_by_kind["roles"], actor_by_field) == stability_by_kind,
                "allocation_role_stability_changed",
            )
            return await _verify_allocation_callees(
                connection, catalog_schema, preparation_schema, relations, namespace_oids, catalog_oid, pins_by_kind
            )

        await _verify()
        yield
        await _verify()


async def require_cms_canonical_callable_owners(connection, *, catalog_schema, migration_role):
    """Reject runtime replacement authority over required canonical callables.

    The caller supplies the migration identity from its genuine Source manifest;
    only that bootstrap login and native superusers remain trusted maintainers.
    This checks ownership reach, not complete callable semantics or writer custody.
    It grants no rights and installs no function, guard or authority.
    """
    from db.plan_release_storage import storage_identifier

    storage_identifier(migration_role)
    _require(connection.is_in_transaction() is True, "transaction_required")
    canonical = await _inventory(connection, catalog_schema, CANONICAL_ROOTS)
    frontier = await _callee_frontier(connection, catalog_schema)
    routines = await connection.fetch(
        _ROUTINES_SQL, [entry["oid"] for entry in canonical], [entry["oid"] for entry in frontier]
    )
    required_oids = [entry["oid"] for entry in routines]
    _require(
        required_oids and await connection.fetchval(_CALLABLE_OWNER_SQL, required_oids, migration_role) is True,
        "callable_owner_unprotected",
    )
    return required_oids


async def _collect(connection, storage_binding, policy):
    """Collect the unchanged v1 evidence before hashing or authenticated projection."""
    coordinates = _decoded(
        await connection.fetchval(
            _COORDINATES_SQL,
            storage_binding.catalog_schema,
            storage_binding.storage_schema,
            policy.owner_role,
            policy.publisher_role,
        )
    )
    native_fields = ("database_oid", "catalog_schema_oid", "storage_schema_oid", "namespace_owner_oid")
    _require(
        type(coordinates) is dict
        and all(coordinates[field] == getattr(storage_binding, field) for field in native_fields)
        and coordinates["owner_oid"] != storage_binding.owner_oid
        and coordinates["owner_login"] is False
        and coordinates["publisher_login"] is True,
        "protected_coordinates_changed",
    )
    canonical = await _inventory(connection, storage_binding.catalog_schema, CANONICAL_ROOTS)
    private = await _inventory(connection, storage_binding.storage_schema, ISOLATED_FHIR_ROOTS)
    relations = sorted(storage_relation(native["nspname"], native["relname"]) for native in (*canonical, *private))
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + relation for relation in relations) + " IN ACCESS SHARE MODE NOWAIT"
    )
    # Pin existing relations against replacement; these locks do not fence new
    # descendants or role changes and this observation grants no lasting custody.
    locked_canonical = await _inventory(connection, storage_binding.catalog_schema, CANONICAL_ROOTS)
    locked_private = await _inventory(connection, storage_binding.storage_schema, ISOLATED_FHIR_ROOTS)
    _require((locked_canonical, locked_private) == (canonical, private), "catalog_changed")
    oids = sorted(native_object["oid"] for native_object in (*canonical, *private))
    frontier = await _callee_frontier(connection, storage_binding.catalog_schema)
    callee_oids = [native_object["oid"] for native_object in frontier]
    routines = [
        {"oid": native_object["oid"], "native": _decoded(native_object["native"])}
        for native_object in await connection.fetch(_ROUTINES_SQL, oids, callee_oids)
    ]
    _require(
        set(callee_oids) <= {native_object["oid"] for native_object in routines}
        and await _callee_frontier(connection, storage_binding.catalog_schema) == frontier,
        "callees_changed",
    )
    # PUBLIC remains pinned on each object; it must not turn unrelated logins
    # into a cluster-wide role roster. Real non-PUBLIC authority paths stay pinned.
    roles = _decoded(
        await connection.fetchval(
            _ROLES_SQL,
            oids,
            [storage_binding.catalog_schema, storage_binding.storage_schema],
            [native_object["oid"] for native_object in routines],
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
        )
    )
    return dict(canonical=canonical, private=private, routines=routines, roles=roles)


def _evidence_digests(evidence_by_kind):
    return {kind + "_sha256": _digest(value) for kind, value in evidence_by_kind.items()}


async def _observe(connection, storage_binding, policy):
    """Observe complete metadata only; the caller owns the short transaction."""
    return _evidence_digests(await _collect(connection, storage_binding, policy))


async def _collect_allocation_evidence(connection, storage_binding, policy, profile_by_field):
    from process import provider_directory_cms_preparation_policy as stable

    if profile_by_field["contract"] in {stable.CONTRACT, stable.DEPENDENCY_CONTRACT}:
        return await stable._collect_profile(connection, storage_binding, policy, profile_by_field)
    return await _collect(connection, storage_binding, policy)


def _allocation_fixed_relations(evidence_by_kind):
    canonical = evidence_by_kind["canonical"]
    return canonical["relations"] if type(canonical) is dict else canonical


async def _lock_allocation_inventory(connection, relations):
    """Pin existing metadata rows without blocking payload DML or new objects."""
    relation_by_oid = {relation["oid"]: relation for relation in relations}
    _require(
        relation_by_oid and all(type(oid) is int and oid > 0 for oid in relation_by_oid),
        "allocation_relations_required",
    )
    names = sorted(storage_relation(relation["nspname"], relation["relname"]) for relation in relation_by_oid.values())
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    )
    oids = sorted(relation_by_oid)
    # Relation locks alone are not an ACL fence. Pin existing class/column rows;
    # new allocation rows remain insertable by this same native transaction.
    relation_rows = await connection.fetch(
        "SELECT c.oid::bigint,c.relname,c.relnamespace::bigint,n.nspname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid FOR SHARE OF c NOWAIT",
        oids,
    )
    _require(
        [relation_row["oid"] for relation_row in relation_rows] == oids
        and all(
            (relation_row["nspname"], relation_row["relname"])
            == (relation_by_oid[relation_row["oid"]]["nspname"], relation_by_oid[relation_row["oid"]]["relname"])
            for relation_row in relation_rows
        ),
        "allocation_relations_changed",
    )
    await connection.fetch(
        "SELECT attrelid,attnum FROM pg_catalog.pg_attribute WHERE attrelid=ANY($1::oid[]) "
        "ORDER BY attrelid,attnum FOR SHARE NOWAIT",
        oids,
    )
    namespace_oids = sorted({relation_row["relnamespace"] for relation_row in relation_rows})
    await connection.fetch(_ALLOCATION_NAMESPACES_SQL, namespace_oids)


def _allocation_admission_relations(source_custody):
    from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
    from process.provider_directory_source_storage_binding import _CATALOG_ROOTS

    _require(
        type(source_custody) is dict and type(source_custody.get("canonical_authority")) is dict,
        "allocation_source_custody_required",
    )
    authority_by_kind = source_custody["canonical_authority"]
    expected_fields = {"admission", "metadata"}
    if "history_bounds" in authority_by_kind:
        expected_fields.add("history_bounds")
    _require(
        set(authority_by_kind) == expected_fields
        and type(authority_by_kind["metadata"]) is dict
        and set(authority_by_kind["metadata"])
        == {signature.replace("character varying", "varchar") for signature in METADATA_FUNCTIONS},
        "allocation_source_metadata_required",
    )
    if "history_bounds" in authority_by_kind:
        from process.provider_directory_cms_history_bounds import require_cms_history_expectations

        require_cms_history_expectations(authority_by_kind["history_bounds"])
    admission_by_name = source_custody["canonical_authority"].get("admission")
    _require(
        type(admission_by_name) is dict and admission_by_name.keys() == _CATALOG_ROOTS, "allocation_admission_required"
    )
    _require(
        all(type(relation) is dict and relation.get("relname") == name for name, relation in admission_by_name.items()),
        "allocation_admission_required",
    )
    return list(admission_by_name.values())


def _allocation_relation_closure(canonical, admission):
    relation_by_oid = {relation["oid"]: relation for relation in (*canonical, *admission)}
    selected_oids = {
        relation["oid"]
        for relation in canonical
        if relation["relname"] in {"provider_directory_endpoint_dataset", *TABLES[:4]}
    }
    _require(len(selected_oids) == 5, "allocation_original_roots_required")
    selected_oids |= {relation["oid"] for relation in admission}
    while True:
        native_references = [
            constraint[0]["confrelid"]
            for oid in selected_oids
            for constraint in relation_by_oid[oid].get("native", {}).get("constraints", [])
            if constraint[0]["contype"] == "f"
        ]
        _require(
            all(
                (type(reference) is int and 0 < reference < 2**32)
                or (type(reference) is str and re.fullmatch(r"[1-9][0-9]{0,9}", reference) and int(reference) < 2**32)
                for reference in native_references
            ),
            "allocation_foreign_relation_unbound",
        )
        # pg_constraint JSON preserves oid strings; normalize only this lookup.
        referenced_oids = {int(reference) for reference in native_references}
        _require(referenced_oids <= relation_by_oid.keys(), "allocation_foreign_relation_unbound")
        if referenced_oids <= selected_oids:
            return sorted(selected_oids)
        selected_oids |= referenced_oids


async def _project_allocation_functions(connection, storage_binding, routines, relation_oids):
    from process import provider_directory_cms_callable_custody as callables

    required, _admission_oids = await _allocation_routines(connection, storage_binding.catalog_schema, relation_oids)
    authenticated_by_oid = {entry["oid"]: entry["native"] for entry in routines}
    required_by_name = {}
    for function_row in required:
        native_by_field = _decoded(function_row["native"])
        if int(native_by_field["definition"]["pronamespace"]) == 11:
            continue
        authenticated = authenticated_by_oid.get(function_row["oid"])
        _require(
            authenticated is not None
            and {
                key: field_value
                for key, field_value in authenticated.items()
                if key not in {"acl", "namespace_identifier"}
            }
            == native_by_field,
            "allocation_routine_unbound",
        )
        name = native_by_field["definition"]["proname"]
        _require(name not in required_by_name, "allocation_callable_closure_changed")
        required_by_name[name] = authenticated
    function_rows = await connection.fetch(
        callables._FUNCTION_SQL,
        list(callables.cms_callee_signatures(storage_binding.catalog_schema)),
        storage_binding.catalog_schema,
        list(required_by_name),
        [],
    )
    functions_by_name = {}
    for function_row in function_rows:
        definition_by_field = _decoded(function_row["definition"])
        name = definition_by_field["proname"]
        _require(
            name in required_by_name
            and name not in functions_by_name
            and definition_by_field == required_by_name[name]["definition"]
            and function_row["language"] == required_by_name[name]["language"]["lanname"]
            and ("acl" not in required_by_name[name] or _decoded(function_row["acl"]) == required_by_name[name]["acl"]),
            "allocation_routine_unbound",
        )
        # Effective ACL is a native expansion of that exact authenticated proacl.
        functions_by_name[name] = dict(
            definition=definition_by_field, language=function_row["language"], acl=_decoded(function_row["acl"])
        )
    _require(functions_by_name.keys() == required_by_name.keys(), "allocation_callable_closure_changed")
    return functions_by_name


async def capture_authenticated_cms_allocation_pins(
    connection, *, storage_binding, policy, cms_profile, source_custody, preparation_schema
):
    """Project existing protected digests after locking and authenticating raw evidence.

    Control must load the genuine enabled Source/profile records, qualify the
    fixed preparation namespace and reverify the complete Source after these
    locks. No caller observation or this return value supplies record provenance.
    All locks belong to the caller's short original-Admin transaction.
    """
    from process import provider_directory_cms_preparation_policy as stable

    validator = (
        stable._profile
        if type(cms_profile) is dict and cms_profile.get("contract") in {stable.CONTRACT, stable.DEPENDENCY_CONTRACT}
        else _profile
    )
    profile_by_field = validator(cms_profile, storage_binding, policy)
    source_custody = json.loads(json.dumps(source_custody, allow_nan=False))
    admission = _allocation_admission_relations(source_custody)
    await _lock_allocation_catalogs(connection)
    evidence_by_kind = await _collect_allocation_evidence(connection, storage_binding, policy, profile_by_field)
    expected_by_field = {field: profile_by_field[field] for field in _DIGEST_FIELDS}
    _require(_evidence_digests(evidence_by_kind) == expected_by_field, "native_evidence_changed")
    canonical = _allocation_fixed_relations(evidence_by_kind)
    await _lock_allocation_inventory(connection, [*canonical, *evidence_by_kind["private"], *admission])
    locked_by_kind = await _collect_allocation_evidence(connection, storage_binding, policy, profile_by_field)
    _require(_evidence_digests(locked_by_kind) == expected_by_field, "native_evidence_changed")
    relations = _allocation_relation_closure(canonical, admission)
    functions_by_name = await _project_allocation_functions(
        connection, storage_binding, locked_by_kind["routines"], relations
    )
    namespaces = [
        dict(namespace_row)
        for namespace_row in await connection.fetch(
            "SELECT oid::bigint,nspname,nspowner::bigint,nspacl::text FROM pg_catalog.pg_namespace "
            "WHERE nspname=ANY($1::text[]) ORDER BY oid FOR SHARE NOWAIT",
            ["pg_catalog", storage_binding.catalog_schema, preparation_schema],
        )
    ]
    return dict(
        functions=functions_by_name,
        namespaces=namespaces,
        roles={field: locked_by_kind["roles"][field] or [] for field in ("roles", "memberships")},
    ), relations


async def compare_cms_preparation_custody(connection, *, storage_binding, policy, cms_profile):
    """Compare a distinct CMS pin before acquisition; never authorize a finalizer.

    No trusted CMS profile loader exists here. Callers must not treat a matching
    caller-supplied profile as issuance, enrollment, or permission to write.
    Canonical payload custody, frozen-workspace validation and safe writer
    admission remain separate requirements; this is only the fixed CMS delta.
    """
    cms_profile = _profile(cms_profile, storage_binding, policy)
    _require(connection.is_in_transaction() is True, "transaction_required")
    actor_by_field = dict(await connection.fetchrow(_ACTOR_SQL))
    _require(
        actor_by_field["session"] == policy.publisher_role
        and actor_by_field["current"] in {policy.owner_role, policy.publisher_role}
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["database_oid"] == storage_binding.database_oid
        and actor_by_field["replication"] == "origin"
        and bool(actor_by_field["transaction"]),
        "original_publisher_required",
    )
    observed = await _observe(connection, storage_binding, policy)
    _require(observed == {field: cms_profile[field] for field in _DIGEST_FIELDS}, "native_evidence_changed")
    _require(
        connection.is_in_transaction() is True and dict(await connection.fetchrow(_ACTOR_SQL)) == actor_by_field,
        "original_publisher_changed",
    )
    return observed
