# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare native CMS preparation evidence; this module issues no authority.

The expected profile must eventually be loaded from a separately protected CMS
issuer record. Shared FHIR enrollment, task input and a detached handoff cannot
supply that provenance. Equality here is evidence, never a write capability.
"""

import hashlib
import json
import re
from dataclasses import asdict

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_provision import ISOLATED_FHIR_ROOTS as ISOLATED_FHIR_ROOTS
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


def _evidence_digests(evidence_by_kind):
    return {kind + "_sha256": _digest(value) for kind, value in evidence_by_kind.items()}
