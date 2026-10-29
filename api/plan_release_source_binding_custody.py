# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only native custody checks for one closed, attached Source cohort."""

import hashlib
import re

from sqlalchemy import text

from api.plan_release_source_bindings import required_registry_schema
from db.plan_release_storage import storage_identifier

_MANIFEST_CUSTODY_SQL = """SELECT count(*)=1 AND bool_and(
 c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND c.relowner=n.nspowner
 AND NOT owner.rolcanlogin AND NOT owner.rolsuper AND NOT owner.rolcreaterole
 AND NOT owner.rolcreatedb AND NOT owner.rolreplication AND NOT owner.rolbypassrls
 AND NOT pg_has_role(current_user,c.relowner,'USAGE')
 AND NOT pg_has_role(current_user,c.relowner,'SET')
 AND NOT has_schema_privilege(current_user,n.oid,'CREATE')
 AND has_table_privilege(current_user,c.oid,'SELECT')
 AND NOT has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AND NOT has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES')
 AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
 AND NOT EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
  WHERE a.grantee<>c.relowner AND (a.grantee=0 OR a.privilege_type<>'SELECT' OR a.is_grantable))
 AND NOT EXISTS(SELECT FROM pg_attribute field CROSS JOIN LATERAL aclexplode(field.attacl) a
  WHERE field.attrelid=c.oid)) AS valid
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_roles owner ON owner.oid=c.relowner
 WHERE c.oid=to_regclass('{registry_schema}.catalog_runtime_manifest')"""

_COHORT_CUSTODY_SQL = """WITH profile AS (
 SELECT to_regrole(policy->>'preparation_owner_role')::oid AS owner_oid,
 to_regrole(policy->>'owner_role')::oid AS namespace_owner_oid,
 ARRAY(SELECT to_regrole(role_name)::oid FROM jsonb_array_elements_text(policy->'runtime_roles') runtime(role_name)) AS reader_oids
 FROM {registry_schema}.catalog_runtime_manifest WHERE manifest_key='runtime'
), family AS (
 SELECT c.*,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY(ARRAY[to_regclass('{registry_schema}.hp_plan_release_binding'),
  to_regclass(:child_name)])
), reader AS (SELECT * FROM pg_roles WHERE rolname=current_user)
SELECT (SELECT count(*)=1 AND bool_and(owner_oid IS NOT NULL AND namespace_owner_oid IS NOT NULL
 AND cardinality(reader_oids)>0 AND array_position(reader_oids,NULL) IS NULL
 AND (SELECT oid FROM pg_roles WHERE rolname=current_user)=ANY(reader_oids)) FROM profile)
 AND (SELECT count(*)=1 AND bool_and(session_user=current_user AND
  NOT(rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) FROM reader)
 AND EXISTS(SELECT FROM profile JOIN pg_roles owner ON owner.oid=profile.owner_oid
  WHERE NOT(owner.rolcanlogin OR owner.rolsuper OR owner.rolcreatedb OR owner.rolcreaterole
   OR owner.rolreplication OR owner.rolbypassrls)
  AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE
   (pg_has_role(owner.oid,elevated.oid,'SET') OR pg_has_role(owner.oid,elevated.oid,'USAGE'))
   AND (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
    OR elevated.rolreplication OR elevated.rolbypassrls)))
 AND (SELECT count(*)=2 AND bool_and(
  c.relowner=profile.owner_oid AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
  AND NOT pg_has_role(current_user,c.relowner,'USAGE') AND NOT pg_has_role(current_user,c.relowner,'SET')
  AND has_table_privilege(current_user,c.oid,'SELECT')
  AND NOT has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
  AND NOT has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES')
  AND (SELECT array_agg(a.attname::text||'|'||format_type(a.atttypid,a.atttypmod)||'|'||a.attnotnull::text
        ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)
   = ARRAY['release_binding_id|character varying(64)|true','plan_release_id|character varying(64)|true',
    'serving_revision_id|character varying(64)|true','role|character varying(64)|true',
    'ordinal|integer|true','required|boolean|true','node_id|character varying(64)|true',
    'source_key|character varying(128)|true','snapshot_id|character varying(96)|true',
    'source_file_import_id|character varying(64)|false','received_installation_id|uuid|false',
    'custom_plan_component_id|character varying(64)|false','plan_id|text|true',
    'market_type|character varying(64)|true','logical_content_id|character varying(128)|false','metadata_json|jsonb|false']
  AND NOT EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=c.oid
    AND (a.attgenerated<>'' OR a.attidentity<>'' OR a.atthasdef))
  AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid)
  AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
  AND NOT EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
   WHERE a.grantee<>c.relowner AND
    (NOT a.grantee=ANY(profile.reader_oids) OR a.privilege_type<>'SELECT' OR a.is_grantable))
  AND NOT EXISTS(SELECT FROM pg_attribute field CROSS JOIN LATERAL aclexplode(field.attacl) a
   WHERE field.attrelid=c.oid AND (a.grantee<>c.relowner AND
    (NOT a.grantee=ANY(profile.reader_oids) OR a.privilege_type<>'SELECT' OR a.is_grantable)))
  AND NOT EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND
   (contype='f' OR NOT convalidated OR condeferrable OR condeferred))
  AND EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND contype='p'
    AND conkey=ARRAY[3,1]::smallint[])
  AND EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND contype='u'
    AND conkey=ARRAY[3,4,5]::smallint[])
  AND NOT EXISTS(SELECT FROM pg_constraint k JOIN pg_depend d
   ON d.classid='pg_constraint'::regclass AND d.objid=k.oid
   JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND d.refobjid=p.oid
   WHERE k.conrelid=c.oid AND p.pronamespace<>'pg_catalog'::regnamespace)
 ) FROM family c CROSS JOIN profile)
 AND EXISTS(SELECT FROM family parent JOIN family child ON child.oid=to_regclass(:child_name)
  WHERE parent.relname='hp_plan_release_binding' AND parent.relkind='p' AND NOT parent.relispartition
  AND pg_get_partkeydef(parent.oid)='LIST (serving_revision_id)'
  AND child.relkind='r' AND child.relispartition
  AND pg_get_expr(child.relpartbound,child.oid)='FOR VALUES IN ('||quote_literal(:revision_id)||')'
  AND ARRAY(SELECT inhparent FROM pg_inherits WHERE inhrelid=child.oid)=ARRAY[parent.oid]
  AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhparent=child.oid))
 AND (SELECT count(*)=3 AND bool_and(
  (c.relowner=profile.namespace_owner_oid
  OR (c.relname='hp_plan_serving_revision' AND c.relowner=
    (SELECT relowner FROM pg_class WHERE oid=to_regclass($3))))
  AND c.relkind='r'
  AND c.relpersistence='p' AND NOT c.relispartition
  AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
  AND NOT pg_has_role(current_user,c.relowner,'USAGE')
  AND NOT pg_has_role(current_user,c.relowner,'SET')
  AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
  AND has_table_privilege(current_user,c.oid,'SELECT')
  AND NOT has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
  AND NOT has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES'))
 FROM pg_class c CROSS JOIN profile WHERE c.oid=ANY(ARRAY[
  to_regclass('{registry_schema}.hp_plan_release'),
  to_regclass('{registry_schema}.hp_plan_serving_revision'),to_regclass('{registry_schema}.hp_plan_current_release')]))
 AS valid"""


async def require_source_binding_custody_fetch(fetch, schema, admission):
    """Hold ordinary read locks; never acquire ownership or adopt live objects."""
    registry_schema = storage_identifier(required_registry_schema())
    child = (
        registry_schema
        + ".catalog_release_binding_"
        + hashlib.sha256(admission.serving_revision_id.encode()).hexdigest()[:32]
    )
    manifest = await fetch(_MANIFEST_CUSTODY_SQL.format(registry_schema=registry_schema))
    if len(manifest) != 1 or manifest[0]["valid"] is not True:
        raise ValueError("plan_release_source_manifest_custody")
    await fetch(f"SELECT 1 FROM {registry_schema}.catalog_runtime_manifest LIMIT 0")
    await fetch(f"SELECT 1 FROM ONLY {registry_schema}.hp_plan_release_binding LIMIT 0")
    await fetch(f"SELECT 1 FROM ONLY {child} LIMIT 0")
    sql = (
        _COHORT_CUSTODY_SQL.format(registry_schema=registry_schema)
        .replace(":child_name", "$1")
        .replace(":revision_id", "$2")
    )
    custody = await fetch(sql, child, admission.serving_revision_id, schema + ".plan_release_serving_revision")
    if len(custody) != 1 or custody[0]["valid"] is not True:
        raise ValueError("plan_release_source_binding_custody")


async def require_source_binding_custody(session, schema, admission):
    """Use the same bounded custody decision through this exact ORM Reader."""

    async def fetch(sql, *parameters):
        """Translate only the fixed native positional parameters for SQLAlchemy."""
        sql = re.sub(r"\$(\d+)", lambda match: ":p" + match[1], sql)
        rows = await session.execute(text(sql), {"p" + str(index): value for index, value in enumerate(parameters, 1)})
        return [dict(getattr(row, "_mapping", row)) for row in rows]

    await require_source_binding_custody_fetch(fetch, schema, admission)
