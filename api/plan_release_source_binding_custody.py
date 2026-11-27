# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only shape checks for one closed, attached Source cohort, located by name."""

import hashlib

from api.plan_release_source_bindings import required_registry_schema
from db.plan_release_storage import storage_identifier

_COHORT_CUSTODY_SQL = """WITH family AS (
 SELECT c.*,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY(ARRAY[to_regclass('{registry_schema}.hp_plan_release_binding'),
  to_regclass(:child_name)])
)
SELECT (SELECT count(*)=2 AND bool_and(
  c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
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
 ) FROM family c)
 AND EXISTS(SELECT FROM family parent JOIN family child ON child.oid=to_regclass(:child_name)
  WHERE parent.relname='hp_plan_release_binding' AND parent.relkind='p' AND NOT parent.relispartition
  AND pg_get_partkeydef(parent.oid)='LIST (serving_revision_id)'
  AND child.relkind='r' AND child.relispartition
  AND pg_get_expr(child.relpartbound,child.oid)='FOR VALUES IN ('||quote_literal(:revision_id)||')'
  AND ARRAY(SELECT inhparent FROM pg_inherits WHERE inhrelid=child.oid)=ARRAY[parent.oid]
  AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhparent=child.oid))
 AND (SELECT count(*)=3 AND bool_and(
  c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
  AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
  AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid))
 FROM pg_class c WHERE c.oid=ANY(ARRAY[
  to_regclass('{registry_schema}.hp_plan_release'),
  to_regclass('{registry_schema}.hp_plan_serving_revision'),to_regclass('{registry_schema}.hp_plan_current_release')]))
 AS valid"""


async def require_source_binding_custody_fetch(fetch, schema, admission):
    """Hold ordinary read locks by name, then check the cohort's shape; never acquire ownership."""
    registry_schema = storage_identifier(required_registry_schema())
    child = (
        registry_schema
        + ".catalog_release_binding_"
        + hashlib.sha256(admission.serving_revision_id.encode()).hexdigest()[:32]
    )
    await fetch(f"SELECT 1 FROM ONLY {registry_schema}.hp_plan_release_binding LIMIT 0")
    await fetch(f"SELECT 1 FROM ONLY {child} LIMIT 0")
    sql = (
        _COHORT_CUSTODY_SQL.format(registry_schema=registry_schema)
        .replace(":child_name", "$1")
        .replace(":revision_id", "$2")
    )
    custody = await fetch(sql, child, admission.serving_revision_id)
    if len(custody) != 1 or custody[0]["valid"] is not True:
        raise ValueError("plan_release_source_binding_custody")
