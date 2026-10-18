# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based native catalog evidence for plan-release custody."""

ROLE_SQL = """SELECT r.oid,r.rolname,r.rolcanlogin,r.rolsuper,r.rolcreatedb,
 r.rolcreaterole,r.rolreplication,r.rolbypassrls,
 EXISTS(SELECT FROM pg_roles elevated WHERE
  (pg_has_role(r.oid,elevated.oid,'SET') OR pg_has_role(r.oid,elevated.oid,'USAGE'))
  AND (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb
    OR elevated.rolreplication OR elevated.rolbypassrls)) AS elevated,
 pg_has_role(r.oid,$2::name,'SET') AS owner_set,
 pg_has_role(r.oid,$3::name,'SET') AS publisher_set,
 has_schema_privilege(r.oid,$4::name,'USAGE') AS namespace_usage,
 EXISTS(SELECT FROM pg_roles reachable WHERE
  (pg_has_role(r.oid,reachable.oid,'SET') OR pg_has_role(r.oid,reachable.oid,'USAGE'))
  AND has_schema_privilege(reachable.oid,n.oid,'CREATE')) AS namespace_create,
 (pg_has_role(r.oid,n.nspowner,'SET') OR pg_has_role(r.oid,n.nspowner,'USAGE')) AS namespace_owner_reachable,
 NOT (schema_owner.rolcanlogin OR schema_owner.rolsuper OR schema_owner.rolcreatedb
  OR schema_owner.rolcreaterole OR schema_owner.rolreplication OR schema_owner.rolbypassrls)
  AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE
   (pg_has_role(n.nspowner,elevated.oid,'SET') OR pg_has_role(n.nspowner,elevated.oid,'USAGE'))
   AND (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb
    OR elevated.rolreplication OR elevated.rolbypassrls)) AS namespace_owner_protected
 FROM pg_roles r JOIN pg_namespace n ON n.nspname=$4::name
 JOIN pg_roles schema_owner ON schema_owner.oid=n.nspowner
 WHERE r.rolname=ANY($1::text[]) ORDER BY r.rolname"""

CATALOG_SQL = """WITH RECURSIVE family(oid) AS (
 SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)
 UNION SELECT child.inhrelid FROM pg_inherits child JOIN family parent ON parent.oid=child.inhparent
)
SELECT c.oid::bigint,n.nspname,c.relname,c.relkind::text,c.relpersistence::text,c.relispartition,
 c.relowner::bigint,c.relrowsecurity,c.relforcerowsecurity,
 pg_get_expr(c.relpartbound,c.oid) AS bound,
 pg_get_partkeydef(c.oid) AS partition_key,
 ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents,
 (SELECT coalesce(jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),
    a.attnotnull,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum),'[]'::jsonb)
  FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('name',conname,'type',contype::text,
    'definition',pg_get_constraintdef(oid),'valid',convalidated,
    'columns',conkey,'oid',oid::bigint) ORDER BY conname),'[]'::jsonb)
  FROM pg_constraint WHERE conrelid=c.oid AND contype<>'n') AS constraints,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('oid',i.indexrelid::bigint,'primary',i.indisprimary,
   'unique',i.indisunique,'valid',i.indisvalid AND i.indisready AND i.indislive,'method',am.amname,
   'native',i.indimmediate AND i.indnkeyatts=i.indnatts AND NOT i.indisexclusion AND coalesce((
      SELECT bool_and(op.opcnamespace='pg_catalog'::regnamespace
       AND op.opcname=CASE attribute.atttypid WHEN 'pg_catalog.int4'::regtype THEN 'int4_ops'
                       WHEN 'pg_catalog.varchar'::regtype THEN 'text_ops' ELSE '' END
       AND index_key.flags=0 AND index_key.collation_oid=attribute.attcollation)
      FROM unnest(i.indkey::smallint[],i.indclass::oid[],i.indcollation::oid[],i.indoption::smallint[])
        index_key(attnum,opclass_oid,collation_oid,flags)
      JOIN pg_attribute attribute ON attribute.attrelid=c.oid AND attribute.attnum=index_key.attnum
      JOIN pg_opclass op ON op.oid=index_key.opclass_oid),false),
   'predicate',pg_get_expr(i.indpred,i.indrelid),'expression',pg_get_expr(i.indexprs,i.indrelid),
   'keys',ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ordinal)
     LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum ORDER BY k.ordinal))
   ORDER BY i.indexrelid),'[]'::jsonb)
  FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam
  WHERE i.indrelid=c.oid) AS indexes,
 NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid)
  AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) AS inert,
 NOT EXISTS(
  SELECT FROM pg_constraint k JOIN pg_depend d ON d.classid='pg_constraint'::regclass AND d.objid=k.oid
  JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND d.refobjid=p.oid
  WHERE k.conrelid=c.oid AND p.pronamespace<>'pg_catalog'::regnamespace) AS native_checks,
 NOT EXISTS(
  SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
  WHERE a.grantee<>$2::oid AND (a.grantee<>ALL($3::oid[]) OR a.privilege_type<>'SELECT' OR a.is_grantable)
  UNION ALL SELECT FROM pg_attribute field CROSS JOIN LATERAL aclexplode(field.attacl) a
  WHERE field.attrelid=c.oid AND a.grantee<>$2::oid
   AND (a.grantee<>ALL($3::oid[]) OR a.privilege_type<>'SELECT' OR a.is_grantable)
 ) AS direct_acl_closed,
 NOT EXISTS(SELECT FROM unnest($3::oid[]) readers(oid) WHERE
   NOT has_table_privilege(readers.oid,c.oid,'SELECT')
   OR has_table_privilege(readers.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   OR has_any_column_privilege(readers.oid,c.oid,'INSERT,UPDATE,REFERENCES')) AS readers_closed
FROM family f JOIN pg_class c ON c.oid=f.oid JOIN pg_namespace n ON n.oid=c.relnamespace
ORDER BY c.oid"""

HEADER_NATIVE_CHECKS = {
    "plan_release_serving_binding_count_check": "CHECK ((expected_binding_count > 0))",
    "plan_release_serving_current_state_check": "CHECK (((NOT is_current) OR (((serving_status)::text = "
    "'published'::text) AND ((release_status)::text = "
    "'published'::text))))",
    "plan_release_serving_digest_check": "CHECK (((binding_set_digest)::text ~ '^[0-9a-f]{64}$'::text))",
    "plan_release_serving_month_check": "CHECK (((release_month)::text ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'::text))",
    "plan_release_serving_plan_id_check": "CHECK (((healthporta_plan_id)::text ~ "
    "'^hpplan_[0-9A-HJKMNP-TV-Z]{26}$'::text))",
    "plan_release_serving_release_id_check": "CHECK (((plan_release_id)::text ~ "
    "'^hprelease_[0-9A-HJKMNP-TV-Z]{26}$'::text))",
    "plan_release_serving_release_status_check": "CHECK (((release_status)::text = ANY "
    "((ARRAY['candidate'::character varying, "
    "'published'::character varying, 'superseded'::character "
    "varying, 'withdrawn'::character varying])::text[])))",
    "plan_release_serving_revision_id_check": "CHECK (((serving_revision_id)::text ~ "
    "'^hpserve_[0-9A-HJKMNP-TV-Z]{26}$'::text))",
    "plan_release_serving_status_check": "CHECK (((serving_status)::text = ANY ((ARRAY['building'::character "
    "varying, 'published'::character varying, 'retired'::character "
    "varying])::text[])))",
    "plan_release_serving_version_id_check": "CHECK (((plan_version_id IS NULL) OR ((plan_version_id)::text ~ "
    "'^hpversion_[0-9A-HJKMNP-TV-Z]{26}$'::text)))",
}

BINDING_NATIVE_CHECKS = {
    "plan_release_snapshot_binding_ordinal_check": "CHECK ((binding_ordinal >= 0))",
    "plan_release_snapshot_binding_plan_check": "CHECK ((btrim((plan_id)::text) <> ''::text))",
    "plan_release_snapshot_binding_role_check": "CHECK (((role)::text = ANY ((ARRAY['in_network'::character varying, 'allowed_amounts'::character varying])::text[])))",
    "plan_release_snapshot_binding_source_check": "CHECK ((btrim((source_key)::text) <> ''::text))",
}
PIN_NATIVE_CHECKS = {
    "plan_release_snapshot_pin_revision_check": "CHECK (((serving_revision_id)::text ~ '^hpserve_[0-9A-HJKMNP-TV-Z]{26}$'::text))",
    "plan_release_snapshot_pin_snapshot_check": "CHECK ((btrim((snapshot_id)::text) <> ''::text))",
}
