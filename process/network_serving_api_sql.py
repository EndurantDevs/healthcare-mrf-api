# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One bounded native metadata statement for protected API serving pins."""

from process.network_address_projection import _identifier

_METADATA_SQL = r"""
WITH control AS MATERIALIZED (SELECT n.oid::bigint AS schema_oid,s.relowner::bigint AS owner_oid,o.*,
 n.nspowner::bigint AS schema_owner_oid,q.rolname::pg_catalog.text AS schema_owner_role,
 c.relowner::bigint AS candidate_owner_oid,q.rolname::pg_catalog.text AS candidate_owner_role,
 d.oid::bigint AS database_oid,pg_catalog.current_database() AS database_name,
 c.oid::bigint AS candidate_table_oid,s.oid::bigint AS seal_table_oid,
 c.relowner=n.nspowner AND c.relkind='r' AND s.relkind='r'
 AND NOT (q.rolcanlogin OR q.rolsuper OR q.rolcreatedb OR q.rolcreaterole OR q.rolreplication OR q.rolbypassrls)
 AND (s.relowner=n.nspowner OR (NOT pg_catalog.pg_has_role(s.relowner,n.nspowner,'MEMBER')
   AND NOT pg_catalog.pg_has_role(n.nspowner,s.relowner,'MEMBER')))
 AND c.relpersistence='p' AND s.relpersistence='p' AND NOT s.relrowsecurity AND NOT s.relforcerowsecurity
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_trigger WHERE tgrelid=s.oid AND NOT tgisinternal)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=s.oid) AS owned,
 NOT EXISTS(SELECT FROM (
   SELECT a.* FROM pg_catalog.aclexplode(coalesce(s.relacl,pg_catalog.acldefault('r',s.relowner))) a
   UNION ALL SELECT a.* FROM pg_catalog.pg_attribute att CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
    WHERE att.attrelid=s.oid AND att.attnum>0 AND NOT att.attisdropped
 ) a WHERE a.grantee<>s.relowner AND (a.grantee=0 OR a.privilege_type<>'SELECT' OR a.is_grantable)) AS closed
 FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles q ON q.oid=n.nspowner
 JOIN pg_catalog.pg_database d ON d.datname=pg_catalog.current_database()
 JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname='network_membership_candidate'
 JOIN pg_catalog.pg_class s ON s.relnamespace=n.oid AND s.relname='network_candidate_custody'
 JOIN pg_catalog.pg_roles o ON o.oid=s.relowner
 WHERE n.nspname=$1),
chosen AS MATERIALIZED (
 SELECT pg_catalog.to_jsonb(candidate) AS candidate,pg_catalog.to_jsonb(manifest) AS manifest,
  candidate.created_at,candidate.candidate_id,candidate.schema_name,
  candidate.approved_custom_revision,
  candidate.validation_json->'serving_readiness'->'address_artifact' AS address,
  seal.receipt_json,seal.seal_sha256
 FROM "PIN_CONTROL_SCHEMA".network_serving_manifest manifest
 JOIN "PIN_CONTROL_SCHEMA".network_membership_candidate candidate USING(candidate_id)
 LEFT JOIN "PIN_CONTROL_SCHEMA".network_candidate_custody seal USING(candidate_id)
 WHERE manifest.generation_id=coalesce($2::bigint,
   (SELECT generation_id FROM "PIN_CONTROL_SCHEMA".network_serving_control WHERE id=1))
  AND manifest.eligible AND candidate.state='published'
  AND pg_catalog.octet_length(candidate.validation_json::pg_catalog.text)<=1048576
  AND (seal.receipt_json IS NULL OR pg_catalog.octet_length(seal.receipt_json::pg_catalog.text)<=2097152)
), edge_ids AS MATERIALIZED (
 SELECT DISTINCT (edge->>'candidate_id')::pg_catalog.uuid AS candidate_id FROM chosen,
 LATERAL pg_catalog.jsonb_array_elements(CASE WHEN pg_catalog.jsonb_typeof(candidate->'validation_json'->'serving_readiness'->'edge_artifacts')='array'
  THEN candidate->'validation_json'->'serving_readiness'->'edge_artifacts' ELSE '[]'::pg_catalog.jsonb END) edge
 LIMIT 129
), family_inputs AS MATERIALIZED (
 SELECT candidate_id,candidate,schema_name,receipt_json,seal_sha256,false AS projected_edge
 FROM chosen WHERE pg_catalog.octet_length(receipt_json::pg_catalog.text)<=2097152
 UNION ALL
 SELECT candidate.candidate_id,
  pg_catalog.jsonb_build_object('dataset_id',candidate.dataset_id,'schema_id',candidate.schema_id,
   'producer_id',candidate.producer_id,'candidate_id',candidate.candidate_id,
   'schema_name',candidate.schema_name,'state',candidate.state) AS candidate,
  candidate.schema_name,projected.receipt_json,seal.seal_sha256,true AS projected_edge
 FROM "PIN_CONTROL_SCHEMA".network_membership_candidate candidate
 LEFT JOIN "PIN_CONTROL_SCHEMA".network_candidate_custody seal USING(candidate_id)
 CROSS JOIN LATERAL (SELECT pg_catalog.jsonb_build_object(
    'component',seal.receipt_json->'component','revision',seal.receipt_json->'revision',
    'control',seal.receipt_json->'control','closure',seal.receipt_json->'closure',
    'index_definition_sha256',seal.receipt_json->'index_definition_sha256',
    'runtime_roles',seal.receipt_json->'runtime_roles') AS receipt_json
 ) projected
 WHERE candidate.candidate_id IN (SELECT candidate_id FROM edge_ids)
  AND candidate.candidate_id<>(SELECT candidate_id FROM chosen)
  AND pg_catalog.octet_length(projected.receipt_json::pg_catalog.text)<=2097152
), family AS MATERIALIZED (
 SELECT family_inputs.*,
  ARRAY(SELECT pg_catalog.jsonb_array_elements_text(receipt_json->'runtime_roles') LIMIT 129) AS runtime_roles
 FROM family_inputs
), family_native_namespaces AS MATERIALIZED (
 SELECT DISTINCT namespace.oid AS schema_oid
 FROM family JOIN pg_catalog.pg_namespace namespace ON namespace.nspname=family.schema_name
), family_native_relations AS MATERIALIZED (
 SELECT relation.* FROM family_native_namespaces namespace
 CROSS JOIN LATERAL (
  SELECT relation.* FROM pg_catalog.pg_class relation
  WHERE relation.relnamespace=namespace.schema_oid OFFSET 0
 ) relation
), family_metadata AS MATERIALIZED (
 SELECT family.*,pg_catalog.to_jsonb(catalog) AS catalog,
  privileges.closed,(SELECT NOT EXISTS(
 SELECT FROM pg_catalog.unnest(family.runtime_roles) configured(name) LEFT JOIN pg_catalog.pg_roles r ON r.rolname=configured.name
 WHERE r.oid IS NULL OR pg_catalog.pg_has_role(r.oid,control.schema_owner_oid::pg_catalog.oid,'MEMBER')
 OR pg_catalog.pg_has_role(r.oid,control.schema_owner_oid::pg_catalog.oid,'USAGE') OR pg_catalog.pg_has_role(r.oid,control.schema_owner_oid::pg_catalog.oid,'SET'))) AS q_reach_closed,(SELECT NOT EXISTS(
 SELECT FROM pg_catalog.unnest(family.runtime_roles) configured(name) LEFT JOIN pg_catalog.pg_roles r ON r.rolname=configured.name
 CROSS JOIN (SELECT control.owner_oid::pg_catalog.oid AS owner_oid) s WHERE (r.oid IS NULL OR r.rolsuper OR r.rolcreaterole
 OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'MEMBER') OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'USAGE')
 OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'SET')
 OR EXISTS(SELECT 1 FROM pg_catalog.pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
   AND pg_catalog.pg_has_role(r.oid,elevated.oid,'MEMBER')))
 OR pg_catalog.has_schema_privilege(r.oid,catalog.schema_oid::pg_catalog.oid,'CREATE')
 OR pg_catalog.pg_has_role(r.oid,'pg_write_all_data','MEMBER')
 OR EXISTS(SELECT FROM pg_catalog.unnest(catalog.heap_oids) heap(oid)
   WHERE pg_catalog.has_table_privilege(r.oid,heap.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   OR pg_catalog.has_any_column_privilege(r.oid,heap.oid,'INSERT,UPDATE,REFERENCES')))
) AS census_closed,
  indexes.rows AS indexes
 FROM family CROSS JOIN control
 LEFT JOIN LATERAL (
  SELECT namespace.oid::bigint AS schema_oid,namespace.nspowner::bigint AS schema_owner_oid,
   owner.oid::bigint AS owner_oid,pg_catalog.to_jsonb(owner) AS owner,
   ARRAY(SELECT relation.oid FROM family_native_relations relation
    WHERE relation.relnamespace=namespace.oid AND relation.relkind='r') AS heap_oids,
   (SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
     'oid',relation.oid::bigint,'relname',relation.relname,'relkind',relation.relkind::pg_catalog.text,
     'relpersistence',relation.relpersistence::pg_catalog.text,'relowner',relation.relowner::bigint,
     'relacl',relation.relacl,'indrelid',index_record.indrelid::bigint)
     ORDER BY relation.relname),'[]'::pg_catalog.jsonb)
    FROM (SELECT * FROM family_native_relations WHERE relnamespace=namespace.oid ORDER BY relname LIMIT 129) relation
    LEFT JOIN pg_catalog.pg_index index_record ON index_record.indexrelid=relation.oid) AS relations,
   EXISTS(SELECT FROM pg_catalog.pg_proc WHERE pronamespace=namespace.oid) AS has_routines,
   (SELECT pg_catalog.count(*)>0 AND pg_catalog.bool_and(
      relation.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
      AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity AND NOT relation.relispartition
      AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid)
      AND NOT EXISTS(SELECT FROM pg_catalog.pg_trigger WHERE tgrelid=relation.oid AND NOT tgisinternal)
      AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=relation.oid))
    FROM family_native_relations relation WHERE relation.relnamespace=namespace.oid AND relation.relkind='r') AS heaps_safe
  FROM pg_catalog.pg_namespace namespace
  LEFT JOIN pg_catalog.pg_roles owner ON owner.rolname=family.receipt_json->'closure'->>'owner_role'
  WHERE namespace.nspname=family.schema_name
 ) catalog ON true
 LEFT JOIN LATERAL (
  WITH closure_scope AS (SELECT catalog.schema_oid::pg_catalog.oid,catalog.owner_oid::pg_catalog.oid,
   ARRAY(SELECT pg_catalog.jsonb_array_elements_text(family.receipt_json->'closure'->'reader_roles') LIMIT 129) AS role_names,
   NULL::pg_catalog.text[] AS relation_names)
  ,
runtime_roles AS (
 SELECT configured.name,r.* FROM closure_scope s CROSS JOIN pg_catalog.unnest(s.role_names) configured(name)
 LEFT JOIN pg_catalog.pg_roles r ON r.rolname=configured.name
), native_acl AS (
 SELECT a.grantee,a.privilege_type,a.is_grantable,'USAGE'::pg_catalog.text AS allowed
 FROM closure_scope s JOIN pg_catalog.pg_namespace n ON n.oid=s.schema_oid
 CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
 WHERE a.grantee<>s.owner_oid
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,'SELECT'
 FROM closure_scope s JOIN family_native_relations c ON c.relnamespace=s.schema_oid AND c.relkind='r'
 CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
 WHERE a.grantee<>s.owner_oid AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,'SELECT'
 FROM closure_scope s JOIN family_native_relations c ON c.relnamespace=s.schema_oid
 JOIN pg_catalog.pg_attribute att ON att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped
 CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
 WHERE a.grantee<>s.owner_oid AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,
 CASE WHEN d.defaclobjtype='r' THEN 'SELECT' ELSE '' END
 FROM closure_scope s JOIN pg_catalog.pg_default_acl d ON d.defaclnamespace=s.schema_oid
 CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) a WHERE a.grantee<>d.defaclrole
)
SELECT (SELECT schema_oid IS NOT NULL AND owner_oid IS NOT NULL AND pg_catalog.cardinality(role_names)<=128
 AND EXISTS(SELECT 1 FROM pg_catalog.pg_roles protected_owner WHERE protected_owner.oid=owner_oid
   AND NOT protected_owner.rolcanlogin AND NOT protected_owner.rolsuper AND NOT protected_owner.rolcreatedb
   AND NOT protected_owner.rolcreaterole AND NOT protected_owner.rolreplication AND NOT protected_owner.rolbypassrls)
 FROM closure_scope)
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s
   WHERE r.oid IS NULL OR r.rolsuper OR r.rolcreaterole
 OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'MEMBER') OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'USAGE')
 OR pg_catalog.pg_has_role(r.oid,s.owner_oid,'SET')
 OR EXISTS(SELECT 1 FROM pg_catalog.pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
   AND pg_catalog.pg_has_role(r.oid,elevated.oid,'MEMBER')))
 AND NOT EXISTS(SELECT 1 FROM native_acl a WHERE a.is_grantable OR a.privilege_type<>a.allowed
   OR NOT EXISTS(SELECT 1 FROM runtime_roles r WHERE r.oid=a.grantee))
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s
   WHERE NOT pg_catalog.has_schema_privilege(r.oid,s.schema_oid,'USAGE') OR pg_catalog.has_schema_privilege(r.oid,s.schema_oid,'CREATE')
   OR EXISTS(SELECT 1 FROM family_native_relations c WHERE c.relnamespace=s.schema_oid AND c.relkind='r'
     AND (s.relation_names IS NULL OR c.relname=ANY(s.relation_names))
     AND (NOT pg_catalog.has_table_privilege(r.oid,c.oid,'SELECT')
       OR pg_catalog.has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
       OR pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))) AS closed

 ) privileges ON true
 LEFT JOIN LATERAL (
  SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(index_row) ORDER BY index_row.index_oid),'[]'::pg_catalog.jsonb) AS rows FROM (
   SELECT relation.relname AS table_name,index_record.indexrelid::bigint AS index_oid,
    index_record.indrelid::bigint AS table_oid,index_record.indisvalid,index_record.indisready,index_record.indislive,
    index_record.indisprimary,index_record.indisunique,index_record.indnkeyatts,index_record.indnatts,method.amname,
    index_record.indpred IS NULL AS no_predicate,index_record.indexprs IS NULL AS no_expressions,
    ARRAY(SELECT attribute.attname FROM pg_catalog.unnest(index_record.indkey::smallint[]) WITH ORDINALITY key(attnum,position)
     JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid=relation.oid AND attribute.attnum=key.attnum
     ORDER BY key.position) AS columns,pg_catalog.pg_get_indexdef(index_record.indexrelid) AS definition
   FROM family_native_relations relation JOIN pg_catalog.pg_index index_record ON index_record.indrelid=relation.oid
   JOIN pg_catalog.pg_class index_relation ON index_relation.oid=index_record.indexrelid
   JOIN pg_catalog.pg_am method ON method.oid=index_relation.relam
   WHERE relation.relnamespace=catalog.schema_oid AND relation.relkind='r'
  ) index_row
 ) indexes ON true
), approved_fingerprints AS MATERIALIZED (
 SELECT record_kind,record_key,
  pg_catalog.octet_length(pg_catalog.jsonb_build_array(record_kind,record_key,record_revision,custom_revision,record_json)::pg_catalog.text) AS document_bytes,
  CASE WHEN pg_catalog.octet_length(pg_catalog.jsonb_build_array(record_kind,record_key,record_revision,custom_revision,record_json)::pg_catalog.text)<=$3
   THEN pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(record_kind,record_key,record_revision,custom_revision,record_json)::pg_catalog.text,
    'UTF8')),'hex') END AS fingerprint,
  CASE WHEN record_kind='membership' AND record_json->'archived'='false'::pg_catalog.jsonb
   THEN CASE WHEN pg_catalog.jsonb_typeof(record_json->'memberships_json')='array'
    THEN pg_catalog.jsonb_array_length(record_json->'memberships_json') END ELSE 0 END AS total_rows
 FROM "PIN_CONTROL_SCHEMA".registry_approved_record
 WHERE approved_revision=(SELECT approved_custom_revision FROM chosen)
), approved_bounds AS MATERIALIZED (
 SELECT coalesce(pg_catalog.max(document_bytes),0)<=$3 AND pg_catalog.count(*)*64+
  pg_catalog.octet_length(pg_catalog.jsonb_build_array($1::pg_catalog.text,(SELECT schema_oid::pg_catalog.oid FROM control),
   (SELECT approved_custom_revision FROM chosen),'')::pg_catalog.text)<=$3 AS is_bounded
 FROM approved_fingerprints
), approved AS MATERIALIZED (
 SELECT bounds.is_bounded,
  pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array($1::pg_catalog.text,(SELECT schema_oid::pg_catalog.oid FROM control),
   (SELECT approved_custom_revision FROM chosen),coalesce((SELECT pg_catalog.string_agg(fingerprint,''
    ORDER BY record_kind COLLATE pg_catalog."C",record_key COLLATE pg_catalog."C")
    FROM approved_fingerprints WHERE bounds.is_bounded),''))::pg_catalog.text,'UTF8')),'hex') AS generation_id,
  (SELECT pg_catalog.sum(total_rows) FROM approved_fingerprints)::bigint AS total_rows
 FROM approved_bounds bounds
), control_tables AS MATERIALIZED (
 SELECT relation.relname,relation.oid::bigint,relation.relowner::bigint,relation.relkind::pg_catalog.text,
  relation.relpersistence::pg_catalog.text,relation.relrowsecurity,relation.relforcerowsecurity,relation.relispartition,
  relation.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap') AS heap,
  EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid) AS inherited,
  EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=relation.oid) AS rules,
  ARRAY(SELECT attribute.attname::pg_catalog.text FROM pg_catalog.pg_attribute attribute
   WHERE attribute.attrelid=relation.oid AND attribute.attnum>0 AND NOT attribute.attisdropped
   ORDER BY attribute.attnum LIMIT 257) AS columns
 FROM pg_catalog.pg_class relation WHERE relation.relnamespace=(SELECT schema_oid FROM control)
  AND relation.relname=ANY(ARRAY['network_membership_candidate','network_serving_manifest','registry_approved_record',
    'network_candidate_custody','network_serving_control'])
), pin_metadata AS MATERIALIZED (
 SELECT
  (SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(family_metadata) ORDER BY candidate_id),'[]'::pg_catalog.jsonb)
   FROM family_metadata) AS families,
  (SELECT coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(control_tables) ORDER BY relname),'[]'::pg_catalog.jsonb)
   FROM control_tables) AS control_tables,pg_catalog.to_jsonb(approved) AS approved
 FROM approved
), indexed_base_input AS MATERIALIZED (
 SELECT (chosen.address->'table'->>'oid')::pg_catalog.oid AS relation_oid,
  chosen.address->'namespace'->>'name' AS schema_name,'entity_address_unified'::pg_catalog.text AS table_name,
  ARRAY(SELECT DISTINCT name FROM (
   SELECT pg_catalog.unnest(actors.descriptor_roles) AS name UNION SELECT current_user::pg_catalog.text
  ) subjects ORDER BY name LIMIT 34) AS subjects,actors.runtime_roles,actors.descriptor_roles
 FROM chosen CROSS JOIN pin_metadata
 CROSS JOIN LATERAL (SELECT
  ARRAY(SELECT pg_catalog.jsonb_array_elements_text(chosen.receipt_json->'runtime_roles') LIMIT 129) AS runtime_roles,
  ARRAY(SELECT pg_catalog.jsonb_array_elements_text(chosen.address->'runtime_roles') LIMIT 33) AS descriptor_roles
 ) actors
 WHERE chosen.address->>'contract'='network_indexed_address_artifact.postgres.v1'
), indexed_actor_safety AS MATERIALIZED (
 SELECT
  (SELECT pg_catalog.count(*)=pg_catalog.cardinality(base.descriptor_roles) AND pg_catalog.bool_and(actor.oid IS NOT NULL
   AND NOT actor.rolsuper AND NOT actor.rolcreaterole AND NOT actor.rolcreatedb
   AND NOT actor.rolreplication AND NOT actor.rolbypassrls
   AND NOT pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'MEMBER')
   AND NOT pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'USAGE') AND NOT pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'SET')
   AND NOT pg_catalog.pg_has_role(actor.oid,physical.schema_owner_oid,'MEMBER')
   AND NOT pg_catalog.pg_has_role(actor.oid,physical.schema_owner_oid,'SET')
   AND NOT pg_catalog.has_schema_privilege(actor.oid,physical.schema_oid,'CREATE')
   AND pg_catalog.has_schema_privilege(actor.oid,physical.schema_oid,'USAGE')
   AND pg_catalog.has_table_privilege(actor.oid,physical.oid,'SELECT')
   AND NOT pg_catalog.has_table_privilege(actor.oid,physical.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   AND NOT pg_catalog.has_any_column_privilege(actor.oid,physical.oid,'INSERT,UPDATE,REFERENCES')
   AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles elevated WHERE
    (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb OR elevated.rolreplication OR elevated.rolbypassrls)
    AND (pg_catalog.pg_has_role(actor.oid,elevated.oid,'MEMBER') OR pg_catalog.pg_has_role(actor.oid,elevated.oid,'SET'))))
   FROM pg_catalog.unnest(base.descriptor_roles) requested(name)
   LEFT JOIN pg_catalog.pg_roles actor ON actor.rolname=requested.name) AS descriptor_roles_safe,
  NOT EXISTS(SELECT FROM pg_catalog.pg_roles principal WHERE EXISTS(SELECT 1 FROM pg_catalog.pg_roles login WHERE login.rolcanlogin AND NOT login.rolsuper AND NOT pg_catalog.pg_has_role(login.oid,CAST(physical.owner_oid AS pg_catalog.oid),'USAGE') AND pg_catalog.pg_has_role(login.oid,principal.oid,'MEMBER')) AND (
   pg_catalog.has_schema_privilege(principal.oid,physical.schema_oid,'CREATE')
   OR pg_catalog.pg_has_role(principal.oid,physical.schema_owner_oid,'MEMBER')
   OR pg_catalog.pg_has_role(principal.oid,physical.schema_owner_oid,'SET')
   OR pg_catalog.has_table_privilege(principal.oid,physical.oid,'MAINTAIN')
   OR pg_catalog.has_table_privilege(principal.oid,physical.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES')
   OR pg_catalog.has_any_column_privilege(principal.oid,physical.oid,'INSERT,UPDATE,REFERENCES')
  )) AS ordinary_mutation_closed,
  owner.oid IS NOT NULL AND physical.owner_oid=physical.schema_owner_oid
   AND NOT (owner.rolcanlogin OR owner.rolsuper OR owner.rolcreaterole OR owner.rolcreatedb
    OR owner.rolreplication OR owner.rolbypassrls) AS owner_safe,
  NOT EXISTS(SELECT FROM pg_catalog.unnest(base.runtime_roles) selected(name)
   LEFT JOIN pg_catalog.pg_roles actor ON actor.rolname=selected.name
   WHERE actor.oid IS NULL OR actor.rolsuper OR actor.rolcreaterole OR actor.rolcreatedb
    OR actor.rolreplication OR actor.rolbypassrls
    OR pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'MEMBER') OR pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'USAGE')
    OR pg_catalog.pg_has_role(actor.oid,physical.owner_oid,'SET') OR pg_catalog.pg_has_role(actor.oid,physical.schema_owner_oid,'MEMBER')
    OR pg_catalog.pg_has_role(actor.oid,physical.schema_owner_oid,'USAGE') OR pg_catalog.pg_has_role(actor.oid,physical.schema_owner_oid,'SET')
    OR pg_catalog.has_schema_privilege(actor.oid,physical.schema_oid,'CREATE')
    OR pg_catalog.has_parameter_privilege(actor.oid,'session_replication_role','SET')
    OR pg_catalog.has_table_privilege(actor.oid,physical.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
    OR pg_catalog.has_any_column_privilege(actor.oid,physical.oid,'INSERT,UPDATE,REFERENCES')
  ) AND NOT EXISTS(SELECT FROM pg_catalog.pg_proc procedure,
   pg_catalog.unnest(base.runtime_roles) selected(name) JOIN pg_catalog.pg_roles actor ON actor.rolname=selected.name
   WHERE procedure.prosecdef AND procedure.prorettype NOT IN ('pg_catalog.trigger'::pg_catalog.regtype,'pg_catalog.event_trigger'::pg_catalog.regtype)
    AND pg_catalog.has_schema_privilege(actor.oid,procedure.pronamespace,'USAGE')
    AND pg_catalog.has_function_privilege(actor.oid,procedure.oid,'EXECUTE')
    AND EXISTS(SELECT FROM pg_catalog.pg_roles definer WHERE definer.oid=procedure.proowner
     AND (definer.rolsuper OR definer.rolcreaterole OR pg_catalog.pg_has_role(definer.oid,physical.owner_oid,'MEMBER')
      OR pg_catalog.pg_has_role(definer.oid,physical.owner_oid,'SET') OR pg_catalog.pg_has_role(definer.oid,physical.schema_owner_oid,'MEMBER')
      OR pg_catalog.pg_has_role(definer.oid,physical.schema_owner_oid,'SET')))
  ) AS runtime_closure_safe
 FROM indexed_base_input base
 JOIN LATERAL (
  SELECT relation.oid,relation.relowner AS owner_oid,namespace.oid AS schema_oid,
   namespace.nspowner AS schema_owner_oid
  FROM pg_catalog.pg_class relation JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
  WHERE relation.oid=base.relation_oid AND namespace.nspname=base.schema_name AND relation.relname=base.table_name
 ) physical ON true LEFT JOIN pg_catalog.pg_roles owner ON owner.oid=physical.owner_oid
), indexed_path_before AS MATERIALIZED (
  SELECT pg_catalog.current_setting('search_path') AS original_path FROM pin_metadata
),
indexed_canonical_path AS MATERIALIZED (
  SELECT original_path,pg_catalog.set_config('search_path','pg_catalog,pg_temp',true) AS applied_path
  FROM indexed_path_before
),
indexed_base_scope AS MATERIALIZED (
  SELECT b.*,p.original_path
  FROM indexed_base_input b CROSS JOIN indexed_canonical_path p
  WHERE p.applied_path='pg_catalog,pg_temp'
),
indexed_canonical_evidence AS MATERIALIZED (
  SELECT b.relation_oid,b.schema_name,b.table_name,b.subjects,b.original_path,
    (SELECT pg_catalog.to_jsonb(r) FROM (SELECT c.oid::pg_catalog.int8,c.relname,pg_catalog.pg_relation_filenode(c.oid)::pg_catalog.int8 AS relfilenode,c.relowner::pg_catalog.int8 AS owner_oid,n.oid::pg_catalog.int8 AS schema_oid,n.nspname AS schema_name,n.nspowner::pg_catalog.int8 AS schema_owner_oid,c.relkind::pg_catalog.text,c.relpersistence::pg_catalog.text,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS rules,EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=b.relation_oid::pg_catalog.oid) r) AS physical,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT attribute.attnum, attribute.attname, pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS type, attribute.attnotnull, attribute.attgenerated::pg_catalog.text, collation_namespace.nspname AS collation_schema, collation_name.collname AS collation_name, attribute.attidentity::pg_catalog.text, pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true) AS default_expression FROM pg_catalog.pg_attribute AS attribute LEFT JOIN pg_catalog.pg_attrdef AS default_value ON default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum LEFT JOIN pg_catalog.pg_collation AS collation_name ON collation_name.oid=attribute.attcollation LEFT JOIN pg_catalog.pg_namespace AS collation_namespace ON collation_namespace.oid=collation_name.collnamespace WHERE attribute.attrelid=b.relation_oid AND attribute.attnum>0 AND NOT attribute.attisdropped ORDER BY attribute.attnum) r),'[]'::pg_catalog.jsonb) AS shape_columns,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT constraint_row.contype, constraint_row.condeferrable, constraint_row.condeferred, constraint_row.convalidated, constraint_row.conkey::pg_catalog.text AS key_columns, constraint_row.confkey::pg_catalog.text AS referenced_columns, referenced_relation.relname AS referenced_table, referenced_namespace.nspname=b.schema_name AS referenced_in_archive_schema, pg_catalog.pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true) AS check_expression FROM pg_catalog.pg_constraint AS constraint_row LEFT JOIN pg_catalog.pg_class AS referenced_relation ON referenced_relation.oid=constraint_row.confrelid LEFT JOIN pg_catalog.pg_namespace AS referenced_namespace ON referenced_namespace.oid=referenced_relation.relnamespace WHERE constraint_row.conrelid=b.relation_oid ORDER BY constraint_row.contype, constraint_row.conkey::pg_catalog.text, constraint_row.confkey::pg_catalog.text, referenced_relation.relname) r),'[]'::pg_catalog.jsonb) AS shape_constraints,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT index_row.indisunique, index_row.indisprimary, index_row.indimmediate, index_row.indisvalid, index_row.indnkeyatts, index_row.indnatts, access_method.amname AS method, pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid, true) AS predicate, pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid, true) AS expressions, index_row.indkey::pg_catalog.text AS keys, index_row.indoption::pg_catalog.text AS options, (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('position', key_position.position, 'attribute_number', index_row.indkey[key_position.position], 'collation_schema', collation_namespace.nspname, 'collation_name', collation_name.collname, 'opclass_schema', opclass_namespace.nspname, 'opclass_name', opclass_name.opcname) ORDER BY key_position.position) FROM pg_catalog.generate_subscripts(index_row.indkey, 1) AS key_position(position) LEFT JOIN pg_catalog.pg_collation AS collation_name ON collation_name.oid=index_row.indcollation[key_position.position] LEFT JOIN pg_catalog.pg_namespace AS collation_namespace ON collation_namespace.oid=collation_name.collnamespace LEFT JOIN pg_catalog.pg_opclass AS opclass_name ON opclass_name.oid=index_row.indclass[key_position.position] LEFT JOIN pg_catalog.pg_namespace AS opclass_namespace ON opclass_namespace.oid=opclass_name.opcnamespace) AS key_attributes FROM pg_catalog.pg_index AS index_row JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid=index_row.indexrelid JOIN pg_catalog.pg_am AS access_method ON access_method.oid=index_relation.relam WHERE index_row.indrelid=b.relation_oid ORDER BY index_row.indisprimary DESC, index_row.indisunique DESC, access_method.amname, index_row.indkey::pg_catalog.text, index_row.indoption::pg_catalog.text) r),'[]'::pg_catalog.jsonb) AS shape_indexes,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT i.indexrelid::pg_catalog.int8 AS index_oid,i.indrelid::pg_catalog.int8 AS relation_oid,c.relowner::pg_catalog.int8 AS owner_oid,pg_catalog.pg_relation_filenode(c.oid)::pg_catalog.int8 AS relfilenode,am.amname AS method,i.indisunique,i.indisprimary,i.indnkeyatts,i.indnatts,i.indkey::pg_catalog.text,i.indclass::pg_catalog.text,i.indcollation::pg_catalog.text,i.indoption::pg_catalog.text,pg_catalog.pg_get_expr(i.indexprs,i.indrelid) AS expression,pg_catalog.pg_get_expr(i.indpred,i.indrelid) AS predicate,i.indisvalid,i.indisready,i.indislive FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE i.indrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) ORDER BY i.indexrelid) r),'[]'::pg_catalog.jsonb) AS catalog_indexes,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT a.attrelid::pg_catalog.int8 AS relation_oid,a.attnum,a.attname,a.atttypid::pg_catalog.int8 AS type_oid,a.atttypmod AS type_modifier,a.attnotnull AS not_null,a.attidentity::pg_catalog.text AS identity,a.attgenerated::pg_catalog.text AS generated,pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS default_expression FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attrelid,a.attnum) r),'[]'::pg_catalog.jsonb) AS catalog_columns,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (SELECT conrelid::pg_catalog.int8 AS relation_oid,contype::pg_catalog.text AS kind,conkey::pg_catalog.text AS columns,convalidated AS validated,condeferrable AS deferrable,condeferred AS deferred,pg_catalog.pg_get_constraintdef(oid) AS definition FROM pg_catalog.pg_constraint WHERE conrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) AND contype<>'n' ORDER BY conrelid,contype,conkey,definition) r),'[]'::pg_catalog.jsonb) AS catalog_constraints,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (
            SELECT index_relation.oid::pg_catalog.int8 AS index_oid,index_relation.relname,index_record.indrelid::pg_catalog.int8 AS indrelid,
                projected.oid::pg_catalog.int8 AS table_oid,index_record.indisvalid,index_record.indisready,index_record.indislive,
                pg_catalog.pg_get_indexdef(index_relation.oid) AS index_definition
            FROM pg_catalog.pg_class projected JOIN pg_catalog.pg_namespace namespace ON namespace.oid=projected.relnamespace
            JOIN pg_catalog.pg_index index_record ON index_record.indrelid=projected.oid
            JOIN pg_catalog.pg_class index_relation ON index_relation.oid=index_record.indexrelid
            WHERE projected.oid=b.relation_oid AND projected.relname='entity_address_unified'
        ) r),'[]'::pg_catalog.jsonb) AS qualified_index_records,
    (SELECT pg_catalog.count(*)=pg_catalog.cardinality(ARRAY[b.relation_oid]::pg_catalog.oid[]) AND pg_catalog.bool_and(c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid)) FROM pg_catalog.pg_class c WHERE c.oid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[])) AS native_structure_safe,
    (SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=idx.relam WHERE i.indrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR am.amname NOT IN ('btree','gin','gist') OR EXISTS(SELECT 1 FROM pg_catalog.pg_opclass op WHERE op.oid=ANY(i.indclass) AND op.opcnamespace<>'pg_catalog'::pg_catalog.regnamespace AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext JOIN pg_catalog.pg_extension e ON e.oid=ext.refobjid WHERE ext.classid='pg_catalog.pg_opclass'::pg_catalog.regclass AND ext.objid=op.oid AND ext.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass AND ext.deptype='e' AND e.extname=ANY(ARRAY['postgis','intarray','btree_gin','btree_gist','pg_trgm']::pg_catalog.text[]))) OR EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=idx.oid AND ((d.refclassid='pg_catalog.pg_proc'::pg_catalog.regclass AND EXISTS(SELECT 1 FROM pg_catalog.pg_proc p WHERE p.oid=d.refobjid AND p.pronamespace<>'pg_catalog'::pg_catalog.regnamespace)) OR (d.refclassid='pg_catalog.pg_operator'::pg_catalog.regclass AND EXISTS(SELECT 1 FROM pg_catalog.pg_operator op WHERE op.oid=d.refobjid AND op.oprnamespace<>'pg_catalog'::pg_catalog.regnamespace))) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext JOIN pg_catalog.pg_extension e ON e.oid=ext.refobjid WHERE ext.classid=d.refclassid AND ext.objid=d.refobjid AND ext.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass AND ext.deptype='e' AND e.extname=ANY(ARRAY['postgis','intarray','btree_gin','btree_gist','pg_trgm']::pg_catalog.text[]))))) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type ty ON ty.oid=a.atttypid WHERE a.attrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) AND a.attnum>0 AND NOT a.attisdropped AND (a.attgenerated NOT IN ('','s') OR (ty.typnamespace<>'pg_catalog'::pg_catalog.regnamespace AND NOT (ty.oid=ANY(ARRAY[]::pg_catalog.oid[])) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext JOIN pg_catalog.pg_extension e ON e.oid=ext.refobjid WHERE ext.classid='pg_catalog.pg_type'::pg_catalog.regclass AND ext.objid=ty.oid AND ext.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass AND ext.deptype='e' AND e.extname='postgis' AND ty.typname IN ('geometry','geography')))))) AS native_execution_safe,
    (WITH objects(classid,objid) AS (SELECT 'pg_catalog.pg_class'::pg_catalog.regclass,indexrelid FROM pg_catalog.pg_index WHERE indrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) UNION ALL SELECT 'pg_catalog.pg_attrdef'::pg_catalog.regclass,oid FROM pg_catalog.pg_attrdef WHERE adrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[]) UNION ALL SELECT 'pg_catalog.pg_constraint'::pg_catalog.regclass,oid FROM pg_catalog.pg_constraint WHERE conrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[])), referenced AS (SELECT d.refclassid,d.refobjid FROM objects x JOIN pg_catalog.pg_depend d ON (d.classid,d.objid)=(x.classid,x.objid)), families AS (SELECT c.opcowner,f.opfowner,f.oid FROM pg_catalog.pg_index i,LATERAL pg_catalog.unnest(i.indclass) cls(oid) JOIN pg_catalog.pg_opclass c ON c.oid=cls.oid JOIN pg_catalog.pg_opfamily f ON f.oid=c.opcfamily WHERE i.indrelid=ANY(ARRAY[b.relation_oid]::pg_catalog.oid[])), owners(oid) AS (SELECT p.proowner FROM referenced d JOIN pg_catalog.pg_proc p ON d.refclassid='pg_catalog.pg_proc'::pg_catalog.regclass AND p.oid=d.refobjid UNION SELECT o.oprowner FROM referenced d JOIN pg_catalog.pg_operator o ON d.refclassid='pg_catalog.pg_operator'::pg_catalog.regclass AND o.oid=d.refobjid UNION SELECT p.proowner FROM referenced d JOIN pg_catalog.pg_operator o ON d.refclassid='pg_catalog.pg_operator'::pg_catalog.regclass AND o.oid=d.refobjid JOIN pg_catalog.pg_proc p ON p.oid=o.oprcode UNION SELECT opcowner FROM families UNION SELECT opfowner FROM families UNION SELECT p.proowner FROM families f JOIN pg_catalog.pg_amproc a ON a.amprocfamily=f.oid JOIN pg_catalog.pg_proc p ON p.oid=a.amproc UNION SELECT o.oprowner FROM families f JOIN pg_catalog.pg_amop a ON a.amopfamily=f.oid JOIN pg_catalog.pg_operator o ON o.oid=a.amopopr UNION SELECT p.proowner FROM families f JOIN pg_catalog.pg_amop a ON a.amopfamily=f.oid JOIN pg_catalog.pg_operator o ON o.oid=a.amopopr JOIN pg_catalog.pg_proc p ON p.oid=o.oprcode), subjects AS (SELECT r.oid FROM pg_catalog.unnest(b.subjects) requested(name) LEFT JOIN pg_catalog.pg_roles r ON r.rolname=requested.name) SELECT (SELECT pg_catalog.count(*)=pg_catalog.cardinality(b.subjects) AND pg_catalog.bool_and(oid IS NOT NULL) FROM subjects) AND NOT EXISTS(SELECT 1 FROM subjects s CROSS JOIN owners o WHERE pg_catalog.pg_has_role(s.oid,o.oid,'MEMBER'))) AS native_subjects_safe
  FROM indexed_base_scope b
),
indexed_legacy_path AS MATERIALIZED (
  SELECT c.*,pg_catalog.set_config('search_path','pg_catalog,public',true) AS applied_legacy_path
  FROM indexed_canonical_evidence c
),
indexed_legacy_evidence AS MATERIALIZED (
  SELECT b.*,
    COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)) FROM (
            SELECT index_relation.oid::pg_catalog.int8 AS index_oid,index_relation.relname,index_record.indrelid::pg_catalog.int8 AS indrelid,
                projected.oid::pg_catalog.int8 AS table_oid,index_record.indisvalid,index_record.indisready,index_record.indislive,
                pg_catalog.pg_get_indexdef(index_relation.oid) AS index_definition
            FROM pg_catalog.pg_class projected JOIN pg_catalog.pg_namespace namespace ON namespace.oid=projected.relnamespace
            JOIN pg_catalog.pg_index index_record ON index_record.indrelid=projected.oid
            JOIN pg_catalog.pg_class index_relation ON index_relation.oid=index_record.indexrelid
            WHERE projected.oid=b.relation_oid AND projected.relname='entity_address_unified'
        ) r),'[]'::pg_catalog.jsonb) AS legacy_index_records
  FROM indexed_legacy_path b
  WHERE b.applied_legacy_path='pg_catalog,public'
),
indexed_base_evidence AS MATERIALIZED (
  -- Return the complete evidence only after restoring the exact original path.
  -- LEFT JOIN also restores if the bounded input unexpectedly yields no row.
  SELECT e.*,pg_catalog.set_config('search_path',p.original_path,true) AS restored_path
  FROM indexed_path_before p LEFT JOIN indexed_legacy_evidence e ON true
)
SELECT chosen.candidate::pg_catalog.text AS candidate_json,chosen.manifest::pg_catalog.text AS manifest_json,chosen.created_at,
 chosen.receipt_json::pg_catalog.text AS receipt_json,chosen.seal_sha256,pg_catalog.to_jsonb(control)::pg_catalog.text AS control_json,
 pin_metadata.families::pg_catalog.text AS families_json,pin_metadata.control_tables::pg_catalog.text AS control_tables_json,
 pin_metadata.approved::pg_catalog.text AS approved_json,
 (pg_catalog.to_jsonb(indexed_base_evidence)||pg_catalog.jsonb_build_object('relation_oid',indexed_base_evidence.relation_oid::bigint))::pg_catalog.text AS indexed_base_json,
 (SELECT pg_catalog.to_jsonb(indexed_actor_safety)::pg_catalog.text FROM indexed_actor_safety) AS indexed_actor_safety_json,
 pg_catalog.current_setting('transaction_isolation') AS isolation,pg_catalog.current_setting('transaction_read_only') AS read_only,
 session_user::pg_catalog.text AS session_user,current_user::pg_catalog.text AS current_user,pg_catalog.pg_backend_pid() AS backend_pid,
 (SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=current_user) AS role_oid,
 pg_catalog.host(pg_catalog.inet_server_addr()) AS server_address,pg_catalog.inet_server_port() AS server_port,
 pg_catalog.current_database() AS database_name,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 (SELECT pg_catalog.count(*)=5 AND pg_catalog.bool_and(
   pg_catalog.to_regclass(pg_catalog.format('%I.%I',$1,table_pin.relname))::pg_catalog.oid::bigint=table_pin.oid
   AND EXISTS(SELECT FROM pg_catalog.pg_locks held WHERE held.pid=pg_catalog.pg_backend_pid()
    AND held.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
    AND held.locktype='relation' AND held.relation::bigint=table_pin.oid AND held.granted
    AND held.mode=ANY(ARRAY['AccessShareLock','RowShareLock','RowExclusiveLock','ShareUpdateExclusiveLock',
      'ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock']))
  ) FROM control_tables table_pin) AS control_consumed,
 (SELECT pg_catalog.count(*) FROM edge_ids) AS edge_candidate_count,
 chosen.address::pg_catalog.text AS indexed_descriptor_json
FROM indexed_base_evidence LEFT JOIN chosen ON true LEFT JOIN control ON true LEFT JOIN pin_metadata ON true
"""


def api_serving_pin_statement(control_schema):
    """Quote only the admitted namespace; all runtime coordinates remain parameters."""
    return _METADATA_SQL.replace('"PIN_CONTROL_SCHEMA"', _identifier(control_schema))
