# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen catalog queries for legacy PTG root validation."""

_ATTRIBUTE_SQL = """
    SELECT attribute_record.attrelid::bigint AS root_oid,
           attribute_record.attnum,
           attribute_record.attname,
           format_type(
               attribute_record.atttypid,
               attribute_record.atttypmod
           ) AS formatted_type,
           attribute_record.attnotnull,
           attribute_record.attidentity,
           attribute_record.attgenerated
      FROM pg_attribute AS attribute_record
     WHERE attribute_record.attrelid = ANY(CAST(:root_oids AS oid[]))
       AND attribute_record.attnum > 0
       AND NOT attribute_record.attisdropped
     ORDER BY root_oid, attribute_record.attnum
"""
_CONSTRAINT_SQL = """
    SELECT constraint_record.conrelid::bigint AS root_oid,
           constraint_record.conname,
           constraint_record.contype,
           pg_get_constraintdef(constraint_record.oid, true) AS definition
      FROM pg_constraint AS constraint_record
     WHERE constraint_record.conrelid = ANY(CAST(:root_oids AS oid[]))
     ORDER BY root_oid, constraint_record.conname
"""
_INDEX_SQL = """
    SELECT index_record.indrelid::bigint AS root_oid,
           index_record.indexrelid::bigint AS dependent_oid,
           index_relation.relname AS dependent_name,
           index_relation.relkind AS dependent_kind,
           index_relation.relnamespace::bigint AS dependent_namespace_oid,
           index_relation.relowner::bigint AS dependent_owner_oid,
           pg_get_indexdef(index_record.indexrelid) AS definition
      FROM pg_index AS index_record
      JOIN pg_class AS index_relation
        ON index_relation.oid = index_record.indexrelid
     WHERE index_record.indrelid = ANY(CAST(:root_oids AS oid[]))
     ORDER BY root_oid, dependent_name, dependent_oid
"""
_SEQUENCE_SQL = """
    SELECT dependency_record.refobjid::bigint AS root_oid,
           sequence_record.oid::bigint AS dependent_oid,
           sequence_record.relname AS dependent_name,
           sequence_record.relkind AS dependent_kind,
           sequence_record.relnamespace::bigint AS dependent_namespace_oid,
           sequence_record.relowner::bigint AS dependent_owner_oid,
           dependency_record.deptype
      FROM pg_depend AS dependency_record
      JOIN pg_class AS sequence_record
        ON sequence_record.oid = dependency_record.objid
     WHERE dependency_record.refobjid = ANY(CAST(:root_oids AS oid[]))
       AND dependency_record.classid = 'pg_class'::regclass
       AND dependency_record.refclassid = 'pg_class'::regclass
       AND dependency_record.refobjsubid > 0
       AND sequence_record.relkind = 'S'
       AND dependency_record.deptype IN ('a', 'i')
     ORDER BY root_oid, dependent_name, dependent_oid
"""
_INHERITANCE_SQL = """
    SELECT inheritance_record.inhrelid::bigint AS child_oid,
           inheritance_record.inhparent::bigint AS parent_oid
      FROM pg_inherits AS inheritance_record
     WHERE inheritance_record.inhrelid = ANY(CAST(:root_oids AS oid[]))
        OR inheritance_record.inhparent = ANY(CAST(:root_oids AS oid[]))
     ORDER BY child_oid, parent_oid
"""
_TRIGGER_SQL = """
    SELECT trigger_record.tgrelid::bigint AS root_oid,
           trigger_record.oid::bigint AS trigger_oid,
           trigger_record.tgname AS trigger_name,
           trigger_record.tgenabled,
           trigger_record.tgtype,
           function_record.oid::bigint AS function_oid,
           function_schema.nspname AS function_schema,
           function_record.proname AS function_name,
           pg_get_triggerdef(trigger_record.oid, true) AS definition,
           pg_get_functiondef(function_record.oid) AS function_definition
      FROM pg_trigger AS trigger_record
      JOIN pg_proc AS function_record
        ON function_record.oid = trigger_record.tgfoid
      JOIN pg_namespace AS function_schema
        ON function_schema.oid = function_record.pronamespace
     WHERE trigger_record.tgrelid = ANY(CAST(:root_oids AS oid[]))
       AND NOT trigger_record.tgisinternal
     ORDER BY root_oid, trigger_name, trigger_oid
"""
_RULE_SQL = """
    SELECT rewrite_record.ev_class::bigint AS root_oid,
           rewrite_record.oid::bigint AS rule_oid,
           rewrite_record.rulename AS rule_name,
           rewrite_record.ev_type,
           rewrite_record.ev_enabled,
           rewrite_record.is_instead,
           pg_get_ruledef(rewrite_record.oid, true) AS definition
      FROM pg_rewrite AS rewrite_record
     WHERE rewrite_record.ev_class = ANY(CAST(:root_oids AS oid[]))
     ORDER BY root_oid, rule_name, rule_oid
"""
_EXTERNAL_DEPENDENCY_SQL = """
    WITH root_target AS (
        SELECT relation_record.oid AS root_oid,
               relation_record.reltype AS root_type_oid,
               type_record.typarray AS root_array_type_oid
          FROM pg_class AS relation_record
          JOIN pg_type AS type_record
            ON type_record.oid = relation_record.reltype
         WHERE relation_record.oid = ANY(CAST(:root_oids AS oid[]))
    )
    SELECT root_target.root_oid::bigint AS root_oid,
           dependency_record.classid::bigint AS dependent_class_oid,
           dependency_record.objid::bigint AS dependent_oid,
           dependency_record.objsubid AS dependent_sub_id,
           dependency_record.refclassid::bigint AS referenced_class_oid,
           dependency_record.refobjid::bigint AS referenced_oid,
           dependency_record.deptype,
           pg_describe_object(
               dependency_record.classid,
               dependency_record.objid,
               dependency_record.objsubid
           ) AS dependent_name
      FROM root_target
      JOIN pg_depend AS dependency_record
        ON (
             dependency_record.refclassid = 'pg_class'::regclass
             AND dependency_record.refobjid = root_target.root_oid
        )
        OR (
             dependency_record.refclassid = 'pg_type'::regclass
             AND dependency_record.refobjid = root_target.root_type_oid
        )
     WHERE NOT (
            dependency_record.refclassid = 'pg_type'::regclass
            AND dependency_record.classid = 'pg_type'::regclass
            AND dependency_record.objid = root_target.root_array_type_oid
            AND dependency_record.objsubid = 0
            AND dependency_record.deptype = 'i'
       )
       AND NOT (
            dependency_record.classid = 'pg_class'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_index AS index_record
                 WHERE index_record.indrelid = root_target.root_oid
                   AND index_record.indexrelid = dependency_record.objid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_class'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_class AS root_relation
                 WHERE root_relation.oid = root_target.root_oid
                   AND root_relation.reltoastrelid = dependency_record.objid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_class'::regclass
            AND dependency_record.refobjsubid > 0
            AND dependency_record.deptype IN ('a', 'i')
            AND EXISTS (
                SELECT 1
                  FROM pg_class AS sequence_record
                 WHERE sequence_record.oid = dependency_record.objid
                   AND sequence_record.relkind = 'S'
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_constraint'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_constraint AS constraint_record
                 WHERE constraint_record.oid = dependency_record.objid
                   AND constraint_record.conrelid = root_target.root_oid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_attrdef'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_attrdef AS default_record
                 WHERE default_record.oid = dependency_record.objid
                   AND default_record.adrelid = root_target.root_oid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_trigger'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_trigger AS trigger_record
                 WHERE trigger_record.oid = dependency_record.objid
                   AND trigger_record.tgrelid = root_target.root_oid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_rewrite'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_rewrite AS rewrite_record
                 WHERE rewrite_record.oid = dependency_record.objid
                   AND rewrite_record.ev_class = root_target.root_oid
            )
       )
       AND NOT (
            dependency_record.classid = 'pg_type'::regclass
            AND EXISTS (
                SELECT 1
                  FROM pg_type AS type_record
                 WHERE type_record.oid = dependency_record.objid
                   AND type_record.typrelid = root_target.root_oid
            )
       )
     ORDER BY root_oid, referenced_class_oid, dependent_name, dependent_oid
"""
_ROOT_SCHEMA_QUERIES = (
    _ATTRIBUTE_SQL,
    _CONSTRAINT_SQL,
    _INDEX_SQL,
    _SEQUENCE_SQL,
    _INHERITANCE_SQL,
    _TRIGGER_SQL,
    _RULE_SQL,
    _EXTERNAL_DEPENDENCY_SQL,
)
