# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Extend existing low-volume retirement guards without adding entrypoints."""

from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as guards
from db import migration_provider_directory_terminal_root_retirement_v2 as v2


def _replace(sql, old, new):
    if sql.count(old) != 1:
        raise RuntimeError("provider_directory_first_retirement_predecessor_changed")
    return sql.replace(old, new, 1)


def evidence_sql(schema):
    """Include an explicit null predecessor in first-acquisition evidence."""
    sql = evidence.evidence_function_sql(schema)
    sql = _replace(
        sql,
        "JOIN " + evidence._qf(schema, "provider_directory_endpoint_dataset") + " AS predecessor",
        "LEFT JOIN " + evidence._qf(schema, "provider_directory_endpoint_dataset") + " AS predecessor",
    )
    return _replace(
        sql,
        "(pg_catalog.to_jsonb(predecessor))::text",
        "(COALESCE(pg_catalog.to_jsonb(predecessor), 'null'::jsonb))::text",
    )


def eligible_sql(schema):
    """Allow the existing eligibility check to examine first acquisitions."""
    sql = guards.eligible_function_sql(schema)
    sql = _replace(sql, "AND parent.previous_dataset_id IS NOT NULL", "")
    old = f"""AND EXISTS (
            SELECT 1 FROM {evidence._qf(schema, "provider_directory_endpoint_dataset")} AS predecessor
             WHERE predecessor.dataset_id = parent.previous_dataset_id
               AND predecessor.endpoint_id = parent.endpoint_id
        )"""
    return _replace(
        sql, old, old.replace("AND EXISTS (", "AND (parent.previous_dataset_id IS NULL OR EXISTS (", 1) + ")"
    )


def parent_sql(schema):
    """Require absent publication history when retiring without a predecessor."""
    sql = v2.parent_guard_function_sql(schema)
    old = v2._transition_predicates_sql(
        schema,
        marker_key=evidence.MARKER,
        eligible_function=guards.ELIGIBLE_FUNCTION,
        marker_function=guards.MARKER_FUNCTION,
        evidence_function=evidence.EVIDENCE_FUNCTION,
    )
    dataset = evidence._qf(schema, "provider_directory_endpoint_dataset")
    predecessor = f"""OR NOT EXISTS (SELECT 1 FROM {dataset} AS predecessor
             WHERE predecessor.dataset_id = OLD.previous_dataset_id
               AND predecessor.endpoint_id = OLD.endpoint_id
               AND predecessor.status = 'published'
               AND predecessor.is_current IS TRUE)"""
    first_acquisition = f"""OR (CASE WHEN OLD.previous_dataset_id IS NULL THEN
            EXISTS (SELECT 1 FROM {dataset} AS serving
                     WHERE serving.endpoint_id = OLD.endpoint_id
                       AND (serving.is_current OR serving.published_at IS NOT NULL))
            ELSE NOT EXISTS (SELECT 1 FROM {dataset} AS predecessor
                 WHERE predecessor.dataset_id = OLD.previous_dataset_id
                   AND predecessor.endpoint_id = OLD.endpoint_id
                   AND predecessor.status = 'published'
                   AND predecessor.is_current IS TRUE) END)"""
    return _replace(sql, old, _replace(old, predecessor, first_acquisition))


def run_sql(schema):
    """Serialize lineage changes with retirement without adding table privileges."""
    sql = guards.import_run_guard_function_sql(schema)
    declaration = "DECLARE old_run text; new_run text; retry_parent text;"
    sql = _replace(sql, "AS $function$\n    BEGIN", "AS $function$\n    " + declaration + "\n    BEGIN")
    fence = f"""
        IF TG_OP <> 'INSERT' THEN old_run := OLD.run_id; END IF;
        IF TG_OP <> 'DELETE' THEN
            new_run := NEW.run_id; retry_parent := NEW.retry_of_run_id;
        END IF;
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
                   'provider-directory-terminal-root-retirement:' || selected.run_id, 0))
          FROM (SELECT DISTINCT run_id
                  FROM pg_catalog.unnest(ARRAY[old_run, new_run, retry_parent]) AS run_id
                 WHERE run_id IS NOT NULL ORDER BY run_id) AS selected;
    """
    return _replace(sql, "        IF (TG_OP <> 'INSERT' AND", fence + "\n        IF (TG_OP <> 'INSERT' AND")


def replacements(schema):
    """Each tuple preserves the signature, owner, ACL and dependent triggers."""
    return (
        (evidence.evidence_function_sql(schema), evidence_sql(schema)),
        (guards.eligible_function_sql(schema), eligible_sql(schema)),
        (v2.parent_guard_function_sql(schema), parent_sql(schema)),
        (guards.import_run_guard_function_sql(schema), run_sql(schema)),
    )


def guarded_replacement(old_sql, new_sql):
    """Refuse unknown installed bodies; CREATE OR REPLACE retains object identity."""
    declaration = old_sql.split("CREATE FUNCTION ", 1)[1].split(")", 1)[0]
    name, arguments = declaration.split("(", 1)
    types = ",".join(argument.split()[-1] for argument in arguments.split(",") if argument.strip())
    signature = name + "(" + types + ")"
    old_body = old_sql.split("AS $function$\n", 1)[1].rsplit("\n    $function$;", 1)[0]
    new_body = new_sql.split("AS $function$\n", 1)[1].rsplit("\n    $function$;", 1)[0]
    quote = evidence._ql
    return (
        f"""
    DO $migration$
    DECLARE matches integer;
    BEGIN
        SELECT count(*) INTO matches FROM pg_proc p
         WHERE p.oid = to_regprocedure({quote(signature)})
           AND p.prosecdef AND p.prokind='f'
           AND btrim(regexp_replace(p.prosrc, '\\s+', ' ', 'g')) IN (
               btrim(regexp_replace({quote(old_body)}, '\\s+', ' ', 'g')),
               btrim(regexp_replace({quote(new_body)}, '\\s+', ' ', 'g')));
        IF matches <> 1 THEN
            RAISE EXCEPTION 'provider_directory_first_retirement_function_changed' USING ERRCODE='55000';
        END IF;
    END;
    $migration$;
    """,
        new_sql.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1),
    )
