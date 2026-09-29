# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared Profile transition SQL and exact catalog recognition for its read-only receipt guards."""

import re

PROFILE_FIELDS = (
    "status",
    "operation",
    "control_generation",
    "generation_id",
    "selection_proof_id",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_vector_hash",
    "source_context_vector_hash",
    "executable_plan_hash",
    "profile_as_of",
    "evidence_target_oid",
    "profile_target_oid",
    "evidence_rows",
    "profile_rows",
)


def profile_transition_body(schema):
    """Return the migration's unchanged Profile transition body for installation and recognition."""
    old_sql = "jsonb_build_object(" + ",".join(f"'{field}',OLD.{field}" for field in PROFILE_FIELDS) + ")"
    new_sql = "jsonb_build_object(" + ",".join(f"'{field}',NEW.{field}" for field in PROFILE_FIELDS) + ")"
    return f"""BEGIN
        IF TG_OP='UPDATE' AND ({old_sql}) IS NOT DISTINCT FROM ({new_sql})
            AND OLD.source_vector_json IS NOT DISTINCT FROM NEW.source_vector_json THEN RETURN NULL; END IF;
        IF ((TG_OP<>'INSERT' AND OLD.source_vector_json @> '[{{"source_id":"cms-npd"}}]'::jsonb) OR
            (TG_OP<>'DELETE' AND NEW.source_vector_json @> '[{{"source_id":"cms-npd"}}]'::jsonb))
            OR EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_serving_receipt) THEN
            PERFORM {schema}.cms_serving_require_fresh('profile',CASE WHEN TG_OP='INSERT' THEN NULL ELSE {old_sql} END);
        END IF;
        RETURN NULL; END"""


_GUARD_CATALOG_SQL = """
SELECT t.tgname AS name,t.tgtype AS event_mask,t.tgenabled::text AS enabled,t.tgdeferrable AS deferrable,
       t.tginitdeferred AS initially_deferred,t.tgnargs AS argument_count,t.tgqual IS NULL AS no_condition,
       t.tgattr::text AS update_columns,n.nspname AS function_schema,p.proname AS function_name,
       p.oid::bigint AS function_oid,p.pronargs AS function_arguments,p.prosecdef AS security_definer,p.proconfig AS function_settings,
       p.prosrc AS function_body,l.lanname AS language,p.prorettype='trigger'::regtype AS returns_trigger
FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
JOIN pg_language l ON l.oid=p.prolang
WHERE t.tgrelid=:relation_oid AND NOT t.tgisinternal ORDER BY t.tgname
"""


def _normalized(body):
    return re.sub(r"\s+", " ", body).strip()


async def profile_receipt_guard_count(database, relation_by_field):
    """Preserve the zero-trigger legacy shape only before receipt installation."""
    if int(relation_by_field.get("user_trigger_count") or 0) == 0:
        schema = '"' + relation_by_field["schema_name"].replace('"', '""') + '"'
        if (
            await database.scalar(
                "SELECT to_regclass(:relation) IS NOT NULL", relation=f"{schema}.provider_directory_cms_serving_receipt"
            )
            is True
        ):
            raise RuntimeError("provider_directory_profile_capacity_receipt_guard_shape_changed")
        return 0
    await assert_profile_receipt_guards(database, relation_by_field)
    return 2


async def assert_profile_receipt_guards(database, relation_by_field, captured_triggers=None):
    """Reject additional, disabled, immediate, relocated or changed-body serving guards."""
    schema = relation_by_field["schema_name"]
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    expected_by_name = {
        "cms_serving_profile_transition": (29, True, profile_transition_body(quoted_schema)),
        "cms_serving_no_truncate": (
            34,
            False,
            f"""BEGIN
            IF EXISTS (SELECT 1 FROM {quoted_schema}.provider_directory_cms_serving_receipt) THEN
            RAISE EXCEPTION 'cms_serving_native_truncate_forbidden'; END IF; RETURN NULL; END""",
        ),
    }
    await _assert_guards(database, relation_by_field, expected_by_name, captured_triggers)


async def assert_common_receipt_guards(database, relation_by_field, captured_triggers=None):
    """Recognize only the installed immutable guard and read-only deferred result check."""
    schema = '"' + relation_by_field["schema_name"].replace('"', '""') + '"'
    table = f"{schema}.provider_directory_cms_serving_receipt"
    expected_by_name = {
        "cms_serving_receipt_immutable": (58, False, "BEGIN RAISE EXCEPTION 'cms_serving_receipt_immutable'; END"),
        "cms_serving_receipt_insert": (
            5,
            True,
            f"""DECLARE prior jsonb;
        BEGIN
            IF NEW.publication_xid<>pg_current_xact_id() OR NOT {schema}.cms_serving_receipt_matches(NEW.payload) THEN
                RAISE EXCEPTION 'cms_serving_receipt_result_mismatch'; END IF;
            IF NEW.predecessor_receipt_id IS NULL THEN
                IF NEW.payload->'expected_incumbent' IS DISTINCT FROM 'null'::jsonb THEN
                    RAISE EXCEPTION 'cms_serving_receipt_predecessor_mismatch'; END IF;
            ELSE
                SELECT payload INTO prior FROM {table} WHERE receipt_id=NEW.predecessor_receipt_id;
                IF prior IS NULL OR NEW.payload->'expected_incumbent' IS DISTINCT FROM
                     ((prior->'cms') - ARRAY['release_id','proof_version']) THEN
                    RAISE EXCEPTION 'cms_serving_receipt_predecessor_mismatch'; END IF;
            END IF;
            RETURN NULL;
        END""",
        ),
    }
    await _assert_guards(database, relation_by_field, expected_by_name, captured_triggers)


async def _assert_guards(database, relation_by_field, expected_by_name, captured_triggers=None):
    schema = relation_by_field["schema_name"]
    observed = await database.all(_GUARD_CATALOG_SQL, relation_oid=relation_by_field["relation_oid"])
    if len(observed) != len(expected_by_name):
        raise RuntimeError("provider_directory_profile_capacity_receipt_guard_shape_changed")
    if captured_triggers is not None and len(captured_triggers) != len(expected_by_name):
        raise RuntimeError("provider_directory_profile_capacity_receipt_guard_shape_changed")
    captured_by_name = {entry["trigger_name"]: entry for entry in captured_triggers or []}
    for guard_row in observed:
        guard_by_field = dict(guard_row._mapping)
        expected = expected_by_name.get(guard_by_field["name"])
        if expected is None or (
            guard_by_field["event_mask"] != expected[0]
            or guard_by_field["enabled"] not in {"O", "A"}
            or guard_by_field["deferrable"] is not expected[1]
            or guard_by_field["initially_deferred"] is not expected[1]
            or guard_by_field["argument_count"] != 0
            or guard_by_field["no_condition"] is not True
            or guard_by_field["update_columns"] != ""
            or guard_by_field["function_schema"] != schema
            or guard_by_field["function_name"] != guard_by_field["name"]
            or guard_by_field["function_arguments"] != 0
            or guard_by_field["security_definer"] is not False
            or guard_by_field["function_settings"] != ["search_path=pg_catalog"]
            or guard_by_field["language"] != "plpgsql"
            or guard_by_field["returns_trigger"] is not True
            or _normalized(guard_by_field["function_body"]) != _normalized(expected[2])
        ):
            raise RuntimeError("provider_directory_profile_capacity_receipt_guard_shape_changed")
        if captured_triggers is not None:
            captured = captured_by_name.get(guard_by_field["name"])
            if captured is None or (
                captured["tgtype"] != guard_by_field["event_mask"]
                or captured["trigger_enabled"] != guard_by_field["enabled"]
                or captured["trigger_function_oid"] != guard_by_field["function_oid"]
                or _normalized(captured["trigger_function_source"]) != _normalized(guard_by_field["function_body"])
            ):
                raise RuntimeError("provider_directory_profile_capacity_receipt_guard_shape_changed")
