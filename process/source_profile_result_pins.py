# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native row seals shared by archive pins, scoped adoption and source GC."""

from __future__ import annotations

import json
import re
from uuid import UUID

from sqlalchemy import text

TABLE = "provider_profile_source_pin"
PAYLOAD_TABLES = (
    "provider_profile_import_run",
    "provider_profile_artifact",
    "provider_profile_source_record",
    "provider_profile_fact",
)


def pin_guard_statements(schema):
    """Install idempotent guards using only the configured native schema."""
    if not isinstance(schema, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("source profile pin schema is invalid")
    function = f'"{schema}".provider_profile_pinned_run_guard'
    yield f"""CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE selected_run text;
        BEGIN
            FOR selected_run IN SELECT DISTINCT value FROM unnest(ARRAY[
                CASE WHEN TG_OP <> 'INSERT' THEN OLD.run_id END,
                CASE WHEN TG_OP <> 'DELETE' THEN NEW.run_id END]) value
                WHERE value IS NOT NULL ORDER BY value
            LOOP
                PERFORM pg_advisory_xact_lock(hashtext('profile-run-seal:' || TG_TABLE_SCHEMA || '.' || selected_run));
                IF EXISTS(SELECT 1 FROM "{schema}".{TABLE} WHERE run_id=selected_run) THEN
                    RAISE EXCEPTION 'source profile retained run is pinned' USING ERRCODE='55000';
                END IF;
            END LOOP;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END $$"""
    for name in PAYLOAD_TABLES:
        yield f"""DO $$ BEGIN
            IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='"{schema}".{name}'::regclass
                AND tgname='provider_profile_pinned_run_guard' AND NOT tgisinternal) THEN
                CREATE TRIGGER provider_profile_pinned_run_guard BEFORE INSERT OR UPDATE OR DELETE
                    ON "{schema}".{name} FOR EACH ROW EXECUTE FUNCTION {function}();
            END IF;
        END $$"""
    truncate_function = f'"{schema}".provider_profile_pinned_truncate_guard'
    yield f"""CREATE OR REPLACE FUNCTION {truncate_function}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS(SELECT 1 FROM "{schema}".{TABLE}) THEN
                RAISE EXCEPTION 'source profile retained run is pinned' USING ERRCODE='55000';
            END IF;
            RETURN NULL;
        END $$"""
    for name in PAYLOAD_TABLES:
        yield f"""DO $$ BEGIN
            IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='"{schema}".{name}'::regclass
                AND tgname='provider_profile_pinned_truncate_guard' AND NOT tgisinternal) THEN
                CREATE TRIGGER provider_profile_pinned_truncate_guard BEFORE TRUNCATE
                    ON "{schema}".{name} FOR EACH STATEMENT EXECUTE FUNCTION {truncate_function}();
            END IF;
        END $$"""


def pin_policy_statements(schema):
    """Separate ordinary export writes from protected-owner adoption seals."""
    if not isinstance(schema, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("source profile pin schema is invalid")
    table = f'"{schema}".{TABLE}'
    owner = f"(SELECT relowner FROM pg_catalog.pg_class WHERE oid='{table}'::regclass)"
    publisher = (
        f"current_user <> pg_catalog.pg_get_userbyid({owner}) AND pg_catalog.pg_has_role(current_user,{owner},'USAGE')"
    )
    yield f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"
    yield f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY"
    yield f"CREATE POLICY source_profile_pin_read ON {table} FOR SELECT USING (true)"
    yield (
        f"CREATE POLICY source_profile_pin_export ON {table} FOR ALL "
        "USING (purpose = 'export') WITH CHECK (purpose = 'export')"
    )
    yield (
        f"CREATE POLICY source_profile_pin_adoption ON {table} FOR ALL "
        f"USING (purpose = 'adoption' AND {publisher}) "
        f"WITH CHECK (purpose = 'adoption' AND {publisher})"
    )


async def lock_run(session, schema, run_id):
    """Serialize row changes with seal acquisition, including in-flight writers."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"profile-run-seal:{schema}.{run_id}"}
    )


async def record_pin(session, *, schema, source_key, run_id, pin_id, purpose, authority):
    """Record exact local authority once; duplicate IDs never replace prior owners."""
    if not isinstance(pin_id, UUID) or purpose not in {"export", "adoption"}:
        raise ValueError("source profile pin identity is invalid")
    await lock_run(session, schema, run_id)
    await session.execute(
        text(
            f'INSERT INTO "{schema}".{TABLE} '
            "(pin_id,source_key,run_id,purpose,authority_json) "
            "VALUES (:pin,:source,:run,:purpose,CAST(:authority AS json))"
        ),
        {
            "pin": str(pin_id),
            "source": source_key,
            "run": run_id,
            "purpose": purpose,
            "authority": json.dumps(authority, sort_keys=True),
        },
    )
