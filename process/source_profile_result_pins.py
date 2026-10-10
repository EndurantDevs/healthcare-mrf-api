# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retained-run seals shared by archive pins, scoped adoption and source GC."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

TABLE = "provider_profile_source_pin"
ROLE_POLICY_ENVIRONMENT = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
PAYLOAD_TABLES = (
    "provider_profile_import_run",
    "provider_profile_artifact",
    "provider_profile_source_record",
    "provider_profile_fact",
)
_UNIQUE_KEYS_BY_TABLE = {
    "provider_profile_import_run": (("run_id",),),
    "provider_profile_artifact": (("artifact_id",), ("run_id", "source_key")),
    "provider_profile_source_record": (("record_id",), ("run_id", "source_key", "source_record_key")),
    "provider_profile_fact": (("fact_id",),),
}


def _require_authority(condition, message):
    if not condition:
        raise ValueError(message)


def _policy_object(pairs):
    policy_by_field = dict(pairs)
    _require_authority(len(policy_by_field) == len(pairs), "source profile role policy is invalid")
    return policy_by_field


def load_role_policy():
    """Read independent deployment policy, never role scope supplied by a receipt."""
    try:
        filename = os.environ[ROLE_POLICY_ENVIRONMENT]
        with Path(filename).open("rb") as stream:
            raw = stream.read(8193)
        _require_authority(len(raw) <= 8192, "source profile role policy is invalid")
        policy = json.loads(raw, object_pairs_hook=_policy_object)
        return _role_policy(policy)
    except KeyError, OSError, TypeError, ValueError:
        raise ValueError("source profile role policy is unavailable or invalid") from None


def _role_policy(policy):
    """Reuse the bounded namespace-policy shape while selecting only pin authority."""
    required_fields = {"owner_role", "migration_role", "runtime_roles", "preparation_owner_role"}
    optional_fields = {
        "credential_broker_role",
        "control_runtime_roles",
        "preparation_owner_schema_create",
        "catalog_pin_role",
    }
    _require_authority(
        isinstance(policy, dict) and required_fields <= set(policy) <= required_fields | optional_fields,
        "source profile role policy is invalid",
    )
    runtime_roles, control_roles = policy["runtime_roles"], policy.get("control_runtime_roles", [])
    _require_authority(
        isinstance(runtime_roles, list)
        and 1 <= len(runtime_roles) <= 16
        and isinstance(control_roles, list)
        and ("control_runtime_roles" not in policy or bool(control_roles))
        and all(isinstance(role, str) and role in runtime_roles for role in control_roles)
        and len(set(control_roles)) == len(control_roles)
        and type(policy.get("preparation_owner_schema_create", False)) is bool,
        "source profile role policy is invalid",
    )
    names = [policy[key] for key in ("owner_role", "migration_role", "preparation_owner_role")]
    names.extend(policy[key] for key in ("credential_broker_role", "catalog_pin_role") if key in policy)
    names.extend(runtime_roles)
    _require_authority(
        all(isinstance(name, str) and re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", name) for name in names)
        and len(set(names)) == len(names),
        "source profile role policy is invalid",
    )
    return policy["preparation_owner_role"], tuple(runtime_roles)


async def require_role_principals(connection, owner_oid, runtime_role_oids):
    """Check the complete configured role closure, including non-inherited membership."""
    _require_authority(
        type(owner_oid) is int and 0 < owner_oid < 2**32,
        "snapshot_generation_owner_unprotected",
    )
    _require_authority(
        isinstance(runtime_role_oids, list)
        and 1 <= len(runtime_role_oids) <= 16
        and all(type(oid) is int and 0 < oid < 2**32 for oid in runtime_role_oids)
        and runtime_role_oids == sorted(set(runtime_role_oids)),
        "snapshot_runtime_role_unprotected",
    )
    owner = await connection.fetchrow(
        "SELECT rolcanlogin,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls "
        "FROM pg_catalog.pg_roles WHERE oid=$1",
        owner_oid,
    )
    _require_authority(
        owner is not None and all(flag is False for flag in owner.values()),
        "snapshot_generation_owner_unprotected",
    )
    roles = await connection.fetch(
        """SELECT r.oid AS role_oid,principal.oid AS principal_oid,
                  pg_catalog.pg_has_role(principal.oid,$1::oid,'MEMBER') AS owner_member,
                  pg_catalog.pg_has_role(principal.oid,d.datdba,'MEMBER') AS database_owner_member,
                  (principal.rolsuper OR principal.rolcreaterole OR principal.rolcreatedb
                   OR principal.rolreplication OR principal.rolbypassrls OR principal.rolname=ANY(ARRAY[
                       'pg_read_server_files','pg_write_server_files','pg_execute_server_program']::name[]))
                    AS elevated_member
           FROM pg_catalog.pg_roles r JOIN pg_catalog.pg_roles principal
             ON pg_catalog.pg_has_role(r.oid,principal.oid,'MEMBER')
           CROSS JOIN pg_catalog.pg_database d
           WHERE r.oid=ANY($2::oid[]) AND d.datname=pg_catalog.current_database()
           ORDER BY r.oid,principal.oid LIMIT 1025""",
        owner_oid,
        runtime_role_oids,
    )
    principal_oids = sorted({role["principal_oid"] for role in roles})
    _require_authority(
        len(roles) <= 1024
        and len(principal_oids) <= 64
        and sorted({role["role_oid"] for role in roles}) == runtime_role_oids
        and all(
            role[field] is False
            for role in roles
            for field in ("owner_member", "database_owner_member", "elevated_member")
        ),
        "snapshot_runtime_role_unprotected",
    )
    return principal_oids


async def require_pin_tables(connection, schema, owner_oid, principals):
    """Keep ordinary payload DML while rejecting ownership and pin-ledger bypasses."""
    names = [*PAYLOAD_TABLES, TABLE]
    relations = await connection.fetch(
        """SELECT c.relname,c.relowner,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,
                  c.relforcerowsecurity,
                  EXISTS(SELECT 1 FROM unnest($2::oid[]) r(oid)
                    WHERE pg_has_role(r.oid,n.nspowner,'MEMBER')
                    OR has_table_privilege(r.oid,c.oid,'TRIGGER,MAINTAIN')
                    OR (c.relname='provider_profile_source_pin' AND has_table_privilege(r.oid,c.oid,'TRUNCATE')))
                    AS unsafe
           FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname=$1 AND c.relname=ANY($3::text[])""",
        schema,
        principals,
        names,
    )
    _require_authority(
        len(relations) == len(names)
        and all(
            relation["relowner"] == owner_oid
            and relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not relation["relispartition"]
            and not relation["unsafe"]
            and relation["relforcerowsecurity"] == (relation["relname"] == TABLE)
            and relation["relrowsecurity"] == (relation["relname"] == TABLE)
            for relation in relations
        ),
        "source-profile native ownership is unprotected",
    )


async def require_pin_policies(connection, schema):
    """Require exactly the migration's read, ordinary export and protected adoption policies."""
    policies = await connection.fetch(
        """SELECT polname,polcmd,polpermissive,polroles,pg_get_expr(polqual,polrelid) AS qual,
                  pg_get_expr(polwithcheck,polrelid) AS check_expr
           FROM pg_policy WHERE polrelid=to_regclass($1) ORDER BY polname""",
        f'"{schema}".{TABLE}',
    )
    export = "((purpose)::text = 'export'::text)"
    owner = (
        f"( SELECT pg_class.relowner\n   FROM pg_class\n  WHERE (pg_class.oid = ('{schema}.{TABLE}'::regclass)::oid))"
    )
    adoption = (
        f"(((purpose)::text = 'adoption'::text) AND (CURRENT_USER <> pg_get_userbyid({owner})) "
        f"AND pg_has_role(CURRENT_USER, {owner}, 'USAGE'::text))"
    )
    _require_authority(
        [tuple(row.values()) for row in policies]
        == [
            ("source_profile_pin_adoption", b"*", True, [0], adoption, adoption),
            ("source_profile_pin_export", b"*", True, [0], export, export),
            ("source_profile_pin_read", b"r", True, [0], "true", None),
        ],
        "source-profile pin policy differs",
    )


async def require_pin_functions(connection, schema, owner_oid):
    """Compare existing installed guards with exact migration-owned invoker definitions."""
    for statement in statement_pin_guard_statements(schema):
        if not statement.startswith("CREATE OR REPLACE FUNCTION"):
            continue
        name = statement.split("FUNCTION ", 1)[1].split(" RETURNS", 1)[0]
        function = await connection.fetchrow(
            "SELECT proowner,prosecdef,proconfig,provolatile::text,prosrc FROM pg_proc WHERE oid=to_regprocedure($1)",
            name,
        )
        _require_authority(
            function is not None
            and tuple(function.values())
            == (
                owner_oid,
                False,
                ["search_path=pg_catalog, pg_temp"],
                "v",
                statement.split("AS $$", 1)[1].rsplit("$$", 1)[0],
            ),
            "source-profile pin guard authority differs",
        )


async def require_worker_authority(session, schema, expected_owner_oid):
    """Recheck the deployment-owned policy on the actual unprivileged worker connection."""
    owner_role, runtime_roles = load_role_policy()
    _require_authority(session.in_transaction(), "source-profile authority transaction is required")
    await session.execute(text("SELECT pg_current_xact_id()"))
    await session.execute(text("SET LOCAL search_path = pg_catalog, public, pg_temp"))
    connection = (await (await session.connection()).get_raw_connection()).driver_connection
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    runtime_oids = sorted(
        role_record["oid"]
        for role_record in await connection.fetch(
            "SELECT oid FROM pg_roles WHERE rolname=ANY($1::text[])", list(runtime_roles)
        )
    )
    _require_authority(
        owner_oid == expected_owner_oid and len(runtime_oids) == len(runtime_roles),
        "source-profile configured roles differ",
    )
    principals = await require_role_principals(connection, owner_oid, runtime_oids)
    identity = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "current_setting('session_replication_role') AS replication"
    )
    _require_authority(
        identity["session"] == identity["current"]
        and identity["current"] in runtime_roles
        and identity["replication"] == "origin",
        "source-profile process role differs",
    )
    await require_pin_tables(connection, schema, owner_oid, principals)
    await require_pin_policies(connection, schema)
    await require_pin_functions(connection, schema, owner_oid)


def _attachment_collision_checks(schema):
    """Parent indexes cover local rows; compare changed key sets with immutable children."""
    branches = []
    for name, keys in _UNIQUE_KEYS_BY_TABLE.items():
        collisions = []
        for columns in keys:
            predicate = " AND ".join(f'incoming."{column}"=stored."{column}"' for column in columns)
            collisions.append(
                f'EXISTS(SELECT 1 FROM profile_guard_new incoming JOIN "{schema}".{name} stored '
                f"ON {predicate} WHERE stored.tableoid<>TG_RELID)"
            )
        branch = "IF" if not branches else "ELSIF"
        branches.append(
            f"{branch} TG_TABLE_NAME='{name}' THEN\n"
            f"                IF {' OR '.join(collisions)} THEN\n"
            "                    RAISE EXCEPTION 'source profile attached key conflicts' USING ERRCODE='23505';\n"
            "                END IF;"
        )
    return "\n            ".join(branches) + "\n            END IF;"


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
    yield from _truncate_guard_statements(schema)


def statement_pin_guard_statements(schema):
    """Check affected run sets once per statement, preserving historical row DDL."""
    if not isinstance(schema, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("source profile pin schema is invalid")
    function = f'"{schema}".provider_profile_pinned_run_guard'
    yield f"""CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
        DECLARE selected_runs text[]; selected_run text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                selected_runs := ARRAY(SELECT DISTINCT run_id::text FROM profile_guard_new
                    WHERE run_id IS NOT NULL ORDER BY run_id::text);
            ELSIF TG_OP = 'DELETE' THEN
                selected_runs := ARRAY(SELECT DISTINCT run_id::text FROM profile_guard_old
                    WHERE run_id IS NOT NULL ORDER BY run_id::text);
            ELSE
                selected_runs := ARRAY(SELECT run_id::text FROM profile_guard_old WHERE run_id IS NOT NULL
                    UNION SELECT run_id::text FROM profile_guard_new WHERE run_id IS NOT NULL ORDER BY 1);
            END IF;
            IF pg_catalog.cardinality(selected_runs) = 0 THEN RETURN NULL; END IF;
            IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'source profile payload writes require read committed' USING ERRCODE='55000';
            END IF;
            FOREACH selected_run IN ARRAY selected_runs LOOP
                PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('profile-run-seal:' || TG_TABLE_SCHEMA || '.' || selected_run));
            END LOOP;
            IF EXISTS(SELECT 1 FROM "{schema}".{TABLE} WHERE run_id=ANY(selected_runs)) THEN
                RAISE EXCEPTION 'source profile retained run is pinned' USING ERRCODE='55000';
            END IF;
            IF TG_OP <> 'DELETE' AND EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent=TG_RELID) THEN
                {_attachment_collision_checks(schema)}
            END IF;
            RETURN NULL;
        END $$"""
    transitions = (
        ("insert", "NEW TABLE AS profile_guard_new"),
        ("update", "OLD TABLE AS profile_guard_old NEW TABLE AS profile_guard_new"),
        ("delete", "OLD TABLE AS profile_guard_old"),
    )
    for name in PAYLOAD_TABLES:
        yield f'DROP TRIGGER IF EXISTS provider_profile_pinned_run_guard ON "{schema}".{name}'
        for operation, transition in transitions:
            yield f"""CREATE TRIGGER provider_profile_pinned_run_guard_{operation} AFTER {operation.upper()}
                ON "{schema}".{name} REFERENCING {transition}
                FOR EACH STATEMENT EXECUTE FUNCTION {function}()"""
    yield from _truncate_guard_statements(schema, require_read_committed=True)
    yield from _attachment_pin_guard_statements(schema)


def _attachment_pin_guard_statements(schema):
    """Retain owning adoption seals until every receipt-bound child is detached."""
    function = f'"{schema}".provider_profile_attached_pin_guard'
    publication = "authority_json::jsonb #> '{validation,publication}'"
    owning = (
        "purpose='adoption' AND authority_json::jsonb->'created_here'='true'::jsonb "
        f"AND ({publication})->>'contract'='source-profile-attachment.v2'"
    )
    yield f"""CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
        DECLARE publications jsonb[];
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                publications := ARRAY(SELECT {publication} FROM "{schema}".{TABLE} WHERE {owning});
            ELSE
                publications := ARRAY(SELECT {publication} FROM profile_pin_old WHERE {owning});
            END IF;
            IF pg_catalog.cardinality(publications) = 0 THEN RETURN NULL; END IF;
            IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'source profile attachment seal writes require read committed' USING ERRCODE='55000';
            END IF;
            IF EXISTS(
                SELECT 1 FROM pg_catalog.unnest(publications) AS publication(value)
                CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(publication.value->'children') AS child(value)
                JOIN pg_catalog.pg_inherits inherited ON inherited.inhrelid::text=child.value->>2
            ) THEN
                RAISE EXCEPTION 'source profile attached adoption seal is retained' USING ERRCODE='55000';
            END IF;
            RETURN NULL;
        END $$"""
    for operation, transition in (
        ("update", "OLD TABLE AS profile_pin_old NEW TABLE AS profile_pin_new"),
        ("delete", "OLD TABLE AS profile_pin_old"),
    ):
        yield f"""CREATE TRIGGER provider_profile_attached_pin_guard_{operation} AFTER {operation.upper()}
            ON "{schema}".{TABLE} REFERENCING {transition}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}()"""
    yield f"""CREATE TRIGGER provider_profile_attached_pin_guard_truncate BEFORE TRUNCATE
        ON "{schema}".{TABLE} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"""


def _truncate_guard_statements(schema, *, require_read_committed=False):
    truncate_function = f'"{schema}".provider_profile_pinned_truncate_guard'
    isolation_guard = ""
    configuration = ""
    if require_read_committed:
        configuration = " SET search_path = pg_catalog, pg_temp"
        isolation_guard = """IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'source profile payload writes require read committed' USING ERRCODE='55000';
            END IF;
            """
    yield f"""CREATE OR REPLACE FUNCTION {truncate_function}() RETURNS trigger LANGUAGE plpgsql{configuration} AS $$
        BEGIN
            {isolation_guard}IF EXISTS(SELECT 1 FROM "{schema}".{TABLE}) THEN
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
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("source profile pin acquisition requires read committed")
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
