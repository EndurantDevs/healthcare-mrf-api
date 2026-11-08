# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inactive, checked extension of the existing header guard for an exact inverse.

The trusted Source operator owns baseline, native CAS, retention and downstream
checks. This helper installs nothing automatically and changes no privileges.
"""

from functools import lru_cache

from db.migration_cms_noncurrent_seal import (
    _cms_transition_sql,
    _migration,
    _snapshot,
    lifecycle_guard_bodies,
)
from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL

SELECTOR = "app.fhir_source_header_rollback"
CONTRACT = "fhir-source-header-rollback-v1"
_CONTEXT_FIELDS = (
    "contract",
    "database_oid",
    "relation_oid",
    "backend_pid",
    "transaction_id",
    "dataset_id",
    "endpoint_id",
    "old_sha256",
    "new_sha256",
    "context_sha256",
)


def _require(condition, code):
    if not condition:
        raise RuntimeError("fhir_source_rollback_" + code)


def _authorization_sql(schema):
    """Keep this a row-specific trusted-admin exception, never a caller capability."""
    fields = "ARRAY[" + ",".join("'" + name + "'" for name in _CONTEXT_FIELDS) + "]::text[]"
    return f"""(
            TG_OP='UPDATE' AND TG_WHEN='BEFORE' AND TG_LEVEL='ROW'
            AND TG_TABLE_SCHEMA='{schema}' AND TG_TABLE_NAME='provider_directory_endpoint_dataset'
            AND EXISTS(SELECT FROM pg_catalog.pg_roles actor WHERE actor.rolname=session_user AND actor.rolsuper)
            AND pg_catalog.current_setting('role')='none'
            AND pg_catalog.current_setting('transaction_read_only')='off'
            AND pg_catalog.current_setting('transaction_isolation')='serializable'
            AND pg_catalog.current_setting('session_replication_role')='origin'
            AND OLD.status='superseded' AND OLD.is_current IS FALSE
            AND OLD.published_at IS NOT NULL AND OLD.superseded_at>=OLD.published_at
            AND NEW.status='published' AND NEW.is_current IS TRUE AND NEW.superseded_at IS NULL
            AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
            AND (pg_catalog.to_jsonb(NEW)-ARRAY['status','is_current','superseded_at'])=
                (pg_catalog.to_jsonb(OLD)-ARRAY['status','is_current','superseded_at'])
            AND pg_catalog.jsonb_typeof(source_rollback_context)='object'
            AND source_rollback_context ?& {fields}
            AND source_rollback_context-{fields}='{{}}'::jsonb
            AND source_rollback_context->>'contract'='{CONTRACT}'
            AND source_rollback_context->>'database_oid'=
                (SELECT oid::text FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
            AND source_rollback_context->>'relation_oid'=TG_RELID::text
            AND source_rollback_context->>'backend_pid'=pg_catalog.pg_backend_pid()::text
            AND source_rollback_context->>'transaction_id'=pg_catalog.pg_current_xact_id()::text
            AND source_rollback_context->>'dataset_id'=OLD.dataset_id
            AND source_rollback_context->>'endpoint_id'=OLD.endpoint_id
            AND source_rollback_context->>'old_sha256'=
                pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.to_jsonb(OLD)::text,'UTF8')),'hex')
            AND source_rollback_context->>'new_sha256'=
                pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.to_jsonb(NEW)::text,'UTF8')),'hex')
            AND source_rollback_context->>'context_sha256' ~ '^[0-9a-f]{{64}}$'
        )"""


def _extend_body(schema, original, *, cms):
    compact = _migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    subset = compact._terminal()._subset()
    lifecycle = subset._endpoint_dataset_lifecycle_sql(
        subset._SUBSET_IMMUTABLE_COMPARISON_SQL, subset_metadata_is_immutable=True
    )
    declaration = "        published_source_metadata jsonb;"
    anchor = (
        "IF (" + lifecycle + ") AND " + _cms_transition_sql() + " IS DISTINCT FROM TRUE THEN"
        if cms
        else "IF " + lifecycle + " THEN"
    )
    _require(original.count(declaration) == original.count(anchor) == 1, "rollback_predecessor_changed")
    original = original.replace(
        declaration,
        declaration + "\n        source_rollback_context jsonb;\n        source_rollback_authorized boolean := false;",
        1,
    )
    parse_context = f"""IF TG_OP='UPDATE' AND OLD.status='superseded' AND NEW.status='published'
           AND pg_catalog.length(pg_catalog.current_setting('{SELECTOR}',true)) BETWEEN 1 AND 4096 THEN
            BEGIN
                source_rollback_context := pg_catalog.current_setting('{SELECTOR}',true)::jsonb;
                source_rollback_authorized := {_authorization_sql(schema)};
            EXCEPTION WHEN data_exception THEN
                source_rollback_authorized := false;
            END;
        END IF;
        """
    allowed = "(" + _cms_transition_sql() + " OR source_rollback_authorized)" if cms else "source_rollback_authorized"
    return original.replace(
        anchor, parse_context + "IF (" + lifecycle + ") AND " + allowed + " IS DISTINCT FROM TRUE THEN", 1
    )


@lru_cache(maxsize=8)
def rollback_guard_bodies(schema):
    """Preserve either installed predecessor, including its existing CMS branch."""
    from db.plan_release_storage import storage_identifier

    storage_identifier(schema)
    old, cms = lifecycle_guard_bodies(schema)
    return old, cms, _extend_body(schema, old, cms=False), _extend_body(schema, cms, cms=True)


def upgrade_fhir_source_rollback_guard(connection, schema, *, expected):
    """Opt in only after shared consumer compatibility and native qualification.

    Preserve the exact routine, trigger, owner, ACL and comment. The caller owns
    the outer independent-admin transaction and accepted installation boundary.
    """
    _require(connection.in_transaction(), "rollback_transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "rollback_search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "rollback_original_admin_required")
    bodies = rollback_guard_bodies(schema)
    before, hooks = _snapshot(connection, schema, expected, bodies)
    if before["body"] in bodies[2:]:
        return
    after_body = bodies[bodies[:2].index(before["body"]) + 2]
    _require(before["definition"].count(before["body"]) == 1, "rollback_definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(before["body"], after_body, 1))
        after, after_hooks = _snapshot(connection, schema, expected, (after_body,))
        _require(
            after["identity"] == before["identity"]
            and after["description"] == before["description"]
            and after_hooks == hooks,
            "rollback_identity_changed",
        )
