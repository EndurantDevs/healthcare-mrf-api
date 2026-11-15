# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Data-only inverse of the existing canonical header lifecycle transition.

Existing PostgreSQL UPDATE privileges authorize header writes. The caller retains
its locked baseline, pointer CAS, downstream exclusions and once-only inverse.
Normal enrollment replaces the existing guard without creating hooks or grants.
"""

from functools import lru_cache

from db.migration_cms_noncurrent_seal import (
    GUARD,
    _cms_transition_sql,
    _migration,
    lifecycle_guard_bodies,
)
from db.plan_release_storage import storage_identifier, storage_relation


def _require(condition, code):
    if not condition:
        raise RuntimeError("fhir_source_rollback_" + code)


def _inverse_transition_sql():
    """Permit only exact lifecycle restoration while every immutable field stays equal."""
    return """(
            OLD.status='superseded' AND OLD.is_current IS FALSE
            AND OLD.published_at IS NOT NULL AND OLD.superseded_at>=OLD.published_at
            AND NEW.status='published' AND NEW.is_current IS TRUE AND NEW.superseded_at IS NULL
            AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
            AND (pg_catalog.to_jsonb(NEW)-ARRAY['status','is_current','superseded_at'])=
                (pg_catalog.to_jsonb(OLD)-ARRAY['status','is_current','superseded_at'])
        )"""


def _cms_data_transition_sql():
    """Preserve the CMS data and transaction predicates without caller identity checks."""
    original = _cms_transition_sql()
    identity = (
        "            EXISTS(SELECT FROM pg_catalog.pg_roles actor WHERE actor.rolname=session_user AND actor.rolsuper)\n"
        "            AND pg_catalog.current_setting('role')='none'\n"
        "            AND "
    )
    _require(original.count(identity) == 1, "cms_transition_changed")
    return original.replace(identity, "            ", 1)


def _lifecycle_body(original, transition):
    """Change only the final lifecycle refusal in the reviewed static guard body."""
    compact = _migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    subset = compact._terminal()._subset()
    lifecycle = subset._endpoint_dataset_lifecycle_sql(
        subset._SUBSET_IMMUTABLE_COMPARISON_SQL, subset_metadata_is_immutable=True
    )
    anchor = "IF " + lifecycle + " THEN"
    _require(original.count(anchor) == 1, "rollback_predecessor_changed")
    return original.replace(anchor, "IF (" + lifecycle + ") AND " + transition + " IS DISTINCT FROM TRUE THEN", 1)


@lru_cache(maxsize=8)
def rollback_guard_bodies(schema):
    """Render the base, CMS, inverse and combined data-only lifecycle alternatives."""
    storage_identifier(schema)
    original = lifecycle_guard_bodies(schema)[0]
    cms, inverse = _cms_data_transition_sql(), _inverse_transition_sql()
    return (
        original,
        _lifecycle_body(original, cms),
        _lifecycle_body(original, inverse),
        _lifecycle_body(original, "(" + cms + " OR " + inverse + ")"),
    )


def rollback_guard_sql(schema):
    """Replace the already installed named guard using only checked-in static DDL."""
    compact = _migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    statement = compact._endpoint_guard_sql(schema, compact=True)
    original = compact._function_body(statement)
    _require(statement.count(original) == 1, "rollback_definition_changed")
    # ALTER fails if the existing routine is missing; CREATE OR REPLACE then
    # preserves its PostgreSQL owner, privileges and existing trigger attachment.
    existing = "ALTER FUNCTION " + storage_relation(schema, GUARD) + "() SET search_path=pg_catalog;\n"
    return existing + statement.replace(original, rollback_guard_bodies(schema)[3], 1)


def upgrade_fhir_source_rollback_guard(connection, schema, *, expected=None):
    """Replace the existing guard in the caller's transaction without catalog census.

    The historical expected keyword remains accepted without inspecting its
    database-local identifiers. PostgreSQL enforces the caller's DDL rights.
    """
    _require(connection.in_transaction(), "rollback_transaction_required")
    existing, replacement = rollback_guard_sql(schema).split(";\n", 1)
    connection.exec_driver_sql(existing)
    connection.exec_driver_sql(replacement)
