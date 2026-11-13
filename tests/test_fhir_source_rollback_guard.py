# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static lifecycle changes preserve data checks without database-local authority."""

from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from db import migration_cms_noncurrent_seal as cms
from db import migration_fhir_source_rollback as rollback


@pytest.mark.parametrize("schema", ["sample", "Mixed_Catalog23"])
@pytest.mark.parametrize("version", [1, 2, 3])
def test_data_alternatives_change_only_the_final_lifecycle_refusal(schema, version):
    original = cms.lifecycle_guard_bodies(schema)[0]
    bodies = rollback.rollback_guard_bodies(schema)
    assert bodies[0] == original
    compact = cms._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    subset = compact._terminal()._subset()
    lifecycle = subset._endpoint_dataset_lifecycle_sql(
        subset._SUBSET_IMMUTABLE_COMPARISON_SQL, subset_metadata_is_immutable=True
    )
    cms_transition, inverse = rollback._cms_data_transition_sql(), rollback._inverse_transition_sql()
    transition = {1: cms_transition, 2: inverse, 3: "(" + cms_transition + " OR " + inverse + ")"}[version]
    replacement = "IF (" + lifecycle + ") AND " + transition + " IS DISTINCT FROM TRUE THEN"
    assert bodies[version].count(replacement) == 1
    assert bodies[version].replace(replacement, "IF " + lifecycle + " THEN", 1) == original
    assert bodies[version].count("RAISE EXCEPTION") == original.count("RAISE EXCEPTION")


def test_inverse_requires_exact_old_and_new_lifecycle_and_all_other_data():
    assert (
        rollback._inverse_transition_sql()
        == """(
            OLD.status='superseded' AND OLD.is_current IS FALSE
            AND OLD.published_at IS NOT NULL AND OLD.superseded_at>=OLD.published_at
            AND NEW.status='published' AND NEW.is_current IS TRUE AND NEW.superseded_at IS NULL
            AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
            AND (pg_catalog.to_jsonb(NEW)-ARRAY['status','is_current','superseded_at'])=
                (pg_catalog.to_jsonb(OLD)-ARRAY['status','is_current','superseded_at'])
        )"""
    )


def test_cms_removes_only_the_two_caller_identity_conditions():
    identity = (
        "            EXISTS(SELECT FROM pg_catalog.pg_roles actor WHERE actor.rolname=session_user AND actor.rolsuper)\n"
        "            AND pg_catalog.current_setting('role')='none'\n"
        "            AND "
    )
    assert rollback._cms_data_transition_sql() == cms._cms_transition_sql().replace(identity, "            ", 1)
    for mode in ("transaction_read_only", "transaction_isolation", "session_replication_role"):
        assert mode in rollback._cms_data_transition_sql()


@pytest.mark.parametrize("schema", ["sample", "Mixed_Catalog23"])
def test_static_ddl_replaces_only_the_existing_named_guard(schema):
    statement = rollback.rollback_guard_sql(schema)
    compact = cms._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    original_statement = compact._endpoint_guard_sql(schema, compact=True)
    body = compact._function_body(original_statement)
    existing = f'ALTER FUNCTION "{schema}"."{cms.GUARD}"() SET search_path=pg_catalog;\n'
    assert statement == existing + original_statement.replace(body, rollback.rollback_guard_bodies(schema)[3], 1)
    assert statement.count("CREATE OR REPLACE FUNCTION") == 1
    for forbidden in (
        "CREATE TRIGGER",
        "DROP TRIGGER",
        "GRANT ",
        "REVOKE ",
        "pg_roles",
        "session_user",
        "current_user",
        "pg_proc",
        "pg_get_functiondef",
        "database_oid",
        "relation_oid",
        "backend_pid",
        "transaction_id",
        "source_rollback_context",
        "old_sha256",
        "new_sha256",
        "current_setting('role')",
    ):
        assert forbidden not in statement


@pytest.mark.parametrize("schema", ["", "bad-name", "schema;SELECT 1", 'a"b'])
def test_static_ddl_rejects_invalid_qualified_names(schema):
    with pytest.raises(ValueError):
        rollback.rollback_guard_sql(schema)
    with pytest.raises(ValueError):
        rollback.rollback_guard_bodies(schema)


def test_static_body_rejects_an_unknown_lifecycle_anchor():
    with pytest.raises(RuntimeError, match="rollback_predecessor_changed"):
        rollback._lifecycle_body("BEGIN RETURN NEW; END", rollback._inverse_transition_sql())


@pytest.mark.parametrize("active", [False, True])
def test_installer_uses_only_static_ddl_inside_the_existing_transaction(active):
    connection = SimpleNamespace(
        in_transaction=lambda: active,
        exec_driver_sql=Mock(),
        execute=Mock(side_effect=AssertionError("catalog census is forbidden")),
        scalar=Mock(side_effect=AssertionError("catalog census is forbidden")),
    )
    if active:
        rollback.upgrade_fhir_source_rollback_guard(connection, "sample", expected=object())
        existing, replacement = rollback.rollback_guard_sql("sample").split(";\n", 1)
        assert connection.exec_driver_sql.call_args_list == [call(existing), call(replacement)]
    else:
        with pytest.raises(RuntimeError, match="rollback_transaction_required"):
            rollback.upgrade_fhir_source_rollback_guard(connection, "sample")
        connection.exec_driver_sql.assert_not_called()
    connection.execute.assert_not_called()
    connection.scalar.assert_not_called()


def test_missing_existing_routine_stops_before_the_replacement_statement():
    original = RuntimeError("existing routine is missing")
    connection = SimpleNamespace(in_transaction=lambda: True, exec_driver_sql=Mock(side_effect=original))
    with pytest.raises(RuntimeError) as caught:
        rollback.upgrade_fhir_source_rollback_guard(connection, "sample")
    assert caught.value is original
    existing = rollback.rollback_guard_sql("sample").split(";\n", 1)[0]
    connection.exec_driver_sql.assert_called_once_with(existing)
