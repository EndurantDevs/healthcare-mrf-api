# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normal Rooted reconciliation closes named writers before retiring private-input FKs."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db import migration_rooted_publication_freeze as freeze
from process.provider_directory_rooted_graph_bulk import _data_module

SCHEMA = "snapshot_schema"
OWNER = "protected_namespace_owner"
APPLICATIONS = ("old_builder", "source_worker", "catalog_publisher", "api_reader")


class _Rows(list):
    def mappings(self):
        return self

    def one(self):
        assert len(self) == 1
        return self[0]


def _relations():
    rooted = _data_module("20260811020000_provider_directory_rooted_graph_acquisition.py")
    roots = (
        rooted._ACQUISITION,
        rooted._WORK,
        rooted._RESOURCE,
        rooted._EDGE,
        rooted._TWIN_ATTEMPT,
        rooted._TWIN_ADMISSION,
        rooted._ROOTED_PROVENANCE,
        rooted._ROOTED_DATASET,
    )
    return [
        {"schema_name": SCHEMA, "relation_name": name, "columns": ["dataset_id", "status"]}
        for name in (
            *roots,
            *(root + "_legacy" for root in (rooted._WORK, rooted._RESOURCE, rooted._EDGE)),
            "work_partition_example",
        )
    ]


def _connection(*, failure=None, missing_root=False, logical_guard=True):
    statements = []
    relations = _relations()[1:] if missing_root else _relations()

    def execute(statement, parameters):
        query = str(statement)
        if query == freeze._ROOTED_WRITE_FAMILY_SQL:
            assert set(parameters) == {"roots"} and len(parameters["roots"]) == 11
            assert {
                f'"{SCHEMA}"."provider_directory_rooted_graph_{kind}_legacy"' for kind in ("work", "resource", "edge")
            } <= set(parameters["roots"])
            return _Rows(relations)
        if "pg_get_functiondef" in query:
            assert set(parameters) == {"signature"}
            return _Rows([{"body": "SELECT true;", "definition": "existing static validator"}])
        if "pg_get_function_identity_arguments" in query:
            assert parameters["schema"] == SCHEMA
            return _Rows([{"signature": f'"{SCHEMA}"."prepare_provider_directory_rooted_graph_storage"(text)'}])
        assert query == "SELECT to_regprocedure(:signature) IS NOT NULL"
        return SimpleNamespace(scalar_one=lambda: logical_guard)

    def apply(statement):
        statements.append(statement)
        if failure is not None and failure in statement:
            raise RuntimeError("synthetic DDL failure")

    return SimpleNamespace(
        in_transaction=lambda: True, execute=Mock(side_effect=execute), exec_driver_sql=Mock(side_effect=apply)
    ), statements


def _upgrade(connection):
    freeze.upgrade_rooted_publication_freeze(
        connection, SCHEMA, application_roles=APPLICATIONS, protected_owner_role=OWNER
    )


def test_upgrade_closes_header_parent_and_leaf_writes_before_fk_retirement():
    connection, statements = _connection()
    _upgrade(connection)
    locks = [statement for statement in statements if statement.startswith("LOCK TABLE")]
    lock = locks[-1]
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in lock and "ONLY " in lock
    for relation in _relations():
        name = f'"{SCHEMA}"."{relation["relation_name"]}"'
        assert name in lock
        assert f'ALTER TABLE {name} OWNER TO "{OWNER}"' in statements
        table_revoke = next(
            statement for statement in statements if statement.startswith("REVOKE INSERT,UPDATE,") and name in statement
        )
        column_revoke = next(
            statement
            for statement in statements
            if statement.startswith('REVOKE INSERT("dataset_id","status")') and name in statement
        )
        for statement in (table_revoke, column_revoke):
            assert "FROM PUBLIC," in statement
            assert all(f'"{role}"' in statement for role in APPLICATIONS)
        read_grant = next(
            statement for statement in statements if statement.startswith(f"GRANT SELECT ON TABLE {name} ")
        )
        assert "PUBLIC" not in read_grant and all(f'"{role}"' in read_grant for role in APPLICATIONS)
    drops = [statement for statement in statements if "DROP CONSTRAINT" in statement]
    header = f'"{SCHEMA}"."provider_directory_rooted_graph_dataset"'
    assert drops == [
        f'ALTER TABLE {header} DROP CONSTRAINT IF EXISTS "pd_rooted_graph_dataset_{kind}_fkey"'
        for kind in ("admission", "attempt", "publication")
    ]
    first_drop = statements.index(drops[0])
    assert all(
        statements.index(statement) < first_drop
        for statement in statements
        if statement.startswith(("REVOKE ", "ALTER TABLE")) and "DROP CONSTRAINT" not in statement
    )
    assert any(
        statement.startswith("REVOKE EXECUTE ON FUNCTION") and "FROM PUBLIC," in statement for statement in statements
    )
    assert all(
        statement.startswith("GRANT SELECT ON TABLE") for statement in statements if statement.startswith("GRANT ")
    )
    assert "SET LOCAL statement_timeout='3s'" in statements


def test_reconciliation_repeats_the_same_bounded_idempotent_ddl():
    connection, statements = _connection()
    _upgrade(connection)
    first_statements = list(statements)
    _upgrade(connection)
    assert statements == first_statements * 2
    assert len([statement for statement in first_statements if "DROP CONSTRAINT IF EXISTS" in statement]) == 3


def test_retirement_preserves_legacy_child_and_lifecycle_guards():
    connection, statements = _connection()
    _upgrade(connection)
    legacy_drops = [
        statement for statement in statements if "DROP TRIGGER" in statement and "pd_uhc_flex_" in statement
    ]
    assert legacy_drops == [
        f'DROP TRIGGER IF EXISTS "pd_uhc_flex_dataset_parent_guard" ON "{SCHEMA}"."provider_directory_endpoint_dataset"',
        f'DROP TRIGGER IF EXISTS "pd_uhc_flex_dataset_valid_guard" ON "{SCHEMA}"."provider_directory_uhc_flex_practitioner_dataset"',
    ]
    retained_guards = (
        "pd_uhc_flex_dataset_row_guard",
        "pd_uhc_flex_dataset_truncate_guard",
        "pd_uhc_flex_dataset_resource_row_guard",
        "pd_uhc_flex_dataset_resource_truncate_guard",
        "pd_uhc_flex_dataset_source_guard",
        "pd_uhc_flex_dataset_source_truncate_guard",
        "pd_uhc_flex_dataset_endpoint_guard",
        "pd_uhc_flex_dataset_endpoint_truncate_guard",
    )
    assert all(guard not in statement for guard in retained_guards for statement in statements)
    assert not any("DROP FUNCTION" in statement for statement in statements)


@pytest.mark.parametrize("kind", ["inactive", "no-applications", "no-owner", "owner-is-application"])
def test_missing_configured_write_closure_refuses_before_any_ddl(kind):
    connection, statements = _connection()
    applications, owner = APPLICATIONS, OWNER
    if kind == "inactive":
        connection.in_transaction = lambda: False
    elif kind == "no-applications":
        applications = ()
    elif kind == "no-owner":
        owner = None
    else:
        owner = APPLICATIONS[0]
    with pytest.raises(RuntimeError, match="rooted_publication_"):
        freeze.upgrade_rooted_publication_freeze(
            connection, SCHEMA, application_roles=applications, protected_owner_role=owner
        )
    assert statements == []
    connection.execute.assert_not_called()


def test_missing_named_root_refuses_before_constraint_retirement():
    connection, statements = _connection(missing_root=True)
    with pytest.raises(RuntimeError, match="rooted_publication_roots_missing"):
        _upgrade(connection)
    assert not any("DROP CONSTRAINT" in statement for statement in statements)


@pytest.mark.parametrize("failure", ["OWNER TO", "REVOKE INSERT(", "REVOKE EXECUTE", "DROP TRIGGER", "DROP CONSTRAINT"])
def test_ddl_failure_propagates_without_running_later_steps(failure):
    connection, statements = _connection(failure=failure)
    with pytest.raises(RuntimeError, match="synthetic DDL failure"):
        _upgrade(connection)
    assert failure in statements[-1]
    if failure != "DROP CONSTRAINT":
        assert not any("DROP CONSTRAINT" in statement for statement in statements)


def test_missing_logical_current_guard_preserves_constraint_protection():
    connection, statements = _connection(logical_guard=False)
    with pytest.raises(RuntimeError, match="rooted_logical_current_guard_missing"):
        _upgrade(connection)
    assert not any("DROP CONSTRAINT" in statement for statement in statements)


def test_partition_enumeration_is_names_only_and_scoped_to_the_fixed_roots():
    query = freeze._ROOTED_WRITE_FAMILY_SQL
    assert "unnest(CAST(:roots AS text[]))" in query
    assert "pg_inherits" in query and "JOIN family" in query
    projection = query.split(" SELECT n.nspname", 1)[1].split(" FROM family", 1)[0]
    assert "schema_name" in projection and "relation_name" in projection and "AS columns" in projection
    for forbidden in ("relowner", "relacl", "pg_roles", "pg_auth_members", "relfilenode", "prosrc"):
        assert forbidden not in query
