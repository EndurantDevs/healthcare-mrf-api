# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog refusal and transaction boundaries for ordinary registry adoption."""

from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.engine import Connection

from db import migration_registry_preparation as preparation

SCHEMA = "registry_test"
BINDING = "registry_network_binding"
OWNER_FLAGS = ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
RELATION_FLAGS = (
    "relispartition",
    "relrowsecurity",
    "relforcerowsecurity",
    "reloptions",
    "inheritance",
    "rules",
    "triggers",
    "dropped_columns",
    "incoming_foreign_keys",
)


def _grant(privilege="SELECT", column=None, **changes):
    return dict(grantor=11, grantee=22, privilege_type=privilege, column_name=column, is_grantable=False) | changes


class Catalog:
    """Return recorded catalog observations without opening a database."""

    def __init__(self, table=BINDING):
        self.owner = dict(oid=11, schema_oid=12, caller_owns=False) | dict.fromkeys(OWNER_FLAGS, False)
        self.relation = dict(oid=31, relname=table, relowner=11, relkind="r", relpersistence="p") | dict.fromkeys(
            RELATION_FLAGS, False
        )
        self.grants = []
        self.safe_grantees = {}
        self.namespace_create = False
        self.serving_owner = True
        self.occupied = None
        self.search_path = '"registry_test", public'
        self.shape = ([("value", "text", False, "", "", 0, None)], [("value_check", "c", "CHECK (value <> '')")], [])
        self.expected_shape = deepcopy(self.shape)
        self.calls = []
        self.ddl = []
        self.events = []
        self.fail_ddl = None

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if sql in (preparation._COLUMNS, preparation._CONSTRAINTS, preparation._INDEXES):
            shape = self.shape if parameters["oid"] == 31 else self.expected_shape
            return shape[(preparation._COLUMNS, preparation._CONSTRAINTS, preparation._INDEXES).index(sql)]
        observation = self._catalog_observation(sql, parameters)
        return SimpleNamespace(
            scalar_one=lambda: observation,
            scalar_one_or_none=lambda: observation,
            mappings=lambda: SimpleNamespace(one_or_none=lambda: observation, all=lambda: observation),
        )

    def _catalog_observation(self, sql, parameters):
        if "FROM pg_namespace n JOIN pg_roles" in sql:
            return self.owner
        if sql == preparation._ACL:
            return self.grants
        if "FROM pg_roles WHERE oid=:grantee" in sql:
            assert parameters["owner"] == 11 and parameters["schema"] == 12
            return self.safe_grantees.get(parameters["grantee"], parameters["grantee"] != 0)
        if "a.privilege_type='CREATE'" in sql:
            return self.namespace_create
        if "relname='network_serving_control'" in sql:
            return self.serving_owner
        if "WHERE n.nspname=:schema AND c.relname=:table" in sql:
            return self.relation
        if sql == "SELECT to_regclass(:name)":
            return self.occupied
        if sql == "SELECT to_regclass(:name)::oid":
            return 32
        if "current_setting('search_path')" in sql:
            return self.search_path
        if "set_config('search_path'" in sql:
            self.search_path = parameters["previous"] if parameters else "pg_catalog,pg_temp"
            return self.search_path
        raise AssertionError(f"Unexpected catalog query: {sql}")

    def exec_driver_sql(self, statement):
        self.ddl.append(statement)
        if len(self.ddl) == self.fail_ddl:
            raise RuntimeError("synthetic temporary DDL failure")

    @contextmanager
    def begin_nested(self):
        previous = self.search_path
        self.events.append("savepoint")
        try:
            yield
        except BaseException:
            self.search_path = previous
            self.events.append("rollback")
            raise
        else:
            self.events.append("release")


def _postimage(table=BINDING):
    revision = {
        BINDING: "20261017030000_registry_ptg_office_recipe_selection",
        "registry_aca_source_notification": "20261018040000_registry_aca_source_notifications",
    }[table]
    migration = preparation.registry_migration(revision)
    return migration._postimage


def _validate(catalog, table=BINDING, schema=SCHEMA):
    preparation.validate_prepared_registry_table(catalog, schema, table, lambda namespace: _postimage(table)(namespace))


@pytest.mark.parametrize("schema", [None, 3, "", "x.y", 'x";DROP SCHEMA public', "a" * 64])
def test_invalid_namespace_is_rejected_before_catalog_reads(schema):
    catalog = Catalog()
    with pytest.raises(ValueError, match="identifier"):
        _validate(catalog, schema=schema)
    assert catalog.calls == catalog.ddl == []


def test_unknown_revision_and_table_cannot_load_or_validate_foreign_storage():
    with pytest.raises(ValueError, match="revision is unsupported"):
        preparation.registry_migration("../foreign_revision")
    catalog = Catalog()
    with pytest.raises(ValueError, match="table is unsupported"):
        _validate(catalog, table="unrelated_storage")
    assert catalog.calls == catalog.ddl == []


@pytest.mark.parametrize("missing", ["owner", "relation"])
def test_missing_owner_preparation_never_creates_a_replacement(missing):
    catalog = Catalog()
    setattr(catalog, missing, None)
    with pytest.raises(RuntimeError, match="namespace|preparation is missing"):
        _validate(catalog)
    assert catalog.ddl == []


@pytest.mark.parametrize("flag", OWNER_FLAGS)
def test_owner_login_and_elevated_capabilities_are_refused(flag):
    catalog = Catalog()
    catalog.owner[flag] = True
    with pytest.raises(RuntimeError, match="owner is not protected"):
        _validate(catalog)
    assert catalog.ddl == []


def test_namespace_create_grants_are_refused_before_relation_validation():
    catalog = Catalog()
    catalog.namespace_create = True
    with pytest.raises(RuntimeError, match="namespace custody"):
        _validate(catalog)
    assert catalog.ddl == []


@pytest.mark.parametrize("serving_owner", [False, None, 1])
def test_aca_notification_requires_positive_native_serving_owner_identity(serving_owner):
    table = "registry_aca_source_notification"
    catalog = Catalog(table)
    catalog.serving_owner = serving_owner
    with pytest.raises(RuntimeError, match="serving owner"):
        _validate(catalog, table)
    assert catalog.ddl == []


@pytest.mark.parametrize(
    "change",
    [{"relowner": 99}, {"relkind": "v"}, {"relpersistence": "u"}] + [{flag: True} for flag in RELATION_FLAGS],
)
def test_nonordinary_storage_features_are_refused_without_ddl(change):
    catalog = Catalog()
    catalog.relation.update(change)
    with pytest.raises(RuntimeError, match="storage custody"):
        _validate(catalog)
    assert catalog.ddl == []


@pytest.mark.parametrize(
    ("table", "grants"),
    [
        (BINDING, [_grant(grantee=11, privilege="DELETE"), _grant(), _grant("INSERT", "source_id")]),
        (BINDING, [_grant("UPDATE", "office_recipe_json")]),
        ("registry_ptg_published_plan_scope", [_grant(), _grant("INSERT")]),
        ("network_candidate_custody", [_grant()]),
        ("registry_cms_source_notification", [_grant(grantee=11)]),
        ("registry_aca_source_notification", []),
    ],
)
def test_acl_preserves_bounded_review_writers_and_owner_only_notifications(table, grants):
    catalog = Catalog(table)
    catalog.grants = grants
    preparation._validate_acl(catalog, catalog.relation, catalog.owner)
    assert catalog.ddl == []


@pytest.mark.parametrize(
    ("table", "grant"),
    [
        (BINDING, _grant("INSERT")),
        (BINDING, _grant("UPDATE", "source_id")),
        (BINDING, _grant("DELETE")),
        (BINDING, _grant(is_grantable=True)),
        (BINDING, _grant(grantor=99)),
        ("registry_ptg_published_plan_scope", _grant("UPDATE", "revision")),
        ("network_candidate_custody", _grant("SELECT", "candidate_id")),
        ("network_candidate_custody", _grant("INSERT")),
        ("registry_cms_source_notification", _grant()),
        ("registry_aca_source_notification", _grant()),
    ],
)
def test_acl_rejects_broader_or_transferable_privileges(table, grant):
    catalog = Catalog(table)
    catalog.grants = [grant]
    with pytest.raises(RuntimeError, match="privileges differ"):
        preparation._validate_acl(catalog, catalog.relation, catalog.owner)


@pytest.mark.parametrize("safe", [False, None, 1])
def test_unknown_or_elevated_grantee_must_be_positively_safe(safe):
    catalog = Catalog()
    catalog.grants = [_grant()]
    catalog.safe_grantees[22] = safe
    with pytest.raises(RuntimeError, match="grantee authority"):
        preparation._validate_acl(catalog, catalog.relation, catalog.owner)


def test_public_acl_and_competing_writers_are_refused():
    catalog = Catalog()
    catalog.grants = [_grant(grantee=0)]
    with pytest.raises(RuntimeError, match="grantee authority"):
        preparation._validate_acl(catalog, catalog.relation, catalog.owner)
    catalog.grants = [_grant("INSERT", "source_id"), _grant("UPDATE", "revision", grantee=23)]
    with pytest.raises(RuntimeError, match="writer inventory"):
        preparation._validate_acl(catalog, catalog.relation, catalog.owner)


def test_grantee_limit_accepts_the_boundary_and_refuses_one_more():
    catalog = Catalog()
    catalog.grants = [_grant(grantee=1000 + index) for index in range(128)]
    preparation._validate_acl(catalog, catalog.relation, catalog.owner)
    catalog.grants.append(_grant(grantee=1128))
    with pytest.raises(RuntimeError, match="grantee authority"):
        preparation._validate_acl(catalog, catalog.relation, catalog.owner)


def test_existing_temporary_name_is_refused_without_savepoint_or_ddl():
    catalog = Catalog()
    catalog.occupied = 32
    with pytest.raises(RuntimeError, match="temporary name"):
        _validate(catalog)
    assert catalog.events == catalog.ddl == []


@pytest.mark.parametrize("component", range(3), ids=["columns", "constraints", "indexes"])
def test_complete_shape_drift_rolls_back_the_private_comparison(component):
    catalog = Catalog()
    catalog.expected_shape[component].append(("unexpected_native_metadata",))
    with pytest.raises(RuntimeError, match="native postimage differs"):
        _validate(catalog)
    assert catalog.events == ["savepoint", "rollback"]
    assert catalog.search_path == '"registry_test", public'
    assert catalog.ddl[0].startswith('CREATE TEMPORARY TABLE "pg_temp".')
    assert all('"registry_test".' not in sql for sql in catalog.ddl)


@pytest.mark.parametrize("failed_statement", [1, 2])
def test_temporary_ddl_failure_propagates_through_the_savepoint(failed_statement):
    catalog = Catalog()
    catalog.fail_ddl = failed_statement
    with pytest.raises(RuntimeError, match="temporary DDL failure"):
        _validate(catalog)
    assert catalog.events == ["savepoint", "rollback"]
    assert catalog.search_path == '"registry_test", public'


@pytest.mark.parametrize("table", [BINDING, "registry_aca_source_notification"])
def test_successful_comparison_drops_private_storage_and_restores_search_path(table):
    catalog = Catalog(table)
    _validate(catalog, table)
    assert catalog.events == ["savepoint", "release"]
    assert catalog.search_path == '"registry_test", public'
    assert catalog.ddl[-1] == f'DROP TABLE IF EXISTS pg_temp."{table}"'
    shape_reads = [
        parameters["oid"]
        for sql, parameters in catalog.calls
        if sql in (preparation._COLUMNS, preparation._CONSTRAINTS, preparation._INDEXES)
    ]
    assert shape_reads == [31, 31, 31, 32, 32, 32]
    assert not any(sql.startswith(("GRANT", "REVOKE", "ALTER ROLE")) for sql in catalog.ddl)


@pytest.mark.parametrize("caller_owns", [False, True])
def test_ordinary_adoption_validates_while_owner_path_retains_ddl_responsibility(caller_owns):
    catalog = Catalog()
    catalog.owner["caller_owns"] = caller_owns
    connection = Mock(spec=Connection)
    connection.execute.side_effect = catalog.execute
    connection.exec_driver_sql.side_effect = catalog.exec_driver_sql
    connection.begin_nested.side_effect = catalog.begin_nested
    operations = SimpleNamespace(get_bind=lambda: connection)
    assert preparation.can_adopt_prepared_registry_table(operations, SCHEMA, BINDING, _postimage()) is not caller_owns
    assert bool(catalog.ddl) is not caller_owns


def test_offline_operations_cannot_claim_prepared_table_adoption():
    operations = SimpleNamespace(get_bind=lambda: object())
    assert preparation.can_adopt_prepared_registry_table(operations, SCHEMA, BINDING, _postimage()) is False
