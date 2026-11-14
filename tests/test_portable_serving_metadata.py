# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable metadata keeps origin and counter validation without local identities."""

import datetime
from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import CheckConstraint

from db.models import EntityAddressResultGeneration
from process import entity_address_result_generation as generation
from process import network_address_artifact as artifact
from tests.test_entity_address_result_generation_postgres import _migration_module


def test_forward_migration_changes_journal_before_serving_check(monkeypatch):
    migration = _migration_module(portable=True)
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "example")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "_has_relation", lambda _bind, _schema, table: table != migration._CMS_RECEIPT)
    monkeypatch.setattr(
        migration, "op", SimpleNamespace(get_bind=lambda: object(), execute=lambda sql: statements.append(str(sql)))
    )

    migration.upgrade()

    assert statements[0] == "SET LOCAL lock_timeout='50ms'"
    assert '"provider_directory_serving_recovery"' in statements[1]
    assert "IN ('1','2')" in statements[1] and "2097152" in statements[1]
    assert '"entity_address_result_generation"' in statements[2]
    assert "relation_oids" not in statements[2]
    assert len(statements) == 3


def test_forward_model_and_migration_require_the_same_portable_shape():
    migration = _migration_module(portable=True)
    constraint = next(
        constraint
        for constraint in EntityAddressResultGeneration.__table__.constraints
        if isinstance(constraint, CheckConstraint) and constraint.name == "entity_address_result_generation_shape_check"
    )
    assert str(constraint.sqltext) == migration._generation_check()


def test_downgrade_refuses_installed_portable_cms_before_any_ddl(monkeypatch):
    migration = _migration_module(portable=True)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "example")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements = []
    monkeypatch.setattr(migration, "_has_relation", lambda _bind, _schema, table: table == migration._CMS_RECEIPT)
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_bind=lambda: object(), execute=statements.append))
    with pytest.raises(RuntimeError, match="cms_routines_prevent_downgrade"):
        migration.downgrade()
    assert statements == []


def test_network_origin_ignores_legacy_ids_without_rewriting_the_descriptor():
    class Authority:
        local_lineage_id = "550e8400-e29b-41d4-a716-446655440000"
        local_generation = 9
        serving_generation = generation.EntityAddressServingGeneration(
            origin_lineage_id="550e8400-e29b-41d4-a716-446655440001",
            origin_generation=27,
            published_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )

        @property
        def relation_oids(self):
            raise AssertionError("legacy IDs must not be inspected")

    portable = artifact._authority_document(Authority())
    assert portable["relation_oids"] is None
    legacy_by_field = {**portable, "relation_oids": [1, 2, 3, 4, 5, 6, 7]}
    binding_by_field = {"authority": legacy_by_field, "runtime_roles": ["reader"]}
    original_binding = deepcopy(binding_by_field)
    artifact._descriptor_origin(binding_by_field)
    assert binding_by_field == original_binding
    assert artifact._authority_document(Authority()) == portable
