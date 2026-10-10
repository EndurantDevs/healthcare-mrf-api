# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A protected registry namespace does not redirect the ordinary source schema."""

import importlib.util
import os
import sys
from pathlib import Path

import pytest

import db.registry_schema as runtime_schema
from db.registry_schema import registry_schema


def _historical_registry_migrations():
    versions = Path(__file__).parents[1] / "alembic/versions"
    for ordinal in range(1, 15):
        if ordinal == 5:
            continue
        (path,) = versions.glob(f"20261007{ordinal:02}0000_*.py")
        specification = importlib.util.spec_from_file_location(path.stem, path)
        migration = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(migration)
        yield migration


@pytest.mark.parametrize("runtime_state", ["changed", "unavailable"])
@pytest.mark.parametrize(
    "source_schema,registry_namespace,expected_schema",
    [
        (None, None, "mrf"),
        ("example_source", None, "example_source"),
        ("example_source", "example_registry", "example_registry"),
        ("example_source", "", "example_source"),
    ],
)
def test_historical_upgrades_keep_frozen_namespace_rules(
    monkeypatch, runtime_state, source_schema, registry_namespace, expected_schema
):
    for variable, value in (
        ("HLTHPRT_DB_SCHEMA", source_schema),
        ("HLTHPRT_NETWORK_REGISTRY_SCHEMA", registry_namespace),
    ):
        if value is None:
            monkeypatch.delenv(variable, raising=False)
        else:
            monkeypatch.setenv(variable, value)
    if runtime_state == "changed":
        monkeypatch.setattr(runtime_schema, "registry_schema", lambda: "redirected_runtime")
    else:
        monkeypatch.setitem(sys.modules, "db.registry_schema", None)
    for migration in _historical_registry_migrations():
        statements = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        migration.upgrade()
        assert statements and all(f'"{expected_schema}".' in statement for statement in statements)
        with pytest.raises(RuntimeError):
            migration.downgrade()


@pytest.mark.parametrize("schema", ['bad"schema', "a.b", "a" * 64, "schema\n"])
def test_historical_upgrades_reject_unsafe_names_before_sql(monkeypatch, schema):
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", schema)
    monkeypatch.setitem(sys.modules, "db.registry_schema", None)
    for migration in _historical_registry_migrations():
        statements = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        with pytest.raises(ValueError, match="Registry schema identifier is invalid"):
            migration.upgrade()
        assert statements == []


def test_registry_namespace_default_and_explicit_source_separation(monkeypatch):
    monkeypatch.delenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", raising=False)
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    assert registry_schema() == "mrf"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "example_source")
    assert registry_schema() == "example_source"
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "example_registry")
    assert registry_schema() == "example_registry"
    assert os.environ["HLTHPRT_DB_SCHEMA"] == "example_source"


@pytest.mark.parametrize("schema", ['bad"schema', "a.b", "a" * 64, "schema\n"])
def test_unsafe_registry_names_reject_before_sql(monkeypatch, schema):
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", schema)
    with pytest.raises(ValueError):
        registry_schema()
