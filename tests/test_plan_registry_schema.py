# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A storage namespace is explicit configuration, never a caller SQL fragment."""

import pytest

from db.plan_registry_schema import plan_registry_schema


def test_missing_plan_registry_preserves_legacy_configuration(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", raising=False)
    assert plan_registry_schema() is None
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "")
    assert plan_registry_schema() is None


@pytest.mark.parametrize("schema", ["registry_data", "Registry_2", "_registry", "r" * 63])
def test_plan_registry_accepts_one_bounded_identifier(monkeypatch, schema):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", schema)
    assert plan_registry_schema() == schema


@pytest.mark.parametrize(
    "schema",
    [
        " ",
        " registry_data",
        "registry_data ",
        "r" * 64,
        "one.two",
        "one;select",
        '"registry"',
        "1registry",
        "répertoire",
    ],
)
def test_plan_registry_rejects_ambiguous_or_unsafe_identifiers(monkeypatch, schema):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", schema)
    with pytest.raises(ValueError, match="Plan registry schema identifier is invalid"):
        plan_registry_schema()
