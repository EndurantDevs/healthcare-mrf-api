# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed pricing policy has no request-supplied or inferred role fallback."""

import json
from copy import deepcopy

import pytest

from process import plan_pricing_control_policy as policy


def _documents():
    namespace_by_field = {
        "owner_role": "database_owner",
        "migration_role": "migration_actor",
        "preparation_owner_role": "snapshot_owner",
        "runtime_roles": ["control_runtime", "snapshot_reader"],
        "control_runtime_roles": ["control_runtime"],
        "catalog_pin_role": "catalog_pin",
    }
    preparation_by_field = {
        "owner_role": "snapshot_owner",
        "publisher_role": "snapshot_publisher",
        "runtime_roles": ["control_runtime", "snapshot_reader"],
        "pin_role": "catalog_pin",
    }
    return namespace_by_field, preparation_by_field


def test_policy_uses_declared_roles_without_a_census():
    declared = policy.pricing_control_policy(*_documents())
    assert declared.storage_arguments() == {
        "owner_role": "snapshot_owner",
        "publisher_role": "snapshot_publisher",
        "reader_roles": ("control_runtime", "snapshot_reader"),
        "control_reader_roles": ("control_runtime",),
    }


@pytest.mark.parametrize("damage", ("owner", "runtime", "publisher", "publisher_owner", "pin", "extra", "controls"))
def test_policy_rejects_document_or_role_substitution(damage):
    namespace_by_field, preparation_by_field = deepcopy(_documents())
    if damage == "controls":
        namespace_by_field["control_runtime_roles"] = ["foreign_control"]
    else:
        changes_by_damage = {
            "owner": ("owner_role", "foreign_owner"),
            "runtime": ("runtime_roles", ["control_runtime"]),
            "publisher": ("publisher_role", "invalid role"),
            "publisher_owner": ("publisher_role", "snapshot_owner"),
            "pin": ("pin_role", "foreign_pin"),
            "extra": ("trusted", True),
        }
        field, changed = changes_by_damage[damage]
        preparation_by_field[field] = changed
    with pytest.raises(ValueError):
        policy.pricing_control_policy(namespace_by_field, preparation_by_field)


@pytest.mark.parametrize("damage", ("missing", "duplicate", "oversized", "missing_controls"))
def test_loader_refuses_missing_or_ambiguous_policy(tmp_path, monkeypatch, damage):
    namespace_by_field, preparation_by_field = _documents()
    if damage == "missing_controls":
        namespace_by_field.pop("control_runtime_roles")
    namespace_path = tmp_path / "namespace-role-policy.json"
    preparation_path = tmp_path / "preparation.json"
    namespace_path.write_text(json.dumps(namespace_by_field))
    preparation_path.write_text(json.dumps(preparation_by_field))
    if damage == "missing":
        preparation_path.unlink()
    elif damage == "duplicate":
        preparation_path.write_text('{"owner_role":"one","owner_role":"two"}')
    elif damage == "oversized":
        preparation_path.write_text(" " * 8193)
    monkeypatch.setattr(policy, "NAMESPACE_POLICY_PATH", namespace_path)
    monkeypatch.setattr(policy, "PREPARATION_POLICY_PATH", preparation_path)
    with pytest.raises(ValueError, match="unavailable or invalid"):
        policy.load_pricing_control_policy()


@pytest.mark.parametrize("schema", (None, "", "wrong-name", "UPPER", "other.table", "x" * 64))
def test_control_schema_requires_explicit_bounded_deployment_identifier(monkeypatch, schema):
    monkeypatch.delenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", raising=False)
    if schema is not None:
        monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", schema)
    with pytest.raises(ValueError, match="schema is unavailable or invalid"):
        policy.pricing_control_schema()


def test_control_schema_has_no_request_or_default_fallback(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "pricing_control")
    assert policy.pricing_control_schema() == "pricing_control"


def _historical_document():
    return {"contract": policy.HISTORICAL_POLICY_CONTRACT, "schema": "control_catalog", "revision": "scalar_successor"}


def test_historical_policy_qualifies_only_exact_independent_identity(tmp_path, monkeypatch):
    path = tmp_path / "historical.json"
    path.write_text(json.dumps(_historical_document()))
    monkeypatch.setattr(policy, "HISTORICAL_POLICY_PATH", path)
    assert policy.is_qualified_historical_revision("control_catalog", "scalar_successor")
    assert not policy.is_qualified_historical_revision("other_catalog", "scalar_successor")
    assert not policy.is_qualified_historical_revision("control_catalog", "other_revision")
    path.unlink()
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_HISTORICAL_REVISION", "scalar_successor")
    assert not policy.is_qualified_historical_revision("control_catalog", "scalar_successor")


@pytest.mark.parametrize("damage", ["contract", "schema", "revision", "extra", "duplicate", "oversized"])
def test_historical_policy_is_closed_bounded_and_unambiguous(tmp_path, monkeypatch, damage):
    document = _historical_document()
    if damage in {"contract", "schema", "revision"}:
        document[damage] = "invalid value"
    elif damage == "extra":
        document["managed"] = True
    encoded = json.dumps(document)
    if damage == "duplicate":
        encoded = '{"revision":"first",' + encoded[1:]
    elif damage == "oversized":
        encoded = encoded.ljust(8193)
    path = tmp_path / "historical.json"
    path.write_text(encoded)
    monkeypatch.setattr(policy, "HISTORICAL_POLICY_PATH", path)
    with pytest.raises(ValueError, match="historical policy is unavailable or invalid"):
        policy.is_qualified_historical_revision("control_catalog", "scalar_successor")
