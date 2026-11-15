# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mandatory compact pins supplement both existing local cleanup callbacks."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_ownership as ownership
from process import network_cms_registry_address_capture as capture
from process import network_compact_retention as retention
from tests.test_entity_address_archive_ownership_guards import _set_owner


@pytest.mark.parametrize("blocked_call", [1, 2])
async def test_cleanup_requires_both_compact_reference_checks(monkeypatch, blocked_call):
    owner = _set_owner()
    connection = object()
    monkeypatch.setattr(
        ownership, "_validated_publication_owner", lambda *_: ("example", [], {"schema_name": owner.schema_name})
    )
    monkeypatch.setattr(ownership, "_require_retained_catalog", lambda *_: None)
    monkeypatch.setattr(
        ownership, "_retained_relation_catalog", AsyncMock(return_value=[{"nspname": "example", "relname": "retained"}])
    )
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=owner.schema_oid))
    monkeypatch.setattr(ownership, "_assert_namespace_is_owned", AsyncMock())
    monkeypatch.setattr(capture, "native_driver", AsyncMock(return_value=connection))
    guard_outcomes = [retention.NetworkCompactRetentionError("retained")]
    if blocked_call == 2:
        guard_outcomes.insert(0, None)
    guard = AsyncMock(side_effect=guard_outcomes)
    monkeypatch.setattr(retention, "require_named_address_artifacts_unreferenced", guard)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    old_hook = AsyncMock(return_value=True)
    with pytest.raises(retention.NetworkCompactRetentionError, match="retained"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication={}, assert_unreferenced=old_hook
        )
    assert guard.await_count == old_hook.await_count == blocked_call
    assert guard.await_args.args == (connection, [(owner.schema_name, "entity_address_alias_authority")])
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == blocked_call - 1
    assert all("ACCESS EXCLUSIVE" in statement for statement in statements)
    assert not any("DROP" in statement for statement in statements)


def _noncompact_report():
    source_by_field = {"schema_name": "example", "table_name": "retained_addresses", "generation_id": "edition/one"}
    scope_by_field = {"address_source": source_by_field, "source_generations": {"unified_address": "edition/one"}}
    return {
        **scope_by_field,
        "validation_revision": 1,
        "valid": True,
        "candidate_readiness": {
            "component": "canonical_address_projection",
            "readiness_revision": 1,
            "ready": True,
            "scope": scope_by_field,
        },
        "serving_readiness": {
            "component": "unified_address_serving",
            "readiness_revision": 1,
            "ready": True,
            "scope": scope_by_field,
            "index_profile": "serving",
            "index_definition_sha256": "a" * 64,
        },
    }


def test_noncompact_reference_keeps_only_its_exact_pinned_name():
    report = _noncompact_report()
    assert retention._named_address_references(report) == {("example", "retained_addresses")}


def test_validation_only_reference_preserves_source_without_serving_readiness():
    report = _noncompact_report()
    report.pop("serving_readiness")
    for state in ("validated", "ready"):
        assert retention._named_address_references(report, candidate_state=state) == {("example", "retained_addresses")}
    with pytest.raises(retention.NetworkCompactRetentionError, match="address_reference_invalid"):
        retention._named_address_references(report)
    report["valid"] = False
    with pytest.raises(retention.NetworkCompactRetentionError, match="address_reference_invalid"):
        retention._named_address_references(report, candidate_state="validated")


@pytest.mark.parametrize(
    "damage",
    [
        "unknown_component",
        "compact_missing_artifact",
        "compact_unknown_artifact",
        "canonical_component",
        "generation",
        "source",
        "ready",
        "revision",
        "scope",
        "digest",
        "unexpected_artifact",
        "invalid_report",
    ],
)
def test_named_references_refuse_unrecognized_or_malformed_reports(damage):
    from copy import deepcopy

    report = deepcopy(_noncompact_report())
    serving = report["serving_readiness"]
    match damage:
        case "unknown_component":
            serving["component"] = "unknown"
        case "compact_missing_artifact":
            report["address_source"]["table_name"] = "entity_address_unified"
            serving["component"] = retention.COMPONENT
        case "compact_unknown_artifact":
            report["address_source"]["table_name"] = "entity_address_unified"
            serving["component"] = retention.COMPONENT
            serving["address_artifact"] = {"contract": "unknown"}
        case "canonical_component":
            report["candidate_readiness"]["component"] = "compact_office_slices"
        case "generation":
            report["address_source"]["generation_id"] = "edition/two"
        case "source":
            report["address_source"]["table_name"] = "invalid.name"
        case "ready":
            serving["ready"] = False
        case "revision":
            serving["readiness_revision"] = True
        case "scope":
            serving["scope"] = {}
        case "digest":
            serving.pop("index_definition_sha256")
        case "unexpected_artifact":
            serving["address_artifact"] = {}
        case "invalid_report":
            report["valid"] = False
    with pytest.raises(retention.NetworkCompactRetentionError, match="address_reference_invalid"):
        retention._named_address_references(report)


@pytest.mark.parametrize("kind", ["ordinary", "bootstrap", "indexed"])
def test_compact_reference_requires_the_artifacts_fixed_logical_table(kind):
    from copy import deepcopy

    from tests.test_network_bootstrap_address_artifact import _descriptor as bootstrap_descriptor
    from tests.test_network_indexed_address_artifact import _descriptor as indexed_descriptor
    from tests.test_network_reader_metadata import _descriptor as ordinary_descriptor

    descriptor = {"ordinary": ordinary_descriptor, "bootstrap": bootstrap_descriptor, "indexed": indexed_descriptor}[
        kind
    ]()
    original = deepcopy(descriptor)
    report_by_field = {
        "address_source": {
            "schema_name": descriptor["namespace"]["name"],
            "table_name": "entity_address_unified",
            "generation_id": descriptor["generation_id"],
        },
        "serving_readiness": {"component": retention.COMPONENT, "address_artifact": descriptor},
    }
    assert (descriptor["namespace"]["name"], "entity_address_unified") in retention._named_address_references(
        report_by_field
    )
    report_by_field["address_source"]["table_name"] = "another_valid_table"
    with pytest.raises(retention.NetworkCompactRetentionError, match="address_reference_invalid"):
        retention._named_address_references(report_by_field)
    assert descriptor == original
