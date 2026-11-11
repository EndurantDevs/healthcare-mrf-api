# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capability wiring after custody; receipt shape alone never grants admission."""

import json

import pytest

from process import network_serving_read as serving
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout
from tests.test_registry_typed_source_recipe import _registered_typed_resume


def _pin(monkeypatch, candidate, *, sealed=True):
    schema = candidate["schema_name"]
    candidate["validation_json"]["serving_readiness"] = {"component": "compact_network_serving"}
    retained = {
        name: candidate[name]
        for name in ("candidate_id", "schema_name", "source_generations", "approved_custom_revision")
    } | {"generation_id": 1, "schema_revision": 1, "manifest_sha256": "a" * 64}
    readiness = (None, None, {"index_definition_sha256": "b" * 64, "address_artifact": {}}, None)
    monkeypatch.setattr(serving, "_publication_readiness", lambda _candidate: readiness)
    monkeypatch.setattr(serving, "_retained_manifest", lambda *_args, **_kwargs: retained)
    closure_by_field = {"relation_oids": {"network_membership": 11, "provider_location_binding": 12}}
    custody_by_field = {
        "receipt": {"index_definition_sha256": "b" * 64, "closure": closure_by_field, "control": {}},
        "seal_sha256": "c" * 64,
        "compact_layout": CompactNetworkLayout(
            CompactAddressRelation(schema, "network_slice_ref", 13),
            CompactAddressRelation("example_address", "entity_address_unified", 14),
            (),
        ),
    }
    return serving._pinned_manifest(
        {"candidate_json": json.dumps(candidate), "manifest_json": "{}"}, custody=custody_by_field if sealed else None
    )


def test_both_authenticated_typed_summaries_enable_membership_pages(monkeypatch):
    _, _, candidate, _ = _registered_typed_resume("published")
    assert _pin(monkeypatch, candidate).supports_membership_pages


def test_missing_office_summary_keeps_full_reader(monkeypatch):
    _, _, candidate, _ = _registered_typed_resume("published")
    summary = "registry_initial_source_offices"
    candidate["validation_json"].pop(summary)
    candidate["source_generations"].pop(summary)
    assert not _pin(monkeypatch, candidate).supports_membership_pages


def test_generic_compact_custody_keeps_full_reader(monkeypatch):
    _, _, candidate, _ = _registered_typed_resume("published")
    candidate["source_recipes_json"] = []
    for summary in ("registry_initial_source_offices", "registry_source_contributions"):
        candidate["validation_json"].pop(summary)
        candidate["source_generations"].pop(summary)
    assert not _pin(monkeypatch, candidate).supports_membership_pages


@pytest.mark.parametrize("summary", ["registry_source_contributions", "registry_initial_source_offices"])
@pytest.mark.parametrize("damage", ["digest", "revision", "inventory", "missing"])
def test_changed_typed_summary_refuses_instead_of_granting_capability(monkeypatch, summary, damage):
    _, _, candidate, _ = _registered_typed_resume("published")
    if damage == "digest":
        candidate["source_generations"][summary] = "f" * 64
    elif damage == "missing":
        candidate["validation_json"].pop(summary)
    else:
        candidate["validation_json"][summary]["revision" if damage == "revision" else "inventory_entries_sha256"] = (
            5 if damage == "revision" else "f" * 64
        )
    with pytest.raises(ValueError):
        _pin(monkeypatch, candidate)


def test_unsealed_typed_summaries_cannot_grant_capability(monkeypatch):
    _, _, candidate, _ = _registered_typed_resume("published")
    with pytest.raises(serving.NetworkServingReadUnavailable, match="protected custody"):
        _pin(monkeypatch, candidate, sealed=False)
