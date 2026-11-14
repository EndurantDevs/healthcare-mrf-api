# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published capability routing and prepublication typed summary validation."""

import json

import pytest

from process import network_serving_read as serving
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_source_contribution_store import verify_registry_source_contributions
from tests.test_registry_typed_source_recipe import _registered_typed_resume


def _pin(candidate):
    """Decode the named candidate and matching logical manifest through the current reader."""
    schema = candidate["schema_name"]
    candidate["schema_revision"] = 1
    candidate["validation_json"]["serving_readiness"] = {"component": "compact_network_serving"}
    manifest_by_field = {
        name: candidate[name]
        for name in ("candidate_id", "schema_revision", "source_generations", "approved_custom_revision")
    } | {"generation_id": 1, "manifest_sha256": "a" * 64}
    layout = CompactNetworkLayout(
        CompactAddressRelation(schema, "network_slice_ref"),
        CompactAddressRelation("example_address", "entity_address_unified"),
        (),
    )
    return serving._pinned_manifest(
        {"candidate_json": json.dumps(candidate), "manifest_json": json.dumps(manifest_by_field)}, layout=layout
    )


def test_validated_typed_summaries_enable_membership_pages():
    _, _, candidate, _ = _registered_typed_resume("published")
    assert verify_registry_source_contributions(candidate)["revision"] == 6
    assert verify_initial_source_office_receipt(candidate)["revision"] == 6
    assert _pin(candidate).supports_membership_pages


def test_missing_office_summary_keeps_full_reader():
    _, _, candidate, _ = _registered_typed_resume("published")
    summary = "registry_initial_source_offices"
    candidate["validation_json"].pop(summary)
    candidate["source_generations"].pop(summary)
    assert not _pin(candidate).supports_membership_pages


def test_generic_compact_generation_keeps_full_reader():
    _, _, candidate, _ = _registered_typed_resume("published")
    candidate["source_recipes_json"] = []
    for summary in ("registry_initial_source_offices", "registry_source_contributions"):
        candidate["validation_json"].pop(summary)
        candidate["source_generations"].pop(summary)
    assert not _pin(candidate).supports_membership_pages


@pytest.mark.parametrize("summary", ["registry_source_contributions", "registry_initial_source_offices"])
@pytest.mark.parametrize("damage", ["digest", "revision", "inventory", "missing"])
def test_changed_typed_summary_refuses_before_publication(summary, damage):
    _, _, candidate, _ = _registered_typed_resume("published")
    if damage == "digest":
        candidate["source_generations"][summary] = "f" * 64
    elif damage == "missing":
        candidate["validation_json"].pop(summary)
    else:
        candidate["validation_json"][summary]["revision" if damage == "revision" else "inventory_entries_sha256"] = (
            5 if damage == "revision" else "f" * 64
        )
    validate = (
        verify_registry_source_contributions
        if summary == "registry_source_contributions"
        else verify_initial_source_office_receipt
    )
    with pytest.raises(ValueError):
        validate(candidate)
