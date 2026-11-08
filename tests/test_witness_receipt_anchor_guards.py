"""Explicit witness descriptors must pass their original closed shape before receipt normalization."""

from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_source_contribution_store as store
from tests.test_registry_source_contributions import _descriptor, _recipe
from tests.test_verified_inventory_v2 import prepared, rows


@pytest.mark.parametrize("anchored", [False, True])
@pytest.mark.parametrize("missing", [None, *sorted(store.FACT_ANCHOR_FIELDS)])
def test_witness_receipt_rejects_missing_original_anchor_fields(anchored, missing):
    recipe, descriptor, derivation = rows(anchored=anchored)
    root, _ = prepared([(recipe, descriptor, derivation)])
    if missing is None:
        for field in store.FACT_ANCHOR_FIELDS:
            descriptor.pop(field)
    else:
        descriptor.pop(missing)
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=2)
    with pytest.raises(store.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        store.registry_source_contribution_receipt(stage, inventory_root=root)


def test_witness_receipt_never_turns_partial_retained_anchors_into_local_ownership():
    recipe, descriptor, derivation = rows(anchored=True)
    root, _ = prepared([(recipe, descriptor, derivation)])
    descriptor.pop("fact_table_oid")
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=2)
    with pytest.raises(store.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        store.registry_source_contribution_receipt(stage, inventory_root=root)
    assert descriptor["fact_candidate_id"] == str(UUID(int=9))
    assert descriptor["fact_seal_sha256"] == "f" * 64
    assert "fact_table_oid" not in descriptor


@pytest.mark.parametrize("version", [True, 3])
def test_receipt_rejects_unknown_explicit_versions_before_anchor_backfill(version):
    recipe, descriptor, derivation = rows()
    root, _ = prepared([(recipe, descriptor, derivation)])
    descriptor["descriptor_version"] = version
    descriptor.pop("fact_table_oid")
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=2)
    with pytest.raises(store.RegistrySourceContributionError, match="witness_descriptor_invalid"):
        store.registry_source_contribution_receipt(stage, inventory_root=root)


@pytest.mark.parametrize("include_partial", [False, True])
def test_legacy_receipt_anchor_backfill_stays_exact(include_partial):
    descriptor = _descriptor(_recipe())
    if include_partial:
        descriptor.update(fact_candidate_id=str(UUID(int=9)), fact_seal_sha256="f" * 64)
    stage = SimpleNamespace(contribution_receipts=(descriptor,), contribution_revision=2)
    receipt = store.registry_source_contribution_receipt(stage)
    assert receipt["revision"] == 2
    assert all(receipt["sources"][0][field] is None for field in store.FACT_ANCHOR_FIELDS)
    assert "fact_table_oid" not in descriptor


def test_page_function_compatibility_names_are_direct_aliases():
    assert store.decode_fhir_source_witness_page_v2 is store.decode_source_witness_page_v2
    assert store.copy_fhir_source_witness_page_v2 is store.copy_source_witness_page_v2
