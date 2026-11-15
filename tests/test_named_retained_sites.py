# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable retained-site metadata keeps complete binding and address-row checks."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_retained_site_adoption as sites
from process.network_approved_site_source import _verify_records
from process.network_compact_layout import CompactAddressRelation
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_ptg_address_carry import retained_site_origin_reference


def manifest():
    return PinnedNetworkServingManifest(
        17,
        str(UUID(int=4)),
        "retained_example",
        1,
        {"source": "edition"},
        3,
        "a" * 64,
        CompactAddressRelation("retained_example", "entity_address_unified"),
    )


def selection():
    return {
        "provider_system": "npi",
        "provider_id": "1234567893",
        "location_id": str(UUID(int=8)),
        "location_key": "b" * 64,
        "address_row_sha256": "c" * 64,
    }


def selected():
    return {
        "requested_count": 1,
        "pair_count": 1,
        "matched_count": 1,
        "unresolved_count": 0,
        "records_json": json.dumps([selection() | {"entity_type": "npi", "entity_id": "1234567893"}]),
    }


@pytest.mark.parametrize("old_key", [1, 999999])
def test_saved_legacy_receipt_projects_to_the_same_named_data(old_key):
    receipt = sites._receipt(manifest(), selected())
    legacy = receipt.as_dict()
    legacy.pop("address_relation")
    legacy.update(
        {
            field: old_key
            for field in (
                "schema_oid",
                "address_table_oid",
                "binding_table_oid",
                "owner_role_oid",
            )
        }
    )
    assert sites.retained_site_receipt_document(legacy) == receipt.as_dict()
    assert retained_site_origin_reference(receipt) == (17, "a" * 64)
    binding = selection() | {"source_receipt_json": legacy}
    _verify_records([binding], receipt)
    assert legacy["schema_oid"] == old_key


def test_legacy_compact_receipt_retains_recorded_relation_names():
    receipt = sites._receipt(manifest(), selected())
    receipt = replace(
        receipt,
        address_relation=CompactAddressRelation("copied_base", "address_rows"),
        compact_address_relations=(("base", "copied_base", "address_rows"),),
    )
    legacy = receipt.as_dict()
    legacy.pop("address_relation")
    legacy.update(
        {
            field: 123
            for field in (
                "schema_oid",
                "address_table_oid",
                "binding_table_oid",
                "owner_role_oid",
            )
        }
    )
    legacy["compact_address_relations"] = [["base", "copied_base", "address_rows", 999]]
    assert sites.retained_site_receipt_document(legacy) == receipt.as_dict()


@pytest.mark.parametrize(
    "field,value",
    [
        ("pair_count", 0),
        ("matched_count", 2),
        ("unresolved_count", 1),
    ],
)
def test_named_receipt_requires_complete_unambiguous_data(field, value):
    with pytest.raises(sites.RetainedSiteAdoptionError, match="unresolved"):
        sites._receipt(manifest(), selected() | {field: value})


def test_approved_receipt_requires_the_exact_saved_address_row():
    receipt = sites._receipt(manifest(), selected())
    changed = receipt.as_dict()
    changed["records"][0]["address_row_sha256"] = "d" * 64
    with pytest.raises(sites.RetainedSiteAdoptionError, match="differs"):
        _verify_records([selection() | {"source_receipt_json": changed}], receipt)


async def test_named_resolver_uses_full_data_checks_without_physical_metadata(monkeypatch):
    source = manifest()
    monkeypatch.setattr(sites, "resolve_network_serving_manifest", AsyncMock(return_value=source))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(return_value=selected()),
    )
    receipt = await sites.resolve_retained_site_adoptions(
        connection,
        source,
        json.dumps([selection()]).encode(),
        control_schema="registry",
    )
    assert receipt.address_relation == source.address_relation
    assert asdict(receipt.records[0])["address_row_sha256"] == "c" * 64
    query, *arguments = connection.fetchrow.await_args.args
    assert '"retained_example".provider_location_binding' in query
    assert "to_jsonb(address)::text" in query and "unresolved_count" in query
    assert "tableoid" not in query and "pg_namespace" not in query and "pg_class" not in query
    assert arguments == [json.dumps([selection()], separators=(",", ":"))]
