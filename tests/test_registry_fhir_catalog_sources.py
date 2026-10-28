# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source inventory never upgrades catalog names or incomplete receipts into authority."""

import copy
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_source_fence as source_fence
from process import registry_fhir_catalog_sources as sources
from process.provider_directory_admission_seal import ADMISSION_GENERIC_PROOF_SUMMARY_KEY, _receipt


def read_profile():
    return sources.FHIRCatalogSourceReadProfile("source_payload", "source_owner", ("source_reader",), "source_catalog")


def native_inventory():
    root_by_field = {
        "oid": 101,
        "relname": "provider_directory_dataset_resource",
        "parents": [],
        "bound": None,
        "relowner": 501,
    }
    leaf_by_field = {
        "oid": 102,
        "relname": "source_resources_one",
        "parents": [101],
        "bound": "FOR VALUES IN ('dataset-one')",
        "relowner": 501,
    }
    return {
        "family": [root_by_field, leaf_by_field],
        "header": {"oid": 103, "relname": "provider_directory_endpoint_dataset", "relowner": 501},
        "database_oid": 104,
    }


def published_row(
    *, endpoint_id="endpoint-one", dataset_id="dataset-one", aliases=("alias-a", "alias-b"), selected=None, scope=None
):
    selected_resources = list(selected or sources.RESOURCE_TYPES)
    counts_by_resource = {resource: int(resource == "Organization") for resource in selected_resources}
    proof_by_field = {
        "dataset_hash": "a" * 64,
        "resource_count": sum(counts_by_resource.values()),
        "resource_counts": counts_by_resource,
        "resource_hashes": {resource: "b" * 64 for resource in counts_by_resource},
    }
    metadata = {"source_ids": list(aliases), "selected_resources": list(reversed(selected_resources))}
    if scope is not None:
        metadata["proof_resource_scope"] = scope
    seal = _receipt(
        metadata,
        admission_kind="generic",
        proof_sha256="c" * 64,
        resource_counts=counts_by_resource,
        proof_summary=proof_by_field,
    )
    return {
        "endpoint_id": endpoint_id,
        "canonical_api_base": "https://example.test/fhir",
        "source_aliases": list(aliases),
        "alias_count": len(aliases),
        "dataset_id": dataset_id,
        "dataset_hash": "a" * 64,
        "status": "published",
        "is_current": True,
        "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "resource_count": sum(counts_by_resource.values()),
        "previous_dataset_id": None,
        "content_proof_admission_version": 1,
        "content_proof_admission_kind": "generic",
        "content_proof_admission_sha256": "c" * 64,
        "content_proof_resource_types": list(seal.resource_types),
        "publication_metadata_sha256": seal.metadata_sha256,
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
        "physical_schema": "source_payload",
        "current_count": 1,
        "metadata_json": json.dumps(seal.metadata_summary),
        "completion_json": None,
        "metadata_bytes": len(json.dumps(seal.metadata_summary)),
        "completion_bytes": 0,
        "row_bytes": 8192,
    }


def descriptor():
    disposition = sources._disposition(published_row(), read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.status == "accepted"
    return disposition.descriptor


def test_closed_descriptor_binds_whole_alias_set_and_exact_existing_admission():
    row = published_row()
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    accepted = disposition.descriptor
    assert accepted["coordinates"] == {
        "source_system": "fhir",
        "source_id": "alias-a",
        "dataset_schema": "source_payload",
        "dataset_id": "dataset-one",
        "producer_id": "registry-fhir-catalog-v1",
        "edition_id": "dataset-one",
    }
    assert accepted["source_aliases"] == ["alias-a", "alias-b"] and accepted["complete"] is True
    assert accepted["resource_table_oid"] == 101 and accepted["resource_leaf_oids"] == [102]
    assert accepted["accepted_receipt_sha256"] == row["publication_metadata_sha256"]
    assert disposition.accepted_at.tzinfo is timezone.utc
    assert sources.validate_fhir_catalog_source_descriptor(accepted) == accepted
    later_by_field = {**row, "published_at": datetime(2026, 2, 1)}
    assert (
        sources._disposition(later_by_field, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES).descriptor
        == accepted
    )
    assert len(sources.fhir_catalog_source_generation(accepted)) == 64


@pytest.mark.parametrize(
    "mutation",
    ["unknown-field", "wrong-producer", "fake-receipt", "unsorted-aliases", "missing-native", "wrong-completeness"],
)
def test_descriptor_shape_is_closed_but_never_source_authority(mutation):
    selected = descriptor()
    if mutation == "unknown-field":
        selected["capture_sha256"] = "d" * 64
    if mutation == "wrong-producer":
        selected["coordinates"]["producer_id"] = "registry-retained-normalized-fhir-v1"
    if mutation == "fake-receipt":
        selected["accepted_receipt_sha256"] = "f" * 64
    if mutation == "unsorted-aliases":
        selected["source_aliases"].reverse()
    if mutation == "missing-native":
        selected["resource_leaf_oids"] = []
    if mutation == "wrong-completeness":
        selected["complete"] = False
    with pytest.raises(sources.FHIRCatalogSourceError):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.parametrize(
    "mutation,expected",
    [
        ("no-dataset", "current_dataset_missing"),
        ("legacy", "legacy_source_not_enrolled"),
        ("not-enrolled", "native_source_profile_unsupported"),
        ("proof-bounds", "source_proof_bounds"),
        ("alias-bounds", "source_alias_bounds"),
        ("unpublished", "current_dataset_unpublished"),
        ("digest-changed", "source_admission_unavailable"),
        ("alias-not-admitted", "source_admission_unavailable"),
        ("bad-completion", "source_admission_unavailable"),
    ],
)
def test_unavailable_and_unsupported_sources_remain_explicit_dispositions(mutation, expected):
    row, native = published_row(), native_inventory()
    if mutation == "no-dataset":
        row["dataset_id"] = None
    if mutation == "legacy":
        row["physical_schema"] = "source_catalog"
    if mutation == "not-enrolled":
        native = None
    if mutation == "proof-bounds":
        row["metadata_bytes"] = sources._PROOF_LIMIT + 1
    if mutation == "alias-bounds":
        row["alias_count"] = sources.MAX_ALIASES + 1
    if mutation == "unpublished":
        row["published_at"] = None
    if mutation == "digest-changed":
        row["publication_metadata_sha256"] = "f" * 64
    if mutation == "alias-not-admitted":
        row["source_aliases"].pop()
    if mutation == "bad-completion":
        row["completion_json"] = "{}"
    disposition = sources._disposition(
        row, read_profile(), native, "native_source_profile_unsupported", sources.DEFAULT_BYTES
    )
    assert disposition.status in {"unavailable", "unsupported"} and disposition.reason == expected
    assert disposition.descriptor is None


@pytest.mark.parametrize(
    "selected,scope",
    [
        (["Organization", "InsurancePlan"], None),
        (list(sources.RESOURCE_TYPES), ["Organization", "InsurancePlan"]),
    ],
)
def test_partial_family_coverage_keeps_an_explicit_incomplete_admitted_descriptor(selected, scope):
    disposition = sources._disposition(
        published_row(selected=selected, scope=scope), read_profile(), native_inventory(), None, sources.DEFAULT_BYTES
    )
    assert disposition.status == "incomplete" and disposition.reason == "catalog_families_incomplete"
    assert disposition.descriptor["complete"] is False


def test_unrelated_source_partition_addition_does_not_change_selected_native_identity():
    native = native_inventory()
    before = sources._native_identity(native, "dataset-one")
    native["family"].append(
        {
            "oid": 202,
            "relname": "other_resources",
            "parents": [101],
            "bound": "FOR VALUES IN ('other-dataset')",
            "relowner": 501,
        }
    )
    assert sources._native_identity(native, "dataset-one") == before
    native["family"][1]["oid"] = 303
    assert sources._native_identity(native, "dataset-one") != before


def test_unaccepted_registration_alias_does_not_change_published_source_authority():
    original = descriptor()
    row = published_row()
    row["source_aliases"].insert(0, "alias-0")
    row["alias_count"] = 3
    disposition = sources._disposition(row, read_profile(), native_inventory(), None, sources.DEFAULT_BYTES)
    assert disposition.source_aliases == ("alias-0", "alias-a", "alias-b")
    assert disposition.descriptor == original
    assert disposition.descriptor["coordinates"]["source_id"] == "alias-a"


@pytest.mark.asyncio
async def test_inventory_uses_endpoint_keyset_pages_and_one_native_verification(monkeypatch):
    profile, native = read_profile(), native_inventory()
    access_by_name = {
        "source_payload.provider_directory_endpoint_dataset": {"readable": True},
        "source_catalog.provider_directory_endpoint_dataset": {"readable": True},
    }
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value=access_by_name))
    verify = AsyncMock(return_value=(native, None))
    monkeypatch.setattr(sources, "_native_source", verify)
    second = published_row(endpoint_id="endpoint-two")
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=[[published_row()], [second], []]),
    )
    pages = [page async for page in sources.iter_fhir_catalog_sources(connection, profile, page_size=1)]
    assert [page[0].endpoint_id for page in pages] == ["endpoint-one", "endpoint-two"]
    assert all(page[0].descriptor["source_aliases"] == ["alias-a", "alias-b"] for page in pages)
    assert verify.await_count == 1 and connection.fetch.await_count == 3
    assert connection.fetch.await_args_list[1].args[1] == "endpoint-one"
    query = connection.fetch.await_args_list[0].args[0]
    assert "GROUP BY endpoint_id" in query and "e.endpoint_id>$1::text" in query
    assert "registry_network" not in query and "import_run" not in query


@pytest.mark.asyncio
async def test_final_source_recheck_detects_new_head_alias_or_native_identity(monkeypatch):
    expected = descriptor()
    profile, native = read_profile(), native_inventory()
    monkeypatch.setattr(sources, "_catalog_access", AsyncMock(return_value={}))
    monkeypatch.setattr(sources, "_native_source", AsyncMock(return_value=(native, None)))
    original_page = sources._source_pages

    async def inventory_page(*arguments, **keywords):
        yield (
            sources.FHIRCatalogSourceDisposition(
                "endpoint-one",
                "dataset-one",
                ("alias-a", "alias-b"),
                2,
                "accepted",
                None,
                copy.deepcopy(expected),
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
        )

    monkeypatch.setattr(sources, "_source_pages", inventory_page)
    lock = AsyncMock()
    monkeypatch.setattr(source_fence, "require_fhir_catalog_source_publication_fence", lock)
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchval=AsyncMock(return_value="repeatable read"))
    assert await sources.require_current_fhir_catalog_sources(connection, (expected,), lock=True) == (
        sources.fhir_catalog_source_generation(expected),
    )
    lock.assert_awaited_once_with(connection, (expected,), required=True)
    changed = copy.deepcopy(expected)
    changed["native_catalog_sha256"] = "f" * 64
    with pytest.raises(sources.FHIRCatalogSourceError, match="source_changed"):
        await sources.require_current_fhir_catalog_sources(connection, (changed,))
    monkeypatch.setattr(sources, "_source_pages", original_page)


@pytest.mark.asyncio
async def test_final_fence_requires_pre_snapshot_session_context_without_source_row_privileges():
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(),
        execute=AsyncMock(),
    )
    with pytest.raises(source_fence.FHIRCatalogSourceFenceError, match="unavailable"):
        await sources.require_current_fhir_catalog_sources(connection, (descriptor(),), lock=True)
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()
