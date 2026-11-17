# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named editions retain their own headers and complete content accounting."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_sources as sources
from tests.test_registry_fhir_catalog_sources import descriptor, read_profile


def content_descriptor():
    selected = descriptor()
    selected["complete"] = True
    resources = list(sources.RESOURCE_TYPES)
    selected["admission"] = {
        "version": 1,
        "kind": "generic",
        "proof_sha256": "a" * 64,
        "metadata_sha256": selected["accepted_receipt_sha256"],
        "resource_count": len(resources),
        "resource_counts": dict.fromkeys(resources, 1),
        "resource_types": resources,
        "selected_resources": resources,
        "completion_required_version": 3,
        "completion_sha256": "b" * 64,
        "proof_resource_scope": resources,
    }
    return selected


@pytest.mark.parametrize(
    "change",
    [
        "negative_count",
        "count_mismatch",
        "scope_outside_selection",
        "partial_complete",
        "completion",
        "version",
        "metadata",
        "canonical_source",
        "completion_version",
    ],
)
def test_content_receipt_rejects_inconsistent_accounting_or_completion(change):
    selected = content_descriptor()
    assert sources.validate_fhir_catalog_source_descriptor(selected) == selected
    admission = selected["admission"]
    match change:
        case "negative_count":
            admission["resource_count"] = -1
        case "count_mismatch":
            admission["resource_counts"]["InsurancePlan"] += 1
        case "scope_outside_selection":
            admission["proof_resource_scope"] = ["Location"]
        case "partial_complete":
            admission["proof_resource_scope"] = ["InsurancePlan"]
        case "completion":
            admission["completion_required_version"] = None
        case "version":
            admission["version"] = True
        case "canonical_source":
            admission["kind"] = "uhc_canonical"
        case "completion_version":
            admission["completion_required_version"] = 2
        case _:
            admission["metadata_sha256"] = "c" * 64
    with pytest.raises(sources.FHIRCatalogSourceError, match="admission_invalid"):
        sources.validate_fhir_catalog_source_descriptor(selected)


@pytest.mark.asyncio
async def test_named_header_selection_retains_previous_and_older_catalog_editions():
    pointer_by_field = {
        "endpoint_id": "endpoint-one",
        "dataset_id": "current",
        "schema_name": "edition_current",
        "previous_dataset_id": "previous",
        "previous_schema_name": "edition_previous",
    }

    async def fetch(query, *args):
        if "previous_schema_name" in query:
            return [pointer_by_field]
        return [{"dataset_id": args[1], "status": "published", "is_current": args[1] == "current"}]

    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False), fetch=AsyncMock(side_effect=fetch))
    assert (
        await sources.fhir_catalog_header_schema(connection, read_profile(), "endpoint-one", "current")
        == "edition_current"
    )
    assert (
        await sources.fhir_catalog_header_schema(
            connection, read_profile(), "endpoint-one", "previous", require_current=False
        )
        == "edition_previous"
    )
    assert (
        await sources.fhir_catalog_header_schema(
            connection, read_profile(), "endpoint-one", "older", require_current=False
        )
        == "source_catalog"
    )
    with pytest.raises(sources.FHIRCatalogSourceError, match="named_edition_header_missing"):
        await sources.fhir_catalog_header_schema(connection, read_profile(), "endpoint-one", "previous")
    assert any('"edition_previous"' in call.args[0] for call in connection.fetch.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [None, [], [{"dataset_id": "different"}], [{"dataset_id": "current"}] * 2])
async def test_named_pointer_never_borrows_a_catalog_header(monkeypatch, headers):
    pointer_by_field = {"endpoint-one": {"dataset_id": "current", "schema_name": "edition_current"}}
    monkeypatch.setattr(sources, "_named_editions", AsyncMock(return_value=pointer_by_field))
    monkeypatch.setattr(sources, "_edition_headers", AsyncMock(return_value=headers))
    with pytest.raises(sources.FHIRCatalogSourceError, match="named_edition_header_missing"):
        await sources.fhir_catalog_header_schema(object(), read_profile(), "endpoint-one", "current")


@pytest.mark.asyncio
async def test_endpoint_filtered_inventory_refuses_foreign_or_missing_endpoints(monkeypatch):
    profile = read_profile()
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="repeatable read"))
    connection.fetch = AsyncMock(return_value=[{"endpoint_id": "foreign"}])
    with pytest.raises(sources.FHIRCatalogSourceError, match="endpoint_scope_changed"):
        await anext(sources.iter_fhir_catalog_sources(connection, profile, endpoint_ids=["requested"]))
    connection.fetch.return_value = []
    with pytest.raises(sources.FHIRCatalogSourceError, match="endpoint_unavailable"):
        await anext(sources.iter_fhir_catalog_sources(connection, profile, endpoint_ids=["requested"]))
    for options in ({"endpoint_ids": []}, {"endpoint_ids": ["same", "same"]}, {"after_endpoint": " padded "}):
        with pytest.raises(sources.FHIRCatalogSourceError):
            await anext(sources.iter_fhir_catalog_sources(connection, profile, **options))
    assert connection.fetch.await_count == 2


def test_partial_content_receipt_preserves_explicit_scope_without_promising_completeness():
    selected = content_descriptor()
    selected["complete"] = False
    selected["admission"].update(
        resource_count=1,
        resource_counts={"InsurancePlan": 1},
        resource_types=["InsurancePlan"],
        selected_resources=["InsurancePlan"],
        proof_resource_scope=None,
        completion_required_version=None,
        completion_sha256=None,
    )
    original = deepcopy(selected)
    assert sources.validate_fhir_catalog_source_descriptor(selected) == original
    assert selected == original


@pytest.mark.parametrize("tables", [None, {"header": {}}, {"header": {}, "resource": {}}])
def test_source_descriptor_refuses_incomplete_named_storage(tables):
    selected = content_descriptor()
    selected["tables"] = tables
    with pytest.raises(sources.FHIRCatalogSourceError):
        sources.validate_fhir_catalog_source_descriptor(selected)
