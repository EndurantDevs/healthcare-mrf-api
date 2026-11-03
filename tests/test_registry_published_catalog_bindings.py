# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published metadata bindings have a closed scope independent of legacy capture readers."""

from copy import deepcopy
from dataclasses import replace

import pytest

from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import (
    FHIRMembershipSourceError,
    PinnedFHIRMembershipSource,
    _reviewed_descriptor,
)
from process.registry_network_catalog_read import RegistryNetworkCatalogError, catalog_binding_document
from process.registry_source_binding_publication import _require_published_catalog_bindings


def published_document():
    return {
        "namespace": "source_binding",
        "binding_id": "01234567-89ab-4def-8123-456789abcdef",
        "revision": 1,
        "source_system": "fhir",
        "source_id": "example-source",
        "dataset_schema": "example_source",
        "dataset_id": "dataset-one",
        "producer_id": "registry-fhir-catalog-v1",
        "edition_id": "dataset-one",
        "source_key": "organization-one",
        "source_scope": {"organization_id": "organization-one", "source_generation": "b" * 64},
        "evidence_id": "b" * 64 + ":Organization/organization-one",
        "evidence_sha256": "a" * 64,
    }


def test_published_catalog_binding_accepts_exact_metadata_scope():
    assert catalog_binding_document(published_document()) is None


@pytest.mark.parametrize("field", ["producer_id", "edition_id", "source_key", "evidence_id", "evidence_sha256"])
def test_published_catalog_binding_rejects_inconsistent_fields(field):
    with pytest.raises(RegistryNetworkCatalogError):
        catalog_binding_document(published_document() | {field: "other"})


@pytest.mark.parametrize(
    "scope",
    [
        {"organization_id": "organization-one", "source_generation": "A" * 64},
        {"organization_id": "organization-one", "source_generation": "b" * 64, "capture_sha256": "a" * 64},
        {
            "organization_id": "organization-one",
            "legacy_uuid": "01234567-89ab-4def-8123-456789abcdef",
            "alias_scope": "scope",
        },
    ],
)
def test_published_catalog_binding_rejects_mixed_or_legacy_scope(scope):
    with pytest.raises(RegistryNetworkCatalogError):
        catalog_binding_document(published_document() | {"source_scope": scope})


def test_old_reader_refuses_catalog_only_pin_and_preserves_legacy_descriptor():
    source = PinnedFHIRMembershipSource(
        "example_source",
        "example-source",
        "endpoint-one",
        "dataset-one",
        "a" * 64,
        "release-one",
        123,
        "scope-one",
        "2026-01-01",
    )
    approved = ApprovedMembershipSource(1, "c" * 64)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "registry-fhir-catalog-v1", source.dataset_id
    )
    with pytest.raises(FHIRMembershipSourceError, match="typed Source reader"):
        _reviewed_descriptor(source, approved, coordinates, True)
    legacy = replace(coordinates, producer_id="legacy-fhir-v1")
    assert _reviewed_descriptor(source, approved, legacy, True)["alias_scope"] == source.alias_scope
    assert _reviewed_descriptor(source, None, None) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "another-source"),
        ("dataset_schema", "another_source"),
        ("dataset_id", "another-dataset"),
        ("edition_id", "another-edition"),
        ("source_key", "another-organization"),
        ("evidence_id", "c" * 64 + ":Organization/organization-one"),
        ("evidence_sha256", "c" * 64),
        ("source_scope_json", {"organization_id": "organization-one", "source_generation": "c" * 64}),
    ],
)
def test_publication_binds_each_selection_to_the_sealed_catalog_document(field, value):
    document = published_document()
    coordinate_by_field = {
        key: document[key]
        for key in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
    }
    pair = (document["source_key"], 42)
    binding_by_field = {**document, "source_scope_json": deepcopy(document["source_scope"])}
    catalog_by_pair = {pair: document}
    original = deepcopy(catalog_by_pair)
    _require_published_catalog_bindings(coordinate_by_field, {pair: binding_by_field}, catalog_by_pair)
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        _require_published_catalog_bindings(
            coordinate_by_field, {pair: binding_by_field | {field: value}}, catalog_by_pair
        )
    assert catalog_by_pair == original


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "source_id", "producer_id", "source_key", "capture_sha256"])
async def test_plan_tier_batch_history_rechecks_the_exact_source_and_scope(damage):
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import UUID

    from process.registry_record_store import RegistryActor
    from process.registry_source_binding_publication import _one_batch_history
    from tests.test_registry_fhir_plan_tier_bindings import binding

    selected = binding() | {"revision": 1}
    if damage == "capture_sha256":
        selected["source_scope_json"][damage] = "invalid"
    elif damage is not None:
        selected[damage] = "other"
    coordinate_by_field = {field: selected[field] for field in RegistryNetworkSourceCoordinates.__dataclass_fields__}
    batch_reference_by_field = {"idempotency_key": "batch-one", "request_sha256": "c" * 64, "custom_revision": 1}
    selection_by_field = {"record_kind": "network_binding", "record_id": selected["binding_id"], "revision": 1}
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "request_sha256": batch_reference_by_field["request_sha256"],
                "receipt": json.dumps({"custom_revision": 1, "records": [selection_by_field]}),
            }
        ),
        fetch=AsyncMock(
            return_value=[
                {
                    "record_key": selected["binding_id"],
                    "revision": 1,
                    "custom_revision": 1,
                    "request_sha256": batch_reference_by_field["request_sha256"],
                    "document": json.dumps(selected),
                }
            ]
        ),
    )
    actor = RegistryActor("platform_admin", UUID(int=1), "synthetic-client")
    arguments = (connection, '"registry"', {"source_coordinates": coordinate_by_field}, actor, batch_reference_by_field)
    if damage is None:
        assert await _one_batch_history(*arguments) == [selected]
    else:
        message = "registry_fhir_plan_tier_invalid" if damage == "capture_sha256" else "registry_source_batch_invalid"
        with pytest.raises(ValueError, match=message):
            await _one_batch_history(*arguments)
    connection.fetchrow.assert_awaited_once()
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_already_approved_network_does_not_select_a_newer_pending_correction():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.registry_source_binding_publication import _missing_network_selections

    connection = SimpleNamespace(fetch=AsyncMock(return_value=[{"record_key": "42", "archived": False}]))
    command_by_field = {"expected_approved_revision": 3}
    assert await _missing_network_selections(connection, '"registry"', command_by_field, {42}) == []
    connection.fetch.assert_awaited_once()
    assert "registry_approved_record" in connection.fetch.await_args.args[0]
    assert connection.fetch.await_args.args[1:] == (3, ["42"])
    connection.fetch.return_value[0]["archived"] = True
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        await _missing_network_selections(connection, '"registry"', command_by_field, {42})
