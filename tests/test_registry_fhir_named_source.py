# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical named publication descriptors stay portable across copied snapshots."""

from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_grant as grants
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    catalog_observation_descriptor_digest,
)
from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor
from tests.test_registry_fhir_catalog_source_companies import source_company
from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor, verifier_case
from tests.test_registry_fhir_catalog_sources import read_profile

_PHYSICAL_FIELDS = {
    "resource_table_oid",
    "resource_leaf_oids",
    "header_table_oid",
    "database_oid",
    "native_catalog_sha256",
}


def _named_descriptor():
    descriptor = observed_descriptor()
    for name in _PHYSICAL_FIELDS:
        descriptor.pop(name, None)
    descriptor.pop("tables")
    descriptor["catalog_schema"] = read_profile().catalog_schema
    descriptor["coordinates"]["dataset_schema"] = "copied_snapshot"
    digest = catalog_observation_descriptor_digest(descriptor)
    descriptor.update(accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest)
    return validate_fhir_catalog_source_descriptor(descriptor)


def test_named_descriptor_discards_legacy_physical_fields_before_hashing():
    descriptor = _named_descriptor()
    # These fields are ignored at the input boundary, including values that cannot be serialized.
    legacy_by_field = {key: value for key, value in descriptor.items() if key != "tables"}
    legacy_by_field.update(catalog_schema=descriptor["tables"]["header"]["schema"])
    legacy_by_field.update(dict.fromkeys(_PHYSICAL_FIELDS, object()))
    assert validate_fhir_catalog_source_descriptor(legacy_by_field) == descriptor
    assert descriptor["coordinates"]["dataset_schema"] == "copied_snapshot"
    assert _PHYSICAL_FIELDS.isdisjoint(descriptor)


@pytest.mark.parametrize("field", ["catalog_schema", "dataset_sha256", "endpoint_id", "source_aliases", "admission"])
def test_named_descriptor_still_checks_policy_scope_and_semantic_receipt(field):
    descriptor = _named_descriptor()
    descriptor[field] = "changed"
    with pytest.raises(ValueError):
        validate_fhir_catalog_source_descriptor(descriptor)


@pytest.mark.parametrize("variant", ["named_profile", "legacy_profile", "legacy_old"])
async def test_grant_export_variant_matches_descriptor_before_callbacks(monkeypatch, variant):
    descriptor = _named_descriptor() if variant == "named_profile" else observed_descriptor()
    case = verifier_case(monkeypatch, (descriptor,))
    case.export["source_companies"] = AsyncMock(return_value=(source_company(),))
    if variant != "legacy_old":
        case.export.pop("database_identity", None)
        case.export["native_read_profile"] = read_profile().document()
        case.connection.fetchrow.side_effect = AssertionError("profile-scoped export must not read physical identity")
    # A profile-scoped export re-observes saved legacy descriptors semantically, so they stay refreshable.
    case.snapshot["source_vector_json"][0]["source_companies"] = [source_company()]
    await grants.verify_current_fhir_catalog_source_companies(
        case.factory, case.connection, case.snapshot["source_vector_json"], case.descriptors, case.grant
    )
    case.export["source_companies"].assert_awaited_once()
    case.export["inventory"].assert_not_called()
    case.export["legacy_bindings"].assert_not_called()
    case.pool.acquire.assert_not_called()
    assert ("exit",) in case.events
