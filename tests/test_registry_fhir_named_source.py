# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical named publication descriptors and existing registry replay remain portable."""

from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_grant as grants
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    catalog_observation_descriptor_digest,
)
from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor
from tests.test_registry_fhir_catalog_source_companies import source_company
from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor, verifier_case, verify
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
        descriptor.pop(name)
    descriptor["catalog_schema"] = read_profile().catalog_schema
    descriptor["coordinates"]["dataset_schema"] = "copied_snapshot"
    digest = catalog_observation_descriptor_digest(descriptor)
    descriptor.update(accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest)
    return validate_fhir_catalog_source_descriptor(descriptor)


def test_named_descriptor_discards_legacy_physical_fields_before_hashing():
    descriptor = _named_descriptor()
    # These fields are ignored at the input boundary, including values that cannot be serialized.
    assert (
        validate_fhir_catalog_source_descriptor({**descriptor, **dict.fromkeys(_PHYSICAL_FIELDS, object())})
        == descriptor
    )
    assert descriptor["coordinates"]["dataset_schema"] == "copied_snapshot"
    assert _PHYSICAL_FIELDS.isdisjoint(descriptor)


@pytest.mark.parametrize("field", ["catalog_schema", "dataset_sha256", "endpoint_id", "source_aliases", "admission"])
def test_named_descriptor_still_checks_policy_scope_and_semantic_receipt(field):
    descriptor = _named_descriptor()
    descriptor[field] = "changed"
    with pytest.raises(ValueError):
        validate_fhir_catalog_source_descriptor(descriptor)


@pytest.mark.asyncio
async def test_named_registry_replay_uses_policy_and_semantics_without_database_identity(monkeypatch):
    descriptor = _named_descriptor()
    case = verifier_case(monkeypatch, (descriptor,))
    case.export.pop("database_identity")
    case.export["native_read_profile"] = read_profile().document()
    case.connection.fetchrow.side_effect = AssertionError("logical source replay must not read connection identity")
    await verify(case)
    case.export["replay"].assert_awaited_once()
    case.pool.acquire.assert_not_called()
    assert case.events[-3:] == [("exit",), ("retained",), ("projection",)]


@pytest.mark.asyncio
async def test_named_registry_replay_refuses_a_changed_configured_profile(monkeypatch):
    case = verifier_case(monkeypatch, (_named_descriptor(),))
    case.export.pop("database_identity")
    case.export["native_read_profile"] = {**read_profile().document(), "schema_name": "other_configured_source"}
    with pytest.raises(ValueError, match="source_export_invalid"):
        await verify(case)
    case.export["replay"].assert_not_awaited()
    case.pool.acquire.assert_not_called()


async def _verify_export_consumer(case, consumer):
    if consumer == "replay":
        await verify(case)
    else:
        case.snapshot["source_vector_json"][0]["source_companies"] = [source_company()]
        await grants.verify_current_fhir_catalog_source_companies(
            case.factory, case.connection, case.snapshot["source_vector_json"], case.descriptors, case.grant
        )


@pytest.mark.parametrize("consumer", ["replay", "companies"])
@pytest.mark.parametrize("variant", ["named_profile", "legacy_profile", "legacy_old"])
async def test_grant_export_variant_matches_descriptor_before_callbacks(monkeypatch, consumer, variant):
    descriptor = _named_descriptor() if variant == "named_profile" else observed_descriptor()
    case = verifier_case(monkeypatch, (descriptor,))
    case.export["source_companies"] = AsyncMock(return_value=(source_company(),))
    if variant != "legacy_old":
        case.export.pop("database_identity")
        case.export["native_read_profile"] = read_profile().document()
        case.connection.fetchrow.side_effect = AssertionError("profile-scoped export must not read physical identity")
    if variant == "legacy_profile":
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
            await _verify_export_consumer(case, consumer)
        case.export["replay"].assert_not_awaited()
        case.export["source_companies"].assert_not_awaited()
    else:
        await _verify_export_consumer(case, consumer)
        case.export[consumer if consumer == "replay" else "source_companies"].assert_awaited_once()
    case.export["inventory"].assert_not_called()
    case.export["legacy_bindings"].assert_not_called()
    case.pool.acquire.assert_not_called()
    assert ("exit",) in case.events
