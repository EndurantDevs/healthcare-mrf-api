# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local entity IDs never infer identity from mutable FHIR facts."""

import pytest

from db.models import (
    ProviderDirectoryCMSDoctorsGroupBinding,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryEntitySourceBinding,
    ProviderDirectoryOrganizationIdentity,
    ProviderDirectorySiteIdentity,
)
from process.provider_directory_entity_identity import (
    _source_resource_identity,
    bind_cms_doctors_group_batch,
    bind_entity_batch,
)


def test_entity_schema_keeps_fhir_binding_and_release_evidence_separate():
    assert set(ProviderDirectoryOrganizationIdentity.__table__.primary_key.columns.keys()) == {"organization_id"}
    assert set(ProviderDirectorySiteIdentity.__table__.primary_key.columns.keys()) == {"site_id"}
    assert set(ProviderDirectoryEntitySourceBinding.__table__.primary_key.columns.keys()) == {
        "source_id",
        "resource_type",
        "resource_id",
    }
    assert set(ProviderDirectoryEntityReleaseEvidence.__table__.primary_key.columns.keys()) == {
        "source_id",
        "resource_type",
        "resource_id",
        "release_id",
    }
    assert set(ProviderDirectoryCMSDoctorsGroupBinding.__table__.primary_key.columns.keys()) == {"org_pac_id"}


@pytest.mark.parametrize("resource", [{"resourceType": "Organization"}, {"resourceType": "Location", "id": ""}])
def test_missing_fhir_id_is_not_replaced_by_a_payload_hash(resource):
    with pytest.raises(ValueError, match="source_identity_invalid"):
        _source_resource_identity("cms-directory", "2026-09", resource)


def test_numeric_fhir_id_stays_opaque_and_unknown_organization_type_is_allowed():
    assert _source_resource_identity(
        "cms-directory",
        "2026-09",
        {"resourceType": "Organization", "id": "1234567890", "type": [{"text": "unknown"}]},
    ) == ("Organization", "1234567890")


def test_other_resource_types_cannot_enter_organization_or_site_binding():
    with pytest.raises(ValueError, match="resource_type_invalid"):
        _source_resource_identity("cms-directory", "2026-09", {"resourceType": "Practitioner", "id": "person"})
    with pytest.raises(ValueError, match="resource_type_invalid"):
        _source_resource_identity("cms-directory", "2026-09", {"resourceType": [], "id": "malformed"})


@pytest.mark.asyncio
async def test_batch_rejects_oversize_mixed_types_and_conflicting_duplicates_before_database_use():
    organization_by_field = {"resourceType": "Organization", "id": "org-1", "name": "One"}
    with pytest.raises(ValueError, match="batch_invalid"):
        await bind_entity_batch(
            None, source_id="cms-directory", release_id="release-one", resources=[organization_by_field] * 101
        )
    with pytest.raises(ValueError, match="resource_type_mixed"):
        await bind_entity_batch(
            None,
            source_id="cms-directory",
            release_id="release-one",
            resources=[
                organization_by_field,
                {"resourceType": "Location", "id": "site-1"},
            ],
        )
    with pytest.raises(ValueError, match="release_payload_conflict"):
        await bind_entity_batch(
            None,
            source_id="cms-directory",
            release_id="release-one",
            resources=[
                organization_by_field,
                {**organization_by_field, "name": "Changed"},
            ],
        )


@pytest.mark.asyncio
async def test_cms_doctors_group_batch_validates_exact_pac_ids_before_database_use():
    with pytest.raises(ValueError, match="batch_invalid"):
        await bind_cms_doctors_group_batch(None, org_pac_ids=[])
    with pytest.raises(ValueError, match="batch_invalid"):
        await bind_cms_doctors_group_batch(None, org_pac_ids=["123"] * 101)
    for invalid_id in ("", " 123", "123 ", "x" * 65):
        with pytest.raises(ValueError, match="org_pac_id_invalid"):
            await bind_cms_doctors_group_batch(None, org_pac_ids=["123", invalid_id])
