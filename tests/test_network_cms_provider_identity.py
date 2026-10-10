# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS identity cannot collapse resource types or replace declared NPI authority."""

import hashlib
from dataclasses import FrozenInstanceError

import pytest

from process.network_cms_provider_identity import CMSProviderIdentity
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, _extraction_sql
from process.network_initial_cms_office_bindings import _CHECK_SQL, _MATCH_SQL, _SOURCE_SQL


def test_typed_identity_is_stable_and_disjoint_from_npi_and_organization():
    practitioner = CMSProviderIdentity("cms-npd", "Practitioner", "1234567893")
    expected = "cms_" + hashlib.sha256(b"cms-provider.v1|cms-npd|Practitioner|1234567893").hexdigest()
    assert practitioner.provider_id == expected and len(expected) == 68
    assert practitioner.provider_id != CMSProviderIdentity("cms-npd", "Organization", "1234567893").provider_id
    assert practitioner.provider_id != CMSProviderIdentity("cms-npd", "Practitioner", "1234567894").provider_id
    with pytest.raises(FrozenInstanceError):
        practitioner.resource_id = "different"


@pytest.mark.parametrize(
    "source,kind,resource_id",
    [
        ("other-source", "Practitioner", "p1"),
        ("cms-npd", "PractitionerRole", "p1"),
        ("cms-npd", "Practitioner", ""),
        ("cms-npd", "Practitioner", "p|1"),
        ("cms-npd", "Practitioner", "p/1"),
        ("cms-npd", "Practitioner", "é"),
        ("cms-npd", "Practitioner", "p" * 65),
        ("cms-npd", "Practitioner", 123),
        ("cms-npd", [], "p1"),
    ],
)
def test_identity_rejects_untyped_or_ambiguous_source_coordinates(source, kind, resource_id):
    with pytest.raises(ValueError, match="cms_provider_identity_invalid"):
        CMSProviderIdentity(source, kind, resource_id)


def _source(source_id):
    return PinnedFHIRMembershipSource(
        "synthetic_source", source_id, "endpoint", "dataset", "a" * 64, "release", 1, "scope", "2026-01-01"
    )


@pytest.mark.parametrize("reviewed", [False, True])
def test_only_cms_exact_raw_provider_witness_can_qualify_a_missing_npi(reviewed):
    sql = _extraction_sql(_source("cms-npd"), '"synthetic_registry"', reviewed)
    parameter = 12 if reviewed else 5
    assert "provider.payload_json::jsonb->>'source_id' IS NULL OR" in sql
    assert f"provider.payload_json::jsonb->>'source_id'=${parameter}" in sql
    for resource in ("page", "provider"):
        assert f"{resource}_witness.raw_payload_sha256 IS NOT NULL" in sql
        assert f"{resource}_witness.normalized_payload_hash={resource}.payload_hash" in sql
        assert f"{resource}_witness.source_id=${parameter}" in sql
        assert f"{resource}_witness.release_id=${parameter + 1}" in sql
        assert f"NOT jsonb_path_exists({resource}_witness.raw_payload_json::jsonb" in sql
    assert "CASE WHEN provider_system='npi' THEN jsonb_build_array" in sql
    assert "provider_resource_type,provider_resource_id,provider_payload_sha256,site_id" in sql
    assert "provider_id IS NULL OR (provider_system='npi' AND provider_id !~" in sql
    legacy = _extraction_sql(_source("synthetic_source"), '"synthetic_registry"', reviewed)
    assert "cms-provider.v1" not in legacy and "page_witness" not in legacy and "provider_witness" not in legacy
    assert "'npi' AS provider_system" in legacy


def test_office_correspondence_keeps_namespaces_and_npi_legacy_encoding_separate():
    source_sql = _SOURCE_SQL.format(source='"synthetic_source"')
    assert "DISTINCT provider_system,provider_id,location_id" in source_sql
    assert "WHEN provider_system='npi' THEN '{}'::jsonb" in source_sql
    assert "provider_system NOT IN ('npi','provider_directory')" in source_sql
    assert "address.entity_type=coalesce(office->>'provider_system','npi')" in _MATCH_SQL
    assert "address.entity_id=office->>'provider_id'" in _MATCH_SQL
    assert "ELSE address.npi IS NULL END" in _MATCH_SQL
    assert "address.inferred_npi IS NULL" in _MATCH_SQL
    assert "count(DISTINCT (provider_system,provider_id,location_id))" in _CHECK_SQL
