# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as archive


def test_registry_is_closed_to_exact_ordered_replacement_families():
    assert archive.reference_family_spec("plan-attributes").table_names == (
        "plan_attributes",
        "plan_prices",
        "plan_rating_areas",
        "plan_benefits",
    )
    assert archive.reference_family_spec("places-zcta").table_names == ("pricing_places_zcta",)
    assert archive.reference_family_spec("lodes").table_names == ("lodes_workplace_aggregate",)
    assert archive.reference_family_spec("medicare-enrollment").table_names == (
        "medicare_enrollment_county_stats",
        "medicare_enrollment_stats",
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unsupported"):
        archive.reference_family_spec("npi")


def test_stage_schema_accepts_only_uuid_ownership():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    assert archive.reference_family_stage_schema(dataset_id) == (
        "reference_family_archive_550e8400e29b41d4a716446655440000"
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="UUID"):
        archive.reference_family_stage_schema(str(dataset_id))


def _manifest():
    metadata = {"source_release": "synthetic-2026", "source_receipt": "receipt-1"}
    _, metadata_sha256 = archive._source_metadata(metadata)
    tables = (
        archive.ReferenceTableReceipt(
            "PricingPlacesZcta",
            "pricing_places_zcta",
            "a" * 64,
            2,
        ),
    )
    schema_sha256 = archive._schema_digest(tables)
    return archive.ReferenceFamilyManifest(
        "places-zcta",
        tables,
        metadata,
        metadata_sha256,
        schema_sha256,
    )


def test_manifest_binds_explicit_provenance_but_remains_manual_only():
    manifest = _manifest()
    assert archive.validate_reference_family_manifest(manifest.as_dict()) == manifest
    assert manifest.as_dict()["publication_authority"] == "manual-only"

    tampered = manifest.as_dict()
    tampered["source_metadata"]["source_release"] = "different"
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="digest differs"):
        archive.validate_reference_family_manifest(tampered)


@pytest.mark.asyncio
async def test_capture_rejects_absent_provenance_before_database_access():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="metadata is required"):
        await archive.capture_reference_family_source(
            session,
            importer_id="places-zcta",
            schema_name="mrf",
            source_metadata={},
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_automatic_activation_is_rejected_before_mutation():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    ownership = archive.ReferenceFamilyStageOwnership(
        "places-zcta",
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        10,
        (("pricing_places_zcta", 11),),
    )
    incumbent = archive.ReferenceFamilyIncumbent(
        "places-zcta",
        "mrf",
        (("pricing_places_zcta", 12),),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="automatic"):
        await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=_manifest(),
            expected_incumbent=incumbent,
            authority="automatic",
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


def test_predecessor_schema_is_uuid_owned_and_bounded():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    predecessor = archive.reference_family_predecessor_schema(dataset_id)
    assert predecessor == "reference_family_predecessor_550e8400e29b41d4a716446655440000"
    assert len(predecessor.encode()) <= 63


def test_model_index_ddl_rejects_unreviewed_sql_fragments():
    model_type = archive.reference_family_spec("places-zcta").model_types[0]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unsupported"):
        archive._additional_index_sql(
            "mrf",
            model_type,
            {"index_elements": ("zcta",), "where": "zcta IS NOT NULL"},
        )
