# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

generation = importlib.import_module("process.reference_family_result_generation")


def _serving(lineage: str, value: int):
    return {
        "origin_lineage_id": lineage,
        "origin_generation": value,
        "published_at": "2026-09-14T08:30:00Z",
    }


def test_closed_relation_families_are_ordered_and_distinct():
    assert generation.RELATION_NAMES_BY_IMPORTER == {
        "label": ("label",),
        "mrf": (
            "issuer",
            "plan",
            "plan_formulary",
            "plan_benefits_marketplace",
            "plan_transparency",
            "plan_drug_raw",
            "plan_drug_stats",
            "plan_drug_tier_stats",
            "log",
            "plan_npi_raw",
            "plan_networktier",
            "mrf_address",
            "mrf_address_evidence",
        ),
        "mrf-address": ("mrf_address", "mrf_address_evidence"),
        "plan-attributes": (
            "plan_attributes",
            "plan_prices",
            "plan_rating_areas",
            "plan_benefits",
        ),
        "places-zcta": ("pricing_places_zcta",),
        "geo": ("geo_zip_lookup",),
        "geo-census": ("geo_zip_census_profile",),
        "lodes": ("lodes_workplace_aggregate",),
        "cms-doctors": ("doctor_clinician_address", "cms_doctor_education"),
        "tiger": ("zip_state", "zcta5"),
        "medicare-enrollment": (
            "medicare_enrollment_county_stats",
            "medicare_enrollment_stats",
        ),
        "pharmacy-economics": ("pharmacy_economics_summary",),
        "terminology-synonyms": ("terminology_synonym",),
        "provider-quality": (
            "pricing_qpp_provider",
            "pricing_svi_zcta",
            "pricing_provider_quality_measure",
            "pricing_provider_quality_domain",
            "pricing_provider_quality_score",
            "pricing_provider_quality_feature",
            "pricing_provider_quality_procedure_lsh",
            "pricing_provider_quality_peer_target",
        ),
    }
    assert all(len(names) == len(set(names)) for names in generation.RELATION_NAMES_BY_IMPORTER.values())


def test_generation_authority_accepts_complete_family_state():
    authority = generation.validate_reference_family_result_generation_authority(
        {
            "importer_id": "medicare-enrollment",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 14,
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
            "origin_generation": 8,
            "published_at": datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
            "relation_oids": [10, 11],
        }
    )

    assert authority.importer_id == "medicare-enrollment"
    assert authority.local_generation == 14
    assert authority.serving_generation.origin_generation == 8
    assert authority.relation_oids == (10, 11)
    assert authority.as_dict()["serving_generation"]["origin_generation"] == 8
    assert authority.as_dict()["relation_oids"] == [10, 11]


@pytest.mark.parametrize(
    ("importer_id", "relation_oids"),
    [
        ("unknown", [10]),
        ("medicare-enrollment", [10]),
        ("medicare-enrollment", [10, 10]),
        ("geo-census", [10, 11]),
        ("places-zcta", [0]),
    ],
)
def test_generation_authority_rejects_wrong_family_or_oids(importer_id, relation_oids):
    with pytest.raises(RuntimeError, match="authority is invalid|serving generation is invalid"):
        generation.validate_reference_family_result_generation_authority(
            {
                "importer_id": importer_id,
                "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
                "local_generation": 14,
                "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
                "origin_generation": 8,
                "published_at": "2026-09-14T08:30:00Z",
                "relation_oids": relation_oids,
            }
        )


def test_automatic_order_requires_strictly_newer_same_lineage():
    lineage = "c8f27af1-56ba-4cda-82d8-0fc67650918f"
    generation.require_reference_family_automatic_generation_order(
        _serving(lineage, 8),
        _serving(lineage, 7),
    )


@pytest.mark.parametrize(
    ("candidate", "incumbent"),
    [
        (None, None),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 8),
            _serving("edfc559e-0067-43ed-b7fa-983fdb7077fe", 7),
        ),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
        ),
    ],
)
def test_automatic_order_fails_closed_without_monotonic_same_lineage(candidate, incumbent):
    with pytest.raises(ValueError, match="unavailable|unsupported"):
        generation.require_reference_family_automatic_generation_order(candidate, incumbent)


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"origin_lineage_id": "bad", "origin_generation": 1, "published_at": "2026-09-14T08:30:00Z"},
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 0,
            "published_at": "2026-09-14T08:30:00Z",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": "bad",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": datetime.datetime(2026, 9, 14, 8, 30),
        },
    ],
)
def test_serving_generation_rejects_malformed_identity(value):
    with pytest.raises(ValueError, match="invalid"):
        generation.validate_reference_family_serving_generation(value)


@pytest.mark.parametrize(
    "value",
    [
        object(),
        {"importer_id": "places-zcta", "local_lineage_id": "bad", "local_generation": 0},
        {
            "importer_id": "places-zcta",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": -1,
        },
        {
            "importer_id": "places-zcta",
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 0,
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        },
    ],
)
def test_generation_authority_rejects_malformed_state(value):
    with pytest.raises(RuntimeError, match="unavailable|invalid|incomplete"):
        generation.validate_reference_family_result_generation_authority(value)


@pytest.mark.asyncio
async def test_database_adapters_cover_execute_fallbacks():
    result = SimpleNamespace(
        mappings=lambda: SimpleNamespace(one_or_none=lambda: {"value": 1}),
        all=lambda: [("value", 1)],
    )
    database = SimpleNamespace(execute=AsyncMock(return_value=result))

    assert await generation._first(database, "statement", value=1) == {"value": 1}
    assert await generation._all(database, "statement", value=1) == [("value", 1)]

    direct = SimpleNamespace(
        first=AsyncMock(return_value={"direct": True}),
        all=AsyncMock(return_value=[("direct", 2)]),
    )
    assert await generation._first(direct, "statement") == {"direct": True}
    assert await generation._all(direct, "statement") == [("direct", 2)]


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[("wrong", 10)], [("pricing_places_zcta", None)]])
async def test_relation_oid_read_rejects_malformed_rows(rows):
    database = SimpleNamespace(all=AsyncMock(return_value=rows))
    with pytest.raises(RuntimeError, match="relations are unavailable"):
        await generation.current_reference_family_relation_oids(
            database,
            importer_id="places-zcta",
            schema_name="mrf",
        )


@pytest.mark.asyncio
async def test_generation_publication_rejects_missing_or_changed_authority(monkeypatch):
    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        1,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    monkeypatch.setattr(
        generation,
        "current_reference_family_relation_oids",
        AsyncMock(return_value=(10,)),
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))

    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_local_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    changed = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        1,
        None,
        None,
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value={"updated": True}))
    monkeypatch.setattr(
        generation,
        "validate_reference_family_result_generation_authority",
        lambda _row: changed,
    )
    with pytest.raises(RuntimeError, match="changed during adoption"):
        await generation.publish_adopted_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf", source_generation=None
        )


@pytest.mark.asyncio
async def test_generation_reads_and_publication_guards(monkeypatch):
    with pytest.raises(ValueError, match="schema is invalid"):
        generation._schema_name("bad-name")

    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.read_reference_family_result_generation_authority(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        generation._MAX_GENERATION,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    with pytest.raises(RuntimeError, match="exhausted"):
        await generation.publish_local_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )

    current = generation.ReferenceFamilyResultGenerationAuthority(
        "places-zcta",
        "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        1,
        None,
        None,
    )
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=current),
    )
    monkeypatch.setattr(generation, "_first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_adopted_reference_family_generation(
            object(), importer_id="places-zcta", schema_name="mrf", source_generation=None
        )

    monkeypatch.setattr(
        generation,
        "current_reference_family_relation_oids",
        AsyncMock(return_value=(10,)),
    )
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_reference_family_serving_generation(
            object(), importer_id="places-zcta", schema_name="mrf"
        )
