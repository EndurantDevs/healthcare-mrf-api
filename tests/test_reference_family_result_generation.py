# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib

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
        "plan-attributes": (
            "plan_attributes",
            "plan_prices",
            "plan_rating_areas",
            "plan_benefits",
        ),
        "places-zcta": ("pricing_places_zcta",),
        "lodes": ("lodes_workplace_aggregate",),
        "medicare-enrollment": (
            "medicare_enrollment_county_stats",
            "medicare_enrollment_stats",
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


@pytest.mark.parametrize(
    ("importer_id", "relation_oids"),
    [
        ("unknown", [10]),
        ("medicare-enrollment", [10]),
        ("medicare-enrollment", [10, 10]),
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
