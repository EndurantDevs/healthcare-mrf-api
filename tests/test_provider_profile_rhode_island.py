# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep Rhode Island portfolio assertions bound to their published source."""

import copy
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api import provider_profile_states as state_api
from process.rhode_island_profile_binding import bind_profile
from process.rhode_island_profile_rows import SCHEMA_VERSION, SOURCE_KEY
from tests.test_provider_profile_cms import _cms_projection
from tests.test_provider_profile_massachusetts import _legacy_envelope
from tests.test_rhode_island_profile_binding import candidate
from tests.test_rhode_island_profile_rows import evidence, occurrence

NPI = 1000000004
GENERATION = "b" * 64


def _manifest():
    return {
        "control_run_id": "synthetic-control",
        "expected_current_run_id": None,
        "max_providers": None,
        "resume_from": None,
        "categories": ["education", "specialties", "privileges"],
        "license_types": ["MD", "DO"],
        "snapshot_sha256": "a" * 64,
        "snapshot_row_count": 1,
        "source": {
            "source_key": SOURCE_KEY,
            "source_kind": "state_regulator",
            "agency": "Rhode Island Department of Health",
            "jurisdiction": "RI",
            "coverage_scope": "active_md_do_excluding_limited_volunteer",
            "registry_generation": "a" * 64,
        },
    }


def _rows(*occurrences, license_number="MD00001", generation=GENERATION):
    profile_bytes = json.dumps(sum(occurrences or [occurrence(license_number)], [])).encode()
    metadata = {
        **evidence(profile_bytes, license_number),
        "run_id": generation,
        "schema_page": {
            "artifact_file": "page.json",
            "source_url": f"https://datahealth.ri.gov/find/providers/results.php?license={license_number}",
            "downloaded_at": "2026-09-09T00:00:00Z",
            "content_sha256": "e" * 64,
        },
    }
    _, facts = bind_profile(
        profile_bytes,
        license_number=license_number,
        evidence=metadata,
        candidates=[candidate(npi=NPI, joined_npi=NPI, license_number=license_number)],
    )
    return [
        {
            **fact,
            "published_at": datetime(2026, 9, 11),
            "publication_source_key": SOURCE_KEY,
            "generation_id": generation,
            "source_published_at": datetime(2026, 9, 11),
            "run_status": "completed",
            "run_schema_version": SCHEMA_VERSION,
            "run_jurisdiction": "RI",
            "source_manifest": _manifest(),
        }
        for fact in facts
    ]


def _envelope(rows, incumbent=None):
    projection = state_api._state_projection(NPI, rows, source_key=SOURCE_KEY)
    return state_api.merge_state_profile_projection(NPI, incumbent, projection)


@pytest.mark.parametrize("license_number", ["MD00001", "DO00001"])
def test_reported_portfolio_retains_values_and_complete_occurrence_provenance(license_number):
    rows = _rows(occurrence(license_number, School_Grad_Year="2030") * 2, license_number=license_number)
    original = copy.deepcopy(rows)
    envelope = _envelope(rows)
    profile = profile_api.compose_provider_profile(NPI, state_projection=envelope, fhir_profile=None)
    assert profile["source_generations"] == {SOURCE_KEY: GENERATION}
    assert profile["important_context"] == [state_api.SOURCE_CONTEXT[SOURCE_KEY]]
    for row in rows:
        (item,) = profile["categories"][row["category"]]["items"]
        (assertion,) = item["assertions"]
        assert item["type"] == row["fact_type"]
        assert assertion["value"] == row["value_json"]
        assert assertion["quality_flags"] == row["source_json"]["quality_flags"]
        assert assertion["source_ids"] == [SOURCE_KEY]
        assert assertion["assertion_type"] == "source_reported"
        assert assertion["verification_status"] == "not_independently_verified"
    for category in ("training", "professional_experience", "certifications"):
        assert profile["categories"][category]["items"] == []
    records = envelope["additional_state_evidence"][SOURCE_KEY]["records"]
    for record, row in zip(records, rows, strict=True):
        expected_by_field = {
            **row["source_json"],
            "source_record_id": f"{SOURCE_KEY}:{row['fact_id']}",
            "profile_source_record_id": row["source_record_id"],
            "fact_id": row["fact_id"],
        }
        assert record == expected_by_field
        assert record["occurrence_indexes"] == [0, 1]
    assert rows == original


def test_school_corroboration_retains_second_education_and_existing_fhir_facts():
    incumbent = cms_api.merge_cms_education_projection(NPI, _legacy_envelope(), _cms_projection())
    original = copy.deepcopy(incumbent)
    ri_facts = _rows(
        occurrence(School_Name="EXAMPLE Medical School"),
        occurrence(School_Name="Second Medical School", School_Grad_Year="2006"),
    )
    envelope = _envelope(ri_facts, incumbent)
    fhir_by_field = {
        "generation_id": "fhir-generation",
        "facts": {"name": {"items": [{"value": {"text": "Alex Example"}, "source_ids": ["synthetic-payer"]}]}},
        "sources": [{"source_id": "synthetic-payer"}],
    }
    profile = profile_api.compose_provider_profile(NPI, state_projection=envelope, fhir_profile=fhir_by_field)
    education = profile["categories"]["education"]["items"]
    assert len(education) == 2
    corroborated = next(education_item for education_item in education if education_item["assertion_count"] == 3)
    assert corroborated["corroborated_fields"] == ["institution", "graduation_year"]
    ri_assertion = next(
        assertion for assertion in corroborated["assertions"] if assertion.get("source_ids") == [SOURCE_KEY]
    )
    assert ri_assertion["value"] == ri_facts[0]["value_json"]
    assert profile["categories"]["identity"]["items"][0]["value"] == {"text": "Alex Example"}
    evidence_by_source = profile_api.compose_provider_profile_evidence(
        state_projection=envelope,
        fhir_evidence={"facts": fhir_by_field["facts"]},
        provider_profile=profile,
    )["sources"]
    assert set(evidence_by_source) == {SOURCE_KEY, "cms_doctors", "state_regulator", "provider_directory_fhir"}
    assert evidence_by_source["provider_directory_fhir"]["facts"] == {
        "name": {**fhir_by_field["facts"]["name"], "total": 1, "truncated": False}
    }
    assert incumbent == original
    assert envelope["evidence"] == incumbent["evidence"]
    assert envelope["cms_evidence"] == incumbent["cms_evidence"]


@pytest.mark.parametrize("category", ["education", "specialties", "privileges"])
def test_each_page_keeps_exact_supporting_fact_evidence(category):
    rows = _rows(
        occurrence(),
        occurrence(
            School_Name="Other School", Specialty_Name="Other Specialty", Mirian_Hospital="N", Bradley_Hospital="Y"
        ),
    )
    envelope = _envelope(rows)
    seen_fact_ids = set()
    for offset in range(2):
        profile = profile_api.compose_provider_profile(
            NPI,
            state_projection=envelope,
            fhir_profile=None,
            requested_categories=[category],
            page_category=category,
            page_limit=1,
            page_offset=offset,
        )
        (item,) = profile["categories"][category]["items"]
        sources = profile_api.compose_provider_profile_evidence(
            state_projection=envelope, fhir_evidence=None, provider_profile=profile, page_category=category
        )["sources"]
        (record,) = sources[SOURCE_KEY]["records"]
        assert item["source_record_ids"] == [record["source_record_id"]]
        assert record["fact_id"] not in seen_fact_ids
        seen_fact_ids.add(record["fact_id"])
    assert seen_fact_ids == {row["fact_id"] for row in rows if row["category"] == category}


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_status", "running"),
        ("run_schema_version", "wrong"),
        ("run_jurisdiction", "TN"),
        ("publication_source_key", "tennessee-tdh"),
        ("run_id", "other"),
        ("npi", 1000000012),
        ("category", "training"),
        ("fact_type", "board_certification"),
    ],
)
def test_wrong_publication_provider_and_fact_type_are_rejected(field, value):
    row = _rows()[0]
    row[field] = value
    with pytest.raises(RuntimeError):
        _envelope([row])


@pytest.mark.parametrize(
    "field,value",
    [
        ("categories", ["education"]),
        ("license_types", ["MD"]),
        ("max_providers", 1),
        ("resume_from", "MD00001"),
        ("snapshot_sha256", None),
        ("snapshot_sha256", "invalid"),
        ("registry_generation", "c" * 64),
        ("agency", "Other board"),
        ("jurisdiction", "TN"),
        ("coverage_scope", "all_physicians"),
    ],
)
def test_partial_or_mislabelled_manifest_is_rejected(field, value):
    row = _rows()[0]
    manifest = row["source_manifest"]
    target = manifest if field in manifest else manifest["source"]
    target[field] = value
    with pytest.raises(RuntimeError):
        _envelope([row])


@pytest.mark.parametrize(
    "field", ["run_id", "schema_version", "source_key", "source_record_id", "agency", "jurisdiction"]
)
def test_evidence_cannot_cross_source_record_or_generation(field):
    row = _rows()[0]
    row["source_json"][field] = "wrong"
    with pytest.raises(RuntimeError):
        _envelope([row])


@pytest.mark.parametrize(
    "changes", [{"fact_id": None}, {"sensitive": True}, {"public_default": False}, {"availability": "restricted"}]
)
def test_nonpublic_facts_do_not_enter_projection(changes):
    row = _rows()[0]
    row.update(changes)
    assert state_api._state_projection(NPI, [row], source_key=SOURCE_KEY) is None


async def test_loader_discovers_rhode_island_with_the_existing_single_snapshot_query(monkeypatch):
    rows = _rows()
    database = SimpleNamespace(
        scalar=AsyncMock(return_value=True), all=AsyncMock(return_value=[SimpleNamespace(_mapping=row) for row in rows])
    )
    monkeypatch.setattr(state_api, "db", database)
    projections = await state_api.fetch_additional_state_profile_projections(NPI)
    assert [projection["source"]["source_key"] for projection in projections] == [SOURCE_KEY]
    database.all.assert_awaited_once()
    assert database.all.call_args.kwargs == {"npi": NPI, "source_keys": list(state_api.STATE_SOURCE_KEYS)}
