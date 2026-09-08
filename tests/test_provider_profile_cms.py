from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api.endpoint import npi as npi_api
from api.provider_profile_composer_parts import _existing_items_by_fhir_key
from db.models import CMSDoctorEducation


NPI = 1000000004
CMS_GENERATION = "c" * 64


def _education_row(key="school-a", school="Example Medical School", year=2001, *, generation=CMS_GENERATION, flags=None):
    return {
        "education_key": key,
        "medical_school": school,
        "graduation_year": year,
        "generation_id": generation,
        "source_json": {
            "source_key": "cms-doctors",
            "dataset_id": "mj5m-pzi6",
            "generation_id": generation,
            "source_url": "https://data.cms.gov/example.csv",
            "content_sha256": "a" * 64,
            "downloaded_at": "2026-09-07T00:00:00",
            "row_number": 2,
            "raw_fields": {"NPI": str(NPI), "Med_sch": school or "OTHER", "Grd_yr": str(year or "")},
            "quality_flags": flags or [],
        },
    }


def _cms_projection(*education_rows):
    return cms_api._cms_projection(NPI, list(education_rows or [_education_row()]))


def _state_projection(*, education_value=None):
    state_fact_by_field = {
        "type": "education_history",
        "display": "State reported medical education",
        "value": education_value or {"institution": "State Medical School", "graduation_date": "2000-06-01"},
        "source_record_id": "state-school",
        "source_record_ids": ["state-school"],
        "assertion_type": "practitioner_reported",
        "verification_status": "not_independently_verified",
        "assertion_count": 1,
        "public_default": True,
        "sensitive": False,
    }
    return {
        "generation_id": "state-generation",
        "profile": {
            "categories": {"education": {"availability": "available", "items": [state_fact_by_field]}},
            "sources": [{"source_key": "florida-mqa", "source_kind": "state_regulator"}],
        },
        "evidence": {"records": [{"source_record_id": "state-school", "source_key": "florida-mqa"}]},
    }


def _compose_projection(projection, **options):
    return profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None, **options)


@pytest.mark.parametrize(("school", "year"), [("Example Medical School", None), (None, 2001), ("Example Medical School", 2001)])
def test_cms_preserves_partial_reported_education(school, year):
    cms_projection = _cms_projection(_education_row(school=school, year=year))
    profile_item = cms_projection["items"][0]

    assert profile_item["value"] == {"institution": school, "graduation_year": year}
    assert profile_item["source_kinds"] == ["cms_doctors"]
    assert profile_item["assertion_type"] == "cms_reported"
    assert profile_item["verification_status"] == "not_independently_verified"
    assert "None" not in profile_item["display"]
    assert cms_projection["evidence"]["records"][0]["raw_fields"]["Med_sch"] == (school or "OTHER")


def test_future_year_retains_frozen_quality_flag():
    cms_projection = _cms_projection(_education_row(year=2028, flags=["graduation_year_in_future"]))
    projection = cms_api.merge_cms_education_projection(NPI, None, cms_projection)
    provider_profile = _compose_projection(projection)
    profile_item = provider_profile["categories"]["education"]["items"][0]

    assert profile_item["value"]["graduation_year"] == 2028
    assert "Reported future graduation year: 2028" in profile_item["display"]
    assert profile_item["quality_flags"] == ["graduation_year_in_future"]
    assert profile_item["assertions"][0]["quality_flags"] == ["graduation_year_in_future"]
    assert provider_profile["categories"]["professional_experience"]["items"] == []
    assert "years" not in json.dumps(provider_profile.get("professional_summary", {})).lower()


def test_source_manifest_is_visible_in_evidence():
    cms_projection = _cms_projection()
    assert cms_projection["source"]["dataset_id"] == "mj5m-pzi6"
    assert cms_projection["source"]["content_sha256"] == "a" * 64
    assert cms_projection["source"]["source_url"] == "https://data.cms.gov/example.csv"
    assert cms_projection["source"]["downloaded_at"] == "2026-09-07T00:00:00"
    assert cms_projection["evidence"]["generation_id"] == CMS_GENERATION
    assert cms_projection["evidence"]["records"][0]["source_record_id"].startswith(f"cms-doctors:{CMS_GENERATION}:")


@pytest.mark.asyncio
async def test_loader_distinguishes_absent_table_and_rows(monkeypatch):
    database = SimpleNamespace(
        scalar=AsyncMock(side_effect=[None, "mrf.cms_doctor_education", "mrf.cms_doctor_education"]),
        all=AsyncMock(side_effect=[[], [SimpleNamespace(_mapping=_education_row()), SimpleNamespace(_mapping=_education_row("school-b", "Second School"))]]),
    )
    monkeypatch.setattr(cms_api, "db", database)

    assert await cms_api.fetch_cms_education_projection(NPI) is None
    assert await cms_api.fetch_cms_education_projection(NPI) is None
    cms_projection = await cms_api.fetch_cms_education_projection(NPI)

    assert len(cms_projection["items"]) == 2
    assert database.all.await_count == 2
    assert database.all.call_args.kwargs == {"npi": NPI}
    assert "ORDER BY education_key" in str(database.all.call_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["scalar", "all"])
async def test_loader_propagates_database_errors(monkeypatch, failure_point):
    database = SimpleNamespace(scalar=AsyncMock(return_value="mrf.cms_doctor_education"), all=AsyncMock())
    getattr(database, failure_point).side_effect = RuntimeError("database-unavailable")
    monkeypatch.setattr(cms_api, "db", database)
    with pytest.raises(RuntimeError, match="database-unavailable"):
        await cms_api.fetch_cms_education_projection(NPI)


@pytest.mark.asyncio
async def test_loader_rejects_unsafe_schema(monkeypatch):
    monkeypatch.setattr(CMSDoctorEducation.__table__, "schema", "unsafe-schema")
    with pytest.raises(RuntimeError, match="schema_invalid"):
        await cms_api.fetch_cms_education_projection(NPI)


def test_loader_rejects_inconsistent_generation():
    with pytest.raises(RuntimeError, match="generation_mixed"):
        _cms_projection(_education_row(), _education_row("school-b", generation="d" * 64))
    education_row = _education_row()
    education_row["source_json"]["generation_id"] = "d" * 64
    with pytest.raises(RuntimeError, match="generation_mismatch"):
        _cms_projection(education_row)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_cms", [False, True])
async def test_combined_loader_preserves_state_projection(monkeypatch, include_cms):
    state_projection = _state_projection()
    original_projection = copy.deepcopy(state_projection)
    state_loader = AsyncMock(return_value=state_projection)
    cms_loader = AsyncMock(return_value=_cms_projection() if include_cms else None)
    monkeypatch.setattr(profile_api, "fetch_state_profile_projection", state_loader)
    monkeypatch.setattr(profile_api, "fetch_cms_education_projection", cms_loader)
    monkeypatch.setattr(profile_api, "fetch_massachusetts_profile_projection", AsyncMock(return_value=None))

    projection = await profile_api.fetch_provider_profile_projection(NPI)

    assert projection["evidence"] == original_projection["evidence"]
    assert state_projection == original_projection
    assert len(projection["profile"]["categories"]["education"]["items"]) == (2 if include_cms else 1)
    state_loader.assert_awaited_once_with(NPI)
    cms_loader.assert_awaited_once_with(NPI)


def test_cms_only_generation_has_no_state_source():
    projection = cms_api.merge_cms_education_projection(NPI, None, _cms_projection())
    provider_profile = _compose_projection(projection)

    assert provider_profile["source_generations"] == {"cms_doctors": CMS_GENERATION}
    assert provider_profile["sources"][0]["source_kind"] == "cms_doctors"
    assert provider_profile["categories"]["education"]["items"][0]["source_kinds"] == ["cms_doctors"]
    assert _compose_projection(None) is None


def test_distinct_schools_preserve_state_and_fhir():
    projection = cms_api.merge_cms_education_projection(NPI, _state_projection(), _cms_projection(
        _education_row(), _education_row("school-b", "Second School"),
    ))
    fhir_profile_by_field = {
        "generation_id": "fhir-generation",
        "facts": {"name": {"items": [{"value": {"text": "Alex Example"}, "source_ids": ["payer-a"]}]}},
        "sources": [{"source_id": "payer-a", "label": "Example Directory"}],
    }
    provider_profile = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=fhir_profile_by_field)
    education_items = provider_profile["categories"]["education"]["items"]

    assert len(education_items) == 3
    assert len({profile_item["item_id"] for profile_item in education_items}) == 3
    assert provider_profile["categories"]["identity"]["items"][0]["source_kinds"] == ["provider_directory_fhir"]
    assert provider_profile["source_generations"] == {"state_regulator": "state-generation", "cms_doctors": CMS_GENERATION, "provider_directory_fhir": "fhir-generation"}
    assert projection["evidence"]["records"][0]["source_record_id"] == "state-school"


def test_equal_education_unions_assertions_and_evidence():
    state_projection = _state_projection(education_value={"institution": "Example Medical School", "graduation_year": 2001})
    cms_projection = _cms_projection()
    projection = cms_api.merge_cms_education_projection(NPI, state_projection, cms_projection)
    provider_profile = _compose_projection(projection)
    education_items = provider_profile["categories"]["education"]["items"]
    profile_item = education_items[0]

    assert len(education_items) == 1
    assert profile_item["source_kinds"] == ["cms_doctors", "state_regulator"]
    assert profile_item["assertion_count"] == 2
    assert {assertion["source_kind"] for assertion in profile_item["assertions"]} == {"cms_doctors", "state_regulator"}
    assert set(profile_item["source_record_ids"]) == {"state-school", cms_projection["items"][0]["source_record_id"]}
    evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=provider_profile)
    assert len(evidence["sources"]["state_regulator"]["records"]) == 1
    assert len(evidence["sources"]["cms_doctors"]["records"]) == 1
    repeated = cms_api.merge_cms_education_projection(NPI, projection, cms_projection)
    repeated_profile = _compose_projection(repeated)
    assert repeated_profile["categories"]["education"]["items"][0]["assertion_count"] == 2


def test_full_state_date_corroborates_year_without_losing_date():
    state_projection = _state_projection(education_value={"institution": "Example Medical School", "graduation_date": "2001-06-01"})
    projection = cms_api.merge_cms_education_projection(NPI, state_projection, _cms_projection())
    profile_item, = _compose_projection(projection)["categories"]["education"]["items"]
    assert profile_item["value"]["graduation_date"] == "2001-06-01"
    assert profile_item["corroborated_fields"] == ["institution", "graduation_year"]


def test_equal_education_keeps_grouped_state_evidence():
    state_projection = _state_projection(education_value={"institution": "Example Medical School", "graduation_year": 2001})
    del state_projection["profile"]["categories"]["education"]["items"][0]["source_record_id"]
    projection = cms_api.merge_cms_education_projection(NPI, state_projection, _cms_projection())
    provider_profile = _compose_projection(projection)
    profile_item = provider_profile["categories"]["education"]["items"][0]
    assert "state-school" in profile_item["source_record_ids"]
    assert profile_item["assertion_count"] == 2
    assert profile_item["source_kinds"] == ["cms_doctors", "state_regulator"]


def test_cms_survives_missing_state_payload():
    projection = cms_api.merge_cms_education_projection(NPI, {"profile": None, "generation_id": "state-generation"}, _cms_projection())
    provider_profile = _compose_projection(projection)
    assert len(provider_profile["categories"]["education"]["items"]) == 1
    assert provider_profile["source_generations"] == {"state_regulator": "state-generation", "cms_doctors": CMS_GENERATION}


def test_cms_pagination_filters_visible_evidence():
    projection = cms_api.merge_cms_education_projection(NPI, _state_projection(), _cms_projection(
        _education_row(), _education_row("school-b", "Second School"),
    ))
    seen_record_ids = set()
    for page_offset in range(3):
        provider_profile = _compose_projection(projection, page_category="education", page_limit=1, page_offset=page_offset)
        profile_item = provider_profile["categories"]["education"]["items"][0]
        evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=provider_profile, page_category="education")
        visible_record_ids = {source_record["source_record_id"] for source_payload in evidence["sources"].values() for source_record in source_payload["records"]}
        assert visible_record_ids == set(profile_item["source_record_ids"])
        assert not seen_record_ids.intersection(visible_record_ids)
        seen_record_ids.update(visible_record_ids)
    assert len(seen_record_ids) == 3


def test_category_filter_omits_cms_records():
    projection = cms_api.merge_cms_education_projection(NPI, None, _cms_projection())
    provider_profile = _compose_projection(projection, requested_categories=["identity"])
    evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=provider_profile)
    assert evidence["sources"]["cms_doctors"]["records"] == []
    unfiltered = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None)
    assert len(unfiltered["sources"]["cms_doctors"]["records"]) == 1


def test_generation_changes_preserve_fact_identity():
    first_projection = cms_api.merge_cms_education_projection(NPI, None, _cms_projection())
    next_projection = cms_api.merge_cms_education_projection(NPI, None, _cms_projection(_education_row(generation="d" * 64)))
    first_profile = _compose_projection(first_projection)
    next_profile = _compose_projection(next_projection)
    assert first_profile["generation_id"] != next_profile["generation_id"]
    assert first_profile["categories"]["education"]["items"][0]["item_id"] == next_profile["categories"]["education"]["items"][0]["item_id"]
    assert first_projection["cms_evidence"]["records"][0]["source_record_id"] != next_projection["cms_evidence"]["records"][0]["source_record_id"]


def test_fhir_index_respects_explicit_cms_provenance():
    cms_item = _cms_projection()["items"][0]
    legacy_state_item_by_field = {"type": "name", "value": "Example", "source_record_id": "state-name", "source_kinds": ["provider_directory_fhir"]}
    _existing_items_by_fhir_key({"items": [cms_item, legacy_state_item_by_field]})
    assert cms_item["source_kinds"] == ["cms_doctors"]
    assert legacy_state_item_by_field["source_kinds"] == ["provider_directory_fhir", "state_regulator"]


@pytest.mark.asyncio
async def test_cms_route_serves_and_fences_pages(monkeypatch):
    projection = cms_api.merge_cms_education_projection(NPI, None, _cms_projection())
    projection_loader = AsyncMock(return_value=projection)
    monkeypatch.setattr(npi_api, "fetch_provider_profile_projection", projection_loader)
    monkeypatch.setattr(npi_api, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    request = SimpleNamespace(args={"category": "education", "limit": "1", "include_evidence": "true"})
    first_response = await npi_api.get_provider_profile(request, str(NPI))
    first_body = json.loads(first_response.body)
    assert first_response.status == 200
    assert first_body["provider_profile"]["source_generations"] == {"cms_doctors": CMS_GENERATION}
    assert len(first_body["provider_profile_evidence"]["sources"]["cms_doctors"]["records"]) == 1
    request.args["generation_id"] = first_body["provider_profile"]["generation_id"]
    projection_loader.return_value = cms_api.merge_cms_education_projection(NPI, None, _cms_projection(_education_row(generation="d" * 64)))
    next_response = await npi_api.get_provider_profile(request, str(NPI))
    assert next_response.status == 409
    assert json.loads(next_response.body)["error"] == "provider_profile_generation_changed"
