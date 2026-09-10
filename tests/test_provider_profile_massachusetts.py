from __future__ import annotations

import copy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_states as state_api
from api.provider_profile_composer_parts import _stable_item_id
from db.models import ProviderProfileSourcePublication


NPI = 1000000004
SOURCE_KEY = "massachusetts-borim"
GENERATION = "ma-generation"
SCHOOL = {"institution": "Example Medical School", "graduation_date": "2001-06-01", "graduation_date_precision": "day"}
TRAINING = {"institution": "Example Hospital", "program_type": "Resident", "specialty": "Internal Medicine", "attendance_start": "2001-07-01", "attendance_end": "2004-06-30"}


def _row(fact_id="school", *, category="education", fact_value=None, generation=GENERATION, **overrides):
    row_by_field = {
        "publication_source_key": SOURCE_KEY,
        "generation_id": generation,
        "source_published_at": datetime(2026, 9, 8),
        "run_status": "completed",
        "source_manifest": {
            "source": {
                "source_key": SOURCE_KEY,
                "source_kind": "state_regulator",
                "agency": "Massachusetts Board of Registration in Medicine",
                "jurisdiction": "MA",
                "coverage_scope": "full_physician_license",
                "registry_generation": "registry-generation",
            },
            "categories": ["education", "training"],
        },
        "fact_id": fact_id,
        "run_id": generation,
        "npi": NPI,
        "source_record_id": "parent-profile-record",
        "logical_fact_key": fact_id,
        "category": category,
        "fact_type": "education_history" if category == "education" else "postgraduate_training",
        "display": f"Reported {fact_id}",
        "value_json": copy.deepcopy(fact_value if fact_value is not None else SCHOOL if category == "education" else TRAINING),
        "availability": "available",
        "assertion_type": "self_reported",
        "verification_status": "not_independently_verified",
        "effective_start": None,
        "effective_end": None,
        "source_json": {
            "source_key": SOURCE_KEY,
            "source_record_id": "parent-profile-record",
            "source_path": f"educationAndTrainings.{fact_id}",
            "raw_fields": {"private_to_this_fact": fact_id},
            "quality_flags": [],
        },
        "sensitive": False,
        "public_default": True,
    }
    row_by_field.update(overrides)
    return row_by_field


def _projection(*rows):
    return state_api._state_projection(NPI, list(rows or [_row()]))


def _envelope(*rows, incumbent=None):
    return state_api.merge_state_profile_projection(NPI, incumbent, _projection(*rows))


def _compose(envelope, **options):
    return profile_api.compose_provider_profile(NPI, state_projection=envelope, fhir_profile=None, **options)


def _legacy_envelope(fact_value=None, *, category="education", sensitive=False):
    fact_by_field = {
        "type": "education_history" if category == "education" else "postgraduate_training",
        "display": "Florida reported fact",
        "value": copy.deepcopy(fact_value if fact_value is not None else SCHOOL),
        "source_record_id": "florida-record",
        "source_record_ids": ["florida-record"],
        "assertion_type": "practitioner_reported",
        "verification_status": "not_independently_verified",
        "sensitive": sensitive,
        "public_default": not sensitive,
    }
    return {
        "generation_id": "florida-generation",
        "profile": {"categories": {category: {"items": [fact_by_field], "availability": "available"}}, "sources": [{"source_key": "florida-mqa"}]},
        "evidence": {"records": [{"source_record_id": "florida-record", "source_key": "florida-mqa"}]},
    }


@pytest.mark.asyncio
async def test_loader_optional_relations_and_absent_pointer(monkeypatch):
    database = SimpleNamespace(scalar=AsyncMock(side_effect=[False, True, True]), all=AsyncMock(side_effect=[[], [SimpleNamespace(_mapping=_row())]]))
    monkeypatch.setattr(state_api, "db", database)
    assert await state_api.fetch_additional_state_profile_projections(NPI) == []
    assert await state_api.fetch_additional_state_profile_projections(NPI) == []
    projection, = await state_api.fetch_additional_state_profile_projections(NPI)
    assert projection["generation_id"] == GENERATION
    assert database.all.await_count == 2
    statement = str(database.all.call_args.args[0])
    assert "publication.current_run_id" in statement
    assert "JOIN mrf.provider_profile_import_run" in statement
    assert "JOIN mrf.provider_profile_fact" in statement
    assert "fact.run_id = publication.current_run_id" in statement
    assert "fact.npi = :npi" in statement
    assert database.all.call_args.kwargs == {"npi": NPI, "source_keys": [SOURCE_KEY, state_api.KENTUCKY_SOURCE_KEY]}
    assert set(database.scalar.call_args.kwargs) == {"publication", "run", "fact"}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["scalar", "all"])
async def test_loader_propagates_real_database_errors(monkeypatch, method):
    database = SimpleNamespace(scalar=AsyncMock(return_value=True), all=AsyncMock())
    getattr(database, method).side_effect = RuntimeError("database-unavailable")
    monkeypatch.setattr(state_api, "db", database)
    with pytest.raises(RuntimeError, match="database-unavailable"):
        await state_api.fetch_additional_state_profile_projections(NPI)


@pytest.mark.asyncio
async def test_loader_rejects_invalid_schema(monkeypatch):
    monkeypatch.setattr(ProviderProfileSourcePublication.__table__, "schema", "unsafe-schema")
    with pytest.raises(RuntimeError, match="schema_invalid"):
        await state_api.fetch_additional_state_profile_projections(NPI)


@pytest.mark.parametrize("row_by_field", [_row(fact_id=None), _row(sensitive=True), _row(public_default=False), _row(availability="restricted")])
def test_no_public_projection_for_absent_or_nonpublic_facts(row_by_field):
    assert _projection(row_by_field) is None


@pytest.mark.parametrize("status", ["running", "failed"])
def test_incomplete_publication_is_not_served(status):
    with pytest.raises(RuntimeError, match="publication_invalid"):
        _projection(_row(run_status=status))


def test_mixed_generations_and_wrong_source_fail_closed():
    with pytest.raises(RuntimeError, match="publication_invalid"):
        _projection(_row(), _row("other", generation="other-generation"))
    fact_by_field = _row()
    fact_by_field["source_json"]["source_key"] = "other-source"
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(fact_by_field)
    fact_by_field = _row()
    fact_by_field["source_manifest"]["source"]["source_key"] = "other-source"
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(fact_by_field)


def test_unrequested_categories_cannot_enter_public_projection():
    with pytest.raises(RuntimeError, match="categories_invalid"):
        _projection(_row(category="licenses"))
    fact_by_field = _row()
    fact_by_field["source_manifest"]["categories"].append("licenses")
    with pytest.raises(RuntimeError, match="categories_invalid"):
        _projection(fact_by_field)


def test_source_assertions_keep_values_flags_and_parent_identity():
    row_by_field = _row()
    row_by_field["source_json"]["quality_flags"] = ["graduation_date_in_future"]
    projection = _projection(row_by_field)
    item = projection["categories"]["education"]["items"][0]
    assertion, = item["assertions"]
    assert assertion["source_ids"] == [SOURCE_KEY]
    assert assertion["source_kind"] == "state_regulator"
    assert assertion["value"] == SCHOOL
    assert assertion["display"] == row_by_field["display"]
    assert assertion["quality_flags"] == ["graduation_date_in_future"]
    assert assertion["source_record_ids"] == [f"{SOURCE_KEY}:school"]
    evidence, = projection["evidence"]["records"]
    assert evidence["source_record_id"] == f"{SOURCE_KEY}:school"
    assert evidence["profile_source_record_id"] == "parent-profile-record"
    assert evidence["fact_id"] == "school"
    assert evidence["source_path"] == row_by_field["source_json"]["source_path"]


def test_massachusetts_only_has_actual_source_generation_and_no_florida_claim():
    profile = _compose(_envelope())
    assert profile["source_generations"] == {SOURCE_KEY: GENERATION}
    assert [source["source_key"] for source in profile["sources"]] == [SOURCE_KEY]
    assert profile["categories"]["training"]["availability"] == "not_reported"
    assert profile["categories"]["professional_experience"]["items"] == []
    assert "Florida" not in str(profile)
    assert profile["composer_version"] == "provider-profile-composer/v10"


@pytest.mark.parametrize("same_date", [True, False])
def test_florida_massachusetts_school_agreement_and_conflict(same_date):
    florida = _legacy_envelope()
    original = copy.deepcopy(florida)
    value_by_field = {**SCHOOL, "graduation_date": "2001-06-01" if same_date else "2001-07-01"}
    envelope = _envelope(_row(fact_value=value_by_field), incumbent=florida)
    profile = _compose(envelope)
    items = profile["categories"]["education"]["items"]
    assert len(items) == (1 if same_date else 2)
    assert envelope["evidence"] == florida["evidence"]
    assert florida == original
    assert profile["source_generations"] == {"state_regulator": "florida-generation", SOURCE_KEY: GENERATION}
    if same_date:
        assert items[0]["assertion_count"] == 2
        assert items[0]["source_kinds"] == ["state_regulator"]
        assert items[0]["corroborated_fields"] == ["institution", "graduation_year"]
        assert {record for assertion in items[0]["assertions"] for record in assertion["source_record_ids"]} == {"florida-record", f"{SOURCE_KEY}:school"}


def test_training_exact_duplicates_union_support_without_changing_public_item_id():
    florida = _legacy_envelope(TRAINING, category="training")
    original_id = _stable_item_id(NPI, "training", florida["profile"]["categories"]["training"]["items"][0])
    profile = _compose(_envelope(_row("training", category="training"), incumbent=florida))
    item, = profile["categories"]["training"]["items"]
    assert item["item_id"] == original_id
    assert item["assertion_count"] == 2
    assert len(item["assertions"]) == 2
    assert set(item["source_record_ids"]) == {"florida-record", f"{SOURCE_KEY}:training"}


def test_training_partial_dates_remain_separate_and_keep_flags_per_assertion():
    partial = _row("partial", category="training", fact_value={**TRAINING, "attendance_end": None})
    partial["source_json"]["quality_flags"] = ["attendance_end_not_reported"]
    profile = _compose(_envelope(_row("complete", category="training"), partial))
    items = profile["categories"]["training"]["items"]
    assert len(items) == len({item["item_id"] for item in items}) == 2
    assert sum(bool(item["quality_flags"]) for item in items) == 1
    assert profile["categories"]["professional_experience"]["items"] == []


def test_training_different_visibility_keeps_distinct_support_and_ids():
    florida = _legacy_envelope(TRAINING, category="training", sensitive=True)
    envelope = _envelope(_row("training", category="training"), incumbent=florida)
    public_item, = _compose(envelope)["categories"]["training"]["items"]
    items = _compose(envelope, include_sensitive=True)["categories"]["training"]["items"]
    assert len(items) == len({item["item_id"] for item in items}) == 2
    assert public_item["source_record_ids"] == [f"{SOURCE_KEY}:training"]
    assert public_item["item_id"] == next(item["item_id"] for item in items if not item["sensitive"])


def test_page_evidence_does_not_leak_sibling_facts_from_same_profile():
    envelope = _envelope(_row(), _row("training1", category="training"), _row("training2", category="training", fact_value={**TRAINING, "specialty": "Surgery"}), _row("hidden", sensitive=True))
    seen_fact_ids = set()
    for category, offset in [("education", 0), ("training", 0), ("training", 1)]:
        profile = _compose(envelope, requested_categories=[category], page_category=category, page_limit=1, page_offset=offset)
        evidence = profile_api.compose_provider_profile_evidence(state_projection=envelope, fhir_evidence=None, provider_profile=profile, page_category=category)
        records = evidence["sources"][SOURCE_KEY]["records"]
        item, = profile["categories"][category]["items"]
        assert {record["source_record_id"] for record in records} == set(item["source_record_ids"])
        assert len(records) == 1
        assert records[0]["profile_source_record_id"] == "parent-profile-record"
        assert records[0]["fact_id"] not in seen_fact_ids
        seen_fact_ids.add(records[0]["fact_id"])
    assert seen_fact_ids == {"school", "training1", "training2"}


def test_source_generation_changes_fence_pagination():
    before = _compose(_envelope(_row()), page_category="education", page_limit=1)
    after = _compose(_envelope(_row(generation="ma-next")), page_category="education", page_limit=1)
    assert before["generation_id"] != after["generation_id"]
    assert before["categories"]["education"]["items"][0]["item_id"] == after["categories"]["education"]["items"][0]["item_id"]


def test_missing_incumbent_payload_preserves_generation_without_relabeling_source():
    envelope = _envelope(incumbent={"profile": None, "generation_id": "florida-generation"})
    profile = _compose(envelope)
    assert profile["source_generations"] == {"state_regulator": "florida-generation", SOURCE_KEY: GENERATION}
    assert profile["sources"][0]["source_key"] == SOURCE_KEY


def test_public_descriptor_does_not_expose_internal_manifest_fields():
    row_by_field = _row()
    row_by_field["source_manifest"]["source"]["internal_field"] = "internal-only"
    assert "internal-only" not in str(_compose(_envelope(row_by_field)))


def test_three_compatible_partial_education_sources_corroborate():
    envelope = _envelope(_row(fact_value={**SCHOOL, "graduation_year": 2001}), incumbent=_legacy_envelope())
    envelope["profile"]["categories"]["education"]["items"].append({
        "type": "education_history",
        "display": "CMS reported education",
        "value": {"institution": "Example Medical School", "graduation_year": 2001},
        "source_kinds": ["cms_doctors"],
        "source_record_id": "cms-record",
        "sensitive": False,
        "public_default": True,
    })
    item, = _compose(envelope)["categories"]["education"]["items"]
    assert item["source_record_ids"] == ["cms-record", "florida-record", f"{SOURCE_KEY}:school"]
    assert item["assertion_count"] == len(item["assertions"]) == 3
    assert item["corroborated_fields"] == ["institution", "graduation_year"]


@pytest.mark.asyncio
async def test_combined_loader_keeps_every_independent_source(monkeypatch):
    from api.provider_profile_cms import _cms_projection
    cms_row_by_field = {
        "education_key": "cms-school", "medical_school": "Example Medical School", "graduation_year": 2001, "generation_id": "cms-generation",
        "source_json": {"generation_id": "cms-generation", "source_key": "cms-doctors", "dataset_id": "mj5m-pzi6", "source_url": "https://data.cms.gov/example.csv", "content_sha256": "a" * 64, "downloaded_at": "2026-09-08T00:00:00"},
    }
    monkeypatch.setattr(profile_api, "fetch_state_profile_projection", AsyncMock(return_value=_legacy_envelope()))
    monkeypatch.setattr(profile_api, "fetch_cms_education_projection", AsyncMock(return_value=_cms_projection(NPI, [cms_row_by_field])))
    monkeypatch.setattr(profile_api, "fetch_additional_state_profile_projections", AsyncMock(return_value=[_projection()]))
    envelope = await profile_api.fetch_provider_profile_projection(NPI)
    fhir_profile_by_field = {"generation_id": "fhir-generation", "facts": {"name": {"items": [{"value": {"text": "Alex Example"}, "source_ids": ["payer"]}]}}, "sources": [{"source_id": "payer"}]}
    profile = profile_api.compose_provider_profile(NPI, state_projection=envelope, fhir_profile=fhir_profile_by_field)
    assert profile["source_generations"] == {"state_regulator": "florida-generation", "cms_doctors": "cms-generation", SOURCE_KEY: GENERATION, "provider_directory_fhir": "fhir-generation"}
    item, = profile["categories"]["education"]["items"]
    assert item["assertion_count"] == 3
    assert len(item["assertions"]) == 3
    assert profile["categories"]["identity"]["items"][0]["value"] == {"text": "Alex Example"}
    evidence = profile_api.compose_provider_profile_evidence(state_projection=envelope, fhir_evidence={"facts": fhir_profile_by_field["facts"]}, provider_profile=profile)
    assert set(evidence["sources"]) == {"state_regulator", "cms_doctors", SOURCE_KEY, "provider_directory_fhir"}
