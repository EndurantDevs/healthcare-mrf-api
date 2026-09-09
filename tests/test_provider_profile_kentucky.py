# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep Kentucky school assertions source-bound through public composition."""

import copy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api import provider_profile_states as state_api
from process.kentucky_profile_rows import SCHEMA_VERSION, SOURCE_KEY, parse_profile
from tests.test_kentucky_profile_rows import CANDIDATE, EVIDENCE, FIELDS, response_html
from tests.test_provider_profile_cms import _cms_projection
from tests.test_provider_profile_massachusetts import _legacy_envelope, _row as _massachusetts_row

NPI = 1000000004
GENERATION = "kentucky-generation"


def _row(*, license_number="C0007", generation=GENERATION, school="Example Medical School", year="2001", **overrides):
    """Compose the actual Kentucky parser's synthetic source-backed education fact."""
    fields = [(label, {"License": license_number, "Medical School": school, "Year Graduated": year}.get(label, value))
              for label, value in FIELDS]
    _record, facts = parse_profile(
        response_html(fields, license_number=license_number), license_number=license_number,
        candidates=[{**CANDIDATE, "license_number": license_number}], evidence={**EVIDENCE, "run_id": generation},
    )
    return {
        **facts[0], "publication_source_key": SOURCE_KEY, "generation_id": generation,
        "source_published_at": datetime(2026, 9, 8), "run_status": "completed", "run_schema_version": SCHEMA_VERSION, "run_jurisdiction": "KY",
        "source_manifest": {"categories": ["education"], "source": {
            "source_key": SOURCE_KEY, "source_kind": "state_regulator", "agency": "Kentucky Board of Medical Licensure",
            "jurisdiction": "KY", "coverage_scope": "nppes_ky_alphanumeric_physician_license_cohort",
            "registry_generation": "ky-registry-generation",
        }}, **overrides,
    }


def _projection(*rows):
    return state_api._state_projection(NPI, list(rows or [_row()]), source_key=SOURCE_KEY)


def _combined_projection(*rows, include_kentucky=True):
    """Reuse existing incumbent fixtures and their exact source assertions."""
    projection = cms_api.merge_cms_education_projection(NPI, _legacy_envelope(), _cms_projection())
    projection = state_api.merge_state_profile_projection(NPI, projection, state_api._state_projection(NPI, [_massachusetts_row()]))
    return state_api.merge_state_profile_projection(NPI, projection, _projection(*rows)) if include_kentucky else projection


def _compose(projection, **options):
    return profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None, **options)


def test_kentucky_only_has_actual_attribution_context_and_original_assertion():
    row = _row(school="  EXAMPLE Medical School  ", year="2030")
    projection = _projection(row)
    profile = _compose(state_api.merge_state_profile_projection(NPI, None, projection))
    item, = profile["categories"]["education"]["items"]
    assertion, = item["assertions"]
    assert assertion["source_ids"] == [SOURCE_KEY]
    assert assertion["assertion_type"] == "source_reported"
    assert assertion["verification_status"] == "not_independently_verified"
    assert assertion["value"] == row["value_json"]
    assert assertion["quality_flags"] == ["graduation_year_in_future"]
    assert profile["source_generations"] == {SOURCE_KEY: GENERATION}
    assert profile["sources"] == [row["source_manifest"]["source"]]
    assert profile["important_context"] == [state_api.SOURCE_CONTEXT[SOURCE_KEY]]
    assert profile["categories"]["training"] == {
        "availability": "unavailable", "items": [], "total": 0, "returned": 0, "truncated": False,
    }
    assert profile["categories"]["professional_experience"]["items"] == []
    assert profile["composer_version"] == "provider-profile-composer/v9"
    evidence, = projection["evidence"]["records"]
    assert evidence["source_record_id"] == f"{SOURCE_KEY}:{row['fact_id']}"
    assert evidence["profile_source_record_id"] == row["source_record_id"]
    assert evidence["raw_fields"] == {"Medical School": "  EXAMPLE Medical School  ", "Year Graduated": "2030"}


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("run_status", "running", "publication_invalid"), ("run_status", "failed", "publication_invalid"),
    ("run_schema_version", "ma-borim-profile/v1", "source_mismatch"),
    ("run_jurisdiction", "MA", "source_mismatch"),
    ("run_id", "other-generation", "publication_invalid"), ("npi", 1000000012, "publication_invalid"),
    ("category", "training", "categories_invalid"), ("fact_type", "postgraduate_training", "categories_invalid"),
])
def test_kentucky_rejects_wrong_generation_identity_schema_and_fact_type(field, value, reason):
    with pytest.raises(RuntimeError, match=reason):
        _projection(_row(**{field: value}))


@pytest.mark.parametrize(("field", "value"), [
    ("source_key", state_api.MASSACHUSETTS_SOURCE_KEY), ("source_kind", "cms_doctors"),
    ("jurisdiction", "MA"), ("agency", "Massachusetts Board of Registration in Medicine"),
])
def test_kentucky_manifest_cannot_claim_another_source(field, value):
    row = _row()
    row["source_manifest"]["source"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


@pytest.mark.parametrize(("field", "value"), [
    ("source_key", state_api.MASSACHUSETTS_SOURCE_KEY), ("source_record_id", "other-profile"),
    ("run_id", "other-generation"), ("schema_version", "wrong-schema"),
    ("agency", "Other board"), ("jurisdiction", "MA"),
])
def test_kentucky_evidence_cannot_cross_source_record_or_generation(field, value):
    row = _row()
    row["source_json"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


@pytest.mark.parametrize("categories", [[], ["training"], ["education", "training"], ["education", "licenses"]])
def test_kentucky_manifest_is_education_only(categories):
    row = _row()
    row["source_manifest"]["categories"] = categories
    with pytest.raises(RuntimeError, match="categories_invalid"):
        _projection(row)


def test_default_projection_call_remains_massachusetts_and_sources_cannot_be_relabelled():
    massachusetts = _massachusetts_row()
    assert state_api._state_projection(NPI, [massachusetts]) == state_api._state_projection(
        NPI, [massachusetts], source_key=state_api.MASSACHUSETTS_SOURCE_KEY,
    )
    with pytest.raises(RuntimeError, match="source_mismatch"):
        state_api._state_projection(NPI, [_row()])
    with pytest.raises(RuntimeError, match="source_mismatch"):
        state_api._state_projection(NPI, [massachusetts], source_key=SOURCE_KEY)
    with pytest.raises(RuntimeError, match="source_mismatch"):
        state_api._state_projection(NPI, [_row()], source_key="unknown-state")
    with pytest.raises(RuntimeError, match="publication_invalid"):
        _projection(_row(), _row(generation="other-generation"))


@pytest.mark.parametrize("changes", [{"fact_id": None}, {"sensitive": True}, {"public_default": False}, {"availability": "restricted"}])
def test_kentucky_does_not_serve_absent_or_nonpublic_facts(changes):
    assert _projection(_row(**changes)) is None


async def test_both_sources_are_loaded_in_one_snapshot_with_explicit_binding(monkeypatch):
    rows = [_row(), _massachusetts_row()]
    database = SimpleNamespace(scalar=AsyncMock(return_value=True), all=AsyncMock(
        return_value=[SimpleNamespace(_mapping=row) for row in rows],
    ))
    monkeypatch.setattr(state_api, "db", database)
    projections = await state_api.fetch_additional_state_profile_projections(NPI)
    assert [projection["source"]["source_key"] for projection in projections] == list(state_api.STATE_SOURCE_KEYS)
    database.all.assert_awaited_once()
    assert database.all.call_args.kwargs == {"npi": NPI, "source_keys": list(state_api.STATE_SOURCE_KEYS)}
    assert "publication.source_key = ANY" in str(database.all.call_args.args[0])
    rows[0]["publication_source_key"] = state_api.MASSACHUSETTS_SOURCE_KEY
    with pytest.raises(RuntimeError, match="publication_invalid"):
        await state_api.fetch_additional_state_profile_projections(NPI)


def test_four_sources_corroborate_school_year_and_preserve_each_original():
    incumbent = _combined_projection(include_kentucky=False)
    original = copy.deepcopy(incumbent)
    kentucky = _row(school="EXAMPLE Medical School")
    projection = state_api.merge_state_profile_projection(NPI, incumbent, _projection(kentucky))
    profile = _compose(projection)
    item, = profile["categories"]["education"]["items"]
    assert item["assertion_count"] == len(item["assertions"]) == 4
    assert item["corroborated_fields"] == ["institution", "graduation_year"]
    assert item["value"]["graduation_date"] == "2001-06-01"
    kentucky_assertions = [assertion for assertion in item["assertions"] if assertion.get("source_ids") == [SOURCE_KEY]]
    assert len(kentucky_assertions) == 1
    kentucky_assertion = kentucky_assertions[0]
    assert kentucky_assertion["value"] == {"institution": "EXAMPLE Medical School", "graduation_year": 2001}
    assert kentucky_assertion["verification_status"] == "not_independently_verified"
    assert incumbent == original
    assert projection["evidence"] == incumbent["evidence"] and projection["cms_evidence"] == incumbent["cms_evidence"]
    assert projection["additional_state_evidence"][state_api.MASSACHUSETTS_SOURCE_KEY] == incumbent["additional_state_evidence"][state_api.MASSACHUSETTS_SOURCE_KEY]
    assert profile["source_generations"] == {**_compose(incumbent)["source_generations"], SOURCE_KEY: GENERATION}
    assert state_api.SOURCE_CONTEXT[state_api.MASSACHUSETTS_SOURCE_KEY] in profile["important_context"]


def test_conflicts_multiple_schools_and_unknown_year_remain_distinct():
    rows = [_row(), _row(license_number="C0008", year="2002"),
            _row(license_number="C0009", school="Second Medical School", year="2006"),
            _row(license_number="C0010", year="")]
    items = _compose(_combined_projection(*rows))["categories"]["education"]["items"]
    assert len(items) == len({item["item_id"] for item in items}) == 4
    assert sorted(item["assertion_count"] for item in items) == [1, 1, 1, 4]
    assert {record for item in items for record in item["source_record_ids"] if record.startswith(SOURCE_KEY)} == {
        f"{SOURCE_KEY}:{row['fact_id']}" for row in rows
    }


def test_pagination_exposes_only_supporting_assertions_and_fences_kentucky_rotation():
    rows = [_row(), _row(license_number="C0008", school="Second Medical School", year="2005"), _row(license_number="C0009", sensitive=True)]
    projection = _combined_projection(*rows)
    seen_records = set()
    for offset in range(2):
        profile = _compose(projection, requested_categories=["education"], page_category="education", page_limit=1, page_offset=offset)
        item, = profile["categories"]["education"]["items"]
        evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None,
                                                                 provider_profile=profile, page_category="education")
        returned_ids = {record["source_record_id"] for source in evidence["sources"].values() for record in source["records"]}
        assert returned_ids == set(item["source_record_ids"])
        kentucky_record, = evidence["sources"][SOURCE_KEY]["records"]
        assert kentucky_record["fact_id"] not in seen_records
        seen_records.add(kentucky_record["fact_id"])
    assert seen_records == {row["fact_id"] for row in rows[:2]}
    before = _compose(_combined_projection(_row()))
    after = _compose(_combined_projection(_row(generation="kentucky-next")))
    assert before["generation_id"] != after["generation_id"]
    assert before["categories"]["education"]["items"][0]["item_id"] == after["categories"]["education"]["items"][0]["item_id"]
    assert before["composer_version"] == after["composer_version"] == "provider-profile-composer/v9"


async def test_absent_kentucky_preserves_incumbent_payload_and_generation(monkeypatch):
    incumbent = _combined_projection(include_kentucky=False)
    monkeypatch.setattr(profile_api, "fetch_state_profile_projection", AsyncMock(return_value=_legacy_envelope()))
    monkeypatch.setattr(profile_api, "fetch_cms_education_projection", AsyncMock(return_value=_cms_projection()))
    monkeypatch.setattr(profile_api, "fetch_additional_state_profile_projections", AsyncMock(
        return_value=[state_api._state_projection(NPI, [_massachusetts_row()])],
    ))
    actual = await profile_api.fetch_provider_profile_projection(NPI)
    assert actual == incumbent
    assert _compose(actual) == _compose(incumbent)
