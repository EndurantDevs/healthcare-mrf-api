# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preserve independently published New York facts through profile composition."""

import copy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api import provider_profile_states as state_api
from api.endpoint import npi as npi_api
from process import new_york_nysed_profile as nysed
from process import new_york_profile_rows as nypp
from tests.test_new_york_nysed_profile import _profile_body
from tests.test_new_york_profile_rows import RESPONSE, parsed_education
from tests.test_provider_profile_cms import _cms_projection
from tests.test_provider_profile_display import _structured_fhir_profile_by_key
from tests.test_provider_profile_massachusetts import NPI, _legacy_envelope
from tests.test_provider_profile_massachusetts import _row as massachusetts_row

NYPP = nypp.SOURCE_KEY
NYSED = nysed.SOURCE_KEY
SNAPSHOT = "d" * 64


def published_rows(facts, source_key):
    """Represent completed independent publication rows using actual parser output."""
    source_evidence = facts[0]["source_json"]
    categories = ["education", "training", "certifications"] if source_key == NYPP else ["education", "licenses"]
    manifest_by_field = {
        "snapshot_sha256": SNAPSHOT,
        "categories": categories,
        "source": {
            "source_key": source_key,
            "source_kind": "state_regulator",
            "agency": source_evidence["agency"],
            "jurisdiction": "NY",
            "coverage_scope": "supported_nppes_derived_ny_physician_license_roots",
            "registry_generation": SNAPSHOT,
        },
    }
    return [
        {
            **fact,
            "npi": NPI,
            "publication_source_key": source_key,
            "generation_id": fact["run_id"],
            "source_published_at": "2026-09-11T00:00:00+00:00",
            "run_status": "completed",
            "run_schema_version": source_evidence["schema_version"],
            "run_jurisdiction": "NY",
            "source_manifest": copy.deepcopy(manifest_by_field),
        }
        for fact in facts
    ]


def rows(source_key, *, generation=None, school="Example Medical School", schools=None):
    generation = generation or ("a" * 64 if source_key == NYPP else "b" * 64)
    if source_key == NYPP:
        payload = copy.deepcopy(RESPONSE)
        payload["data"]["medSchools"] = schools or [{"schoolName": school, "gradDate": "2001"}]
        _, facts = parsed_education(payload, evidence={"run_id": generation, "artifact_id": "captured-profile"})
        # The managed bundle keeps the capture identity alongside its bundle reference.
        for fact in facts:
            fact["source_json"]["capture_artifact_id"] = fact["source_json"]["artifact_id"]
            fact["source_json"]["artifact_id"] = "retained-bundle"
    else:
        body = nysed.encoded_json(_profile_body(schoolName=school, schoolDegreeDate="June 1, 2001"))
        _, facts = nysed.parse_profile(
            body,
            license_number="654321",
            evidence={
                "run_id": generation,
                "artifact_id": "captured-license",
                "source_url": nysed.request_descriptor("654321")["source_url"],
                "downloaded_at": "2026-09-11T00:00:00+00:00",
                "content_sha256": hashlib.sha256(body).hexdigest(),
            },
        )
    return published_rows(facts, source_key)


def projection(source_key, fact_rows=None):
    return state_api._state_projection(NPI, rows(source_key) if fact_rows is None else fact_rows, source_key=source_key)


def envelope(nypp_rows=None, nysed_rows=None, *, incumbent=None):
    for source_key, fact_rows in ((NYPP, nypp_rows), (NYSED, nysed_rows)):
        incumbent = state_api.merge_state_profile_projection(NPI, incumbent, projection(source_key, fact_rows))
    return incumbent


def compose(state_projection, **options):
    return profile_api.compose_provider_profile(NPI, state_projection=state_projection, fhir_profile=None, **options)


@pytest.fixture(params=[NYPP, NYSED])
def source_key(request):
    return request.param


def test_each_source_preserves_original_values_capture_and_meaning(source_key):
    fact_rows = rows(source_key)
    original = copy.deepcopy(fact_rows)
    state = projection(source_key, fact_rows)
    profile = compose(state_api.merge_state_profile_projection(NPI, None, state))
    evidence_by_id = {record["fact_id"]: record for record in state["evidence"]["records"]}
    assert profile["source_generations"] == {source_key: fact_rows[0]["run_id"]}
    assert profile["important_context"] == [state_api.SOURCE_CONTEXT[source_key]]
    assert profile["sources"] == [fact_rows[0]["source_manifest"]["source"]]
    for fact in fact_rows:
        (item,) = profile["categories"][fact["category"]]["items"]
        (assertion,) = item["assertions"]
        assert assertion["value"] == fact["value_json"]
        assert assertion["source_ids"] == [source_key]
        assert assertion["assertion_type"] == "source_reported"
        assert assertion["verification_status"] == "not_independently_verified"
        assert assertion["quality_flags"] == fact["source_json"]["quality_flags"]
        assert not {"degree", "years_of_experience", "completed"} & item["value"].keys()
        assert evidence_by_id[fact["fact_id"]] == {
            **fact["source_json"],
            "source_record_id": f"{source_key}:{fact['fact_id']}",
            "profile_source_record_id": fact["source_record_id"],
            "fact_id": fact["fact_id"],
        }
    assert profile["categories"]["professional_experience"]["items"] == []
    assert fact_rows == original


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("run_status", "running", "publication_invalid"),
        ("run_status", "failed", "publication_invalid"),
        ("publication_source_key", "other-source", "source_mismatch"),
        ("run_schema_version", "wrong-schema", "source_mismatch"),
        ("run_jurisdiction", "FL", "source_mismatch"),
        ("run_id", "other-run", "publication_invalid"),
        ("npi", 1000000012, "publication_invalid"),
        ("npi", None, "publication_invalid"),
        ("category", "professional_experience", "categories_invalid"),
        ("fact_type", "years_of_practice", "categories_invalid"),
    ],
)
def test_publication_and_fact_identity_are_fenced(source_key, field, value, error):
    fact = rows(source_key)[0] | {field: value}
    with pytest.raises(RuntimeError, match=error):
        projection(source_key, [fact])


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_key", "other-source"),
        ("source_kind", "cms_doctors"),
        ("agency", "Other agency"),
        ("jurisdiction", "FL"),
        ("coverage_scope", "state_census"),
        ("registry_generation", "e" * 64),
    ],
)
def test_manifest_cannot_relabel_source_or_expand_coverage(source_key, field, value):
    fact = rows(source_key)[0]
    fact["source_manifest"]["source"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        projection(source_key, [fact])


@pytest.mark.parametrize("snapshot", [None, "not-a-snapshot"])
def test_registry_generation_requires_a_retained_snapshot_digest(source_key, snapshot):
    fact = rows(source_key)[0]
    fact["source_manifest"]["snapshot_sha256"] = snapshot
    with pytest.raises(RuntimeError, match="source_mismatch"):
        projection(source_key, [fact])


@pytest.mark.parametrize("categories", [[], ["education"], ["education", "professional_experience"]])
def test_exact_source_category_scope_is_required(source_key, categories):
    fact = rows(source_key)[0]
    fact["source_manifest"]["categories"] = categories
    with pytest.raises(RuntimeError, match="categories_invalid"):
        projection(source_key, [fact])


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_key", "other-source"),
        ("source_record_id", "other-profile"),
        ("run_id", "other-run"),
        ("schema_version", "wrong-schema"),
        ("agency", "Other agency"),
        ("jurisdiction", "FL"),
    ],
)
def test_original_evidence_cannot_cross_source_record_or_generation(source_key, field, value):
    fact = rows(source_key)[0]
    fact["source_json"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        projection(source_key, [fact])


@pytest.mark.parametrize(
    "changes", [{"fact_id": None}, {"sensitive": True}, {"public_default": False}, {"availability": "restricted"}]
)
def test_missing_or_nonpublic_assertions_are_not_exposed(source_key, changes):
    assert projection(source_key, [rows(source_key)[0] | changes]) is None


def test_independent_source_generations_cannot_be_mixed(source_key):
    fact_rows = rows(source_key)
    fact_rows[1]["generation_id"] = "other-run"
    with pytest.raises(RuntimeError, match="publication_invalid"):
        projection(source_key, fact_rows)


async def test_loader_uses_only_independent_source_publication_pointers(monkeypatch):
    nypp_rows, nysed_rows = rows(NYPP), rows(NYSED)
    database = SimpleNamespace(scalar=AsyncMock(return_value=True), all=AsyncMock())
    monkeypatch.setattr(state_api, "db", database)
    for captured, expected in (([], []), (nypp_rows, [NYPP]), (nypp_rows + nysed_rows, [NYPP, NYSED])):
        database.all.return_value = [SimpleNamespace(_mapping=row) for row in captured]
        loaded = await state_api.fetch_additional_state_profile_projections(NPI)
        assert [state["source"]["source_key"] for state in loaded] == expected
    query = str(database.all.call_args.args[0])
    assert "run.run_id = publication.current_run_id" in query
    assert "run.source_key = publication.source_key" in query
    assert "fact.npi = :npi" in query
    assert database.all.call_args.kwargs == {"npi": NPI, "source_keys": list(state_api.STATE_SOURCE_KEYS)}
    database.all.return_value = [SimpleNamespace(_mapping=massachusetts_row())]
    (loaded,) = await state_api.fetch_additional_state_profile_projections(NPI)
    assert loaded == state_api._state_projection(NPI, [massachusetts_row()])


def test_sources_corroborate_school_and_keep_incumbent_and_fhir_evidence():
    incumbent = cms_api.merge_cms_education_projection(NPI, _legacy_envelope(), _cms_projection())
    original = copy.deepcopy(incumbent)
    combined = envelope(incumbent=incumbent)
    fhir = _structured_fhir_profile_by_key()
    original_fhir = copy.deepcopy(fhir)
    profile = profile_api.compose_provider_profile(NPI, state_projection=combined, fhir_profile=fhir)
    (education_item,) = profile["categories"]["education"]["items"]
    assert len(education_item["assertions"]) == education_item["assertion_count"] == 4
    assert education_item["corroborated_fields"] == ["institution", "graduation_year"]
    assert education_item["value"]["graduation_year"] == 2001
    ny_values_by_source = {
        assertion["source_ids"][0]: assertion["value"]
        for assertion in education_item["assertions"]
        if assertion.get("source_ids") in ([NYPP], [NYSED])
    }
    assert ny_values_by_source[NYPP]["graduation_date"] == "2001"
    assert ny_values_by_source[NYPP]["graduation_date_precision"] == "year"
    assert ny_values_by_source[NYSED]["graduation_date"] == "2001-06-01"
    assert ny_values_by_source[NYSED]["graduation_date_precision"] == "day"
    assert profile["categories"]["languages"]["items"] and profile["categories"]["services"]["items"]
    evidence = profile_api.compose_provider_profile_evidence(
        state_projection=combined, fhir_evidence={"facts": fhir["facts"]}, provider_profile=profile
    )
    assert set(evidence["sources"]) == {"state_regulator", "cms_doctors", NYPP, NYSED, "provider_directory_fhir"}
    fhir_facts_by_type = evidence["sources"]["provider_directory_fhir"]["facts"]
    assert set(fhir_facts_by_type) == set(fhir["facts"])
    for fact_type, group in fhir["facts"].items():
        assert fhir_facts_by_type[fact_type]["items"] == group["items"]
    assert combined["evidence"] == original["evidence"] and combined["cms_evidence"] == original["cms_evidence"]
    assert incumbent == original
    assert fhir == original_fhir


@pytest.mark.parametrize(
    "abbreviation,full_name", [("Ross", "Ross University School of Medicine"), ("Iowa", "University of Iowa")]
)
def test_ambiguous_institution_names_do_not_gain_global_aliases(abbreviation, full_name):
    profile = compose(envelope(rows(NYPP, school=abbreviation), rows(NYSED, school=full_name)))
    items = profile["categories"]["education"]["items"]
    assert len(items) == 2
    assert {item["value"]["institution"] for item in items} == {abbreviation, full_name}


def test_multiple_schools_conflicting_years_and_partial_dates_remain_distinct():
    school_rows = [
        {"schoolName": "Example Medical School", "gradDate": "2001"},
        {"schoolName": "Example Medical School", "gradDate": "2002"},
        {"schoolName": "Second Medical School", "gradDate": "2004"},
        {"schoolName": "Example Medical School", "gradDate": ""},
    ]
    profile = compose(envelope(rows(NYPP, schools=school_rows)))
    items = profile["categories"]["education"]["items"]
    assert len(items) == len({item["item_id"] for item in items}) == 4
    assert sorted(item["assertion_count"] for item in items) == [1, 1, 1, 2]
    assert any(item["value"] == {"institution": "Example Medical School"} for item in items)


async def test_full_and_paged_route_preserves_support_and_rejects_rotated_source(monkeypatch, source_key):
    school_rows = [
        {"schoolName": "Example Medical School", "gradDate": "2001"},
        {"schoolName": "Second Medical School", "gradDate": "2004"},
    ]
    nypp_rows = rows(NYPP, schools=school_rows)
    state = envelope(nypp_rows)
    fetched = AsyncMock(return_value=state)
    monkeypatch.setattr(npi_api, "fetch_provider_profile_projection", fetched)
    monkeypatch.setattr(npi_api, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    response = await npi_api.get_provider_profile(SimpleNamespace(args={"include_evidence": "1"}), str(NPI))
    assert response.status == 200
    full = json.loads(response.body)
    generation = full["provider_profile"]["generation_id"]
    seen_record_ids = set()
    for offset in range(2):
        args_by_name = {
            "category": "education",
            "limit": "1",
            "offset": str(offset),
            "generation_id": generation,
            "include_evidence": "1",
        }
        response = await npi_api.get_provider_profile(SimpleNamespace(args=args_by_name), str(NPI))
        assert response.status == 200
        page = json.loads(response.body)
        (education_item,) = page["provider_profile"]["categories"]["education"]["items"]
        evidence_ids = {
            evidence_record["source_record_id"]
            for source_evidence in page["provider_profile_evidence"]["sources"].values()
            for evidence_record in source_evidence["records"]
        }
        assert evidence_ids == set(education_item["source_record_ids"])
        assert not seen_record_ids & evidence_ids
        seen_record_ids.update(evidence_ids)
    expected_record_ids = {
        evidence_record["source_record_id"]
        for key, source_evidence in full["provider_profile_evidence"]["sources"].items()
        for evidence_record in source_evidence["records"]
        if evidence_record["fact_id"]
        in {fact["fact_id"] for fact in (nypp_rows if key == NYPP else rows(NYSED)) if fact["category"] == "education"}
    }
    assert seen_record_ids == expected_record_ids
    fetched.return_value = envelope(
        rows(NYPP, generation="e" * 64, schools=school_rows) if source_key == NYPP else nypp_rows,
        rows(NYSED, generation="f" * 64) if source_key == NYSED else rows(NYSED),
    )
    refreshed = compose(fetched.return_value)
    assert refreshed["generation_id"] != generation
    assert [education_item["item_id"] for education_item in refreshed["categories"]["education"]["items"]] == [
        education_item["item_id"] for education_item in full["provider_profile"]["categories"]["education"]["items"]
    ]
    response = await npi_api.get_provider_profile(SimpleNamespace(args=args_by_name), str(NPI))
    assert response.status == 409
    assert json.loads(response.body)["error"] == "provider_profile_generation_changed"
