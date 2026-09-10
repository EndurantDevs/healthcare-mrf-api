# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep Tennessee public facts and corroborating assertions source-bound."""

import copy
import hashlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api import provider_profile_states as state_api
from process.tennessee_profile import _source_manifest
from process.tennessee_profile_rows import SCHEMA_VERSION, SOURCE_KEY, parse_report
from tests.test_provider_profile_cms import _cms_projection
from tests.test_provider_profile_kentucky import _row as _kentucky_row
from tests.test_provider_profile_massachusetts import _legacy_envelope
from tests.test_provider_profile_massachusetts import _row as _massachusetts_row
from tests.test_tennessee_profile_binding import _snapshot
from tests.test_tennessee_profile_rows import EVIDENCE, report_bytes, source_row

NPI = 1000000004
GENERATION = "tennessee-generation"


def _rows(*source_rows, generation=GENERATION):
    content = report_bytes(
        source_rows
        or [
            source_row(
                EducationProvider="Example Medical School",
                GraduationDate="06/01/2001",
            )
        ]
    )
    _, facts = parse_report(
        content,
        evidence={
            **EVIDENCE,
            "run_id": generation,
            "content_sha256": hashlib.sha256(content).hexdigest(),
        },
    )
    return [
        {
            **fact,
            "npi": NPI,
            "publication_source_key": SOURCE_KEY,
            "generation_id": generation,
            "source_published_at": datetime(2026, 9, 10),
            "run_status": "completed",
            "run_schema_version": SCHEMA_VERSION,
            "run_jurisdiction": "TN",
            "source_manifest": {
                "categories": ["education", "training", "specialties"],
                "snapshot_sha256": "a" * 64,
                "source": {
                    "source_key": SOURCE_KEY,
                    "source_kind": "state_regulator",
                    "agency": "Tennessee Department of Health",
                    "jurisdiction": "TN",
                    "coverage_scope": "regular_md_do_all_ranks_statuses_locations",
                    "registry_generation": "a" * 64,
                },
            },
        }
        for fact in facts
    ]


def _projection(*rows):
    return state_api._state_projection(NPI, list(rows or _rows()), source_key=SOURCE_KEY)


def _envelope(*rows, incumbent=None):
    return state_api.merge_state_profile_projection(NPI, incumbent, _projection(*rows))


def _compose(projection, **options):
    return profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None, **options)


def test_reported_portfolio_keeps_literal_values_uncertainty_and_per_fact_evidence():
    rows = _rows(source_row(GraduationDate="06/01/2030", OtherTrainingEndDate=""))
    projection = _projection(*rows)
    profile = _compose(_envelope(*rows))
    assert profile["sources"] == [rows[0]["source_manifest"]["source"]]
    assert profile["source_generations"] == {SOURCE_KEY: GENERATION}
    assert profile["important_context"] == [state_api.SOURCE_CONTEXT[SOURCE_KEY]]
    assert profile["categories"]["professional_experience"]["items"] == []
    assert profile["categories"]["certifications"]["items"] == []
    for row in rows:
        (item,) = profile["categories"][row["category"]]["items"]
        (assertion,) = item["assertions"]
        assert assertion["value"] == row["value_json"]
        assert assertion["source_ids"] == [SOURCE_KEY]
        assert assertion["assertion_type"] == "source_reported"
        assert assertion["verification_status"] == "not_independently_verified"
        assert assertion["quality_flags"] == row["source_json"]["quality_flags"]
        assert item["type"] == row["fact_type"]
    (education,) = profile["categories"]["education"]["items"]
    assert education["quality_flags"] == ["graduation_date_in_future", "graduation_year_in_future"]
    (training,) = profile["categories"]["training"]["items"]
    assert "attendance_end" not in training["value"]
    assert {record["source_record_id"] for record in projection["evidence"]["records"]} == {
        f"{SOURCE_KEY}:{row['fact_id']}" for row in rows
    }
    assert len({record["profile_source_record_id"] for record in projection["evidence"]["records"]}) == 1
    assert [record["raw_fields"] for record in projection["evidence"]["records"]] == [
        row["source_json"]["raw_fields"] for row in rows
    ]


def test_tennessee_cms_and_florida_corroborate_school_and_preserve_distinct_education():
    incumbent = cms_api.merge_cms_education_projection(NPI, _legacy_envelope(), _cms_projection())
    original = copy.deepcopy(incumbent)
    fact_rows = _rows(
        source_row(EducationProvider="EXAMPLE Medical School", GraduationDate="06/01/2001"),
        source_row(EducationProvider="Second Medical School", GraduationDate="06/01/2006"),
    )
    projection = _envelope(*fact_rows, incumbent=incumbent)
    profile = _compose(projection)
    education_items = profile["categories"]["education"]["items"]
    assert len(education_items) == 2
    corroborated_items = [
        education_item for education_item in education_items if education_item["assertion_count"] == 3
    ]
    assert len(corroborated_items) == 1
    corroborated = corroborated_items[0]
    assert len(corroborated["assertions"]) == 3
    assert {profile_source["source_key"] for profile_source in profile["sources"]} == {
        SOURCE_KEY,
        "cms-doctors",
        "florida-mqa",
    }
    assert {assertion["assertion_type"] for assertion in corroborated["assertions"]} == {
        "source_reported",
        "cms_reported",
        "practitioner_reported",
    }
    assert corroborated["corroborated_fields"] == ["institution", "graduation_year"]
    tennessee_assertions = [
        assertion for assertion in corroborated["assertions"] if assertion.get("source_ids") == [SOURCE_KEY]
    ]
    assert len(tennessee_assertions) == 1 and tennessee_assertions[0]["value"] == fact_rows[0]["value_json"]
    separate_items = [education_item for education_item in education_items if education_item["assertion_count"] == 1]
    assert len(separate_items) == 1 and separate_items[0]["value"]["institution"] == "Second Medical School"
    assert incumbent == original
    assert projection["evidence"] == incumbent["evidence"] and projection["cms_evidence"] == incumbent["cms_evidence"]
    assert profile["source_generations"] == {**_compose(incumbent)["source_generations"], SOURCE_KEY: GENERATION}


def test_distinct_degrees_dates_and_training_remain_distinct_with_exact_specialty_corroboration():
    rows = _rows(
        source_row(),
        source_row(DegreeEarned="PHD"),
        source_row(GraduationDate="06/16/1998"),
        source_row(LicenseNumber="000124", OtherTrainingProvider="Second Hospital"),
    )
    original = copy.deepcopy(rows)
    profile = _compose(_envelope(*rows))
    education = profile["categories"]["education"]["items"]
    assert len(education) == 3 and sorted(item["assertion_count"] for item in education) == [1, 1, 2]
    assert len(profile["categories"]["training"]["items"]) == 2
    (specialty,) = profile["categories"]["specialties"]["items"]
    assert specialty["assertion_count"] == len(specialty["assertions"]) == 2
    assert rows == original


@pytest.mark.parametrize("category", ["education", "training", "specialties"])
def test_each_page_has_exact_supporting_fact_evidence(category):
    fact_rows = _rows(
        source_row(EducationProvider="Example Medical School", GraduationDate="06/01/2001"),
        source_row(
            EducationProvider="Second Medical School",
            GraduationDate="06/01/2006",
            OtherTrainingProvider="Second Hospital",
            ModifierDescription="Pediatrics",
        ),
    )
    incumbent = cms_api.merge_cms_education_projection(NPI, _legacy_envelope(), _cms_projection())
    projection = _envelope(*fact_rows, incumbent=incumbent)
    seen_fact_ids = set()
    for offset in range(2):
        profile = _compose(
            projection, requested_categories=[category], page_category=category, page_limit=1, page_offset=offset
        )
        (profile_item,) = profile["categories"][category]["items"]
        evidence = profile_api.compose_provider_profile_evidence(
            state_projection=projection,
            fhir_evidence=None,
            provider_profile=profile,
            page_category=category,
        )
        assert {
            evidence_record["source_record_id"]
            for evidence_source in evidence["sources"].values()
            for evidence_record in evidence_source["records"]
        } == set(profile_item["source_record_ids"])
        (tennessee_record,) = evidence["sources"][SOURCE_KEY]["records"]
        assert tennessee_record["fact_id"] not in seen_fact_ids
        seen_fact_ids.add(tennessee_record["fact_id"])
    assert seen_fact_ids == {fact_row["fact_id"] for fact_row in fact_rows if fact_row["category"] == category}


def test_tennessee_rotation_changes_generation_without_changing_logical_item_identity():
    before = _compose(_envelope(*_rows()))
    after = _compose(_envelope(*_rows(generation="tennessee-next")))
    assert before["generation_id"] != after["generation_id"]
    for category in ("education", "training", "specialties"):
        assert (
            before["categories"][category]["items"][0]["item_id"]
            == after["categories"][category]["items"][0]["item_id"]
        )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("run_status", "running", "publication_invalid"),
        ("run_status", "failed", "publication_invalid"),
        ("run_schema_version", "ma-borim-profile/v1", "source_mismatch"),
        ("run_jurisdiction", "MA", "source_mismatch"),
        ("publication_source_key", "kentucky-kbml", "source_mismatch"),
        ("run_id", "other-generation", "publication_invalid"),
        ("npi", 1000000012, "publication_invalid"),
        ("category", "services", "categories_invalid"),
        ("fact_type", "postgraduate_training", "categories_invalid"),
    ],
)
def test_wrong_publication_provider_generation_and_fact_type_are_rejected(field, value, reason):
    row = _rows()[0]
    row[field] = value
    with pytest.raises(RuntimeError, match=reason):
        _projection(row)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_key", "massachusetts-borim"),
        ("source_kind", "cms_doctors"),
        ("jurisdiction", "MA"),
        ("agency", "Other board"),
    ],
)
def test_manifest_cannot_relabel_tennessee(field, value):
    row = _rows()[0]
    row["source_manifest"]["source"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


@pytest.mark.parametrize("field", ["coverage_scope", "registry_generation", "snapshot_sha256"])
@pytest.mark.parametrize("value", [None, "", 123, {}, "b" * 64])
def test_report_scope_and_snapshot_binding_reject_invalid_values(field, value):
    row = _rows()[0]
    manifest = row["source_manifest"]
    target = manifest if field == "snapshot_sha256" else manifest["source"]
    target[field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


@pytest.mark.parametrize("field", ["coverage_scope", "registry_generation", "snapshot_sha256", "both_hashes"])
def test_report_scope_and_snapshot_binding_require_present_values(field):
    row = _rows()[0]
    manifest = row["source_manifest"]
    if field == "both_hashes":
        del manifest["snapshot_sha256"], manifest["source"]["registry_generation"]
    else:
        target = manifest if field == "snapshot_sha256" else manifest["source"]
        del target[field]
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


def test_reader_accepts_actual_producer_scope_and_snapshot_descriptor():
    row = _rows()[0]
    row["source_manifest"] = manifest = _source_manifest({"run_id": "synthetic-control"}, _snapshot(), None)
    projection = _projection(row)
    assert projection["source"]["coverage_scope"] == "regular_md_do_all_ranks_statuses_locations"
    assert projection["source"]["registry_generation"] == manifest["snapshot_sha256"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_key", "massachusetts-borim"),
        ("source_record_id", "other-profile"),
        ("run_id", "other-generation"),
        ("schema_version", "wrong-schema"),
        ("agency", "Other board"),
        ("jurisdiction", "MA"),
    ],
)
def test_evidence_cannot_cross_source_record_or_generation(field, value):
    row = _rows()[0]
    row["source_json"][field] = value
    with pytest.raises(RuntimeError, match="source_mismatch"):
        _projection(row)


@pytest.mark.parametrize(
    "categories",
    [
        [],
        ["education"],
        ["education", "training"],
        ["education", "training", "certifications"],
        ["education", "training", "specialties", "services"],
        ["education", "training", "specialties", "specialties"],
    ],
)
def test_publication_requires_exact_tennessee_category_scope(categories):
    row = _rows()[0]
    row["source_manifest"]["categories"] = categories
    with pytest.raises(RuntimeError, match="categories_invalid"):
        _projection(row)


@pytest.mark.parametrize(
    "changes",
    [
        {"fact_id": None},
        {"sensitive": True},
        {"public_default": False},
        {"availability": "restricted"},
    ],
)
def test_absent_or_nonpublic_facts_are_not_projected(changes):
    row = _rows()[0]
    row.update(changes)
    assert _projection(row) is None


def test_mixed_generations_are_rejected():
    with pytest.raises(RuntimeError, match="publication_invalid"):
        _projection(*_rows(), *_rows(generation="other-generation"))


@pytest.mark.parametrize("include_tennessee", [False, True])
async def test_loader_discovers_present_sources_in_one_query(monkeypatch, include_tennessee):
    rows = [_massachusetts_row(), _kentucky_row()]
    if include_tennessee:
        rows.extend(_rows())
    database = SimpleNamespace(
        scalar=AsyncMock(return_value=True),
        all=AsyncMock(
            return_value=[SimpleNamespace(_mapping=row) for row in rows],
        ),
    )
    monkeypatch.setattr(state_api, "db", database)
    projections = await state_api.fetch_additional_state_profile_projections(NPI)
    expected_sources = [state_api.MASSACHUSETTS_SOURCE_KEY, state_api.KENTUCKY_SOURCE_KEY]
    if include_tennessee:
        expected_sources.append(SOURCE_KEY)
    assert [projection["source"]["source_key"] for projection in projections] == expected_sources
    database.all.assert_awaited_once()
    assert database.all.call_args.kwargs == {"npi": NPI, "source_keys": list(state_api.STATE_SOURCE_KEYS)}
