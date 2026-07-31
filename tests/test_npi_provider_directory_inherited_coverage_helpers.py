# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import time
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


def test_fhir_meta_accepts_json_text():
    assert npi_module._provider_directory_fhir_meta(
        '{"versionId":"one"}'
    ) == {"version_id": "one"}


def test_fhir_provenance_skips_invalid_url():
    assert (
        npi_module._provider_directory_fhir_provenance(
            {"role_fhir_self_url": "https://example.test:invalid/path"},
            "role",
        )
        is None
    )


def test_healthcare_service_details_include_provenance(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_fhir_provenance",
        lambda *_args: {"fetch_mode": "bundle"},
    )

    details = npi_module._provider_directory_healthcare_service_details(
        [{"source_id": "source", "resource_id": "service"}]
    )

    assert details[0]["fhir_provenance"] == {"fetch_mode": "bundle"}


def test_affiliation_mapping_updates_single_plan_metadata(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_plan_metadata",
        lambda _mapping: {
            "returned": 0,
            "total": 2,
            "truncated": True,
            "catalog_complete": True,
        },
    )

    mapped = npi_module._map_provider_directory_affiliation_evidence(
        [
            {
                "source_id": "source",
                "affiliation_id": "affiliation",
                "evidence_type": "affiliation",
                "evidence_row_total": 1,
            }
        ]
    )

    assert mapped[("source", "affiliation")]["insurance_plan_metadata"][
        "returned"
    ] == 0


@pytest.mark.asyncio
async def test_classification_npi_cache_hit():
    cache = npi_module._CLASSIFICATION_NPI_CACHE
    cache.clear()
    cache["hospital"] = (time.time(), [123])

    assert await npi_module._get_classification_npi_list("Hospital") == [123]


@pytest.mark.parametrize(
    ("raw_value", "normalizer", "expected_message"),
    [
        (
            "infinity",
            lambda value: npi_module._normalize_match_candidate_float(
                value,
                param_name="radius",
                minimum=0,
                maximum=10,
            ),
            "radius must be between",
        ),
        (
            "3",
            lambda value: npi_module._normalize_match_candidate_entity_type(
                value,
                None,
            ),
            "entity_type_code must be either",
        ),
    ],
)
def test_match_candidate_normalizers_reject_out_of_range(
    raw_value,
    normalizer,
    expected_message,
):
    with pytest.raises(sanic.exceptions.InvalidUsage, match=expected_message):
        normalizer(raw_value)


def test_map_source_details_skips_blank_source():
    details_by_id = npi_module._map_source_details(
        [
            {
                "source_id": "",
                "endpoint_id": None,
                "canonical_api_base": None,
                "org_name": None,
                "plan_name": None,
            },
            {
                "source_id": "source",
                "endpoint_id": "endpoint",
                "canonical_api_base": "https://example.test/fhir",
                "org_name": "Example",
                "plan_name": "Example Plan",
            },
        ]
    )

    assert list(details_by_id) == ["source"]


def test_affiliation_keys_skip_nonmapping_addresses():
    assert npi_module._provider_directory_affiliation_keys_from_addresses(
        [object()]
    ) == []


@pytest.mark.asyncio
async def test_geo_record_attachment_stops_without_candidate_pairs(
    monkeypatch,
):
    monkeypatch.setattr(
        npi_module,
        "_replace_stale_geo_provider_directory_evidence",
        lambda _rows: None,
    )
    monkeypatch.setattr(
        npi_module,
        "_geo_candidate_address_pairs",
        lambda _rows: [],
    )
    execute = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    await npi_module._attach_geo_candidate_record_ids(
        [{}],
        {"lat": 1.0, "long": 2.0},
    )

    execute.assert_not_awaited()


def test_general_acute_care_boost_rejects_requested_individual():
    assert not npi_module._should_boost_general_acute_care_candidate(
        {"entity_type_code": 2},
        {"entity_kind": "individual"},
        [{"taxonomy_code": "282N00000X"}],
        {"has_hospital_enrollment": True},
    )
