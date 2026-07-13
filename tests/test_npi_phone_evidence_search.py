# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


def _provider_directory_phone_match_query():
    """Build one representative Provider Directory phone-match query."""
    query_inputs_dict = {
        "address_site_key": None,
        "address_key": None,
        "lat": None,
        "long": None,
        "radius_miles": None,
        "phone_digits": "4192517960",
        "entity_type_code": None,
        "taxonomy_exact": (),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
        "limit": 5,
    }
    return npi_module._match_candidate_query(
        query_inputs_dict,
        "mrf.entity_address_unified",
    )


def test_match_candidate_phone_lookup_includes_current_provider_directory_evidence():
    """Phone search must bound unique providers without dropping service rows."""
    query, query_params = _provider_directory_phone_match_query()
    sql = str(query)

    assert query_params["phone_digits"] == "4192517960"
    assert "current_provider_directory_runs AS MATERIALIZED" in sql
    assert "matching_provider_directory_phone_rows AS MATERIALIZED" in sql
    assert "phone_candidate_rows AS MATERIALIZED" in sql
    assert "phone_candidates_unranked AS MATERIALIZED" in sql
    assert "phone_candidate_best_addresses AS MATERIALIZED" in sql
    assert "phone_candidates AS MATERIALIZED" in sql
    assert "phone_provider_directory_evidence AS MATERIALIZED" in sql
    assert "provider_directory_address_overlay AS overlay" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "current_run.run_id = overlay.last_seen_run_id" in sql
    assert "false AS provider_directory_matched" in sql
    assert "true AS provider_directory_matched" in sql
    assert "overlay.source_id::varchar" in sql
    assert "overlay.source_record_id::varchar" in sql
    assert "MAX(candidate.source_count) AS source_count" in sql
    assert "SELECT DISTINCT ON (candidate.provider_npi)" in sql
    assert "FROM phone_candidate_best_addresses AS candidate" in sql
    assert "candidate.provider_directory_matched DESC" in sql
    assert "candidate.source_count DESC NULLS LAST" in sql
    assert "LIMIT :candidate_limit" in sql
    assert "MIN(candidate.source_record_id) AS source_record_id" in sql
    assert "GROUP BY candidate.provider_npi, candidate.source_id" in sql
    assert "ARRAY_AGG(evidence.source_record_id ORDER BY evidence.source_id)" in sql
    assert "JOIN phone_candidates AS selected_candidate" in sql
    assert "selected_candidate.address_key = candidate.address_key" not in sql
    assert "FROM phone_candidates AS phone_match" in sql
    assert "LEFT JOIN phone_provider_directory_evidence AS phone_evidence" in sql
    assert "CROSS JOIN LATERAL" in sql
    assert "candidate_address.address_key = phone_match.address_key" in sql
    assert "candidate_address.type IN ('primary', 'secondary', 'practice', 'site')" in sql
    assert "candidate_address.source_count DESC NULLS LAST" in sql
    assert "LIMIT 1 OFFSET 0" in sql
    assert "OFFSET 0" in sql
    assert "(true)::boolean AS phone_matched" in sql
    assert "phone_provider_directory_matched DESC" in sql


@pytest.mark.asyncio
async def test_phone_match_attaches_every_current_directory_source(monkeypatch):
    cigna_source = "pdfhir_cigna"
    contra_source = "pdfhir_contra"
    candidate_dict = {
        "source_record_ids": [
            f"provider_directory_fhir:organization_address:{cigna_source}:org-1:1"
        ],
        "phone_source_record_ids": [
            f"provider_directory_fhir:organization_address:{cigna_source}:org-1:1",
            (
                "provider_directory_fhir:organization_affiliation:"
                f"{contra_source}:affiliation-1:org-2"
            ),
        ],
    }
    source_details_by_id = {
        cigna_source: {
            "source_id": cigna_source,
            "endpoint_id": "endpoint-cigna",
            "canonical_api_base": "https://cigna.example/fhir",
        },
        contra_source: {
            "source_id": contra_source,
            "endpoint_id": "endpoint-contra",
            "canonical_api_base": "https://contra.example/fhir",
        },
    }
    fetch_details = AsyncMock(return_value=source_details_by_id)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        fetch_details,
    )

    await npi_module._attach_provider_directory_source_details([candidate_dict])

    fetch_details.assert_awaited_once_with(
        [cigna_source, contra_source],
        session=None,
    )
    attached_source_ids = {
        source_id
        for endpoint in candidate_dict["provider_directory_sources"]
        for source_id in endpoint["source_ids"]
    }
    assert attached_source_ids == {cigna_source, contra_source}


@pytest.mark.asyncio
async def test_match_candidate_source_details_stay_compact(monkeypatch):
    """Candidate search must defer the typed role graph to provider detail."""
    attach_source_details = AsyncMock()
    monkeypatch.setattr(
        npi_module,
        "_attach_provider_directory_source_details",
        attach_source_details,
    )
    candidate_rows = [{"npi": 1234567890}]

    await npi_module._attach_match_candidate_source_details(
        candidate_rows,
        {"include_evidence": True},
        session="request-session",
    )

    attach_source_details.assert_awaited_once_with(
        candidate_rows,
        include_role_evidence=False,
        session="request-session",
    )
