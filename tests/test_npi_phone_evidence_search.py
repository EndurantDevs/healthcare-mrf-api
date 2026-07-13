# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


def test_match_candidate_phone_lookup_includes_current_provider_directory_evidence():
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

    query, query_params = npi_module._match_candidate_query(
        query_inputs_dict,
        "mrf.entity_address_unified",
    )
    sql = str(query)

    assert query_params["phone_digits"] == "4192517960"
    assert "phone_candidate_rows AS MATERIALIZED" in sql
    assert "phone_candidates AS MATERIALIZED" in sql
    assert "phone_provider_directory_evidence AS MATERIALIZED" in sql
    assert "provider_directory_address_overlay AS overlay" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) = overlay.last_seen_run_id" in sql
    assert "false AS provider_directory_matched" in sql
    assert "true AS provider_directory_matched" in sql
    assert "overlay.source_id::varchar" in sql
    assert "overlay.source_record_id::varchar" in sql
    assert "MIN(candidate.source_record_id) AS source_record_id" in sql
    assert "GROUP BY candidate.provider_npi, candidate.source_id" in sql
    assert "ARRAY_AGG(evidence.source_record_id ORDER BY evidence.source_id)" in sql
    assert "FROM phone_candidates AS phone_match" in sql
    assert "LEFT JOIN phone_provider_directory_evidence AS phone_evidence" in sql
    assert "CROSS JOIN LATERAL" in sql
    assert "candidate_address.address_key = phone_match.address_key" in sql
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
