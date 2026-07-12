# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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
    assert "phone_candidates AS MATERIALIZED" in sql
    assert "provider_directory_address_overlay AS overlay" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.import_run_id = overlay.last_seen_run_id" in sql
    assert "FROM phone_candidates AS phone_match" in sql
    assert "CROSS JOIN LATERAL" in sql
    assert "candidate_address.address_key = phone_match.address_key" in sql
    assert "OFFSET 0" in sql
    assert "(true)::boolean AS phone_matched" in sql
