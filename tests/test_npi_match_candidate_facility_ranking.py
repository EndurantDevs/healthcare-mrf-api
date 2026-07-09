# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api.endpoint import npi as npi_module


def test_match_candidate_query_returns_overfetch_window_for_scoring():
    query, query_params = npi_module._match_candidate_query(
        {
            "address_key": "d8c8e7f0-d765-4786-9349-3663085a23b3",
            "entity_kind": "organization",
            "limit": 5,
        },
        "mrf.entity_address_unified",
    )

    assert query_params["candidate_limit"] == 100
    assert str(query).rstrip().endswith("LIMIT :candidate_limit")


def test_match_candidate_output_boosts_untyped_general_acute_care_org():
    candidate = npi_module._match_candidate_output(
        {
            "npi": 1730166224,
            "entity_type_code": 2,
            "address_key": "d8c8e7f0-d765-4786-9349-3663085a23b3",
            "address_key_matched": True,
            "taxonomy_list": [
                {"taxonomy_code": "282N00000X", "primary": True, "display_name": "Hospital"}
            ],
        },
        {"entity_kind": "organization", "taxonomy_exact": (), "taxonomy_prefixes": ()},
        {"has_hospital_enrollment": True, "has_ffs_enrollment": True},
    )

    assert candidate["match_score"] == 0.61
    assert candidate["match_signals"]["facility"]["canonical_hospital"] is True
