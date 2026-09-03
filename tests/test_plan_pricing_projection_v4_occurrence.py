# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

from api import plan_pricing_projection_v3_code as code_stage


def test_v4_rate_occurrence_preserves_final_group_fields_and_multiplicity() -> None:
    base_occurrence_by_field = {
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "27447",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_procedure_name": "Synthetic knee procedure",
        "source_procedure_description": None,
        "network_names": [" Network B ", "Network A", "Network A", ""],
        "_ptg_provider_set_key": 7,
        "provider_set_global_id_128": "1" * 32,
        "price_key": 9,
        "price_set_global_id_128": "2" * 32,
        "serving_content_hash_128": "3" * 32,
        "source_key": 11,
        "provider_count": 2,
    }
    occurrence_rows = list(
        code_stage._rate_occurrence_rows(
            SimpleNamespace(_ptg2_manifest_id=lambda manifest_id: manifest_id),
            0,
            [
                base_occurrence_by_field,
                dict(base_occurrence_by_field),
                {
                    **base_occurrence_by_field,
                    "source_procedure_name": None,
                },
            ],
            {"2" * 32},
        )
    )

    assert len(occurrence_rows) == 2
    assert sorted(occurrence_by_field["occurrence_multiplicity"] for occurrence_by_field in occurrence_rows) == [1, 2]
    assert all(occurrence_by_field["provider_set_ref"] == "1" * 32 for occurrence_by_field in occurrence_rows)
    assert all(occurrence_by_field["price_set_ref"] == "2" * 32 for occurrence_by_field in occurrence_rows)
    assert all(occurrence_by_field["rate_pack_ref"] == "3" * 32 for occurrence_by_field in occurrence_rows)
    assert '"network_names":["Network A","Network B"]' in occurrence_rows[0]["group_fragment"]
