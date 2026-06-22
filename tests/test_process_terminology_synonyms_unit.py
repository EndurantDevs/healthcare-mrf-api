# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib

terminology_synonyms = importlib.import_module("process.terminology_synonyms")


def test_curated_provider_aliases_map_common_terms_to_pricing_provider_type():
    rows = terminology_synonyms._curated_rows()
    family_rows = [
        row
        for row in rows
        if row["domain"] == "provider_type" and row["term_key"] == "family medicine"
    ]

    assert family_rows
    assert family_rows[0]["target_system"] == "PROVIDER_TYPE"
    assert family_rows[0]["target_code"] == "Family Practice"
    assert "207Q00000X" in family_rows[0]["metadata_json"]


def test_curated_procedure_aliases_allow_broad_common_names():
    rows = terminology_synonyms._curated_rows()
    office_visit_codes = {
        row["target_code"]
        for row in rows
        if row["domain"] == "procedure" and row["term_key"] == "office visit"
    }

    assert {"99213", "99214", "99215"}.issubset(office_visit_codes)
