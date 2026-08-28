# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request, identity, taxonomy, and frozen-cell card contracts."""

from __future__ import annotations

import json

import pytest

from api import ptg2_serving as serving
from api.plan_pricing_projection import PlanPricingProjectionUnsupported
from tests.ptg2_factorized_card_support import (
    card_pagination,
    provider_cell_row,
)


def test_factorized_cards_accept_zip_or_coordinates_with_bounded_window():
    """Explicit cards retain both supported radius origins and a 200-row cap."""

    assert serving._uses_factorized_release_cards(
        {"view": "card", "zip5": "60601"},
        card_pagination(limit=175, offset=25),
    )
    assert serving._uses_factorized_release_cards(
        {
            "view": "card",
            "lat": 41.88,
            "long": -87.63,
            "radius_miles": 25,
        },
        card_pagination(),
    )
    assert not serving._uses_factorized_release_cards(
        {"view": "full", "zip5": "60601"},
        card_pagination(),
    )
    assert not serving._uses_factorized_release_cards(
        {"view": "card", "zip5": "60601", "include_providers": "false"},
        card_pagination(),
    )
    with pytest.raises(PlanPricingProjectionUnsupported, match="at most 200"):
        serving._uses_factorized_release_cards(
            {"view": "card", "zip5": "60601"},
            card_pagination(limit=176, offset=25),
        )


@pytest.mark.parametrize(
    "request_by_field, message",
    [
        ({"view": "card"}, "ZIP5 or coordinates"),
        ({"view": "card", "lat": 41.88}, "both latitude and longitude"),
        (
            {"view": "card", "zip5": "60601", "order": "desc"},
            "ascending cost",
        ),
    ],
)
def test_factorized_card_request_rejects_unsupported_boundaries(
    request_by_field,
    message,
):
    """Incomplete location and descending cost requests fail explicitly."""

    with pytest.raises(PlanPricingProjectionUnsupported, match=message):
        serving._uses_factorized_release_cards(
            request_by_field,
            card_pagination(),
        )


def test_numeric_cpt_hcpcs_share_identity_but_g_code_remains_hcpcs():
    """Online profile lookup uses the build's equivalent-code identity."""

    cpt_identity = serving._factorized_card_code_identity(
        {"code_system": "CPT", "code": "27447"}
    )
    hcpcs_identity = serving._factorized_card_code_identity(
        {"code_system": "HCPCS", "code": "27447"}
    )
    assert cpt_identity == hcpcs_identity == ("CPT", "27447")
    assert serving._factorized_card_code_identity(
        {"code_system": "HCPCS", "code": "G0439"}
    ) == ("HCPCS", "G0439")


def test_frozen_taxonomy_matches_build_normalization():
    """Frozen taxonomy admission trims and uppercases stored codes."""

    taxonomy_sql, parameters_by_name = (
        serving._factorized_card_frozen_taxonomy(
            {"code_system": "CPT", "code": "27447"}
        )
    )
    assert "UPPER(BTRIM(stored_taxonomy.taxonomy_code))" in taxonomy_sql
    assert "provider.entity_type_code = 1" in taxonomy_sql
    assert "207X00000X" in parameters_by_name["taxonomy_codes"]
    assert parameters_by_name["taxonomy_codes"] == sorted(
        {
            taxonomy_code.strip().upper()
            for taxonomy_code in parameters_by_name["taxonomy_codes"]
        }
    )

    provider_row_by_field = provider_cell_row()
    provider_row_by_field["taxonomy_codes"] = [" 207x00000x "]
    provider_cell = serving._factorized_card_provider_fragment(
        provider_row_by_field
    )
    assert provider_cell.npi == 101


def test_provider_fragment_must_match_frozen_columns():
    """A compact fragment cannot disagree with its immutable row keys."""

    provider_row_by_field = provider_cell_row()
    provider_row_by_field["fragment"] = memoryview(
        provider_row_by_field["fragment"]
    )
    assert serving._factorized_card_provider_fragment(
        provider_row_by_field
    ).geo_cell == "60601"

    mismatched_row_by_field = provider_cell_row()
    mismatched_row_by_field["geo_cell"] = "60602"
    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="disagrees with its key",
    ):
        serving._factorized_card_provider_fragment(mismatched_row_by_field)

    malformed_row_by_field = provider_cell_row()
    malformed_row_by_field["fragment"] = json.dumps([]).encode()
    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="fragment is incomplete",
    ):
        serving._factorized_card_provider_fragment(malformed_row_by_field)
