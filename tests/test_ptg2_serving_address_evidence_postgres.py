# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path
import re

from tests.ptg2_serving_address_evidence_postgres_geo import (
    test_knn_template_executes_precedence_and_empty_probe_shape,
    test_optimized_membership_rejects_npi_wide_cms_anchor,
)
from tests.ptg2_serving_address_evidence_postgres_knn import (
    test_knn_assurance_precedes_per_npi_distance_rank,
    test_knn_broad_underfilled_radius_reaches_exact_zip,
    test_knn_exact_zip_runs_after_out_of_radius_source_exhaustion,
    test_knn_radius_fence_preserves_spheroid_boundary,
    test_knn_withholds_exact_zip_while_radius_source_is_capped,
)
from tests.ptg2_serving_address_evidence_postgres_coverage import (
    test_provider_set_geo_candidate_scope_executes_beyond_old_prefix,
)
from tests.ptg2_serving_address_evidence_postgres_enrichment import (
    test_provider_enrichment_selects_one_truthful_address_row,
)
from tests.ptg2_serving_address_evidence_postgres_allowed import (
    test_allowed_page_preserves_rates_and_rejects_incoherent_locations,
    test_allowed_state_city_page_rejects_postal_boxes,
)
from tests.ptg2_serving_address_evidence_postgres_capability import (
    test_geo_capability_probe_requires_usable_zcta_zip_index,
)
from tests.ptg2_serving_address_evidence_postgres_spatial import (
    test_exact_zip_accepts_coherent_points_and_rejects_incoherent_points,
    test_radius_membership_rejects_incoherent_and_out_of_radius_points,
    test_state_city_membership_rejects_evidenced_postal_boxes,
)
from tests.ptg2_serving_address_evidence_postgres_lineage import (
    test_admitted_mrf_recovers_specific_lineage_without_fabrication,
    test_admitted_source_never_falls_back_to_generic_materialization,
    test_incomplete_specific_and_source_zero_are_not_public_lineage,
    test_live_fallback_rejects_mismatched_stored_identity_without_live_row,
    test_live_nppes_and_cms_use_source_specific_versions_as_whole_rows,
    test_specific_evidence_excludes_retired_and_blank_rows,
    test_stored_compact_run_date_is_complete_without_timestamps,
    test_stored_only_lineage_does_not_consult_live_mrf_fallback,
)
from tests.ptg2_serving_address_evidence_postgres_ranking import (
    test_optimized_membership_requires_distinct_normalized_mrf_issuers,
    test_optimized_membership_uses_location_key_as_final_tie_breaker,
)


def _is_valid_npi_check_digit(npi_text: str) -> bool:
    npi_digits = f"80840{npi_text}"
    checksum_total = 0
    for digit_index, digit_text in enumerate(npi_digits):
        digit = int(digit_text)
        if digit_index % 2 == len(npi_digits) % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum_total += digit
    return checksum_total % 10 == 0


def test_fixture_npis_are_deliberately_checksum_invalid():
    fixture_source_paths = (
        Path(__file__),
        *Path(__file__).parent.glob(
            "ptg2_serving_address_evidence_postgres_*.py"
        ),
    )
    fixture_npis = {
        npi_text
        for fixture_source_path in fixture_source_paths
        for npi_text in re.findall(
            r"\b[1-9][0-9]{9}\b",
            fixture_source_path.read_text(encoding="utf-8"),
        )
    }

    assert fixture_npis
    assert all(not _is_valid_npi_check_digit(npi_text) for npi_text in fixture_npis)
