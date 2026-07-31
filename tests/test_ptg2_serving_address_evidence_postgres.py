# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path
import re

from tests.ptg2_serving_address_evidence_postgres_geo import (
    test_knn_template_executes_precedence_and_empty_probe_shape,
    test_optimized_membership_rejects_npi_wide_cms_anchor,
)
from tests.ptg2_serving_address_evidence_postgres_lineage import (
    test_admitted_mrf_recovers_specific_lineage_without_fabrication,
    test_admitted_source_never_falls_back_to_generic_materialization,
    test_incomplete_specific_and_source_zero_are_not_public_lineage,
    test_live_nppes_and_cms_use_source_specific_versions_as_whole_rows,
    test_specific_evidence_excludes_retired_and_blank_rows,
    test_stored_compact_run_date_is_complete_without_timestamps,
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
