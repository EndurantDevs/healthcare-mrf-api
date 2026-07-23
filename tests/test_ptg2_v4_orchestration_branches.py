from __future__ import annotations

import pytest

pytest.register_assert_rewrite(
    "tests.ptg2_v4_orchestration_lookup_cases",
    "tests.ptg2_v4_orchestration_reuse_cases",
)

from tests.ptg2_v4_orchestration_reuse_cases import (
    test_reused_v4_serving_index_accepts_only_complete_packed_contract,
    test_reused_v4_serving_index_rejects_invalid_counts_and_evidence,
    test_failed_v4_layout_is_left_for_generation_aware_gc,
    test_failed_v3_layout_abandonment_retries_without_masking_failure,
    test_scanner_identity_binds_v4_compiler_and_policy,
)
from tests.ptg2_v4_orchestration_lookup_cases import (
    test_v4_sets_by_npi_layouts,
    test_v4_sets_by_npi_unscoped_pattern_projection,
    test_v4_rate_scope_and_explicit_npi_orchestration,
    test_v4_provider_set_dictionary_and_selected_npi_cache,
)
