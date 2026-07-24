from __future__ import annotations

import pytest

pytest.register_assert_rewrite(
    "tests.ptg2_v4_coverage_graph_audit_cases",
    "tests.ptg2_v4_coverage_graph_compiler_cases",
    "tests.ptg2_v4_coverage_prefix_diagnostics",
    "tests.ptg2_v4_coverage_summary_mutations",
    "tests.ptg2_v4_coverage_publication_cases",
    "tests.ptg2_v4_coverage_snapshot_map_cases",
    "tests.ptg2_v4_coverage_snapshot_seal_cases",
    "tests.ptg2_v4_coverage_validation_matrix_cases",
)

from tests.ptg2_v4_coverage_snapshot_map_cases import (
    test_snapshot_map_manifest_and_codec_fail_closed,
    test_snapshot_persisted_pack_and_metadata_validators,
)
from tests.ptg2_v4_coverage_publication_cases import (
    test_snapshot_publication_helpers_batch_and_verify,
    test_snapshot_reservation_root_and_binding_lifecycle,
)
from tests.ptg2_v4_coverage_snapshot_seal_cases import (
    test_snapshot_summary_publication_and_seal_paths,
)
from tests.ptg2_v4_coverage_graph_audit_cases import (
    test_graph_root_manifest_map_and_physical_cas_reads,
    test_audit_reader_scalar_edges_and_provider_witnesses,
)
from tests.ptg2_v4_coverage_publication_cases import (
    test_audit_publish_orchestration,
)
from tests.ptg2_v4_coverage_graph_compiler_cases import (
    test_compiler_and_intersection_validation_edges,
)
from tests.ptg2_v4_coverage_validation_matrix_cases import (
    test_snapshot_additional_fail_closed_branch_matrix,
    test_snapshot_database_validation_branch_matrix,
)
from tests.ptg2_v4_coverage_graph_audit_cases import (
    test_audit_manifest_heavy_owner_and_payload_branch_matrix,
)
from tests.ptg2_v4_coverage_graph_compiler_cases import (
    test_compiler_progress_and_file_validation_branch_matrix,
)
from tests.ptg2_v4_coverage_prefix_diagnostics import (
    test_compiler_prefix_owner_diagnostics_fail_closed,
)
from tests.ptg2_v4_coverage_summary_mutations import (
    test_compiler_progress_rejects_post_terminal_and_backward_events,
    test_compiler_summary_authentication_branch_matrix,
)
