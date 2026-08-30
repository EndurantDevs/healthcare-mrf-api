# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Target and receipt contract for the projection-v3 work census."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Mapping

from api.plan_pricing_projection_v3_aggregate import (
    MAX_CODE_AGGREGATE_WORK_ROWS,
    MAX_PROJECTION_AGGREGATE_WORK_ROWS,
)
from api.plan_pricing_projection_v3_code import (
    MAX_CODE_OCCURRENCES,
    MAX_CODE_RATE_PROFILE_WORK_ROWS,
    MAX_PROJECTION_RATE_PROFILE_WORK_ROWS,
    MAX_RATE_PROFILE_RATES,
)
from api.plan_pricing_projection_v3_price import (
    MAX_CODE_STAGED_PRICE_ATOMS,
    MAX_PRICE_HYDRATION_ATOMS,
)
from api.plan_pricing_projection_v3_provider import (
    MAX_PROJECTION_PROVIDER_MEMBERSHIPS,
    MAX_PROJECTION_PROVIDER_SETS,
    MAX_PROVIDER_NPIS_PER_SET,
)
from api.plan_pricing_projection_v3_provider_cells import (
    MAX_PROJECTION_PROVIDER_CELLS,
    MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES,
)
from api.plan_pricing_projection_v3_work import MAX_BIGINT
from api.ptg2_db_sidecars import (
    MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES,
    MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS,
    MAX_PRICE_MEMBERSHIP_CACHED_FRAGMENTS,
    PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK,
    PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT,
)
from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from scripts.research import plan_pricing_projection_v3_census_identity as identity
from scripts.research.plan_pricing_projection_v3_census_support import (
    PROJECTION_RELATIONS,
    ReleaseInput,
)
from scripts.research.plan_pricing_projection_v3_census_transaction import (
    CENSUS_DATABASE_STAGE_KEYS,
    census_database_application_name,
    census_database_run_token,
    expected_census_database_settings,
)

RESOURCE_PROOF_LIMITATIONS = (
    "postgres_temp_file_bytes_unmeasured",
    "postgres_wal_bytes_unmeasured",
    "full_build_memory_headroom_unmeasured",
    "provider_cell_batch_headroom_unmeasured",
    "provider_source_read_batch_headroom_unmeasured",
    "source_catalog_headroom_unmeasured",
    "postgres_backend_private_memory_unmeasured",
    "postgres_statement_peak_memory_unmeasured",
    "postgres_host_capacity_reservation_absent",
)
EXPECTED_FIXED_CAP_GATE_KEYS = frozenset(
    {
        "occurrence_code_within_cap",
        "price_atom_code_within_cap",
        "price_key_hydration_within_cap",
        "profile_code_within_cap",
        "profile_release_within_cap",
        "aggregate_code_within_cap",
        "aggregate_release_within_cap",
        "profile_rate_count_within_bigint",
        "profile_distinct_rate_count_within_cap",
        "aggregate_rate_count_within_bigint",
        "provider_set_count_within_cap",
        "provider_membership_count_within_cap",
        "maximum_provider_set_membership_count_within_cap",
        "provider_cell_count_within_cap",
        "provider_fragment_byte_count_within_cap",
        "price_membership_cached_block_count_within_cap",
        "price_membership_identity_retained_bytes_within_cap",
        "price_membership_metadata_fragment_count_within_cap",
        "price_membership_singleton_peak_bytes_within_cap",
    }
)
EXPECTED_OBSERVED_WORK_LIMIT_KEYS = frozenset(
    {
        "maximum_code_membership_probe_rows",
        "maximum_projection_membership_probe_rows",
        "maximum_code_member_cell_rows",
        "maximum_projection_member_cell_rows",
    }
)
EXPECTED_WORK_FIELD_KEYS = frozenset(
    {
        "normalized_occurrence_rows",
        "staged_price_atom_membership_rows",
        "maximum_price_key_atom_membership_rows",
        "membership_probe_rows",
        "member_cell_rows",
        "eligible_member_cell_rows",
        "set_cell_rows",
        "profile_join_rows",
        "aggregate_join_rows",
        "profile_rate_count_sum",
        "profile_rate_count_max",
        "profile_distinct_rate_count_max",
        "aggregate_rate_count_sum",
        "aggregate_rate_count_max",
    }
)
EXPECTED_STAGED_FIELD_KEYS = frozenset(
    {
        "provider_set_count",
        "provider_membership_count",
        "maximum_provider_set_membership_count",
        "provider_cell_count",
        "provider_fragment_byte_count",
        "provider_npi_count",
        "pending_npi_count",
        "referenced_empty_provider_set_count",
        "price_membership_cached_block_count",
        "price_membership_identity_retained_bytes",
        "price_membership_metadata_fragment_count",
        "price_membership_maximum_fragments_per_block",
        "price_membership_singleton_peak_bytes",
    }
)
_WORK_VALUE_KEYS = frozenset({"total", "maximum_per_code"})
_POSTFLIGHT_KEYS = frozenset(
    {
        "release_matches",
        "provider_signature_matches",
        "persistent_counts_match",
        "persistent_counts_after",
        "accepted",
    }
)


def _has_headroom(observed: int, cap: int) -> bool:
    """Require at least twenty-five percent retained capacity."""

    return (
        type(observed) is int
        and observed >= 0
        and type(cap) is int
        and cap > 0
        and observed * 5 <= cap * 4
    )


def _code_work_cap_gates(
    metrics_by_field: Mapping[str, Mapping[str, int]],
) -> dict[str, bool]:
    """Evaluate code and release work caps."""

    maximum = "maximum_per_code"
    return {
        "occurrence_code_within_cap": _has_headroom(
            metrics_by_field["normalized_occurrence_rows"][maximum],
            MAX_CODE_OCCURRENCES,
        ),
        "price_atom_code_within_cap": _has_headroom(
            metrics_by_field["staged_price_atom_membership_rows"][maximum],
            MAX_CODE_STAGED_PRICE_ATOMS,
        ),
        "price_key_hydration_within_cap": _has_headroom(
            metrics_by_field["maximum_price_key_atom_membership_rows"][maximum],
            MAX_PRICE_HYDRATION_ATOMS,
        ),
        "profile_code_within_cap": _has_headroom(
            metrics_by_field["profile_join_rows"][maximum],
            MAX_CODE_RATE_PROFILE_WORK_ROWS,
        ),
        "profile_release_within_cap": _has_headroom(
            metrics_by_field["profile_join_rows"]["total"],
            MAX_PROJECTION_RATE_PROFILE_WORK_ROWS,
        ),
        "aggregate_code_within_cap": _has_headroom(
            metrics_by_field["aggregate_join_rows"][maximum],
            MAX_CODE_AGGREGATE_WORK_ROWS,
        ),
        "aggregate_release_within_cap": _has_headroom(
            metrics_by_field["aggregate_join_rows"]["total"],
            MAX_PROJECTION_AGGREGATE_WORK_ROWS,
        ),
        "profile_rate_count_within_bigint": _has_headroom(
            metrics_by_field["profile_rate_count_max"][maximum],
            MAX_BIGINT,
        ),
        "profile_distinct_rate_count_within_cap": _has_headroom(
            metrics_by_field["profile_distinct_rate_count_max"][maximum],
            MAX_RATE_PROFILE_RATES,
        ),
        "aggregate_rate_count_within_bigint": _has_headroom(
            metrics_by_field["aggregate_rate_count_max"][maximum],
            MAX_BIGINT,
        ),
    }


def _staged_cap_gates(staged_by_field: Mapping[str, int]) -> dict[str, bool]:
    """Evaluate staged provider and retained metadata caps."""

    return {
        "provider_set_count_within_cap": _has_headroom(
            staged_by_field["provider_set_count"],
            MAX_PROJECTION_PROVIDER_SETS,
        ),
        "provider_membership_count_within_cap": _has_headroom(
            staged_by_field["provider_membership_count"],
            MAX_PROJECTION_PROVIDER_MEMBERSHIPS,
        ),
        "maximum_provider_set_membership_count_within_cap": _has_headroom(
            staged_by_field["maximum_provider_set_membership_count"],
            MAX_PROVIDER_NPIS_PER_SET,
        ),
        "provider_cell_count_within_cap": _has_headroom(
            staged_by_field["provider_cell_count"],
            MAX_PROJECTION_PROVIDER_CELLS,
        ),
        "provider_fragment_byte_count_within_cap": _has_headroom(
            staged_by_field["provider_fragment_byte_count"],
            MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES,
        ),
        "price_membership_cached_block_count_within_cap": _has_headroom(
            staged_by_field["price_membership_cached_block_count"],
            MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS,
        ),
        "price_membership_identity_retained_bytes_within_cap": _has_headroom(
            staged_by_field["price_membership_identity_retained_bytes"],
            MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES,
        ),
        "price_membership_metadata_fragment_count_within_cap": _has_headroom(
            staged_by_field["price_membership_metadata_fragment_count"],
            MAX_PRICE_MEMBERSHIP_CACHED_FRAGMENTS,
        ),
        "price_membership_singleton_peak_bytes_within_cap": _has_headroom(
            staged_by_field["price_membership_singleton_peak_bytes"],
            MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES,
        ),
    }


def fixed_cap_gates(
    metrics_by_field: Mapping[str, Mapping[str, int]],
    staged_by_field: Mapping[str, int],
) -> dict[str, bool]:
    """Evaluate every production cap covered by this receipt."""

    return {
        **_code_work_cap_gates(metrics_by_field),
        **_staged_cap_gates(staged_by_field),
    }


def expected_target(args: argparse.Namespace) -> dict[str, Any]:
    """Return the exact operator-declared release and serving shape."""

    return identity.validated_target(
        {
            "healthporta_plan_id": str(args.expected_healthporta_plan_id).strip(),
            "plan_release_id": str(args.plan_release_id).strip(),
            "serving_revision_id": str(args.expected_serving_revision_id).strip(),
            "binding_set_digest": str(args.expected_binding_set_digest).strip(),
            "binding_count": int(args.expected_binding_count),
            "in_network_binding_count": int(args.expected_in_network_binding_count),
            "distinct_snapshot_count": int(args.expected_snapshot_count),
            "distinct_plan_count": int(args.expected_plan_count),
        }
    )


def require_expected_target(
    args: argparse.Namespace,
    release_input: ReleaseInput,
) -> dict[str, Any]:
    """Fail unless the locked release matches the exact declared target."""

    binding_list = release_input.binding_manifest
    observed_target_by_field = {
        **{
            field_name: release_input.identity[field_name]
            for field_name in (
                "healthporta_plan_id",
                "plan_release_id",
                "serving_revision_id",
                "binding_set_digest",
            )
        },
        "binding_count": len(binding_list),
        "in_network_binding_count": sum(
            str(binding_by_field.get("role")) == "in_network"
            for binding_by_field in binding_list
        ),
        "distinct_snapshot_count": len(
            {
                str(binding_by_field.get("snapshot_id") or "")
                for binding_by_field in binding_list
            }
        ),
        "distinct_plan_count": len(
            {
                str(binding_by_field.get("plan_id") or "")
                for binding_by_field in binding_list
            }
        ),
    }
    if observed_target_by_field != expected_target(args):
        raise RuntimeError("pricing projection census target changed")
    return observed_target_by_field


def observed_work_limits(
    metrics_by_field: Mapping[str, Mapping[str, int]],
) -> dict[str, int]:
    """Project the four executable limits from raw observed row counts."""

    return {
        "maximum_code_membership_probe_rows": metrics_by_field["membership_probe_rows"][
            "maximum_per_code"
        ],
        "maximum_projection_membership_probe_rows": metrics_by_field[
            "membership_probe_rows"
        ]["total"],
        "maximum_code_member_cell_rows": metrics_by_field["member_cell_rows"][
            "maximum_per_code"
        ],
        "maximum_projection_member_cell_rows": metrics_by_field["member_cell_rows"][
            "total"
        ],
    }


def _is_measurement_schema_valid(measured_result: Mapping[str, Any]) -> bool:
    work_by_field = measured_result.get("work")
    staged_by_field = measured_result.get("staged")
    if (
        not isinstance(work_by_field, Mapping)
        or frozenset(work_by_field) != EXPECTED_WORK_FIELD_KEYS
        or not isinstance(staged_by_field, Mapping)
        or frozenset(staged_by_field) != EXPECTED_STAGED_FIELD_KEYS
    ):
        return False
    for metric_by_field in work_by_field.values():
        if (
            not isinstance(metric_by_field, Mapping)
            or frozenset(metric_by_field) != _WORK_VALUE_KEYS
            or any(
                type(metric_value) is not int or metric_value < 0
                for metric_value in metric_by_field.values()
            )
            or metric_by_field["maximum_per_code"] > metric_by_field["total"]
        ):
            return False
    if any(
        type(staged_value) is not int or staged_value < 0
        for staged_value in staged_by_field.values()
    ):
        return False
    retained_identity_bytes = (
        staged_by_field["price_membership_cached_block_count"]
        * PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK
    )
    maximum_fragments = staged_by_field["price_membership_maximum_fragments_per_block"]
    return (
        maximum_fragments <= staged_by_field["price_membership_metadata_fragment_count"]
        and staged_by_field["price_membership_identity_retained_bytes"]
        == retained_identity_bytes
        and staged_by_field["price_membership_singleton_peak_bytes"]
        == retained_identity_bytes
        + maximum_fragments * PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
    )


def is_cardinality_candidate_accepted(
    receipt_by_field: Mapping[str, Any],
    measured_result: Mapping[str, Any],
    source_matches: bool,
) -> bool:
    """Return whether one inner receipt passed every cardinality gate."""

    fixed_gates = measured_result.get("fixed_cap_gates")
    observed_limits = measured_result.get("observed_work_limits")
    work_by_field = measured_result.get("work")
    staged_by_field = measured_result.get("staged")
    postflight = receipt_by_field.get("postflight")
    persistent_counts_before = measured_result.get("persistent_counts_before")
    return (
        source_matches
        and diagnostics.is_database_receipt_valid(receipt_by_field)
        and receipt_by_field.get("rollback_complete") is True
        and receipt_by_field.get("temporary_relations_after_rollback") == []
        and isinstance(fixed_gates, Mapping)
        and frozenset(fixed_gates) == EXPECTED_FIXED_CAP_GATE_KEYS
        and all(gate_passed is True for gate_passed in fixed_gates.values())
        and _is_measurement_schema_valid(measured_result)
        and identity.is_measurement_identity_valid(receipt_by_field, measured_result)
        and fixed_gates == fixed_cap_gates(work_by_field, staged_by_field)
        and isinstance(observed_limits, Mapping)
        and frozenset(observed_limits) == EXPECTED_OBSERVED_WORK_LIMIT_KEYS
        and all(
            type(observed_limit) is int and observed_limit > 0
            for observed_limit in observed_limits.values()
        )
        and observed_limits == observed_work_limits(work_by_field)
        and isinstance(persistent_counts_before, Mapping)
        and frozenset(persistent_counts_before) == frozenset(PROJECTION_RELATIONS)
        and all(
            type(count) is int and count >= 0
            for count in persistent_counts_before.values()
        )
        and isinstance(postflight, Mapping)
        and frozenset(postflight) == _POSTFLIGHT_KEYS
        and postflight.get("release_matches") is True
        and postflight.get("provider_signature_matches") is True
        and postflight.get("persistent_counts_match") is True
        and postflight.get("persistent_counts_after") == persistent_counts_before
        and postflight.get("accepted") is True
    )


def census_parser(description: str) -> argparse.ArgumentParser:
    """Return the exact source-bound census CLI parser."""

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--plan-release-id", required=True)
    parser.add_argument("--expected-healthporta-plan-id", required=True)
    parser.add_argument("--expected-serving-revision-id", required=True)
    parser.add_argument("--expected-binding-set-digest", required=True)
    parser.add_argument("--expected-binding-count", required=True, type=int)
    parser.add_argument("--expected-in-network-binding-count", required=True, type=int)
    parser.add_argument("--expected-snapshot-count", required=True, type=int)
    parser.add_argument("--expected-plan-count", required=True, type=int)
    parser.add_argument("--expected-source-sha", required=True)
    parser.add_argument("--expected-source-manifest-sha256", required=True)
    parser.add_argument("--expected-harness-manifest-sha256", required=True)
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--source-only", action="store_true")
    return parser


def seal_cardinality_census(
    receipt_by_field: dict[str, Any],
    candidate_accepted: bool,
    finished_at: str,
) -> int:
    """Seal inner row-limit evidence as provisional pending outer authority."""

    receipt_by_field.update(
        status="provisional" if candidate_accepted else "gate_failed",
        accepted=False,
        cap_calibration_admissible=False,
        resource_proof_admissible=False,
        acceptance_authority=diagnostics.CENSUS_ACCEPTANCE_AUTHORITY,
        proof_scope="row_count_limits_only",
        finished_at=finished_at,
        phase="complete",
    )
    return 0 if candidate_accepted else 2
