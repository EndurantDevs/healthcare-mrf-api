"""Sealed size-relative elapsed-time budget for the PTG V4 dev canary."""

from __future__ import annotations

from typing import Any, Mapping

from scripts.ptg_v4_dev_canary_support import (
    CanaryConfigurationError,
    ElapsedBudget,
)

ELAPSED_BUDGET_POLICY = "ptg_v4_elapsed_budget_v1"
ELAPSED_FIXED_SECONDS = 300.0
SECONDS_PER_INPUT_GIB = 30.0
SECONDS_PER_MILLION_COMPONENT_FACTS = 3.0


def elapsed_budget(
    database_evidence_by_field: Mapping[str, Any],
) -> ElapsedBudget:
    """Build the source-controlled ceiling from authenticated work inputs."""

    resources_by_field = sealed_resource_admission(database_evidence_by_field)
    budget = ElapsedBudget(
        resources_by_field["compressed_acquisition_bytes"],
        resources_by_field["factor_edge_count"],
        ELAPSED_FIXED_SECONDS,
        SECONDS_PER_INPUT_GIB,
        SECONDS_PER_MILLION_COMPONENT_FACTS,
    )
    if budget.ceiling_seconds <= 0:
        raise CanaryConfigurationError("elapsed budget ceiling must be positive")
    return budget


def sealed_resource_admission(
    database_evidence_by_field: Mapping[str, Any],
) -> dict[str, int]:
    """Return exact sealed acquisition/factor inputs or fail closed."""

    diagnostic_evidence = database_evidence_by_field.get(
        "provider_graph_diagnostic"
    )
    resources = (
        diagnostic_evidence.get("resources")
        if isinstance(diagnostic_evidence, Mapping)
        else None
    )
    required_fields = (
        "compressed_acquisition_bytes",
        "input_factor_bytes",
        "factor_edge_count",
        "empty_npi_tin_only_normalization_count",
    )
    if not isinstance(resources, Mapping) or set(resources) != set(required_fields):
        raise CanaryConfigurationError(
            "sealed provider-graph resource evidence is missing"
        )
    try:
        normalized_by_field = {
            field_name: int(resources[field_name]) for field_name in required_fields
        }
    except (TypeError, ValueError) as exc:
        raise CanaryConfigurationError(
            "sealed provider-graph resource evidence is invalid"
        ) from exc
    if (
        normalized_by_field["compressed_acquisition_bytes"] <= 0
        or normalized_by_field["input_factor_bytes"] < 0
        or normalized_by_field["factor_edge_count"] < 0
        or normalized_by_field["empty_npi_tin_only_normalization_count"] < 0
    ):
        raise CanaryConfigurationError(
            "sealed provider-graph resource evidence is invalid"
        )
    return normalized_by_field
