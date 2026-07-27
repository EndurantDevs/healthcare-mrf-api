from __future__ import annotations

from copy import deepcopy
from typing import Any


def _mutated(summary: dict[str, Any], path: tuple[str, ...], value: Any):
    changed = deepcopy(summary)
    target: Any = changed
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    return changed


def _resource_input_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    admission = summary["resource_admission"]
    return (
        (
            _mutated(summary, ("resource_admission",), None),
            "invalid resource admission",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "input_factor_bytes"),
                admission["input_factor_bytes"] + 1,
            ),
            "resource input byte count changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "factor_edge_count"),
                admission["factor_edge_count"] + 1,
            ),
            "resource factor edge count changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "factor_owner_count"),
                admission["factor_owner_count"] + 1,
            ),
            "resource factor owner count changed",
        ),
    )


def _tax_resource_mutations(
    summary: dict[str, Any],
) -> tuple[tuple[Any, str], ...]:
    admission = summary["resource_admission"]
    return (
        (
            _mutated(
                summary,
                (
                    "resource_admission",
                    "tax_identity_merge_bitmap_upper_bound_bytes",
                ),
                admission["tax_identity_merge_bitmap_upper_bound_bytes"] + 1,
            ),
            "tax identity merge bitmap admission changed",
        ),
        (
            _mutated(
                summary,
                (
                    "resource_admission",
                    "tax_identity_source_ordinal_upper_bound_bytes",
                ),
                admission["tax_identity_source_ordinal_upper_bound_bytes"] + 1,
            ),
            "tax identity source ordinal admission changed",
        ),
        (
            _mutated(
                summary,
                (
                    "resource_admission",
                    "tax_identity_projection_upper_bound_bytes",
                ),
                admission["tax_identity_projection_upper_bound_bytes"] + 1,
            ),
            "tax identity projection admission changed",
        ),
    )


def _resource_limit_mutations(
    summary: dict[str, Any],
    option_by_name: dict[str, int],
) -> tuple[tuple[Any, str], ...]:
    return (
        (
            _mutated(
                summary,
                ("resource_admission", "max_estimated_model_bytes"),
                option_by_name["max_estimated_model_bytes"] + 1,
            ),
            "resource model byte limit changed",
        ),
        (
            _mutated(
                summary,
                ("resource_admission", "max_factor_edges"),
                option_by_name["max_factor_edges"] + 1,
            ),
            "resource factor edge limit changed",
        ),
        (
            _mutated(summary, ("resource_admission", "formula"), None),
            "admission formula is missing",
        ),
    )


def resource_admission_mutations(
    summary: dict[str, Any],
    option_by_name: dict[str, int],
) -> tuple[tuple[Any, str], ...]:
    """Build authenticated resource-accounting drift cases."""

    return (
        *_resource_input_mutations(summary),
        *_tax_resource_mutations(summary),
        *_resource_limit_mutations(summary, option_by_name),
    )
