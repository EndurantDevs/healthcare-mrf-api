# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound provider expansion before reading a release's market."""

from sanic.exceptions import InvalidUsage


def _is_bounded_provider_expansion(args, *, zip5, latitude, longitude, has_taxonomy_scope):
    """Identify scopes accepted without consulting the release's market."""
    return bool(zip5 or (latitude is not None and longitude is not None)) and (
        has_taxonomy_scope
        or (
            str(args.get("order_by") or "").strip().lower() in {"distance", "distance_miles"}
            and (args.get("order") in (None, "", "null") or str(args.get("order")).strip().lower() == "asc")
        )
    )


def _raise_broad_group_plan_provider_expansion(*, has_taxonomy_scope: bool) -> None:
    """Raise the actionable error for an unbounded provider expansion."""
    if has_taxonomy_scope:
        raise InvalidUsage(
            "Statewide taxonomy-scoped CPT office-visit provider expansion is too broad; "
            "add zip5 with zip_radius_miles (or lat, long, and radius_miles).",
            context={
                "fix_it": {
                    "reason": (
                        "A statewide taxonomy search expands too many provider-rate "
                        "candidates for an interactive request."
                    ),
                    "retry_options": [
                        {
                            "zip5": "<member ZIP>",
                            "zip_radius_miles": 10,
                            "note": "Retry inside a bounded ZIP radius.",
                        }
                    ],
                }
            },
        )
    raise InvalidUsage(
        "Broad CPT office-visit provider expansion for a group plan is a provider-directory request; "
        "use /api/v1/pricing/group-plan-providers with specialty/classification/location filters, or add "
        "specialty/taxonomy/npi and use procedure pricing only when the user asks for office-visit cost."
    )
