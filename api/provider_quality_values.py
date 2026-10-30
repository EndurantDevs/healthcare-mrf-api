# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure provider-quality cohort, geography and availability values."""

from __future__ import annotations

from typing import Any


def as_bool(value: Any) -> bool | None:
    """Normalize optional quality booleans, including on/off strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "yes", "y", "on"}:
        return True
    if text_value in {"0", "false", "no", "n", "off"}:
        return False
    return None


def empty_domain_payload() -> dict[str, Any]:
    """Create an independent empty quality domain with unset evidence intervals."""
    return {
        "risk_ratio_point": None,
        "score_0_100": None,
        "evidence_n": None,
        "ci_75": {"low": None, "high": None},
        "ci_90": {"low": None, "high": None},
    }


def normalize_cohort_level(raw: Any) -> str | None:
    """Normalize recognized cohort levels to L0 through L3; return None otherwise."""
    text = str(raw or "").strip().upper()
    if not text:
        return None
    if text in {"L0", "L1", "L2", "L3"}:
        return text
    if text in {"0", "1", "2", "3"}:
        return f"L{text}"
    return None


def normalize_peer_geography_scope(raw: Any) -> str | None:
    """Normalize geography aliases while retaining unknown scope labels."""
    text = str(raw or "").strip().lower()
    if text in {"zip", "zip5", "zip_code", "zipcode", "postal", "postal_code", "zip_ring"}:
        return "zip"
    if text in {"state", "state_key", "region", "province"}:
        return "state"
    if text in {"national", "country", "us", "usa", "all"}:
        return "national"
    return text or None


def geography_priority_for_benchmark_mode(
    benchmark_mode: str,
    *,
    state_key: str | None,
    zip5: str | None,
) -> list[tuple[str, str]]:
    """Order applicable ZIP, state and national scopes without duplicates."""
    candidates: list[tuple[str, str]] = []
    if benchmark_mode == "zip" and zip5:
        candidates.append(("zip", zip5))
    if benchmark_mode in {"zip", "state"} and state_key:
        candidates.append(("state", state_key))
    candidates.append(("national", "US"))

    deduped_geographies: list[tuple[str, str]] = []
    seen_geographies: set[tuple[str, str]] = set()
    for scope, value in candidates:
        item = (scope, str(value).strip())
        if item in seen_geographies:
            continue
        seen_geographies.add(item)
        deduped_geographies.append(item)
    return deduped_geographies


def is_value_match_or_generic(row_value: Any, requested_value: str | None, *, upper: bool) -> bool:
    """Match a requested code or accept empty and generic row values."""
    if requested_value is None:
        return True
    text = str(row_value or "").strip()
    if not text:
        return True
    normalized = text.upper() if upper else text.lower()
    generic_values = {"__all__", "all", "any", "*", "n/a", "na", "none", "null"}
    if normalized.lower() in generic_values:
        return True
    request_normalized = requested_value.upper() if upper else requested_value.lower()
    return normalized == request_normalized


def provider_quality_unavailable_reasons(
    profile: dict[str, Any] | None,
    *,
    benchmark_mode: str | None = None,
) -> list[str]:
    """Describe missing profile, specialty and requested benchmark geography."""
    if profile is None:
        return ["provider_profile_not_found"]

    reasons: list[str] = []
    if not profile.get("specialty_key") and not profile.get("taxonomy_code"):
        reasons.append("missing_specialty_or_taxonomy")
    if not profile.get("state_key") and not profile.get("zip5"):
        reasons.append("missing_geography")
    if benchmark_mode == "zip" and not profile.get("zip5"):
        reasons.append("missing_zip5_for_zip_benchmark")
    if benchmark_mode == "state" and not profile.get("state_key"):
        reasons.append("missing_state_for_state_benchmark")
    return reasons


def estimated_benchmark_modes_for_profile(
    profile: dict[str, Any],
    *,
    benchmark_mode: str | None = None,
) -> list[str]:
    """List benchmark modes supported by the profile geography in priority order."""
    if benchmark_mode is not None:
        if benchmark_mode == "zip":
            return ["zip"] if profile.get("zip5") else []
        if benchmark_mode == "state":
            return ["state"] if profile.get("state_key") else []
        return ["national"]

    modes: list[str] = []
    if profile.get("zip5"):
        modes.append("zip")
    if profile.get("state_key"):
        modes.append("state")
    modes.append("national")
    return modes


def quality_peer_target_query_variants(select_sql: str, state_enabled: bool, zip_enabled: bool) -> tuple[str, str]:
    # Fast path: run index-friendly scope-specific probes.
    """Build scoped UNION and legacy-fallback SQL for quality-peer targets."""
    query_parts = [select_sql + "\n          AND geography_scope = 'national'"]
    if state_enabled:
        query_parts.append(
            select_sql + "\n          AND geography_scope = 'state'\n          AND geography_value = :state_key"
        )
    if zip_enabled:
        query_parts.append(
            select_sql + "\n          AND geography_scope = 'zip'\n          AND geography_value = :zip5"
        )
    geography_clause = """
          AND (
                geography_scope IS NULL
                OR geography_scope = 'national'
                OR (
                    :state_enabled
                    AND geography_scope = 'state'
                    AND geography_value = :state_key
                )
                OR (
                    :zip_enabled
                    AND geography_scope = 'zip'
                    AND geography_value = :zip5
                )
          )
    """
    return "\n        UNION ALL\n".join(query_parts), select_sql + geography_clause
