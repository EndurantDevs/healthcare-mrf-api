"""Bounded singleton-direct validation for signed exact-wave admission."""

from __future__ import annotations

from typing import Any, Mapping

from api.control_import_wave_attestation import _canonical
from process.ptg_singleton_direct_control import (
    protected_singleton_direct_presence,
    require_exact_wave_singleton_direct_params,
)
from process.ptg_frozen_control import normalize_protected_rate_params


MAX_INTENT_CANONICAL_BYTES = 64 * 1024
MAX_ATTESTATION_CANONICAL_BYTES = 32 * 1024 * 1024


def require_bounded_wave_request(request_body: object) -> None:
    """Reject an oversized canonical request before signature processing."""

    if len(_canonical(request_body)) > MAX_ATTESTATION_CANONICAL_BYTES:
        raise ValueError("cohort_attestation exceeds its canonical byte limit")


def require_bounded_direct_intent(raw_intent: object) -> None:
    """Apply the direct-only per-intent limit even to malformed versions."""

    raw_params = (
        raw_intent.get("params") if isinstance(raw_intent, dict) else None
    )
    if (
        isinstance(raw_params, dict)
        and protected_singleton_direct_presence(raw_params)
        and len(_canonical(raw_intent)) > MAX_INTENT_CANONICAL_BYTES
    ):
        raise ValueError(
            "each signed intent must fit its canonical byte limit"
        )


def normalized_wave_params(params_by_name: object) -> dict[str, Any]:
    """Validate frozen or singleton-direct protected input parameters."""

    if not isinstance(params_by_name, Mapping):
        raise ValueError("signed intent params must be an object")
    return normalize_protected_rate_params(params_by_name)


def require_matching_direct_coordinate(
    params_by_name: Mapping[str, Any],
    content_version: str,
    *,
    source_file_import_id: str,
    wave_id: str,
) -> None:
    """Bind the signed envelope to the exact protected direct contract."""

    require_exact_wave_singleton_direct_params(
        params_by_name,
        wave_id=wave_id,
    )
    if (
        protected_singleton_direct_presence(params_by_name)
        and (
            params_by_name.get("source_file_import_id")
            != source_file_import_id
            or params_by_name.get("content_version") != content_version
        )
    ):
        raise ValueError(
            "signed intent coordinate conflicts with direct input"
        )
