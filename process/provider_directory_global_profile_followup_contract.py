# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure descriptor contract for external global Provider Directory Profile work."""

from __future__ import annotations

import re
from typing import Any

from process.provider_directory_profile_capacity_types import (
    PROFILE_STRATEGY_VERSION,
)


PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID = (
    "healthporta.provider-directory.global-profile-followup.v1"
)
PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_KIND = "provider_directory_global_profile"
_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,254}\Z")


def _exact_identifier(value: object, field_name: str, limit: int) -> str:
    if (
        type(value) is not str
        or len(value) > limit
        or _IDENTIFIER_PATTERN.fullmatch(value) is None
    ):
        raise ValueError(
            f"provider_directory_global_profile_followup_{field_name}_invalid"
        )
    return value


def build_provider_directory_global_profile_followup(
    *,
    source_id: str,
    dataset_id: str,
    parent_run_id: str,
) -> dict[str, Any]:
    """Bind one immutable publication to the closed global controller shape."""

    exact_source_id = _exact_identifier(source_id, "source_id", 96)
    exact_dataset_id = _exact_identifier(dataset_id, "dataset_id", 128)
    exact_parent_run_id = _exact_identifier(parent_run_id, "parent_run_id", 64)
    return {
        "status": "required",
        "kind": PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_KIND,
        "intent": "ensure_desired_generation_observed",
        "importer": "provider-directory-fhir",
        "source_id": exact_source_id,
        "dataset_id": exact_dataset_id,
        "parent_run_id": exact_parent_run_id,
        "idempotency_key": "provider-directory-global-profile:" + exact_dataset_id,
        "triggered_by": "pd_profile_followup",
        "params": {
            "publish_artifacts_only": True,
            "publish_artifacts_targets": ["profile"],
            "source_ids": [],
            "require_complete_global_profile_fence": True,
            "publish_corroboration": False,
            "probe": False,
            "import_resources": False,
            "provider_directory_profile_parent_run_id": exact_parent_run_id,
            "provider_directory_profile_dataset_id": exact_dataset_id,
        },
    }


def profile_followup_receipt_metadata() -> dict[str, str]:
    """Return receipt metadata excluded from the closed controller payload."""

    return {
        "external_followup_contract_id": (
            PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID
        ),
        "profile_strategy_version": PROFILE_STRATEGY_VERSION,
    }


__all__ = (
    "PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_CONTRACT_ID",
    "PROVIDER_DIRECTORY_GLOBAL_PROFILE_FOLLOWUP_KIND",
    "build_provider_directory_global_profile_followup",
    "profile_followup_receipt_metadata",
)
