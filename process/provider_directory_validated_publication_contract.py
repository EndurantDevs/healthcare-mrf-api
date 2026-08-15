# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed request contract for one validated dataset publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from process.provider_directory_validated_publication_candidate import (
    AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS,
    AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS,
    ProviderDirectoryDatasetIdentity,
    ValidatedPublicationCandidate,
    ValidatedPublicationCandidateError,
    canonical_utc_timestamp,
    validated_publication_source_status,
)
from process.provider_directory_validated_publication_policies import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
)


VALIDATED_PUBLICATION_CANDIDATE_FIELD = "validated_publication_candidate"
AUTOMATIC_VALIDATED_PUBLICATION_ROLE = "verification_candidate"
VALIDATED_PUBLICATION_NON_PROFILE_TARGETS = (
    "dataset_network_plan",
    "dataset_affiliation_organization",
    "location_contacts",
    "location_coordinates",
    "resource_id_npis",
    "location_address_keys",
    "location_archive",
    "address_overlay",
    "network_catalog",
)

_PROFILE_FIELDS = frozenset(
    {
        "provider_directory_profile_contract_id",
        "provider_directory_profile_generation",
        "provider_directory_profile_selection_attestation",
    }
)
_ACQUISITION_FIELDS = frozenset(
    {
        "provider_directory_acquisition_strategy",
        "provider_directory_census_cutoff",
        "provider_directory_pagination_root_run_id",
        "provider_directory_reviewed_root_count",
        "provider_directory_reviewed_root_policy",
        "retry_of_run_id",
    }
)
_AMBIGUOUS_ALIAS_FIELDS = frozenset(
    {
        "provider_directory_source_id",
        "provider_directory_source_ids",
        "publish_artifact_targets",
        "publish_targets",
        "source_id",
    }
)
_INCOMPATIBLE_TRUE_FIELDS = frozenset(
    {
        "canonical_backfill_only",
        "contact_backfill_only",
        "dataset_followup_only",
        "dataset_rehydrate_only",
        "full_address_artifact_rebuild",
        "full_refresh",
        "import_resources",
        "publish_after_acquisition",
        "publish_artifacts",
        "require_complete_global_profile_fence",
        "seed_only",
        "stale_cleanup",
        "test",
        "test_mode",
    }
)


def _invalid(reason: str) -> ValidatedPublicationCandidateError:
    return ValidatedPublicationCandidateError(
        "provider_directory_validated_publication_candidate_" + reason
    )


def _canonical_targets(raw_targets: Any) -> tuple[str, ...] | None:
    if isinstance(raw_targets, str):
        target_values = raw_targets.split(",")
    elif isinstance(raw_targets, (list, tuple)):
        target_values = list(raw_targets)
    else:
        return None
    if (
        not target_values
        or any(
            not isinstance(target, str)
            or not target
            or target != target.strip()
            for target in target_values
        )
        or len(target_values) != len(set(target_values))
    ):
        return None
    return tuple(sorted(target_values))


def validated_publication_candidate_from_params(
    params: Mapping[str, Any],
) -> ValidatedPublicationCandidate | None:
    """Validate the only request shape allowed to carry observed identity."""

    if VALIDATED_PUBLICATION_CANDIDATE_FIELD not in params:
        return None
    candidate = ValidatedPublicationCandidate.from_payload(
        params.get(VALIDATED_PUBLICATION_CANDIDATE_FIELD)
    )
    source_ids = params.get("source_ids")
    if not isinstance(source_ids, list) or source_ids != [candidate.source_id]:
        raise _invalid("source_scope_invalid")
    if params.get("publish_artifacts_only") is not True:
        raise _invalid("publication_mode_invalid")
    if params.get("publish_corroboration") is not False:
        raise _invalid("corroboration_mode_invalid")
    if _canonical_targets(params.get("publish_artifacts_targets")) != tuple(
        sorted(VALIDATED_PUBLICATION_NON_PROFILE_TARGETS)
    ):
        raise _invalid("target_set_invalid")
    if any(field_name in params for field_name in _PROFILE_FIELDS):
        raise _invalid("profile_mode_invalid")
    if any(field_name in params for field_name in _ACQUISITION_FIELDS):
        raise _invalid("acquisition_mode_invalid")
    if any(field_name in params for field_name in _AMBIGUOUS_ALIAS_FIELDS):
        raise _invalid("alias_invalid")
    if any(params.get(field_name) for field_name in _INCOMPATIBLE_TRUE_FIELDS):
        raise _invalid("incompatible_mode")
    if params.get("refresh_preset") not in (None, "") or params.get(
        "preset"
    ) not in (None, ""):
        raise _invalid("preset_mode_invalid")
    return candidate
