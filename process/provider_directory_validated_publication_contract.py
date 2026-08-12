# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identity contract for one validated dataset publication request."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from process.provider_directory_fhir_subset_completion import (
    SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
)


VALIDATED_PUBLICATION_CANDIDATE_FIELD = "validated_publication_candidate"
AUTOMATIC_VALIDATED_PUBLICATION_POLICY = "automatic_after_verified_twin_v1"
AUTOMATIC_VALIDATED_PUBLICATION_ROLE = "verification_candidate"
AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS = (
    "verified_two_matching_exhaustive_acquisitions"
)
AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS = (
    "verified_two_matching_reviewed_subset_acquisitions"
)
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

_CANDIDATE_FIELDS = frozenset(
    {
        "source_id",
        "endpoint_id",
        "dataset_id",
        "dataset_hash",
        "acquisition_root_run_id",
        "validated_at",
        "automatic_publication_policy",
        "completion_proof_required_version",
        "completion_proof_sha256",
        "verification_campaign_id",
        "verification_source_scope_sha256",
        "expected_current",
    }
)
_DATASET_IDENTITY_FIELDS = frozenset(
    {
        "endpoint_id",
        "dataset_id",
        "dataset_hash",
        "acquisition_root_run_id",
    }
)
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
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


class ValidatedPublicationCandidateError(ValueError):
    """Raised when an observed publication identity is not exact."""


def _invalid(reason: str) -> ValidatedPublicationCandidateError:
    return ValidatedPublicationCandidateError(
        "provider_directory_validated_publication_candidate_" + reason
    )


def _strict_identity_text(value: Any, field_name: str, limit: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > limit
    ):
        raise _invalid("identity_invalid:" + field_name)
    return value


def _strict_hash(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None:
        raise _invalid("identity_invalid:" + field_name)
    return value


def _source_status_for_proof_pair(
    completion_proof_required_version: Any,
    completion_proof_sha256: Any,
) -> str:
    if (
        completion_proof_required_version is None
        and completion_proof_sha256 is None
    ):
        return AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS
    if (
        type(completion_proof_required_version) is int
        and completion_proof_required_version
        == SERVER_ISSUED_SUBSET_REQUIRED_VERSION
        and isinstance(completion_proof_sha256, str)
        and _HASH_PATTERN.fullmatch(completion_proof_sha256) is not None
    ):
        return AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS
    raise _invalid("completion_proof_pair_invalid")


def canonical_utc_timestamp(value: Any) -> str | None:
    """Return one deterministic UTC timestamp for a DB or JSON value."""

    if isinstance(value, str):
        raw_value = value
        if not raw_value or raw_value != raw_value.strip():
            return None
        try:
            timestamp = dt.datetime.fromisoformat(
                raw_value[:-1] + "+00:00"
                if raw_value.endswith("Z")
                else raw_value
            )
        except ValueError:
            return None
    elif isinstance(value, dt.datetime):
        timestamp = value
    else:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dt.UTC)
    return timestamp.astimezone(dt.UTC).isoformat()


def _strict_timestamp(value: Any) -> str:
    canonical = canonical_utc_timestamp(value)
    if not isinstance(value, str) or canonical is None or canonical != value:
        raise _invalid("identity_invalid:validated_at")
    return canonical


@dataclass(frozen=True)
class ProviderDirectoryDatasetIdentity:
    """Immutable identity of one endpoint dataset."""

    endpoint_id: str
    dataset_id: str
    dataset_hash: str
    acquisition_root_run_id: str

    @classmethod
    def from_payload(
        cls, identity_payload: Any
    ) -> "ProviderDirectoryDatasetIdentity | None":
        """Parse explicit null or one exact incumbent dataset identity."""

        if identity_payload is None:
            return None
        if (
            not isinstance(identity_payload, Mapping)
            or set(identity_payload) != _DATASET_IDENTITY_FIELDS
        ):
            raise _invalid("expected_current_schema_invalid")
        return cls(
            endpoint_id=_strict_identity_text(
                identity_payload.get("endpoint_id"),
                "expected_current.endpoint_id",
                64,
            ),
            dataset_id=_strict_identity_text(
                identity_payload.get("dataset_id"),
                "expected_current.dataset_id",
                96,
            ),
            dataset_hash=_strict_hash(
                identity_payload.get("dataset_hash"),
                "expected_current.dataset_hash",
            ),
            acquisition_root_run_id=_strict_identity_text(
                identity_payload.get("acquisition_root_run_id"),
                "expected_current.acquisition_root_run_id",
                64,
            ),
        )

    def to_payload(self) -> dict[str, str]:
        """Return the closed incumbent identity payload."""

        return {
            "endpoint_id": self.endpoint_id,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "acquisition_root_run_id": self.acquisition_root_run_id,
        }


@dataclass(frozen=True)
class ValidatedPublicationCandidate:
    """Observed candidate and expected current identity echoed by a client."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_hash: str
    acquisition_root_run_id: str
    validated_at: str
    automatic_publication_policy: str
    completion_proof_required_version: int | None
    completion_proof_sha256: str | None
    verification_campaign_id: str
    verification_source_scope_sha256: str
    expected_current: ProviderDirectoryDatasetIdentity | None

    @classmethod
    def from_payload(
        cls,
        candidate_payload: Any,
    ) -> "ValidatedPublicationCandidate":
        """Parse one exact validated publication candidate identity."""

        if (
            not isinstance(candidate_payload, Mapping)
            or set(candidate_payload) != _CANDIDATE_FIELDS
        ):
            raise _invalid("schema_invalid")
        candidate = cls(
            source_id=_strict_identity_text(
                candidate_payload.get("source_id"), "source_id", 64
            ),
            endpoint_id=_strict_identity_text(
                candidate_payload.get("endpoint_id"), "endpoint_id", 64
            ),
            dataset_id=_strict_identity_text(
                candidate_payload.get("dataset_id"), "dataset_id", 96
            ),
            dataset_hash=_strict_hash(
                candidate_payload.get("dataset_hash"), "dataset_hash"
            ),
            acquisition_root_run_id=_strict_identity_text(
                candidate_payload.get("acquisition_root_run_id"),
                "acquisition_root_run_id",
                64,
            ),
            validated_at=_strict_timestamp(
                candidate_payload.get("validated_at")
            ),
            automatic_publication_policy=_strict_identity_text(
                candidate_payload.get("automatic_publication_policy"),
                "automatic_publication_policy",
                64,
            ),
            completion_proof_required_version=candidate_payload.get(
                "completion_proof_required_version"
            ),
            completion_proof_sha256=candidate_payload.get(
                "completion_proof_sha256"
            ),
            verification_campaign_id=_strict_identity_text(
                candidate_payload.get("verification_campaign_id"),
                "verification_campaign_id",
                160,
            ),
            verification_source_scope_sha256=_strict_hash(
                candidate_payload.get("verification_source_scope_sha256"),
                "verification_source_scope_sha256",
            ),
            expected_current=ProviderDirectoryDatasetIdentity.from_payload(
                candidate_payload["expected_current"]
            ),
        )
        candidate._assert_exact_payload(candidate_payload)
        return candidate

    def _assert_exact_payload(self, candidate_payload: Mapping[str, Any]) -> None:
        if (
            self.automatic_publication_policy
            != AUTOMATIC_VALIDATED_PUBLICATION_POLICY
        ):
            raise _invalid("automatic_policy_invalid")
        validated_publication_source_status(self)
        if self.expected_current is not None:
            if self.expected_current.endpoint_id != self.endpoint_id:
                raise _invalid("expected_current_endpoint_mismatch")
            if self.expected_current.dataset_id == self.dataset_id:
                raise _invalid("expected_current_candidate_collision")
        if self.to_payload() != candidate_payload:
            raise _invalid("echo_changed")

    def to_payload(self) -> dict[str, Any]:
        """Return the closed candidate and expected current identity payload."""

        return {
            "source_id": self.source_id,
            "endpoint_id": self.endpoint_id,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "acquisition_root_run_id": self.acquisition_root_run_id,
            "validated_at": self.validated_at,
            "automatic_publication_policy": self.automatic_publication_policy,
            "completion_proof_required_version": (
                self.completion_proof_required_version
            ),
            "completion_proof_sha256": self.completion_proof_sha256,
            "verification_campaign_id": self.verification_campaign_id,
            "verification_source_scope_sha256": (
                self.verification_source_scope_sha256
            ),
            "expected_current": (
                self.expected_current.to_payload()
                if self.expected_current is not None
                else None
            ),
        }


def validated_publication_source_status(
    candidate: ValidatedPublicationCandidate,
) -> str:
    """Derive the sole allowed source status from the exact proof pair."""

    return _source_status_for_proof_pair(
        candidate.completion_proof_required_version,
        candidate.completion_proof_sha256,
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
