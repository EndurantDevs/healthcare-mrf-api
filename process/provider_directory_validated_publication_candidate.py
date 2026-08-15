# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identity for one validated Provider Directory publication."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from process.provider_directory_fhir_subset_completion import (
    SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
)
from process.provider_directory_validated_publication_policies import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    GENERIC_PUBLICATION_POLICIES,
    REVIEWED_PUBLICATION_POLICIES,
)


AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS = (
    "verified_two_matching_exhaustive_acquisitions"
)
AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS = (
    "verified_two_matching_reviewed_subset_acquisitions"
)

_CANDIDATE_COMMON_FIELDS = frozenset(
    {
        "source_id",
        "endpoint_id",
        "dataset_id",
        "dataset_hash",
        "acquisition_root_run_id",
        "validated_at",
        "automatic_publication_policy",
        "expected_current",
    }
)
_CANDIDATE_TWIN_FIELDS = _CANDIDATE_COMMON_FIELDS | frozenset(
    {
        "completion_proof_required_version",
        "completion_proof_sha256",
        "verification_campaign_id",
        "verification_source_scope_sha256",
    }
)
_CANDIDATE_GENERIC_FIELDS = _CANDIDATE_COMMON_FIELDS | frozenset(
    {"content_proof_admission_sha256"}
)
_CANDIDATE_BOOTSTRAP_FIELDS = _CANDIDATE_GENERIC_FIELDS | frozenset(
    {"source_catalog_entry_id", "source_catalog_digest_sha256"}
)
_CANDIDATE_REVIEWED_SINGLE_FIELDS = _CANDIDATE_TWIN_FIELDS | frozenset(
    {"content_proof_admission_sha256"}
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


def _candidate_common_values(
    candidate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": _strict_identity_text(
            candidate_payload.get("source_id"), "source_id", 64
        ),
        "endpoint_id": _strict_identity_text(
            candidate_payload.get("endpoint_id"), "endpoint_id", 64
        ),
        "dataset_id": _strict_identity_text(
            candidate_payload.get("dataset_id"), "dataset_id", 96
        ),
        "dataset_hash": _strict_hash(
            candidate_payload.get("dataset_hash"), "dataset_hash"
        ),
        "acquisition_root_run_id": _strict_identity_text(
            candidate_payload.get("acquisition_root_run_id"),
            "acquisition_root_run_id",
            64,
        ),
        "validated_at": _strict_timestamp(candidate_payload.get("validated_at")),
        "expected_current": ProviderDirectoryDatasetIdentity.from_payload(
            candidate_payload["expected_current"]
        ),
    }


def _generic_candidate_policy_values(
    candidate_payload: Mapping[str, Any],
    automatic_policy: str,
) -> tuple[str, frozenset[str], dict[str, Any]]:
    is_bootstrap = (
        automatic_policy == AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
    )
    expected_fields = (
        _CANDIDATE_BOOTSTRAP_FIELDS
        if is_bootstrap
        else _CANDIDATE_GENERIC_FIELDS
    )
    catalog_values_by_field = {
        "source_catalog_entry_id": (
            _strict_identity_text(
                candidate_payload.get("source_catalog_entry_id"),
                "source_catalog_entry_id",
                160,
            )
            if is_bootstrap
            else None
        ),
        "source_catalog_digest_sha256": (
            _strict_hash(
                candidate_payload.get("source_catalog_digest_sha256"),
                "source_catalog_digest_sha256",
            )
            if is_bootstrap
            else None
        ),
    }
    return automatic_policy, expected_fields, {
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
        "verification_campaign_id": None,
        "verification_source_scope_sha256": None,
        "content_proof_admission_sha256": _strict_hash(
            candidate_payload.get("content_proof_admission_sha256"),
            "content_proof_admission_sha256",
        ),
        **catalog_values_by_field,
    }


def _proof_candidate_policy_values(
    candidate_payload: Mapping[str, Any],
    automatic_policy: str,
) -> tuple[str, frozenset[str], dict[str, Any]]:
    if automatic_policy not in {
        AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
        *REVIEWED_PUBLICATION_POLICIES,
    }:
        raise _invalid("automatic_policy_invalid")
    is_reviewed = automatic_policy in REVIEWED_PUBLICATION_POLICIES
    is_single_root = (
        automatic_policy == AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY
    )
    proof_hash = candidate_payload.get("completion_proof_sha256")
    expected_fields = (
        _CANDIDATE_REVIEWED_SINGLE_FIELDS
        if is_single_root
        else _CANDIDATE_TWIN_FIELDS
    )
    return automatic_policy, expected_fields, {
        "completion_proof_required_version": candidate_payload.get(
            "completion_proof_required_version"
        ),
        "completion_proof_sha256": (
            _strict_hash(proof_hash, "completion_proof_sha256")
            if is_reviewed
            else proof_hash
        ),
        "verification_campaign_id": _strict_identity_text(
            candidate_payload.get("verification_campaign_id"),
            "verification_campaign_id",
            160,
        ),
        "verification_source_scope_sha256": _strict_hash(
            candidate_payload.get("verification_source_scope_sha256"),
            "verification_source_scope_sha256",
        ),
        "content_proof_admission_sha256": (
            _strict_hash(
                candidate_payload.get("content_proof_admission_sha256"),
                "content_proof_admission_sha256",
            )
            if is_single_root
            else None
        ),
        "source_catalog_entry_id": None,
        "source_catalog_digest_sha256": None,
    }


def _candidate_policy_values(
    candidate_payload: Mapping[str, Any],
) -> tuple[str, frozenset[str], dict[str, Any]]:
    automatic_policy = _strict_identity_text(
        candidate_payload.get("automatic_publication_policy"),
        "automatic_publication_policy",
        64,
    )
    if automatic_policy in GENERIC_PUBLICATION_POLICIES:
        return _generic_candidate_policy_values(
            candidate_payload,
            automatic_policy,
        )
    return _proof_candidate_policy_values(candidate_payload, automatic_policy)


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
    verification_campaign_id: str | None
    verification_source_scope_sha256: str | None
    content_proof_admission_sha256: str | None
    source_catalog_entry_id: str | None
    source_catalog_digest_sha256: str | None
    expected_current: ProviderDirectoryDatasetIdentity | None

    @classmethod
    def from_payload(
        cls,
        candidate_payload: Any,
    ) -> "ValidatedPublicationCandidate":
        """Parse one exact validated publication candidate identity."""

        if not isinstance(candidate_payload, Mapping):
            raise _invalid("schema_invalid")
        automatic_policy, expected_fields, policy_values_by_field = (
            _candidate_policy_values(candidate_payload)
        )
        if set(candidate_payload) != expected_fields:
            raise _invalid("schema_invalid")
        candidate = cls(
            **_candidate_common_values(candidate_payload),
            automatic_publication_policy=automatic_policy,
            **policy_values_by_field,
        )
        candidate._assert_exact_payload(candidate_payload)
        return candidate

    def _assert_exact_payload(self, candidate_payload: Mapping[str, Any]) -> None:
        if (
            self.automatic_publication_policy
            == AUTOMATIC_VALIDATED_PUBLICATION_POLICY
        ):
            validated_publication_source_status(self)
        elif self.automatic_publication_policy not in {
            *GENERIC_PUBLICATION_POLICIES,
            *REVIEWED_PUBLICATION_POLICIES,
        }:
            raise _invalid("automatic_policy_invalid")
        if (
            self.automatic_publication_policy
            == AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
            and self.expected_current is None
        ):
            raise _invalid("expected_current_required")
        if (
            self.automatic_publication_policy
            == AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
            and self.expected_current is not None
        ):
            raise _invalid("expected_current_forbidden")
        if self.automatic_publication_policy in REVIEWED_PUBLICATION_POLICIES and not (
            type(self.completion_proof_required_version) is int
            and self.completion_proof_required_version
            == SERVER_ISSUED_SUBSET_REQUIRED_VERSION
        ):
            raise _invalid("completion_proof_pair_invalid")
        if self.expected_current is not None:
            if self.expected_current.endpoint_id != self.endpoint_id:
                raise _invalid("expected_current_endpoint_mismatch")
            if self.expected_current.dataset_id == self.dataset_id:
                raise _invalid("expected_current_candidate_collision")
        if self.to_payload() != candidate_payload:
            raise _invalid("echo_changed")

    def to_payload(self) -> dict[str, Any]:
        """Return the closed candidate and expected current identity payload."""

        candidate_by_field = {
            "source_id": self.source_id,
            "endpoint_id": self.endpoint_id,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "acquisition_root_run_id": self.acquisition_root_run_id,
            "validated_at": self.validated_at,
            "automatic_publication_policy": self.automatic_publication_policy,
            "expected_current": (
                self.expected_current.to_payload()
                if self.expected_current is not None
                else None
            ),
        }
        if self.automatic_publication_policy not in GENERIC_PUBLICATION_POLICIES:
            candidate_by_field.update(
                completion_proof_required_version=(
                    self.completion_proof_required_version
                ),
                completion_proof_sha256=self.completion_proof_sha256,
                verification_campaign_id=self.verification_campaign_id,
                verification_source_scope_sha256=(
                    self.verification_source_scope_sha256
                ),
            )
            if self.automatic_publication_policy == (
                AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY
            ):
                candidate_by_field["content_proof_admission_sha256"] = (
                    self.content_proof_admission_sha256
                )
        else:
            candidate_by_field["content_proof_admission_sha256"] = (
                self.content_proof_admission_sha256
            )
            if self.automatic_publication_policy == (
                AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
            ):
                candidate_by_field.update(
                    source_catalog_entry_id=self.source_catalog_entry_id,
                    source_catalog_digest_sha256=(
                        self.source_catalog_digest_sha256
                    ),
                )
        return candidate_by_field


def validated_publication_source_status(
    candidate: ValidatedPublicationCandidate,
) -> str:
    """Derive the sole allowed source status from the exact proof pair."""

    if (
        candidate.automatic_publication_policy
        != AUTOMATIC_VALIDATED_PUBLICATION_POLICY
    ):
        raise _invalid("source_status_invalid")
    return _source_status_for_proof_pair(
        candidate.completion_proof_required_version,
        candidate.completion_proof_sha256,
    )
