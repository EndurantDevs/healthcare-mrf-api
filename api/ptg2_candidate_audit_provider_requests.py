# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build bounded exact provider/code requests for candidate audits."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    INTEGER_KEY_SET_BYTES,
    INTEGER_KEY_SET_MEMBERSHIP_BYTES,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_maps import (
    INTEGER_MAP_BUCKET_BYTES,
    INTEGER_MAP_BYTES,
)
from api.ptg2_db_sidecars import (
    _ValidatedProviderCodeRequests,
    _freeze_provider_code_requests,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_COMPOSITE_SET_MEMBERSHIP_BYTES = 160
_PROVIDER_CODE_REQUEST_BYTES = 512
_PROVIDER_CODE_REQUEST_BUCKET_BYTES = 160
_PROVIDER_CODE_REQUEST_MEMBERSHIP_BYTES = 16


@dataclass
class _ProviderCodeRequestBuilder:
    maximum_memberships: int
    maximum_code_keys: int
    retention_budget: CandidateAuditDecodedRetentionBudget | None
    code_keys_by_provider_set: dict[int, set[int]] = field(default_factory=dict)
    requested_code_keys: set[int] = field(default_factory=set)
    seen_npi_code_keys: set[tuple[int, int]] = field(default_factory=set)
    membership_count: int = 0

    def __post_init__(self) -> None:
        if self.retention_budget is not None:
            self.retention_budget.claim(
                INTEGER_MAP_BYTES + 2 * INTEGER_KEY_SET_BYTES,
                category="the requested provider/code indexes",
            )

    def add(self, provider_set_key: int, code_key: int) -> None:
        """Add one unique request pair only after validating both limits."""

        normalized_provider_key = int(provider_set_key)
        provider_code_keys = self.code_keys_by_provider_set.get(normalized_provider_key)
        if provider_code_keys is None:
            if self.retention_budget is not None:
                self.retention_budget.claim(
                    INTEGER_MAP_BUCKET_BYTES,
                    category="a requested provider/code bucket",
                )
            provider_code_keys = set()
            self.code_keys_by_provider_set[normalized_provider_key] = provider_code_keys
        normalized_code_key = int(code_key)
        if normalized_code_key in provider_code_keys:
            return
        if (
            normalized_code_key not in self.requested_code_keys
            and len(self.requested_code_keys) >= self.maximum_code_keys
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate requested code scope exceeds its bounded limit"
            )
        if self.membership_count >= self.maximum_memberships:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate provider-code scope exceeds its bounded limit"
            )
        if self.retention_budget is not None:
            self.retention_budget.claim(
                INTEGER_KEY_SET_MEMBERSHIP_BYTES,
                category="a requested provider/code membership",
            )
            if normalized_code_key not in self.requested_code_keys:
                self.retention_budget.claim(
                    INTEGER_KEY_SET_MEMBERSHIP_BYTES,
                    category="a requested distinct code key",
                )
        provider_code_keys.add(normalized_code_key)
        self.requested_code_keys.add(normalized_code_key)
        self.membership_count += 1

    def add_challenges(
        self,
        challenges: Sequence[AuditBatchChallenge],
        code_index: CandidateCodeIndex,
        provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    ) -> None:
        """Add request pairs reachable from each unique challenge NPI/code."""

        for challenge in challenges:
            provider_set_keys = provider_set_keys_by_npi.get(challenge.npi, ())
            code_pair = (challenge.code_system, challenge.code)
            for code_record in code_index.by_pair[code_pair]:
                code_key = int(code_record["code_key"])
                npi_code_key = (challenge.npi, code_key)
                if npi_code_key in self.seen_npi_code_keys:
                    continue
                if self.retention_budget is not None:
                    self.retention_budget.claim(
                        _COMPOSITE_SET_MEMBERSHIP_BYTES,
                        category="a requested NPI/code key",
                    )
                self.seen_npi_code_keys.add(npi_code_key)
                for provider_set_key in provider_set_keys:
                    self.add(provider_set_key, code_key)

    def freeze(self) -> _ValidatedProviderCodeRequests:
        """Freeze the exact request and release all mutable builder storage."""

        retained_request_bytes = _PROVIDER_CODE_REQUEST_BYTES + sum(
            _PROVIDER_CODE_REQUEST_BUCKET_BYTES
            + len(code_keys) * _PROVIDER_CODE_REQUEST_MEMBERSHIP_BYTES
            for code_keys in self.code_keys_by_provider_set.values()
        )
        if self.retention_budget is not None:
            self.retention_budget.claim(
                retained_request_bytes,
                category="the frozen provider/code request",
            )
        frozen_requests = _freeze_provider_code_requests(
            self.code_keys_by_provider_set,
            membership_count=self.membership_count,
        )
        if self.retention_budget is not None:
            temporary_bytes = (
                INTEGER_MAP_BYTES
                + 2 * INTEGER_KEY_SET_BYTES
                + sum(
                    INTEGER_MAP_BUCKET_BYTES
                    + len(code_keys) * INTEGER_KEY_SET_MEMBERSHIP_BYTES
                    for code_keys in self.code_keys_by_provider_set.values()
                )
                + len(self.requested_code_keys) * INTEGER_KEY_SET_MEMBERSHIP_BYTES
                + len(self.seen_npi_code_keys) * _COMPOSITE_SET_MEMBERSHIP_BYTES
            )
            self.retention_budget.release(temporary_bytes)
        return frozen_requests


def requested_code_keys_by_provider_set(
    challenges: Sequence[AuditBatchChallenge],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    maximum_memberships: int,
    maximum_code_keys: int,
) -> _ValidatedProviderCodeRequests:
    """Build the exact bounded request map for graph and persisted scopes."""

    request_builder = _ProviderCodeRequestBuilder(
        maximum_memberships,
        maximum_code_keys,
        retention_budget,
    )
    request_builder.add_challenges(
        challenges,
        code_index,
        provider_set_keys_by_npi,
    )
    for occurrence in persisted_audit_occurrences:
        request_builder.add(occurrence.provider_set_key, occurrence.code_key)
    return request_builder.freeze()


__all__ = ["requested_code_keys_by_provider_set"]
