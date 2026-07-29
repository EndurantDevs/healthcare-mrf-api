# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete FHIR Organization scan records and digest proofs."""

from __future__ import annotations

import hashlib
import hmac
import json
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from process.tin_npi_connector_evidence import (
    FhirTinNpiEvidence,
    _fhir_organization_identity_bytes,
    _fhir_organization_record_identity_sha256,
)
from process.tin_npi_connector_security import canonical_token_policy_id
from process.tin_npi_connector_source import _strict_hash_hex
from process.tin_npi_connector_support import (
    _EVIDENCE_SET_HASH_DOMAIN,
    _SCAN_PROOF_HASH_DOMAIN,
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID,
    TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
    strict_evidence_text,
)

_strict_evidence_id = strict_evidence_text


@dataclass
class _ScanEvidenceSummary:
    """Mutable validation state for one Organization's policy projections."""

    evidence_keys: list[tuple[str, int, bytes]]
    npi_sets_by_policy: dict[str, set[int]]
    source_records_by_policy: dict[str, set[bytes]]
    token_hmacs_by_policy: dict[str, set[bytes]]
    identifier_policy_identities: set[tuple[str, str]]
    identifier_rule_identities: set[tuple[str, str]]


def _empty_scan_evidence_summary() -> _ScanEvidenceSummary:
    """Return isolated empty collections for one scan-record validation."""

    return _ScanEvidenceSummary([], {}, {}, {}, set(), set())


def _validate_scan_record_shape(scan_record: FhirOrganizationScanRecord) -> None:
    """Validate the record scope, terminal state, and evidence tuple shape."""

    _strict_evidence_id(scan_record.source_id, "source ID", limit=64)
    _strict_evidence_id(scan_record.source_endpoint_id, "endpoint ID", limit=64)
    _strict_evidence_id(scan_record.source_dataset_id, "dataset ID", limit=128)
    _fhir_organization_identity_bytes(scan_record.resource_id, scan_record.payload_hash)
    is_invalid = (
        type(scan_record.state) is not FhirOrganizationEvidenceState
        or scan_record.state not in FHIR_ORGANIZATION_SCAN_TERMINAL_STATES
        or type(scan_record.evidence) is not tuple
        or any(
            type(evidence_row) is not FhirTinNpiEvidence
            for evidence_row in scan_record.evidence
        )
        or (scan_record.state is FhirOrganizationEvidenceState.MATCHED)
        != bool(scan_record.evidence)
    )
    if is_invalid:
        raise TinNpiConnectorError("FHIR Organization scan record is invalid")


def _add_evidence_to_summary(
    summary: _ScanEvidenceSummary,
    evidence_row: FhirTinNpiEvidence,
) -> None:
    """Add one already scoped evidence row to its policy parity summary."""

    policy_id = evidence_row.token.token_policy_id
    summary.evidence_keys.append(
        (policy_id, evidence_row.npi, evidence_row.evidence_id)
    )
    summary.npi_sets_by_policy.setdefault(policy_id, set()).add(evidence_row.npi)
    summary.source_records_by_policy.setdefault(policy_id, set()).add(
        evidence_row.source_record_hmac_sha256
    )
    summary.token_hmacs_by_policy.setdefault(policy_id, set()).add(
        evidence_row.token.tin_hmac_sha256
    )
    summary.identifier_policy_identities.add(
        (evidence_row.identifier_policy_id, evidence_row.identifier_policy_sha256)
    )
    summary.identifier_rule_identities.add(
        (evidence_row.identifier_rule_id, evidence_row.identifier_rule_sha256)
    )


def _scan_evidence_summary(
    scan_record: FhirOrganizationScanRecord,
) -> _ScanEvidenceSummary:
    """Validate record binding and summarize evidence by token policy."""

    expected_record_identity = _fhir_organization_record_identity_sha256(
        scan_record.resource_id,
        scan_record.payload_hash,
    )
    summary = _empty_scan_evidence_summary()
    for evidence_row in scan_record.evidence:
        if (
            evidence_row.source_id != scan_record.source_id
            or evidence_row.source_endpoint_id != scan_record.source_endpoint_id
            or evidence_row.source_dataset_id != scan_record.source_dataset_id
        ):
            raise TinNpiConnectorError(
                "FHIR Organization scan evidence is outside its record"
            )
        if not hmac.compare_digest(
            evidence_row.source_record_identity_sha256,
            expected_record_identity,
        ) or not hmac.compare_digest(
            evidence_row.source_record_payload_hash,
            scan_record.payload_hash,
        ):
            raise TinNpiConnectorError(
                "FHIR Organization scan evidence identity is inconsistent"
            )
        _add_evidence_to_summary(summary, evidence_row)
    return summary


def _validate_scan_evidence_parity(summary: _ScanEvidenceSummary) -> None:
    """Require sorted evidence and an exact policy-by-NPI Cartesian product."""

    if summary.evidence_keys != sorted(set(summary.evidence_keys)):
        raise TinNpiConnectorError(
            "FHIR Organization scan evidence is duplicated or unordered"
        )
    npi_sets = summary.npi_sets_by_policy
    is_inconsistent = npi_sets and (
        len(summary.identifier_policy_identities) != 1
        or len(summary.identifier_rule_identities) != 1
        or len({tuple(sorted(npis)) for npis in npi_sets.values()}) != 1
        or any(
            len(source_records) != 1
            for source_records in summary.source_records_by_policy.values()
        )
        or any(
            len(token_hmacs) != 1
            for token_hmacs in summary.token_hmacs_by_policy.values()
        )
        or len(summary.evidence_keys)
        != len(npi_sets) * len(next(iter(npi_sets.values())))
    )
    if is_inconsistent:
        raise TinNpiConnectorError(
            "FHIR Organization scan policy evidence is inconsistent"
        )


@dataclass(frozen=True)
class FhirOrganizationScanRecord:
    """One Organization and its single terminal outcome from the stable scan."""

    source_id: str
    source_endpoint_id: str
    source_dataset_id: str
    resource_id: str
    payload_hash: str
    state: FhirOrganizationEvidenceState
    evidence: tuple[FhirTinNpiEvidence, ...] = ()

    def __post_init__(self) -> None:
        """Validate terminal-state, source identity, and policy parity."""

        _validate_scan_record_shape(self)
        _validate_scan_evidence_parity(_scan_evidence_summary(self))

    @property
    def scan_key(self) -> tuple[bytes, bytes, bytes, bytes]:
        """Return the bytewise source/dataset/resource stable scan order key."""

        return (
            self.source_id.encode("utf-8"),
            self.source_endpoint_id.encode("utf-8"),
            self.source_dataset_id.encode("utf-8"),
            self.resource_id.encode("utf-8"),
        )


@dataclass(frozen=True)
class FhirOrganizationScanProof:
    """Compact proof that every Organization reached one terminal state."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    source_summary_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    organization_resource_count: int
    organization_resource_sha256: str
    state_counts: tuple[tuple[str, int], ...]
    matched_evidence_counts: tuple[tuple[str, int], ...]
    matched_evidence_sha256: str

    def __post_init__(self) -> None:
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.dataset_id, "dataset ID", limit=128)
        _strict_hash_hex(self.source_summary_sha256, "FHIR source-summary hash")
        _strict_evidence_id(
            self.identifier_rule_id,
            "identifier rule ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_rule_sha256,
            "FHIR identifier rule hash",
        )
        _strict_hash_hex(
            self.organization_resource_sha256,
            "FHIR Organization resource hash",
        )
        _strict_hash_hex(
            self.matched_evidence_sha256,
            "FHIR matched evidence hash",
        )
        expected_state_names = tuple(
            sorted(state.value for state in FHIR_ORGANIZATION_SCAN_TERMINAL_STATES)
        )
        if (
            type(self.organization_resource_count) is not int
            or self.organization_resource_count < 0
            or type(self.state_counts) is not tuple
            or tuple(name for name, _count in self.state_counts) != expected_state_names
            or any(
                type(count) is not int or count < 0
                for _name, count in self.state_counts
            )
            or sum(count for _name, count in self.state_counts)
            != self.organization_resource_count
            or type(self.matched_evidence_counts) is not tuple
            or not self.matched_evidence_counts
            or tuple(policy_id for policy_id, _count in self.matched_evidence_counts)
            != tuple(
                sorted(
                    {policy_id for policy_id, _count in self.matched_evidence_counts}
                )
            )
            or any(
                canonical_token_policy_id(policy_id) != policy_id
                or type(count) is not int
                or count < self.matched_organization_count
                for policy_id, count in self.matched_evidence_counts
            )
            or (
                self.matched_evidence_counts
                and len({count for _policy_id, count in self.matched_evidence_counts})
                != 1
            )
            or (self.matched_organization_count == 0)
            != (sum(count for _policy_id, count in self.matched_evidence_counts) == 0)
        ):
            raise TinNpiConnectorError("FHIR Organization scan proof is invalid")

    @property
    def matched_organization_count(self) -> int:
        """Return the number of Organizations in the matched terminal state."""

        return dict(self.state_counts)[FhirOrganizationEvidenceState.MATCHED.value]

    @property
    def matched_evidence_count(self) -> int:
        """Return evidence rows across every retained token policy."""

        return sum(count for _policy_id, count in self.matched_evidence_counts)

    def public_payload(self) -> dict[str, Any]:
        """Return the complete non-secret per-source completeness proof."""

        return {
            "dataset_id": self.dataset_id,
            "endpoint_id": self.endpoint_id,
            "identifier_rule_id": self.identifier_rule_id,
            "identifier_rule_sha256": self.identifier_rule_sha256,
            "matched_evidence_counts": dict(self.matched_evidence_counts),
            "matched_evidence_sha256": self.matched_evidence_sha256,
            "matched_organization_count": self.matched_organization_count,
            "organization_resource_count": self.organization_resource_count,
            "organization_resource_sha256": self.organization_resource_sha256,
            "source_id": self.source_id,
            "source_summary_sha256": self.source_summary_sha256,
            "state_counts": dict(self.state_counts),
        }


def canonical_fhir_evidence_set_digest(
    evidence: Iterable[FhirTinNpiEvidence],
) -> bytes:
    """Hash a complete evidence set by its immutable evidence identities."""

    if isinstance(evidence, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR evidence set is invalid")
    try:
        evidence_rows = tuple(evidence)
        evidence_ids = tuple(
            sorted(
                item.evidence_id
                for item in evidence_rows
                if type(item) is FhirTinNpiEvidence
            )
        )
    except TypeError:
        raise TinNpiConnectorError("FHIR evidence set is invalid") from None
    if len(evidence_ids) != len(evidence_rows) or len(set(evidence_ids)) != len(
        evidence_ids
    ):
        raise TinNpiConnectorError("FHIR evidence set is invalid")
    return hashlib.sha256(_EVIDENCE_SET_HASH_DOMAIN + b"".join(evidence_ids)).digest()


def canonical_fhir_organization_scan_proof_json(
    proofs: Iterable[FhirOrganizationScanProof],
) -> str:
    """Serialize the complete per-dataset proof under the frozen scan contract."""

    if isinstance(proofs, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR Organization scan proofs are invalid")
    try:
        canonical_proofs = tuple(proofs)
    except TypeError:
        raise TinNpiConnectorError(
            "FHIR Organization scan proofs are invalid"
        ) from None
    if any(type(proof) is not FhirOrganizationScanProof for proof in canonical_proofs):
        raise TinNpiConnectorError("FHIR Organization scan proofs are invalid")
    proof_keys = tuple(
        (proof.source_id, proof.endpoint_id, proof.dataset_id)
        for proof in canonical_proofs
    )
    if proof_keys != tuple(sorted(set(proof_keys))):
        raise TinNpiConnectorError(
            "FHIR Organization scan proofs are duplicated or unordered"
        )
    return json.dumps(
        {
            "contract_id": TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
            "datasets": [proof.public_payload() for proof in canonical_proofs],
            "organization_identity_contract_id": (
                TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_fhir_organization_scan_proof_digest(
    proofs: Iterable[FhirOrganizationScanProof],
) -> bytes:
    """Bind the full scan proof into the physical connector generation."""

    return hashlib.sha256(
        _SCAN_PROOF_HASH_DOMAIN
        + canonical_fhir_organization_scan_proof_json(proofs).encode("utf-8")
    ).digest()
