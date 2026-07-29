# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validated immutable connector generation model."""

from __future__ import annotations

import hmac
from dataclasses import dataclass

from process.tin_npi_connector_evidence import FhirTinNpiEvidence
from process.tin_npi_connector_lookup import (
    NpiTinLookupRow,
    TinNpiLookupRow,
    _factor_forward_rows,
    _forward_row_key,
    _generation_id,
    _lookup_digest,
)
from process.tin_npi_connector_scan import (
    FhirOrganizationScanProof,
    canonical_fhir_evidence_set_digest,
    canonical_fhir_organization_scan_proof_digest,
    canonical_fhir_organization_scan_proof_json,
)
from process.tin_npi_connector_source import (
    _canonical_source_ids,
    _source_bitmap,
    _strict_hash_hex,
    canonical_source_ordinal_map_digest,
    canonical_source_ordinal_map_json,
)
from process.tin_npi_connector_support import TinNpiConnectorError


@dataclass(frozen=True)
class CompactTinNpiGeneration:
    """Deterministic factored lookup payload ready for staged publication."""

    generation_id: str
    source_vector_id: str
    source_ordinal_map: tuple[str, ...]
    source_ordinal_map_digest: bytes
    scan_proofs: tuple[FhirOrganizationScanProof, ...]
    scan_proof_digest: bytes
    lookup_digest: bytes
    evidence_rows: tuple[FhirTinNpiEvidence, ...]
    forward_rows: tuple[TinNpiLookupRow, ...]
    reverse_rows: tuple[NpiTinLookupRow, ...]

    def __post_init__(self) -> None:
        """Validate row shapes, factored parity, proofs, and all digests."""

        _validate_generation_shapes(self)
        _validate_generation_order(self)
        _validate_generation_content(self)

    @property
    def source_ordinal_map_json(self) -> str:
        """Return the authenticated source-ordinal map as canonical JSON."""

        return canonical_source_ordinal_map_json(self.source_ordinal_map)

    @property
    def evidence_count(self) -> int:
        """Return the evidence count represented by all forward rows."""

        return sum(forward_row.evidence_count for forward_row in self.forward_rows)

    @property
    def organization_count(self) -> int:
        """Return every Organization covered by source completeness proofs."""

        return sum(proof.organization_resource_count for proof in self.scan_proofs)

    @property
    def matched_organization_count(self) -> int:
        """Return Organizations whose terminal state produced evidence."""

        return sum(proof.matched_organization_count for proof in self.scan_proofs)

    @property
    def scan_proof_canonical_json(self) -> str:
        """Return stable JSON for every source-specific scan proof."""

        return canonical_fhir_organization_scan_proof_json(self.scan_proofs)

    def _expected_source_policy_evidence_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        return {
            (proof.source_id, policy_id): count
            for proof in self.scan_proofs
            for policy_id, count in proof.matched_evidence_counts
        }

    def _observed_source_policy_evidence_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        observed_count_by_key = {
            (source_id, policy_id): 0
            for source_id, policy_id in self._expected_source_policy_evidence_counts()
        }
        for forward_row in self.forward_rows:
            if len(forward_row.source_evidence_counts) != len(self.source_ordinal_map):
                raise TinNpiConnectorError(
                    "compact connector source evidence counts are invalid"
                )
            for source_ordinal, evidence_count in enumerate(
                forward_row.source_evidence_counts
            ):
                source_id = self.source_ordinal_map[source_ordinal]
                expected_bit = bool(evidence_count)
                observed_bit = bool(
                    forward_row.source_bitmap[source_ordinal // 8]
                    & (1 << (source_ordinal % 8))
                )
                if expected_bit != observed_bit:
                    raise TinNpiConnectorError(
                        "compact connector source evidence counts are invalid"
                    )
                evidence_key = (source_id, forward_row.token.token_policy_id)
                observed_count_by_key[evidence_key] = (
                    observed_count_by_key.get(evidence_key, 0) + evidence_count
                )
        return observed_count_by_key


def _validate_generation_shapes(generation: CompactTinNpiGeneration) -> None:
    """Validate generation identities and the exact tuple element classes."""

    _strict_hash_hex(generation.generation_id, "connector generation ID")
    _strict_hash_hex(generation.source_vector_id, "connector source-vector ID")
    canonical_source_ids = _canonical_source_ids(generation.source_ordinal_map)
    is_invalid = (
        type(generation.source_ordinal_map) is not tuple
        or generation.source_ordinal_map != canonical_source_ids
        or type(generation.source_ordinal_map_digest) is not bytes
        or len(generation.source_ordinal_map_digest) != 32
        or type(generation.lookup_digest) is not bytes
        or len(generation.lookup_digest) != 32
        or type(generation.scan_proofs) is not tuple
        or any(
            type(proof) is not FhirOrganizationScanProof
            for proof in generation.scan_proofs
        )
        or type(generation.scan_proof_digest) is not bytes
        or len(generation.scan_proof_digest) != 32
        or type(generation.evidence_rows) is not tuple
        or any(
            type(evidence_row) is not FhirTinNpiEvidence
            for evidence_row in generation.evidence_rows
        )
        or type(generation.forward_rows) is not tuple
        or any(
            type(forward_row) is not TinNpiLookupRow
            for forward_row in generation.forward_rows
        )
        or type(generation.reverse_rows) is not tuple
        or any(
            type(reverse_row) is not NpiTinLookupRow
            for reverse_row in generation.reverse_rows
        )
    )
    if is_invalid:
        raise TinNpiConnectorError("compact connector generation is invalid")


def _validate_generation_order(generation: CompactTinNpiGeneration) -> None:
    """Require deterministic unique ordering for evidence and lookup rows."""

    evidence_ids = tuple(
        evidence_row.evidence_id for evidence_row in generation.evidence_rows
    )
    if evidence_ids != tuple(sorted(set(evidence_ids))):
        raise TinNpiConnectorError("compact connector evidence rows are invalid")
    forward_keys = tuple(
        _forward_row_key(forward_row) for forward_row in generation.forward_rows
    )
    if forward_keys != tuple(sorted(set(forward_keys))):
        raise TinNpiConnectorError("compact connector forward rows are invalid")
    reverse_npis = tuple(reverse_row.npi for reverse_row in generation.reverse_rows)
    if reverse_npis != tuple(sorted(set(reverse_npis))):
        raise TinNpiConnectorError("compact connector reverse rows are invalid")


def _expected_reverse_keys(
    generation: CompactTinNpiGeneration,
) -> set[tuple[object, ...]]:
    """Return reverse keys implied by every NPI in every forward row."""

    return {
        (npi, *_forward_row_key(forward_row))
        for forward_row in generation.forward_rows
        for npi in forward_row.npis
    }


def _actual_reverse_keys(
    generation: CompactTinNpiGeneration,
) -> set[tuple[object, ...]]:
    """Return reverse keys materialized in compact reverse rows."""

    return {
        (
            reverse_row.npi,
            reference.token.token_policy_id,
            reference.token.tin_hmac_sha256,
            reference.relationship_class,
        )
        for reverse_row in generation.reverse_rows
        for reference in reverse_row.tax_identities
    }


def _has_valid_evidence_scope(generation: CompactTinNpiGeneration) -> bool:
    """Return whether all evidence matches its source-specific scan proof."""

    proof_by_source_id = {proof.source_id: proof for proof in generation.scan_proofs}
    for evidence_row in generation.evidence_rows:
        proof = proof_by_source_id.get(evidence_row.source_id)
        if (
            proof is None
            or evidence_row.source_endpoint_id != proof.endpoint_id
            or evidence_row.source_dataset_id != proof.dataset_id
            or evidence_row.identifier_rule_id != proof.identifier_rule_id
            or evidence_row.identifier_rule_sha256 != proof.identifier_rule_sha256
        ):
            return False
    return True


def _has_valid_source_digests(generation: CompactTinNpiGeneration) -> bool:
    """Return whether scan proofs match the exact evidence set per source."""

    proof_digest_by_source = {
        proof.source_id: proof.matched_evidence_sha256
        for proof in generation.scan_proofs
    }
    evidence_digest_by_source = {
        source_id: canonical_fhir_evidence_set_digest(
            evidence_row
            for evidence_row in generation.evidence_rows
            if evidence_row.source_id == source_id
        ).hex()
        for source_id in generation.source_ordinal_map
    }
    return proof_digest_by_source == evidence_digest_by_source


def _has_valid_forward_source_bitmaps(
    generation: CompactTinNpiGeneration,
) -> bool:
    """Return whether each forward aggregate binds its exact source IDs."""

    return all(
        hmac.compare_digest(
            forward_row.source_bitmap,
            _source_bitmap(
                forward_row.source_ids,
                source_ordinal_map=generation.source_ordinal_map,
            ),
        )
        for forward_row in generation.forward_rows
    )


def _has_valid_generation_digests(
    generation: CompactTinNpiGeneration,
) -> bool:
    """Return whether map, lookup, scan, and generation digests all match."""

    expected_generation_id = _generation_id(
        source_vector_id=generation.source_vector_id,
        scan_proof_digest=generation.scan_proof_digest,
        lookup_digest=generation.lookup_digest,
    )
    return (
        hmac.compare_digest(
            generation.source_ordinal_map_digest,
            canonical_source_ordinal_map_digest(generation.source_ordinal_map),
        )
        and hmac.compare_digest(
            generation.lookup_digest,
            _lookup_digest(generation.forward_rows),
        )
        and hmac.compare_digest(
            generation.scan_proof_digest,
            canonical_fhir_organization_scan_proof_digest(generation.scan_proofs),
        )
        and generation.generation_id == expected_generation_id
    )


def _validate_generation_content(generation: CompactTinNpiGeneration) -> None:
    """Validate exact forward/reverse/evidence parity and all sealed digests."""

    evidence_policy_identities = {
        (
            evidence_row.identifier_policy_id,
            evidence_row.identifier_policy_sha256,
        )
        for evidence_row in generation.evidence_rows
    }
    is_consistent = (
        _expected_reverse_keys(generation) == _actual_reverse_keys(generation)
        and tuple(proof.source_id for proof in generation.scan_proofs)
        == generation.source_ordinal_map
        and len(evidence_policy_identities) <= 1
        and _has_valid_evidence_scope(generation)
        and len(generation.evidence_rows) == generation.evidence_count
        and generation.forward_rows
        == _factor_forward_rows(
            generation.evidence_rows,
            source_ordinal_map=generation.source_ordinal_map,
        )
        and _has_valid_source_digests(generation)
        and generation.evidence_count <= 0x7FFF_FFFF_FFFF_FFFF
        and _has_valid_forward_source_bitmaps(generation)
        and _has_valid_generation_digests(generation)
        and generation._observed_source_policy_evidence_counts()
        == generation._expected_source_policy_evidence_counts()
    )
    if not is_consistent:
        raise TinNpiConnectorError("compact connector generation is inconsistent")


def is_generation_reuse_compatible(
    incumbent: CompactTinNpiGeneration,
    candidate: CompactTinNpiGeneration,
) -> bool:
    """Reject nondeterministic content for an already-seen source vector."""

    if (
        type(incumbent) is not CompactTinNpiGeneration
        or type(candidate) is not CompactTinNpiGeneration
    ):
        raise TinNpiConnectorError("connector generation reuse input is invalid")
    if incumbent.source_vector_id != candidate.source_vector_id:
        return False
    if incumbent != candidate:
        raise TinNpiConnectorError("connector source vector produced different content")
    return True


assert_generation_reuse_compatible = is_generation_reuse_compatible


def _lookup_key(
    evidence: FhirTinNpiEvidence,
) -> tuple[str, bytes, bytes, str]:
    return (
        evidence.token.token_policy_id,
        evidence.token.tin_id_128,
        evidence.token.tin_hmac_sha256,
        evidence.relationship_class,
    )
