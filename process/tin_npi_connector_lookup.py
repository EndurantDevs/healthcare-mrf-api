# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact forward and reverse TIN-to-NPI lookup rows and digests."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence
from dataclasses import dataclass

from process.tin_npi_connector_evidence import FhirTinNpiEvidence
from process.tin_npi_connector_security import TinTaxIdentityToken
from process.tin_npi_connector_source import (
    _canonical_source_ids,
    _source_bitmap,
    _strict_hash_hex,
)
from process.tin_npi_connector_support import (
    _GENERATION_HASH_DOMAIN,
    _LOOKUP_BUCKET_HASH_DOMAIN,
    _LOOKUP_ROW_HASH_DOMAIN,
    _LOOKUP_SET_HASH_DOMAIN,
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    TinNpiConnectorError,
)
from process.tin_npi_connector_temporal import _normalize_npi


def _validate_forward_lookup_shape(forward_row: TinNpiLookupRow) -> tuple[int, int]:
    """Validate scalar/tuple fields and return source count and bitmap width."""

    if type(forward_row.token) is not TinTaxIdentityToken:
        raise TinNpiConnectorError("forward lookup token is invalid")
    if forward_row.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
        raise TinNpiConnectorError("forward lookup relationship is invalid")
    is_invalid_npi_set = (
        type(forward_row.npis) is not tuple
        or not forward_row.npis
        or forward_row.npis != tuple(sorted(set(forward_row.npis)))
        or any(
            type(npi) is not int or _normalize_npi(str(npi)) != npi
            for npi in forward_row.npis
        )
    )
    if is_invalid_npi_set:
        raise TinNpiConnectorError("forward lookup NPIs are invalid")
    if (
        type(forward_row.evidence_count) is not int
        or not len(forward_row.npis)
        <= forward_row.evidence_count
        <= 0x7FFF_FFFF_FFFF_FFFF
    ):
        raise TinNpiConnectorError("forward lookup evidence count is invalid")
    if forward_row.source_ids != _canonical_source_ids(forward_row.source_ids):
        raise TinNpiConnectorError("forward lookup source IDs are invalid")
    if type(forward_row.source_evidence_counts) is not tuple:
        raise TinNpiConnectorError("forward lookup source bitmap is invalid")
    source_count = len(forward_row.source_evidence_counts)
    bitmap_width = (source_count + 7) // 8
    is_invalid_bitmap_shape = (
        source_count <= 0
        or type(forward_row.source_bitmap) is not bytes
        or len(forward_row.source_bitmap) != bitmap_width
        or not any(forward_row.source_bitmap)
        or type(forward_row.npi_source_bitmap_matrix) is not bytes
        or len(forward_row.npi_source_bitmap_matrix)
        != len(forward_row.npis) * bitmap_width
        or any(
            type(count) is not int or count < 0
            for count in forward_row.source_evidence_counts
        )
        or sum(forward_row.source_evidence_counts) != forward_row.evidence_count
    )
    if is_invalid_bitmap_shape:
        raise TinNpiConnectorError("forward lookup source bitmap is invalid")
    return source_count, bitmap_width


def _summarize_source_matrix(
    forward_row: TinNpiLookupRow,
    *,
    source_count: int,
    bitmap_width: int,
) -> tuple[bytes, tuple[int, ...]]:
    """Validate every aligned NPI segment and summarize its source support."""

    aggregate_bitmap = bytearray(bitmap_width)
    npi_support_counts = [0] * source_count
    for npi_ordinal in range(len(forward_row.npis)):
        segment_start = npi_ordinal * bitmap_width
        source_segment = forward_row.npi_source_bitmap_matrix[
            slice(segment_start, segment_start + bitmap_width)
        ]
        if not any(source_segment):
            raise TinNpiConnectorError("forward lookup source bitmap is invalid")
        if source_count % 8 and source_segment[-1] >= 1 << (source_count % 8):
            raise TinNpiConnectorError("forward lookup source bitmap is invalid")
        for byte_ordinal, source_byte in enumerate(source_segment):
            aggregate_bitmap[byte_ordinal] |= source_byte
        for source_ordinal in range(source_count):
            if source_segment[source_ordinal // 8] & (1 << (source_ordinal % 8)):
                npi_support_counts[source_ordinal] += 1
    return bytes(aggregate_bitmap), tuple(npi_support_counts)


def _validate_source_matrix_aggregate(forward_row: TinNpiLookupRow) -> None:
    """Require aggregate bitmap/count parity with aligned NPI source segments."""

    source_count, bitmap_width = _validate_forward_lookup_shape(forward_row)
    aggregate_bitmap, npi_support_counts = _summarize_source_matrix(
        forward_row,
        source_count=source_count,
        bitmap_width=bitmap_width,
    )
    has_invalid_trailing_bits = source_count % 8 and forward_row.source_bitmap[
        -1
    ] >= 1 << (source_count % 8)
    has_invalid_counts = any(
        (evidence_count > 0)
        != bool(
            forward_row.source_bitmap[source_ordinal // 8] & (1 << (source_ordinal % 8))
        )
        or evidence_count < npi_support_counts[source_ordinal]
        for source_ordinal, evidence_count in enumerate(
            forward_row.source_evidence_counts
        )
    )
    if (
        aggregate_bitmap != forward_row.source_bitmap
        or has_invalid_trailing_bits
        or has_invalid_counts
    ):
        raise TinNpiConnectorError("forward lookup source bitmap is invalid")


@dataclass(frozen=True, repr=False)
class TinNpiLookupRow:
    """Compact forward row: one policy/token/relationship to an NPI array."""

    token: TinTaxIdentityToken
    relationship_class: str
    npis: tuple[int, ...]
    evidence_count: int
    source_ids: tuple[str, ...]
    source_bitmap: bytes
    npi_source_bitmap_matrix: bytes
    source_evidence_counts: tuple[int, ...]

    def __post_init__(self) -> None:
        """Validate token identity, NPI order, and aligned source provenance."""

        _validate_source_matrix_aggregate(self)

    def source_bitmap_for_npi(self, npi: int) -> bytes:
        """Return the fixed-width source segment aligned to one sorted NPI."""

        try:
            npi_ordinal = self.npis.index(npi)
        except ValueError:
            raise TinNpiConnectorError("forward lookup NPI is unavailable") from None
        bitmap_width = (len(self.source_evidence_counts) + 7) // 8
        segment_start = npi_ordinal * bitmap_width
        return self.npi_source_bitmap_matrix[
            slice(segment_start, segment_start + bitmap_width)
        ]

    def npis_supported_by_source_ordinal(
        self,
        source_ordinal: int,
    ) -> tuple[int, ...]:
        """Filter NPIs by one authenticated source-map ordinal."""

        source_count = len(self.source_evidence_counts)
        if type(source_ordinal) is not int or not 0 <= source_ordinal < source_count:
            raise TinNpiConnectorError("forward lookup source ordinal is invalid")
        return tuple(
            npi
            for npi in self.npis
            if self.source_bitmap_for_npi(npi)[source_ordinal // 8]
            & (1 << (source_ordinal % 8))
        )

    def __repr__(self) -> str:
        return (
            "<tin-npi-lookup-row "
            f"token_policy_id={self.token.token_policy_id!r} "
            f"relationship_class={self.relationship_class!r} "
            f"npi_count={len(self.npis)} token=<redacted>>"
        )


@dataclass(frozen=True, repr=False)
class NpiTinLookupReference:
    """One reverse reference from an NPI to a policy-scoped TIN token."""

    token: TinTaxIdentityToken
    relationship_class: str

    def __post_init__(self) -> None:
        if type(self.token) is not TinTaxIdentityToken:
            raise TinNpiConnectorError("reverse lookup token is invalid")
        if self.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
            raise TinNpiConnectorError("reverse lookup relationship is invalid")

    def __repr__(self) -> str:
        return (
            "<npi-tin-lookup-reference "
            f"token_policy_id={self.token.token_policy_id!r} token=<redacted>>"
        )


@dataclass(frozen=True)
class NpiTinLookupRow:
    """Compact reverse row used for refresh and evidence diagnostics."""

    npi: int
    tax_identities: tuple[NpiTinLookupReference, ...]

    def __post_init__(self) -> None:
        if (
            type(self.npi) is not int
            or _normalize_npi(str(self.npi)) != self.npi
            or type(self.tax_identities) is not tuple
            or not self.tax_identities
            or any(
                type(reference) is not NpiTinLookupReference
                for reference in self.tax_identities
            )
        ):
            raise TinNpiConnectorError("reverse lookup row is invalid")
        reference_keys = tuple(
            (
                reference.token.token_policy_id,
                reference.token.tin_hmac_sha256,
                reference.relationship_class,
            )
            for reference in self.tax_identities
        )
        if reference_keys != tuple(sorted(set(reference_keys))):
            raise TinNpiConnectorError("reverse lookup references are invalid")


def _forward_row_key(
    row: TinNpiLookupRow,
) -> tuple[str, bytes, str]:
    return (
        row.token.token_policy_id,
        row.token.tin_hmac_sha256,
        row.relationship_class,
    )


def _lookup_row_hash(row: TinNpiLookupRow) -> tuple[bytes, bytes, bytes]:
    policy_bytes = row.token.token_policy_id.encode("ascii")
    if (
        len(policy_bytes) > 0xFFFF
        or len(row.npis) > 0xFFFF_FFFF
        or len(row.source_bitmap) > 0xFFFF_FFFF
        or len(row.npi_source_bitmap_matrix) > 0xFFFF_FFFF
    ):
        raise TinNpiConnectorError("forward lookup row cannot be encoded")
    row_hash = hashlib.sha256(
        b"".join(
            (
                _LOOKUP_ROW_HASH_DOMAIN,
                struct.pack(">H", len(policy_bytes)),
                policy_bytes,
                row.token.tin_hmac_sha256,
                struct.pack(">I", len(row.npis)),
                *(struct.pack(">q", npi) for npi in row.npis),
                struct.pack(">q", row.evidence_count),
                struct.pack(">I", len(row.source_bitmap)),
                row.source_bitmap,
                struct.pack(">I", len(row.npi_source_bitmap_matrix)),
                row.npi_source_bitmap_matrix,
                struct.pack(">I", len(row.source_evidence_counts)),
                *(struct.pack(">q", count) for count in row.source_evidence_counts),
            )
        )
    ).digest()
    return policy_bytes, row.token.tin_hmac_sha256, row_hash


def _lookup_digest(rows: Sequence[TinNpiLookupRow]) -> bytes:
    buckets: list[list[tuple[bytes, bytes, bytes]]] = [[] for _ in range(256)]
    for row in rows:
        encoded_row = _lookup_row_hash(row)
        buckets[encoded_row[2][0]].append(encoded_row)
    bucket_hashes = b"".join(
        hashlib.sha256(
            _LOOKUP_BUCKET_HASH_DOMAIN
            + struct.pack(">H", bucket)
            + b"".join(
                row_hash
                for _, _, row_hash in sorted(
                    bucket_rows,
                    key=lambda encoded_row: (
                        encoded_row[0],
                        encoded_row[1],
                    ),
                )
            )
        ).digest()
        for bucket, bucket_rows in enumerate(buckets)
    )
    return hashlib.sha256(_LOOKUP_SET_HASH_DOMAIN + bucket_hashes).digest()


def _generation_id(
    *,
    source_vector_id: str,
    scan_proof_digest: bytes,
    lookup_digest: bytes,
) -> str:
    _strict_hash_hex(source_vector_id, "connector source-vector ID")
    if (
        type(scan_proof_digest) is not bytes
        or len(scan_proof_digest) != 32
        or type(lookup_digest) is not bytes
        or len(lookup_digest) != 32
    ):
        raise TinNpiConnectorError("connector generation digests are invalid")
    return hashlib.sha256(
        _GENERATION_HASH_DOMAIN
        + bytes.fromhex(source_vector_id)
        + scan_proof_digest
        + lookup_digest
    ).hexdigest()


def _evidence_lookup_key(
    evidence_row: FhirTinNpiEvidence,
) -> tuple[str, bytes, bytes, str]:
    """Return the full-HMAC-authoritative forward grouping key."""

    return (
        evidence_row.token.token_policy_id,
        evidence_row.token.tin_id_128,
        evidence_row.token.tin_hmac_sha256,
        evidence_row.relationship_class,
    )


def _group_evidence_rows(
    evidence_rows: Sequence[FhirTinNpiEvidence],
) -> dict[tuple[str, bytes, bytes, str], list[FhirTinNpiEvidence]]:
    """Group evidence without merging colliding 128-bit token prefixes."""

    evidence_by_lookup_key: dict[
        tuple[str, bytes, bytes, str],
        list[FhirTinNpiEvidence],
    ] = {}
    for evidence_row in evidence_rows:
        lookup_key = _evidence_lookup_key(evidence_row)
        evidence_by_lookup_key.setdefault(lookup_key, []).append(evidence_row)
    return evidence_by_lookup_key


def _source_support_for_evidence(
    evidence_rows: Sequence[FhirTinNpiEvidence],
    *,
    source_ordinal_by_id: dict[str, int],
    source_count: int,
) -> tuple[tuple[int, ...], dict[int, set[int]]]:
    """Return evidence counts and source ordinals aligned by NPI."""

    source_evidence_counts = [0] * source_count
    source_ordinals_by_npi: dict[int, set[int]] = {}
    for evidence_row in evidence_rows:
        try:
            source_ordinal = source_ordinal_by_id[evidence_row.source_id]
        except KeyError:
            raise TinNpiConnectorError(
                "connector evidence source is outside the ordinal map"
            ) from None
        source_evidence_counts[source_ordinal] += 1
        source_ordinals_by_npi.setdefault(evidence_row.npi, set()).add(source_ordinal)
    return tuple(source_evidence_counts), source_ordinals_by_npi


def _npi_source_matrix(
    npis: tuple[int, ...],
    *,
    source_ordinals_by_npi: dict[int, set[int]],
    source_ordinal_map: tuple[str, ...],
) -> bytes:
    """Encode fixed-width source bitmap segments aligned to sorted NPIs."""

    return b"".join(
        _source_bitmap(
            tuple(
                source_ordinal_map[source_ordinal]
                for source_ordinal in sorted(source_ordinals_by_npi[npi])
            ),
            source_ordinal_map=source_ordinal_map,
        )
        for npi in npis
    )


def _build_forward_row(
    lookup_key: tuple[str, bytes, bytes, str],
    evidence_rows: Sequence[FhirTinNpiEvidence],
    *,
    source_ordinal_map: tuple[str, ...],
    source_ordinal_by_id: dict[str, int],
) -> TinNpiLookupRow:
    """Build one deterministic compact row from one exact evidence group."""

    source_counts, source_ordinals_by_npi = _source_support_for_evidence(
        evidence_rows,
        source_ordinal_by_id=source_ordinal_by_id,
        source_count=len(source_ordinal_map),
    )
    npis = tuple(sorted(source_ordinals_by_npi))
    source_ids = tuple(
        source_id
        for source_id, evidence_count in zip(source_ordinal_map, source_counts)
        if evidence_count
    )
    return TinNpiLookupRow(
        token=evidence_rows[0].token,
        relationship_class=lookup_key[3],
        npis=npis,
        evidence_count=len(evidence_rows),
        source_ids=source_ids,
        source_bitmap=_source_bitmap(
            source_ids,
            source_ordinal_map=source_ordinal_map,
        ),
        npi_source_bitmap_matrix=_npi_source_matrix(
            npis,
            source_ordinals_by_npi=source_ordinals_by_npi,
            source_ordinal_map=source_ordinal_map,
        ),
        source_evidence_counts=source_counts,
    )


def _factor_forward_rows(
    evidence_rows: Sequence[FhirTinNpiEvidence],
    *,
    source_ordinal_map: tuple[str, ...],
) -> tuple[TinNpiLookupRow, ...]:
    """Derive the exact hot lookup projection from immutable evidence rows."""

    source_ordinal_by_id = {
        source_id: ordinal for ordinal, source_id in enumerate(source_ordinal_map)
    }
    evidence_by_lookup_key = _group_evidence_rows(evidence_rows)
    return tuple(
        _build_forward_row(
            lookup_key,
            evidence_by_lookup_key[lookup_key],
            source_ordinal_map=source_ordinal_map,
            source_ordinal_by_id=source_ordinal_by_id,
        )
        for lookup_key in sorted(evidence_by_lookup_key)
    )
