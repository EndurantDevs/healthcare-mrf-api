# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Snapshot-pinned inferred-taxonomy candidates for PTG V4 serving."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)


PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE = (
    "ptg2_v4_inferred_taxonomy_candidate"
)
PTG2_V4_NPI_TABLE = "ptg2_v4_npi_scope"
PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT = (
    "ptg2_v4_inferred_taxonomy_candidates_v3"
)
PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT = (
    "snapshot_npi_live_catalog_individual_v1"
)
PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT = "sorted_u32le_v1"
PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT = "pattern_sorted_u32le_v1"
PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION = "direct_v1"
PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION = "pattern_v1"
PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION = "observe_v1"
PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS = "observe_only"
PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON = (
    "candidate_cap_exceeded"
)
PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON = (
    "pattern_projection_cap_exceeded"
)

PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES = 37_000
PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_SETS = 6_600
PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_OCCURRENCES = 6_700
PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS = 131_072
PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_RETAINED_MEMBERSHIPS = 65_536
PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_PAGES = 256
PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BYTES = 4_194_304
PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BATCHES = 32

_RULE_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-rule:v1\x00"
_RULE_SET_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-rule-set:v1\x00"
_MEMBER_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-members:v1\x00"
_PATTERN_MEMBER_DIGEST_DOMAIN = (
    b"ptg2:v4:inferred-taxonomy-pattern-members:v2\x00"
)
_CATALOG_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-catalog:v1\x00"
_PROJECTION_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-projection:v3\x00"
_PATTERN_PAYLOAD_MAGIC = b"PTG4TXP2"
_PATTERN_PAYLOAD_VERSION = 1
_PATTERN_PAYLOAD_HEADER = struct.Struct("<8sIIQ")
_PATTERN_PAYLOAD_RECORD = struct.Struct("<II")


@dataclass(frozen=True)
class V4InferredTaxonomyCandidates:
    """One authenticated, snapshot-local candidate vector."""

    rule_digest: bytes
    catalog_digest: bytes
    member_digest: bytes
    member_count: int
    npi_keys: tuple[int, ...]
    catalog_contract: str = PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
    vector_format: str = PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
    representation: str = PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    pattern_count: int = 0
    pattern_member_count: int = 0
    pattern_member_bytes: int = 0
    pattern_member_digest: bytes = b""
    npi_keys_by_pattern: Mapping[int, tuple[int, ...]] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class V4InferredTaxonomyPublication:
    rule_count: int
    member_count: int
    packed_byte_count: int
    projection_digest: bytes
    manifest: Mapping[str, Any]
    pattern_count: int = 0
    pattern_member_count: int = 0
    pattern_member_bytes: int = 0
    observe_only_rule_count: int = 0


@dataclass(frozen=True)
class V4InferredTaxonomyProjectionRule:
    """One rule descriptor authenticated by the sealed projection manifest."""

    rule_digest: bytes
    catalog_digest: bytes
    member_digest: bytes
    member_count: int
    packed_byte_count: int
    representation: str
    pattern_count: int
    pattern_member_count: int
    pattern_member_bytes: int
    pattern_member_digest: bytes
    max_online_filtered_reverse_code_sets: int
    max_online_filtered_reverse_code_occurrences: int
    max_online_inferred_taxonomy_candidates: int
    max_online_candidate_pattern_projection_members: int
    max_online_inferred_taxonomy_retained_memberships: int
    max_online_inferred_taxonomy_graph_pages: int
    max_online_inferred_taxonomy_graph_bytes: int
    max_online_inferred_taxonomy_graph_batches: int


@dataclass(frozen=True)
class _CandidateEvidence:
    npi_key: int
    npi: int
    matched_taxonomy_codes: tuple[str, ...]


class _PatternProjectionCapExceeded(RuntimeError):
    """The authenticated pattern projection exceeded its sealed hot cap."""


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _digest_bytes(value: Any, *, label: str) -> bytes:
    try:
        normalized = bytes(value)
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            f"PTG V4 {label} is invalid"
        ) from exc
    if len(normalized) != 32:
        raise PTG2ManifestArtifactError(f"PTG V4 {label} is invalid")
    return normalized


def _projection_cap_values() -> dict[str, int]:
    """Return the sealed online bounds in their digest order."""

    return {
        "max_online_filtered_reverse_code_sets": (
            PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_SETS
        ),
        "max_online_filtered_reverse_code_occurrences": (
            PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_OCCURRENCES
        ),
        "max_online_inferred_taxonomy_candidates": (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES
        ),
        "max_online_candidate_pattern_projection_members": (
            PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS
        ),
        "max_online_inferred_taxonomy_retained_memberships": (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_RETAINED_MEMBERSHIPS
        ),
        "max_online_inferred_taxonomy_graph_pages": (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_PAGES
        ),
        "max_online_inferred_taxonomy_graph_bytes": (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BYTES
        ),
        "max_online_inferred_taxonomy_graph_batches": (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BATCHES
        ),
    }


def _update_projection_cap_digest(
    digest: Any,
    caps: Mapping[str, int],
) -> None:
    for field_name in _projection_cap_values():
        digest.update(int(caps[field_name]).to_bytes(8, "big"))


def inferred_provider_taxonomy_rule_digest(
    rule: InferredProviderTaxonomyRule,
) -> bytes:
    """Return the stable identity of one selection rule, not its display text."""

    ranges = tuple(
        sorted((int(start), int(end)) for start, end in rule.ranges)
    )
    taxonomy_codes = tuple(
        sorted({str(code).strip().upper() for code in rule.taxonomy_codes})
    )
    if (
        not ranges
        or any(start < 0 or end < start for start, end in ranges)
        or not taxonomy_codes
        or any(not code for code in taxonomy_codes)
    ):
        raise ValueError("inferred taxonomy rule is invalid")
    payload = json.dumps(
        {
            "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
            "individual_only": True,
            "ranges": ranges,
            "taxonomy_codes": taxonomy_codes,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(_RULE_DIGEST_DOMAIN + payload).digest()


def pack_inferred_taxonomy_npi_keys(npi_keys: Iterable[int]) -> bytes:
    """Pack one strict ascending snapshot-local key vector."""

    payload = bytearray()
    previous_key = -1
    for raw_npi_key in npi_keys:
        npi_key = int(raw_npi_key)
        if npi_key <= previous_key or npi_key > 0xFFFFFFFF:
            raise ValueError(
                "PTG V4 inferred-taxonomy NPI keys must be strict uint32 order"
            )
        payload.extend(struct.pack("<I", npi_key))
        previous_key = npi_key
    return bytes(payload)


def unpack_inferred_taxonomy_npi_keys(
    payload: bytes,
    *,
    member_count: int,
) -> tuple[int, ...]:
    """Decode one exact vector while rejecting count and ordering drift."""

    normalized_count = int(member_count)
    normalized_payload = bytes(payload)
    if normalized_count < 0 or len(normalized_payload) != normalized_count * 4:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate vector has an invalid size"
        )
    members = tuple(
        int(member[0]) for member in struct.iter_unpack("<I", normalized_payload)
    )
    if any(left >= right for left, right in zip(members, members[1:])):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate vector is not strict and ordered"
        )
    return members


def pack_inferred_taxonomy_pattern_npi_keys(
    npi_keys_by_pattern: Mapping[int, Iterable[int]],
) -> bytes:
    """Pack strict pattern postings into one deterministic compact payload."""

    if not isinstance(npi_keys_by_pattern, Mapping):
        raise ValueError("PTG V4 inferred-taxonomy pattern postings are invalid")
    if not npi_keys_by_pattern:
        return b""
    normalized_patterns: list[tuple[int, tuple[int, ...]]] = []
    seen_pattern_keys: set[int] = set()
    total_members = 0
    for raw_pattern_key, raw_npi_keys in npi_keys_by_pattern.items():
        if isinstance(raw_pattern_key, bool):
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern key is invalid"
            )
        try:
            pattern_key = int(raw_pattern_key)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern key is invalid"
            ) from exc
        if (
            pattern_key < 0
            or pattern_key > 0xFFFFFFFF
            or pattern_key in seen_pattern_keys
        ):
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern key is invalid"
            )
        seen_pattern_keys.add(pattern_key)
        npi_keys = tuple(raw_npi_keys)
        if not npi_keys or len(npi_keys) > 0xFFFFFFFF:
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern postings must be nonempty"
            )
        previous_npi_key = -1
        normalized_npi_keys: list[int] = []
        for raw_npi_key in npi_keys:
            if isinstance(raw_npi_key, bool):
                raise ValueError(
                    "PTG V4 inferred-taxonomy pattern NPI key is invalid"
                )
            try:
                npi_key = int(raw_npi_key)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "PTG V4 inferred-taxonomy pattern NPI key is invalid"
                ) from exc
            if npi_key <= previous_npi_key or npi_key > 0xFFFFFFFF:
                raise ValueError(
                    "PTG V4 inferred-taxonomy pattern NPI keys must be strict "
                    "uint32 order"
                )
            normalized_npi_keys.append(npi_key)
            previous_npi_key = npi_key
        normalized_patterns.append(
            (pattern_key, tuple(normalized_npi_keys))
        )
        total_members += len(normalized_npi_keys)
        if total_members > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern member count is invalid"
            )
    if len(normalized_patterns) > 0xFFFFFFFF:
        raise ValueError("PTG V4 inferred-taxonomy pattern count is invalid")
    normalized_patterns.sort(key=lambda item: item[0])
    payload = bytearray(
        _PATTERN_PAYLOAD_HEADER.pack(
            _PATTERN_PAYLOAD_MAGIC,
            _PATTERN_PAYLOAD_VERSION,
            len(normalized_patterns),
            total_members,
        )
    )
    for pattern_key, npi_keys in normalized_patterns:
        payload.extend(_PATTERN_PAYLOAD_RECORD.pack(pattern_key, len(npi_keys)))
        for npi_key in npi_keys:
            payload.extend(struct.pack("<I", npi_key))
    return bytes(payload)


def unpack_inferred_taxonomy_pattern_npi_keys(
    payload: bytes,
    *,
    pattern_count: int,
    pattern_member_count: int,
) -> dict[int, tuple[int, ...]]:
    """Decode exact pattern postings while rejecting structural drift."""

    normalized_payload = bytes(payload)
    normalized_pattern_count = int(pattern_count)
    normalized_member_count = int(pattern_member_count)
    if normalized_pattern_count < 0 or normalized_member_count < 0:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern counts are invalid"
        )
    if not normalized_payload:
        if normalized_pattern_count == 0 and normalized_member_count == 0:
            return {}
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload is missing"
        )
    if (
        normalized_pattern_count <= 0
        or normalized_member_count < normalized_pattern_count
        or len(normalized_payload) < _PATTERN_PAYLOAD_HEADER.size
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload has an invalid size"
        )
    magic, version, encoded_pattern_count, encoded_member_count = (
        _PATTERN_PAYLOAD_HEADER.unpack_from(normalized_payload)
    )
    if (
        magic != _PATTERN_PAYLOAD_MAGIC
        or version != _PATTERN_PAYLOAD_VERSION
        or encoded_pattern_count != normalized_pattern_count
        or encoded_member_count != normalized_member_count
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload header changed"
        )
    offset = _PATTERN_PAYLOAD_HEADER.size
    previous_pattern_key = -1
    observed_members = 0
    postings: dict[int, tuple[int, ...]] = {}
    for _index in range(normalized_pattern_count):
        record_end = offset + _PATTERN_PAYLOAD_RECORD.size
        if record_end > len(normalized_payload):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy pattern payload is truncated"
            )
        pattern_key, member_count = _PATTERN_PAYLOAD_RECORD.unpack_from(
            normalized_payload,
            offset,
        )
        offset = record_end
        if pattern_key <= previous_pattern_key or member_count <= 0:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy pattern postings are not strict"
            )
        member_end = offset + member_count * 4
        if member_end > len(normalized_payload):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy pattern payload is truncated"
            )
        npi_keys = tuple(
            member[0]
            for member in struct.iter_unpack(
                "<I",
                normalized_payload[offset:member_end],
            )
        )
        if any(
            left >= right for left, right in zip(npi_keys, npi_keys[1:])
        ):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy pattern NPI keys are not strict"
            )
        postings[pattern_key] = npi_keys
        previous_pattern_key = pattern_key
        observed_members += member_count
        offset = member_end
    if offset != len(normalized_payload) or observed_members != normalized_member_count:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload has trailing data"
        )
    return postings


def inferred_taxonomy_member_digest(
    rule_digest: bytes,
    *,
    member_count: int,
    payload: bytes,
) -> bytes:
    """Authenticate the rule-bound packed vector."""

    normalized_rule_digest = bytes(rule_digest)
    normalized_count = int(member_count)
    normalized_payload = bytes(payload)
    if len(normalized_rule_digest) != 32 or normalized_count < 0:
        raise ValueError("PTG V4 inferred-taxonomy member identity is invalid")
    if len(normalized_payload) != normalized_count * 4:
        raise ValueError("PTG V4 inferred-taxonomy member vector size changed")
    digest = hashlib.sha256()
    digest.update(_MEMBER_DIGEST_DOMAIN)
    digest.update(normalized_rule_digest)
    digest.update(normalized_count.to_bytes(8, "big"))
    digest.update(normalized_payload)
    return digest.digest()


def inferred_taxonomy_pattern_member_digest(
    rule_digest: bytes,
    *,
    representation: str,
    pattern_count: int,
    pattern_member_count: int,
    payload: bytes,
) -> bytes:
    """Authenticate one rule-bound direct or factored pattern projection."""

    normalized_rule_digest = bytes(rule_digest)
    normalized_representation = str(representation)
    normalized_pattern_count = int(pattern_count)
    normalized_member_count = int(pattern_member_count)
    normalized_payload = bytes(payload)
    if len(normalized_rule_digest) != 32:
        raise ValueError("PTG V4 inferred-taxonomy pattern identity is invalid")
    if normalized_representation not in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
    }:
        raise ValueError(
            "PTG V4 inferred-taxonomy pattern representation is invalid"
        )
    if normalized_representation in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
    }:
        if (
            normalized_pattern_count != 0
            or normalized_member_count != 0
            or normalized_payload
        ):
            raise ValueError(
                "PTG V4 direct inferred-taxonomy projection has patterns"
            )
    else:
        try:
            unpack_inferred_taxonomy_pattern_npi_keys(
                normalized_payload,
                pattern_count=normalized_pattern_count,
                pattern_member_count=normalized_member_count,
            )
        except PTG2ManifestArtifactError as exc:
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern payload is invalid"
            ) from exc
    encoded_representation = normalized_representation.encode("ascii")
    digest = hashlib.sha256()
    digest.update(_PATTERN_MEMBER_DIGEST_DOMAIN)
    digest.update(normalized_rule_digest)
    digest.update(len(encoded_representation).to_bytes(2, "big"))
    digest.update(encoded_representation)
    digest.update(normalized_pattern_count.to_bytes(8, "big"))
    digest.update(normalized_member_count.to_bytes(8, "big"))
    digest.update(len(normalized_payload).to_bytes(8, "big"))
    digest.update(normalized_payload)
    return digest.digest()


def _catalog_digest(
    rule_digest: bytes,
    evidence_rows: Sequence[_CandidateEvidence],
) -> bytes:
    """Pin the exact individual and taxonomy catalog evidence used at publish."""

    digest = hashlib.sha256()
    digest.update(_CATALOG_DIGEST_DOMAIN)
    contract = PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT.encode("ascii")
    digest.update(len(contract).to_bytes(2, "big"))
    digest.update(contract)
    digest.update(bytes(rule_digest))
    digest.update(len(evidence_rows).to_bytes(8, "big"))
    for row in evidence_rows:
        digest.update(int(row.npi_key).to_bytes(4, "big"))
        digest.update(int(row.npi).to_bytes(8, "big"))
        digest.update(len(row.matched_taxonomy_codes).to_bytes(2, "big"))
        for taxonomy_code in row.matched_taxonomy_codes:
            encoded_code = taxonomy_code.encode("ascii")
            digest.update(len(encoded_code).to_bytes(2, "big"))
            digest.update(encoded_code)
    return digest.digest()


def _normalized_rules(
    rules: Iterable[InferredProviderTaxonomyRule],
) -> tuple[tuple[bytes, InferredProviderTaxonomyRule, frozenset[str]], ...]:
    normalized: list[
        tuple[bytes, InferredProviderTaxonomyRule, frozenset[str]]
    ] = []
    seen_digests: set[bytes] = set()
    for rule in rules:
        rule_digest = inferred_provider_taxonomy_rule_digest(rule)
        if rule_digest in seen_digests:
            raise ValueError("inferred taxonomy rule digest is duplicated")
        seen_digests.add(rule_digest)
        normalized.append(
            (
                rule_digest,
                rule,
                frozenset(
                    str(code).strip().upper() for code in rule.taxonomy_codes
                ),
            )
        )
    if not normalized:
        raise ValueError("inferred taxonomy candidate publication needs rules")
    return tuple(sorted(normalized, key=lambda item: item[0]))


def inferred_provider_taxonomy_rule_set_digest(
    rules: Iterable[InferredProviderTaxonomyRule],
) -> bytes:
    """Bind publisher identity to the ordered semantic rule set."""

    normalized_rules = _normalized_rules(rules)
    return _rule_set_digest_from_digests(
        rule_digest for rule_digest, _rule, _codes in normalized_rules
    )


def _rule_set_digest_from_digests(rule_digests: Iterable[bytes]) -> bytes:
    """Authenticate one complete, strict semantic rule-digest set."""

    normalized_digests = tuple(sorted(bytes(value) for value in rule_digests))
    if (
        not normalized_digests
        or any(len(value) != 32 for value in normalized_digests)
        or any(
            left >= right
            for left, right in zip(
                normalized_digests,
                normalized_digests[1:],
            )
        )
    ):
        raise ValueError("inferred taxonomy rule digest set is invalid")
    digest = hashlib.sha256()
    digest.update(_RULE_SET_DIGEST_DOMAIN)
    digest.update(len(normalized_digests).to_bytes(4, "big"))
    for rule_digest in normalized_digests:
        digest.update(rule_digest)
    return digest.digest()


def _update_projection_rule_digest(
    digest: Any,
    rule_manifest: Mapping[str, Any],
) -> None:
    digest.update(b"\x01")
    representation = str(rule_manifest["representation"])
    encoded_representation = representation.encode("ascii")
    digest.update(bytes.fromhex(str(rule_manifest["rule_digest"])))
    digest.update(bytes.fromhex(str(rule_manifest["catalog_digest"])))
    digest.update(bytes.fromhex(str(rule_manifest["member_digest"])))
    digest.update(int(rule_manifest["member_count"]).to_bytes(8, "big"))
    digest.update(int(rule_manifest["packed_byte_count"]).to_bytes(8, "big"))
    digest.update(len(encoded_representation).to_bytes(2, "big"))
    digest.update(encoded_representation)
    digest.update(int(rule_manifest["pattern_count"]).to_bytes(8, "big"))
    digest.update(
        int(rule_manifest["pattern_member_count"]).to_bytes(8, "big")
    )
    digest.update(
        int(rule_manifest["pattern_member_bytes"]).to_bytes(8, "big")
    )
    digest.update(bytes.fromhex(str(rule_manifest["pattern_member_digest"])))


def _update_projection_observe_rule_digest(
    digest: Any,
    rule_manifest: Mapping[str, Any],
) -> None:
    """Bind one unsupported rule's exact cap witness into the projection."""

    digest.update(b"\x02")
    digest.update(bytes.fromhex(str(rule_manifest["rule_digest"])))
    digest.update(bytes.fromhex(str(rule_manifest["catalog_digest"])))
    digest.update(bytes.fromhex(str(rule_manifest["member_digest"])))
    digest.update(int(rule_manifest["member_count"]).to_bytes(8, "big"))
    digest.update(
        int(rule_manifest["observed_count_lower_bound"]).to_bytes(
            8,
            "big",
        )
    )
    digest.update(
        int(rule_manifest["packed_byte_count"]).to_bytes(8, "big")
    )
    for field_name in ("status", "reason", "representation"):
        encoded_value = str(rule_manifest[field_name]).encode("ascii")
        digest.update(len(encoded_value).to_bytes(2, "big"))
        digest.update(encoded_value)
    digest.update(bytes.fromhex(str(rule_manifest["pattern_member_digest"])))


def shape_v4_inferred_taxonomy_projection_manifest(
    rows: Sequence[Mapping[str, Any]],
    *,
    npi_count: int,
    pattern_count: int,
) -> dict[str, Any]:
    """Shape persisted V3 candidate and observe rows under root bounds."""

    caps = _projection_cap_values()
    if isinstance(npi_count, bool) or isinstance(pattern_count, bool):
        raise RuntimeError(
            "PTG V4 inferred-taxonomy dictionary bounds are invalid"
        )
    normalized_npi_count = int(npi_count)
    normalized_pattern_count = int(pattern_count)
    if (
        normalized_npi_count < 0
        or normalized_npi_count > 0x100000000
        or normalized_pattern_count < 0
        or normalized_pattern_count > 0x100000000
    ):
        raise RuntimeError(
            "PTG V4 inferred-taxonomy dictionary bounds are invalid"
        )
    rule_manifests: list[dict[str, Any]] = []
    observe_only_rule_manifests: list[dict[str, Any]] = []
    digest_entries: list[tuple[bytes, bool, dict[str, Any]]] = []
    total_members = 0
    total_packed_bytes = 0
    total_patterns = 0
    total_pattern_members = 0
    total_pattern_bytes = 0
    previous_rule_digest: bytes | None = None
    for raw_row in rows:
        rule_digest = bytes(raw_row["rule_digest"])
        catalog_digest = bytes(raw_row["catalog_digest"])
        member_digest = bytes(raw_row["member_digest"])
        member_count = int(raw_row["member_count"])
        payload = bytes(raw_row["member_keys"])
        representation = str(raw_row["representation"])
        observe_reason = raw_row.get("observe_reason")
        raw_observe_count = raw_row.get("observe_count_lower_bound")
        if isinstance(raw_observe_count, bool):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy observe witness is invalid"
            )
        observe_count_lower_bound = (
            None
            if raw_observe_count is None
            else int(raw_observe_count)
        )
        row_pattern_count = int(raw_row["pattern_count"])
        pattern_member_count = int(raw_row["pattern_member_count"])
        pattern_member_bytes = int(raw_row["pattern_member_bytes"])
        pattern_member_digest = bytes(raw_row["pattern_member_digest"])
        pattern_payload = bytes(raw_row["pattern_member_payload"])
        if (
            len(rule_digest) != 32
            or len(catalog_digest) != 32
            or len(member_digest) != 32
            or len(pattern_member_digest) != 32
            or member_count < 0
            or len(payload) != member_count * 4
            or row_pattern_count < 0
            or pattern_member_count < 0
            or pattern_member_bytes != len(pattern_payload)
            or (
                previous_rule_digest is not None
                and rule_digest <= previous_rule_digest
            )
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate manifest is invalid"
            )
        try:
            candidate_members = unpack_inferred_taxonomy_npi_keys(
                payload,
                member_count=member_count,
            )
            pattern_postings = unpack_inferred_taxonomy_pattern_npi_keys(
                pattern_payload,
                pattern_count=row_pattern_count,
                pattern_member_count=pattern_member_count,
            )
            expected_pattern_digest = inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=row_pattern_count,
                pattern_member_count=pattern_member_count,
                payload=pattern_payload,
            )
        except (PTG2ManifestArtifactError, ValueError) as exc:
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate manifest changed"
            ) from exc
        candidate_member_set = frozenset(candidate_members)
        posting_member_set = frozenset(
            npi_key
            for npi_keys in pattern_postings.values()
            for npi_key in npi_keys
        )
        is_pattern_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        )
        is_direct_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        )
        is_observe_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
        )
        if not (
            is_pattern_projection
            or is_direct_projection
            or is_observe_projection
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy projection representation is invalid"
            )
        if is_observe_projection:
            candidate_cap_observe = observe_reason == (
                PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
            )
            pattern_cap_observe = observe_reason == (
                PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
            )
            if (
                not (candidate_cap_observe or pattern_cap_observe)
                or observe_count_lower_bound is None
                or (
                    candidate_cap_observe
                    and (
                        member_count
                        != caps["max_online_inferred_taxonomy_candidates"] + 1
                        or observe_count_lower_bound
                        != caps[
                            "max_online_inferred_taxonomy_candidates"
                        ]
                        + 1
                    )
                )
                or (
                    pattern_cap_observe
                    and (
                        member_count
                        > caps["max_online_inferred_taxonomy_candidates"]
                        or observe_count_lower_bound
                        != caps[
                            "max_online_candidate_pattern_projection_members"
                        ]
                        + 1
                    )
                )
                or row_pattern_count != 0
                or pattern_member_count != 0
                or pattern_member_bytes != 0
            ):
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy observe witness is invalid"
                )
        elif (
            observe_reason is not None
            or observe_count_lower_bound is not None
            or
            member_count
            > caps["max_online_inferred_taxonomy_candidates"]
            or pattern_member_count
            > caps[
                "max_online_candidate_pattern_projection_members"
            ]
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy projection exceeds its online cap"
            )
        if any(
            npi_key >= normalized_npi_count for npi_key in candidate_members
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate exceeds its NPI root"
            )
        if any(
            pattern_key >= normalized_pattern_count
            for pattern_key in pattern_postings
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy pattern exceeds its pattern root"
            )
        if (
            is_pattern_projection
            and posting_member_set != candidate_member_set
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy pattern projection is incomplete"
            )
        if (
            not is_pattern_projection
            and not is_observe_projection
            and normalized_pattern_count > 0
            and candidate_member_set
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy pattern projection is missing"
            )
        if (
            raw_row.get("catalog_contract")
            != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
            or raw_row.get("vector_format")
            != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
            or inferred_taxonomy_member_digest(
                rule_digest,
                member_count=member_count,
                payload=payload,
            )
            != member_digest
            or expected_pattern_digest != pattern_member_digest
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate manifest changed"
            )
        if is_observe_projection:
            rule_manifest = {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": catalog_digest.hex(),
                "member_digest": member_digest.hex(),
                "member_count": member_count,
                "observed_count_lower_bound": observe_count_lower_bound,
                "packed_byte_count": len(payload),
                "status": PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
                "reason": observe_reason,
                "representation": representation,
                "pattern_member_digest": pattern_member_digest.hex(),
            }
            observe_only_rule_manifests.append(rule_manifest)
            digest_entries.append((rule_digest, True, rule_manifest))
        else:
            rule_manifest = {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": catalog_digest.hex(),
                "member_digest": member_digest.hex(),
                "member_count": member_count,
                "packed_byte_count": len(payload),
                "representation": representation,
                "pattern_count": row_pattern_count,
                "pattern_member_count": pattern_member_count,
                "pattern_member_bytes": pattern_member_bytes,
                "pattern_member_digest": pattern_member_digest.hex(),
            }
            rule_manifests.append(rule_manifest)
            digest_entries.append((rule_digest, False, rule_manifest))
        total_members += member_count
        total_packed_bytes += len(payload)
        total_patterns += row_pattern_count
        total_pattern_members += pattern_member_count
        total_pattern_bytes += pattern_member_bytes
        previous_rule_digest = rule_digest
    if not digest_entries:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy projection has no rule evidence"
        )
    rule_set_digest = _rule_set_digest_from_digests(
        rule_digest for rule_digest, _is_observe, _entry in digest_entries
    )
    digest = hashlib.sha256()
    digest.update(_PROJECTION_DIGEST_DOMAIN)
    _update_projection_cap_digest(digest, caps)
    digest.update(rule_set_digest)
    for _rule_digest, is_observe, rule_manifest in digest_entries:
        if is_observe:
            _update_projection_observe_rule_digest(digest, rule_manifest)
        else:
            _update_projection_rule_digest(digest, rule_manifest)
    projection_digest = digest.digest()
    return {
        "contract": PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT,
        "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "pattern_format": PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT,
        **caps,
        "rule_count": len(rule_manifests),
        "observe_only_rule_count": len(observe_only_rule_manifests),
        "member_count": total_members,
        "packed_byte_count": total_packed_bytes,
        "pattern_count": total_patterns,
        "pattern_member_count": total_pattern_members,
        "pattern_member_bytes": total_pattern_bytes,
        "rule_set_digest": rule_set_digest.hex(),
        "projection_digest": projection_digest.hex(),
        "rules": rule_manifests,
        "observe_only_rules": observe_only_rule_manifests,
    }


def _candidate_projection_manifest(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compatibility shaper for tests that construct self-bounded rows."""

    materialized_rows = tuple(rows)
    maximum_npi_key = -1
    maximum_pattern_key = -1
    for row in materialized_rows:
        candidate_members = unpack_inferred_taxonomy_npi_keys(
            bytes(row["member_keys"]),
            member_count=int(row["member_count"]),
        )
        pattern_postings = unpack_inferred_taxonomy_pattern_npi_keys(
            bytes(row["pattern_member_payload"]),
            pattern_count=int(row["pattern_count"]),
            pattern_member_count=int(row["pattern_member_count"]),
        )
        if candidate_members:
            maximum_npi_key = max(maximum_npi_key, candidate_members[-1])
        if pattern_postings:
            maximum_pattern_key = max(maximum_pattern_key, *pattern_postings)
    return shape_v4_inferred_taxonomy_projection_manifest(
        materialized_rows,
        npi_count=maximum_npi_key + 1,
        pattern_count=maximum_pattern_key + 1,
    )


def validate_v4_inferred_taxonomy_projection_manifest(
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and canonicalize one optional sealed projection descriptor."""

    if not isinstance(manifest, Mapping):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection manifest is invalid"
        )
    cap_fields = tuple(_projection_cap_values())
    aggregate_fields = (
        "rule_count",
        "observe_only_rule_count",
        "member_count",
        "packed_byte_count",
        "pattern_count",
        "pattern_member_count",
        "pattern_member_bytes",
    )
    required_fields = {
        "contract",
        "catalog_contract",
        "vector_format",
        "pattern_format",
        *cap_fields,
        *aggregate_fields,
        "rule_set_digest",
        "projection_digest",
        "rules",
        "observe_only_rules",
    }
    if set(manifest) != required_fields:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection manifest fields changed"
        )
    if (
        manifest.get("contract")
        != PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT
        or manifest.get("catalog_contract")
        != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        or manifest.get("vector_format")
        != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
        or manifest.get("pattern_format")
        != PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection contract is incompatible"
        )
    integer_fields = (*cap_fields, *aggregate_fields)
    try:
        integers = {
            field_name: int(manifest[field_name])
            for field_name in integer_fields
            if not isinstance(manifest[field_name], bool)
        }
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection counts are invalid"
        ) from exc
    if len(integers) != len(integer_fields):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection counts are invalid"
        )
    if (
        any(integers[field_name] <= 0 for field_name in cap_fields)
        or any(
            integers[field_name] < 0
            for field_name in aggregate_fields
        )
        or integers["rule_count"] + integers["observe_only_rule_count"] <= 0
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection bounds are invalid"
        )
    raw_rules = manifest.get("rules")
    raw_observe_rules = manifest.get("observe_only_rules")
    if not isinstance(raw_rules, (list, tuple)) or not isinstance(
        raw_observe_rules,
        (list, tuple),
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection rules are invalid"
        )
    caps = {field_name: integers[field_name] for field_name in cap_fields}
    canonical_rules: list[dict[str, Any]] = []
    canonical_observe_rules: list[dict[str, Any]] = []
    digest_entries: list[tuple[bytes, bool, dict[str, Any]]] = []
    total_members = 0
    total_packed_bytes = 0
    total_patterns = 0
    total_pattern_members = 0
    total_pattern_bytes = 0
    previous_rule_digest: bytes | None = None
    rule_fields = {
        "rule_digest",
        "catalog_digest",
        "member_digest",
        "member_count",
        "packed_byte_count",
        "representation",
        "pattern_count",
        "pattern_member_count",
        "pattern_member_bytes",
        "pattern_member_digest",
    }
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, Mapping) or set(raw_rule) != rule_fields:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy projection rule fields changed"
            )
        try:
            rule_digest = bytes.fromhex(str(raw_rule["rule_digest"]))
            catalog_digest = bytes.fromhex(str(raw_rule["catalog_digest"]))
            member_digest = bytes.fromhex(str(raw_rule["member_digest"]))
            pattern_member_digest = bytes.fromhex(
                str(raw_rule["pattern_member_digest"])
            )
            member_count = int(raw_rule["member_count"])
            packed_byte_count = int(raw_rule["packed_byte_count"])
            pattern_count = int(raw_rule["pattern_count"])
            pattern_member_count = int(raw_rule["pattern_member_count"])
            pattern_member_bytes = int(raw_rule["pattern_member_bytes"])
            representation = str(raw_rule["representation"])
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy projection rule is invalid"
            ) from exc
        integer_rule_fields = (
            "member_count",
            "packed_byte_count",
            "pattern_count",
            "pattern_member_count",
            "pattern_member_bytes",
        )
        direct_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        )
        pattern_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        )
        pattern_size = (
            _PATTERN_PAYLOAD_HEADER.size
            + pattern_count * _PATTERN_PAYLOAD_RECORD.size
            + pattern_member_count * 4
        )
        if (
            any(isinstance(raw_rule[field_name], bool) for field_name in integer_rule_fields)
            or len(rule_digest) != 32
            or len(catalog_digest) != 32
            or len(member_digest) != 32
            or len(pattern_member_digest) != 32
            or member_count < 0
            or packed_byte_count != member_count * 4
            or member_count > integers[
                "max_online_inferred_taxonomy_candidates"
            ]
            or pattern_count < 0
            or pattern_member_count < 0
            or pattern_member_count > integers[
                "max_online_candidate_pattern_projection_members"
            ]
            or pattern_member_bytes < 0
            or (
                direct_projection
                and any(
                    (pattern_count, pattern_member_count, pattern_member_bytes)
                )
            )
            or (
                pattern_projection
                and (
                    pattern_count <= 0
                    or pattern_member_count < pattern_count
                    or pattern_member_bytes != pattern_size
                )
            )
            or not (direct_projection or pattern_projection)
            or (
                previous_rule_digest is not None
                and rule_digest <= previous_rule_digest
            )
        ):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy projection rule is invalid"
            )
        canonical_rule = {
            "rule_digest": rule_digest.hex(),
            "catalog_digest": catalog_digest.hex(),
            "member_digest": member_digest.hex(),
            "member_count": member_count,
            "packed_byte_count": packed_byte_count,
            "representation": representation,
            "pattern_count": pattern_count,
            "pattern_member_count": pattern_member_count,
            "pattern_member_bytes": pattern_member_bytes,
            "pattern_member_digest": pattern_member_digest.hex(),
        }
        canonical_rules.append(canonical_rule)
        digest_entries.append((rule_digest, False, canonical_rule))
        total_members += member_count
        total_packed_bytes += packed_byte_count
        total_patterns += pattern_count
        total_pattern_members += pattern_member_count
        total_pattern_bytes += pattern_member_bytes
        previous_rule_digest = rule_digest
    previous_observe_rule_digest: bytes | None = None
    observe_rule_fields = {
        "rule_digest",
        "catalog_digest",
        "member_digest",
        "member_count",
        "observed_count_lower_bound",
        "packed_byte_count",
        "status",
        "reason",
        "representation",
        "pattern_member_digest",
    }
    for raw_rule in raw_observe_rules:
        if not isinstance(raw_rule, Mapping) or set(raw_rule) != observe_rule_fields:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy observe rule fields changed"
            )
        try:
            rule_digest = bytes.fromhex(str(raw_rule["rule_digest"]))
            catalog_digest = bytes.fromhex(str(raw_rule["catalog_digest"]))
            member_digest = bytes.fromhex(str(raw_rule["member_digest"]))
            pattern_member_digest = bytes.fromhex(
                str(raw_rule["pattern_member_digest"])
            )
            member_count = int(raw_rule["member_count"])
            observed_count_lower_bound = int(
                raw_rule["observed_count_lower_bound"]
            )
            packed_byte_count = int(raw_rule["packed_byte_count"])
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy observe rule is invalid"
            ) from exc
        expected_pattern_member_digest = (
            inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=(
                    PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
                ),
                pattern_count=0,
                pattern_member_count=0,
                payload=b"",
            )
            if len(rule_digest) == 32
            else b""
        )
        if (
            isinstance(raw_rule["member_count"], bool)
            or isinstance(raw_rule["observed_count_lower_bound"], bool)
            or isinstance(raw_rule["packed_byte_count"], bool)
            or len(rule_digest) != 32
            or len(catalog_digest) != 32
            or len(member_digest) != 32
            or len(pattern_member_digest) != 32
            or member_count < 0
            or packed_byte_count != member_count * 4
            or raw_rule.get("status")
            != PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS
            or not (
                (
                    raw_rule.get("reason")
                    == PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
                    and member_count
                    == integers["max_online_inferred_taxonomy_candidates"]
                    + 1
                    and observed_count_lower_bound
                    == integers["max_online_inferred_taxonomy_candidates"]
                    + 1
                )
                or (
                    raw_rule.get("reason")
                    == PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
                    and member_count
                    <= integers["max_online_inferred_taxonomy_candidates"]
                    and observed_count_lower_bound
                    == integers[
                        "max_online_candidate_pattern_projection_members"
                    ]
                    + 1
                )
            )
            or raw_rule.get("representation")
            != PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
            or pattern_member_digest != expected_pattern_member_digest
            or (
                previous_observe_rule_digest is not None
                and rule_digest <= previous_observe_rule_digest
            )
        ):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy observe rule is invalid"
            )
        canonical_rule = {
            "rule_digest": rule_digest.hex(),
            "catalog_digest": catalog_digest.hex(),
            "member_digest": member_digest.hex(),
            "member_count": member_count,
            "observed_count_lower_bound": observed_count_lower_bound,
            "packed_byte_count": packed_byte_count,
            "status": PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
            "reason": str(raw_rule["reason"]),
            "representation": (
                PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
            ),
            "pattern_member_digest": pattern_member_digest.hex(),
        }
        canonical_observe_rules.append(canonical_rule)
        digest_entries.append((rule_digest, True, canonical_rule))
        total_members += member_count
        total_packed_bytes += packed_byte_count
        previous_observe_rule_digest = rule_digest
    ordered_digest_entries = sorted(digest_entries, key=lambda item: item[0])
    if any(
        left[0] >= right[0]
        for left, right in zip(
            ordered_digest_entries,
            ordered_digest_entries[1:],
        )
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy rule status is duplicated"
        )
    try:
        raw_rule_set_digest = bytes.fromhex(
            str(manifest.get("rule_set_digest") or "")
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy rule-set digest is invalid"
        ) from exc
    rule_set_digest = _digest_bytes(
        raw_rule_set_digest,
        label="inferred-taxonomy rule-set digest",
    )
    try:
        expected_rule_set_digest = _rule_set_digest_from_digests(
            rule_digest
            for rule_digest, _is_observe, _entry in ordered_digest_entries
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy rule-set digest is invalid"
        ) from exc
    digest = hashlib.sha256()
    digest.update(_PROJECTION_DIGEST_DOMAIN)
    _update_projection_cap_digest(digest, caps)
    digest.update(rule_set_digest)
    for _rule_digest, is_observe, canonical_rule in ordered_digest_entries:
        if is_observe:
            _update_projection_observe_rule_digest(digest, canonical_rule)
        else:
            _update_projection_rule_digest(digest, canonical_rule)
    try:
        raw_projection_digest = bytes.fromhex(
            str(manifest.get("projection_digest") or "")
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection digest is invalid"
        ) from exc
    projection_digest = _digest_bytes(
        raw_projection_digest,
        label="inferred-taxonomy projection digest",
    )
    if (
        len(canonical_rules) != integers["rule_count"]
        or len(canonical_observe_rules)
        != integers["observe_only_rule_count"]
        or total_members != integers["member_count"]
        or total_packed_bytes != integers["packed_byte_count"]
        or total_patterns != integers["pattern_count"]
        or total_pattern_members != integers["pattern_member_count"]
        or total_pattern_bytes != integers["pattern_member_bytes"]
        or rule_set_digest != expected_rule_set_digest
        or digest.digest() != projection_digest
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection digest changed"
        )
    return {
        "contract": PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT,
        "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "pattern_format": PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT,
        **caps,
        "rule_count": len(canonical_rules),
        "observe_only_rule_count": len(canonical_observe_rules),
        "member_count": total_members,
        "packed_byte_count": total_packed_bytes,
        "pattern_count": total_patterns,
        "pattern_member_count": total_pattern_members,
        "pattern_member_bytes": total_pattern_bytes,
        "rule_set_digest": rule_set_digest.hex(),
        "projection_digest": projection_digest.hex(),
        "rules": canonical_rules,
        "observe_only_rules": canonical_observe_rules,
    }


def resolve_inferred_taxonomy_projection_rule_manifest(
    manifest: Mapping[str, Any],
    rule_digest: bytes,
) -> V4InferredTaxonomyProjectionRule | None:
    """Resolve an online rule or one explicit observe-only fallback."""

    canonical_manifest = validate_v4_inferred_taxonomy_projection_manifest(
        manifest
    )
    normalized_rule_digest = _digest_bytes(rule_digest, label="rule digest")
    for raw_rule in canonical_manifest["rules"]:
        if raw_rule["rule_digest"] == normalized_rule_digest.hex():
            return V4InferredTaxonomyProjectionRule(
                rule_digest=normalized_rule_digest,
                catalog_digest=bytes.fromhex(raw_rule["catalog_digest"]),
                member_digest=bytes.fromhex(raw_rule["member_digest"]),
                member_count=int(raw_rule["member_count"]),
                packed_byte_count=int(raw_rule["packed_byte_count"]),
                representation=str(raw_rule["representation"]),
                pattern_count=int(raw_rule["pattern_count"]),
                pattern_member_count=int(raw_rule["pattern_member_count"]),
                pattern_member_bytes=int(raw_rule["pattern_member_bytes"]),
                pattern_member_digest=bytes.fromhex(
                    raw_rule["pattern_member_digest"]
                ),
                max_online_filtered_reverse_code_sets=int(
                    canonical_manifest[
                        "max_online_filtered_reverse_code_sets"
                    ]
                ),
                max_online_filtered_reverse_code_occurrences=int(
                    canonical_manifest[
                        "max_online_filtered_reverse_code_occurrences"
                    ]
                ),
                max_online_inferred_taxonomy_candidates=int(
                    canonical_manifest[
                        "max_online_inferred_taxonomy_candidates"
                    ]
                ),
                max_online_candidate_pattern_projection_members=int(
                    canonical_manifest[
                        "max_online_candidate_pattern_projection_members"
                    ]
                ),
                max_online_inferred_taxonomy_retained_memberships=int(
                    canonical_manifest[
                        "max_online_inferred_taxonomy_retained_memberships"
                    ]
                ),
                max_online_inferred_taxonomy_graph_pages=int(
                    canonical_manifest[
                        "max_online_inferred_taxonomy_graph_pages"
                    ]
                ),
                max_online_inferred_taxonomy_graph_bytes=int(
                    canonical_manifest[
                        "max_online_inferred_taxonomy_graph_bytes"
                    ]
                ),
                max_online_inferred_taxonomy_graph_batches=int(
                    canonical_manifest[
                        "max_online_inferred_taxonomy_graph_batches"
                    ]
                ),
            )
    if any(
        raw_rule["rule_digest"] == normalized_rule_digest.hex()
        for raw_rule in canonical_manifest["observe_only_rules"]
    ):
        return None
    raise PTG2ManifestArtifactError(
        "PTG V4 inferred-taxonomy rule is not in the sealed projection"
    )


def inferred_taxonomy_projection_rule_manifest(
    manifest: Mapping[str, Any],
    rule_digest: bytes,
) -> V4InferredTaxonomyProjectionRule:
    """Return one authenticated online rule descriptor and its limits."""

    projection_rule = resolve_inferred_taxonomy_projection_rule_manifest(
        manifest,
        rule_digest,
    )
    if projection_rule is None:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy rule is observe-only"
        )
    return projection_rule


def _normalized_root_identity(
    representation: str,
    pattern_count: int,
) -> tuple[str, int]:
    normalized_representation = str(representation)
    if isinstance(pattern_count, bool):
        raise ValueError("PTG V4 inferred-taxonomy root identity is invalid")
    normalized_pattern_count = int(pattern_count)
    if (
        normalized_representation
        == PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        and normalized_pattern_count == 0
    ):
        return normalized_representation, normalized_pattern_count
    if (
        normalized_representation
        == PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        and 0 < normalized_pattern_count <= 0xFFFFFFFF
    ):
        return normalized_representation, normalized_pattern_count
    raise ValueError("PTG V4 inferred-taxonomy root identity is invalid")


async def _candidate_pattern_postings_for_rule(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    candidate_npi_keys: tuple[int, ...],
    root_pattern_count: int,
) -> dict[int, tuple[int, ...]]:
    """Project one bounded rule through the authenticated building graph."""

    if not candidate_npi_keys:
        return {}
    # Local import avoids loading serving-facing graph code while the sidecar
    # manifest is imported by snapshot sealing.
    from api.ptg2_shared_blocks import PTG2SharedBlockError
    from api.ptg2_v4_graph import lookup_building_v4_relation_members

    try:
        npi_patterns = await lookup_building_v4_relation_members(
            session,
            snapshot_key=int(snapshot_key),
            relation="npi_patterns",
            owner_keys=candidate_npi_keys,
            schema_name=schema_name,
            build_token=build_token,
            max_members=(
                PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS
            ),
        )
    except PTG2SharedBlockError as exc:
        if str(exc) != "PTG V4 graph selection exceeds max_members":
            raise
        raise _PatternProjectionCapExceeded(
            "PTG V4 inferred-taxonomy pattern projection exceeds the online cap"
        ) from exc
    if set(npi_patterns) != set(candidate_npi_keys):
        raise RuntimeError(
            "PTG V4 inferred-taxonomy pattern projection is incomplete"
        )
    npi_keys_by_pattern: dict[int, list[int]] = {}
    observed_member_count = 0
    for npi_key in candidate_npi_keys:
        previous_pattern_key = -1
        for raw_pattern_key in npi_patterns[npi_key]:
            if isinstance(raw_pattern_key, bool):
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy pattern key is invalid"
                )
            pattern_key = int(raw_pattern_key)
            if (
                pattern_key <= previous_pattern_key
                or pattern_key < 0
                or pattern_key >= root_pattern_count
            ):
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy pattern key is outside its root"
                )
            npi_keys_by_pattern.setdefault(pattern_key, []).append(npi_key)
            previous_pattern_key = pattern_key
            observed_member_count += 1
            if (
                observed_member_count
                > PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS
            ):
                raise _PatternProjectionCapExceeded(
                    "PTG V4 inferred-taxonomy pattern projection exceeds the "
                    "online cap"
                )
    projected_npi_keys = frozenset(
        npi_key
        for pattern_npi_keys in npi_keys_by_pattern.values()
        for npi_key in pattern_npi_keys
    )
    if projected_npi_keys != frozenset(candidate_npi_keys):
        raise RuntimeError(
            "PTG V4 inferred-taxonomy pattern evidence is incomplete"
        )
    return {
        pattern_key: tuple(npi_keys)
        for pattern_key, npi_keys in sorted(npi_keys_by_pattern.items())
    }


async def publish_v4_inferred_taxonomy_candidates(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    rules: Iterable[InferredProviderTaxonomyRule],
    npi_count: int,
    representation: str,
    pattern_count: int,
) -> V4InferredTaxonomyPublication:
    """Freeze individual-only rule candidates after the V4 NPI dictionary."""

    normalized_rules = _normalized_rules(rules)
    root_representation, root_pattern_count = _normalized_root_identity(
        representation,
        pattern_count,
    )
    if isinstance(npi_count, bool):
        raise ValueError("PTG V4 inferred-taxonomy NPI count is invalid")
    normalized_npi_count = int(npi_count)
    if not 0 <= normalized_npi_count <= 0x100000000:
        raise ValueError("PTG V4 inferred-taxonomy NPI count is invalid")
    # Local import avoids a module cycle: snapshot-map sealing imports the
    # projection summarizer below to bind these rows into layout reuse.
    from process.ptg_parts.ptg2_v4_snapshot_maps import (
        lock_v4_shared_layout_for_map_write,
    )

    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    schema = _quote_ident(schema_name)
    evidence_by_rule_digest: dict[bytes, list[_CandidateEvidence]] = {
        rule_digest: [] for rule_digest, _rule, _codes in normalized_rules
    }
    for rule_digest, _rule, rule_codes in normalized_rules:
        catalog_result = await session.execute(
            text(
                f"""
                SELECT scoped.npi_key,
                       scoped.npi,
                       ARRAY_AGG(
                           DISTINCT taxonomy.healthcare_provider_taxonomy_code
                           ORDER BY taxonomy.healthcare_provider_taxonomy_code
                       ) AS matched_taxonomy_codes
                  FROM {schema}.{PTG2_V4_NPI_TABLE} AS scoped
                  JOIN {schema}.npi AS entity
                    ON entity.npi = scoped.npi
                   AND COALESCE(entity.entity_type_code, 0) = 1
                  JOIN {schema}.npi_taxonomy AS taxonomy
                    ON taxonomy.npi = scoped.npi
                 WHERE scoped.snapshot_key = :snapshot_key
                   AND taxonomy.healthcare_provider_taxonomy_code = ANY(
                       CAST(:taxonomy_codes AS varchar[])
                   )
                 GROUP BY scoped.npi_key, scoped.npi
                 ORDER BY scoped.npi_key
                 LIMIT :candidate_limit
                """
            ),
            {
                "snapshot_key": int(snapshot_key),
                "taxonomy_codes": tuple(sorted(rule_codes)),
                "candidate_limit": (
                    PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
                ),
            },
        )
        catalog_rows = tuple(_row_mapping(row) for row in catalog_result)
        for catalog_row in catalog_rows:
            npi_key = int(catalog_row["npi_key"])
            npi = int(catalog_row["npi"])
            if (
                npi_key < 0
                or npi_key > 0xFFFFFFFF
                or npi_key >= normalized_npi_count
            ):
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy NPI key is invalid"
                )
            if npi < 1_000_000_000 or npi > 9_999_999_999:
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy NPI is invalid"
                )
            matched_codes = frozenset(
                str(code).strip().upper()
                for code in (
                    catalog_row.get("matched_taxonomy_codes") or ()
                )
            )
            selected_codes = tuple(sorted(matched_codes & rule_codes))
            if not selected_codes:
                raise RuntimeError(
                    "PTG V4 inferred-taxonomy catalog evidence changed"
                )
            evidence_by_rule_digest[rule_digest].append(
                _CandidateEvidence(npi_key, npi, selected_codes)
            )

    expected_rows: list[dict[str, Any]] = []
    for rule_digest, _rule, _rule_codes in normalized_rules:
        evidence_rows = tuple(evidence_by_rule_digest[rule_digest])
        npi_keys = tuple(row.npi_key for row in evidence_rows)
        candidate_cap_exceeded = (
            len(npi_keys)
            > PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES
        )
        observe_reason = (
            PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
            if candidate_cap_exceeded
            else None
        )
        observe_count_lower_bound = (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
            if candidate_cap_exceeded
            else None
        )
        payload = pack_inferred_taxonomy_npi_keys(npi_keys)
        pattern_postings: dict[int, tuple[int, ...]] = {}
        if (
            observe_reason is None
            and root_representation
            == PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
            and npi_keys
        ):
            try:
                pattern_postings = await _candidate_pattern_postings_for_rule(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(snapshot_key),
                    build_token=build_token,
                    candidate_npi_keys=npi_keys,
                    root_pattern_count=root_pattern_count,
                )
            except _PatternProjectionCapExceeded:
                observe_reason = (
                    PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
                )
                observe_count_lower_bound = (
                    PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS
                    + 1
                )
        observed_pattern_members = sum(
            len(pattern_npi_keys)
            for pattern_npi_keys in pattern_postings.values()
        )
        if (
            observe_reason is None
            and observed_pattern_members
            > PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS
        ):
            observe_reason = PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
            observe_count_lower_bound = (
                PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS + 1
            )
        observe_only = observe_reason is not None
        if observe_only:
            pattern_postings = {}
        pattern_payload = pack_inferred_taxonomy_pattern_npi_keys(
            pattern_postings
        )
        if observe_only:
            representation = (
                PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
            )
            pattern_count = 0
            pattern_member_count = 0
        elif pattern_payload:
            representation = (
                PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
            )
            pattern_count = len(pattern_postings)
            pattern_postings = unpack_inferred_taxonomy_pattern_npi_keys(
                pattern_payload,
                pattern_count=pattern_count,
                pattern_member_count=observed_pattern_members,
            )
            pattern_member_count = observed_pattern_members
        else:
            representation = (
                PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
            )
            pattern_count = 0
            pattern_member_count = 0
            pattern_postings = {}
        candidate_member_set = frozenset(npi_keys)
        if any(
            npi_key not in candidate_member_set
            for pattern_npi_keys in pattern_postings.values()
            for npi_key in pattern_npi_keys
        ):
            raise ValueError(
                "PTG V4 inferred-taxonomy pattern projection escaped its "
                "candidate vector"
            )
        pattern_member_digest = inferred_taxonomy_pattern_member_digest(
            rule_digest,
            representation=representation,
            pattern_count=pattern_count,
            pattern_member_count=pattern_member_count,
            payload=pattern_payload,
        )
        expected_rows.append(
            {
                "snapshot_key": int(snapshot_key),
                "rule_digest": rule_digest,
                "catalog_contract": (
                    PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
                ),
                "catalog_digest": _catalog_digest(rule_digest, evidence_rows),
                "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
                "member_count": len(npi_keys),
                "member_digest": inferred_taxonomy_member_digest(
                    rule_digest,
                    member_count=len(npi_keys),
                    payload=payload,
                ),
                "member_keys": payload,
                "representation": representation,
                "observe_reason": observe_reason,
                "observe_count_lower_bound": observe_count_lower_bound,
                "pattern_count": pattern_count,
                "pattern_member_count": pattern_member_count,
                "pattern_member_bytes": len(pattern_payload),
                "pattern_member_digest": pattern_member_digest,
                "pattern_member_payload": pattern_payload,
            }
        )
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{table} (
                snapshot_key,
                rule_digest,
                catalog_contract,
                catalog_digest,
                vector_format,
                member_count,
                member_digest,
                member_keys,
                representation,
                observe_reason,
                observe_count_lower_bound,
                pattern_count,
                pattern_member_count,
                pattern_member_bytes,
                pattern_member_digest,
                pattern_member_payload
            ) VALUES (
                :snapshot_key,
                :rule_digest,
                :catalog_contract,
                :catalog_digest,
                :vector_format,
                :member_count,
                :member_digest,
                :member_keys,
                :representation,
                :observe_reason,
                :observe_count_lower_bound,
                :pattern_count,
                :pattern_member_count,
                :pattern_member_bytes,
                :pattern_member_digest,
                :pattern_member_payload
            )
            ON CONFLICT DO NOTHING
            """
        ),
        expected_rows,
    )
    stored_result = await session.execute(
        text(
            f"""
            SELECT rule_digest,
                   catalog_contract,
                   catalog_digest,
                   vector_format,
                   member_count,
                   member_digest,
                   member_keys,
                   representation,
                   observe_reason,
                   observe_count_lower_bound,
                   pattern_count,
                   pattern_member_count,
                   pattern_member_bytes,
                   pattern_member_digest,
                   pattern_member_payload
              FROM {schema}.{table}
             WHERE snapshot_key = :snapshot_key
             ORDER BY rule_digest
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    stored_rows = tuple(_row_mapping(row) for row in stored_result)
    comparable_fields = (
        "rule_digest",
        "catalog_contract",
        "catalog_digest",
        "vector_format",
        "member_count",
        "member_digest",
        "member_keys",
        "representation",
        "observe_reason",
        "observe_count_lower_bound",
        "pattern_count",
        "pattern_member_count",
        "pattern_member_bytes",
        "pattern_member_digest",
        "pattern_member_payload",
    )
    normalized_stored_rows = tuple(
        {
            field_name: (
                bytes(row[field_name])
                if field_name.endswith("digest")
                or field_name in {"member_keys", "pattern_member_payload"}
                else row[field_name]
            )
            for field_name in comparable_fields
        }
        for row in stored_rows
    )
    normalized_expected_rows = tuple(
        {
            field_name: row[field_name]
            for field_name in comparable_fields
        }
        for row in expected_rows
    )
    if normalized_stored_rows != normalized_expected_rows:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy candidate publication changed"
        )
    manifest = shape_v4_inferred_taxonomy_projection_manifest(
        normalized_stored_rows,
        npi_count=normalized_npi_count,
        pattern_count=root_pattern_count,
    )
    return V4InferredTaxonomyPublication(
        rule_count=int(manifest["rule_count"]),
        member_count=int(manifest["member_count"]),
        packed_byte_count=int(manifest["packed_byte_count"]),
        projection_digest=bytes.fromhex(str(manifest["projection_digest"])),
        manifest=manifest,
        pattern_count=int(manifest["pattern_count"]),
        pattern_member_count=int(manifest["pattern_member_count"]),
        pattern_member_bytes=int(manifest["pattern_member_bytes"]),
        observe_only_rule_count=int(manifest["observe_only_rule_count"]),
    )


async def summarize_v4_inferred_taxonomy_candidates(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    npi_count: int,
    pattern_count: int,
    rules: Iterable[InferredProviderTaxonomyRule],
) -> dict[str, Any]:
    """Re-authenticate all rule vectors immediately before V4 seal."""

    normalized_rules = _normalized_rules(rules)
    expected_rule_digests = tuple(item[0] for item in normalized_rules)
    schema = _quote_ident(schema_name)
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)
    result = await session.execute(
        text(
            f"""
            SELECT rule_digest,
                   catalog_contract,
                   catalog_digest,
                   vector_format,
                   member_count,
                   member_digest,
                   member_keys,
                   representation,
                   observe_reason,
                   observe_count_lower_bound,
                   pattern_count,
                   pattern_member_count,
                   pattern_member_bytes,
                   pattern_member_digest,
                   pattern_member_payload
              FROM {schema}.{table}
             WHERE snapshot_key = :snapshot_key
             ORDER BY rule_digest
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    rows = tuple(_row_mapping(row) for row in result)
    observed_rule_digests = tuple(bytes(row["rule_digest"]) for row in rows)
    if observed_rule_digests != expected_rule_digests:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy candidate rules are incomplete"
        )
    return shape_v4_inferred_taxonomy_projection_manifest(
        rows,
        npi_count=npi_count,
        pattern_count=pattern_count,
    )


async def load_v4_inferred_taxonomy_candidates(
    session: Any,
    *,
    snapshot_key: int,
    rule_digest: bytes,
    schema_name: str,
    projection_manifest: Mapping[str, Any],
) -> V4InferredTaxonomyCandidates:
    """Load one sealed rule vector under its projection-authenticated cap."""

    projection_rule = inferred_taxonomy_projection_rule_manifest(
        projection_manifest,
        rule_digest,
    )
    normalized_rule_digest = projection_rule.rule_digest
    if (
        projection_rule.member_count
        > projection_rule.max_online_inferred_taxonomy_candidates
        or projection_rule.pattern_member_count
        > projection_rule.max_online_candidate_pattern_projection_members
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection exceeds the sealed online cap"
        )
    schema = _quote_ident(schema_name)
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)
    metadata_result = await session.execute(
        text(
            f"""
            SELECT candidate.catalog_contract,
                   candidate.catalog_digest,
                   candidate.vector_format,
                   candidate.member_count,
                   candidate.member_digest,
                   candidate.member_keys,
                   OCTET_LENGTH(candidate.member_keys) AS member_bytes,
                   candidate.representation,
                   candidate.pattern_count,
                   candidate.pattern_member_count,
                   candidate.pattern_member_bytes,
                   candidate.pattern_member_digest,
                   candidate.pattern_member_payload,
                   OCTET_LENGTH(candidate.pattern_member_payload)
                       AS pattern_payload_bytes,
                   root.state AS root_state,
                   root.npi_count,
                   root.pattern_count AS root_pattern_count
              FROM {schema}.{table} AS candidate
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = candidate.snapshot_key
             WHERE candidate.snapshot_key = :snapshot_key
               AND candidate.rule_digest = :rule_digest
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "rule_digest": normalized_rule_digest,
        },
    )
    metadata_rows = tuple(_row_mapping(row) for row in metadata_result)
    if len(metadata_rows) != 1:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate vector is unavailable"
        )
    metadata = metadata_rows[0]
    if metadata.get("root_state") != "complete":
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate snapshot is not complete"
        )
    if (
        metadata.get("catalog_contract")
        != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        or metadata.get("vector_format")
        != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate contract is incompatible"
        )
    member_count = int(metadata.get("member_count") or 0)
    member_bytes = int(metadata.get("member_bytes") or 0)
    representation = str(metadata.get("representation") or "")
    pattern_count = int(metadata.get("pattern_count") or 0)
    pattern_member_count = int(metadata.get("pattern_member_count") or 0)
    pattern_member_bytes = int(metadata.get("pattern_member_bytes") or 0)
    pattern_payload_bytes = int(metadata.get("pattern_payload_bytes") or 0)
    npi_count = int(metadata.get("npi_count") or 0)
    root_pattern_count = int(metadata.get("root_pattern_count") or 0)
    if (
        member_count < 0
        or member_bytes != member_count * 4
        or pattern_count < 0
        or pattern_member_count < 0
        or pattern_member_bytes < 0
        or pattern_payload_bytes != pattern_member_bytes
        or npi_count < member_count
        or root_pattern_count < 0
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate metadata is inconsistent"
        )
    catalog_digest = _digest_bytes(
        metadata.get("catalog_digest"), label="catalog digest"
    )
    member_digest = _digest_bytes(
        metadata.get("member_digest"), label="member digest"
    )
    pattern_member_digest = _digest_bytes(
        metadata.get("pattern_member_digest"),
        label="pattern member digest",
    )
    if (
        member_count != projection_rule.member_count
        or member_bytes != projection_rule.packed_byte_count
        or catalog_digest != projection_rule.catalog_digest
        or member_digest != projection_rule.member_digest
        or representation != projection_rule.representation
        or pattern_count != projection_rule.pattern_count
        or pattern_member_count != projection_rule.pattern_member_count
        or pattern_member_bytes != projection_rule.pattern_member_bytes
        or pattern_member_digest != projection_rule.pattern_member_digest
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate metadata changed from its seal"
        )
    payload = bytes(metadata.get("member_keys") or b"")
    try:
        observed_member_digest = inferred_taxonomy_member_digest(
            normalized_rule_digest,
            member_count=member_count,
            payload=payload,
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate payload is inconsistent"
        ) from exc
    if observed_member_digest != member_digest:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate digest changed"
        )
    npi_keys = unpack_inferred_taxonomy_npi_keys(
        payload,
        member_count=member_count,
    )
    pattern_payload = bytes(metadata.get("pattern_member_payload") or b"")
    try:
        observed_pattern_member_digest = (
            inferred_taxonomy_pattern_member_digest(
                normalized_rule_digest,
                representation=representation,
                pattern_count=pattern_count,
                pattern_member_count=pattern_member_count,
                payload=pattern_payload,
            )
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload is inconsistent"
        ) from exc
    if observed_pattern_member_digest != pattern_member_digest:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern digest changed"
        )
    npi_keys_by_pattern = unpack_inferred_taxonomy_pattern_npi_keys(
        pattern_payload,
        pattern_count=pattern_count,
        pattern_member_count=pattern_member_count,
    )
    try:
        shape_v4_inferred_taxonomy_projection_manifest(
            (
                {
                    "rule_digest": normalized_rule_digest,
                    "catalog_contract": metadata["catalog_contract"],
                    "catalog_digest": catalog_digest,
                    "vector_format": metadata["vector_format"],
                    "member_count": member_count,
                    "member_digest": member_digest,
                    "member_keys": payload,
                    "representation": representation,
                    "pattern_count": pattern_count,
                    "pattern_member_count": pattern_member_count,
                    "pattern_member_bytes": pattern_member_bytes,
                    "pattern_member_digest": pattern_member_digest,
                    "pattern_member_payload": pattern_payload,
                },
            ),
            npi_count=npi_count,
            pattern_count=root_pattern_count,
        )
    except RuntimeError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate row violates its root"
        ) from exc
    return V4InferredTaxonomyCandidates(
        rule_digest=normalized_rule_digest,
        catalog_digest=catalog_digest,
        member_digest=member_digest,
        member_count=member_count,
        npi_keys=npi_keys,
        representation=representation,
        pattern_count=pattern_count,
        pattern_member_count=pattern_member_count,
        pattern_member_bytes=pattern_member_bytes,
        pattern_member_digest=pattern_member_digest,
        npi_keys_by_pattern=npi_keys_by_pattern,
    )


__all__ = [
    "PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE",
    "PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT",
    "PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION",
    "PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION",
    "PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS",
    "PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT",
    "PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION",
    "PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT",
    "PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT",
    "PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS",
    "PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_OCCURRENCES",
    "PTG2_V4_MAX_ONLINE_FILTERED_REVERSE_CODE_SETS",
    "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BATCHES",
    "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_BYTES",
    "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_GRAPH_PAGES",
    "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
    "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_RETAINED_MEMBERSHIPS",
    "V4InferredTaxonomyCandidates",
    "V4InferredTaxonomyPublication",
    "V4InferredTaxonomyProjectionRule",
    "inferred_provider_taxonomy_rule_digest",
    "inferred_provider_taxonomy_rule_set_digest",
    "inferred_taxonomy_projection_rule_manifest",
    "inferred_taxonomy_member_digest",
    "inferred_taxonomy_pattern_member_digest",
    "load_v4_inferred_taxonomy_candidates",
    "pack_inferred_taxonomy_npi_keys",
    "pack_inferred_taxonomy_pattern_npi_keys",
    "publish_v4_inferred_taxonomy_candidates",
    "resolve_inferred_taxonomy_projection_rule_manifest",
    "shape_v4_inferred_taxonomy_projection_manifest",
    "summarize_v4_inferred_taxonomy_candidates",
    "unpack_inferred_taxonomy_npi_keys",
    "unpack_inferred_taxonomy_pattern_npi_keys",
    "validate_v4_inferred_taxonomy_projection_manifest",
]
