# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Snapshot-pinned inferred-taxonomy candidates for PTG V4 serving."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import uuid
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, BinaryIO, Iterable, Iterator, Mapping, Sequence

from sqlalchemy import text

from api.ptg2_code_filters import InferredProviderTaxonomyRule
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)


PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE = "ptg2_v4_inferred_taxonomy_candidate"
PTG2_V4_NPI_TABLE = "ptg2_v4_npi_scope"
PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT = (
    "ptg2_v4_inferred_taxonomy_candidates_v3"
)
PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT = "snapshot_npi_live_catalog_individual_v1"
PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT = "sorted_u32le_v1"
PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT = "pattern_sorted_u32le_v1"
PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION = "direct_v1"
PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION = "pattern_v1"
PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION = "observe_v1"
PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS = "observe_only"
PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON = "candidate_cap_exceeded"
PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON = "pattern_projection_cap_exceeded"
PTG2_V4_INFERRED_TAXONOMY_COMPILER_INPUT_CONTRACT = (
    "ptg2_v4_inferred_taxonomy_compiler_input_v1"
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
_PATTERN_MEMBER_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-pattern-members:v2\x00"
_CATALOG_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-catalog:v1\x00"
_PROJECTION_DIGEST_DOMAIN = b"ptg2:v4:inferred-taxonomy-projection:v3\x00"
_PATTERN_PAYLOAD_MAGIC = b"PTG4TXP2"
_PATTERN_PAYLOAD_VERSION = 1
_PATTERN_PAYLOAD_HEADER = struct.Struct("<8sIIQ")
_PATTERN_PAYLOAD_RECORD = struct.Struct("<II")
_COMPILER_INPUT_RULE_COUNT = 10
_COMPILER_STAGE_COLUMNS = (
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
_COMPILER_STAGE_PREFIX = "ptg2_v4_taxonomy_"
_SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


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
    npi_keys_by_pattern: Mapping[int, tuple[int, ...]] = field(default_factory=dict)


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
class V4InferredTaxonomyCopyStage:
    """One random selected-layout COPY stage awaiting atomic publication."""

    table_name: str
    copy_path: Path
    byte_count: int
    row_count: int


@dataclass(frozen=True)
class _LoadedTaxonomyCandidate:
    catalog_digest: bytes
    member_digest: bytes
    member_count: int
    member_keys: bytes
    representation: str
    pattern_count: int
    pattern_member_count: int
    pattern_member_bytes: int
    pattern_member_digest: bytes
    pattern_payload: bytes
    npi_count: int
    root_pattern_count: int


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


_NormalizedRule = tuple[
    bytes,
    InferredProviderTaxonomyRule,
    frozenset[str],
]
_NormalizedRules = Sequence[_NormalizedRule]


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
        raise PTG2ManifestArtifactError(f"PTG V4 {label} is invalid") from exc
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

    ranges = tuple(sorted((int(start), int(end)) for start, end in rule.ranges))
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


def _normalized_pattern_key(
    raw_pattern_key: Any,
    seen_pattern_keys: set[int],
) -> int:
    if isinstance(raw_pattern_key, bool):
        raise ValueError("PTG V4 inferred-taxonomy pattern key is invalid")
    try:
        pattern_key = int(raw_pattern_key)
    except (TypeError, ValueError) as exc:
        raise ValueError("PTG V4 inferred-taxonomy pattern key is invalid") from exc
    if pattern_key < 0 or pattern_key > 0xFFFFFFFF or pattern_key in seen_pattern_keys:
        raise ValueError("PTG V4 inferred-taxonomy pattern key is invalid")
    seen_pattern_keys.add(pattern_key)
    return pattern_key


def _normalized_pattern_npi_keys(
    raw_npi_keys: Iterable[int],
) -> tuple[int, ...]:
    npi_keys = tuple(raw_npi_keys)
    if not npi_keys or len(npi_keys) > 0xFFFFFFFF:
        raise ValueError("PTG V4 inferred-taxonomy pattern postings must be nonempty")
    normalized_npi_keys: list[int] = []
    previous_npi_key = -1
    for raw_npi_key in npi_keys:
        if isinstance(raw_npi_key, bool):
            raise ValueError("PTG V4 inferred-taxonomy pattern NPI key is invalid")
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
    return tuple(normalized_npi_keys)


def _normalized_pattern_postings(
    npi_keys_by_pattern: Mapping[int, Iterable[int]],
) -> tuple[list[tuple[int, tuple[int, ...]]], int]:
    normalized_patterns: list[tuple[int, tuple[int, ...]]] = []
    seen_pattern_keys: set[int] = set()
    total_members = 0
    for raw_pattern_key, raw_npi_keys in npi_keys_by_pattern.items():
        pattern_key = _normalized_pattern_key(
            raw_pattern_key,
            seen_pattern_keys,
        )
        npi_keys = _normalized_pattern_npi_keys(raw_npi_keys)
        normalized_patterns.append((pattern_key, npi_keys))
        total_members += len(npi_keys)
        if total_members > 0xFFFFFFFFFFFFFFFF:
            raise ValueError("PTG V4 inferred-taxonomy pattern member count is invalid")
    if len(normalized_patterns) > 0xFFFFFFFF:
        raise ValueError("PTG V4 inferred-taxonomy pattern count is invalid")
    normalized_patterns.sort(key=lambda item: item[0])
    return normalized_patterns, total_members


def _pack_pattern_postings(
    normalized_patterns: Sequence[tuple[int, tuple[int, ...]]],
    total_members: int,
) -> bytes:
    packed_pattern_members = bytearray(
        _PATTERN_PAYLOAD_HEADER.pack(
            _PATTERN_PAYLOAD_MAGIC,
            _PATTERN_PAYLOAD_VERSION,
            len(normalized_patterns),
            total_members,
        )
    )
    for pattern_key, npi_keys in normalized_patterns:
        packed_pattern_members.extend(
            _PATTERN_PAYLOAD_RECORD.pack(pattern_key, len(npi_keys))
        )
        for npi_key in npi_keys:
            packed_pattern_members.extend(struct.pack("<I", npi_key))
    return bytes(packed_pattern_members)


def pack_inferred_taxonomy_pattern_npi_keys(
    npi_keys_by_pattern: Mapping[int, Iterable[int]],
) -> bytes:
    """Pack strict pattern postings into one deterministic compact payload."""

    if not isinstance(npi_keys_by_pattern, Mapping):
        raise ValueError("PTG V4 inferred-taxonomy pattern postings are invalid")
    if not npi_keys_by_pattern:
        return b""
    normalized_patterns, total_members = _normalized_pattern_postings(
        npi_keys_by_pattern
    )
    return _pack_pattern_postings(normalized_patterns, total_members)


def _validated_pattern_payload_header(
    packed_pattern_payload: bytes,
    *,
    pattern_count: int,
    pattern_member_count: int,
) -> tuple[bytes, int, int]:
    normalized_payload = bytes(packed_pattern_payload)
    normalized_pattern_count = int(pattern_count)
    normalized_member_count = int(pattern_member_count)
    if normalized_pattern_count < 0 or normalized_member_count < 0:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern counts are invalid"
        )
    if not normalized_payload:
        if normalized_pattern_count == 0 and normalized_member_count == 0:
            return normalized_payload, 0, 0
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
    return (
        normalized_payload,
        normalized_pattern_count,
        normalized_member_count,
    )


def _unpack_pattern_record(
    packed_payload: bytes,
    *,
    offset: int,
    previous_pattern_key: int,
) -> tuple[int, tuple[int, ...], int]:
    record_end = offset + _PATTERN_PAYLOAD_RECORD.size
    if record_end > len(packed_payload):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload is truncated"
        )
    pattern_key, member_count = _PATTERN_PAYLOAD_RECORD.unpack_from(
        packed_payload,
        offset,
    )
    if pattern_key <= previous_pattern_key or member_count <= 0:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern postings are not strict"
        )
    member_end = record_end + member_count * 4
    if member_end > len(packed_payload):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload is truncated"
        )
    npi_keys = tuple(
        member[0]
        for member in struct.iter_unpack(
            "<I",
            packed_payload[record_end:member_end],
        )
    )
    if any(left >= right for left, right in zip(npi_keys, npi_keys[1:])):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern NPI keys are not strict"
        )
    return pattern_key, npi_keys, member_end


def unpack_inferred_taxonomy_pattern_npi_keys(
    packed_pattern_payload: bytes,
    *,
    pattern_count: int,
    pattern_member_count: int,
) -> dict[int, tuple[int, ...]]:
    """Decode exact pattern postings while rejecting structural drift."""

    packed_payload, normalized_count, normalized_member_count = (
        _validated_pattern_payload_header(
            packed_pattern_payload,
            pattern_count=pattern_count,
            pattern_member_count=pattern_member_count,
        )
    )
    if not packed_payload:
        return {}
    offset = _PATTERN_PAYLOAD_HEADER.size
    npi_keys_by_pattern: dict[int, tuple[int, ...]] = {}
    for _index in range(normalized_count):
        pattern_key, npi_keys, offset = _unpack_pattern_record(
            packed_payload,
            offset=offset,
            previous_pattern_key=max(npi_keys_by_pattern, default=-1),
        )
        npi_keys_by_pattern[pattern_key] = npi_keys
    observed_members = sum(map(len, npi_keys_by_pattern.values()))
    if offset != len(packed_payload) or observed_members != normalized_member_count:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload has trailing data"
        )
    return npi_keys_by_pattern


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
    packed_pattern_payload: bytes,
) -> bytes:
    """Authenticate one rule-bound direct or factored pattern projection."""

    normalized_rule_digest = bytes(rule_digest)
    normalized_representation = str(representation)
    normalized_pattern_count = int(pattern_count)
    normalized_member_count = int(pattern_member_count)
    normalized_payload = bytes(packed_pattern_payload)
    if len(normalized_rule_digest) != 32:
        raise ValueError("PTG V4 inferred-taxonomy pattern identity is invalid")
    if normalized_representation not in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
    }:
        raise ValueError("PTG V4 inferred-taxonomy pattern representation is invalid")
    if normalized_representation in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
    }:
        if (
            normalized_pattern_count != 0
            or normalized_member_count != 0
            or normalized_payload
        ):
            raise ValueError("PTG V4 direct inferred-taxonomy projection has patterns")
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
    normalized_rules: list[
        tuple[bytes, InferredProviderTaxonomyRule, frozenset[str]]
    ] = []
    seen_digests: set[bytes] = set()
    for rule in rules:
        rule_digest = inferred_provider_taxonomy_rule_digest(rule)
        if rule_digest in seen_digests:
            raise ValueError("inferred taxonomy rule digest is duplicated")
        seen_digests.add(rule_digest)
        normalized_rules.append(
            (
                rule_digest,
                rule,
                frozenset(str(code).strip().upper() for code in rule.taxonomy_codes),
            )
        )
    if not normalized_rules:
        raise ValueError("inferred taxonomy candidate publication needs rules")
    return tuple(sorted(normalized_rules, key=lambda item: item[0]))


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


def _strict_sha256_hex(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip().lower()
    try:
        raw_digest = bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"PTG V4 {label} is invalid") from exc
    if len(normalized) != 64 or len(raw_digest) != 32:
        raise ValueError(f"PTG V4 {label} is invalid")
    return normalized


def _safe_stage_table_name(value: str) -> str:
    normalized = str(value)
    if not _SAFE_IDENTIFIER.fullmatch(normalized):
        raise ValueError("PTG V4 inferred-taxonomy stage name is invalid")
    return normalized


def _nofollow_flag() -> int:
    nofollow_flag = getattr(os, "O_NOFOLLOW", None)
    if nofollow_flag is None:
        raise RuntimeError("PTG V4 inferred-taxonomy artifact no-follow is unavailable")
    return int(nofollow_flag)


def _is_same_node(
    left: os.stat_result,
    right: os.stat_result,
) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        stat.S_IFMT(left.st_mode),
        left.st_uid,
    ) == (
        right.st_dev,
        right.st_ino,
        stat.S_IFMT(right.st_mode),
        right.st_uid,
    )


@contextmanager
def _open_private_artifact_parent(
    path: Path,
) -> Iterator[tuple[Path, int]]:
    artifact_parent = Path(path).parent
    try:
        parent_metadata = artifact_parent.lstat()
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy artifact directory is unavailable"
        ) from exc
    if (
        artifact_parent.is_symlink()
        or not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_IMODE(parent_metadata.st_mode) & 0o077
        or parent_metadata.st_uid != os.geteuid()
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy artifact directory is not private")
    resolved_parent = artifact_parent.resolve(strict=True)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | _nofollow_flag()
    )
    try:
        directory_descriptor = os.open(resolved_parent, directory_flags)
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy artifact directory is unsafe"
        ) from exc
    try:
        opened_metadata = os.fstat(directory_descriptor)
        if (
            not _is_same_node(opened_metadata, parent_metadata)
            or stat.S_IMODE(opened_metadata.st_mode) & 0o077
        ):
            raise RuntimeError("PTG V4 inferred-taxonomy artifact directory changed")
        yield resolved_parent, directory_descriptor
    finally:
        os.close(directory_descriptor)


def _write_fsynced_bytes(descriptor: int, artifact_bytes: bytes) -> None:
    remaining_bytes = memoryview(artifact_bytes)
    while remaining_bytes:
        written_byte_count = os.write(descriptor, remaining_bytes)
        if written_byte_count <= 0:
            raise OSError("PTG V4 inferred-taxonomy member write stalled")
        remaining_bytes = remaining_bytes[written_byte_count:]
    os.fsync(descriptor)


def _create_private_artifact(
    directory_descriptor: int,
    artifact_name: str,
    artifact_bytes: bytes,
) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | _nofollow_flag()
    )
    descriptor = os.open(
        artifact_name,
        flags,
        0o600,
        dir_fd=directory_descriptor,
    )
    try:
        created_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(created_metadata.st_mode)
            or created_metadata.st_nlink != 1
            or created_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(created_metadata.st_mode) != 0o600
        ):
            raise RuntimeError("PTG V4 inferred-taxonomy member artifact is unsafe")
        _write_fsynced_bytes(descriptor, artifact_bytes)
        named_metadata = os.stat(
            artifact_name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if not _is_same_node(created_metadata, named_metadata):
            raise RuntimeError("PTG V4 inferred-taxonomy member artifact changed")
    finally:
        os.close(descriptor)


def _exclusive_fsynced_artifact(
    path: Path,
    artifact_bytes: bytes,
) -> Path:
    """Create one immutable scratch artifact without following a final symlink."""

    normalized_path = Path(path)
    try:
        existing_metadata = normalized_path.lstat()
    except FileNotFoundError:
        existing_metadata = None
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy member artifact is unavailable"
        ) from exc
    if existing_metadata is not None:
        if normalized_path.is_symlink():
            raise RuntimeError("PTG V4 inferred-taxonomy member artifact is unsafe")
        raise FileExistsError("PTG V4 inferred-taxonomy member artifact already exists")
    with _open_private_artifact_parent(normalized_path) as (
        resolved_parent,
        directory_descriptor,
    ):
        resolved_path = resolved_parent / normalized_path.name
        try:
            _create_private_artifact(
                directory_descriptor,
                normalized_path.name,
                artifact_bytes,
            )
            os.fsync(directory_descriptor)
        except BaseException:
            with suppress(OSError):
                os.unlink(
                    normalized_path.name,
                    dir_fd=directory_descriptor,
                )
            raise
        return resolved_path


def _candidate_evidence_rows(
    catalog_rows: Iterable[Any],
    *,
    rule_codes: frozenset[str],
) -> tuple[_CandidateEvidence, ...]:
    evidence_rows: list[_CandidateEvidence] = []
    previous_npi_key = -1
    for raw_catalog_row in catalog_rows:
        catalog_row = _row_mapping(raw_catalog_row)
        npi_key = int(catalog_row["npi_key"])
        npi = int(catalog_row["npi"])
        if npi_key <= previous_npi_key or npi_key < 0 or npi_key > 0xFFFFFFFF:
            raise RuntimeError("PTG V4 inferred-taxonomy NPI keys are not strict")
        if npi < 1_000_000_000 or npi > 9_999_999_999:
            raise RuntimeError("PTG V4 inferred-taxonomy NPI is invalid")
        matched_codes = frozenset(
            str(code).strip().upper()
            for code in (catalog_row.get("matched_taxonomy_codes") or ())
        )
        selected_codes = tuple(sorted(matched_codes & rule_codes))
        if not selected_codes:
            raise RuntimeError("PTG V4 inferred-taxonomy catalog evidence changed")
        evidence_rows.append(_CandidateEvidence(npi_key, npi, selected_codes))
        previous_npi_key = npi_key
    return tuple(evidence_rows)


async def _read_taxonomy_rule_evidence(
    session: Any,
    *,
    catalog_schema: str,
    stage_schema: str,
    stage: str,
    rule_codes: frozenset[str],
) -> tuple[_CandidateEvidence, ...]:
    catalog_query = await session.execute(
        text(
            f"""
            SELECT scoped.npi_key,
                   scoped.npi,
                   ARRAY_AGG(
                       DISTINCT taxonomy.healthcare_provider_taxonomy_code
                       ORDER BY taxonomy.healthcare_provider_taxonomy_code
                   ) AS matched_taxonomy_codes
              FROM {stage_schema}.{stage} AS scoped
              JOIN {catalog_schema}.npi AS entity
                ON entity.npi = scoped.npi
               AND COALESCE(entity.entity_type_code, 0) = 1
              JOIN {catalog_schema}.npi_taxonomy AS taxonomy
                ON taxonomy.npi = scoped.npi
             WHERE taxonomy.healthcare_provider_taxonomy_code = ANY(
                   CAST(:taxonomy_codes AS varchar[])
               )
             GROUP BY scoped.npi_key, scoped.npi
             ORDER BY scoped.npi_key
             LIMIT :candidate_limit
            """
        ),
        {
            "taxonomy_codes": tuple(sorted(rule_codes)),
            "candidate_limit": (PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1),
        },
    )
    return _candidate_evidence_rows(
        catalog_query,
        rule_codes=rule_codes,
    )


async def _validate_stable_catalog_transaction(session: Any) -> None:
    transaction_query = await session.execute(
        text(
            """
            SELECT current_setting('transaction_isolation') AS isolation,
                   current_setting('transaction_read_only') AS read_only
            """
        )
    )
    transaction_rows = tuple(
        _row_mapping(transaction_entry) for transaction_entry in transaction_query
    )
    if (
        len(transaction_rows) != 1
        or transaction_rows[0].get("isolation")
        not in {"repeatable read", "serializable"}
        or transaction_rows[0].get("read_only") != "on"
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy catalog transaction is not stable")


def _append_taxonomy_rule_input(
    member_bytes: bytearray,
    rule_inputs: list[dict[str, Any]],
    *,
    rule_digest: bytes,
    evidence_rows: tuple[_CandidateEvidence, ...],
) -> None:
    npi_keys = tuple(candidate.npi_key for candidate in evidence_rows)
    if len(npi_keys) > PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1:
        raise RuntimeError("PTG V4 inferred-taxonomy catalog query exceeded its bound")
    packed_members = pack_inferred_taxonomy_npi_keys(npi_keys)
    member_offset = len(member_bytes)
    member_bytes.extend(packed_members)
    rule_inputs.append(
        {
            "rule_digest": rule_digest.hex(),
            "catalog_digest": _catalog_digest(
                rule_digest,
                evidence_rows,
            ).hex(),
            "member_count": len(npi_keys),
            "member_offset_bytes": member_offset,
            "member_byte_count": len(packed_members),
        }
    )


def _taxonomy_compiler_manifest(
    *,
    normalized_rules: _NormalizedRules,
    npi_scope_sha256: str,
    artifact_path: Path,
    member_bytes: bytes,
    rule_inputs: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "contract": PTG2_V4_INFERRED_TAXONOMY_COMPILER_INPUT_CONTRACT,
        "catalog_contract": (PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT),
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "npi_scope_sha256": _strict_sha256_hex(
            npi_scope_sha256,
            label="inferred-taxonomy NPI scope digest",
        ),
        "rule_set_digest": _rule_set_digest_from_digests(
            rule_digest for rule_digest, _rule, _codes in normalized_rules
        ).hex(),
        "members": {
            "path": str(artifact_path),
            "byte_count": len(member_bytes),
            "sha256": hashlib.sha256(member_bytes).hexdigest(),
        },
        "rules": rule_inputs,
    }


async def prepare_v4_taxonomy_input(
    session: Any,
    *,
    schema_name: str,
    npi_scope_stage_table: str,
    npi_scope_stage_schema_name: str | None = None,
    npi_scope_sha256: str,
    rules: Iterable[InferredProviderTaxonomyRule],
    members_path: str | Path,
) -> dict[str, Any]:
    """Create bounded compiler input from ten reads in one stable snapshot."""

    normalized_rules = _normalized_rules(rules)
    if len(normalized_rules) != _COMPILER_INPUT_RULE_COUNT:
        raise ValueError(
            "PTG V4 inferred-taxonomy compiler input needs exactly 10 rules"
        )
    stage = _quote_ident(_safe_stage_table_name(npi_scope_stage_table))
    catalog_schema = _quote_ident(schema_name)
    stage_schema = _quote_ident(npi_scope_stage_schema_name or schema_name)
    await _validate_stable_catalog_transaction(session)
    member_bytes = bytearray()
    rule_inputs: list[dict[str, Any]] = []
    for rule_digest, _rule, rule_codes in normalized_rules:
        evidence_rows = await _read_taxonomy_rule_evidence(
            session,
            catalog_schema=catalog_schema,
            stage_schema=stage_schema,
            stage=stage,
            rule_codes=rule_codes,
        )
        _append_taxonomy_rule_input(
            member_bytes,
            rule_inputs,
            rule_digest=rule_digest,
            evidence_rows=evidence_rows,
        )
    artifact_path = _exclusive_fsynced_artifact(
        Path(members_path),
        bytes(member_bytes),
    )
    return _taxonomy_compiler_manifest(
        normalized_rules=normalized_rules,
        npi_scope_sha256=npi_scope_sha256,
        artifact_path=artifact_path,
        member_bytes=bytes(member_bytes),
        rule_inputs=rule_inputs,
    )


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
    digest.update(int(rule_manifest["pattern_member_count"]).to_bytes(8, "big"))
    digest.update(int(rule_manifest["pattern_member_bytes"]).to_bytes(8, "big"))
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
    digest.update(int(rule_manifest["packed_byte_count"]).to_bytes(8, "big"))
    for field_name in ("status", "reason", "representation"):
        encoded_value = str(rule_manifest[field_name]).encode("ascii")
        digest.update(len(encoded_value).to_bytes(2, "big"))
        digest.update(encoded_value)
    digest.update(bytes.fromhex(str(rule_manifest["pattern_member_digest"])))


def _shape_projection_manifest(
    projection_rows: Sequence[Mapping[str, Any]],
    *,
    npi_count: int,
    pattern_count: int,
) -> dict[str, Any]:
    """Shape persisted V3 candidate and observe rows under root bounds."""

    caps_by_name = _projection_cap_values()
    if isinstance(npi_count, bool) or isinstance(pattern_count, bool):
        raise RuntimeError("PTG V4 inferred-taxonomy dictionary bounds are invalid")
    normalized_npi_count = int(npi_count)
    normalized_pattern_count = int(pattern_count)
    if (
        normalized_npi_count < 0
        or normalized_npi_count > 0x100000000
        or normalized_pattern_count < 0
        or normalized_pattern_count > 0x100000000
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy dictionary bounds are invalid")
    rule_manifests: list[dict[str, Any]] = []
    observe_only_rule_manifests: list[dict[str, Any]] = []
    digest_entries: list[tuple[bytes, bool, dict[str, Any]]] = []
    total_members = 0
    total_packed_bytes = 0
    total_patterns = 0
    total_pattern_members = 0
    total_pattern_bytes = 0
    previous_rule_digest: bytes | None = None
    for raw_row in projection_rows:
        rule_digest = bytes(raw_row["rule_digest"])
        catalog_digest = bytes(raw_row["catalog_digest"])
        member_digest = bytes(raw_row["member_digest"])
        member_count = int(raw_row["member_count"])
        packed_candidate_keys = bytes(raw_row["member_keys"])
        representation = str(raw_row["representation"])
        observe_reason = raw_row.get("observe_reason")
        raw_observe_count = raw_row.get("observe_count_lower_bound")
        if isinstance(raw_observe_count, bool):
            raise RuntimeError("PTG V4 inferred-taxonomy observe witness is invalid")
        observe_count_lower_bound = (
            None if raw_observe_count is None else int(raw_observe_count)
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
            or len(packed_candidate_keys) != member_count * 4
            or row_pattern_count < 0
            or pattern_member_count < 0
            or pattern_member_bytes != len(pattern_payload)
            or (
                previous_rule_digest is not None and rule_digest <= previous_rule_digest
            )
        ):
            raise RuntimeError("PTG V4 inferred-taxonomy candidate manifest is invalid")
        try:
            candidate_members = unpack_inferred_taxonomy_npi_keys(
                packed_candidate_keys,
                member_count=member_count,
            )
            npi_keys_by_pattern = unpack_inferred_taxonomy_pattern_npi_keys(
                pattern_payload,
                pattern_count=row_pattern_count,
                pattern_member_count=pattern_member_count,
            )
            expected_pattern_digest = inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=row_pattern_count,
                pattern_member_count=pattern_member_count,
                packed_pattern_payload=pattern_payload,
            )
        except (PTG2ManifestArtifactError, ValueError) as exc:
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate manifest changed"
            ) from exc
        candidate_member_set = frozenset(candidate_members)
        posting_member_set = frozenset(
            npi_key for npi_keys in npi_keys_by_pattern.values() for npi_key in npi_keys
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
        if not (is_pattern_projection or is_direct_projection or is_observe_projection):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy projection representation is invalid"
            )
        if is_observe_projection:
            is_candidate_cap_observe = observe_reason == (
                PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
            )
            is_pattern_cap_observe = observe_reason == (
                PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
            )
            if (
                not (is_candidate_cap_observe or is_pattern_cap_observe)
                or observe_count_lower_bound is None
                or (
                    is_candidate_cap_observe
                    and (
                        member_count
                        != caps_by_name["max_online_inferred_taxonomy_candidates"] + 1
                        or observe_count_lower_bound
                        != caps_by_name["max_online_inferred_taxonomy_candidates"] + 1
                    )
                )
                or (
                    is_pattern_cap_observe
                    and (
                        member_count
                        > caps_by_name["max_online_inferred_taxonomy_candidates"]
                        or observe_count_lower_bound
                        != caps_by_name[
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
            or member_count > caps_by_name["max_online_inferred_taxonomy_candidates"]
            or pattern_member_count
            > caps_by_name["max_online_candidate_pattern_projection_members"]
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy projection exceeds its online cap"
            )
        if any(npi_key >= normalized_npi_count for npi_key in candidate_members):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy candidate exceeds its NPI root"
            )
        if any(
            pattern_key >= normalized_pattern_count
            for pattern_key in npi_keys_by_pattern
        ):
            raise RuntimeError(
                "PTG V4 inferred-taxonomy pattern exceeds its pattern root"
            )
        if is_pattern_projection and posting_member_set != candidate_member_set:
            raise RuntimeError(
                "PTG V4 inferred-taxonomy pattern projection is incomplete"
            )
        if (
            not is_pattern_projection
            and not is_observe_projection
            and normalized_pattern_count > 0
            and candidate_member_set
        ):
            raise RuntimeError("PTG V4 inferred-taxonomy pattern projection is missing")
        if (
            raw_row.get("catalog_contract")
            != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
            or raw_row.get("vector_format") != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
            or inferred_taxonomy_member_digest(
                rule_digest,
                member_count=member_count,
                payload=packed_candidate_keys,
            )
            != member_digest
            or expected_pattern_digest != pattern_member_digest
        ):
            raise RuntimeError("PTG V4 inferred-taxonomy candidate manifest changed")
        if is_observe_projection:
            rule_manifest_by_field = {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": catalog_digest.hex(),
                "member_digest": member_digest.hex(),
                "member_count": member_count,
                "observed_count_lower_bound": observe_count_lower_bound,
                "packed_byte_count": len(packed_candidate_keys),
                "status": PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
                "reason": observe_reason,
                "representation": representation,
                "pattern_member_digest": pattern_member_digest.hex(),
            }
            observe_only_rule_manifests.append(rule_manifest_by_field)
            digest_entries.append((rule_digest, True, rule_manifest_by_field))
        else:
            rule_manifest_by_field = {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": catalog_digest.hex(),
                "member_digest": member_digest.hex(),
                "member_count": member_count,
                "packed_byte_count": len(packed_candidate_keys),
                "representation": representation,
                "pattern_count": row_pattern_count,
                "pattern_member_count": pattern_member_count,
                "pattern_member_bytes": pattern_member_bytes,
                "pattern_member_digest": pattern_member_digest.hex(),
            }
            rule_manifests.append(rule_manifest_by_field)
            digest_entries.append((rule_digest, False, rule_manifest_by_field))
        total_members += member_count
        total_packed_bytes += len(packed_candidate_keys)
        total_patterns += row_pattern_count
        total_pattern_members += pattern_member_count
        total_pattern_bytes += pattern_member_bytes
        previous_rule_digest = rule_digest
    if not digest_entries:
        raise RuntimeError("PTG V4 inferred-taxonomy projection has no rule evidence")
    rule_set_digest = _rule_set_digest_from_digests(
        rule_digest for rule_digest, _is_observe, _entry in digest_entries
    )
    digest = hashlib.sha256()
    digest.update(_PROJECTION_DIGEST_DOMAIN)
    _update_projection_cap_digest(digest, caps_by_name)
    digest.update(rule_set_digest)
    for _rule_digest, is_observe, rule_manifest_by_field in digest_entries:
        if is_observe:
            _update_projection_observe_rule_digest(
                digest,
                rule_manifest_by_field,
            )
        else:
            _update_projection_rule_digest(digest, rule_manifest_by_field)
    projection_digest = digest.digest()
    return {
        "contract": PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT,
        "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "pattern_format": PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT,
        **caps_by_name,
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


shape_v4_inferred_taxonomy_projection_manifest = _shape_projection_manifest


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
    return _shape_projection_manifest(
        materialized_rows,
        npi_count=maximum_npi_key + 1,
        pattern_count=maximum_pattern_key + 1,
    )


def _validate_projection_manifest(
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
        manifest.get("contract") != PTG2_V4_INFERRED_TAXONOMY_PROJECTION_CONTRACT
        or manifest.get("catalog_contract")
        != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        or manifest.get("vector_format") != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
        or manifest.get("pattern_format") != PTG2_V4_INFERRED_TAXONOMY_PATTERN_FORMAT
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection contract is incompatible"
        )
    integer_fields = (*cap_fields, *aggregate_fields)
    try:
        integer_by_field = {
            field_name: int(manifest[field_name])
            for field_name in integer_fields
            if not isinstance(manifest[field_name], bool)
        }
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection counts are invalid"
        ) from exc
    if len(integer_by_field) != len(integer_fields):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection counts are invalid"
        )
    if (
        any(integer_by_field[field_name] <= 0 for field_name in cap_fields)
        or any(integer_by_field[field_name] < 0 for field_name in aggregate_fields)
        or integer_by_field["rule_count"] + integer_by_field["observe_only_rule_count"]
        <= 0
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
    caps_by_name = {
        field_name: integer_by_field[field_name] for field_name in cap_fields
    }
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
        is_direct_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        )
        is_pattern_projection = representation == (
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        )
        pattern_size = (
            _PATTERN_PAYLOAD_HEADER.size
            + pattern_count * _PATTERN_PAYLOAD_RECORD.size
            + pattern_member_count * 4
        )
        if (
            any(
                isinstance(raw_rule[field_name], bool)
                for field_name in integer_rule_fields
            )
            or len(rule_digest) != 32
            or len(catalog_digest) != 32
            or len(member_digest) != 32
            or len(pattern_member_digest) != 32
            or member_count < 0
            or packed_byte_count != member_count * 4
            or member_count
            > integer_by_field["max_online_inferred_taxonomy_candidates"]
            or pattern_count < 0
            or pattern_member_count < 0
            or pattern_member_count
            > integer_by_field["max_online_candidate_pattern_projection_members"]
            or pattern_member_bytes < 0
            or (
                is_direct_projection
                and any((pattern_count, pattern_member_count, pattern_member_bytes))
            )
            or (
                is_pattern_projection
                and (
                    pattern_count <= 0
                    or pattern_member_count < pattern_count
                    or pattern_member_bytes != pattern_size
                )
            )
            or not (is_direct_projection or is_pattern_projection)
            or (
                previous_rule_digest is not None and rule_digest <= previous_rule_digest
            )
        ):
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy projection rule is invalid"
            )
        canonical_rule_by_field = {
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
        canonical_rules.append(canonical_rule_by_field)
        digest_entries.append((rule_digest, False, canonical_rule_by_field))
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
            observed_count_lower_bound = int(raw_rule["observed_count_lower_bound"])
            packed_byte_count = int(raw_rule["packed_byte_count"])
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG V4 inferred-taxonomy observe rule is invalid"
            ) from exc
        expected_pattern_member_digest = (
            inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=(PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION),
                pattern_count=0,
                pattern_member_count=0,
                packed_pattern_payload=b"",
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
            or raw_rule.get("status") != PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS
            or not (
                (
                    raw_rule.get("reason")
                    == PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
                    and member_count
                    == integer_by_field["max_online_inferred_taxonomy_candidates"] + 1
                    and observed_count_lower_bound
                    == integer_by_field["max_online_inferred_taxonomy_candidates"] + 1
                )
                or (
                    raw_rule.get("reason")
                    == PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
                    and member_count
                    <= integer_by_field["max_online_inferred_taxonomy_candidates"]
                    and observed_count_lower_bound
                    == integer_by_field[
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
        canonical_rule_by_field = {
            "rule_digest": rule_digest.hex(),
            "catalog_digest": catalog_digest.hex(),
            "member_digest": member_digest.hex(),
            "member_count": member_count,
            "observed_count_lower_bound": observed_count_lower_bound,
            "packed_byte_count": packed_byte_count,
            "status": PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
            "reason": str(raw_rule["reason"]),
            "representation": (PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION),
            "pattern_member_digest": pattern_member_digest.hex(),
        }
        canonical_observe_rules.append(canonical_rule_by_field)
        digest_entries.append((rule_digest, True, canonical_rule_by_field))
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
        raw_rule_set_digest = bytes.fromhex(str(manifest.get("rule_set_digest") or ""))
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
            rule_digest for rule_digest, _is_observe, _entry in ordered_digest_entries
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy rule-set digest is invalid"
        ) from exc
    digest = hashlib.sha256()
    digest.update(_PROJECTION_DIGEST_DOMAIN)
    _update_projection_cap_digest(digest, caps_by_name)
    digest.update(rule_set_digest)
    for _rule_digest, is_observe, canonical_rule_by_field in ordered_digest_entries:
        if is_observe:
            _update_projection_observe_rule_digest(
                digest,
                canonical_rule_by_field,
            )
        else:
            _update_projection_rule_digest(digest, canonical_rule_by_field)
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
        len(canonical_rules) != integer_by_field["rule_count"]
        or len(canonical_observe_rules) != integer_by_field["observe_only_rule_count"]
        or total_members != integer_by_field["member_count"]
        or total_packed_bytes != integer_by_field["packed_byte_count"]
        or total_patterns != integer_by_field["pattern_count"]
        or total_pattern_members != integer_by_field["pattern_member_count"]
        or total_pattern_bytes != integer_by_field["pattern_member_bytes"]
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
        **caps_by_name,
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


validate_v4_inferred_taxonomy_projection_manifest = _validate_projection_manifest


def _projection_rule_from_manifest(
    canonical_manifest: Mapping[str, Any],
    raw_rule: Mapping[str, Any],
    rule_digest: bytes,
) -> V4InferredTaxonomyProjectionRule:
    return V4InferredTaxonomyProjectionRule(
        rule_digest=rule_digest,
        catalog_digest=bytes.fromhex(raw_rule["catalog_digest"]),
        member_digest=bytes.fromhex(raw_rule["member_digest"]),
        member_count=int(raw_rule["member_count"]),
        packed_byte_count=int(raw_rule["packed_byte_count"]),
        representation=str(raw_rule["representation"]),
        pattern_count=int(raw_rule["pattern_count"]),
        pattern_member_count=int(raw_rule["pattern_member_count"]),
        pattern_member_bytes=int(raw_rule["pattern_member_bytes"]),
        pattern_member_digest=bytes.fromhex(raw_rule["pattern_member_digest"]),
        max_online_filtered_reverse_code_sets=int(
            canonical_manifest["max_online_filtered_reverse_code_sets"]
        ),
        max_online_filtered_reverse_code_occurrences=int(
            canonical_manifest["max_online_filtered_reverse_code_occurrences"]
        ),
        max_online_inferred_taxonomy_candidates=int(
            canonical_manifest["max_online_inferred_taxonomy_candidates"]
        ),
        max_online_candidate_pattern_projection_members=int(
            canonical_manifest["max_online_candidate_pattern_projection_members"]
        ),
        max_online_inferred_taxonomy_retained_memberships=int(
            canonical_manifest["max_online_inferred_taxonomy_retained_memberships"]
        ),
        max_online_inferred_taxonomy_graph_pages=int(
            canonical_manifest["max_online_inferred_taxonomy_graph_pages"]
        ),
        max_online_inferred_taxonomy_graph_bytes=int(
            canonical_manifest["max_online_inferred_taxonomy_graph_bytes"]
        ),
        max_online_inferred_taxonomy_graph_batches=int(
            canonical_manifest["max_online_inferred_taxonomy_graph_batches"]
        ),
    )


def resolve_inferred_taxonomy_projection_rule_manifest(
    manifest: Mapping[str, Any],
    rule_digest: bytes,
) -> V4InferredTaxonomyProjectionRule | None:
    """Resolve an online rule or one explicit observe-only fallback."""

    canonical_manifest = _validate_projection_manifest(manifest)
    normalized_rule_digest = _digest_bytes(rule_digest, label="rule digest")
    for raw_rule in canonical_manifest["rules"]:
        if raw_rule["rule_digest"] == normalized_rule_digest.hex():
            return _projection_rule_from_manifest(
                canonical_manifest,
                raw_rule,
                normalized_rule_digest,
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
        raise PTG2ManifestArtifactError("PTG V4 inferred-taxonomy rule is observe-only")
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
        normalized_representation == PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        and normalized_pattern_count == 0
    ):
        return normalized_representation, normalized_pattern_count
    if (
        normalized_representation == PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        and 0 < normalized_pattern_count <= 0xFFFFFFFF
    ):
        return normalized_representation, normalized_pattern_count
    raise ValueError("PTG V4 inferred-taxonomy root identity is invalid")


async def _lookup_candidate_npi_patterns(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    candidate_npi_keys: tuple[int, ...],
) -> Mapping[int, Sequence[int]]:
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
            max_members=(PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS),
        )
    except PTG2SharedBlockError as exc:
        if str(exc) != "PTG V4 graph selection exceeds max_members":
            raise
        raise _PatternProjectionCapExceeded(
            "PTG V4 inferred-taxonomy pattern projection exceeds the online cap"
        ) from exc
    if set(npi_patterns) != set(candidate_npi_keys):
        raise RuntimeError("PTG V4 inferred-taxonomy pattern projection is incomplete")
    return npi_patterns


def _transpose_candidate_patterns(
    npi_patterns: Mapping[int, Sequence[int]],
    *,
    candidate_npi_keys: tuple[int, ...],
    root_pattern_count: int,
) -> dict[int, tuple[int, ...]]:
    npi_keys_by_pattern: dict[int, list[int]] = {}
    observed_member_count = 0
    for npi_key in candidate_npi_keys:
        previous_pattern_key = -1
        for raw_pattern_key in npi_patterns[npi_key]:
            if isinstance(raw_pattern_key, bool):
                raise RuntimeError("PTG V4 inferred-taxonomy pattern key is invalid")
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
        raise RuntimeError("PTG V4 inferred-taxonomy pattern evidence is incomplete")
    return {
        pattern_key: tuple(npi_keys)
        for pattern_key, npi_keys in sorted(npi_keys_by_pattern.items())
    }


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
    npi_patterns = await _lookup_candidate_npi_patterns(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        candidate_npi_keys=candidate_npi_keys,
    )
    return _transpose_candidate_patterns(
        npi_patterns,
        candidate_npi_keys=candidate_npi_keys,
        root_pattern_count=root_pattern_count,
    )


def _normalized_persisted_projection_rows(
    rows: Iterable[Any],
) -> tuple[dict[str, Any], ...]:
    binary_fields = {
        "rule_digest",
        "catalog_digest",
        "member_digest",
        "member_keys",
        "pattern_member_digest",
        "pattern_member_payload",
    }
    return tuple(
        {
            field_name: (
                bytes(projected_row[field_name])
                if field_name in binary_fields
                else (
                    projected_row.get(field_name)
                    if field_name in {"observe_reason", "observe_count_lower_bound"}
                    else projected_row[field_name]
                )
            )
            for field_name in _COMPILER_STAGE_COLUMNS
        }
        for projected_row in (
            _row_mapping(raw_projected_row) for raw_projected_row in rows
        )
    )


def _taxonomy_publication(
    manifest: Mapping[str, Any],
) -> V4InferredTaxonomyPublication:
    return V4InferredTaxonomyPublication(
        rule_count=int(manifest["rule_count"]),
        member_count=int(manifest["member_count"]),
        packed_byte_count=int(manifest["packed_byte_count"]),
        projection_digest=bytes.fromhex(str(manifest["projection_digest"])),
        manifest=dict(manifest),
        pattern_count=int(manifest["pattern_count"]),
        pattern_member_count=int(manifest["pattern_member_count"]),
        pattern_member_bytes=int(manifest["pattern_member_bytes"]),
        observe_only_rule_count=int(manifest["observe_only_rule_count"]),
    )


async def remove_v4_taxonomy_stage(
    session: Any,
    *,
    stage_table: str,
) -> None:
    """Drop one session-local candidate stage without cascading."""

    stage = _safe_stage_table_name(stage_table)
    await session.execute(
        text(
            f"DROP TABLE IF EXISTS {_quote_ident('pg_temp')}." f"{_quote_ident(stage)}"
        )
    )


def _taxonomy_stage_create_sql(stage: str) -> str:
    return f"""
    CREATE TEMP TABLE {stage} (
        rule_digest bytea PRIMARY KEY CHECK (octet_length(rule_digest) = 32),
        catalog_contract text NOT NULL,
        catalog_digest bytea NOT NULL
            CHECK (octet_length(catalog_digest) = 32),
        vector_format text NOT NULL,
        member_count integer NOT NULL
            CHECK (member_count BETWEEN 0 AND
                   {PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1}),
        member_digest bytea NOT NULL
            CHECK (octet_length(member_digest) = 32),
        member_keys bytea NOT NULL
            CHECK (octet_length(member_keys) = member_count * 4),
        representation text NOT NULL,
        observe_reason text,
        observe_count_lower_bound bigint,
        pattern_count integer NOT NULL CHECK (pattern_count >= 0),
        pattern_member_count bigint NOT NULL CHECK (pattern_member_count >= 0),
        pattern_member_bytes bigint NOT NULL CHECK (pattern_member_bytes >= 0),
        pattern_member_digest bytea NOT NULL
            CHECK (octet_length(pattern_member_digest) = 32),
        pattern_member_payload bytea NOT NULL
            CHECK (octet_length(pattern_member_payload) = pattern_member_bytes)
    ) ON COMMIT DROP
    """


def _stream_sha256(copy_stream: Any) -> str:
    copy_digest = hashlib.sha256()
    while copy_chunk := copy_stream.read(1024 * 1024):
        copy_digest.update(copy_chunk)
    return copy_digest.hexdigest()


def _authenticated_copy_metadata(
    copy_stream: Any,
    *,
    expected_byte_count: int,
    expected_sha256: str,
) -> os.stat_result:
    opened_metadata = os.fstat(copy_stream.fileno())
    if (
        not stat.S_ISREG(opened_metadata.st_mode)
        or opened_metadata.st_size != expected_byte_count
        or _stream_sha256(copy_stream) != expected_sha256
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY changed")
    copy_stream.seek(0)
    return opened_metadata


def _assert_copy_unchanged(
    copy_stream: Any,
    opened_metadata: os.stat_result,
    expected_sha256: str,
) -> None:
    copied_metadata = os.fstat(copy_stream.fileno())
    copy_stream.seek(0)
    if (
        copied_metadata.st_dev != opened_metadata.st_dev
        or copied_metadata.st_ino != opened_metadata.st_ino
        or copied_metadata.st_size != opened_metadata.st_size
        or copied_metadata.st_mtime_ns != opened_metadata.st_mtime_ns
        or _stream_sha256(copy_stream) != expected_sha256
    ):
        raise RuntimeError(
            "PTG V4 inferred-taxonomy compiler COPY changed during staging"
        )


def _validate_named_copy(
    path: Path,
    *,
    directory_descriptor: int,
    expected_byte_count: int,
) -> os.stat_result:
    try:
        named_metadata = os.stat(
            path.name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy compiler COPY is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(named_metadata.st_mode)
        or named_metadata.st_nlink != 1
        or named_metadata.st_uid != os.geteuid()
        or named_metadata.st_size != expected_byte_count
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY is invalid")
    return named_metadata


@contextmanager
def _open_taxonomy_copy(
    copy_path: Path,
    *,
    expected_byte_count: int,
) -> Iterator[BinaryIO]:
    try:
        path_metadata = copy_path.lstat()
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy compiler COPY is unavailable"
        ) from exc
    if copy_path.is_symlink() or not stat.S_ISREG(path_metadata.st_mode):
        raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY is invalid")
    with _open_private_artifact_parent(copy_path) as (
        resolved_parent,
        directory_descriptor,
    ):
        resolved_path = resolved_parent / copy_path.name
        named_metadata = _validate_named_copy(
            resolved_path,
            directory_descriptor=directory_descriptor,
            expected_byte_count=expected_byte_count,
        )
        if not _is_same_node(path_metadata, named_metadata):
            raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY changed")
        file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | _nofollow_flag()
        descriptor = os.open(
            copy_path.name,
            file_flags,
            dir_fd=directory_descriptor,
        )
        try:
            opened_metadata = os.fstat(descriptor)
            if not _is_same_node(named_metadata, opened_metadata):
                raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY changed")
            with os.fdopen(descriptor, "rb", closefd=False) as copy_stream:
                yield copy_stream
        finally:
            os.close(descriptor)


async def _copy_taxonomy_stage(
    session: Any,
    *,
    stage_table: str,
    copy_path: Path,
    expected_byte_count: int,
    expected_sha256: str,
) -> None:
    with _open_taxonomy_copy(
        copy_path,
        expected_byte_count=expected_byte_count,
    ) as copy_stream:
        opened_metadata = _authenticated_copy_metadata(
            copy_stream,
            expected_byte_count=expected_byte_count,
            expected_sha256=expected_sha256,
        )
        get_raw_connection = getattr(session, "get_raw_connection", None)
        if callable(get_raw_connection):
            raw_connection = await get_raw_connection()
        else:
            session_connection = await session.connection()
            raw_connection = await session_connection.get_raw_connection()
        driver_connection = getattr(
            raw_connection,
            "driver_connection",
            raw_connection,
        )
        copy_to_table = getattr(driver_connection, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError(
                "active database driver does not expose binary COPY"
            )
        await copy_to_table(
            stage_table,
            source=copy_stream,
            schema_name="pg_temp",
            columns=list(_COMPILER_STAGE_COLUMNS),
            format="binary",
        )
        _assert_copy_unchanged(
            copy_stream,
            opened_metadata,
            expected_sha256,
        )


def _validated_copy_identity(
    copy_path: str | Path,
    expected_byte_count: int,
    expected_sha256: str,
) -> tuple[Path, int, str]:
    if isinstance(expected_byte_count, bool):
        raise ValueError("PTG V4 inferred-taxonomy compiler COPY byte count is invalid")
    normalized_byte_count = int(expected_byte_count)
    if normalized_byte_count <= 0:
        raise ValueError("PTG V4 inferred-taxonomy compiler COPY byte count is invalid")
    normalized_sha256 = _strict_sha256_hex(
        expected_sha256,
        label="inferred-taxonomy compiler COPY digest",
    )
    path = Path(copy_path)
    try:
        path_metadata = path.lstat()
    except OSError as exc:
        raise RuntimeError(
            "PTG V4 inferred-taxonomy compiler COPY is unavailable"
        ) from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(path_metadata.st_mode)
        or path_metadata.st_size != normalized_byte_count
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY is invalid")
    with _open_private_artifact_parent(path) as (
        resolved_parent,
        directory_descriptor,
    ):
        resolved_path = resolved_parent / path.name
        named_metadata = _validate_named_copy(
            resolved_path,
            directory_descriptor=directory_descriptor,
            expected_byte_count=normalized_byte_count,
        )
        if not _is_same_node(path_metadata, named_metadata):
            raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY changed")
    return resolved_path, normalized_byte_count, normalized_sha256


async def stage_v4_taxonomy_copy(
    session: Any,
    *,
    copy_path: str | Path,
    expected_byte_count: int,
    expected_sha256: str,
) -> V4InferredTaxonomyCopyStage:
    """Load the selected compiler COPY into one random transaction-local stage."""

    resolved_path, normalized_byte_count, normalized_sha256 = _validated_copy_identity(
        copy_path,
        expected_byte_count,
        expected_sha256,
    )
    stage_table = _safe_stage_table_name(f"{_COMPILER_STAGE_PREFIX}{uuid.uuid4().hex}")
    stage = _quote_ident(stage_table)
    await session.execute(text(_taxonomy_stage_create_sql(stage)))
    try:
        await _copy_taxonomy_stage(
            session,
            stage_table=stage_table,
            copy_path=resolved_path,
            expected_byte_count=normalized_byte_count,
            expected_sha256=normalized_sha256,
        )
        row_count_query = await session.execute(
            text(f"SELECT COUNT(*)::bigint FROM " f"{_quote_ident('pg_temp')}.{stage}")
        )
        row_count = int(row_count_query.scalar() or 0)
        if row_count != _COMPILER_INPUT_RULE_COUNT:
            raise RuntimeError(
                "PTG V4 inferred-taxonomy compiler COPY rule count changed"
            )
    except BaseException:
        with suppress(BaseException):
            await remove_v4_taxonomy_stage(
                session,
                stage_table=stage_table,
            )
        raise
    return V4InferredTaxonomyCopyStage(
        table_name=stage_table,
        copy_path=resolved_path,
        byte_count=normalized_byte_count,
        row_count=row_count,
    )


@asynccontextmanager
async def managed_v4_taxonomy_copy_stage(
    session: Any,
    *,
    copy_path: str | Path,
    expected_byte_count: int,
    expected_sha256: str,
) -> AsyncIterator[V4InferredTaxonomyCopyStage]:
    """Retain one authenticated COPY stage only for a bounded publication."""

    stage = await stage_v4_taxonomy_copy(
        session,
        copy_path=copy_path,
        expected_byte_count=expected_byte_count,
        expected_sha256=expected_sha256,
    )
    try:
        yield stage
    except BaseException:
        with suppress(BaseException):
            await remove_v4_taxonomy_stage(
                session,
                stage_table=stage.table_name,
            )
        raise
    else:
        await remove_v4_taxonomy_stage(
            session,
            stage_table=stage.table_name,
        )


def _validated_taxonomy_root_bounds(
    npi_count: int,
    pattern_count: int,
) -> tuple[int, int]:
    if isinstance(npi_count, bool) or isinstance(pattern_count, bool):
        raise ValueError("PTG V4 inferred-taxonomy dictionary bounds are invalid")
    normalized_npi_count = int(npi_count)
    normalized_pattern_count = int(pattern_count)
    if (
        normalized_npi_count < 0
        or normalized_npi_count > 0x100000000
        or normalized_pattern_count < 0
        or normalized_pattern_count > 0x100000000
    ):
        raise ValueError("PTG V4 inferred-taxonomy dictionary bounds are invalid")
    return normalized_npi_count, normalized_pattern_count


async def _read_projection_stage(
    session: Any,
    *,
    stage_relation: str,
) -> tuple[dict[str, Any], ...]:
    stage_query = await session.execute(
        text(
            f"""
            SELECT {", ".join(_COMPILER_STAGE_COLUMNS)}
              FROM {stage_relation}
             ORDER BY rule_digest
            """
        )
    )
    return _normalized_persisted_projection_rows(stage_query)


async def _insert_projection_stage(
    session: Any,
    *,
    schema: str,
    stage_relation: str,
    table: str,
    snapshot_key: int,
) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{table} (
                snapshot_key,
                {", ".join(_COMPILER_STAGE_COLUMNS)}
            )
            SELECT :snapshot_key,
                   {", ".join(_COMPILER_STAGE_COLUMNS)}
              FROM {stage_relation}
             ORDER BY rule_digest
            ON CONFLICT DO NOTHING
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )


async def _read_persisted_projection(
    session: Any,
    *,
    schema: str,
    table: str,
    snapshot_key: int,
) -> tuple[dict[str, Any], ...]:
    persisted_query = await session.execute(
        text(
            f"""
            SELECT {", ".join(_COMPILER_STAGE_COLUMNS)}
              FROM {schema}.{table}
             WHERE snapshot_key = :snapshot_key
             ORDER BY rule_digest
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    return _normalized_persisted_projection_rows(persisted_query)


def _validate_selected_stage_rules(
    stage_rows: tuple[dict[str, Any], ...],
    normalized_rules: _NormalizedRules,
) -> None:
    expected_rule_digests = tuple(
        rule_digest for rule_digest, _rule, _codes in normalized_rules
    )
    staged_rule_digests = tuple(candidate["rule_digest"] for candidate in stage_rows)
    if (
        len(stage_rows) != _COMPILER_INPUT_RULE_COUNT
        or staged_rule_digests != expected_rule_digests
    ):
        raise RuntimeError("PTG V4 inferred-taxonomy compiler COPY rules changed")


async def _publish_locked_taxonomy_stage(
    session: Any,
    *,
    schema: str,
    stage_relation: str,
    table: str,
    snapshot_key: int,
    npi_count: int,
    pattern_count: int,
    normalized_rules: _NormalizedRules,
) -> V4InferredTaxonomyPublication:
    stage_rows = await _read_projection_stage(
        session,
        stage_relation=stage_relation,
    )
    _validate_selected_stage_rules(stage_rows, normalized_rules)
    _shape_projection_manifest(
        stage_rows,
        npi_count=npi_count,
        pattern_count=pattern_count,
    )
    await _insert_projection_stage(
        session,
        schema=schema,
        stage_relation=stage_relation,
        table=table,
        snapshot_key=snapshot_key,
    )
    stored_rows = await _read_persisted_projection(
        session,
        schema=schema,
        table=table,
        snapshot_key=snapshot_key,
    )
    if stored_rows != stage_rows:
        raise RuntimeError("PTG V4 inferred-taxonomy prepared publication changed")
    return _taxonomy_publication(
        _shape_projection_manifest(
            stored_rows,
            npi_count=npi_count,
            pattern_count=pattern_count,
        )
    )


async def publish_v4_taxonomy_stage(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    stage_table: str,
    rules: Iterable[InferredProviderTaxonomyRule],
    npi_count: int,
    pattern_count: int,
) -> V4InferredTaxonomyPublication:
    """Publish only the compiler-selected candidates without catalog reads."""

    normalized_rules = _normalized_rules(rules)
    if len(normalized_rules) != _COMPILER_INPUT_RULE_COUNT:
        raise ValueError(
            "PTG V4 inferred-taxonomy prepared publication needs exactly " "10 rules"
        )
    normalized_npi_count, normalized_pattern_count = _validated_taxonomy_root_bounds(
        npi_count, pattern_count
    )
    normalized_stage_table = _safe_stage_table_name(stage_table)
    schema = _quote_ident(schema_name)
    stage_relation = (
        f"{_quote_ident('pg_temp')}." f"{_quote_ident(normalized_stage_table)}"
    )
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)

    from process.ptg_parts.ptg2_v4_snapshot_maps import (
        lock_v4_shared_layout_for_map_write,
    )

    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    return await _publish_locked_taxonomy_stage(
        session,
        schema=schema,
        stage_relation=stage_relation,
        table=table,
        snapshot_key=snapshot_key,
        npi_count=normalized_npi_count,
        pattern_count=normalized_pattern_count,
        normalized_rules=normalized_rules,
    )


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
        catalog_rows = tuple(
            _row_mapping(catalog_result_row) for catalog_result_row in catalog_result
        )
        for catalog_row in catalog_rows:
            npi_key = int(catalog_row["npi_key"])
            npi = int(catalog_row["npi"])
            if npi_key < 0 or npi_key > 0xFFFFFFFF or npi_key >= normalized_npi_count:
                raise RuntimeError("PTG V4 inferred-taxonomy NPI key is invalid")
            if npi < 1_000_000_000 or npi > 9_999_999_999:
                raise RuntimeError("PTG V4 inferred-taxonomy NPI is invalid")
            matched_codes = frozenset(
                str(code).strip().upper()
                for code in (catalog_row.get("matched_taxonomy_codes") or ())
            )
            selected_codes = tuple(sorted(matched_codes & rule_codes))
            if not selected_codes:
                raise RuntimeError("PTG V4 inferred-taxonomy catalog evidence changed")
            evidence_by_rule_digest[rule_digest].append(
                _CandidateEvidence(npi_key, npi, selected_codes)
            )

    expected_rows: list[dict[str, Any]] = []
    for rule_digest, _rule, _rule_codes in normalized_rules:
        evidence_rows = tuple(evidence_by_rule_digest[rule_digest])
        npi_keys = tuple(evidence_row.npi_key for evidence_row in evidence_rows)
        is_candidate_cap_exceeded = (
            len(npi_keys) > PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES
        )
        observe_reason = (
            PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
            if is_candidate_cap_exceeded
            else None
        )
        observe_count_lower_bound = (
            PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
            if is_candidate_cap_exceeded
            else None
        )
        packed_candidate_keys = pack_inferred_taxonomy_npi_keys(npi_keys)
        npi_keys_by_pattern: dict[int, tuple[int, ...]] = {}
        if (
            observe_reason is None
            and root_representation == PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
            and npi_keys
        ):
            try:
                npi_keys_by_pattern = await _candidate_pattern_postings_for_rule(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(snapshot_key),
                    build_token=build_token,
                    candidate_npi_keys=npi_keys,
                    root_pattern_count=root_pattern_count,
                )
            except _PatternProjectionCapExceeded:
                observe_reason = PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
                observe_count_lower_bound = (
                    PTG2_V4_MAX_ONLINE_CANDIDATE_PATTERN_PROJECTION_MEMBERS + 1
                )
        observed_pattern_members = sum(
            len(pattern_npi_keys) for pattern_npi_keys in npi_keys_by_pattern.values()
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
        is_observe_only = observe_reason is not None
        if is_observe_only:
            npi_keys_by_pattern = {}
        pattern_payload = pack_inferred_taxonomy_pattern_npi_keys(npi_keys_by_pattern)
        if is_observe_only:
            representation = PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
            pattern_count = 0
            pattern_member_count = 0
        elif pattern_payload:
            representation = PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
            pattern_count = len(npi_keys_by_pattern)
            npi_keys_by_pattern = unpack_inferred_taxonomy_pattern_npi_keys(
                pattern_payload,
                pattern_count=pattern_count,
                pattern_member_count=observed_pattern_members,
            )
            pattern_member_count = observed_pattern_members
        else:
            representation = PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
            pattern_count = 0
            pattern_member_count = 0
            npi_keys_by_pattern = {}
        candidate_member_set = frozenset(npi_keys)
        if any(
            npi_key not in candidate_member_set
            for pattern_npi_keys in npi_keys_by_pattern.values()
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
            packed_pattern_payload=pattern_payload,
        )
        expected_rows.append(
            {
                "snapshot_key": int(snapshot_key),
                "rule_digest": rule_digest,
                "catalog_contract": (PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT),
                "catalog_digest": _catalog_digest(rule_digest, evidence_rows),
                "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
                "member_count": len(npi_keys),
                "member_digest": inferred_taxonomy_member_digest(
                    rule_digest,
                    member_count=len(npi_keys),
                    payload=packed_candidate_keys,
                ),
                "member_keys": packed_candidate_keys,
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
    stored_rows = tuple(
        _row_mapping(stored_result_row) for stored_result_row in stored_result
    )
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
                bytes(persisted_candidate_row[field_name])
                if field_name.endswith("digest")
                or field_name in {"member_keys", "pattern_member_payload"}
                else persisted_candidate_row[field_name]
            )
            for field_name in comparable_fields
        }
        for persisted_candidate_row in stored_rows
    )
    normalized_expected_rows = tuple(
        {
            field_name: expected_candidate_row[field_name]
            for field_name in comparable_fields
        }
        for expected_candidate_row in expected_rows
    )
    if normalized_stored_rows != normalized_expected_rows:
        raise RuntimeError("PTG V4 inferred-taxonomy candidate publication changed")
    manifest = _shape_projection_manifest(
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
    expected_rule_digests = tuple(
        configured_rule_entry[0] for configured_rule_entry in normalized_rules
    )
    schema = _quote_ident(schema_name)
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)
    candidate_query_result = await session.execute(
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
    candidate_summary_rows = tuple(
        _row_mapping(candidate_summary_row)
        for candidate_summary_row in candidate_query_result
    )
    observed_rule_digests = tuple(
        bytes(candidate_summary_row["rule_digest"])
        for candidate_summary_row in candidate_summary_rows
    )
    if observed_rule_digests != expected_rule_digests:
        raise RuntimeError("PTG V4 inferred-taxonomy candidate rules are incomplete")
    return _shape_projection_manifest(
        candidate_summary_rows,
        npi_count=npi_count,
        pattern_count=pattern_count,
    )


def _taxonomy_candidate_metadata_query(
    schema_name: str,
) -> Any:
    schema = _quote_ident(schema_name)
    table = _quote_ident(PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE)
    return text(
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
    )


async def _load_taxonomy_candidate_metadata(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    rule_digest: bytes,
) -> dict[str, Any]:
    metadata_result = await session.execute(
        _taxonomy_candidate_metadata_query(schema_name),
        {
            "snapshot_key": int(snapshot_key),
            "rule_digest": rule_digest,
        },
    )
    metadata_rows = tuple(
        _row_mapping(candidate_metadata) for candidate_metadata in metadata_result
    )
    if len(metadata_rows) != 1:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate vector is unavailable"
        )
    return metadata_rows[0]


def _assert_candidate_contract(metadata: Mapping[str, Any]) -> None:
    if metadata.get("root_state") != "complete":
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate snapshot is not complete"
        )
    if (
        metadata.get("catalog_contract") != PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
        or metadata.get("vector_format") != PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate contract is incompatible"
        )


def _assert_loaded_candidate_shape(
    candidate: _LoadedTaxonomyCandidate,
    *,
    member_bytes: int,
    pattern_payload_bytes: int,
) -> None:
    if (
        candidate.member_count < 0
        or member_bytes != candidate.member_count * 4
        or len(candidate.member_keys) != member_bytes
        or candidate.pattern_count < 0
        or candidate.pattern_member_count < 0
        or candidate.pattern_member_bytes < 0
        or pattern_payload_bytes != candidate.pattern_member_bytes
        or len(candidate.pattern_payload) != pattern_payload_bytes
        or candidate.npi_count < candidate.member_count
        or candidate.root_pattern_count < 0
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate metadata is inconsistent"
        )


def _loaded_taxonomy_candidate(
    metadata: Mapping[str, Any],
) -> _LoadedTaxonomyCandidate:
    """Normalize one sealed database row and reject structural drift."""

    _assert_candidate_contract(metadata)
    candidate = _LoadedTaxonomyCandidate(
        catalog_digest=_digest_bytes(
            metadata.get("catalog_digest"),
            label="catalog digest",
        ),
        member_digest=_digest_bytes(
            metadata.get("member_digest"),
            label="member digest",
        ),
        member_count=int(metadata.get("member_count") or 0),
        member_keys=bytes(metadata.get("member_keys") or b""),
        representation=str(metadata.get("representation") or ""),
        pattern_count=int(metadata.get("pattern_count") or 0),
        pattern_member_count=int(metadata.get("pattern_member_count") or 0),
        pattern_member_bytes=int(metadata.get("pattern_member_bytes") or 0),
        pattern_member_digest=_digest_bytes(
            metadata.get("pattern_member_digest"),
            label="pattern member digest",
        ),
        pattern_payload=bytes(metadata.get("pattern_member_payload") or b""),
        npi_count=int(metadata.get("npi_count") or 0),
        root_pattern_count=int(metadata.get("root_pattern_count") or 0),
    )
    member_bytes = int(metadata.get("member_bytes") or 0)
    pattern_payload_bytes = int(metadata.get("pattern_payload_bytes") or 0)
    _assert_loaded_candidate_shape(
        candidate,
        member_bytes=member_bytes,
        pattern_payload_bytes=pattern_payload_bytes,
    )
    return candidate


def _assert_candidate_matches_seal(
    candidate: _LoadedTaxonomyCandidate,
    projection_rule: V4InferredTaxonomyProjectionRule,
) -> None:
    if (
        candidate.member_count != projection_rule.member_count
        or len(candidate.member_keys) != projection_rule.packed_byte_count
        or candidate.catalog_digest != projection_rule.catalog_digest
        or candidate.member_digest != projection_rule.member_digest
        or candidate.representation != projection_rule.representation
        or candidate.pattern_count != projection_rule.pattern_count
        or candidate.pattern_member_count != projection_rule.pattern_member_count
        or candidate.pattern_member_bytes != projection_rule.pattern_member_bytes
        or candidate.pattern_member_digest != projection_rule.pattern_member_digest
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate metadata changed from its seal"
        )


def _candidate_member_vectors(
    rule_digest: bytes,
    candidate: _LoadedTaxonomyCandidate,
) -> tuple[tuple[int, ...], dict[int, tuple[int, ...]]]:
    try:
        observed_member_digest = inferred_taxonomy_member_digest(
            rule_digest,
            member_count=candidate.member_count,
            payload=candidate.member_keys,
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate payload is inconsistent"
        ) from exc
    if observed_member_digest != candidate.member_digest:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate digest changed"
        )
    try:
        observed_pattern_digest = inferred_taxonomy_pattern_member_digest(
            rule_digest,
            representation=candidate.representation,
            pattern_count=candidate.pattern_count,
            pattern_member_count=candidate.pattern_member_count,
            packed_pattern_payload=candidate.pattern_payload,
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern payload is inconsistent"
        ) from exc
    if observed_pattern_digest != candidate.pattern_member_digest:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy pattern digest changed"
        )
    return (
        unpack_inferred_taxonomy_npi_keys(
            candidate.member_keys,
            member_count=candidate.member_count,
        ),
        unpack_inferred_taxonomy_pattern_npi_keys(
            candidate.pattern_payload,
            pattern_count=candidate.pattern_count,
            pattern_member_count=candidate.pattern_member_count,
        ),
    )


def _assert_candidate_within_root(
    metadata: Mapping[str, Any],
    rule_digest: bytes,
    candidate: _LoadedTaxonomyCandidate,
) -> None:
    try:
        _shape_projection_manifest(
            (
                {
                    "rule_digest": rule_digest,
                    "catalog_contract": metadata["catalog_contract"],
                    "catalog_digest": candidate.catalog_digest,
                    "vector_format": metadata["vector_format"],
                    "member_count": candidate.member_count,
                    "member_digest": candidate.member_digest,
                    "member_keys": candidate.member_keys,
                    "representation": candidate.representation,
                    "pattern_count": candidate.pattern_count,
                    "pattern_member_count": candidate.pattern_member_count,
                    "pattern_member_bytes": candidate.pattern_member_bytes,
                    "pattern_member_digest": candidate.pattern_member_digest,
                    "pattern_member_payload": candidate.pattern_payload,
                },
            ),
            npi_count=candidate.npi_count,
            pattern_count=candidate.root_pattern_count,
        )
    except RuntimeError as exc:
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy candidate row violates its root"
        ) from exc


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
    if (
        projection_rule.member_count
        > projection_rule.max_online_inferred_taxonomy_candidates
        or projection_rule.pattern_member_count
        > projection_rule.max_online_candidate_pattern_projection_members
    ):
        raise PTG2ManifestArtifactError(
            "PTG V4 inferred-taxonomy projection exceeds the sealed online cap"
        )
    metadata = await _load_taxonomy_candidate_metadata(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        rule_digest=projection_rule.rule_digest,
    )
    candidate = _loaded_taxonomy_candidate(metadata)
    _assert_candidate_matches_seal(candidate, projection_rule)
    npi_keys, npi_keys_by_pattern = _candidate_member_vectors(
        projection_rule.rule_digest,
        candidate,
    )
    _assert_candidate_within_root(
        metadata,
        projection_rule.rule_digest,
        candidate,
    )
    return V4InferredTaxonomyCandidates(
        rule_digest=projection_rule.rule_digest,
        catalog_digest=candidate.catalog_digest,
        member_digest=candidate.member_digest,
        member_count=candidate.member_count,
        npi_keys=npi_keys,
        representation=candidate.representation,
        pattern_count=candidate.pattern_count,
        pattern_member_count=candidate.pattern_member_count,
        pattern_member_bytes=candidate.pattern_member_bytes,
        pattern_member_digest=candidate.pattern_member_digest,
        npi_keys_by_pattern=npi_keys_by_pattern,
    )


# Compatibility names preserve the explicit public workflow while keeping each
# implementation name short enough for the repository readability contract.
prepare_v4_inferred_taxonomy_compiler_input = prepare_v4_taxonomy_input
stage_v4_inferred_taxonomy_compiler_copy = stage_v4_taxonomy_copy
managed_v4_inferred_taxonomy_compiler_copy_stage = managed_v4_taxonomy_copy_stage
publish_prepared_v4_inferred_taxonomy_candidates = publish_v4_taxonomy_stage
drop_v4_inferred_taxonomy_copy_stage = remove_v4_taxonomy_stage


__all__ = [
    "PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_TABLE",
    "PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT",
    "PTG2_V4_INFERRED_TAXONOMY_COMPILER_INPUT_CONTRACT",
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
    "V4InferredTaxonomyCopyStage",
    "V4InferredTaxonomyPublication",
    "V4InferredTaxonomyProjectionRule",
    "drop_v4_inferred_taxonomy_copy_stage",
    "inferred_provider_taxonomy_rule_digest",
    "inferred_provider_taxonomy_rule_set_digest",
    "inferred_taxonomy_projection_rule_manifest",
    "inferred_taxonomy_member_digest",
    "inferred_taxonomy_pattern_member_digest",
    "load_v4_inferred_taxonomy_candidates",
    "managed_v4_inferred_taxonomy_compiler_copy_stage",
    "managed_v4_taxonomy_copy_stage",
    "pack_inferred_taxonomy_npi_keys",
    "pack_inferred_taxonomy_pattern_npi_keys",
    "prepare_v4_inferred_taxonomy_compiler_input",
    "prepare_v4_taxonomy_input",
    "publish_prepared_v4_inferred_taxonomy_candidates",
    "publish_v4_taxonomy_stage",
    "publish_v4_inferred_taxonomy_candidates",
    "remove_v4_taxonomy_stage",
    "resolve_inferred_taxonomy_projection_rule_manifest",
    "shape_v4_inferred_taxonomy_projection_manifest",
    "stage_v4_inferred_taxonomy_compiler_copy",
    "stage_v4_taxonomy_copy",
    "summarize_v4_inferred_taxonomy_candidates",
    "unpack_inferred_taxonomy_npi_keys",
    "unpack_inferred_taxonomy_pattern_npi_keys",
    "validate_v4_inferred_taxonomy_projection_manifest",
]
