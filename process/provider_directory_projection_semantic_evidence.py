# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate candidate semantic rows and their paired Profile-shaped evidence.

This module validates caller-provided shapes and pair consistency.  It does not
establish that values were natively decoded from retained FHIR source bytes and
must not be used as a publication attestation boundary.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import hashlib
import json
from typing import Any

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    required_hash,
    stable_json,
)
from process.provider_directory_projection_typed_evidence import (
    SEMANTIC_EVIDENCE_HASH_FIELD,
    SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID as SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID,
    evidence_derived_semantic_summary,
    normalized_semantic_evidence,
)


# Historical wire identifier for a candidate contribution shape, not a native
# retained-byte attestation contract.
SEMANTIC_CONTRIBUTION_CONTRACT_ID = (
    "healthporta.provider-directory.semantic-contribution.v2"
)
_STORED_SEMANTIC_CONTRIBUTION_COLUMNS = (
    "summary_npi",
    "summary_address_count",
    "summary_addressed_location",
    "summary_geocoded_location",
    "summary_network_link_count",
    "summary_affiliation_link_count",
)
SEMANTIC_CONTRIBUTION_COLUMNS = (
    *_STORED_SEMANTIC_CONTRIBUTION_COLUMNS,
    SEMANTIC_EVIDENCE_HASH_FIELD,
)
PROFILE_CONTRIBUTION_ARRAY_FIELDS = (
    ("names", "names_json"),
    ("specialties", "specialties_json"),
    ("contacts", "contacts_json"),
    ("addresses", "addresses_json"),
    ("references", "references_json"),
)
_NPI_RESOURCE_TYPES = frozenset(
    {
        "HealthcareService",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    }
)
_MAX_INT64 = 2**63 - 1
NPI_ACCUMULATOR_MODULUS = 2**256 - 2**32 - 977
_MAX_PROFILE_ARRAY_ITEMS = 10_000
_MAX_PROFILE_ARRAY_BYTES = 4 * 1024 * 1024
_RESOURCE_REQUIRED_FIELDS = (
    "resource_type",
    "resource_id",
    "proof_partition_id",
    "payload_hash",
    "source_rank",
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
    "profile_evidence_json",
)
_PROFILE_REQUIRED_FIELDS = (
    "resource_type",
    "resource_id",
    "proof_partition_id",
    "payload_hash",
    "source_rank",
    "direct_npi",
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
    *(column_name for _evidence_name, column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS),
)


class SemanticStableListDigest:
    """Hash one canonical JSON list without retaining its entries."""

    def __init__(self, domain: str) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(domain.encode("utf-8"))
        self._digest.update(b"\x00[")
        self._entry_count = 0

    def append(self, entry: Any) -> None:
        """Append one already normalized canonical list entry."""

        if self._entry_count:
            self._digest.update(b",")
        self._digest.update(stable_json(entry).encode("utf-8"))
        self._entry_count += 1

    def hexdigest(self) -> str:
        """Return the digest of the canonical list without closing the stream."""

        completed_digest = self._digest.copy()
        completed_digest.update(b"]")
        return completed_digest.hexdigest()


def semantic_identity(candidate: Mapping[str, Any]) -> tuple[str, str]:
    """Return the canonical winner identity from a normalized row."""

    return candidate["resource_type"], candidate["resource_id"]


def canonical_semantic_resource_row(resource: Mapping[str, Any]) -> bytes:
    """Frame one normalized semantic winner for the canonical row digest."""

    framed_fields = [
        resource["resource_type"],
        resource["resource_id"],
        resource["payload_hash"],
        resource["source_rank"],
        *(resource[field_name] for field_name in SEMANTIC_CONTRIBUTION_COLUMNS),
    ]
    return stable_json(framed_fields).encode("utf-8")


def _strict_text(raw_text: Any, field_name: str, *, limit: int) -> str:
    if (
        not isinstance(raw_text, str)
        or not raw_text
        or raw_text != raw_text.strip()
        or len(raw_text) > limit
    ):
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return raw_text


def _optional_text(raw_text: Any, field_name: str) -> str | None:
    if raw_text is None:
        return None
    return _strict_text(raw_text, field_name, limit=64)


def _optional_bool(raw_flag: Any, field_name: str) -> bool | None:
    if raw_flag is not None and type(raw_flag) is not bool:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return raw_flag


def _normalized_json_array(raw_array: Any, field_name: str) -> list[Any]:
    if not isinstance(raw_array, list) or len(raw_array) > _MAX_PROFILE_ARRAY_ITEMS:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    try:
        encoded_array = json.dumps(
            raw_array,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        normalized_array = json.loads(encoded_array)
    except (RecursionError, TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        ) from error
    if (
        len(encoded_array.encode("utf-8")) > _MAX_PROFILE_ARRAY_BYTES
        or normalized_array != raw_array
    ):
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return normalized_array


def _normalized_inline_profile(resource: Mapping[str, Any]) -> dict[str, list[Any]]:
    raw_evidence_by_name = resource.get("profile_evidence_json")
    if raw_evidence_by_name is None:
        return {
            evidence_name: []
            for evidence_name, _column in PROFILE_CONTRIBUTION_ARRAY_FIELDS
        }
    if not isinstance(raw_evidence_by_name, dict):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_profile_evidence_invalid"
        )
    allowed_fields = {name for name, _column in PROFILE_CONTRIBUTION_ARRAY_FIELDS}
    if not raw_evidence_by_name or set(raw_evidence_by_name) - allowed_fields:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_profile_evidence_invalid"
        )
    normalized_evidence_by_name: dict[str, list[Any]] = {}
    for evidence_name, _column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS:
        if evidence_name not in raw_evidence_by_name:
            normalized_evidence_by_name[evidence_name] = []
            continue
        normalized_array = _normalized_json_array(
            raw_evidence_by_name[evidence_name],
            f"profile_{evidence_name}",
        )
        if not normalized_array:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_profile_evidence_invalid"
            )
        normalized_evidence_by_name[evidence_name] = normalized_array
    return normalized_evidence_by_name


def _semantic_contribution_from_evidence(
    resource: Mapping[str, Any],
    profile_evidence: Mapping[str, list[Any]],
) -> dict[str, Any]:
    resource_type = _strict_text(
        resource.get("resource_type"),
        "resource_type",
        limit=64,
    )
    raw_npi = resource.get("summary_npi")
    if raw_npi is not None and (
        type(raw_npi) is not int
        or not 1_000_000_000 <= raw_npi <= 2_999_999_999
        or resource_type not in _NPI_RESOURCE_TYPES
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_summary_npi_invalid"
        )
    return {
        "summary_npi": raw_npi,
        **evidence_derived_semantic_summary(
            resource_type,
            resource,
            profile_evidence,
        ),
    }


def normalized_semantic_contribution(
    resource: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate evidence-derived semantic contribution for one physical fact."""

    if set(_STORED_SEMANTIC_CONTRIBUTION_COLUMNS) - set(resource):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_contribution_missing"
        )
    profile_evidence = _normalized_inline_profile(resource)
    return _semantic_contribution_from_evidence(resource, profile_evidence)


def _normalized_shared_identity(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "resource_type": _strict_text(
            candidate.get("resource_type"),
            "resource_type",
            limit=64,
        ),
        "resource_id": _strict_text(
            candidate.get("resource_id"),
            "resource_id",
            limit=256,
        ),
        "proof_partition_id": required_hash(
            candidate.get("proof_partition_id"),
            "proof_partition_id",
        ),
        "payload_hash": required_hash(candidate.get("payload_hash"), "payload_hash"),
        "source_rank": _strict_text(
            candidate.get("source_rank"),
            "source_rank",
            limit=512,
        ),
        "active": _optional_bool(candidate.get("active"), "active"),
        "effective_start": _optional_text(
            candidate.get("effective_start"),
            "effective_start",
        ),
        "effective_end": _optional_text(
            candidate.get("effective_end"),
            "effective_end",
        ),
        "observed_at": _optional_text(candidate.get("observed_at"), "observed_at"),
    }


def normalized_semantic_winner(resource: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one final physical winner with its inline semantic evidence."""

    if not isinstance(resource, Mapping) or set(_RESOURCE_REQUIRED_FIELDS) - set(
        resource
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_resource_fields_missing"
        )
    profile_evidence = _normalized_inline_profile(resource)
    return {
        **_normalized_shared_identity(resource),
        **_semantic_contribution_from_evidence(resource, profile_evidence),
        "profile_evidence": profile_evidence,
    }


def normalized_profile_contribution(
    contribution: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize the exact Profile row paired to one final physical winner."""

    if not isinstance(contribution, Mapping) or set(_PROFILE_REQUIRED_FIELDS) - set(
        contribution
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_profile_contribution_fields_missing"
        )
    direct_npi = contribution.get("direct_npi")
    if direct_npi is not None and (
        type(direct_npi) is not int
        or not 1_000_000_000 <= direct_npi <= 2_999_999_999
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_direct_npi_invalid"
        )
    normalized_contribution_map = {
        **_normalized_shared_identity(contribution),
        "direct_npi": direct_npi,
    }
    for evidence_name, column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS:
        normalized_contribution_map[evidence_name] = _normalized_json_array(
            contribution.get(column_name),
            column_name,
        )
    normalized_semantic_evidence(
        normalized_contribution_map["resource_type"],
        {
            evidence_name: normalized_contribution_map[evidence_name]
            for evidence_name, _column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS
        },
    )
    return normalized_contribution_map


def validate_semantic_pair(
    resource: Mapping[str, Any],
    contribution: Mapping[str, Any],
) -> None:
    """Require exact identity, typed value, and array agreement for one pair."""

    shared_fields = (
        "resource_type",
        "resource_id",
        "proof_partition_id",
        "payload_hash",
        "source_rank",
        "active",
        "effective_start",
        "effective_end",
        "observed_at",
    )
    if any(resource[field] != contribution[field] for field in shared_fields) or (
        resource["summary_npi"] != contribution["direct_npi"]
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_pair_identity_mismatch"
        )
    for evidence_name, _column_name in PROFILE_CONTRIBUTION_ARRAY_FIELDS:
        if resource["profile_evidence"][evidence_name] != contribution[evidence_name]:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_semantic_pair_evidence_mismatch"
            )
    if len(contribution["addresses"]) != resource["summary_address_count"]:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_semantic_pair_address_count_mismatch"
        )


def normalized_semantic_pair_stream(
    semantic_pairs: Iterable[
        tuple[Mapping[str, Any], Mapping[str, Any]]
    ],
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    """Normalize one ordered winner/Profile pair at a time."""

    for raw_pair in semantic_pairs:
        try:
            raw_resource, raw_contribution = raw_pair
        except (TypeError, ValueError) as error:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_semantic_pair_invalid"
            ) from error
        yield (
            normalized_semantic_winner(raw_resource),
            normalized_profile_contribution(raw_contribution),
        )


def npi_occurrence_factor(npi: int) -> int:
    """Map a validated NPI to the streaming multiset accumulator field."""

    digest = hashlib.sha256()
    digest.update(b"provider-directory-projection-npi-occurrence-v1\x00")
    digest.update(str(npi).encode("ascii"))
    return int.from_bytes(digest.digest(), "big") % (
        NPI_ACCUMULATOR_MODULUS - 1
    ) + 1


def _normalized_npi_occurrence(raw_group: Any) -> tuple[int, int]:
    try:
        npi, occurrence_count = raw_group
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_npi_occurrence_invalid"
        ) from error
    if (
        type(npi) is not int
        or not 1_000_000_000 <= npi <= 2_999_999_999
        or type(occurrence_count) is not int
        or occurrence_count < 1
        or occurrence_count > _MAX_INT64
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_npi_occurrence_invalid"
        )
    return npi, occurrence_count


def streamed_npi_proof(
    npi_occurrences: Iterable[tuple[int, int]],
) -> tuple[int, int, str, int]:
    """Validate and commit a strict set-based NPI occurrence stream."""

    npi_digest = SemanticStableListDigest(
        "provider-directory-projection-distinct-npi-set-v1"
    )
    occurrence_total = 0
    occurrence_accumulator = 1
    previous_npi: int | None = None
    distinct_count = 0
    for raw_group in npi_occurrences:
        npi, occurrence_count = _normalized_npi_occurrence(raw_group)
        if previous_npi is not None and npi <= previous_npi:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_npi_occurrences_not_strictly_sorted"
            )
        occurrence_total += occurrence_count
        if occurrence_total > _MAX_INT64:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_npi_occurrence_count_invalid"
            )
        occurrence_accumulator = (
            occurrence_accumulator
            * pow(npi_occurrence_factor(npi), occurrence_count, NPI_ACCUMULATOR_MODULUS)
        ) % NPI_ACCUMULATOR_MODULUS
        npi_digest.append(npi)
        distinct_count += 1
        previous_npi = npi
    return (
        occurrence_total,
        distinct_count,
        npi_digest.hexdigest(),
        occurrence_accumulator,
    )


__all__ = [
    "PROFILE_CONTRIBUTION_ARRAY_FIELDS",
    "NPI_ACCUMULATOR_MODULUS",
    "SEMANTIC_CONTRIBUTION_COLUMNS",
    "SEMANTIC_CONTRIBUTION_CONTRACT_ID",
    "SemanticStableListDigest",
    "canonical_semantic_resource_row",
    "normalized_semantic_pair_stream",
    "normalized_profile_contribution",
    "normalized_semantic_contribution",
    "normalized_semantic_winner",
    "npi_occurrence_factor",
    "semantic_identity",
    "streamed_npi_proof",
    "validate_semantic_pair",
]
