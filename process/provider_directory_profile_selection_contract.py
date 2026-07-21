# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-service contracts for global Provider Directory Profile selection."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from process import provider_directory_profile as profile_artifact


PROFILE_SELECTION_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation-request.v1"
)
PROFILE_SELECTION_ATTESTATION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-attestation.v1"
)
PROFILE_SELECTION_RESULT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-selection-result.v1"
)
PROFILE_EXECUTION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-execution.v1"
)
PROFILE_SELECTION_LINEAGE_AUTHORITY = (
    "healthcare-mrf-api.provider-directory-selection-proof.v1"
)
PROFILE_SELECTION_RESULT_METRIC = "profile_selection_result"

_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PAIR_FIELDS = {
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_hash",
    "acquisition_root_run_id",
    "publication_status",
    "is_current",
    "lineage_authority",
}
_ATTESTATION_FIELDS = {
    "contract_id",
    "proof_id",
    "node_id",
    "catalog_digest",
    "selection_fingerprint",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_context_digest",
    "profile_input_digest",
    "operation",
    "pairs",
}
_GLOBAL_PROFILE_PARAMS = {
    "publish_artifacts_only": True,
    "publish_artifacts_targets": ["profile"],
    "source_ids": [],
    "require_complete_global_profile_fence": True,
    "publish_corroboration": False,
    "probe": False,
    "import_resources": False,
    "provider_directory_profile_contract_id": PROFILE_EXECUTION_CONTRACT_ID,
}


class ProviderDirectoryProfileSelectionError(ValueError):
    """Report malformed Profile selection input or proof data."""


class ProviderDirectoryProfileSelectionDrift(RuntimeError):
    """Report a caller selection that differs from authoritative state."""


class ProviderDirectoryProfileSelectionStale(RuntimeError):
    """Report a registered proof that no longer matches live state."""


@dataclass(frozen=True)
class ProviderDirectoryProfileSelectionAttestation:
    """Validated immutable global Profile selection proof."""

    proof_id: str
    node_id: str
    catalog_digest: str
    selection_fingerprint: str
    authority_revision: int
    profile_schema_version: int
    profile_strategy_version: str
    source_context_digest: str
    profile_input_digest: str
    operation: str
    pairs: tuple[dict[str, Any], ...]
    payload: dict[str, Any]


@dataclass(frozen=True)
class ProviderDirectoryProfileExecution:
    """Validated proof plus its exact durable outbox generation."""

    attestation: ProviderDirectoryProfileSelectionAttestation
    generation: int


def stable_hash(payload: Any, *, domain: str) -> str:
    """Match the peer control plane's canonical JSON identity hash."""

    body = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(f"{domain}:{body}".encode("utf-8")).hexdigest()


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned_text = value.strip()
    return cleaned_text or None


def _required_text(
    value_map: Mapping[str, Any],
    name: str,
    *,
    limit: int,
) -> str:
    raw_text = value_map.get(name)
    cleaned_text = _clean_text(raw_text)
    if cleaned_text is None or cleaned_text != raw_text or len(cleaned_text) > limit:
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return cleaned_text


def _required_hash(value_map: Mapping[str, Any], name: str) -> str:
    digest = _required_text(value_map, name, limit=64)
    if not _HASH_PATTERN.fullmatch(digest):
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return digest


def _positive_integer(value_map: Mapping[str, Any], name: str) -> int:
    integer_value = value_map.get(name)
    if (
        not isinstance(integer_value, int)
        or isinstance(integer_value, bool)
        or integer_value < 1
    ):
        raise ProviderDirectoryProfileSelectionError(
            f"Profile selection {name} is invalid"
        )
    return integer_value


def configured_node_id() -> str:
    """Return the independently configured healthcare import-node identity."""

    node_id = _clean_text(os.getenv("HLTHPRT_IMPORT_NODE_ID"))
    if node_id is None or len(node_id) > 64:
        raise RuntimeError("provider_directory_profile_selection_node_not_configured")
    return node_id


def _proof_id(attestation_map: Mapping[str, Any]) -> str:
    proof_identity_map = {
        name: attestation_map[name]
        for name in sorted(
            _ATTESTATION_FIELDS - {"proof_id", "authority_revision"}
        )
    }
    return stable_hash(
        proof_identity_map,
        domain="provider_directory_profile_selection_attestation.v1",
    )


def _validated_pair(raw_pair: Any) -> dict[str, Any]:
    if not isinstance(raw_pair, Mapping) or set(raw_pair) != _PAIR_FIELDS:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pair fields are invalid"
        )
    pair_map = dict(raw_pair)
    normalized_pair_map = {
        "source_id": _required_text(pair_map, "source_id", limit=96),
        "endpoint_id": _required_text(pair_map, "endpoint_id", limit=128),
        "dataset_id": _required_text(pair_map, "dataset_id", limit=128),
        "dataset_hash": _required_hash(pair_map, "dataset_hash"),
        "acquisition_root_run_id": _required_text(
            pair_map,
            "acquisition_root_run_id",
            limit=64,
        ),
        "publication_status": pair_map.get("publication_status"),
        "is_current": pair_map.get("is_current"),
        "lineage_authority": pair_map.get("lineage_authority"),
    }
    if (
        normalized_pair_map["publication_status"] != "published"
        or normalized_pair_map["is_current"] is not True
        or normalized_pair_map["lineage_authority"]
        != PROFILE_SELECTION_LINEAGE_AUTHORITY
    ):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pair publication authority is invalid"
        )
    return normalized_pair_map


def _normalized_attestation_pairs(
    attestation_map: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    raw_pairs = attestation_map.get("pairs")
    if not isinstance(raw_pairs, list):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pairs are invalid"
        )
    normalized_pairs = [_validated_pair(raw_pair) for raw_pair in raw_pairs]
    sorted_pairs = sorted(
        normalized_pairs,
        key=lambda pair_map: (
            pair_map["source_id"],
            pair_map["dataset_id"],
            pair_map["endpoint_id"],
        ),
    )
    pair_keys = [
        (pair_map["source_id"], pair_map["dataset_id"])
        for pair_map in normalized_pairs
    ]
    if normalized_pairs != sorted_pairs or len(pair_keys) != len(set(pair_keys)):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection pairs are not unique and sorted"
        )
    return ("publish" if normalized_pairs else "purge"), normalized_pairs


def _normalized_attestation_map(
    attestation_map: Mapping[str, Any],
) -> dict[str, Any]:
    operation, normalized_pairs = _normalized_attestation_pairs(attestation_map)
    return {
        "contract_id": PROFILE_SELECTION_ATTESTATION_CONTRACT_ID,
        "proof_id": _required_hash(attestation_map, "proof_id"),
        "node_id": _required_text(attestation_map, "node_id", limit=64),
        "catalog_digest": _required_hash(attestation_map, "catalog_digest"),
        "selection_fingerprint": _required_hash(
            attestation_map,
            "selection_fingerprint",
        ),
        "authority_revision": _positive_integer(
            attestation_map,
            "authority_revision",
        ),
        "profile_schema_version": _positive_integer(
            attestation_map,
            "profile_schema_version",
        ),
        "profile_strategy_version": _required_text(
            attestation_map,
            "profile_strategy_version",
            limit=128,
        ),
        "source_context_digest": _required_hash(
            attestation_map,
            "source_context_digest",
        ),
        "profile_input_digest": _required_hash(
            attestation_map,
            "profile_input_digest",
        ),
        "operation": operation,
        "pairs": normalized_pairs,
    }


def validated_profile_selection_attestation(
    raw_attestation: Any,
) -> ProviderDirectoryProfileSelectionAttestation:
    """Validate the exact immutable attestation response contract."""

    if (
        not isinstance(raw_attestation, Mapping)
        or set(raw_attestation) != _ATTESTATION_FIELDS
        or raw_attestation.get("contract_id")
        != PROFILE_SELECTION_ATTESTATION_CONTRACT_ID
    ):
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection attestation fields are invalid"
        )
    attestation_map = dict(raw_attestation)
    normalized_map = _normalized_attestation_map(attestation_map)
    if attestation_map != normalized_map:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection attestation is not canonical"
        )
    if _proof_id(normalized_map) != normalized_map["proof_id"]:
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection proof_id is invalid"
        )
    return ProviderDirectoryProfileSelectionAttestation(
        proof_id=normalized_map["proof_id"],
        node_id=normalized_map["node_id"],
        catalog_digest=normalized_map["catalog_digest"],
        selection_fingerprint=normalized_map["selection_fingerprint"],
        authority_revision=normalized_map["authority_revision"],
        profile_schema_version=normalized_map["profile_schema_version"],
        profile_strategy_version=normalized_map["profile_strategy_version"],
        source_context_digest=normalized_map["source_context_digest"],
        profile_input_digest=normalized_map["profile_input_digest"],
        operation=normalized_map["operation"],
        pairs=tuple(dict(pair_map) for pair_map in normalized_map["pairs"]),
        payload=normalized_map,
    )


def _identity_without_authority(
    attestation_map: Mapping[str, Any],
) -> dict[str, Any]:
    identity_fields = (
        "contract_id",
        "node_id",
        "catalog_digest",
        "selection_fingerprint",
        "profile_schema_version",
        "profile_strategy_version",
        "source_context_digest",
        "profile_input_digest",
        "operation",
        "pairs",
    )
    return {name: attestation_map[name] for name in identity_fields}


def validated_profile_execution(
    task_map: Mapping[str, Any],
) -> ProviderDirectoryProfileExecution:
    """Validate the full proof-bearing global Profile execution contract."""

    for field_name, expected_value in _GLOBAL_PROFILE_PARAMS.items():
        if task_map.get(field_name) != expected_value:
            raise ProviderDirectoryProfileSelectionError(
                f"Profile execution {field_name} is invalid"
            )
    generation = _positive_integer(
        task_map,
        "provider_directory_profile_generation",
    )
    attestation = validated_profile_selection_attestation(
        task_map.get("provider_directory_profile_selection_attestation")
    )
    if attestation.node_id != configured_node_id():
        raise ProviderDirectoryProfileSelectionError(
            "Profile selection node does not match this engine"
        )
    return ProviderDirectoryProfileExecution(attestation, generation)


def _profile_result_counts(
    execution: ProviderDirectoryProfileExecution,
    profile_rows: int,
    evidence_rows: int,
) -> dict[str, int]:
    for row_count in (profile_rows, evidence_rows):
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            raise ProviderDirectoryProfileSelectionError(
                "Profile result row count is invalid"
            )
    if execution.attestation.operation == "purge" and (profile_rows or evidence_rows):
        raise ProviderDirectoryProfileSelectionError(
            "Profile purge result is not empty"
        )
    return {
        "profile_rows": profile_rows,
        "profile_source_evidence_rows": evidence_rows,
        "source_count": len(execution.attestation.pairs),
        "dataset_count": len({
            pair_map["dataset_id"] for pair_map in execution.attestation.pairs
        }),
    }


def profile_selection_result(
    execution: ProviderDirectoryProfileExecution,
    *,
    profile_generation_id: str,
    profile_rows: int,
    profile_source_evidence_rows: int,
) -> dict[str, Any]:
    """Build strict terminal evidence for one promoted Profile generation."""

    generation_id = _clean_text(profile_generation_id)
    if generation_id is None or len(generation_id) > 128:
        raise ProviderDirectoryProfileSelectionError(
            "Profile result generation identity is invalid"
        )
    attestation = execution.attestation
    identity_fields = (
        "proof_id",
        "node_id",
        "catalog_digest",
        "selection_fingerprint",
        "authority_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "source_context_digest",
        "profile_input_digest",
        "operation",
        "pairs",
    )
    return {
        "contract_id": PROFILE_SELECTION_RESULT_CONTRACT_ID,
        **{name: attestation.payload[name] for name in identity_fields},
        "status": "published" if attestation.operation == "publish" else "purged",
        "generation": execution.generation,
        "profile_generation_id": generation_id,
        "row_counts": _profile_result_counts(
            execution,
            profile_rows,
            profile_source_evidence_rows,
        ),
    }
