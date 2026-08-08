# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sealed per-root replay evidence for reviewed FHIR subsets."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_canonical import (
    ALLOWED_SUBSET_RESOURCE_TYPES,
    _is_sha256,
    canonical_sha256,
    validate_subset_completion_proof_pair,
)


SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_VERSION = (
    "provider-directory-fhir-server-issued-replay-evidence-v1"
)
_REPLAY_EVIDENCE_FIELDS = frozenset(
    {"version", "completion_proof_sha256", "resources"}
)
_REPLAY_RESOURCE_FIELDS = frozenset(
    {
        "pages",
        "continuation_hop_sha256",
        "continuation_hop_chain_sha256",
        "continuation_shape_sha256",
        "continuation_shape_chain_sha256",
    }
)


def _replay_resource_evidence(
    execution_proof: Mapping[str, Any],
    canonical_resource: Mapping[str, Any],
) -> dict[str, Any]:
    hop_hashes = execution_proof.get("continuation_hop_sha256")
    shape_hashes = execution_proof.get("continuation_shape_sha256")
    expected_hops = canonical_resource["pages"] - 1
    if (
        type(hop_hashes) is not list
        or type(shape_hashes) is not list
        or len(hop_hashes) != expected_hops
        or len(shape_hashes) != expected_hops
        or any(not _is_sha256(hop_digest) for hop_digest in hop_hashes)
        or shape_hashes != canonical_resource["continuation_shape_sha256"]
    ):
        raise ValueError("provider_directory_subset_replay_evidence_invalid")
    return {
        "pages": canonical_resource["pages"],
        "continuation_hop_sha256": list(hop_hashes),
        "continuation_hop_chain_sha256": canonical_sha256(hop_hashes),
        "continuation_shape_sha256": list(shape_hashes),
        "continuation_shape_chain_sha256": canonical_sha256(shape_hashes),
    }


def build_subset_replay_evidence(
    *,
    resource_proof_by_type: Mapping[str, Mapping[str, Any]],
    completion_proof: Mapping[str, Any],
    completion_sha256: str,
) -> tuple[dict[str, Any], str]:
    """Build sealed per-root evidence without placing it in twin equality."""

    canonical_proof, canonical_sha256_value = (
        validate_subset_completion_proof_pair(
            completion_proof,
            completion_sha256,
        )
    )
    if set(resource_proof_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES:
        raise ValueError("provider_directory_subset_replay_evidence_invalid")
    replay_resource_by_type = {
        resource_type: _replay_resource_evidence(
            resource_proof_by_type[resource_type],
            canonical_proof["resources"][resource_type],
        )
        for resource_type in sorted(ALLOWED_SUBSET_RESOURCE_TYPES)
    }
    replay_evidence_by_field = {
        "version": SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_VERSION,
        "completion_proof_sha256": canonical_sha256_value,
        "resources": replay_resource_by_type,
    }
    replay_sha256 = canonical_sha256(replay_evidence_by_field)
    validate_subset_replay_evidence_pair(
        replay_evidence_by_field,
        replay_sha256,
        canonical_proof,
        canonical_sha256_value,
    )
    return replay_evidence_by_field, replay_sha256


def _validate_replay_resource(
    resource_evidence: Any,
    canonical_resource: Mapping[str, Any],
) -> None:
    if not isinstance(resource_evidence, Mapping):
        raise ValueError("provider_directory_subset_replay_evidence_invalid")
    hop_hashes = resource_evidence.get("continuation_hop_sha256")
    shape_hashes = resource_evidence.get("continuation_shape_sha256")
    if (
        set(resource_evidence) != _REPLAY_RESOURCE_FIELDS
        or resource_evidence.get("pages") != canonical_resource["pages"]
        or type(hop_hashes) is not list
        or len(hop_hashes) != canonical_resource["pages"] - 1
        or any(not _is_sha256(hop_digest) for hop_digest in hop_hashes)
        or canonical_sha256(hop_hashes)
        != resource_evidence.get("continuation_hop_chain_sha256")
        or shape_hashes != canonical_resource["continuation_shape_sha256"]
        or canonical_sha256(shape_hashes)
        != resource_evidence.get("continuation_shape_chain_sha256")
    ):
        raise ValueError("provider_directory_subset_replay_evidence_invalid")


def validate_subset_replay_evidence_pair(
    replay_evidence: Any,
    replay_sha256: Any,
    completion_proof: Any,
    completion_sha256: Any,
) -> tuple[dict[str, Any], str]:
    """Validate one root's sealed opaque-hop evidence against neutral proof."""

    canonical_proof, canonical_sha256_value = (
        validate_subset_completion_proof_pair(
            completion_proof,
            completion_sha256,
        )
    )
    if (
        not isinstance(replay_evidence, Mapping)
        or set(replay_evidence) != _REPLAY_EVIDENCE_FIELDS
        or replay_evidence.get("version")
        != SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_VERSION
        or replay_evidence.get("completion_proof_sha256")
        != canonical_sha256_value
        or not _is_sha256(replay_sha256)
        or canonical_sha256(replay_evidence) != replay_sha256
    ):
        raise ValueError("provider_directory_subset_replay_evidence_invalid")
    replay_resource_by_type = replay_evidence.get("resources")
    if (
        not isinstance(replay_resource_by_type, Mapping)
        or set(replay_resource_by_type) != ALLOWED_SUBSET_RESOURCE_TYPES
    ):
        raise ValueError("provider_directory_subset_replay_evidence_invalid")
    for resource_type in ALLOWED_SUBSET_RESOURCE_TYPES:
        _validate_replay_resource(
            replay_resource_by_type[resource_type],
            canonical_proof["resources"][resource_type],
        )
    return dict(replay_evidence), replay_sha256
