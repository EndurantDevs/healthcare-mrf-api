# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared values, records, and normalization for physical projections."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Iterable, Mapping


PROJECTION_RECIPE_CONTRACT_ID = "healthporta.provider-directory.projection-recipe.v1"
PROJECTION_CONTENT_HASH_CONTRACT_ID = (
    "healthporta.provider-directory.physical-projection.v1"
)
PROJECTION_PROOF_SHARD_CONTRACT_ID = (
    "healthporta.provider-directory.projection-proof-shard.v1"
)
PROJECTION_REDUCER_BUCKET_COUNT = 16
PROJECTION_MIXED_RESOURCE_TYPE = "__mixed__"
PROJECTION_INPUT_BLOCK_MAX_BYTES = 32 * 1024 * 1024
PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS = 100_000
PROJECTION_INPUT_BLOCK_MAX_RESOURCES = 100_000
PHYSICAL_PROJECTION_REFERENCE_OWNER_KINDS = frozenset({"artifact", "build", "dataset"})
SEMANTIC_OUTCOME_FIELDS = (
    "distinct_npis",
    "individual_practitioners",
    "organization_resources",
    "address_records",
    "addressed_locations",
    "geocoded_locations",
    "practitioner_role_resources",
    "network_plan_links",
    "organization_affiliation_links",
    "specialty_records",
    "contact_records",
    "reference_links",
)

HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class ProviderDirectoryProjectionError(RuntimeError):
    """Fail closed when projection lineage or storage cannot be proven."""


class ProviderDirectoryProjectionBusy(ProviderDirectoryProjectionError):
    """Another live worker owns the exact recipe lease."""


class ProviderDirectoryProjectionLeaseLost(ProviderDirectoryProjectionError):
    """The caller no longer owns the recipe lease."""


@dataclass(frozen=True)
class ProjectionRecipeIdentity:
    recipe_id: str
    decoder_contract_id: str
    acquisition_adapter_id: str
    input_set_sha256: str
    source_scope_hash: str
    source_ids: tuple[str, ...]
    transform_contract_id: str
    scope_contract_id: str
    transform_context_hash: str
    transform_context: Mapping[str, Any]
    resource_profile_hash: str
    selected_resources: tuple[str, ...]
    required_resources: tuple[str, ...]
    completeness_manifest_hash: str
    completeness_manifest: Mapping[str, Any]

    @property
    def identity_payload(self) -> dict[str, Any]:
        """Return source-neutral physical recipe fields covered by ``recipe_id``."""

        return {
            "contract_id": PROJECTION_RECIPE_CONTRACT_ID,
            "decoder_contract_id": self.decoder_contract_id,
            "input_set_sha256": self.input_set_sha256,
            "transform_contract_id": self.transform_contract_id,
            "scope_contract_id": self.scope_contract_id,
            "transform_context_hash": self.transform_context_hash,
            "transform_context": dict(self.transform_context),
            "resource_profile_hash": self.resource_profile_hash,
            "selected_resources": list(self.selected_resources),
            "required_resources": list(self.required_resources),
            "completeness_manifest_hash": self.completeness_manifest_hash,
        }


@dataclass(frozen=True)
class ProjectionLease:
    recipe: ProjectionRecipeIdentity
    attempt: int
    lease_token: str
    stage_schema: str | None = None
    stage_relation: str | None = None
    stage_relation_oid: int | None = None


@dataclass(frozen=True)
class PhysicalProjectionReferenceLease:
    """Identity- and token-fenced ownership of one sealed projection."""

    physical_projection_id: str
    owner_kind: str
    owner_id: str
    reference_identity_hash: str
    lease_token: str


@dataclass(frozen=True)
class ProjectionInputBlock:
    """One immutable retained-input block covered by a physical recipe."""

    block_id: str
    block_ordinal: int
    upstream_artifact_id: str
    source_object_id: str
    block_kind: str
    input_contract_id: str
    source_partition_ordinal: int
    record_start: int
    record_count: int
    content_sha256: str
    payload_sha256: str
    payload_bytes: int
    summary: Mapping[str, int]
    retained_campaign_id: str
    retained_campaign_sha256: str
    retained_source_item_id: str
    retained_range_ordinal: int | None
    block_proof_sha256: str


@dataclass(frozen=True)
class ProjectionCompletenessManifest:
    """Terminal acquisition census required before physical projection."""

    manifest_sha256: str
    proof: Mapping[str, Any]


@dataclass(frozen=True)
class ProjectionShardSpec:
    """One deterministic, claimable transform unit within a recipe."""

    partition_id: str
    partition_ordinal: int
    partition_key: str
    input_block_id: str
    resource_type: str
    input_sha256: str


@dataclass(frozen=True)
class ProjectionShardClaim:
    """One lease-fenced shard generation claimed by a projection worker."""

    recipe_lease: ProjectionLease
    shard: ProjectionShardSpec
    partition_attempt: int
    lease_token: str


@dataclass(frozen=True)
class ProjectionClaim:
    lease: ProjectionLease | None
    physical_projection_id: str | None

    @property
    def is_reused(self) -> bool:
        """Return whether claim resolution avoided a new physical build."""

        return self.physical_projection_id is not None


@dataclass(frozen=True)
class ProjectionProofShard:
    recipe_id: str
    attempt: int
    partition_attempt: int
    partition_id: str
    partition_ordinal: int
    resource_type: str
    input_sha256: str
    canonical_row_sha256: str
    resource_count: int
    first_identity: tuple[str, str]
    last_identity: tuple[str, str]
    proof: dict[str, Any]

    @property
    def descriptor(self) -> dict[str, Any]:
        """Return the canonical shard fields included in the Merkle root."""

        return {
            "partition_id": self.partition_id,
            "partition_ordinal": self.partition_ordinal,
            "resource_type": self.resource_type,
            "input_sha256": self.input_sha256,
            "canonical_row_sha256": self.canonical_row_sha256,
            "resource_count": self.resource_count,
            "first_identity": list(self.first_identity),
            "last_identity": list(self.last_identity),
        }


@dataclass(frozen=True)
class ProjectionSemanticOutcomeProof:
    """Reducer-derived semantic outcomes for one exact physical winner set."""

    canonical_row_sha256: str
    profile_contribution_sha256: str
    distinct_npi_sha256: str
    resource_count: int
    resource_counts: dict[str, int]
    outcome_counts: dict[str, int]
    proof_sha256: str
    proof: dict[str, Any]


@dataclass(frozen=True)
class PhysicalProjectionProof:
    physical_projection_id: str
    canonical_row_sha256: str
    dataset_hash: str
    resource_count: int
    resource_counts: dict[str, int]
    proof: dict[str, Any]


@dataclass(frozen=True)
class ProjectionStage:
    schema: str
    relation: str
    relation_oid: int
    profile_contribution_stage: ProjectionStage | None = None


@dataclass(frozen=True)
class PreparedProjectionStage:
    stage: ProjectionStage
    storage_trigger_oid: int
    proof: PhysicalProjectionProof
    membership_stage: ProjectionStage | None = None
    membership_storage_trigger_oid: int | None = None
    profile_contribution_stage: ProjectionStage | None = None
    profile_contribution_storage_trigger_oid: int | None = None


def stable_json(document: Any) -> str:
    """Encode a deterministic JSON document."""

    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def stable_hash(document: Any, *, domain: str) -> str:
    """Hash a canonical document under an explicit domain separator."""

    digest = hashlib.sha256()
    digest.update(domain.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(stable_json(document).encode("utf-8"))
    return digest.hexdigest()


def required_hash(candidate: Any, field_name: str) -> str:
    """Normalize one lowercase SHA-256 value or fail closed."""

    normalized_hash = str(candidate or "").strip().lower()
    if HASH_PATTERN.fullmatch(normalized_hash) is None:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return normalized_hash


def required_text(candidate: Any, field_name: str, *, limit: int = 128) -> str:
    """Normalize one bounded nonempty identity component."""

    normalized_text = str(candidate or "").strip()
    if not normalized_text or len(normalized_text) > limit:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_invalid"
        )
    return normalized_text


def sorted_unique_texts(
    candidates: Iterable[Any],
    field_name: str,
    *,
    limit: int = 128,
) -> tuple[str, ...]:
    """Return a nonempty sorted set of bounded text identities."""

    normalized_texts = tuple(
        sorted(
            {
                required_text(candidate, field_name, limit=limit)
                for candidate in candidates
            }
        )
    )
    if not normalized_texts:
        raise ProviderDirectoryProjectionError(
            f"provider_directory_projection_{field_name}_empty"
        )
    return normalized_texts
