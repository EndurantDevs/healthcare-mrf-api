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
class PhysicalProjectionRecipeIdentity:
    """Only fields allowed to influence shared physical output."""

    recipe_id: str
    decoder_contract_id: str
    input_set_sha256: str
    transform_contract_id: str
    scope_contract_id: str
    transform_context_hash: str
    transform_context: Mapping[str, Any]
    resource_profile_hash: str
    selected_resources: tuple[str, ...]
    required_resources: tuple[str, ...]

    @property
    def identity_payload(self) -> dict[str, Any]:
        """Return source-neutral fields covered by ``recipe_id``."""

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
        }


@dataclass(frozen=True)
class ProjectionRecipeIdentity:
    """Planning identity containing separate physical and admission context."""

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
    def physical(self) -> PhysicalProjectionRecipeIdentity:
        """Return the campaign-blind view passed to workers and reducers."""

        return PhysicalProjectionRecipeIdentity(
            recipe_id=self.recipe_id,
            decoder_contract_id=self.decoder_contract_id,
            input_set_sha256=self.input_set_sha256,
            transform_contract_id=self.transform_contract_id,
            scope_contract_id=self.scope_contract_id,
            transform_context_hash=self.transform_context_hash,
            transform_context=dict(self.transform_context),
            resource_profile_hash=self.resource_profile_hash,
            selected_resources=self.selected_resources,
            required_resources=self.required_resources,
        )

    @property
    def identity_payload(self) -> dict[str, Any]:
        """Preserve the physical identity payload for planning validation."""

        return self.physical.identity_payload


@dataclass(frozen=True)
class ProjectionLease:
    recipe: PhysicalProjectionRecipeIdentity
    attempt: int
    lease_token: str
    stage_schema: str | None = None
    stage_relation: str | None = None
    stage_relation_oid: int | None = None


@dataclass(frozen=True)
class ProjectionAdmissionIdentity:
    """One immutable acquisition proof bound many-to-one to a recipe."""

    admission_id: str
    planned_recipe_id: str
    recipe_id: str | None
    outcome_kind: str
    acquisition_adapter_id: str
    source_scope_hash: str
    source_ids: tuple[str, ...]
    completeness_manifest_hash: str
    completeness_manifest: Mapping[str, Any]
    retained_campaign_id: str
    retained_campaign_sha256: str
    retained_consumer_recipe_id: str
    claim_generation: int
    input_block_set_sha256: str
    input_block_count: int
    binding_count: int
    binding_set_sha256: str
    stream_set_sha256: str
    stream_count: int


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
    """One campaign-blind immutable byte block covered by a recipe."""

    block_id: str
    upstream_artifact_id: str
    source_object_id: str
    block_kind: str
    input_contract_id: str
    record_start: int
    record_count: int
    content_sha256: str
    payload_sha256: str
    payload_bytes: int
    summary: Mapping[str, int]
    block_proof_sha256: str


@dataclass(frozen=True)
class ProjectionAdmissionInputBlock:
    """Campaign coordinate binding for one neutral physical block."""

    binding_id: str
    block: ProjectionInputBlock
    retained_campaign_id: str
    retained_campaign_sha256: str
    retained_source_item_id: str
    retained_range_ordinal: int | None
    stream_identity_sha256: str
    sequence_ordinal: int
    resource_type: str
    partition_key_hash: str
    source_partition_ordinal: int


@dataclass(frozen=True)
class ProjectionAdmissionTerminalZero:
    """Terminal ordered-stream evidence with no physical projection input."""

    binding_id: str
    retained_campaign_id: str
    retained_campaign_sha256: str
    retained_source_item_id: str
    resource_type: str
    partition_key_hash: str
    source_partition_ordinal: int
    stream_identity_sha256: str
    stream_ordinal: int
    terminal_sequence_ordinal: int
    terminal_proof_sha256: str


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
    admission_id: str
    shard: ProjectionShardSpec
    partition_attempt: int
    lease_token: str


@dataclass(frozen=True)
class ProjectionRetainedChildLease:
    """One exact retained byte binding owned by one shard generation."""

    shard_claim: ProjectionShardClaim
    binding_id: str
    block_id: str
    retained_campaign_id: str
    retained_campaign_sha256: str
    retained_consumer_recipe_id: str
    retained_claim_generation: int
    retained_source_item_id: str
    retained_artifact_sha256: str
    retained_layout_sha256: str
    retained_range_ordinal: int | None
    artifact_byte_count: int
    raw_byte_start: int
    expected_byte_count: int
    expected_record_count: int
    input_sha256: str
    expected_payload_sha256: str
    child_generation: int
    child_lease_token: str


@dataclass(frozen=True)
class ProjectionClaim:
    """One live build lease; sealed reuse requires admission resolution."""

    lease: ProjectionLease


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


@dataclass(frozen=True)
class PreparedProjectionStage:
    stage: ProjectionStage
    storage_trigger_oid: int
    proof: PhysicalProjectionProof


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
