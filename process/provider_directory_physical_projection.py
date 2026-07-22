# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral contracts and leased worksets for physical projections.

Runtime materialization, publication, and retained-byte streaming are layered
onto this module by later slices.  Keeping this facade narrow makes the
foundation independently deployable without exposing a second input path.
"""

from process.provider_directory_projection_contract import (
    canonical_row_digest,
    canonical_row_line,
    prepare_projection_proof_shard,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_proof_shard,
    projection_recipe_identity,
    projection_shard_spec,
    reduced_physical_projection_proof,
)
from process.provider_directory_projection_contribution import (
    SEMANTIC_CONTRIBUTION_COLUMNS,
    SEMANTIC_CONTRIBUTION_CONTRACT_ID,
    SEMANTIC_OUTCOME_PROOF_CONTRACT_ID,
    SEMANTIC_STREAMING_REDUCER_CONTRACT_ID,
    normalized_semantic_contribution,
    reduce_semantic_outcomes,
    reduce_streaming_semantic_outcomes,
    validated_semantic_outcome_proof,
)
from process.provider_directory_projection_inline_profile import (
    INLINE_PROFILE_EVIDENCE_COLUMNS,
    INLINE_PROFILE_EVIDENCE_CONTRACT_ID,
)
from process.provider_directory_projection_lease import (
    claim_projection_recipe,
    heartbeat_projection_lease,
)
from process.provider_directory_projection_summary import (
    MAX_SUPPLEMENTAL_COUNT_FIELDS,
    SEMANTIC_OUTCOME_FIELDS,
    SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID,
    normalized_supplemental_counts,
    semantic_source_summary,
    validated_semantic_source_summary,
)
from process.provider_directory_projection_types import (
    PROJECTION_CONTENT_HASH_CONTRACT_ID,
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS,
    PROJECTION_INPUT_BLOCK_MAX_RESOURCES,
    PROJECTION_MIXED_RESOURCE_TYPE,
    PROJECTION_PROOF_SHARD_CONTRACT_ID,
    PROJECTION_RECIPE_CONTRACT_ID,
    PhysicalProjectionProof,
    ProjectionClaim,
    ProjectionCompletenessManifest,
    ProjectionInputBlock,
    ProjectionLease,
    ProjectionProofShard,
    ProjectionRecipeIdentity,
    ProjectionSemanticOutcomeProof,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from process.provider_directory_projection_workset import (
    claim_projection_shard,
    heartbeat_projection_shard,
    load_projection_input_block,
    register_projection_workset,
)


__all__ = [
    "INLINE_PROFILE_EVIDENCE_COLUMNS",
    "INLINE_PROFILE_EVIDENCE_CONTRACT_ID",
    "MAX_SUPPLEMENTAL_COUNT_FIELDS",
    "PROJECTION_CONTENT_HASH_CONTRACT_ID",
    "PROJECTION_INPUT_BLOCK_MAX_BYTES",
    "PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS",
    "PROJECTION_INPUT_BLOCK_MAX_RESOURCES",
    "PROJECTION_MIXED_RESOURCE_TYPE",
    "PROJECTION_PROOF_SHARD_CONTRACT_ID",
    "PROJECTION_RECIPE_CONTRACT_ID",
    "SEMANTIC_CONTRIBUTION_COLUMNS",
    "SEMANTIC_CONTRIBUTION_CONTRACT_ID",
    "SEMANTIC_OUTCOME_FIELDS",
    "SEMANTIC_OUTCOME_PROOF_CONTRACT_ID",
    "SEMANTIC_SOURCE_SUMMARY_CONTRACT_ID",
    "SEMANTIC_STREAMING_REDUCER_CONTRACT_ID",
    "PhysicalProjectionProof",
    "ProjectionClaim",
    "ProjectionCompletenessManifest",
    "ProjectionInputBlock",
    "ProjectionLease",
    "ProjectionProofShard",
    "ProjectionRecipeIdentity",
    "ProjectionSemanticOutcomeProof",
    "ProjectionShardClaim",
    "ProjectionShardSpec",
    "ProviderDirectoryProjectionBusy",
    "ProviderDirectoryProjectionError",
    "ProviderDirectoryProjectionLeaseLost",
    "canonical_row_digest",
    "canonical_row_line",
    "claim_projection_recipe",
    "claim_projection_shard",
    "heartbeat_projection_lease",
    "heartbeat_projection_shard",
    "load_projection_input_block",
    "normalized_semantic_contribution",
    "normalized_supplemental_counts",
    "prepare_projection_proof_shard",
    "projection_completeness_manifest",
    "projection_input_block",
    "projection_input_set_sha256",
    "projection_proof_shard",
    "projection_recipe_identity",
    "projection_shard_spec",
    "reduce_semantic_outcomes",
    "reduce_streaming_semantic_outcomes",
    "reduced_physical_projection_proof",
    "register_projection_workset",
    "semantic_source_summary",
    "validated_semantic_outcome_proof",
    "validated_semantic_source_summary",
]
