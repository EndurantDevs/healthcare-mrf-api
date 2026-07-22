# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public contracts, leased worksets, and native physical materialization.

Publication remains a later slice. Candidate row reducers, semantic summaries,
and physical-output envelopes remain in their owning internal modules: none of
them is native retained-byte attestation or a publish gate.
"""

from process.provider_directory_projection_admission import (
    PROJECTION_ADMISSION_CONTRACT_ID,
    projection_admission_consumer_id,
    projection_admission_identity,
    register_projection_admission,
)

from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_admission_terminal_zero,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
    validated_physical_projection_recipe_identity,
    validated_projection_recipe_identity,
)
from process.provider_directory_projection_lease import (
    claim_projection_recipe,
    heartbeat_projection_lease,
)
from process.provider_directory_projection_materializer import (
    ProjectionChildStreamFactory,
    materialize_projection_shards,
)
from process.provider_directory_projection_types import (
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS,
    PROJECTION_INPUT_BLOCK_MAX_RESOURCES,
    PROJECTION_MIXED_RESOURCE_TYPE,
    PROJECTION_RECIPE_CONTRACT_ID,
    PhysicalProjectionRecipeIdentity,
    ProjectionClaim,
    ProjectionAdmissionIdentity,
    ProjectionAdmissionInputBlock,
    ProjectionAdmissionTerminalZero,
    ProjectionCompletenessManifest,
    ProjectionInputBlock,
    ProjectionLease,
    ProjectionRecipeIdentity,
    ProjectionShardClaim,
    ProjectionShardSpec,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from process.provider_directory_projection_workset import (
    claim_projection_shard,
    heartbeat_projection_shard,
    register_projection_workset,
)


__all__ = [
    "PROJECTION_ADMISSION_CONTRACT_ID",
    "PROJECTION_INPUT_BLOCK_MAX_BYTES",
    "PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS",
    "PROJECTION_INPUT_BLOCK_MAX_RESOURCES",
    "PROJECTION_MIXED_RESOURCE_TYPE",
    "PROJECTION_RECIPE_CONTRACT_ID",
    "PhysicalProjectionRecipeIdentity",
    "ProjectionChildStreamFactory",
    "ProjectionClaim",
    "ProjectionAdmissionIdentity",
    "ProjectionAdmissionInputBlock",
    "ProjectionAdmissionTerminalZero",
    "ProjectionCompletenessManifest",
    "ProjectionInputBlock",
    "ProjectionLease",
    "ProjectionRecipeIdentity",
    "ProjectionShardClaim",
    "ProjectionShardSpec",
    "ProviderDirectoryProjectionBusy",
    "ProviderDirectoryProjectionError",
    "ProviderDirectoryProjectionLeaseLost",
    "claim_projection_recipe",
    "claim_projection_shard",
    "heartbeat_projection_lease",
    "heartbeat_projection_shard",
    "materialize_projection_shards",
    "projection_completeness_manifest",
    "projection_admission_input_block",
    "projection_admission_terminal_zero",
    "projection_admission_consumer_id",
    "projection_admission_identity",
    "projection_input_block",
    "projection_input_set_sha256",
    "projection_recipe_identity",
    "projection_shard_spec",
    "register_projection_workset",
    "register_projection_admission",
    "validated_physical_projection_recipe_identity",
    "validated_projection_recipe_identity",
]
