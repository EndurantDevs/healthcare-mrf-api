# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish already-prepared repairs and their checkpoints in one native cut.

Artifact preparation is a separate prerequisite. This module never invokes
compatibility backfills or activates the public retained-rebuild entrypoint.
"""

from process import provider_directory_dataset_rehydrate_journal as protocol
from process import provider_directory_dataset_rehydrate_journal_store as journal_store
from process import provider_directory_dataset_rehydrate_pin as pin
from process import provider_directory_output_snapshot_custody as custody
from process.provider_directory_artifact_bundle_preparation import apply_prepared_output_bundle
from process.provider_directory_cms_preparation import desired_fence_hash
from process.provider_directory_dataset_rehydrate_prepare import (
    PreparedCompatibilityOutputs,
    _journal_scope,
    _require,
)
from process.provider_directory_dataset_rehydrate_store import _load_pinned_resource_proof
from process.provider_directory_dataset_rehydrate_types import ResourceContext, _run_cancel_check


def _fence_scope(directory, pinned_source, operation, fence):
    _require(type(fence) is directory.ProviderDirectoryArtifactDatasetFence, "artifact_fence_required")
    _require(
        not fence.should_select_validated_candidates
        and not fence.promotion_aliases
        and fence.validated_publication_candidate is None
        and all(
            dataset.status == directory.ENDPOINT_DATASET_PUBLISHED
            and dataset.is_current is True
            and not dataset.promote_on_cutover
            and not dataset.reconcile_source_alias_on_cutover
            for dataset in fence.datasets
        ),
        "artifact_fence_changed",
    )
    scope = pinned_source.scope
    selected_datasets = [dataset for dataset in fence.datasets if dataset.source_id == scope.source_id]
    _require(
        len(selected_datasets) == 1
        and (
            selected_datasets[0].endpoint_id,
            selected_datasets[0].dataset_id,
            selected_datasets[0].evidence_run_id,
            selected_datasets[0].dataset_hash,
        )
        == (scope.endpoint_id, scope.dataset_id, scope.acquisition_root_run_id, scope.dataset_hash)
        and selected_datasets[0].resource_count == scope.resource_count
        and set(operation["resource_types"]) <= set(selected_datasets[0].artifact_resources)
        and operation["source_fence_digest"] == desired_fence_hash(fence),
        "artifact_fence_changed",
    )


def _prepared_generation(snapshot, capability, prepared):
    active = snapshot.journal["active"]
    generation = snapshot.journal["generations"].get(capability.operation_digest)
    receipts = [receipt.to_payload() for receipt in prepared.receipts]
    _require(
        active is not None
        and active["operation_digest"] == capability.operation_digest
        and generation is not None
        and generation["published"] is None
        and generation["prepared"] == receipts
        and set(active["stages"]) == set(prepared.targets)
        and [active["stages"][target]["stage"] for target in prepared.targets]
        == [stage.to_payload() for stage in prepared.stages]
        and [active["stages"][target]["receipt"] for target in prepared.targets] == receipts,
        "preparation_changed",
    )


async def _complete_generation(directory, pinned_source, journal, snapshot, capability, retained):
    proofs_by_resource = {}
    for resource, expected in journal.expected_by_resource:
        await _run_cancel_check(journal.runtime)
        context = ResourceContext(
            journal.runtime,
            journal.request,
            journal.scope,
            resource,
            directory.RESOURCE_MODELS_BY_TYPE[resource],
            expected,
        )
        proof = await _load_pinned_resource_proof(context, pinned_source, directory)
        proofs_by_resource[resource] = proof.as_dict()
    value = protocol.publish_generation(snapshot.journal, capability.operation_digest, retained, proofs_by_resource)
    await journal_store.write_output_journal(
        journal, snapshot, value, completed=proofs_by_resource, capability=capability
    )


async def _lock_current_source(directory, pinned_source, checkpoint_journal, fence):
    pinned_source.source_relation(directory)
    await _run_cancel_check(checkpoint_journal.runtime)
    await directory._lock_and_verify_artifact_dataset_fence(fence)
    fresh_scope, _policy, fresh_binding = await pin._capture_source(
        checkpoint_journal.runtime, checkpoint_journal.request
    )
    _require(
        fresh_scope == pinned_source.scope and fresh_binding == pinned_source.binding(directory),
        "source_binding_changed",
    )


async def publish_prepared_rehydration(
    directory, pinned_source, operation, prepared, *, checkpoint_journal, fence, artifact_bundle
):
    """Compose supplied sealed stages; callers still own complete artifact preparation.

    The SOURCE pin remains active through commit. The common primitive owns
    the complete live lock set and rollback of every swap and the final CAS.
    """
    output_targets = _journal_scope(directory, pinned_source, operation, checkpoint_journal)
    _fence_scope(directory, pinned_source, operation, fence)
    _require(
        type(prepared) is PreparedCompatibilityOutputs and prepared.targets == output_targets, "preparation_changed"
    )
    _require(
        type(artifact_bundle) is directory.ProviderDirectoryArtifactBundle
        and not artifact_bundle.promoted
        and artifact_bundle.archive_delta is None,
        "artifact_bundle_required",
    )
    artifact_stages, profile_delta = tuple(artifact_bundle.stages), artifact_bundle.profile_delta
    await _run_cancel_check(checkpoint_journal.runtime)
    async with custody.output_snapshot_transaction(
        directory, operation=operation, targets=output_targets, isolation="read committed"
    ) as capability:
        checkpoint_snapshots = []

        async def before_swaps():
            """Release only the nested savepoint; its row locks survive through the outer cut."""
            await _lock_current_source(directory, pinned_source, checkpoint_journal, fence)
            async with journal_store.checkpoint_output_transaction(checkpoint_journal) as (session, snapshot):
                _require(session is capability.session, "checkpoint_transaction_changed")
                _prepared_generation(snapshot, capability, prepared)
                checkpoint_snapshots.append(snapshot)

        async def after_output_publish(original, retained):
            """Write complete pinned proofs and retained history on the original transaction."""
            _require(original is capability and len(checkpoint_snapshots) == 1, "checkpoint_transaction_changed")
            await _complete_generation(
                directory, pinned_source, checkpoint_journal, checkpoint_snapshots[0], original, retained
            )

        return await apply_prepared_output_bundle(
            directory,
            capability,
            prepared.receipts,
            artifact_stages=artifact_stages,
            profile_delta=profile_delta,
            before_swaps=before_swaps,
            after_output_publish=after_output_publish,
        )
