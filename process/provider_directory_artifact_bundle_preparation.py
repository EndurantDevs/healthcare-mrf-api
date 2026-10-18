# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare serving artifacts and apply them inside a caller-owned publication."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, AsyncIterator


@asynccontextmanager
async def prepare_artifact_bundle(
    fhir: Any,
    fence: Any,
    request: Any,
    *,
    artifact_resource_types: frozenset[str],
    resource_fence: Any = None,
) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    """Keep an immutable serving bundle staged until its owner publishes it."""
    resource_fence = resource_fence or await fhir._provider_directory_profile_resource_scope_fence(
        fence, request.publish_artifacts_targets
    )
    async with fhir._provider_directory_artifact_dataset_scope(
        run_id=request.run_id,
        source_ids=request.source_ids,
        fence=fence,
        resource_fence=resource_fence,
        metrics=request.metrics,
        resource_types=artifact_resource_types,
    ):
        async with fhir._provider_directory_artifact_bundle_scope() as artifact_bundle:
            fhir._attach_artifact_fence_metrics(request.metrics, fence)
            publish_metrics = await fhir._publish_provider_directory_artifacts(
                replace(
                    request,
                    address_key_run_id=None,
                    publish_scope_run_id=None,
                    source_ids=[dataset.source_id for dataset in fence.datasets],
                )
            )
            fhir._assert_candidate_artifact_bundle_complete(
                fence,
                publish_metrics,
                artifact_bundle,
                publish_corroboration=request.publish_corroboration,
                publish_artifacts_targets=request.publish_artifacts_targets,
            )
            yield artifact_bundle, publish_metrics


async def apply_prepared_artifact_bundle(
    fhir: Any,
    stages: tuple[Any, ...],
    *,
    profile_delta: Any = None,
    cutover_timeout: Any = None,
    settings_configured: bool = False,
    before_swaps: Any = None,
    extra_live_relations: tuple[str, ...] = (),
) -> Any:
    """Apply prepared relations without committing the owner's publication."""
    if fhir.db._transaction_binding() is None:
        raise RuntimeError("provider_directory_artifact_bundle_requires_transaction")
    ordered_stages = fhir._ordered_provider_directory_artifact_bundle(stages)
    if not ordered_stages and profile_delta is None:
        if extra_live_relations:
            await fhir._lock_provider_directory_artifact_live_tables(fhir._schema(), extra_live_relations)
        if before_swaps is not None:
            await before_swaps()
        return
    schema, relation_names, lock_timeout, statement_timeout = fhir._provider_directory_artifact_bundle_context(
        ordered_stages, profile_delta
    )
    capacity_admission = (
        fhir._provider_directory_profile_capacity_admission()
        if settings_configured
        else await fhir._configure_provider_directory_artifact_promotion(lock_timeout, statement_timeout)
    )
    await fhir.profile_initial.lock_metadata(fhir, ordered_stages, profile_delta)
    if any(
        stage.build_fence is not None and stage.build_fence.alias_generation is not None for stage in ordered_stages
    ):
        await fhir.db.scalar(fhir.address_alias_sql.alias_advisory_xact_lock_sql())
    await fhir._lock_provider_directory_artifact_bundle_targets(ordered_stages, schema, profile_delta)
    active_fence = await fhir._reserve_provider_directory_artifact_cutover_budget(profile_delta, capacity_admission)
    return await fhir._apply_locked_provider_directory_artifact_bundle(
        ordered_stages,
        schema,
        relation_names,
        profile_delta,
        active_fence,
        cutover_timeout,
        before_swaps=before_swaps,
        extra_live_relations=extra_live_relations,
    )


async def apply_prepared_output_bundle(
    fhir,
    capability,
    prepared_receipts,
    *,
    artifact_stages=(),
    profile_delta=None,
    before_swaps=None,
    after_output_publish,
):
    """Keep every family's publication and its final journal in one original savepoint."""
    from process import provider_directory_output_snapshot_custody as custody
    from process.provider_directory_artifact_read_access import preserve_artifact_read_access
    from process.provider_directory_generic_source_policy import _savepoint
    from process.provider_directory_output_snapshot_retention import retain_prepared_outputs

    await custody._require_capability(capability)
    binding = fhir.db._transaction_binding()
    if binding is None or binding.session is not capability.session or not callable(after_output_publish):
        raise RuntimeError("provider_directory_output_bundle_original_session_required")
    preparations = tuple(prepared_receipts)
    artifact_stages = tuple(artifact_stages)
    await custody.recheck_output_cut(capability, preparations)
    output_targets = {receipt.incumbent.target for receipt in preparations}
    if output_targets.intersection(stage.target_relation for stage in artifact_stages):
        raise ValueError("provider_directory_output_bundle_duplicate_target")
    retained_results = []

    async def publish_outputs():
        """Publish outputs after the common helper locks every live family."""
        await preserve_artifact_read_access(capability, artifact_stages)
        if before_swaps is not None:
            await before_swaps()
        retained_results.extend(await retain_prepared_outputs(capability, preparations))

    async with _savepoint(capability.connection):
        await fhir._lock_provider_directory_artifact_tables(
            capability.policy.schema, tuple(receipt.successor.relation for receipt in preparations)
        )
        forecast = await apply_prepared_artifact_bundle(
            fhir,
            artifact_stages,
            profile_delta=profile_delta,
            before_swaps=publish_outputs,
            extra_live_relations=tuple(sorted(output_targets)),
        )
        await custody._require_capability(capability)
        if len(retained_results) != len(preparations):
            raise RuntimeError("provider_directory_output_bundle_publication_incomplete")
        await after_output_publish(capability, tuple(retained_results))
    return forecast, tuple(retained_results)


async def validate_profile_delta_total_wal(fhir, capacity_admission, capacity_forecast) -> None:
    """Keep the signed whole-admission bound and original commit envelope at the owner boundary."""
    if capacity_admission.geometry.bounded_admission:
        await fhir._assert_provider_directory_profile_wal_budget(capacity_admission)
    final_wal_bytes = await fhir._provider_directory_profile_current_wal_bytes(capacity_admission)
    maximum_wal_bytes = capacity_admission.geometry.reservation_bytes_by_storage_class["wal"]
    commit_envelope_bytes = capacity_forecast.metadata_projection.commit_envelope_bytes
    if final_wal_bytes + commit_envelope_bytes > maximum_wal_bytes:
        raise RuntimeError(
            "provider_directory_profile_capacity_final_wal_exceeded:"
            f"observed={final_wal_bytes}:"
            f"commit_envelope={commit_envelope_bytes}:"
            f"maximum={maximum_wal_bytes}"
        )
