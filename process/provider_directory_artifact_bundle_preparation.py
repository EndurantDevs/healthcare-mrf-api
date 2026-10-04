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
) -> Any:
    """Apply prepared relations without committing the owner's publication."""
    if fhir.db._transaction_binding() is None:
        raise RuntimeError("provider_directory_artifact_bundle_requires_transaction")
    ordered_stages = fhir._ordered_provider_directory_artifact_bundle(stages)
    if not ordered_stages and profile_delta is None:
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
    )


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
