# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare serving artifacts and apply them inside a caller-owned publication."""

from __future__ import annotations

from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass, replace
from typing import Any, AsyncIterator


async def artifact_publication(fhir, schema, relation, *, lock=False):
    """Capture a named target and its existing logical publication revision."""
    present = await fhir.db.scalar("SELECT to_regclass(:relation) IS NOT NULL", relation=fhir._qt(schema, relation))
    profile_targets = {fhir.profile_artifact.PROFILE_TABLE, fhir.profile_artifact.PROFILE_EVIDENCE_TABLE}
    if relation in profile_targets:
        state = fhir._provider_directory_profile_serving_generation_ref(schema)
        predicate = "singleton_key='global'"
        columns = "generation_id,updated_at"
    else:
        state = fhir._qt(schema, fhir.address_alias_sql.ADDRESS_ALIAS_ARTIFACT_STATE_TABLE)
        predicate = "artifact_name=:artifact_name"
        columns = "generation,updated_at"
    if not await fhir.db.scalar("SELECT to_regclass(:relation) IS NOT NULL", relation=state):
        raise fhir.ProviderDirectoryArtifactBuildStale("artifact_publication_metadata_missing")
    row = await fhir.db.first(
        f"SELECT {columns} FROM {state} WHERE {predicate}" + (" FOR SHARE NOWAIT" if lock else ""),
        **({} if relation in profile_targets else {"artifact_name": relation}),
    )
    stamp = None if row is None else (str(row[0]), fhir._json_default(row[1]))
    return bool(present), stamp


async def address_overlay_publication(fhir, schema, *, lock=False):
    """Read the alias generation and revision of the current named overlay."""
    present, stamp = await artifact_publication(fhir, schema, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, lock=lock)
    return (int(stamp[0]), stamp[1]) if present and stamp is not None else None


async def freeze_artifact_stage(fhir, schema, stage_table):
    """Close candidate writes before it can be handed to another build or publisher."""
    from sqlalchemy import text

    from process.entity_address_publication_permissions import make_relation_read_only

    async with fhir.db.transaction() as session:
        await session.execute(text("SET LOCAL lock_timeout='100ms'"))
        await session.execute(text(f"LOCK TABLE {fhir._qt(schema, stage_table)} IN SHARE ROW EXCLUSIVE MODE"))
        await make_relation_read_only(session, schema, stage_table)


async def assert_artifact_build_fence(fhir, stage):
    """Reject a changed publication even when its qualified name stays unchanged."""
    fence = stage.build_fence
    if fence is None:
        return
    if fence.target_name != (
        stage.schema,
        stage.target_relation,
    ) or fence.target_publication != await artifact_publication(fhir, stage.schema, stage.target_relation, lock=True):
        raise fhir.ProviderDirectoryArtifactBuildStale(stage.target_relation)
    if (
        fence.alias_generation is not None
        and await fhir._address_alias_generation(stage.schema) != fence.alias_generation
    ):
        raise fhir.ProviderDirectoryArtifactBuildStale(f"{stage.target_relation}:address_alias_generation")
    if fence.dependency_relation == fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE:
        publication = await address_overlay_publication(fhir, stage.schema, lock=True)
        if publication is None or publication != fence.dependency_publication:
            raise fhir.ProviderDirectoryArtifactBuildStale(f"{stage.target_relation}:dependency_relation")
        if publication[0] != fence.alias_generation:
            raise fhir.ProviderDirectoryArtifactBuildStale(f"{stage.target_relation}:address_overlay_generation")
    elif fence.dependency_relation is not None:
        present = await fhir.db.scalar(
            "SELECT to_regclass(:relation) IS NOT NULL", relation=fhir._qt(stage.schema, fence.dependency_relation)
        )
        if not present:
            raise fhir.ProviderDirectoryArtifactBuildStale(f"{stage.target_relation}:dependency_relation")


async def capture_address_corroboration_fence(fhir, schema, relation, source_ids, build_fence):
    """Bind corroboration to alias rules and the selected overlay publication."""
    generation = await fhir._address_alias_generation(schema)
    if await fhir._address_alias_artifact_generation(schema, relation) != generation and source_ids:
        raise RuntimeError(
            "address alias generation changed; a full Provider Directory address corroboration rebuild is required"
        )
    dependency = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get().get(
        fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
    )
    publication = None
    if dependency == fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE:
        publication = await address_overlay_publication(fhir, schema)
        if publication is None or publication[0] != generation:
            raise RuntimeError("Provider Directory address overlay uses a stale address alias generation")
    elif not await fhir.db.scalar("SELECT to_regclass(:relation) IS NOT NULL", relation=fhir._qt(schema, dependency)):
        raise RuntimeError("Provider Directory address overlay dependency is missing")
    return replace(
        build_fence, alias_generation=generation, dependency_relation=dependency, dependency_publication=publication
    )


@dataclass(frozen=True)
class ArtifactBundlePublication:
    """One callback and named native lock plan for the atomic publication phase."""

    before_swaps: Any = None
    extra_live_relations: tuple[str, ...] = ()
    read_only: bool = False
    promotion_identities: tuple[Any, ...] = ()

    @property
    def lock_options(self):
        """Preserve ordinary exclusive prelocks unless adopting native-frozen snapshots."""
        return {"read_only": True} if self.read_only else {}

    async def lock_candidates(self, fhir, schema, relation_names, live_names):
        """Pin prepared names before any data or native rename changes their namespace."""
        await fhir._lock_provider_directory_artifact_tables(
            schema, tuple(name for name in relation_names if name not in live_names), **self.lock_options
        )

    async def apply(self, fhir, stages, *, profile_delta=None, cutover_timeout=None, settings_configured=False):
        """Apply one explicit publication plan in its caller-owned transaction."""
        if fhir.db._transaction_binding() is None:
            raise RuntimeError("provider_directory_artifact_bundle_requires_transaction")
        ordered_stages = fhir._ordered_provider_directory_artifact_bundle(stages)
        if not ordered_stages and profile_delta is None:
            if self.extra_live_relations:
                await fhir._lock_provider_directory_artifact_live_tables(
                    fhir._schema(), self.extra_live_relations, **self.lock_options
                )
            if self.before_swaps is not None:
                await self.before_swaps()
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
            publication=self,
        )


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
    read_only: bool = False,
) -> Any:
    """Apply prepared relations without committing the owner's publication."""
    return await ArtifactBundlePublication(before_swaps, extra_live_relations, read_only).apply(
        fhir,
        stages,
        profile_delta=profile_delta,
        cutover_timeout=cutover_timeout,
        settings_configured=settings_configured,
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
    if not callable(after_output_publish):
        raise RuntimeError("provider_directory_output_bundle_callback_required")
    session_scope = (
        fhir.db.bind_existing_session(capability.session) if fhir.db._transaction_binding() is None else nullcontext()
    )
    async with session_scope:
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
                capability.policy.schema,
                tuple(receipt.successor.relation for receipt in preparations),
                read_only=True,
            )
            forecast = await apply_prepared_artifact_bundle(
                fhir,
                artifact_stages,
                profile_delta=profile_delta,
                before_swaps=publish_outputs,
                extra_live_relations=tuple(sorted(output_targets)),
                read_only=True,
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
