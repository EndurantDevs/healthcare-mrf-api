# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finish an ordinary published replacement through the existing staged serving recipe."""

from uuid import uuid4

from process import entity_address_candidate_preparation as address_preparation
from process import entity_address_ordinary_preparation as ordinary_preparation
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_generic_source_policy as source
from process import provider_directory_published_replacement_inputs as serving_inputs
from process import provider_directory_published_source_replacement as replacement
from process.provider_directory_output_snapshot_catalog import require_output


async def _has_valid_incumbent(directory, pin):
    """Leave aligned and orphan aliases on their existing ordinary paths."""
    sources, datasets = replacement._tables(directory)
    return await directory.db.scalar(
        f"SELECT EXISTS(SELECT FROM {sources} source JOIN {datasets} dataset "
        "ON dataset.endpoint_id=source.endpoint_id WHERE source.source_id=:source_id "
        "AND source.endpoint_id<>:endpoint_id AND dataset.is_current AND dataset.status='published' "
        "AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL)",
        source_id=pin["source_id"],
        endpoint_id=pin["endpoint_id"],
    )


async def _new_address_context(directory, run_id):
    native = address_preparation._native()
    require_output(native.db is directory.db, "rest_artifact_replacement_database_changed")
    context_by_field = {"control_run_id": run_id}
    await native.startup(context_by_field)
    context_by_field["import_date"] = "pdr" + uuid4().hex[:20]
    for stage in ordinary_preparation.declared_names(native, context_by_field["import_date"]):
        require_output(
            await directory.db.scalar(
                "SELECT to_regclass(:relation)::oid::bigint", relation=f"{directory._schema()}.{stage}"
            )
            is None,
            "rest_artifact_replacement_stage_occupied",
        )
    return context_by_field


async def _prepare_address(directory, fence, bundle, run_id):
    native = address_preparation._native()
    admission = artifacts.current(directory)
    require_output(
        admission is not None and admission.recovery is not None and len(bundle.stages) == 1,
        "rest_artifact_replacement_preparation_required",
    )
    stage, dataset = bundle.stages[0], fence.datasets[0]
    stage_oid = await directory.db.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{stage.schema}.{stage.stage_table}"
    )
    overrides = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    inputs = address_preparation.ProviderDirectoryAddressPreparationInput(
        serving_inputs.desired_pins(admission.recovery["serving_inputs"]),
        stage.stage_table,
        stage_oid,
        await native._address_alias_generation(directory._schema()),
        tuple((name, overrides[name]) for name in sorted(address_preparation._RELATION_OVERRIDES) if name in overrides),
        ordinary_serving_peer_pins=serving_inputs.peer_pins(admission.recovery["serving_inputs"]),
    )
    context_by_field = await _new_address_context(directory, run_id)
    async with ordinary_preparation.scope(
        directory._schema(), ordinary_preparation.declared_names(native, context_by_field["import_date"])
    ) as owned:
        prepared = await address_preparation.prepare_provider_directory_entity_address(
            context_by_field,
            dict(
                refresh_mode="provider-directory-partial",
                serving_only_refresh=True,
                publish=True,
                test_mode=False,
                provider_directory_partial_scope="latest-run",
                provider_directory_source_ids=[dataset.source_id],
                provider_directory_dataset_id=dataset.dataset_id,
                provider_directory_run_id=dataset.evidence_run_id,
            ),
            preparation_input=inputs,
        )
        owned.keep(prepared.stage_oids)
        return prepared


async def publish_if_required(directory, request):
    """Consume the unchanged exact V1 overlay descriptor before the default alias fence."""
    if artifacts._FOLLOWUP_PREFIX + "version" not in request.metrics or source.is_source_storage_required():
        return None
    require_output(
        source._FACTORY.get() is None
        and request.publish_artifacts_targets == {"address_overlay"}
        and not request.publish_corroboration
        and not request.full_address_artifact_rebuild
        and directory._provider_directory_profile_capacity_admission() is None,
        "rest_artifact_replacement_request_invalid",
    )
    pin = replacement._descriptor_fields(directory, request.metrics)
    require_output(list(request.source_ids or ()) == [pin["source_id"]], "rest_artifact_replacement_request_invalid")
    resumed = await artifacts.replay_published_replacement(directory, request.metrics)
    if resumed is not None:
        request.metrics.update(resumed)
        return request.metrics
    if not await _has_valid_incumbent(directory, pin):
        resumed = await artifacts.replay_published_replacement(directory, request.metrics)
        if resumed is not None:
            request.metrics.update(resumed)
            return request.metrics
        return None
    async with artifacts.prepare_published_replacement(
        directory,
        request,
        artifact_resource_types=directory._provider_directory_artifact_resource_types(
            request.publish_artifacts_targets, publish_corroboration=request.publish_corroboration
        ),
    ) as (fence, bundle, metrics):
        address = await _prepare_address(directory, fence, bundle, request.run_id)
        try:
            metrics.update(await artifacts.publish_prepared_replacement(directory, fence, bundle, address))
            return await directory._record_artifact_promotion_metrics(fence, metrics, len(bundle.stages))
        finally:
            await address_preparation.cleanup_prepared_entity_address_generation(address)


async def publish_dataset_artifacts(directory, request, *, validated_publication_candidate=None):
    """Retain ordinary artifact publication after the exact replacement entry declines."""
    replacement_result = await publish_if_required(directory, request)
    if replacement_result is not None:
        return replacement_result
    fence = await directory._prepare_artifact_publication_fence(
        request.source_ids,
        run_id=request.run_id,
        metrics=request.metrics,
        publish_artifacts_targets=request.publish_artifacts_targets,
        publish_corroboration=request.publish_corroboration,
        should_select_validated_candidates=directory._should_select_validated_artifacts(
            request.publish_artifacts_targets
        ),
        validated_publication_candidate=validated_publication_candidate,
    )
    if directory._is_dataset_relation_only_artifact_target(request.publish_artifacts_targets):
        request.metrics["publish_artifacts_targets"] = sorted(request.publish_artifacts_targets or ())
        return request.metrics
    return await directory._publish_artifact_bundle_from_fence(
        fence,
        request,
        artifact_resource_types=directory._provider_directory_artifact_resource_types(
            request.publish_artifacts_targets, publish_corroboration=request.publish_corroboration
        ),
    )
