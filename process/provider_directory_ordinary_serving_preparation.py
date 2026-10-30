# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare ordinary serving data through the existing independently admitted scopes."""

import sys
from contextlib import AsyncExitStack, asynccontextmanager

from process import provider_directory_cms_preparation as preparation
from process.provider_directory_ordinary_address_admission import require_capture
from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan


async def _admit(fhir, execution, fence, admission, address_preparation, retention_preparation):
    """Match the exact preflight projection before either builder creates scratch storage."""
    from process.provider_directory_profile_scope_copy import input_transaction
    from process.provider_directory_profile_source_inputs import profile_source_input_scope

    if (
        not isinstance(admission, preparation.NonprofileAdmission)
        or type(admission.plan) is not OrdinaryServingAdmissionPlan
        or not callable(address_preparation)
        or not callable(retention_preparation)
        or fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is not None
        or preparation._ACTIVE.get() is not None
    ):
        raise RuntimeError("provider_directory_ordinary_preparation_invalid")
    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    if capture.is_unchanged:
        raise RuntimeError("provider_directory_ordinary_preparation_empty")
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    publish_targets = {"address_overlay"}
    types = fhir._provider_directory_artifact_resource_types(publish_targets, publish_corroboration=False)
    overlay_fence = capture.overlay_fence
    async with profile_source_input_scope(fhir, overlay_fence, overlay_fence), input_transaction(fhir):
        async with preparation.nonprofile_sql_transaction(fhir, admission):
            projection = await fhir._provider_directory_artifact_scope_exact_projection(
                fhir._schema(),
                overlay_fence,
                types,
                source_fence=overlay_fence,
                batch_size=admission.plan.batch_size,
            )
    fhir._assert_provider_directory_artifact_scope_exact_capacity(projection)
    await admission.before_scratch(execution, fence, projection, publish_targets, types)
    return capture, types, projection


@asynccontextmanager
async def _separate_address_window(fhir, execution, fence, admission):
    """Suspend only Profile's meter; retain its exact admission and signed workload."""
    from process.provider_directory_profile_scope_copy import input_transaction, resource_fence
    from process.provider_directory_profile_source_inputs import profile_source_input_scope

    capacity = fhir._provider_directory_profile_capacity_admission()
    if capacity is None or not callable(admission.pause_profile) or not callable(admission.resume_profile):
        raise RuntimeError("provider_directory_ordinary_profile_admission_missing")
    paused = await admission.pause_profile(capacity)
    admission.profile_admission = paused
    capacity_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    admission_token = preparation._ACTIVE.set(admission)
    try:
        yield
        selected_resources = await resource_fence(fhir, fence, {"profile"})
        types = fhir._provider_directory_artifact_resource_types({"profile"}, publish_corroboration=False)
        async with profile_source_input_scope(fhir, fence, selected_resources), input_transaction(fhir):
            resumed = await admission.resume_profile(paused, selected_resources, types)
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(resumed)
    except BaseException:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)
        raise
    finally:
        preparation._ACTIVE.reset(admission_token)


@asynccontextmanager
async def prepare_ordinary_serving_artifacts(
    prepared,
    *,
    run_id,
    control_run_id,
    address_preparation,
    retention_preparation,
):
    """Finish Profile, then Address and retained images, before resuming for one cut."""
    fhir, execution, fence = prepared.fhir, prepared.execution, prepared.fence
    admission = prepared.nonprofile_admission
    if prepared.profile_bundle is not None or prepared.address is not None:
        raise RuntimeError("provider_directory_ordinary_preparation_already_started")
    capture, types, projection = await _admit(
        fhir, execution, fence, admission, address_preparation, retention_preparation
    )
    execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    try:
        async with preparation._profile_scope(
            fhir, execution, fence, run_id, control_run_id, prepared.metrics, None
        ) as (profile_bundle, profile_metrics):
            prepared.profile_bundle = profile_bundle
            prepared.metrics["profile"] = profile_metrics["profile"]
            try:
                async with _prepare_address_resources(
                    prepared,
                    capture,
                    types,
                    projection,
                    run_id,
                    address_preparation,
                    retention_preparation,
                ):
                    yield prepared
            finally:
                # The outer Profile owner restores both contexts after its own scratch cleanup.
                fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
                preparation._ACTIVE.set(admission)
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)


@asynccontextmanager
async def _prepare_address_resources(
    prepared,
    capture,
    types,
    projection,
    run_id,
    address_preparation,
    retention_preparation,
):
    """Keep candidates alive through publication while restoring the original Profile scope."""
    from process.provider_directory_profile_scope_copy import resource_fence

    fhir, execution, fence = prepared.fhir, prepared.execution, prepared.fence
    admission = prepared.nonprofile_admission
    profile_resources = await resource_fence(fhir, fence, {"profile"})
    profile_relation_by_target = dict(fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() or {})
    profile_oid_by_relation = dict(fhir._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.get() or {})
    resources = AsyncExitStack()
    try:
        async with _separate_address_window(fhir, execution, fence, admission):
            inputs = await resources.enter_async_context(
                preparation._full_scope(fhir, capture.overlay_fence, types, projection, admission)
            )
            overlay_bundle = await resources.enter_async_context(fhir._provider_directory_artifact_bundle_scope())
            overlay_metrics = await preparation._prepare_full_bundle(
                fhir, capture.overlay_fence, run_id, prepared.metrics, {"address_overlay"}, overlay_bundle
            )
            overlay = await preparation._overlay_identity(fhir, overlay_bundle, admission)
            prepared.nonprofile_bundle = overlay_bundle
            prepared.metrics.update(overlay_metrics)
            prepared.overlay_identity = overlay
            prepared.relation_overrides = {**inputs, **overlay_bundle.relation_overrides}
            prepared.address = await resources.enter_async_context(
                address_preparation(fence, prepared.relation_overrides, overlay, admission)
            )
            if prepared.address is None:
                raise RuntimeError("provider_directory_ordinary_address_preparation_incomplete")
            prepared.retained_generation = await retention_preparation(prepared)
            if prepared.retained_generation is None:
                raise RuntimeError("provider_directory_ordinary_retention_preparation_incomplete")
        with fhir._artifact_scope_tokens(fence, profile_resources, profile_relation_by_target, profile_oid_by_relation):
            await prepared.assert_ready()
            yield prepared
    finally:
        capacity_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
        admission_token = preparation._ACTIVE.set(admission)
        try:
            await resources.__aexit__(*sys.exc_info())
        finally:
            preparation._ACTIVE.reset(admission_token)
            fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)
