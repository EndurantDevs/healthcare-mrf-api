# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep an issued typed family alive under its original preparation task."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

from process import provider_directory_artifact_scratch_inputs as inputs
from process import provider_directory_cms_preparation as preparation
from process.provider_directory_output_snapshot_catalog import require_output

_PROJECTION = ContextVar("provider_directory_prepared_scratch_projection", default=None)


async def _discard_original(directory, scope):
    """Drain cancellation on the issuer; a child task cannot borrow its custody."""
    from process.provider_directory_artifact_scratch_reader import drain_scratch_children

    cancellation = None
    while True:
        try:
            await drain_scratch_children(directory, scope)
            await inputs.discard_scratch_inputs(directory, scope)
        except asyncio.CancelledError as error:
            cancellation = error
            continue
        break
    if cancellation is not None:
        raise cancellation


def _relations(metadata):
    require_output(
        type(metadata) is dict and set(metadata) == {"relation_by_table", "relation_oids"}, "scratch_metadata_changed"
    )
    names, oids = metadata["relation_by_table"], metadata["relation_oids"]
    require_output(
        type(names) is dict
        and type(oids) is dict
        and set(names) == set(oids) == {inputs.SOURCE, *inputs.TYPED_ROOTS}
        and all(type(name) is str and name for name in names.values())
        and all(type(oid) is int and oid > 0 for oid in oids.values())
        and len(set(names.values())) == len(names)
        and len(set(oids.values())) == len(oids),
        "scratch_metadata_changed",
    )
    return names, oids


@asynccontextmanager
async def prepared_scratch_scope(directory, admission, fence, resource_types, projection):
    """Prepare once, expose attested names, then discard on the same original task."""
    scope = await inputs.prepare_scratch_inputs(directory, admission, fence, resource_types, projection)
    try:
        metadata = await inputs.scratch_input_metadata(directory, scope)
        names, oids = _relations(metadata)
        token = _PROJECTION.set((scope, projection))
        try:
            with inputs.scratch_input_scope(scope), directory._artifact_scope_tokens(fence, fence, names, oids):
                yield dict(names)
        finally:
            _PROJECTION.reset(token)
    except BaseException as error:
        try:
            await _discard_original(directory, scope)
        except BaseException as cleanup_error:
            if not (isinstance(error, asyncio.CancelledError) and isinstance(cleanup_error, asyncio.CancelledError)):
                raise BaseExceptionGroup(
                    "provider_directory_scratch_scope_and_cleanup_failed", [error, cleanup_error]
                ) from error
        raise
    else:
        await _discard_original(directory, scope)


async def reuse_scratch_scope(directory, fence, resource_fence, resource_types):
    """Reuse only the exact admitted full scope; Profile admission cannot issue one."""
    from process.provider_directory_artifact_scratch_reader import child_scope_projection

    child_projection = await child_scope_projection(directory, fence, resource_fence, resource_types)
    if child_projection is not None:
        return child_projection
    scope = inputs.active_scratch_inputs(directory)
    admission = preparation._ACTIVE.get()
    captured = _PROJECTION.get()
    require_output(
        scope is not None
        and type(admission) is preparation.NonprofileAdmission
        and admission._started
        and scope.admission is admission
        and captured is not None
        and captured[0] is scope,
        "scratch_admission_required",
    )
    projection = captured[1]
    require_output(
        preparation.desired_fence_hash(fence)
        == preparation.desired_fence_hash(resource_fence)
        == admission.plan.desired_fence_hash
        and tuple(sorted(resource_types)) == admission.plan.resource_types
        and projection.projection_hash == admission.plan.artifact_scope_projection_hash,
        "scratch_plan_changed",
    )
    metadata = await inputs.scratch_input_metadata(directory, scope)
    names, oids = _relations(metadata)
    require_output(
        names == directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
        and oids == directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.get(),
        "scratch_metadata_changed",
    )
    return projection


async def reuse_artifact_scope(directory, fence, resource_fence, metrics, resource_types):
    """Keep enrolled callers out of legacy materialization before any SQL."""
    if fence is None:
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_artifact_scratch_fence_required")
    projection = await reuse_scratch_scope(
        directory, fence, resource_fence or fence, directory._resolved_artifact_scope_resource_types(resource_types)
    )
    directory._record_artifact_scope_metrics(metrics, projection, [])
    return fence
