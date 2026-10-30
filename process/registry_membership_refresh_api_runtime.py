# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own explicitly configured refresh capabilities for one API worker lifecycle."""

from contextlib import AsyncExitStack

from process.uhc_flex_practitioner_async_safety import drain_operation

_FIELDS = (
    "registry_membership_refresh_authority",
    "registry_membership_refresh_address_profile",
    "registry_membership_refresh_inputs",
    "registry_refresh_anchor_service",
)


def register_registry_membership_refresh_runtime(app):
    """Register lifecycle hooks without enabling or granting refresh authority."""
    if getattr(app.ctx, "registry_membership_refresh_runtime_registered", False):
        raise ValueError("registry_membership_refresh_runtime_already_registered")
    app.ctx.registry_membership_refresh_runtime_registered = True
    app.listener("before_server_start")(_start_runtime)
    app.listener("after_server_stop")(_stop_runtime)


async def _start_runtime(app, _loop=None):
    configured = app.config.get("REGISTRY_MEMBERSHIP_REFRESH_RUNTIME")
    if configured is None:
        return
    if getattr(app.ctx, "registry_membership_refresh_cleanup", None) is not None:
        raise ValueError("registry_membership_refresh_runtime_already_started")
    from process.registry_membership_refresh_runtime import build_registry_membership_refresh_runtime

    cleanup = AsyncExitStack()
    try:
        bundle = await build_registry_membership_refresh_runtime(configured, cleanup)
        if bundle is None:
            await cleanup.aclose()
            return
        if any(getattr(app.ctx, field, None) is not None for field in _FIELDS):
            raise ValueError("registry_membership_refresh_runtime_already_started")
        owned = (
            bundle.runtime.authorize,
            bundle.runtime.address_profile,
            bundle.inputs_service,
            bundle.anchors_service,
        )
        for field, capability in zip(_FIELDS, owned, strict=True):
            setattr(app.ctx, field, capability)
        app.ctx.registry_membership_refresh_owned = owned
        app.ctx.registry_membership_refresh_cleanup = cleanup
    except BaseException:
        await drain_operation(cleanup.aclose(), preserve_cancellation=False)
        raise


async def _stop_runtime(app, _loop=None):
    cleanup = getattr(app.ctx, "registry_membership_refresh_cleanup", None)
    if cleanup is None:
        return
    owned = app.ctx.registry_membership_refresh_owned
    for field, capability in zip(_FIELDS, owned, strict=True):
        if getattr(app.ctx, field, None) is capability:
            setattr(app.ctx, field, None)
    app.ctx.registry_membership_refresh_owned = None
    app.ctx.registry_membership_refresh_cleanup = None
    await drain_operation(cleanup.aclose(), preserve_cancellation=False)
