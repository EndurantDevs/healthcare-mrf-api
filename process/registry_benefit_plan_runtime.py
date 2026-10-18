# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own the server-configured selected-plan metadata client per API worker."""

import os

from process.registry_benefit_plan_metadata import (
    ORIGIN_CONFIG,
    TOKEN_CONFIG,
    RegistryBenefitPlanMetadataClient,
)


def register_registry_benefit_plan_runtime(app):
    """Use only trusted configuration; requests cannot select metadata authority."""
    if getattr(app.ctx, "registry_plan_metadata_runtime_registered", False):
        raise ValueError("registry_plan_metadata_runtime_already_registered")
    app.ctx.registry_plan_metadata_runtime_registered = True
    app.listener("before_server_start")(_start_runtime)
    app.listener("after_server_stop")(_stop_runtime)


async def _start_runtime(app, _loop=None):
    if getattr(app.ctx, "registry_plan_metadata_client", None) is not None:
        raise ValueError("registry_plan_metadata_runtime_already_started")
    configured_by_name = {
        key: app.config.get(key, app.config.get(key.removeprefix("HLTHPRT_"), os.environ.get(key)))
        for key in (ORIGIN_CONFIG, TOKEN_CONFIG)
    }
    client = RegistryBenefitPlanMetadataClient.from_config(configured_by_name)
    if client is not None:
        await client.__aenter__()
    app.ctx.registry_plan_metadata_client = client


async def _stop_runtime(app, _loop=None):
    client = getattr(app.ctx, "registry_plan_metadata_client", None)
    app.ctx.registry_plan_metadata_client = None
    if client is not None:
        await client.aclose()
