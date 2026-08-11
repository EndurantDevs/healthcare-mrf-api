"""Independent bounded projection of non-gating PTG compatibility state."""

from __future__ import annotations

import asyncio
import logging
import os

from process.ptg_parts.ptg2_legacy_global_projection_queue import (
    drain_legacy_global_projection_queue,
)
from process.ptg_parts.ptg2_plan_catalog_outbox import (
    drain_immutable_plan_catalog_outbox,
)


logger = logging.getLogger(__name__)


async def run_ptg_compatibility_projector() -> None:
    """Drain migration-owned compatibility queues without wave authority."""

    interval = max(
        float(
            os.getenv(
                "HLTHPRT_PTG_COMPATIBILITY_PROJECTOR_INTERVAL_SECONDS",
                "2",
            )
        ),
        0.25,
    )
    while True:
        try:
            await drain_immutable_plan_catalog_outbox(max_requests=8)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PTG plan catalog compatibility drain deferred")
        try:
            await drain_legacy_global_projection_queue(max_requests=8)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PTG legacy global pointer projection deferred")
        await asyncio.sleep(interval)


__all__ = ["run_ptg_compatibility_projector"]
