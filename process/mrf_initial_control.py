# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Report initialization failures without completing the MRF parser fan-out."""

import logging

from process import initial
from process.control_lifecycle import (
    _control_failure_error,
    _flush_terminal_status_events,
    mark_control_run,
)
from process.import_status_events import bind_status_event_loop

logger = logging.getLogger(__name__)


async def init_file(ctx, task=None):
    """Keep the registered ARQ entry point and publish initialization failures."""

    bind_status_event_loop()
    try:
        return await initial.init_file(ctx, task)
    except Exception as error:
        run_id = str(task.get("run_id") or "").strip() if isinstance(task, dict) else ""
        if run_id:
            try:
                await mark_control_run(
                    run_id,
                    status="failed",
                    phase_detail="mrf initialization failed",
                    progress_message="initialization failed",
                    error=_control_failure_error(error),
                )
                await _flush_terminal_status_events(run_id)
            except Exception:
                logger.exception("Failed to report MRF initialization failure")
        raise
