# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read existing queue and exact-worker evidence while root controls are locked."""

import asyncio
import re

from process.provider_directory_terminal_root_retirement_contract import (
    TERMINAL_FAILURE_STATUSES,
    TerminalRootRetirementError,
)


async def _arq_presence_records(run):
    """Check a bound legacy random ID as well as the current canonical ID."""
    from api.control_imports import (
        _adapter_for_import_row,
        _arq_job_presence,
        _arq_worker_presence,
        _enqueue_job_options,
    )

    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    legacy_job_id = metrics.get("job_id")
    if isinstance(legacy_job_id, str) and re.fullmatch(r"[0-9a-f]{32}", legacy_job_id):
        adapter = _adapter_for_import_row(run)
        run_id = run.get("run_id")
        if (
            run.get("importer") != "provider-directory-fhir"
            or run.get("status") not in TERMINAL_FAILURE_STATUSES
            or not isinstance(run_id, str)
            or not run_id
            or run_id != run_id.strip()
            or adapter is None
            or metrics.get("queue") != adapter["queue"]
            or metrics.get("function") != adapter["function"]
        ):
            raise TerminalRootRetirementError("state_invalid")
        canonical_job_id = _enqueue_job_options(adapter, {"run_id": run_id})["_job_id"]
        return (
            await _arq_job_presence(adapter["queue"], canonical_job_id),
            await _arq_job_presence(adapter["queue"], legacy_job_id),
        )
    return (await _arq_worker_presence(run),)


async def require_stopped_lineage(lineage):
    """A terminal result may remain; executable work and unavailable evidence reject."""
    from api.control_workers import exact_worker_presence

    for run in lineage:
        for arq in await _arq_presence_records(dict(run)):
            if any(arq.get(key) is not False for key in ("queue_member", "job", "retry", "in_progress")):
                raise TerminalRootRetirementError("busy")
        presence = await asyncio.to_thread(
            exact_worker_presence, {"run_id": run["run_id"], "importer": "provider-directory-fhir", "role": "start"}
        )
        if presence.get("enabled") is not True or presence.get("stopped") is not True:
            raise TerminalRootRetirementError("busy")
