# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read existing queue and exact-worker evidence while root controls are locked."""

import asyncio

from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError


async def require_stopped_lineage(lineage):
    """A terminal result may remain; executable work and unavailable evidence reject."""
    from api.control_imports import _arq_worker_presence
    from api.control_workers import exact_worker_presence

    for run in lineage:
        arq = await _arq_worker_presence(dict(run))
        if any(arq.get(key) is not False for key in ("queue_member", "job", "retry", "in_progress")):
            raise TerminalRootRetirementError("busy")
        presence = await asyncio.to_thread(
            exact_worker_presence, {"run_id": run["run_id"], "importer": "provider-directory-fhir", "role": "start"}
        )
        if presence.get("enabled") is not True or presence.get("stopped") is not True:
            raise TerminalRootRetirementError("busy")
