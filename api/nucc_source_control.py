# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep protected taxonomy control actions on their genuine Source transaction."""

import re
from contextlib import asynccontextmanager
from datetime import datetime

from db.models import db
from process import nucc_protected_control as control
from process.control_lifecycle import _is_protected_nucc_claim_parent
from process.nucc_snapshot_partition import capture_nucc_parent


@asynccontextmanager
async def _source_action_session():
    """Reuse an existing original writer without creating a nested Source transaction."""
    binding = db._transaction_binding()
    if binding is not None:
        yield binding.session
    else:
        async with db.transaction() as session:
            yield session


@asynccontextmanager
async def source_action(run_id):
    """Fence an actual protected run; release the probe before legacy actions."""
    async with _source_action_session() as session:
        if await _is_protected_nucc_claim_parent(session):
            receipt = await control.reconcile_source_publication(session, run_id)
            await capture_nucc_parent(session)
            actor = await control._source_transaction(session)
            current = await control._locked_source_run(session, run_id)
            if (
                current["run_id"] != run_id
                or current["engine"] != "healthcare-mrf-api"
                or current["importer"] != "nucc"
            ):
                raise RuntimeError("NUCC Source action identity differs")
            if receipt is None and current["status"] not in {"canceling", "canceled", "cancelled"}:
                from process.nucc_protected_source import reconcile_nucc_source_handoff

                if await reconcile_nucc_source_handoff(session, run_id) is not None:
                    current = await control._locked_source_run(session, run_id)
            yield current, receipt
            await control._recheck_source_transaction(session, actor)
            return
    yield None


def require_source_attempt(run):
    """Allow an unclaimed queued run, or require its complete native attempt."""
    progress = run.get("progress")
    if type(progress) is not dict:
        raise RuntimeError("NUCC Source progress differs")
    attempt = progress.get("attempt_id"), progress.get("attempt_started_at")
    if attempt == (None, None) and run["status"] == "queued":
        return
    if (
        any(type(value) is not str or not value or value != value.strip() for value in attempt)
        or re.fullmatch(re.escape(run["run_id"]) + r":[0-9a-f]{32}", attempt[0]) is None
    ):
        raise RuntimeError("NUCC Source attempt differs")
    try:
        started = datetime.fromisoformat(attempt[1].replace("Z", "+00:00"))
    except ValueError:
        raise RuntimeError("NUCC Source attempt time differs") from None
    if started.tzinfo is None:
        raise RuntimeError("NUCC Source attempt time differs")


def require_unpublished_source(run):
    """A durable handoff remains Publisher-owned until genuine reconciliation."""
    require_source_attempt(run)
    metrics = run.get("metrics")
    if type(metrics) is not dict or metrics.get("nucc_handoff") is not None:
        raise RuntimeError("NUCC Source publication is pending")
