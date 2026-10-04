# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, safe progress observations for the CMS directory intake."""

from __future__ import annotations

import re
import time

from process.cms_npd_source import RESOURCE_FILES
from process.live_progress import enqueue_live_progress

_RESOURCE_FAMILIES = frozenset(resource_type for _, resource_type in RESOURCE_FILES)


_INTAKE_PHASES = frozenset(
    {
        "parameters",
        "source_probe",
        "acquisition",
        "retained_validation",
        "intake_guard",
        "release_validation",
        "candidate_setup",
        "staging",
        "count_validation",
        "witness_validation",
        "identity",
        "identity_validation",
        "relationships",
        "candidate_validation",
        "coverage",
        "followup",
        "complete",
    }
)


def intake_exception_chain(error: Exception) -> list[dict[str, str]]:
    """Keep only bounded exception classes and validated SQLSTATEs, never messages."""
    exception_nodes = []
    seen_error_ids = set()
    while error is not None and id(error) not in seen_error_ids and len(exception_nodes) < 6:
        seen_error_ids.add(id(error))
        name = type(error).__name__
        node_by_field = {"class": name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name) else "Exception"}
        sqlstate = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
        if isinstance(sqlstate, str) and re.fullmatch(r"[0-9A-Z]{5}", sqlstate):
            node_by_field["sqlstate"] = sqlstate
        exception_nodes.append(node_by_field)
        error = getattr(error, "orig", None) or error.__cause__
    return exception_nodes


def _publish_intake_observation(context, state, now, error, has_changed, force):
    """Retain safe audit fields and enqueue progress only at an observed transition or interval."""
    observation_by_field = {
        "phase": state["phase"],
        "completed_input_rows": state["completed_rows"],
        "elapsed_seconds": round(max(0.0, now - state["started"]), 3),
        "phase_elapsed_seconds": round(max(0.0, now - state["phase_started"]), 3),
    }
    if state["family"] is not None:
        observation_by_field["family"] = state["family"]
    if error is not None:
        observation_by_field["exception_chain"] = intake_exception_chain(error)
    audit = context.get("audit")
    context["audit"] = {**(audit if isinstance(audit, dict) else {}), "cms_intake": observation_by_field}
    if error is not None:
        return
    if has_changed or force or state["last_emitted"] is None or now - state["last_emitted"] >= 15:
        state["last_emitted"] = now
        enqueue_live_progress(
            source="cms-intake",
            confidence="observed",
            phase="cms-npd-" + state["phase"],
            unit="run",
            done=0,
            total=1,
            pct=0,
            message="CMS intake",
            label=state["family"],
            counters={"completed_input_rows": state["completed_rows"]},
            elapsed_seconds=observation_by_field["elapsed_seconds"],
            detail={
                key: observation_by_field[key]
                for key in ("family", "phase_elapsed_seconds")
                if key in observation_by_field
            },
        )


def observe_intake(ctx, *, phase=None, family=None, completed_rows=None, error=None, force=False):
    """Publish bounded observations without awaiting or affecting intake failure handling."""
    try:
        if not isinstance(ctx, dict):
            return
        context = ctx.setdefault("context", {})
        if not isinstance(context, dict):
            return
        if phase is not None and phase not in _INTAKE_PHASES or family is not None and family not in _RESOURCE_FAMILIES:
            return
        now = time.monotonic()
        state = context.setdefault(
            "_cms_intake_observation",
            {
                "started": now,
                "phase_started": now,
                "last_emitted": None,
                "phase": "parameters",
                "family": None,
                "completed_rows": 0,
            },
        )
        if (
            state["phase"] not in _INTAKE_PHASES
            or state["family"] is not None
            and state["family"] not in _RESOURCE_FAMILIES
            or type(state["completed_rows"]) is not int
            or not 0 <= state["completed_rows"] < 2**63
        ):
            return
        next_phase = phase or state["phase"]
        next_family = family if phase is not None else state["family"]
        has_changed = next_phase != state["phase"] or next_family != state["family"]
        if has_changed:
            state.update(phase=next_phase, family=next_family, phase_started=now, completed_rows=0)
        if type(completed_rows) is int and 0 <= completed_rows < 2**63:
            state["completed_rows"] = completed_rows
        _publish_intake_observation(context, state, now, error, has_changed, force)
    except Exception:
        # Telemetry must never replace the original import outcome or cancellation.
        return
