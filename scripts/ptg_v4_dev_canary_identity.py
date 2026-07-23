"""Fail-closed identity binding for PTG V4 canary evidence."""

from __future__ import annotations

from typing import Any, Mapping

from scripts.ptg_v4_dev_canary_io import CanaryInfrastructureError
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


TERMINAL_IDENTITY_FIELDS = frozenset(
    {"control_run_id", "snapshot_id", "import_run_id"}
)


def normalize_terminal_run_identity(
    control_run_by_field: Mapping[str, Any],
) -> dict[str, str]:
    """Normalize one successful control row's authoritative PTG identities."""

    metrics_by_field = control_run_by_field.get("metrics")
    if not isinstance(metrics_by_field, Mapping):
        raise CanaryInfrastructureError(
            "successful control run lacks terminal PTG metrics"
        )
    terminal_identity_by_field = {
        "control_run_id": _required_identity(
            control_run_by_field.get("run_id"),
            "control run",
        ),
        "snapshot_id": _required_identity(
            control_run_by_field.get("snapshot_id"),
            "control snapshot",
        ),
        "import_run_id": _required_identity(
            metrics_by_field.get("import_run_id"),
            "terminal PTG import run",
        ),
    }
    metrics_snapshot_id = _required_identity(
        metrics_by_field.get("snapshot_id"),
        "terminal PTG snapshot",
    )
    if metrics_snapshot_id != terminal_identity_by_field["snapshot_id"]:
        raise CanaryInfrastructureError(
            "control snapshot differs from terminal PTG metrics"
        )
    return terminal_identity_by_field


def bind_terminal_run_identity(
    timeline_by_field: Mapping[str, Any],
    *,
    expected_snapshot_id: str,
) -> dict[str, str]:
    """Fail closed unless progress, CLI, and terminal control identity agree."""

    control_run_by_field = timeline_by_field.get("run")
    terminal_identity_by_field = (
        control_run_by_field.get("terminal_identity")
        if isinstance(control_run_by_field, Mapping)
        else None
    )
    if (
        not isinstance(terminal_identity_by_field, Mapping)
        or set(terminal_identity_by_field) != TERMINAL_IDENTITY_FIELDS
    ):
        raise CanaryConfigurationError(
            "progress evidence lacks exact terminal PTG identity"
        )
    normalized_identity_by_field = {
        field_name: str(terminal_identity_by_field[field_name]).strip()
        for field_name in TERMINAL_IDENTITY_FIELDS
    }
    timeline_run_id = str(
        timeline_by_field.get("control_run_id") or ""
    ).strip()
    summarized_run_id = str(
        control_run_by_field.get("run_id") or ""
    ).strip()
    if (
        not all(normalized_identity_by_field.values())
        or normalized_identity_by_field["control_run_id"] != timeline_run_id
        or normalized_identity_by_field["control_run_id"] != summarized_run_id
        or normalized_identity_by_field["snapshot_id"]
        != str(expected_snapshot_id).strip()
    ):
        raise CanaryConfigurationError(
            "terminal control identity differs from progress or CLI snapshot"
        )
    return normalized_identity_by_field


def require_matching_database_identity(
    database_evidence_by_field: Mapping[str, Any],
    terminal_identity_by_field: Mapping[str, str],
) -> None:
    """Bind the sealed database snapshot to the monitored control attempt."""

    snapshot_by_field = database_evidence_by_field.get("snapshot")
    if not isinstance(snapshot_by_field, Mapping):
        raise CanaryConfigurationError("database snapshot identity is missing")
    database_identity_by_field = {
        "snapshot_id": str(snapshot_by_field.get("snapshot_id") or "").strip(),
        "import_run_id": str(
            snapshot_by_field.get("import_run_id") or ""
        ).strip(),
    }
    if any(
        database_identity_by_field[field_name]
        != terminal_identity_by_field[field_name]
        for field_name in database_identity_by_field
    ):
        raise CanaryConfigurationError(
            "database snapshot belongs to a different terminal control run"
        )


def _required_identity(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CanaryInfrastructureError(f"{label} identity is missing")
    return value.strip()
