# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from scripts.ptg_v4_dev_canary_identity import (
    bind_terminal_run_identity,
    normalize_terminal_run_identity,
    require_matching_database_identity,
)
from scripts.ptg_v4_dev_canary_io import CanaryInfrastructureError
from scripts.ptg_v4_dev_canary_support import CanaryConfigurationError


def _terminal_identity_timeline() -> dict[str, object]:
    return {
        "control_run_id": "run-1",
        "run": {
            "run_id": "run-1",
            "terminal_identity": {
                "control_run_id": "run-1",
                "snapshot_id": "snapshot-1",
                "import_run_id": "ptg2:import-1",
            },
        },
    }


def test_terminal_identity_rejects_operator_selected_snapshot() -> None:
    with pytest.raises(
        CanaryConfigurationError,
        match="differs from progress or CLI snapshot",
    ):
        bind_terminal_run_identity(
            _terminal_identity_timeline(),
            expected_snapshot_id="snapshot-from-another-run",
        )


def test_database_identity_rejects_mixed_run_evidence() -> None:
    terminal_identity_by_field = bind_terminal_run_identity(
        _terminal_identity_timeline(),
        expected_snapshot_id="snapshot-1",
    )

    with pytest.raises(
        CanaryConfigurationError,
        match="different terminal control run",
    ):
        require_matching_database_identity(
            {
                "snapshot": {
                    "snapshot_id": "snapshot-1",
                    "import_run_id": "ptg2:older-import",
                }
            },
            terminal_identity_by_field,
        )


def test_successful_control_identity_rejects_conflicting_terminal_metrics() -> None:
    with pytest.raises(
        CanaryInfrastructureError,
        match="differs from terminal PTG metrics",
    ):
        normalize_terminal_run_identity(
            {
                "run_id": "run-1",
                "snapshot_id": "snapshot-1",
                "metrics": {
                    "snapshot_id": "snapshot-2",
                    "import_run_id": "ptg2:import-1",
                },
            }
        )
