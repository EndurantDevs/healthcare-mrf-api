# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Register the separate PTG V4 recovery control capabilities."""

from __future__ import annotations

from api.control_ptg2_v4_stale_metadata import register_v4_stale_routes
from api.control_ptg_v4_recovery import register_v4_recovery_routes


def register_v4_control_routes(control_blueprint) -> None:
    """Register metadata-only and physical-layout recovery routes."""

    register_v4_stale_routes(control_blueprint)
    register_v4_recovery_routes(control_blueprint)


__all__ = ["register_v4_control_routes"]
