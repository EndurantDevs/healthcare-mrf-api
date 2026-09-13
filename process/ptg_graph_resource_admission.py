# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded terminal evidence for provider graph resource admission failures."""

from __future__ import annotations

import os
import re
from typing import Any, Mapping


class V4GraphResourceAdmissionError(RuntimeError):
    """The compiler rejected graph allocation under its active resource policy."""

    def __init__(
        self,
        message: str,
        *,
        input_bytes: int | None = None,
        factor_edges: int | None = None,
        factor_owners: int | None = None,
        options: Mapping[str, int] | None = None,
    ) -> None:
        super().__init__(message)
        evidence_by_name = {
            "input_factor_bytes": input_bytes,
            "factor_edge_count": factor_edges,
            "factor_owner_count": factor_owners,
            "max_factor_edges": (options or {}).get("max_factor_edges"),
            "max_estimated_model_bytes": (options or {}).get("max_estimated_model_bytes"),
        }
        matched = re.search(
            r"resource_admission: estimated peak bytes ([0-9]+) exceeds configured limit ([0-9]+)",
            message,
        )
        if matched:
            evidence_by_name["estimated_peak_bytes"] = int(matched[1])
        self.resource_admission: dict[str, Any] = {
            "version": 1,
            **{key: value for key, value in evidence_by_name.items() if type(value) is int and value >= 0},
        }


def graph_admission_environment_name(name: str) -> str:
    """Select an explicitly configured large-worker admission policy."""
    lane = {
        "process.PTGLarge": "LARGE",
        "process.PTGHuge": "HUGE",
    }.get(os.getenv("HLTHPRT_ACTIVE_WORKER_CLASS", "").strip())
    override = f"{name}_{lane}" if lane else name
    if override == name or override not in os.environ:
        return name
    value = os.environ[override].strip()
    if not value.isascii() or not value.isdecimal() or int(value) <= 0:
        raise ValueError(f"{override} must be a positive decimal integer")
    return override
