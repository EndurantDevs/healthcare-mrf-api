# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed public-serving admission failures for sealed PTG2 work budgets."""

from __future__ import annotations

from api.ptg2_shared_blocks import PTG2SharedBlockError


class PTG2OnlineWorkBudgetExceeded(PTG2SharedBlockError):
    """Raised when an exact query cannot fit its sealed online-work budget."""

    error_code = "ptg2_online_work_budget_exceeded"

    def __init__(self, dimension: str, *, message: str | None = None) -> None:
        self.dimension = str(dimension or "unknown")
        super().__init__(
            message
            or (
                "PTG2 exact query exceeds its sealed online "
                f"{self.dimension} budget"
            )
        )


__all__ = ["PTG2OnlineWorkBudgetExceeded"]
