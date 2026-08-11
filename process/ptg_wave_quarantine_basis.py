"""Closed quarantine bases that release PTG admission capacity."""

from __future__ import annotations


MATERIALIZED_PRECLAIM_FAILURE_BASIS = "materialized_preclaim_failure"
V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS = (
    "v12_pristine_materialized_cutover"
)
CAPACITY_RELEASING_QUARANTINE_BASES = frozenset(
    {
        MATERIALIZED_PRECLAIM_FAILURE_BASIS,
        V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
    }
)


__all__ = [
    "CAPACITY_RELEASING_QUARANTINE_BASES",
    "MATERIALIZED_PRECLAIM_FAILURE_BASIS",
    "V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS",
]
