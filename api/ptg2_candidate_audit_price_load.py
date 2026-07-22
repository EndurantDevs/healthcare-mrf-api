# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Materialize exact candidate prices and their bounded selection ledger."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from api.ptg2_candidate_audit_projection import CandidatePriceData


@dataclass(frozen=True)
class CandidatePriceLoad:
    """Candidate prices plus proof that only exact forward rows were retained."""

    data: CandidatePriceData
    selection_io: Mapping[str, int]


def build_candidate_price_load(
    price_keys_by_occurrence: Mapping[
        tuple[int, int, int], tuple[int, ...]
    ],
    required_occurrence_keys: frozenset[tuple[int, int, int]],
    hydration: Any,
) -> CandidatePriceLoad:
    """Bind hydrated payloads to exact occurrences and summarize selection I/O."""

    return CandidatePriceLoad(
        data=CandidatePriceData(
            price_keys_by_occurrence,
            hydration.atom_keys_by_price_key,
            hydration.prices_by_key,
        ),
        selection_io={
            "exact_candidate_occurrence_coordinates": len(
                required_occurrence_keys
            ),
            "exact_forward_occurrence_coordinates_returned": len(
                price_keys_by_occurrence
            ),
            "exact_forward_price_key_deliveries_returned": sum(
                len(price_keys)
                for price_keys in price_keys_by_occurrence.values()
            ),
        },
    )


__all__ = ["CandidatePriceLoad", "build_candidate_price_load"]
