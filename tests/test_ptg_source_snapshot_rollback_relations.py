# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict-serving relation checks for exact snapshot rollback."""

from __future__ import annotations

import pytest

from tests.test_ptg_source_snapshot_rollback import (
    _activated_attestation,
    _context,
    _decision,
)


@pytest.mark.parametrize(
    ("context_override", "message"),
    [
        (
            {"target_snapshot_scope_by_field": {}},
            "no immutable snapshot scope",
        ),
        (
            {
                "target_plan_scope_records": (
                    {"plan_id": "another-plan", "plan_market_type": "group"},
                )
            },
            "absent from logical plan mappings",
        ),
        (
            {
                "target_attestation_by_field": _activated_attestation(
                    activated_at=None
                )
            },
            "no activated source-matched audit attestation",
        ),
        (
            {
                "target_attestation_by_field": _activated_attestation(
                    source_key="another_source"
                )
            },
            "no activated source-matched audit attestation",
        ),
        (
            {
                "target_attestation_by_field": _activated_attestation(
                    coverage_scope_id=b"x" * 32
                )
            },
            "no activated source-matched audit attestation",
        ),
    ],
)
def test_rollback_rejects_incomplete_resolver_relations_before_mutation(
    context_override,
    message,
):
    with pytest.raises(ValueError, match=message):
        _decision(_context(**context_override))
