# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact code lookup tests for billing-search POST serving."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_code_reader as reader
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_post_support import binding, serving_tables


@pytest.mark.asyncio
async def test_code_lookup_is_exact_and_release_binding_scoped(monkeypatch) -> None:
    code_rows = AsyncMock(
        return_value=[
            {
                "code_key": 5,
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "plan_id": "synthetic-plan-token",
                "plan_market_type": "group",
                "negotiation_arrangement": "ffs",
                "billing_code_type_version": "2026",
                "source_name": None,
                "source_description": None,
            }
        ]
    )
    monkeypatch.setattr(reader.ptg2_serving, "_manifest_reverse_code_rows", code_rows)

    witnesses = await reader.load_exact_billing_code_witnesses(
        object(),
        serving_tables(),
        binding(),
        code_system="CPT",
        code="99213",
    )

    assert [
        (witness.code_key, witness.code_system, witness.code) for witness in witnesses
    ] == [(5, "CPT", "99213")]
    assert code_rows.await_args.kwargs == {
        "requested_plan": "synthetic-plan-token",
        "plan_market_type": "group",
        "code_value": "99213",
        "code_system": "CPT",
        "q_text": "",
        "code_context": None,
        "limit_rows": 257,
        "offset_rows": 0,
    }


@pytest.mark.asyncio
async def test_code_lookup_rejects_a_row_from_another_plan(monkeypatch) -> None:
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_reverse_code_rows",
        AsyncMock(
            return_value=[
                {
                    "code_key": 5,
                    "reported_code_system": "CPT",
                    "reported_code": "99213",
                    "plan_id": "different-plan-token",
                    "plan_market_type": "group",
                }
            ]
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="crossed"):
        await reader.load_exact_billing_code_witnesses(
            object(),
            serving_tables(),
            binding(),
            code_system="CPT",
            code="99213",
        )


@pytest.mark.asyncio
async def test_code_lookup_fails_closed_when_dictionary_is_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_reverse_code_rows",
        AsyncMock(return_value=None),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="unavailable"):
        await reader.load_exact_billing_code_witnesses(
            object(),
            serving_tables(),
            binding(),
            code_system="CPT",
            code="99213",
        )
