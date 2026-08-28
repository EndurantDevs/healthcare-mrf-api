# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-wide code and price admission for projection v3."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as code_stage
from api import ptg2_serving as serving
from api.plan_pricing_projection_source import BindingProjection
from tests.test_plan_pricing_projection_v3 import _ExecuteSession, _binding


@pytest.mark.asyncio
async def test_normalized_code_occurrence_bound_precedes_binding_reads(
    monkeypatch,
) -> None:
    monkeypatch.setattr(code_stage, "MAX_CODE_OCCURRENCES", 3)
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 2
    )
    binding_code_rows = AsyncMock()

    with pytest.raises(ValueError, match="normalized occurrence bound"):
        await code_stage._has_staged_code_inputs(
            _ExecuteSession(),
            projection._BuildState(hashlib.sha256()),
            ("CPT", "27447"),
            [_binding(0), _binding(1)],
            binding_code_rows=binding_code_rows,
            stage_code_provider_sets=AsyncMock(),
        )

    binding_code_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_normalized_code_atom_bound_precedes_any_binding_stage(
    monkeypatch,
) -> None:
    monkeypatch.setattr(code_stage, "MAX_CODE_PRICE_ATOMS", 3)
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 1
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)

    async def binding_code_rows(_session, binding, _code_rows):
        ordinal = int(binding.binding["ordinal"])
        price_set_id = str(ordinal + 1) * 32
        return (
            [
                {
                    "_ptg_provider_set_key": ordinal + 10,
                    "price_set_global_id_128": price_set_id,
                }
            ],
            {
                price_set_id: (
                    {"negotiated_rate": "1"},
                    {"negotiated_rate": "2"},
                )
            },
        )

    stage_code_provider_sets = AsyncMock()
    with pytest.raises(ValueError, match="normalized price-atom bound"):
        await code_stage._has_staged_code_inputs(
            _ExecuteSession(),
            projection._BuildState(hashlib.sha256()),
            ("CPT", "27447"),
            [_binding(0), _binding(1)],
            binding_code_rows=binding_code_rows,
            stage_code_provider_sets=stage_code_provider_sets,
        )

    stage_code_provider_sets.assert_not_awaited()


@pytest.mark.asyncio
async def test_numeric_cpt_hcpcs_aliases_keep_occurrence_multiplicity(
    monkeypatch,
) -> None:
    """Canonical aliases retain occurrence multiplicity without duplicating atoms."""

    price_set_id = "1" * 32
    provider_set_id = "2" * 32
    code_rows = [
        {"reported_code_system": system, "reported_code": "27447"}
        for system in ("CPT", "HCPCS")
    ]
    binding = BindingProjection(
        {"ordinal": 0},
        SimpleNamespace(network_names=()),
        {("CPT", "27447"): code_rows},
    )
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda rows: len(rows))
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)

    async def binding_code_rows(_session, _binding, rows):
        assert rows == code_rows
        serving_row_by_field = {
            "_ptg_provider_set_key": 7,
            "provider_set_global_id_128": provider_set_id,
            "price_set_global_id_128": price_set_id,
        }
        return [serving_row_by_field, dict(serving_row_by_field)], {
            price_set_id: ({"negotiated_rate": "10"},)
        }

    session = _ExecuteSession()
    assert await code_stage._has_staged_code_inputs(
        session,
        projection._BuildState(hashlib.sha256()),
        ("CPT", "27447"),
        [binding],
        binding_code_rows=binding_code_rows,
        stage_code_provider_sets=AsyncMock(),
    )
    occurrence_rows = next(
        parameters
        for statement, parameters in session.calls
        if "INSERT INTO plan_pricing_code_occurrence_stage" in statement
    )
    price_rows = next(
        parameters
        for statement, parameters in session.calls
        if "INSERT INTO plan_pricing_price_rate_stage" in statement
    )
    assert occurrence_rows == [
        {
            "binding_ordinal": 0,
            "provider_set_key": 7,
            "price_set_id": price_set_id,
            "occurrence_count": 2,
        }
    ]
    assert len(price_rows) == 1
