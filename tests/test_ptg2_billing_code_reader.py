# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact release-bound billing code reader tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_code_reader as reader
from api.plan_release_serving import PlanReleaseSnapshotBinding
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

SNAPSHOT_ID = "ptg2:203101:synthetic"


def _tables(**overrides) -> PTG2ServingTables:
    fields_by_name = {
        "snapshot_id": SNAPSHOT_ID,
        "arch_version": "postgres_binary_v3",
        "shared_snapshot_key": 17,
        "storage_generation": "shared_blocks_v4",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "source_count": 1,
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "source_key": "synthetic-source",
    }
    fields_by_name.update(overrides)
    return PTG2ServingTables(**fields_by_name)


def _binding(**overrides) -> PlanReleaseSnapshotBinding:
    fields_by_name = {
        "binding_ordinal": 0,
        "snapshot_id": SNAPSHOT_ID,
        "source_key": "synthetic-source",
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "role": "in_network",
        "required": True,
    }
    fields_by_name.update(overrides)
    return PlanReleaseSnapshotBinding(**fields_by_name)


def _row(**overrides):
    fields_by_name = {
        "code_key": 7,
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_name": "Synthetic service",
        "source_description": "Synthetic description",
    }
    fields_by_name.update(overrides)
    return fields_by_name


@pytest.mark.asyncio
async def test_code_reader_binds_exact_snapshot_plan_market_and_code(
    monkeypatch,
) -> None:
    lookup = AsyncMock(
        return_value=[
            _row(code_key=9),
            _row(
                code_key=7,
                negotiation_arrangement=None,
                billing_code_type_version=None,
                source_name=None,
                source_description=None,
            ),
        ]
    )
    monkeypatch.setattr(reader.ptg2_serving, "_manifest_reverse_code_rows", lookup)

    witnesses = await reader.load_exact_billing_code_witnesses(
        object(),
        _tables(),
        _binding(),
        code_system="CPT",
        code="99213",
    )

    assert tuple(witness.code_key for witness in witnesses) == (7, 9)
    assert all(witness.code_system == "CPT" for witness in witnesses)
    assert witnesses[0].stable_sort_key == ("CPT", "99213", "", 7)
    assert repr(witnesses[0]) == (
        "<billing-code-witness " "code_system=CPT code=99213 code_key=<internal>>"
    )
    assert (
        witnesses[0].negotiation_arrangement,
        witnesses[0].billing_code_type_version,
        witnesses[0].source_name,
        witnesses[0].source_description,
    ) == (None, None, None, None)
    lookup.assert_awaited_once()
    lookup_call = lookup.await_args
    assert lookup_call.args[1] == _tables()
    assert lookup_call.kwargs == {
        "requested_plan": "synthetic-plan",
        "plan_market_type": "group",
        "code_value": "99213",
        "code_system": "CPT",
        "q_text": "",
        "code_context": None,
        "limit_rows": reader.MAX_EXACT_BILLING_CODE_WITNESSES + 1,
        "offset_rows": 0,
    }


@pytest.mark.asyncio
async def test_code_reader_returns_an_explicit_empty_exact_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_reverse_code_rows",
        AsyncMock(return_value=[]),
    )

    assert (
        await reader.load_exact_billing_code_witnesses(
            object(),
            _tables(),
            _binding(),
            code_system="CPT",
            code="99213",
        )
        == ()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        None,
        ["malformed-row"],
        [_row(code_key=7), _row(code_key=7)],
        [_row(plan_id="different")],
        [_row(plan_market_type="individual")],
        [_row(reported_code_system="HCPCS")],
        [_row(reported_code="99214")],
        [_row(code_key=True)],
        [_row(source_name="line\nbreak")],
        [
            _row(code_key=index)
            for index in range(reader.MAX_EXACT_BILLING_CODE_WITNESSES + 1)
        ],
    ],
)
async def test_code_reader_rejects_unavailable_crossed_or_malformed_rows(
    monkeypatch,
    rows,
) -> None:
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_reverse_code_rows",
        AsyncMock(return_value=rows),
    )

    with pytest.raises(PTG2ManifestArtifactError):
        await reader.load_exact_billing_code_witnesses(
            object(),
            _tables(),
            _binding(),
            code_system="CPT",
            code="99213",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tables", "binding", "code_system", "code"),
    [
        (_tables(storage_generation="shared_blocks_v3"), _binding(), "CPT", "99213"),
        (_tables(snapshot_id="other"), _binding(), "CPT", "99213"),
        (_tables(plan_id="different"), _binding(), "CPT", "99213"),
        (_tables(plan_market_type="individual"), _binding(), "CPT", "99213"),
        (_tables(source_key="different"), _binding(), "CPT", "99213"),
        (_tables(), _binding(role="allowed_amounts"), "CPT", "99213"),
        (_tables(), _binding(), None, "99213"),
        (_tables(), _binding(), "CPT", "123"),
        (_tables(), _binding(), "cpt", "99213"),
        (_tables(), _binding(), "CPT", " 99213"),
    ],
)
async def test_code_reader_fails_closed_before_lookup(
    monkeypatch,
    tables,
    binding,
    code_system,
    code,
) -> None:
    lookup = AsyncMock()
    monkeypatch.setattr(reader.ptg2_serving, "_manifest_reverse_code_rows", lookup)

    with pytest.raises(PTG2ManifestArtifactError):
        await reader.load_exact_billing_code_witnesses(
            object(),
            tables,
            binding,
            code_system=code_system,
            code=code,
        )

    lookup.assert_not_awaited()
