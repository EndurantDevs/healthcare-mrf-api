# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-set membership boundaries for pricing projection v3."""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_provider as provider_stage
from api import ptg2_serving as serving
from tests.test_plan_pricing_projection_v3 import (
    _ExecuteSession,
    _binding,
    _provider_metadata,
)


async def _stage_memberships(
    monkeypatch,
    metadata_by_id,
    npis_by_set,
    provider_sets,
    binding_ordinal: int,
):
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value=metadata_by_id),
    )
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value=npis_by_set),
    )
    inserted_rows = []

    async def insert_batches(_session, statement, rows):
        inserted_rows.append((statement, list(rows)))

    state = projection._BuildState(hashlib.sha256())
    session = _ExecuteSession()
    await provider_stage._stage_provider_set_batch(
        session,
        _binding(binding_ordinal),
        provider_sets,
        state,
        insert_batches=insert_batches,
    )
    return inserted_rows, state, session


@pytest.mark.asyncio
async def test_provider_set_membership_keeps_complete_set_semantics(
    monkeypatch,
) -> None:
    first_id = "1" * 32
    second_id = "2" * 32
    inserted_rows, state, session = await _stage_memberships(
        monkeypatch,
        _provider_metadata((first_id, 7, 2), (second_id, 8, 1)),
        {first_id: (12, 11), second_id: (13,)},
        [
            {"provider_set_key": 7, "provider_set_id": first_id},
            {"provider_set_key": 8, "provider_set_id": second_id},
        ],
        3,
    )

    assert [
        provider_set_by_field["membership_count"]
        for provider_set_by_field in inserted_rows[0][1]
    ] == [2, 1]
    assert inserted_rows[1][1] == [
        {"binding_ordinal": 3, "provider_set_key": 7, "npi": 11},
        {"binding_ordinal": 3, "provider_set_key": 7, "npi": 12},
        {"binding_ordinal": 3, "provider_set_key": 8, "npi": 13},
    ]
    expected_digest = hashlib.sha256()
    for provider_set_key, npi in ((7, 11), (7, 12), (8, 13)):
        projection.digest_row(
            expected_digest,
            "provider-membership",
            (3, provider_set_key, npi),
            b"",
        )
    assert state.provider_membership_count == 3
    assert state.staged_provider_set_count == 2
    assert state.content_digest.digest() == expected_digest.digest()
    assert "plan_pricing_provider_npi_pending_stage" in session.calls[0][0]
    assert all(
        "plan_pricing_provider_membership" not in statement
        for statement, _parameters in session.calls
    )


@pytest.mark.asyncio
async def test_declared_empty_provider_set_contributes_no_memberships(
    monkeypatch,
) -> None:
    empty_provider_set_id = "1" * 32
    populated_provider_set_id = "2" * 32
    inserted_rows, state, _session = await _stage_memberships(
        monkeypatch,
        _provider_metadata(
            (empty_provider_set_id, 7, 0),
            (populated_provider_set_id, 8, 1),
        ),
        {empty_provider_set_id: (), populated_provider_set_id: (11,)},
        [
            {"provider_set_key": 7, "provider_set_id": empty_provider_set_id},
            {
                "provider_set_key": 8,
                "provider_set_id": populated_provider_set_id,
            },
        ],
        0,
    )

    assert [
        provider_set_by_field["membership_count"]
        for provider_set_by_field in inserted_rows[0][1]
    ] == [0, 1]
    assert inserted_rows[1][1] == [
        {"binding_ordinal": 0, "provider_set_key": 8, "npi": 11}
    ]
    assert state.provider_membership_count == 1


@pytest.mark.parametrize(
    ("metadata_by_id", "npis_by_set"),
    [
        ({}, {"1" * 32: ()}),
        (_provider_metadata(("1" * 32, 7, 1)), {"1" * 32: ()}),
    ],
)
def test_provider_membership_requires_authoritative_count_parity(
    metadata_by_id,
    npis_by_set,
) -> None:
    with pytest.raises(ValueError, match="membership is incomplete"):
        provider_stage._validate_provider_set_memberships(
            [{"provider_set_key": 7, "provider_set_id": "1" * 32}],
            metadata_by_id,
            npis_by_set,
        )
