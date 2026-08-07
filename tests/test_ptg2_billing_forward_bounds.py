# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request-local bounds for exact billing forward occurrence reads."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


class _AuditAbort(BaseException):
    pass


def _read_options() -> dict[str, int]:
    return {
        "shared_snapshot_key": 17,
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 32,
    }


@pytest.mark.asyncio
async def test_occurrence_lookup_forwards_retention_budget_and_result_cap(
    monkeypatch,
) -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    async def visit(_session, _codes, _options, consumer, *, retention_budget):
        assert retention_budget is budget
        consumer(10, 3, 100, 0)
        consumer(10, 3, 100, 0)

    monkeypatch.setattr(
        sidecars, "_visit_forward_batch_keys", AsyncMock(side_effect=visit)
    )

    result = await sidecars.lookup_forward_occurrences_batch_from_db(
        object(),
        (10,),
        retention_budget=budget,
        max_occurrences=2,
        **_read_options(),
    )

    assert result == {10: ((3, 100, 0), (3, 100, 0))}
    retained_bytes = sidecars.forward_occurrence_batch_retained_bytes(result)
    assert budget.retained_bytes == retained_bytes
    assert budget.peak_retained_bytes > retained_bytes
    budget.release(retained_bytes)
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_occurrence_lookup_fails_before_retaining_past_result_cap(
    monkeypatch,
) -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    async def visit(_session, _codes, _options, consumer, *, retention_budget):
        assert retention_budget is budget
        consumer(10, 3, 100, 0)
        consumer(10, 3, 101, 0)

    monkeypatch.setattr(
        sidecars, "_visit_forward_batch_keys", AsyncMock(side_effect=visit)
    )

    with pytest.raises(PTG2ManifestArtifactError, match="result exceeds its limit"):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            retention_budget=budget,
            max_occurrences=1,
            **_read_options(),
        )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_occurrence_lookup_claims_before_retaining_first_row(
    monkeypatch,
) -> None:
    code_key_bytes = (
        sidecars.INTEGER_KEY_TUPLE_BYTES
        + sidecars.INTEGER_KEY_TUPLE_MEMBERSHIP_BYTES
    )
    mutable_container_bytes = (
        sidecars._FORWARD_OCCURRENCE_BATCH_MAP_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_BATCH_MUTABLE_CODE_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            code_key_bytes
            + mutable_container_bytes
            + sidecars._FORWARD_OCCURRENCE_BATCH_ROW_RETAINED_BYTES
            - 1
        )
    )

    async def visit(_session, _codes, _options, consumer, *, retention_budget):
        assert retention_budget is budget
        consumer(10, 3, 100, 0)

    monkeypatch.setattr(
        sidecars, "_visit_forward_batch_keys", AsyncMock(side_effect=visit)
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="forward occurrence result row",
    ):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            retention_budget=budget,
            max_occurrences=1,
            **_read_options(),
        )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_occurrence_lookup_releases_partial_rows_on_base_exception(
    monkeypatch,
) -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    async def visit(_session, _codes, _options, consumer, *, retention_budget):
        assert retention_budget is budget
        consumer(10, 3, 100, 0)
        raise _AuditAbort("stop")

    monkeypatch.setattr(
        sidecars, "_visit_forward_batch_keys", AsyncMock(side_effect=visit)
    )

    with pytest.raises(_AuditAbort, match="stop"):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            retention_budget=budget,
            max_occurrences=2,
            **_read_options(),
        )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_occurrence_lookup_restores_entry_budget_after_untracked_failure(
    monkeypatch,
) -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    entry_bytes = 64
    budget.claim(entry_bytes, category="the caller sentinel")

    async def abandon_claim(*_args, **_kwargs):
        budget.claim(
            sidecars._FORWARD_OCCURRENCE_BATCH_ROW_RETAINED_BYTES,
            category="an interrupted forward occurrence row",
        )
        raise _AuditAbort("stop")

    monkeypatch.setattr(
        sidecars,
        "_lookup_forward_occurrences_batch_claimed",
        AsyncMock(side_effect=abandon_claim),
    )

    with pytest.raises(_AuditAbort, match="stop"):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            retention_budget=budget,
            max_occurrences=1,
            **_read_options(),
        )
    assert budget.retained_bytes == entry_bytes
    budget.release(entry_bytes)


@pytest.mark.asyncio
async def test_occurrence_lookup_accounts_and_cleans_up_freeze_peak(
    monkeypatch,
) -> None:
    code_key_bytes = (
        sidecars.INTEGER_KEY_TUPLE_BYTES
        + sidecars.INTEGER_KEY_TUPLE_MEMBERSHIP_BYTES
    )
    mutable_bytes = (
        sidecars._FORWARD_OCCURRENCE_BATCH_MAP_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_BATCH_MUTABLE_CODE_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_BATCH_ROW_RETAINED_BYTES
    )
    freeze_bytes = (
        sidecars._FORWARD_OCCURRENCE_BATCH_MAP_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_BATCH_FROZEN_CODE_RETAINED_BYTES
        + sidecars._FORWARD_OCCURRENCE_BATCH_FROZEN_ROW_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=code_key_bytes + mutable_bytes + freeze_bytes - 1
    )

    async def visit(_session, _codes, _options, consumer, *, retention_budget):
        assert retention_budget is budget
        consumer(10, 3, 100, 0)

    monkeypatch.setattr(
        sidecars, "_visit_forward_batch_keys", AsyncMock(side_effect=visit)
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="frozen forward occurrence result",
    ):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            retention_budget=budget,
            max_occurrences=1,
            **_read_options(),
        )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_limit", (-1, True, 1.5, "1"))
async def test_occurrence_lookup_rejects_invalid_result_cap(
    monkeypatch,
    invalid_limit,
) -> None:
    visit = AsyncMock()
    monkeypatch.setattr(sidecars, "_visit_forward_batch_keys", visit)
    with pytest.raises(ValueError, match="must not be negative"):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (10,),
            max_occurrences=invalid_limit,
            **_read_options(),
        )
    visit.assert_not_awaited()


@pytest.mark.asyncio
async def test_occurrence_lookup_rejects_invalid_cap_for_empty_code_scope() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        await sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (),
            max_occurrences=True,
            **_read_options(),
        )


@pytest.mark.asyncio
async def test_empty_occurrence_scope_retains_no_result_claim() -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    result = await sidecars.lookup_forward_occurrences_batch_from_db(
        object(),
        (),
        retention_budget=budget,
        max_occurrences=0,
        **_read_options(),
    )

    assert result == {}
    assert sidecars.forward_occurrence_batch_retained_bytes(result) == 0
    assert budget.retained_bytes == 0
