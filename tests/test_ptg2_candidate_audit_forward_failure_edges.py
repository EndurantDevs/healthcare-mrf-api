from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)


def _fragment_view():
    return sidecars._ForwardBatchFragmentView(
        code_key=7,
        block_key=0,
        fragment_no=0,
        provider_key_min=0,
        provider_key_max=1024,
        provider_filter=None,
        source_filter=None,
        occurrence_filter=None,
        fragment_row={"_block_hash": b"h" * 32},
    )


def test_unbudgeted_forward_coordinate_map_deduplicates_and_propagates():
    rows_by_coordinate, retained_bytes = (
        sidecars._retained_forward_rows_by_coordinate(
            (_fragment_view(), _fragment_view()),
            None,
        )
    )

    assert rows_by_coordinate == {(7, 0, 0): []}
    assert retained_bytes == sidecars._FORWARD_COORDINATE_RETAINED_BYTES

    def failing_views():
        yield _fragment_view()
        raise RuntimeError("view iteration failed")

    with pytest.raises(RuntimeError, match="view iteration failed"):
        sidecars._retained_forward_rows_by_coordinate(
            failing_views(),
            None,
        )


def test_unbudgeted_price_atom_release_is_a_noop():
    retained_atoms = sidecars._RetainedPriceAtoms(None, {})
    price_atom = object()

    retained_atoms.add(1, price_atom)
    retained_atoms.release()

    assert retained_atoms.atoms_by_key == {1: price_atom}
    assert retained_atoms.retained_bytes == 0


@pytest.mark.parametrize("with_budget", (False, True))
def test_forward_price_membership_releases_get_failure(with_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
        if with_budget
        else None
    )

    class FailingIndex:
        def get(self, _occurrence_key):
            raise RuntimeError("index lookup failed")

    with pytest.raises(RuntimeError, match="index lookup failed"):
        sidecars._append_forward_price_membership(
            FailingIndex(),
            budget,
            7,
            5,
            11,
            0,
        )

    if budget is not None:
        assert budget.retained_bytes == 0


@pytest.mark.parametrize("with_budget", (False, True))
def test_forward_price_membership_releases_append_failure(with_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
        if with_budget
        else None
    )

    class FailingPriceKeys(list):
        def append(self, _price_key):
            raise RuntimeError("price append failed")

    price_keys_by_occurrence = {(7, 5, 0): FailingPriceKeys()}
    with pytest.raises(RuntimeError, match="price append failed"):
        sidecars._append_forward_price_membership(
            price_keys_by_occurrence,
            budget,
            7,
            5,
            11,
            0,
        )

    if budget is not None:
        assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_empty_forward_price_index_releases_budgeted_code_scope():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    observed_index = await sidecars.lookup_forward_price_index_from_db(
        object(),
        (),
        retention_budget=budget,
        shared_snapshot_key=1,
        source_count=1,
        price_dictionary_item_count=1,
        price_dictionary_block_bytes=32,
    )

    assert observed_index == {}
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_unbudgeted_forward_price_freeze_failure_propagates(monkeypatch):
    monkeypatch.setattr(
        sidecars,
        "_mutable_forward_price_index",
        AsyncMock(return_value={(7, 5, 0): [11]}),
    )
    monkeypatch.setattr(
        sidecars,
        "_build_frozen_forward_price_index",
        Mock(side_effect=RuntimeError("freeze failed")),
    )

    with pytest.raises(RuntimeError, match="freeze failed"):
        await sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            shared_snapshot_key=1,
            source_count=1,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=32,
        )
