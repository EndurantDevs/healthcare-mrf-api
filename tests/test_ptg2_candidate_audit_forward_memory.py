from __future__ import annotations

import gc
import sys
import tracemalloc
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_db_serving_v3
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)


class _AuditAbort(BaseException):
    pass


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


def _fanout_capture(retained_rows_by_coordinate, budget):
    return sidecars._ForwardFanoutCapture(
        exact_views_by_occurrence={},
        fallback_views=(_fragment_view(),),
        provider_filter_set=None,
        retained_by_coordinate=retained_rows_by_coordinate,
        retention_budget=budget,
    )


def test_forward_fanout_claims_budget_before_retaining_row():
    coordinate = (7, 0, 0)
    retained_rows_by_coordinate = {coordinate: []}
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES - 1
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="forward fanout row",
    ):
        _fanout_capture(retained_rows_by_coordinate, budget)(5, 10, 0)

    assert retained_rows_by_coordinate == {coordinate: []}
    assert budget.retained_bytes == 0


def test_forward_fanout_exact_deep_budget_retains_one_row():
    coordinate = (7, 0, 0)
    retained_rows_by_coordinate = {coordinate: []}
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES
    )

    _fanout_capture(retained_rows_by_coordinate, budget)(
        1_000_000_001,
        1_000_000_002,
        1_000_000_003,
    )

    assert retained_rows_by_coordinate[coordinate] == [
        (1_000_000_001, 1_000_000_002, 1_000_000_003)
    ]
    assert budget.retained_bytes == sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES


@pytest.mark.parametrize("use_budget", (False, True))
def test_forward_fanout_releases_claim_when_coordinate_insert_fails(use_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=4096) if use_budget else None
    )
    capture = sidecars._ForwardFanoutCapture(
        exact_views_by_occurrence={},
        fallback_views=(_fragment_view(),),
        provider_filter_set=None,
        retained_by_coordinate={},
        retention_budget=budget,
    )

    with pytest.raises(KeyError):
        capture(5, 10, 0)

    if budget is not None:
        assert budget.retained_bytes == 0


def test_forward_coordinate_map_exact_and_one_under_budget():
    required_bytes = (
        sidecars._FORWARD_COORDINATE_MAP_RETAINED_BYTES
        + sidecars._FORWARD_COORDINATE_RETAINED_BYTES
    )
    exact_budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=required_bytes)

    rows_by_coordinate, retained_bytes = sidecars._retained_forward_rows_by_coordinate(
        (_fragment_view(),), exact_budget
    )

    assert rows_by_coordinate == {(7, 0, 0): []}
    assert retained_bytes == required_bytes
    assert exact_budget.retained_bytes == required_bytes
    under_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes - 1
    )
    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="coordinate bucket",
    ):
        sidecars._retained_forward_rows_by_coordinate((_fragment_view(),), under_budget)
    assert under_budget.retained_bytes == 0


def test_forward_coordinate_map_releases_duplicate_bucket_claim():
    required_peak_bytes = (
        sidecars._FORWARD_COORDINATE_MAP_RETAINED_BYTES
        + 2 * sidecars._FORWARD_COORDINATE_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=required_peak_bytes)

    rows_by_coordinate, retained_bytes = sidecars._retained_forward_rows_by_coordinate(
        (_fragment_view(), _fragment_view()),
        budget,
    )

    expected_retained_bytes = (
        sidecars._FORWARD_COORDINATE_MAP_RETAINED_BYTES
        + sidecars._FORWARD_COORDINATE_RETAINED_BYTES
    )
    assert rows_by_coordinate == {(7, 0, 0): []}
    assert retained_bytes == expected_retained_bytes
    assert budget.retained_bytes == expected_retained_bytes


def test_forward_physical_parse_base_exception_releases_rows_and_coordinates(
    monkeypatch,
):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=16 * 1024)

    def abort_parse(
        _physical_views,
        _options,
        _price_item_count,
        retained_by_coordinate,
        retention_budget,
    ):
        retention_budget.claim(
            sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES,
            category="a decoded forward fanout row",
        )
        retained_by_coordinate[(7, 0, 0)].append((5, 10, 0))
        raise _AuditAbort("stop")

    monkeypatch.setattr(
        sidecars,
        "_parse_physical_forward_fragment_once",
        abort_parse,
    )

    with pytest.raises(_AuditAbort, match="stop"):
        sidecars._parse_forward_batch_physical_fragments_once(
            (_fragment_view(),),
            options=sidecars._ForwardBatchOptions(1, 1, 1, 32),
            price_item_count=1,
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


def test_forward_result_freeze_releases_partial_peak_on_overflow():
    occurrence_bytes = (
        sidecars._FORWARD_RESULT_OCCURRENCE_RETAINED_BYTES
        + sidecars._FORWARD_RESULT_PRICE_KEY_RETAINED_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(sidecars._FORWARD_RESULT_MAP_RETAINED_BYTES + occurrence_bytes)
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="frozen forward occurrence",
    ):
        sidecars._freeze_forward_price_index(
            {(7, 5, 0): [8], (7, 6, 0): [9]},
            budget,
        )

    assert budget.retained_bytes == 0


def test_forward_group_rejects_one_block_assigned_to_two_codes(monkeypatch):
    monkeypatch.setattr(
        sidecars,
        "_forward_provider_range_for_block",
        Mock(return_value=(0, 1)),
    )

    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="assigned to multiple codes",
    ):
        sidecars._group_forward_fragments_by_code((), {7: (1,), 8: (1,)})


def test_price_atom_retention_duplicate_and_release_are_exact():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    retained_atoms = sidecars._RetainedPriceAtoms(budget, {})
    price_atom = object()
    retained_atoms.add(1, price_atom)

    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="duplicate key",
    ):
        retained_atoms.add(1, price_atom)

    retained_atoms.release()
    assert retained_atoms.atoms_by_key == {1: price_atom}
    assert retained_atoms.retained_bytes == 0
    assert budget.retained_bytes == 0


def test_price_atom_alias_base_exception_releases_retained_atoms(monkeypatch):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    logical_block = sidecars._SharedLogicalBlock(
        payload=b"x",
        entry_count=1,
        physical_hashes=(b"h" * 32,),
    )

    def abort_alias(retained_atoms, *_args):
        retained_atoms.add(0, object())
        raise _AuditAbort("stop")

    monkeypatch.setattr(
        sidecars,
        "_retain_price_atom_alias_group",
        abort_alias,
    )

    with pytest.raises(_AuditAbort, match="stop"):
        sidecars._price_atoms_from_aliases(
            {0: logical_block},
            requested_key_set={0},
            block_span=1,
            schema_name="mrf",
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


def test_provider_code_duplicate_guard_precedes_retention():
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="duplicate provider key",
    ):
        sidecars._claim_decoded_provider_code_bucket(
            {5: (7,)},
            5,
            (8,),
            CandidateAuditDecodedRetentionBudget(maximum_bytes=4096),
        )


def test_provider_code_alias_base_exception_releases_decoded_map(monkeypatch):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    logical_block = sidecars._SharedLogicalBlock(
        payload=b"x",
        entry_count=1,
        physical_hashes=(b"h" * 32,),
    )

    def abort_alias(code_keys_by_provider, _physical_aliases, context):
        retained_bytes = (
            sidecars._PROVIDER_CODE_DECODED_BUCKET_BYTES
            + sidecars._PROVIDER_CODE_DECODED_MEMBERSHIP_BYTES
        )
        context.decoded_retention_budget.claim(
            retained_bytes,
            category="a decoded provider/code bucket",
        )
        code_keys_by_provider[0] = (7,)
        raise _AuditAbort("stop")

    monkeypatch.setattr(
        sidecars,
        "_retain_provider_code_alias_group",
        abort_alias,
    )

    with pytest.raises(_AuditAbort, match="stop"):
        sidecars._provider_code_keys_from_aliases(
            {0: logical_block},
            requested_key_set={0},
            decoded_retention_budget=budget,
            schema_name="mrf",
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_price_membership_duplicate_releases_first_claim(monkeypatch):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    first_block = sidecars._SharedLogicalBlock(
        payload=b"first",
        entry_count=1,
        physical_hashes=(b"a" * 32,),
    )
    second_block = sidecars._SharedLogicalBlock(
        payload=b"second",
        entry_count=1,
        physical_hashes=(b"b" * 32,),
    )
    monkeypatch.setattr(
        sidecars,
        "_shared_logical_blocks_by_key",
        AsyncMock(return_value={0: first_block, 1: second_block}),
    )
    monkeypatch.setattr(
        sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        ptg2_db_serving_v3,
        "_decode_price_membership_block",
        Mock(return_value={0: (2,)}),
    )

    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="duplicate key",
    ):
        await sidecars.lookup_price_atom_memberships_from_db(
            object(),
            1,
            (0, 1),
            block_span=1,
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


def test_forward_deep_accounting_covers_cpython_314_objects():
    row_values = tuple(int(str(1_000_000_001 + offset)) for offset in range(3))
    coordinate_values = tuple(int(str(2_000_000_001 + offset)) for offset in range(3))
    row_list = []
    empty_row_list_bytes = sys.getsizeof(row_list)
    row_list.append(row_values)
    observed_row_peak = (
        sys.getsizeof(row_values)
        + sum(sys.getsizeof(integer_value) for integer_value in row_values)
        + sys.getsizeof(row_list)
        - empty_row_list_bytes
        + sys.getsizeof(coordinate_values)
    )
    observed_coordinate_map = (
        sys.getsizeof({coordinate_values: []})
        + sys.getsizeof(coordinate_values)
        + sys.getsizeof([])
    )

    assert observed_row_peak <= sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES
    assert observed_coordinate_map <= (
        sidecars._FORWARD_COORDINATE_MAP_RETAINED_BYTES
        + sidecars._FORWARD_COORDINATE_RETAINED_BYTES
    )


def test_forward_row_claim_exceeds_tracemalloc_retained_bytes():
    row_count = 10_000
    gc.collect()
    tracemalloc.start()
    try:
        before_bytes, _before_peak = tracemalloc.get_traced_memory()
        retained_rows = [
            (
                int(str(1_000_000_000 + row_index * 3)),
                int(str(1_000_000_001 + row_index * 3)),
                int(str(1_000_000_002 + row_index * 3)),
            )
            for row_index in range(row_count)
        ]
        observed_bytes, _observed_peak = tracemalloc.get_traced_memory()
        assert retained_rows[-1][0] > 256
        assert observed_bytes - before_bytes <= (
            sidecars._FORWARD_COORDINATE_MAP_RETAINED_BYTES
            + row_count * sidecars._FORWARD_FANOUT_ROW_RETAINED_BYTES
        )
    finally:
        tracemalloc.stop()
