from types import SimpleNamespace

import pytest

from api import ptg2_db_sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)


def test_forward_capture_preserves_first_physical_occurrence():
    capture = ptg2_db_sidecars._ForwardFanoutCapture(
        exact_views_by_occurrence={},
        fallback_views=(),
        provider_filter_set=None,
        retained_by_coordinate={},
    )

    capture.capture_first(5, 7, 0)
    capture.capture_first(6, 8, 1)

    assert capture.first_provider_set_key == 5
    assert capture.first_occurrence == (7, 0)


@pytest.mark.asyncio
async def test_forward_query_iterator_accepts_synchronous_rows():
    observed_rows = [
        row
        async for row in ptg2_db_sidecars._iterate_forward_query_rows(
            ({"code_key": 7}, {"code_key": 8})
        )
    ]

    assert observed_rows == [{"code_key": 7}, {"code_key": 8}]


def test_forward_price_array_rejects_out_of_range_key():
    with pytest.raises(Exception, match="price key is out of range"):
        ptg2_db_sidecars._validated_forward_price_key_array(
            b"\x01",
            0,
            price_key_count=1,
            price_item_count=1,
        )


def test_forward_source_filter_rejects_negative_key():
    with pytest.raises(Exception, match="invalid source key"):
        ptg2_db_sidecars._normalized_source_filter((-1,), None)


@pytest.mark.asyncio
async def test_forward_prefix_rejects_boolean_limit_before_io():
    with pytest.raises(ValueError, match="limit must be positive"):
        await ptg2_db_sidecars.lookup_code_prefix_rows_from_db(
            object(),
            7,
            limit=False,
        )


def test_forward_filter_workspace_requires_sized_input():
    with pytest.raises(Exception, match="must expose a bounded size"):
        ptg2_db_sidecars._budgeted_forward_input_length(
            (key for key in (5, 6)),
            category="provider filter",
        )


def test_forward_provider_filter_copies_cover_shared_scope():
    options = SimpleNamespace(
        provider_set_keys_by_code=None,
        provider_set_keys=(5, 6),
    )

    assert (
        ptg2_db_sidecars._forward_provider_filter_copy_count(options, 2)
        == 6
    )
    assert ptg2_db_sidecars._normalized_batch_provider_filters(
        options,
        (7, 8),
    ) == {7: (5, 6), 8: (5, 6)}


def test_forward_occurrence_scope_requires_provider_match():
    with pytest.raises(Exception, match="must equal its provider scope"):
        ptg2_db_sidecars._validate_forward_occurrence_scope(
            None,
            None,
            frozenset(((5, 0),)),
        )


def test_forward_fragment_identity_requires_physical_hash():
    with pytest.raises(Exception, match="missing its physical block identity"):
        ptg2_db_sidecars._forward_fragment_physical_identity(
            SimpleNamespace(fragment_row={})
        )


def test_forward_coordinate_workspace_deduplicates_views():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    view = SimpleNamespace(code_key=7, block_key=8, fragment_no=0)

    retained, retained_bytes = (
        ptg2_db_sidecars._retained_forward_rows_by_coordinate(
            (view, view),
            budget,
        )
    )

    assert retained == {(7, 8, 0): []}
    assert budget.retained_bytes == retained_bytes
    budget.release(retained_bytes)


@pytest.mark.asyncio
async def test_forward_occurrence_lookup_skips_io_for_empty_codes():
    observed_rows = (
        await ptg2_db_sidecars.lookup_forward_occurrences_batch_from_db(
            object(),
            (),
            shared_snapshot_key=1,
            source_count=1,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=1,
        )
    )

    assert observed_rows == {}


def test_forward_price_membership_deduplicates_under_budget():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    price_keys_by_occurrence = {}

    ptg2_db_sidecars._append_forward_price_membership(
        price_keys_by_occurrence,
        budget,
        7,
        5,
        11,
        0,
    )
    retained_bytes = budget.retained_bytes
    ptg2_db_sidecars._append_forward_price_membership(
        price_keys_by_occurrence,
        budget,
        7,
        5,
        11,
        0,
    )

    assert price_keys_by_occurrence == {(7, 5, 0): [11]}
    assert budget.retained_bytes == retained_bytes
    budget.release(retained_bytes)


@pytest.mark.parametrize("with_budget", (False, True))
def test_forward_price_index_freeze_releases_peak_claim(with_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
        if with_budget
        else None
    )

    frozen = ptg2_db_sidecars._freeze_forward_price_index(
        {(7, 5, 0): [11]},
        budget,
    )

    assert frozen == {(7, 5, 0): (11,)}
    if budget is not None:
        assert budget.retained_bytes == 0


def test_provider_code_request_rejects_distinct_code_overflow():
    with pytest.raises(Exception, match="requested key scope"):
        ptg2_db_sidecars._normalize_provider_code_requests(
            {5: (7, 8)},
            max_memberships=None,
            max_distinct_code_keys=1,
        )


def test_provider_code_request_reuses_provider_bucket():
    requests = ptg2_db_sidecars._normalize_provider_code_requests(
        {5: (7, 8)},
        max_memberships=None,
    )

    assert requests.provider_set_keys == (5,)
    assert requests.code_keys_by_provider_set == {5: (7, 8)}
