# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared slice facts retain complete accounting and pinned office selection."""

from unittest.mock import ANY, AsyncMock, MagicMock, call

import pytest

from process import network_membership_slices as slices
from process import network_typed_membership_accounting as accounting
from process import network_typed_slice_native as native
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation
from process.network_compact_offices import CompactOfficeSources


def _complete_counts():
    return {"membership_rows": 4, "matched_locations": 4, "invalid_memberships": 0, "invalid_location_keys": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("typed,affected", [(False, None), (True, '"affected_slices"')])
async def test_prepared_facts_refuse_untyped_or_partial_candidates(monkeypatch, typed, affected):
    monkeypatch.setattr(slices.registry_typed_membership_parity, "is_typed_candidate", lambda _candidate: typed)
    build = AsyncMock()
    monkeypatch.setattr(slices, "_matched_facts", build)
    with pytest.raises(slices.NetworkMembershipSliceError, match="complete typed candidate"):
        await slices._slice_facts(
            object(),
            {"accepted_rows": 4, "expected_rows": 4},
            ('"candidate"', '"addresses"', None),
            affected,
            None,
            ('"prepared_facts"', _complete_counts()),
        )
    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_prepared_facts_are_reused_without_rebuilding(monkeypatch):
    monkeypatch.setattr(slices.registry_typed_membership_parity, "is_typed_candidate", lambda _candidate: True)
    build = AsyncMock()
    monkeypatch.setattr(slices, "_matched_facts", build)
    counts = _complete_counts()
    result = await slices._slice_facts(
        object(),
        {"accepted_rows": 4, "expected_rows": 4},
        ('"candidate"', '"addresses"', None),
        None,
        None,
        ('"prepared_facts"', counts),
    )
    assert result == ('"prepared_facts"', counts, True) and result[1] is counts
    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_typed_facts_use_pinned_compact_office_arms(monkeypatch):
    monkeypatch.setattr(slices.registry_typed_membership_parity, "is_typed_candidate", lambda _candidate: True)
    counts = _complete_counts()
    build = AsyncMock(return_value=counts)
    monkeypatch.setattr(slices, "_matched_facts", build)
    connection, candidate = object(), {"accepted_rows": 4, "expected_rows": 4}
    offices = CompactOfficeSources(
        PinnedAddressSource("retained_offices", "addresses", "a" * 64),
        custom=CompactAddressRelation("candidate", "entity_address_unified"),
        physical_base=CompactAddressRelation("retained_offices", "address_heap"),
    )
    work, actual, typed = await slices._slice_facts(
        connection, candidate, ('"candidate"', '"addresses"', "unselected"), None, offices, None
    )
    assert actual is counts and typed is True and work.startswith('"network_slice_work_')
    build.assert_awaited_once_with(connection, '"candidate"', '"addresses"', work, candidate, None, ANY)
    office_sql = build.await_args.args[-1]
    assert 'FROM "retained_offices"."address_heap" a' in office_sql
    assert 'FROM "candidate"."entity_address_unified" a' in office_sql
    assert '"retained_offices"."addresses"' not in office_sql
    assert office_sql.count(" UNION ALL ") == 1


@pytest.mark.asyncio
async def test_orphan_binding_validation_requires_primary_domain_index(monkeypatch):
    index = AsyncMock(return_value=False)
    page = AsyncMock()
    monkeypatch.setattr(accounting, "_has_pair_key_index", index)
    monkeypatch.setattr(accounting, "_upper_bound", page)
    connection = object()
    with pytest.raises(ValueError, match="registry_typed_binding_domain_index_required"):
        await accounting._orphan_bindings(connection, '"bindings"', '"addresses"')
    index.assert_awaited_once_with(connection, '"bindings"', primary=True)
    page.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted_rows", [3, 4])
async def test_fresh_facts_must_pass_complete_accounting_before_return(monkeypatch, accepted_rows):
    counts = _complete_counts()
    build = AsyncMock(return_value=counts)
    monkeypatch.setattr(native, "build_matched_facts", build)
    connection = object()
    candidate_by_field = {"accepted_rows": accepted_rows, "expected_rows": accepted_rows}
    arguments = (connection, '"candidate"', '"addresses"', '"facts"', candidate_by_field, None)
    if accepted_rows == 3:
        with pytest.raises(slices.NetworkMembershipSliceError, match="accounting is incomplete"):
            await native.matched_facts(*arguments)
    else:
        assert await native.matched_facts(*arguments) is counts
    build.assert_awaited_once_with(connection, '"candidate"', '"addresses"', '"facts"', None)


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_native_ref_preparation_keeps_last_page_and_reuse_count(monkeypatch, empty):
    monkeypatch.setattr(native, "_METADATA_ROWS", 2)
    metadata_records = []
    metadata_values = []
    for network_id in (7, 8, 9):
        entry_by_field = {"network_id": network_id, "office_bucket": 2, "row_count": 3, "content_sha256": "a" * 64}
        metadata_record = MagicMock()
        metadata_record.__getitem__.side_effect = entry_by_field.__getitem__
        metadata_record.__iter__.return_value = iter(entry_by_field.values())
        metadata_records.append(metadata_record)
        metadata_values.append(tuple(entry_by_field.values()))
    pages = AsyncMock(side_effect=[[]] if empty else [metadata_records[:2], metadata_records[2:], []])
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(native, "_ref_metadata_page", pages)
    monkeypatch.setattr(slices, "_copy_ref_page", copy)
    connection, reference_target, previous = object(), object(), object()
    assert await native.prepare_refs(connection, reference_target, '"facts"', previous) == ((0, 0) if empty else (3, 2))
    pages.assert_any_await(connection, '"facts"', (0, -1))
    if empty:
        copy.assert_not_awaited()
    else:
        assert pages.await_args_list == [
            call(connection, '"facts"', (0, -1)),
            call(connection, '"facts"', (8, 2)),
            call(connection, '"facts"', (9, 2)),
        ]
        assert copy.await_args_list == [
            call(connection, reference_target, metadata_values[:2], previous),
            call(connection, reference_target, metadata_values[2:], previous),
        ]
