# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local input capture refuses expanded scope before touching publication state."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_registration as registration
from tests.test_cms_native_input_registration import NativeProtocol, _writes


async def _capture(native):
    return await registration.capture_native_input_candidate(
        native,
        ledger_schema="sample",
        target_schema="sample",
        target_table="mrf_address",
        stage_schema="stage",
        stage_table="incoming",
    )


@pytest.mark.asyncio
async def test_candidate_description_preserves_exact_heap_identity_without_enrolling_it():
    native = NativeProtocol()
    before = deepcopy((native.rows, native.guards, native.heap))

    candidate = await _capture(native)

    assert candidate == native.candidate()
    assert (native.rows, native.guards, native.heap) == before
    assert not _writes(native)
    assert len(native.calls) == 1
    assert native.calls[0][2] == ('"stage"."incoming"', '"sample"."mrf_address"')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("relpersistence", "u"),
        ("relpersistence", "t"),
        ("relforcerowsecurity", True),
        ("relispartition", True),
    ],
)
async def test_capture_cannot_describe_transient_or_policy_bound_heaps_as_publishable(field, value):
    native = NativeProtocol()
    native.heap[field] = value
    before = deepcopy((native.rows, native.guards))
    with pytest.raises(RuntimeError, match="cms_native_input_candidate_heap_changed"):
        await _capture(native)
    assert (native.rows, native.guards) == before
    assert not _writes(native)


@pytest.mark.asyncio
async def test_capture_requires_the_original_transaction_before_reading_catalogs():
    native = NativeProtocol()
    native.active = False
    with pytest.raises(RuntimeError, match="cms_native_input_original_transaction_required"):
        await _capture(native)
    assert not native.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [("foreign", "mrf_address"), ("sample", "npi"), ("sample", "unrelated_table")])
async def test_fixed_registration_scope_cannot_expand_to_other_owners_or_specialized_inputs(target):
    native = NativeProtocol()
    with pytest.raises(RuntimeError, match="cms_native_input_candidate_scope_changed"):
        await registration.capture_native_input_candidate(
            native,
            ledger_schema="sample",
            target_schema=target[0],
            target_table=target[1],
            stage_schema="stage",
            stage_table="incoming",
        )
    assert not native.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relations",
    [
        [],
        (),
        (["mrf_address", "incoming"],),
        (("mrf_address",),),
        (("mrf_address", "mrf_address"),),
        (("mrf_address", "first"), ("mrf_address", "second")),
        (("mrf_address", None),),
    ],
)
async def test_local_publication_refuses_ambiguous_replacement_requests_before_session_io(relations):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), connection=AsyncMock())
    with pytest.raises(RuntimeError, match="cms_native_input_local_publication_scope_changed"):
        await registration.prepare_local_native_input_publication(session, ledger_schema="sample", relations=relations)
    session.execute.assert_not_awaited()
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_local_replacement_still_requires_a_caller_owned_transaction():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock(), connection=AsyncMock())
    with pytest.raises(RuntimeError, match="cms_native_input_original_transaction_required"):
        await registration.prepare_local_native_input_publication(
            session, ledger_schema="sample", relations=(("mrf_address", "incoming"),)
        )
    session.execute.assert_not_awaited()
    session.connection.assert_not_awaited()
