# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request-local reuse contracts for validated release descriptors."""

import asyncio
from types import SimpleNamespace

from api import plan_release_serving, ptg2_serving

from .test_plan_release_serving import (
    PLAN_RELEASE_ID,
    _Session,
    _binding_row,
    _install_single_snapshot_search,
    _network_binding,
    _release_selection,
)
from .test_plan_release_serving_readiness import _serving_table_descriptor


def test_release_resolver_retains_validated_serving_descriptor(monkeypatch):
    """Carry the exact readiness descriptor into the immutable release cut."""

    descriptor = _serving_table_descriptor()

    async def is_serving_ready(
        _session,
        binding,
        *,
        validated_serving_tables_by_snapshot_id,
    ):
        validated_serving_tables_by_snapshot_id[
            binding.snapshot_id
        ] = descriptor
        return True

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_serving_ready,
    )
    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            _Session([_binding_row()]),
            PLAN_RELEASE_ID,
        )
    )

    assert selection is not None
    assert selection.serving_tables_for_snapshot("ptg2:release-old") is descriptor


def test_release_resolver_rejects_missing_in_network_descriptor(monkeypatch):
    """Reject a release cut whose ready binding omitted its descriptor proof."""

    async def is_ready_without_descriptor(
        _session,
        _binding_value,
        **_readiness_context,
    ):
        return True

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_ready_without_descriptor,
    )

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            _Session([_binding_row()]),
            PLAN_RELEASE_ID,
        )
    )

    assert selection is None


def test_single_release_query_reuses_validated_descriptor(monkeypatch):
    """Pass the readiness descriptor directly into the physical search."""

    descriptor = object()
    binding = _network_binding(0, "ptg2:release-old", "source-network-a")
    selection = _release_selection(
        binding,
        validated_serving_tables=((binding.snapshot_id, descriptor),),
    )
    serving_table_calls = []
    _install_single_snapshot_search(
        monkeypatch,
        selection,
        [],
        serving_table_calls,
    )

    asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "00001"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
        )
    )

    assert serving_table_calls == [descriptor]


def test_single_release_query_rejects_missing_descriptor_before_search(
    monkeypatch,
):
    """Do not reopen descriptor loading for an incomplete release selection."""

    binding = _network_binding(0, "ptg2:release-old", "source-network-a")
    selection = _release_selection(binding, validated_serving_tables=())

    async def fail_physical_search(*_args, **_kwargs):
        raise AssertionError("missing descriptor must fail before searching")

    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        fail_physical_search,
    )

    response = asyncio.run(
        ptg2_serving._search_plan_release_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert response is None


def test_single_release_query_rejects_descriptor_for_another_snapshot(
    monkeypatch,
):
    """Refuse a map that omits the selected binding before physical search."""

    binding = _network_binding(0, "snapshot-a", "source-a")
    selection = SimpleNamespace(
        in_network_bindings=(binding,),
        network_tables_by_snapshot=lambda: {"snapshot-b": object()},
    )

    async def fail_physical_search(*_args, **_kwargs):
        raise AssertionError("mismatched descriptor must stop before search")

    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        fail_physical_search,
    )

    response = asyncio.run(
        ptg2_serving._search_plan_release_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert response is None


def _install_multi_descriptor_spies(monkeypatch, search_calls):
    async def fail_descriptor_reload(*_args, **_kwargs):
        raise AssertionError("validated descriptors must satisfy this request")

    async def search_snapshot(
        _session,
        snapshot_id,
        args,
        _pagination,
        *,
        serving_tables,
    ):
        search_calls.append(
            (
                _session,
                snapshot_id,
                args["source_key"],
                args["plan_id"],
                serving_tables,
            )
        )
        return None

    monkeypatch.setattr(
        ptg2_serving,
        "_network_tables_by_snapshot_id",
        fail_descriptor_reload,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        search_snapshot,
    )


def test_multi_release_query_reuses_every_validated_descriptor(monkeypatch):
    """Avoid descriptor reloads when every frozen network was validated."""

    bindings = (
        _network_binding(
            0,
            "ptg2:release-network-a",
            "source-network-a",
            plan_id="synthetic-plan-a",
        ),
        _network_binding(
            1,
            "ptg2:release-network-b",
            "source-network-b",
            plan_id="synthetic-plan-b",
        ),
    )
    descriptors = (object(), object())
    selection = _release_selection(
        *bindings,
        validated_serving_tables=tuple(
            (binding.snapshot_id, descriptor)
            for binding, descriptor in zip(bindings, descriptors, strict=True)
        ),
    )
    search_calls = []
    _install_multi_descriptor_spies(monkeypatch, search_calls)
    request_session = object()

    responses = asyncio.run(
        ptg2_serving._read_multi_ptg2_snapshots(
            request_session,
            [(binding.source_key, binding.snapshot_id) for binding in bindings],
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert search_calls == [
        (
            request_session,
            binding.snapshot_id,
            binding.source_key,
            binding.plan_id,
            descriptor,
        )
        for binding, descriptor in zip(bindings, descriptors, strict=True)
    ]
    assert responses == [
        (binding.source_key, binding.snapshot_id, None)
        for binding in bindings
    ]


def _mixed_representation_release():
    bindings = (
        _network_binding(0, "snapshot-direct", "source-direct"),
        _network_binding(1, "snapshot-pattern", "source-pattern"),
    )
    descriptor_by_snapshot_id = {
        bindings[0].snapshot_id: SimpleNamespace(representation="direct_v1"),
        bindings[1].snapshot_id: SimpleNamespace(representation="pattern_v1"),
    }
    selection = _release_selection(
        *bindings,
        validated_serving_tables=tuple(descriptor_by_snapshot_id.items()),
    )
    return bindings, selection


def _install_mixed_representation_search(monkeypatch):
    """Install exact direct and pattern payloads for ordered reuse."""

    async def search_snapshot(
        _session,
        snapshot_id,
        _args,
        _pagination,
        *,
        serving_tables,
    ):
        return {
            "items": [
                {
                    "provider_npi": 1234567890,
                    "prices": [{"negotiated_rate": "125.00"}],
                    "source_trace": [{"snapshot_id": snapshot_id}],
                }
            ],
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        search_snapshot,
    )


def test_multi_release_query_preserves_direct_and_pattern_payloads(monkeypatch):
    """Keep exact V4 prices and provenance stable in binding order."""

    bindings, selection = _mixed_representation_release()
    _install_mixed_representation_search(monkeypatch)
    responses = asyncio.run(
        ptg2_serving._read_multi_ptg2_snapshots(
            object(),
            [(binding.source_key, binding.snapshot_id) for binding in bindings],
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert [
        (source_key, snapshot_id)
        for source_key, snapshot_id, _payload in responses
    ] == [
        (binding.source_key, binding.snapshot_id)
        for binding in bindings
    ]
    assert [
        network_response["items"][0]["prices"]
        for _, _, network_response in responses
    ] == [[{"negotiated_rate": "125.00"}]] * 2
    assert [
        network_response["items"][0]["source_trace"]
        for _, _, network_response in responses
    ] == [
        [{"snapshot_id": binding.snapshot_id}]
        for binding in bindings
    ]


def test_multi_release_query_rejects_partial_descriptors_before_io(
    monkeypatch,
):
    """Reject a partial multi-network proof without loading or searching."""

    bindings = (
        _network_binding(0, "ptg2:release-network-a", "source-network-a"),
        _network_binding(1, "ptg2:release-network-b", "source-network-b"),
    )
    selection = _release_selection(
        *bindings,
        validated_serving_tables=((bindings[0].snapshot_id, object()),),
    )

    async def fail_network_io(*_args, **_kwargs):
        raise AssertionError("partial descriptors must fail before I/O")

    monkeypatch.setattr(
        ptg2_serving,
        "_network_tables_by_snapshot_id",
        fail_network_io,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        fail_network_io,
    )

    responses = asyncio.run(
        ptg2_serving._read_multi_ptg2_snapshots(
            object(),
            [(binding.source_key, binding.snapshot_id) for binding in bindings],
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert responses is None


def test_multi_release_query_rejects_network_mismatch_before_io(monkeypatch):
    """Reject a release/network mismatch without loading or searching."""

    binding = _network_binding(0, "snapshot-a", "source-a")
    selection = _release_selection(binding)

    async def fail_network_io(*_args, **_kwargs):
        raise AssertionError("network mismatch must fail before I/O")

    monkeypatch.setattr(
        ptg2_serving,
        "_network_tables_by_snapshot_id",
        fail_network_io,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        fail_network_io,
    )

    responses = asyncio.run(
        ptg2_serving._read_multi_ptg2_snapshots(
            object(),
            [("source-a", "snapshot-a"), ("source-b", "snapshot-b")],
            {"plan_release_id": PLAN_RELEASE_ID},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            selection,
        )
    )

    assert responses is None
