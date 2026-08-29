# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused trust-boundary coverage for factorized pricing projections."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_aggregate_pack as packs
from api import plan_pricing_projection_build as projection_build
from api import plan_pricing_projection_source as projection_source
from api import plan_pricing_projection_v3 as projection_v3
from api import plan_pricing_projection_v3_aggregate as projection_aggregate
from api import plan_pricing_projection_v3_code as code_stage
from api import plan_pricing_projection_v3_price as price_stage
from api import plan_pricing_projection_v3_provider as provider_stage
from api import plan_pricing_projection_v3_provider_cells as provider_cells
from api import plan_pricing_projection_v3_receipts as projection_receipts
from api import plan_pricing_projection_v3_types as projection_types
from api import plan_pricing_projection_v3_work as projection_work
from api import ptg2_serving as serving
from api.plan_pricing_projection_source import BindingProjection

PROJECTION_ID = "a" * 64
PROVIDER_SET_ID = "1" * 32


def _record(zip5: str = "10001") -> packs.AggregateZipRecord:
    return packs.AggregateZipRecord(
        zip5, 1, 1, Decimal("1"), Decimal("1"), Decimal("1")
    )


def _binding(
    ordinal: object = 0,
    *,
    code_rows_by_identity: dict[tuple[str, str], list[dict[str, object]]] | None = None,
) -> BindingProjection:
    return BindingProjection(
        {"ordinal": ordinal},
        SimpleNamespace(
            network_names=(),
            price_key_block_span=512,
            shared_snapshot_key=1,
            uses_shared_blocks=True,
        ),
        code_rows_by_identity if code_rows_by_identity is not None else {},
        1,
    )


def _build_state() -> projection_types._BuildState:
    return projection_types._BuildState(hashlib.sha256())


def test_provider_identities_fail_closed_and_ignore_unrequested_rows() -> None:
    """Validate ordinals and provider-set identities before staging."""

    for ordinal in (True, -1):
        with pytest.raises(ValueError, match="ordinal is invalid"):
            provider_stage._binding_ordinal(_binding(ordinal))
    with pytest.raises(ValueError, match="identity is invalid"):
        provider_stage._provider_set_ids_by_key(({"_ptg_provider_set_key": True},), {7})
    assert provider_stage._provider_set_ids_by_key(
        (
            {
                "_ptg_provider_set_key": 99,
                "provider_set_global_id_128": "9" * 32,
            },
            {
                "_ptg_provider_set_key": 7,
                "provider_set_global_id_128": PROVIDER_SET_ID,
            },
        ),
        {7},
    ) == {7: PROVIDER_SET_ID}
    with pytest.raises(ValueError, match="identity is invalid"):
        provider_stage._provider_set_ids_by_key(
            ({"_ptg_provider_set_key": 7, "provider_set_global_id_128": None},),
            {7},
        )


def test_provider_set_identity_rejects_aliases_and_missing_keys() -> None:
    """Reject inconsistent or incomplete provider-set manifests."""

    with pytest.raises(ValueError, match="identity is inconsistent"):
        provider_stage._provider_set_ids_by_key(
            (
                {
                    "_ptg_provider_set_key": 7,
                    "provider_set_global_id_128": PROVIDER_SET_ID,
                },
                {
                    "_ptg_provider_set_key": 7,
                    "provider_set_global_id_128": "2" * 32,
                },
            ),
            {7},
        )
    with pytest.raises(ValueError, match="identity is incomplete"):
        provider_stage._provider_set_ids_by_key(
            (
                {
                    "_ptg_provider_set_key": 7,
                    "provider_set_global_id_128": PROVIDER_SET_ID,
                },
            ),
            {7, 8},
        )


@pytest.mark.asyncio
async def test_provider_staging_noops_and_rejects_replay_identity_drift() -> None:
    """Avoid empty reads and reject a changed staged identity."""

    no_execute = AsyncMock(side_effect=AssertionError("unexpected execute"))
    session = SimpleNamespace(execute=no_execute)
    assert await provider_stage._existing_provider_set_ids(session, 0, ()) == {}
    await provider_stage._stage_code_provider_sets(
        session, _binding(), (), set(), _build_state()
    )
    no_execute.assert_not_awaited()

    rows = SimpleNamespace(
        mappings=lambda: iter(({"provider_set_key": 7, "provider_set_id": "2" * 32},))
    )
    session.execute = AsyncMock(return_value=rows)
    with pytest.raises(ValueError, match="identity is inconsistent"):
        await provider_stage._stage_code_provider_sets(
            session,
            _binding(),
            (
                {
                    "_ptg_provider_set_key": 7,
                    "provider_set_global_id_128": PROVIDER_SET_ID,
                },
            ),
            {7},
            _build_state(),
        )


def test_provider_cells_reject_rows_outside_the_requested_batch() -> None:
    """Reject provider hydration that returns an unrequested NPI."""

    with pytest.raises(ValueError, match="provider-cell bound exceeded"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _build_state(),
            [1],
            {2: ()},
        )


@pytest.mark.asyncio
async def test_code_identity_and_empty_binding_paths_fail_closed(monkeypatch) -> None:
    """Reject malformed rate identities and skip absent code bindings."""

    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda _rows: 1)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(
            return_value=[
                {
                    "price_set_global_id_128": "1" * 32,
                    "price_key": True,
                    "_ptg_provider_set_key": 7,
                }
            ]
        ),
    )
    with pytest.raises(ValueError, match="rate identity is incomplete"):
        await code_stage._binding_code_rows(object(), _binding(), [{}])

    invalid_serving = SimpleNamespace(_ptg2_manifest_id=lambda _value: None)
    with pytest.raises(ValueError, match="rate identity is incomplete"):
        code_stage._code_occurrences(
            invalid_serving,
            ({"_ptg_provider_set_key": 7, "price_set_global_id_128": None},),
        )

    empty_binding = _binding(code_rows_by_identity={})
    session = SimpleNamespace(execute=AsyncMock())
    assert not await code_stage._has_staged_code_inputs(
        session,
        _build_state(),
        ("CPT", "27447"),
        [empty_binding],
    )


@pytest.mark.asyncio
async def test_code_staging_rejects_missing_prices_and_filters_empty_rates(
    monkeypatch,
) -> None:
    """Reject missing price identities and avoid empty provider staging."""

    binding = _binding(code_rows_by_identity={("CPT", "27447"): [{"code_key": 1}]})
    serving_by_field = {
        "_ptg_provider_set_key": 7,
        "price_set_global_id_128": "1" * 32,
    }
    with pytest.raises(ValueError, match="price hydration is incomplete"):
        await code_stage._bounded_binding_code_input(
            object(),
            binding,
            ("CPT", "27447"),
            AsyncMock(return_value=([serving_by_field], {})),
        )

    monkeypatch.setattr(
        code_stage,
        "_preflight_binding_price_memberships",
        AsyncMock(return_value=512),
    )
    monkeypatch.setattr(
        code_stage,
        "_stage_binding_price_rates",
        AsyncMock(return_value=(set(), 0)),
    )
    stage_provider_sets = AsyncMock()
    assert await code_stage._stage_bounded_binding_input(
        object(),
        _build_state(),
        (
            binding,
            [serving_by_field],
            Counter({(7, "1" * 32): 1}),
            {"1" * 32: 1},
        ),
        10,
        stage_provider_sets,
        AsyncMock(),
    ) == (False, 0)
    stage_provider_sets.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_hydration_rejects_missing_and_invalid_batches(monkeypatch) -> None:
    """Reject incomplete hydration and invalid cumulative atom limits."""

    binding = _binding()
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={}),
    )
    with pytest.raises(ValueError, match="price hydration is incomplete"):
        await price_stage._price_batch_rates(object(), binding, (("1" * 32, 1),))
    with pytest.raises(ValueError, match="price-atom bound is invalid"):
        await price_stage._stage_binding_price_rates(
            object(), binding, {}, maximum_atom_count=-1
        )

    monkeypatch.setattr(price_stage, "_price_batch_rates", AsyncMock(return_value={}))
    insert_rates = AsyncMock()
    assert await price_stage._stage_binding_price_rates(
        object(),
        binding,
        {"1" * 32: 1},
        insert_price_rates=insert_rates,
    ) == (set(), 0)
    insert_rates.assert_not_awaited()


@pytest.mark.asyncio
async def test_build_state_helpers_cover_exact_batch_and_heap_bounds(
    monkeypatch,
) -> None:
    """Exercise exact batch completion and bounded prewarm rejection."""

    with pytest.raises(ValueError, match="counts are invalid"):
        projection_types.ProjectionV3Counts(-1, 0, 0, 0, 0, 0, 0, 0)
    item = projection_types._PrewarmHeapItem(
        projection_types._PrewarmShape("CPT", "1", "10001", 1)
    )
    assert item.__lt__(object()) is NotImplemented

    monkeypatch.setattr(projection_types, "INSERT_BATCH_SIZE", 2)
    session = SimpleNamespace(execute=AsyncMock())
    await projection_types._insert_batches(
        session, "SELECT 1", ({"value": 1}, {"value": 2})
    )
    session.execute.assert_awaited_once()

    monkeypatch.setattr(projection_types, "MAX_PREWARM_SHAPES", 1)
    heap_items: list[projection_types._PrewarmHeapItem] = []
    projection_types._retain_prewarm_shape(heap_items, ("CPT", "1"), _record())
    projection_types._retain_prewarm_shape(
        heap_items,
        ("CPT", "2"),
        SimpleNamespace(zip5="10002", provider_count=0),
    )
    assert heap_items[0].shape.provider_count == 1


@pytest.mark.asyncio
async def test_work_metrics_and_diagnostic_markers_fail_closed() -> None:
    """Reject negative work and malformed diagnostic markers."""

    raw_work_by_field = {
        "set_cell_rows": -1,
        "profile_join_rows": 0,
        "aggregate_join_rows": 0,
        "profile_rate_count_sum": 0,
        "profile_rate_count_max": 0,
        "profile_distinct_rate_count_max": 0,
        "aggregate_rate_count_sum": 0,
        "aggregate_rate_count_max": 0,
    }
    with pytest.raises(ValueError, match="code work is invalid"):
        projection_work._code_work_from_row(0, 0, raw_work_by_field)

    async def diagnostic_stage(stage: str) -> str:
        return "valid:marker" if stage == "valid" else "not valid"

    assert "valid:marker" in str(
        await projection_work._diagnostic_statement(
            diagnostic_stage, "valid", "SELECT 1"
        )
    )
    with pytest.raises(ValueError, match="marker is invalid"):
        await projection_work._diagnostic_statement(
            diagnostic_stage, "invalid", "SELECT 1"
        )


def test_receipt_contract_and_source_query_bounds_fail_closed() -> None:
    """Preserve legacy receipts while rejecting unknown contracts and bad bounds."""

    candidate_by_field = {
        "contract_version": projection_build.LEGACY_PROJECTION_CONTRACT,
        "projection_id": PROJECTION_ID,
        "binding_manifest_digest": "b" * 64,
        "provider_signature": "c" * 64,
        "content_digest": "d" * 64,
        "build_seconds": 1,
        "card_row_count": 1,
        "aggregate_row_count": 1,
        "fragment_byte_count": 1,
    }
    assert projection_build.receipt(candidate_by_field)["card_row_count"] == 1
    with pytest.raises(ValueError, match="contract is unsupported"):
        projection_build.receipt({**candidate_by_field, "contract_version": "unknown"})

    source_reader = SimpleNamespace(
        _shared_v3_code_scope_sql=lambda *_args, **_kwargs: ("", [], {}, ""),
        _required_shared_snapshot_key=lambda _tables: 1,
    )
    with pytest.raises(ValueError, match="code-row bound is invalid"):
        projection_source._binding_code_query(
            source_reader,
            object(),
            {"plan_id": "plan"},
            0,
        )


@pytest.mark.asyncio
async def test_empty_materialization_and_preflight_paths_do_no_work(
    monkeypatch,
) -> None:
    """Avoid writes for empty packs and code identities without staged rates."""

    session = SimpleNamespace(execute=AsyncMock())
    await projection_aggregate._store_aggregate_packs(
        session,
        PROJECTION_ID,
        ("CPT", "27447"),
        (),
        _build_state(),
    )
    session.execute.assert_not_awaited()

    monkeypatch.setattr(
        projection_v3,
        "_has_staged_code_inputs",
        AsyncMock(return_value=False),
    )
    assert (
        await projection_v3._preflight_code_work(
            session,
            PROJECTION_ID,
            [("CPT", "27447")],
            [_binding()],
            _build_state(),
        )
        == {}
    )


def test_stored_pack_payload_rejects_non_bytes() -> None:
    """Reject a stored aggregate payload before hashing or decoding it."""

    with pytest.raises(ValueError, match="payload is invalid"):
        projection_receipts._stored_pack_bytes({"payload": "not-bytes"})
