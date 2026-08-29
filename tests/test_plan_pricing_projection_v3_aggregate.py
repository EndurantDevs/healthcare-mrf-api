# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from api import plan_pricing_projection_v3 as projection
from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    decode_aggregate_pack,
)
from api.plan_pricing_projection_source import BindingProjection


PROJECTION_ID = "a" * 64


class _ExecuteSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        return SimpleNamespace()


def _binding(ordinal: int = 0) -> BindingProjection:
    return BindingProjection(
        {"ordinal": ordinal},
        SimpleNamespace(network_names=()),
        {("CPT", "27447"): [{"code_key": 1, "rate_count": 1}]},
    )


def _aggregate(zip5: str, provider_count: int = 3) -> AggregateZipRecord:
    return AggregateZipRecord(
        zip5,
        provider_count,
        4,
        Decimal("1"),
        Decimal("2.5"),
        Decimal("4"),
    )


def test_prewarm_heap_is_bounded_deterministic_and_excludes_broad_em() -> None:
    prewarm_heap_items: list[projection._PrewarmHeapItem] = []
    projection._retain_prewarm_shape(
        prewarm_heap_items,
        ("CPT", "99213"),
        _aggregate("10001", 1_000_000),
    )
    for ordinal in range(800):
        projection._retain_prewarm_shape(
            prewarm_heap_items,
            ("CPT", f"{10000 + ordinal}"),
            _aggregate("10001", ordinal + 1),
        )

    selected_shapes = projection._ordered_prewarm_shapes(prewarm_heap_items)

    assert len(selected_shapes) == projection.MAX_PREWARM_SHAPES == 768
    assert selected_shapes[0].provider_count == 800
    assert selected_shapes[-1].provider_count == 33
    assert all(shape.code != "99213" for shape in selected_shapes)
    assert list(selected_shapes) == sorted(
        selected_shapes,
        key=lambda shape: (-shape.provider_count, *shape.identity),
    )


@pytest.mark.asyncio
async def test_aggregate_pack_insert_binds_hash_and_exact_receipts() -> None:
    session = _ExecuteSession()
    state = projection._BuildState(hashlib.sha256())
    code_identity = ("CPT", "27447")
    aggregate_rows = (_aggregate("10001"), _aggregate("11001", 5))

    await projection._store_aggregate_packs(
        session, PROJECTION_ID, code_identity, aggregate_rows, state
    )

    insert_sql, parameter_rows = session.calls[0]
    assert "pg_catalog.sha256(:payload)" in insert_sql
    assert ":payload_sha256" not in insert_sql
    assert len(parameter_rows) == 2
    for parameter_by_field in parameter_rows:
        assert "payload_sha256" not in parameter_by_field
        encoded_pack = parameter_by_field["payload"]
        assert parameter_by_field["stored_byte_count"] == len(encoded_pack)
        assert aggregate_pack_raw_byte_count(
            encoded_pack,
            expected_raw_byte_count=parameter_by_field["raw_byte_count"],
        ) == parameter_by_field["raw_byte_count"]
        pack_key = AggregatePackKey(
            PROJECTION_ID,
            AggregateCodeIdentity(*code_identity),
            parameter_by_field["zip_prefix_2"],
        )
        decoded_pack = decode_aggregate_pack(
            encoded_pack, expected_key=pack_key
        )
        assert len(decoded_pack.records) == parameter_by_field["entry_count"]
        assert aggregate_logical_digest(
            pack_key.code_identity, decoded_pack.records
        ) == parameter_by_field["logical_digest"]
    assert state.aggregate_entry_count == 2
    assert state.aggregate_pack_count == 2
    assert state.aggregate_raw_byte_count == sum(
        parameter_by_field["raw_byte_count"]
        for parameter_by_field in parameter_rows
    )
    assert state.aggregate_stored_byte_count == sum(
        parameter_by_field["stored_byte_count"]
        for parameter_by_field in parameter_rows
    )
    assert state.content_digest.hexdigest() != hashlib.sha256().hexdigest()


def test_aggregate_sql_preserves_exact_old_materializer_multiplicity() -> None:
    aggregate_sql = projection._AGGREGATE_STATS_SQL

    assert "COUNT(DISTINCT npi)" in aggregate_sql
    assert "plan_pricing_eligible_member_cell_stage" in aggregate_sql
    assert "plan_pricing_set_cell_stage" in aggregate_sql
    assert "plan_pricing_rate_frequency_stage" in aggregate_sql
    assert "SUM(rate.multiplicity)" in aggregate_sql
    assert "plan_pricing_code_occurrence_stage" not in aggregate_sql
    assert "plan_pricing_price_rate_stage" not in aggregate_sql
    assert "(ranked.total + 1) / 2" in aggregate_sql
    assert "(ranked.total + 2) / 2" in aggregate_sql


def test_seal_counts_reject_inconsistent_builder_outputs() -> None:
    counts = projection.ProjectionV3Counts(1, 1, 2, 2, 1, 10, 8, 2)
    assert counts.prewarm_shape_count == 2

    for inconsistent_counts in (
        (0, 0, 0, 1, 0, 0, 0, 0),
        (0, 0, 0, 1, 1, 0, 1, 0),
        (0, 0, 0, 1, 1, 1, 1, 2),
        (0, 1, 0, 0, 0, 0, 0, 0),
    ):
        with pytest.raises(ValueError, match="inconsistent"):
            projection.ProjectionV3Counts(*inconsistent_counts)


@pytest.mark.asyncio
async def test_materializer_validates_bindings_and_yields_per_code(monkeypatch) -> None:
    create_stage = AsyncMock()
    materialize_cells = AsyncMock()
    stage_code = AsyncMock(side_effect=(True, True))
    admitted_work = SimpleNamespace(
        membership_probe_rows=1,
        member_cell_rows=1,
    )
    prepare_code_work = AsyncMock(return_value=admitted_work)
    restage_code_work = AsyncMock(return_value=admitted_work)
    persist_provider = AsyncMock()
    store_rate_profiles = AsyncMock()
    aggregate_records = AsyncMock(return_value=())
    store_aggregate_packs = AsyncMock()
    store_prewarm = AsyncMock(return_value=0)
    event_loop_yield = AsyncMock()
    monkeypatch.setattr(projection, "_create_stage_tables", create_stage)
    monkeypatch.setattr(
        projection, "_materialize_provider_cells", materialize_cells
    )
    monkeypatch.setattr(projection, "_has_staged_code_inputs", stage_code)
    monkeypatch.setattr(projection, "_prepare_code_work", prepare_code_work)
    monkeypatch.setattr(projection, "_stage_code_work", restage_code_work)
    monkeypatch.setattr(
        projection, "_persist_provider_projection", persist_provider
    )
    monkeypatch.setattr(
        projection, "_store_rate_profiles", store_rate_profiles
    )
    monkeypatch.setattr(projection, "_aggregate_records", aggregate_records)
    monkeypatch.setattr(
        projection, "_store_aggregate_packs", store_aggregate_packs
    )
    monkeypatch.setattr(projection, "_store_prewarm_shapes", store_prewarm)
    monkeypatch.setattr(projection.asyncio, "sleep", event_loop_yield)

    await projection.materialize_factorized_projection(
        object(), PROJECTION_ID, [_binding(1)], hashlib.sha256()
    )
    assert event_loop_yield.await_count == 2
    event_loop_yield.assert_awaited_with(0)
    assert materialize_cells.await_count == 1
    assert materialize_cells.await_args.args[1] == PROJECTION_ID
    prepare_code_work.assert_awaited_once_with(
        ANY, PROJECTION_ID, ("CPT", "27447"), ANY
    )
    persist_provider.assert_awaited_once_with(ANY, PROJECTION_ID)
    restage_code_work.assert_awaited_once_with(
        ANY, PROJECTION_ID, ("CPT", "27447"), 1, 1
    )
    store_rate_profiles.assert_awaited_once()
    store_aggregate_packs.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift_kind", ("missing_input", "actual_work", "provider_counter")
)
async def test_materializer_rejects_second_pass_admission_drift(
    monkeypatch, drift_kind
) -> None:
    admitted_work = SimpleNamespace(
        membership_probe_rows=1,
        member_cell_rows=1,
    )
    actual_work = admitted_work
    if drift_kind == "actual_work":
        actual_work = SimpleNamespace(
            membership_probe_rows=1,
            member_cell_rows=2,
        )
    state = projection._BuildState(hashlib.sha256())

    def has_staged_inputs(*_args) -> bool:
        if drift_kind == "provider_counter":
            state.provider_cell_count += 1
        return drift_kind != "missing_input"

    stage_code_inputs = AsyncMock(side_effect=has_staged_inputs)
    stage_code_work = AsyncMock(return_value=actual_work)
    store_rate_profiles = AsyncMock()
    aggregate_records = AsyncMock()
    store_aggregate_packs = AsyncMock()
    monkeypatch.setattr(
        projection, "_has_staged_code_inputs", stage_code_inputs
    )
    monkeypatch.setattr(projection, "_stage_code_work", stage_code_work)
    monkeypatch.setattr(projection, "_store_rate_profiles", store_rate_profiles)
    monkeypatch.setattr(projection, "_aggregate_records", aggregate_records)
    monkeypatch.setattr(
        projection, "_store_aggregate_packs", store_aggregate_packs
    )

    with pytest.raises(ValueError, match="code admission changed"):
        await projection._store_admitted_codes(
            object(),
            PROJECTION_ID,
            [_binding()],
            {("CPT", "27447"): admitted_work},
            state,
        )

    if drift_kind == "missing_input":
        stage_code_work.assert_not_awaited()
    else:
        stage_code_work.assert_awaited_once()
    store_rate_profiles.assert_not_awaited()
    aggregate_records.assert_not_awaited()
    store_aggregate_packs.assert_not_awaited()


@pytest.mark.asyncio
async def test_materializer_rejects_duplicate_binding_ordinals() -> None:
    with pytest.raises(ValueError, match="not unique"):
        await projection.materialize_factorized_projection(
            object(), PROJECTION_ID, [_binding(1), _binding(1)], hashlib.sha256()
        )


@pytest.mark.asyncio
async def test_materializer_work_rejection_precedes_both_writers(monkeypatch) -> None:
    monkeypatch.setattr(projection, "_create_stage_tables", AsyncMock())
    monkeypatch.setattr(
        projection, "_has_staged_code_inputs", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(projection, "_materialize_provider_cells", AsyncMock())
    persist_provider = AsyncMock()
    monkeypatch.setattr(
        projection, "_persist_provider_projection", persist_provider
    )
    monkeypatch.setattr(
        projection,
        "_prepare_code_work",
        AsyncMock(side_effect=ValueError("member-cell work bound exceeded")),
    )
    store_rate_profiles = AsyncMock()
    aggregate_records = AsyncMock()
    store_aggregate_packs = AsyncMock()
    monkeypatch.setattr(projection, "_store_rate_profiles", store_rate_profiles)
    monkeypatch.setattr(projection, "_aggregate_records", aggregate_records)
    monkeypatch.setattr(
        projection, "_store_aggregate_packs", store_aggregate_packs
    )

    with pytest.raises(ValueError, match="member-cell work bound exceeded"):
        await projection.materialize_factorized_projection(
            object(), PROJECTION_ID, [_binding()], hashlib.sha256()
        )

    store_rate_profiles.assert_not_awaited()
    aggregate_records.assert_not_awaited()
    store_aggregate_packs.assert_not_awaited()
    persist_provider.assert_not_awaited()
