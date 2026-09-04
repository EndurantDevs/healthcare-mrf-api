# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as code_stage
from api import plan_pricing_projection_v3_provider as provider_stage
from api import plan_pricing_projection_v3_provider_cells as provider_cells
from api import ptg2_serving as serving
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
        SimpleNamespace(
            network_names=(),
            price_key_block_span=512,
            shared_snapshot_key=1,
            uses_shared_blocks=True,
        ),
        {("CPT", "27447"): [{"code_key": 1, "rate_count": 1}]},
        1,
    )


def _provider_metadata(*entries: tuple[str, int, int]):
    return {
        provider_set_id: SimpleNamespace(
            provider_set_key=provider_set_key,
            provider_count=provider_count,
        )
        for provider_set_id, provider_set_key, provider_count in entries
    }


def test_provider_cell_digest_binds_normalized_full_taxonomy_array() -> None:
    provider_by_field = {
        "npi": 1000000001,
        "zip5": "10001",
        "entity_type_code": 1,
        "taxonomy_codes": [" 207x00000x ", "208D00000X"],
    }
    first_state = projection._BuildState(hashlib.sha256())
    first_rows = projection._provider_cell_rows(
        PROJECTION_ID,
        first_state,
        [1000000001],
        {1000000001: [provider_by_field]},
    )
    second_state = projection._BuildState(hashlib.sha256())
    projection._provider_cell_rows(
        PROJECTION_ID,
        second_state,
        [1000000001],
        {1000000001: [{**provider_by_field, "taxonomy_codes": ["207X00000X", "207Q00000X"]}]},
    )

    assert first_rows[0]["taxonomy_codes"] == ["207X00000X", "208D00000X"]
    assert first_state.content_digest.digest() != second_state.content_digest.digest()


@pytest.mark.asyncio
async def test_stage_ddl_uses_one_statement_per_execute() -> None:
    session = _ExecuteSession()

    await projection._create_stage_tables(session)
    assert len(session.calls) == 11
    assert all(sql.count("CREATE TEMP TABLE") == 1 for sql, _ in session.calls)
    assert "plan_pricing_provider_set_stage" in session.calls[0][0]
    assert "plan_pricing_provider_npi_materialized_stage" in session.calls[2][0]
    assert "plan_pricing_provider_npi_pending_stage" in session.calls[3][0]
    assert "plan_pricing_rate_occurrence_stage" in session.calls[5][0]
    assert "plan_pricing_rate_frequency_stage" in session.calls[7][0]
    assert "plan_pricing_provider_cell_stage" in session.calls[8][0]
    assert "state_fragment bytea NULL" in session.calls[8][0]
    assert "plan_pricing_eligible_member_cell_stage" in session.calls[9][0]
    assert "plan_pricing_set_cell_stage" in session.calls[10][0]


@pytest.mark.asyncio
async def test_provider_set_membership_is_exactly_bounded(monkeypatch) -> None:
    provider_npis = AsyncMock(return_value={"1" * 32: tuple(range(1, projection.MAX_PROVIDER_NPIS_PER_SET + 2))})
    monkeypatch.setattr(serving, "_provider_npis_for_sets", provider_npis)
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(
            return_value=_provider_metadata(
                (
                    "1" * 32,
                    7,
                    projection.MAX_PROVIDER_NPIS_PER_SET + 1,
                )
            )
        ),
    )

    with pytest.raises(ValueError, match="membership exceeds its bound"):
        await provider_stage._stage_provider_set_batch(
            _ExecuteSession(),
            _binding(),
            [{"provider_set_key": 7, "provider_set_id": "1" * 32}],
            projection._BuildState(hashlib.sha256()),
        )

    assert provider_npis.await_args.kwargs["limit_per_set"] == (projection.MAX_PROVIDER_NPIS_PER_SET + 1)


@pytest.mark.asyncio
async def test_provider_projection_persists_only_after_admission() -> None:
    session = _ExecuteSession()

    await provider_stage._persist_provider_projection(session, PROJECTION_ID)

    assert len(session.calls) == 3
    assert "plan_pricing_provider_membership" in session.calls[0][0]
    assert "plan_pricing_provider_member_stage" in session.calls[0][0]
    assert "plan_pricing_provider_cell" in session.calls[1][0]
    assert "plan_pricing_provider_cell_stage" in session.calls[1][0]
    assert "plan_pricing_provider_state" in session.calls[2][0]
    assert "provider_fragment" in session.calls[2][0]
    assert "convert_from(state_fragment, 'UTF8')" in session.calls[2][0]
    assert "SELECT DISTINCT" not in session.calls[2][0]
    assert "^[A-Z]{2}$" in session.calls[2][0]
    assert all(parameters == {"projection_id": PROJECTION_ID} for _, parameters in session.calls)


@pytest.mark.asyncio
async def test_code_provider_sets_stage_only_new_referenced_keys(monkeypatch) -> None:
    class _MappingRows:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return iter(self.rows)

    class _ExistingSession:
        async def execute(self, *_args, **_kwargs):
            return _MappingRows([{"provider_set_key": 7, "provider_set_id": "1" * 32}])

    stage_batch = AsyncMock()
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    state = projection._BuildState(hashlib.sha256())
    state.staged_provider_set_count = 1
    await provider_stage._stage_code_provider_sets(
        _ExistingSession(),
        _binding(3),
        [
            {
                "_ptg_provider_set_key": 7,
                "provider_set_global_id_128": "1" * 32,
            },
            {
                "_ptg_provider_set_key": 8,
                "provider_set_global_id_128": "2" * 32,
            },
        ],
        {7, 8},
        state,
        stage_provider_set_batch=stage_batch,
    )

    assert stage_batch.await_count == 1
    assert stage_batch.await_args.args[2] == [{"provider_set_key": 8, "provider_set_id": "2" * 32}]


@pytest.mark.asyncio
async def test_provider_set_release_cap_is_inclusive(monkeypatch) -> None:
    class _EmptyRows:
        def mappings(self):
            return iter(())

    class _EmptySession:
        async def execute(self, *_args, **_kwargs):
            return _EmptyRows()

    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    monkeypatch.setattr(provider_stage, "MAX_PROJECTION_PROVIDER_SETS", 1)
    serving_rows = [
        {
            "_ptg_provider_set_key": 7,
            "provider_set_global_id_128": "1" * 32,
        }
    ]
    accepted_stage = AsyncMock()
    await provider_stage._stage_code_provider_sets(
        _EmptySession(),
        _binding(),
        serving_rows,
        {7},
        projection._BuildState(hashlib.sha256()),
        stage_provider_set_batch=accepted_stage,
    )
    accepted_stage.assert_awaited_once()

    full_state = projection._BuildState(hashlib.sha256())
    full_state.staged_provider_set_count = 1
    with pytest.raises(ValueError, match="provider-set bound exceeded"):
        await provider_stage._stage_code_provider_sets(
            _EmptySession(),
            _binding(),
            serving_rows,
            {7},
            full_state,
            stage_provider_set_batch=AsyncMock(),
        )


@pytest.mark.asyncio
async def test_provider_membership_release_cap_is_inclusive(monkeypatch) -> None:
    provider_set_id = "1" * 32
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value={provider_set_id: (11, 12)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value=_provider_metadata((provider_set_id, 7, 2))),
    )
    monkeypatch.setattr(provider_stage, "MAX_PROJECTION_PROVIDER_MEMBERSHIPS", 2)

    async def insert_batches(_session, _statement, rows):
        list(rows)

    await provider_stage._stage_provider_set_batch(
        _ExecuteSession(),
        _binding(),
        [{"provider_set_key": 7, "provider_set_id": provider_set_id}],
        projection._BuildState(hashlib.sha256()),
        insert_batches=insert_batches,
    )

    full_state = projection._BuildState(hashlib.sha256())
    full_state.provider_membership_count = 1
    with pytest.raises(ValueError, match="membership bound exceeded"):
        await provider_stage._stage_provider_set_batch(
            _ExecuteSession(),
            _binding(),
            [{"provider_set_key": 7, "provider_set_id": provider_set_id}],
            full_state,
            insert_batches=insert_batches,
        )


@pytest.mark.asyncio
async def test_pending_provider_queue_avoids_membership_rescan() -> None:
    class _Scalars:
        def all(self):
            return [11, 12]

    class _Result:
        def scalars(self):
            return _Scalars()

    class _PendingSession(_ExecuteSession):
        async def execute(self, statement, parameters=None):
            self.calls.append((str(statement), parameters))
            return _Result()

    session = _PendingSession()
    assert await provider_cells._next_provider_npis(session, 10) == [11, 12]
    assert "plan_pricing_provider_npi_pending_stage" in session.calls[0][0]
    assert "plan_pricing_provider_member_stage" not in session.calls[0][0]


@pytest.mark.asyncio
async def test_provider_materialization_drains_pending_npis_once() -> None:
    next_provider_npis = AsyncMock(side_effect=[[11], []])
    provider_rows_for_npis = AsyncMock(return_value={11: ()})
    provider_cell_rows = Mock(return_value=[])
    insert_batches = AsyncMock()
    session = _ExecuteSession()

    await provider_cells._materialize_provider_cells(
        session,
        PROJECTION_ID,
        projection._BuildState(hashlib.sha256()),
        next_provider_npis=next_provider_npis,
        provider_rows_for_npis=provider_rows_for_npis,
        provider_cell_rows=provider_cell_rows,
        insert_batches=insert_batches,
    )

    assert "plan_pricing_provider_cell_stage" in str(insert_batches.await_args.args[1])
    assert "provider_npi_materialized_stage" in session.calls[0][0]
    assert "DELETE FROM plan_pricing_provider_npi_pending_stage" in (session.calls[1][0])
    assert session.calls[0][1] == session.calls[1][1] == {"npis": [11]}


def test_provider_cell_release_caps_are_inclusive(monkeypatch) -> None:
    provider_by_field = {
        "npi": 1000000001,
        "zip5": "10001",
        "entity_type_code": 1,
        "taxonomy_codes": ["207X00000X"],
    }
    fragment_size = len(provider_cells._provider_fragment(provider_by_field))
    monkeypatch.setattr(provider_cells, "MAX_PROJECTION_PROVIDER_CELLS", 1)
    monkeypatch.setattr(
        provider_cells,
        "MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES",
        fragment_size,
    )
    state = projection._BuildState(hashlib.sha256())
    assert (
        len(
            provider_cells._provider_cell_rows(
                PROJECTION_ID,
                state,
                [1000000001],
                {1000000001: [provider_by_field]},
            )
        )
        == 1
    )

    with pytest.raises(ValueError, match="provider-cell bound exceeded"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            state,
            [1000000001],
            {1000000001: [provider_by_field]},
        )

    byte_state = projection._BuildState(hashlib.sha256())
    monkeypatch.setattr(
        provider_cells,
        "MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES",
        fragment_size - 1,
    )
    with pytest.raises(ValueError, match="provider-cell bound exceeded"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            byte_state,
            [1000000001],
            {1000000001: [provider_by_field]},
        )


@pytest.mark.asyncio
async def test_code_read_fails_before_io_above_declared_bound(monkeypatch) -> None:
    merge_rows = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_declared_geo_rate_count",
        lambda _code_rows: projection.MAX_CODE_OCCURRENCES + 1,
    )
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)

    with pytest.raises(ValueError, match="occurrence bound"):
        await projection._binding_code_rows(object(), _binding(), [{"code_key": 1, "rate_count": 1}])

    merge_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_code_read_preserves_declared_rows_and_price_identities(
    monkeypatch,
) -> None:
    serving_rows = [
        {
            "_ptg_provider_set_key": 3,
            "price_set_global_id_128": "1" * 32,
            "price_key": 9,
        },
        {
            "_ptg_provider_set_key": 4,
            "price_set_global_id_128": "1" * 32,
            "price_key": 9,
        },
    ]
    merge_rows = AsyncMock(return_value=serving_rows)
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda _code_rows: 2)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    selected_rows, price_keys_by_set = await projection._binding_code_rows(
        object(), _binding(), [{"code_key": 1, "rate_count": 2}]
    )

    assert selected_rows == serving_rows
    assert price_keys_by_set == {"1" * 32: 9}
    assert merge_rows.await_args.kwargs["limit"] == (projection.MAX_CODE_OCCURRENCES + 1)


@pytest.mark.asyncio
async def test_code_read_rejects_declared_count_or_price_identity_drift(
    monkeypatch,
) -> None:
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda _code_rows: 2)
    merge_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)

    with pytest.raises(ValueError, match="bounded rate layout"):
        await projection._binding_code_rows(object(), _binding(), [{}])

    merge_rows.return_value = [
        {
            "_ptg_provider_set_key": 1,
            "price_set_global_id_128": "1" * 32,
            "price_key": 1,
        },
        {
            "_ptg_provider_set_key": 2,
            "price_set_global_id_128": "1" * 32,
            "price_key": 2,
        },
    ]
    with pytest.raises(ValueError, match="price identity is inconsistent"):
        await projection._binding_code_rows(object(), _binding(), [{}])


def test_numeric_rate_parity_rejects_silent_atom_loss() -> None:
    with pytest.raises(ValueError, match="non-numeric rate"):
        projection._exact_numeric_rates(({"negotiated_rate": "1"}, {"negotiated_rate": "bad"}))
