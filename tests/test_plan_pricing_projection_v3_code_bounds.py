# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-wide code and price admission for projection v3."""

from __future__ import annotations

import hashlib
import gc
import weakref
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import plan_pricing_projection_v3 as projection
from api import plan_pricing_projection_v3_code as code_stage
from api import plan_pricing_projection_v3_price as price_stage
from api import ptg2_serving as serving
from api.plan_pricing_projection_source import BindingProjection
from tests.test_plan_pricing_projection_v3 import _ExecuteSession, _binding


_ALIAS_PRICE_SET_ID = "1" * 32
_ALIAS_PROVIDER_SET_ID = "2" * 32


def _numeric_alias_binding() -> BindingProjection:
    code_rows = [
        {"reported_code_system": system, "reported_code": "27447"}
        for system in ("CPT", "HCPCS")
    ]
    return BindingProjection(
        {"ordinal": 0},
        SimpleNamespace(
            network_names=(),
            price_key_block_span=512,
            shared_snapshot_key=1,
            uses_shared_blocks=True,
        ),
        {("CPT", "27447"): code_rows},
        len(code_rows),
    )


async def _numeric_alias_code_rows(_session, _binding, rows):
    assert [row["reported_code_system"] for row in rows] == ["CPT", "HCPCS"]
    serving_row_by_field = {
        "_ptg_provider_set_key": 7,
        "provider_set_global_id_128": _ALIAS_PROVIDER_SET_ID,
        "price_key": 1,
        "price_set_global_id_128": _ALIAS_PRICE_SET_ID,
        "serving_content_hash_128": "3" * 32,
        "source_key": 1,
        "provider_count": 1,
    }
    return [serving_row_by_field, dict(serving_row_by_field)], {
        _ALIAS_PRICE_SET_ID: 1
    }


async def _two_binding_code_rows(_session, binding, _code_rows):
    ordinal = int(binding.binding["ordinal"])
    price_set_id = str(ordinal + 1) * 32
    return [
        {
            "_ptg_provider_set_key": ordinal + 10,
            "provider_set_global_id_128": str(ordinal + 3) * 32,
            "price_key": ordinal + 1,
            "price_set_global_id_128": price_set_id,
            "serving_content_hash_128": str(ordinal + 5) * 32,
            "source_key": ordinal + 1,
            "provider_count": 1,
        }
    ], {price_set_id: ordinal + 1}


async def _two_rates_per_price_key(_session, _tables, price_keys, **_kwargs):
    assert _kwargs["maximum_atom_count"] == 3
    return {
        price_key: [
            {"negotiated_rate": "1"},
            {"negotiated_rate": "2"},
        ]
        for price_key in price_keys
    }


@pytest.mark.asyncio
async def test_normalized_code_occurrence_bound_precedes_binding_reads(
    monkeypatch,
) -> None:
    monkeypatch.setattr(code_stage, "MAX_CODE_OCCURRENCES", 3)
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 2
    )
    binding_code_rows = AsyncMock()

    with pytest.raises(ValueError, match="normalized occurrence bound"):
        await code_stage._has_staged_code_inputs(
            _ExecuteSession(),
            projection._BuildState(hashlib.sha256()),
            ("CPT", "27447"),
            [_binding(0), _binding(1)],
            binding_code_rows=binding_code_rows,
            stage_code_provider_sets=AsyncMock(),
        )

    binding_code_rows.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code_atom_cap", "is_successful"), ((4, True), (3, False))
)
async def test_price_atom_batches_are_bounded_per_read_and_per_code(
    monkeypatch,
    code_atom_cap: int,
    is_successful: bool,
) -> None:
    """Each read and the cumulative TEMP staging stay independently bounded."""

    monkeypatch.setattr(price_stage, "MAX_PRICE_HYDRATION_ATOMS", 3)
    monkeypatch.setattr(
        code_stage, "MAX_CODE_STAGED_PRICE_ATOMS", code_atom_cap
    )
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 1
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)

    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        _two_rates_per_price_key,
    )

    session = _ExecuteSession()
    stage_code_provider_sets = AsyncMock()
    has_staged_code_inputs = code_stage._has_staged_code_inputs(
        session,
        projection._BuildState(hashlib.sha256()),
        ("CPT", "27447"),
        [_binding(0), _binding(1)],
        binding_code_rows=_two_binding_code_rows,
        stage_code_provider_sets=stage_code_provider_sets,
        preflight_price_membership_aliases=AsyncMock(),
    )
    if is_successful:
        assert await has_staged_code_inputs
    else:
        with pytest.raises(ValueError, match="staged price-atom bound"):
            await has_staged_code_inputs
    expected_insert_count = 2 if is_successful else 1
    assert stage_code_provider_sets.await_count == expected_insert_count
    price_stage_inserts = [
        parameters
        for statement, parameters in session.calls
        if "INSERT INTO plan_pricing_price_rate_stage" in statement
    ]
    assert len(price_stage_inserts) == expected_insert_count
    assert all(len(parameters) == 2 for parameters in price_stage_inserts)


@pytest.mark.asyncio
async def test_price_hydration_bisects_only_the_overflowing_batch(
    monkeypatch,
) -> None:
    price_id_by_key = {1: "1" * 32, 2: "2" * 32}
    hydration_calls = []

    async def bounded_prices(_session, _tables, price_keys, **_kwargs):
        normalized_keys = tuple(price_keys)
        hydration_calls.append(normalized_keys)
        if len(normalized_keys) > 1:
            raise serving.ManifestReadLimitError("atom limit")
        return {
            normalized_keys[0]: [
                {"negotiated_rate": str(normalized_keys[0])}
            ]
        }

    monkeypatch.setattr(
        serving, "_version_three_bounded_prices_by_key", bounded_prices
    )
    insert_price_rates = AsyncMock()

    retained_price_ids, consumed_atom_count = (
        await code_stage._stage_binding_price_rates(
        object(),
        _binding(0),
        {price_id: price_key for price_key, price_id in price_id_by_key.items()},
        insert_price_rates=insert_price_rates,
        )
    )

    assert retained_price_ids == set(price_id_by_key.values())
    assert consumed_atom_count == 2
    assert hydration_calls == [(1, 2), (1,), (2,)]
    assert insert_price_rates.await_count == 2


@pytest.mark.asyncio
async def test_price_hydration_converts_each_reverse_key_once(monkeypatch) -> None:
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={1: [{"negotiated_rate": "10"}]}),
    )
    exact_numeric_rates = Mock(wraps=price_stage._exact_numeric_rates)
    monkeypatch.setattr(price_stage, "_exact_numeric_rates", exact_numeric_rates)
    insert_price_rates = AsyncMock()

    await code_stage._stage_binding_price_rates(
        object(),
        _binding(0),
        {"1" * 32: 1, "2" * 32: 1},
        insert_price_rates=insert_price_rates,
    )

    rates_by_price_id = insert_price_rates.await_args.args[2]
    assert exact_numeric_rates.call_count == 1
    assert rates_by_price_id["1" * 32] is rates_by_price_id["2" * 32]


@pytest.mark.asyncio
async def test_membership_alias_preflight_precedes_price_hydration(
    monkeypatch,
) -> None:
    price_set_id = "1" * 32
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 1
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    bounded_prices = AsyncMock()
    monkeypatch.setattr(
        serving, "_version_three_bounded_prices_by_key", bounded_prices
    )
    preflight_error = serving.PTG2ManifestArtifactError("physical alias")

    async def binding_code_rows(_session, _binding, _code_rows):
        return [
            {
                "_ptg_provider_set_key": 7,
                "price_set_global_id_128": price_set_id,
            }
        ], {price_set_id: 1}

    with pytest.raises(serving.PTG2ManifestArtifactError) as exc:
        await code_stage._has_staged_code_inputs(
            _ExecuteSession(),
            projection._BuildState(hashlib.sha256()),
            ("CPT", "27447"),
            [_binding()],
            binding_code_rows=binding_code_rows,
            stage_code_provider_sets=AsyncMock(),
            preflight_price_membership_aliases=AsyncMock(
                side_effect=preflight_error
            ),
        )

    assert exc.value is preflight_error
    bounded_prices.assert_not_awaited()


@pytest.mark.asyncio
async def test_membership_alias_cache_is_reused_across_code_bindings(
    monkeypatch,
) -> None:
    monkeypatch.setattr(price_stage, "MAX_PRICE_HYDRATION_ATOMS", 3)
    monkeypatch.setattr(
        serving, "_declared_geo_rate_count", lambda _code_rows: 1
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        _two_rates_per_price_key,
    )
    state = projection._BuildState(hashlib.sha256())
    retained_counts = []

    async def preflight(*_args, **kwargs):
        cache = kwargs["cache"]
        assert cache is state.price_membership_alias_cache
        retained_counts.append(cache.metadata_record_count)
        cache.metadata_record_count += 1
        cache.maximum_fragment_count += 1

    assert await code_stage._has_staged_code_inputs(
        _ExecuteSession(),
        state,
        ("CPT", "27447"),
        [_binding(0), _binding(1)],
        binding_code_rows=_two_binding_code_rows,
        stage_code_provider_sets=AsyncMock(),
        preflight_price_membership_aliases=preflight,
    )

    assert retained_counts == [0, 1]
    assert state.price_membership_alias_cache.metadata_record_count == 2
    assert state.price_membership_alias_cache.maximum_fragment_count == 2


@pytest.mark.asyncio
async def test_metadata_read_limit_wrapper_preserves_original_message(
    monkeypatch,
) -> None:
    original_error = code_stage.ManifestReadLimitError("metadata detail")
    price_hydration = AsyncMock()
    monkeypatch.setattr(
        code_stage,
        "_stage_binding_price_rates",
        price_hydration,
    )
    stage_code_provider_sets = AsyncMock()

    with pytest.raises(
        code_stage._PriceMembershipMetadataReadLimitError
    ) as raised:
        await code_stage._stage_bounded_binding_input(
            object(),
            projection._BuildState(hashlib.sha256()),
            (_binding(), [], {}, {"1" * 32: 1}),
            3,
            stage_code_provider_sets,
            AsyncMock(side_effect=original_error),
        )

    assert str(raised.value) == str(original_error)
    assert raised.value.__cause__ is original_error
    price_hydration.assert_not_awaited()
    stage_code_provider_sets.assert_not_awaited()


@pytest.mark.asyncio
async def test_hydration_read_limit_wrapper_preserves_original_message(
    monkeypatch,
) -> None:
    original_error = code_stage.ManifestReadLimitError("hydration detail")
    price_hydration = AsyncMock(side_effect=original_error)
    monkeypatch.setattr(
        code_stage,
        "_stage_binding_price_rates",
        price_hydration,
    )
    stage_code_provider_sets = AsyncMock()

    with pytest.raises(code_stage._PriceHydrationReadLimitError) as raised:
        await code_stage._stage_bounded_binding_input(
            object(),
            projection._BuildState(hashlib.sha256()),
            (_binding(), [], {}, {"1" * 32: 1}),
            3,
            stage_code_provider_sets,
            AsyncMock(),
        )

    assert str(raised.value) == str(original_error)
    assert raised.value.__cause__ is original_error
    price_hydration.assert_awaited_once()
    stage_code_provider_sets.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_price_key_overflow_remains_terminal(monkeypatch) -> None:
    atom_limit_error = serving.ManifestReadLimitError("atom limit")
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(side_effect=atom_limit_error),
    )
    insert_price_rates = AsyncMock()

    with pytest.raises(serving.ManifestReadLimitError) as exc:
        await code_stage._stage_binding_price_rates(
            object(),
            _binding(0),
            {"1" * 32: 1, "2" * 32: 1},
            insert_price_rates=insert_price_rates,
        )

    assert exc.value is atom_limit_error
    serving._version_three_bounded_prices_by_key.assert_awaited_once()
    insert_price_rates.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_artifact_corruption_is_not_split(monkeypatch) -> None:
    artifact_error = serving.PTG2ManifestArtifactError("corrupt")
    bounded_prices = AsyncMock(side_effect=artifact_error)
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        bounded_prices,
    )

    with pytest.raises(serving.PTG2ManifestArtifactError) as exc:
        await code_stage._stage_binding_price_rates(
            object(),
            _binding(0),
            {"1" * 32: 1, "2" * 32: 2},
        )

    assert exc.value is artifact_error
    bounded_prices.assert_awaited_once()


@pytest.mark.asyncio
async def test_price_hydration_uses_declared_block_span_and_releases_prior_rows(
    monkeypatch,
) -> None:
    class PriceRow(dict):
        pass

    binding = _binding(0)
    binding.serving_tables.price_key_block_span = 256
    first_row_refs = []
    hydration_calls = []

    async def bounded_prices(_session, _tables, price_keys, **_kwargs):
        normalized_keys = tuple(price_keys)
        hydration_calls.append(normalized_keys)
        if normalized_keys == (1,):
            first_row = PriceRow(negotiated_rate="1")
            first_row_refs.append(weakref.ref(first_row))
            return {1: [first_row]}
        gc.collect()
        assert first_row_refs[0]() is None
        return {300: [PriceRow(negotiated_rate="2")]}

    monkeypatch.setattr(
        serving, "_version_three_bounded_prices_by_key", bounded_prices
    )

    async def discard_price_rates(*_args) -> None:
        return None

    retained_price_ids, consumed_atom_count = (
        await code_stage._stage_binding_price_rates(
            object(),
            binding,
            {"1" * 32: 1, "2" * 32: 300},
            insert_price_rates=discard_price_rates,
        )
    )

    assert retained_price_ids == {"1" * 32, "2" * 32}
    assert consumed_atom_count == 2
    assert hydration_calls == [(1,), (300,)]


@pytest.mark.asyncio
async def test_numeric_cpt_hcpcs_aliases_keep_occurrence_multiplicity(
    monkeypatch,
) -> None:
    """Canonical aliases retain occurrence multiplicity without duplicating atoms."""

    binding = _numeric_alias_binding()
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda rows: len(rows))
    monkeypatch.setattr(serving, "_ptg2_manifest_id", str)

    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={1: [{"negotiated_rate": "10"}]}),
    )

    session = _ExecuteSession()
    assert await code_stage._has_staged_code_inputs(
        session,
        projection._BuildState(hashlib.sha256()),
        ("CPT", "27447"),
        [binding],
        binding_code_rows=_numeric_alias_code_rows,
        stage_code_provider_sets=AsyncMock(),
        preflight_price_membership_aliases=AsyncMock(),
    )
    occurrence_rows = next(
        parameters
        for statement, parameters in session.calls
        if "INSERT INTO plan_pricing_code_occurrence_stage" in statement
    )
    price_rows = next(
        parameters
        for statement, parameters in session.calls
        if "INSERT INTO plan_pricing_price_rate_stage" in statement
    )
    assert occurrence_rows == [
        {
            "binding_ordinal": 0,
            "provider_set_key": 7,
            "price_set_id": _ALIAS_PRICE_SET_ID,
            "occurrence_count": 2,
        }
    ]
    assert len(price_rows) == 1
