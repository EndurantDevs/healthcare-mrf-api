# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compatibility facade for bounded plan-pricing projection v3."""

from __future__ import annotations

import asyncio
from typing import Any

from api import plan_pricing_projection_v3_aggregate as _aggregate
from api import plan_pricing_projection_v3_code as _code
from api import plan_pricing_projection_v3_provider_cells as _provider_cells
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import (
    BindingProjection,
    projection_provider_rows_for_npis,
)
from api.plan_pricing_projection_v3_aggregate import (
    _aggregate_pack_row,
    _ordered_prewarm_shapes,
    _retain_prewarm_shape,
)
from api.plan_pricing_projection_v3_code import (
    MAX_CODE_OCCURRENCES,
    MAX_CODE_PRICE_ATOMS,
    _binding_code_rows,
    _code_occurrences,
    _exact_numeric_rates,
    _manifest_id,
    _store_rate_profiles as _store_rate_profiles_impl,
)
from api.plan_pricing_projection_v3_provider import (
    MAX_PROVIDER_NPIS_PER_SET,
    PROVIDER_SET_BATCH_SIZE,
    _create_stage_tables,
    _stage_code_provider_sets,
    _validate_provider_set_memberships,
    _validated_binding_ordinals,
)
from api.plan_pricing_projection_v3_provider_cells import (
    MAX_PROVIDER_CELLS_PER_BATCH,
    PROVIDER_NPI_BATCH_SIZE,
    _next_provider_npis,
    _normalized_taxonomy_codes,
    _provider_cell_rows,
    _provider_fragment,
)
from api.plan_pricing_projection_v3_receipts import (
    _stored_pack_bytes,
    _validated_stored_pack,
    validate_stored_aggregate_packs,
)
from api.plan_pricing_projection_v3_types import (
    MAX_PREWARM_SHAPES,
    ProjectionV3Counts,
    _BuildState,
    _PrewarmHeapItem,
    _PrewarmShape,
    _insert_batches,
)


_AGGREGATE_STATS_SQL = _aggregate._AGGREGATE_STATS_SQL
_AGGREGATE_WORK_SQL = _aggregate._AGGREGATE_WORK_SQL


async def _materialize_provider_cells(
    session: Any,
    projection_id: str,
    state: _BuildState,
) -> None:
    await _provider_cells._materialize_provider_cells(
        session,
        projection_id,
        state,
        next_provider_npis=_next_provider_npis,
        provider_rows_for_npis=projection_provider_rows_for_npis,
        provider_cell_rows=_provider_cell_rows,
        insert_batches=_insert_batches,
    )


async def _insert_code_occurrences(
    session: Any,
    binding_ordinal: int,
    occurrences: Any,
) -> None:
    await _code._insert_code_occurrences(
        session,
        binding_ordinal,
        occurrences,
        insert_batches=_insert_batches,
    )


async def _insert_price_rates(
    session: Any,
    binding_ordinal: int,
    rates_by_price_id: Any,
) -> None:
    await _code._insert_price_rates(
        session,
        binding_ordinal,
        rates_by_price_id,
        insert_batches=_insert_batches,
    )


async def _has_staged_code_inputs(
    session: Any,
    projection_id: str,
    state: _BuildState,
    code_identity: tuple[str, str],
    bindings: list[BindingProjection],
) -> bool:
    return await _code._has_staged_code_inputs(
        session,
        projection_id,
        state,
        code_identity,
        bindings,
        binding_code_rows=_binding_code_rows,
        stage_code_provider_sets=_stage_code_provider_sets,
    )


async def _store_rate_profiles(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
) -> None:
    await _store_rate_profiles_impl(
        session,
        projection_id,
        code_identity,
        state,
    )


def _aggregate_stats_sql(has_taxonomy_rule: bool) -> str:
    return _aggregate._aggregate_stats_sql(
        has_taxonomy_rule,
        aggregate_stats_sql=_AGGREGATE_STATS_SQL,
    )


async def _aggregate_records(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState | None = None,
) -> Any:
    return await _aggregate._aggregate_records(
        session,
        projection_id,
        code_identity,
        state,
        aggregate_stats_sql=_AGGREGATE_STATS_SQL,
        aggregate_work_sql=_AGGREGATE_WORK_SQL,
    )


async def _store_aggregate_packs(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    aggregate_records: Any,
    state: _BuildState,
) -> None:
    await _aggregate._store_aggregate_packs(
        session,
        projection_id,
        code_identity,
        aggregate_records,
        state,
        aggregate_pack_row=_aggregate_pack_row,
        retain_prewarm_shape=_retain_prewarm_shape,
        insert_batches=_insert_batches,
    )


async def _store_prewarm_shapes(
    session: Any,
    projection_id: str,
    state: _BuildState,
) -> int:
    return await _aggregate._store_prewarm_shapes(
        session,
        projection_id,
        state,
        ordered_prewarm_shapes=_ordered_prewarm_shapes,
        insert_batches=_insert_batches,
    )


async def materialize_factorized_projection(
    session: Any,
    projection_id: str,
    bindings: list[BindingProjection],
    content_digest: Any,
) -> ProjectionV3Counts:
    """Build provider cells and exact aggregate packs with bounded app memory."""

    _validated_binding_ordinals(bindings)
    state = _BuildState(content_digest)
    await _create_stage_tables(session)
    code_identities = sorted(
        {
            code_identity
            for binding in bindings
            for code_identity in binding.code_rows_by_identity
        }
    )
    for code_identity in code_identities:
        if await _has_staged_code_inputs(
            session, projection_id, state, code_identity, bindings
        ):
            await _materialize_provider_cells(session, projection_id, state)
            await _store_rate_profiles(
                session, projection_id, code_identity, state
            )
            aggregate_records = await _aggregate_records(
                session, projection_id, code_identity, state
            )
            await _store_aggregate_packs(
                session,
                projection_id,
                code_identity,
                aggregate_records,
                state,
            )
        await asyncio.sleep(0)
    prewarm_shape_count = await _store_prewarm_shapes(
        session, projection_id, state
    )
    return ProjectionV3Counts(
        state.provider_membership_count,
        state.provider_cell_count,
        state.provider_fragment_byte_count,
        state.aggregate_entry_count,
        state.aggregate_pack_count,
        state.aggregate_raw_byte_count,
        state.aggregate_stored_byte_count,
        prewarm_shape_count,
        state.rate_profile_count,
    )
