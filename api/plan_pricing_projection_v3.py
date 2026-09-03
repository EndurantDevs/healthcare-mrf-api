# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compatibility facade for bounded plan-pricing projection v3."""

from __future__ import annotations

import asyncio
from typing import Any

from api import plan_pricing_projection_v3_aggregate as _aggregate
from api import plan_pricing_projection_v3_code as _code
from api import plan_pricing_projection_v3_provider_cells as _provider_cells
from api import plan_pricing_projection_v3_work as _work
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
    _binding_code_rows,
    _code_occurrences,
    _exact_numeric_rates,
    _manifest_id,
    _store_rate_occurrences as _store_rate_occurrences_impl,
    _store_rate_profiles as _store_rate_profiles_impl,
)
from api.plan_pricing_projection_v3_provider import (
    MAX_PROVIDER_NPIS_PER_SET,
    PROVIDER_SET_BATCH_SIZE,
    _create_stage_tables,
    _persist_provider_projection,
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
    state: _BuildState,
    code_identity: tuple[str, str],
    bindings: list[BindingProjection],
    *,
    diagnostic_stage: Any = None,
) -> bool:
    return await _code._has_staged_code_inputs(
        session,
        state,
        code_identity,
        bindings,
        binding_code_rows=_binding_code_rows,
        stage_code_provider_sets=_stage_code_provider_sets,
        diagnostic_stage=diagnostic_stage,
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


async def _store_rate_occurrences(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
) -> None:
    await _store_rate_occurrences_impl(
        session,
        projection_id,
        code_identity,
        state,
    )


async def _prepare_code_work(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
) -> Any:
    return await _work._prepare_code_work(
        session,
        projection_id,
        code_identity,
        state,
    )


async def _stage_code_work(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    membership_probe_limit: int,
    member_cell_limit: int,
    rate_profile_work_limit: int = _code.MAX_CODE_RATE_PROFILE_WORK_ROWS,
    *,
    diagnostic_stage: Any = None,
) -> Any:
    return await _work._stage_code_work(
        session,
        projection_id,
        code_identity,
        membership_probe_limit,
        member_cell_limit,
        rate_profile_work_limit,
        diagnostic_stage=diagnostic_stage,
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


async def _preflight_code_work(
    session: Any,
    projection_id: str,
    code_identities: list[tuple[str, str]],
    bindings: list[BindingProjection],
    state: _BuildState,
) -> dict[tuple[str, str], Any]:
    admitted_work_by_code: dict[tuple[str, str], Any] = {}
    for code_identity in code_identities:
        if await _has_staged_code_inputs(
            session, state, code_identity, bindings
        ):
            await _materialize_provider_cells(session, projection_id, state)
            admitted_work_by_code[code_identity] = await _prepare_code_work(
                session, projection_id, code_identity, state
            )
        await asyncio.sleep(0)
    return admitted_work_by_code


async def _store_admitted_codes(
    session: Any,
    projection_id: str,
    bindings: list[BindingProjection],
    admitted_work_by_code: dict[tuple[str, str], Any],
    state: _BuildState,
) -> None:
    staged_provider_counts = (
        state.staged_provider_set_count,
        state.provider_membership_count,
        state.provider_cell_count,
        state.provider_fragment_byte_count,
    )
    for code_identity, admitted_work in admitted_work_by_code.items():
        if not await _has_staged_code_inputs(
            session, state, code_identity, bindings
        ):
            raise ValueError("pricing projection code admission changed")
        actual_work = await _stage_code_work(
            session,
            projection_id,
            code_identity,
            admitted_work.membership_probe_rows,
            admitted_work.member_cell_rows,
            admitted_work.profile_join_rows,
        )
        if actual_work != admitted_work or staged_provider_counts != (
            state.staged_provider_set_count,
            state.provider_membership_count,
            state.provider_cell_count,
            state.provider_fragment_byte_count,
        ):
            raise ValueError("pricing projection code admission changed")
        await _store_rate_occurrences(
            session, projection_id, code_identity, state
        )
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
    admitted_work_by_code = await _preflight_code_work(
        session, projection_id, code_identities, bindings, state
    )
    await _persist_provider_projection(session, projection_id, state)
    await _store_admitted_codes(
        session, projection_id, bindings, admitted_work_by_code, state
    )
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
        state.provider_state_count,
        state.rate_occurrence_count,
    )
