# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select the bounded release-scoped pricing prewarm shapes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from api.plan_pricing_projection_contract import (
    LEGACY_PROJECTION_CONTRACT,
    PROJECTION_CONTRACT,
    ZIP5,
    projection_code_identity,
    row_mapping,
    table,
)


MAX_PREWARM_SHAPES = 768
_BROAD_EM_CODE = re.compile(r"^992(?:0[2-9]|1[0-5])$")


@dataclass(frozen=True)
class PrewarmShape:
    code_system: str
    code: str
    geo_cell: str
    provider_count: int


def is_broad_em_shape(shape: PrewarmShape) -> bool:
    """Return whether a shape needs a directory scope not represented here."""

    return shape.code_system in {"CPT", "HCPCS"} and bool(
        _BROAD_EM_CODE.fullmatch(shape.code)
    )


def _shape_source(contract: str) -> tuple[str, str, str, str]:
    if contract == PROJECTION_CONTRACT:
        return "plan_pricing_prewarm_shape", ", shape_rank", "", "shape_rank"
    if contract == LEGACY_PROJECTION_CONTRACT:
        return (
            "plan_pricing_cell_aggregate",
            "",
            """
               AND NOT (
                   code_system IN ('CPT', 'HCPCS')
                   AND code ~ '^992(0[2-9]|1[0-5])$'
               )
            """,
            "provider_count DESC, code_system, code, geo_cell",
        )
    raise ValueError("plan-pricing prewarm projection contract is unsupported")


def _validated_shape(
    raw_shape_row: Any,
    *,
    projection_id: str,
    contract: str,
    expected_rank: int,
) -> PrewarmShape:
    shape_row_by_field = row_mapping(raw_shape_row)
    code_identity = projection_code_identity(
        shape_row_by_field.get("code_system"),
        shape_row_by_field.get("code"),
    )
    geo_cell = str(shape_row_by_field.get("geo_cell") or "")
    provider_count = shape_row_by_field.get("provider_count")
    if (
        str(shape_row_by_field.get("projection_id") or "") != projection_id
        or code_identity is None
        or not ZIP5.fullmatch(geo_cell)
        or type(provider_count) is not int
        or provider_count <= 0
        or (
            contract == PROJECTION_CONTRACT
            and shape_row_by_field.get("shape_rank") != expected_rank
        )
    ):
        raise ValueError("plan-pricing prewarm aggregate row is invalid")
    return PrewarmShape(*code_identity, geo_cell, provider_count)


async def select_shapes(
    session: Any,
    projection_id: str,
    contract: str = LEGACY_PROJECTION_CONTRACT,
) -> tuple[PrewarmShape, ...]:
    """Read the exact ranked shapes for a known projection contract."""

    table_name, rank_select_sql, filter_sql, order_sql = _shape_source(contract)
    shape_result = await session.execute(
        text(
            f"""
            SELECT projection_id, code_system, code, geo_cell, provider_count
                   {rank_select_sql}
              FROM {table(table_name)}
             WHERE projection_id = :projection_id {filter_sql}
          ORDER BY {order_sql}
             LIMIT {MAX_PREWARM_SHAPES}
            """
        ),
        {"projection_id": projection_id},
    )
    selected_shapes = (
        _validated_shape(
            raw_shape_row,
            projection_id=projection_id,
            contract=contract,
            expected_rank=expected_rank,
        )
        for expected_rank, raw_shape_row in enumerate(
            shape_result.mappings().all()[:MAX_PREWARM_SHAPES], start=1
        )
    )
    return tuple(
        shape for shape in selected_shapes if not is_broad_em_shape(shape)
    )
