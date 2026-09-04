# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable provider children retained by plan-pricing projection v4."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_v3_types import _BuildState


async def _copy_memberships(session: Any, projection_id: str) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_provider_membership')} (
                projection_id, binding_ordinal, provider_set_key, npi
            )
            SELECT :projection_id, binding_ordinal, provider_set_key, npi
              FROM plan_pricing_provider_member_stage
             ORDER BY binding_ordinal, provider_set_key, npi
            """
        ),
        {"projection_id": projection_id},
    )


async def _copy_provider_cells(session: Any, projection_id: str) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_provider_cell')} (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment
            )
            SELECT projection_id, geo_cell, npi, entity_type_code,
                   taxonomy_codes, fragment
              FROM plan_pricing_provider_cell_stage
             WHERE projection_id = :projection_id
             ORDER BY geo_cell, npi
            """
        ),
        {"projection_id": projection_id},
    )


async def _copy_provider_states(session: Any, projection_id: str) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_provider_state')} (
                projection_id, state, npi, provider_fragment
            )
            SELECT :projection_id,
                   upper(
                       convert_from(state_fragment, 'UTF8')::jsonb
                       -> 'provider' ->> 'state'
                   ),
                   npi,
                   state_fragment
              FROM plan_pricing_provider_cell_stage
             WHERE projection_id = :projection_id
               AND state_fragment IS NOT NULL
               AND upper(
                       convert_from(state_fragment, 'UTF8')::jsonb
                       -> 'provider' ->> 'state'
                   )
                   ~ '^[A-Z]{{2}}$'
             ORDER BY 2, 3
            """
        ),
        {"projection_id": projection_id},
    )


async def persist_provider_projection(
    session: Any,
    projection_id: str,
    state: _BuildState | None = None,
) -> None:
    """Copy fully admitted provider stages into the immutable projection."""

    await _copy_memberships(session, projection_id)
    await _copy_provider_cells(session, projection_id)
    await _copy_provider_states(session, projection_id)


__all__ = ["persist_provider_projection"]
