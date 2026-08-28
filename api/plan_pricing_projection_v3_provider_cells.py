# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen provider-cell materialization for pricing projection v3."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import projection_provider_rows_for_npis
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches


PROVIDER_NPI_BATCH_SIZE = 5_000
MAX_PROVIDER_CELLS_PER_BATCH = 100_000
MAX_PROJECTION_PROVIDER_CELLS = 8_000_000
MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES = 16 * 1024 * 1024 * 1024


async def _next_provider_npis(session: Any, after_npi: int) -> list[int]:
    result = await session.execute(
        text(
            f"""
            SELECT npi
              FROM plan_pricing_provider_npi_pending_stage
             WHERE npi > :after_npi
             ORDER BY npi
             LIMIT {PROVIDER_NPI_BATCH_SIZE}
            """
        ),
        {"after_npi": after_npi},
    )
    return [int(npi) for npi in result.scalars().all()]


def _normalized_taxonomy_codes(
    provider_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            normalized_code
            for taxonomy_code in provider_by_field.get("taxonomy_codes") or ()
            if (normalized_code := str(taxonomy_code).strip().upper())
        )
    )


def _provider_fragment(
    provider_by_field: Mapping[str, Any],
    taxonomy_codes: tuple[str, ...] | None = None,
) -> bytes:
    if taxonomy_codes is None:
        taxonomy_codes = _normalized_taxonomy_codes(provider_by_field)
    classifications = list(provider_by_field.get("classifications") or ())
    return orjson.dumps(
        {
            "npi": int(provider_by_field["npi"]),
            "provider_name": provider_by_field.get("provider_name")
            or "TiC provider",
            "entity_type_code": provider_by_field.get("entity_type_code"),
            "credential": provider_by_field.get("credential"),
            "taxonomy_code": taxonomy_codes[0] if taxonomy_codes else None,
            "primary_specialty": provider_by_field.get("primary_specialty"),
            "classification": classifications[0] if classifications else None,
            "city": provider_by_field.get("city"),
            "state": provider_by_field.get("state"),
            "zip5": provider_by_field["zip5"],
        }
    )


def _provider_cell_rows(
    projection_id: str,
    state: _BuildState,
    npi_batch: list[int],
    providers_by_npi: Mapping[int, Iterable[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    if (
        set(providers_by_npi) - set(npi_batch)
        or sum(map(len, providers_by_npi.values()))
        > MAX_PROVIDER_CELLS_PER_BATCH
    ):
        raise ValueError("pricing projection provider-cell bound exceeded")
    provider_cell_rows: list[dict[str, Any]] = []
    for npi in npi_batch:
        for provider_by_field in providers_by_npi.get(npi, ()):
            taxonomy_codes = _normalized_taxonomy_codes(provider_by_field)
            fragment = _provider_fragment(provider_by_field, taxonomy_codes)
            geo_cell = str(provider_by_field["zip5"])
            semantic_fragment = orjson.dumps(
                (
                    fragment.decode("utf-8"),
                    provider_by_field.get("entity_type_code"),
                    taxonomy_codes,
                )
            )
            if (
                state.provider_cell_count >= MAX_PROJECTION_PROVIDER_CELLS
                or state.provider_fragment_byte_count + len(fragment)
                > MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES
            ):
                raise ValueError("pricing projection provider-cell bound exceeded")
            digest_row(
                state.content_digest,
                "provider-cell",
                (npi, geo_cell),
                semantic_fragment,
            )
            provider_cell_rows.append(
                {
                    "projection_id": projection_id,
                    "geo_cell": geo_cell,
                    "npi": npi,
                    "entity_type_code": provider_by_field.get(
                        "entity_type_code"
                    ),
                    "taxonomy_codes": list(taxonomy_codes),
                    "fragment": fragment,
                }
            )
            state.provider_cell_count += 1
            state.provider_fragment_byte_count += len(fragment)
    return provider_cell_rows


async def _materialize_provider_cells(
    session: Any,
    projection_id: str,
    state: _BuildState,
    *,
    next_provider_npis: Any = _next_provider_npis,
    provider_rows_for_npis: Any = projection_provider_rows_for_npis,
    provider_cell_rows: Any = _provider_cell_rows,
    insert_batches: Any = _insert_batches,
) -> None:
    after_npi = 0
    while True:
        npi_batch = await next_provider_npis(session, after_npi)
        if not npi_batch:
            break
        providers_by_npi = await provider_rows_for_npis(session, npi_batch)
        cell_rows = provider_cell_rows(
            projection_id, state, npi_batch, providers_by_npi
        )
        await insert_batches(
            session,
            f"""
            INSERT INTO {table('plan_pricing_provider_cell')} (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment
            ) VALUES (
                :projection_id, :geo_cell, :npi, :entity_type_code,
                :taxonomy_codes, :fragment
            )
            """,
            cell_rows,
        )
        await session.execute(
            text(
                """
                INSERT INTO plan_pricing_provider_npi_materialized_stage (npi)
                SELECT UNNEST(CAST(:npis AS bigint[]))
                ON CONFLICT DO NOTHING
                """
            ),
            {"npis": npi_batch},
        )
        await session.execute(
            text(
                """
                DELETE FROM plan_pricing_provider_npi_pending_stage
                 WHERE npi = ANY(CAST(:npis AS bigint[]))
                """
            ),
            {"npis": npi_batch},
        )
        after_npi = npi_batch[-1]
