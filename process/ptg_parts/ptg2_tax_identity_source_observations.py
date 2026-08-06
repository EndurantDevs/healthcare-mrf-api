# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish bounded ranges of source-local tax-identity observations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from db.connection import db
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    _fail,
    _strict_int,
)

_OBSERVATION_BATCH_ROWS = 10_000


def _range_predicate(table_alias: str) -> str:
    return f"""
        ({table_alias}.source_key > :previous_source_key OR
         ({table_alias}.source_key = :previous_source_key AND
          {table_alias}.source_record_ordinal > :previous_ordinal))
        AND
        ({table_alias}.source_key < :last_source_key OR
         ({table_alias}.source_key = :last_source_key AND
          {table_alias}.source_record_ordinal <= :last_ordinal))
    """


async def _observation_boundary(
    session: Any,
    *,
    stage: str,
    previous_source_key: int,
    previous_ordinal: int,
) -> tuple[int, int, int] | None:
    observation_rows = (
        await session.execute(
            db.text(f"""
                SELECT source_key, source_record_ordinal
                  FROM {stage}
                 WHERE source_key > :previous_source_key
                    OR (source_key = :previous_source_key
                        AND source_record_ordinal > :previous_ordinal)
                 ORDER BY source_key, source_record_ordinal
                 LIMIT :batch_rows
                """),
            {
                "previous_source_key": previous_source_key,
                "previous_ordinal": previous_ordinal,
                "batch_rows": _OBSERVATION_BATCH_ROWS,
            },
        )
    ).all()
    if not observation_rows:
        return None
    return (
        len(observation_rows),
        int(observation_rows[-1][0]),
        int(observation_rows[-1][1]),
    )


async def _count_unresolved_identities(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    range_parameters_by_name: Mapping[str, int],
) -> int:
    unresolved_count = await session.scalar(
        db.text(f"""
            SELECT COUNT(*)::bigint
              FROM {stage} AS staged
              LEFT JOIN {schema}.ptg2_provider_tax_identity AS identity
                ON identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = staged.tin_id_128
               AND identity.tin_hmac_sha256 = staged.tin_hmac_sha256
             WHERE {_range_predicate("staged")}
               AND staged.tax_identity_state = 'matched_ein'
               AND identity.tin_key IS NULL
            """),
        {
            "snapshot_key": _strict_int(snapshot_key),
            **dict(range_parameters_by_name),
        },
    )
    return int(unresolved_count or 0)


async def _insert_observation_range(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    range_parameters_by_name: Mapping[str, int],
) -> None:
    await session.execute(
        db.text(f"""
            INSERT INTO {schema}.ptg2_provider_group_tax_identity_source
                (snapshot_key, source_key, provider_group_global_id_128,
                 source_record_ordinal, tax_identity_state, tin_key)
            SELECT :snapshot_key, staged.source_key,
                   staged.provider_group_global_id_128,
                   staged.source_record_ordinal, staged.tax_identity_state,
                   identity.tin_key
              FROM {stage} AS staged
              LEFT JOIN {schema}.ptg2_provider_tax_identity AS identity
                ON identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = staged.tin_id_128
               AND identity.tin_hmac_sha256 = staged.tin_hmac_sha256
             WHERE {_range_predicate("staged")}
            ON CONFLICT DO NOTHING
            """),
        {
            "snapshot_key": _strict_int(snapshot_key),
            **dict(range_parameters_by_name),
        },
    )


async def _count_matching_observations(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    range_parameters_by_name: Mapping[str, int],
) -> int:
    matching_count = await session.scalar(
        db.text(f"""
            SELECT COUNT(*)::bigint
              FROM {stage} AS staged
              JOIN {schema}.ptg2_provider_group_tax_identity_source AS stored
                ON stored.snapshot_key = :snapshot_key
               AND stored.source_key = staged.source_key
               AND stored.provider_group_global_id_128 =
                       staged.provider_group_global_id_128
               AND stored.source_record_ordinal = staged.source_record_ordinal
               AND stored.tax_identity_state = staged.tax_identity_state
              LEFT JOIN {schema}.ptg2_provider_tax_identity AS identity
                ON identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = staged.tin_id_128
               AND identity.tin_hmac_sha256 = staged.tin_hmac_sha256
             WHERE {_range_predicate("staged")}
               AND stored.tin_key IS NOT DISTINCT FROM identity.tin_key
            """),
        {
            "snapshot_key": _strict_int(snapshot_key),
            **dict(range_parameters_by_name),
        },
    )
    return int(matching_count or 0)


async def _count_witness_mismatches(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    range_parameters_by_name: Mapping[str, int],
) -> int:
    mismatch_count = await session.scalar(
        db.text(f"""
            SELECT COUNT(*)::bigint
              FROM {stage} AS staged
              LEFT JOIN {schema}.ptg2_provider_group_tax_identity AS merged
                ON merged.snapshot_key = :snapshot_key
               AND merged.provider_group_global_id_128 =
                       staged.provider_group_global_id_128
              LEFT JOIN {schema}.ptg2_provider_tax_identity AS identity
                ON identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = staged.tin_id_128
               AND identity.tin_hmac_sha256 = staged.tin_hmac_sha256
             WHERE {_range_predicate("staged")}
               AND (
                    merged.snapshot_key IS NULL
                    OR (get_byte(merged.source_bitmap,
                                 staged.source_ordinal / 8)
                        & (1 << (staged.source_ordinal % 8))) = 0
                    OR (
                        staged.tax_identity_state = 'matched_ein'
                        AND (
                            merged.tax_identity_state <> 'matched_ein'
                            OR merged.tin_key IS DISTINCT FROM identity.tin_key
                        )
                    )
               )
            """),
        {
            "snapshot_key": _strict_int(snapshot_key),
            **dict(range_parameters_by_name),
        },
    )
    return int(mismatch_count or 0)


async def _publish_observation_batch(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    range_parameters_by_name: Mapping[str, int],
    expected_count: int,
) -> None:
    """Publish and prove one bounded source-order observation range."""

    if await _count_unresolved_identities(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        range_parameters_by_name=range_parameters_by_name,
    ):
        raise _fail()
    await _insert_observation_range(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        range_parameters_by_name=range_parameters_by_name,
    )
    matching_count = await _count_matching_observations(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        range_parameters_by_name=range_parameters_by_name,
    )
    mismatch_count = await _count_witness_mismatches(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        range_parameters_by_name=range_parameters_by_name,
    )
    if matching_count != expected_count or mismatch_count:
        raise _fail()


async def _publish_observations(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Publish all observations through bounded source-order ranges."""

    previous_source_key = -1
    previous_ordinal = -1
    published_count = 0
    while boundary := await _observation_boundary(
        session,
        stage=stage,
        previous_source_key=previous_source_key,
        previous_ordinal=previous_ordinal,
    ):
        batch_count, last_source_key, last_ordinal = boundary
        range_parameters_by_name = {
            "previous_source_key": previous_source_key,
            "previous_ordinal": previous_ordinal,
            "last_source_key": last_source_key,
            "last_ordinal": last_ordinal,
        }
        await _publish_observation_batch(
            session,
            schema=schema,
            stage=stage,
            snapshot_key=snapshot_key,
            range_parameters_by_name=range_parameters_by_name,
            expected_count=batch_count,
        )
        published_count += batch_count
        previous_source_key = last_source_key
        previous_ordinal = last_ordinal
        if heartbeat_callback is not None:
            heartbeat_callback()
    if published_count != prepared.provider_group_occurrence_count:
        raise _fail()


__all__ = ["_publish_observations"]
