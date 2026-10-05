# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""COPY source-local observations into an isolated snapshot candidate."""
from __future__ import annotations

import tempfile
from collections.abc import Callable
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_snapshot_candidates import (
    COPY_MAX_BYTES, COPY_MAX_ROWS, begin_snapshot_candidate, candidate_driver,
    finish_snapshot_candidate, snapshot_candidate_relation,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection, _fail,
)


async def _observation_boundary(session, stage, cursor):
    """Bound binary COPY by both source-order rows and exact encoded bytes."""
    records = (await session.execute(db.text(f"""
        SELECT source_key,source_record_ordinal,
               2+4*6+8+4+octet_length(provider_group_global_id_128)+8+
               octet_length(tax_identity_state)+4 AS byte_count
          FROM {stage}
         WHERE (source_key,source_record_ordinal)>(:source,:ordinal)
         ORDER BY source_key,source_record_ordinal LIMIT :rows
    """), {"source": cursor[0], "ordinal": cursor[1], "rows": COPY_MAX_ROWS})).all()
    byte_count, count, boundary = 21, 0, None
    for source, ordinal, size in records:
        if byte_count+size>COPY_MAX_BYTES:
            break
        byte_count += size
        count += 1
        boundary = (int(source),int(ordinal))
    if records and not count:
        raise _fail()
    return boundary,count


async def _copy_observation_batch(session, *, schema_name, stage, candidate, snapshot_key, bounds):
    """Resolve the indexed dictionary and transfer one bounded native buffer."""
    cursor,boundary,count=bounds
    identity=snapshot_candidate_relation(session,_quote_ident(schema_name),"ptg2_provider_tax_identity")
    driver=await candidate_driver(session)
    with tempfile.TemporaryFile() as payload:
        copied=await driver.copy_from_query(f"""
            SELECT $1::bigint,staged.source_key,staged.provider_group_global_id_128,
                   staged.source_record_ordinal,staged.tax_identity_state,identity.tin_key
              FROM {stage} staged LEFT JOIN {identity} identity
                ON identity.snapshot_key=$1 AND identity.tin_id_128=staged.tin_id_128
               AND identity.tin_hmac_sha256=staged.tin_hmac_sha256
             WHERE (staged.source_key,staged.source_record_ordinal)>($2::integer,$3::bigint)
               AND (staged.source_key,staged.source_record_ordinal)<=($4::integer,$5::bigint)
             ORDER BY staged.source_key,staged.source_record_ordinal
        """, snapshot_key,*cursor,*boundary,output=payload,format="binary")
        if copied!=f"COPY {count}" or payload.tell()>COPY_MAX_BYTES:
            raise _fail()
        payload.seek(0)
        copied=await driver.copy_to_table(candidate,schema_name=schema_name,
            columns=("snapshot_key","source_key","provider_group_global_id_128",
                     "source_record_ordinal","tax_identity_state","tin_key"),
            source=payload,format="binary")
        if copied!=f"COPY {count}":
            raise _fail()


async def _count_witness_mismatches(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
) -> int:
    mismatch_count = await session.scalar(
        db.text(f"""
            SELECT COUNT(*)::bigint
              FROM {stage} AS staged
              LEFT JOIN {snapshot_candidate_relation(session, schema, "ptg2_provider_group_tax_identity")} AS merged
                ON merged.snapshot_key = :snapshot_key
               AND merged.provider_group_global_id_128 =
                       staged.provider_group_global_id_128
              LEFT JOIN {snapshot_candidate_relation(session, schema, "ptg2_provider_tax_identity")} AS identity
                ON identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = staged.tin_id_128
               AND identity.tin_hmac_sha256 = staged.tin_hmac_sha256
             WHERE TRUE
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
            "snapshot_key": int(snapshot_key),
        },
    )
    return int(mismatch_count or 0)



async def _publish_observations(
    session: Any, *, schema: str, stage: str, snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Load all bounded buffers before indexes, set checks and aggregate accounting."""
    schema_name=schema[1:-1].replace('""','"')
    build_token=await session.scalar(db.text(f"SELECT build_token FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key=:snapshot"), {"snapshot":snapshot_key})
    candidate=await begin_snapshot_candidate(session,schema_name,"ptg2_provider_group_tax_identity_source",snapshot_key,build_token)
    cursor,total=(-1,-1),0
    while True:
        boundary,count=await _observation_boundary(session,stage,cursor)
        if boundary is None:
            break
        await _copy_observation_batch(session,schema_name=schema_name,stage=stage,candidate=candidate,
            snapshot_key=snapshot_key,bounds=(cursor,boundary,count))
        total+=count
        cursor=boundary
        if heartbeat_callback is not None:
            heartbeat_callback()
    if total!=prepared.provider_group_occurrence_count:
        raise _fail()
    await finish_snapshot_candidate(session,schema_name,candidate,total)
    if await _count_witness_mismatches(session,schema=schema,stage=stage,snapshot_key=snapshot_key):
        raise _fail()


__all__ = ["_publish_observations"]
