# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate staged and target state before source-local publication."""

from __future__ import annotations

import hmac
import re
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_artifact import (
    _ProjectionInputs,
    _STATE_CODE,
    _projection_content_digest,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    _fail,
    _strict_int,
)
from process.ptg_parts.ptg2_tax_identity_source_stage import (
    _STAGE_GUARD_FUNCTION,
    StagedTaxIdentitySourceProjection,
)

_STAGE_NAME = re.compile(r"ptg2_tax_source_stage_[0-9a-f]{20}\Z", re.ASCII)
_SEAL_TOKEN = re.compile(r"[0-9a-f]{32}\Z", re.ASCII)
_STAGE_VALIDATION_BATCH_ROWS = 10_000
_STATE_NAMES = (
    "matched_ein",
    "missing",
    "malformed",
    "unsupported_type",
)


def _validated_stage_handle(
    staged: object,
) -> StagedTaxIdentitySourceProjection:
    if (
        type(staged) is not StagedTaxIdentitySourceProjection
        or _STAGE_NAME.fullmatch(staged.table_name) is None
        or staged.seal_table_name != f"{staged.table_name}_seal"
        or _SEAL_TOKEN.fullmatch(staged.seal_token) is None
        or not staged.table_name.endswith(staged.seal_token[:20])
        or type(staged.stage_oid) is not int
        or staged.stage_oid <= 0
        or type(staged.seal_oid) is not int
        or staged.seal_oid <= 0
        or staged.stage_oid == staged.seal_oid
    ):
        raise _fail()
    return staged


async def _current_temp_relation_oids(
    session: Any,
    staged: StagedTaxIdentitySourceProjection,
) -> dict[str, int]:
    relation_rows = (
        await session.execute(
            db.text("""
                SELECT relation.relname, relation.oid::bigint
                  FROM pg_class AS relation
                 WHERE relation.relnamespace = pg_my_temp_schema()
                   AND relation.relname IN (:stage_table, :seal_table)
                   AND relation.relkind = 'r'
                   AND relation.relpersistence = 't'
                """),
            {
                "stage_table": staged.table_name,
                "seal_table": staged.seal_table_name,
            },
        )
    ).all()
    return {str(name): int(relation_oid) for name, relation_oid in relation_rows}


async def _sealed_stage_values(
    session: Any,
    *,
    staged: StagedTaxIdentitySourceProjection,
) -> tuple[object, ...]:
    quoted_seal = f'{_quote_ident("pg_temp")}.{_quote_ident(staged.seal_table_name)}'
    seal_values = (
        await session.execute(
            db.text(f"""
                SELECT stage_oid::bigint, seal_oid::bigint,
                       stage_table, copy_sha256,
                       copy_byte_count, content_digest,
                       source_ordinal_map_digest,
                       binding_vector_digest,
                       aggregate_tax_content_digest, source_count,
                       provider_group_occurrence_count, provider_group_count,
                       (SELECT COUNT(*)::bigint FROM {quoted_seal})
                  FROM {quoted_seal}
                 WHERE seal_token = :seal_token
                """),
            {"seal_token": staged.seal_token},
        )
    ).one_or_none()
    if seal_values is None:
        raise _fail()
    return tuple(seal_values)


async def _stage_guard_records(
    session: Any,
    *,
    staged: StagedTaxIdentitySourceProjection,
) -> tuple[tuple[object, ...], ...]:
    guard_records = (
        await session.execute(
            db.text("""
                SELECT guarded.relname, trigger.tgname,
                       trigger.tgenabled::text,
                       trigger.tgtype::integer, guard.proname,
                       guard.pronamespace = pg_my_temp_schema()
                  FROM pg_trigger AS trigger
                  JOIN pg_class AS guarded
                    ON guarded.oid = trigger.tgrelid
                  JOIN pg_proc AS guard
                    ON guard.oid = trigger.tgfoid
                 WHERE guarded.relnamespace = pg_my_temp_schema()
                   AND guarded.relname IN (:stage_table, :seal_table)
                   AND NOT trigger.tgisinternal
                 ORDER BY guarded.relname
                """),
            {
                "stage_table": staged.table_name,
                "seal_table": staged.seal_table_name,
            },
        )
    ).all()
    return tuple(tuple(record) for record in guard_records)


async def _validate_stage_guards(
    session: Any,
    *,
    staged: StagedTaxIdentitySourceProjection,
) -> None:
    expected_records = tuple(
        (
            relation_name,
            f"{relation_name}_guard",
            "A",
            62,
            _STAGE_GUARD_FUNCTION,
            True,
        )
        for relation_name in sorted((staged.table_name, staged.seal_table_name))
    )
    if await _stage_guard_records(session, staged=staged) != expected_records:
        raise _fail()


def _hash_staged_record(
    content_digest: Any,
    staged_record: Any,
) -> tuple[int, int, int, str]:
    try:
        source_key = int(staged_record[0])
        source_ordinal = int(staged_record[1])
        source_record_ordinal = int(staged_record[2])
        provider_group_id = bytes(staged_record[3])
        identity_state = str(staged_record[4])
        full_hmac = staged_record[5]
        if (
            isinstance(staged_record[0], bool)
            or isinstance(staged_record[1], bool)
            or isinstance(staged_record[2], bool)
            or source_key < 0
            or source_ordinal < 0
            or source_record_ordinal < 0
            or len(provider_group_id) != 16
            or identity_state not in _STATE_CODE
            or identity_state == "matched_ein"
            and (full_hmac is None or len(bytes(full_hmac)) != 32)
            or identity_state != "matched_ein"
            and full_hmac is not None
        ):
            raise _fail()
        content_digest.update(source_key.to_bytes(4, "big"))
        content_digest.update(source_record_ordinal.to_bytes(8, "big"))
        content_digest.update(provider_group_id)
        content_digest.update(bytes((_STATE_CODE[identity_state],)))
        content_digest.update(
            bytes(full_hmac) if identity_state == "matched_ein" else bytes(32)
        )
        return source_key, source_ordinal, source_record_ordinal, identity_state
    except Exception:
        raise _fail() from None


async def _validate_stage_content_digest(
    session: Any,
    *,
    quoted_stage: str,
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Recompute exact staged evidence through bounded keyset batches."""

    digest = _projection_content_digest(
        _ProjectionInputs(
            policy_id=prepared.token_policy_id,
            policy_descriptor=prepared.token_policy_descriptor_sha256,
            ordinal_digest=prepared.source_ordinal_map_digest,
            aggregate_digest=prepared.aggregate_tax_content_digest,
            bindings=prepared.bindings,
        )
    )
    observed_record_count = 0
    previous_source_key = -1
    previous_record_ordinal = -1
    source_counts = [
        {state_name: 0 for state_name in _STATE_NAMES} for _binding in prepared.bindings
    ]
    while True:
        staged_records = await _next_stage_record_batch(
            session,
            quoted_stage=quoted_stage,
            previous_source_key=previous_source_key,
            previous_record_ordinal=previous_record_ordinal,
        )
        if not staged_records:
            break
        batch_count, previous_source_key, previous_record_ordinal = (
            _consume_stage_record_batch(
                digest,
                staged_records=staged_records,
                prepared=prepared,
                source_counts=source_counts,
            )
        )
        observed_record_count += batch_count
    if (
        observed_record_count != prepared.provider_group_occurrence_count
        or _observed_source_counts(source_counts) != _expected_source_counts(prepared)
        or not hmac.compare_digest(digest.digest(), prepared.content_digest)
    ):
        raise _fail()


async def _next_stage_record_batch(
    session: Any,
    *,
    quoted_stage: str,
    previous_source_key: int,
    previous_record_ordinal: int,
) -> tuple[Any, ...]:
    return tuple(
        (
            await session.execute(
                db.text(f"""
                    SELECT source_key, source_ordinal, source_record_ordinal,
                           provider_group_global_id_128, tax_identity_state,
                           tin_hmac_sha256
                      FROM {quoted_stage}
                     WHERE source_key > :previous_source_key
                        OR (source_key = :previous_source_key AND
                            source_record_ordinal > :previous_record_ordinal)
                     ORDER BY source_key, source_record_ordinal
                     LIMIT :batch_rows
                    """),
                {
                    "previous_source_key": previous_source_key,
                    "previous_record_ordinal": previous_record_ordinal,
                    "batch_rows": _STAGE_VALIDATION_BATCH_ROWS,
                },
            )
        ).all()
    )


def _consume_stage_record_batch(
    digest: Any,
    *,
    staged_records: tuple[Any, ...],
    prepared: PreparedTaxIdentitySourceProjection,
    source_counts: list[dict[str, int]],
) -> tuple[int, int, int]:
    source_key = -1
    source_record_ordinal = -1
    for staged_record in staged_records:
        source_key, source_ordinal, source_record_ordinal, identity_state = (
            _hash_staged_record(digest, staged_record)
        )
        if source_key >= len(prepared.bindings):
            raise _fail()
        binding = prepared.bindings[source_key]
        if (
            source_key != binding.source_key
            or source_ordinal != binding.source_ordinal
            or source_record_ordinal != sum(source_counts[source_key].values())
        ):
            raise _fail()
        source_counts[source_key][identity_state] += 1
    return len(staged_records), source_key, source_record_ordinal


def _expected_source_counts(
    prepared: PreparedTaxIdentitySourceProjection,
) -> tuple[tuple[int, ...], ...]:
    return tuple(
        tuple(getattr(binding, f"{state_name}_count") for state_name in _STATE_NAMES)
        for binding in prepared.bindings
    )


def _observed_source_counts(
    source_counts: list[dict[str, int]],
) -> tuple[tuple[int, ...], ...]:
    return tuple(
        tuple(counts_by_state[state_name] for state_name in _STATE_NAMES)
        for counts_by_state in source_counts
    )


async def validate_staged_tax_identity_source_projection(
    session: Any,
    *,
    staged: object,
    prepared: PreparedTaxIdentitySourceProjection,
) -> tuple[str, int]:
    """Validate the exact backend-local stage, seal, and source attribution."""

    handle = _validated_stage_handle(staged)
    relation_oids = await _current_temp_relation_oids(session, handle)
    if relation_oids != {
        handle.table_name: handle.stage_oid,
        handle.seal_table_name: handle.seal_oid,
    }:
        raise _fail()
    await _validate_stage_guards(session, staged=handle)
    seal_values = await _sealed_stage_values(session, staged=handle)
    expected_seal_values = (
        handle.stage_oid,
        handle.seal_oid,
        handle.table_name,
        prepared.copy_sha256,
        prepared.copy_byte_count,
        prepared.content_digest,
        prepared.source_ordinal_map_digest,
        prepared.binding_vector_digest,
        prepared.aggregate_tax_content_digest,
        prepared.source_count,
        prepared.provider_group_occurrence_count,
    )
    if seal_values[:11] != expected_seal_values or int(seal_values[12]) != 1:
        raise _fail()
    quoted_stage = f'{_quote_ident("pg_temp")}.{_quote_ident(handle.table_name)}'
    await _validate_stage_content_digest(
        session,
        quoted_stage=quoted_stage,
        prepared=prepared,
    )
    provider_group_count = await session.scalar(db.text(f"""
        SELECT COUNT(DISTINCT provider_group_global_id_128)::bigint
          FROM {quoted_stage}
        """))
    normalized_group_count = int(provider_group_count or 0)
    if normalized_group_count != int(seal_values[11]):
        raise _fail()
    await _validate_stage_guards(session, staged=handle)
    return quoted_stage, normalized_group_count


__all__ = ["validate_staged_tax_identity_source_projection"]
