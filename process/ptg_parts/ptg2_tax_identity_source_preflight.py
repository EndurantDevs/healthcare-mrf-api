# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate staged rows before source-local publication."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_artifact import _STATE_CODE
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    _fail,
)
from process.ptg_parts.ptg2_tax_identity_source_stage import (
    StagedTaxIdentitySourceProjection,
)

_STAGE_VALIDATION_BATCH_ROWS = 10_000
_STATE_NAMES = (
    "matched_ein",
    "missing",
    "malformed",
    "unsupported_type",
)


def _validated_staged_record(staged_record: Any) -> tuple[int, int, int, str]:
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
        return source_key, source_ordinal, source_record_ordinal, identity_state
    except Exception:
        raise _fail() from None


async def _validate_stage_records(
    session: Any,
    *,
    quoted_stage: str,
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Check staged row shape, source attribution and counts in keyset batches."""

    observed_record_count = 0
    previous_source_key = -1
    previous_record_ordinal = -1
    source_counts = [{state_name: 0 for state_name in _STATE_NAMES} for _binding in prepared.bindings]
    while True:
        staged_records = await _next_stage_record_batch(
            session,
            quoted_stage=quoted_stage,
            previous_source_key=previous_source_key,
            previous_record_ordinal=previous_record_ordinal,
        )
        if not staged_records:
            break
        batch_count, previous_source_key, previous_record_ordinal = _consume_stage_record_batch(
            staged_records=staged_records,
            prepared=prepared,
            source_counts=source_counts,
        )
        observed_record_count += batch_count
    if observed_record_count != prepared.provider_group_occurrence_count or (
        _observed_source_counts(source_counts) != _expected_source_counts(prepared)
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
    *,
    staged_records: tuple[Any, ...],
    prepared: PreparedTaxIdentitySourceProjection,
    source_counts: list[dict[str, int]],
) -> tuple[int, int, int]:
    source_key = -1
    source_record_ordinal = -1
    for staged_record in staged_records:
        source_key, source_ordinal, source_record_ordinal, identity_state = _validated_staged_record(staged_record)
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
        tuple(getattr(binding, f"{state_name}_count") for state_name in _STATE_NAMES) for binding in prepared.bindings
    )


def _observed_source_counts(
    source_counts: list[dict[str, int]],
) -> tuple[tuple[int, ...], ...]:
    return tuple(tuple(counts_by_state[state_name] for state_name in _STATE_NAMES) for counts_by_state in source_counts)


async def validate_staged_tax_identity_source_projection(
    session: Any,
    *,
    staged: StagedTaxIdentitySourceProjection,
    prepared: PreparedTaxIdentitySourceProjection,
) -> tuple[str, int]:
    """Validate staged rows against the prepared source bindings."""

    quoted_stage = f"{_quote_ident('pg_temp')}.{_quote_ident(staged.table_name)}"
    await _validate_stage_records(
        session,
        quoted_stage=quoted_stage,
        prepared=prepared,
    )
    return quoted_stage, staged.provider_group_count


__all__ = ["validate_staged_tax_identity_source_projection"]
