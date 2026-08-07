# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final source-evidence validation under the V4 build-owner fence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    _fail,
    _strict_int,
)
from process.ptg_parts.ptg2_tax_identity_source_validation import (
    _VALIDATION_BATCH_ROWS,
    _count_reduction_mismatches,
    _validate_tax_identity_source_projection_state,
)


async def _source_projection_relation_state(
    session: Any,
    *,
    schema: str,
) -> tuple[bool, bool, bool]:
    manifest_relation = f"{schema}.ptg2_provider_tax_identity_source_manifest"
    binding_relation = f"{schema}.ptg2_provider_tax_identity_source_binding"
    observation_relation = f"{schema}.ptg2_provider_group_tax_identity_source"
    manifest_oid, binding_oid, observation_oid = (
        await session.execute(
            db.text("""
                SELECT
                  to_regclass(:manifest_relation),
                  to_regclass(:binding_relation),
                  to_regclass(:observation_relation)
                """),
            {
                "manifest_relation": manifest_relation,
                "binding_relation": binding_relation,
                "observation_relation": observation_relation,
            },
        )
    ).one()
    return (
        manifest_oid is not None,
        binding_oid is not None,
        observation_oid is not None,
    )


async def validate_source_projection_absence(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> None:
    """Allow omitted source metadata only when no source evidence exists."""

    schema = _quote_ident(schema_name)
    relation_state = await _source_projection_relation_state(
        session,
        schema=schema,
    )
    if not any(relation_state):
        return
    if not all(relation_state):
        raise _fail()
    has_source_evidence = await session.scalar(
        db.text(f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_provider_tax_identity_source_manifest
                 WHERE snapshot_key = :snapshot_key
            ) OR EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_provider_tax_identity_source_binding
                 WHERE snapshot_key = :snapshot_key
            ) OR EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_provider_group_tax_identity_source
                 WHERE snapshot_key = :snapshot_key
            )
            """),
        {"snapshot_key": _strict_int(snapshot_key)},
    )
    if bool(has_source_evidence):
        raise _fail()


async def _next_stored_group_boundary(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    previous_group_id: bytes,
) -> bytes | None:
    group_rows = (
        await session.execute(
            db.text(f"""
                SELECT DISTINCT provider_group_global_id_128
                  FROM {schema}.ptg2_provider_group_tax_identity_source
                 WHERE snapshot_key = :snapshot_key
                   AND provider_group_global_id_128 > :previous_group_id
                 ORDER BY provider_group_global_id_128
                 LIMIT :batch_rows
                """),
            {
                "snapshot_key": _strict_int(snapshot_key),
                "previous_group_id": previous_group_id,
                "batch_rows": _VALIDATION_BATCH_ROWS,
            },
        )
    ).all()
    return bytes(group_rows[-1][0]) if group_rows else None


async def _validate_durable_source_reduction(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> None:
    source_group_count, merged_group_count = (
        await session.execute(
            db.text(f"""
                SELECT
                  (SELECT COUNT(DISTINCT provider_group_global_id_128)::bigint
                     FROM {schema}.ptg2_provider_group_tax_identity_source
                    WHERE snapshot_key = :snapshot_key),
                  (SELECT COUNT(*)::bigint
                     FROM {schema}.ptg2_provider_group_tax_identity
                    WHERE snapshot_key = :snapshot_key)
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one()
    if int(source_group_count or 0) != int(merged_group_count or 0):
        raise _fail()
    previous_group_id = b""
    while last_group_id := await _next_stored_group_boundary(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        previous_group_id=previous_group_id,
    ):
        if await _count_reduction_mismatches(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            previous_group_id=previous_group_id,
            last_group_id=last_group_id,
        ):
            raise _fail()
        previous_group_id = last_group_id


async def validate_building_tax_identity_source_projection(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    sealed_metadata: Mapping[str, Any],
    aggregate_metadata: Mapping[str, Any],
) -> TaxIdentitySourcePublication:
    """Revalidate source evidence while the caller holds the V4 build fence."""

    try:
        expected, _stored_bindings = (
            await _validate_tax_identity_source_projection_state(
                session,
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                sealed_metadata=sealed_metadata,
                aggregate_metadata=aggregate_metadata,
                require_sealed_layout=False,
            )
        )
        await _validate_durable_source_reduction(
            session,
            schema=_quote_ident(schema_name),
            snapshot_key=snapshot_key,
        )
        return expected
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "validate_building_tax_identity_source_projection",
    "validate_source_projection_absence",
]
