# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate durable and reused source-local tax-identity projections."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_aggregate_reuse import (
    validate_reused_tax_identity_aggregate_manifest,
)
from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT,
)
from process.ptg_parts.ptg2_tax_identity_source_persisted import (
    SOURCE_BINDING_FIELDS,
    load_source_bindings,
    validate_source_binding_seal,
    validate_source_observation_counts,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    _fail,
    _strict_int,
    _strict_policy,
    _strict_sha256,
)

_VALIDATION_BATCH_ROWS = 10_000


async def validate_stored_tax_identity_source_counts(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Require exact durable state totals and aggregate group coverage."""

    stored_counts = (
        await session.execute(
            db.text(f"""
                SELECT COUNT(*)::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'matched_ein'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'missing'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'malformed'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'unsupported_type'
                       )::bigint
                  FROM {schema}.ptg2_provider_group_tax_identity_source
                 WHERE snapshot_key = :snapshot_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one()
    expected_counts = (
        prepared.provider_group_occurrence_count,
        prepared.matched_ein_count,
        prepared.missing_count,
        prepared.malformed_count,
        prepared.unsupported_type_count,
    )
    if tuple(int(stored_count) for stored_count in stored_counts) != expected_counts:
        raise _fail()
    distinct_stage_groups = await session.scalar(
        db.text(
            f"SELECT COUNT(DISTINCT provider_group_global_id_128)::bigint "
            f"FROM {stage}"
        )
    )
    merged_group_count = await session.scalar(
        db.text(f"""
            SELECT COUNT(*)::bigint
              FROM {schema}.ptg2_provider_group_tax_identity
             WHERE snapshot_key = :snapshot_key
            """),
        {"snapshot_key": _strict_int(snapshot_key)},
    )
    if int(distinct_stage_groups or 0) != int(merged_group_count or 0):
        raise _fail()


async def _next_group_boundary(
    session: Any,
    *,
    stage: str,
    previous_group_id: bytes,
) -> bytes | None:
    group_rows = (
        await session.execute(
            db.text(f"""
                SELECT DISTINCT provider_group_global_id_128
                  FROM {stage}
                 WHERE provider_group_global_id_128 > :previous_group_id
                 ORDER BY provider_group_global_id_128
                 LIMIT :batch_rows
                """),
            {
                "previous_group_id": previous_group_id,
                "batch_rows": _VALIDATION_BATCH_ROWS,
            },
        )
    ).all()
    return bytes(group_rows[-1][0]) if group_rows else None


async def _count_reduction_mismatches(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    previous_group_id: bytes,
    last_group_id: bytes,
) -> int:
    mismatch_count = await session.scalar(
        db.text(f"""
            WITH local AS (
                SELECT stored.provider_group_global_id_128 AS group_id,
                       COUNT(*)::bigint AS occurrence_count,
                       MAX(CASE stored.tax_identity_state
                           WHEN 'matched_ein' THEN 4
                           WHEN 'unsupported_type' THEN 3
                           WHEN 'malformed' THEN 2
                           ELSE 1 END) AS state_priority,
                       MIN(stored.tin_key) FILTER (
                           WHERE stored.tax_identity_state = 'matched_ein'
                       ) AS minimum_tin_key,
                       MAX(stored.tin_key) FILTER (
                           WHERE stored.tax_identity_state = 'matched_ein'
                       ) AS maximum_tin_key
                  FROM {schema}.ptg2_provider_group_tax_identity_source AS stored
                 WHERE stored.snapshot_key = :snapshot_key
                   AND stored.provider_group_global_id_128 > :previous_group_id
                   AND stored.provider_group_global_id_128 <= :last_group_id
                 GROUP BY stored.provider_group_global_id_128
            )
            SELECT COUNT(*)::bigint
              FROM local
              LEFT JOIN {schema}.ptg2_provider_group_tax_identity AS merged
                ON merged.snapshot_key = :snapshot_key
               AND merged.provider_group_global_id_128 = local.group_id
             WHERE merged.snapshot_key IS NULL
                OR bit_count(merged.source_bitmap) <> local.occurrence_count
                OR merged.tax_identity_state <>
                   CASE local.state_priority
                     WHEN 4 THEN 'matched_ein'
                     WHEN 3 THEN 'unsupported_type'
                     WHEN 2 THEN 'malformed'
                     ELSE 'missing'
                   END
                OR local.minimum_tin_key IS DISTINCT FROM local.maximum_tin_key
                OR merged.tin_key IS DISTINCT FROM
                   CASE WHEN local.state_priority = 4
                        THEN local.minimum_tin_key ELSE NULL END
            """),
        {
            "snapshot_key": _strict_int(snapshot_key),
            "previous_group_id": previous_group_id,
            "last_group_id": last_group_id,
        },
    )
    return int(mismatch_count or 0)


async def validate_merged_tax_identity_source_reduction(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Recompute each merged group from exact source-local observations."""

    previous_group_id = b""
    while last_group_id := await _next_group_boundary(
        session,
        stage=stage,
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
        if heartbeat_callback is not None:
            heartbeat_callback()


def _publication_from_metadata(
    metadata_by_field: Mapping[str, Any],
) -> TaxIdentitySourcePublication:
    try:
        if (
            metadata_by_field.get("contract") != PTG2_TAX_IDENTITY_SOURCE_CONTRACT
            or metadata_by_field.get("content_contract")
            != PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT
            or metadata_by_field.get("binding_contract")
            != PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT
            or metadata_by_field.get("binding_vector_contract")
            != PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT
        ):
            raise _fail()
        return TaxIdentitySourcePublication(
            token_policy_id=_strict_policy(metadata_by_field.get("token_policy_id")),
            token_policy_descriptor_sha256=bytes.fromhex(
                _strict_sha256(metadata_by_field.get("token_policy_descriptor_sha256"))
            ),
            source_ordinal_map_digest=bytes.fromhex(
                _strict_sha256(metadata_by_field.get("source_ordinal_map_digest"))
            ),
            source_count=_strict_int(metadata_by_field.get("source_count"), minimum=1),
            provider_group_occurrence_count=_strict_int(
                metadata_by_field.get("provider_group_occurrence_count")
            ),
            matched_ein_count=_strict_int(metadata_by_field.get("matched_ein_count")),
            missing_count=_strict_int(metadata_by_field.get("missing_count")),
            malformed_count=_strict_int(metadata_by_field.get("malformed_count")),
            unsupported_type_count=_strict_int(
                metadata_by_field.get("unsupported_type_count")
            ),
            content_digest=bytes.fromhex(
                _strict_sha256(metadata_by_field.get("content_digest"))
            ),
            artifact_byte_count=_strict_int(
                metadata_by_field.get("artifact_byte_count")
            ),
            binding_vector_digest=bytes.fromhex(
                _strict_sha256(metadata_by_field.get("binding_vector_digest"))
            ),
        )
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


async def _validate_reused_layout_state(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> None:
    layout_state = (
        await session.execute(
            db.text(f"""
                SELECT root.state, layout.state, layout.generation
                  FROM {schema}.ptg2_v4_snapshot_map_root AS root
                  JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                    ON layout.snapshot_key = root.snapshot_key
                 WHERE root.snapshot_key = :snapshot_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one_or_none()
    if layout_state is None or tuple(layout_state) != (
        "complete",
        "sealed",
        "shared_blocks_v4",
    ):
        raise _fail()


async def _validate_reused_manifest(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    expected: TaxIdentitySourcePublication,
) -> None:
    manifest_values = (
        await session.execute(
            db.text(f"""
                SELECT token_policy_id, token_policy_descriptor_sha256,
                       source_count, provider_group_occurrence_count,
                       matched_ein_count, missing_count, malformed_count,
                       unsupported_type_count, content_digest
                  FROM {schema}.ptg2_provider_tax_identity_source_manifest
                 WHERE snapshot_key = :snapshot_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one_or_none()
    expected_values = (
        expected.token_policy_id,
        expected.token_policy_descriptor_sha256,
        expected.source_count,
        expected.provider_group_occurrence_count,
        expected.matched_ein_count,
        expected.missing_count,
        expected.malformed_count,
        expected.unsupported_type_count,
        expected.content_digest,
    )
    if manifest_values is None or tuple(manifest_values) != expected_values:
        raise _fail()


def _expected_binding_values(
    expected_bindings: Iterable[Mapping[str, Any]],
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            _strict_int(binding_by_field.get("source_key")),
            binding_by_field.get("source_type"),
            binding_by_field.get("identity_kind"),
            _strict_sha256(binding_by_field.get("identity_sha256")),
        )
        for binding_by_field in expected_bindings
    )


def _validate_reused_binding_identities(
    stored_binding_records: tuple[dict[str, object], ...],
    *,
    expected_bindings: Iterable[Mapping[str, Any]],
) -> None:
    stored_identity_values = tuple(
        tuple(binding_by_field[field_name] for field_name in SOURCE_BINDING_FIELDS[:4])
        for binding_by_field in stored_binding_records
    )
    if stored_identity_values != _expected_binding_values(expected_bindings):
        raise _fail()


async def _validate_tax_identity_source_projection_state(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    sealed_metadata: Mapping[str, Any],
    aggregate_metadata: Mapping[str, Any],
    require_sealed_layout: bool,
) -> tuple[TaxIdentitySourcePublication, tuple[dict[str, object], ...]]:
    expected = _publication_from_metadata(sealed_metadata)
    schema = _quote_ident(schema_name)
    if require_sealed_layout:
        await _validate_reused_layout_state(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
        )
    await validate_reused_tax_identity_aggregate_manifest(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        sealed_metadata=aggregate_metadata,
    )
    await _validate_reused_manifest(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        expected=expected,
    )
    stored_bindings = await load_source_bindings(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
    )
    validate_source_binding_seal(
        stored_bindings,
        expected=expected,
    )
    await validate_source_observation_counts(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        expected=expected,
    )
    return expected, stored_bindings


async def validate_reused_tax_identity_source_projection(
    *,
    schema_name: str,
    snapshot_key: int,
    expected_bindings: Iterable[Mapping[str, Any]],
    sealed_metadata: Mapping[str, Any],
    aggregate_metadata: Mapping[str, Any],
) -> TaxIdentitySourcePublication:
    """Validate sealed pathless evidence without rescanning deleted sidecars."""

    bindings = tuple(expected_bindings)
    try:
        async with db.transaction() as session:
            expected, stored_bindings = (
                await _validate_tax_identity_source_projection_state(
                    session,
                    schema_name=schema_name,
                    snapshot_key=snapshot_key,
                    sealed_metadata=sealed_metadata,
                    aggregate_metadata=aggregate_metadata,
                    require_sealed_layout=True,
                )
            )
            if len(bindings) != expected.source_count:
                raise _fail()
            _validate_reused_binding_identities(
                stored_bindings,
                expected_bindings=bindings,
            )
        return expected
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "validate_merged_tax_identity_source_reduction",
    "validate_reused_tax_identity_source_projection",
    "validate_stored_tax_identity_source_counts",
]
