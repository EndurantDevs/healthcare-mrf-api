# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish staged source-local tax-identity observations transactionally."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_observations import (
    _publish_observations,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    _fail,
    _strict_int,
)
from process.ptg_parts.ptg2_tax_identity_source_preflight import (
    validate_staged_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_tax_identity_source_target_preflight import (
    lock_tax_identity_source_target_vector,
    validate_tax_identity_source_target_aggregate,
    validate_tax_identity_source_target_sources,
)
from process.ptg_parts.ptg2_tax_identity_source_stage import (
    StagedTaxIdentitySourceProjection,
    _drop_staged_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_tax_identity_source_validation import (
    validate_merged_tax_identity_source_reduction,
    validate_stored_tax_identity_source_counts,
)

_MANIFEST_FIELDS = (
    "contract",
    "binding_contract",
    "token_policy_id",
    "token_policy_descriptor_sha256",
    "source_count",
    "provider_group_occurrence_count",
    "matched_ein_count",
    "missing_count",
    "malformed_count",
    "unsupported_type_count",
    "content_digest",
)
_BINDING_FIELDS = (
    "source_key",
    "source_type",
    "identity_kind",
    "identity_sha256",
    "token_policy_id",
    "token_policy_descriptor_sha256",
    "record_format",
    "format_version",
    "record_bytes",
    "artifact_sha256",
    "artifact_byte_count",
    "provider_group_count",
    "matched_ein_count",
    "missing_count",
    "malformed_count",
    "unsupported_type_count",
)


def _publication(
    prepared: PreparedTaxIdentitySourceProjection,
) -> TaxIdentitySourcePublication:
    return TaxIdentitySourcePublication(
        token_policy_id=prepared.token_policy_id,
        token_policy_descriptor_sha256=prepared.token_policy_descriptor_sha256,
        source_ordinal_map_digest=prepared.source_ordinal_map_digest,
        source_count=prepared.source_count,
        provider_group_occurrence_count=prepared.provider_group_occurrence_count,
        matched_ein_count=prepared.matched_ein_count,
        missing_count=prepared.missing_count,
        malformed_count=prepared.malformed_count,
        unsupported_type_count=prepared.unsupported_type_count,
        content_digest=prepared.content_digest,
        artifact_byte_count=prepared.artifact_byte_count,
        binding_vector_digest=prepared.binding_vector_digest,
    )


def _manifest_parameters(
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    snapshot_key: int,
) -> dict[str, object]:
    return {
        "snapshot_key": _strict_int(snapshot_key),
        "contract": PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
        "binding_contract": PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
        "token_policy_id": prepared.token_policy_id,
        "token_policy_descriptor_sha256": prepared.token_policy_descriptor_sha256,
        "source_count": prepared.source_count,
        "provider_group_occurrence_count": prepared.provider_group_occurrence_count,
        "matched_ein_count": prepared.matched_ein_count,
        "missing_count": prepared.missing_count,
        "malformed_count": prepared.malformed_count,
        "unsupported_type_count": prepared.unsupported_type_count,
        "content_digest": prepared.content_digest,
    }


async def _publish_manifest(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Insert or verify the immutable source-local manifest."""

    parameters_by_name = _manifest_parameters(prepared, snapshot_key=snapshot_key)
    await session.execute(
        db.text(f"""
            INSERT INTO {schema}.ptg2_provider_tax_identity_source_manifest
                (snapshot_key, contract, binding_contract, token_policy_id,
                 token_policy_descriptor_sha256, source_count,
                 provider_group_occurrence_count, matched_ein_count,
                 missing_count, malformed_count, unsupported_type_count,
                 content_digest)
            VALUES
                (:snapshot_key, :contract, :binding_contract, :token_policy_id,
                 :token_policy_descriptor_sha256, :source_count,
                 :provider_group_occurrence_count, :matched_ein_count,
                 :missing_count, :malformed_count, :unsupported_type_count,
                 :content_digest)
            ON CONFLICT DO NOTHING
            """),
        parameters_by_name,
    )
    stored_values = (
        await session.execute(
            db.text(f"""
                SELECT {", ".join(_MANIFEST_FIELDS)}
                  FROM {schema}.ptg2_provider_tax_identity_source_manifest
                 WHERE snapshot_key = :snapshot_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one_or_none()
    expected_values = tuple(parameters_by_name[name] for name in _MANIFEST_FIELDS)
    if stored_values is None or tuple(stored_values) != expected_values:
        raise _fail()


def _binding_values_by_source(
    prepared: PreparedTaxIdentitySourceProjection,
    *,
    snapshot_key: int,
) -> tuple[dict[str, object], ...]:
    return tuple(
        binding.persisted_values(
            snapshot_key=snapshot_key,
            token_policy_id=prepared.token_policy_id,
            token_policy_descriptor_sha256=(prepared.token_policy_descriptor_sha256),
        )
        for binding in prepared.bindings
    )


async def _stored_binding_values(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> tuple[tuple[object, ...], ...]:
    stored_bindings = (
        await session.execute(
            db.text(f"""
                SELECT {", ".join(_BINDING_FIELDS)}
                  FROM {schema}.ptg2_provider_tax_identity_source_binding
                 WHERE snapshot_key = :snapshot_key
                 ORDER BY source_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).all()
    return tuple(tuple(stored_binding) for stored_binding in stored_bindings)


async def _publish_bindings(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Insert dense physical-source bindings and verify exact replay."""

    insert_statement = db.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_source_binding
            (snapshot_key, source_key, source_type, identity_kind,
             identity_sha256, token_policy_id,
             token_policy_descriptor_sha256, record_format, format_version,
             record_bytes, artifact_sha256, artifact_byte_count,
             provider_group_count, matched_ein_count, missing_count,
             malformed_count, unsupported_type_count)
        VALUES
            (:snapshot_key, :source_key, :source_type, :identity_kind,
             :identity_sha256, :token_policy_id,
             :token_policy_descriptor_sha256, :record_format, :format_version,
             :record_bytes, :artifact_sha256, :artifact_byte_count,
             :provider_group_count, :matched_ein_count, :missing_count,
             :malformed_count, :unsupported_type_count)
        ON CONFLICT DO NOTHING
        """)
    binding_values_by_source = _binding_values_by_source(
        prepared, snapshot_key=snapshot_key
    )
    for source_number, binding_values_by_name in enumerate(
        binding_values_by_source, start=1
    ):
        await session.execute(insert_statement, binding_values_by_name)
        if heartbeat_callback is not None and source_number % 1000 == 0:
            heartbeat_callback()
    expected_values = tuple(
        tuple(binding_values_by_name[name] for name in _BINDING_FIELDS)
        for binding_values_by_name in binding_values_by_source
    )
    if (
        await _stored_binding_values(session, schema=schema, snapshot_key=snapshot_key)
        != expected_values
    ):
        raise _fail()


async def _validated_publication_stage(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    snapshot_key: int,
    staged: StagedTaxIdentitySourceProjection,
    prepared: PreparedTaxIdentitySourceProjection,
) -> tuple[str, int]:
    stage, provider_group_count = await validate_staged_tax_identity_source_projection(
        session,
        staged=staged,
        prepared=prepared,
    )
    await validate_tax_identity_source_target_aggregate(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        prepared=prepared,
        provider_group_count=provider_group_count,
    )
    await validate_tax_identity_source_target_sources(
        session,
        schema_name=schema_name,
        logical_snapshot_id=logical_snapshot_id,
        prepared=prepared,
    )
    return stage, provider_group_count


async def _publish_and_validate_source_rows(
    session: Any,
    *,
    schema: str,
    stage: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    await _publish_manifest(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        prepared=prepared,
    )
    await _publish_bindings(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        prepared=prepared,
        heartbeat_callback=heartbeat_callback,
    )
    await _publish_observations(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        prepared=prepared,
        heartbeat_callback=heartbeat_callback,
    )
    await validate_stored_tax_identity_source_counts(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        prepared=prepared,
    )
    await validate_merged_tax_identity_source_reduction(
        session,
        schema=schema,
        stage=stage,
        snapshot_key=snapshot_key,
        heartbeat_callback=heartbeat_callback,
    )


async def publish_staged_tax_identity_source_projection(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    snapshot_key: int,
    staged: StagedTaxIdentitySourceProjection,
    prepared: PreparedTaxIdentitySourceProjection,
    heartbeat_callback: Callable[[], None] | None = None,
) -> TaxIdentitySourcePublication:
    """Publish the complete immutable source bundle in the caller transaction."""

    schema = _quote_ident(schema_name)
    try:
        stage, provider_group_count = await _validated_publication_stage(
            session,
            schema_name=schema_name,
            logical_snapshot_id=logical_snapshot_id,
            snapshot_key=snapshot_key,
            staged=staged,
            prepared=prepared,
        )
        async with session.begin_nested():
            await _publish_and_validate_source_rows(
                session,
                schema=schema,
                stage=stage,
                snapshot_key=snapshot_key,
                prepared=prepared,
                heartbeat_callback=heartbeat_callback,
            )
            await lock_tax_identity_source_target_vector(
                session,
                schema_name=schema_name,
                logical_snapshot_id=logical_snapshot_id,
                prepared=prepared,
            )
            await validate_tax_identity_source_target_aggregate(
                session,
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                prepared=prepared,
                provider_group_count=provider_group_count,
                lock_for_update=True,
            )
            await _drop_staged_tax_identity_source_projection(session, staged)
        return _publication(prepared)
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


__all__ = ["publish_staged_tax_identity_source_projection"]
