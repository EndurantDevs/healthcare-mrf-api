# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Inactive PostgreSQL load-and-seal store for admitted connector bundles."""

from __future__ import annotations

import asyncio
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass

from process.tin_npi_connector_generation_store_copy import (
    copy_generation_children,
)
from process.tin_npi_connector_generation_store_metadata import (
    expected_generation_metadata,
    insert_generation,
    is_exact_record_match,
    read_generation,
    register_and_verify_policies,
    seal_generation,
    set_transaction_guards,
)
from process.tin_npi_connector_generation_store_types import (
    ConnectorGenerationStoreConnection,
    TinNpiConnectorGenerationStoreError,
)
from process.tin_npi_connector_publication import (
    ConnectorPublicationBundle,
    ConnectorPublicationCounts,
    ConnectorPublicationLimits,
    admit_connector_publication_bundle,
)

_SCHEMA_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_HASH_HEX_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_ADVISORY_LOCK_DOMAIN = "healthporta.tin-npi.load-seal.v1:"


@dataclass(frozen=True, repr=False)
class SealedConnectorGeneration:
    """Redacted identity and counts for one committed complete generation."""

    generation_key: int
    generation_id: str
    source_vector_id: str
    counts: ConnectorPublicationCounts
    reused: bool

    def __post_init__(self) -> None:
        is_invalid = (
            type(self.generation_key) is not int
            or self.generation_key <= 0
            or type(self.generation_id) is not str
            or _HASH_HEX_PATTERN.fullmatch(self.generation_id) is None
            or type(self.source_vector_id) is not str
            or _HASH_HEX_PATTERN.fullmatch(self.source_vector_id) is None
            or type(self.counts) is not ConnectorPublicationCounts
            or type(self.reused) is not bool
        )
        if is_invalid:
            raise TinNpiConnectorGenerationStoreError(
                "sealed connector generation result is invalid"
            )

    def __repr__(self) -> str:
        return (
            "<sealed-connector-generation "
            f"key={self.generation_key} reused={str(self.reused).lower()} "
            f"sources={self.counts.source_count} "
            f"evidence={self.counts.evidence_row_count}>"
        )


async def load_and_seal_admitted_connector_generation(
    connection: ConnectorGenerationStoreConnection,
    bundle: ConnectorPublicationBundle,
    *,
    limits: ConnectorPublicationLimits,
    schema: str,
) -> SealedConnectorGeneration:
    """Atomically load and seal one bounded bundle without publishing it."""

    counts = admit_connector_publication_bundle(bundle, limits=limits)
    canonical_schema = _validated_schema(schema)
    _require_source_schema(bundle, canonical_schema)
    _require_idle_connection(connection)
    translated_error_message = "connector generation database operation failed"
    try:
        async with asyncio.timeout(limits.operation_timeout_seconds):
            async with connection.transaction():
                return await _load_in_transaction(
                    connection,
                    bundle,
                    counts=counts,
                    limits=limits,
                    schema=canonical_schema,
                )
    except TinNpiConnectorGenerationStoreError:
        raise
    except TimeoutError:
        translated_error_message = "connector generation load timed out"
    except Exception:
        translated_error_message = "connector generation database operation failed"
    raise TinNpiConnectorGenerationStoreError(translated_error_message)


def _validated_schema(candidate: object) -> str:
    if type(candidate) is not str or _SCHEMA_PATTERN.fullmatch(candidate) is None:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation schema is invalid"
        )
    return candidate


def _require_source_schema(
    bundle: ConnectorPublicationBundle,
    schema: str,
) -> None:
    input_relations = bundle.source_vector.input_relations
    if len(input_relations) != 1 or input_relations[0].schema != schema:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation source schema binding is invalid"
        )


def _require_idle_connection(connection: ConnectorGenerationStoreConnection) -> None:
    is_connection_in_transaction: object = None
    is_state_available = True
    try:
        is_connection_in_transaction = connection.is_in_transaction()
    except Exception:
        is_state_available = False
    if not is_state_available:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation connection state is unavailable"
        )
    if (
        type(is_connection_in_transaction) is not bool
        or is_connection_in_transaction
    ):
        raise TinNpiConnectorGenerationStoreError(
            "connector generation requires an idle connection"
        )


async def _load_in_transaction(
    connection: ConnectorGenerationStoreConnection,
    bundle: ConnectorPublicationBundle,
    *,
    counts: ConnectorPublicationCounts,
    limits: ConnectorPublicationLimits,
    schema: str,
) -> SealedConnectorGeneration:
    await set_transaction_guards(connection, limits)
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        _ADVISORY_LOCK_DOMAIN + bundle.source_vector.source_vector_id,
    )
    expected = expected_generation_metadata(bundle, counts)
    incumbent = await _bound_generation(connection, schema, bundle)
    if incumbent is not None:
        return _sealed_result(
            incumbent,
            bundle,
            counts=counts,
            expected=expected,
            reused=True,
        )
    return await _create_and_seal_generation(
        connection,
        schema,
        bundle,
        counts=counts,
        limits=limits,
        expected=expected,
    )


async def _create_and_seal_generation(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
    *,
    counts: ConnectorPublicationCounts,
    limits: ConnectorPublicationLimits,
    expected: Mapping[str, object],
) -> SealedConnectorGeneration:
    await register_and_verify_policies(connection, schema, bundle)
    build_token = secrets.token_hex(32)
    generation_key = await insert_generation(
        connection,
        schema,
        bundle,
        counts=counts,
        limits=limits,
        build_token=build_token,
    )
    if generation_key is None:
        return await _reuse_after_insert_conflict(
            connection,
            schema,
            bundle,
            counts=counts,
            expected=expected,
        )
    await connection.execute(
        "SELECT set_config('healthporta.tin_npi_build_token', $1, true)",
        build_token,
    )
    await copy_generation_children(
        connection,
        schema,
        generation_key,
        bundle,
        limits.copy_batch_size,
    )
    sealed = await seal_generation(
        connection,
        schema,
        generation_key,
        bytes.fromhex(bundle.source_vector.source_vector_id),
    )
    return _sealed_result(
        sealed,
        bundle,
        counts=counts,
        expected=expected,
        reused=False,
    )


async def _reuse_after_insert_conflict(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
    *,
    counts: ConnectorPublicationCounts,
    expected: Mapping[str, object],
) -> SealedConnectorGeneration:
    incumbent = await _bound_generation(connection, schema, bundle)
    if incumbent is None:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation identity conflict"
        )
    return _sealed_result(
        incumbent,
        bundle,
        counts=counts,
        expected=expected,
        reused=True,
    )


async def _bound_generation(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
) -> Mapping[str, object] | None:
    return await read_generation(
        connection,
        schema,
        bytes.fromhex(bundle.source_vector.source_vector_id),
    )


def _sealed_result(
    generation_record: Mapping[str, object],
    bundle: ConnectorPublicationBundle,
    *,
    counts: ConnectorPublicationCounts,
    expected: Mapping[str, object],
    reused: bool,
) -> SealedConnectorGeneration:
    is_lifecycle_match = (
        generation_record["state"] == "complete"
        and generation_record["completed_at"] is not None
        and generation_record["failed_at"] is None
        and generation_record["retired_at"] is None
        and generation_record["gc_after"] is None
    )
    if not is_lifecycle_match or not is_exact_record_match(
        generation_record,
        expected,
    ):
        raise TinNpiConnectorGenerationStoreError(
            "connector generation reuse conflict"
        )
    generation_key = generation_record["generation_key"]
    if type(generation_key) is not int or generation_key <= 0:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation key is invalid"
        )
    return SealedConnectorGeneration(
        generation_key=generation_key,
        generation_id=bundle.generation.generation_id,
        source_vector_id=bundle.source_vector.source_vector_id,
        counts=counts,
        reused=reused,
    )
