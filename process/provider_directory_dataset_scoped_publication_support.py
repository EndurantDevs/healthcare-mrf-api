# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Registry and schema helpers for exact dataset publication locking."""

from __future__ import annotations

import os
import re
from typing import Any, Mapping

from process.provider_directory_dataset_scoped_publication_contract import (
    ExactDatasetPair,
    ProviderDirectoryDatasetScopedPublicationError,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def schema_name() -> str:
    """Resolve one non-ambiguous validated runtime schema."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    resolved_schema = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(resolved_schema) is None:
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    return resolved_schema


def qualified_relation(relation: str) -> str:
    """Quote one validated schema and caller-owned relation name."""

    schema = schema_name().replace('"', '""')
    identifier = relation.replace('"', '""')
    return f'"{schema}"."{identifier}"'


def database_row_fields(database_row: Any) -> dict[str, Any]:
    """Return a copied mapping for one database row."""

    if database_row is None:
        return {}
    mapping = (
        database_row._mapping if hasattr(database_row, "_mapping") else database_row
    )
    if not isinstance(mapping, Mapping):
        raise ProviderDirectoryDatasetScopedPublicationError("state")
    return dict(mapping)


async def exact_pair_registry_by_coordinate(
    database: Any,
    pair: ExactDatasetPair,
) -> dict[tuple[object, object], dict[str, Any]]:
    """Lock and return the two exact registry coordinates."""

    registry_rows = await database.all(
        f"""
        SELECT source.source_id, source.endpoint_id,
               source.canonical_api_base AS source_api_base,
               endpoint.canonical_api_base AS endpoint_api_base,
               source.metadata_json::jsonb
                   ->> 'provider_directory_authority_id' AS source_authority_id,
               endpoint.metadata_json::jsonb ->> 'authority_id'
                   AS endpoint_authority_id,
               endpoint.endpoint_signature_hash
                   AS endpoint_signature_sha256
          FROM {qualified_relation('provider_directory_source')} AS source
          JOIN {qualified_relation('provider_directory_api_endpoint')} AS endpoint
            ON endpoint.endpoint_id = source.endpoint_id
         WHERE (source.source_id, source.endpoint_id) IN (
               (:legacy_source_id, :legacy_endpoint_id),
               (:rooted_source_id, :rooted_endpoint_id)
         )
         ORDER BY source.source_id, source.endpoint_id
         FOR SHARE OF source, endpoint;
        """,
        legacy_source_id=pair.legacy_source_id,
        legacy_endpoint_id=pair.legacy_endpoint_id,
        rooted_source_id=pair.rooted_source_id,
        rooted_endpoint_id=pair.rooted_endpoint_id,
    )
    return {
        (fields.get("source_id"), fields.get("endpoint_id")): fields
        for registry_row in registry_rows
        if (fields := database_row_fields(registry_row))
    }


def validate_exact_pair_registry(
    by_coordinate: dict[tuple[object, object], dict[str, Any]],
    pair: ExactDatasetPair,
) -> None:
    """Prove both reviewed registry rows retain one exact coordinate."""

    expected_coordinates = {
        (pair.legacy_source_id, pair.legacy_endpoint_id),
        (pair.rooted_source_id, pair.rooted_endpoint_id),
    }
    if set(by_coordinate) != expected_coordinates:
        raise ProviderDirectoryDatasetScopedPublicationError("source_drift")


async def lock_exact_pair_registry(database: Any, pair: ExactDatasetPair) -> None:
    """Lock and validate both exact source and endpoint registry pairs."""

    by_coordinate = await exact_pair_registry_by_coordinate(database, pair)
    validate_exact_pair_registry(by_coordinate, pair)
    legacy = by_coordinate[(pair.legacy_source_id, pair.legacy_endpoint_id)]
    rooted = by_coordinate[(pair.rooted_source_id, pair.rooted_endpoint_id)]
    authority = legacy.get("source_authority_id")
    canonical_base = legacy.get("source_api_base")
    if (
        type(authority) is not str
        or not authority
        or len(authority) > 64
        or legacy.get("endpoint_authority_id") != authority
        or rooted.get("source_authority_id") != authority
        or rooted.get("endpoint_authority_id") != authority
        or type(canonical_base) is not str
        or not canonical_base
        or canonical_base != PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE
        or legacy.get("endpoint_api_base") != canonical_base
        or rooted.get("source_api_base") != canonical_base
        or rooted.get("endpoint_api_base") != canonical_base
        or rooted.get("endpoint_signature_sha256")
        != PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        or legacy.get("endpoint_signature_sha256")
        != uhc_flex_practitioner_endpoint_identity().endpoint_signature_hash
    ):
        raise ProviderDirectoryDatasetScopedPublicationError("source_drift")
