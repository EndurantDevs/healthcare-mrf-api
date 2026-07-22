# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound candidate provider-network metadata under one decoded budget."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable, Mapping

from sqlalchemy import text

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    retain_unique_integer_keys,
)
from api.ptg2_serving import PTG2_SCHEMA, _required_shared_snapshot_key
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from scripts.validation import ptg2_v3_source_api_audit as source_audit


NETWORK_MAP_BYTES = 224
NETWORK_NAME_BUCKET_BYTES = 512
NETWORK_NAME_BYTES = 1024
NETWORK_DIGEST_BUCKET_BYTES = 512
NETWORK_DIGEST_BYTES = 256


def _network_record_fields(database_record: Any) -> dict[str, Any]:
    record_mapping = getattr(database_record, "_mapping", None)
    if record_mapping is not None:
        return dict(record_mapping)
    if isinstance(database_record, Mapping):
        return dict(database_record)
    return dict(database_record or {})


async def _provider_network_query(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_set_keys: tuple[int, ...],
    *,
    schema_name: str = PTG2_SCHEMA,
) -> Any:
    return await session.execute(
        text(
            f"""
            SELECT provider_set_key, network_names
              FROM {schema_name}.ptg2_v3_provider_set
             WHERE snapshot_key = :shared_snapshot_key
               AND provider_set_key = ANY(CAST(:provider_set_keys AS integer[]))
             ORDER BY provider_set_key
            """
        ),
        {
            "shared_snapshot_key": _required_shared_snapshot_key(serving_tables),
            "provider_set_keys": provider_set_keys,
        },
    )


def _decoded_network_names(
    network_query: Iterable[Any],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> tuple[dict[int, tuple[str, ...]], int]:
    retained_network_bytes = NETWORK_MAP_BYTES
    network_names_by_key: dict[int, tuple[str, ...]] = {}
    try:
        for raw_network_record in network_query:
            network_record = _network_record_fields(raw_network_record)
            provider_set_key = int(network_record["provider_set_key"])
            if provider_set_key in network_names_by_key:
                raise PTG2ManifestArtifactError(
                    "PTG2 candidate provider-set network metadata is duplicated"
                )
            canonical_names = source_audit.canonical_list(
                network_record["network_names"]
            )
            network_bytes = (
                NETWORK_NAME_BUCKET_BYTES + len(canonical_names) * NETWORK_NAME_BYTES
            )
            if retention_budget is not None:
                retention_budget.claim(
                    network_bytes,
                    category="a provider network-name bucket",
                )
                retained_network_bytes += network_bytes
            network_names_by_key[provider_set_key] = tuple(canonical_names)
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(retained_network_bytes - NETWORK_MAP_BYTES)
        raise
    return network_names_by_key, retained_network_bytes


async def provider_network_names_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_set_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
    *,
    schema_name: str = PTG2_SCHEMA,
) -> dict[int, tuple[str, ...]]:
    """Read complete provider network names within the candidate budget."""

    requested_keys, retained_requested_bytes = retain_unique_integer_keys(
        provider_set_keys,
        retention_budget,
        category="provider network",
    )
    if not requested_keys:
        if retention_budget is not None:
            retention_budget.release(retained_requested_bytes)
        return {}
    if retention_budget is not None:
        retention_budget.claim(
            NETWORK_MAP_BYTES,
            category="the provider network-name map",
        )
    retained_network_bytes = NETWORK_MAP_BYTES
    try:
        network_query = await _provider_network_query(
            session,
            serving_tables,
            requested_keys,
            schema_name=schema_name,
        )
        network_names_by_key, retained_network_bytes = _decoded_network_names(
            network_query,
            retention_budget,
        )
        if len(network_names_by_key) != len(requested_keys) or any(
            provider_set_key not in network_names_by_key
            for provider_set_key in requested_keys
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate provider-set network metadata is incomplete"
            )
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(retained_requested_bytes + retained_network_bytes)
        raise
    if retention_budget is not None:
        retention_budget.release(retained_requested_bytes)
    return network_names_by_key


def _network_name_retained_bytes(
    network_names_by_key: Mapping[int, tuple[str, ...]],
) -> int:
    return NETWORK_MAP_BYTES + sum(
        NETWORK_NAME_BUCKET_BYTES + len(network_names) * NETWORK_NAME_BYTES
        for network_names in network_names_by_key.values()
    )


async def provider_network_digests_by_key(
    session: Any,
    serving_tables: PTG2ServingTables,
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None = None,
    network_name_loader: Callable[..., Awaitable[dict[int, tuple[str, ...]]]] = (
        provider_network_names_by_key
    ),
) -> dict[int, frozenset[str]]:
    """Replace retained names with exact canonical network digest sets."""

    provider_set_keys = (
        provider_key
        for provider_keys in provider_filters_by_code_key.values()
        for provider_key in provider_keys
    )
    network_names_by_key = await network_name_loader(
        session,
        serving_tables,
        provider_set_keys,
        retention_budget,
    )
    retained_digest_bytes = NETWORK_MAP_BYTES
    if retention_budget is not None:
        retention_budget.claim(
            retained_digest_bytes,
            category="the provider network-digest map",
        )
    network_digests_by_key: dict[int, frozenset[str]] = {}
    try:
        for provider_set_key, network_names in network_names_by_key.items():
            digest_bytes = (
                NETWORK_DIGEST_BUCKET_BYTES + len(network_names) * NETWORK_DIGEST_BYTES
            )
            if retention_budget is not None:
                retention_budget.claim(
                    digest_bytes,
                    category="a provider network-digest bucket",
                )
                retained_digest_bytes += digest_bytes
            network_digests_by_key[provider_set_key] = frozenset(
                canonical_network_name_digests(network_names)
            )
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(
                _network_name_retained_bytes(network_names_by_key)
                + retained_digest_bytes
            )
        raise
    if retention_budget is not None:
        retention_budget.release(_network_name_retained_bytes(network_names_by_key))
    return network_digests_by_key


__all__ = [
    "provider_network_digests_by_key",
    "provider_network_names_by_key",
]
