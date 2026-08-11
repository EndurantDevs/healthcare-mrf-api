# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared SQL coordinates for dormant rooted-graph persistence."""

from __future__ import annotations

import os
import re
from typing import Any

from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphStoreError,
    ProviderDirectoryRootedGraphWorkSpec,
)


ACQUISITION_TABLE = "provider_directory_rooted_graph_acquisition"
WORK_TABLE = "provider_directory_rooted_graph_work"
RESOURCE_TABLE = "provider_directory_rooted_graph_resource"
EDGE_TABLE = "provider_directory_rooted_graph_edge"
ENDPOINT_TABLE = "provider_directory_api_endpoint"
DATASET_TABLE = "provider_directory_endpoint_dataset"
DATASET_RESOURCE_TABLE = "provider_directory_dataset_resource"
TERMINAL_SET_FUNCTION = "provider_directory_rooted_graph_terminal_set_sha256"
RESOURCE_SET_FUNCTION = "provider_directory_rooted_graph_resource_set_sha256"
EDGE_SET_FUNCTION = "provider_directory_rooted_graph_edge_set_sha256"
ROOT_FUNCTION = "provider_directory_rooted_graph_sha256"
ACTION_SETTING = "healthporta.rooted_graph_action"
ACQUISITION_SETTING = "healthporta.rooted_graph_acquisition"
LEASE_SETTING = "healthporta.rooted_graph_lease"

_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def schema_name() -> str:
    """Resolve one safe runtime schema under the dual-env convention."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ProviderDirectoryRootedGraphStoreError("state")
    schema = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema) is None:
        raise ProviderDirectoryRootedGraphStoreError("state")
    return schema


def table_ref(table_name: str) -> str:
    """Return one safely quoted relation reference."""

    schema = schema_name().replace('"', '""')
    return f'"{schema}"."{table_name}"'


def function_ref(function_name: str) -> str:
    """Return one safely quoted stored-function reference."""

    schema = schema_name().replace('"', '""')
    return f'"{schema}"."{function_name}"'


def row_fields(database_row: Any) -> dict[str, Any]:
    """Normalize a SQLAlchemy, asyncpg, or fake database row."""

    if database_row is None:
        return {}
    mapping = (
        database_row._mapping if hasattr(database_row, "_mapping") else database_row
    )
    return dict(mapping)


async def set_store_action(
    database: Any,
    action: str,
    acquisition_id: str,
    lease_token: str = "",
) -> None:
    """Fence one trigger-visible operation inside the current transaction."""

    await database.scalar(
        """
        SELECT pg_catalog.set_config(:action_key, :action, true)
            || pg_catalog.set_config(:acquisition_key, :acquisition_id, true)
            || pg_catalog.set_config(:lease_key, :lease_token, true);
        """,
        action_key=ACTION_SETTING,
        action=action,
        acquisition_key=ACQUISITION_SETTING,
        acquisition_id=acquisition_id,
        lease_key=LEASE_SETTING,
        lease_token=lease_token,
    )


def work_fields(spec: ProviderDirectoryRootedGraphWorkSpec) -> dict[str, object]:
    """Return every immutable work-identity and discovery field."""

    return {
        name: getattr(spec, name)
        for name in (
            "query_id",
            "scope_id",
            "query_identity_sha256",
            "query_identity_json_text",
            "kind",
            "resource_type",
            "search_parameter",
            "reference_type",
            "reference_id",
            "closure_scope",
            "discovered_by_query_id",
            "discovered_source_type",
            "discovered_source_id",
            "discovered_edge_sha256",
        )
    }


async def insert_work_spec(
    database: Any,
    acquisition_id: str,
    spec: ProviderDirectoryRootedGraphWorkSpec,
) -> int:
    """Insert one canonical work identity without duplicating acquisition."""

    return await database.status(
        f"""
        INSERT INTO {table_ref(WORK_TABLE)} (
            acquisition_id, scope_id, query_id, query_identity_sha256,
            query_identity_json_text, kind, resource_type, search_parameter,
            reference_type, reference_id, closure_scope,
            discovered_by_query_id, discovered_source_type,
            discovered_source_id, discovered_edge_sha256,
            status, attempt_count, pagination_terminal
        ) VALUES (
            :acquisition_id, :scope_id, :query_id, :query_identity_sha256,
            :query_identity_json_text, :kind, :resource_type, :search_parameter,
            :reference_type, :reference_id, :closure_scope,
            :discovered_by_query_id, :discovered_source_type,
            :discovered_source_id, :discovered_edge_sha256,
            'pending', 0, false
        ) ON CONFLICT (acquisition_id, query_id) DO NOTHING;
        """,
        acquisition_id=acquisition_id,
        **work_fields(spec),
    )


def identity_fields(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> dict[str, object]:
    """Return every immutable acquisition header field."""

    names = (
        "acquisition_id",
        "storage_contract_id",
        "scope_id",
        "root_dataset_variant",
        "root_publication_contract_id",
        "root_source_id",
        "root_endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "source_authority_id",
        "endpoint_signature_sha256",
        "root_dataset_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "root_resource_count",
        "max_work_items",
        "max_resource_rows",
        "max_edge_rows",
        "max_payload_bytes",
        "connector_id",
        "graph_contract_sha256",
        "query_contract_sha256",
        "acquisition_role",
        "run_id",
        "dataset_intent_id",
        "rooted_graph_complete",
        "endpoint_collection_complete",
        "endpoint_complete",
    )
    return {name: getattr(identity, name) for name in names}


def assert_identity_row(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    database_row: Any,
) -> dict[str, Any]:
    """Reject missing, drifted, or unknown acquisition state."""

    fields = row_fields(database_row)
    status = fields.get("status")
    if status not in {"building", "sealed"}:
        raise ProviderDirectoryRootedGraphStoreError("state")
    expected_fields = identity_fields(identity)
    expected_fields["rooted_graph_complete"] = status == "sealed"
    if any(fields.get(name) != expected for name, expected in expected_fields.items()):
        raise ProviderDirectoryRootedGraphStoreError("state")
    return fields


__all__ = (
    "assert_identity_row",
    "function_ref",
    "identity_fields",
    "insert_work_spec",
    "row_fields",
    "schema_name",
    "set_store_action",
    "table_ref",
    "work_fields",
    "ACQUISITION_TABLE",
    "DATASET_RESOURCE_TABLE",
    "DATASET_TABLE",
    "EDGE_SET_FUNCTION",
    "EDGE_TABLE",
    "ENDPOINT_TABLE",
    "RESOURCE_SET_FUNCTION",
    "RESOURCE_TABLE",
    "ROOT_FUNCTION",
    "TERMINAL_SET_FUNCTION",
    "WORK_TABLE",
)
