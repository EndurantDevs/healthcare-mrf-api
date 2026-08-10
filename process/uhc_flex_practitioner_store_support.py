# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared SQL coordinates for Flex Practitioner persistence operations."""

from __future__ import annotations

import os
import re
from typing import Any

from process.uhc_flex_practitioner_store_contract import (
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerStoreError,
)


ACQUISITION_TABLE = "provider_directory_uhc_flex_practitioner_acquisition"
WORK_TABLE = "provider_directory_uhc_flex_practitioner_work"
RESOURCE_TABLE = "provider_directory_uhc_flex_practitioner_resource"
MEMBER_TABLE = "provider_directory_uhc_flex_npi_member"
TERMINAL_SET_FUNCTION = "pd_uhc_flex_practitioner_terminal_set_sha256"
ACTION_SETTING = "healthporta.uhc_flex_practitioner_action"
ACQUISITION_SETTING = "healthporta.uhc_flex_practitioner_acquisition"
LEASE_SETTING = "healthporta.uhc_flex_practitioner_lease"

_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def schema_name() -> str:
    """Resolve one safe runtime schema under the repository dual-env rule."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise UHCFlexPractitionerStoreError("state")
    schema = runtime_schema or legacy_schema or "mrf"
    if _SCHEMA_PATTERN.fullmatch(schema) is None:
        raise UHCFlexPractitionerStoreError("state")
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
        database_row._mapping
        if hasattr(database_row, "_mapping")
        else database_row
    )
    return dict(mapping)


async def set_store_action(
    database: Any,
    action: str,
    acquisition_id: str,
    lease_token: str = "",
) -> None:
    """Fence one trigger-visible operation for the current transaction."""

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


def identity_fields(
    identity: UHCFlexPractitionerAcquisitionIdentity,
) -> dict[str, object]:
    """Return the immutable header fields used by insert and replay checks."""

    return {
        field_name: getattr(identity, field_name)
        for field_name in (
            "acquisition_id",
            "storage_contract_id",
            "cohort_id",
            "acquisition_role",
            "source_id",
            "connector_id",
            "query_contract_id",
            "run_id",
            "dataset_intent_id",
            "expected_npi_count",
            "endpoint_collection_complete",
            "endpoint_complete",
        )
    }


def assert_identity_row(
    identity: UHCFlexPractitionerAcquisitionIdentity,
    database_row: Any,
) -> dict[str, Any]:
    """Reject a missing or drifted acquisition header."""

    fields = row_fields(database_row)
    expected = identity_fields(identity)
    if any(fields.get(name) != expected_value for name, expected_value in expected.items()):
        raise UHCFlexPractitionerStoreError("state")
    if fields.get("status") not in {"building", "sealed"}:
        raise UHCFlexPractitionerStoreError("state")
    return fields


__all__ = (
    "assert_identity_row",
    "function_ref",
    "identity_fields",
    "row_fields",
    "schema_name",
    "set_store_action",
    "table_ref",
    "ACQUISITION_TABLE",
    "MEMBER_TABLE",
    "RESOURCE_TABLE",
    "TERMINAL_SET_FUNCTION",
    "WORK_TABLE",
)
