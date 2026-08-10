# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate exact retained payloads before typed-row rehydration."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import types as sa_types

from process.provider_directory_fhir_subset_completion import (
    canonical_payload_sha256 as subset_payload_sha256,
)
from process.provider_directory_resource_hash import (
    is_semantic_resource_hash_contract,
    resource_content_hash_payload,
    resource_payload_sha256_for_contract,
)


def _validate_payload(
    model: type,
    resource_id: str,
    stored_hash: str,
    mapped_payload: Any,
    *,
    resource_hash_contract: str,
    resource_type: str | None = None,
    acquired_resource_sha256: Any = None,
) -> str | None:
    """Return a stable reason when a retained mapped payload is unsafe."""

    if not resource_id or not isinstance(mapped_payload, dict):
        return "payload_hash_mismatch"
    try:
        if acquired_resource_sha256 is not None:
            if is_semantic_resource_hash_contract(resource_hash_contract):
                return "payload_hash_mismatch"
            if (
                type(acquired_resource_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", acquired_resource_sha256)
                is None
            ):
                return "payload_hash_mismatch"
            expected_hash = subset_payload_sha256(
                resource_content_hash_payload(mapped_payload)
            )
        else:
            expected_hash = resource_payload_sha256_for_contract(
                mapped_payload,
                resource_hash_contract,
                resource_type=resource_type,
            )
    except (TypeError, ValueError):
        return "payload_hash_mismatch"
    if stored_hash != expected_hash:
        return "payload_hash_mismatch"
    reserved_fields = {
        "source_id",
        "last_seen_run_id",
        "observed_at",
        "updated_at",
    }
    if mapped_payload.get("resource_id") != resource_id or reserved_fields & set(
        mapped_payload
    ):
        return "payload_provenance_invalid"
    column_by_name = {column.name: column for column in model.__table__.columns}
    if set(mapped_payload) - set(column_by_name):
        return "payload_unknown_field"
    for field_name, field_value in mapped_payload.items():
        column_type = column_by_name[field_name].type
        if isinstance(column_type, sa_types.String) and field_value is not None:
            if not isinstance(field_value, str):
                return "payload_column_type_invalid"
        if isinstance(column_type, sa_types.Boolean) and field_value is not None:
            if not isinstance(field_value, bool):
                return "payload_column_type_invalid"
    return None
