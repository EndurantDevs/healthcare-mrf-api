"""Insert-once storage for bounded PTG plan-catalog outbox chunks."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from db.connection import db
from process.ptg_parts.ptg2_plan_catalog_payload import (
    PTG2PlanCatalogOutboxConflict,
    chunk_request_id,
    json_value,
    row_mapping,
)


PTG2_PLAN_CATALOG_OUTBOX_TABLE = "ptg2_plan_catalog_outbox"


def _catalog_chunk_parameters(
    *,
    snapshot_id: str,
    chunk_index: int,
    chunk_count: int,
    chunk: tuple[list[dict[str, Any]], list[dict[str, Any]], str, int],
) -> tuple[str, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Project one canonical chunk into its exact bound SQL parameters."""

    chunk_plan_rows, chunk_alias_rows, payload_sha256, payload_bytes = chunk
    request_id = chunk_request_id(
        snapshot_id,
        chunk_index=chunk_index,
        payload_sha256=payload_sha256,
    )
    parameters_by_name = {
        "request_id": request_id,
        "snapshot_id": snapshot_id,
        "chunk_index": chunk_index,
        "chunk_count": chunk_count,
        "payload_sha256": payload_sha256,
        "plan_rows": json.dumps(
            chunk_plan_rows, sort_keys=True, separators=(",", ":")
        ),
        "alias_rows": json.dumps(
            chunk_alias_rows, sort_keys=True, separators=(",", ":")
        ),
        "plan_count": len(chunk_plan_rows),
        "alias_count": len(chunk_alias_rows),
        "payload_bytes": payload_bytes,
    }
    return request_id, parameters_by_name, chunk_plan_rows, chunk_alias_rows


async def enqueue_catalog_chunk(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    chunk_index: int,
    chunk_count: int,
    chunk: tuple[list[dict[str, Any]], list[dict[str, Any]], str, int],
) -> str:
    """Insert and re-read one immutable bounded catalog chunk."""

    request_id, parameters_by_name, plan_rows, alias_rows = (
        _catalog_chunk_parameters(
            snapshot_id=snapshot_id,
            chunk_index=chunk_index,
            chunk_count=chunk_count,
            chunk=chunk,
        )
    )
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
                (request_id, snapshot_id, chunk_index, chunk_count,
                 payload_sha256, plan_rows, alias_rows, plan_count,
                 alias_count, payload_bytes)
            VALUES
                (:request_id, :snapshot_id, :chunk_index, :chunk_count,
                 :payload_sha256, CAST(:plan_rows AS jsonb),
                 CAST(:alias_rows AS jsonb), :plan_count, :alias_count,
                 :payload_bytes)
            ON CONFLICT (snapshot_id, chunk_index) DO NOTHING
            """
        ),
        parameters_by_name,
    )
    stored_request = await _load_catalog_request(
        session, schema=schema, request_id=request_id
    )
    if stored_request is None:
        raise PTG2PlanCatalogOutboxConflict(
            "PTG plan catalog outbox request disappeared during enqueue"
        )
    _require_exact_stored_request(
        stored_request,
        expected=parameters_by_name,
        normalized_plans=plan_rows,
        normalized_aliases=alias_rows,
    )
    return request_id


async def _load_catalog_request(
    session: Any,
    *,
    schema: str,
    request_id: str,
) -> Any:
    """Read one immutable request immediately after its insert attempt."""

    query_result = await session.execute(
        db.text(
            f"""
            SELECT request_id, snapshot_id, chunk_index, chunk_count,
                   payload_sha256, plan_rows, alias_rows, plan_count,
                   alias_count, payload_bytes
              FROM {schema}.{PTG2_PLAN_CATALOG_OUTBOX_TABLE}
             WHERE request_id = :request_id
            """
        ),
        {"request_id": request_id},
    )
    return query_result.one_or_none()


def _require_exact_stored_request(
    stored_request: Any,
    *,
    expected: Mapping[str, Any],
    normalized_plans: Sequence[Mapping[str, Any]],
    normalized_aliases: Sequence[Mapping[str, Any]],
) -> None:
    """Reject a request ID that already binds different catalog bytes."""

    actual_by_field = row_mapping(stored_request)
    scalar_fields = (
        "request_id",
        "snapshot_id",
        "chunk_index",
        "chunk_count",
        "payload_sha256",
        "plan_count",
        "alias_count",
        "payload_bytes",
    )
    if (
        any(
            actual_by_field.get(field_name) != expected[field_name]
            for field_name in scalar_fields
        )
        or json_value(actual_by_field.get("plan_rows")) != normalized_plans
        or json_value(actual_by_field.get("alias_rows")) != normalized_aliases
    ):
        raise PTG2PlanCatalogOutboxConflict(
            "PTG plan catalog outbox request conflicts with its snapshot"
        )


__all__ = ["PTG2_PLAN_CATALOG_OUTBOX_TABLE", "enqueue_catalog_chunk"]
