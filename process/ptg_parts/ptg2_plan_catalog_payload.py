"""Canonical bounded payloads for the durable PTG plan-catalog queue."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any


OUTBOX_MAX_PLAN_ROWS = 16
OUTBOX_MAX_ALIAS_ROWS = 128
OUTBOX_MAX_PAYLOAD_BYTES = 512 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PLAN_FIELDS = (
    "plan_hash",
    "hash_prefix",
    "plan_id",
    "plan_id_type",
    "plan_name",
    "plan_market_type",
    "issuer_name",
    "plan_sponsor_name",
    "canonical_payload",
)
_ALIAS_FIELDS = (
    "alias_hash",
    "plan_hash",
    "alias_type",
    "alias_value",
)


class PTG2PlanCatalogOutboxConflict(RuntimeError):
    """One source-local request identity resolved to different content."""


def row_mapping(database_row: Any) -> dict[str, Any]:
    """Project a SQLAlchemy row or mapping into an owned dictionary."""

    if isinstance(database_row, dict):
        return dict(database_row)
    return dict(getattr(database_row, "_mapping", database_row))


def json_value(serialized_value: Any) -> Any:
    """Decode stored JSON text while preserving already-decoded values."""

    if isinstance(serialized_value, str):
        return json.loads(serialized_value)
    return serialized_value


def _normalized_rows(
    source_rows: Sequence[Mapping[str, Any]],
    *,
    fields: Sequence[str],
    identity_field: str,
) -> list[dict[str, Any]]:
    rows_by_identity: dict[str, dict[str, Any]] = {}
    for source_row in source_rows:
        normalized_row_by_field = {
            field: (
                json_value(source_row.get(field))
                if field == "canonical_payload"
                else source_row.get(field)
            )
            for field in fields
        }
        identity = str(
            normalized_row_by_field.get(identity_field) or ""
        ).strip().lower()
        if not _SHA256_RE.fullmatch(identity):
            raise ValueError(f"PTG plan catalog {identity_field} is invalid")
        normalized_row_by_field[identity_field] = identity
        existing_row = rows_by_identity.get(identity)
        if existing_row is not None and existing_row != normalized_row_by_field:
            raise PTG2PlanCatalogOutboxConflict(
                f"PTG plan catalog {identity_field} has conflicting payloads"
            )
        rows_by_identity[identity] = normalized_row_by_field
    return [rows_by_identity[key] for key in sorted(rows_by_identity)]


def canonical_request_payload(
    *,
    plan_rows: Sequence[Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Normalize one exact request and return its canonical digest."""

    normalized_plan_rows = _normalized_rows(
        plan_rows,
        fields=_PLAN_FIELDS,
        identity_field="plan_hash",
    )
    normalized_alias_rows = _normalized_rows(
        alias_rows,
        fields=_ALIAS_FIELDS,
        identity_field="alias_hash",
    )
    plan_hashes = {plan_row["plan_hash"] for plan_row in normalized_plan_rows}
    if any(
        alias_row["plan_hash"] not in plan_hashes
        for alias_row in normalized_alias_rows
    ):
        raise ValueError("PTG plan alias refers outside its catalog request")
    canonical = json.dumps(
        {"alias_rows": normalized_alias_rows, "plan_rows": normalized_plan_rows},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return (
        normalized_plan_rows,
        normalized_alias_rows,
        hashlib.sha256(canonical).hexdigest(),
    )


def canonical_chunk(
    plan_rows: Sequence[Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> tuple[bytes, str]:
    """Return exact canonical bytes and digest for one bounded queue chunk."""

    canonical = json.dumps(
        {"alias_rows": list(alias_rows), "plan_rows": list(plan_rows)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return canonical, hashlib.sha256(canonical).hexdigest()


def _is_catalog_chunk_within_bounds(
    plan_rows_by_hash: Mapping[str, Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> bool:
    """Return whether one candidate chunk satisfies every durable bound."""

    if (
        len(plan_rows_by_hash) > OUTBOX_MAX_PLAN_ROWS
        or len(alias_rows) > OUTBOX_MAX_ALIAS_ROWS
    ):
        return False
    canonical, _digest = canonical_chunk(
        [plan_rows_by_hash[key] for key in sorted(plan_rows_by_hash)],
        alias_rows,
    )
    return len(canonical) <= OUTBOX_MAX_PAYLOAD_BYTES


def _catalog_chunk_record(
    plan_rows_by_hash: Mapping[str, Mapping[str, Any]],
    alias_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, int]:
    """Freeze one bounded chunk into its persisted row collections."""

    plan_rows = [dict(plan_rows_by_hash[key]) for key in sorted(plan_rows_by_hash)]
    frozen_alias_rows = [dict(alias_row) for alias_row in alias_rows]
    canonical, digest = canonical_chunk(plan_rows, frozen_alias_rows)
    return plan_rows, frozen_alias_rows, digest, len(canonical)


def bounded_catalog_chunks(
    normalized_plan_rows: Sequence[Mapping[str, Any]],
    normalized_alias_rows: Sequence[Mapping[str, Any]],
) -> list[tuple[list[dict[str, Any]], list[dict[str, Any]], str, int]]:
    """Partition plans with their aliases into deterministic bounded chunks."""

    alias_rows_by_plan_hash: dict[str, list[dict[str, Any]]] = {}
    for alias_row in normalized_alias_rows:
        alias_rows_by_plan_hash.setdefault(
            str(alias_row["plan_hash"]), []
        ).append(dict(alias_row))
    catalog_chunks = []
    chunk_plan_rows_by_hash: dict[str, dict[str, Any]] = {}
    chunk_alias_rows: list[dict[str, Any]] = []
    for plan_row in normalized_plan_rows:
        plan_hash = str(plan_row["plan_hash"])
        plan_alias_rows = alias_rows_by_plan_hash.get(plan_hash) or [None]
        for alias_row in plan_alias_rows:
            candidate_plan_rows_by_hash = {
                **chunk_plan_rows_by_hash,
                plan_hash: dict(plan_row),
            }
            candidate_alias_rows = list(chunk_alias_rows)
            if alias_row is not None:
                candidate_alias_rows.append(alias_row)
            if not _is_catalog_chunk_within_bounds(
                candidate_plan_rows_by_hash, candidate_alias_rows
            ):
                if chunk_plan_rows_by_hash or chunk_alias_rows:
                    catalog_chunks.append(
                        _catalog_chunk_record(
                            chunk_plan_rows_by_hash, chunk_alias_rows
                        )
                    )
                candidate_plan_rows_by_hash = {plan_hash: dict(plan_row)}
                candidate_alias_rows = [] if alias_row is None else [alias_row]
                if not _is_catalog_chunk_within_bounds(
                    candidate_plan_rows_by_hash, candidate_alias_rows
                ):
                    raise ValueError(
                        "one PTG plan catalog row exceeds the durable chunk bound"
                    )
            chunk_plan_rows_by_hash = candidate_plan_rows_by_hash
            chunk_alias_rows = candidate_alias_rows
    if chunk_plan_rows_by_hash or chunk_alias_rows:
        catalog_chunks.append(
            _catalog_chunk_record(chunk_plan_rows_by_hash, chunk_alias_rows)
        )
    return catalog_chunks


def validated_snapshot_id(snapshot_id: str) -> str:
    """Return one bounded source-local snapshot identity."""

    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id or len(normalized_snapshot_id) > 96:
        raise ValueError("PTG plan catalog snapshot identity is invalid")
    return normalized_snapshot_id


def chunk_request_id(
    snapshot_id: str,
    *,
    chunk_index: int,
    payload_sha256: str,
) -> str:
    """Derive one immutable request ID from its source-local chunk bytes."""

    request_identity = (
        f"ptg-plan-catalog-chunk-v1\0{snapshot_id}\0{chunk_index}\0"
        f"{payload_sha256}"
    ).encode("utf-8")
    return hashlib.sha256(request_identity).hexdigest()


__all__ = [
    "OUTBOX_MAX_ALIAS_ROWS",
    "OUTBOX_MAX_PAYLOAD_BYTES",
    "OUTBOX_MAX_PLAN_ROWS",
    "PTG2PlanCatalogOutboxConflict",
    "bounded_catalog_chunks",
    "canonical_chunk",
    "canonical_request_payload",
    "chunk_request_id",
    "json_value",
    "row_mapping",
    "validated_snapshot_id",
]
