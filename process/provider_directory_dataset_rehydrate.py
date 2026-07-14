"""Bounded rehydration of typed Provider Directory rows from one dataset.

The retained dataset payload is the source of truth. This module never resolves
or fetches a FHIR endpoint; callers supply the normal typed/canonical upsert
path so rehydrated rows retain exactly the original acquisition provenance.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from sqlalchemy import types as sa_types


DEFAULT_BATCH_SIZE = 5_000
MAX_BATCH_SIZE = 25_000
CHECKPOINT_TABLE = "provider_directory_dataset_rehydration_checkpoint"
DATASET_TABLE = "provider_directory_endpoint_dataset"
DATASET_RESOURCE_TABLE = "provider_directory_dataset_resource"
SOURCE_TABLE = "provider_directory_source"
ENDPOINT_TABLE = "provider_directory_api_endpoint"
CANONICAL_TABLE = "provider_directory_canonical_resource"
SOURCE_EDGE_TABLE = "provider_directory_source_resource"


class DatasetRehydrationError(RuntimeError):
    """The requested immutable dataset scope is unsafe or incomplete."""


@dataclass(frozen=True)
class DatasetScope:
    source_id: str
    dataset_id: str
    acquisition_root_run_id: str
    endpoint_id: str
    canonical_api_base: str
    dataset_hash: str
    resource_count: int
    resource_types: tuple[str, ...]


UpsertBatch = Callable[[type, list[dict[str, Any]], DatasetScope], Awaitable[int]]
CancelCheck = Callable[[], Awaitable[None]]
Progress = Callable[[dict[str, Any]], Awaitable[None]]


def _quote(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def _table(schema: str, name: str) -> str:
    return f"{_quote(schema)}.{_quote(name)}"


def _fields(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    return dict(mapping) if mapping is not None else dict(row)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


async def rehydrate_current_dataset(
    database: Any,
    schema: str,
    models_by_type: Mapping[str, type],
    upsert_batch: UpsertBatch,
    *,
    source_id: str,
    dataset_id: str,
    acquisition_root_run_id: str,
    owner_run_id: str,
    resource_types: tuple[str, ...] = (),
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancel_check: CancelCheck | None = None,
    progress: Progress | None = None,
) -> dict[str, Any]:
    """Restore one current published dataset without network I/O.

    Each batch writes typed rows, canonical rows/source edges, and its checkpoint
    under one database transaction. An interrupted run resumes at the committed
    resource-id cursor; a completed run is read-only after its exact proof.
    """
    identity = (source_id, dataset_id, acquisition_root_run_id, owner_run_id)
    if any(not _clean(value) for value in identity):
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_identity_required")
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_batch_size_invalid")

    lock_name = "provider-directory-dataset-rehydrate:" + ":".join(identity[:3])
    async with database.acquire() as connection:
        acquired = await connection.scalar(
            "SELECT pg_try_advisory_lock(hashtextextended(:identity, 0));",
            identity=lock_name,
        )
        if acquired is not True:
            raise DatasetRehydrationError("provider_directory_dataset_rehydrate_scope_busy")
        try:
            scope = await _load_scope(
                database, schema, source_id, dataset_id, acquisition_root_run_id
            )
            selected_types = resource_types or scope.resource_types
            if len(selected_types) != len(set(selected_types)) or any(
                resource_type not in models_by_type or resource_type not in scope.resource_types
                for resource_type in selected_types
            ):
                raise DatasetRehydrationError("provider_directory_dataset_rehydrate_resource_scope_invalid")
            before_hash = await _verify_hash(database, schema, scope, cancel_check)
            results: dict[str, dict[str, Any]] = {}
            for resource_type in selected_types:
                results[resource_type] = await _rehydrate_type(
                    database, schema, scope, resource_type, models_by_type[resource_type],
                    upsert_batch, owner_run_id, batch_size, cancel_check, progress,
                )
            after_hash = await _verify_hash(database, schema, scope, cancel_check)
        finally:
            await connection.scalar(
                "SELECT pg_advisory_unlock(hashtextextended(:identity, 0));",
                identity=lock_name,
            )
    rejected_count = sum(item["rejected"] for item in results.values())
    return {
        "mode": "provider_directory_dataset_rehydrate",
        "status": "rejected" if rejected_count else "complete",
        "network_calls": 0,
        "source_id": source_id,
        "endpoint_id": scope.endpoint_id,
        "dataset_id": dataset_id,
        "acquisition_root_run_id": acquisition_root_run_id,
        "owner_run_id": owner_run_id,
        "dataset_hash": scope.dataset_hash,
        "dataset_hash_verified_before": before_hash,
        "dataset_hash_verified_after": after_hash,
        "batch_size": batch_size,
        "dataset_resource_count": scope.resource_count,
        "input_count": sum(item["input"] for item in results.values()),
        "mapped_count": sum(item["mapped"] for item in results.values()),
        "rejected_count": rejected_count,
        "resources": results,
    }


async def _load_scope(database: Any, schema: str, source_id: str, dataset_id: str, root: str) -> DatasetScope:
    row = await database.first(
        f"""
        SELECT source_record.endpoint_id AS source_endpoint_id,
               source_record.canonical_api_base AS source_base,
               endpoint_record.endpoint_id, endpoint_record.canonical_api_base,
               dataset_record.acquisition_root_run_id, dataset_record.dataset_hash,
               dataset_record.resource_count, dataset_record.status, dataset_record.is_current,
               dataset_record.superseded_at, dataset_record.published_at,
               dataset_record.publication_metadata_json,
               (SELECT count(*) FROM {_table(schema, DATASET_TABLE)} current_dataset
                 WHERE current_dataset.endpoint_id = endpoint_record.endpoint_id
                   AND current_dataset.is_current = true) AS current_count
          FROM {_table(schema, SOURCE_TABLE)} source_record
          JOIN {_table(schema, ENDPOINT_TABLE)} endpoint_record
            ON endpoint_record.endpoint_id = source_record.endpoint_id
          JOIN {_table(schema, DATASET_TABLE)} dataset_record
            ON dataset_record.endpoint_id = endpoint_record.endpoint_id
           AND dataset_record.dataset_id = :dataset_id
         WHERE source_record.source_id = :source_id;
        """,
        source_id=source_id, dataset_id=dataset_id,
    )
    if row is None:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_scope_not_found")
    fields = _fields(row)
    metadata = fields.get("publication_metadata_json")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = None
    types = metadata.get("selected_resources") if isinstance(metadata, dict) else None
    source_ids = metadata.get("source_ids") if isinstance(metadata, dict) else None
    invalid = (
        _clean(fields.get("source_endpoint_id")) != _clean(fields.get("endpoint_id")),
        _clean(fields.get("source_base")).rstrip("/") != _clean(fields.get("canonical_api_base")).rstrip("/"),
        _clean(fields.get("acquisition_root_run_id")) != root,
        _clean(fields.get("status")) != "published",
        fields.get("is_current") is not True,
        int(fields.get("current_count") or 0) != 1,
        fields.get("superseded_at") is not None,
        fields.get("published_at") is None,
        not isinstance(types, list) or not types or len(types) != len(set(types)),
        not isinstance(source_ids, list) or source_id not in source_ids or len(source_ids) != len(set(source_ids)),
        re.fullmatch(r"[0-9a-f]{64}", _clean(fields.get("dataset_hash"))) is None,
    )
    if any(invalid):
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_scope_not_current")
    return DatasetScope(
        source_id=source_id, dataset_id=dataset_id, acquisition_root_run_id=root,
        endpoint_id=_clean(fields.get("endpoint_id")),
        canonical_api_base=_clean(fields.get("canonical_api_base")).rstrip("/"),
        dataset_hash=_clean(fields.get("dataset_hash")),
        resource_count=int(fields.get("resource_count") or 0),
        resource_types=tuple(types),
    )


async def _verify_hash(database: Any, schema: str, scope: DatasetScope, cancel_check: CancelCheck | None) -> str:
    digest = hashlib.sha256()
    cursor: tuple[str, str] | None = None
    total = 0
    while True:
        if cancel_check:
            await cancel_check()
        filter_sql = "" if cursor is None else "AND (resource_type, resource_id) > (:after_type, :after_id)"
        rows = await database.all(
            f"""SELECT resource_type, resource_id, payload_hash
                  FROM {_table(schema, DATASET_RESOURCE_TABLE)}
                 WHERE dataset_id = :dataset_id AND resource_type NOT LIKE 'LU:%:pass:%'
                 {filter_sql}
                 ORDER BY resource_type, resource_id LIMIT 10000;""",
            dataset_id=scope.dataset_id,
            **({} if cursor is None else {"after_type": cursor[0], "after_id": cursor[1]}),
        )
        if not rows:
            break
        for row in rows:
            item = _fields(row)
            identity = (_clean(item.get("resource_type")), _clean(item.get("resource_id")), _clean(item.get("payload_hash")))
            if not identity[0] or not identity[1] or re.fullmatch(r"[0-9a-f]{64}", identity[2]) is None:
                raise DatasetRehydrationError("provider_directory_dataset_rehydrate_hash_identity_invalid")
            if total:
                digest.update(b"\n")
            digest.update(json.dumps(identity, separators=(",", ":")).encode())
            total += 1
            cursor = identity[:2]
        if len(rows) < 10000:
            break
    calculated = digest.hexdigest()
    if total != scope.resource_count or calculated != scope.dataset_hash:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_dataset_hash_mismatch")
    return calculated


async def _rehydrate_type(database: Any, schema: str, scope: DatasetScope, resource_type: str, model: type, upsert_batch: UpsertBatch, owner_run_id: str, batch_size: int, cancel_check: CancelCheck | None, progress: Progress | None) -> dict[str, Any]:
    expected = int(await database.scalar(
        f"SELECT count(*) FROM {_table(schema, DATASET_RESOURCE_TABLE)} WHERE dataset_id=:dataset_id AND resource_type=:resource_type;",
        dataset_id=scope.dataset_id, resource_type=resource_type,
    ) or 0)
    checkpoint = await _checkpoint(database, schema, scope, resource_type)
    proof = await _proof(database, schema, scope, resource_type, model.__tablename__)
    if checkpoint and checkpoint["state"] == "complete" and _proof_complete(checkpoint, proof, expected):
        return {**checkpoint, **proof, "reused_complete_checkpoint": True}
    cursor = checkpoint.get("last_resource_id") if checkpoint and checkpoint["state"] in {"running", "interrupted"} else None
    input_count = int(checkpoint.get("input", 0)) if cursor else 0
    mapped_count = int(checkpoint.get("mapped", 0)) if cursor else 0
    rejected_count = int(checkpoint.get("rejected", 0)) if cursor else 0
    if cursor:
        prefix = int(await database.scalar(
            f"SELECT count(*) FROM {_table(schema, DATASET_RESOURCE_TABLE)} WHERE dataset_id=:dataset_id AND resource_type=:resource_type AND resource_id<=:cursor;",
            dataset_id=scope.dataset_id, resource_type=resource_type, cursor=cursor,
        ) or 0)
        if prefix != input_count:
            cursor, input_count, mapped_count, rejected_count = None, 0, 0, 0
    await _save_checkpoint(database, schema, scope, resource_type, owner_run_id, "running", cursor, expected, input_count, mapped_count, rejected_count, {})
    while input_count < expected:
        if cancel_check:
            await cancel_check()
        async with database.transaction():
            rows = await database.all(
                f"""SELECT resource_id, payload_hash, payload_json FROM {_table(schema, DATASET_RESOURCE_TABLE)}
                      WHERE dataset_id=:dataset_id AND resource_type=:resource_type
                        AND (:cursor IS NULL OR resource_id > :cursor)
                      ORDER BY resource_id LIMIT :batch_size;""",
                dataset_id=scope.dataset_id, resource_type=resource_type, cursor=cursor, batch_size=batch_size,
            )
            if not rows:
                raise DatasetRehydrationError("provider_directory_dataset_rehydrate_input_ended_early")
            typed_rows = []
            rejected: list[str] = []
            for row in rows:
                item = _fields(row)
                payload = item.get("payload_json")
                reason = _validate_payload(model, _clean(item.get("resource_id")), _clean(item.get("payload_hash")), payload)
                if reason:
                    rejected.append(reason)
                else:
                    typed_rows.append({**payload, "source_id": scope.source_id, "last_seen_run_id": scope.acquisition_root_run_id})
            last_id = _clean(_fields(rows[-1]).get("resource_id"))
            input_count += len(rows)
            rejected_count += len(rejected)
            if rejected:
                evidence = {"rejections": sorted(set(rejected)), "rejected_in_batch": len(rejected)}
                await _save_checkpoint(database, schema, scope, resource_type, owner_run_id, "rejected", last_id, expected, input_count, mapped_count, rejected_count, evidence)
                raise DatasetRehydrationError("provider_directory_dataset_rehydrate_payload_rejected")
            written = await upsert_batch(model, typed_rows, scope)
            if written != len(typed_rows):
                raise DatasetRehydrationError("provider_directory_dataset_rehydrate_upsert_count_mismatch")
            mapped_count += written
            cursor = last_id
            await _save_checkpoint(database, schema, scope, resource_type, owner_run_id, "running", cursor, expected, input_count, mapped_count, rejected_count, {})
        if progress:
            await progress({"phase": f"rehydrating {resource_type}", "unit": "rows", "done": input_count, "total": expected, "pct": round(100 * input_count / max(expected, 1), 2)})
    proof = await _proof(database, schema, scope, resource_type, model.__tablename__)
    state = "complete" if mapped_count == expected and rejected_count == 0 and _proof_complete({"input": input_count, "mapped": mapped_count, "rejected": rejected_count}, proof, expected) else "proof_failed"
    await _save_checkpoint(database, schema, scope, resource_type, owner_run_id, state, cursor, expected, input_count, mapped_count, rejected_count, proof)
    if state != "complete":
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_proof_failed")
    return {"state": state, "input": input_count, "mapped": mapped_count, "rejected": rejected_count, **proof, "reused_complete_checkpoint": False}


def _validate_payload(model: type, resource_id: str, stored_hash: str, payload: Any) -> str | None:
    if not resource_id or not isinstance(payload, dict) or _payload_hash(payload) != stored_hash:
        return "payload_hash_mismatch"
    if payload.get("resource_id") != resource_id or {"source_id", "last_seen_run_id", "observed_at", "updated_at"} & set(payload):
        return "payload_provenance_invalid"
    columns = {column.name: column for column in model.__table__.columns}
    if set(payload) - set(columns):
        return "payload_unknown_field"
    for name, value in payload.items():
        column_type = columns[name].type
        if isinstance(column_type, sa_types.String) and value is not None and not isinstance(value, str):
            return "payload_column_type_invalid"
        if isinstance(column_type, sa_types.Boolean) and value is not None and not isinstance(value, bool):
            return "payload_column_type_invalid"
    return None


async def _checkpoint(database: Any, schema: str, scope: DatasetScope, resource_type: str) -> dict[str, Any] | None:
    row = await database.first(f"SELECT endpoint_id,dataset_hash,state,last_resource_id,input_count,mapped_count,rejected_count FROM {_table(schema, CHECKPOINT_TABLE)} WHERE source_id=:source_id AND dataset_id=:dataset_id AND acquisition_root_run_id=:root AND resource_type=:resource_type;", source_id=scope.source_id, dataset_id=scope.dataset_id, root=scope.acquisition_root_run_id, resource_type=resource_type)
    if row is None:
        return None
    fields = _fields(row)
    if _clean(fields.get("endpoint_id")) != scope.endpoint_id or _clean(fields.get("dataset_hash")) != scope.dataset_hash:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_checkpoint_scope_mismatch")
    return {"state": _clean(fields.get("state")), "last_resource_id": _clean(fields.get("last_resource_id")) or None, "input": int(fields.get("input_count") or 0), "mapped": int(fields.get("mapped_count") or 0), "rejected": int(fields.get("rejected_count") or 0)}


async def _save_checkpoint(database: Any, schema: str, scope: DatasetScope, resource_type: str, owner: str, state: str, cursor: str | None, expected: int, input_count: int, mapped: int, rejected: int, evidence: dict[str, Any]) -> None:
    await database.status(f"""INSERT INTO {_table(schema, CHECKPOINT_TABLE)} (source_id,dataset_id,acquisition_root_run_id,resource_type,endpoint_id,dataset_hash,owner_run_id,state,last_resource_id,expected_input_count,input_count,mapped_count,rejected_count,evidence_json,created_at,started_at,updated_at,completed_at) VALUES (:source_id,:dataset_id,:root,:resource_type,:endpoint_id,:dataset_hash,:owner,:state,:cursor,:expected,:input,:mapped,:rejected,CAST(:evidence AS jsonb),now(),now(),now(),CASE WHEN :state='complete' THEN now() END) ON CONFLICT (source_id,dataset_id,acquisition_root_run_id,resource_type) DO UPDATE SET owner_run_id=EXCLUDED.owner_run_id,state=EXCLUDED.state,last_resource_id=EXCLUDED.last_resource_id,expected_input_count=EXCLUDED.expected_input_count,input_count=EXCLUDED.input_count,mapped_count=EXCLUDED.mapped_count,rejected_count=EXCLUDED.rejected_count,evidence_json=EXCLUDED.evidence_json,updated_at=now(),completed_at=CASE WHEN EXCLUDED.state='complete' THEN now() ELSE NULL END;""", source_id=scope.source_id, dataset_id=scope.dataset_id, root=scope.acquisition_root_run_id, resource_type=resource_type, endpoint_id=scope.endpoint_id, dataset_hash=scope.dataset_hash, owner=owner, state=state, cursor=cursor, expected=expected, input=input_count, mapped=mapped, rejected=rejected, evidence=json.dumps(evidence, sort_keys=True))


async def _proof(database: Any, schema: str, scope: DatasetScope, resource_type: str, typed_table: str) -> dict[str, int]:
    row = await database.first(f"""SELECT count(*)::bigint AS input, count(typed.resource_id) FILTER (WHERE typed.last_seen_run_id=:root)::bigint AS typed, GREATEST((SELECT count(*) FROM {_table(schema, typed_table)} extra WHERE extra.source_id=:source_id AND extra.last_seen_run_id=:root)-count(typed.resource_id) FILTER (WHERE typed.last_seen_run_id=:root),0)::bigint AS typed_extra, count(canonical.resource_id) FILTER (WHERE canonical.last_seen_run_id=:root AND canonical.payload_hash=dataset.payload_hash)::bigint AS canonical_hash_matched, count(edge.resource_id) FILTER (WHERE edge.last_seen_run_id=:root)::bigint AS source_edges, GREATEST((SELECT count(*) FROM {_table(schema, SOURCE_EDGE_TABLE)} extra_edge WHERE extra_edge.source_id=:source_id AND extra_edge.resource_type=:resource_type AND extra_edge.last_seen_run_id=:root)-count(edge.resource_id) FILTER (WHERE edge.last_seen_run_id=:root),0)::bigint AS source_edge_extra FROM {_table(schema, DATASET_RESOURCE_TABLE)} dataset LEFT JOIN {_table(schema, typed_table)} typed ON typed.source_id=:source_id AND typed.resource_id=dataset.resource_id LEFT JOIN {_table(schema, CANONICAL_TABLE)} canonical ON canonical.canonical_api_base=:canonical_base AND canonical.resource_type=dataset.resource_type AND canonical.resource_id=dataset.resource_id LEFT JOIN {_table(schema, SOURCE_EDGE_TABLE)} edge ON edge.source_id=:source_id AND edge.canonical_api_base=:canonical_base AND edge.resource_type=dataset.resource_type AND edge.resource_id=dataset.resource_id WHERE dataset.dataset_id=:dataset_id AND dataset.resource_type=:resource_type;""", source_id=scope.source_id, dataset_id=scope.dataset_id, resource_type=resource_type, root=scope.acquisition_root_run_id, canonical_base=scope.canonical_api_base)
    return {name: int(value or 0) for name, value in _fields(row).items()}


def _proof_complete(checkpoint: dict[str, Any], proof: dict[str, int], expected: int) -> bool:
    return checkpoint.get("input") == expected and checkpoint.get("mapped") == expected and checkpoint.get("rejected", 0) == 0 and proof["input"] == expected and proof["typed"] == expected and proof["typed_extra"] == 0 and proof["canonical_hash_matched"] == expected and proof["source_edges"] == expected and proof["source_edge_extra"] == 0
