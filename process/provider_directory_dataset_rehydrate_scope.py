# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current-dataset scope and retained-digest fencing."""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
from typing import Any, AsyncIterator

from process.provider_directory_dataset_rehydrate_types import (
    DATASET_RESOURCE_TABLE,
    DATASET_TABLE,
    ENDPOINT_TABLE,
    SOURCE_TABLE,
    DatasetRehydrationError,
    DatasetScope,
    RehydrationRequest,
    RehydrationRuntime,
    _clean_text,
    _payload_hash,
    _record_fields,
    _table_ref,
)


@contextlib.asynccontextmanager
async def _dataset_guard(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
) -> AsyncIterator[None]:
    """Hold a session advisory lock for one exact dataset lineage."""
    lock_identity = (
        "provider-directory-dataset-rehydrate:"
        f"{request.source_id}:{request.dataset_id}:"
        f"{request.acquisition_root_run_id}"
    )
    async with runtime.database.acquire() as database_connection:
        is_acquired = await database_connection.scalar(
            "SELECT pg_try_advisory_lock(hashtextextended(:identity, 0));",
            identity=lock_identity,
        )
        if is_acquired is not True:
            raise DatasetRehydrationError(
                "provider_directory_dataset_rehydrate_scope_busy"
            )
        try:
            yield
        finally:
            await database_connection.scalar(
                "SELECT pg_advisory_unlock(hashtextextended(:identity, 0));",
                identity=lock_identity,
            )


async def _load_dataset_scope(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    *,
    lock: bool = False,
) -> DatasetScope:
    """Load and validate one source-bound current published dataset."""
    scope_record = await runtime.database.first(
        _scope_sql(runtime.schema, lock),
        source_id=request.source_id,
        dataset_id=request.dataset_id,
    )
    if scope_record is None:
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_scope_not_found"
        )
    return _decode_dataset_scope(request, _record_fields(scope_record))


def _scope_sql(schema: str, lock: bool) -> str:
    """Build the source, endpoint, and current-dataset fence query."""
    lock_clause = (
        "FOR SHARE OF source_record, endpoint_record, dataset_record"
        if lock
        else ""
    )
    return f"""
        SELECT source_record.endpoint_id AS source_endpoint_id,
               source_record.canonical_api_base AS source_base,
               endpoint_record.endpoint_id, endpoint_record.canonical_api_base,
               dataset_record.acquisition_root_run_id, dataset_record.dataset_hash,
               dataset_record.resource_count, dataset_record.status,
               dataset_record.is_current, dataset_record.superseded_at,
               dataset_record.published_at,
               dataset_record.publication_metadata_json,
               current_state.current_count
          FROM {_table_ref(schema, SOURCE_TABLE)} source_record
          JOIN {_table_ref(schema, ENDPOINT_TABLE)} endpoint_record
            ON endpoint_record.endpoint_id = source_record.endpoint_id
          JOIN {_table_ref(schema, DATASET_TABLE)} dataset_record
            ON dataset_record.endpoint_id = endpoint_record.endpoint_id
           AND dataset_record.dataset_id = :dataset_id
          CROSS JOIN LATERAL (
                SELECT count(*)::bigint AS current_count
                  FROM {_table_ref(schema, DATASET_TABLE)} current_dataset
                 WHERE current_dataset.endpoint_id = endpoint_record.endpoint_id
                   AND current_dataset.is_current = true
          ) current_state
         WHERE source_record.source_id = :source_id
         {lock_clause};
    """


def _decode_dataset_scope(
    request: RehydrationRequest,
    scope_fields_by_name: dict[str, Any],
) -> DatasetScope:
    """Decode publication metadata and reject an inexact scope."""
    publication_metadata = _json_object(
        scope_fields_by_name.get("publication_metadata_json")
    )
    selected_resource_types = publication_metadata.get("selected_resources")
    published_source_ids = publication_metadata.get("source_ids")
    if _is_scope_invalid(
        request,
        scope_fields_by_name,
        selected_resource_types,
        published_source_ids,
    ):
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_scope_not_current"
        )
    return DatasetScope(
        source_id=request.source_id,
        dataset_id=request.dataset_id,
        acquisition_root_run_id=request.acquisition_root_run_id,
        endpoint_id=_clean_text(scope_fields_by_name.get("endpoint_id")),
        canonical_api_base=_clean_text(
            scope_fields_by_name.get("canonical_api_base")
        ).rstrip("/"),
        dataset_hash=_clean_text(scope_fields_by_name.get("dataset_hash")),
        resource_count=int(scope_fields_by_name.get("resource_count") or 0),
        resource_types=tuple(selected_resource_types),
        publication_metadata_hash=_payload_hash(publication_metadata),
        published_at=scope_fields_by_name.get("published_at"),
    )


def _json_object(json_value: Any) -> dict[str, Any]:
    """Decode a JSON object and reject malformed or non-object values."""
    decoded_json = json_value
    if isinstance(decoded_json, str):
        try:
            decoded_json = json.loads(decoded_json)
        except json.JSONDecodeError:
            return {}
    return decoded_json if isinstance(decoded_json, dict) else {}


def _is_scope_invalid(
    request: RehydrationRequest,
    scope_fields_by_name: dict[str, Any],
    selected_resource_types: Any,
    published_source_ids: Any,
) -> bool:
    """Return whether any immutable current-dataset invariant fails."""
    endpoint_id = _clean_text(scope_fields_by_name.get("endpoint_id"))
    endpoint_base = _clean_text(
        scope_fields_by_name.get("canonical_api_base")
    ).rstrip("/")
    return any(
        (
            _clean_text(scope_fields_by_name.get("source_endpoint_id")) != endpoint_id,
            _clean_text(scope_fields_by_name.get("source_base")).rstrip("/")
            != endpoint_base,
            _clean_text(scope_fields_by_name.get("acquisition_root_run_id"))
            != request.acquisition_root_run_id,
            _clean_text(scope_fields_by_name.get("status")) != "published",
            scope_fields_by_name.get("is_current") is not True,
            int(scope_fields_by_name.get("current_count") or 0) != 1,
            scope_fields_by_name.get("superseded_at") is not None,
            scope_fields_by_name.get("published_at") is None,
            not _is_unique_text_list(selected_resource_types),
            not _is_unique_text_list(published_source_ids),
            request.source_id not in (published_source_ids or ()),
            re.fullmatch(
                r"[0-9a-f]{64}",
                _clean_text(scope_fields_by_name.get("dataset_hash")),
            )
            is None,
        )
    )


def _is_unique_text_list(candidate_values: Any) -> bool:
    """Return whether a value is a nonempty duplicate-free text list."""
    return (
        isinstance(candidate_values, list)
        and bool(candidate_values)
        and all(isinstance(candidate_value, str) for candidate_value in candidate_values)
        and len(candidate_values) == len(set(candidate_values))
    )


def _selected_resource_types(
    runtime: RehydrationRuntime,
    request: RehydrationRequest,
    scope: DatasetScope,
) -> tuple[str, ...]:
    """Resolve and validate the requested resource subset."""
    selected_resource_types = request.resource_types or scope.resource_types
    if len(selected_resource_types) != len(set(selected_resource_types)) or any(
        resource_type not in runtime.models_by_type
        or resource_type not in scope.resource_types
        for resource_type in selected_resource_types
    ):
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_resource_scope_invalid"
        )
    return selected_resource_types


async def _verify_dataset_digest(
    runtime: RehydrationRuntime,
    scope: DatasetScope,
) -> str:
    """Stream and verify the ordered retained-resource digest."""
    dataset_digest = hashlib.sha256()
    digest_cursor: tuple[str, str] | None = None
    processed_count = 0
    while True:
        await _check_cancel(runtime)
        digest_rows = await _read_digest_page(runtime, scope, digest_cursor)
        if not digest_rows:
            break
        processed_count, digest_cursor = _append_digest_rows(
            dataset_digest, digest_rows, processed_count
        )
        if len(digest_rows) < 10_000:
            break
    calculated_digest = dataset_digest.hexdigest()
    if processed_count != scope.resource_count or calculated_digest != scope.dataset_hash:
        raise DatasetRehydrationError(
            "provider_directory_dataset_rehydrate_dataset_hash_mismatch"
        )
    return calculated_digest


async def _read_digest_page(
    runtime: RehydrationRuntime,
    scope: DatasetScope,
    digest_cursor: tuple[str, str] | None,
) -> list[Any]:
    """Read one keyset page of retained hash identities."""
    cursor_filter = ""
    query_params_by_name: dict[str, Any] = {"dataset_id": scope.dataset_id}
    if digest_cursor is not None:
        cursor_filter = "AND (resource_type, resource_id) > (:after_type, :after_id)"
        query_params_by_name.update(
            after_type=digest_cursor[0], after_id=digest_cursor[1]
        )
    return await runtime.database.all(
        f"""SELECT resource_type, resource_id, payload_hash
              FROM {_table_ref(runtime.schema, DATASET_RESOURCE_TABLE)}
             WHERE dataset_id = :dataset_id
               AND resource_type NOT LIKE 'LU:%:pass:%'
               {cursor_filter}
             ORDER BY resource_type, resource_id
             LIMIT 10000;""",
        **query_params_by_name,
    )


def _append_digest_rows(
    dataset_digest: Any,
    digest_rows: list[Any],
    processed_count: int,
) -> tuple[int, tuple[str, str]]:
    """Append one page to the stable dataset digest."""
    digest_cursor = ("", "")
    for digest_record in digest_rows:
        digest_fields = _record_fields(digest_record)
        digest_identity_parts = tuple(
            _clean_text(digest_fields.get(field_name))
            for field_name in ("resource_type", "resource_id", "payload_hash")
        )
        if (
            not digest_identity_parts[0]
            or not digest_identity_parts[1]
            or re.fullmatch(r"[0-9a-f]{64}", digest_identity_parts[2]) is None
        ):
            raise DatasetRehydrationError(
                "provider_directory_dataset_rehydrate_hash_identity_invalid"
            )
        if processed_count:
            dataset_digest.update(b"\n")
        dataset_digest.update(
            json.dumps(digest_identity_parts, separators=(",", ":")).encode()
        )
        processed_count += 1
        digest_cursor = digest_identity_parts[:2]
    return processed_count, digest_cursor

