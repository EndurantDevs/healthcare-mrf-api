# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mark an exact stopped acquisition failed without modifying its retained inputs."""

import json

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_terminal_root_retirement_contract import (
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
    json_object,
    require_terminal_root_retirement_gate,
    schema_name,
)
from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage
from process.provider_directory_terminal_root_retirement_selection import (
    _initial_endpoint,
    _lock_endpoint_scope,
    _locked_lineage,
    _locked_predecessor,
    _locked_source,
    _require_no_competing_candidate,
    _try_scope_lock,
    _validate_new_target,
)

_ADMISSION_LOCK = "import-run-admission:provider-directory-fhir"
_HEADER = "provider_directory_endpoint_dataset"


def _require(condition, code="evidence_invalid"):
    if not condition:
        raise TerminalRootRetirementError(code)


class _Executor:
    """Run existing named selection queries on the caller's transaction."""

    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def _statement(query, parameters):
        compiled = text(query).compile(dialect=dialect())
        return str(compiled), [parameters[name] for name in compiled.positiontup]

    async def all(self, query, **parameters):
        """Fetch rows and decode the JSON used by the existing run probes."""
        statement, values = self._statement(query, parameters)
        rows = [dict(row) for row in await self.connection.fetch(statement, *values)]
        for row in rows:
            for field in ("metrics", "params"):
                if field in row:
                    decoded = json.loads(row[field]) if isinstance(row[field], str) else row[field]
                    _require(decoded is None or type(decoded) is dict)
                    row[field] = decoded
        return rows

    async def scalar(self, query, **parameters):
        """Fetch one scalar from the caller's transaction."""
        statement, values = self._statement(query, parameters)
        return await self.connection.fetchval(statement, *values)


async def _header(connection, relation, request):
    row = await connection.fetchrow(
        f"SELECT dataset_id,endpoint_id,import_run_id,acquisition_root_run_id,previous_dataset_id,"
        "status,is_current,dataset_hash,validated_at,published_at,superseded_at,resource_count,"
        "completion_proof_json,completion_proof_sha256,publication_metadata_json "
        f"FROM {relation} WHERE dataset_id=$1 AND endpoint_id=$2 AND import_run_id=$3 "
        "AND acquisition_root_run_id=$4 AND previous_dataset_id IS NOT DISTINCT FROM $5 FOR UPDATE NOWAIT",
        request.dataset_id,
        request.endpoint_id,
        request.owner_run_id,
        request.acquisition_root_run_id,
        request.expected_current_dataset_id,
    )
    _require(row is not None)
    header_by_field = dict(row)
    metadata = json_object(header_by_field["publication_metadata_json"])
    _require(metadata.get("source_ids") == [request.source_id] and header_by_field["status"] in {"acquiring", "failed"})
    _validate_new_target({**header_by_field, "status": "acquiring"}, request, metadata)
    return header_by_field


async def _selection(request, connection, policy, storage_binding):
    require_terminal_root_retirement_gate()
    _require(
        type(request) is TerminalRootRetirementRequest and request.expected_evidence_sha256 is None,
        "request_invalid",
    )
    _require(connection.is_in_transaction() is True, "state_invalid")
    _require(type(storage_binding) is dict, "request_invalid")
    catalog_schema = storage_binding.get("catalog_schema")
    storage_schema = storage_binding.get("storage_schema")
    storage_identifier(catalog_schema)
    storage_identifier(storage_schema)
    _require(catalog_schema == schema_name() and storage_schema == policy.schema and catalog_schema != storage_schema)
    await connection.execute("SET LOCAL TIME ZONE 'UTC'")
    executor = _Executor(connection)
    await _try_scope_lock(executor, _ADMISSION_LOCK)
    endpoint = await _initial_endpoint(executor, request.endpoint_id)
    base = await _lock_endpoint_scope(executor, request, endpoint)
    relation = storage_relation(storage_schema, _HEADER)
    header = await _header(connection, relation, request)
    await _locked_source(executor, request, base)
    await _locked_predecessor(executor, request)
    await _require_no_competing_candidate(executor, request)
    competing = await connection.fetch(
        f"SELECT dataset_id FROM {relation} WHERE endpoint_id=$1 AND dataset_id<>$2 "
        "AND (status IN ('acquiring','incomplete') OR (is_current AND dataset_id IS DISTINCT FROM $3) "
        "OR ($4::boolean AND published_at IS NOT NULL)) ORDER BY dataset_id FOR UPDATE NOWAIT",
        request.endpoint_id,
        request.dataset_id,
        request.expected_current_dataset_id,
        request.expect_no_current_dataset,
    )
    _require(not competing)
    lineage = await _locked_lineage(executor, request)
    await connection.fetch(
        f"SELECT run_id FROM {storage_relation(catalog_schema, 'import_run')} "
        "WHERE run_id=ANY($1::varchar[]) ORDER BY run_id FOR UPDATE NOWAIT",
        [run["run_id"] for run in lineage],
    )
    for run in sorted(lineage, key=lambda row: row["run_id"]):
        await _try_scope_lock(executor, f"provider-directory-terminal-root-retirement:{run['run_id']}")
        await _try_scope_lock(executor, f"control-run-worker-action:v1:{run['run_id']}")
    _require(await _locked_lineage(executor, request) == lineage, "evidence_changed")
    await require_stopped_lineage(lineage)
    return relation, header


async def preview_stopped_source_failure(request, *, connection, policy, storage_binding):
    """Read logical candidate state under the same locks used by apply."""
    _, header = await _selection(request, connection, policy, storage_binding)
    return {"candidate_status": header["status"], "would_change": header["status"] == "acquiring"}


async def apply_stopped_source_failure(request, *, connection, policy, storage_binding):
    """Change only the stopped candidate's status, with an idempotent failed-state retry."""
    relation, header = await _selection(request, connection, policy, storage_binding)
    if header["status"] == "failed":
        return {"changed": False}
    updated = await connection.execute(
        f"UPDATE {relation} SET status='failed' WHERE dataset_id=$1 AND endpoint_id=$2 "
        "AND import_run_id=$3 AND acquisition_root_run_id=$4 AND status='acquiring' AND NOT is_current",
        request.dataset_id,
        request.endpoint_id,
        request.owner_run_id,
        request.acquisition_root_run_id,
    )
    _require(updated == "UPDATE 1", "state_invalid")
    _require((await _header(connection, relation, request))["status"] == "failed", "state_invalid")
    return {"changed": True}


__all__ = ("apply_stopped_source_failure", "preview_stopped_source_failure")
