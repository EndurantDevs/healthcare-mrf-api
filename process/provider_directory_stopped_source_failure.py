# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reconcile one stopped native acquisition while preserving its retained inputs.

The caller acquires the existing global admission session lock before opening
its serializable administrator transaction and holds it through commit or rollback.
It authenticates the issued manifest before and after this operation and rolls
back the transaction on failure.
"""

import re
from datetime import datetime
from hashlib import sha256

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS, capture_checkpoint_controls
from process.provider_directory_source_proof_control import PROOF_ROOT, verify_proof_control
from process.provider_directory_source_storage_binding import (
    FHIRSourceStorageBinding,
    verify_isolated_source_storage_coordinates,
)
from process.provider_directory_terminal_root_retirement_contract import (
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
    canonical_json_sha256,
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

STOPPED_SOURCE_FAILURE_METADATA_KEY = "provider_directory_stopped_source_failure_v1"
_CONTRACT = "healthporta.provider-directory.stopped-source-failure.v1"
_MARKER_FIELDS = {"contract_version", "evidence", "evidence_sha256", "reconciled_at"}
_ADMISSION_LOCK = "import-run-admission:provider-directory-fhir"
_ROW_CHUNK_ROWS = 4096
_CHUNK_PREFETCH = 64
_ROW_HASH_DOMAIN = b"provider-directory-stopped-source-row-chunks/v1\0"
_SEAL_FIELDS = (
    "artifact_selection_receipt_json",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
)


def _require(condition, code="evidence_invalid"):
    if not condition:
        raise TerminalRootRetirementError(code)


class _Executor:
    """Use the existing named-query selection helpers on the caller's asyncpg transaction."""

    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def _statement(query, parameters):
        compiled = text(query).compile(dialect=dialect())
        return str(compiled), [parameters[name] for name in compiled.positiontup]

    async def all(self, query, **parameters):
        """Fetch rows through the original supplied connection."""
        statement, values = self._statement(query, parameters)
        rows = [dict(row) for row in await self.connection.fetch(statement, *values)]
        for row in rows:
            for field in ("metrics", "params"):
                if field in row:
                    row[field] = storage._json(row[field])
                    _require(row[field] is None or type(row[field]) is dict)
        return rows

    async def scalar(self, query, **parameters):
        """Fetch one value through the original supplied connection."""
        statement, values = self._statement(query, parameters)
        return await self.connection.fetchval(statement, *values)


async def _administrator(connection, policy):
    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction() is True, "administrator_required")
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS administrator,"
        "current_setting('role') AS role,current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS readonly,"
        "current_setting('session_replication_role') AS replication,"
        "pg_backend_pid() AS backend,pg_current_xact_id()::text AS transaction_id,"
        "EXISTS(SELECT FROM pg_catalog.pg_locks WHERE locktype='advisory' AND granted "
        "AND mode='ExclusiveLock' AND pid=pg_backend_pid() AND objsubid=1 "
        "AND classid=((hashtextextended($1::text,0)>>32)&4294967295)::oid "
        "AND objid=(hashtextextended($1::text,0)&4294967295)::oid) AS admission_fenced",
        _ADMISSION_LOCK,
    )
    _require(
        actor is not None
        and actor["session"] == actor["current"]
        and actor["administrator"] is True
        and actor["current"] not in {policy.owner_role, policy.publisher_role, *policy.runtime_roles}
        and actor["role"] == "none",
        "administrator_required",
    )
    _require(
        actor["isolation"] == "serializable" and actor["readonly"] == "off" and actor["replication"] == "origin",
        "state_invalid",
    )
    _require(actor["admission_fenced"] is True, "busy")
    return dict(actor)


def _storage_binding(policy, document):
    fields = set(FHIRSourceStorageBinding.__dataclass_fields__)
    _require(type(document) is dict and set(document) == fields | {"producer"})
    producer = document["producer"]
    _require(
        type(producer) is list
        and len(producer) == 2
        and all(type(field_value) is str for field_value in producer)
        and re.fullmatch(r"[0-9a-f]{40}", producer[0]) is not None
        and re.fullmatch(r"sha256:[0-9a-f]{64}", producer[1]) is not None
    )
    binding = FHIRSourceStorageBinding(**{field: document[field] for field in fields})
    _require(policy.schema == binding.storage_schema and schema_name() == binding.catalog_schema)
    return binding


async def _header(connection, policy, request):
    row = await connection.fetchrow(
        f"SELECT to_jsonb(dataset) AS header FROM {storage_relation(policy.schema, storage.HEADER)} dataset "
        "WHERE dataset_id=$1 AND endpoint_id=$2 AND import_run_id=$3 AND acquisition_root_run_id=$4 "
        "AND previous_dataset_id IS NOT DISTINCT FROM $5 FOR UPDATE NOWAIT",
        request.dataset_id,
        request.endpoint_id,
        request.owner_run_id,
        request.acquisition_root_run_id,
        request.expected_current_dataset_id,
    )
    _require(row is not None)
    return storage._json(row["header"])


def _candidate(header, request, storage_document):
    metadata = storage._json(header.get("publication_metadata_json"))
    _require(type(metadata) is dict and metadata.get("source_ids") == [request.source_id])
    marker_by_field = metadata.get(STOPPED_SOURCE_FAILURE_METADATA_KEY)
    _require(
        header["status"] == "acquiring"
        and STOPPED_SOURCE_FAILURE_METADATA_KEY not in metadata
        or header["status"] == "failed"
        and type(marker_by_field) is dict
    )
    original_metadata_by_field = {
        key: field_value for key, field_value in metadata.items() if key != STOPPED_SOURCE_FAILURE_METADATA_KEY
    }
    original_header_by_field = {
        **header,
        "status": "acquiring",
        "publication_metadata_json": original_metadata_by_field,
    }
    # A required version declares future validation before proof material exists.
    _validate_new_target(
        {**original_header_by_field, "completion_proof_required_version": None}, request, original_metadata_by_field
    )
    _require(all(header.get(field) is None for field in _SEAL_FIELDS))
    candidate = storage._json(header.get("candidate_storage_json"))
    _require(type(candidate) is dict and candidate.get("contract") == storage.ISOLATED_CONTRACT)
    identity = candidate.get("identity")
    storage._validate_binding_shape(candidate, identity, phases=("loading", "prepared"))
    storage._validate_binding_tables(candidate, identity)
    storage._validate_isolated_binding_storage(candidate, storage_document)
    _require(
        all(identity[field] == header[field] for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id"))
        and all(original_metadata_by_field.get(key) == field_value for key, field_value in identity["metadata"].items())
    )
    return candidate, original_header_by_field, marker_by_field


async def _private_admission(connection, policy, request):
    header = storage_relation(policy.schema, storage.HEADER)
    competing = await connection.fetch(
        f"SELECT dataset_id FROM {header} WHERE endpoint_id=$1 AND dataset_id<>$2 "
        "AND (status IN ('acquiring','incomplete') OR (is_current AND dataset_id IS DISTINCT FROM $3) "
        "OR ($4::boolean AND published_at IS NOT NULL)) "
        "ORDER BY dataset_id FOR UPDATE NOWAIT",
        request.endpoint_id,
        request.dataset_id,
        request.expected_current_dataset_id,
        request.expect_no_current_dataset,
    )
    _require(not competing)


async def _relation_evidence(connection, relation, where, arguments, *, join="", lock_rows=False):
    """Stream bounded ordered row-hash chunks using the existing native receipt pattern."""
    lock = "FOR UPDATE OF row NOWAIT" if lock_rows else ""
    query = f"""
        WITH row_hashes AS MATERIALIZED (
            SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       pg_catalog.to_jsonb(scoped_row)::text,'UTF8')),'hex') AS row_sha256
              FROM (SELECT row.* FROM {relation} row {join} WHERE {where} {lock}) scoped_row
        ), ordered AS (
            SELECT row_sha256,pg_catalog.row_number() OVER(ORDER BY row_sha256 COLLATE "C")-1 AS ordinal FROM row_hashes
        )
        SELECT ordinal/{_ROW_CHUNK_ROWS} AS chunk_ordinal,pg_catalog.count(*)::bigint AS row_count,
               pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                   pg_catalog.string_agg(row_sha256,'' ORDER BY ordinal),'UTF8')),'hex') AS row_sha256
          FROM ordered GROUP BY ordinal/{_ROW_CHUNK_ROWS} ORDER BY chunk_ordinal
    """
    digest = sha256(_ROW_HASH_DOMAIN)
    row_count, expected_ordinal = 0, 0
    previous_count = _ROW_CHUNK_ROWS
    async for chunk in connection.cursor(query, *arguments, prefetch=_CHUNK_PREFETCH):
        count, chunk_digest = chunk["row_count"], chunk["row_sha256"]
        _require(
            chunk["chunk_ordinal"] == expected_ordinal
            and type(count) is int
            and 0 < count <= _ROW_CHUNK_ROWS
            and previous_count == _ROW_CHUNK_ROWS
            and type(chunk_digest) is str
            and re.fullmatch(r"[0-9a-f]{64}", chunk_digest) is not None
        )
        digest.update(count.to_bytes(4, "big"))
        digest.update(bytes.fromhex(chunk_digest))
        row_count += count
        expected_ordinal += 1
        previous_count = count
    return {"row_count": row_count, "row_sha256": digest.hexdigest()}


async def _storage_controls(connection, policy, binding, candidate):
    for table in sorted(proof["name"] for proof in candidate["tables"].values()):
        await connection.execute(f"LOCK TABLE {storage_relation(policy.schema, table)} IN SHARE MODE NOWAIT")
    controls = ",".join(storage_relation(policy.schema, root) for root in (*CHECKPOINT_LAYOUTS, PROOF_ROOT))
    await connection.execute(f"LOCK TABLE {controls} IN ACCESS SHARE MODE NOWAIT")
    await storage._verify_tables(connection, policy, candidate)
    checkpoints = await capture_checkpoint_controls(connection, policy, storage_binding=binding)
    proof = await verify_proof_control(connection, policy, storage_binding=binding)
    return {
        "checkpoints": checkpoints,
        "proof": {
            "oid": proof["oid"],
            "layout_sha256": canonical_json_sha256(
                {key: storage._json(proof[key]) for key in ("columns", "constraints", "indexes")}
            ),
        },
    }


async def _evidence(connection, policy, binding, request, candidate, original_header_by_field, lineage):
    evidence_by_relation = {}
    for root, proof in sorted(candidate["tables"].items()):
        relation = storage_relation(policy.schema, proof["name"])
        _require(
            await connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {relation} WHERE dataset_id IS DISTINCT FROM $1)", request.dataset_id
            )
            is False
        )
        evidence_by_relation[root] = await _relation_evidence(connection, relation, "TRUE", ())
    acquisition = storage_relation(policy.schema, "provider_directory_bulk_acquisition_checkpoint")
    for root in (*CHECKPOINT_LAYOUTS, PROOF_ROOT):
        join = (
            f"JOIN {acquisition} acquisition USING(checkpoint_id)"
            if root == "provider_directory_bulk_output_checkpoint"
            else ""
        )
        where = "acquisition.dataset_id=$1" if join else "row.dataset_id=$1"
        evidence_by_relation[root] = await _relation_evidence(
            connection, storage_relation(policy.schema, root), where, (request.dataset_id,), join=join, lock_rows=True
        )
    runs = await _relation_evidence(
        connection,
        storage_relation(binding.catalog_schema, "import_run"),
        "row.run_id=ANY($1::varchar[])",
        ([run["run_id"] for run in lineage],),
    )
    return {
        "header_sha256": canonical_json_sha256(original_header_by_field),
        "lineage": runs,
        "minimum_terminal_age_seconds": request.minimum_terminal_age_seconds,
        "relations": evidence_by_relation,
        "source": await _relation_evidence(
            connection,
            storage_relation(binding.catalog_schema, "provider_directory_source"),
            "row.source_id=$1",
            (request.source_id,),
        ),
        "endpoint": await _relation_evidence(
            connection,
            storage_relation(binding.catalog_schema, "provider_directory_api_endpoint"),
            "row.endpoint_id=$1",
            (request.endpoint_id,),
        ),
    }


def _verify_replay(marker_by_field, evidence, evidence_sha256):
    _require(
        set(marker_by_field) == _MARKER_FIELDS
        and marker_by_field["contract_version"] == _CONTRACT
        and marker_by_field["evidence"] == evidence
        and marker_by_field["evidence_sha256"] == evidence_sha256,
        "evidence_changed",
    )
    try:
        reconciled_at = datetime.fromisoformat(marker_by_field["reconciled_at"].replace("Z", "+00:00"))
    except AttributeError, TypeError, ValueError:
        raise TerminalRootRetirementError("evidence_invalid") from None
    _require(reconciled_at.tzinfo is not None)


async def _selection(request, connection, policy, storage_document):
    require_terminal_root_retirement_gate()
    _require(type(request) is TerminalRootRetirementRequest, "request_invalid")
    actor = await _administrator(connection, policy)
    binding = _storage_binding(policy, storage_document)
    await verify_isolated_source_storage_coordinates(connection, policy, binding)
    await connection.execute("SET LOCAL TIME ZONE 'UTC'")
    executor = _Executor(connection)
    await _try_scope_lock(executor, _ADMISSION_LOCK)
    endpoint = await _initial_endpoint(executor, request.endpoint_id)
    base = await _lock_endpoint_scope(executor, request, endpoint)
    header = await _header(connection, policy, request)
    candidate, original_header_by_field, marker_by_field = _candidate(header, request, storage_document)
    await _locked_source(executor, request, base)
    await _locked_predecessor(executor, request)
    await _require_no_competing_candidate(executor, request)
    await _private_admission(connection, policy, request)
    lineage = await _locked_lineage(executor, request)
    await connection.fetch(
        f"SELECT run_id FROM {storage_relation(binding.catalog_schema, 'import_run')} "
        "WHERE run_id=ANY($1::varchar[]) ORDER BY run_id FOR UPDATE NOWAIT",
        [run["run_id"] for run in lineage],
    )
    for run in sorted(lineage, key=lambda row: row["run_id"]):
        await _try_scope_lock(executor, f"provider-directory-terminal-root-retirement:{run['run_id']}")
        await _try_scope_lock(executor, f"control-run-worker-action:v1:{run['run_id']}")
    _require(await _locked_lineage(executor, request) == lineage, "evidence_changed")
    await require_stopped_lineage(lineage)
    controls = await _storage_controls(connection, policy, binding, candidate)
    evidence = await _evidence(connection, policy, binding, request, candidate, original_header_by_field, lineage)
    evidence["controls"] = controls
    token = canonical_json_sha256(evidence)
    if marker_by_field is not None:
        _verify_replay(marker_by_field, evidence, token)
    if request.expected_evidence_sha256 is not None:
        _require(request.expected_evidence_sha256 == token, "evidence_changed")
    _require(await _administrator(connection, policy) == actor, "state_invalid")
    return header, marker_by_field, evidence, token, actor


async def preview_stopped_source_failure(request, *, connection, policy, storage_binding):
    """Return a preservation token within the caller's admitted admin transaction."""
    _require(
        type(request) is TerminalRootRetirementRequest and request.expected_evidence_sha256 is None, "request_invalid"
    )
    _, _, _, token, _ = await _selection(request, connection, policy, storage_binding)
    return token


async def apply_stopped_source_failure(request, *, connection, policy, storage_binding):
    """Change only an exact acquiring header's status and appended reconciliation marker."""
    _require(
        type(request) is TerminalRootRetirementRequest and request.expected_evidence_sha256 is not None,
        "request_invalid",
    )
    header, marker_by_field, evidence, token, actor = await _selection(request, connection, policy, storage_binding)
    if marker_by_field is not None:
        return {"changed": False, "evidence_sha256": token}
    marker_by_field = {
        "contract_version": _CONTRACT,
        "evidence": evidence,
        "evidence_sha256": token,
        "reconciled_at": await connection.fetchval(
            "SELECT to_char(transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        ),
    }
    updated = await connection.execute(
        f"UPDATE {storage_relation(policy.schema, storage.HEADER)} dataset SET status='failed',"
        "publication_metadata_json=publication_metadata_json::jsonb||jsonb_build_object($2::text,$3::jsonb) "
        "WHERE dataset_id=$1 AND to_jsonb(dataset)=$4::jsonb AND status='acquiring' "
        "AND NOT (publication_metadata_json::jsonb ? $2::text)",
        request.dataset_id,
        STOPPED_SOURCE_FAILURE_METADATA_KEY,
        storage._encoded(marker_by_field),
        storage._encoded(header),
    )
    _require(updated == "UPDATE 1", "state_invalid")
    observed = await _header(connection, policy, request)
    _require(
        observed
        == {
            **header,
            "status": "failed",
            "publication_metadata_json": {
                **header["publication_metadata_json"],
                STOPPED_SOURCE_FAILURE_METADATA_KEY: marker_by_field,
            },
        },
        "state_invalid",
    )
    _require(await _administrator(connection, policy) == actor, "state_invalid")
    return {"changed": True, "evidence_sha256": token}


__all__ = ("apply_stopped_source_failure", "preview_stopped_source_failure")
