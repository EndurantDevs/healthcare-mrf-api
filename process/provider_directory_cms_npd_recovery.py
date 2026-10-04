# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dispose one stale CMS candidate without changing its sealed release proof."""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import Any
from uuid import UUID

DELETE_BATCH_SIZE = 5_000
RECOVERY_PAGE_SIZE = 100
TABLE = "provider_directory_cms_npd_stale_candidate"
_DISPATCH_FIELDS = (
    "provider_directory_dispatch_id",
    "provider_directory_dispatch_request_id",
    "provider_directory_dispatch_request_fingerprint",
    "provider_directory_dispatch_catalog_digest",
    "provider_directory_dispatch_contract_version",
)
_RETAINED_FIELDS = ("cms_npd_retained_operation", "cms_npd_retained_vector_sha256", "cms_npd_retained_receipt_sha256")
_FAILED_STATUSES = {"failed", "canceled", "dead_letter"}


def candidate_available_sql(dataset_alias: str, schema: str) -> str:
    """Exclude exact dispositions from shared candidate selection."""

    disposition = '"' + schema.replace('"', '""') + '"."' + TABLE + '"'
    return (
        f"NOT EXISTS (SELECT 1 FROM {disposition} AS cms_stale WHERE cms_stale.dataset_id = {dataset_alias}.dataset_id)"
    )


def _table(fhir: Any, name: str) -> str:
    return fhir._qt(fhir._schema(), name)


async def is_disposed(fhir: Any, dataset_id: str) -> bool:
    """Check the exact immutable candidate disposition."""

    return bool(
        await fhir.db.scalar(
            f"SELECT EXISTS (SELECT 1 FROM {_table(fhir, TABLE)} WHERE dataset_id=:dataset_id)",
            dataset_id=dataset_id,
        )
    )


async def reusable_vector_candidate(
    fhir: Any, endpoint_id: str, identity: dict[str, Any], *, previous_dataset_id: str | None = None
) -> str | None:
    """Resume an exact recurring vector after its first candidate was disposed."""

    dataset = _table(fhir, "provider_directory_endpoint_dataset")
    row = await fhir.db.first(
        f"SELECT candidate.dataset_id FROM {dataset} AS candidate "
        "WHERE candidate.endpoint_id=:endpoint_id "
        "AND candidate.publication_metadata_json::jsonb -> 'source_release' = CAST(:release AS jsonb) "
        "AND (candidate.status IN (:acquiring, :validated) "
        "OR (candidate.status=:published AND candidate.is_current=true)) "
        "AND (CAST(:previous_dataset_id AS text) IS NULL OR candidate.previous_dataset_id=:previous_dataset_id) "
        f"AND {candidate_available_sql('candidate', fhir._schema())} "
        "ORDER BY candidate.created_at DESC, candidate.dataset_id DESC LIMIT 1",
        endpoint_id=endpoint_id,
        release=json.dumps(identity, sort_keys=True),
        acquiring=fhir.ENDPOINT_DATASET_ACQUIRING,
        validated=fhir.ENDPOINT_DATASET_VALIDATED,
        published=fhir.ENDPOINT_DATASET_PUBLISHED,
        previous_dataset_id=previous_dataset_id,
    )
    return row[0] if row is not None else None


async def _locked_candidate_state(fhir: Any, candidate: Any, identity: dict[str, Any]) -> dict[str, Any]:
    """Lock and verify the exact unpublished CMS candidate."""

    await fhir._lock_endpoint_dataset_candidate_admission(fhir.db, candidate.endpoint_id)
    candidate_record = await fhir.db.first(
        f"SELECT endpoint_id, acquisition_root_run_id, status, is_current, "
        f"published_at, dataset_hash, publication_metadata_json "
        f"FROM {_table(fhir, 'provider_directory_endpoint_dataset')} "
        "WHERE dataset_id=:dataset_id FOR UPDATE",
        dataset_id=candidate.dataset_id,
    )
    state = fhir._pagination_checkpoint_row_mapping(candidate_record)
    if (
        state.get("endpoint_id") != candidate.endpoint_id
        or state.get("acquisition_root_run_id") != candidate.acquisition_root_run_id
        or state.get("is_current") is not False
        or state.get("published_at") is not None
        or not isinstance(state.get("publication_metadata_json"), dict)
        or state["publication_metadata_json"].get("source_release") != identity
    ):
        raise RuntimeError("cms_npd_stale_disposition_identity_changed")
    return state


async def _prior_disposition(fhir: Any, candidate: Any, identity: dict[str, Any], status: str) -> str | None:
    """Permit exact replay of a previously recorded stale observation."""

    existing = await fhir.db.first(
        f"SELECT prior_status, endpoint_id, acquisition_root_run_id, vector_sha256 "
        f"FROM {_table(fhir, TABLE)} WHERE dataset_id=:dataset_id",
        dataset_id=candidate.dataset_id,
    )
    if existing is not None:
        prior = fhir._pagination_checkpoint_row_mapping(existing)
        if (
            prior.get("endpoint_id") != candidate.endpoint_id
            or prior.get("acquisition_root_run_id") != candidate.acquisition_root_run_id
            or prior.get("vector_sha256") != identity["vector_sha256"]
            or status
            != (
                fhir.ENDPOINT_DATASET_FAILED
                if prior.get("prior_status") == fhir.ENDPOINT_DATASET_ACQUIRING
                else fhir.ENDPOINT_DATASET_VALIDATED
            )
        ):
            raise RuntimeError("cms_npd_stale_disposition_conflict")
        return prior["prior_status"]
    return None


async def _record_disposition(fhir: Any, candidate: Any, identity: dict[str, Any], state: dict[str, Any]) -> None:
    """Insert the sealed observation and fail an acquiring parent atomically."""

    status = state["status"]
    await fhir.db.status(
        f"INSERT INTO {_table(fhir, TABLE)} "
        "(dataset_id, endpoint_id, acquisition_root_run_id, vector_sha256, prior_status, dataset_hash) "
        "VALUES (:dataset_id, :endpoint_id, :root_run_id, :vector_sha256, :prior_status, :dataset_hash)",
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        root_run_id=candidate.acquisition_root_run_id,
        vector_sha256=identity["vector_sha256"],
        prior_status=status,
        dataset_hash=state.get("dataset_hash"),
    )
    if status == fhir.ENDPOINT_DATASET_ACQUIRING:
        changed = await fhir.db.status(
            f"UPDATE {_table(fhir, 'provider_directory_endpoint_dataset')} "
            "SET status=:failed WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id "
            "AND acquisition_root_run_id=:root_run_id AND status=:acquiring "
            "AND is_current=false AND published_at IS NULL "
            "AND publication_metadata_json::jsonb -> 'source_release' = CAST(:release AS jsonb)",
            failed=fhir.ENDPOINT_DATASET_FAILED,
            dataset_id=candidate.dataset_id,
            endpoint_id=candidate.endpoint_id,
            root_run_id=candidate.acquisition_root_run_id,
            acquiring=fhir.ENDPOINT_DATASET_ACQUIRING,
            release=json.dumps(identity, sort_keys=True),
        )
        if fhir._coerce_rowcount(changed) != 1:
            raise RuntimeError("cms_npd_stale_disposition_lost")


async def _dispose_locked(fhir: Any, candidate: Any, identity: dict[str, Any]) -> str:
    """Record only an exact unpublished acquiring or validated candidate disposition."""

    state = await _locked_candidate_state(fhir, candidate, identity)
    status = state.get("status")
    prior_status = await _prior_disposition(fhir, candidate, identity, status)
    if prior_status is not None:
        return prior_status
    if status not in (fhir.ENDPOINT_DATASET_ACQUIRING, fhir.ENDPOINT_DATASET_VALIDATED):
        raise RuntimeError("cms_npd_stale_disposition_state_invalid")
    await _record_disposition(fhir, candidate, identity, state)
    return status


async def _clear_failed_rows(fhir: Any, candidate: Any, identity: dict[str, Any]) -> None:
    """Reclaim mutable payload in bounded transactions; keep stable identity evidence."""

    while True:
        async with fhir.db.transaction():
            parent = await fhir.db.first(
                f"SELECT dataset_id FROM {_table(fhir, 'provider_directory_endpoint_dataset')} "
                "WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id "
                "AND acquisition_root_run_id=:root_run_id AND status=:failed "
                "AND is_current=false AND publication_metadata_json::jsonb "
                "-> 'source_release' = CAST(:release AS jsonb) FOR SHARE",
                dataset_id=candidate.dataset_id,
                endpoint_id=candidate.endpoint_id,
                root_run_id=candidate.acquisition_root_run_id,
                failed=fhir.ENDPOINT_DATASET_FAILED,
                release=json.dumps(identity, sort_keys=True),
            )
            if parent is None or not await is_disposed(fhir, candidate.dataset_id):
                raise RuntimeError("cms_npd_stale_cleanup_parent_changed")
            await fhir.db.status(
                f"DELETE FROM {_table(fhir, 'provider_directory_cms_npd_relationship_receipt')} "
                "WHERE dataset_id=:dataset_id",
                dataset_id=candidate.dataset_id,
            )
            deleted = await fhir.db.status(
                f"DELETE FROM {_table(fhir, 'provider_directory_dataset_resource')} "
                "WHERE ctid IN (SELECT ctid FROM "
                f"{_table(fhir, 'provider_directory_dataset_resource')} "
                "WHERE dataset_id=:dataset_id LIMIT :batch_size)",
                dataset_id=candidate.dataset_id,
                batch_size=DELETE_BATCH_SIZE,
            )
            if fhir._coerce_rowcount(deleted) == 0:
                await fhir.delete_dataset_proof_shards(fhir.db, fhir._schema(), candidate.dataset_id)
                return


async def dispose_changed_vector(fhir: Any, candidate: Any, identity: dict[str, Any]) -> None:
    """Make a proven replaced release nonpublishable; clean only unsealed rows."""

    async with fhir.db.transaction():
        prior_status = await _dispose_locked(fhir, candidate, identity)
    if prior_status == fhir.ENDPOINT_DATASET_ACQUIRING:
        await _clear_failed_rows(fhir, candidate, identity)


async def retire_unwitnessed_validated_candidate(fhir: Any, candidate: Any, identity: dict[str, Any]) -> None:
    """Exclude an exact old sealed candidate before witnessed reacquisition."""

    async with fhir.db.transaction():
        prior_status = await _dispose_locked(fhir, candidate, identity)
        if prior_status != fhir.ENDPOINT_DATASET_VALIDATED:
            raise RuntimeError("cms_npd_witness_upgrade_candidate_state_changed")


def _retained_selection(params):
    """Compare optional selectors without admitting partial or altered retained requests."""
    values = tuple(params.get(name) for name in _RETAINED_FIELDS)
    if values == (None, None, None):
        return values
    if values[0] not in ("baseline", "rollback") or any(
        not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in values[1:]
    ):
        raise RuntimeError("cms_npd_repair_identity_invalid")
    return values


def _dispatch_identity(params, *, is_repair=False):
    """Validate the existing typed dispatch identity without treating declared fields as authority."""
    generation = params.get("provider_directory_dispatch_generation")
    try:
        request_id = params["provider_directory_dispatch_request_id"]
        if str(UUID(request_id)) != request_id:
            raise ValueError
        if is_repair:
            repair_id = params["provider_directory_repair_id"]
            if str(UUID(repair_id)) != repair_id:
                raise ValueError
    except KeyError, TypeError, ValueError, AttributeError:
        raise RuntimeError("cms_npd_repair_identity_invalid") from None
    if (
        type(generation) is not int
        or generation < int(is_repair)
        or type(params.get("provider_directory_dispatch_contract_version")) is not int
        or params["provider_directory_dispatch_contract_version"] != 2
        or not isinstance(params.get("provider_directory_dispatch_id"), str)
        or re.fullmatch(r"pdd_[0-9a-f]{32}", params["provider_directory_dispatch_id"]) is None
        or any(
            not isinstance(params.get(name), str) or re.fullmatch(r"[0-9a-f]{64}", params[name]) is None
            for name in _DISPATCH_FIELDS[2:4]
        )
    ):
        raise RuntimeError("cms_npd_repair_identity_invalid")
    return tuple(params[name] for name in _DISPATCH_FIELDS), generation, _retained_selection(params)


async def _run_records(fhir, *, run_id=None, parent_id=None):
    """Lock a primary-key run or bounded retry children under importer admission serialization."""
    predicate = "run_id=:run_id" if parent_id is None else "retry_of_run_id=:parent_id"
    rows = await fhir.db.all(
        "SELECT run_id, retry_of_run_id, engine, node_id, importer, status, finished_at, params "
        f"FROM {_table(fhir, 'import_run')} WHERE {predicate} LIMIT 2 FOR UPDATE",
        **({"run_id": run_id} if parent_id is None else {"parent_id": parent_id}),
    )
    return [fhir._pagination_checkpoint_row_mapping(row) for row in rows]


async def _run_lineage(fhir, root_run_id, *, running_run_id=None):
    """Follow actual unique retry edges; only the executing leaf may still be running."""
    if not isinstance(root_run_id, str) or not root_run_id or len(root_run_id) > 64:
        raise RuntimeError("cms_npd_acquisition_lineage_invalid")
    run_records = await _run_records(fhir, run_id=root_run_id)
    lineage_runs = []
    visited_run_ids = set()
    for _depth in range(256):
        if len(run_records) != 1:
            break
        run = run_records[0]
        params = run.get("params")
        if (
            run["run_id"] in visited_run_ids
            or not isinstance(params, dict)
            or run["importer"] != "provider-directory-fhir"
            or run["engine"] != "healthcare-mrf-api"
            or not run["node_id"]
            or (lineage_runs and run["node_id"] != lineage_runs[0]["node_id"])
            or params.get("source_ids") != ["cms-npd"]
            or params.get("import_resources") is not True
            or params.get("full_refresh") is not True
            or (params.get("provider_directory_pagination_root_run_id") or run["run_id"]) != root_run_id
            or (not lineage_runs and run["retry_of_run_id"] is not None)
        ):
            break
        visited_run_ids.add(run["run_id"])
        lineage_runs.append(run)
        children = await _run_records(fhir, parent_id=run["run_id"])
        is_executing = run["run_id"] == running_run_id and run["status"] == "running"
        if is_executing:
            if run["finished_at"] is not None or children:
                break
        elif run["status"] not in _FAILED_STATUSES | {"succeeded"} or run["finished_at"] is None:
            break
        if not children:
            if running_run_id is None or run["run_id"] == running_run_id:
                return lineage_runs
            break
        run_records = children
    raise RuntimeError("cms_npd_acquisition_lineage_invalid")


async def _executing_lineage(fhir, task, run_id):
    """Bind the supplied selector and root to the stored executing run and its actual ancestors."""
    rows = await _run_records(fhir, run_id=run_id)
    if len(rows) != 1 or not isinstance(rows[0].get("params"), dict):
        raise RuntimeError("cms_npd_acquisition_lineage_invalid")
    params = rows[0]["params"]
    root = params.get("provider_directory_pagination_root_run_id") or run_id
    if root != (task.get("provider_directory_pagination_root_run_id") or run_id):
        raise RuntimeError("cms_npd_acquisition_lineage_invalid")
    lineage = await _run_lineage(fhir, root, running_run_id=run_id)
    identity_fields = (*_DISPATCH_FIELDS, "provider_directory_dispatch_generation", "provider_directory_repair_id")
    if any(
        _retained_selection(run["params"]) != _retained_selection(task)
        or any(run["params"].get(name) != task.get(name) for name in identity_fields)
        for run in lineage
    ):
        raise RuntimeError("cms_npd_acquisition_lineage_invalid")
    return lineage


async def is_same_acquisition_replay(fhir, owner_run_id, task, run_id):
    """Accept only an actual owning run on the current retry chain, never a claimed root string."""
    from api.control_imports import _PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY

    async with fhir.db.transaction():
        await fhir.db.first(
            "SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))",
            lock_key=_PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY,
        )
        lineage = await _executing_lineage(fhir, task, run_id)
        return owner_run_id in {run["run_id"] for run in lineage}


async def _repair_candidate_state(fhir, endpoint_id, identity):
    """Find at most one exact reusable vector, including protected current state."""
    rows = await fhir.db.all(
        f"SELECT dataset_id, acquisition_root_run_id, import_run_id FROM {_table(fhir, 'provider_directory_endpoint_dataset')} "
        "AS candidate WHERE endpoint_id=:endpoint_id "
        "AND (status IN (:acquiring,:validated) OR (status=:published AND is_current)) "
        "AND publication_metadata_json::jsonb -> 'source_release' = CAST(:release AS jsonb) "
        f"AND {candidate_available_sql('candidate', fhir._schema())} LIMIT 2",
        endpoint_id=endpoint_id,
        release=json.dumps(identity, sort_keys=True),
        acquiring=fhir.ENDPOINT_DATASET_ACQUIRING,
        validated=fhir.ENDPOINT_DATASET_VALIDATED,
        published=fhir.ENDPOINT_DATASET_PUBLISHED,
    )
    if len(rows) > 1:
        raise RuntimeError("cms_npd_repair_candidate_ambiguous")
    return fhir._pagination_checkpoint_row_mapping(rows[0]) if rows else None


async def _retire_repaired_owner(fhir, candidate, identity, current_lineage, declared_identity):
    """Retire only the failed older generation of this exact authenticated dispatch."""
    rows = await _run_records(fhir, run_id=candidate.acquisition_root_run_id)
    if len(rows) != 1 or not isinstance(rows[0].get("params"), dict):
        raise RuntimeError("cms_npd_repair_owner_invalid")
    owner_params = rows[0]["params"]
    owner_identity = _dispatch_identity(owner_params)
    if (
        owner_identity[0] != declared_identity[0]
        or owner_identity[2] != declared_identity[2]
        or owner_identity[1] >= declared_identity[1]
    ):
        raise RuntimeError("cms_npd_repair_owner_invalid")
    old_root = owner_params.get("provider_directory_pagination_root_run_id") or candidate.acquisition_root_run_id
    old_lineage = await _run_lineage(fhir, old_root)
    if (
        old_lineage[-1]["status"] not in _FAILED_STATUSES
        or old_lineage[0]["node_id"] != current_lineage[0]["node_id"]
        or any(
            _dispatch_identity(run["params"], is_repair=owner_identity[1] > 0) != owner_identity
            or run["params"].get("provider_directory_repair_id") != owner_params.get("provider_directory_repair_id")
            for run in old_lineage
        )
        or not {candidate.acquisition_root_run_id, candidate.import_run_id} <= {run["run_id"] for run in old_lineage}
    ):
        raise RuntimeError("cms_npd_repair_owner_invalid")
    return await _dispose_locked(fhir, candidate, identity)


async def repaired_candidate_selection(fhir, endpoint_id, identity, task, run_id):
    """Move a typed repair to a fresh candidate while retaining immutable prior ownership and seals."""
    if task.get("provider_directory_repair_id") is None:
        if task.get("provider_directory_dispatch_generation") not in (None, 0):
            raise RuntimeError("cms_npd_repair_identity_invalid")
        return None
    from api.control_imports import _PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY

    declared_identity = _dispatch_identity(task, is_repair=True)
    prior_status = None
    async with fhir.db.transaction():
        await fhir.db.first(
            "SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))",
            lock_key=_PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY,
        )
        lineage = await _executing_lineage(fhir, task, run_id)
        if (
            lineage[-1]["status"] != "running"
            or any(_dispatch_identity(run["params"], is_repair=True) != declared_identity for run in lineage)
            or any(
                run["params"].get("provider_directory_repair_id") != task["provider_directory_repair_id"]
                for run in lineage
            )
        ):
            raise RuntimeError("cms_npd_repair_identity_invalid")
        await fhir._lock_endpoint_dataset_candidate_admission(fhir.db, endpoint_id)
        state = await _repair_candidate_state(fhir, endpoint_id, identity)
        if state is None:
            return lineage[0]["run_id"], None
        if {state["acquisition_root_run_id"], state["import_run_id"]} <= {run["run_id"] for run in lineage}:
            return lineage[0]["run_id"], state["dataset_id"]
        candidate = SimpleNamespace(**state, endpoint_id=endpoint_id)
        prior_status = await _retire_repaired_owner(fhir, candidate, identity, lineage, declared_identity)
    if prior_status == fhir.ENDPOINT_DATASET_ACQUIRING:
        await _clear_failed_rows(fhir, candidate, identity)
    return lineage[0]["run_id"], None


async def dispose_prior_vectors(fhir: Any, endpoint_id: str, new_vector_sha256: str) -> None:
    """Retire old active CMS vectors after the new upstream seal is verified."""

    recovery_cursor = None
    while True:
        seek_clause = (
            ""
            if recovery_cursor is None
            else "AND (dataset.created_at, dataset.dataset_id) > (:created_at, :cursor_id) "
        )
        candidate_page = await fhir.db.all(
            f"SELECT dataset.dataset_id, dataset.acquisition_root_run_id, dataset.created_at, "
            "dataset.publication_metadata_json::jsonb -> 'source_release' AS source_release "
            f"FROM {_table(fhir, 'provider_directory_endpoint_dataset')} AS dataset "
            "WHERE dataset.endpoint_id=:endpoint_id AND dataset.is_current=false "
            "AND dataset.status IN (:acquiring, :validated) "
            "AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'source_id' = 'cms-npd' "
            "AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'vector_sha256' "
            "IS DISTINCT FROM :new_vector "
            f"AND NOT EXISTS (SELECT 1 FROM {_table(fhir, TABLE)} AS stale "
            "WHERE stale.dataset_id=dataset.dataset_id) "
            f"{seek_clause}ORDER BY dataset.created_at, dataset.dataset_id LIMIT :page_size",
            endpoint_id=endpoint_id,
            acquiring=fhir.ENDPOINT_DATASET_ACQUIRING,
            validated=fhir.ENDPOINT_DATASET_VALIDATED,
            new_vector=new_vector_sha256,
            page_size=RECOVERY_PAGE_SIZE,
            **({} if recovery_cursor is None else {"created_at": recovery_cursor[0], "cursor_id": recovery_cursor[1]}),
        )
        if not candidate_page:
            return
        for candidate_record in candidate_page:
            candidate_state = fhir._pagination_checkpoint_row_mapping(candidate_record)
            release_identity = candidate_state.get("source_release")
            if isinstance(release_identity, str):
                release_identity = json.loads(release_identity)
            if not isinstance(release_identity, dict) or not isinstance(release_identity.get("vector_sha256"), str):
                raise RuntimeError("cms_npd_stale_disposition_identity_changed")
            stale_candidate = SimpleNamespace(
                dataset_id=candidate_state["dataset_id"],
                endpoint_id=endpoint_id,
                acquisition_root_run_id=candidate_state["acquisition_root_run_id"],
            )
            await dispose_changed_vector(fhir, stale_candidate, release_identity)
        last_candidate = fhir._pagination_checkpoint_row_mapping(candidate_page[-1])
        recovery_cursor = (last_candidate["created_at"], last_candidate["dataset_id"])


async def resume_pending_cleanup(fhir: Any, endpoint_id: str) -> None:
    """Finish exact failed-candidate cleanup after an interrupted prior run."""

    parent = _table(fhir, "provider_directory_endpoint_dataset")
    resource = _table(fhir, "provider_directory_dataset_resource")
    shard = _table(fhir, "provider_directory_dataset_proof_shard")
    while True:
        pending_record = await fhir.db.first(
            f"SELECT dataset.dataset_id, dataset.acquisition_root_run_id, "
            f"dataset.publication_metadata_json, stale.vector_sha256 "
            f"FROM {_table(fhir, TABLE)} AS stale "
            f"JOIN {parent} AS dataset USING (dataset_id) "
            "WHERE dataset.endpoint_id=:endpoint_id AND dataset.status=:failed "
            "AND stale.prior_status=:acquiring AND ("
            f"EXISTS (SELECT 1 FROM {resource} WHERE dataset_id=dataset.dataset_id) OR "
            f"EXISTS (SELECT 1 FROM {shard} WHERE dataset_id=dataset.dataset_id)) "
            "ORDER BY stale.observed_at, dataset.dataset_id LIMIT 1",
            endpoint_id=endpoint_id,
            failed=fhir.ENDPOINT_DATASET_FAILED,
            acquiring=fhir.ENDPOINT_DATASET_ACQUIRING,
        )
        if pending_record is None:
            return
        pending = fhir._pagination_checkpoint_row_mapping(pending_record)
        metadata = pending.get("publication_metadata_json")
        identity = metadata.get("source_release") if isinstance(metadata, dict) else None
        if not isinstance(identity, dict) or identity.get("vector_sha256") != pending["vector_sha256"]:
            raise RuntimeError("cms_npd_stale_cleanup_identity_changed")
        candidate = SimpleNamespace(
            dataset_id=pending["dataset_id"],
            endpoint_id=endpoint_id,
            acquisition_root_run_id=pending["acquisition_root_run_id"],
        )
        await _clear_failed_rows(fhir, candidate, identity)
