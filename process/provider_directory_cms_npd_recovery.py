# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dispose one stale CMS candidate without changing its sealed release proof."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

DELETE_BATCH_SIZE = 5_000
RECOVERY_PAGE_SIZE = 100
TABLE = "provider_directory_cms_npd_stale_candidate"


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
    """Record only an exact acquiring or validated source-vector replacement."""

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
