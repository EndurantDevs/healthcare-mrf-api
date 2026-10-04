# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admit one complete CMS bulk release into a source-local FHIR dataset."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import importlib
import importlib.util
import json
import os
import re
import tempfile
from compression import zstd
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
from sqlalchemy import select, text

from db.models import ProviderDirectoryCMSNPDResourceWitness
from process import cms_npd_source as source
from process import provider_directory_cms_npd_recovery as recovery
from process import provider_directory_cms_npd_relationship as relationships
from process.provider_directory_cms_observation import observe_intake as _observe_intake
from process.provider_directory_profile_selection_dataset import _cms_dataset_pair

SOURCE_ID = source.SOURCE_ID
RESOURCE_TYPES = tuple(resource_type for _, resource_type in source.RESOURCE_FILES)
RESOURCE_SET = frozenset(RESOURCE_TYPES)
ADAPTER_CONTRACT = "cms-npd-bulk-fhir-v1"
BATCH_SIZE = 1_000
BATCH_MAX_DECODED_BYTES = 8 * 1024 * 1024
IDENTITY_BATCH_SIZE = 1_000
_LOCAL_ORGANIZATION_REF = re.compile(r"Organization/([A-Za-z0-9.-]{1,64})\Z")
_NETWORK_ROLE_SQL = (
    'organization.payload_json::jsonb @> \'{"type":[{"text":"ntwk"}]}\'::jsonb '
    'OR organization.payload_json::jsonb @> \'{"type":[{"coding":[{"code":"ntwk"}]}]}\'::jsonb'
)


def durable_artifact_root() -> Path:
    """Require configured durable storage before any download begins."""

    configured = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    if not configured:
        raise ValueError("cms_npd_artifact_root_required")
    path = Path(configured)
    try:
        root = path.resolve(strict=True)
        temporary_roots = (
            Path(tempfile.gettempdir()).resolve(),
            Path("/tmp"),
            Path("/var/tmp"),
            Path("/private/tmp"),
            Path("/private/var/tmp"),
            Path("/var/folders"),
            Path("/private/var/folders"),
            Path("/dev/shm"),
            Path("/run"),
        )
        if (
            path.is_symlink()
            or not root.is_dir()
            or root == Path("/")
            or any(root.is_relative_to(temporary) for temporary in temporary_roots)
        ):
            raise ValueError("cms_npd_artifact_root_unsafe")
    except OSError as error:
        raise ValueError("cms_npd_artifact_root_unavailable") from error
    return root


def validate_task(task: dict[str, Any], run_id: str | None) -> None:
    """Exclude bounded or mixed-source runs from complete-release admission."""

    retained_input = retained_input_selection(task)
    if retained_input is not None and task.get("source_ids") not in ([SOURCE_ID], (SOURCE_ID,), SOURCE_ID):
        raise ValueError("cms_npd_retained_requires_exclusive_source_scope")
    if retained_input is not None and task.get("source_query"):
        raise ValueError("cms_npd_import_parameters_invalid")
    if not run_id or not task.get("import_resources") or task.get("full_refresh") is not True:
        raise ValueError("cms_npd_complete_import_required")
    if any(
        task.get(field)
        for field in (
            "test",
            "test_mode",
            "limit",
            "resource_limit",
            "page_limit",
            "seed_only",
            "stale_cleanup",
            "publish_artifacts",
            "publish_after_acquisition",
            "dataset_rehydrate_only",
            "dataset_followup_only",
            "publish_artifacts_only",
            "canonical_backfill_only",
            "contact_backfill_only",
        )
    ):
        raise ValueError("cms_npd_import_parameters_invalid")
    resources = task.get("resources")
    if resources not in (None, ""):
        selected = resources.split(",") if isinstance(resources, str) else resources
        if not isinstance(selected, (list, tuple)) or set(selected) != RESOURCE_SET:
            raise ValueError("cms_npd_resource_scope_incomplete")
    rollback_vector = task.get("cms_npd_rollback_vector_sha256")
    rollback_root = task.get("cms_npd_rollback_root_run_id")
    if rollback_vector is not None and (
        not isinstance(rollback_vector, str) or source._SHA256.fullmatch(rollback_vector) is None
    ):
        raise ValueError("cms_npd_rollback_vector_invalid")
    if rollback_root is not None and (
        rollback_vector is None
        or not isinstance(rollback_root, str)
        or not rollback_root.strip()
        or rollback_root != rollback_root.strip()
        or len(rollback_root) > 160
    ):
        raise ValueError("cms_npd_rollback_root_invalid")


def retained_input_selection(task: dict[str, Any]) -> dict[str, str] | None:
    """Require one closed operator selection without changing ordinary requests."""
    operation = task.get("cms_npd_retained_operation")
    vector = task.get("cms_npd_retained_vector_sha256")
    receipt = task.get("cms_npd_retained_receipt_sha256")
    if operation is None and vector is None and receipt is None:
        return None
    if (
        operation not in ("baseline", "rollback")
        or any(not isinstance(value, str) or source._SHA256.fullmatch(value) is None for value in (vector, receipt))
        or any(
            task.get(name) is not None for name in ("cms_npd_rollback_vector_sha256", "cms_npd_rollback_root_run_id")
        )
    ):
        raise ValueError("cms_npd_retained_selection_invalid")
    root = task.get("provider_directory_pagination_root_run_id")
    if root is not None and (not isinstance(root, str) or not root or root != root.strip() or len(root) > 160):
        raise ValueError("cms_npd_retained_root_invalid")
    return {"operation": operation, "vector_sha256": vector, "receipt_sha256": receipt}


def validate_source_scope(task, run_id, requested_source_ids, test_mode):
    """Reject retained controls before generic importer setup can write."""
    if retained_input_selection(task) is not None:
        if requested_source_ids != [SOURCE_ID]:
            raise ValueError("cms_npd_retained_requires_exclusive_source_scope")
        validate_task(task, run_id)
        if test_mode:
            raise ValueError("cms_npd_import_parameters_invalid")
    if any(
        task.get(name) is not None for name in ("cms_npd_rollback_vector_sha256", "cms_npd_rollback_root_run_id")
    ) and requested_source_ids != [SOURCE_ID]:
        raise ValueError("cms_npd_rollback_requires_exclusive_source_scope")


def release_identity(receipt_by_field: dict[str, Any]) -> dict[str, Any]:
    """Keep the exact complete vector in immutable candidate metadata."""

    files = receipt_by_field.get("files")
    if not isinstance(files, dict) or set(files) != {name for name, _ in source.RESOURCE_FILES}:
        raise source.CmsNpdSourceError("cms_npd_receipt_invalid")
    return {
        "source_id": SOURCE_ID,
        "generated_at": receipt_by_field["generated_at"],
        "manifest_sha256": receipt_by_field["manifest_sha256"],
        "vector_sha256": receipt_by_field["vector_sha256"],
        "files": {
            name: {
                key: files[name][key]
                for key in ("sha256", "compressed_bytes", "original_bytes", "row_count", "distinct_count")
            }
            for name, _ in source.RESOURCE_FILES
        },
    }


async def _register_source(fhir: Any) -> str:
    """Bind one stable official-file endpoint and one source-scoped identity."""

    admitted = _source_endpoint(fhir)
    now = fhir._now()
    endpoint_id = admitted["endpoint_id"]
    await fhir._upsert_rows(
        fhir.ProviderDirectoryAPIEndpoint,
        [
            {
                **admitted,
                "first_seen_at": now,
                "last_seen_at": now,
                "metadata_json": {"transport": "cms_npd_bulk_files", "adapter_contract": ADAPTER_CONTRACT},
                "created_at": now,
                "updated_at": now,
            }
        ],
    )
    await fhir._upsert_rows(fhir.ProviderDirectorySource, [_source_row(fhir, endpoint_id, now)])
    return endpoint_id


def _source_endpoint(fhir: Any) -> dict[str, Any]:
    """Derive the registered endpoint before baseline history checks can write."""
    return fhir._admit_provider_directory_endpoint_components(
        canonical_api_base=source.DOWNLOADS_URL,
        credential_descriptor_json={},
        endpoint_signature_json={
            "transport": "cms_npd_bulk_files",
            "adapter_contract": ADAPTER_CONTRACT,
            "resource_types": sorted(RESOURCE_SET),
        },
    )


def _source_row(fhir: Any, endpoint_id: str, now: Any) -> dict[str, Any]:
    """Describe the exact configured CMS source without inferring other endpoints."""

    return {
        "source_id": SOURCE_ID,
        "org_name": "CMS National Provider Directory",
        "plan_name": None,
        "portal_url": source.DOWNLOADS_URL,
        "api_base": source.DOWNLOADS_URL,
        "canonical_api_base": source.DOWNLOADS_URL,
        "endpoint_id": endpoint_id,
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "compliance_flag": "official_bulk_files",
        "data_quality_flag": "retained_release_proof",
        "seed_source": SOURCE_ID,
        "seed_source_detail": ADAPTER_CONTRACT,
        "seed_source_url": source.DOWNLOADS_URL,
        "metadata_json": {
            "provider_directory_source_kind": "official_bulk_files",
            "provider_directory_transport": "cms_npd_bulk_files",
            "provider_directory_source_entry_id": SOURCE_ID,
            fhir.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: endpoint_id,
            "provider_directory_supported_resources": sorted(RESOURCE_SET),
            "provider_directory_acquisition_enabled": True,
            "provider_directory_fhir_endpoint": False,
        },
        "created_at": now,
        "updated_at": now,
    }


def _assert_candidate_release(state: dict[str, Any], identity: dict[str, Any]) -> None:
    metadata = state.get("publication_metadata_json")
    if not isinstance(metadata, dict) or metadata.get("source_release") != identity:
        raise RuntimeError("cms_npd_candidate_release_changed")


async def _candidate(
    fhir: Any,
    endpoint_id: str,
    run_id: str,
    identity: dict[str, Any],
    *,
    candidate_key: str | None = None,
    existing_dataset_id: str | None = None,
) -> Any:
    dataset_id = existing_dataset_id or fhir._endpoint_dataset_candidate_id(
        endpoint_id, tuple(sorted(RESOURCE_SET)), candidate_key or identity["vector_sha256"]
    )
    state = await fhir._endpoint_dataset_state(dataset_id)
    if state:
        if await recovery.is_disposed(fhir, dataset_id):
            raise RuntimeError("cms_npd_candidate_disposed")
        _assert_candidate_release(state, identity)
        if (
            state.get("endpoint_id") != endpoint_id
            or fhir._dataset_resource_hash_contract(state) != fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            or fhir._dataset_semantic_projection_as_of(state, fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT)
            != identity["generated_at"]
            or (state.get("status") == fhir.ENDPOINT_DATASET_PUBLISHED and state.get("is_current") is not True)
        ):
            raise RuntimeError("cms_npd_candidate_identity_changed")
        root_run_id = state.get("acquisition_root_run_id")
        if not isinstance(root_run_id, str) or not root_run_id:
            raise RuntimeError("cms_npd_candidate_root_invalid")
    else:
        root_run_id = run_id
    candidate = fhir.EndpointDatasetCandidate(
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        source_ids=(SOURCE_ID,),
        selected_resources=tuple(sorted(RESOURCE_SET)),
        expected_resources=tuple(sorted(RESOURCE_SET)),
        import_run_id=run_id,
        previous_dataset_id=(
            state.get("previous_dataset_id")
            if state
            else await fhir._current_endpoint_dataset_id(endpoint_id, exclude_dataset_id=dataset_id)
        ),
        reused_from_checkpoint=bool(state),
        already_validated=state.get("status") == fhir.ENDPOINT_DATASET_VALIDATED,
        already_published=(state.get("status") == fhir.ENDPOINT_DATASET_PUBLISHED and state.get("is_current") is True),
        resource_hash_contract=fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=identity["generated_at"],
        source_release=identity,
    )
    if state and state.get("status") not in (
        fhir.ENDPOINT_DATASET_ACQUIRING,
        fhir.ENDPOINT_DATASET_VALIDATED,
        fhir.ENDPOINT_DATASET_PUBLISHED,
    ):
        raise RuntimeError("cms_npd_candidate_state_invalid")
    if not state:
        candidate = await fhir._initialize_endpoint_dataset_candidate(candidate, ())
    return candidate


async def _assert_rollback_predecessor(fhir: Any, endpoint_id: str, identity: dict[str, Any]) -> None:
    """Only replay a locally retained release that was previously published."""

    prior = await fhir.db.first(
        f"SELECT dataset_id FROM {fhir._qt(fhir._schema(), 'provider_directory_endpoint_dataset')} "
        "WHERE endpoint_id=:endpoint_id AND status=:superseded AND is_current=false "
        "AND published_at IS NOT NULL "
        "AND publication_metadata_json::jsonb -> 'source_release' = CAST(:release AS jsonb) "
        "ORDER BY published_at DESC, dataset_id DESC LIMIT 1",
        endpoint_id=endpoint_id,
        superseded=fhir.ENDPOINT_DATASET_SUPERSEDED,
        release=json.dumps(identity, sort_keys=True),
    )
    if prior is None:
        raise RuntimeError("cms_npd_rollback_prior_publication_missing")
    state = await fhir._endpoint_dataset_state(prior[0])
    if not state:
        raise RuntimeError("cms_npd_rollback_prior_publication_missing")
    _assert_candidate_release(state, identity)
    if state.get("status") != fhir.ENDPOINT_DATASET_SUPERSEDED or state.get("is_current") is not False:
        raise RuntimeError("cms_npd_rollback_prior_publication_not_superseded")


async def _assert_baseline_history(fhir, endpoint_id, identity, task, run_id):
    """Allow first intake or the same acquisition's proved current composite result."""
    history = await fhir.db.all(
        f"SELECT dataset_id FROM {fhir._qt(fhir._schema(), 'provider_directory_endpoint_dataset')} "
        "WHERE endpoint_id=:endpoint_id AND (published_at IS NOT NULL OR is_current) LIMIT 2",
        endpoint_id=endpoint_id,
    )
    if not history:
        return
    if len(history) == 1:
        current = await _current_release_publication(
            identity["vector_sha256"], identity["manifest_sha256"], identity["generated_at"]
        )
        if (
            current is not None
            and current["dataset_id"] == history[0][0]
            and current["endpoint_id"] == endpoint_id
            and current["publication_metadata_json"].get("source_release") == identity
            and await recovery.is_same_acquisition_replay(fhir, current["acquisition_root_run_id"], task, run_id)
        ):
            return
    raise RuntimeError("cms_npd_baseline_prior_publication_exists")


def _assert_retained_receipt_pin(directory: Path, task: dict[str, Any] | None) -> None:
    """Recheck approved receipt bytes at the existing retained verification boundaries."""
    expected = (task or {}).get("cms_npd_retained_receipt_sha256")
    if expected is None:
        return
    path = directory / "receipt.json"
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_size > source.MAX_MANIFEST_BYTES
        or hashlib.sha256(path.read_bytes()).hexdigest() != expected
    ):
        raise source.CmsNpdSourceError("cms_npd_retained_receipt_changed")


async def _verify_release(
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
    task: dict[str, Any] | None = None,
) -> None:
    _assert_retained_receipt_pin(directory, task)
    if client is None:
        await asyncio.to_thread(source.verify_retained_release, directory, receipt_by_field)
    else:
        await asyncio.to_thread(source.verify_release, directory, receipt_by_field, client=client)
    _assert_retained_receipt_pin(directory, task)


async def _verify_or_dispose(
    fhir: Any,
    candidate: Any,
    identity: dict[str, Any],
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
    task: dict[str, Any] | None = None,
) -> None:
    try:
        await _verify_release(directory, receipt_by_field, client, task)
    except source.CmsNpdSourceError as error:
        if not candidate.already_published and str(error) in {
            "cms_npd_retained_file_missing",
            "cms_npd_source_vector_changed",
            "cms_npd_retained_receipt_changed",
        }:
            await recovery.dispose_changed_vector(fhir, candidate, identity)
        raise


def _rollback_selection(task, run_id):
    """Keep legacy rollback roots and use the durable retry root for retained selections."""
    if task.get("cms_npd_retained_operation") == "rollback":
        return task.get("cms_npd_retained_vector_sha256"), task.get(
            "provider_directory_pagination_root_run_id"
        ) or run_id
    return task.get("cms_npd_rollback_vector_sha256"), task.get("cms_npd_rollback_root_run_id") or run_id


async def _admission_candidate(
    fhir: Any,
    endpoint_id: str,
    run_id: str,
    identity: dict[str, Any],
    task: dict[str, Any],
    repair_selection=None,
) -> Any:
    """Select a reusable exact vector candidate or a fresh recurring one."""

    rollback_vector, rollback_root = _rollback_selection(task, run_id)
    candidate_key = None
    existing_dataset_id = None
    if rollback_vector is not None:
        if rollback_vector != identity["vector_sha256"]:
            raise RuntimeError("cms_npd_rollback_vector_changed")
        await _assert_rollback_predecessor(fhir, endpoint_id, identity)
    if repair_selection is None:
        repair_selection = await recovery.repaired_candidate_selection(fhir, endpoint_id, identity, task, run_id)
    if repair_selection is not None:
        repair_root, existing_dataset_id = repair_selection
        candidate_key = f"cms-npd-repair:{identity['vector_sha256']}:{repair_root}"
    elif rollback_vector is not None:
        existing_dataset_id = await _rollback_candidate_id(fhir, endpoint_id, identity)
        candidate_key = f"cms-npd-rollback:{rollback_vector}:{rollback_root}"
    else:
        original_id = fhir._endpoint_dataset_candidate_id(
            endpoint_id, tuple(sorted(RESOURCE_SET)), identity["vector_sha256"]
        )
        original_state = await fhir._endpoint_dataset_state(original_id)
        was_superseded = bool(original_state and original_state.get("status") == fhir.ENDPOINT_DATASET_SUPERSEDED)
        if was_superseded:
            _assert_candidate_release(original_state, identity)
            if (
                original_state.get("endpoint_id") != endpoint_id
                or original_state.get("is_current") is not False
                or original_state.get("published_at") is None
            ):
                raise RuntimeError("cms_npd_candidate_identity_changed")
        if was_superseded or await recovery.is_disposed(fhir, original_id):
            existing_dataset_id = await recovery.reusable_vector_candidate(fhir, endpoint_id, identity)
            if existing_dataset_id is None:
                candidate_key = f"cms-npd-reacquire:{identity['vector_sha256']}:{run_id}"
    selected_candidate = await _candidate(
        fhir,
        endpoint_id,
        run_id,
        identity,
        candidate_key=candidate_key,
        existing_dataset_id=existing_dataset_id,
    )
    return await _reacquire_unwitnessed_candidate(fhir, endpoint_id, run_id, identity, selected_candidate)


async def _rollback_candidate_id(fhir, endpoint_id, identity):
    """Reuse only this exact current release or a candidate pinned to its current predecessor."""
    current_id = await fhir._current_endpoint_dataset_id(endpoint_id, exclude_dataset_id="")
    current_state = await fhir._endpoint_dataset_state(current_id) if current_id else None
    metadata = current_state.get("publication_metadata_json") if current_state else None
    if (
        current_state
        and current_state.get("status") == fhir.ENDPOINT_DATASET_PUBLISHED
        and current_state.get("is_current") is True
        and isinstance(metadata, dict)
        and metadata.get("source_release") == identity
    ):
        return current_id
    if current_id:
        return await recovery.reusable_vector_candidate(fhir, endpoint_id, identity, previous_dataset_id=current_id)
    return None


async def _reacquire_unwitnessed_candidate(
    fhir: Any, endpoint_id: str, run_id: str, identity: dict[str, Any], selected_candidate: Any
) -> Any:
    """Upgrade a sealed pre-witness candidate without editing its authority."""

    if not (selected_candidate.already_validated or selected_candidate.already_published):
        return selected_candidate
    has_rows = bool(sum(file_by_field["distinct_count"] for file_by_field in identity["files"].values()))
    if (not has_rows or await _has_raw_witnesses(fhir, selected_candidate.dataset_id)) and (
        await relationships.completed_receipt_count(fhir, selected_candidate.dataset_id, identity["vector_sha256"])
        is not None
    ):
        return selected_candidate
    # An older sealed candidate cannot be edited. Reacquire into a stable new
    # candidate while a published predecessor remains the current authority.
    if selected_candidate.already_validated:
        await recovery.retire_unwitnessed_validated_candidate(fhir, selected_candidate, identity)
    predecessor_id = (
        selected_candidate.dataset_id
        if selected_candidate.already_published
        else selected_candidate.previous_dataset_id
    )
    reusable_id = await recovery.reusable_vector_candidate(
        fhir, endpoint_id, identity, previous_dataset_id=predecessor_id
    )
    return await _candidate(
        fhir,
        endpoint_id,
        run_id,
        identity,
        candidate_key=f"cms-npd-witness-upgrade:{selected_candidate.dataset_id}:{run_id}",
        existing_dataset_id=reusable_id,
    )


async def _has_raw_witnesses(fhir: Any, dataset_id: str) -> bool:
    witness_table = fhir._qt(fhir._schema(), ProviderDirectoryCMSNPDResourceWitness.__tablename__)
    return bool(
        await fhir.db.scalar(
            f"SELECT EXISTS (SELECT 1 FROM {witness_table} WHERE dataset_id=:dataset_id)",
            dataset_id=dataset_id,
        )
    )


def _parse_batch_row(fhir: Any, resource: dict[str, Any], candidate: Any) -> tuple[type, dict[str, Any]]:
    projection_date = dt.date.fromisoformat(candidate.semantic_projection_as_of)
    parsed = fhir.parse_fhir_resource(
        SOURCE_ID,
        resource,
        acquisition=fhir.FHIRAcquisitionContext(semantic_projection_as_of=projection_date),
        run_id=candidate.acquisition_root_run_id,
    )
    if parsed is None:
        raise source.CmsNpdSourceError("cms_npd_resource_invalid")
    model, resource_row_by_field = parsed
    if model is fhir.ProviderDirectoryOrganization:
        resource_row_by_field["tax_id"] = None  # CMS pseudo-EIN remains only in source identifiers.
    return model, resource_row_by_field


def _source_witnesses_by_id(
    raw_resources: list[dict[str, Any]], candidate: Any, resource_type: str
) -> dict[str, dict[str, Any]]:
    """Deduplicate identical raw occurrences under one release identity."""

    witness_by_id: dict[str, dict[str, Any]] = {}
    for raw_resource_by_field in raw_resources:
        resource_id = raw_resource_by_field["id"]
        raw_payload = json.dumps(
            raw_resource_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        raw_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
        existing_witness = witness_by_id.get(resource_id)
        if existing_witness is not None and existing_witness["raw_payload_sha256"] != raw_hash:
            raise RuntimeError("cms_npd_witness_payload_conflict")
        witness_by_id[resource_id] = {
            "dataset_id": candidate.dataset_id,
            "source_id": SOURCE_ID,
            "release_id": candidate.source_release["vector_sha256"],
            "resource_type": resource_type,
            "resource_id": resource_id,
            "raw_payload_sha256": raw_hash,
            "raw_payload_json": raw_resource_by_field,
        }
    return witness_by_id


async def _insert_verified_witnesses(session: Any, witness_by_id: dict[str, dict[str, Any]]) -> None:
    """Insert new witnesses and check replay using fixed-size bindings and hashes."""

    fhir = importlib.import_module("process.provider_directory_fhir")
    table = ProviderDirectoryCMSNPDResourceWitness.__table__
    # The shared normalized COPY serializer strips NULs; raw witnesses must never change.
    if any(
        fhir._strip_postgres_nuls(witness["raw_payload_json"]) != witness["raw_payload_json"]
        for witness in witness_by_id.values()
    ):
        raise source.CmsNpdSourceError("cms_npd_resource_invalid")
    replay_fields = (
        "dataset_id",
        "source_id",
        "release_id",
        "resource_type",
        "resource_id",
        "raw_payload_sha256",
        "normalized_payload_hash",
    )
    first_witness = next(iter(witness_by_id.values()))
    await fhir._copy_upsert_rows(
        ProviderDirectoryCMSNPDResourceWitness,
        list(witness_by_id.values()),
        [column.name for column in table.columns],
        ["dataset_id", "resource_type", "resource_id"],
        skip_unchanged=False,
        transaction_session=session,
        ignore_conflicts=True,
    )
    stored_witnesses = (
        (
            await session.execute(
                select(*(table.c[field] for field in replay_fields)).where(
                    table.c.dataset_id == first_witness["dataset_id"],
                    table.c.resource_type == first_witness["resource_type"],
                    table.c.resource_id.in_(witness_by_id),
                )
            )
        )
        .mappings()
        .all()
    )
    if len(stored_witnesses) != len(witness_by_id) or any(
        any(stored_witness[field] != witness_by_id[stored_witness["resource_id"]][field] for field in replay_fields)
        for stored_witness in stored_witnesses
    ):
        raise RuntimeError("cms_npd_witness_payload_conflict")


async def _persist_source_batch(
    fhir: Any,
    model: type,
    resource_rows: list[dict[str, Any]],
    raw_resources: list[dict[str, Any]],
    candidate: Any,
    resource_type: str,
) -> None:
    """Commit normalized rows and their complete CMS witnesses together."""

    if len(resource_rows) != len(raw_resources):
        raise RuntimeError("cms_npd_witness_batch_invalid")
    witness_by_id = _source_witnesses_by_id(raw_resources, candidate, resource_type)
    async with fhir.db.transaction() as session:
        normalized_rows = await fhir._persist_endpoint_dataset_rows(
            model,
            resource_rows,
            candidate.dataset_id,
            resource_hash_contract=candidate.resource_hash_contract,
            semantic_projection_as_of=candidate.semantic_projection_as_of,
        )
        normalized_by_id = {normalized_row["resource_id"]: normalized_row for normalized_row in normalized_rows}
        if set(normalized_by_id) != set(witness_by_id) or any(
            normalized_row["resource_type"] != resource_type or normalized_row["dataset_id"] != candidate.dataset_id
            for normalized_row in normalized_rows
        ):
            raise RuntimeError("cms_npd_witness_projection_mismatch")
        for resource_id, witness in witness_by_id.items():
            witness["normalized_payload_hash"] = normalized_by_id[resource_id]["payload_hash"]
        await _insert_verified_witnesses(session, witness_by_id)


async def _stream_file(fhir: Any, path: Path, candidate: Any, resource_type: str, ctx: dict, task: dict) -> int:
    model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
    resource_rows: list[dict[str, Any]] = []
    raw_resources: list[dict[str, Any]] = []
    row_count = 0
    batch_bytes = 0
    with zstd.open(path, "rb") as decoded:
        while line := decoded.readline(source.MAX_RESOURCE_LINE_BYTES + 1):
            if len(line) > source.MAX_RESOURCE_LINE_BYTES:
                raise source.CmsNpdSourceError("cms_npd_decoded_size_invalid")
            try:
                resource = json.loads(line)
            except (UnicodeError, ValueError) as error:
                raise source.CmsNpdSourceError("cms_npd_ndjson_invalid") from error
            if not isinstance(resource, dict) or resource.get("resourceType") != resource_type:
                raise source.CmsNpdSourceError("cms_npd_resource_invalid")
            parsed_model, resource_row_by_field = _parse_batch_row(fhir, resource, candidate)
            if parsed_model is not model:
                raise source.CmsNpdSourceError("cms_npd_resource_invalid")
            resource_rows.append(resource_row_by_field)
            raw_resources.append(resource)
            row_count += 1
            batch_bytes += len(line)
            if len(resource_rows) >= BATCH_SIZE or batch_bytes >= BATCH_MAX_DECODED_BYTES:
                await _persist_source_batch(fhir, model, resource_rows, raw_resources, candidate, resource_type)
                _observe_intake(ctx, phase="staging", family=resource_type, completed_rows=row_count)
                resource_rows.clear()
                raw_resources.clear()
                batch_bytes = 0
                await fhir._raise_if_resource_import_cancelled(ctx, task)
    if resource_rows:
        await _persist_source_batch(fhir, model, resource_rows, raw_resources, candidate, resource_type)
    _observe_intake(ctx, phase="staging", family=resource_type, completed_rows=row_count, force=True)
    return row_count


async def _materialize_identity_evidence(
    fhir: Any,
    directory: Path,
    candidate: Any,
    identity: dict[str, Any],
    ctx: dict,
    task: dict,
) -> None:
    """Replay exact entity and network facts in bounded, committed transactions."""

    if candidate.already_validated or candidate.already_published:
        await _backfill_network_roles(fhir, identity, ctx, task)
        _observe_intake(ctx, phase="identity_validation")
        await _assert_identity_evidence(fhir, candidate, identity)
        return

    entity_writer = importlib.import_module("process.provider_directory_entity_identity")
    network_writer = importlib.import_module("process.provider_directory_insurance_network_identity")
    resource_writer = importlib.import_module("process.provider_directory_resource_identity")
    selected_resource_types = {"Organization", "Location", "InsurancePlan", "PractitionerRole"}
    for name, resource_type in source.RESOURCE_FILES:
        if resource_type not in selected_resource_types:
            continue
        _observe_intake(ctx, phase="identity", family=resource_type)
        resources: list[dict[str, Any]] = []
        batch_bytes = 0
        row_count = 0
        with zstd.open(directory / f"{name}.zst", "rb") as decoded:
            while line := decoded.readline(source.MAX_RESOURCE_LINE_BYTES + 1):
                if len(line) > source.MAX_RESOURCE_LINE_BYTES:
                    raise source.CmsNpdSourceError("cms_npd_decoded_size_invalid")
                try:
                    resource = json.loads(line)
                except (UnicodeError, ValueError) as error:
                    raise source.CmsNpdSourceError("cms_npd_ndjson_invalid") from error
                if not isinstance(resource, dict) or resource.get("resourceType") != resource_type:
                    raise source.CmsNpdSourceError("cms_npd_resource_invalid")
                resources.append(resource)
                row_count += 1
                batch_bytes += len(line)
                if len(resources) >= IDENTITY_BATCH_SIZE or batch_bytes >= BATCH_MAX_DECODED_BYTES:
                    await _write_identity_batch(
                        fhir, entity_writer, network_writer, resource_writer, resource_type, resources, identity
                    )
                    _observe_intake(ctx, phase="identity", family=resource_type, completed_rows=row_count)
                    resources.clear()
                    batch_bytes = 0
                    await fhir._raise_if_resource_import_cancelled(ctx, task)
        if resources:
            await _write_identity_batch(
                fhir, entity_writer, network_writer, resource_writer, resource_type, resources, identity
            )
        _observe_intake(ctx, phase="identity", family=resource_type, completed_rows=row_count, force=True)
        if row_count != identity["files"][name]["row_count"]:
            raise RuntimeError("cms_npd_identity_file_row_count_changed")
    _observe_intake(ctx, phase="identity_validation")
    await _assert_identity_evidence(fhir, candidate, identity)


async def _backfill_network_roles(fhir: Any, identity: dict[str, Any], ctx: dict, task: dict) -> None:
    """Bind newly recognized network roles from immutable same-release source facts."""

    schema = fhir._schema()
    evidence_table = fhir._qt(schema, "provider_directory_entity_release_evidence")
    binding_table = fhir._qt(schema, "provider_directory_insurance_network_source_binding")
    network_writer = importlib.import_module("process.provider_directory_insurance_network_identity")
    after_id = ""
    while True:
        missing = await fhir.db.all(
            f"SELECT organization.resource_id, organization.payload_json FROM {evidence_table} AS organization "
            "WHERE organization.source_id=:source_id AND organization.release_id=:release_id "
            "AND organization.resource_type='Organization' AND organization.resource_id>:after_id "
            f"AND ({_NETWORK_ROLE_SQL}) "
            f"AND NOT EXISTS (SELECT 1 FROM {binding_table} AS network "
            "WHERE network.source_id=organization.source_id AND network.resource_type='Organization' "
            "AND network.resource_id=organization.resource_id) "
            "ORDER BY organization.resource_id LIMIT :batch_size",
            source_id=SOURCE_ID,
            release_id=identity["vector_sha256"],
            after_id=after_id,
            batch_size=IDENTITY_BATCH_SIZE,
        )
        if not missing:
            return
        async with fhir.db.session() as session:
            for resource_id, organization in missing:
                await network_writer.record_insurance_network_organization(
                    session, source_id=SOURCE_ID, release_id=identity["vector_sha256"], organization=organization
                )
                after_id = resource_id
        await fhir._raise_if_resource_import_cancelled(ctx, task)


async def _write_identity_batch(
    fhir: Any,
    entity_writer: Any,
    network_writer: Any,
    resource_writer: Any,
    resource_type: str,
    resources: list[dict[str, Any]],
    identity: dict[str, Any],
) -> None:
    """Persist only exact, source-scoped identities and direct network evidence."""
    if resource_type in {"Organization", "Location"}:
        async with fhir.db.session() as session:
            await entity_writer.bind_entity_batch(
                session,
                source_id=SOURCE_ID,
                release_id=identity["vector_sha256"],
                resources=resources,
            )
            if resource_type == "Organization":
                for resource in filter(network_writer.has_source_declared_network_role, resources):
                    await network_writer.record_insurance_network_organization(
                        session, source_id=SOURCE_ID, release_id=identity["vector_sha256"], organization=resource
                    )
        return

    async with fhir.db.session() as session:
        await resource_writer.bind_resource_identity_batch(
            session,
            source_id=SOURCE_ID,
            resource_type=resource_type,
            resource_ids=[resource["id"] for resource in resources],
        )
    if resource_type == "PractitionerRole":
        return

    await _write_plan_network_batch(fhir, network_writer, resources, identity)


async def _write_plan_network_batch(
    fhir: Any,
    network_writer: Any,
    resources: list[dict[str, Any]],
    identity: dict[str, Any],
) -> None:
    """Record only plan links to Organization IDs seen in this release."""

    plan_network_targets: list[tuple[dict[str, Any], list[str]]] = []
    all_network_resource_ids: set[str] = set()
    for resource in resources:
        network_resource_ids = [
            match.group(1)
            for reference in fhir._insurance_plan_network_references(resource)
            if (match := _LOCAL_ORGANIZATION_REF.fullmatch(reference))
        ]
        plan_network_targets.append((resource, network_resource_ids))
        all_network_resource_ids.update(network_resource_ids)
    if not all_network_resource_ids:
        return
    entity_table = fhir._qt(fhir._schema(), "provider_directory_entity_release_evidence")
    bound_org_rows = await fhir.db.all(
        f"SELECT resource_id FROM {entity_table} "
        "WHERE source_id=:source_id AND release_id=:release_id "
        "AND resource_type='Organization' "
        "AND resource_id = ANY(CAST(:resource_ids AS varchar[]))",
        source_id=SOURCE_ID,
        release_id=identity["vector_sha256"],
        resource_ids=list(all_network_resource_ids),
    )
    bound_network_resource_ids = {str(bound_org_row[0]) for bound_org_row in bound_org_rows}
    if not bound_network_resource_ids:
        return
    async with fhir.db.session() as session:
        for resource, network_resource_ids in plan_network_targets:
            for network_resource_id in network_resource_ids:
                if network_resource_id not in bound_network_resource_ids:
                    continue
                await network_writer.record_insurance_network_plan(
                    session,
                    source_id=SOURCE_ID,
                    release_id=identity["vector_sha256"],
                    network_resource_id=network_resource_id,
                    plan=resource,
                )


async def _assert_identity_evidence(fhir: Any, candidate: Any, identity: dict[str, Any]) -> None:
    """Require the exact release's durable bindings before source-local cutover."""

    schema = fhir._schema()
    dataset_table = fhir._qt(schema, fhir.ProviderDirectoryDatasetResource.__tablename__)
    entity_table = fhir._qt(schema, "provider_directory_entity_release_evidence")
    network_table = fhir._qt(schema, "provider_directory_insurance_network_plan_evidence")
    resource_identity_table = fhir._qt(schema, "provider_directory_resource_identity")
    for resource_type in ("Organization", "Location"):
        actual = await fhir.db.scalar(
            f"SELECT count(*) FROM {entity_table} "
            "WHERE source_id=:source_id AND release_id=:release_id AND resource_type=:resource_type",
            source_id=SOURCE_ID,
            release_id=identity["vector_sha256"],
            resource_type=resource_type,
        )
        name = next(name for name, kind in source.RESOURCE_FILES if kind == resource_type)
        if int(actual or 0) != identity["files"][name]["distinct_count"]:
            raise RuntimeError("cms_npd_identity_evidence_incomplete")
    for resource_type in ("InsurancePlan", "PractitionerRole"):
        name = next(name for name, kind in source.RESOURCE_FILES if kind == resource_type)
        actual = await fhir.db.scalar(
            f"SELECT count(*) FROM {dataset_table} AS resource "
            f"JOIN {resource_identity_table} AS binding ON binding.source_id=:source_id "
            "AND binding.resource_type=resource.resource_type AND binding.resource_id=resource.resource_id "
            "WHERE resource.dataset_id=:dataset_id AND resource.resource_type=:resource_type",
            source_id=SOURCE_ID,
            dataset_id=candidate.dataset_id,
            resource_type=resource_type,
        )
        if int(actual or 0) != identity["files"][name]["distinct_count"]:
            raise RuntimeError("cms_npd_identity_evidence_incomplete")
    expected_networks = await fhir.db.scalar(
        "SELECT count(*) FROM ("
        " SELECT DISTINCT resource.resource_id, reference.value"
        f" FROM {dataset_table} AS resource"
        " CROSS JOIN LATERAL jsonb_array_elements_text("
        " CASE WHEN jsonb_typeof(resource.payload_json::jsonb -> 'network_refs') = 'array'"
        " THEN resource.payload_json::jsonb -> 'network_refs' ELSE '[]'::jsonb END"
        " ) AS reference(value)"
        " WHERE resource.dataset_id=:dataset_id AND resource.resource_type='InsurancePlan'"
        " AND reference.value ~ '^Organization/[A-Za-z0-9.-]{1,64}$'"
        f" AND EXISTS (SELECT 1 FROM {entity_table} AS organization"
        " WHERE organization.source_id=:source_id AND organization.release_id=:release_id"
        " AND organization.resource_type='Organization'"
        " AND organization.resource_id=split_part(reference.value, '/', 2))"
        ") AS expected",
        dataset_id=candidate.dataset_id,
        source_id=SOURCE_ID,
        release_id=identity["vector_sha256"],
    )
    actual_networks = await fhir.db.scalar(
        f"SELECT count(*) FROM {network_table} WHERE source_id=:source_id AND release_id=:release_id",
        source_id=SOURCE_ID,
        release_id=identity["vector_sha256"],
    )
    if int(actual_networks or 0) != int(expected_networks or 0):
        raise RuntimeError("cms_npd_identity_evidence_incomplete")
    await _assert_network_role_bindings(fhir, entity_table, identity["vector_sha256"])


async def _assert_network_role_bindings(fhir: Any, entity_table: str, release_id: str) -> None:
    """Require bindings for all exact source-declared network roles."""
    network_binding_table = fhir._qt(fhir._schema(), "provider_directory_insurance_network_source_binding")
    missing_role_networks = await fhir.db.scalar(
        f"SELECT count(*) FROM {entity_table} AS organization "
        "WHERE organization.source_id=:source_id AND organization.release_id=:release_id "
        "AND organization.resource_type='Organization' "
        f"AND ({_NETWORK_ROLE_SQL}) "
        f"AND NOT EXISTS (SELECT 1 FROM {network_binding_table} AS network "
        "WHERE network.source_id=organization.source_id AND network.resource_type='Organization' "
        "AND network.resource_id=organization.resource_id)",
        source_id=SOURCE_ID,
        release_id=release_id,
    )
    if missing_role_networks:
        raise RuntimeError("cms_npd_identity_evidence_incomplete")


async def _assert_counts(fhir: Any, candidate: Any, identity: dict[str, Any]) -> dict[str, int]:
    rows = await fhir.db.all(
        f"SELECT resource_type, count(*) AS row_count FROM "
        f"{fhir._qt(fhir._schema(), fhir.ProviderDirectoryDatasetResource.__tablename__)} "
        "WHERE dataset_id=:dataset_id GROUP BY resource_type",
        dataset_id=candidate.dataset_id,
    )
    counts_by_type = {}
    for row in rows:
        row_by_field = fhir._pagination_checkpoint_row_mapping(row)
        counts_by_type[str(row_by_field["resource_type"])] = int(row_by_field["row_count"])
    expected_by_type = {
        resource_type: identity["files"][name]["distinct_count"] for name, resource_type in source.RESOURCE_FILES
    }
    if {
        resource_type: counts_by_type.get(resource_type, 0) for resource_type in RESOURCE_TYPES
    } != expected_by_type or set(counts_by_type) - RESOURCE_SET:
        raise RuntimeError("cms_npd_candidate_counts_incomplete")
    return expected_by_type


async def _assert_witness_counts(fhir: Any, candidate: Any, identity: dict[str, Any]) -> None:
    """Require one exact raw witness for every normalized release row."""

    resource_table = fhir._qt(fhir._schema(), fhir.ProviderDirectoryDatasetResource.__tablename__)
    witness_table = fhir._qt(fhir._schema(), ProviderDirectoryCMSNPDResourceWitness.__tablename__)
    projection_count_rows = await fhir.db.all(
        "SELECT resource.resource_type, count(*) AS projected_count, "
        "count(witness.resource_id) FILTER (WHERE witness.source_id=:source_id "
        "AND witness.release_id=:release_id "
        "AND witness.normalized_payload_hash=resource.payload_hash "
        "AND witness.raw_payload_json->>'resourceType'=resource.resource_type "
        "AND witness.raw_payload_json->>'id'=resource.resource_id) AS witnessed_count "
        f"FROM {resource_table} AS resource LEFT JOIN {witness_table} AS witness "
        "ON witness.dataset_id=resource.dataset_id AND witness.resource_type=resource.resource_type "
        "AND witness.resource_id=resource.resource_id "
        "WHERE resource.dataset_id=:dataset_id GROUP BY resource.resource_type",
        source_id=SOURCE_ID,
        release_id=identity["vector_sha256"],
        dataset_id=candidate.dataset_id,
    )
    count_by_type = {
        str(projection_count_row[0]): (int(projection_count_row[1]), int(projection_count_row[2]))
        for projection_count_row in projection_count_rows
    }
    expected_count_by_type = {
        resource_type: identity["files"][name]["distinct_count"] for name, resource_type in source.RESOURCE_FILES
    }
    if set(count_by_type) - RESOURCE_SET or any(
        count_by_type.get(resource_type, (0, 0)) != (count, count)
        for resource_type, count in expected_count_by_type.items()
    ):
        raise RuntimeError("cms_npd_witness_counts_incomplete")


async def _validate_candidate(fhir: Any, candidate: Any, identity: dict, counts_by_type: dict) -> None:
    """Validate the complete resource vector without moving the serving pointer."""

    if not candidate.already_validated and not candidate.already_published:
        diagnostics_by_resource = {
            resource_type: {
                "complete": True,
                "collection_complete": True,
                "bounded": False,
                "next_url_remaining": False,
                "rows_fetched": identity["files"][name]["row_count"],
                "rows_written": counts_by_type[resource_type],
                "fetch_mode": "cms_npd_retained_file",
            }
            for name, resource_type in source.RESOURCE_FILES
        }
        finalization = await fhir._finalize_endpoint_dataset_candidate(candidate, diagnostics_by_resource)
        if not finalization or finalization.get("validated") is not True:
            raise RuntimeError("cms_npd_candidate_validation_failed")


async def _serving_candidate_descriptor(fhir, state, proof_by_field):
    """Bind prepared coverage to the exact desired parent and observed incumbent."""
    desired_pair = _cms_dataset_pair(state, allow_desired=True)
    current_dataset_id = await fhir._current_endpoint_dataset_id(state["endpoint_id"], exclude_dataset_id="")
    current_state = await fhir._endpoint_dataset_state(current_dataset_id) if current_dataset_id else None
    incumbent_pair = _cms_dataset_pair(current_state) if current_state else None
    if any(state[field] != proof_by_field[field] for field in ("dataset_id", "endpoint_id", "dataset_hash")):
        raise RuntimeError("cms_npd_candidate_coverage_changed")
    return {
        "version": 1,
        "status": "ready",
        "desired_cms_dataset": desired_pair,
        "expected_cms_incumbent": incumbent_pair,
        "release_id": proof_by_field["release_id"],
        "proof_version": proof_by_field["proof_version"],
    }


async def _prepare_serving_candidate(fhir, candidate, identity, directory, receipt_by_field, client, ctx, task):
    """Seal complete candidate coverage without moving any serving pointer."""
    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, client, task)
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    _assert_candidate_release(state, identity)
    dataset_hash = state.get("dataset_hash")
    if not isinstance(dataset_hash, str) or len(dataset_hash) != 64:
        raise RuntimeError("cms_npd_coverage_candidate_hash_invalid")
    await fhir._raise_if_resource_import_cancelled(ctx, task)
    proof_by_field = await coverage.prepare_cms_candidate_coverage(
        fhir, candidate, identity["vector_sha256"], dataset_hash
    )
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    _assert_candidate_release(state, identity)
    _assert_retained_receipt_pin(directory, task)
    return state, await _serving_candidate_descriptor(fhir, state, proof_by_field)


async def _assert_intake_guard(connection, lock_key, backend_id):
    """Reject connection replacement or loss of the exact endpoint advisory lock."""
    try:
        async with asyncio.timeout(2):
            is_owner = await connection.scalar(
                text("""SELECT pg_backend_pid()=:backend_id AND EXISTS (
                SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted
                  AND mode='ExclusiveLock' AND objsubid=1
                  AND classid::bigint=((hashtextextended(:lock_key,0) >> 32) & 4294967295)
                  AND objid::bigint=(hashtextextended(:lock_key,0) & 4294967295))"""),
                {"lock_key": lock_key, "backend_id": backend_id},
            )
            await connection.commit()
    except Exception as error:
        raise RuntimeError("cms_npd_intake_guard_lost") from error
    if is_owner is not True:
        raise RuntimeError("cms_npd_intake_guard_lost")


async def _watch_intake_guard(connection, lock_key, backend_id, owner, failures, stop):
    """Cancel active staging promptly if its dedicated ownership backend disappears."""
    try:
        while not stop.is_set():
            try:
                async with asyncio.timeout(1):
                    await stop.wait()
            except TimeoutError:
                await _assert_intake_guard(connection, lock_key, backend_id)
    except Exception as error:
        failures.append(error)
        owner.cancel()


@asynccontextmanager
async def _intake_guard(fhir, endpoint_id):
    """Serialize CMS intake and rollback; abort staged work on native lock loss."""
    if fhir._provider_directory_database_pool_capacity() < 2:
        raise RuntimeError("cms_npd_intake_pool_capacity_exceeded")
    if fhir.db.engine is None:
        await fhir.db.connect()
    lock_key = "provider-directory-cms-intake:" + endpoint_id
    connection = await fhir._acquire_provider_directory_artifact_build_lock(fhir.db.engine, lock_key)
    if connection is None:
        raise RuntimeError("cms_npd_acquisition_in_progress")
    watcher = None
    stop = asyncio.Event()
    failures = []
    owner = asyncio.current_task()
    try:
        backend_id = await connection.scalar(text("SELECT pg_backend_pid()"))
        await _assert_intake_guard(connection, lock_key, backend_id)
        watcher = asyncio.create_task(_watch_intake_guard(connection, lock_key, backend_id, owner, failures, stop))
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(watcher, return_exceptions=True)
        await _assert_intake_guard(connection, lock_key, backend_id)
    except asyncio.CancelledError:
        if failures:
            owner.uncancel()
            if owner.cancelling():
                raise
            raise RuntimeError("cms_npd_intake_guard_lost") from failures[0]
        raise
    finally:
        if watcher is not None and not watcher.done():
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        await fhir._release_provider_directory_artifact_build_lock(connection, lock_key)


async def _tax_candidate_followup_status(fhir: Any, directory: Path, candidate: Any, identity: dict[str, Any]):
    """Keep optional tax-candidate work outside the core publication result."""
    try:
        return await importlib.import_module("process.cms_npd_tax_candidate_followup").cms_npd_tax_candidate_followup(
            fhir,
            release_directory=directory,
            dataset_id=candidate.dataset_id,
            vector_sha256=identity["vector_sha256"],
            generated_at=identity["generated_at"],
        )
    except Exception:
        return {"status": "failed", "retryable": True, "retry_via": "same_byte_import"}


async def _stage_and_validate_candidate(fhir, directory, candidate, identity, receipt_by_field, ctx, task):
    """Retain bounded resources and verify their counts, witnesses, and resolved evidence."""
    if not candidate.already_validated and not candidate.already_published:
        for name, resource_type in source.RESOURCE_FILES:
            _observe_intake(ctx, phase="staging", family=resource_type)
            count = await _stream_file(fhir, directory / f"{name}.zst", candidate, resource_type, ctx, task)
            if count != identity["files"][name]["row_count"]:
                raise RuntimeError("cms_npd_file_row_count_changed")
    _observe_intake(ctx, phase="count_validation")
    counts_by_type = await _assert_counts(fhir, candidate, identity)
    _observe_intake(ctx, phase="release_validation")
    await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, None, task)
    _observe_intake(ctx, phase="witness_validation")
    await _assert_witness_counts(fhir, candidate, identity)
    _observe_intake(ctx, phase="identity")
    await _materialize_identity_evidence(fhir, directory, candidate, identity, ctx, task)
    _observe_intake(ctx, phase="relationships")
    await relationships.materialize(fhir, candidate, identity["vector_sha256"], ctx, task)
    _observe_intake(ctx, phase="release_validation")
    await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, None, task)
    _observe_intake(ctx, phase="candidate_validation")
    await _validate_candidate(fhir, candidate, identity, counts_by_type)


async def _stage_acquired(
    ctx: dict[str, Any],
    task: dict[str, Any],
    run_id: str,
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
    endpoint_id: str,
) -> dict[str, Any]:
    """Stage a verified release and retain it for composite serving publication."""

    fhir = importlib.import_module("process.provider_directory_fhir")

    identity = release_identity(receipt_by_field)
    _observe_intake(ctx, phase="release_validation")
    await _verify_release(directory, receipt_by_field, client, task)
    _observe_intake(ctx, phase="candidate_setup")
    repair_selection = await recovery.repaired_candidate_selection(fhir, endpoint_id, identity, task, run_id)
    if task.get("cms_npd_retained_operation") or task.get("provider_directory_repair_id") is not None:
        await _register_source(fhir)
    await recovery.resume_pending_cleanup(fhir, endpoint_id)
    await recovery.dispose_prior_vectors(fhir, endpoint_id, identity["vector_sha256"])
    candidate = await _admission_candidate(fhir, endpoint_id, run_id, identity, task, repair_selection)
    await _stage_and_validate_candidate(fhir, directory, candidate, identity, receipt_by_field, ctx, task)
    _observe_intake(ctx, phase="coverage")
    state, serving_candidate = await _prepare_serving_candidate(
        fhir, candidate, identity, directory, receipt_by_field, client, ctx, task
    )
    # Optional tax evidence is produced only after a common serving generation exists.
    _observe_intake(ctx, phase="followup")
    tax_candidate_status_by_field = (
        await _tax_candidate_followup_status(fhir, directory, candidate, identity)
        if candidate.already_published
        else {"status": "pending", "retryable": True, "retry_via": "same_byte_import"}
    )
    return {
        "source_id": SOURCE_ID,
        "dataset_id": candidate.dataset_id,
        "vector_sha256": identity["vector_sha256"],
        "resource_count": int(state.get("resource_count") or 0),
        "status": state["status"],
        "replayed": candidate.already_published,
        "cms_serving_candidate": serving_candidate,
        "tax_candidates": tax_candidate_status_by_field,
    }


async def _run_acquired(ctx, task, run_id, directory, receipt_by_field, client):
    """Retain one endpoint owner's complete candidate before requesting publication."""
    fhir = importlib.import_module("process.provider_directory_fhir")
    retained_operation = task.get("cms_npd_retained_operation")
    has_retained_controls = bool(retained_operation) or task.get("provider_directory_repair_id") is not None
    endpoint_id = _source_endpoint(fhir)["endpoint_id"] if has_retained_controls else await _register_source(fhir)
    _observe_intake(ctx, phase="intake_guard")
    async with _intake_guard(fhir, endpoint_id):
        if retained_operation == "baseline":
            await _assert_baseline_history(fhir, endpoint_id, release_identity(receipt_by_field), task, run_id)
        elif retained_operation == "rollback" or (
            task.get("provider_directory_repair_id") is not None
            and task.get("cms_npd_rollback_vector_sha256") is not None
        ):
            await _assert_rollback_predecessor(fhir, endpoint_id, release_identity(receipt_by_field))
        return await _stage_acquired(ctx, task, run_id, directory, receipt_by_field, client, endpoint_id)


async def _current_observed_publication(observed: source.ObservedRelease) -> dict[str, Any] | None:
    """Find the one covered CMS publication for this exact observed release."""
    return await _current_release_publication(
        observed.vector_sha256, observed.manifest.sha256, observed.manifest.generated_at
    )


async def _current_release_publication(vector_sha256, manifest_sha256, generated_at):
    """Require the current composite receipt and coverage for the exact release."""

    from api.provider_directory_cms_generation import accepted_cms_generation
    from api.provider_directory_entities_contract import DirectoryReadError

    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    fhir = importlib.import_module("process.provider_directory_fhir")
    try:
        async with fhir.db.session() as session:
            generation = await accepted_cms_generation(session, None)
            await coverage.require_cms_coverage(session, fhir._schema(), generation)
    except DirectoryReadError:
        return None
    if generation["release_id"] != vector_sha256:
        return None
    state = await fhir._endpoint_dataset_state(generation["dataset_id"])
    metadata = state.get("publication_metadata_json")
    release = metadata.get("source_release") if isinstance(metadata, dict) else None
    if (
        state.get("status") != fhir.ENDPOINT_DATASET_PUBLISHED
        or state.get("is_current") is not True
        or state.get("dataset_hash") != generation["dataset_hash"]
        or state.get("published_at") != generation["observed_at"]
        or not isinstance(release, dict)
        or release.get("source_id") != SOURCE_ID
        or release.get("vector_sha256") != vector_sha256
        or release.get("manifest_sha256") != manifest_sha256
        or release.get("generated_at") != generated_at
        or not isinstance(state.get("acquisition_root_run_id"), str)
        or not state["acquisition_root_run_id"]
    ):
        return None
    if sum(file["distinct_count"] for file in release["files"].values()) and not await _has_raw_witnesses(
        fhir, state["dataset_id"]
    ):
        return None
    if await relationships.completed_receipt_count(fhir, state["dataset_id"], vector_sha256) is None:
        return None
    return state


async def _prepare_current_serving_candidate(fhir, state, release_id):
    """Repair bounded candidate coverage and bind unchanged current serving inputs."""
    await recovery.resume_pending_cleanup(fhir, state["endpoint_id"])
    await recovery.dispose_prior_vectors(fhir, state["endpoint_id"], release_id)
    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    candidate = SimpleNamespace(dataset_id=state["dataset_id"], endpoint_id=state["endpoint_id"])
    proof_by_field = await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, state["dataset_hash"])
    return await _serving_candidate_descriptor(fhir, state, proof_by_field)


async def _unchanged_publication_result(
    observed: source.ObservedRelease,
    state: dict[str, Any],
    client: httpx.Client,
    *,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Recheck upstream and request the exact current composite serving inputs."""

    _observe_intake(ctx, phase="release_validation")
    await asyncio.to_thread(source.assert_observed_release_unchanged, observed, client=client)
    fhir = importlib.import_module("process.provider_directory_fhir")
    _observe_intake(ctx, phase="intake_guard")
    async with _intake_guard(fhir, state["endpoint_id"]):
        _observe_intake(ctx, phase="coverage")
        serving_candidate = await _prepare_current_serving_candidate(fhir, state, observed.vector_sha256)
    return {
        "source_id": SOURCE_ID,
        "dataset_id": state["dataset_id"],
        "vector_sha256": observed.vector_sha256,
        "resource_count": int(state.get("resource_count") or 0),
        "status": "published",
        "replayed": True,
        "cms_serving_candidate": serving_candidate,
    }


async def _completed_tax_candidate_status(
    observed: source.ObservedRelease, current: dict[str, Any], root: Path
) -> dict[str, Any] | None:
    """Use only an exact completed tax report; otherwise retry the retained release."""

    try:
        followup = importlib.import_module("process.cms_npd_tax_candidate_followup")
        completed = getattr(followup, "completed_cms_tax_candidate_report")
        fhir = importlib.import_module("process.provider_directory_fhir")
        release_directory = source.retained_release_directory(root, observed.vector_sha256)
        status_by_field = await completed(
            fhir,
            release_directory=release_directory,
            dataset_id=current["dataset_id"],
            vector_sha256=observed.vector_sha256,
            generated_at=observed.manifest.generated_at,
        )
    except Exception:
        return None
    if (
        isinstance(status_by_field, dict)
        and status_by_field.get("status") in ("complete", "empty")
        and status_by_field.get("retryable") is False
    ):
        return status_by_field
    return None


async def _run_upstream_check(ctx: dict[str, Any], task: dict[str, Any], run_id: str, root: Path) -> dict[str, Any]:
    """Skip retained bytes only when current proof and final probes agree."""

    with httpx.Client(timeout=httpx.Timeout(120.0), follow_redirects=False) as client:
        cms_npd_check_by_field = {
            "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "vector_sha256": None,
            "outcome": "probe_failed",
        }
        ctx.setdefault("context", {})["audit"] = {"cms_npd_check": cms_npd_check_by_field}
        _observe_intake(ctx, phase="source_probe")
        observed = await asyncio.to_thread(source.observe_release, client=client)
        cms_npd_check_by_field.update(
            checked_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            vector_sha256=observed.vector_sha256,
            outcome="acquisition_required",
        )
        current = await _current_observed_publication(observed)
        has_tax_followup = importlib.util.find_spec("process.cms_npd_tax_candidate_followup") is not None
        tax_status_by_field = (
            await _completed_tax_candidate_status(observed, current, root)
            if current is not None and has_tax_followup
            else None
        )
        if current is not None and (not has_tax_followup or tax_status_by_field is not None):
            try:
                admission_result_by_field = await _unchanged_publication_result(observed, current, client, ctx=ctx)
            except source.CmsNpdSourceError:
                cms_npd_check_by_field["outcome"] = "source_vector_changed"
                raise
            cms_npd_check_by_field.update(
                checked_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                outcome="unchanged_current_publication",
            )
            if tax_status_by_field is not None:
                admission_result_by_field["tax_candidates"] = tax_status_by_field
        else:
            if current is not None:
                cms_npd_check_by_field["outcome"] = "followup_replay_required"
            _observe_intake(ctx, phase="acquisition")
            directory, receipt_by_field = await asyncio.to_thread(source.acquire_release, root, client=client)
            with source._release_lock(directory):
                admission_result_by_field = await _run_acquired(ctx, task, run_id, directory, receipt_by_field, client)
        admission_result_by_field["cms_npd_check"] = cms_npd_check_by_field
    return admission_result_by_field


async def _execute_intake(ctx: dict[str, Any], task: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    """Acquire, batch-stage, validate, and request composite CMS serving publication."""

    try:
        validate_task(task, run_id)
        if ctx.get("context", {}).get("test_mode"):
            raise ValueError("cms_npd_import_parameters_invalid")
        assert run_id is not None
        root = durable_artifact_root()
        retained_input = retained_input_selection(task)
        rollback_vector = task.get("cms_npd_rollback_vector_sha256")
        retained_vector = retained_input["vector_sha256"] if retained_input is not None else rollback_vector
        if retained_vector is not None:
            directory = source.retained_release_directory(root, retained_vector)
            with source._release_lock(directory):
                _assert_retained_receipt_pin(directory, task)
                _observe_intake(ctx, phase="retained_validation")
                _, receipt_by_field = await asyncio.to_thread(source.load_retained_release, root, retained_vector)
                _assert_retained_receipt_pin(directory, task)
                admission_result_by_field = await _run_acquired(ctx, task, run_id, directory, receipt_by_field, None)
                _assert_retained_receipt_pin(directory, task)
                if retained_input is not None:
                    admission_result_by_field["cms_retained_input"] = retained_input
        else:
            admission_result_by_field = await _run_upstream_check(ctx, task, run_id, root)
        _observe_intake(ctx, phase="complete", force=True)
        ctx.setdefault("context", {})["audit"] = admission_result_by_field
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        return admission_result_by_field
    except Exception as error:
        _observe_intake(ctx, error=error)
        raise


async def run(ctx: dict[str, Any], task: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    """Acquire, batch-stage, validate, and request composite CMS serving publication."""
    return await _execute_intake(ctx, task, run_id)
