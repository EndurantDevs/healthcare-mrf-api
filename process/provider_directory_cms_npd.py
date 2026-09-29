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
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import ProviderDirectoryCMSNPDResourceWitness
from process import cms_npd_source as source
from process import provider_directory_cms_npd_recovery as recovery
from process import provider_directory_cms_npd_relationship as relationships
from process.provider_directory_source_local_publication import (
    publish_validated_source_local_dataset,
)

SOURCE_ID = source.SOURCE_ID
RESOURCE_TYPES = tuple(resource_type for _, resource_type in source.RESOURCE_FILES)
RESOURCE_SET = frozenset(RESOURCE_TYPES)
ADAPTER_CONTRACT = "cms-npd-bulk-fhir-v1"
BATCH_SIZE = 1_000
BATCH_MAX_DECODED_BYTES = 8 * 1024 * 1024
IDENTITY_BATCH_SIZE = 1_000
_LOCAL_ORGANIZATION_REF = re.compile(r"Organization/([A-Za-z0-9.-]{1,64})\Z")


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

    admitted = fhir._admit_provider_directory_endpoint_components(
        canonical_api_base=source.DOWNLOADS_URL,
        credential_descriptor_json={},
        endpoint_signature_json={
            "transport": "cms_npd_bulk_files",
            "adapter_contract": ADAPTER_CONTRACT,
            "resource_types": sorted(RESOURCE_SET),
        },
    )
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


async def _verify_release(
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
) -> None:
    if client is None:
        await asyncio.to_thread(source.verify_retained_release, directory, receipt_by_field)
    else:
        await asyncio.to_thread(source.verify_release, directory, receipt_by_field, client=client)


async def _verify_or_dispose(
    fhir: Any,
    candidate: Any,
    identity: dict[str, Any],
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
) -> None:
    try:
        await _verify_release(directory, receipt_by_field, client)
    except source.CmsNpdSourceError as error:
        if not candidate.already_published and str(error) in {
            "cms_npd_retained_file_missing",
            "cms_npd_source_vector_changed",
        }:
            await recovery.dispose_changed_vector(fhir, candidate, identity)
        raise


async def _admission_candidate(
    fhir: Any,
    endpoint_id: str,
    run_id: str,
    identity: dict[str, Any],
    task: dict[str, Any],
) -> Any:
    """Select a reusable exact vector candidate or a fresh recurring one."""

    rollback_vector = task.get("cms_npd_rollback_vector_sha256")
    candidate_key = None
    existing_dataset_id = None
    if rollback_vector is not None:
        if rollback_vector != identity["vector_sha256"]:
            raise RuntimeError("cms_npd_rollback_vector_changed")
        await _assert_rollback_predecessor(fhir, endpoint_id, identity)
        rollback_root = task.get("cms_npd_rollback_root_run_id") or run_id
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

    table = ProviderDirectoryCMSNPDResourceWitness.__table__
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
    await session.execute(
        pg_insert(table)
        .values(list(witness_by_id.values()))
        .on_conflict_do_nothing(index_elements=["dataset_id", "resource_type", "resource_id"]),
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
                resource_rows.clear()
                raw_resources.clear()
                batch_bytes = 0
                await fhir._raise_if_resource_import_cancelled(ctx, task)
    if resource_rows:
        await _persist_source_batch(fhir, model, resource_rows, raw_resources, candidate, resource_type)
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
        await _assert_identity_evidence(fhir, candidate, identity)
        return

    entity_writer = importlib.import_module("process.provider_directory_entity_identity")
    network_writer = importlib.import_module("process.provider_directory_insurance_network_identity")
    resource_writer = importlib.import_module("process.provider_directory_resource_identity")
    selected_resource_types = {"Organization", "Location", "InsurancePlan", "PractitionerRole"}
    for name, resource_type in source.RESOURCE_FILES:
        if resource_type not in selected_resource_types:
            continue
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
                    resources.clear()
                    batch_bytes = 0
                    await fhir._raise_if_resource_import_cancelled(ctx, task)
        if resources:
            await _write_identity_batch(
                fhir, entity_writer, network_writer, resource_writer, resource_type, resources, identity
            )
        if row_count != identity["files"][name]["row_count"]:
            raise RuntimeError("cms_npd_identity_file_row_count_changed")
    await _assert_identity_evidence(fhir, candidate, identity)


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
            for offset in range(0, len(resources), 100):
                await entity_writer.bind_entity_batch(
                    session,
                    source_id=SOURCE_ID,
                    release_id=identity["vector_sha256"],
                    resources=resources[offset : offset + 100],
                )
        return

    async with fhir.db.session() as session:
        for offset in range(0, len(resources), 100):
            await resource_writer.bind_resource_identity_batch(
                session,
                source_id=SOURCE_ID,
                resource_type=resource_type,
                resource_ids=[resource["id"] for resource in resources[offset : offset + 100]],
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


async def _publish_covered_candidate(fhir, candidate, identity, directory, receipt_by_field, client, ctx, task):
    """Keep validated CMS evidence stable until coverage and pointer commit together."""

    coverage = importlib.import_module("process.provider_directory_cms_serving_coverage")
    if not candidate.already_published:
        candidate_state = await fhir._endpoint_dataset_state(candidate.dataset_id)
        _assert_candidate_release(candidate_state, identity)
        dataset_hash = candidate_state.get("dataset_hash")
        if not isinstance(dataset_hash, str) or len(dataset_hash) != 64:
            raise RuntimeError("cms_npd_coverage_candidate_hash_invalid")
        await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, client)

        async def before_cutover(session):
            """Validate the candidate without external I/O under proof-table locks."""
            await coverage.validate_cms_candidate_coverage(session, candidate.dataset_id, identity["vector_sha256"])
            _assert_candidate_release(await fhir._endpoint_dataset_state(candidate.dataset_id), identity)
            await fhir._raise_if_resource_import_cancelled(ctx, task)

        await publish_validated_source_local_dataset(
            fhir,
            candidate,
            SOURCE_ID,
            before_cutover=before_cutover,
            before_cutover_timeout_seconds=16 * 60,
            after_promotion=lambda: coverage.seal_cms_candidate_coverage(
                fhir, candidate, identity["vector_sha256"], dataset_hash
            ),
        )
    else:
        await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, client)
        _assert_candidate_release(await fhir._endpoint_dataset_state(candidate.dataset_id), identity)
        async with fhir.db.session() as session:
            covered_dataset_id = await coverage.build_cms_coverage(session)
        if covered_dataset_id != candidate.dataset_id:
            raise RuntimeError("cms_npd_serving_coverage_wrong_dataset")
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    _assert_candidate_release(state, identity)
    if state.get("status") != fhir.ENDPOINT_DATASET_PUBLISHED or state.get("is_current") is not True:
        raise RuntimeError("cms_npd_publication_not_current")
    return state


async def _run_acquired(
    ctx: dict[str, Any],
    task: dict[str, Any],
    run_id: str,
    directory: Path,
    receipt_by_field: dict[str, Any],
    client: httpx.Client | None,
) -> dict[str, Any]:
    """Stage and publish a fully verified release in bounded transactions."""

    fhir = importlib.import_module("process.provider_directory_fhir")

    identity = release_identity(receipt_by_field)
    endpoint_id = await _register_source(fhir)
    await recovery.resume_pending_cleanup(fhir, endpoint_id)
    if client is not None:
        await _verify_release(directory, receipt_by_field, client)
        await recovery.dispose_prior_vectors(fhir, endpoint_id, identity["vector_sha256"])
    candidate = await _admission_candidate(fhir, endpoint_id, run_id, identity, task)
    if not candidate.already_validated and not candidate.already_published:
        for name, resource_type in source.RESOURCE_FILES:
            count = await _stream_file(fhir, directory / f"{name}.zst", candidate, resource_type, ctx, task)
            if count != identity["files"][name]["row_count"]:
                raise RuntimeError("cms_npd_file_row_count_changed")
    counts_by_type = await _assert_counts(fhir, candidate, identity)
    await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, None)
    await _assert_witness_counts(fhir, candidate, identity)
    await _materialize_identity_evidence(fhir, directory, candidate, identity, ctx, task)
    await relationships.materialize(fhir, candidate, identity["vector_sha256"], ctx, task)
    await _verify_or_dispose(fhir, candidate, identity, directory, receipt_by_field, None)
    await _validate_candidate(fhir, candidate, identity, counts_by_type)
    state = await _publish_covered_candidate(fhir, candidate, identity, directory, receipt_by_field, client, ctx, task)
    dataset_followup = await fhir._source_local_dataset_followup_if_current(
        source_ids=[SOURCE_ID],
        expected_acquisition_root_run_id=candidate.acquisition_root_run_id,
    )
    if dataset_followup is None:
        raise RuntimeError("cms_npd_dataset_followup_missing")
    return {
        "source_id": SOURCE_ID,
        "dataset_id": candidate.dataset_id,
        "vector_sha256": identity["vector_sha256"],
        "resource_count": int(state.get("resource_count") or 0),
        "status": "published",
        "replayed": candidate.already_published,
        "dataset_followup": dataset_followup,
    }


async def _current_observed_publication(observed: source.ObservedRelease) -> dict[str, Any] | None:
    """Find the one covered CMS publication for this exact observed release."""

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
    if generation["release_id"] != observed.vector_sha256:
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
        or release.get("vector_sha256") != observed.vector_sha256
        or release.get("manifest_sha256") != observed.manifest.sha256
        or release.get("generated_at") != observed.manifest.generated_at
        or not isinstance(state.get("acquisition_root_run_id"), str)
        or not state["acquisition_root_run_id"]
    ):
        return None
    if sum(file["distinct_count"] for file in release["files"].values()) and not await _has_raw_witnesses(
        fhir, state["dataset_id"]
    ):
        return None
    if await relationships.completed_receipt_count(fhir, state["dataset_id"], observed.vector_sha256) is None:
        return None
    return state


async def _unchanged_publication_result(
    observed: source.ObservedRelease,
    state: dict[str, Any],
    client: httpx.Client,
) -> dict[str, Any]:
    """Recheck upstream, then emit only the exact current dataset's follow-up."""

    await asyncio.to_thread(source.assert_observed_release_unchanged, observed, client=client)
    fhir = importlib.import_module("process.provider_directory_fhir")
    await recovery.resume_pending_cleanup(fhir, state["endpoint_id"])
    await recovery.dispose_prior_vectors(fhir, state["endpoint_id"], observed.vector_sha256)
    dataset_followup = await fhir._source_local_dataset_followup_if_current(
        source_ids=[SOURCE_ID],
        expected_acquisition_root_run_id=state["acquisition_root_run_id"],
    )
    if (
        dataset_followup is None
        or dataset_followup.get("dataset_id") != state["dataset_id"]
        or dataset_followup.get("dataset_hash") != state["dataset_hash"]
        or dataset_followup.get("endpoint_id") != state["endpoint_id"]
    ):
        raise RuntimeError("cms_npd_dataset_followup_missing")
    return {
        "source_id": SOURCE_ID,
        "dataset_id": state["dataset_id"],
        "vector_sha256": observed.vector_sha256,
        "resource_count": int(state.get("resource_count") or 0),
        "status": "published",
        "replayed": True,
        "dataset_followup": dataset_followup,
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
                admission_result_by_field = await _unchanged_publication_result(observed, current, client)
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
            directory, receipt_by_field = await asyncio.to_thread(source.acquire_release, root, client=client)
            with source._release_lock(directory):
                admission_result_by_field = await _run_acquired(ctx, task, run_id, directory, receipt_by_field, client)
        admission_result_by_field["cms_npd_check"] = cms_npd_check_by_field
    return admission_result_by_field


async def run(ctx: dict[str, Any], task: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    """Acquire, batch-stage, validate, and source-locally publish CMS NPD."""

    validate_task(task, run_id)
    if ctx.get("context", {}).get("test_mode"):
        raise ValueError("cms_npd_import_parameters_invalid")
    assert run_id is not None
    root = durable_artifact_root()
    rollback_vector = task.get("cms_npd_rollback_vector_sha256")
    if rollback_vector is not None:
        directory = source.retained_release_directory(root, rollback_vector)
        with source._release_lock(directory):
            _, receipt_by_field = await asyncio.to_thread(source.load_retained_release, root, rollback_vector)
            result = await _run_acquired(ctx, task, run_id, directory, receipt_by_field, None)
    else:
        result = await _run_upstream_check(ctx, task, run_id, root)
    ctx.setdefault("context", {})["audit"] = result
    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    return result
