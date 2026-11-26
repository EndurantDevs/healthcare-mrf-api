# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect a scoped Nomi capture to the ordinary source dataset publisher."""

from datetime import date
from itertools import batched

from process.nomi_directory_capture import (
    ADAPTER_ID,
    ENDPOINT_SCOPE,
    ENTRY_ID,
    RESOURCE_TYPES,
    SOURCE_ID,
    TRANSPORT,
    acquire_directory,
    load_search_plan,
)
from process.nomi_directory_normalization import NETWORK_ID


def is_explicit_selection(source_ids):
    """Require Nomi to be the only selected source."""
    if SOURCE_ID not in (source_ids or ()):
        return False
    if list(source_ids) != [SOURCE_ID]:
        raise ValueError("nomi_directory_single_source_required")
    return True


def source_row(observed_at):
    """Register a scoped search adapter independently of ordinary FHIR sources."""
    return {
        "source_id": SOURCE_ID,
        "org_name": "Nomi Health",
        "api_base": ENDPOINT_SCOPE,
        "canonical_api_base": ENDPOINT_SCOPE,
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "seed_source": ENTRY_ID,
        "seed_source_detail": ADAPTER_ID,
        "seed_source_url": ENDPOINT_SCOPE,
        "metadata_json": {
            "provider_directory_source_entry_id": ENTRY_ID,
            "provider_directory_transport": TRANSPORT,
            "provider_directory_adapter_id": ADAPTER_ID,
            "provider_directory_supported_resources": list(RESOURCE_TYPES),
            "provider_directory_fully_enumerable_resources": list(RESOURCE_TYPES),
            "provider_directory_acquisition_enabled": True,
            "provider_directory_bulk_export_eligible": False,
            "provider_directory_fhir_endpoint": False,
        },
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def is_source(record):
    """Check the exact recorded adapter identity before allowing acquisition."""
    metadata = record.get("metadata_json")
    transport = metadata.get("provider_directory_transport") if isinstance(metadata, dict) else None
    if record.get("source_id") != SOURCE_ID and transport != TRANSPORT:
        return False
    expected = source_row(None)
    if any(record.get(key) != expected[key] for key in ("source_id", "api_base", "canonical_api_base")) or any(
        metadata.get(key) != value for key, value in expected["metadata_json"].items()
    ):
        raise ValueError("nomi_directory_source_identity_changed")
    return True


def endpoint_contract():
    """Describe the endpoint independently of edition and publisher scope."""
    return {
        "adapter": ADAPTER_ID,
        "transport": TRANSPORT,
        "network": NETWORK_ID,
        "resources": list(RESOURCE_TYPES),
    }


def require_task(task, *, test_mode):
    """Allow only an ordinary deferred acquisition against the configured endpoint."""
    if (
        test_mode
        or task.get("provider_directory_endpoint_scope") != ENDPOINT_SCOPE
        or task.get("probe") is not False
        or task.get("import_resources") is not True
        or any(
            task.get(name)
            for name in (
                "bulk_export",
                "stale_cleanup",
                "seed_only",
                "publish_after_acquisition",
                "publish_source_dataset_only",
                "publish_artifacts_only",
                "publish_artifacts",
                "publish_corroboration",
                "import_completion",
                "source_query",
            )
        )
    ):
        raise ValueError("nomi_directory_acquisition_mode_invalid")


async def _persist_capture(directory, capture, as_of, run_id, persist, cancel_check):
    """Feed bounded batches to the ordinary isolated source candidate writer."""
    written = dict.fromkeys(RESOURCE_TYPES, 0)
    acquisition = directory.FHIRAcquisitionContext(
        fetch_mode=TRANSPORT, semantic_projection_as_of=date.fromisoformat(as_of) if as_of else None
    )
    for batch in batched(capture.iter_resources(), 512):
        await cancel_check()
        rows_by_model = {}
        for resource in batch:
            family, identifier = resource["resourceType"], resource["id"]
            model, parsed_row = directory.parse_fhir_resource(
                SOURCE_ID,
                resource,
                resource_url=f"urn:nomi:normalized:{family}:{identifier}",
                acquisition=acquisition,
                run_id=run_id,
            )
            written[family] += 1
            rows_by_model.setdefault(model, []).append(parsed_row)
        for model, parsed_rows in rows_by_model.items():
            if await persist(model, parsed_rows) != len(parsed_rows):
                raise RuntimeError("nomi_directory_candidate_copy_incomplete")
    return written


async def import_group(directory, source_record, *, run_id, persist, progress_callback, cancel_check):
    """COPY the bounded normalized families after all authorized searches terminate."""
    from process.provider_directory_source_proxy import current_run

    checkpoint = source_record.get("_pagination_checkpoint_context")
    if (
        checkpoint is None
        or not checkpoint.acquisition_root_run_id
        or source_record.get("_endpoint_dataset_id") != checkpoint.dataset_id
        or not source_record.get("_resource_hash_contract")
    ):
        raise ValueError("nomi_directory_prepared_candidate_required")
    scope = current_run()
    binding = scope.bindings.get(SOURCE_ID) if scope is not None else None
    if binding is None or scope.credentials.get(SOURCE_ID):
        raise ValueError("nomi_directory_authenticated_proxy_required")

    async def pause(boundary, cooldowns):
        """Record the publisher boundary in the run audit."""
        scope.record_destination_hold(boundary, cooldowns)

    async with acquire_directory(
        load_search_plan(),
        checkpoint.acquisition_root_run_id,
        binding,
        cancel_check=cancel_check,
        progress=progress_callback,
        pause=pause,
    ) as (capture, proof):
        written = await _persist_capture(
            directory, capture, source_record.get("_semantic_projection_as_of"), run_id, persist, cancel_check
        )
        if written != proof["resource_counts"]:
            raise RuntimeError("nomi_directory_candidate_counts_changed")
    await cancel_check()
    diagnostics_by_family = {
        family: {
            "fetch_mode": TRANSPORT,
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
            "rows_fetched": count,
            "rows_written": count,
            "normalized_output_only": True,
            "nomi_capture": proof,
        }
        for family, count in written.items()
    }
    stats_by_family = {
        family: {
            "sources_attempted": 1,
            "sources_completed": 1,
            "sources_empty": 0,
            "collection_complete_sources": 1,
            "rows_fetched": count,
        }
        for family, count in written.items()
    }
    return [SOURCE_ID], diagnostics_by_family, written, {}, stats_by_family, {}, {}
