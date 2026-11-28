# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect a scoped locator capture to the ordinary source dataset publisher."""

from contextlib import nullcontext
from datetime import date
from itertools import batched

from process import provider_directory_source_proxy as source_proxy


def is_explicit_selection(adapter, source_ids):
    """Admit only a single named locator."""
    if adapter.SOURCE_ID not in (source_ids or ()):
        return False
    if list(source_ids) != [adapter.SOURCE_ID]:
        raise ValueError(f"{adapter.PREFIX}_single_source_required")
    return True


def source_row(adapter, observed_at):
    """Register a scoped search adapter independently of ordinary FHIR sources."""
    return {
        "source_id": adapter.SOURCE_ID,
        "org_name": adapter.ORG_NAME,
        "api_base": adapter.ENDPOINT_SCOPE,
        "canonical_api_base": adapter.ENDPOINT_SCOPE,
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "seed_source": adapter.ENTRY_ID,
        "seed_source_detail": adapter.ADAPTER_ID,
        "seed_source_url": adapter.ENDPOINT_SCOPE,
        "metadata_json": {
            "provider_directory_source_entry_id": adapter.ENTRY_ID,
            "provider_directory_transport": adapter.TRANSPORT,
            "provider_directory_adapter_id": adapter.ADAPTER_ID,
            "provider_directory_supported_resources": list(adapter.RESOURCE_TYPES),
            **(
                {"provider_directory_coverage_mode": adapter.COVERAGE_MODE}
                if hasattr(adapter, "COVERAGE_MODE")
                else {"provider_directory_fully_enumerable_resources": list(adapter.RESOURCE_TYPES)}
            ),
            "provider_directory_acquisition_enabled": True,
            "provider_directory_bulk_export_eligible": False,
            "provider_directory_fhir_endpoint": False,
        },
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def is_source(adapter, record):
    """Check the exact recorded adapter identity before allowing acquisition."""
    metadata = record.get("metadata_json")
    transport = metadata.get("provider_directory_transport") if isinstance(metadata, dict) else None
    if record.get("source_id") != adapter.SOURCE_ID and transport != adapter.TRANSPORT:
        return False
    expected = source_row(adapter, None)
    if any(record.get(key) != expected[key] for key in ("source_id", "api_base", "canonical_api_base")) or any(
        not isinstance(metadata, dict) or metadata.get(key) != value for key, value in expected["metadata_json"].items()
    ):
        raise ValueError(f"{adapter.PREFIX}_source_identity_changed")
    return True


def require_task(adapter, task, *, test_mode):
    """Allow only an ordinary deferred acquisition against the configured endpoint."""
    if (
        test_mode
        or task.get("provider_directory_endpoint_scope") != adapter.ENDPOINT_SCOPE
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
        raise ValueError(f"{adapter.PREFIX}_acquisition_mode_invalid")


async def _persist_capture(adapter, directory, capture, as_of, run_id, persist, cancel_check):
    """Feed bounded batches to the ordinary isolated source candidate writer."""
    written = dict.fromkeys(adapter.RESOURCE_TYPES, 0)
    acquisition = directory.FHIRAcquisitionContext(
        fetch_mode=adapter.TRANSPORT, semantic_projection_as_of=date.fromisoformat(as_of) if as_of else None
    )
    for batch in batched(capture.iter_resources(), 512):
        await cancel_check()
        rows_by_model = {}
        for resource in batch:
            family, identifier = resource["resourceType"], resource["id"]
            model, parsed_row = directory.parse_fhir_resource(
                adapter.SOURCE_ID,
                resource,
                resource_url=f"urn:{adapter.URN}:normalized:{family}:{identifier}",
                acquisition=acquisition,
                run_id=run_id,
            )
            written[family] += 1
            rows_by_model.setdefault(model, []).append(parsed_row)
        for model, parsed_rows in rows_by_model.items():
            if await persist(model, parsed_rows) != len(parsed_rows):
                raise RuntimeError(f"{adapter.PREFIX}_candidate_copy_incomplete")
    return written


async def import_group(
    adapter, directory, source_record, *, load_plan, run_id, persist, progress_callback, cancel_check
):
    """COPY the bounded normalized families after all authorized searches terminate."""
    checkpoint = source_record.get("_pagination_checkpoint_context")
    if (
        checkpoint is None
        or not checkpoint.acquisition_root_run_id
        or source_record.get("_endpoint_dataset_id") != checkpoint.dataset_id
        or not source_record.get("_resource_hash_contract")
    ):
        raise ValueError(f"{adapter.PREFIX}_prepared_candidate_required")
    scope = source_proxy.current_run()
    binding = scope.bindings.get(adapter.SOURCE_ID) if scope is not None else None
    if binding is None or scope.credentials.get(adapter.SOURCE_ID):
        raise ValueError(f"{adapter.PREFIX}_authenticated_proxy_required")

    async def pause(boundary, cooldowns):
        """Record the publisher boundary in the run audit."""
        scope.record_destination_hold(boundary, cooldowns)

    async with adapter.acquire_directory(
        load_plan(),
        checkpoint.acquisition_root_run_id,
        binding,
        cancel_check=cancel_check,
        progress=progress_callback,
        pause=pause,
        require_retained=bool(
            checkpoint.retry_of_run_id and not getattr(checkpoint, "allow_initial_nomi_capture", False)
        ),
    ) as (capture, proof):
        async with getattr(adapter, "candidate_write_scope", nullcontext)(directory.db):
            written = await _persist_capture(
                adapter,
                directory,
                capture,
                source_record.get("_semantic_projection_as_of"),
                run_id,
                persist,
                cancel_check,
            )
            if written != proof["resource_counts"]:
                raise RuntimeError(f"{adapter.PREFIX}_candidate_counts_changed")
            await cancel_check()
    return _capture_results(adapter, written, proof)


def _capture_results(adapter, written, proof):
    diagnostics_by_family = {
        family: {
            "fetch_mode": adapter.TRANSPORT,
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
            "rows_fetched": count,
            "rows_written": count,
            "normalized_output_only": True,
            f"{adapter.URN}_capture": proof,
        }
        for family, count in written.items()
    }
    stats_by_family = {
        family: {
            "sources_attempted": 1,
            "sources_completed": 1,
            "sources_empty": int(count == 0),
            "collection_complete_sources": 1,
            "rows_fetched": count,
        }
        for family, count in written.items()
    }
    return [adapter.SOURCE_ID], diagnostics_by_family, written, {}, stats_by_family, {}, {}
