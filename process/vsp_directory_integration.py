# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect a completed VSP directory capture to the ordinary dataset publisher."""

import asyncio
import re
import time
from datetime import date
from itertools import batched

from process.vsp_directory_contract import (
    ADAPTER_ID,
    ENDPOINT_SCOPE,
    NETWORKS,
    RESOURCE_TYPES,
    SEARCH_URL,
    SOURCE_ID,
    TRANSPORT,
    canonical_json,
    require,
)

ENTRY_ID = "vsp-provider-directory"
_EMITTED_TYPES = frozenset({"Location", "Organization", "Practitioner", "PractitionerRole"})
_SOURCE_QUERIES = {
    "vsp",
    "vsp vision care",
    "vsp provider directory",
    ENTRY_ID,
    SOURCE_ID,
    *(f"vsp {name.lower()}" for name in NETWORKS.values()),
}


def explicit_selection(source_ids, source_query):
    """An explicit source ID takes precedence over an exact VSP name."""
    if source_ids:
        if SOURCE_ID not in source_ids:
            return False
        require(list(source_ids) == [SOURCE_ID], "vsp_single_source_required")
        return True
    return isinstance(source_query, str) and source_query.strip().lower() in _SOURCE_QUERIES


def source_row(observed_at):
    """Register a public JSON adapter without claiming a FHIR server or a successful probe."""
    return {
        "source_id": SOURCE_ID,
        "org_name": "VSP Vision Care",
        "plan_name": None,
        "portal_url": "https://www.vsp.com/eye-doctor",
        "api_base": ENDPOINT_SCOPE,
        "canonical_api_base": ENDPOINT_SCOPE,
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "seed_source": ENTRY_ID,
        "seed_source_detail": ADAPTER_ID,
        "seed_source_url": SEARCH_URL,
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
    """Refuse changed VSP identities instead of interpreting their URLs as FHIR."""
    metadata = record.get("metadata_json") or {}
    transport = metadata.get("provider_directory_transport") if isinstance(metadata, dict) else None
    if record.get("source_id") != SOURCE_ID and transport != TRANSPORT:
        return False
    require(
        record.get("source_id") == SOURCE_ID
        and record.get("api_base") == record.get("canonical_api_base") == ENDPOINT_SCOPE
        and transport == TRANSPORT
        and metadata.get("provider_directory_source_entry_id") == ENTRY_ID
        and metadata.get("provider_directory_adapter_id") == ADAPTER_ID
        and metadata.get("provider_directory_fhir_endpoint") is False
        and metadata.get("provider_directory_supported_resources") == list(RESOURCE_TYPES)
        and metadata.get("provider_directory_fully_enumerable_resources") == list(RESOURCE_TYPES),
        "vsp_source_identity_changed",
    )
    return True


def endpoint_contract():
    """Separate normalized JSON candidates from REST datasets at the same origin."""
    return {
        "adapter": ADAPTER_ID,
        "transport": TRANSPORT,
        "networks": NETWORKS.copy(),
        "resources": list(RESOURCE_TYPES),
    }


def is_failed_capture_retry(existing_by_field, checkpoint):
    """Identify a failed VSP parent; the locked storage proof decides whether it can reopen."""
    return bool(
        checkpoint is not None
        and checkpoint.source_ids == (SOURCE_ID,)
        and checkpoint.canonical_api_base == ENDPOINT_SCOPE
        and checkpoint.retry_of_run_id
        and checkpoint.owner_run_id not in (checkpoint.retry_of_run_id, checkpoint.acquisition_root_run_id)
        and existing_by_field.get("acquisition_root_run_id") == checkpoint.acquisition_root_run_id
        and existing_by_field.get("import_run_id") == checkpoint.retry_of_run_id
        and existing_by_field.get("status") == "failed"
        and existing_by_field.get("is_current") is False
        and existing_by_field.get("resource_count") == 0
        and all(
            existing_by_field.get(field) is None
            for field in (
                "published_at",
                "validated_at",
                "dataset_hash",
                "completion_proof_json",
                "completion_proof_sha256",
            )
        )
    )


def _proxy(directory, record):
    """Use direct transport only when this run's frozen source policy allows it."""
    require(is_source(record), "vsp_source_identity_changed")
    scope = directory.source_proxy.current_run()
    require(scope is not None, "vsp_source_transport_scope_required")
    is_proxy_enabled = scope.enabled(record)
    require(
        not scope.credentials.get(SOURCE_ID) and not directory._credential_spec_for_source(record),
        "vsp_source_credentials_forbidden",
    )
    if not is_proxy_enabled:
        require(scope.expected_binding is None and SOURCE_ID not in scope.bindings, "vsp_source_binding_changed")
        return None
    binding = scope.bindings.get(SOURCE_ID)
    require(binding is not None, "vsp_source_proxy_required")
    return binding


async def probe(directory, source_record, *, timeout, run_id):
    """Read one retained publisher census; a probe is not a complete directory."""
    from process.vsp_directory_capture import probe_vsp_directory

    require(type(run_id) is str and bool(run_id), "vsp_probe_root_required")
    started = time.monotonic()
    async with asyncio.timeout(timeout):
        census_proof = await probe_vsp_directory(
            source_record=source_record, root_run_id=run_id, proxy_binding=_proxy(directory, source_record)
        )
    require(
        type(census_proof) is dict
        and census_proof.get("complete") is False
        and census_proof.get("network") == "Signature"
        and type(census_proof.get("total_count")) is int
        and census_proof["total_count"] > 0
        and _is_digest(census_proof.get("capture_sha256")),
        "vsp_probe_evidence_invalid",
    )
    return {
        "status": "valid",
        "http_status": 200,
        "transport": TRANSPORT,
        "url": SEARCH_URL,
        "api_base": ENDPOINT_SCOPE,
        "run_id": run_id,
        "credential": None,
        "error": None,
        "response_time_ms": int((time.monotonic() - started) * 1000),
        "source_census": {
            name: census_proof[name] for name in ("network", "total_count", "capture_sha256", "complete")
        },
    }, None


def _is_digest(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _capture_counts(capture):
    counts, proof = capture.resource_counts, capture.proof
    require(
        type(counts) is dict
        and set(counts) == set(RESOURCE_TYPES)
        and all(type(count) is int and count >= 0 for count in counts.values())
        and all(counts[family] > 0 for family in _EMITTED_TYPES)
        and counts["Organization"] == len(NETWORKS)
        and counts["InsurancePlan"] == counts["OrganizationAffiliation"] == 0
        and type(proof) is dict
        and proof.get("normalized_output_only") is True
        and proof.get("adapter_id") == ADAPTER_ID
        and type(proof.get("resource_counts")) is dict
        and all(type(count) is int for count in proof["resource_counts"].values())
        and proof["resource_counts"] == counts
        and _is_digest(proof.get("capture_sha256"))
        and len(canonical_json(proof).encode()) <= 32 * 1024,
        "vsp_capture_proof_invalid",
    )
    return counts.copy()


def _resource_key(resource):
    require(
        type(resource) is dict
        and type(resource.get("resourceType")) is str
        and resource["resourceType"] in _EMITTED_TYPES,
        "vsp_capture_resource_invalid",
    )
    identifier = resource.get("id")
    require(
        type(identifier) is str and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", identifier), "vsp_capture_resource_invalid"
    )
    return resource["resourceType"], identifier


async def _persist_capture(directory, capture, source_record, run_id, persist, cancel_check):
    expected = _capture_counts(capture)
    written = dict.fromkeys(RESOURCE_TYPES, 0)
    as_of = source_record.get("_semantic_projection_as_of")
    acquisition = directory.FHIRAcquisitionContext(
        fetch_mode=TRANSPORT, semantic_projection_as_of=date.fromisoformat(as_of) if as_of else None
    )
    previous = None
    resources = capture.iter_resources()
    try:
        for batch in batched(resources, 512):
            await cancel_check()
            rows_by_model = {}
            for resource in batch:
                key = _resource_key(resource)
                require(previous is None or previous < key, "vsp_capture_resource_order_invalid")
                previous = key
                family, identifier = key
                written[family] += 1
                require(written[family] <= expected[family], "vsp_capture_counts_changed")
                model, parsed = directory.parse_fhir_resource(
                    SOURCE_ID,
                    resource,
                    resource_url=f"urn:vsp:normalized:{family}:{identifier}",
                    acquisition=acquisition,
                    run_id=run_id,
                )
                rows_by_model.setdefault(model, []).append(parsed)
            for model, candidate_rows in rows_by_model.items():
                require(await persist(model, candidate_rows) == len(candidate_rows), "vsp_candidate_write_incomplete")
    finally:
        if close := getattr(resources, "close", None):
            close()
    require(written == expected, "vsp_capture_counts_changed")
    return written


async def import_group(directory, source_record, *, run_id, persist, progress_callback, cancel_check):
    """Stage six normalized families only after the publisher capture is complete."""
    from process.vsp_directory_capture import capture_vsp_directory

    context = source_record.get("_pagination_checkpoint_context")
    require(
        context is not None
        and context.acquisition_root_run_id
        and source_record.get("_endpoint_dataset_id") == context.dataset_id
        and context.dataset_id
        and source_record.get("_resource_hash_contract"),
        "vsp_prepared_candidate_required",
    )
    await cancel_check()
    capture = await capture_vsp_directory(
        source_record=source_record,
        root_run_id=context.acquisition_root_run_id,
        proxy_binding=_proxy(directory, source_record),
        progress_callback=progress_callback,
        cancel_check=cancel_check,
        require_retained=context.require_retained_vsp_capture,
    )
    counts = await _persist_capture(directory, capture, source_record, run_id, persist, cancel_check)
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
            "vsp_capture": capture.proof,
        }
        for family, count in counts.items()
    }
    stats_by_family = {
        family: {
            "sources_attempted": 1,
            "sources_completed": 1,
            "sources_empty": int(count == 0),
            "collection_complete_sources": 1,
            "rows_fetched": count,
        }
        for family, count in counts.items()
    }
    return [SOURCE_ID], diagnostics_by_family, counts, {}, stats_by_family, {}, {}
