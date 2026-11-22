# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build an immutable positive Delta source edition from authorized retained pages."""

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from itertools import batched
from pathlib import Path
from threading import Event

from process.control_cancel import raise_if_cancelled
from process.delta_dental_directory_capture import NETWORK_LABELS
from process.delta_dental_directory_json import SOURCE_ENDPOINT, SOURCE_ID
from process.delta_dental_directory_normalization import MAX_CUMULATIVE_LINEAGES, normalized_delta_capture
from process.provider_directory_endpoint_admission import _admit_provider_directory_endpoint_components
from process.uhc_flex_practitioner_async_safety import drain_operation

TASK_FLAG = "delta_dental_observed_dataset"
REVIEW_FIELD = "delta_dental_observed_review_id"
METADATA_KEY = "delta_positive_observation_v1"
RESOURCE_TYPES = ("Location", "Organization", "Practitioner", "PractitionerRole")
TRANSPORT = "retained_observed_json"
_ENDPOINT_FIELDS = _admit_provider_directory_endpoint_components(
    canonical_api_base=SOURCE_ENDPOINT,
    credential_descriptor_json={},
    endpoint_signature_json={
        "transport": TRANSPORT,
        "adapter_id": SOURCE_ID,
        "adapter_contract": METADATA_KEY,
        "resource_types": list(RESOURCE_TYPES),
    },
)
DELTA_OBSERVED_ENDPOINT_ID = _ENDPOINT_FIELDS["endpoint_id"]


def observed_source_row(observed_at):
    """Identify retained observations without registering a REST acquisition."""
    return {
        "source_id": SOURCE_ID,
        "org_name": "Delta Dental",
        "api_base": SOURCE_ENDPOINT,
        "canonical_api_base": SOURCE_ENDPOINT,
        "endpoint_id": DELTA_OBSERVED_ENDPOINT_ID,
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "seed_source": SOURCE_ID,
        "seed_source_url": SOURCE_ENDPOINT,
        "metadata_json": {
            "provider_directory_source_kind": "retained_positive_observations",
            "provider_directory_transport": TRANSPORT,
            "provider_directory_adapter_contract": METADATA_KEY,
            "provider_directory_supported_resources": list(RESOURCE_TYPES),
            "provider_directory_fully_enumerable_resources": [],
            "provider_directory_manual_only": True,
            "provider_directory_acquisition_enabled": False,
            "provider_directory_profile_eligible": False,
            "provider_directory_fhir_endpoint": False,
        },
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def observed_endpoint_row(source, observed_at):
    """Return the existing endpoint identity shape only for the exact retained contract."""
    metadata_by_field = source.get("metadata_json")
    if (
        source.get("source_id") != SOURCE_ID
        or source.get("endpoint_id") != DELTA_OBSERVED_ENDPOINT_ID
        or source.get("canonical_api_base") != SOURCE_ENDPOINT
        or type(metadata_by_field) is not dict
        or metadata_by_field.get("provider_directory_adapter_contract") != METADATA_KEY
    ):
        return None
    return {
        **{field: value for field, value in _ENDPOINT_FIELDS.items() if not field.endswith("_canonical_json")},
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "metadata_json": {"identity_version": METADATA_KEY, "transport": TRANSPORT},
        "created_at": observed_at,
        "updated_at": observed_at,
    }


@dataclass(frozen=True)
class DeltaObservedSourceScope:
    """Exact acquisition cache and reviews selected by the named Source worker."""

    database: Path
    edition: str
    lineages: tuple[str, ...]
    reviewed_products: frozenset[str]
    endpoint_id: str
    semantic_projection_as_of: date


def validate_delta_positive_receipt(receipt, *, resource_count=None):
    """Require exact positive coverage and internally consistent output counts."""
    counts = receipt.get("resource_counts") if type(receipt) is dict else None
    if (
        type(receipt) is not dict
        or receipt.get("adapter_id") != SOURCE_ID
        or receipt.get("directory_complete") is not False
        or receipt.get("normalized_output_only") is not True
        or receipt.get("positive_observed_only") is not True
        or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("capture_sha256")))
        or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("capture_edition")))
        or type(counts) is not dict
        or set(counts) != set(RESOURCE_TYPES)
        or any(type(count) is not int or count <= 0 for count in counts.values())
        or (resource_count is not None and sum(counts.values()) != resource_count)
    ):
        raise ValueError("delta_observed_receipt_invalid")
    lineages = receipt.get("capture_lineages")
    products = receipt.get("reviewed_products")
    if (
        type(lineages) is not list
        or not 0 < len(lineages) <= MAX_CUMULATIVE_LINEAGES
        or any(type(lineage) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", lineage) for lineage in lineages)
        or len(set(lineages)) != len(lineages)
        or type(products) is not list
        or not products
        or any(type(label) is not str or label not in NETWORK_LABELS for label in products)
        or sorted(set(products)) != products
    ):
        raise ValueError("delta_observed_scope_invalid")
    planned = receipt.get("planned_windows")
    observed = receipt.get("observed_windows")
    unresolved = receipt.get("unobserved_windows")
    capped = receipt.get("capped_windows")
    if (
        not all(type(count) is int and count >= 0 for count in (planned, observed, unresolved, capped))
        or planned != observed + unresolved
        or not 0 <= capped <= observed
        or not observed
    ):
        raise ValueError("delta_observed_coverage_invalid")


def require_delta_observed_task(task):
    """Recognize the exact protected review and refuse incompatible work modes."""
    review_id = task.get(REVIEW_FIELD)
    if (
        task.get(TASK_FLAG) is not True
        or task.get("source_ids") != [SOURCE_ID]
        or task.get("provider_directory_endpoint_scope") != SOURCE_ENDPOINT
        or type(review_id) is not str
        or not re.fullmatch(r"[0-9a-f]{32}", review_id)
        or "delta_dental_directory_observation" in task
        or any(
            marker in task
            for marker in (
                "rooted_graph",
                "provider_directory_serving_operation",
                "provider_directory_serving_generation",
                "provider_directory_profile_contract_id",
                "provider_directory_profile_generation",
                "provider_directory_profile_selection_attestation",
            )
        )
        or any(
            task.get(option)
            for option in (
                "probe",
                "test",
                "test_mode",
                "bulk_export",
                "stale_cleanup",
                "publish_source_dataset_only",
                "publish_after_acquisition",
                "publish_artifacts_only",
                "dataset_rehydrate_only",
                "rehydrate_dataset_id",
                "import_resources",
                "full_refresh",
                "import_completion",
                "refresh_preset",
                "resources",
                "run_post_import_corroboration",
                "publish_artifacts",
                "publish_corroboration",
                "validated_publication_candidate",
                "defer_typed_materialization",
                "seed_only",
                "canonical_backfill_only",
                "contact_backfill_only",
                "dataset_followup_only",
                "preset",
                "source_query",
            )
        )
    ):
        raise ValueError("delta_observed_task_invalid")
    return review_id


def _require_task(context, task):
    """Bind the successful native lifecycle claim before opening the candidate."""
    require_delta_observed_task(task)
    run_id = context.get("control_run_id")
    if (
        not isinstance(run_id, str)
        or task.get("run_id") != run_id
        or not re.fullmatch(r"run_[0-9a-f]{32}", run_id)
        or bool((context.get("context") or {}).get("test_mode"))
    ):
        raise ValueError("delta_observed_task_invalid")
    return run_id


async def _authorized_scope(context, task):
    """Require a named transaction factory with the protected Delta authorization hook."""
    from process.provider_directory_generic_source_policy import (
        _FACTORY,
        require_source_selection,
        source_transaction_available,
    )
    from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

    source_transaction_available()
    factory = _FACTORY.get()
    authorize = getattr(factory, "authorize_delta_observed_source", None)
    if type(getattr(factory, "storage_binding", None)) is not NamedFHIRSourceStorage or not callable(authorize):
        raise ValueError("delta_observed_named_source_authorization_required")
    scope = await authorize(context, task)
    if (
        type(scope) is not DeltaObservedSourceScope
        or type(scope.semantic_projection_as_of) is not date
        or scope.endpoint_id != DELTA_OBSERVED_ENDPOINT_ID
        or not isinstance(scope.database, Path)
        or not scope.database.is_absolute()
    ):
        raise ValueError("delta_observed_named_source_authorization_invalid")
    require_source_selection(
        [
            {
                "source_id": SOURCE_ID,
                "endpoint_id": scope.endpoint_id,
                "api_base": SOURCE_ENDPOINT,
                "canonical_api_base": SOURCE_ENDPOINT,
            }
        ]
    )
    return scope


def _receipt_for_capture(capture, scope):
    """Bind the reviewed product labels and cumulative lineages to the normalized extent."""
    receipt_by_field = {
        **capture.proof,
        "capture_edition": scope.edition,
        "capture_lineages": list(scope.lineages),
        "reviewed_products": sorted(scope.reviewed_products),
    }
    validate_delta_positive_receipt(receipt_by_field)
    return receipt_by_field


async def _persist_delta_resources(directory, candidate, capture, check_cancelled):
    """Feed ordered normalized output to the ordinary source COPY writer."""
    written_by_family = dict.fromkeys(RESOURCE_TYPES, 0)
    acquisition = directory.FHIRAcquisitionContext(
        semantic_projection_as_of=date.fromisoformat(candidate.semantic_projection_as_of)
    )
    last_key = None
    batches = batched(capture.iter_resources(), 512)
    while resources := await drain_operation(asyncio.to_thread(next, batches, ()), preserve_cancellation=True):
        await check_cancelled()
        rows_by_model = {}
        for resource in resources:
            family, resource_id = resource["resourceType"], resource["id"]
            key = family, resource_id
            if family not in written_by_family or (last_key is not None and key <= last_key):
                raise ValueError("delta_observed_resource_order_invalid")
            last_key = key
            model, prepared = directory.parse_fhir_resource(
                SOURCE_ID,
                resource,
                resource_url=f"urn:{SOURCE_ID}:{family}:{resource_id}",
                acquisition=acquisition,
                run_id=candidate.import_run_id,
            )
            written_by_family[family] += 1
            rows_by_model.setdefault(model, []).append(prepared)
        for model, parsed_rows in rows_by_model.items():
            if await directory._upsert_deferred_resource_rows(
                model,
                parsed_rows,
                dataset_id=candidate.dataset_id,
                track_seen=False,
                resource_hash_contract=candidate.resource_hash_contract,
                semantic_projection_as_of=candidate.semantic_projection_as_of,
            ) != len(parsed_rows):
                raise RuntimeError("delta_observed_copy_incomplete")
    if written_by_family != capture.resource_counts:
        raise RuntimeError("delta_observed_resource_counts_changed")


async def _close_staged_capture(opening, manager):
    """Drain only this managed staging task before releasing its SQLite connection."""
    await asyncio.wait((opening,))
    if not opening.cancelled() and opening.exception() is None:
        await asyncio.to_thread(manager.__exit__, None, None, None)


@asynccontextmanager
async def _open_staged_capture(scope, check_cancelled):
    """Keep worker heartbeats and control cancellation responsive during staging."""
    stop = Event()
    manager = normalized_delta_capture(
        scope.database,
        lineage=scope.lineages,
        edition=scope.edition,
        reviewed_products=scope.reviewed_products,
        stop_requested=stop.is_set,
    )
    opening = asyncio.create_task(asyncio.to_thread(manager.__enter__))
    try:
        while not opening.done():
            completed, _ = await asyncio.wait((opening,), timeout=2)
            if not completed:
                await check_cancelled()
        yield opening.result()
    finally:
        stop.set()
        await drain_operation(_close_staged_capture(opening, manager), preserve_cancellation=True)


async def run_delta_observed_source(directory, context, task):
    """Stage and verify an authorized positive edition; publication uses the named handoff."""

    async def check_cancelled():
        """Honor cancellation before candidate operations and each bounded batch."""
        await raise_if_cancelled(context, task)

    run_id = _require_task(context, task)
    scope = await _authorized_scope(context, task)
    await check_cancelled()
    if replay := await _validated_candidate_replay(directory, scope, run_id):
        return await _named_handoff_result(directory, *replay)
    async with _open_staged_capture(scope, check_cancelled) as capture:
        receipt_by_field = _receipt_for_capture(capture, scope)
        candidate = await _prepare_candidate(directory, scope, run_id, receipt_by_field)
        if not candidate.already_validated:
            await _persist_delta_resources(directory, candidate, capture, check_cancelled)
            await check_cancelled()
            diagnostics_by_family = {
                family: {"fetch_mode": TRANSPORT, "complete": False, "rows_written": count}
                for family, count in capture.resource_counts.items()
            }
            await directory._validate_endpoint_dataset_candidate(candidate, diagnostics_by_family)
    return await _named_handoff_result(directory, candidate, receipt_by_field)


async def _named_handoff_result(directory, candidate, receipt):
    """Return the established immutable publisher receipt to normal Control orchestration."""
    from process.provider_directory_generic_source_completion import record_candidate_handoff

    handoffs_by_dataset = {}
    await record_candidate_handoff(directory, candidate, handoffs_by_dataset)
    if set(handoffs_by_dataset) != {candidate.dataset_id}:
        raise RuntimeError("delta_observed_handoff_missing")
    return {"source_candidate_handoffs": handoffs_by_dataset, METADATA_KEY: receipt}


def _candidate_from_scope(directory, scope, run_id, receipt, previous_dataset_id, *, is_validated=False):
    """Keep fresh and finished candidates on the same native dataset coordinates."""
    return directory.EndpointDatasetCandidate(
        endpoint_id=scope.endpoint_id,
        dataset_id=directory._endpoint_dataset_candidate_id(scope.endpoint_id, RESOURCE_TYPES, run_id),
        acquisition_root_run_id=run_id,
        source_ids=(SOURCE_ID,),
        selected_resources=RESOURCE_TYPES,
        import_run_id=run_id,
        previous_dataset_id=previous_dataset_id,
        expected_resources=RESOURCE_TYPES,
        semantic_projection_as_of=scope.semantic_projection_as_of.isoformat(),
        proof_resource_scope=RESOURCE_TYPES,
        delta_observation=receipt,
        already_validated=is_validated,
    )


async def _validated_candidate_replay(directory, scope, run_id):
    """Reuse the exact valid handoff without rescanning the national SQLite cache."""
    dataset_id = directory._endpoint_dataset_candidate_id(scope.endpoint_id, RESOURCE_TYPES, run_id)
    retained = await directory._endpoint_dataset_state(dataset_id)
    if retained.get("status") != directory.ENDPOINT_DATASET_VALIDATED:
        return None
    metadata_by_field = directory._json_object(retained.get("publication_metadata_json"))
    receipt = metadata_by_field.get(METADATA_KEY)
    validate_delta_positive_receipt(receipt, resource_count=retained["resource_count"])
    if (
        retained["endpoint_id"] != scope.endpoint_id
        or retained["acquisition_root_run_id"] != run_id
        or metadata_by_field.get(directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY)
        != scope.semantic_projection_as_of.isoformat()
        or receipt["capture_edition"] != scope.edition
        or receipt["capture_lineages"] != list(scope.lineages)
        or receipt["reviewed_products"] != sorted(scope.reviewed_products)
    ):
        raise ValueError("delta_observed_candidate_identity_changed")
    candidate = _candidate_from_scope(
        directory, scope, run_id, receipt, retained["previous_dataset_id"], is_validated=True
    )
    return candidate, receipt


def _require_cumulative_scope(previous, current):
    """Reject any replacement that could erase previously admitted positive observations."""
    if previous is None:
        return
    validate_delta_positive_receipt(previous)
    prior_lineages = previous["capture_lineages"]
    is_same_edition = previous["capture_edition"] == current["capture_edition"]
    if (is_same_edition and prior_lineages != current["capture_lineages"][: len(prior_lineages)]) or not set(
        previous["reviewed_products"]
    ) <= set(current["reviewed_products"]):
        raise ValueError("delta_observed_predecessor_scope_changed")


async def _prepare_candidate(directory, scope, run_id, receipt):
    """Prepare through the existing named dataset authority and reject changed replay."""
    from process.provider_directory_generic_source_adapters import current_endpoint_dataset_record

    dataset_id = directory._endpoint_dataset_candidate_id(scope.endpoint_id, RESOURCE_TYPES, run_id)
    existing_by_field = await directory._endpoint_dataset_state(dataset_id)
    is_validated = existing_by_field.get("status") == directory.ENDPOINT_DATASET_VALIDATED
    if existing_by_field:
        metadata_by_field = directory._json_object(existing_by_field.get("publication_metadata_json"))
        if (
            existing_by_field.get("endpoint_id") != scope.endpoint_id
            or existing_by_field.get("acquisition_root_run_id") != run_id
            or metadata_by_field.get(METADATA_KEY) != receipt
            or metadata_by_field.get(directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY)
            != scope.semantic_projection_as_of.isoformat()
        ):
            raise ValueError("delta_observed_candidate_identity_changed")
    predecessor_by_field = await current_endpoint_dataset_record(
        directory,
        scope.endpoint_id,
        exclude_dataset_id=dataset_id,
    )
    if predecessor_by_field:
        previous_receipt = predecessor_by_field.get(METADATA_KEY)
        if previous_receipt is None:
            raise ValueError("delta_observed_predecessor_receipt_required")
        _require_cumulative_scope(previous_receipt, receipt)
    previous_dataset_id = predecessor_by_field.get("dataset_id")
    if existing_by_field and existing_by_field.get("previous_dataset_id") != previous_dataset_id:
        raise ValueError("delta_observed_predecessor_changed")
    candidate = _candidate_from_scope(directory, scope, run_id, receipt, previous_dataset_id, is_validated=is_validated)
    if is_validated:
        return candidate
    async with directory.db.transaction():
        if await directory._upsert_provider_directory_source_rows([observed_source_row(directory._now())]) != 1:
            raise RuntimeError("delta_observed_source_enrollment_incomplete")
    return await directory._initialize_endpoint_dataset_candidate(candidate, ())
