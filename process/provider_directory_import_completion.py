# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finalize retained REST input without converting an unfinished traversal to EOF."""

from dataclasses import replace

from process.import_completion_policy import (
    completion_receipt,
    validate_completion_receipt,
    validate_completion_request,
)


class CompletionNotAccepted(ValueError):
    """An inadmissible request must leave retained acquisition data resumable."""


def _require(condition, reason):
    if not condition:
        raise CompletionNotAccepted("provider_directory_import_completion_" + reason)


def require_candidate_request(candidate, request):
    """Bind completion before either replay or candidate finalization can mutate state."""
    stored = getattr(candidate, "published_metadata", None) or getattr(candidate, "validated_metadata", None)
    if request is None:
        _require(not isinstance(stored, dict) or "import_completion" not in stored, "replay_request_missing")
        return
    validate_completion_request(request)
    _require(
        candidate is not None
        and candidate.dataset_id == request["candidate_id"]
        and candidate.acquisition_root_run_id == request["acquisition_root_run_id"]
        and set(candidate.selected_resources) == request["retained_counts"].keys(),
        "scope_changed",
    )
    if candidate.already_validated or candidate.already_published:
        _require(
            isinstance(stored, dict)
            and isinstance(stored.get("import_completion"), dict)
            and stored["import_completion"].get("request") == request,
            "replay_request_changed",
        )


def is_publishable(directory, candidate, diagnostic_by_resource):
    """Accept either proven completion or the exact authorized unavailable scope."""
    if candidate.expected_resources and candidate.selected_resources != candidate.expected_resources:
        return False
    accepted_partitions = accepted_resources(candidate)
    for resource_type in candidate.selected_resources:
        diagnostic_by_field = diagnostic_by_resource.get(resource_type)
        if resource_type in accepted_partitions:
            if not diagnostic_by_field or diagnostic_by_field.get("complete") is not False:
                return False
            continue
        if (
            not diagnostic_by_field
            or diagnostic_by_field.get("complete") is not True
            or diagnostic_by_field.get("error")
            or diagnostic_by_field.get("bounded")
            or diagnostic_by_field.get("next_url_remaining")
        ):
            return False
        if not directory.timestamp_recovery.diagnostic_is_complete(
            diagnostic_by_field,
            expected={
                "dataset_id": candidate.dataset_id,
                "acquisition_root_run_id": candidate.acquisition_root_run_id,
                "resource_type": resource_type,
                **({"source_id": candidate.source_ids[0]} if len(candidate.source_ids) == 1 else {}),
            },
        ):
            return False
    return True


def finalized_import_summary(directory, candidate, resource_completion=None):
    """Replay validated input while preserving unavailable-resource diagnostics."""
    directory._assert_finalized_endpoint_dataset_replay(candidate)
    finalized_metadata, state_name = directory._finalized_endpoint_dataset_metadata(candidate)
    assert finalized_metadata is not None
    persisted_diagnostics = finalized_metadata["resource_diagnostics"]
    diagnostics_by_resource = {
        resource_type: dict(persisted_diagnostics[resource_type])
        for resource_type in candidate.selected_resources
        if isinstance(persisted_diagnostics.get(resource_type), dict)
    }
    if set(diagnostics_by_resource) != set(candidate.selected_resources):
        raise RuntimeError(f"provider_directory_endpoint_dataset_{state_name}_diagnostics_incomplete")
    if not directory._is_endpoint_dataset_publishable(candidate, diagnostics_by_resource):
        raise RuntimeError(f"provider_directory_endpoint_dataset_{state_name}_completion_proof_invalid")
    if resource_completion is not None:
        for resource_type in candidate.selected_resources:
            if directory._is_resource_diagnostic_acquisition_complete(diagnostics_by_resource[resource_type]):
                resource_completion.setdefault(resource_type, set()).update(candidate.source_ids)
    return (
        list(candidate.source_ids),
        diagnostics_by_resource,
        {resource_type: 0 for resource_type in candidate.selected_resources},
        {},
        {},
        {},
        {},
    )


async def _settled_attempt_metrics(directory, candidate, parent_id):
    async with directory.profile_scope_copy.source_read_transaction(directory):
        previous_row = await directory.db.first(
            "SELECT status,finished_at,params,metrics FROM "
            + directory._qt(directory._schema(), directory.ImportRun.__tablename__)
            + " WHERE run_id=:run_id AND importer='provider-directory-fhir'",
            run_id=parent_id,
        )
    previous = directory._pagination_checkpoint_row_mapping(previous_row)
    metrics_by_key = directory._json_object(previous.get("metrics"))
    audit = metrics_by_key.get("audit")
    if isinstance(audit, dict):
        metrics_by_key = {**metrics_by_key, **audit}
    params = directory._json_object(previous.get("params"))
    _require(
        previous.get("finished_at") is not None
        and previous.get("status") in {"failed", "canceled", "cancelled", "succeeded"}
        and params.get("source_ids") == list(candidate.source_ids)
        and params.get("provider_directory_pagination_root_run_id", parent_id) == candidate.acquisition_root_run_id
        and (
            previous.get("status") != "succeeded"
            or metrics_by_key.get("source_publication", {}).get("state") == "incomplete"
        ),
        "previous_attempt_not_settled",
    )
    return previous["status"], metrics_by_key


def _checkpoint_diagnostic(directory, resume):
    is_recovery = directory.timestamp_recovery.marked(resume.completeness)
    diagnostic_by_field = {
        "complete": resume.complete,
        "collection_complete": resume.complete and not is_recovery,
        "bounded": False,
        "error": None if resume.complete else "source_input_unavailable",
        "fetch_mode": directory.timestamp_recovery.FETCH_MODE if is_recovery else "rest_checkpoint",
        "pages_fetched": resume.pages_processed,
        "rows_fetched": resume.rows_processed,
        "rows_written": 0,
        "next_url_remaining": bool(resume.next_url),
        "absence_semantics": "unknown_for_unavailable_input",
    }
    if is_recovery:
        diagnostic_by_field[directory.timestamp_recovery.KEY] = directory.timestamp_recovery.completion_receipt(
            resume.completeness,
        )
    return diagnostic_by_field


def _resource_retry_evidence(metrics_by_key, resource, previous_status):
    stats = metrics_by_key.get("resource_fetch_stats", {}).get(resource, {})
    # Canceled scans can retain running timings before producing final aggregate stats.
    active_timings = [
        group.get("resource_fetch_timing", {}).get(resource, {})
        for group in metrics_by_key.get("active_source_groups", [])
        if isinstance(group, dict)
    ]
    retries = max(
        [stats.get("pagination_cooldown_retries", 0)]
        + [timing.get("pagination_cooldown_retries", 0) for timing in active_timings]
    )
    _require(type(retries) is int and retries >= 1, "retry_evidence_missing")
    return {
        "retries": retries,
        "exhausted": stats.get("pagination_cooldown_exhausted_sources", 0) > 0,
        "operator_stopped": previous_status in {"canceled", "cancelled"},
    }


async def _retained_diagnostics(directory, candidate, request, previous_status, metrics_by_key):
    diagnostic_by_resource, retry_evidence_by_resource = {}, {}
    for resource in candidate.selected_resources:
        checkpoint = await directory._fetch_pagination_checkpoint(candidate.checkpoint_context, resource)
        _require(bool(checkpoint), "checkpoint_missing")
        resume = directory._compatible_pagination_resume_state(
            checkpoint,
            candidate.checkpoint_context,
            checkpoint.get("start_url_hash"),
        )
        _require(resume is not None, "checkpoint_changed")
        diagnostic_by_resource[resource] = _checkpoint_diagnostic(directory, resume)
        if not resume.complete:
            _require(request["expected_counts"][resource] is None, "expected_count_evidence_missing")
            _require(request["accept_unknown_coverage"] is True, "not_accepted")
            retry_evidence_by_resource[resource] = _resource_retry_evidence(metrics_by_key, resource, previous_status)
    return diagnostic_by_resource, retry_evidence_by_resource


async def retained_completion(directory, candidate, request, *, retry_of_run_id):
    """Read settled native retry/checkpoint evidence; perform no source requests."""
    require_candidate_request(candidate, request)
    _require(
        candidate.checkpoint_context is not None
        and candidate.reused_from_checkpoint
        and retry_of_run_id == request["retry_of_run_id"]
        and candidate.completion_proof_required_version is None
        and not candidate.requires_twin_root_verification
        and candidate.source_release is None,
        "scope_changed",
    )
    previous_status, metrics_by_key = await _settled_attempt_metrics(directory, candidate, retry_of_run_id)
    diagnostic_by_resource, retry_evidence_by_resource = await _retained_diagnostics(
        directory, candidate, request, previous_status, metrics_by_key
    )
    evidence_by_field = {
        "request": request,
        "retry_evidence": retry_evidence_by_resource,
        "incomplete_partitions": sorted(retry_evidence_by_resource),
    }
    # This preliminary decision does not publish; closed candidate validation recomputes it.
    completion_receipt(
        request,
        retained_counts=request["retained_counts"],
        content_hash="0" * 64,
        incomplete_partitions=evidence_by_field["incomplete_partitions"],
        retry_evidence=retry_evidence_by_resource,
    )
    return replace(candidate, import_completion=evidence_by_field), diagnostic_by_resource


def metadata(candidate, content_proof):
    """Bind the shared decision to independently validated distinct records."""
    evidence_by_field = getattr(candidate, "import_completion", None)
    if evidence_by_field is None:
        return {}
    count_by_resource = {
        resource: content_proof.resource_counts.get(resource, 0) for resource in candidate.selected_resources
    }
    try:
        receipt_by_field = completion_receipt(
            evidence_by_field["request"],
            retained_counts=count_by_resource,
            content_hash=content_proof.dataset_hash,
            incomplete_partitions=evidence_by_field["incomplete_partitions"],
            retry_evidence=evidence_by_field["retry_evidence"],
        )
    except ValueError as error:
        raise CompletionNotAccepted(str(error)) from error
    return {"import_completion": receipt_by_field}


def accepted_resources(candidate):
    """Return only the exact explicitly accepted unavailable resource scope."""
    evidence_by_field = getattr(candidate, "import_completion", None)
    if evidence_by_field is None:
        stored = getattr(candidate, "published_metadata", None) or getattr(candidate, "validated_metadata", None)
        receipt_by_field = stored.get("import_completion") if isinstance(stored, dict) else None
        if receipt_by_field is None:
            return set()
        validate_completion_receipt(
            receipt_by_field,
            retained_counts=receipt_by_field["request"]["retained_counts"],
            content_hash=stored.get("dataset_hash"),
        )
        evidence_by_field = receipt_by_field
    request = validate_completion_request(evidence_by_field["request"])
    _require(
        request["candidate_id"] == candidate.dataset_id
        and request["acquisition_root_run_id"] == candidate.acquisition_root_run_id
        and request["retained_counts"].keys() == set(candidate.selected_resources),
        "scope_changed",
    )
    completion_receipt(
        request,
        retained_counts=request["retained_counts"],
        content_hash="0" * 64,
        incomplete_partitions=evidence_by_field["incomplete_partitions"],
        retry_evidence=evidence_by_field["retry_evidence"],
    )
    return set(evidence_by_field["incomplete_partitions"])


def validate_metadata(header, *, retained_counts=None):
    """Recheck the receipt at native handoff and frozen content verification."""
    import json

    stored = header.get("publication_metadata_json")
    stored = json.loads(stored) if isinstance(stored, str) else stored
    diagnostic_by_resource = stored.get("resource_diagnostics", {}) if isinstance(stored, dict) else {}
    receipt_by_field = stored.get("import_completion") if isinstance(stored, dict) else None
    if receipt_by_field is None:
        _require(
            not isinstance(diagnostic_by_resource, dict)
            or not any(
                isinstance(diagnostic_by_field, dict) and diagnostic_by_field.get("error") == "source_input_unavailable"
                for diagnostic_by_field in diagnostic_by_resource.values()
            ),
            "receipt_missing",
        )
        return set()
    request = validate_completion_request(receipt_by_field.get("request"))
    count_by_resource = (
        request["retained_counts"]
        if retained_counts is None
        else {resource: retained_counts.get(resource, 0) for resource in request["retained_counts"]}
    )
    _require(
        request["candidate_id"] == header.get("dataset_id")
        and request["acquisition_root_run_id"] == header.get("acquisition_root_run_id")
        and request["retained_counts"].keys() == set(stored.get("selected_resources", []))
        and sum(count_by_resource.values()) == header.get("resource_count")
        and (retained_counts is None or sum(retained_counts.values()) == header.get("resource_count")),
        "scope_changed",
    )
    _require(
        type(diagnostic_by_resource) is dict
        and diagnostic_by_resource.keys() == request["retained_counts"].keys()
        and all(
            type(diagnostic_by_field) is dict and type(diagnostic_by_field.get("complete")) is bool
            for diagnostic_by_field in diagnostic_by_resource.values()
        )
        and sorted(
            resource
            for resource, diagnostic_by_field in diagnostic_by_resource.items()
            if not diagnostic_by_field["complete"]
        )
        == receipt_by_field.get("incomplete_partitions"),
        "diagnostics_changed",
    )
    validate_completion_receipt(
        receipt_by_field, retained_counts=count_by_resource, content_hash=header.get("dataset_hash")
    )
    return set(receipt_by_field["incomplete_partitions"])
