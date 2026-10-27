# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact terminal CMS ownership and the matching native worker launch fence."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import re
from collections.abc import Mapping
from uuid import UUID

from process.import_status_events import isoformat_utc
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_capacity_contract import validated_registry_source_retention_policy
from process.provider_directory_cms_source_runtime import _assert_selected_source
from process.provider_directory_profile_capacity_preflight_contract import validated_capacity_preflight_request
from process.provider_directory_profile_selection_contract import (
    CMS_CAPACITY_EXECUTION_PARAM,
    validated_profile_execution,
)

CONTRACT = "cms-registry-source-stop.v1"
IMPORTER = "provider-directory-fhir"
TERMINAL_STATUSES = frozenset({"failed", "canceled", "dead_letter"})
REQUEST_FIELDS = frozenset(
    {
        "expected_importer",
        "expected_status",
        "expected_attempt_id",
        "expected_attempt_started_at",
        "expected_capture_id",
        "expected_policy_sha256",
    }
)


def is_request(payload):
    """Keep CMS away from the weaker historical two-field request."""
    return isinstance(payload, dict) and payload.get("expected_importer") == IMPORTER


def validated_request(payload, run_id):
    """Decode assertions only; the locked native run must independently match them."""
    if type(payload) is not dict or set(payload) != REQUEST_FIELDS:
        raise ValueError("CMS stop request fields differ")
    if any(type(value) is not str or not value or value != value.strip() for value in payload.values()):
        raise ValueError("CMS stop request requires non-empty scalar identities")
    if payload["expected_importer"] != IMPORTER or payload["expected_status"] not in TERMINAL_STATUSES:
        raise ValueError("CMS stop request requires an eligible terminal importer")
    RegistryCMSSourceAttempt(run_id, payload["expected_attempt_id"], payload["expected_attempt_started_at"])
    capture = UUID(payload["expected_capture_id"])
    if not capture.int or str(capture) != payload["expected_capture_id"]:
        raise ValueError("CMS stop capture identity differs")
    if re.fullmatch(r"[0-9a-f]{64}", payload["expected_policy_sha256"]) is None:
        raise ValueError("CMS stop policy identity differs")
    return dict(payload)


def retention_requested(run):
    """Recognize the stored policy marker without treating its presence as authority."""
    if run.get("importer") != IMPORTER:
        return False
    value = run.get("params")
    for name in (
        CMS_CAPACITY_EXECUTION_PARAM,
        "lease",
        "signing_preflight_guard",
        "healthcare_request",
        "cms_nonprofile_admission",
    ):
        value = value.get(name) if isinstance(value, Mapping) else None
    return isinstance(value, Mapping) and "registry_source_retention" in value


def stored_policy(run):
    """Bind the original closed paired execution; cleanup does not renew a capacity lease."""
    if not retention_requested(run):
        raise ValueError("CMS stored retention policy is unavailable")
    execution = validated_profile_execution(run["params"])
    guard = execution.cms_nonprofile_capacity_attestation["lease"]["signing_preflight_guard"]
    request = validated_capacity_preflight_request(guard["healthcare_request"])
    policy = validated_registry_source_retention_policy(request.cms_nonprofile_admission["registry_source_retention"])
    _assert_selected_source(execution, request, policy, execution.attestation.desired_profile_as_of)
    return policy


def native_attempt(run):
    """Read original immutable coordinates, never infer them from a retained copy."""
    progress = run.get("progress")
    if not isinstance(progress, dict):
        raise ValueError("CMS original worker attempt is unavailable")
    return RegistryCMSSourceAttempt(run["run_id"], progress.get("attempt_id"), progress.get("attempt_started_at"))


def failure_attempt(run):
    """Protect only genuine CMS retention work during failed-Job reconciliation."""
    if not retention_requested(run):
        return None
    stored_policy(run)
    return native_attempt(run)


def terminal_proof(run, expected):
    """Require the actual row locked by the control endpoint, not a mirrored status."""
    if run.get("importer") != IMPORTER or run.get("status") != expected["expected_status"]:
        raise ValueError("CMS terminal owner changed")
    if run.get("status") not in TERMINAL_STATUSES:
        raise ValueError("CMS owner is not eligible for cleanup")
    attempt = native_attempt(run)
    if (attempt.attempt_id, attempt.attempt_started_at) != (
        expected["expected_attempt_id"],
        expected["expected_attempt_started_at"],
    ):
        raise ValueError("CMS original worker attempt changed")
    finished = run.get("finished_at")
    if type(finished) is not dt.datetime:
        raise ValueError("CMS terminal finish is unavailable")
    finished = finished.replace(tzinfo=dt.UTC) if finished.tzinfo is None else finished.astimezone(dt.UTC)
    if finished < dt.datetime.fromisoformat(attempt.attempt_started_at):
        raise ValueError("CMS terminal finish precedes its original attempt")
    policy = stored_policy(run)
    digest = hashlib.sha256(
        json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    if (policy["capture_id"], digest) != (expected["expected_capture_id"], expected["expected_policy_sha256"]):
        raise ValueError("CMS original capture policy changed")
    return {
        "contract": CONTRACT,
        "attempt_id": attempt.attempt_id,
        "attempt_started_at": attempt.attempt_started_at,
        "finished_at": isoformat_utc(finished),
        "capture_id": policy["capture_id"],
        "policy_sha256": digest,
    }


def require_worker_presence(presence):
    """Validate a fresh native lookup; callers cannot supply a stop flag over the API."""
    if (
        type(presence) is not dict
        or set(presence) != {"enabled", "job_count", "pod_count", "stopped"}
        or presence["enabled"] is not True
        or type(presence["stopped"]) is not bool
        or any(type(presence[name]) is not int or presence[name] < 0 for name in ("job_count", "pod_count"))
    ):
        raise ValueError("CMS exact worker lookup is unavailable")
    return presence


async def guarded_ensure(worker_payload, *, run_id, importer, selected_specs):
    """Hold the existing exact-run fence until native admission and launch settle."""
    from api import control_workers as workers

    async with workers.db.acquire() as connection:
        await workers.acquire_control_run_worker_action_lock(connection, run_id)
        failure = await workers._admit_worker_ensure(
            worker_payload,
            run_id=run_id,
            importer=importer,
            selected_specs=selected_specs,
        )
        if failure is not None:
            return failure
        if worker_payload.fhir_source_launch is not None:
            from api.provider_directory_source_worker import require_fhir_source_route_ancestry

            try:
                await require_fhir_source_route_ancestry(connection, worker_payload.fhir_source_launch)
            except ValueError:
                return workers._failed_worker_admission(worker_payload, "FHIR source worker route has changed")
        launch = asyncio.create_task(asyncio.to_thread(workers.ensure_worker, worker_payload))
        return await workers._await_worker_launch(launch)
