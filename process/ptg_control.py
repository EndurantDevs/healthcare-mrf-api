# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import os
import uuid
from typing import Any

from process.control_cancel import ImportCancelledError, raise_if_cancelled
from process.control_lifecycle import (
    _live_progress_heartbeat,
    _stop_live_progress_heartbeat,
    mark_control_run,
)
from process.import_status_events import bind_status_event_loop, flush_status_events
from process.live_progress import (
    reset_live_progress_context,
    set_live_progress_context,
)
from process.ptg import (
    PTG2FullRebuildFreshnessError,
    full_rebuild_failure_metrics,
    main as ptg_main,
)
from process.ptg_parts.ptg_source_worker_admission import (
    guard_ptg_worker_start,
)
from process.ptg_wave_claims import claim_wave_job_start, reconcile_wave_claim_exception
from process.ptg_frozen_control import frozen_rate_main_kwargs, validated_worker_frozen_rate_params
from process.ptg_singleton_direct_control import singleton_direct_main_kwargs, validated_worker_singleton_direct_params
from process.ptg_control_failures import ptg_failure_error
from process.ptg_allowed_amount_blank_evidence import load_blank_failure_metrics
from process.ptg_control_runtime import (
    PTG_CONTROL_HEARTBEAT_SOURCE,
    _stale_ptg_job_result,
    _start_threaded_ptg_heartbeat,
    _stop_threaded_ptg_heartbeat,
)
from process.ptg_control_environment import (
    PTG2_RUST_WORKERS_ENV,
    _optional_int,
    _ptg_lane_environment,
    _string_list,
)
from process.ptg_wave_worker_claim_adapter import (
    exact_wave_claim_values as _exact_wave_claim_values,
)

PTG_CONTROL_QUEUE_NAME = "arq:PTG"
_FULL_REBUILD_TOKEN_PARAM = "_full_rebuild_token"
_FULL_REBUILD_SCOPE_PARAM = "_full_rebuild_scope_digest"
async def ptg_control_start(ctx, task: dict[str, Any] | None = None):
    """Run one PTG control task with cancellation and heartbeat handling."""
    bind_status_event_loop()
    task_payload = task if isinstance(task, dict) else {}
    run_id = str(task_payload.get("run_id") or "").strip()
    params_by_name = (
        task_payload.get("params")
        if isinstance(task_payload.get("params"), dict)
        else task_payload
    )
    params_by_name = dict(params_by_name)
    attempt_started_at = dt.datetime.now(dt.UTC).isoformat(
        timespec="microseconds"
    )
    attempt_id = f"{run_id}:{uuid.uuid4().hex}" if run_id else None
    claim_attempt_token = uuid.uuid4().hex
    try:
        await _claim_exact_wave_worker_start(
            ctx,
            params_by_name,
            run_id=run_id,
            claim_attempt_token=claim_attempt_token,
        )
    except Exception as exc:
        reconciliation = None
        if _is_complete_exact_wave_payload(params_by_name):
            # Reconcile only a fully revalidated exact identity.  A malformed
            # or mismatched worker context must not be allowed to terminalize
            # an admitted intent merely by presenting its run id.
            reconciliation = await _reconcile_exact_wave_claim_exception(
                ctx,
                params_by_name,
                run_id=run_id,
                claim_attempt_token=claim_attempt_token,
            )
        if not (
            reconciliation is not None
            and reconciliation.status == "claimed"
            and reconciliation.same_attempt
        ):
            raise
    admission_failure = await guard_ptg_worker_start(
        task_payload,
        run_id=run_id,
        attempt_id=attempt_id,
    )
    if admission_failure is not None:
        if _is_complete_exact_wave_payload(params_by_name):
            await _mark_exact_wave_preexecution_failure(
                run_id,
                reason=str(admission_failure.get("reason") or "source admission failed"),
            )
        return admission_failure
    stale_result = await _stale_ptg_job_result(run_id)
    if stale_result is not None:
        return stale_result
    full_rebuild_scope_digest = None
    full_rebuild_proof_metrics_by_name: dict[str, bool] = {}
    heartbeat_task = None
    heartbeat_stop = None
    live_token = (
        set_live_progress_context(
            run_id=run_id,
            importer="ptg",
            status="running",
            started_at=attempt_started_at,
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
        )
        if run_id
        else None
    )
    try:
        attempt_claimed = await mark_control_run(
            run_id,
            status="running",
            phase_detail="ptg import running",
            progress_message="running",
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at if run_id else None,
        )
        if run_id and attempt_claimed is not True:
            return {
                "status": "skipped",
                "run_id": run_id,
                "reason": "newer_attempt_active",
            }
        if run_id:
            heartbeat_task = asyncio.create_task(
                _live_progress_heartbeat(
                    run_id,
                    "ptg",
                    "ptg_control_start",
                    attempt_started_at,
                    attempt_id=attempt_id,
                    attempt_started_at=attempt_started_at,
                )
            )
            heartbeat_stop = _start_threaded_ptg_heartbeat(
                run_id,
                attempt_started_at,
                attempt_id=attempt_id,
            )
        params_by_name = await validated_worker_frozen_rate_params(task_payload, params_by_name)
        params_by_name = validated_worker_singleton_direct_params(task_payload, params_by_name)
        full_rebuild_scope_digest = _full_rebuild_scope_digest(
            params_by_name,
        )
        full_rebuild_proof_metrics_by_name = (
            _full_rebuild_proof_metrics_by_name(full_rebuild_scope_digest)
        )
        should_reuse_raw_artifacts = bool(
            params_by_name.get("reuse_raw_artifacts", True)
        )
        should_keep_partial_artifacts = params_by_name.get(
            "keep_partial_artifacts"
        )
        if full_rebuild_scope_digest is not None:
            should_reuse_raw_artifacts = False
            should_keep_partial_artifacts = False
        await raise_if_cancelled(ctx, task_payload)
        _assert_expected_lane(params_by_name)
        with _ptg_lane_environment(params_by_name):
            import_result = await ptg_main(
                test_mode=bool(
                    params_by_name.get(
                        "test_mode",
                        params_by_name.get("test", False),
                    )
                ),
                toc_urls=_string_list(
                    params_by_name.get("toc_urls")
                    or params_by_name.get("toc_url")
                ),
                toc_list=params_by_name.get("toc_list"),
                in_network_url=params_by_name.get("in_network_url"),
                allowed_url=params_by_name.get("allowed_url"),
                **frozen_rate_main_kwargs(params_by_name),
                **singleton_direct_main_kwargs(params_by_name),
                provider_ref_url=params_by_name.get("provider_ref_url"),
                import_id=params_by_name.get("import_id"),
                source_key=params_by_name.get("source_key"),
                import_month=params_by_name.get("import_month"),
                max_files=_optional_int(params_by_name.get("max_files")),
                max_items=_optional_int(params_by_name.get("max_items")),
                plan_ids=_string_list(
                    params_by_name.get("plan_ids")
                    or params_by_name.get("plan_id")
                ),
                plan_name_contains=_string_list(
                    params_by_name.get("plan_name_contains")
                ),
                plan_market_types=_string_list(
                    params_by_name.get("plan_market_types")
                    or params_by_name.get("plan_market_type")
                ),
                file_url_contains=_string_list(
                    params_by_name.get("file_url_contains")
                ),
                source_network_names=_string_list(
                    params_by_name.get("source_network_names")
                    or params_by_name.get("source_network_name")
                ),
                reuse_raw_artifacts=should_reuse_raw_artifacts,
                keep_partial_artifacts=should_keep_partial_artifacts,
                control_run_id=run_id,
                control_attempt_id=attempt_id,
                control_attempt_started_at=(
                    attempt_started_at if run_id else None
                ),
                **(
                    {"full_rebuild_scope_digest": full_rebuild_scope_digest}
                    if full_rebuild_scope_digest is not None
                    else {}
                ),
            )
    except ImportCancelledError as exc:
        failure_metrics_by_name = _build_rebuild_terminal_metrics_by_name(
            exc,
            full_rebuild_proof_metrics_by_name,
        )
        await mark_control_run(
            run_id,
            status="canceled",
            phase_detail="ptg import canceled",
            progress_message="canceled",
            **(
                {"metrics": failure_metrics_by_name}
                if failure_metrics_by_name
                else {}
            ),
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        return {"status": "canceled", "run_id": run_id}
    except asyncio.CancelledError as exc:
        failure_metrics_by_name = _build_rebuild_terminal_metrics_by_name(
            exc,
            full_rebuild_proof_metrics_by_name,
        )
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="ptg import interrupted",
            progress_message="interrupted",
            error={"code": "import_interrupted", "message": "worker task was cancelled"},
            **(
                {"metrics": failure_metrics_by_name}
                if failure_metrics_by_name
                else {}
            ),
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        raise
    except PTG2FullRebuildFreshnessError as exc:
        freshness_metrics_by_name = _build_rebuild_terminal_metrics_by_name(
            exc,
            full_rebuild_proof_metrics_by_name,
            reported_metrics_by_name=dict(exc.metrics_by_name),
        )
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="ptg full rebuild freshness failed",
            progress_message="failed",
            metrics=freshness_metrics_by_name,
            error={
                "code": "ptg_full_rebuild_reuse_detected",
                "message": "controlled PTG full rebuild reused prior work",
            },
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        raise
    except Exception as exc:
        failure_metrics_by_name = _build_rebuild_terminal_metrics_by_name(
            exc,
            full_rebuild_proof_metrics_by_name,
        )
        failure_error_by_name = (
            {
                "code": "ptg_full_rebuild_failed",
                "message": "controlled PTG full rebuild failed",
            }
            if full_rebuild_proof_metrics_by_name
            else ptg_failure_error(exc)
        )
        failure_metrics_by_name.update(
            await load_blank_failure_metrics(params_by_name, failure_error_by_name)
        )
        await mark_control_run(
            run_id,
            status="failed",
            phase_detail="ptg import failed",
            progress_message="failed",
            error=failure_error_by_name,
            **(
                {"metrics": failure_metrics_by_name}
                if failure_metrics_by_name
                else {}
            ),
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at if run_id else None,
        )
        await _flush_terminal_status_events()
        raise
    finally:
        _stop_threaded_ptg_heartbeat(heartbeat_stop)
        await _stop_live_progress_heartbeat(heartbeat_task)
        if live_token is not None:
            reset_live_progress_context(live_token)
    result_metrics_by_name = import_result if isinstance(import_result, dict) else {}
    if full_rebuild_proof_metrics_by_name:
        result_metrics_by_name = {
            **result_metrics_by_name,
            **full_rebuild_proof_metrics_by_name,
        }
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="ptg import succeeded",
        progress_message="succeeded",
        metrics=result_metrics_by_name or None,
        snapshot_id=(
            str(result_metrics_by_name.get("snapshot_id") or "").strip()
            or None
        ),
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at if run_id else None,
    )
    await _flush_terminal_status_events()
    return {**result_metrics_by_name, "status": "succeeded", "run_id": run_id}


async def _claim_exact_wave_worker_start(
    ctx: Any,
    params_by_name: dict[str, Any],
    *,
    run_id: str,
    claim_attempt_token: str,
) -> None:
    """Bind a released wave job to its attested Pod before source admission."""

    claim_field_map = _exact_wave_claim_values(
        ctx, params_by_name, run_id=run_id, claim_attempt_token=claim_attempt_token,
    )
    if claim_field_map is None:
        return
    await claim_wave_job_start(**claim_field_map)


async def _reconcile_exact_wave_claim_exception(
    ctx: Any,
    params_by_name: dict[str, Any],
    *,
    run_id: str,
    claim_attempt_token: str,
) -> Any | None:
    """Persist a valid claim rejection; leave malformed identities untouched."""

    try:
        claim_field_map = _exact_wave_claim_values(
            ctx, params_by_name, run_id=run_id, claim_attempt_token=claim_attempt_token,
        )
    except Exception:
        return None
    if claim_field_map is None:
        return None
    try:
        resolution = await reconcile_wave_claim_exception(**claim_field_map)
    except Exception:
        # The original claim exception remains authoritative.  Do not guess
        # whether a failed reconciliation committed anything or issue a retry.
        return
    if resolution.status == "rejected":
        await _flush_terminal_status_events()
    return resolution


def _is_complete_exact_wave_payload(params_by_name: dict[str, Any]) -> bool:
    return all(
        isinstance(params_by_name.get(name), str)
        and bool(params_by_name[name])
        and params_by_name[name] == params_by_name[name].strip()
        for name in ("_wave_id", "_wave_digest", "_wave_job_id")
    )


async def _mark_exact_wave_preexecution_failure(
    run_id: str,
    *,
    reason: str,
    error: BaseException | None = None,
) -> None:
    """Make a one-shot wave start rejection terminal instead of leaking capacity."""

    message = str(reason or "worker start failed").strip() or "worker start failed"
    if error is not None and str(error).strip():
        message = f"{message}: {str(error).strip()}"
    await mark_control_run(
        run_id,
        status="failed",
        phase_detail="PTG exact-wave worker start failed",
        progress_message="failed",
        error={
            "code": "ptg_exact_wave_worker_start_failed",
            "message": message,
            "retryable": False,
        },
    )
    await _flush_terminal_status_events()


async def _flush_terminal_status_events() -> None:
    timeout = float(os.getenv("HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS", "0.25"))
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


def _assert_expected_lane(params: dict[str, Any]) -> None:
    expected_queue = str(params.get("_expected_queue") or "").strip()
    active_queue = os.getenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "").strip()
    if expected_queue and active_queue and expected_queue != active_queue:
        raise RuntimeError(f"PTG payload expected {expected_queue}, but active worker queue is {active_queue}")
    expected_class = str(params.get("_expected_worker_class") or "").strip()
    active_class = os.getenv("HLTHPRT_ACTIVE_WORKER_CLASS", "").strip()
    if expected_class and active_class and expected_class != active_class:
        raise RuntimeError(f"PTG payload expected {expected_class}, but active worker class is {active_class}")


def _full_rebuild_scope_digest(
    params: dict[str, Any],
) -> str | None:
    """Validate the opaque rebuild scope accepted from the control API."""

    if _FULL_REBUILD_TOKEN_PARAM in params:
        raise ValueError(
            "PTG workers accept only an internal full rebuild scope"
        )
    if _FULL_REBUILD_SCOPE_PARAM not in params:
        return None
    scope_digest = params[_FULL_REBUILD_SCOPE_PARAM]
    if (
        not isinstance(scope_digest, str)
        or len(scope_digest) != 64
        or scope_digest != scope_digest.lower()
        or any(character not in "0123456789abcdef" for character in scope_digest)
    ):
        raise ValueError("private PTG full rebuild scope digest is invalid")
    return scope_digest


def _full_rebuild_proof_metrics_by_name(
    scope_digest: str | None,
) -> dict[str, bool]:
    """Return safe terminal proof fields when a rebuild scope was accepted."""

    if scope_digest is None:
        return {}
    return {
        "full_rebuild_requested": True,
        "raw_artifact_reuse_forced_off": True,
        "partial_artifact_retention_forced_off": True,
    }


def _build_rebuild_terminal_metrics_by_name(
    error: BaseException,
    policy_metrics_by_name: dict[str, bool],
    *,
    reported_metrics_by_name: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge safe runtime proof with control-plane rebuild policy proof."""

    if not policy_metrics_by_name:
        return {}
    return {
        **dict(reported_metrics_by_name or {}),
        **full_rebuild_failure_metrics(error),
        **policy_metrics_by_name,
    }
