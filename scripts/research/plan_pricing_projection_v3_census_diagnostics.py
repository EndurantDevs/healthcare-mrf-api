# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy-safe database and process diagnostics for the projection-v3 census."""

from __future__ import annotations

import asyncio
import hashlib
import json
import signal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from api.plan_pricing_projection_v3_code import (
    _PriceHydrationReadLimitError,
    _PriceMembershipMetadataReadLimitError,
)
from scripts.ptg_v4_dev_canary_io import utc_now_text, write_json
from scripts.research.plan_pricing_projection_v3_census_transaction import (
    CENSUS_DATABASE_STAGE_KEYS,
    census_database_application_name,
    census_database_run_token,
    expected_census_database_settings,
    set_census_database_stage,
)

ERROR_DIMENSION_BY_TYPE = {
    _PriceMembershipMetadataReadLimitError: "price_membership_metadata",
    _PriceHydrationReadLimitError: "price_hydration",
}
DATABASE_PHASE = "measuring database stage"
DATABASE_MESSAGE = "executing bounded census database stage"
_REQUIRED_DATABASE_STAGE_KEYS = CENSUS_DATABASE_STAGE_KEYS - {"taxonomy_filter"}
_PROCESS_SIGNALS = (signal.SIGINT, signal.SIGTERM)
CENSUS_ENVELOPE_CONTRACT = "healthporta.plan-pricing-v3-census-envelope.v1"
CENSUS_RECEIPT_CONTRACT = "healthporta.plan-pricing-v3-work-census.v1"
CENSUS_ACCEPTANCE_AUTHORITY = "outer_envelope_with_zero_child_exit"
_EXPECTED_ENVELOPE_KEYS = frozenset(
    {
        "contract",
        "status",
        "exit_code",
        "reviewed_source_sha",
        "envelope_script_sha256",
        "owner_token",
        "resource_uids",
        "prior_drain_mode",
        "child_command_sha256",
        "child_exit_code",
        "census_job",
        "census_receipt_sha256",
        "timed_out",
        "probe_verified",
        "quota_probe_verified",
        "pre_child_fence_verified",
        "post_child_fence_verified",
        "cleanup",
        "postgresql_boundary",
    }
)
_EXPECTED_ENVELOPE_RESOURCE_KEYS = frozenset(
    {"quota", "policy", "binding", "lock_invocation"}
)
_EXPECTED_ENVELOPE_CLEANUP_KEYS = frozenset(
    {
        "binding_removed",
        "policy_removed",
        "drain_restored",
        "quota_removed",
        "lock_released",
        "complete",
    }
)


class _CensusInterrupted(Exception):
    """One handled process signal after rollback and cleanup completed."""

    def __init__(self, signal_number: int) -> None:
        self.signal_number = signal_number
        super().__init__(signal.Signals(signal_number).name)

    @property
    def exit_code(self) -> int:
        """Return the conventional process exit code for the signal."""

        return 128 + self.signal_number


def _record_resource_sample(
    resources_by_stage: dict[str, dict[str, int]],
    stage: str,
    boundary: str,
    sample_by_field: Mapping[str, int | str],
) -> None:
    """Retain compact before/after maxima for one privacy-safe stage."""

    stage_by_field = resources_by_stage.setdefault(stage, {})
    count_field = f"{boundary}_count"
    stage_by_field[count_field] = stage_by_field.get(count_field, 0) + 1
    for sample_field in (
        "backend_memory_context_bytes",
        "temporary_relation_bytes",
    ):
        maximum_field = f"{boundary}_{sample_field}_maximum"
        stage_by_field[maximum_field] = max(
            stage_by_field.get(maximum_field, 0),
            int(sample_by_field[sample_field]),
        )


@dataclass
class CensusDatabaseStages:
    """Bind each closed census substage to one PostgreSQL backend."""

    receipt_by_field: dict[str, Any]
    receipt_path: Path
    run_token: str
    previous_stage: str | None = None
    resources_by_stage: dict[str, dict[str, int]] = field(default_factory=dict)

    async def checkpoint(
        self,
        session: Any,
        stage: str,
        code_ordinal: int | None = None,
    ) -> str:
        """Persist one closed substage after binding it to PostgreSQL."""

        if stage not in CENSUS_DATABASE_STAGE_KEYS:
            raise ValueError("pricing projection census database stage is invalid")
        sample_by_field = await set_census_database_stage(
            session,
            self.run_token,
            stage,
            str(
                self.receipt_by_field.get("database_application_name")
                or self.receipt_by_field["database_session_settings"][
                    "application_name"
                ]
            ),
            code_ordinal,
        )
        if int(sample_by_field["backend_pid"]) != self.receipt_by_field.get(
            "database_backend_pid"
        ):
            raise RuntimeError("pricing projection census database backend changed")
        if self.previous_stage is not None:
            _record_resource_sample(
                self.resources_by_stage,
                self.previous_stage,
                "after",
                sample_by_field,
            )
        _record_resource_sample(
            self.resources_by_stage,
            stage,
            "before",
            sample_by_field,
        )
        self.previous_stage = stage
        application_name = str(sample_by_field["application_name"])
        self.receipt_by_field.update(
            phase=DATABASE_PHASE,
            message=DATABASE_MESSAGE,
            database_stage=stage,
            database_application_name=application_name,
            database_stage_resources=self.resources_by_stage,
        )
        write_json(self.receipt_path, self.receipt_by_field)
        return application_name


class _CensusSignalState:
    """Own one census task and its first process signal."""

    def __init__(self, receipt_by_field: dict[str, Any]) -> None:
        self.receipt_by_field = receipt_by_field
        self.number: int | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.task: asyncio.Task[int] | None = None

    def interrupt(self, number: int, _frame: Any) -> None:
        """Cancel active work on the first handled signal."""

        if self.number is not None:
            return
        self.number = number
        if self.task is None or self.task.done():
            raise _CensusInterrupted(number)
        if (
            self.loop is not None
            and not self.loop.is_closed()
            and self.task is not None
            and not self.task.done()
        ):
            self.loop.call_soon_threadsafe(self.task.cancel)

    def apply_interruption(self, current_exit_code: int) -> int:
        """Seal any captured signal as a failed receipt and exit code."""

        if self.number is None:
            return current_exit_code
        self.receipt_by_field.update(
            status="failed",
            accepted=False,
            cap_calibration_admissible=False,
            resource_proof_admissible=False,
            finished_at=utc_now_text(),
            error={
                "type": "_CensusInterrupted",
                "signal": signal.Signals(self.number).name,
            },
        )
        return 128 + self.number

    async def run(
        self,
        runner: Callable[[Any, dict[str, Any]], Awaitable[int]],
        args: Any,
    ) -> int:
        """Run the owned task and translate signal cancellation."""

        self.loop = asyncio.get_running_loop()
        self.task = asyncio.create_task(runner(args, self.receipt_by_field))
        if self.number is not None:
            self.task.cancel()
        try:
            return await self.task
        except asyncio.CancelledError:
            if self.number is not None:
                raise _CensusInterrupted(self.number) from None
            raise


def _seal_failure(
    args: Any,
    receipt_by_field: dict[str, Any],
    exc: BaseException,
    source_identity: Callable[[Any], dict[str, Any]],
) -> int:
    """Seal one privacy-safe census failure."""

    try:
        receipt_by_field["source_after"] = source_identity(args)
    except BaseException as source_exc:
        receipt_by_field["source_after_error"] = {"type": type(source_exc).__name__}
    error_by_field = {"type": type(exc).__name__}
    if isinstance(exc, _CensusInterrupted):
        error_by_field["signal"] = signal.Signals(exc.signal_number).name
    error_dimension = ERROR_DIMENSION_BY_TYPE.get(type(exc))
    if error_dimension is not None:
        error_by_field["dimension"] = error_dimension
    receipt_by_field.update(
        status="failed",
        accepted=False,
        cap_calibration_admissible=False,
        resource_proof_admissible=False,
        finished_at=utc_now_text(),
        error=error_by_field,
    )
    return exc.exit_code if isinstance(exc, _CensusInterrupted) else 1


def _write_final_receipt(
    args: Any,
    receipt_by_field: dict[str, Any],
    signal_state: _CensusSignalState,
    exit_code: int,
    previous_handler_by_signal: Mapping[int, Any],
) -> int:
    """Commit acceptance only after prior signal handlers are restored."""

    exit_code = signal_state.apply_interruption(exit_code)
    final_receipt_by_field = dict(receipt_by_field)
    provisional_receipt_by_field = {
        **final_receipt_by_field,
        "status": "finalizing",
        "accepted": False,
        "cap_calibration_admissible": False,
        "resource_proof_admissible": False,
    }
    try:
        write_json(args.receipt.resolve(), provisional_receipt_by_field)
        exit_code = signal_state.apply_interruption(exit_code)
        for number, previous_handler in previous_handler_by_signal.items():
            signal.signal(number, previous_handler)
        write_json(args.receipt.resolve(), final_receipt_by_field)
        return exit_code
    except (_CensusInterrupted, KeyboardInterrupt):
        if signal_state.number is None:
            signal_state.number = signal.SIGINT
        for number in _PROCESS_SIGNALS:
            signal.signal(number, signal_state.interrupt)
        exit_code = signal_state.apply_interruption(exit_code)
        write_json(args.receipt.resolve(), receipt_by_field)
        return exit_code


def run_census_process(
    args: Any,
    receipt_by_field: dict[str, Any],
    runner: Callable[[Any, dict[str, Any]], Awaitable[int]],
    source_identity: Callable[[Any], dict[str, Any]],
) -> int:
    """Run one signal-aware census process and atomically seal its receipt."""

    signal_state = _CensusSignalState(receipt_by_field)
    previous_handler_by_signal = {
        number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for number in previous_handler_by_signal:
            signal.signal(number, signal_state.interrupt)
        try:
            exit_code = asyncio.run(signal_state.run(runner, args))
        except BaseException as exc:
            exit_code = _seal_failure(args, receipt_by_field, exc, source_identity)
        return _write_final_receipt(
            args,
            receipt_by_field,
            signal_state,
            exit_code,
            previous_handler_by_signal,
        )
    finally:
        for number, previous_handler in previous_handler_by_signal.items():
            if signal.getsignal(number) != previous_handler:
                signal.signal(number, previous_handler)


def is_database_receipt_valid(receipt_by_field: Mapping[str, Any]) -> bool:
    """Return whether one receipt binds the complete closed database lifecycle."""

    runtime_by_field = receipt_by_field.get("runtime")
    resources_by_stage = receipt_by_field.get("database_stage_resources")
    if not isinstance(runtime_by_field, Mapping) or not isinstance(
        resources_by_stage, Mapping
    ):
        return False
    try:
        run_token = census_database_run_token(runtime_by_field)
        final_application_name = census_database_application_name(
            run_token,
            "measurement_complete",
        )
        expected_settings = expected_census_database_settings(run_token)
    except (TypeError, ValueError):
        return False
    if (
        receipt_by_field.get("database_run_token") != run_token
        or type(receipt_by_field.get("database_backend_pid")) is not int
        or receipt_by_field["database_backend_pid"] <= 0
        or receipt_by_field.get("database_session_settings") != expected_settings
        or receipt_by_field.get("database_stage") != "measurement_complete"
        or receipt_by_field.get("database_application_name") != final_application_name
        or not _REQUIRED_DATABASE_STAGE_KEYS.issubset(resources_by_stage)
        or not frozenset(resources_by_stage).issubset(CENSUS_DATABASE_STAGE_KEYS)
    ):
        return False
    for stage, resource_by_field in resources_by_stage.items():
        expected_fields = {
            "before_count",
            "before_backend_memory_context_bytes_maximum",
            "before_temporary_relation_bytes_maximum",
        }
        if stage != "measurement_complete":
            expected_fields.update(
                {
                    "after_count",
                    "after_backend_memory_context_bytes_maximum",
                    "after_temporary_relation_bytes_maximum",
                }
            )
        if (
            not isinstance(resource_by_field, Mapping)
            or set(resource_by_field) != expected_fields
            or any(
                type(resource_value) is not int or resource_value < 0
                for resource_value in resource_by_field.values()
            )
            or resource_by_field["before_count"] <= 0
            or (
                stage != "measurement_complete"
                and resource_by_field["after_count"]
                != resource_by_field["before_count"]
            )
        ):
            return False
    return True


def census_receipt_sha256(receipt_by_field: Mapping[str, Any]) -> str:
    """Hash the exact canonical bytes written by the census receipt writer."""

    serialized = json.dumps(receipt_by_field, indent=2, sort_keys=True) + "\n"
    return hashlib.sha256(serialized.encode()).hexdigest()


def _is_sha256(field_value: Any) -> bool:
    return (
        isinstance(field_value, str)
        and len(field_value) == 64
        and not (set(field_value) - set("0123456789abcdef"))
    )


def _is_successful_envelope(envelope_by_field: Mapping[str, Any]) -> bool:
    cleanup_by_field = envelope_by_field.get("cleanup")
    resource_uids = envelope_by_field.get("resource_uids")
    return (
        frozenset(envelope_by_field) == _EXPECTED_ENVELOPE_KEYS
        and envelope_by_field.get("contract") == CENSUS_ENVELOPE_CONTRACT
        and envelope_by_field.get("status") == "complete"
        and type(envelope_by_field.get("exit_code")) is int
        and envelope_by_field["exit_code"] == 0
        and type(envelope_by_field.get("child_exit_code")) is int
        and envelope_by_field["child_exit_code"] == 0
        and envelope_by_field.get("timed_out") is False
        and envelope_by_field.get("probe_verified") is True
        and envelope_by_field.get("quota_probe_verified") is True
        and envelope_by_field.get("pre_child_fence_verified") is True
        and envelope_by_field.get("post_child_fence_verified") is True
        and type(envelope_by_field.get("prior_drain_mode")) is bool
        and isinstance(envelope_by_field.get("owner_token"), str)
        and bool(envelope_by_field["owner_token"])
        and _is_sha256(envelope_by_field.get("envelope_script_sha256"))
        and _is_sha256(envelope_by_field.get("child_command_sha256"))
        and envelope_by_field.get("postgresql_boundary")
        == "Kubernetes QoS does not reserve or cap off-node PostgreSQL"
        and isinstance(cleanup_by_field, Mapping)
        and frozenset(cleanup_by_field) == _EXPECTED_ENVELOPE_CLEANUP_KEYS
        and all(field_value is True for field_value in cleanup_by_field.values())
        and isinstance(resource_uids, Mapping)
        and frozenset(resource_uids) == _EXPECTED_ENVELOPE_RESOURCE_KEYS
        and all(
            isinstance(field_value, str) and field_value
            for field_value in resource_uids.values()
        )
    )


def is_authoritative_envelope(
    receipt_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
) -> bool:
    """Bind the exact inner receipt to one successful outer process envelope."""

    runtime_by_field = receipt_by_field.get("runtime")
    source_before = receipt_by_field.get("source_before")
    source_after = receipt_by_field.get("source_after")
    reviewed_source_sha = envelope_by_field.get("reviewed_source_sha")
    try:
        receipt_sha256 = census_receipt_sha256(receipt_by_field)
    except (TypeError, ValueError):
        return False
    return (
        isinstance(runtime_by_field, Mapping)
        and receipt_by_field.get("contract") == CENSUS_RECEIPT_CONTRACT
        and receipt_by_field.get("status") == "complete"
        and receipt_by_field.get("accepted") is True
        and receipt_by_field.get("mode") == "cardinality_census"
        and receipt_by_field.get("cap_calibration_admissible") is True
        and receipt_by_field.get("resource_proof_admissible") is False
        and receipt_by_field.get("proof_scope") == "row_count_limits_only"
        and receipt_by_field.get("acceptance_authority") == CENSUS_ACCEPTANCE_AUTHORITY
        and envelope_by_field.get("census_job") == runtime_by_field.get("job_name")
        and envelope_by_field.get("census_receipt_sha256") == receipt_sha256
        and isinstance(source_before, Mapping)
        and isinstance(source_after, Mapping)
        and source_before.get("declared_git_head") == reviewed_source_sha
        and source_after.get("declared_git_head") == reviewed_source_sha
        and _is_successful_envelope(envelope_by_field)
    )
