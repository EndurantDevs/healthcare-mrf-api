"""Per-member RSA receipts for later ordinary PTG terminal runs.

The exact-wave member run remains pristine and abandoned. This module signs
only a separate ordinary ``ImportRun`` after independently correlating its
frozen source/scope inputs with one immutable V6 member and the signed V12
abandonment. There is deliberately no all-member completion or linkage gate.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select, text

from db.models import (
    ImportRun,
    PTG2ImportRun,
    PTG2Snapshot,
    PTGImportWave,
    PTGImportWaveIntent,
    PTGImportWaveOrdinaryTerminalReceipt,
    PTGImportWaveQuarantine,
    db,
)
from process.ptg_wave_ordinary_terminal_contract import (
    COORDINATE_DIGEST_DOMAIN,
    ENGINE_OPTIONS_DIGEST_DOMAIN,
    ENGINE_REPORT_DIGEST_DOMAIN,
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
    PTGWaveOrdinaryTerminalConflict,
    PTGWaveOrdinaryTerminalRetryable,
    RUN_METRICS_DIGEST_DOMAIN,
    RUN_PARAMS_DIGEST_DOMAIN,
    SCOPE_DIGEST_DOMAIN,
    SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
    TERMINAL_RESULT_DIGEST_DOMAIN,
    _receipt_datetime,
    validate_ordinary_terminal_request,
)
from process.ptg_wave_ordinary_terminal_payload import (
    ordinary_terminal_receipt_payload,
)
from process.ptg_wave_ordinary_terminal_validation import (
    _outer_result_identities,
    _validated_abandonment,
)
from process.ptg_allowed_amount_blank import (
    allowed_amount_blank_metrics,
    is_allowed_amount_blank_error,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg_wave_terminal_snapshot_pin import (
    delete_ordinary_terminal_snapshot_pin,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    canonical_receipt_timestamp,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)


ORDINARY_TERMINAL_LOCK_TIMEOUT = "250ms"
ORDINARY_TERMINAL_STATEMENT_TIMEOUT = "5s"
_RETRYABLE_DATABASE_SQLSTATES = frozenset({"55P03", "57014"})


async def issue_ordinary_terminal_receipt(
    operation_id: object,
    request: object,
    *,
    receipt_keyring: PTGWaveReceiptKeyring | None = None,
    receipt_issued_at: dt.datetime | str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Sign or exactly replay one independently terminal ordinary member."""

    validated = validate_ordinary_terminal_request(
        request,
        operation_id=operation_id,
    )
    try:
        async with db.transaction() as session:
            await _configure_bounded_receipt_transaction(session)
            await _acquire_member_receipt_lock(session, validated)
            existing = await _load_existing_receipt(session, validated)
            keyring = require_process_receipt_keyring(receipt_keyring)
            snapshot = await _load_terminal_snapshot(session, validated)
            _verify_abandonment_signature(snapshot, validated, keyring)
            receipt_payload = ordinary_terminal_receipt_payload(**snapshot)
            if existing is not None:
                replayed = _validate_existing_receipt(
                    existing,
                    request=validated,
                    expected_payload=receipt_payload,
                    keyring=keyring,
                )
                await _release_terminal_snapshot_pin(
                    session,
                    request=validated,
                    payload=receipt_payload,
                )
                return replayed, False
            receipt = keyring.sign_receipt(
                schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
                key_id=validated["key_id"],
                issued_at=receipt_issued_at or dt.datetime.now(dt.UTC),
                receipt_payload=receipt_payload,
            )
            await _persist_terminal_receipt(session, validated, receipt)
            await _release_terminal_snapshot_pin(
                session,
                request=validated,
                payload=receipt_payload,
            )
            return receipt, True
    except Exception as exc:
        if _database_sqlstate(exc) not in _RETRYABLE_DATABASE_SQLSTATES:
            raise
        raise PTGWaveOrdinaryTerminalRetryable(
            "ordinary terminal receipt database wait expired; retry"
        ) from exc


async def _configure_bounded_receipt_transaction(session: Any) -> None:
    """Make every lock and statement wait finite before reading any row."""

    await session.execute(
        text(
            "SELECT set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)"
        ),
        {
            "lock_timeout": ORDINARY_TERMINAL_LOCK_TIMEOUT,
            "statement_timeout": ORDINARY_TERMINAL_STATEMENT_TIMEOUT,
        },
    )


def _database_sqlstate(error: BaseException) -> str:
    pending_errors = [error]
    visited_error_ids: set[int] = set()
    while pending_errors:
        candidate = pending_errors.pop()
        if id(candidate) in visited_error_ids:
            continue
        visited_error_ids.add(id(candidate))
        for field in ("sqlstate", "pgcode"):
            value = getattr(candidate, field, None)
            if value:
                return str(value)
        for field in ("orig", "__cause__", "__context__"):
            nested = getattr(candidate, field, None)
            if isinstance(nested, BaseException):
                pending_errors.append(nested)
    return ""


async def _acquire_member_receipt_lock(
    session: Any,
    request: Mapping[str, Any],
) -> None:
    # The lock is member-local. A slow source cannot prevent another completed
    # member from obtaining its receipt.
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended("
            ":receipt_lock_identity, 0))"
        ),
        {
            "receipt_lock_identity": (
                "ptg-wave-ordinary-terminal-receipt:"
                f"{request['operation_id']}:{request['member_ordinal']}"
            )
        },
    )


async def _load_existing_receipt(
    session: Any,
    request: Mapping[str, Any],
) -> Any:
    return (
        await session.execute(
            select(PTGImportWaveOrdinaryTerminalReceipt)
            .where(
                PTGImportWaveOrdinaryTerminalReceipt.wave_id
                == request["operation_id"],
                PTGImportWaveOrdinaryTerminalReceipt.member_ordinal
                == request["member_ordinal"],
            )
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _load_terminal_snapshot(
    session: Any,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    """Read exactly one member result under member-local terminal row locks."""

    operation_id = request["operation_id"]
    wave = (
        await session.execute(
            select(PTGImportWave).where(PTGImportWave.wave_id == operation_id)
        )
    ).scalar_one_or_none()
    if wave is None:
        raise PTGWaveOrdinaryTerminalConflict("V12 operation is unavailable")
    intent = (
        await session.execute(
            select(PTGImportWaveIntent).where(
                PTGImportWaveIntent.wave_id == operation_id,
                PTGImportWaveIntent.ordinal == request["member_ordinal"],
            )
        )
    ).scalar_one_or_none()
    if intent is None:
        raise PTGWaveOrdinaryTerminalConflict(
            "V12 operation member is unavailable"
        )
    quarantine = (
        await session.execute(
            select(PTGImportWaveQuarantine).where(
                PTGImportWaveQuarantine.predecessor_wave_id == operation_id
            )
        )
    ).scalar_one_or_none()
    run = await _load_outer_run(session, request, intent)
    engine_run, engine_snapshot = await _load_engine_result(
        session,
        run,
        request=request,
        intent=intent,
    )
    projected_metrics_by_name = _project_blank_metrics(
        run, engine_run, engine_snapshot
    )
    if projected_metrics_by_name is not None:
        # Persist before receipt insertion so synchronized mirrors see blank metrics.
        run.metrics = projected_metrics_by_name
        await session.flush()
    return {
        "request": request,
        "wave": wave,
        "intent": intent,
        "quarantine": quarantine,
        "run": run,
        "engine_run": engine_run,
        "engine_snapshot": engine_snapshot,
    }


async def _load_outer_run(
    session: Any,
    request: Mapping[str, Any],
    intent: Any,
) -> Any:
    run = (
        await session.execute(
            select(ImportRun)
            .where(ImportRun.run_id == request["run_id"])
            .with_for_update()
        )
    ).scalar_one_or_none()
    if run is None:
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal run is unavailable"
        )
    _outer_engine_import_run_id(run, request=request, intent=intent)
    return run


async def _load_engine_result(
    session: Any,
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
) -> tuple[Any, Any]:
    engine_import_run_id = _outer_engine_import_run_id(
        run, request=request, intent=intent
    )
    engine_run = (
        await session.execute(
            select(PTG2ImportRun)
            .where(PTG2ImportRun.import_run_id == engine_import_run_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if getattr(run, "status", None) == "succeeded":
        _, snapshot_id = _outer_result_identities(
            run, request=request, intent=intent
        )
    else:
        report = (
            getattr(engine_run, "report", None)
            if engine_run is not None
            else None
        )
        snapshot_id = (
            report.get("snapshot_id") if isinstance(report, Mapping) else None
        )
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise PTGWaveOrdinaryTerminalConflict(
                "durable PTG terminal result is unavailable"
            )
    engine_snapshot = (
        await session.execute(
            select(PTG2Snapshot)
            .where(PTG2Snapshot.snapshot_id == snapshot_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    return engine_run, engine_snapshot


def _outer_engine_import_run_id(
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
) -> str:
    if (
        getattr(run, "run_id", None) != request["run_id"]
        or getattr(run, "run_id", None) == getattr(intent, "run_id", None)
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal run identity is invalid"
        )
    if getattr(run, "status", None) == "succeeded":
        return _outer_result_identities(
            run, request=request, intent=intent
        )[0]
    if (
        getattr(run, "status", None) == "failed"
        and is_allowed_amount_blank_error(getattr(run, "error", None))
        and getattr(run, "source_file_import_id", None)
        == request["source_file_import_id"]
        and getattr(run, "import_id", None) == request["source_file_import_id"]
    ):
        return f"ptg2:{request['source_file_import_id']}"
    raise PTGWaveOrdinaryTerminalConflict(
        "ordinary terminal run is not a supported terminal result"
    )


def _project_blank_metrics(
    run: Any,
    engine_run: Any,
    engine_snapshot: Any,
) -> dict[str, Any] | None:
    if getattr(run, "status", None) != "failed":
        return None
    params_map = getattr(run, "params", None)
    if not isinstance(params_map, Mapping):
        return None
    blank_metrics = allowed_amount_blank_metrics(
        source_file_import_id=str(
            getattr(run, "source_file_import_id", None) or ""
        ),
        source_key=str(params_map.get("source_key") or ""),
        import_month=params_map.get("import_month"),
        plan_ids=params_map.get("plan_ids") or [],
        plan_market_types=params_map.get("plan_market_types") or [],
        outer_error=getattr(run, "error", None),
        engine_run=engine_run,
        engine_snapshot=engine_snapshot,
    )
    if blank_metrics is None:
        return None
    projected_metrics_by_name = {
        **dict(getattr(run, "metrics", None) or {}),
        **blank_metrics,
    }
    if projected_metrics_by_name != getattr(run, "metrics", None):
        return projected_metrics_by_name
    return None


def _verify_abandonment_signature(
    snapshot: Mapping[str, Any],
    request: Mapping[str, Any],
    keyring: PTGWaveReceiptKeyring,
) -> None:
    abandonment_payload = _validated_abandonment(
        snapshot["quarantine"],
        wave=snapshot["wave"],
        key_id=request["key_id"],
    )
    try:
        keyring.validate_stored_receipt(
            snapshot["quarantine"].abandonment_receipt,
            schema=ABANDONMENT_RECEIPT_SCHEMA,
            key_id=request["key_id"],
            expected_payload=abandonment_payload,
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveOrdinaryTerminalConflict(
            "stored V12 abandonment signature is invalid"
        ) from exc


async def _persist_terminal_receipt(
    session: Any,
    request: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> None:
    issued_at = _receipt_datetime(receipt["issued_at"])
    session.add(
        PTGImportWaveOrdinaryTerminalReceipt(
            wave_id=request["operation_id"],
            member_ordinal=request["member_ordinal"],
            source_file_import_id=request["source_file_import_id"],
            run_id=request["run_id"],
            receipt_key_id=request["key_id"],
            receipt=dict(receipt),
            payload_digest=receipt["payload_digest"],
            issued_at=issued_at,
            created_at=issued_at,
        )
    )
    await session.flush()


async def _release_terminal_snapshot_pin(
    session: Any,
    *,
    request: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> int:
    """Release only this member's manifest pin with its receipt commit."""

    return await delete_ordinary_terminal_snapshot_pin(
        session,
        schema_name=resolve_ptg2_schema(),
        operation_id=request["operation_id"],
        member_ordinal=request["member_ordinal"],
        snapshot_id=payload["snapshot_id"],
    )


def _validate_existing_receipt(
    existing: Any,
    *,
    request: Mapping[str, Any],
    expected_payload: Mapping[str, Any],
    keyring: PTGWaveReceiptKeyring,
) -> dict[str, Any]:
    receipt = getattr(existing, "receipt", {})
    if (
        getattr(existing, "wave_id", None) != request["operation_id"]
        or getattr(existing, "member_ordinal", None)
        != request["member_ordinal"]
        or getattr(existing, "source_file_import_id", None)
        != request["source_file_import_id"]
        or getattr(existing, "run_id", None) != request["run_id"]
        or getattr(existing, "receipt_key_id", None) != request["key_id"]
        or getattr(existing, "payload_digest", None)
        != receipt.get("payload_digest")
        or canonical_receipt_timestamp(getattr(existing, "issued_at", None))
        != receipt.get("issued_at")
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "stored ordinary terminal receipt identity is invalid"
        )
    try:
        return keyring.validate_stored_receipt(
            receipt,
            schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
            key_id=request["key_id"],
            expected_payload=expected_payload,
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveOrdinaryTerminalConflict(str(exc)) from exc


__all__ = [
    "COORDINATE_DIGEST_DOMAIN",
    "ENGINE_OPTIONS_DIGEST_DOMAIN",
    "ENGINE_REPORT_DIGEST_DOMAIN",
    "ORDINARY_TERMINAL_PAYLOAD_FIELDS",
    "ORDINARY_TERMINAL_REQUEST_SCHEMA",
    "PTGWaveOrdinaryTerminalConflict",
    "PTGWaveOrdinaryTerminalRetryable",
    "RUN_METRICS_DIGEST_DOMAIN",
    "RUN_PARAMS_DIGEST_DOMAIN",
    "SCOPE_DIGEST_DOMAIN",
    "SNAPSHOT_MANIFEST_DIGEST_DOMAIN",
    "TERMINAL_RESULT_DIGEST_DOMAIN",
    "issue_ordinary_terminal_receipt",
    "ordinary_terminal_receipt_payload",
    "validate_ordinary_terminal_request",
]
