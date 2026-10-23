"""Per-member RSA receipts for later ordinary PTG terminal runs.

The exact-wave member run remains pristine and abandoned. This module signs
only a separate ordinary ``ImportRun`` after independently correlating its
frozen source/scope inputs with one immutable V6 member and the signed V12
abandonment. There is deliberately no all-member completion or linkage gate.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
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
from process.ptg_allowed_amount_blank import (
    allowed_amount_blank_metrics,
    is_allowed_amount_blank_error,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg_wave_terminal_snapshot_pin import (
    delete_ordinary_terminal_snapshot_pin,
)
from process.ptg_parts.snapshot_tables import _normalize_source_key
from process.ptg_parts.source_snapshot_reference_lifetime import (
    SnapshotReferenceIdentityError,
    acquire_snapshot_reference_lifetime,
)
from process.ptg_wave_ordinary_terminal_contract import (
    COORDINATE_DIGEST_DOMAIN,
    ENGINE_OPTIONS_DIGEST_DOMAIN,
    ENGINE_REPORT_DIGEST_DOMAIN,
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
    RUN_METRICS_DIGEST_DOMAIN,
    RUN_PARAMS_DIGEST_DOMAIN,
    SCOPE_DIGEST_DOMAIN,
    SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
    TERMINAL_RESULT_DIGEST_DOMAIN,
    PTGWaveOrdinaryTerminalConflict,
    PTGWaveOrdinaryTerminalRetryable,
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


@dataclass(frozen=True)
class _TerminalSnapshotReference:
    internal_run_id: str
    snapshot_id: str
    source_key: str


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
        keyring = require_process_receipt_keyring(receipt_keyring)
        reference = await _preflight_terminal_snapshot_reference(validated, keyring)
        async with db.transaction() as session:
            await _acquire_terminal_reference(session, reference)
            await _acquire_member_receipt_lock(session, validated)
            existing = await _load_existing_receipt(session, validated)
            snapshot = await _load_terminal_snapshot(session, validated, expected_reference=reference)
            _verify_abandonment_signature(snapshot, validated, keyring)
            receipt_payload = ordinary_terminal_receipt_payload(**snapshot)
            return await _issue_or_replay_receipt(
                session,
                validated,
                receipt_payload,
                keyring,
                existing,
                receipt_issued_at,
            )
    except SnapshotReferenceIdentityError as exc:
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal snapshot identity changed") from exc
    except Exception as exc:
        if not getattr(exc, "retryable", False) and _database_sqlstate(exc) not in _RETRYABLE_DATABASE_SQLSTATES:
            raise
        raise PTGWaveOrdinaryTerminalRetryable("ordinary terminal receipt database wait expired; retry") from exc


async def _acquire_terminal_reference(
    session: Any,
    reference: _TerminalSnapshotReference,
) -> None:
    """Fence the verified source before acquiring member or terminal row locks."""

    _require_receipt_database_engine(session)
    await _configure_bounded_receipt_transaction(session)
    source_key = await acquire_snapshot_reference_lifetime(
        session,
        schema_name=resolve_ptg2_schema(),
        snapshot_id=reference.snapshot_id,
        internal_run_id=reference.internal_run_id,
        lock_timeout=ORDINARY_TERMINAL_LOCK_TIMEOUT,
        statement_timeout=ORDINARY_TERMINAL_STATEMENT_TIMEOUT,
    )
    if source_key != reference.source_key:
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal snapshot source changed")


async def _issue_or_replay_receipt(
    session: Any,
    request: Mapping[str, Any],
    receipt_payload: Mapping[str, Any],
    keyring: PTGWaveReceiptKeyring,
    existing: Any,
    issued_at: dt.datetime | str | None,
) -> tuple[dict[str, Any], bool]:
    if existing is not None:
        receipt = _validate_existing_receipt(
            existing,
            request=request,
            expected_payload=receipt_payload,
            keyring=keyring,
        )
    else:
        receipt = keyring.sign_receipt(
            schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
            key_id=request["key_id"],
            issued_at=issued_at or dt.datetime.now(dt.UTC),
            receipt_payload=receipt_payload,
        )
        await _persist_terminal_receipt(session, request, receipt)
    await _release_terminal_snapshot_pin(
        session,
        request=request,
        payload=receipt_payload,
    )
    return receipt, existing is None


async def _preflight_terminal_snapshot_reference(
    request: Mapping[str, Any],
    keyring: PTGWaveReceiptKeyring,
) -> _TerminalSnapshotReference:
    """Read coordinates without row locks or projecting durable blank metrics."""

    if db.session_factory is None:
        await db.connect()
    assert db.session_factory is not None
    async with db.session_factory() as session:
        _require_receipt_database_engine(session)
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await _configure_bounded_receipt_transaction(session)
        snapshot = await _load_terminal_snapshot(session, request, lock_rows=False)
        _verify_abandonment_signature(snapshot, request, keyring)
        return _terminal_snapshot_reference(snapshot)


def _require_receipt_database_engine(session: Any) -> None:
    """Keep independent preflight and any caller-owned writer on one engine."""

    if db.engine is None or session.get_bind().engine is not db.engine.sync_engine:
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal receipt database binding changed")


def _terminal_snapshot_reference(
    snapshot: Mapping[str, Any],
) -> _TerminalSnapshotReference:
    engine_run = snapshot["engine_run"]
    engine_snapshot = snapshot["engine_snapshot"]
    internal_run_id = getattr(engine_run, "import_run_id", None)
    snapshot_id = getattr(engine_snapshot, "snapshot_id", None)
    options = getattr(engine_run, "options", None)
    source_key = options.get("source_key") if isinstance(options, Mapping) else None
    if (
        not isinstance(internal_run_id, str)
        or not internal_run_id
        or not isinstance(snapshot_id, str)
        or not snapshot_id
        or getattr(engine_snapshot, "import_run_id", None) != internal_run_id
        or not isinstance(source_key, str)
        or not source_key
        or _normalize_source_key(source_key) != source_key
    ):
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal snapshot identity is unavailable")
    return _TerminalSnapshotReference(internal_run_id, snapshot_id, source_key)


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
        text("SELECT pg_advisory_xact_lock(hashtextextended(:receipt_lock_identity, 0))"),
        {
            "receipt_lock_identity": (
                f"ptg-wave-ordinary-terminal-receipt:{request['operation_id']}:{request['member_ordinal']}"
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
                PTGImportWaveOrdinaryTerminalReceipt.wave_id == request["operation_id"],
                PTGImportWaveOrdinaryTerminalReceipt.member_ordinal == request["member_ordinal"],
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _load_terminal_snapshot(
    session: Any,
    request: Mapping[str, Any],
    *,
    lock_rows: bool = True,
    expected_reference: _TerminalSnapshotReference | None = None,
) -> dict[str, Any]:
    """Read exactly one member result under member-local terminal row locks."""

    wave, intent, quarantine = await _load_abandoned_member(session, request)
    run = await _load_outer_run(session, request, intent, lock_rows=lock_rows)
    engine_run, engine_snapshot = await _load_engine_result(
        session,
        run,
        request=request,
        intent=intent,
        lock_rows=lock_rows,
    )
    snapshot_by_field = {
        "request": request,
        "wave": wave,
        "intent": intent,
        "quarantine": quarantine,
        "run": run,
        "engine_run": engine_run,
        "engine_snapshot": engine_snapshot,
    }
    if expected_reference is not None and _terminal_snapshot_reference(snapshot_by_field) != expected_reference:
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal snapshot identity changed")
    if lock_rows:
        await _persist_projected_blank_metrics(session, run, engine_run, engine_snapshot)
    return snapshot_by_field


async def _load_abandoned_member(
    session: Any,
    request: Mapping[str, Any],
) -> tuple[Any, Any, Any]:
    operation_id = request["operation_id"]
    wave = (
        await session.execute(
            select(PTGImportWave).where(PTGImportWave.wave_id == operation_id).execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if wave is None:
        raise PTGWaveOrdinaryTerminalConflict("V12 operation is unavailable")
    intent = (
        await session.execute(
            select(PTGImportWaveIntent)
            .where(
                PTGImportWaveIntent.wave_id == operation_id,
                PTGImportWaveIntent.ordinal == request["member_ordinal"],
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if intent is None:
        raise PTGWaveOrdinaryTerminalConflict("V12 operation member is unavailable")
    quarantine = (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.predecessor_wave_id == operation_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return wave, intent, quarantine


async def _persist_projected_blank_metrics(
    session: Any,
    run: Any,
    engine_run: Any,
    engine_snapshot: Any,
) -> None:
    projected_metrics_by_name = _project_blank_metrics(run, engine_run, engine_snapshot)
    if projected_metrics_by_name is not None:
        run.metrics = projected_metrics_by_name
        await session.flush()


async def _load_outer_run(
    session: Any,
    request: Mapping[str, Any],
    intent: Any,
    *,
    lock_rows: bool = True,
) -> Any:
    statement = select(ImportRun).where(ImportRun.run_id == request["run_id"])
    if lock_rows:
        statement = statement.with_for_update().execution_options(populate_existing=True)
    run = (await session.execute(statement)).scalar_one_or_none()
    if run is None:
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal run is unavailable")
    _outer_engine_import_run_id(run, request=request, intent=intent)
    return run


async def _load_engine_result(
    session: Any,
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
    lock_rows: bool = True,
) -> tuple[Any, Any]:
    engine_import_run_id = _outer_engine_import_run_id(run, request=request, intent=intent)
    statement = select(PTG2ImportRun).where(PTG2ImportRun.import_run_id == engine_import_run_id)
    if lock_rows:
        statement = statement.with_for_update().execution_options(populate_existing=True)
    engine_run = (await session.execute(statement)).scalar_one_or_none()
    if getattr(run, "status", None) == "succeeded":
        _, snapshot_id = _outer_result_identities(run, request=request, intent=intent)
    else:
        report = getattr(engine_run, "report", None) if engine_run is not None else None
        snapshot_id = report.get("snapshot_id") if isinstance(report, Mapping) else None
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise PTGWaveOrdinaryTerminalConflict("durable PTG terminal result is unavailable")
    statement = select(PTG2Snapshot).where(PTG2Snapshot.snapshot_id == snapshot_id)
    if lock_rows:
        statement = statement.with_for_update().execution_options(populate_existing=True)
    engine_snapshot = (await session.execute(statement)).scalar_one_or_none()
    return engine_run, engine_snapshot


def _outer_engine_import_run_id(
    run: Any,
    *,
    request: Mapping[str, Any],
    intent: Any,
) -> str:
    if getattr(run, "run_id", None) != request["run_id"] or getattr(run, "run_id", None) == getattr(
        intent, "run_id", None
    ):
        raise PTGWaveOrdinaryTerminalConflict("ordinary terminal run identity is invalid")
    if getattr(run, "status", None) == "succeeded":
        return _outer_result_identities(run, request=request, intent=intent)[0]
    if (
        getattr(run, "status", None) == "failed"
        and is_allowed_amount_blank_error(getattr(run, "error", None))
        and getattr(run, "source_file_import_id", None) == request["source_file_import_id"]
        and getattr(run, "import_id", None) == request["source_file_import_id"]
    ):
        return f"ptg2:{request['source_file_import_id']}"
    raise PTGWaveOrdinaryTerminalConflict("ordinary terminal run is not a supported terminal result")


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
        source_file_import_id=str(getattr(run, "source_file_import_id", None) or ""),
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
        raise PTGWaveOrdinaryTerminalConflict("stored V12 abandonment signature is invalid") from exc


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
        lock_timeout=ORDINARY_TERMINAL_LOCK_TIMEOUT,
        statement_timeout=ORDINARY_TERMINAL_STATEMENT_TIMEOUT,
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
        or getattr(existing, "member_ordinal", None) != request["member_ordinal"]
        or getattr(existing, "source_file_import_id", None) != request["source_file_import_id"]
        or getattr(existing, "run_id", None) != request["run_id"]
        or getattr(existing, "receipt_key_id", None) != request["key_id"]
        or getattr(existing, "payload_digest", None) != receipt.get("payload_digest")
        or canonical_receipt_timestamp(getattr(existing, "issued_at", None)) != receipt.get("issued_at")
    ):
        raise PTGWaveOrdinaryTerminalConflict("stored ordinary terminal receipt identity is invalid")
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
