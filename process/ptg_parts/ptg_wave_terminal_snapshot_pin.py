# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retention pin spanning ordinary PTG completion through receipt commit."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_wave_receipt_contract import ordinary_cutover_id


ORDINARY_TERMINAL_PIN_OWNER_TYPE = "ptg-wave-ordinary-terminal"
ORDINARY_TERMINAL_PIN_REASON = (
    "retain snapshot manifest through ordinary terminal receipt"
)
_OPERATION_ID_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class PTGWaveOrdinaryTerminalPin:
    operation_id: str
    member_ordinal: int
    owner_id: str
    snapshot_id: str


async def _locked_terminal_pin_rows(
    session: Any,
    *,
    schema: str,
    parameters_by_name: Mapping[str, Any],
) -> list[dict[str, Any]]:
    stored_records = (
        await session.execute(
            text(
                f"""
                SELECT snapshot_id, reason
                  FROM {schema}.ptg2_snapshot_pin
                 WHERE owner_type = :owner_type
                   AND owner_id = :owner_id
                 ORDER BY snapshot_id
                 FOR UPDATE
                """
            ),
            parameters_by_name,
        )
    ).all()
    return [
        dict(getattr(pin_record, "_mapping", pin_record))
        for pin_record in stored_records
    ]


def ordinary_terminal_pin(
    options: Mapping[str, Any],
    *,
    snapshot_id: str,
) -> PTGWaveOrdinaryTerminalPin | None:
    """Derive a pin only for a complete, exact ordinary-cutover identity."""

    has_operation_id = "ordinary_cutover_operation_id" in options
    has_member_ordinal = "ordinary_cutover_member_ordinal" in options
    if not has_operation_id and not has_member_ordinal:
        return None
    if not has_operation_id or not has_member_ordinal:
        raise ValueError("ordinary terminal pin identity is incomplete")
    operation_id = str(options.get("ordinary_cutover_operation_id") or "")
    member_ordinal = options.get("ordinary_cutover_member_ordinal")
    if not _OPERATION_ID_RE.fullmatch(operation_id):
        raise ValueError("ordinary terminal pin operation identity is invalid")
    if (
        isinstance(member_ordinal, bool)
        or not isinstance(member_ordinal, int)
        or member_ordinal < 0
    ):
        raise ValueError("ordinary terminal pin member ordinal is invalid")
    if options.get("ordinary_cutover_id") != ordinary_cutover_id(operation_id):
        raise ValueError("ordinary terminal pin cutover identity is invalid")
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id or len(normalized_snapshot_id) > 128:
        raise ValueError("ordinary terminal pin snapshot identity is invalid")
    owner_id = f"{operation_id}:{member_ordinal}"
    if len(owner_id) > 96:
        raise ValueError("ordinary terminal pin owner identity is invalid")
    return PTGWaveOrdinaryTerminalPin(
        operation_id=operation_id,
        member_ordinal=member_ordinal,
        owner_id=owner_id,
        snapshot_id=normalized_snapshot_id,
    )


async def insert_ordinary_terminal_snapshot_pin(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str,
    options: Mapping[str, Any],
) -> PTGWaveOrdinaryTerminalPin | None:
    """Insert or validate the exact pin in the terminal run transaction."""

    pin = ordinary_terminal_pin(options, snapshot_id=snapshot_id)
    if pin is None:
        return None
    schema = _quote_ident(schema_name)
    pin_parameters_by_name = {
        "owner_type": ORDINARY_TERMINAL_PIN_OWNER_TYPE,
        "owner_id": pin.owner_id,
        "snapshot_id": pin.snapshot_id,
        "internal_run_id": str(internal_run_id),
        "reason": ORDINARY_TERMINAL_PIN_REASON,
    }
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_snapshot_pin
                (owner_type, owner_id, snapshot_id, reason, created_at)
            SELECT :owner_type, :owner_id, snapshot.snapshot_id,
                   :reason, transaction_timestamp()
              FROM {schema}.ptg2_snapshot AS snapshot
             WHERE snapshot.snapshot_id = :snapshot_id
               AND snapshot.import_run_id = :internal_run_id
            ON CONFLICT (owner_type, owner_id, snapshot_id) DO NOTHING
            """
        ),
        pin_parameters_by_name,
    )
    stored_rows = await _locked_terminal_pin_rows(
        session,
        schema=schema,
        parameters_by_name=pin_parameters_by_name,
    )
    if stored_rows != [
        {
            "snapshot_id": pin.snapshot_id,
            "reason": ORDINARY_TERMINAL_PIN_REASON,
        }
    ]:
        raise RuntimeError("ordinary terminal snapshot pin conflicts with its member")
    return pin


async def delete_ordinary_terminal_snapshot_pin(
    session: Any,
    *,
    schema_name: str,
    operation_id: str,
    member_ordinal: int,
    snapshot_id: str,
) -> int:
    """Delete only the pin authenticated by one receipt payload."""

    pin = ordinary_terminal_pin(
        {
            "ordinary_cutover_operation_id": operation_id,
            "ordinary_cutover_member_ordinal": member_ordinal,
            "ordinary_cutover_id": ordinary_cutover_id(operation_id),
        },
        snapshot_id=snapshot_id,
    )
    assert pin is not None
    deletion_result = await session.execute(
        text(
            f"""
            DELETE FROM {_quote_ident(schema_name)}.ptg2_snapshot_pin
             WHERE owner_type = :owner_type
               AND owner_id = :owner_id
               AND snapshot_id = :snapshot_id
            RETURNING snapshot_id
            """
        ),
        {
            "owner_type": ORDINARY_TERMINAL_PIN_OWNER_TYPE,
            "owner_id": pin.owner_id,
            "snapshot_id": pin.snapshot_id,
        },
    )
    if hasattr(deletion_result, "all"):
        return len(deletion_result.all())
    rowcount = getattr(deletion_result, "rowcount", None)
    return int(rowcount or 0)


__all__ = [
    "ORDINARY_TERMINAL_PIN_OWNER_TYPE",
    "ORDINARY_TERMINAL_PIN_REASON",
    "PTGWaveOrdinaryTerminalPin",
    "delete_ordinary_terminal_snapshot_pin",
    "insert_ordinary_terminal_snapshot_pin",
    "ordinary_terminal_pin",
]
