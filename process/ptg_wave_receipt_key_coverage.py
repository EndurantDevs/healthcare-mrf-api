"""Database-backed receipt-key coverage checks used during startup."""

from __future__ import annotations

from typing import Any


async def assert_nonterminal_receipt_key_coverage(*, keyring: Any = None) -> None:
    """Check persisted pins without importing ORM state into pure tooling."""

    from sqlalchemy import exists, select

    from db.models import (
        PTGImportWave,
        PTGImportWaveIntent,
        PTGImportWaveOrdinaryTerminalReceipt,
        PTGImportWaveQuarantine,
        db,
    )
    from process.ptg_wave_receipt_authority import require_persisted_receipt_key_coverage
    from process.ptg_wave_receipt_process_authority import (
        require_process_receipt_keyring,
    )

    terminal_states = ("succeeded", "failed", "canceled", "dead_letter")
    async with db.session() as session:
        all_pinned_rows = await _load_all_pinned_receipt_rows(
            session,
            select,
            PTGImportWave,
        )
        nonterminal_pinned_rows = await _load_nonterminal_pinned_receipt_rows(
            session,
            select,
            exists,
            PTGImportWave,
            PTGImportWaveQuarantine,
            terminal_states,
        )
        pending_ordinary_key_ids = await _load_pending_ordinary_key_ids(
            session,
            select,
            exists,
            PTGImportWave,
            PTGImportWaveIntent,
            PTGImportWaveOrdinaryTerminalReceipt,
            PTGImportWaveQuarantine,
        )
    if not all_pinned_rows:
        return
    configured = require_process_receipt_keyring(keyring)
    require_persisted_receipt_key_coverage(
        all_pinned_rows,
        [
            *(pinned_row[0] for pinned_row in nonterminal_pinned_rows),
            *pending_ordinary_key_ids,
        ],
        keyring=configured,
    )


async def _load_all_pinned_receipt_rows(
    session: Any,
    select: Any,
    wave_model: Any,
) -> list[Any]:
    rows = (
        await session.execute(
            select(
                wave_model.receipt_key_id,
                wave_model.receipt_public_modulus_hex,
                wave_model.receipt_public_exponent,
            )
            .where(wave_model.receipt_key_id.is_not(None))
            .distinct()
        )
    ).all()
    return [tuple(row) for row in rows]


async def _load_nonterminal_pinned_receipt_rows(
    session: Any,
    select: Any,
    exists: Any,
    wave_model: Any,
    quarantine_model: Any,
    terminal_states: tuple[str, ...],
) -> list[Any]:
    rows = (
        await session.execute(
            select(
                wave_model.receipt_key_id,
                wave_model.receipt_public_modulus_hex,
                wave_model.receipt_public_exponent,
            )
            .where(
                wave_model.receipt_key_id.is_not(None),
                wave_model.state.not_in(terminal_states),
                ~exists(
                    select(quarantine_model.predecessor_wave_id).where(
                        quarantine_model.predecessor_wave_id
                        == wave_model.wave_id
                    )
                ),
            )
            .distinct()
        )
    ).all()
    return [tuple(row) for row in rows]


async def _load_pending_ordinary_key_ids(
    session: Any,
    select: Any,
    exists: Any,
    wave_model: Any,
    intent_model: Any,
    receipt_model: Any,
    quarantine_model: Any,
) -> list[str]:
    return (
        await session.execute(
            select(wave_model.receipt_key_id)
            .join(
                quarantine_model,
                quarantine_model.predecessor_wave_id == wave_model.wave_id,
            )
            .where(
                quarantine_model.recovery_basis
                == "v12_pristine_materialized_cutover",
                exists(
                    select(intent_model.ordinal).where(
                        intent_model.wave_id == wave_model.wave_id,
                        ~exists(
                            select(receipt_model.member_ordinal).where(
                                receipt_model.wave_id == intent_model.wave_id,
                                receipt_model.member_ordinal
                                == intent_model.ordinal,
                            )
                        ),
                    )
                ),
            )
            .distinct()
        )
    ).scalars().all()
