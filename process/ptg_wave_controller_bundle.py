"""Durable selection of the sole controller-owned exact PTG wave."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import exists, select

from db.models import (
    PTGImportWave,
    PTGImportWaveIntent,
    PTGImportWaveQuarantine,
    db,
)
from process.ptg_parts.ptg_wave_admission_fence import (
    PTG_WAVE_CAPACITY_OWNING_STATES,
)
from process.ptg_wave_state import PTGWaveStateConflict


@dataclass(frozen=True)
class PTGWaveBundle:
    """One durable wave with its complete ordered intent set."""

    wave: PTGImportWave
    intents: tuple[PTGImportWaveIntent, ...]


async def load_capacity_owning_wave() -> PTGWaveBundle | None:
    """Load the sole non-quarantined capacity owner and all its intents."""

    async with db.session() as session:
        waves = (await session.execute(
            select(PTGImportWave)
            .where(
                PTGImportWave.state.in_(PTG_WAVE_CAPACITY_OWNING_STATES),
                ~exists(
                    select(PTGImportWaveQuarantine.predecessor_wave_id).where(
                        PTGImportWaveQuarantine.predecessor_wave_id
                        == PTGImportWave.wave_id
                    )
                ),
            )
            .order_by(PTGImportWave.created_at, PTGImportWave.wave_id)
            .limit(2)
        )).scalars().all()
        if not waves:
            return None
        if len(waves) != 1:
            raise PTGWaveStateConflict(
                "PTG wave capacity ownership is ambiguous"
            )
        wave = waves[0]
        intents = tuple((await session.execute(
            select(PTGImportWaveIntent)
            .where(PTGImportWaveIntent.wave_id == wave.wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
        )).scalars().all())
    expected_ordinals = list(range(wave.intent_count))
    if (
        len(intents) != wave.intent_count
        or [intent.ordinal for intent in intents] != expected_ordinals
    ):
        raise PTGWaveStateConflict(
            "persisted exact-wave intents are incomplete"
        )
    return PTGWaveBundle(wave=wave, intents=intents)


__all__ = ["PTGWaveBundle", "load_capacity_owning_wave"]
