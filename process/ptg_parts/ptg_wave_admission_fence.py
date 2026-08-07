"""PTG wave capacity and controller-ownership fences.

Provider-directory work is deliberately absent from these predicates.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text

from db.models import ImportRun, PTGImportWave, PTGImportWaveIntent


PTG_ADMISSION_LOCK_NAME = "import-run-admission:ptg-source-file"
PTG_WAVE_FENCED_IMPORTERS = frozenset({"ptg", "ptg-candidate-audit"})
PTG_ACTIVE_RUN_STATES = frozenset({"queued", "starting", "running", "finalizing", "canceling"})
PTG_WAVE_CAPACITY_OWNING_STATES = frozenset({
    "admitted", "materializing", "slots_waiting", "redis_releasing",
    "released", "executing", "awaiting_linkage", "terminalizing", "cleaning", "uncertain",
})
PTG_WAVE_TERMINAL_STATES = frozenset({"succeeded", "failed", "canceled", "dead_letter"})


class PTGWaveCapacityConflict(RuntimeError):
    """PTG capacity is owned by a wave or incompatible active work."""


class PTGWaveOwnershipConflict(RuntimeError):
    """Only the exact-wave controller may mutate a wave-owned run."""


async def _all(executor: Any, statement: Any) -> list[Any]:
    all_rows = getattr(executor, "all", None)
    if all_rows is not None:
        return list(await all_rows(statement))
    return list((await executor.execute(statement)).all())


async def _scalar(executor: Any, statement: Any, parameters: dict[str, Any]) -> Any:
    scalar = getattr(executor, "scalar", None)
    if scalar is not None:
        try:
            return await scalar(statement, dict(parameters))
        except TypeError:
            return await scalar(statement, **dict(parameters))
    return (await executor.execute(statement, dict(parameters))).scalar()


async def acquire_ptg_admission_lock(executor: Any) -> None:
    """Serialize PTG source admission without acquiring any FHIR lock."""

    await _scalar(executor, text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_name, 0))"),
                  {"lock_name": PTG_ADMISSION_LOCK_NAME})


async def _capacity_owning_waves(executor: Any) -> list[Any]:
    # A database without this migration cannot contain an admitted wave.  This
    # keeps legacy startup/readiness tests compatible while deployed wave
    # admission remains impossible until the migration is ready.
    if not hasattr(executor, "all") and not hasattr(executor, "execute"):
        return []
    if not await _has_wave_table(executor, "ptg_import_wave"):
        return []
    return await _all(executor, select(PTGImportWave.wave_id, PTGImportWave.state)
                      .where(PTGImportWave.state.in_(PTG_WAVE_CAPACITY_OWNING_STATES))
                      .order_by(PTGImportWave.created_at, PTGImportWave.wave_id).limit(2))


async def _has_wave_table(executor: Any, table: str) -> bool:
    schema = PTGImportWave.__table__.schema or "mrf"
    relation = f"{schema}.{table}"
    return bool(await _scalar(executor, text("SELECT to_regclass(:relation)"), {"relation": relation}))


async def require_no_capacity_owning_wave(executor: Any, *, owner_run_id: str | None = None) -> None:
    """Reject non-wave PTG admission while an exact wave owns capacity."""

    active = await _capacity_owning_waves(executor)
    if not active:
        return
    if len(active) != 1:
        raise PTGWaveCapacityConflict("PTG wave capacity ownership is ambiguous")
    if owner_run_id:
        owner = await _all(executor, select(PTGImportWaveIntent.run_id).where(
            PTGImportWaveIntent.wave_id == str(active[0][0]),
            PTGImportWaveIntent.run_id == owner_run_id).limit(1))
        if owner:
            return
    raise PTGWaveCapacityConflict("PTG wave capacity is reserved")


async def require_wave_admission_capacity(executor: Any) -> None:
    """Require an idle PTG lane before admitting one complete exact wave."""

    if await _capacity_owning_waves(executor):
        raise PTGWaveCapacityConflict("PTG wave capacity is already reserved")
    active = await _all(executor, select(ImportRun.run_id).where(
        ImportRun.importer.in_(PTG_WAVE_FENCED_IMPORTERS),
        ImportRun.status.in_(PTG_ACTIVE_RUN_STATES)).order_by(ImportRun.created_at, ImportRun.run_id).limit(1))
    if active:
        raise PTGWaveCapacityConflict("active PTG work prevents wave admission")


async def require_not_wave_owned_run(executor: Any, run_id: str) -> None:
    """Reject direct lifecycle mutation before it reaches Redis or run-state DML."""

    if await is_ptg_wave_owned_run(executor, run_id):
        raise PTGWaveOwnershipConflict("wave-owned import run is controller-managed")


async def is_ptg_wave_owned_run(executor: Any, run_id: str) -> bool:
    """Read durable controller ownership, tolerating pre-wave schemas."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return False
    if not hasattr(executor, "all") and not hasattr(executor, "execute"):
        return False
    if not await _has_wave_table(executor, "ptg_import_wave_intent"):
        return False
    row = await _all(executor, select(PTGImportWaveIntent.wave_id).where(
        PTGImportWaveIntent.run_id == normalized_run_id).limit(1))
    return bool(row)


__all__ = [
    "PTG_ACTIVE_RUN_STATES", "PTG_ADMISSION_LOCK_NAME", "PTG_WAVE_CAPACITY_OWNING_STATES",
    "PTG_WAVE_FENCED_IMPORTERS", "PTG_WAVE_TERMINAL_STATES", "PTGWaveCapacityConflict",
    "PTGWaveOwnershipConflict", "acquire_ptg_admission_lock", "require_no_capacity_owning_wave",
    "is_ptg_wave_owned_run", "require_not_wave_owned_run", "require_wave_admission_capacity",
]
