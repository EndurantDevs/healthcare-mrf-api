"""Synthetic state for legacy-V3 coverage contract tests."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile
from process.ptg_parts import ptg2_legacy_v3_metadata_store as store


SNAPSHOT_ID = "ptg2:202607:coverage"
INTERNAL_RUN_ID = "ptg2:coverage-run"
OUTER_RUN_ID = "run_coverage"
SOURCE_IMPORT_ID = "source-import-coverage"
DIGEST = "a" * 64


class QueryResult:
    """Minimal SQLAlchemy result used by legacy coverage tests."""

    def __init__(self, *, scalar=None, rowcount: int = 1) -> None:
        self._scalar = scalar
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar


class Session:
    """Queue deterministic SQL results and retain call evidence."""

    def __init__(self, *responses: QueryResult) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict | None]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if self.responses:
            return self.responses.pop(0)
        return QueryResult()


class Transaction:
    """Async context adapter for one synthetic session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def database(session: Session):
    """Return a transaction-only database facade."""
    return SimpleNamespace(transaction=lambda: Transaction(session))


def coordinates() -> reconcile._ReconcileCoordinates:
    """Return the fixed reconciliation coordinates."""
    return reconcile._ReconcileCoordinates(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )


def ready_plan() -> dict[str, object]:
    """Return one exact ready reconciliation plan."""
    return {
        "status": "ready",
        "reason_codes": [],
        "target_digest": "b" * 64,
        "plan_digest": DIGEST,
        "attachment_digest": "c" * 64,
        "catalog_digest": "d" * 64,
        "event_high_water_mark": "7",
        "retained_state_digest": "e" * 64,
        "preserved_row_digest": "f" * 64,
    }


def reconcile_write() -> store.LegacyV3ReconcileWrite:
    """Return one exact legacy reconciliation write."""
    return store.LegacyV3ReconcileWrite(
        schema_name="mrf",
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        source_file_import_id=SOURCE_IMPORT_ID,
        outer_run_id=OUTER_RUN_ID,
        target_digest="b" * 64,
        plan_digest=DIGEST,
        attachment_digest="c" * 64,
        catalog_digest="d" * 64,
        event_high_water_mark=7,
        reconciliation_id="e" * 64,
        marker={"observed_at": dt.datetime(2026, 8, 1, tzinfo=dt.UTC)},
    )
