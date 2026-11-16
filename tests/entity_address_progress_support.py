# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Late publication progress through Redis and guarded control persistence."""

from __future__ import annotations

import asyncio
import json

import pytest

from process import control_lifecycle as lifecycle
from process import live_progress
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis
from tests.test_entity_address_unified_publication_db import _temporary_schema

_ATTEMPT_STARTED = "2026-06-01T00:00:00Z"


def progress_context(monkeypatch):
    monkeypatch.setattr(live_progress, "_redis", lambda: redis)
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda *_args, **_kwargs: None)
    redis = AtomicLiveProgressRedis()
    return live_progress.set_live_progress_context(
        run_id="publication-parent",
        attempt_id="publication-attempt",
        attempt_started_at=_ATTEMPT_STARTED,
        importer="entity-address-unified",
        stage_id="publication",
        stage_ordinal=5,
    )


async def persist_progress(database, schema, *, attempt_id="publication-attempt"):
    assert await lifecycle._is_control_run_heartbeat_persisted(
        "publication-parent",
        "shutdown",
        attempt_id=attempt_id,
        attempt_started_at=_ATTEMPT_STARTED,
    ) == (attempt_id == "publication-attempt")
    row = await database.first(f"SELECT phase_detail, progress FROM {schema}.import_run")
    return row[0], row[1]


async def assert_late_progress_persists(monkeypatch, native):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(lifecycle, "db", database)
        monkeypatch.setattr(lifecycle.ImportRun.__table__, "schema", schema)
        await database.status(f"""CREATE TABLE {schema}.import_run (
            run_id text PRIMARY KEY, importer text, status text, phase_detail text,
            heartbeat_at timestamp, progress json, metrics json, finished_at timestamp)""")
        initial_progress_by_field = {"attempt_id": "publication-attempt", "attempt_started_at": _ATTEMPT_STARTED}
        await database.status(
            f"INSERT INTO {schema}.import_run VALUES "
            "('publication-parent', 'entity-address-unified', 'running', 'aggregating', NULL, "
            "CAST(:progress AS json), '{}'::json, NULL)",
            progress=json.dumps(initial_progress_by_field),
        )
        token = progress_context(monkeypatch)
        try:
            live_progress.write_live_progress(
                phase="aggregating", done=32, total=32, unit="shards", publish_event=False
            )
            await native._emit_geo_assurance_progress("publication-parent", 100)
            phase, stored = await persist_progress(database, schema)
            assert phase.endswith("projecting geo assurance")
            assert (stored["done"], stored["total"], stored["phase_pct"]) == (0, 1, 0)
            assert "pct" not in stored and stored["stage_ordinal"] == 5
            await native._emit_geo_assurance_progress("publication-parent", 100, projected_rows=100, elapsed=1)
            live_progress.write_live_progress(phase="aggregating", done=32, total=32, pct=99.9, publish_event=False)
            phase, stored = await persist_progress(database, schema)
            assert phase.endswith("projecting geo assurance") and stored["phase_pct"] == 100
            before = stored.copy()
            live_progress.write_live_progress(
                attempt_id="obsolete-attempt",
                attempt_started_at="2026-05-01T00:00:00Z",
                phase="obsolete",
                basis="phase",
                done=0,
                total=1,
                publish_event=False,
            )
            assert (await persist_progress(database, schema))[1] == before
            assert (await persist_progress(database, schema, attempt_id="obsolete-attempt"))[1] == before
            for next_phase, unit, total in (
                ("durability", "operations", 1),
                ("indexing", "indexes", 24),
                ("validation", "operations", 1),
            ):
                for done in (0, total):
                    await native._emit_phase_progress(
                        "publication-parent", next_phase, unit=unit, done=done, total=total, message=next_phase
                    )
                    phase, stored = await persist_progress(database, schema)
                    assert phase == next_phase and (stored["done"], stored["total"]) == (done, total)
                    assert "pct" not in stored
            live_progress.write_live_progress(status="succeeded", phase="published", publish_event=False)
            assert live_progress.read_live_progress("publication-parent")["pct"] == 100
        finally:
            live_progress.reset_live_progress_context(token)


async def assert_index_progress_counts(monkeypatch, native):
    token = progress_context(monkeypatch)
    progress_snapshots = []
    original_write = live_progress.write_live_progress

    def write(**payload):
        is_written = original_write(**payload)
        progress_snapshots.append(live_progress.read_live_progress("publication-parent").copy())
        return is_written

    async def index_statement(_statement, **_kwargs):
        await asyncio.sleep(0)

    monkeypatch.setattr(live_progress, "write_live_progress", write)
    monkeypatch.setattr(native, "_run_sql_phase", index_statement)
    try:
        await native._run_stage_index_statements(
            [(f"index_{number}", "SELECT 1") for number in range(24)],
            {"control_run_id": "publication-parent", "finalizing_phase_progress": True},
            4,
            [],
        )
        counts = [progress_snapshot["done"] for progress_snapshot in progress_snapshots]
        assert len(progress_snapshots) == 48 and counts == sorted(counts)
        assert counts[0] == 0 and counts[-1] == 24
        assert all(
            progress_snapshot["total"] == 24 and "pct" not in progress_snapshot
            for progress_snapshot in progress_snapshots
        )
        assert all(
            "eta_seconds" not in progress_snapshot and "estimated_finish_at" not in progress_snapshot
            for progress_snapshot in progress_snapshots
        )
        assert all(progress_snapshot["attempt_id"] == "publication-attempt" for progress_snapshot in progress_snapshots)
        assert_phase_ordering()
        live_progress.write_live_progress(phase="next stage", stage_ordinal=6, done=1, total=2, pct=50)
        next_stage = live_progress.read_live_progress("publication-parent")
        assert next_stage["phase"] == "next stage" and next_stage["pct"] == 50
        assert "basis" not in next_stage
    finally:
        live_progress.reset_live_progress_context(token)


async def assert_failed_phase_stays_incomplete(monkeypatch, native):
    token = progress_context(monkeypatch)

    async def fail(*_args, **_kwargs):
        raise RuntimeError("work failed")

    monkeypatch.setattr(native, "_run_sql_phase", fail)
    monkeypatch.setattr(native, "_validate_publish_integrity", fail)
    try:
        with pytest.raises(RuntimeError, match="work failed"):
            await native._run_stage_index_statements(
                [("index_a", "SELECT 1")],
                {"control_run_id": "publication-parent", "finalizing_phase_progress": True},
                1,
                [],
            )
        assert live_progress.read_live_progress("publication-parent")["done"] == 0
        with pytest.raises(RuntimeError, match="work failed"):
            await native._run_publish_validation("fixture", "stage", {}, {}, "publication-parent")
        current = live_progress.read_live_progress("publication-parent")
        assert current["phase"].endswith("validating publication") and current["phase_pct"] == 0
        assert "pct" not in current
    finally:
        live_progress.reset_live_progress_context(token)


def assert_phase_ordering():
    previous = live_progress.read_live_progress("publication-parent")
    for guards in ({"stage_ordinal": 4}, {"progress_seq": previous["progress_seq"] - 1}):
        live_progress.write_live_progress(phase="obsolete phase", basis="phase", phase_pct=0, **guards)
        assert live_progress.read_live_progress("publication-parent")["phase"] == previous["phase"]
    live_progress.write_live_progress(phase=previous["phase"], basis="phase", phase_pct=0, done=0)
    assert live_progress.read_live_progress("publication-parent")["done"] == 24


async def assert_snapshot_publication_progress(monkeypatch, native, *, is_failed):
    token = progress_context(monkeypatch)
    context_by_field = {"staged_rows": 7}

    async def publish():
        async with native._snapshot_publication_progress(context_by_field, "publication-parent"):
            progress = live_progress.read_live_progress("publication-parent")
            assert progress["phase"].endswith("preparing snapshot publication")
            assert (progress["done"], progress["total"], progress["phase_pct"]) == (0, 1, 0)
            if is_failed:
                raise RuntimeError("snapshot validation refused")
            return "prepared"

    try:
        if is_failed:
            with pytest.raises(RuntimeError, match="snapshot validation refused"):
                await publish()
        else:
            assert await publish() == "prepared"
        progress = live_progress.read_live_progress("publication-parent")
        assert progress["done"] == int(not is_failed) and progress["total"] == 1
        assert "pct" not in progress and "eta_seconds" not in progress
        timings = context_by_field.get("phase_timings", {}).get(progress["phase"])
        assert (timings is None) is is_failed
        if timings is not None:
            assert timings["rows"] == 7 and timings["count"] == 1
    finally:
        live_progress.reset_live_progress_context(token)
