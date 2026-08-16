import datetime as dt

from process import live_progress


def test_terminal_status_event_preserves_database_heartbeat():
    database_heartbeat = "2026-08-09T02:03:04.000000+00:00"

    event_by_field = live_progress._status_event_for_accepted_progress(
        {
            "run_id": "run_terminal",
            "status": "succeeded",
            "heartbeat_at": database_heartbeat,
            "progress": {"message": "succeeded"},
        },
        {
            "attempt_id": "run_terminal:attempt",
            "attempt_started_at": "2026-08-09T01:00:00.000000+00:00",
            "event_seq": 4,
            "progress_seq": 3,
            "observed_at": "2026-08-09T02:04:05.000000+00:00",
        },
    )

    assert event_by_field["heartbeat_at"] == database_heartbeat
    assert event_by_field["progress"]["event_seq"] == 4
    assert event_by_field["progress"]["progress_seq"] == 3


def test_heartbeat_preserves_old_importer_progress():
    merged_by_field = {
        "source": "engine-heartbeat",
        "unit": "run",
        "done": 0,
        "total": 1,
        "pct": 0,
        "message": "running",
        "phase": "process_data running",
        "confidence": "heartbeat",
    }
    previous_by_field = {
        "source": "entity-address-unified-sql-progress",
        "confidence": "live",
        "updated_at": (dt.datetime.now(dt.UTC) - dt.timedelta(minutes=10)).isoformat(),
        "unit": "shards",
        "done": 16,
        "total": 64,
        "pct": 25.0,
        "message": "enriched 16/64 raw shards",
        "phase": "entity-address-unified enriching raw",
    }

    live_progress._preserve_progress_for_heartbeat(
        merged_by_field,
        previous_by_field,
        now=dt.datetime.now(dt.UTC),
    )

    assert merged_by_field["source"] == "entity-address-unified-sql-progress"
    assert merged_by_field["confidence"] == "live"
    assert merged_by_field["unit"] == "shards"
    assert merged_by_field["done"] == 16
    assert merged_by_field["total"] == 64
    assert merged_by_field["pct"] == 25.0
    assert merged_by_field["message"] == "enriched 16/64 raw shards"
    assert merged_by_field["phase"] == "entity-address-unified enriching raw"


def test_alias_shard_progress_replaces_phase_without_regressing_outer_pct():
    previous_by_field = {
        "source": "entity-address-unified-sql-progress",
        "unit": "sources",
        "done": 257,
        "total": 258,
        "pct": 99.6,
        "phase": "entity-address-unified loading sources",
        "message": "loaded 257/258 sources",
    }
    incoming_by_field = {
        "source": "entity-address-unified-sql-progress",
        "unit": "shards",
        "done": 0,
        "total": 64,
        "stage_id": "entity-address-unified-alias-integrity",
        "stage_pct": 0.0,
        "phase": "entity-address-unified validating aliases",
        "message": "validating 64 alias-integrity shards",
    }

    live_progress._merge_previous_progress(
        incoming_by_field,
        previous_by_field,
        now=dt.datetime.now(dt.UTC),
    )

    assert incoming_by_field["phase"] == "entity-address-unified validating aliases"
    assert incoming_by_field["done"] == 0
    assert incoming_by_field["total"] == 64
    assert incoming_by_field["pct"] == 99.6


def test_alias_shard_progress_rejects_delayed_completion():
    previous_by_field = {
        "source": "entity-address-unified-sql-progress",
        "unit": "shards",
        "done": 2,
        "total": 4,
        "pct": 99.6,
        "stage_id": "entity-address-unified-alias-integrity",
        "stage_pct": 50.0,
        "elapsed_seconds": 10.0,
        "eta_seconds": 10.0,
        "phase": "entity-address-unified validating aliases",
        "message": "validated 2/4 alias-integrity shards",
    }
    delayed_by_field = {
        "source": "entity-address-unified-sql-progress",
        "unit": "shards",
        "done": 1,
        "total": 4,
        "stage_id": "entity-address-unified-alias-integrity",
        "stage_pct": 25.0,
        "elapsed_seconds": 6.0,
        "eta_seconds": 18.0,
        "phase": "entity-address-unified validating aliases",
        "message": "validated 1/4 alias-integrity shards",
    }

    live_progress._merge_previous_progress(
        delayed_by_field,
        previous_by_field,
        now=dt.datetime.now(dt.UTC),
    )

    assert delayed_by_field["done"] == 2
    assert delayed_by_field["stage_pct"] == 50.0
    assert delayed_by_field["elapsed_seconds"] == 10.0
    assert delayed_by_field["eta_seconds"] == 10.0


def test_nested_profile_progress_replaces_outer_publish_snapshot():
    """Accept exact inner batch state inside the monotonic outer envelope."""
    previous_by_field = {
        "run_id": "run-profile",
        "source": "provider-directory-sql-progress",
        "unit": "steps",
        "done": 4,
        "total": 7,
        "pct": 57.14,
        "phase": "provider-directory publishing artifacts",
        "message": "published address artifacts",
    }
    incoming_by_field = {
        "run_id": "run-profile",
        "source": "provider-directory-sql-progress",
        "unit": "batches",
        "done": 1,
        "total": 2_185,
        "pct": 57.15,
        "phase": "provider-directory profile evidence batches",
        "message": "1 of 2185 complete",
    }

    disposition = live_progress._merge_previous_progress(
        incoming_by_field,
        previous_by_field,
        now=dt.datetime.now(dt.UTC),
    )

    assert disposition == live_progress._ATTEMPT_CURRENT
    assert incoming_by_field["done"] == 1
    assert incoming_by_field["total"] == 2_185
    assert incoming_by_field["pct"] == 57.15
    assert incoming_by_field["phase"] == (
        "provider-directory profile evidence batches"
    )


def _nested_profile_snapshots():
    return (
        {
            "unit": "steps",
            "done": 4,
            "total": 7,
            "pct": 57.14,
            "phase": "provider-directory publishing artifacts",
        },
        {
            "unit": "batches",
            "done": 0,
            "total": 2_185,
            "pct": 57.14,
            "phase": "provider-directory profile evidence batches",
        },
        {
            "unit": "batches",
            "done": 2_185,
            "total": 2_185,
            "pct": 69.22,
            "phase": "provider-directory profile evidence batches",
        },
        {
            "unit": "batches",
            "done": 0,
            "total": 400,
            "pct": 69.22,
            "phase": "provider-directory profile compact NPI batches",
        },
        {
            "unit": "batches",
            "done": 400,
            "total": 400,
            "pct": 71.43,
            "phase": "provider-directory profile compact NPI batches",
        },
        {
            "unit": "steps",
            "done": 5,
            "total": 7,
            "pct": 71.43,
            "phase": "provider-directory publishing artifacts",
        },
    )


def _merge_nested_profile_snapshots(run_id, snapshots):
    now = dt.datetime.now(dt.UTC)
    observed_at = now.isoformat()
    previous = None
    merged_snapshots = []
    for snapshot in snapshots:
        candidate = live_progress._merged_live_progress_candidate(
            run_id=run_id,
            context={},
            progress_by_field=snapshot,
            observed_at=observed_at,
            now=now,
            previous=previous,
        )
        assert candidate is not None
        merged_snapshots.append(candidate)
        previous = candidate
    return merged_snapshots


def test_nested_profile_progress_advances_through_live_candidate_merge():
    """Keep outer, evidence, compact, and resumed outer snapshots ordered."""
    run_id = "run-nested-profile-candidate"
    live_progress._reset_attempt_sequences(run_id)
    snapshots = _nested_profile_snapshots()
    merged_snapshots = _merge_nested_profile_snapshots(run_id, snapshots)

    assert [snapshot["phase"] for snapshot in merged_snapshots] == [
        snapshot["phase"] for snapshot in snapshots
    ]
    assert [snapshot["done"] for snapshot in merged_snapshots] == [
        4,
        0,
        2_185,
        0,
        400,
        5,
    ]
    progress_sequences = [
        int(snapshot["progress_seq"])
        for snapshot in merged_snapshots
    ]
    assert progress_sequences == sorted(set(progress_sequences))
