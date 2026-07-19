import datetime as dt

from process import live_progress


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
