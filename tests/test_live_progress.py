import datetime as dt

from process import live_progress


def test_heartbeat_preserves_old_importer_progress():
    merged = {
        "source": "engine-heartbeat",
        "unit": "run",
        "done": 0,
        "total": 1,
        "pct": 0,
        "message": "running",
        "phase": "process_data running",
        "confidence": "heartbeat",
    }
    previous = {
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
        merged,
        previous,
        now=dt.datetime.now(dt.UTC),
    )

    assert merged["source"] == "entity-address-unified-sql-progress"
    assert merged["confidence"] == "live"
    assert merged["unit"] == "shards"
    assert merged["done"] == 16
    assert merged["total"] == 64
    assert merged["pct"] == 25.0
    assert merged["message"] == "enriched 16/64 raw shards"
    assert merged["phase"] == "entity-address-unified enriching raw"
