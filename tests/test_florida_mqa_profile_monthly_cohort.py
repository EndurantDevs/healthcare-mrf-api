from datetime import UTC, datetime, timedelta

import pytest

from tests.test_florida_mqa_profile_publication_contracts import (
    _one_projection_row,
    _PublicationDb,
    _Row,
    _source_metrics,
    florida,
)


def _cohort_metrics(rows):
    return {
        "source_records": 1_000 + rows,
        "selected_sources": ["profile_master", "pharmacy_pharmacist"],
        "source_metrics": {
            **_source_metrics(1_000),
            "pharmacy_pharmacist": _source_metrics(rows)["profile_master"],
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("defect", "reason"),
    [
        (None, None),
        ("snapshot_drop", "source_rows_ratio:profile_master"),
        ("missing_source", "source_metrics_missing:pharmacy_pharmacist"),
        ("schema", "source_schema_incomplete:pharmacy_pharmacist"),
        ("missing_header", "source_header_hash_missing:pharmacy_pharmacist"),
        ("header_drift", "source_header_sha256_changed:pharmacy_pharmacist"),
        ("quarantine", "source_quarantine_ratio_exceeded:pharmacy_pharmacist"),
        ("empty", "source_rows_empty:pharmacy_pharmacist"),
        ("provider_drop", "provider_count_ratio"),
        ("overall_source_drop", "source_record_count_ratio"),
    ],
)
async def test_monthly_cohort_publication_preserves_other_guards(monkeypatch, defect, reason):
    started_at = datetime(2026, 1, 2, tzinfo=UTC)
    metrics = _cohort_metrics(2)
    pharmacy = metrics["source_metrics"]["pharmacy_pharmacist"]
    match defect:
        case "snapshot_drop":
            metrics["source_metrics"]["profile_master"] = _source_metrics(100)["profile_master"]
        case "missing_source":
            del metrics["source_metrics"]["pharmacy_pharmacist"]
        case "schema":
            pharmacy["schema_complete"] = False
        case "missing_header":
            del pharmacy["header_sha256"]
        case "header_drift":
            pharmacy["header_sha256"] = "b" * 64
        case "quarantine":
            pharmacy["quarantined_rows"] = 1
        case "empty":
            pharmacy["rows"] = 0
        case "overall_source_drop":
            metrics["source_records"] = 100
    database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[[_Row(generation_id="b" * 32, provider_count=10 if defect == "provider_drop" else 1)], []],
        current_run=_Row(started_at=started_at - timedelta(days=1), metrics=_cohort_metrics(100)),
    )
    monkeypatch.setattr(florida, "db", database)
    monkeypatch.setattr(florida, "enqueue_live_progress", lambda **_event: None)
    publication = florida._publish_projection_swap(
        "a" * 32,
        _one_projection_row("a" * 32),
        started_at=started_at,
        completion_metrics=metrics,
        allow_volume_drop=False,
        min_first_publish_providers=1,
        min_publish_ratio=0.8,
    )
    if reason:
        with pytest.raises(RuntimeError, match=reason):
            await publication
        assert not any("RENAME TO" in statement for statement in database.status_calls)
    else:
        receipt, _ = await publication
        assert receipt["source_guard"]["ratio_reasons"] == []
        assert receipt["volume_guard"]["allow_volume_drop"] is False
        assert any("RENAME TO" in statement for statement in database.status_calls)
