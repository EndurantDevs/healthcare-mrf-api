"""Progress reflects consumed rows and completed writes, never mere time."""

import asyncio

import pytest

from process import provider_profile_live_progress as progress


def _rows(values, events):
    return progress.normalization_rows(
        values,
        title="Example source",
        file_name="example.txt",
        file_index=2,
        file_count=4,
        report=lambda **event: events.append(event),
    )


@pytest.mark.asyncio
async def test_normalization_reports_start_and_throttled_consumed_rows(monkeypatch):
    ticks = iter([0.0, 5.0, 10.0, 11.0, 20.0])
    monkeypatch.setattr(progress, "monotonic", lambda: next(ticks))
    events = []
    assert [row async for row in _rows(["one", "two", "three", "four"], events)] == [
        "one",
        "two",
        "three",
        "four",
    ]
    assert [event["counters"]["file_rows_processed"] for event in events] == [0, 2, 4]
    assert all(event["pct"] == 48 for event in events)
    assert all(event["phase"] == "normalizing" for event in events)
    assert all(event["file_index"] == 2 for event in events)
    assert events[0]["message"] == "Normalizing Example source"


@pytest.mark.asyncio
async def test_unfinished_row_does_not_report_progress(monkeypatch):
    monkeypatch.setattr(progress, "monotonic", lambda: 1000.0)
    events = []
    rows = _rows(["one", "two"], events)
    assert await anext(rows) == "one"
    await rows.aclose()
    assert len(events) == 1
    assert events[0]["counters"] == {"file_rows_processed": 0}


@pytest.mark.asyncio
async def test_source_read_failure_is_not_hidden_or_reported_as_completion(monkeypatch):
    monkeypatch.setattr(progress, "monotonic", lambda: 0.0)

    def broken_source():
        yield "one"
        raise OSError("source read failed")

    events = []
    with pytest.raises(OSError, match="source read failed"):
        async for row in _rows(broken_source(), events):
            assert row == "one"
    assert len(events) == 1
    assert events[0]["pct"] < 100


@pytest.mark.asyncio
async def test_filtered_scan_runs_queued_progress_before_scan_finishes(monkeypatch):
    ticks = iter([0.0, 10.0, 11.0])
    monkeypatch.setattr(progress, "monotonic", lambda: next(ticks))
    published_counts = []

    async def publish(event):
        published_counts.append(event["counters"]["file_rows_processed"])

    def enqueue(**event):
        asyncio.create_task(publish(event))

    rows = progress.normalization_rows(
        ["filtered-one", "filtered-two"],
        title="Example source",
        file_name="example.txt",
        file_index=1,
        file_count=1,
        report=enqueue,
    )
    async for row in rows:
        if row == "filtered-one":
            assert published_counts == [0]
            continue
        assert published_counts == [0, 1]
        continue
    assert published_counts == [0, 1]
