"""Progress reflects consumed rows and completed writes, never mere time."""

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


def test_normalization_reports_start_and_throttled_consumed_rows(monkeypatch):
    ticks = iter([0.0, 5.0, 10.0, 11.0, 20.0])
    monkeypatch.setattr(progress.time, "monotonic", lambda: next(ticks))
    events = []
    assert list(_rows(["one", "two", "three", "four"], events)) == [
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


def test_unfinished_row_does_not_report_progress(monkeypatch):
    monkeypatch.setattr(progress.time, "monotonic", lambda: 1000.0)
    events = []
    rows = _rows(["one", "two"], events)
    assert next(rows) == "one"
    rows.close()
    assert len(events) == 1
    assert events[0]["counters"] == {"file_rows_processed": 0}


def test_source_read_failure_is_not_hidden_or_reported_as_completion(monkeypatch):
    monkeypatch.setattr(progress.time, "monotonic", lambda: 0.0)

    def broken_source():
        yield "one"
        raise OSError("source read failed")

    events = []
    with pytest.raises(OSError, match="source read failed"):
        list(_rows(broken_source(), events))
    assert len(events) == 1
    assert events[0]["pct"] < 100
