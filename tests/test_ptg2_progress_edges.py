from __future__ import annotations

import types

import pytest

from process.ptg_parts import progress


class _TellStream:
    def __init__(self, value=0, error=None):
        self.value = value
        self.error = error

    def tell(self):
        if self.error is not None:
            raise self.error
        return self.value


def test_artifact_position_uses_wrapped_raw_stream_then_falls_back():
    wrapped = types.SimpleNamespace(
        raw=types.SimpleNamespace(fileobj=_TellStream(12)),
    )
    assert progress._artifact_progress_position(wrapped) == 12

    fallback = _TellStream(15)
    fallback.raw = types.SimpleNamespace(fileobj=_TellStream(error=OSError()))
    assert progress._artifact_progress_position(fallback) == 15
    assert progress._artifact_progress_position(_TellStream(error=ValueError())) is None
    assert progress._artifact_progress_position(object()) is None


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(None, "unknown"), (-1, "unknown"), (0, "0s"), (61, "1m01s"), (3661, "1h01m01s")],
)
def test_format_duration_covers_unknown_seconds_minutes_and_hours(seconds, expected):
    assert progress._format_duration(seconds) == expected


@pytest.mark.parametrize(
    ("phase", "start", "end", "expected"),
    [
        ("bad", 0, 10, None),
        (50, 10, 10, None),
        (-10, 20, 40, 20),
        (50, 20, 40, 30),
        (200, 20, 40, 40),
    ],
)
def test_scale_stage_progress_validates_and_clamps(phase, start, end, expected):
    assert progress._scale_stage_progress_pct(phase, start, end) == expected


def test_artifact_progress_returns_early_without_due_readable_nonempty_input(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(progress.time, "monotonic", lambda: 10)
    monkeypatch.setattr(progress, "_env_int", lambda *_args: 30)
    emitted_lines = []
    monkeypatch.setattr(progress, "_emit_screen_line", emitted_lines.append)

    progress._maybe_log_artifact_progress(
        tmp_path / "missing",
        _TellStream(1),
        {"last_log": 9},
        "read",
    )
    progress._maybe_log_artifact_progress(
        tmp_path / "missing",
        object(),
        {},
        "read",
    )
    progress._maybe_log_artifact_progress(
        tmp_path / "missing",
        _TellStream(1),
        {},
        "read",
    )
    empty_path = tmp_path / "empty"
    empty_path.touch()
    progress._maybe_log_artifact_progress(empty_path, _TellStream(0), {}, "read")

    assert emitted_lines == []


@pytest.mark.parametrize("expected_items", [0, 100])
def test_artifact_progress_emits_truthful_byte_or_item_measurement(
    monkeypatch,
    tmp_path,
    expected_items,
):
    artifact_path = tmp_path / "rates.bin"
    artifact_path.write_bytes(b"x" * 100)
    emitted_lines = []
    live_updates = []
    monkeypatch.setattr(progress.time, "monotonic", lambda: 10)
    monkeypatch.setattr(
        progress,
        "_env_int",
        lambda name, default: (
            expected_items
            if name == progress.PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV
            else default
        ),
    )
    monkeypatch.setattr(progress, "_emit_screen_line", emitted_lines.append)
    monkeypatch.setattr(progress.logger, "info", emitted_lines.append)
    monkeypatch.setattr(
        progress,
        "write_live_progress",
        lambda **values: live_updates.append(values),
    )
    progress_state_by_field = {"started_at": 5, "first_item_at": 5}

    progress._maybe_log_artifact_progress(
        artifact_path,
        _TellStream(50),
        progress_state_by_field,
        "read",
        ref_count=3,
        item_count=25,
    )

    assert len(emitted_lines) == 2
    assert "compressed_read=50.00%" in emitted_lines[0]
    assert progress_state_by_field["last_log"] == 10
    assert live_updates[0]["unit"] == "items"
    assert live_updates[0]["done"] == 25
    if expected_items:
        assert live_updates[0]["total"] == 100
        assert live_updates[0]["pct"] == 25
    else:
        assert live_updates[0]["total"] == 100
        assert live_updates[0]["pct"] == 50


def test_artifact_progress_without_items_uses_compressed_byte_unit(
    monkeypatch,
    tmp_path,
):
    artifact_path = tmp_path / "rates.bin"
    artifact_path.write_bytes(b"x" * 100)
    live_updates = []
    monkeypatch.setattr(progress.time, "monotonic", lambda: 10)
    monkeypatch.setattr(progress, "_env_int", lambda _name, default: default)
    monkeypatch.setattr(progress, "_emit_screen_line", lambda _message: None)
    monkeypatch.setattr(progress.logger, "info", lambda _message: None)
    monkeypatch.setattr(
        progress,
        "write_live_progress",
        lambda **values: live_updates.append(values),
    )

    progress._maybe_log_artifact_progress(
        artifact_path,
        _TellStream(100),
        {"started_at": 0},
        "read",
    )

    assert live_updates[0]["unit"] == "compressed_bytes"
    assert live_updates[0]["done"] == 100
    assert live_updates[0]["pct"] == 100
