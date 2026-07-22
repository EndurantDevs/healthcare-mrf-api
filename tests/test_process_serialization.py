from datetime import date, datetime, time
from pathlib import Path

from process import serialization


def test_msgpack_normalization_handles_supported_nonprimitive_types():
    assert serialization._normalize_for_msgpack(
        datetime(2026, 7, 22, 1, 2, 3)
    ) == {"__type__": "datetime", "value": "2026-07-22T01:02:03"}
    assert serialization._normalize_for_msgpack(date(2026, 7, 22)) == {
        "__type__": "date",
        "value": "2026-07-22",
    }
    assert serialization._normalize_for_msgpack(time(1, 2, 3)) == {
        "__type__": "time",
        "value": "01:02:03",
    }
    assert serialization._normalize_for_msgpack(Path("artifact.json")) == (
        "artifact.json"
    )
    assert set(serialization._normalize_for_msgpack({"a", "b"})) == {"a", "b"}


def test_default_encoder_distinguishes_normalized_and_repr_fallbacks():
    path = Path("artifact.json")
    assert serialization._default_encoder(path) == "artifact.json"

    unsupported = object()
    assert serialization._default_encoder(unsupported) == {
        "__type__": "repr",
        "repr": repr(unsupported),
    }


def test_restore_special_recovers_temporal_and_repr_payloads():
    assert serialization._restore_special(
        {"__type__": "datetime", "value": "2026-07-22T01:02:03"}
    ) == datetime(2026, 7, 22, 1, 2, 3)
    assert serialization._restore_special(
        {"__type__": "date", "value": "2026-07-22"}
    ) == date(2026, 7, 22)
    assert serialization._restore_special(
        {"__type__": "time", "value": "01:02:03"}
    ) == time(1, 2, 3)
    assert serialization._restore_special(
        {"__type__": "repr", "repr": "opaque"}
    ) == "opaque"
