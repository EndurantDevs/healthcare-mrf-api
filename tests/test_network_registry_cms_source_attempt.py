# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared custody tags preserve exact native attempt evidence."""

from dataclasses import FrozenInstanceError

import pytest

from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt


def _attempt_fields():
    return {
        "run_id": "synthetic-run",
        "attempt_id": "synthetic-run:" + "a" * 32,
        "attempt_started_at": "2026-01-02T03:04:05.123456+00:00",
    }


def test_attempt_roundtrip_preserves_timestamp_and_is_immutable():
    fields = _attempt_fields()
    attempt = RegistryCMSSourceAttempt.from_dict(fields)
    assert attempt.as_dict() == fields
    with pytest.raises(FrozenInstanceError):
        attempt.run_id = "changed"


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", ""),
        ("run_id", "synthetic-run "),
        ("run_id", True),
        ("run_id", "x" * 129),
        ("attempt_id", "another-run:" + "a" * 32),
        ("attempt_id", "synthetic-run:" + "a" * 31),
        ("attempt_id", "synthetic-run:" + "A" * 32),
        ("attempt_started_at", "2026-01-02T03:04:05"),
        ("attempt_started_at", "2026-01-02T03:04:05+01:00"),
        ("attempt_started_at", "not-a-time"),
    ],
)
def test_attempt_rejects_malformed_or_different_native_identity(field, value):
    fields = _attempt_fields()
    fields[field] = value
    with pytest.raises(ValueError, match="^registry_cms_source_attempt_invalid$"):
        RegistryCMSSourceAttempt.from_dict(fields)


@pytest.mark.parametrize("change", ["extra", "missing", "not-dict"])
def test_attempt_decoder_rejects_open_documents(change):
    fields = _attempt_fields()
    if change == "extra":
        fields["authorized"] = True
    elif change == "missing":
        del fields["attempt_id"]
    else:
        fields = list(fields.items())
    with pytest.raises(ValueError, match="^registry_cms_source_attempt_invalid$"):
        RegistryCMSSourceAttempt.from_dict(fields)
