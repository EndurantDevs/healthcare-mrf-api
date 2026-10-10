# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed native run-attempt identity for prepared source custody."""

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class RegistryCMSSourceAttempt:
    """Preserve the authenticated attempt; this tag alone never authorizes cleanup."""

    run_id: str
    attempt_id: str
    attempt_started_at: str

    def __post_init__(self):
        for value, maximum in (
            (self.run_id, 128),
            (self.attempt_id, 161),
            (self.attempt_started_at, 64),
        ):
            if type(value) is not str or not value or value != value.strip() or len(value) > maximum:
                raise ValueError("registry_cms_source_attempt_invalid")
        if re.fullmatch(re.escape(self.run_id) + r":[0-9a-f]{32}", self.attempt_id) is None:
            raise ValueError("registry_cms_source_attempt_invalid")
        try:
            started_at = datetime.fromisoformat(self.attempt_started_at)
        except ValueError:
            raise ValueError("registry_cms_source_attempt_invalid") from None
        if started_at.tzinfo is None or started_at.utcoffset() != timedelta(0):
            raise ValueError("registry_cms_source_attempt_invalid")

    def as_dict(self):
        """Persist exact scalar evidence without normalizing the native timestamp."""
        return asdict(self)

    @classmethod
    def from_dict(cls, raw):
        """Reject extra fields before constructing the closed attempt tag."""
        if type(raw) is not dict or set(raw) != {"run_id", "attempt_id", "attempt_started_at"}:
            raise ValueError("registry_cms_source_attempt_invalid")
        return cls(**raw)
