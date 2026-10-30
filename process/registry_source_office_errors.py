# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Separate proved missing office rows from invalid or temporarily unavailable evidence."""


class SourceOfficeNotReady(RuntimeError):
    """Valid source offices lack matching rows in the exact authenticated address generation."""
