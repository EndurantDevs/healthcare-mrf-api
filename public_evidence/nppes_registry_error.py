# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value-free public error boundary for NPPES registry replay."""


_INVALID = "nppes_registry_replay_invalid"


class NppesRegistryReplayError(RuntimeError):
    """One value-free failure for NPPES replay contract validation."""


def replay_error() -> NppesRegistryReplayError:
    """Return a fresh public failure without retaining source values."""

    return NppesRegistryReplayError(_INVALID)


__all__ = ("NppesRegistryReplayError", "replay_error")
