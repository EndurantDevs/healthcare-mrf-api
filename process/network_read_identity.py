# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy error type retained for source validators during migration."""


class NetworkReadIdentityError(RuntimeError):
    """A legacy source validator rejected its input."""
