# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Column grants used by the administrative publication installers."""

LIFECYCLE_COLUMNS = frozenset({"state", "lease_id", "lease_expires_at", "result_json", "updated_at"})
INSERT_COLUMNS = frozenset(
    {
        "request_id",
        "actor_key",
        "session_token_sha256",
        "actor_json",
        "command_json",
        "idempotency_key",
        "request_sha256",
    }
)
