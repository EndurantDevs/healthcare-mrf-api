# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refuse reserved release pricing runs; no deployment configures their managed control schema."""


def require(condition, code):
    """Refuse an incomplete protected control invariant."""
    if not condition:
        raise ValueError("pricing_control_" + code)


def is_reserved_pricing_key(key):
    """Recognize the existing reserved prefix before validating its exact form."""
    return type(key) is str and key.startswith(("plan-pricing-projection:", "plan-pricing-projection-retry:"))


def require_unreserved_pricing_run(run):
    """Release keys and retries were admitted only through the removed managed control schema."""
    if run.get("importer") == "plan-pricing-projection" and (
        is_reserved_pricing_key(run.get("idempotency_key")) or run.get("retry_of_run_id")
    ):
        raise ValueError("pricing control schema is unavailable or invalid")
