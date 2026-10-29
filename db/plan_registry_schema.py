# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configure the existing canonical plan registry without provisioning it."""

import os
import re


def plan_registry_schema() -> str | None:
    """Return an explicit safe namespace; absent configuration keeps legacy reads."""
    schema = os.getenv("HLTHPRT_PLAN_REGISTRY_SCHEMA")
    if schema is None or schema == "":
        return None
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Plan registry schema identifier is invalid")
    return schema
