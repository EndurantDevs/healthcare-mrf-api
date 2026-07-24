# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared PTG database-schema resolution."""

from __future__ import annotations

import os


def resolve_ptg2_schema(schema_name: str | None = None) -> str:
    """Resolve the PTG schema with the same conflict rules as Alembic."""

    if schema_name:
        return str(schema_name)
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


__all__ = ["resolve_ptg2_schema"]
