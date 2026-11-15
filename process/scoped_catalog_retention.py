# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable retention entry points for the shared native catalog publisher."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from uuid import UUID

CONTRACT = "scoped-catalog-retained.v2"
GENERATION_TABLES = {"code-sets": "code_sets_result_generation", "ms-drg": "ms_drg_result_generation"}


def canonical_metadata(value):
    """Encode bounded control metadata without lossy UUID or timezone conversions."""
    return json.dumps(value, default=_metadata_scalar, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _metadata_scalar(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.isoformat()
    raise TypeError("unsupported catalog metadata scalar")


def generation_value(value):
    """Normalize native generation values for exact durable JSON comparison."""
    return json.loads(canonical_metadata(asdict(value) if is_dataclass(value) else value))


def validate_retained_catalog(receipt, *, importer=None, live_schema=None):
    """Validate logical scope and content while ignoring historical physical metadata."""
    from process.scoped_catalog_named_retention import logical_receipt

    logical = logical_receipt(receipt)
    if (importer is not None and logical["importer"] != importer) or (
        live_schema is not None and logical["live_schema"] != live_schema
    ):
        raise RuntimeError("retained catalog destination differs")
    return receipt


async def require_retained_catalog(session, receipt):
    """Resolve exact retained names and verify logical generations, data and native shape."""
    from process.scoped_catalog_named_retention import require_retained

    await require_retained(session, receipt)
    return receipt


async def cleanup_retained_catalog(session, receipt):
    """Drop only the validated named retained family with native RESTRICT semantics."""
    from process.scoped_catalog_named_retention import cleanup_retained

    await cleanup_retained(session, receipt)
