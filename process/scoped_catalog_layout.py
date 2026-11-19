# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve the target's explicitly configured catalog and control namespaces."""

import os

DATA_SCHEMA_ENV = "HLTHPRT_SCOPED_CATALOG_DATA_SCHEMA"
CONTROL_SCHEMA = "mrf"
DATA_SCHEMA = "mrf_scoped_catalog"
OWNER_ROLE = "mrf_scoped_catalog_owner"
STAGE_ROLE = "mrf_scoped_catalog_stage"
FENCE_SCHEMA = "mrf_scoped_catalog_fence"
FENCE_VIEW = "legacy_catalog"
FENCE_OWNER_ROLE = "mrf_scoped_catalog_fence_owner"


def catalog_schema(schema):
    """Keep existing layouts unless the target explicitly selects the protected slice."""
    configured = os.getenv(DATA_SCHEMA_ENV, "")
    if not configured:
        return schema
    if configured != DATA_SCHEMA or schema not in (CONTROL_SCHEMA, DATA_SCHEMA):
        raise ValueError("scoped catalog schema configuration differs")
    return DATA_SCHEMA


def control_schema(schema):
    """The isolated data layout uses the same durable run locus as its predecessor."""
    return CONTROL_SCHEMA if schema == DATA_SCHEMA else schema
