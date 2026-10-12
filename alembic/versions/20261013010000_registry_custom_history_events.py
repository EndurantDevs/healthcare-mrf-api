# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain explicit future mutation metadata without guessing legacy operations."""

import os
import re

from alembic import op

revision = "20261013010000_registry_custom_history_events"
down_revision = "20261012010000_company_registry_name_origin"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    return [
        f"ALTER TABLE {namespace}.registry_record_history ADD COLUMN event_json JSONB",
        f"ALTER TABLE {namespace}.registry_record_history ADD CONSTRAINT registry_history_event CHECK ("
        + "event_json IS NULL OR COALESCE(jsonb_typeof(event_json)='object' AND event_json ?& ARRAY['operation','before_revision','operation_context'] AND (event_json - ARRAY['operation','before_revision','operation_context'])='{}'::jsonb AND event_json->>'operation' IN ('create','correct','archive','restore','company_links_batch','catalog_seed','bind','rebind','close','preserve_materialized') AND jsonb_typeof(event_json->'operation_context')='object' AND octet_length(event_json::text)<=16384 AND ((event_json->>'operation'='preserve_materialized' AND event_json->'before_revision'='null'::jsonb) OR (jsonb_typeof(event_json->'before_revision')='number' AND event_json->>'before_revision' ~ '^(0|[1-9][0-9]*)$' AND (event_json->>'before_revision')::numeric=revision-1)),false)"
        + ")",
    ]


def upgrade():
    """Add nullable metadata; retained snapshots and unknown legacy events stay exact."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard retained mutation provenance implicitly."""
    raise RuntimeError("Custom history events require an explicit retained-data migration")
