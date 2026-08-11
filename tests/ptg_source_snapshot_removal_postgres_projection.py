"""Pure SQL and manifest projections for snapshot-removal fixtures."""

import json

from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION


def schema_sql(statement: str, schema: str) -> str:
    """Bind the quoted disposable schema token in one fixture statement."""
    return statement.replace("__SCHEMA__", schema)


def snapshot_manifest(source_key: str) -> str:
    """Serialize one shared-layout source manifest."""
    return json.dumps(
        {
            "serving_index": {
                "arch_version": "postgres_binary_v3",
                "storage_generation": PTG2_V3_SHARED_GENERATION,
                "shared_snapshot_key": 10,
                "source_key": source_key,
            }
        }
    )
