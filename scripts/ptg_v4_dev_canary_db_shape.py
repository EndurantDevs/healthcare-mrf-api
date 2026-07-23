"""Pure PostgreSQL record shaping for PTG V4 canary evidence."""

from __future__ import annotations

import json
from typing import Any, Mapping

from scripts.ptg_v4_dev_canary_db_reference import _json_safe


def shape_snapshot_and_root(
    fields_by_name: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Separate snapshot and root fields from one joined database record."""

    snapshot_fields = (
        "snapshot_id",
        "import_run_id",
        "snapshot_status",
        "published_at",
        "snapshot_key",
        "layout_state",
        "layout_generation",
        "layout_logical_byte_count",
    )
    snapshot_by_field = {
        field_name: fields_by_name[field_name] for field_name in snapshot_fields
    }
    snapshot_by_field["layout_manifest"] = json.loads(
        fields_by_name["layout_manifest_text"]
    )
    root_by_field = {
        field_name.removeprefix("root_"): field_value
        for field_name, field_value in fields_by_name.items()
        if field_name.startswith("root_")
    }
    root_by_field.update(
        {
            field_name: fields_by_name[field_name]
            for field_name in (
                "format_version",
                "map_format",
                "representation",
                "projection_id_scope",
                "map_digest",
                "object_kind_count",
                "map_pack_count",
                "coordinate_count",
                "entry_count",
                "logical_byte_count",
                "stored_map_byte_count",
                "npi_count",
                "component_count",
                "pattern_count",
                "relation_count",
                "heavy_owner_count",
            )
        }
    )
    return _json_safe(snapshot_by_field), _json_safe(root_by_field)
