# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared typed scalar projection for full-family reads and their byte bounds."""

from datetime import timezone
from typing import Any


def field_value_payload(value: Any) -> dict[str, object]:
    """Preserve typed scalar states and exact decimal/date encodings."""

    encoded: object = value.value
    if value.state == "value":
        if value.field_type == "decimal":
            encoded = format(encoded, "f")
        elif value.field_type == "date":
            encoded = encoded.isoformat()
        elif value.field_type == "timestamp":
            encoded = encoded.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "field_id": value.field_id,
        "field_type": value.field_type,
        "state": value.state,
        "value": encoded,
    }


def full_family_payload(detail: Any) -> dict[str, object]:
    """Project all granted root fields and children without internal locators."""

    return {
        "root_fields": [field_value_payload(value) for value in detail.root_fields],
        "children": [
            {
                "collection": child.collection,
                "fields": [field_value_payload(value) for value in child.fields],
            }
            for child in detail.children
        ],
    }
