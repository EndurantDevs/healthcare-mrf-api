# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable source-catalog authority for first publication."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_manifest_digest(manifest: Any) -> str:
    """Hash the exact canonical source manifest representation."""

    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def bootstrap_catalog_authority(source_id: str) -> tuple[str, str] | None:
    """Return one current runnable singleton entry and its manifest digest."""

    if (
        not isinstance(source_id, str)
        or not source_id
        or source_id != source_id.strip()
        or len(source_id) > 96
    ):
        return None
    # Import lazily so the API catalog and this process-side fence share the
    # same validation without creating a module-import cycle.
    from api.provider_directory_sources import (
        provider_directory_source_catalog,
        RUNNABLE_CLASSIFICATIONS,
    )

    catalog = provider_directory_source_catalog()
    entries = catalog.get("items")
    digest = catalog.get("catalog_digest")
    if not isinstance(entries, list):
        return None
    matching_entries = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and entry.get("classification") in RUNNABLE_CLASSIFICATIONS
        and entry.get("runnable") is True
        and entry.get("profile_enabled") is True
        and entry.get("source_ids") == [source_id]
        and isinstance(entry.get("entry_id"), str)
        and entry["entry_id"]
        and entry["entry_id"] == entry["entry_id"].strip()
    ]
    if (
        catalog.get("schema_version") != 1
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or len(matching_entries) != 1
    ):
        return None
    return matching_entries[0]["entry_id"], digest
