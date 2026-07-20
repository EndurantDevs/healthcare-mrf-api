# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Credential-free Provider Directory source catalog for control clients."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from process import provider_directory_profile as profile_artifact


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
)
RUNNABLE_CLASSIFICATIONS = frozenset({"acquisition", "bulk_acquisition"})
PUBLIC_ENTRY_FIELDS = (
    "entry_id",
    "display_name",
    "owner_id",
    "source_ids",
    "canonical_base",
    "classification",
    "resource_profile",
    "resources",
)


def _json_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _public_catalog_entry(
    raw_entry: dict[str, Any],
    support_by_entry: dict[str, Any],
) -> tuple[dict[str, Any], list[str], bool]:
    """Normalize and validate one reviewed manifest entry for public clients."""
    catalog_entry_by_field = {
        field_name: raw_entry.get(field_name)
        for field_name in PUBLIC_ENTRY_FIELDS
    }
    source_ids = catalog_entry_by_field.get("source_ids")
    if (
        not isinstance(catalog_entry_by_field.get("entry_id"), str)
        or not isinstance(source_ids, list)
        or not source_ids
        or not all(isinstance(source_id, str) for source_id in source_ids)
    ):
        raise RuntimeError("provider_directory_source_manifest_invalid")
    is_runnable = catalog_entry_by_field["classification"] in RUNNABLE_CLASSIFICATIONS
    support_record = support_by_entry.get(catalog_entry_by_field["entry_id"], {})
    documented_resources = (
        support_record.get("documented_resources")
        if isinstance(support_record, dict)
        else None
    )
    if documented_resources is not None and (
        not isinstance(documented_resources, list)
        or not all(isinstance(resource_type, str) for resource_type in documented_resources)
        or len(documented_resources) != len(set(documented_resources))
    ):
        raise RuntimeError("provider_directory_source_manifest_invalid")
    executable_resources = catalog_entry_by_field.get("resources")
    catalog_entry_by_field["supported_resources"] = list(
        documented_resources
        if documented_resources is not None
        else executable_resources or []
    )
    catalog_entry_by_field["runnable"] = is_runnable
    return catalog_entry_by_field, source_ids, is_runnable


def provider_directory_source_catalog(
    manifest_path: Path = DEFAULT_MANIFEST,
) -> dict[str, Any]:
    """Return every reviewed source while fencing runnable Profile aliases."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("importer") != "provider-directory-fhir"
        or not isinstance(manifest.get("entries"), list)
    ):
        raise RuntimeError("provider_directory_source_manifest_invalid")

    profile_source_ids = set(profile_artifact.configured_profile_source_ids())
    support_documentation = manifest.get("support_documentation")
    support_by_entry = (
        support_documentation.get("entry_support", {})
        if isinstance(support_documentation, dict)
        else {}
    )
    runnable_source_ids: set[str] = set()
    catalog_items: list[dict[str, Any]] = []
    for raw_entry in manifest["entries"]:
        if not isinstance(raw_entry, dict):
            raise RuntimeError("provider_directory_source_manifest_invalid")
        catalog_entry_by_field, source_ids, is_runnable = _public_catalog_entry(
            raw_entry,
            support_by_entry,
        )
        if is_runnable:
            runnable_source_ids.update(source_ids)
        catalog_entry_by_field["profile_enabled"] = all(
            source_id in profile_source_ids for source_id in source_ids
        )
        catalog_items.append(catalog_entry_by_field)

    if runnable_source_ids != profile_source_ids:
        raise RuntimeError("provider_directory_profile_source_catalog_drift")
    return {
        "schema_version": 1,
        "campaign_id": manifest.get("campaign_id"),
        "catalog_digest": _json_digest(manifest),
        "entry_count": len(catalog_items),
        "runnable_count": sum(
            bool(catalog_entry_by_field["runnable"])
            for catalog_entry_by_field in catalog_items
        ),
        "profile_source_count": len(profile_source_ids),
        "items": catalog_items,
    }
