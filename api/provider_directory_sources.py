# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Credential-free Provider Directory source catalog for control clients."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.provider_directory_rooted_fhir_publication import (
    is_rooted_fhir_catalog_entry,
    unavailable_rooted_fhir_publication,
    ROOTED_FHIR_PUBLICATION_FIELD,
)
from process import provider_directory_profile as profile_artifact
from process.provider_directory_publication_catalog_authority import (
    canonical_manifest_digest,
)


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
        or not catalog_entry_by_field["entry_id"]
        or catalog_entry_by_field["entry_id"]
        != catalog_entry_by_field["entry_id"].strip()
        or len(catalog_entry_by_field["entry_id"]) > 160
        or not isinstance(source_ids, list)
        or not source_ids
        or not all(
            isinstance(source_id, str)
            and source_id
            and source_id == source_id.strip()
            and len(source_id) <= 96
            for source_id in source_ids
        )
        or len(source_ids) != len(set(source_ids))
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


def _nonrunnable_profile_source_ids(
    profile_source_ids: set[str],
) -> set[str]:
    """Return reviewed sources admitted outside the generic runnable catalog."""

    retained_source_ids = set(
        profile_artifact.configured_retained_profile_source_ids()
    )
    dataset_source_ids = set(
        profile_artifact.configured_dataset_scoped_profile_source_ids()
    )
    return (retained_source_ids | dataset_source_ids).intersection(
        profile_source_ids
    )


def _validated_catalog_items(
    raw_entries: list[Any],
    support_by_entry: dict[str, Any],
    profile_source_ids: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    catalog_items: list[dict[str, Any]] = []
    runnable_source_ids: set[str] = set()
    seen_entry_ids: set[str] = set()
    seen_source_ids: set[str] = set()
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            raise RuntimeError("provider_directory_source_manifest_invalid")
        catalog_entry_by_field, source_ids, is_runnable = _public_catalog_entry(
            raw_entry,
            support_by_entry,
        )
        entry_id = catalog_entry_by_field["entry_id"]
        if entry_id in seen_entry_ids or seen_source_ids.intersection(source_ids):
            raise RuntimeError("provider_directory_source_manifest_invalid")
        seen_entry_ids.add(entry_id)
        seen_source_ids.update(source_ids)
        if is_runnable:
            runnable_source_ids.update(source_ids)
        catalog_entry_by_field["profile_enabled"] = all(
            source_id in profile_source_ids for source_id in source_ids
        )
        if is_rooted_fhir_catalog_entry(catalog_entry_by_field):
            catalog_entry_by_field[ROOTED_FHIR_PUBLICATION_FIELD] = (
                unavailable_rooted_fhir_publication()
            )
        catalog_items.append(catalog_entry_by_field)
    return catalog_items, runnable_source_ids


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
    nonrunnable_profile_source_ids = _nonrunnable_profile_source_ids(
        profile_source_ids
    )
    support_documentation = manifest.get("support_documentation")
    support_by_entry = (
        support_documentation.get("entry_support", {})
        if isinstance(support_documentation, dict)
        else {}
    )
    catalog_items, runnable_source_ids = _validated_catalog_items(
        manifest["entries"],
        support_by_entry,
        profile_source_ids,
    )

    if runnable_source_ids | nonrunnable_profile_source_ids != profile_source_ids:
        raise RuntimeError("provider_directory_profile_source_catalog_drift")
    return {
        "schema_version": 1,
        "campaign_id": manifest.get("campaign_id"),
        "catalog_digest": canonical_manifest_digest(manifest),
        "entry_count": len(catalog_items),
        "runnable_count": sum(
            bool(catalog_entry_by_field["runnable"])
            for catalog_entry_by_field in catalog_items
        ),
        "profile_source_count": len(profile_source_ids),
        "items": catalog_items,
    }
