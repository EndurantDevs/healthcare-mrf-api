# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate source-owned catalog exports and their bounded binding pages."""

import re
from contextlib import asynccontextmanager
from uuid import UUID

from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, MAX_ROWS, fhir_catalog_json_bytes, fhir_catalog_text
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile, parse_fhir_catalog_source_profile

_OPERATIONS = {"inventory", "replay", "legacy_bindings"}


def iter_source_owned_observation_batches(rows, *, page_rows=MAX_ROWS, page_bytes=DEFAULT_BYTES):
    """Split expanded observations into bounded JSON batches without limiting edition totals."""
    if (
        type(page_rows) is not int
        or not 1 <= page_rows <= MAX_ROWS
        or type(page_bytes) is not int
        or not 1 <= page_bytes <= DEFAULT_BYTES
    ):
        raise ValueError("registry_fhir_catalog_source_export_batch_bounds_invalid")
    pending, size = [], 2
    for row in rows:
        row_bytes = len(fhir_catalog_json_bytes(row))
        if row_bytes + 2 > page_bytes:
            raise ValueError("registry_fhir_catalog_source_export_observation_too_large")
        if pending and (len(pending) == page_rows or size + 1 + row_bytes > page_bytes):
            yield tuple(pending)
            pending, size = [], 2
        size += row_bytes + (1 if pending else 0)
        pending.append(row)
    if pending:
        yield tuple(pending)


async def iter_source_owned_catalog_legacy_bindings(source_export, descriptor, organization_ids):
    """Yield validated binding pages for only the admitted aliases and requested organizations."""
    aliases = descriptor["source_aliases"]
    if (
        type(organization_ids) is not tuple
        or not 1 <= len(organization_ids) <= MAX_ROWS
        or any(type(key) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", key) is None for key in organization_ids)
        or len(set(organization_ids)) != len(organization_ids)
        or type(aliases) is not list
        or not 1 <= len(aliases) <= MAX_ROWS
        or any(
            type(alias) is not str
            or not alias
            or alias != alias.strip()
            or fhir_catalog_text(alias, 64, strip=False) != alias
            for alias in aliases
        )
        or aliases != sorted(set(aliases))
    ):
        raise ValueError("registry_fhir_catalog_identity_source_invalid")
    allowed_aliases, allowed_organizations = set(aliases), set(organization_ids)
    async for bindings in source_export["legacy_bindings"](descriptor, organization_ids):
        if (
            type(bindings) is not tuple
            or not 1 <= len(bindings) <= MAX_ROWS
            or len(fhir_catalog_json_bytes(bindings)) > DEFAULT_BYTES
        ):
            raise ValueError("registry_fhir_catalog_identity_source_bounds")
        for binding in bindings:
            if (
                type(binding) is not dict
                or set(binding) != {"source_id", "resource_id", "network_id"}
                or type(binding["source_id"]) is not str
                or binding["source_id"] not in allowed_aliases
                or type(binding["resource_id"]) is not str
                or binding["resource_id"] not in allowed_organizations
                or type(binding["network_id"]) is not str
            ):
                raise ValueError("registry_fhir_catalog_identity_source_invalid")
            identifier = UUID(binding["network_id"])
            if not identifier.int or str(identifier) != binding["network_id"]:
                raise ValueError("registry_fhir_catalog_identity_source_invalid")
        yield bindings


@asynccontextmanager
async def open_source_owned_catalog_export(factory, profile, registry_connection):
    """Keep configured logical profile agreement and Source cleanup around its export."""
    if type(profile) is not FHIRCatalogSourceReadProfile:
        raise ValueError("registry_fhir_catalog_source_export_profile_invalid")
    if not callable(factory):
        raise ValueError("registry_fhir_catalog_source_export_factory_invalid")
    async with factory(source_profile=profile) as export:
        if (
            type(export) is not dict
            or not _OPERATIONS <= set(export)
            or not set(export) <= _OPERATIONS | {"source_companies", "native_read_profile"}
        ):
            raise ValueError("registry_fhir_catalog_source_export_invalid")
        if "native_read_profile" in export:
            try:
                declared_profile = parse_fhir_catalog_source_profile(export["native_read_profile"])
            except ValueError, TypeError:
                raise ValueError("registry_fhir_catalog_source_export_invalid") from None
            if declared_profile.document() != profile.document():
                raise ValueError("registry_fhir_catalog_source_export_invalid")
        if any(not callable(callback) for name, callback in export.items() if name != "native_read_profile"):
            raise ValueError("registry_fhir_catalog_source_export_invalid")
        yield export
