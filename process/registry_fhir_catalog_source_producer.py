# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate source-owned catalog exports and their bounded binding pages."""

import re
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, MAX_ROWS, fhir_catalog_json_bytes, fhir_catalog_text
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

CATALOG_DATABASE_IDENTITY_SQL = """SELECT pg_catalog.current_database()::text AS database_name,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 (pg_catalog.pg_control_system()).system_identifier::text AS system_identifier,
 pg_catalog.pg_postmaster_start_time() AS postmaster_started,pg_catalog.pg_is_in_recovery() AS recovering"""
_IDENTITY_TYPES = {
    "database_name": str,
    "database_oid": int,
    "system_identifier": str,
    "postmaster_started": datetime,
    "recovering": bool,
}
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
    """Keep source-owned validation and cleanup around an identity-matched export."""
    if type(profile) is not FHIRCatalogSourceReadProfile or profile.published_read_scope is not None:
        raise ValueError("registry_fhir_catalog_source_export_profile_invalid")
    if not callable(factory):
        raise ValueError("registry_fhir_catalog_source_export_factory_invalid")
    async with factory(source_profile=profile) as export:
        if type(export) is not dict or set(export) != _OPERATIONS | {"database_identity"}:
            raise ValueError("registry_fhir_catalog_source_export_invalid")
        if any(not callable(export[name]) for name in _OPERATIONS):
            raise ValueError("registry_fhir_catalog_source_export_invalid")
        identity = export["database_identity"]
        if (
            type(identity) is not dict
            or set(identity) != _IDENTITY_TYPES.keys()
            or any(type(identity[name]) is not field_type for name, field_type in _IDENTITY_TYPES.items())
            or identity["recovering"] is not False
        ):
            raise ValueError("registry_fhir_catalog_source_export_database_changed")
        registry_identity = await registry_connection.fetchrow(CATALOG_DATABASE_IDENTITY_SQL)
        if (
            registry_identity is None
            or dict(registry_identity) != identity
            or any(type(registry_identity[name]) is not field_type for name, field_type in _IDENTITY_TYPES.items())
        ):
            raise ValueError("registry_fhir_catalog_source_export_database_changed")
        yield export
