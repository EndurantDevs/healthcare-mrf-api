# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact CMS group bindings only from an accepted source-local publication."""

import os
import re
import unicodedata
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api.network_read_execution import execute_network_read, network_read_name_inventory
from api.provider_directory_entities_contract import (
    DirectoryReadError,
    directory_cursor_key,
    issue_directory_cursor,
    opaque_directory_key,
    read_directory_cursor,
)
from process.network_compact_layout import CompactAddressRelation
from process.provider_directory_profile import is_valid_npi

_SOURCE = "cms-doctors"
_TABLES = ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site")
_BINDING = "provider_directory_cms_doctors_group_binding"
_SITE_BINDING = "provider_directory_cms_doctors_site_binding"
_AUTHORITY = "reference_family_result_generation"
_READ_TABLES = (*_TABLES, _BINDING, _SITE_BINDING, _AUTHORITY)


def _schema_name():
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise DirectoryReadError(503)
    return f'"{schema}"'


def _generation_key(key, authority, relation_oids):
    """Require real publication authority; manual adoption cannot become accepted."""
    try:
        identities = [str(UUID(str(authority[field]))) for field in ("local_lineage_id", "origin_lineage_id")]
        generations = [authority[field] for field in ("local_generation", "origin_generation")]
        if any(type(value) is not int or value < 1 for value in generations):
            raise ValueError
        published_at = authority["published_at"]
        if not isinstance(published_at, datetime) or published_at.utcoffset() is None:
            raise ValueError
        if list(authority["relation_oids"]) != relation_oids[:3]:
            raise ValueError
        if len(set(relation_oids)) != 5 or any(type(value) is not int or value <= 0 for value in relation_oids):
            raise ValueError
    except KeyError, TypeError, ValueError, AttributeError:
        raise DirectoryReadError(503) from None
    return opaque_directory_key(key, "gen_", _SOURCE, identities, generations, published_at.isoformat(), relation_oids)


async def _read_inventory(session, schema):
    """Capture fixed names only; accepted generation authority is checked separately."""
    if session.in_transaction() is not True:
        raise DirectoryReadError(503)
    modes = (
        await execute_network_read(
            session, text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
        )
    ).one()
    if tuple(modes) not in (("repeatable read", "on"), ("serializable", "on")):
        raise DirectoryReadError(503)
    try:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        backend_pid = driver.get_server_pid()
        if driver.is_in_transaction() is not True or type(backend_pid) is not int or not 0 < backend_pid <= 2147483647:
            raise DirectoryReadError(503)
    except Exception as error:
        raise DirectoryReadError(503) from error
    catalog = await execute_network_read(
        session,
        text("""SELECT requested.name,c.oid::bigint,
      c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,
      c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap') AS heap,
      EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
      EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS rules
      FROM unnest(CAST(:names AS text[])) WITH ORDINALITY requested(name,ordinal)
      LEFT JOIN pg_catalog.pg_namespace n ON n.nspname=:schema
      LEFT JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=requested.name
      ORDER BY requested.ordinal"""),
        {"names": list(_READ_TABLES), "schema": schema[1:-1]},
    )
    relations = []
    catalog_records = catalog.mappings().all()
    if tuple(catalog_record["name"] for catalog_record in catalog_records) != _READ_TABLES:
        raise DirectoryReadError(503)
    for catalog_record in catalog_records:
        if (
            type(catalog_record["oid"]) is not int
            or not 0 < catalog_record["oid"] < 2**32
            or catalog_record["relkind"] != "r"
            or catalog_record["relpersistence"] != "p"
            or catalog_record["heap"] is not True
            or any(
                catalog_record[field]
                for field in ("relrowsecurity", "relforcerowsecurity", "relispartition", "inherited", "rules")
            )
        ):
            raise DirectoryReadError(503)
        relations.append(CompactAddressRelation(schema[1:-1], catalog_record["name"], catalog_record["oid"]))
    return driver, tuple(relations)


@asynccontextmanager
async def _guarded_tables(session, schema):
    try:
        driver, relations = await _read_inventory(session, schema)
        with network_read_name_inventory(driver, relations):
            yield relations
    except ServiceUnavailable as error:
        raise DirectoryReadError(503) from error


async def _accepted_generation(session, schema, key, relations):
    """Read real publication authority under the complete fixed-name guard."""
    authority = (
        (
            await execute_network_read(
                session,
                text(
                    f"SELECT local_lineage_id, local_generation, origin_lineage_id, origin_generation, "
                    f"published_at, relation_oids FROM {schema}.{_AUTHORITY} "
                    "WHERE importer_id = 'cms-doctors'"
                ),
            )
        )
        .mappings()
        .one_or_none()
    )
    if authority is None:
        raise DirectoryReadError(503)
    return _generation_key(key, authority, [relation.table_oid for relation in relations[:5]])


def _evidence(key, row, resource_type="CMSDoctorsGroup"):
    observed_at = row["observed_at"]
    if not isinstance(observed_at, datetime) or not row["generation_id"]:
        raise DirectoryReadError(503)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return [
        {
            "source_id": _SOURCE,
            "resource_type": resource_type,
            "record_key": opaque_directory_key(key, "src_", _SOURCE, row["generation_id"], row["row_number"]),
            "release_id": opaque_directory_key(key, "release_", _SOURCE, row["generation_id"]),
            "observed_at": observed_at.isoformat(),
        }
    ]


def _entity(key, row):
    is_site = "site_id" in row
    name = row["first_name"]
    has_conflict = name != row["last_name"]
    name = name.strip() if name else None
    if has_conflict or not name or len(name) > 2048 or any(unicodedata.category(char).startswith("C") for char in name):
        name = None
    return {
        "id": str(row["site_id"] if is_site else row["organization_id"]),
        "kind": "sites" if is_site else "medical-groups",
        "source_id": _SOURCE,
        "display_name": name,
        "status": "conflict" if has_conflict else "unknown",
        "effective_start": None,
        "effective_end": None,
        "evidence": _evidence(key, row, "CMSDoctorsSite" if is_site else "CMSDoctorsGroup"),
    }


def _relationship(key, row, kind):
    target_id = row["site_id"] if kind == "medical-groups" else row["organization_id"]
    if target_id is None or not is_valid_npi(row["npi"]):
        raise DirectoryReadError(503)
    return {
        "relationship_key": opaque_directory_key(key, "rel_", _SOURCE, row["generation_id"], row["row_number"]),
        "relationship_type": "group-site" if kind == "medical-groups" else "site-group",
        "source_id": _SOURCE,
        "target_kind": "sites" if kind == "medical-groups" else "medical-groups",
        "target_id": str(target_id),
        "provider_npi": str(row["npi"]),
        "provider_profile_path": f"/api/v1/providers/{row['npi']}/profile",
        "status": "resolved",
        "effective_start": None,
        "effective_end": None,
        "evidence": _evidence(key, row),
    }


async def _entity_rows(session, schema, query, position):
    """Seek stable UUIDs and aggregate only the selected bounded page of exact PACs."""
    result = await execute_network_read(
        session,
        text(f"""
        WITH page AS (
            SELECT b.organization_id, b.org_pac_id FROM {schema}.{_BINDING} b
            WHERE (CAST(:entity_id AS uuid) IS NULL OR b.organization_id = CAST(:entity_id AS uuid))
              AND (CAST(:position AS uuid) IS NULL OR b.organization_id > CAST(:position AS uuid))
              AND EXISTS (SELECT 1 FROM {schema}.cms_doctor_group_site g WHERE g.org_pac_id = b.org_pac_id)
            ORDER BY b.organization_id LIMIT :page_size
        )
        SELECT page.organization_id, first_row.row_number, first_row.generation_id, first_row.observed_at,
               names.first_name, names.last_name
        FROM page
        CROSS JOIN LATERAL (
            SELECT row_number, generation_id, observed_at FROM {schema}.cms_doctor_group_site
            WHERE org_pac_id = page.org_pac_id ORDER BY row_number LIMIT 1
        ) first_row
        CROSS JOIN LATERAL (
            SELECT min(nullif(left(btrim(facility_name), 2049), '')) AS first_name,
                   max(nullif(left(btrim(facility_name), 2049), '')) AS last_name
            FROM {schema}.cms_doctor_group_site WHERE org_pac_id = page.org_pac_id
        ) names ORDER BY page.organization_id
    """),
        {"entity_id": query.entity_id, "position": position, "page_size": query.limit + 1},
    )
    return result.mappings().all()


async def _site_rows(session, schema, query, position):
    result = await execute_network_read(
        session,
        text(f"""
        WITH page AS (
            SELECT b.site_id, b.adrs_id FROM {schema}.{_SITE_BINDING} b
            WHERE (CAST(:entity_id AS uuid) IS NULL OR b.site_id = CAST(:entity_id AS uuid))
              AND (CAST(:position AS uuid) IS NULL OR b.site_id > CAST(:position AS uuid))
              AND EXISTS (SELECT 1 FROM {schema}.cms_doctor_group_site g WHERE g.adrs_id = b.adrs_id)
            ORDER BY b.site_id LIMIT :page_size
        )
        SELECT page.site_id, first_row.row_number, first_row.generation_id, first_row.observed_at,
               names.first_name, names.last_name
        FROM page
        CROSS JOIN LATERAL (
            SELECT row_number, generation_id, observed_at FROM {schema}.cms_doctor_group_site
            WHERE adrs_id = page.adrs_id ORDER BY row_number LIMIT 1
        ) first_row
        CROSS JOIN LATERAL (
            SELECT min(nullif(left(btrim(facility_name), 2049), '')) AS first_name,
                   max(nullif(left(btrim(facility_name), 2049), '')) AS last_name
            FROM {schema}.cms_doctor_group_site WHERE adrs_id = page.adrs_id
        ) names ORDER BY page.site_id
    """),
        {"entity_id": query.entity_id, "position": position, "page_size": query.limit + 1},
    )
    return result.mappings().all()


async def _relationship_rows(session, schema, query, position):
    result = await execute_network_read(
        session,
        text(f"""
        SELECT g.row_number, g.generation_id, g.observed_at, g.npi, s.site_id
        FROM {schema}.cms_doctor_group_site g
        JOIN {schema}.{_BINDING} b ON b.org_pac_id = g.org_pac_id
        LEFT JOIN {schema}.{_SITE_BINDING} s ON s.adrs_id = g.adrs_id
        WHERE b.organization_id = CAST(:entity_id AS uuid)
          AND g.row_number > :position AND nullif(btrim(g.adrs_id), '') IS NOT NULL
        ORDER BY g.row_number LIMIT :page_size
    """),
        {"entity_id": query.entity_id, "position": position or 0, "page_size": query.limit + 1},
    )
    return result.mappings().all()


async def _site_relationship_rows(session, schema, query, position):
    result = await execute_network_read(
        session,
        text(f"""
        SELECT g.row_number, g.generation_id, g.observed_at, g.npi, b.organization_id
        FROM {schema}.cms_doctor_group_site g
        JOIN {schema}.{_SITE_BINDING} s ON s.adrs_id = g.adrs_id
        LEFT JOIN {schema}.{_BINDING} b ON b.org_pac_id = g.org_pac_id
        WHERE s.site_id = CAST(:entity_id AS uuid) AND g.row_number > :position
          AND nullif(btrim(g.org_pac_id), '') IS NOT NULL
        ORDER BY g.row_number LIMIT :page_size
    """),
        {"entity_id": query.entity_id, "position": position or 0, "page_size": query.limit + 1},
    )
    return result.mappings().all()


async def _require_group(session, schema, entity_id):
    has_group = (
        await execute_network_read(
            session,
            text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {schema}.{_BINDING} b
            JOIN {schema}.cms_doctor_group_site g ON g.org_pac_id = b.org_pac_id
            WHERE b.organization_id = CAST(:entity_id AS uuid)
        )
    """),
            {"entity_id": entity_id},
        )
    ).scalar_one()
    if not has_group:
        raise DirectoryReadError(404)


async def _require_site(session, schema, entity_id):
    has_site = (
        await execute_network_read(
            session,
            text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {schema}.{_SITE_BINDING} b
            JOIN {schema}.cms_doctor_group_site g ON g.adrs_id = b.adrs_id
            WHERE b.site_id = CAST(:entity_id AS uuid)
        )
    """),
            {"entity_id": entity_id},
        )
    ).scalar_one()
    if not has_site:
        raise DirectoryReadError(404)


async def _read_page(session, schema, key, query, generation_id):
    position = read_directory_cursor(key, query, generation_id)
    if query.shape == "relationships":
        if query.kind == "sites":
            await _require_site(session, schema, query.entity_id)
            rows = await _site_relationship_rows(session, schema, query, position)
        else:
            await _require_group(session, schema, query.entity_id)
            rows = await _relationship_rows(session, schema, query, position)
        items = [_relationship(key, row, query.kind) for row in rows[: query.limit]]
        items.sort(key=lambda item: item["relationship_key"])
        next_position = rows[query.limit - 1]["row_number"] if len(rows) > query.limit else None
    else:
        rows = await (_site_rows if query.kind == "sites" else _entity_rows)(session, schema, query, position)
        items = [_entity(key, row) for row in rows[: query.limit]]
        next_position = str(items[-1]["id"]) if len(rows) > query.limit else None
    if query.shape == "entity":
        if not items:
            raise DirectoryReadError(404)
        return {"generation_id": generation_id, "item": items[0]}
    cursor = issue_directory_cursor(key, query, generation_id, next_position) if next_position is not None else None
    return {"generation_id": generation_id, "items": items, "next_cursor": cursor}


async def read_medical_groups(session, query):
    """Serve one bounded read snapshot, failing closed for unsupported or unaccepted sources."""
    if (
        query.network_ids is not None
        or query.kind not in {"medical-groups", "sites"}
        or query.source_id != _SOURCE
        or session.in_transaction()
    ):
        raise DirectoryReadError(503)
    key = directory_cursor_key()
    schema = _schema_name()
    async with session.begin():
        await execute_network_read(session, text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await execute_network_read(session, text("SET LOCAL statement_timeout = '2s'"))
        await execute_network_read(session, text("SET LOCAL lock_timeout = '250ms'"))
        await execute_network_read(session, text("SET LOCAL work_mem = '4MB'"))
        async with _guarded_tables(session, schema) as relations:
            generation_id = await _accepted_generation(session, schema, key, relations)
            if query.generation_id is not None and query.generation_id != generation_id:
                raise DirectoryReadError(409)
            await _require_complete_bindings(session, schema)
            return await _read_page(session, schema, key, query, generation_id)


async def read_medical_groups_snapshot(session, query):
    """Keep public Doctors facts inside the caller's existing Reader snapshot."""
    if (
        query.network_ids is not None
        or query.kind not in {"medical-groups", "sites"}
        or query.source_id != _SOURCE
        or not session.in_transaction()
    ):
        raise DirectoryReadError(503)
    key, schema = directory_cursor_key(), _schema_name()
    async with _guarded_tables(session, schema) as relations:
        generation_id = await _accepted_generation(session, schema, key, relations)
        if query.excluded_network_ids is not None:
            from api.network_address_scope import current_network_address_scope

            scope = current_network_address_scope()
            if scope is None or scope.manifest is not query.network_manifest:
                raise DirectoryReadError(503)
            generation_id = opaque_directory_key(
                key,
                "gen_",
                generation_id,
                scope.manifest.generation_id,
                scope.manifest.approved_custom_revision,
                scope.manifest.manifest_sha256,
                query.access_scope_sha256,
            )
        if query.generation_id is not None and query.generation_id != generation_id:
            raise DirectoryReadError(409)
        await _require_complete_bindings(session, schema)
        return await _read_page(session, schema, key, query, generation_id)


async def _require_complete_bindings(session, schema):
    """Do not silently omit accepted groups whose durable identities are missing."""
    has_missing_binding = (
        await execute_network_read(
            session,
            text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {schema}.cms_doctor_group_site g
            LEFT JOIN {schema}.{_BINDING} b ON b.org_pac_id = g.org_pac_id
            LEFT JOIN {schema}.{_SITE_BINDING} s ON s.adrs_id = g.adrs_id
            WHERE (nullif(btrim(g.org_pac_id), '') IS NOT NULL AND b.organization_id IS NULL)
               OR (nullif(btrim(g.adrs_id), '') IS NOT NULL AND s.site_id IS NULL)
        )
    """),
        )
    ).scalar_one()
    if has_missing_binding:
        raise DirectoryReadError(503)
