# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe registered imported metadata in an authenticated read-only snapshot."""

import hashlib
import json
import re
from collections import deque
from contextlib import AsyncExitStack
from datetime import datetime, timezone

import asyncpg

from db.plan_release_storage import storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_generic_source_provision import fhir_source_catalog_row
from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    RESOURCE_TYPES,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
    fhir_source_uri,
    iter_fhir_catalog_batches,
    iter_fhir_catalog_raw_pages,
)
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    FHIRCatalogObservationDigests,
    catalog_observation_descriptor_digest,
    validate_catalog_observation_receipt,
)
from process.registry_fhir_catalog_published_read import fhir_catalog_published_reads, pin_fhir_catalog_published_read
from process.registry_fhir_catalog_reference_rollup import MAX_PAGE_REFERENCES
from process.registry_fhir_catalog_reporting import current_fhir_catalog_report

_MAX_CATALOG_ROWS = 1000
_HEADER = "provider_directory_endpoint_dataset"
_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"
_DEFINITION_FIELDS = (
    "oid",
    "nspname",
    "relname",
    "relkind",
    "relpersistence",
    "relispartition",
    "relowner",
    "relrowsecurity",
    "relforcerowsecurity",
    "bound",
    "partition_key",
    "parents",
    "columns",
    "constraints",
    "indexes",
)
_READER_SQL = """SELECT session_user::text AS login,current_user::text AS actor,r.oid::bigint AS reader_oid,
 owner.oid::bigint AS owner_oid,r.rolcanlogin,
 pg_catalog.current_setting('transaction_read_only') AS readonly,
 pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('session_replication_role') AS replication_role,
 (pg_catalog.current_schemas(true))[1]='pg_catalog' AS safe_search_path,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 pg_catalog.transaction_timestamp() AS observed_at,
 NOT EXISTS(SELECT FROM pg_catalog.pg_roles elevated WHERE
  (pg_catalog.pg_has_role(r.oid,elevated.oid,'MEMBER') OR pg_catalog.pg_has_role(r.oid,elevated.oid,'SET')
   OR pg_catalog.pg_has_role(r.oid,elevated.oid,'USAGE'))
  AND (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole OR elevated.rolreplication
   OR elevated.rolbypassrls OR elevated.rolname IN ('pg_read_all_data','pg_write_all_data','pg_read_server_files',
    'pg_write_server_files','pg_execute_server_program','pg_signal_backend','pg_checkpoint','pg_create_subscription')))
 AS no_dangerous_role,
 EXISTS(SELECT FROM pg_catalog.pg_roles allowed WHERE allowed.rolname=ANY($2::text[])
  AND pg_catalog.pg_has_role(r.oid,allowed.oid,'USAGE')) AS admitted_role,
 (SELECT count(*) FROM pg_catalog.pg_roles allowed WHERE allowed.rolname=ANY($2::text[])) AS runtime_role_count
 FROM pg_catalog.pg_roles r LEFT JOIN pg_catalog.pg_roles owner ON owner.rolname=$1
 WHERE r.rolname=session_user"""
_ACCESS_SQL = """WITH reachable AS (
 SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.oid=$1::oid
 OR pg_catalog.pg_has_role($1::oid,r.oid,'MEMBER') OR pg_catalog.pg_has_role($1::oid,r.oid,'SET')
 OR pg_catalog.pg_has_role($1::oid,r.oid,'USAGE')
), targets AS (SELECT * FROM pg_catalog.pg_class WHERE oid=ANY($2::oid[])),
 namespaces AS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname=ANY($3::text[]))
 SELECT (SELECT count(*) FROM targets)=cardinality($2::oid[])
 AND (SELECT count(*) FROM namespaces)=cardinality($3::text[])
 AND NOT EXISTS(SELECT FROM reachable r CROSS JOIN targets c WHERE
  pg_catalog.pg_has_role(r.oid,c.relowner,'MEMBER') OR pg_catalog.pg_has_role(r.oid,c.relowner,'SET')
  OR pg_catalog.pg_has_role(r.oid,c.relowner,'USAGE')
  OR pg_catalog.has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
  OR pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES'))
 AND NOT EXISTS(SELECT FROM reachable r CROSS JOIN namespaces n WHERE
  pg_catalog.has_schema_privilege(r.oid,n.oid,'CREATE')
  OR pg_catalog.pg_has_role(r.oid,n.nspowner,'MEMBER') OR pg_catalog.pg_has_role(r.oid,n.nspowner,'SET')
  OR pg_catalog.pg_has_role(r.oid,n.nspowner,'USAGE'))
 AND NOT EXISTS(SELECT FROM reachable r WHERE
  pg_catalog.has_parameter_privilege(r.oid,'session_replication_role','SET'))"""
_PHYSICAL_SQL = (
    """WITH physical AS MATERIALIZED (
 SELECT row_to_json(c)::text AS document,c.oid FROM ("""
    + CATALOG_SQL
    + """) c LIMIT 1001
), bounded AS (
 SELECT *,sum(octet_length(document)) OVER(ORDER BY oid) AS bytes FROM physical
) SELECT CASE WHEN bytes<=$4 THEN document END AS document FROM bounded ORDER BY oid"""
)

FHIR_CATALOG_PHYSICAL_SQL = _PHYSICAL_SQL
FHIR_CATALOG_MAX_PHYSICAL_ROWS = _MAX_CATALOG_ROWS
FHIR_CATALOG_PHYSICAL_DEFINITION_FIELDS = _DEFINITION_FIELDS
FHIR_CATALOG_HEADER_TABLE = _HEADER
FHIR_CATALOG_RESOURCE_TABLE = _RESOURCE
FHIR_CATALOG_SOURCE_TABLE = _SOURCE
FHIR_CATALOG_ENDPOINT_TABLE = _ENDPOINT


class FHIRCatalogObservedSourceError(ValueError):
    """Refuse unavailable reader authority or incomplete observed source evidence."""


def _fail(reason="invalid"):
    raise FHIRCatalogObservedSourceError("registry_fhir_catalog_observed_source_" + reason)


def _text(value, maximum):
    if not value or fhir_catalog_text(value, maximum, strip=False) != value or value.strip() != value:
        _fail()


async def _reader(connection, profile):
    if connection.is_in_transaction() is not True:
        _fail("readonly_snapshot_required")
    actor = await connection.fetchrow(_READER_SQL, profile.owner_role, list(profile.runtime_roles))
    if (
        actor is None
        or actor["login"] != actor["actor"]
        or actor["rolcanlogin"] is not True
        or actor["readonly"] != "on"
        or actor["isolation"] != "repeatable read"
        or actor["replication_role"] != "origin"
        or actor["owner_oid"] is None
        or actor["runtime_role_count"] != len(profile.runtime_roles)
        or any(actor[field] is not True for field in ("safe_search_path", "no_dangerous_role", "admitted_role"))
        or type(actor["observed_at"]) is not datetime
        or actor["observed_at"].utcoffset() is None
    ):
        _fail("reader_unsafe")
    return actor


def _descendants(rows_by_oid, root_oid):
    selected_oids = {root_oid}
    while (
        child_oids := {oid for oid, row in rows_by_oid.items() if set(row["parents"]) & selected_oids} - selected_oids
    ):
        selected_oids.update(child_oids)
    return selected_oids


def _relation_families(rows, roots, profile, owner_oid):
    rows_by_oid = {row["oid"]: row for row in rows}
    roots_by_name = {(row["nspname"], row["relname"]): row for row in rows if not row["parents"]}
    if len(rows_by_oid) != len(rows) or not set(roots) <= set(roots_by_name):
        _fail("physical_identity_invalid")
    for row in rows:
        if (
            row["relkind"] not in {"r", "p"}
            or row["relpersistence"] != "p"
            or row["relrowsecurity"] is not False
            or row["relforcerowsecurity"] is not False
            or row["nspname"] not in {profile.schema_name, profile.catalog_schema}
            or not set(row["parents"]) <= set(rows_by_oid)
            or (bool(row["parents"]) != row["relispartition"])
        ):
            _fail("physical_identity_invalid")
    families_by_name = {name: _descendants(rows_by_oid, roots_by_name[(schema, name)]["oid"]) for schema, name in roots}
    for name in (_HEADER, _RESOURCE):
        if any(rows_by_oid[oid]["relowner"] != owner_oid for oid in families_by_name[name]):
            _fail("physical_owner_changed")
    return rows_by_oid, roots_by_name, families_by_name


async def _physical(connection, profile, actor):
    roots = [
        (profile.schema_name, _HEADER),
        (profile.schema_name, _RESOURCE),
        (profile.catalog_schema, _SOURCE),
        (profile.catalog_schema, _ENDPOINT),
    ]
    names = [storage_relation(*root) for root in roots]
    await connection.execute("LOCK TABLE " + ",".join(names[:2]) + " IN ACCESS SHARE MODE NOWAIT")
    await _pin_registration_catalog(connection, profile)
    catalog_records = await connection.fetch(
        _PHYSICAL_SQL, names, actor["owner_oid"], [actor["reader_oid"]], DEFAULT_BYTES
    )
    if not 4 <= len(catalog_records) <= _MAX_CATALOG_ROWS or any(
        catalog_record["document"] is None for catalog_record in catalog_records
    ):
        _fail("catalog_bounds")
    physical_rows = [
        fhir_source_catalog_row(json.loads(catalog_record["document"])) for catalog_record in catalog_records
    ]
    rows_by_oid, roots_by_name, families_by_name = _relation_families(physical_rows, roots, profile, actor["owner_oid"])
    if (
        await connection.fetchval(
            _ACCESS_SQL, actor["reader_oid"], sorted(rows_by_oid), sorted({profile.schema_name, profile.catalog_schema})
        )
        is not True
    ):
        _fail("reader_write_path")
    leaves = sorted(
        oid
        for oid in families_by_name[_RESOURCE]
        if rows_by_oid[oid]["relkind"] == "r"
        and not any(oid in physical_row["parents"] for physical_row in physical_rows)
    )
    if not leaves:
        _fail("resource_leaves_missing")
    definitions = [
        {field: physical_row[field] for field in _DEFINITION_FIELDS}
        for physical_row in sorted(physical_rows, key=lambda physical_row: physical_row["oid"])
    ]
    return {
        "resource_table_oid": roots_by_name[(profile.schema_name, _RESOURCE)]["oid"],
        "resource_leaf_oids": leaves,
        "header_table_oid": roots_by_name[(profile.schema_name, _HEADER)]["oid"],
        "database_oid": actor["database_oid"],
        "native_catalog_sha256": hashlib.sha256(fhir_catalog_json_bytes(definitions)).hexdigest(),
    }, families_by_name


async def _pin_registration_catalog(connection, profile):
    """Pin catalog relations with column reads instead of requiring table-wide SELECT."""
    async with connection.transaction():
        previous_timeout = await connection.fetchval("SELECT current_setting('lock_timeout')")
        await connection.execute("SET LOCAL lock_timeout='1ms'")
        for table, columns in (
            (_SOURCE, "tableoid,source_id,endpoint_id"),
            (_ENDPOINT, "tableoid,endpoint_id,canonical_api_base"),
        ):
            await connection.fetch(f"SELECT {columns} FROM {storage_relation(profile.catalog_schema, table)} LIMIT 0")
        await connection.fetchval("SELECT set_config('lock_timeout',$1,true)", previous_timeout)


async def _header(connection, profile, endpoint_id, dataset_id, require_current, families):
    relation = storage_relation(profile.schema_name, _HEADER)
    headers = await connection.fetch(
        f"SELECT tableoid::oid::bigint AS oid,dataset_id,endpoint_id,left(dataset_hash,65) AS dataset_hash,status,is_current,"
        "published_at,resource_count,left(previous_dataset_id,97) AS previous_dataset_id,"
        "left(acquisition_root_run_id,65) AS root_run_id,"
        "json_typeof((publication_metadata_json->'resource_hash_contract')::json) AS hash_contract_type,"
        "left(publication_metadata_json->>'resource_hash_contract',129) AS resource_hash_contract,"
        "content_proof_admission_version AS original_admission_version,"
        "left(content_proof_admission_kind,65) AS original_admission_kind,"
        "left(content_proof_admission_sha256,65) AS original_admission_sha256,"
        "left(publication_metadata_sha256,65) AS original_metadata_sha256,"
        f"(SELECT count(*) FROM {relation} current_head WHERE current_head.endpoint_id=$1 AND current_head.is_current) "
        f"AS current_count FROM {relation} WHERE endpoint_id=$1 AND dataset_id=$2 LIMIT 2",
        endpoint_id,
        dataset_id,
    )
    if len(headers) != 1:
        _fail("publication_unavailable")
    header = headers[0]
    if (
        header["oid"] not in families[_HEADER]
        or header["endpoint_id"] != endpoint_id
        or header["dataset_id"] != dataset_id
        or header["status"] not in {"published", "superseded"}
        or type(header["is_current"]) is not bool
        or type(header["published_at"]) is not datetime
        or header["hash_contract_type"] not in {None, "null", "string"}
        or (header["status"] == "superseded" and header["is_current"])
        or (
            require_current
            and (header["status"] != "published" or not header["is_current"] or header["current_count"] != 1)
        )
        or type(header["dataset_hash"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", header["dataset_hash"]) is None
    ):
        _fail("publication_invalid")
    if header["previous_dataset_id"] is not None:
        _text(header["previous_dataset_id"], 96)
    return header


async def _registration(connection, profile, endpoint_id, families):
    endpoints = await connection.fetch(
        "SELECT tableoid::oid::bigint AS oid,endpoint_id,left(canonical_api_base,2049) AS source_uri FROM "
        + storage_relation(profile.catalog_schema, _ENDPOINT)
        + " WHERE endpoint_id=$1 LIMIT 2",
        endpoint_id,
    )
    aliases = await connection.fetch(
        "SELECT tableoid::oid::bigint AS oid,endpoint_id,left(source_id,65) AS source_id FROM "
        + storage_relation(profile.catalog_schema, _SOURCE)
        + ' WHERE endpoint_id=$1 ORDER BY source_id COLLATE "C" LIMIT 1001',
        endpoint_id,
    )
    if (
        len(endpoints) != 1
        or not 1 <= len(aliases) <= _MAX_CATALOG_ROWS
        or endpoints[0]["oid"] not in families[_ENDPOINT]
        or endpoints[0]["endpoint_id"] != endpoint_id
        or any(alias["oid"] not in families[_SOURCE] or alias["endpoint_id"] != endpoint_id for alias in aliases)
    ):
        _fail("registration_unavailable")
    source_aliases = [alias["source_id"] for alias in aliases]
    for alias in source_aliases:
        _text(alias, 64)
    if source_aliases != sorted(set(source_aliases)):
        _fail("registration_invalid")
    source_uri = endpoints[0]["source_uri"]
    _text(source_uri, 2048)
    fhir_source_uri(source_uri)
    return source_uri, source_aliases


async def _facts(connection, profile, dataset_id, native, source_uri, *, published_read=None):
    report = current_fhir_catalog_report()
    if report:
        report.observation_progress(dataset_id, 0)
    resource_relation = storage_relation(profile.schema_name, _RESOURCE)
    table_oid = "tableoid::oid"
    if published_read is not None:
        await published_read.verify(connection, schema_name=profile.schema_name, dataset_id=dataset_id)
        resource_relation = "(" + published_read.resource_sql() + ") AS catalog_resources"
        table_oid = "source_table_oid::oid"
    counts = await connection.fetch(
        f"SELECT resource_type,count(*) AS rows,bool_and({table_oid}=ANY($3::oid[])) AS physical_match FROM "
        + resource_relation
        + ' WHERE dataset_id=$1 AND resource_type=ANY($2::text[]) GROUP BY resource_type ORDER BY resource_type COLLATE "C"',
        dataset_id,
        RESOURCE_TYPES,
        native["resource_leaf_oids"],
    )
    digests = FHIRCatalogObservationDigests()
    async for page in iter_fhir_catalog_raw_pages(
        connection, schema_name=profile.schema_name, dataset_id=dataset_id, source_read=published_read
    ):
        digests.add_page(page, lambda rows: _validate_metadata_page(rows, source_uri))
        if report:
            report.observation_progress(dataset_id, len(page))
    families = digests.document()
    counts_by_type = dict.fromkeys(RESOURCE_TYPES, 0)
    for count in counts:
        if count["physical_match"] is not True or count["resource_type"] not in RESOURCE_TYPES:
            _fail("resource_identity_changed")
        counts_by_type[count["resource_type"]] = count["rows"]
    if any(families[family]["rows"] != counts_by_type[family] for family in RESOURCE_TYPES):
        _fail("resource_count_changed")
    if report:
        report.observation_progress(dataset_id, 0)
    return families


def _validate_metadata_page(page, source_uri):
    metadata_rows = tuple(row for row in page if row["resource_type"] in {"Organization", "InsurancePlan"})
    if metadata_rows:
        deque(iter_fhir_catalog_batches(metadata_rows, profile="published_normalized", source_uri=source_uri), maxlen=0)
    for row in page:
        if row["resource_type"] in {"OrganizationAffiliation", "PractitionerRole"}:
            references = row["payload_json"].get("network_refs")
            if references is None:
                continue
            if type(references) is not list or len(references) > MAX_PAGE_REFERENCES:
                _fail("reference_bounds")
            for reference in references:
                fhir_catalog_text(reference, 2048, strip=False)


async def collect_fhir_catalog_observation(connection, profile, *, endpoint_id, dataset_id, require_current=True):
    """Return observed metadata evidence; preserve the caller's authenticated RO/RR transaction."""
    from process.registry_fhir_catalog_sources import (
        FHIRCatalogSourceDisposition,
        FHIRCatalogSourceReadProfile,
        validate_fhir_catalog_source_descriptor,
    )

    if type(profile) is not FHIRCatalogSourceReadProfile or type(require_current) is not bool:
        _fail()
    _text(endpoint_id, 64)
    _text(dataset_id, 96)
    try:
        actor = await _reader(connection, profile) if profile.published_read_scope is None else None
        if profile.published_read_scope is not None and connection.is_in_transaction() is not True:
            _fail("readonly_snapshot_required")
        async with AsyncExitStack() as reads, connection.transaction():
            reads.enter_context(fhir_catalog_published_reads(connection))
            published_read = None
            if profile.published_read_scope is None:
                native, physical_families = await _physical(connection, profile, actor)
                header = await _header(connection, profile, endpoint_id, dataset_id, require_current, physical_families)
                source_uri, aliases = await _registration(connection, profile, endpoint_id, physical_families)
            else:
                published_read = await pin_fhir_catalog_published_read(
                    connection, profile, endpoint_id=endpoint_id, dataset_id=dataset_id, require_current=require_current
                )
                native, header = published_read.native, published_read.header
                source_uri, aliases = published_read.source_uri, list(published_read.source_aliases)
            families = await _facts(connection, profile, dataset_id, native, source_uri, published_read=published_read)
    except asyncpg.PostgresError as error:
        raise FHIRCatalogObservedSourceError("registry_fhir_catalog_observed_source_read_unavailable") from error
    descriptor_by_field = _descriptor(profile, header, native, (source_uri, aliases), families)
    return FHIRCatalogSourceDisposition(
        endpoint_id,
        dataset_id,
        tuple(aliases),
        len(aliases),
        "accepted",
        None,
        validate_fhir_catalog_source_descriptor(descriptor_by_field),
        datetime.fromisoformat(descriptor_by_field["admission"]["publication"]["published_at"]),
    )


def _receipt(header, families):
    publication_by_field = {
        field: header[field]
        for field in (
            "root_run_id",
            "resource_hash_contract",
            "original_admission_version",
            "original_admission_kind",
            "original_admission_sha256",
            "original_metadata_sha256",
        )
    }
    published_at = header["published_at"]
    published_at = (
        published_at.replace(tzinfo=timezone.utc)
        if published_at.tzinfo is None
        else published_at.astimezone(timezone.utc)
    )
    publication_by_field.update(
        published_at=published_at.isoformat(),
        claimed_resource_count=header["resource_count"],
    )
    return validate_catalog_observation_receipt(
        {
            "version": 1,
            "kind": "catalog_observation",
            "contract": CATALOG_OBSERVATION_CONTRACT,
            "publication": publication_by_field,
            "families": families,
        }
    )


def _descriptor(profile, header, native, registration, families):
    from process.registry_fhir_catalog_sources import PRODUCER

    source_uri, aliases = registration
    dataset_id = header["dataset_id"]
    descriptor_by_field = {
        "coordinates": {
            "source_system": "fhir",
            "source_id": aliases[0],
            "dataset_schema": profile.schema_name,
            "dataset_id": dataset_id,
            "producer_id": PRODUCER,
            "edition_id": dataset_id,
        },
        "profile": "published_normalized",
        "endpoint_id": header["endpoint_id"],
        "dataset_sha256": header["dataset_hash"],
        "source_uri": source_uri,
        "complete": True,
        "previous_dataset_id": header["previous_dataset_id"],
        "source_aliases": aliases,
        "native_read_profile": profile.document(),
        "admission": _receipt(header, families),
        **native,
    }
    digest = catalog_observation_descriptor_digest(descriptor_by_field)
    descriptor_by_field.update(
        accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest,
        accepted_receipt_sha256=digest,
    )
    return descriptor_by_field


# Share observation mechanics without changing either caller's authority checks.
pin_fhir_catalog_registration = _pin_registration_catalog
fhir_catalog_relation_families = _relation_families
read_fhir_catalog_observation_header = _header
read_fhir_catalog_observation_registration = _registration
collect_fhir_catalog_observation_facts = _facts
build_fhir_catalog_observation_descriptor = _descriptor
