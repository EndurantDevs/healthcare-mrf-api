# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe registered source data in a caller-owned read-only snapshot."""

import re
from collections import deque
from datetime import datetime, timezone

import asyncpg

from db.plan_release_storage import storage_relation
from process.provider_directory_source_storage_binding import dataset_source_relations
from process.registry_fhir_catalog_extraction import (
    RESOURCE_TYPES,
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
from process.registry_fhir_catalog_reference_rollup import MAX_PAGE_REFERENCES
from process.registry_fhir_catalog_reporting import current_fhir_catalog_report

_MAX_CATALOG_ROWS = 1000
_HEADER = "provider_directory_endpoint_dataset"
_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"
FHIR_CATALOG_HEADER_TABLE = _HEADER
FHIR_CATALOG_RESOURCE_TABLE = _RESOURCE
FHIR_CATALOG_SOURCE_TABLE = _SOURCE
FHIR_CATALOG_ENDPOINT_TABLE = _ENDPOINT


class FHIRCatalogObservedSourceError(ValueError):
    """Refuse incomplete or inconsistent observed source data."""


def _fail(reason="invalid"):
    raise FHIRCatalogObservedSourceError("registry_fhir_catalog_observed_source_" + reason)


def _text(value, maximum):
    if not value or fhir_catalog_text(value, maximum, strip=False) != value or value.strip() != value:
        _fail()


async def catalog_observation_tables(connection, profile, dataset_id):
    """Resolve the named header and resource relations in the selected catalog snapshot."""
    relations = await dataset_source_relations(connection, profile.catalog_schema, dataset_id, (_RESOURCE,))
    return {"header": {"schema": profile.catalog_schema, "name": _HEADER}, "resource": relations[_RESOURCE]}


async def _header(connection, profile, endpoint_id, dataset_id, require_current):
    relation = storage_relation(profile.catalog_schema, _HEADER)
    headers = await connection.fetch(
        f"SELECT dataset_id,endpoint_id,left(dataset_hash,65) AS dataset_hash,status,is_current,"
        "published_at,resource_count,left(previous_dataset_id,97) AS previous_dataset_id,"
        "left(acquisition_root_run_id,65) AS root_run_id,"
        "json_typeof((publication_metadata_json->'resource_hash_contract')::json) AS hash_contract_type,"
        "left(publication_metadata_json->>'resource_hash_contract',129) AS resource_hash_contract,"
        f"(SELECT count(*) FROM {relation} current_head WHERE current_head.endpoint_id=$1 AND current_head.is_current) "
        f"AS current_count FROM {relation} WHERE endpoint_id=$1 AND dataset_id=$2 LIMIT 2",
        endpoint_id,
        dataset_id,
    )
    if len(headers) != 1:
        _fail("publication_unavailable")
    header = headers[0]
    if (
        header["endpoint_id"] != endpoint_id
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


async def _registration(connection, profile, endpoint_id):
    endpoints = await connection.fetch(
        "SELECT endpoint_id,left(canonical_api_base,2049) AS source_uri FROM "
        + storage_relation(profile.catalog_schema, _ENDPOINT)
        + " WHERE endpoint_id=$1 LIMIT 2",
        endpoint_id,
    )
    aliases = await connection.fetch(
        "SELECT endpoint_id,left(source_id,65) AS source_id FROM "
        + storage_relation(profile.catalog_schema, _SOURCE)
        + ' WHERE endpoint_id=$1 ORDER BY source_id COLLATE "C" LIMIT 1001',
        endpoint_id,
    )
    if (
        len(endpoints) != 1
        or not 1 <= len(aliases) <= _MAX_CATALOG_ROWS
        or endpoints[0]["endpoint_id"] != endpoint_id
        or any(alias["endpoint_id"] != endpoint_id for alias in aliases)
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


async def _facts(connection, profile, dataset_id, tables, source_uri):
    report = current_fhir_catalog_report()
    if report:
        report.observation_progress(dataset_id, 0)
    resource_relation = storage_relation(tables["resource"]["schema"], tables["resource"]["name"])
    counts = await connection.fetch(
        "SELECT resource_type,count(*) AS rows FROM "
        + resource_relation
        + ' WHERE dataset_id=$1 AND resource_type=ANY($2::text[]) GROUP BY resource_type ORDER BY resource_type COLLATE "C"',
        dataset_id,
        RESOURCE_TYPES,
    )
    digests = FHIRCatalogObservationDigests()
    async for page in iter_fhir_catalog_raw_pages(
        connection,
        schema_name=tables["resource"]["schema"],
        dataset_id=dataset_id,
        resource_relation=tables["resource"],
    ):
        digests.add_page(page, lambda rows: _validate_metadata_page(rows, source_uri))
        if report:
            report.observation_progress(dataset_id, len(page))
    families = digests.document()
    counts_by_type = dict.fromkeys(RESOURCE_TYPES, 0)
    for count in counts:
        if count["resource_type"] not in RESOURCE_TYPES or type(count["rows"]) is not int or count["rows"] < 0:
            _fail("resource_count_changed")
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
    """Collect bounded source content while preserving the caller's pinned transaction."""
    from process.registry_fhir_catalog_sources import (
        FHIRCatalogSourceDisposition,
        FHIRCatalogSourceReadProfile,
        require_fhir_catalog_snapshot_transaction,
        validate_fhir_catalog_source_descriptor,
    )

    if type(profile) is not FHIRCatalogSourceReadProfile or type(require_current) is not bool:
        _fail()
    _text(endpoint_id, 64)
    _text(dataset_id, 96)
    try:
        await require_fhir_catalog_snapshot_transaction(connection)
        async with connection.transaction():
            header = await _header(connection, profile, endpoint_id, dataset_id, require_current)
            source_uri, aliases = await _registration(connection, profile, endpoint_id)
            tables = await catalog_observation_tables(connection, profile, dataset_id)
            families = await _facts(connection, profile, dataset_id, tables, source_uri)
    except asyncpg.PostgresError as error:
        raise FHIRCatalogObservedSourceError("registry_fhir_catalog_observed_source_read_unavailable") from error
    descriptor = validate_fhir_catalog_source_descriptor(
        _descriptor(profile, header, tables, (source_uri, aliases), families)
    )
    return FHIRCatalogSourceDisposition(
        endpoint_id,
        dataset_id,
        tuple(aliases),
        len(aliases),
        "accepted",
        None,
        descriptor,
        datetime.fromisoformat(descriptor["admission"]["publication"]["published_at"]),
    )


def _receipt(header, families):
    publication_by_field = {
        field: header[field]
        for field in (
            "root_run_id",
            "resource_hash_contract",
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


def catalog_replacement_scope(descriptor):
    """Replace one catalog source while retaining its separate payload edition names."""
    if "catalog_schema" in descriptor:
        return descriptor["catalog_schema"]
    if "tables" in descriptor:
        return descriptor["tables"]["header"]["schema"]
    return descriptor["coordinates"]["dataset_schema"]


def catalog_replacement_scope_sql(descriptor_sql):
    """Select the same logical replacement scope for named and legacy stored inputs."""
    return (
        f"COALESCE({descriptor_sql}->>'catalog_schema',"
        f"{descriptor_sql}->'tables'->'header'->>'schema',"
        f"{descriptor_sql}->'coordinates'->>'dataset_schema')"
    )


def _descriptor(profile, header, tables, registration, families):
    from process.registry_fhir_catalog_sources import PRODUCER

    source_uri, aliases = registration
    dataset_id = header["dataset_id"]
    descriptor_by_field = {
        "coordinates": {
            "source_system": "fhir",
            "source_id": aliases[0],
            "dataset_schema": tables["resource"]["schema"],
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
        "tables": tables,
    }
    digest = catalog_observation_descriptor_digest(descriptor_by_field)
    descriptor_by_field.update(
        accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest,
        accepted_receipt_sha256=digest,
    )
    return descriptor_by_field


read_fhir_catalog_observation_header = _header
read_fhir_catalog_observation_registration = _registration
collect_fhir_catalog_observation_facts = _facts
build_fhir_catalog_observation_descriptor = _descriptor
