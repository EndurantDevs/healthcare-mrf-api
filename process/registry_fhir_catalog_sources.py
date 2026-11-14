# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded registered FHIR source inventory and logical content observations."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    RESOURCE_TYPES,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
    fhir_source_uri,
)
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    catalog_observation_descriptor_digest,
    validate_catalog_observation_receipt,
)

PRODUCER = "registry-fhir-catalog-v1"
MAX_PAGE = 1000
MAX_ALIASES = 1000
_HEADER_TABLE = "provider_directory_endpoint_dataset"
_HEADER_FIELDS = (
    "dataset_id",
    "endpoint_id",
    "dataset_hash",
    "status",
    "is_current",
    "published_at",
    "resource_count",
    "previous_dataset_id",
)


class FHIRCatalogSourceError(ValueError):
    """Refuse malformed source coordinates or inconsistent observed content."""


@dataclass(frozen=True)
class FHIRCatalogSourceReadProfile:
    """Configured storage and registration namespaces for catalog data reads."""

    schema_name: str
    catalog_schema_name: str | None = None

    def __post_init__(self):
        _identifier(self.schema_name)
        _identifier(self.catalog_schema)

    @property
    def catalog_schema(self):
        """Use the configured registration namespace, falling back to the data namespace."""
        return self.schema_name if self.catalog_schema_name is None else self.catalog_schema_name

    def document(self):
        """Export only portable named storage configuration."""
        return {"schema_name": self.schema_name, "catalog_schema_name": self.catalog_schema}


@dataclass(frozen=True)
class FHIRCatalogSourceDisposition:
    endpoint_id: str
    dataset_id: str | None
    source_aliases: tuple[str, ...]
    alias_count: int
    status: str
    reason: str | None
    descriptor: dict | None
    accepted_at: datetime | None


def _fail(reason="invalid"):
    raise FHIRCatalogSourceError("registry_fhir_catalog_source_" + reason)


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) is None:
        _fail()
    return '"' + value + '"'


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        _fail()
    return value


def _bounded_text(value, maximum):
    if not value or fhir_catalog_text(value, maximum, strip=False) != value or value.strip() != value:
        _fail()
    return value


def _strings(values, maximum, *, count=MAX_ALIASES):
    if type(values) is not list or not 1 <= len(values) <= count:
        _fail()
    if any(_bounded_text(value, maximum) != value for value in values) or sorted(set(values)) != values:
        _fail()
    return values


def parse_fhir_catalog_source_profile(document):
    """Read named source configuration, ignoring retired physical policy fields."""
    required_fields = {"schema_name"}
    allowed = required_fields | {"catalog_schema_name", "owner_role", "runtime_roles", "published_read_scope"}
    if type(document) is not dict or not required_fields <= set(document) or not set(document) <= allowed:
        _fail("profile_invalid")
    return FHIRCatalogSourceReadProfile(document["schema_name"], document.get("catalog_schema_name"))


_profile = parse_fhir_catalog_source_profile


def validate_fhir_catalog_source_descriptor(descriptor):
    """Validate logical source coordinates, named inputs and content observation evidence."""
    if type(descriptor) is dict and descriptor.get("profile") == "retained_normalized":
        from process.registry_fhir_catalog_retained import validate_retained_fhir_catalog_descriptor

        return validate_retained_fhir_catalog_descriptor(descriptor)
    normalized_descriptor, is_legacy = _normalize_fhir_catalog_descriptor(descriptor)
    descriptor_by_field = json.loads(fhir_catalog_json_bytes(normalized_descriptor))
    coordinates = RegistryNetworkSourceCoordinates(**descriptor_by_field["coordinates"])
    profile = _profile(descriptor_by_field["native_read_profile"])
    descriptor_by_field["native_read_profile"] = profile.document()
    aliases = _strings(descriptor_by_field["source_aliases"], 64)
    if (
        coordinates.source_system != "fhir"
        or coordinates.producer_id != PRODUCER
        or descriptor_by_field["profile"] != "published_normalized"
        or coordinates.source_id != aliases[0]
        or coordinates.edition_id != coordinates.dataset_id
        or type(descriptor_by_field["complete"]) is not bool
    ):
        _fail()
    _bounded_text(descriptor_by_field["endpoint_id"], 64)
    _bounded_text(descriptor_by_field["accepted_receipt_id"], 128)
    fhir_source_uri(descriptor_by_field["source_uri"])
    for field in ("dataset_sha256", "accepted_receipt_sha256"):
        _digest(descriptor_by_field[field])
    if descriptor_by_field["previous_dataset_id"] is not None:
        _bounded_text(descriptor_by_field["previous_dataset_id"], 96)
    _validate_catalog_source_tables(descriptor_by_field["tables"], coordinates, profile)
    if (
        type(descriptor_by_field["admission"]) is dict
        and descriptor_by_field["admission"].get("kind") == "catalog_observation"
    ):
        descriptor_by_field["admission"] = validate_catalog_observation_receipt(descriptor_by_field["admission"])
        if descriptor_by_field["complete"] is not True:
            _fail("observation_invalid")
        content_digest = catalog_observation_descriptor_digest(descriptor_by_field)
        if is_legacy:
            descriptor_by_field.update(
                accepted_receipt_sha256=content_digest,
                accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + content_digest,
            )
        elif (
            descriptor_by_field["accepted_receipt_sha256"] != content_digest
            or descriptor_by_field["accepted_receipt_id"] != CATALOG_OBSERVATION_CONTRACT + ":" + content_digest
        ):
            _fail("observation_invalid")
    else:
        _validate_content_admission(descriptor_by_field["admission"], descriptor_by_field)
    return descriptor_by_field


def _normalize_fhir_catalog_descriptor(descriptor_by_field):
    """Project only the complete known legacy wire shape before validating named inputs."""
    required_fields = {
        "coordinates",
        "profile",
        "endpoint_id",
        "dataset_sha256",
        "source_uri",
        "accepted_receipt_id",
        "accepted_receipt_sha256",
        "complete",
        "previous_dataset_id",
        "source_aliases",
        "native_read_profile",
        "admission",
        "tables",
    }
    physical_fields = {
        "resource_table_oid",
        "resource_leaf_oids",
        "header_table_oid",
        "database_oid",
        "native_catalog_sha256",
    }
    legacy_fields = required_fields - {"tables"} | physical_fields
    is_legacy = type(descriptor_by_field) is dict and set(descriptor_by_field) == legacy_fields
    schema_fields = required_fields - {"tables"} | {"catalog_schema"}
    is_schema_descriptor = (
        type(descriptor_by_field) is dict and set(descriptor_by_field) - physical_fields == schema_fields
    )
    if is_schema_descriptor:
        descriptor_by_field = _project_catalog_schema_descriptor(descriptor_by_field, physical_fields)
        is_legacy = True
    elif is_legacy:
        descriptor_by_field = _project_legacy_catalog_descriptor(descriptor_by_field, physical_fields)
    if type(descriptor_by_field) is not dict or set(descriptor_by_field) != required_fields:
        _fail()
    descriptor_by_field = {
        **descriptor_by_field,
        "native_read_profile": _profile(descriptor_by_field["native_read_profile"]).document(),
    }
    if (
        is_legacy
        and type(descriptor_by_field["admission"]) is dict
        and descriptor_by_field["admission"].get("kind") == "catalog_observation"
    ):
        projected_digest = catalog_observation_descriptor_digest(descriptor_by_field)
        descriptor_by_field.update(
            accepted_receipt_sha256=projected_digest,
            accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + projected_digest,
        )
    return descriptor_by_field, is_legacy


def _project_catalog_schema_descriptor(descriptor_by_field, physical_fields):
    """Read the known catalog-schema observation form through its fixed model names."""
    catalog_schema = descriptor_by_field["catalog_schema"]
    _identifier(catalog_schema)
    profile = _profile(descriptor_by_field["native_read_profile"])
    if (
        catalog_schema != profile.catalog_schema
        or type(descriptor_by_field["admission"]) is not dict
        or descriptor_by_field["admission"].get("kind") != "catalog_observation"
    ):
        _fail("observation_invalid")
    projected_by_field = _project_legacy_catalog_descriptor(
        {key: value for key, value in descriptor_by_field.items() if key != "catalog_schema"}, physical_fields
    )
    projected_by_field["tables"]["header"]["schema"] = catalog_schema
    return projected_by_field


def _project_legacy_catalog_descriptor(descriptor_by_field, physical_fields):
    """Remove retired physical fields while preserving the closed content-admission contract."""
    descriptor_by_field = {
        key: field_value for key, field_value in descriptor_by_field.items() if key not in physical_fields
    }
    profile = _profile(descriptor_by_field["native_read_profile"])
    descriptor_by_field["tables"] = {
        "header": {"schema": profile.schema_name, "name": _HEADER_TABLE},
        "resource": {
            "schema": descriptor_by_field["coordinates"]["dataset_schema"],
            "name": "provider_directory_dataset_resource",
        },
    }
    admission = descriptor_by_field["admission"]
    if type(admission) is dict and admission.get("kind") == "catalog_observation":
        publication = admission.get("publication")
        fields = {"root_run_id", "published_at", "resource_hash_contract", "claimed_resource_count"}
        retired_fields = {
            "original_admission_version",
            "original_admission_kind",
            "original_admission_sha256",
            "original_metadata_sha256",
        }
        if type(publication) is dict and set(publication) == fields | retired_fields:
            descriptor_by_field["admission"] = {
                **admission,
                "publication": {key: field_value for key, field_value in publication.items() if key in fields},
            }
    return descriptor_by_field


def _validate_catalog_source_tables(tables, coordinates, profile):
    """Require qualified named inputs to match the declared source coordinates."""
    if type(tables) is not dict or set(tables) != {"header", "resource"}:
        _fail()
    for relation in tables.values():
        if type(relation) is not dict or set(relation) != {"schema", "name"}:
            _fail()
        _identifier(relation["schema"])
        _identifier(relation["name"])
    if (
        tables["header"]
        not in (
            {"schema": profile.catalog_schema, "name": _HEADER_TABLE},
            {"schema": profile.schema_name, "name": _HEADER_TABLE},
        )
        or tables["resource"]["schema"] != coordinates.dataset_schema
    ):
        _fail()


def _validate_content_admission(admission, descriptor):
    """Read retained content-admission metadata without native custody declarations."""
    fields = {
        "version",
        "kind",
        "proof_sha256",
        "metadata_sha256",
        "resource_count",
        "resource_counts",
        "resource_types",
        "selected_resources",
        "completion_required_version",
        "completion_sha256",
        "proof_resource_scope",
    }
    if type(admission) is not dict or set(admission) != fields:
        _fail("admission_invalid")
    if (
        type(admission["version"]) is not int
        or admission["version"] != 1
        or type(admission["kind"]) is not str
        or admission["kind"] not in {"generic", "uhc_canonical"}
    ):
        _fail("admission_invalid")
    for field in ("proof_sha256", "metadata_sha256"):
        _digest(admission[field])
    if admission["metadata_sha256"] != descriptor["accepted_receipt_sha256"]:
        _fail("admission_invalid")
    count = admission["resource_count"]
    if type(count) is not int or not 0 <= count < 2**63:
        _fail("admission_invalid")
    _validate_content_admission_scope(admission, descriptor, count)


def _validate_content_admission_scope(admission, descriptor, count):
    """Check resource accounting, completeness, canonical scope and completion evidence together."""
    resources = _strings(admission["resource_types"], 64, count=64)
    counts = admission["resource_counts"]
    if (
        type(counts) is not dict
        or sorted(counts) != resources
        or any(type(resource_count) is not int or resource_count < 0 for resource_count in counts.values())
        or sum(counts.values()) != count
    ):
        _fail("admission_invalid")
    selected = _strings(admission["selected_resources"], 64, count=64)
    scope = admission["proof_resource_scope"]
    if scope is not None:
        _strings(scope, 64, count=64)
    covered = selected if scope is None else scope
    if (
        not set(resources) <= set(selected)
        or not set(covered) <= set(selected)
        or descriptor["complete"] != (set(RESOURCE_TYPES) <= set(covered))
    ):
        _fail("admission_invalid")
    if admission["kind"] == "uhc_canonical":
        from process.provider_directory_source_summary import SOURCE_SUMMARY_UHC_SELECTED_RESOURCES
        from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

        if (
            descriptor["source_aliases"] != [UHC_PROVIDER_FILE_SOURCE_ID]
            or resources != list(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES)
            or selected != list(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES)
            or scope is not None
        ):
            _fail("admission_invalid")
    version, completion = admission["completion_required_version"], admission["completion_sha256"]
    if version is None:
        if completion is not None:
            _fail("admission_invalid")
    elif type(version) is not int or version != 3:
        _fail("admission_invalid")
    else:
        _digest(completion)


def fhir_catalog_source_generation(descriptor):
    """Hash stable accepted evidence; freshness timestamps remain outside this identity."""
    return fhir_catalog_source_digest(validate_fhir_catalog_source_descriptor(descriptor))


def fhir_catalog_source_digest(descriptor):
    """Hash already-validated source evidence independently of its manifest locator."""
    identity_by_field = dict(descriptor)
    if identity_by_field["profile"] == "retained_normalized":
        identity_by_field.pop("retained_manifest", None)
    return hashlib.sha256(fhir_catalog_json_bytes(identity_by_field)).hexdigest()


async def _transaction(connection):
    if not connection.is_in_transaction():
        _fail("transaction_required")
    if await connection.fetchval("SELECT current_setting('transaction_isolation')") not in {
        "repeatable read",
        "serializable",
    }:
        _fail("transaction_required")


def _header_query(schema):
    columns = ",".join("d." + field for field in _HEADER_FIELDS)
    return f"SELECT {columns} FROM {_identifier(schema)}.{_HEADER_TABLE} d JOIN endpoint_page p USING(endpoint_id) WHERE d.is_current"


_PAGE_SQL = """WITH endpoint_page AS MATERIALIZED (
 SELECT e.endpoint_id FROM {catalog}.provider_directory_api_endpoint e
 WHERE e.endpoint_id>$1::text AND ($5::text[] IS NULL OR e.endpoint_id=ANY($5::text[]))
 AND EXISTS(SELECT FROM {catalog}.provider_directory_source s WHERE s.endpoint_id=e.endpoint_id)
 ORDER BY e.endpoint_id LIMIT $2
), registered AS MATERIALIZED (
 SELECT s.endpoint_id,s.source_id,row_number() OVER(PARTITION BY s.endpoint_id ORDER BY s.source_id COLLATE "C") AS ordinal,
 count(*) OVER(PARTITION BY s.endpoint_id) AS alias_count
 FROM {catalog}.provider_directory_source s JOIN endpoint_page p USING(endpoint_id)
), aliases AS (
 SELECT endpoint_id,array_agg(source_id ORDER BY source_id COLLATE "C") FILTER(WHERE ordinal<=$3) AS source_aliases,
 max(alias_count) AS alias_count FROM registered GROUP BY endpoint_id
), headers AS MATERIALIZED ({headers}), ranked AS (
 SELECT *,count(*) OVER(PARTITION BY endpoint_id) AS current_count,
 row_number() OVER(PARTITION BY endpoint_id ORDER BY dataset_id COLLATE "C") AS preference FROM headers
), inventory AS MATERIALIZED (
 SELECT p.endpoint_id,a.source_aliases,a.alias_count,d.dataset_id,d.status,d.is_current,d.current_count,
 octet_length(a.source_aliases::text)+4096 AS row_bytes
 FROM endpoint_page p JOIN aliases a USING(endpoint_id) LEFT JOIN ranked d ON d.endpoint_id=p.endpoint_id AND d.preference=1
), bounded AS (
 SELECT *,sum(row_bytes) OVER(ORDER BY endpoint_id) AS page_bytes,
 row_number() OVER(ORDER BY endpoint_id) AS ordinal FROM inventory
) SELECT endpoint_id,source_aliases,alias_count,dataset_id,status,is_current,current_count,row_bytes
 FROM bounded WHERE ordinal=1 OR page_bytes<=$4 ORDER BY endpoint_id"""


def _page_query(profile):
    headers = _header_query(profile.catalog_schema)
    return _PAGE_SQL.format(catalog=_identifier(profile.catalog_schema), headers=headers)


async def _source_pages(connection, profile, page_size, page_bytes, after_endpoint=None, endpoint_ids=None):
    query, cursor, seen = _page_query(profile), after_endpoint or "", set()
    while True:
        records = await connection.fetch(query, cursor, page_size, MAX_ALIASES, page_bytes, endpoint_ids)
        if not records:
            if endpoint_ids is not None and seen != set(endpoint_ids):
                _fail("endpoint_unavailable")
            return
        ids = [record["endpoint_id"] for record in records]
        if len(records) > page_size or ids != sorted(set(ids)) or ids[0] <= cursor:
            _fail("inventory_order_invalid")
        if endpoint_ids is not None and not set(ids) <= set(endpoint_ids):
            _fail("endpoint_scope_changed")
        seen.update(ids)
        yield tuple([await _observed_disposition(connection, profile, row, page_bytes) for row in records])
        cursor = ids[-1]


async def iter_fhir_catalog_sources(
    connection,
    profile,
    *,
    page_size=100,
    page_bytes=DEFAULT_BYTES,
    after_endpoint=None,
    observation_contract=None,
    endpoint_ids=None,
):
    """Inventory registered endpoint editions through bounded pages and genuine EOF."""
    if type(profile) is not FHIRCatalogSourceReadProfile:
        _fail("profile_required")
    if observation_contract not in (None, CATALOG_OBSERVATION_CONTRACT):
        _fail("observation_contract_invalid")
    if (
        type(page_size) is not int
        or not 1 <= page_size <= MAX_PAGE
        or type(page_bytes) is not int
        or not 1 <= page_bytes <= MAX_BYTES
    ):
        _fail("bounds_invalid")
    if after_endpoint is not None:
        _bounded_text(after_endpoint, 64)
    if endpoint_ids is not None:
        if type(endpoint_ids) not in {tuple, list} or not endpoint_ids or len(endpoint_ids) > MAX_PAGE:
            _fail("endpoint_scope_invalid")
        for endpoint in endpoint_ids:
            _bounded_text(endpoint, 64)
        if len(set(endpoint_ids)) != len(endpoint_ids):
            _fail("endpoint_scope_invalid")
    await _transaction(connection)
    async for page in _source_pages(connection, profile, page_size, page_bytes, after_endpoint, endpoint_ids):
        yield page


def _unavailable_catalog_source_reason(row, page_bytes):
    """Classify bounded inventory failures before opening the source observation savepoint."""
    reason = None
    if row["dataset_id"] is None:
        reason = "current_dataset_missing"
    elif row["alias_count"] > MAX_ALIASES:
        reason = "source_alias_bounds"
    elif row["row_bytes"] > page_bytes:
        reason = "source_page_bounds"
    elif row["current_count"] != 1:
        reason = "current_dataset_ambiguous"
    return reason


async def _observed_disposition(connection, profile, row, page_bytes=DEFAULT_BYTES):
    from process.registry_fhir_catalog_observed_source import collect_fhir_catalog_observation

    reason = _unavailable_catalog_source_reason(row, page_bytes)
    if reason is None:
        try:
            async with connection.transaction():
                return await collect_fhir_catalog_observation(
                    connection, profile, endpoint_id=row["endpoint_id"], dataset_id=row["dataset_id"]
                )
        except ValueError, KeyError, TypeError:
            reason = "catalog_observation_unavailable"
    return FHIRCatalogSourceDisposition(
        row["endpoint_id"],
        row["dataset_id"],
        tuple(row["source_aliases"]),
        row["alias_count"],
        "unavailable",
        reason,
        None,
        None,
    )


async def require_current_fhir_catalog_sources(connection, descriptors, *, lock=False):
    """Recheck exact logical publication data in the caller's selected snapshot."""
    if type(descriptors) is not tuple or not 1 <= len(descriptors) <= MAX_PAGE or type(lock) is not bool:
        _fail("bounds_invalid")
    await _transaction(connection)
    from process.registry_fhir_catalog_observed_source import collect_fhir_catalog_observation

    generations = []
    for descriptor in descriptors:
        selected = validate_fhir_catalog_source_descriptor(descriptor)
        observed = await collect_fhir_catalog_observation(
            connection,
            _profile(selected["native_read_profile"]),
            endpoint_id=selected["endpoint_id"],
            dataset_id=selected["coordinates"]["dataset_id"],
            require_current=True,
        )
        if observed.descriptor != selected:
            _fail("source_changed")
        generations.append(fhir_catalog_source_generation(selected))
    return tuple(generations)


require_fhir_catalog_snapshot_transaction = _transaction
build_fhir_catalog_inventory_query = _page_query
bounded_fhir_catalog_source_text = _bounded_text
