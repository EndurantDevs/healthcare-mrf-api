# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Paged registered FHIR inventory with existing native Source and admission proofs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import asyncpg

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    ADMISSION_SEAL_CONTRACT,
    AdmissionSealError,
    parse_source_admission_receipt,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_provision import (
    fhir_source_catalog_row,
    verify_generic_source_read_family,
)
from process.provider_directory_generic_source_storage import validate_handoff_completion
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
from process.registry_fhir_catalog_published_read import (
    FHIRCatalogPublishedReadScope,
    parse_fhir_catalog_published_read_scope,
    validate_fhir_catalog_published_descriptor,
)

PRODUCER = "registry-fhir-catalog-v1"
MAX_PAGE = 1000
MAX_ALIASES = 1000
_RESOURCE_TABLE = "provider_directory_dataset_resource"
_HEADER_TABLE = "provider_directory_endpoint_dataset"
_SOURCE_TABLE = "provider_directory_source"
_ENDPOINT_TABLE = "provider_directory_api_endpoint"
_PROOF_LIMIT = ADMISSION_METADATA_SUMMARY_MAX_BYTES
FHIR_CATALOG_PROOF_MAX_BYTES = _PROOF_LIMIT
_SUMMARY_FIELDS = {"dataset_hash", "resource_count", "resource_hashes", "resource_counts"}
_DESCRIPTOR_FIELDS = {
    "coordinates",
    "profile",
    "endpoint_id",
    "dataset_sha256",
    "resource_table_oid",
    "resource_leaf_oids",
    "header_table_oid",
    "database_oid",
    "source_uri",
    "accepted_receipt_id",
    "accepted_receipt_sha256",
    "complete",
    "previous_dataset_id",
    "source_aliases",
    "native_read_profile",
    "native_catalog_sha256",
    "admission",
}
_ADMISSION_FIELDS = {
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
_HEADER_FIELDS = (
    "dataset_id",
    "endpoint_id",
    "dataset_hash",
    "status",
    "is_current",
    "published_at",
    "resource_count",
    "previous_dataset_id",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
    "publication_metadata_sha256",
    "publication_metadata_summary_json",
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
)


class FHIRCatalogSourceError(ValueError):
    """Refuse an unproved source or changed selected physical dataset."""


@dataclass(frozen=True)
class FHIRCatalogSourceReadProfile:
    """Explicit standing-grant input; native ACL observations never create this policy."""

    schema_name: str
    owner_role: str
    runtime_roles: tuple[str, ...]
    catalog_schema_name: str | None = None
    published_read_scope: FHIRCatalogPublishedReadScope | None = None

    def __post_init__(self):
        if type(self.runtime_roles) is not tuple or not 1 <= len(self.runtime_roles) <= 64:
            _fail("profile_invalid")
        for identifier in (
            self.schema_name,
            self.owner_role,
            self.catalog_schema_name or self.schema_name,
            *self.runtime_roles,
        ):
            _identifier(identifier)
        if tuple(sorted(set(self.runtime_roles))) != self.runtime_roles or self.owner_role in self.runtime_roles:
            _fail("profile_invalid")
        if (
            self.published_read_scope is not None
            and type(self.published_read_scope) is not FHIRCatalogPublishedReadScope
        ):
            _fail("profile_invalid")
        if self.published_read_scope is not None:
            scope = self.published_read_scope.document()
            if scope["reader_role"] in (self.owner_role, *self.runtime_roles):
                _fail("profile_invalid")
            pins = (
                (scope["header"]["relation"], self.schema_name, _HEADER_TABLE),
                (scope["resource_root"], self.schema_name, _RESOURCE_TABLE),
                (
                    scope["bindings"]["relation"],
                    self.schema_name,
                    "provider_directory_insurance_network_source_binding",
                ),
                (scope["source_registration"], self.catalog_schema, _SOURCE_TABLE),
                (scope["endpoint_registration"], self.catalog_schema, _ENDPOINT_TABLE),
            )
            if any(pin["schema_name"] != schema or pin["relation_name"] != name for pin, schema, name in pins):
                _fail("profile_invalid")

    @property
    def catalog_schema(self):
        """Return the separately supplied registration catalog namespace."""
        return self.catalog_schema_name or self.schema_name

    def document(self):
        """Serialize the explicit read profile for standing-grant comparison."""
        document_by_field = {
            "schema_name": self.schema_name,
            "owner_role": self.owner_role,
            "runtime_roles": list(self.runtime_roles),
            "catalog_schema_name": self.catalog_schema,
        }
        if self.published_read_scope is not None:
            document_by_field["published_read_scope"] = self.published_read_scope.document()
        return document_by_field


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


def _positive(value):
    if type(value) is not int or not 0 < value < 2**32:
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


def _resource_scope(values):
    if type(values) is not list or any(type(value) is not str for value in values):
        _fail("admission_invalid")
    return _strings(sorted(values), 64, count=64)


def parse_fhir_catalog_source_profile(document):
    """Decode the closed native reader profile and reject malformed runtime roles."""
    fields = {
        "schema_name",
        "owner_role",
        "runtime_roles",
        "catalog_schema_name",
    }
    if type(document) is not dict or set(document) not in (fields, fields | {"published_read_scope"}):
        _fail()
    if type(document["runtime_roles"]) is not list:
        _fail()
    return FHIRCatalogSourceReadProfile(
        document["schema_name"],
        document["owner_role"],
        tuple(document["runtime_roles"]),
        document["catalog_schema_name"],
        parse_fhir_catalog_published_read_scope(document["published_read_scope"])
        if "published_read_scope" in document
        else None,
    )


_profile = parse_fhir_catalog_source_profile


def validate_fhir_catalog_source_descriptor(descriptor):
    """Validate linkage only; native recapture and the standing grant confer authority."""
    if type(descriptor) is dict and descriptor.get("profile") == "retained_normalized":
        from process.registry_fhir_catalog_retained import validate_retained_fhir_catalog_descriptor

        try:
            return validate_retained_fhir_catalog_descriptor(descriptor)
        except ValueError, TypeError, KeyError:
            _fail("retained_invalid")
    if type(descriptor) is not dict or set(descriptor) != _DESCRIPTOR_FIELDS:
        _fail()
    coordinates = descriptor["coordinates"]
    if type(coordinates) is not dict:
        _fail()
    coordinates = RegistryNetworkSourceCoordinates(**coordinates)
    profile = _profile(descriptor["native_read_profile"])
    aliases = _strings(descriptor["source_aliases"], 64)
    if coordinates.source_system != "fhir" or coordinates.producer_id != PRODUCER:
        _fail()
    if descriptor["profile"] != "published_normalized" or coordinates.dataset_schema != profile.schema_name:
        _fail()
    if coordinates.source_id != aliases[0] or coordinates.edition_id != coordinates.dataset_id:
        _fail()
    _bounded_text(descriptor["endpoint_id"], 64)
    if descriptor["source_uri"] is None:
        _fail()
    fhir_source_uri(descriptor["source_uri"])
    for field in ("dataset_sha256", "accepted_receipt_sha256", "native_catalog_sha256"):
        _digest(descriptor[field])
    for field in ("resource_table_oid", "header_table_oid", "database_oid"):
        _positive(descriptor[field])
    leaves = descriptor["resource_leaf_oids"]
    if type(leaves) is not list or not leaves or len(leaves) > MAX_PAGE or sorted(set(leaves)) != leaves:
        _fail()
    for oid in leaves:
        _positive(oid)
    has_observation_receipt = (
        type(descriptor["admission"]) is dict and descriptor["admission"].get("kind") == "catalog_observation"
    )
    expected_receipt = (
        CATALOG_OBSERVATION_CONTRACT + ":" + descriptor["accepted_receipt_sha256"]
        if has_observation_receipt
        else ADMISSION_SEAL_CONTRACT + ":" + coordinates.dataset_id
    )
    if descriptor["accepted_receipt_id"] != expected_receipt:
        _fail()
    if type(descriptor["complete"]) is not bool:
        _fail()
    if descriptor["previous_dataset_id"] is not None:
        _bounded_text(descriptor["previous_dataset_id"], 96)
    _validate_admission_descriptor(descriptor["admission"], descriptor)
    if profile.published_read_scope is not None:
        validate_fhir_catalog_published_descriptor(profile, descriptor)
    return json.loads(fhir_catalog_json_bytes(descriptor))


def _validate_admission_descriptor(admission, descriptor):
    if type(admission) is dict and admission.get("kind") == "catalog_observation":
        try:
            validate_catalog_observation_receipt(admission)
        except ValueError:
            _fail("observation_invalid")
        if descriptor["complete"] is not True or (
            descriptor["accepted_receipt_sha256"] != catalog_observation_descriptor_digest(descriptor)
        ):
            _fail("observation_invalid")
        return
    if type(admission) is not dict or set(admission) != _ADMISSION_FIELDS:
        _fail()
    if type(admission["version"]) is not int or admission["version"] != 1 or admission["kind"] != "generic":
        _fail()
    for field in ("proof_sha256", "metadata_sha256"):
        _digest(admission[field])
    if admission["metadata_sha256"] != descriptor["accepted_receipt_sha256"]:
        _fail()
    if type(admission["resource_count"]) is not int or not 0 <= admission["resource_count"] < 2**63:
        _fail()
    resources = _strings(admission["resource_types"], 64, count=64)
    counts = admission["resource_counts"]
    if type(counts) is not dict or sorted(counts) != resources:
        _fail()
    if (
        any(type(count) is not int or count < 0 for count in counts.values())
        or sum(counts.values()) != admission["resource_count"]
    ):
        _fail()
    selected = _strings(admission["selected_resources"], 64, count=64)
    scope = admission["proof_resource_scope"]
    if scope is not None:
        _strings(scope, 64, count=64)
    covered = selected if scope is None else scope
    if not set(resources) <= set(selected) or not set(covered) <= set(selected):
        _fail()
    if descriptor["complete"] != (set(RESOURCE_TYPES) <= set(covered)):
        _fail()
    version, digest = admission["completion_required_version"], admission["completion_sha256"]
    if version is None:
        if digest is not None:
            _fail()
    elif type(version) is not int or version != 3:
        _fail()
    else:
        _digest(digest)


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


async def _catalog_access(connection, profile):
    if profile.published_read_scope is not None:
        return await _scoped_catalog_access(connection, profile)
    relations = (
        (profile.schema_name, _HEADER_TABLE),
        (profile.catalog_schema, _HEADER_TABLE),
        (profile.catalog_schema, _SOURCE_TABLE),
        (profile.catalog_schema, _ENDPOINT_TABLE),
    )
    names = [f"{schema}.{relation}" for schema, relation in relations]
    access_records = await connection.fetch(
        "SELECT name,relation_oid::oid::bigint AS oid,"
        "coalesce(CASE WHEN name=$2 THEN "
        "has_column_privilege(current_user,relation_oid,'source_id','SELECT') AND "
        "has_column_privilege(current_user,relation_oid,'endpoint_id','SELECT') "
        "WHEN name=$3 THEN has_column_privilege(current_user,relation_oid,'endpoint_id','SELECT') AND "
        "has_column_privilege(current_user,relation_oid,'canonical_api_base','SELECT') "
        "ELSE has_table_privilege(current_user,relation_oid,'SELECT') END,false) AS readable "
        "FROM jsonb_to_recordset($1::jsonb) names(name text,schema_name text,relation_name text) "
        "CROSS JOIN LATERAL (SELECT pg_catalog.to_regclass("
        "pg_catalog.format('%I.%I',schema_name,relation_name)) AS relation_oid) resolved",
        json.dumps(
            [
                {"name": name, "schema_name": schema, "relation_name": relation}
                for name, (schema, relation) in zip(names, relations)
            ]
        ),
        names[2],
        names[3],
    )
    access_by_name = {access_record["name"]: dict(access_record) for access_record in access_records}
    if any(not access_by_name[name]["readable"] for name in names[2:]):
        _fail("catalog_unavailable")
    return access_by_name


async def _scoped_catalog_access(connection, profile):
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_inventory

    await pin_fhir_catalog_published_inventory(connection, profile)
    scope = profile.published_read_scope.document()
    header = scope["header"]["relation"]
    reads = (
        (
            header,
            (
                "dataset_id",
                "endpoint_id",
                "dataset_hash",
                "status",
                "is_current",
                "published_at",
                "resource_count",
                "previous_dataset_id",
            ),
        ),
        (scope["source_registration"], ("source_id", "endpoint_id")),
        (scope["endpoint_registration"], ("endpoint_id", "canonical_api_base")),
    )
    access_records = await connection.fetch(
        "SELECT p.name,p.relation_oid AS oid,"
        "pg_catalog.to_regclass(pg_catalog.format('%I.%I',p.schema_name,p.relation_name))::oid=p.relation_oid::oid "
        "AND NOT EXISTS(SELECT FROM unnest(p.columns) field(column_name) WHERE NOT coalesce("
        "pg_catalog.has_column_privilege(current_user,p.relation_oid::oid,field.column_name,'SELECT'),false)) AS readable "
        "FROM jsonb_to_recordset($1::jsonb) "
        "p(name text,schema_name text,relation_name text,relation_oid bigint,columns text[])",
        json.dumps(
            [
                {
                    "name": pin["schema_name"] + "." + pin["relation_name"],
                    "schema_name": pin["schema_name"],
                    "relation_name": pin["relation_name"],
                    "relation_oid": pin["relation_oid"],
                    "columns": list(columns),
                }
                for pin, columns in reads
            ]
        ),
    )
    access_by_name = {access_record["name"]: dict(access_record) for access_record in access_records}
    if len(access_by_name) != 3 or any(access_record["readable"] is not True for access_record in access_records):
        _fail("catalog_unavailable")
    return access_by_name


async def _native_source(connection, profile, access):
    """Reuse existing whole-family reader/ACL checks, without inferring their policy."""
    if profile.published_read_scope is not None:
        # The Source owner must qualify its separate published-reader ACL contract.
        return None, "scoped_semantic_compatibility_required"
    source_header = f"{profile.schema_name}.{_HEADER_TABLE}"
    if not access[source_header]["readable"]:
        return None, "source_namespace_unavailable"
    try:
        async with connection.transaction():
            family = await verify_generic_source_read_family(
                connection, profile.schema_name, profile.owner_role, profile.runtime_roles
            )
            roots = {_RESOURCE_TABLE, "provider_directory_insurance_network_source_binding"}
            source_roots = [
                fhir_source_catalog_row(catalog_row) for catalog_row in family if catalog_row["relname"] in roots
            ]
            if {catalog_row["relname"] for catalog_row in source_roots} != roots:
                _fail("native_family_unsupported")
            owner = source_roots[0]["relowner"]
            readers = await connection.fetch(
                "SELECT oid::bigint FROM pg_roles WHERE rolname=ANY($1::text[])", list(profile.runtime_roles)
            )
            header_rows = await connection.fetch(
                CATALOG_SQL, [source_header], owner, [catalog_row["oid"] for catalog_row in readers]
            )
            if len(header_rows) != 1:
                _fail("native_header_unsupported")
            header = fhir_source_catalog_row(header_rows[0])
            if header["relowner"] != owner or not header["direct_acl_closed"] or not header["readers_closed"]:
                _fail("native_header_unsupported")
            await connection.execute(
                "LOCK TABLE "
                + _identifier(profile.schema_name)
                + "."
                + _RESOURCE_TABLE
                + ","
                + _identifier(profile.schema_name)
                + "."
                + _HEADER_TABLE
                + " IN ACCESS SHARE MODE"
            )
            database_oid = await connection.fetchval(
                "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
            )
            return {
                "family": [fhir_source_catalog_row(catalog_row) for catalog_row in family],
                "header": header,
                "database_oid": database_oid,
            }, None
    except GenericCandidateError, FHIRCatalogSourceError, asyncpg.PostgresError:
        return None, "native_source_profile_unsupported"


def _dataset_leaves(native, dataset_id):
    root = next(row for row in native["family"] if row["relname"] == _RESOURCE_TABLE)
    leaves = []
    for row in native["family"]:
        if row["parents"] != [root["oid"]]:
            continue
        bound = row["bound"] or ""
        if re.fullmatch(r"FOR VALUES IN \('(?:[^']|'')*'(?:, '(?:[^']|'')*')*\)", bound) is None:
            _fail("native_partition_unsupported")
        identities = [value.replace("''", "'") for value in re.findall(r"'((?:[^']|'')*)'", bound)]
        if dataset_id in identities:
            leaves.append(row)
    if not leaves:
        _fail("dataset_not_enrolled")
    return root, sorted(leaves, key=lambda row: row["oid"])


# Direct LIST-bound membership; callers verify complete physical closure separately.
fhir_catalog_list_dataset_routes = _dataset_leaves


def _native_identity(native, dataset_id):
    root, leaves = _dataset_leaves(native, dataset_id)
    scope_by_field = {
        "database_oid": native["database_oid"],
        "header": native["header"],
        "resource": root,
        "leaves": leaves,
    }
    return {
        "resource_table_oid": root["oid"],
        "resource_leaf_oids": [leaf["oid"] for leaf in leaves],
        "header_table_oid": native["header"]["oid"],
        "database_oid": native["database_oid"],
        "native_catalog_sha256": hashlib.sha256(fhir_catalog_json_bytes(scope_by_field)).hexdigest(),
    }


def _header_query(schema, priority):
    columns = ",".join("d." + field for field in _HEADER_FIELDS)
    return (
        f"SELECT {columns},{priority} AS priority,'{schema}'::text AS physical_schema "
        f"FROM {_identifier(schema)}.{_HEADER_TABLE} d JOIN endpoint_page p USING(endpoint_id) WHERE d.is_current"
    )


_PAGE_SQL = """WITH endpoint_page AS MATERIALIZED (
 SELECT e.endpoint_id,e.canonical_api_base FROM {catalog}.provider_directory_api_endpoint e
 WHERE e.endpoint_id>$1::text
   AND ($6::text[] IS NULL OR e.endpoint_id=ANY($6::text[]))
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
 SELECT *,count(*) OVER(PARTITION BY endpoint_id,physical_schema) AS current_count,
 row_number() OVER(PARTITION BY endpoint_id ORDER BY priority,dataset_id COLLATE "C") AS preference FROM headers
), inventory AS MATERIALIZED (
 SELECT p.endpoint_id,p.canonical_api_base,a.source_aliases,a.alias_count,d.dataset_id,d.dataset_hash,d.status,d.is_current,
 d.published_at,d.resource_count,d.previous_dataset_id,d.content_proof_admission_version,d.content_proof_admission_kind,
 d.content_proof_admission_sha256,d.content_proof_resource_types,d.publication_metadata_sha256,
 d.completion_proof_required_version,d.completion_proof_sha256,d.physical_schema,d.current_count,
 octet_length(d.publication_metadata_summary_json::text) AS metadata_bytes,
 octet_length(d.completion_proof_json::text) AS completion_bytes,
 CASE WHEN octet_length(d.publication_metadata_summary_json::text)<=$4
   THEN d.publication_metadata_summary_json::text END AS metadata_json,
 CASE WHEN octet_length(d.completion_proof_json::text)<=$4 THEN d.completion_proof_json::text END AS completion_json,
 coalesce(least(octet_length(d.publication_metadata_summary_json::text),$4),0)
 +coalesce(least(octet_length(d.completion_proof_json::text),$4),0)+octet_length(a.source_aliases::text)+4096 AS row_bytes
 FROM endpoint_page p JOIN aliases a USING(endpoint_id) LEFT JOIN ranked d ON d.endpoint_id=p.endpoint_id AND d.preference=1
), bounded AS (
 SELECT *,sum(row_bytes) OVER(ORDER BY endpoint_id) AS page_bytes,
 row_number() OVER(ORDER BY endpoint_id) AS ordinal FROM inventory
)
SELECT endpoint_id,canonical_api_base,source_aliases,alias_count,dataset_id,dataset_hash,status,is_current,published_at,
 resource_count,previous_dataset_id,content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256,
 content_proof_resource_types,publication_metadata_sha256,completion_proof_required_version,completion_proof_sha256,
 physical_schema,current_count,metadata_bytes,completion_bytes,row_bytes,
 CASE WHEN row_bytes<=$5 THEN metadata_json END AS metadata_json,
 CASE WHEN row_bytes<=$5 THEN completion_json END AS completion_json
FROM bounded WHERE ordinal=1 OR page_bytes<=$5 ORDER BY endpoint_id"""


def _page_query(profile, access):
    if profile.published_read_scope is not None:
        return _scoped_page_query(profile)
    schemas = [profile.schema_name]
    if profile.catalog_schema != profile.schema_name:
        schemas.append(profile.catalog_schema)
    headers = [
        _header_query(schema, index)
        for index, schema in enumerate(schemas)
        if access[f"{schema}.{_HEADER_TABLE}"]["readable"]
    ]
    if not headers:
        _fail("headers_unavailable")
    return _PAGE_SQL.format(catalog=_identifier(profile.catalog_schema), headers=" UNION ALL ".join(headers))


def _scoped_page_query(profile):
    # Observation inventory does not require the larger semantic proof projection.
    direct_fields = set(_HEADER_FIELDS[:8])
    columns = ",".join("d." + field if field in direct_fields else "NULL AS " + field for field in _HEADER_FIELDS)
    headers = (
        f"SELECT {columns},0 AS priority,'{profile.schema_name}'::text AS physical_schema "
        f"FROM ONLY {_identifier(profile.schema_name)}.{_HEADER_TABLE} d "
        "JOIN endpoint_page p USING(endpoint_id) WHERE d.is_current"
    )
    query = _PAGE_SQL.format(catalog=_identifier(profile.catalog_schema), headers=headers)
    for relation in (_SOURCE_TABLE, _ENDPOINT_TABLE):
        query = query.replace(
            f"FROM {_identifier(profile.catalog_schema)}.{relation} ",
            f"FROM ONLY {_identifier(profile.catalog_schema)}.{relation} ",
        )
    return query


def _decoded(value):
    if value is None:
        return None
    try:
        return json.loads(value) if type(value) is str else value
    except ValueError, TypeError:
        _fail("proof_invalid")


def _validate_proof_counts(proof_summary, source_row):
    counts, hashes = proof_summary["resource_counts"], proof_summary["resource_hashes"]
    if type(counts) is not dict or type(hashes) is not dict or not counts or set(counts) != set(hashes):
        _fail("admission_invalid")
    if (
        any(type(count) is not int or count < 0 for count in counts.values())
        or sum(counts.values()) != source_row["resource_count"]
    ):
        _fail("admission_invalid")
    for digest in hashes.values():
        _digest(digest)


def _admission(source_row):
    """Revalidate the existing fixed receipt and completion evidence without raw proof copies."""
    metadata = _decoded(source_row["metadata_json"])
    if type(metadata) is not dict or ADMISSION_GENERIC_PROOF_SUMMARY_KEY not in metadata:
        _fail("admission_missing")
    proof_summary = metadata[ADMISSION_GENERIC_PROOF_SUMMARY_KEY]
    if type(proof_summary) is not dict or set(proof_summary) != _SUMMARY_FIELDS:
        _fail("admission_invalid")
    if (
        proof_summary["dataset_hash"] != source_row["dataset_hash"]
        or proof_summary["resource_count"] != source_row["resource_count"]
    ):
        _fail("admission_changed")
    _validate_proof_counts(proof_summary, source_row)
    _digest(source_row["content_proof_admission_sha256"])
    seal = parse_source_admission_receipt(
        {key: metadata_value for key, metadata_value in metadata.items() if key != ADMISSION_GENERIC_PROOF_SUMMARY_KEY},
        admission_kind="generic",
        proof_sha256=source_row["content_proof_admission_sha256"],
        resource_counts=proof_summary["resource_counts"],
        proof_summary=proof_summary,
    )
    if (
        source_row["content_proof_admission_version"],
        source_row["content_proof_admission_kind"],
        source_row["publication_metadata_sha256"],
        tuple(source_row["content_proof_resource_types"] or ()),
    ) != (seal.admission_version, seal.admission_kind, seal.metadata_sha256, seal.resource_types):
        _fail("admission_changed")
    if canonical_payload_sha256(seal.digest_envelope()) != source_row["publication_metadata_sha256"]:
        _fail("admission_changed")
    aliases = metadata.get("source_ids")
    if type(aliases) is not list or any(type(alias) is not str for alias in aliases):
        _fail("source_aliases_not_admitted")
    aliases = _strings(sorted(aliases), 64)
    if not set(aliases) <= set(source_row["source_aliases"]):
        _fail("source_aliases_not_admitted")
    resources = _resource_scope(metadata.get("selected_resources"))
    scope = metadata.get("proof_resource_scope")
    if scope is not None:
        scope = _resource_scope(scope)
    header_by_field = {**dict(source_row), "completion_proof_json": _decoded(source_row["completion_json"])}
    validate_handoff_completion(header_by_field)
    return {
        "version": seal.admission_version,
        "kind": seal.admission_kind,
        "proof_sha256": seal.proof_sha256,
        "metadata_sha256": seal.metadata_sha256,
        "resource_count": source_row["resource_count"],
        "resource_counts": proof_summary["resource_counts"],
        "resource_types": list(seal.resource_types),
        "selected_resources": resources,
        "proof_resource_scope": scope,
        "completion_required_version": source_row["completion_proof_required_version"],
        "completion_sha256": source_row["completion_proof_sha256"],
    }, aliases


def _descriptor(row, profile, native):
    admission, aliases = _admission(row)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", aliases[0], profile.schema_name, row["dataset_id"], PRODUCER, row["dataset_id"]
    )
    descriptor_by_field = {
        "coordinates": asdict(coordinates),
        "profile": "published_normalized",
        "endpoint_id": row["endpoint_id"],
        "dataset_sha256": _digest(row["dataset_hash"]),
        "source_uri": row["canonical_api_base"],
        "accepted_receipt_id": ADMISSION_SEAL_CONTRACT + ":" + row["dataset_id"],
        "accepted_receipt_sha256": admission["metadata_sha256"],
        "complete": set(RESOURCE_TYPES) <= set(admission["proof_resource_scope"] or admission["selected_resources"]),
        "previous_dataset_id": row["previous_dataset_id"],
        "source_aliases": aliases,
        "native_read_profile": profile.document(),
        "admission": admission,
        **_native_identity(native, row["dataset_id"]),
    }
    return validate_fhir_catalog_source_descriptor(descriptor_by_field)


def _source_status(source_row, profile, native, native_reason, page_bytes):
    if source_row["dataset_id"] is None:
        return "unavailable", "current_dataset_missing"
    if source_row["alias_count"] > MAX_ALIASES:
        return "unsupported", "source_alias_bounds"
    if (
        source_row["row_bytes"] > page_bytes
        or max(source_row["metadata_bytes"] or 0, source_row["completion_bytes"] or 0) > _PROOF_LIMIT
    ):
        return "unsupported", "source_proof_bounds"
    if source_row["physical_schema"] != profile.schema_name:
        return "unsupported", "legacy_source_not_enrolled"
    if native is None:
        return "unsupported", native_reason or "native_source_profile_unsupported"
    if (
        source_row["current_count"] != 1
        or source_row["status"] != "published"
        or source_row["is_current"] is not True
        or type(source_row["published_at"]) is not datetime
    ):
        return "unavailable", "current_dataset_unpublished"
    return "accepted", None


def _disposition(source_row, profile, native, native_reason, page_bytes):
    status, reason = _source_status(source_row, profile, native, native_reason, page_bytes)
    descriptor, accepted_at = None, None
    if status == "accepted":
        try:
            descriptor = _descriptor(source_row, profile, native)
            # Existing UTC-naive publication columns expose their explicit UTC instant.
            published_at = source_row["published_at"]
            accepted_at = (
                published_at.replace(tzinfo=timezone.utc)
                if published_at.tzinfo is None
                else published_at.astimezone(timezone.utc)
            )
            if not descriptor["complete"]:
                status, reason = "incomplete", "catalog_families_incomplete"
        except ValueError, TypeError, KeyError, AdmissionSealError, GenericCandidateError:
            status, reason, descriptor = "unsupported", "source_admission_unavailable", None
    return FHIRCatalogSourceDisposition(
        source_row["endpoint_id"],
        source_row["dataset_id"],
        tuple(source_row["source_aliases"]),
        source_row["alias_count"],
        status,
        reason,
        descriptor,
        accepted_at,
    )


async def _source_pages(
    connection,
    profile,
    native_state,
    page_size,
    page_bytes,
    after_endpoint,
    endpoint_ids=None,
    observation_contract=None,
):
    native, native_reason, access = native_state
    query = _page_query(profile, access)
    cursor = after_endpoint or ""
    while True:
        records = await connection.fetch(query, cursor, page_size, MAX_ALIASES, _PROOF_LIMIT, page_bytes, endpoint_ids)
        if not records:
            return
        if observation_contract is None:
            yield tuple(_disposition(row, profile, native, native_reason, page_bytes) for row in records)
        else:
            yield tuple([await _observed_disposition(connection, profile, row) for row in records])
        cursor = records[-1]["endpoint_id"]


async def iter_fhir_catalog_sources(
    connection, profile, *, page_size=100, page_bytes=DEFAULT_BYTES, after_endpoint=None, observation_contract=None
):
    """Inventory every registered physical endpoint, including unsupported legacy imports."""
    if type(profile) is not FHIRCatalogSourceReadProfile:
        _fail("profile_required")
    if observation_contract is not None and (
        type(observation_contract) is not str or observation_contract != CATALOG_OBSERVATION_CONTRACT
    ):
        _fail("observation_contract_invalid")
    if profile.published_read_scope is not None and observation_contract is None:
        _fail("scoped_semantic_compatibility_required")
    if (
        type(page_size) is not int
        or not 1 <= page_size <= MAX_PAGE
        or type(page_bytes) is not int
        or not 1 <= page_bytes <= MAX_BYTES
    ):
        _fail("bounds_invalid")
    if after_endpoint is not None:
        _bounded_text(after_endpoint, 64)
    await _transaction(connection)
    access = await _catalog_access(connection, profile)
    native, reason = (None, None) if observation_contract else await _native_source(connection, profile, access)
    async for page in _source_pages(
        connection,
        profile,
        (native, reason, access),
        page_size,
        page_bytes,
        after_endpoint,
        observation_contract=observation_contract,
    ):
        yield page


async def _observed_disposition(connection, profile, row):
    from process.registry_fhir_catalog_observed_source import collect_fhir_catalog_observation

    reason = None
    if row["dataset_id"] is None:
        reason = "current_dataset_missing"
    elif row["alias_count"] > MAX_ALIASES:
        reason = "source_alias_bounds"
    elif row["physical_schema"] != profile.schema_name:
        reason = "source_namespace_unavailable"
    elif row["current_count"] != 1:
        reason = "current_dataset_ambiguous"
    if reason is None:
        try:
            return await collect_fhir_catalog_observation(
                connection, profile, endpoint_id=row["endpoint_id"], dataset_id=row["dataset_id"]
            )
        except ValueError:
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
    """Recheck native/source vectors inside an optional pre-snapshot session fence."""
    if type(descriptors) is not tuple or not 1 <= len(descriptors) <= MAX_PAGE or type(lock) is not bool:
        _fail("bounds_invalid")
    await _transaction(connection)
    from process.registry_fhir_catalog_source_fence import require_fhir_catalog_source_publication_fence

    await require_fhir_catalog_source_publication_fence(connection, descriptors, required=lock)
    by_profile = {}
    for descriptor in descriptors:
        validated = validate_fhir_catalog_source_descriptor(descriptor)
        key = fhir_catalog_json_bytes(validated["native_read_profile"])
        by_profile.setdefault(key, []).append(validated)
    for profile_bytes, selected in by_profile.items():
        profile = _profile(json.loads(profile_bytes))
        if profile.published_read_scope is not None:
            _fail("scoped_semantic_compatibility_required")
        access = await _catalog_access(connection, profile)
        native, reason = await _native_source(connection, profile, access)
        if native is None:
            _fail(reason)
        expected_by_endpoint = {descriptor["endpoint_id"]: descriptor for descriptor in selected}
        if len(expected_by_endpoint) != len(selected):
            _fail("duplicate_endpoint")
        seen_endpoints = set()
        async for page in _source_pages(
            connection, profile, (native, None, access), 100, DEFAULT_BYTES, None, sorted(expected_by_endpoint)
        ):
            for disposition in page:
                if disposition.descriptor != expected_by_endpoint.get(disposition.endpoint_id):
                    _fail("source_changed")
                seen_endpoints.add(disposition.endpoint_id)
        if seen_endpoints != set(expected_by_endpoint):
            _fail("source_changed")
    return tuple(fhir_catalog_source_generation(descriptor) for descriptor in descriptors)


require_fhir_catalog_snapshot_transaction = _transaction
read_fhir_catalog_inventory_access = _catalog_access
build_fhir_catalog_inventory_query = _page_query
bounded_fhir_catalog_source_text = _bounded_text
