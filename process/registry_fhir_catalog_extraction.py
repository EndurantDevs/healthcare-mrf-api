# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded catalog facts from admitted FHIR rows; no identity or approval writes."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from urllib.parse import urlsplit

MAX_ROWS = 1000
MAX_BYTES = 32 * 1024 * 1024
MAX_EDGES = 10_000
DEFAULT_BYTES = 8 * 1024 * 1024
RESOURCE_TYPES = ("InsurancePlan", "Organization", "OrganizationAffiliation", "PractitionerRole")
PROFILES = ("raw_fhir", "published_normalized", "retained_normalized")
_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")
_NETWORK_EXTENSIONS = {
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference",
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-participatingnetwork-extension",
}


class FHIRCatalogExtractionError(ValueError):
    """Refuse the complete batch or page without truncating source facts."""


@dataclass(frozen=True)
class FHIRCatalogOrganization:
    resource_id: str
    name: str | None
    aliases: tuple[str, ...]
    declared_network: bool
    status: str
    identifiers: tuple[dict, ...]
    source_hash: str
    part_of_ref: str | None = None


@dataclass(frozen=True)
class FHIRCatalogReferenceEdge:
    owner_resource_type: str
    owner_resource_id: str
    scope_path: str
    network_ref: str | None
    network_resource_id: str | None
    resolution_reason: str | None
    plan_resource_id: str | None
    plan_name: str | None
    plan_aliases: tuple[str, ...]
    plan_identifiers: tuple[dict, ...]
    owner_ref: str | None
    administrator_ref: str | None
    valid_from: str | None
    valid_to: str | None
    status: str | None
    scope_identifiers: tuple[dict, ...]
    scope_type_codes: tuple[dict, ...]
    benefits: tuple[dict, ...]
    geography: tuple[str | None, ...]
    source_hash: str


@dataclass(frozen=True)
class FHIRCatalogBatch:
    organizations: tuple[FHIRCatalogOrganization, ...]
    edges: tuple[FHIRCatalogReferenceEdge, ...]
    source_rows: int
    output_bytes: int


def _fail(reason="invalid"):
    raise FHIRCatalogExtractionError("registry_fhir_catalog_" + reason)


def fhir_catalog_json_bytes(value):
    """Encode deterministic UTF-8 JSON and reject unsupported or nonfinite values."""
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    except TypeError, ValueError, UnicodeError, RecursionError:
        _fail()


_json_bytes = fhir_catalog_json_bytes


def _bound(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        _fail("bounds_invalid")
    return value


def fhir_catalog_text(value, maximum, *, strip=True):
    """Validate optional bounded UTF-8 text, applying trimming only when requested."""
    if value is None:
        return None
    if type(value) is not str:
        _fail()
    if strip:
        value = value.strip()
    try:
        encoded = value.encode()
    except UnicodeError:
        _fail()
    if len(encoded) > maximum or any(ord(character) < 32 or ord(character) == 127 for character in value):
        _fail()
    return value or None


_text = fhir_catalog_text


def _items(value):
    if value is None:
        return ()
    if type(value) is not list:
        _fail()
    return value


def _documents(value):
    documents = _items(value)
    if any(type(document) is not dict for document in documents):
        _fail()
    return tuple(documents)


def _aliases(value):
    aliases = tuple(_text(alias, 512) for alias in _items(value))
    if len(aliases) > 100 or any(alias is None for alias in aliases):
        _fail()
    return tuple(dict.fromkeys(aliases))


def _reference_text(value, is_normalized):
    if is_normalized:
        return _text(value, 2048, strip=False)
    if value is None:
        return None
    if type(value) is not dict:
        _fail()
    return _text(value.get("reference"), 2048, strip=False)


def _reference_values(reference_values, is_normalized):
    if not is_normalized and type(reference_values) is dict:
        reference_values = [reference_values]
    return tuple(_reference_text(reference, is_normalized) for reference in _items(reference_values))


def fhir_source_uri(source_uri):
    """Validate an optional HTTP endpoint and normalize its trailing slash."""
    if source_uri is None:
        return None
    source_uri = _text(source_uri, 2048, strip=False)
    if source_uri is None:
        _fail()
    try:
        parsed = urlsplit(source_uri)
    except ValueError:
        _fail()
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        _fail()
    if parsed.username is not None or parsed.password is not None or source_uri.strip() != source_uri:
        _fail()
    return source_uri.rstrip("/") + "/"


_source_uri = fhir_source_uri


def fhir_network_reference_target(reference, source_uri):
    """Resolve a local Organization reference or return its explicit unresolved reason."""
    if reference is None:
        return None, "reference_missing"
    local = reference[len(source_uri) :] if source_uri is not None and reference.startswith(source_uri) else reference
    if local.startswith("Organization/") and _FHIR_ID.fullmatch(local[len("Organization/") :]):
        return local[len("Organization/") :], None
    return None, "reference_outside_source" if "://" in reference else "reference_invalid"


_network_target = fhir_network_reference_target


def _resource(row, profile):
    if type(row) is not dict or set(row) != {"resource_type", "resource_id", "payload_hash", "payload_json"}:
        _fail()
    resource_type, resource_id = row["resource_type"], row["resource_id"]
    if resource_type not in RESOURCE_TYPES or type(resource_id) is not str or not _FHIR_ID.fullmatch(resource_id):
        _fail()
    if type(row["payload_hash"]) is not str or re.fullmatch(r"[0-9a-f]{64}", row["payload_hash"]) is None:
        _fail()
    payload = row["payload_json"]
    if type(payload) is not dict:
        _fail()
    if profile == "raw_fhir" and (payload.get("resourceType"), payload.get("id")) != (resource_type, resource_id):
        _fail()
    if profile != "raw_fhir" and payload.get("resource_id", resource_id) != resource_id:
        _fail()
    return resource_type, resource_id, payload


def _organization(row, payload, is_normalized):
    codes = list(_items(payload.get("type_codes" if is_normalized else "type")))
    if is_normalized:
        codes = [{"code": _text(code, 128)} if type(code) is str else code for code in codes]
    codes = _documents(codes)
    declared = any(
        concept.get("text") == "ntwk"
        or (is_normalized and concept.get("code") == "ntwk")
        or (not is_normalized and any(coding.get("code") == "ntwk" for coding in _documents(concept.get("coding"))))
        for concept in codes
    )
    active = payload.get("active")
    if active is not None and type(active) is not bool:
        _fail()
    return FHIRCatalogOrganization(
        resource_id=row["resource_id"],
        name=_text(payload.get("name"), 512),
        aliases=_aliases(payload.get("aliases" if is_normalized else "alias")),
        declared_network=declared,
        status="active" if active is True else "inactive" if active is False else "unknown",
        identifiers=_documents(payload.get("identifiers" if is_normalized else "identifier")),
        source_hash=row["payload_hash"],
        part_of_ref=_reference_text(payload.get("part_of_ref" if is_normalized else "partOf"), is_normalized),
    )


def _edge_context(row, payload, is_normalized):
    is_plan = row["resource_type"] == "InsurancePlan"
    period = payload.get("period") if not is_normalized else None
    if period is not None and type(period) is not dict:
        _fail()
    return {
        "owner_resource_type": row["resource_type"],
        "owner_resource_id": row["resource_id"],
        "plan_resource_id": row["resource_id"] if is_plan else None,
        "plan_name": _text(payload.get("name"), 512) if is_plan else None,
        "plan_aliases": _aliases(payload.get("aliases" if is_normalized else "alias")) if is_plan else (),
        "plan_identifiers": _documents(payload.get("product_identifiers" if is_normalized else "identifier"))
        if is_plan
        else (),
        "owner_ref": _reference_text(payload.get("owned_by_ref" if is_normalized else "ownedBy"), is_normalized)
        if is_plan
        else None,
        "administrator_ref": _reference_text(
            payload.get("administered_by_ref" if is_normalized else "administeredBy"), is_normalized
        )
        if is_plan
        else None,
        "valid_from": _text(payload.get("period_start") if is_normalized else (period or {}).get("start"), 64),
        "valid_to": _text(payload.get("period_end") if is_normalized else (period or {}).get("end"), 64),
        "status": _text(payload.get("status"), 64) if is_plan else None,
        "source_hash": row["payload_hash"],
    }


def _edge_scopes(payload, resource_type, is_normalized):
    if is_normalized:
        yield "/normalized_aggregate", payload, payload.get("network_refs")
    else:
        yield "/network", payload, payload.get("network")
    if resource_type == "InsurancePlan":
        fields = (
            (("plan_backbones", "plan"), ("coverage", "coverage"))
            if is_normalized
            else (("plan", "plan"), ("coverage", "coverage"))
        )
        for field, path in fields:
            for index, scope in enumerate(_documents(payload.get(field))):
                yield f"/{path}/{index}/network", scope, scope.get("network_refs" if is_normalized else "network")
    elif not is_normalized:
        for index, extension in enumerate(_documents(payload.get("extension"))):
            url = extension.get("url")
            if type(url) is str and url.split("|", 1)[0].rstrip("/").lower() in _NETWORK_EXTENSIONS:
                yield f"/extension/{index}/valueReference", {}, [extension.get("valueReference")]


def _scope_metadata(payload, scope, is_normalized):
    geography_field = "coverage_area_refs" if is_normalized else "coverageArea"
    geography = scope.get(geography_field, payload.get(geography_field))
    return {
        "scope_identifiers": _documents(scope.get("identifiers" if is_normalized else "identifier")),
        "scope_type_codes": _documents(scope.get("type_codes" if is_normalized else "type"))
        if type(scope.get("type_codes" if is_normalized else "type")) is not dict
        else (scope["type_codes" if is_normalized else "type"],),
        "benefits": _documents(scope.get("benefits" if is_normalized else "benefit")),
        "geography": _reference_values(geography, is_normalized),
    }


def _resource_edges(source_row, resource_payload, is_normalized, source_uri):
    context = _edge_context(source_row, resource_payload, is_normalized)
    for path, scope, references in _edge_scopes(resource_payload, source_row["resource_type"], is_normalized):
        metadata = _scope_metadata(resource_payload, scope, is_normalized)
        network_references = _reference_values(references, is_normalized)
        if not network_references and source_row["resource_type"] == "InsurancePlan":
            network_references = (None,)
        for index, reference in enumerate(network_references):
            network_resource_id, reason = _network_target(reference, source_uri)
            yield FHIRCatalogReferenceEdge(
                **context,
                **metadata,
                scope_path=f"{path}/{index}",
                network_ref=reference,
                network_resource_id=network_resource_id,
                resolution_reason=reason,
            )


def extract_fhir_catalog_batch(
    resource_rows,
    *,
    profile,
    source_uri=None,
    max_rows=MAX_ROWS,
    max_input_bytes=DEFAULT_BYTES,
    max_output_bytes=DEFAULT_BYTES,
):
    """Extract source facts only; hashes, local references and names grant no authority."""
    if profile not in PROFILES or type(resource_rows) not in {list, tuple}:
        _fail()
    _bound(max_rows, MAX_ROWS)
    _bound(max_input_bytes, MAX_BYTES)
    _bound(max_output_bytes, MAX_BYTES)
    if len(resource_rows) > max_rows or len(_json_bytes(resource_rows)) > max_input_bytes:
        _fail("input_bounds")
    source_uri = _source_uri(source_uri)
    organizations, edges, identities = [], [], set()
    used_bytes = len(b'{"organizations":[],"edges":[]}')
    for source_row in resource_rows:
        resource_type, resource_id, resource_payload = _resource(source_row, profile)
        if (resource_type, resource_id) in identities:
            _fail("duplicate_resource")
        identities.add((resource_type, resource_id))
        is_normalized = profile != "raw_fhir"
        if resource_type == "Organization":
            observation = _organization(source_row, resource_payload, is_normalized)
            organizations.append(observation)
            used_bytes += len(_json_bytes(asdict(observation))) + (len(organizations) > 1)
        else:
            for edge in _resource_edges(source_row, resource_payload, is_normalized, source_uri):
                used_bytes += len(_json_bytes(asdict(edge))) + bool(edges)
                if len(edges) >= MAX_EDGES or used_bytes > max_output_bytes:
                    _fail("output_bounds")
                edges.append(edge)
        if used_bytes > max_output_bytes:
            _fail("output_bounds")
    return FHIRCatalogBatch(tuple(organizations), tuple(edges), len(resource_rows), used_bytes)


_PAGE_SQL = """WITH page AS MATERIALIZED (
  SELECT resource_type,resource_id,payload_hash,payload_json,
    octet_length(payload_json::text)+octet_length(resource_type)+octet_length(resource_id)+256 AS row_bytes
  FROM {namespace}.provider_directory_dataset_resource
  WHERE dataset_id=$1 AND resource_type=ANY($2::text[])
    AND (resource_type COLLATE "C",resource_id COLLATE "C")>($3::text COLLATE "C",$4::text COLLATE "C")
  ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $5
), bounded AS (
  SELECT *,sum(row_bytes) OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS page_bytes,
    row_number() OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS ordinal FROM page
)
SELECT resource_type,resource_id,payload_hash,row_bytes,
  CASE WHEN row_bytes<=$6 THEN payload_json::jsonb::text END AS payload_json
FROM bounded WHERE ordinal=1 OR page_bytes<=$6
ORDER BY resource_type COLLATE "C",resource_id COLLATE "C"
"""


def _selected_resource_types(resource_types):
    if type(resource_types) is not tuple or not resource_types:
        _fail()
    if any(type(resource_type) is not str or resource_type not in RESOURCE_TYPES for resource_type in resource_types):
        _fail()
    if len(set(resource_types)) != len(resource_types):
        _fail()


async def verify_fhir_catalog_retained_read(connection, *, schema_name, dataset_id, retained_read):
    """Require this operation's retained read and its exact physical resource heap."""
    from process.network_fhir_retained_capture import RetainedFHIRRead

    if (
        type(retained_read) is not RetainedFHIRRead
        or schema_name != retained_read.capture.schema_name
        or dataset_id != retained_read.capture.origin.dataset_id
    ):
        _fail("retained_read_changed")
    await retained_read.verify(connection, retained_read.capture)
    resource_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", schema_name + ".provider_directory_dataset_resource"
    )
    if resource_oid != dict(retained_read.capture.relation_oids)["provider_directory_dataset_resource"]:
        _fail("retained_read_changed")


def _validate_fhir_catalog_page_scope(schema_name, dataset_id, after_resource, resource_types):
    """Validate the physical dataset and exact keyset cursor before page reads."""
    if type(schema_name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema_name) is None:
        _fail()
    if _text(dataset_id, 128, strip=False) != dataset_id or not dataset_id or dataset_id.strip() != dataset_id:
        _fail()
    if after_resource is not None and (
        type(after_resource) is not tuple
        or len(after_resource) != 2
        or after_resource[0] not in resource_types
        or type(after_resource[1]) is not str
        or _FHIR_ID.fullmatch(after_resource[1]) is None
    ):
        _fail()


async def iter_fhir_catalog_pages(
    connection,
    *,
    schema_name,
    dataset_id,
    after_resource=None,
    page_rows=MAX_ROWS,
    page_bytes=DEFAULT_BYTES,
    resource_types=RESOURCE_TYPES,
    retained_read=None,
):
    """Read admitted immutable data in a caller-owned repeatable snapshot; never commit."""
    _bound(page_rows, MAX_ROWS)
    _bound(page_bytes, MAX_BYTES)
    _selected_resource_types(resource_types)
    _validate_fhir_catalog_page_scope(schema_name, dataset_id, after_resource, resource_types)
    if not connection.is_in_transaction():
        _fail("transaction_required")
    isolation = await connection.fetchval("SELECT current_setting('transaction_isolation')")
    if isolation not in {"repeatable read", "serializable"}:
        _fail("transaction_required")
    if retained_read is not None:
        await verify_fhir_catalog_retained_read(
            connection, schema_name=schema_name, dataset_id=dataset_id, retained_read=retained_read
        )
    cursor = after_resource or ("", "")
    query = _PAGE_SQL.format(namespace='"' + schema_name + '"')
    while True:
        if retained_read is not None:
            await retained_read.verify(connection, retained_read.capture)
        source_records = await connection.fetch(query, dataset_id, resource_types, *cursor, page_rows, page_bytes)
        if not source_records:
            return
        if any(
            source_record["row_bytes"] > page_bytes or source_record["payload_json"] is None
            for source_record in source_records
        ):
            _fail("page_bounds")
        source_rows = tuple(
            {key: source_record[key] for key in ("resource_type", "resource_id", "payload_hash", "payload_json")}
            for source_record in source_records
        )
        try:
            source_rows = tuple(
                {**source_row, "payload_json": json.loads(source_row["payload_json"])} for source_row in source_rows
            )
        except TypeError, ValueError:
            _fail()
        if len(_json_bytes(source_rows)) > page_bytes:
            _fail("page_bounds")
        cursor = (source_rows[-1]["resource_type"], source_rows[-1]["resource_id"])
        yield source_rows
