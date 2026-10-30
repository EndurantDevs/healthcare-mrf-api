# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final bounded batch validation after metadata candidate writers close."""

import hashlib
import json
import re
from dataclasses import fields
from datetime import date, datetime, timezone

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import PROJECTOR_VERSION
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_enrichment import (
    FHIRCatalogEnrichmentContext,
    enrich_fhir_catalog_edges,
)
from process.registry_fhir_catalog_extraction import (
    FHIRCatalogOrganization,
    FHIRCatalogReferenceEdge,
    fhir_network_reference_target,
    fhir_source_uri,
)
from process.registry_fhir_catalog_extraction import fhir_catalog_text as source_text
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_read import (
    fhir_catalog_binding_document,
    fhir_catalog_relationship_document,
)
from process.registry_network_catalog_read import (
    catalog_network_document,
    catalog_text,
)

INPUT_FIELDS = (
    "network_generation",
    "manifest_sha256",
    "metadata_approved_revision",
    "membership_mapping_sha256",
    "mapping_sha256",
    "standing_grant_sha256",
    "source_vector_json",
    "projector_version",
    "as_of",
)
PAGE_ROWS = 1000
PAGE_BYTES = 34 * 1024 * 1024
_KEYS = {
    "registry_fhir_catalog_source": (("source_generation", "text"),),
    "registry_fhir_catalog_observation": (
        ("source_generation", "text"),
        ("resource_type", "text"),
        ("resource_id", "text"),
        ("observation_kind", "text"),
        ("ordinal", "integer"),
    ),
    "registry_fhir_catalog_network": (("metadata_generation", "text"), ("network_id", "integer")),
    "registry_fhir_catalog_relationship": (("metadata_generation", "text"), ("relationship_id", "text")),
    "registry_fhir_catalog_binding": (("metadata_generation", "text"), ("binding_id", "uuid")),
}
_SOURCE_COORDINATES = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}


def _invalid(reason="validation_failed"):
    raise ValueError("registry_fhir_catalog_" + reason)


def canonical_json(document):
    """Encode semantic metadata consistently for input and content fingerprints."""
    return json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _digest(encoded):
    if type(encoded) is not str or re.fullmatch(r"[0-9a-f]{64}", encoded) is None:
        _invalid("inputs_invalid")


def validate_fhir_catalog_source_vector(vector):
    """Validate the complete ordered source vector before selecting active inputs."""
    if type(vector) is not list or len(canonical_json(vector).encode()) > PAGE_BYTES:
        _invalid("inputs_invalid")
    previous, seen = None, set()
    for entry in vector:
        if type(entry) is not dict or set(entry) != {"source_generation", "source_id", "status"}:
            _invalid("inputs_invalid")
        _digest(entry["source_generation"])
        catalog_text(entry["source_id"], 128)
        key = (entry["source_id"], entry["source_generation"])
        if entry["status"] not in {"current", "last_good"} or entry["source_id"] == "registry-manual":
            _invalid("inputs_invalid")
        if entry["source_generation"] in seen or previous is not None and key <= previous:
            _invalid("inputs_invalid")
        seen.add(entry["source_generation"])
        previous = key


_vector_entries = validate_fhir_catalog_source_vector


def snapshot_inputs(snapshot):
    """Normalize serialized pins without including physical or publication state."""
    inputs_by_field = {field: snapshot[field] for field in INPUT_FIELDS}
    if type(inputs_by_field["source_vector_json"]) is str:
        inputs_by_field["source_vector_json"] = json.loads(inputs_by_field["source_vector_json"])
    if type(inputs_by_field["as_of"]) is date:
        inputs_by_field["as_of"] = inputs_by_field["as_of"].isoformat()
    if inputs_by_field["projector_version"] != PROJECTOR_VERSION:
        _invalid("projector_invalid")
    for field in ("membership_mapping_sha256", "mapping_sha256", "standing_grant_sha256"):
        _digest(inputs_by_field[field])
    if (inputs_by_field["network_generation"] is None) != (inputs_by_field["manifest_sha256"] is None):
        _invalid("inputs_invalid")
    if inputs_by_field["network_generation"] is not None:
        _digest(inputs_by_field["manifest_sha256"])
        if (
            type(inputs_by_field["network_generation"]) is not int
            or not 0 < inputs_by_field["network_generation"] < 2**63
        ):
            _invalid("inputs_invalid")
    validate_fhir_catalog_source_vector(inputs_by_field["source_vector_json"])
    for field, minimum in (("metadata_approved_revision", 0),):
        if type(inputs_by_field[field]) is not int or not minimum <= inputs_by_field[field] < 2**63:
            _invalid("inputs_invalid")
    as_of = inputs_by_field["as_of"]
    if type(as_of) is not str or date.fromisoformat(as_of).isoformat() != as_of:
        _invalid("inputs_invalid")
    return inputs_by_field


def metadata_generation(snapshot):
    """Hash semantic pins independently of the metadata candidate location."""
    return hashlib.sha256(canonical_json(snapshot_inputs(snapshot)).encode()).hexdigest()


def _source_descriptor(document):
    required_fields = {
        "coordinates",
        "profile",
        "endpoint_id",
        "dataset_sha256",
        "resource_table_oid",
        "source_uri",
        "accepted_receipt_id",
        "accepted_receipt_sha256",
        "complete",
        "previous_dataset_id",
    }
    if type(document) is not dict or not required_fields <= document.keys():
        _invalid("source_invalid")
    coordinates = document["coordinates"]
    if type(coordinates) is not dict or set(coordinates) != _SOURCE_COORDINATES:
        _invalid("source_invalid")
    RegistryNetworkSourceCoordinates(**coordinates)
    if coordinates["source_system"] != "fhir" or coordinates["source_id"] == "registry-manual":
        _invalid("source_invalid")
    if (
        document["profile"] not in {"raw_fhir", "published_normalized", "retained_normalized"}
        or type(document["complete"]) is not bool
    ):
        _invalid("source_invalid")
    if (document["profile"] == "retained_normalized") != (
        coordinates["producer_id"] == "registry-retained-normalized-fhir-v1"
    ):
        _invalid("source_invalid")
    for field, maximum in (("endpoint_id", 64), ("accepted_receipt_id", 128)):
        catalog_text(document[field], maximum)
    for field in ("dataset_sha256", "accepted_receipt_sha256"):
        _digest(document[field])
    if document["previous_dataset_id"] is not None:
        catalog_text(document["previous_dataset_id"], 128)
    if type(document["resource_table_oid"]) is not int or not 0 < document["resource_table_oid"] <= 4294967295:
        _invalid("source_invalid")
    if document["source_uri"] is None:
        if document["profile"] != "retained_normalized":
            _invalid("source_invalid")
    else:
        catalog_text(document["source_uri"], 2048)
        fhir_source_uri(document["source_uri"])
    if document["profile"] == "retained_normalized":
        _digest(document.get("capture_sha256"))
    return coordinates


def _source_document(row):
    descriptor = row["descriptor_json"]
    coordinates = _source_descriptor(descriptor)
    if row["source_id"] != coordinates["source_id"] or row["profile"] != descriptor["profile"]:
        _invalid("source_invalid")
    from process.registry_fhir_catalog_sources import fhir_catalog_source_digest

    if row["source_generation"] != fhir_catalog_source_digest(descriptor):
        _invalid("source_invalid")
    for field in ("accepted_at", "created_at"):
        timestamp = datetime.fromisoformat(row[field])
        if timestamp.utcoffset() is None:
            _invalid("source_invalid")
        row[field] = timestamp.astimezone(timezone.utc).isoformat()


def _observation_document(observation_by_field):
    document = observation_by_field["document_json"]
    kind = (
        FHIRCatalogOrganization
        if observation_by_field["observation_kind"] == "organization"
        else FHIRCatalogReferenceEdge
    )
    expected_fields = {field.name for field in fields(kind)}
    if kind is FHIRCatalogOrganization and type(document) is dict and "part_of_ref" not in document:
        expected_fields.remove("part_of_ref")
    if type(document) is not dict or set(document) != expected_fields:
        _invalid("observation_invalid")
    _digest(document["source_hash"])
    if kind is FHIRCatalogOrganization:
        if (
            observation_by_field["resource_type"] != "Organization"
            or observation_by_field["resource_id"] != document["resource_id"]
            or observation_by_field["ordinal"] != 0
        ):
            _invalid("observation_invalid")
        if type(document["declared_network"]) is not bool or document["status"] not in {
            "active",
            "inactive",
            "unknown",
        }:
            _invalid("observation_invalid")
        if (
            type(document["resource_id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", document["resource_id"]) is None
        ):
            _invalid("observation_invalid")
        _observation_name(document["name"], document["aliases"])
        if document.get("part_of_ref") is not None:
            source_text(document["part_of_ref"], 2048, strip=False)
        if type(document["identifiers"]) is not list or any(
            type(entry) is not dict for entry in document["identifiers"]
        ):
            _invalid("observation_invalid")
    else:
        if (observation_by_field["resource_type"], observation_by_field["resource_id"]) != (
            document["owner_resource_type"],
            document["owner_resource_id"],
        ):
            _invalid("observation_invalid")
        _observation_edge(document)


def _observation_name(name, aliases):
    if name is not None:
        catalog_text(name, 512)
    if type(aliases) is not list or len(aliases) > 100 or len(set(aliases)) != len(aliases):
        _invalid("observation_invalid")
    for alias in aliases:
        catalog_text(alias, 512)


def _observation_edge(document):
    if document["owner_resource_type"] not in {"InsurancePlan", "OrganizationAffiliation", "PractitionerRole"}:
        _invalid("observation_invalid")
    catalog_text(document["scope_path"], 512)
    _observation_name(document["plan_name"], document["plan_aliases"])
    for field in ("network_ref", "owner_ref", "administrator_ref"):
        if document[field] is not None:
            source_text(document[field], 2048, strip=False)
    for field, maximum in (("valid_from", 64), ("valid_to", 64), ("status", 64), ("resolution_reason", 128)):
        if document[field] is not None:
            catalog_text(document[field], maximum)
    for field in ("network_resource_id", "plan_resource_id", "owner_resource_id"):
        if document[field] is not None and (
            type(document[field]) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", document[field]) is None
        ):
            _invalid("observation_invalid")
    for field in ("plan_identifiers", "scope_identifiers", "scope_type_codes", "benefits"):
        if type(document[field]) is not list or any(type(entry) is not dict for entry in document[field]):
            _invalid("observation_invalid")
    if type(document["geography"]) is not list:
        _invalid("observation_invalid")
    for reference in document["geography"]:
        if reference is not None:
            source_text(reference, 2048, strip=False)


def _network_record(row):
    document_by_field = {
        field: row[field]
        for field in ("network_id", "display_name", "aliases", "archived", "revision", "benefit_codes")
    }
    document_by_field.update(directory_available=False, priceable=None, companies=[], source_bindings=[])
    catalog_network_document(document_by_field)
    if row["search_name"] != "\n".join((row["display_name"], *row["aliases"])).lower():
        _invalid("network_invalid")


def _validate_document(table, row):
    if table.endswith("_network"):
        _network_record(row)
    elif table.endswith("_relationship"):
        document = fhir_catalog_relationship_document(row["document_json"])
        company, group = document["company"], document["group"]
        actual = (
            document["relationship_id"],
            document["source_id"],
            company["company_id"] if company else None,
            group["group_id"] if group else None,
        )
        if actual != (row["relationship_id"], row["source_id"], row["company_id"], row["group_id"]):
            _invalid("relationship_invalid")
        if any(
            type(row[field]) is not str or row[field] != row[field].lower()
            for field in ("company_search", "plan_search")
        ):
            _invalid("relationship_invalid")
    elif table.endswith("_binding"):
        document = fhir_catalog_binding_document(row["document_json"], network_id=row["network_id"])
        if (document["binding_id"], document["source_id"]) != (row["binding_id"], row["source_id"]):
            _invalid("binding_invalid")
    elif table.endswith("_source"):
        _source_document(row)
    else:
        _observation_document(row)


def _page_query(namespace, table, after, source_generation=None):
    keys = _KEYS[table]
    ordering = ",".join('"' + name + '"' + (' COLLATE "C"' if kind == "text" else "") for name, kind in keys)
    parameters = list(after) if after is not None else []
    selectors = ",".join(
        f"${index}::{kind}" + (' COLLATE "C"' if kind == "text" else "") for index, (_, kind) in enumerate(keys, 1)
    )
    predicates = [f"({ordering})>({selectors})"] if after is not None else []
    if source_generation is not None:
        parameters.append(source_generation)
        predicates.append(f"source_generation=${len(parameters)}::text")
    predicate = "WHERE " + " AND ".join(predicates) if predicates else ""
    row_limit, byte_limit = len(parameters) + 1, len(parameters) + 2
    parameters.extend((PAGE_ROWS, PAGE_BYTES))
    return (
        f"""WITH page AS MATERIALIZED (
      SELECT stored.*,to_jsonb(stored)::text AS document FROM {namespace}.{table} stored {predicate}
      ORDER BY {ordering} LIMIT ${row_limit}
    ), sized AS (
      SELECT *,octet_length(document) AS row_bytes,sum(octet_length(document)+1) OVER(ORDER BY {ordering}) AS page_bytes,
        row_number() OVER(ORDER BY {ordering}) AS ordinal_position FROM page
    ) SELECT {",".join(name for name, _ in keys)},row_bytes,
      CASE WHEN row_bytes<=${byte_limit} THEN document END AS document
      FROM sized WHERE ordinal_position=1 OR page_bytes<=${byte_limit} ORDER BY {ordering}""",
        parameters,
    )


async def _table_digest(connection, namespace, table, source_generation=None):
    native_keys = await connection.fetchval(
        """SELECT jsonb_agg(jsonb_build_array(a.attname,CASE op.opcname
          WHEN 'int4_ops' THEN 'integer' WHEN 'uuid_ops' THEN 'uuid' ELSE 'text' END)
          ORDER BY key.ordinal)::text
        FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ix ON ix.oid=i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid=ix.relam AND am.amname='btree'
        CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal)
        JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum
        JOIN pg_catalog.pg_opclass op ON op.oid=i.indclass[(key.ordinal-1)::integer]
        WHERE i.indrelid=pg_catalog.to_regclass($1) AND i.indisprimary AND i.indisunique
          AND i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate
          AND i.indnkeyatts=i.indnatts AND i.indexprs IS NULL AND i.indpred IS NULL
          AND a.attnotnull AND NOT a.attisdropped AND op.opcnamespace='pg_catalog'::regnamespace
          AND op.opcmethod=am.oid AND op.opcname IN ('text_ops','int4_ops','uuid_ops')
          AND (a.atttypid=op.opcintype
            OR a.atttypid='pg_catalog.varchar'::regtype AND op.opcintype='pg_catalog.text'::regtype)
          AND i.indcollation[(key.ordinal-1)::integer]=a.attcollation
          AND (a.attcollation=0 OR a.attcollation='pg_catalog."C"'::regcollation)
        GROUP BY i.indexrelid,i.indnkeyatts HAVING count(*)=i.indnkeyatts""",
        namespace + "." + table,
    )
    if native_keys is None or json.loads(native_keys) != [list(key) for key in _KEYS[table]]:
        _invalid("validation_key_invalid")
    digest, count, batches, byte_count, after_keys = hashlib.sha256(), 0, 0, 0, None
    while True:
        sql, parameters = _page_query(namespace, table, after_keys, source_generation)
        entries = await connection.fetch(sql, *parameters)
        if not entries:
            break
        batches += 1
        for entry in entries:
            if entry["document"] is None or entry["row_bytes"] > PAGE_BYTES:
                _invalid("validation_bounds")
            document = json.loads(entry["document"])
            _validate_document(table, document)
            digest.update(canonical_json(document).encode() + b"\n")
            count += 1
            byte_count += entry["row_bytes"]
        after_keys = tuple(entries[-1][name] for name, _ in _KEYS[table])
    return {"rows": count, "sha256": digest.hexdigest(), "batches": batches, "bytes": byte_count}


async def fhir_catalog_observation_digest(connection, *, schema_name, source_generation):
    """Digest the actual compact facts for one source in an already-closed candidate."""
    _digest(source_generation)
    return await _table_digest(
        connection, storage_identifier(schema_name), "registry_fhir_catalog_observation", source_generation
    )


async def validate_fhir_catalog_observed_projection(
    connection, snapshot, *, standing_grant, prior_snapshot, registry_reader=None
):
    """Verify complete derived output after authenticated observations and writer closure."""
    from process.registry_fhir_catalog_projection_validation import validate_observed_projection

    await validate_observed_projection(
        connection,
        snapshot,
        standing_grant=standing_grant,
        prior_snapshot=prior_snapshot,
        registry_reader=registry_reader,
    )


_SET_SQL = """WITH observation_counts AS (
 SELECT source_generation,count(*) AS observations,count(DISTINCT(resource_type,resource_id)) AS resources
 FROM {namespace}.registry_fhir_catalog_observation GROUP BY source_generation
), vector AS (
 SELECT * FROM jsonb_to_recordset($3::jsonb) AS selected(source_generation text,source_id text,status text)
), source_bindings AS (
 SELECT b.*,s.source_generation AS matched_generation,s.descriptor_json,s.accepted_at,v.status AS source_status,
   b.document_json->'source_scope' ? 'insurance_plan_id' AS is_product,
   count(s.source_generation) OVER(PARTITION BY b.binding_id) AS source_matches
 FROM {namespace}.registry_fhir_catalog_binding b
 LEFT JOIN {namespace}.registry_fhir_catalog_source s
   ON b.document_json->>'evidence_id'=s.source_generation||':Organization/'||(b.document_json->>'source_key')
   OR (b.document_json->'source_scope' ? 'insurance_plan_id'
     AND s.profile='retained_normalized'
     AND s.descriptor_json->>'capture_sha256'=b.document_json->'source_scope'->>'capture_sha256'
     AND NOT EXISTS(SELECT FROM unnest(ARRAY['source_system','source_id','dataset_schema','dataset_id','producer_id','edition_id']) field
       WHERE b.document_json->>field IS DISTINCT FROM s.descriptor_json->'coordinates'->>field))
 LEFT JOIN vector v ON v.source_generation=s.source_generation
)
SELECT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_network n
 LEFT JOIN {control}.network_registry_identity i USING(network_id)
 LEFT JOIN {control}.registry_approved_record a ON a.approved_revision=$2 AND a.record_kind='network' AND a.record_key=n.network_id::text
 WHERE i.network_id IS NULL OR n.metadata_generation<>$1
   OR n.archived IS DISTINCT FROM coalesce((a.record_json->>'archived')::boolean,false))
OR EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_relationship r
 LEFT JOIN {namespace}.registry_fhir_catalog_network n USING(metadata_generation,network_id)
 LEFT JOIN source_bindings b USING(metadata_generation,network_id,binding_id,source_id)
 WHERE n.network_id IS NULL OR r.metadata_generation<>$1
   OR (r.source_id<>'registry-manual' AND (b.binding_id IS NULL OR b.matched_generation IS DISTINCT FROM r.source_generation))
   OR (r.source_id='registry-manual' AND (r.binding_id IS NOT NULL OR r.source_generation<>$4))
   OR (r.source_id='registry-manual' AND NOT EXISTS(SELECT FROM {control}.registry_approved_record a
     WHERE a.approved_revision=$2 AND a.record_kind='company_links' AND a.record_key=r.company_id::text
       AND a.record_json->'archived'='false'::jsonb AND a.record_json->'network_ids' @> jsonb_build_array(r.network_id)))
   OR (r.source_id<>'registry-manual' AND NOT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_observation o
     WHERE o.source_generation=r.source_generation AND o.observation_kind='edge'
       AND ((NOT b.is_product AND o.document_json->>'network_resource_id'=b.document_json->>'source_key')
         OR (b.is_product AND o.resource_type='InsurancePlan'
           AND o.resource_id=b.document_json->'source_scope'->>'insurance_plan_id'
           AND o.document_json->>'scope_path' ~ '^/normalized_aggregate/(0|[1-9][0-9]*)$'))
       AND o.document_json->>'plan_resource_id' IS NOT DISTINCT FROM r.document_json->'plan'->>'resource_id'
       AND (o.document_json->>'plan_resource_id' IS NULL OR (
         r.document_json->'plan'->>'scope_path'=o.resource_type||'/'||o.resource_id||(o.document_json->>'scope_path')
         AND r.document_json->'plan'->>'display_name' IS NOT DISTINCT FROM o.document_json->>'plan_name'
         AND r.document_json->'plan'->>'valid_from' IS NOT DISTINCT FROM o.document_json->>'valid_from'
         AND r.document_json->'plan'->>'valid_to' IS NOT DISTINCT FROM o.document_json->>'valid_to'))
       AND CASE r.document_json->>'company_role' WHEN 'insurer' THEN o.document_json->>'owner_ref'
         WHEN 'administrator' THEN o.document_json->>'administrator_ref' END
         IS NOT DISTINCT FROM r.document_json->>'organization_reference'))
   OR (r.source_id='registry-manual' AND r.company_id IS NOT NULL AND NOT EXISTS(SELECT FROM {control}.registry_approved_record a
     WHERE a.approved_revision=$2 AND a.record_kind='company' AND a.record_key=r.company_id::text
       AND a.record_json->'archived'='false'::jsonb AND a.record_json->>'display_name'=r.document_json->'company'->>'display_name'))
   OR (r.source_id='registry-manual' AND r.group_id IS NOT NULL AND NOT EXISTS(SELECT FROM {control}.registry_approved_record a
     WHERE a.approved_revision=$2 AND a.record_kind='group' AND a.record_key=r.group_id::text
       AND a.record_json->'archived'='false'::jsonb AND a.record_json->>'display_name'=r.document_json->'group'->>'display_name'
       AND a.record_json->>'group_kind'=r.document_json->'group'->>'group_kind')))
OR EXISTS(SELECT FROM source_bindings b
 LEFT JOIN {namespace}.registry_fhir_catalog_network n USING(metadata_generation,network_id)
 LEFT JOIN {namespace}.registry_fhir_catalog_observation o ON o.source_generation=b.matched_generation
   AND o.resource_type='Organization' AND o.resource_id=b.document_json->>'source_key' AND o.observation_kind='organization'
 LEFT JOIN {control}.registry_approved_record product ON b.is_product
   AND product.approved_revision=$2 AND product.record_kind='network_binding' AND product.record_key=b.binding_id::text
 WHERE n.network_id IS NULL OR b.metadata_generation<>$1 OR b.matched_generation IS NULL
   OR b.source_matches<>1
   OR b.source_id IS DISTINCT FROM b.descriptor_json->'coordinates'->>'source_id'
   OR (NOT b.is_product AND (b.document_json->'source_scope'->>'organization_id') IS DISTINCT FROM (b.document_json->>'source_key'))
   OR (b.document_json->>'producer_id'='registry-fhir-catalog-v1' AND (
     b.document_json->'source_scope'->>'source_generation' IS DISTINCT FROM b.matched_generation
     OR coalesce(b.descriptor_json->>'profile','') NOT IN ('published_normalized','legacy_published_normalized')))
   OR (NOT b.is_product AND (b.document_json->>'evidence_sha256') IS DISTINCT FROM (o.document_json->>'source_hash'))
   OR (b.is_product AND (product.record_key IS NULL
     OR product.record_json->'archived' IS DISTINCT FROM 'false'::jsonb
     OR product.record_json->>'network_id' IS DISTINCT FROM b.network_id::text
     OR product.record_json->'source_scope_json' IS DISTINCT FROM b.document_json->'source_scope'
     OR product.record_revision::text IS DISTINCT FROM b.document_json->>'revision'
     OR b.descriptor_json->>'profile' IS DISTINCT FROM 'retained_normalized'
     OR b.descriptor_json->'complete' IS DISTINCT FROM 'true'::jsonb
     OR b.document_json->'source_scope'->>'product_network_id' IS DISTINCT FROM b.network_id::text
     OR b.document_json->>'source_key' IS DISTINCT FROM 'InsurancePlan/'||(b.document_json->'source_scope'->>'insurance_plan_id')
     OR EXISTS(SELECT FROM unnest(ARRAY['source_system','source_id','dataset_schema','dataset_id','producer_id','edition_id',
         'source_key','evidence_id','evidence_sha256']) field
       WHERE b.document_json->>field IS DISTINCT FROM product.record_json->>field)))
   OR (b.document_json->>'status') IS DISTINCT FROM b.source_status
   OR (b.document_json->>'accepted_at')::timestamptz IS DISTINCT FROM b.accepted_at
   OR EXISTS(SELECT FROM unnest(ARRAY['source_system','source_id','dataset_schema','dataset_id','producer_id','edition_id']) field
     WHERE b.document_json->>field IS DISTINCT FROM b.descriptor_json->'coordinates'->>field))
OR EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_observation o
 LEFT JOIN {namespace}.registry_fhir_catalog_source s USING(source_generation) WHERE s.source_generation IS NULL)
OR EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_source s
 LEFT JOIN observation_counts counts USING(source_generation) LEFT JOIN vector v USING(source_generation)
 WHERE s.observation_rows<>coalesce(counts.observations,0) OR s.source_rows<coalesce(counts.resources,0)
   OR s.source_id IS DISTINCT FROM v.source_id OR v.source_generation IS NULL
   OR (v.status='current' AND s.descriptor_json->'complete'<>'true'::jsonb))
OR EXISTS(SELECT FROM vector v LEFT JOIN {namespace}.registry_fhir_catalog_source s USING(source_generation)
 WHERE s.source_generation IS NULL) AS invalid"""


async def _validate_sets(connection, namespace, generation, inputs, control_schema):
    invalid = await connection.fetchval(
        _SET_SQL.format(namespace=namespace, control=storage_identifier(control_schema)),
        generation,
        inputs["metadata_approved_revision"],
        canonical_json(inputs["source_vector_json"]),
        inputs["mapping_sha256"],
    )
    if invalid:
        _invalid("set_validation_failed")


def _content_digest(tables):
    content_by_table = {
        table: {field: report[field] for field in ("rows", "sha256")} for table, report in tables.items()
    }
    return hashlib.sha256(canonical_json(content_by_table).encode()).hexdigest()


_ORGANIZATION_PROOF_SQL = """WITH page AS MATERIALIZED (
 SELECT r.network_id,r.relationship_id,jsonb_build_object(
   'relationship',r.document_json,'source',s.descriptor_json,'organization',o.document_json,
   'parent',p.document_json,'edges',e.documents,
   'approved_company',a.record_json,'approved_group',g.record_json,
   'product_binding_id',CASE WHEN b.document_json->'source_scope' ? 'insurance_plan_id' THEN b.binding_id::text END,
   'source_generation',r.source_generation,'network_id',r.network_id)::text AS document
 FROM {namespace}.registry_fhir_catalog_relationship r
 JOIN {namespace}.registry_fhir_catalog_source s USING(source_generation)
 LEFT JOIN {namespace}.registry_fhir_catalog_binding b ON b.binding_id=r.binding_id AND b.network_id=r.network_id
 LEFT JOIN LATERAL (SELECT jsonb_agg(candidate.document_json ORDER BY candidate.ordinal) AS documents FROM (
   SELECT edge.document_json,edge.ordinal FROM {namespace}.registry_fhir_catalog_observation edge
   WHERE edge.source_generation=r.source_generation AND edge.observation_kind='edge'
     AND edge.resource_type='InsurancePlan' AND edge.resource_id=r.document_json->'plan'->>'resource_id'
     AND edge.document_json->>'plan_resource_id'=r.document_json->'plan'->>'resource_id'
     AND r.document_json->'plan'->>'scope_path'=edge.resource_type||'/'||edge.resource_id||(edge.document_json->>'scope_path')
     AND ((NOT(b.document_json->'source_scope' ? 'insurance_plan_id')
         AND edge.document_json->>'network_resource_id'=b.document_json->>'source_key')
       OR (b.document_json->'source_scope' ? 'insurance_plan_id'
         AND edge.resource_id=b.document_json->'source_scope'->>'insurance_plan_id'
         AND edge.document_json->>'scope_path' ~ '^/normalized_aggregate/(0|[1-9][0-9]*)$'))
     AND CASE r.document_json->>'company_role' WHEN 'insurer' THEN edge.document_json->>'owner_ref'
         WHEN 'administrator' THEN edge.document_json->>'administrator_ref' END=r.document_json->>'organization_reference'
   ORDER BY edge.ordinal LIMIT 1001) candidate) e ON true
 LEFT JOIN {namespace}.registry_fhir_catalog_observation o ON o.source_generation=r.source_generation
   AND o.resource_type='Organization' AND o.observation_kind='organization'
   AND o.resource_id=regexp_replace(r.document_json->>'organization_reference','^.*/','')
 LEFT JOIN {namespace}.registry_fhir_catalog_observation p ON p.source_generation=r.source_generation
   AND p.resource_type='Organization' AND p.observation_kind='organization'
   AND p.resource_id=regexp_replace(o.document_json->>'part_of_ref','^.*/','')
 LEFT JOIN {control}.registry_approved_record a ON a.approved_revision=$1
   AND a.record_kind='company' AND a.record_key=r.company_id::text
 LEFT JOIN {control}.registry_approved_record g ON g.approved_revision=$1
   AND g.record_kind='group' AND g.record_key=r.group_id::text
 WHERE r.source_id<>'registry-manual' AND (r.company_id IS NOT NULL OR r.group_id IS NOT NULL)
   AND (r.network_id,r.relationship_id)>($2,$3)
 ORDER BY r.network_id,r.relationship_id LIMIT $4
), sized AS (
 SELECT *,octet_length(document) AS row_bytes,
 sum(octet_length(document)+1) OVER(ORDER BY network_id,relationship_id) AS page_bytes,
 row_number() OVER(ORDER BY network_id,relationship_id) AS ordinal FROM page
) SELECT network_id,relationship_id,row_bytes,CASE WHEN row_bytes<=$5 THEN document END AS document
 FROM sized WHERE ordinal=1 OR page_bytes<=$5 ORDER BY network_id,relationship_id"""


def _has_matching_organization(reference, organization, source_uri):
    resource_id, reason = fhir_network_reference_target(reference, source_uri)
    return organization is not None and reason is None and organization["resource_id"] == resource_id


def _has_matching_approved_organization(actual, approved, identity):
    return (
        approved is not None
        and approved.get("archived") is False
        and all(actual[field] == approved[field] for field in (identity, "display_name"))
    )


def _validate_approved_organization_proof(proof):
    """Keep direct legacy sealing limited to exact approved organization records."""
    relationship = proof["relationship"]
    company, group = relationship["company"], relationship["group"]
    if company is None or not _has_matching_approved_organization(company, proof["approved_company"], "company_id"):
        _invalid("organization_grant_required")
    if not _has_matching_organization(
        relationship["organization_reference"], proof["organization"], fhir_source_uri(proof["source"]["source_uri"])
    ):
        _invalid("organization_proof_invalid")
    if (
        group is not None
        and group["group_id"] is not None
        and (
            not _has_matching_approved_organization(group, proof["approved_group"], "group_id")
            or group["group_kind"] != proof["approved_group"]["group_kind"]
        )
    ):
        _invalid("organization_proof_invalid")


def validate_fhir_catalog_organization_batch(proofs, context):
    """Replay bounded source facts against the actual approved maps and grant crosswalk."""
    organizations_by_id, edges_by_key = {}, {}
    for proof in proofs:
        for key in ("organization", "parent"):
            document = proof[key]
            if document is not None:
                organizations_by_id[document["resource_id"]] = FHIRCatalogOrganization(
                    **{**document, "aliases": tuple(document["aliases"]), "identifiers": tuple(document["identifiers"])}
                )
        candidates = proof["edges"]
        if type(candidates) is not list or not 0 < len(candidates) <= PAGE_ROWS:
            _invalid("organization_proof_invalid")
        for edge in candidates:
            edge_by_field = {**edge}
            for key in (
                "plan_aliases",
                "plan_identifiers",
                "scope_identifiers",
                "scope_type_codes",
                "benefits",
                "geography",
            ):
                edge_by_field[key] = tuple(edge_by_field[key])
            edges_by_key[canonical_json(edge)] = FHIRCatalogReferenceEdge(**edge_by_field)
    from process.registry_fhir_catalog_projection import reviewed_fhir_catalog_product_relationship_id

    expected_by_id = {}
    requested_ids = {proof["relationship"]["relationship_id"] for proof in proofs}
    products_by_path = {}
    for proof in proofs:
        if proof.get("product_binding_id") is not None:
            path = proof["relationship"]["plan"]["scope_path"]
            products_by_path.setdefault(path, set()).add(
                (proof["network_id"], proof["product_binding_id"], proof["source_generation"])
            )
    edges = tuple(edges_by_key.values())
    for offset in range(0, len(edges), 100):
        for relationship in enrich_fhir_catalog_edges(
            tuple(organizations_by_id.values()), edges[offset : offset + 100], context
        ):
            if relationship.document["relationship_id"] in requested_ids:
                expected_by_id[relationship.document["relationship_id"]] = relationship.document
            plan = relationship.document["plan"]
            for product in products_by_path.get(plan["scope_path"] if plan is not None else None, ()):
                identifier = reviewed_fhir_catalog_product_relationship_id(
                    *product, relationship.document["relationship_id"]
                )
                if identifier in requested_ids:
                    expected_by_id[identifier] = {**relationship.document, "relationship_id": identifier}
    if any(expected_by_id.get(proof["relationship"]["relationship_id"]) != proof["relationship"] for proof in proofs):
        _invalid("organization_precedence_invalid")


async def _organization_maps(connection, control, revision):
    records = await connection.fetch(
        f"SELECT record_kind,record_json::text AS document FROM {control}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind IN ('company','group') ORDER BY record_kind,record_key LIMIT 2001",
        revision,
    )
    records_by_kind = {
        kind: tuple(json.loads(row["document"]) for row in records if row["record_kind"] == kind)
        for kind in ("company", "group")
    }
    if any(len(documents) > PAGE_ROWS for documents in records_by_kind.values()):
        _invalid("validation_bounds")
    return records_by_kind


def _replay_organization_page(proofs, maps, inputs, standing_grant):
    proofs_by_source = {}
    for proof in proofs:
        if standing_grant is None:
            _validate_approved_organization_proof(proof)
            continue
        proofs_by_source.setdefault(canonical_json(proof["source"]), []).append(proof)
    for same_source in proofs_by_source.values():
        descriptor = same_source[0]["source"]
        context = FHIRCatalogEnrichmentContext(
            descriptor["coordinates"]["source_id"],
            date.fromisoformat(inputs["as_of"]),
            maps["company"],
            maps["group"],
            descriptor["endpoint_id"],
            standing_grant.crosswalk_for(descriptor["coordinates"]["source_id"]),
            descriptor["source_uri"],
        )
        for offset in range(0, len(same_source), 100):
            validate_fhir_catalog_organization_batch(same_source[offset : offset + 100], context)


async def _validate_source_organizations(connection, namespace, control, inputs, standing_grant):
    revision = inputs["metadata_approved_revision"]
    maps = await _organization_maps(connection, control, revision)
    after = (0, "")
    while True:
        rows = await connection.fetch(
            _ORGANIZATION_PROOF_SQL.format(namespace=namespace, control=control),
            revision,
            *after,
            PAGE_ROWS,
            PAGE_BYTES,
        )
        if not rows:
            return
        proofs = []
        for row in rows:
            if row["document"] is None or row["row_bytes"] > PAGE_BYTES:
                _invalid("validation_bounds")
            proof = json.loads(row["document"])
            proofs.append(proof)
        _replay_organization_page(proofs, maps, inputs, standing_grant)
        after = (rows[-1]["network_id"], rows[-1]["relationship_id"])


async def validate_fhir_catalog_candidate(connection, copy_target, snapshot, *, control_schema, standing_grant=None):
    """Validate and digest complete PK-ordered batches only after the writer fence."""
    generation = metadata_generation(snapshot)
    if snapshot["metadata_generation"] != generation or snapshot["schema_name"] != copy_target.schema_name:
        _invalid("inputs_changed")
    namespace = storage_identifier(copy_target.schema_name)
    accounting_by_table = {}
    for table in sorted(_KEYS):
        accounting_by_table[table] = await _table_digest(connection, namespace, table)
    inputs = snapshot_inputs(snapshot)
    if standing_grant is not None:
        from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant

        if (
            type(standing_grant) is not FHIRCatalogStandingGrant
            or standing_grant.sha256 != inputs["standing_grant_sha256"]
        ):
            _invalid("inputs_changed")
    fingerprints = await approved_fhir_catalog_fingerprints(
        connection, (inputs["metadata_approved_revision"],), control_schema=control_schema
    )
    if fingerprints[inputs["metadata_approved_revision"]] != {
        "metadata": inputs["mapping_sha256"],
        "membership": inputs["membership_mapping_sha256"],
    }:
        _invalid("inputs_changed")
    await _validate_source_organizations(
        connection, namespace, storage_identifier(control_schema), inputs, standing_grant
    )
    await _validate_sets(connection, namespace, generation, inputs, control_schema)
    return {
        "metadata_generation": generation,
        "inputs": inputs,
        "tables": accounting_by_table,
        "content_sha256": _content_digest(accounting_by_table),
    }


def verify_fhir_catalog_validation(snapshot, validation):
    """Verify the retained input/content attestation without scanning sealed heaps."""
    if type(validation) is not dict or set(validation) != {"metadata_generation", "inputs", "tables", "content_sha256"}:
        _invalid("custody_invalid")
    generation = metadata_generation(snapshot)
    if (
        snapshot["metadata_generation"] != generation
        or validation["metadata_generation"] != generation
        or validation["inputs"] != snapshot_inputs(snapshot)
    ):
        _invalid("inputs_changed")
    tables = validation["tables"]
    if type(tables) is not dict or set(tables) != set(_KEYS):
        _invalid("custody_invalid")
    for report in tables.values():
        if type(report) is not dict or set(report) != {"rows", "sha256", "batches", "bytes"}:
            _invalid("custody_invalid")
        _digest(report["sha256"])
        if any(type(report[field]) is not int or report[field] < 0 for field in ("rows", "batches", "bytes")):
            _invalid("custody_invalid")
        if not (report["rows"] >= report["batches"] >= bool(report["rows"])):
            _invalid("custody_invalid")
    if validation["content_sha256"] != _content_digest(tables):
        _invalid("custody_invalid")
    accounting = snapshot.get("accounting_json")
    if type(accounting) is str:
        accounting = json.loads(accounting)
    if accounting is not None and accounting != tables:
        _invalid("inputs_changed")
