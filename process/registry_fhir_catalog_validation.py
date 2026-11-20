# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final bounded batch validation after metadata candidate writers close."""

import hashlib
import json
import re
from datetime import date, datetime, timezone

from db.plan_release_storage import storage_identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_extraction import (
    fhir_source_uri,
)
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_source_companies import validate_fhir_catalog_source_companies
from process.registry_fhir_plan_tier import (
    SUPPORTED_TRANSFORMS,
    TRANSFORM_VERSION,
)
from process.registry_network_catalog_read import (
    catalog_text,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

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
_TABLES = (
    "registry_fhir_catalog_binding",
    "registry_fhir_catalog_network",
    "registry_fhir_catalog_observation",
    "registry_fhir_catalog_relationship",
    "registry_fhir_catalog_source",
)
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
        fields = {"source_generation", "source_id", "status"}
        optional_fields = {"extraction_transform", "source_companies"}
        if type(entry) is not dict or not fields <= set(entry) <= fields | optional_fields:
            _invalid("inputs_invalid")
        if "extraction_transform" in entry and (
            entry["extraction_transform"] != TRANSFORM_VERSION or entry["source_id"] != UHC_PROVIDER_FILE_SOURCE_ID
        ):
            _invalid("inputs_invalid")
        if "source_companies" in entry:
            validate_fhir_catalog_source_companies(entry["source_companies"])
        _digest(entry["source_generation"])
        catalog_text(entry["source_id"], 128)
        key = (entry["source_id"], entry["source_generation"])
        if entry["status"] not in {"current", "last_good"} or entry["source_id"] == "registry-manual":
            _invalid("inputs_invalid")
        if entry["source_generation"] in seen or previous is not None and key <= previous:
            _invalid("inputs_invalid")
        seen.add(entry["source_generation"])
        previous = key


def snapshot_inputs(snapshot):
    """Normalize serialized pins without including physical or publication state."""
    inputs_by_field = {field: snapshot[field] for field in INPUT_FIELDS}
    if type(inputs_by_field["source_vector_json"]) is str:
        inputs_by_field["source_vector_json"] = json.loads(inputs_by_field["source_vector_json"])
    if type(inputs_by_field["as_of"]) is date:
        inputs_by_field["as_of"] = inputs_by_field["as_of"].isoformat()
    if inputs_by_field["projector_version"] not in SUPPORTED_TRANSFORMS:
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
    if inputs_by_field["projector_version"] != TRANSFORM_VERSION and any(
        "extraction_transform" in entry for entry in inputs_by_field["source_vector_json"]
    ):
        _invalid("inputs_invalid")
    for field, minimum in (("metadata_approved_revision", 0),):
        if type(inputs_by_field[field]) is not int or not minimum <= inputs_by_field[field] < 2**63:
            _invalid("inputs_invalid")
    as_of = inputs_by_field["as_of"]
    if type(as_of) is not str or date.fromisoformat(as_of).isoformat() != as_of:
        _invalid("inputs_invalid")
    accounting = snapshot.get("accounting_json") or {}
    accounting = json.loads(accounting) if type(accounting) is str else accounting
    native_inputs = snapshot.get("imported_source_inventory", accounting.get("imported_source_inventory"))
    if native_inputs is not None:
        if (
            type(native_inputs) is not dict
            or set(native_inputs) != {"contract", "token", "native_sources"}
            or native_inputs["contract"] != "registry-imported-catalog-inputs-v1"
            or type(native_inputs["native_sources"]) is not list
            or len(native_inputs["native_sources"]) > 1000
        ):
            _invalid("inputs_invalid")
        _digest(native_inputs["token"])
        inputs_by_field["imported_source_inventory"] = native_inputs
    return inputs_by_field


def metadata_generation(snapshot):
    """Hash semantic pins independently of the metadata candidate location."""
    inputs = snapshot_inputs(snapshot)
    native_inputs = inputs.get("imported_source_inventory")
    if native_inputs is not None:
        # Membership points back to this cohort; association fields cannot also define its content hash.
        inputs.update(network_generation=None, manifest_sha256=None)
    return hashlib.sha256(canonical_json(inputs).encode()).hexdigest()


def _source_descriptor(document):
    """Dispatch only explicit native descriptors; historical FHIR records keep their existing validator."""
    from process.registry_native_network_catalog import is_native_catalog_source, validate_native_catalog_descriptor

    if is_native_catalog_source(document):
        return validate_native_catalog_descriptor(document)["coordinates"]
    return _fhir_source_descriptor(document)


def _fhir_source_descriptor(document):
    """Validate the supported historical or named FHIR source data shape."""
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
    }
    # Descriptors carried from before named publication record a resource heap number instead of table names;
    # their content stays bound by the source generation digest over the stored document.
    if (
        type(document) is not dict
        or not required_fields <= document.keys()
        or ("tables" in document) == ("resource_table_oid" in document)
    ):
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
    tables = document.get("tables", {})
    if "tables" in document and (type(tables) is not dict or set(tables) != {"header", "resource"}):
        _invalid("source_invalid")
    for relation in tables.values():
        if type(relation) is not dict or set(relation) != {"schema", "name"}:
            _invalid("source_invalid")
        storage_identifier(relation["schema"])
        storage_identifier(relation["name"])
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
    for field in ("accepted_at", "created_at"):
        timestamp = datetime.fromisoformat(row[field])
        if timestamp.utcoffset() is None:
            _invalid("source_invalid")
        row[field] = timestamp.astimezone(timezone.utc).isoformat()


_SET_SQL = """WITH observation_counts AS (
 SELECT source_generation,count(*) AS observations,count(DISTINCT(resource_type,resource_id)) AS resources
 FROM {namespace}.registry_fhir_catalog_observation GROUP BY source_generation
), vector AS (
 SELECT * FROM jsonb_to_recordset($3::jsonb) AS selected(source_generation text,source_id text,status text)
), source_bindings AS (
 SELECT b.*,s.source_generation AS matched_generation,s.descriptor_json,s.accepted_at,v.status AS source_status,
   coalesce(s.descriptor_json->>'contract'='registry-native-network-source-v1',false) AS is_native,
   b.document_json->'source_scope' ? 'insurance_plan_id' AS is_product,
   coalesce(b.document_json->'source_scope'->>'kind'='uhc_plan_tier_v1',false) AS is_tier,
   count(s.source_generation) OVER(PARTITION BY b.binding_id) AS source_matches
 FROM {namespace}.registry_fhir_catalog_binding b
 LEFT JOIN {namespace}.registry_fhir_catalog_source s
   ON b.document_json->>'evidence_id'=s.source_generation||':Organization/'||(b.document_json->>'source_key')
   OR (s.descriptor_json->>'contract'='registry-native-network-source-v1'
     AND b.document_json->>'producer_id'='registry-native-network-source-v1'
     AND b.document_json->'source_scope'->>'source_generation'=s.source_generation)
   OR (b.document_json->'source_scope'->>'kind'='uhc_plan_tier_v1' AND s.profile='published_normalized'
     AND b.document_json->'source_scope'->>'source_generation'=s.source_generation)
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
 LEFT JOIN source_bindings b USING(metadata_generation,network_id,binding_id,source_id)
 WHERE NOT coalesce((SELECT true FROM {namespace}.registry_fhir_catalog_network n
   WHERE n.metadata_generation=r.metadata_generation AND n.network_id=r.network_id LIMIT 1),false)
   OR r.metadata_generation<>$1
   OR (r.source_id<>'registry-manual' AND (b.binding_id IS NULL OR b.matched_generation IS DISTINCT FROM r.source_generation))
   OR (r.source_id='registry-manual' AND (r.binding_id IS NOT NULL OR r.source_generation<>$4))
   OR (r.source_id='registry-manual' AND NOT EXISTS(SELECT FROM {control}.registry_approved_record a
     WHERE a.approved_revision=$2 AND a.record_kind='company_links' AND a.record_key=r.company_id::text
       AND a.record_json->'archived'='false'::jsonb AND a.record_json->'network_ids' @> jsonb_build_array(r.network_id)))
   OR (r.source_id<>'registry-manual' AND NOT b.is_tier AND NOT b.is_native AND NOT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_observation o
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
   OR (NOT b.is_product AND NOT b.is_tier AND NOT b.is_native AND (b.document_json->'source_scope'->>'organization_id') IS DISTINCT FROM (b.document_json->>'source_key'))
   OR (b.document_json->>'producer_id'='registry-fhir-catalog-v1' AND (
     b.document_json->'source_scope'->>'source_generation' IS DISTINCT FROM b.matched_generation
     OR coalesce(b.descriptor_json->>'profile','') NOT IN ('published_normalized','legacy_published_normalized')))
   OR (NOT b.is_product AND NOT b.is_tier AND NOT b.is_native AND (b.document_json->>'evidence_sha256') IS DISTINCT FROM (o.document_json->>'source_hash'))
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


# Every approved retained product projects at least its binding row (keyed by the approved record key);
# its relationships may legitimately be absent when none resolve to the product network.
_PRODUCTS_SQL = """SELECT EXISTS(SELECT FROM {control}.registry_approved_record a
 WHERE a.approved_revision=$2 AND a.record_kind='network_binding' AND a.record_json->'archived'='false'::jsonb
   AND a.record_json->>'source_system'='fhir'
   AND (a.record_json->'source_scope_json' ? 'insurance_plan_id' OR a.record_json->>'source_key' LIKE 'InsurancePlan/%')
   AND NOT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_binding b WHERE b.metadata_generation=$1
     AND b.binding_id=CASE WHEN pg_input_is_valid(a.record_key,'uuid') THEN a.record_key::uuid END))"""


async def _validate_sets(connection, namespace, generation, inputs, control_schema):
    control = storage_identifier(control_schema)
    invalid = await connection.fetchval(
        _SET_SQL.format(namespace=namespace, control=control),
        generation,
        inputs["metadata_approved_revision"],
        canonical_json(inputs["source_vector_json"]),
        inputs["mapping_sha256"],
    )
    if invalid or await connection.fetchval(
        _PRODUCTS_SQL.format(namespace=namespace, control=control), generation, inputs["metadata_approved_revision"]
    ):
        _invalid("set_validation_failed")


def _content_digest(tables):
    content_by_table = {
        table: {field: report[field] for field in ("rows", "sha256")} for table, report in tables.items()
    }
    return hashlib.sha256(canonical_json(content_by_table).encode()).hexdigest()


def _accounting(table, rows, byte_count):
    """Report one table in the retained record shape."""
    # sha256 is an accounting digest of the table name, rows and bytes, not a hash of row content.
    accounting = canonical_json({"table": table, "rows": rows, "bytes": byte_count})
    return {
        "rows": rows,
        "sha256": hashlib.sha256(accounting.encode()).hexdigest(),
        "batches": -(-rows // PAGE_ROWS),
        "bytes": byte_count,
    }


async def _account_tables(connection, namespace):
    """Count rows and stored row bytes in one statement; primary keys already exclude duplicates."""
    # Row datum sizes, not relation file sizes: re-sealing the same rows must reproduce the same report.
    columns = ",".join(
        f"(SELECT row(count(*),coalesce(sum(pg_column_size(stored)),0)::bigint) FROM {namespace}.{table} stored)"
        for table in _TABLES
    )
    counts = await connection.fetchrow(f"SELECT {columns}")
    return {table: _accounting(table, *counts[index]) for index, table in enumerate(_TABLES)}


async def _validate_sources(connection, namespace):
    """Check the small source table's descriptors against their own columns."""
    source_rows = await connection.fetch(
        f"SELECT to_jsonb(stored)::text AS document FROM {namespace}.registry_fhir_catalog_source stored"
    )
    for source in source_rows:
        _source_document(json.loads(source["document"]))


async def _validate_prior_networks(connection, namespace, control, inputs):
    """Refuse a successor that drops a network the serving generation still publishes."""
    if "imported_source_inventory" in inputs:
        return
    prior = await connection.fetchrow(
        f"SELECT c.metadata_generation,s.schema_name FROM {control}.network_serving_control c "
        f"LEFT JOIN {control}.registry_fhir_catalog_snapshot s "
        "ON s.metadata_generation=c.metadata_generation AND s.published_at IS NOT NULL WHERE c.id=1"
    )
    if prior is None or prior["metadata_generation"] is None:
        return
    if prior["schema_name"] is None:
        _invalid("snapshot_unavailable")
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {storage_identifier(prior['schema_name'])}.registry_fhir_catalog_network prior "
        f"LEFT JOIN {namespace}.registry_fhir_catalog_network candidate USING(network_id) "
        "WHERE candidate.network_id IS NULL)"
    ):
        _invalid("observation_history_changed")


async def validate_fhir_catalog_candidate(connection, copy_target, snapshot, *, control_schema, standing_grant=None):
    """Account and validate complete candidate data after the writer fence."""
    generation = metadata_generation(snapshot)
    if snapshot["metadata_generation"] != generation or snapshot["schema_name"] != copy_target.schema_name:
        _invalid("inputs_changed")
    namespace = storage_identifier(copy_target.schema_name)
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
    await _validate_sources(connection, namespace)
    accounting_by_table = await _account_tables(connection, namespace)
    await _validate_sets(connection, namespace, generation, inputs, control_schema)
    await _validate_prior_networks(connection, namespace, storage_identifier(control_schema), inputs)
    return {
        "metadata_generation": generation,
        "inputs": inputs,
        "tables": accounting_by_table,
        "content_sha256": _content_digest(accounting_by_table),
    }


def verify_fhir_catalog_validation(snapshot, validation):
    """Check the retained logical inputs and complete content accounting."""
    if type(validation) is not dict or set(validation) != {"metadata_generation", "inputs", "tables", "content_sha256"}:
        _invalid("validation_invalid")
    generation = metadata_generation(snapshot)
    expected_by_field = snapshot_inputs(snapshot)
    retained_by_field = validation["inputs"]
    if type(retained_by_field) is not dict:
        _invalid("inputs_changed")
    if "imported_source_inventory" in expected_by_field:
        expected_by_field = {**expected_by_field, "network_generation": None, "manifest_sha256": None}
        retained_by_field = {**retained_by_field, "network_generation": None, "manifest_sha256": None}
    if (
        snapshot["metadata_generation"] != generation
        or validation["metadata_generation"] != generation
        or retained_by_field != expected_by_field
    ):
        _invalid("inputs_changed")
    tables = validation["tables"]
    if type(tables) is not dict or set(tables) != set(_TABLES):
        _invalid("validation_invalid")
    for report in tables.values():
        if type(report) is not dict or set(report) != {"rows", "sha256", "batches", "bytes"}:
            _invalid("validation_invalid")
        _digest(report["sha256"])
        if any(type(report[field]) is not int or report[field] < 0 for field in ("rows", "batches", "bytes")):
            _invalid("validation_invalid")
        if not (report["rows"] >= report["batches"] >= bool(report["rows"])):
            _invalid("validation_invalid")
    if validation["content_sha256"] != _content_digest(tables):
        _invalid("validation_invalid")
    accounting = snapshot.get("accounting_json")
    if type(accounting) is str:
        accounting = json.loads(accounting)
    expected_accounting_by_key = dict(tables)
    if "imported_source_inventory" in expected_by_field:
        expected_accounting_by_key["imported_source_inventory"] = expected_by_field["imported_source_inventory"]
    if accounting is not None and accounting != expected_accounting_by_key:
        _invalid("inputs_changed")
