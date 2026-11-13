# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate published native generic snapshots through the original Reader."""

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, require
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT


def published_generic_binding(header, *, dataset_id, endpoint_id, root_run_id, source_id, schema_name=None):
    """Check publication metadata only; a valid document grants no native capability."""
    require(header["completion_proof_required_version"] is None)
    binding = storage._json(header["candidate_storage_json"])
    if binding is None:
        return None
    require(type(binding) is dict)
    if binding.get("contract") == storage.NAMED_ISOLATED_CONTRACT:
        binding = _named_published_binding(header, binding, schema_name)
    else:
        require(binding.get("contract") == storage.CONTRACT)
        binding = _named_attached_binding(binding, schema_name)
    identity = binding.get("identity")
    require(type(identity) is dict and type(identity.get("metadata")) is dict)
    if binding["contract"] == storage.CONTRACT:
        storage._validate_binding(binding, identity, phases=("published",))
        if schema_name is not None:
            require(all(table["schema"] == schema_name for table in binding["tables"].values()))
    require(
        (identity["dataset_id"], identity["endpoint_id"], identity["acquisition_root_run_id"])
        == (dataset_id, endpoint_id, root_run_id)
        and identity["metadata"].get("source_ids") == [source_id]
        and identity["metadata"].get("resource_hash_contract") == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    return binding


def _named_published_binding(header, binding, catalog_schema):
    """Validate the unchanged named handoff against its actual published catalog row."""
    require(type(catalog_schema) is str and type(binding.get("storage")) is dict)
    storage.validate_handoff_document(
        binding,
        {"catalog_schema": catalog_schema, "storage_schema": binding["storage"].get("storage_schema")},
    )
    require(
        header["status"] in {"published", "superseded"}
        and header["published_at"] is not None
        and header["is_current"] is (header["status"] == "published")
        and {field: header[field] for field in storage.HANDOFF_HEADER_FIELDS} == binding["admission"]
    )
    return binding


def _named_attached_binding(binding, schema_name):
    """Project the one historical attached table layout using its recorded names."""
    if type(binding.get("tables")) is not dict:
        return binding
    table_by_root = {}
    legacy_fields = {"name", "oid", "owner", "layout_sha256"}
    for root, table in binding["tables"].items():
        if type(table) is dict and set(table) == legacy_fields:
            require(type(schema_name) is str)
            table_by_root[root] = {"schema": schema_name, "name": table["name"]}
        else:
            table_by_root[root] = table
    return {**binding, "tables": table_by_root}


async def _require_header_binding(reader, origin, binding):
    """Match every immutable acquisition field against the complete published header."""
    header = await reader.fetchrow(
        f"SELECT candidate_storage_json,completion_proof_required_version,endpoint_id,acquisition_root_run_id,"
        "dataset_hash,status,published_at,is_current,resource_count,previous_dataset_id,completion_proof_sha256,"
        "NOT EXISTS(SELECT FROM jsonb_each($2::jsonb) acquisition(key,value) "
        "WHERE coalesce(d.publication_metadata_json->acquisition.key,'null'::jsonb) "
        "IS DISTINCT FROM acquisition.value) AS metadata_matches "
        f"FROM {storage_relation(origin.header_relation['schema'], origin.header_relation['name'])} d "
        "WHERE dataset_id=$1",
        origin.dataset_id,
        storage._encoded(binding["identity"]["metadata"]),
    )
    require(
        header is not None
        and header["status"] in {"published", "superseded"}
        and header["published_at"] is not None
        and header["metadata_matches"] is True
        and (header["endpoint_id"], header["acquisition_root_run_id"], header["dataset_hash"])
        == (origin.endpoint_id, origin.root_run_id, origin.dataset_sha256)
    )
    current = published_generic_binding(
        header,
        dataset_id=origin.dataset_id,
        endpoint_id=origin.endpoint_id,
        root_run_id=origin.root_run_id,
        source_id=origin.source_id,
        schema_name=origin.header_relation["schema"],
    )
    require(current == binding)


async def require_native_generic_snapshot(reader, writer, origin, policy, binding):
    """Validate the original named publication using the fixed Source policy and data scope."""
    require(
        type(policy) is GenericSourcePolicy
        and policy.schema == origin.schema_name
        and origin.admission_profile == SEMANTIC_PROFILE
        and reader.is_in_transaction()
        and writer.is_in_transaction()
        and binding is not None
    )
    await _require_header_binding(reader, origin, binding)
    resource = binding["tables"][storage.RESOURCE]
    require(origin.resource_relation in (resource, {"schema": origin.schema_name, "name": storage.RESOURCE}))
    for relation in binding["tables"].values():
        await reader.fetch("SELECT * FROM " + storage_relation(relation["schema"], relation["name"]) + " LIMIT 0")
    return tuple((root, table["schema"], table["name"]) for root, table in sorted(binding["tables"].items()))
