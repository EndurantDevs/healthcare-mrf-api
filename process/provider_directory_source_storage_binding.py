# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed isolated SOURCE coordinates; data alone never grants publisher authority."""

import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass

from db.plan_release_storage import storage_identifier
from process.provider_directory_generic_candidate import GenericCandidateError

SHARED_BODY_FORMAT = "provider-directory-shared-bodies-v1"
COMPACT_V2_FORMAT = "provider-directory-compact-v2"
_RELATIONS = ContextVar("provider_directory_isolated_source_relations", default=None)
_CATALOG_ROOTS = frozenset({"provider_directory_source", "provider_directory_api_endpoint", "import_run"})


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_source_" + code)


@dataclass(frozen=True)
class FHIRSourceStorageBinding:
    """Schema-qualified storage supplied by import orchestration."""

    catalog_schema: str
    storage_schema: str
    payload_format = None

    def __post_init__(self):
        for schema in (self.catalog_schema, self.storage_schema):
            storage_identifier(schema)
            _require(schema not in {"public", "information_schema"} and not schema.startswith("pg_"), "schema_invalid")
        _require(self.catalog_schema != self.storage_schema, "isolated_schema_required")


NamedFHIRSourceStorage = FHIRSourceStorageBinding


@dataclass(frozen=True)
class SharedBodyFHIRSourceStorage(FHIRSourceStorageBinding):
    """Select shared payload bodies while keeping the same storage coordinates."""

    payload_format = SHARED_BODY_FORMAT


@dataclass(frozen=True)
class CompactV2FHIRSourceStorage(FHIRSourceStorageBinding):
    """Select unified compact-v2 block-packed shared bodies and token sets."""

    payload_format = COMPACT_V2_FORMAT


def is_named_source_storage(binding):
    """Recognize the issued named storage bindings."""
    return type(binding) in (FHIRSourceStorageBinding, SharedBodyFHIRSourceStorage, CompactV2FHIRSourceStorage)


@dataclass(frozen=True)
class FHIRSourceCandidateHandoff:
    """Named candidate data awaiting validation and publication."""

    document_json: str

    @property
    def document_sha256(self):
        """Hash the complete canonical snapshot document, including its proofs."""
        encoded = json.dumps(self.document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        return hashlib.sha256(encoded.encode()).hexdigest()

    @property
    def document(self):
        """Return a copy without claiming the underlying tables are immutable."""
        return named_source_document(json.loads(self.document_json))


def named_source_document(document):
    """Read retained candidates using their recorded names and logical data only."""
    if not isinstance(document, dict) or not isinstance(document.get("storage"), dict):
        return document
    storage = document["storage"]
    if not {"catalog_schema", "storage_schema"} <= set(storage):
        return document
    document_by_field = {
        **document,
        "storage": {field: storage[field] for field in ("catalog_schema", "storage_schema")},
    }
    if isinstance(document.get("tables"), dict):
        document_by_field["tables"] = {
            root: {"schema": table.get("schema", storage["storage_schema"]), "name": table.get("name")}
            if isinstance(table, dict)
            else table
            for root, table in document["tables"].items()
        }
    if document.get("contract") == "healthporta.provider-directory.rooted-candidate.v1":
        document_by_field.pop("catalog", None)
        has_legacy_coordinates = "catalog" in document or bool(set(storage) - {"catalog_schema", "storage_schema"})
        if "inputs" not in document and has_legacy_coordinates:
            document_by_field["inputs"] = {
                "provider_directory_rooted_graph_" + root: {
                    "schema": storage["catalog_schema"],
                    "name": "provider_directory_rooted_graph_" + root,
                }
                for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
            }
        document_by_field["snapshot"] = _logical_snapshot(document.get("snapshot"))
        previous_by_field = document.get("predecessor_proof")
        if isinstance(previous_by_field, dict) and "snapshot" in previous_by_field:
            previous_by_field = _project_rooted_predecessor(document, previous_by_field)
            document_by_field["predecessor_proof"] = previous_by_field
    if document.get("contract") in {"generic-source-detached-v1", "generic-source-detached-by-name-v1"}:
        document_by_field["relationship_sha256"] = None
    return document_by_field


def _project_rooted_predecessor(document, previous_by_field):
    """Project the stored snapshot and logical fence without changing predecessor identity."""
    previous_by_field = {**previous_by_field, "snapshot": _logical_snapshot(previous_by_field["snapshot"])}
    fence = previous_by_field.get("input_fence")
    snapshot_by_field = previous_by_field["snapshot"]
    if isinstance(fence, dict) and "catalog" in fence and isinstance(snapshot_by_field, dict):
        mutable_fields = {
            "status",
            "is_current",
            "created_at",
            "validated_at",
            "published_at",
            "superseded_at",
            "candidate_storage_json",
        }
        if document["identity"]["root_dataset_variant"] == "rooted_combined":
            snapshot_by_field = {
                **snapshot_by_field,
                "header": {**snapshot_by_field["header"], "dataset_hash": previous_by_field["dataset_hash"]},
                "parent": {**snapshot_by_field["parent"], "dataset_hash": previous_by_field["dataset_hash"]},
            }
        previous_by_field["input_fence"] = {
            "dataset_id": snapshot_by_field["header"]["dataset_id"],
            "variant": document["identity"]["root_dataset_variant"],
            "header": {
                key: field_value
                for key, field_value in snapshot_by_field["header"].items()
                if key not in mutable_fields
            },
            "parent": {
                key: field_value
                for key, field_value in snapshot_by_field["parent"].items()
                if key not in mutable_fields
            },
        }
    return previous_by_field


def _logical_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        return snapshot
    snapshot_by_field = {
        key: value for key, value in snapshot.items() if key not in {"header_check", "foreign_guards", "validator"}
    }
    if isinstance(snapshot_by_field.get("parent"), dict):
        snapshot_by_field["parent"] = {
            key: value for key, value in snapshot_by_field["parent"].items() if key != "candidate_storage_json"
        }
    return snapshot_by_field


async def verify_isolated_source_storage_coordinates(connection, policy, binding):
    """Resolve configured schema names through ordinary SQL access."""
    _require(
        is_named_source_storage(binding) and policy.schema == binding.storage_schema,
        "storage_changed",
    )
    from db.plan_release_storage import storage_relation

    await connection.execute(
        "SELECT dataset_id FROM "
        + storage_relation(binding.storage_schema, "provider_directory_endpoint_dataset")
        + " LIMIT 0"
    )


def require_storage_schema(publisher, catalog_schema):
    """Keep catalog authority separate from the issued private namespace."""
    binding = getattr(publisher, "storage_binding", None)
    if binding is None:
        _require(publisher.policy.schema == catalog_schema, "schema_changed")
        return None
    from process.provider_directory_generic_source_policy import bound_publisher

    _require(bound_publisher() is publisher, "original_storage_publisher_required")
    _require(
        catalog_schema == binding.catalog_schema and publisher.policy.schema == binding.storage_schema,
        "schema_changed",
    )
    return binding


def storage_document(publisher):
    """Return the two configured schema names without local database identities."""
    binding = publisher.storage_binding
    _require(is_named_source_storage(binding), "isolated_storage_required")
    return asdict(binding)


@contextmanager
def isolated_source_relation_scope(publisher, tables_by_root):
    """Route the closed logical output group within the selected candidate schema."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import RELATIONS

    _require(bound_publisher() is publisher and publisher.storage_binding is not None, "isolated_storage_required")
    _require(set(tables_by_root) == set(RELATIONS), "isolated_relation_unsupported")
    token = _RELATIONS.set((publisher, tuple(sorted(tables_by_root.items()))))
    try:
        yield
    finally:
        _RELATIONS.reset(token)


def source_relation_coordinates(schema, root, scoped_name):
    """Route a closed root set; never redirect canonical identities or aliases."""
    from process import provider_directory_generic_source_policy as source
    from process.provider_directory_generic_source_storage import HEADER, RELATIONS
    from process.provider_directory_rooted_graph_source_runtime import rooted_graph_relation_coordinates
    from process.provider_directory_source_checkpoint_catalog import CHECKPOINT_LAYOUTS
    from process.provider_directory_source_observation import current_storage_binding
    from process.provider_directory_source_proof_control import PROOF_ROOT

    rooted_coordinates = rooted_graph_relation_coordinates(schema, root, scoped_name)
    if rooted_coordinates is not None:
        return rooted_coordinates

    if root in _CATALOG_ROOTS:
        factory = source._FACTORY.get()
        binding = getattr(factory, "storage_binding", None)
        if binding is not None:
            _require(schema == binding.catalog_schema, "schema_changed")
            _require(scoped_name == root, "catalog_override_forbidden")
        return schema, scoped_name
    publisher = source.bound_publisher() if source._PUBLISHER.get() is not None else None
    binding = publisher.storage_binding if publisher is not None else current_storage_binding(schema)
    if binding is None:
        binding = source.current_source_storage_binding()
    if binding is None:
        return schema, scoped_name
    if publisher is not None:
        require_storage_schema(publisher, schema)
    _require(root in {HEADER, PROOF_ROOT, *RELATIONS, *CHECKPOINT_LAYOUTS}, "isolated_relation_unsupported")
    if root in RELATIONS:
        scope = _RELATIONS.get()
        _require(
            publisher is not None
            and scope is not None
            and scope[0] is publisher
            and dict(scope[1]).get(root) == scoped_name,
            "isolated_relation_override_changed",
        )
    else:
        _require(scoped_name == root, "isolated_header_override_changed")
    return binding.storage_schema, scoped_name


async def dataset_source_relations(database, catalog_schema, dataset_id, roots, *, canonical_roots=()):
    """Resolve one logical dataset's recorded relation names once for a bounded operation."""
    from db.plan_release_storage import storage_relation

    _require(type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "dataset_invalid")
    parent = storage_relation(catalog_schema, "provider_directory_endpoint_dataset")
    if hasattr(database, "fetchrow"):
        dataset_record = await database.fetchrow(
            f"SELECT candidate_storage_json FROM {parent} WHERE dataset_id=$1", dataset_id
        )
    else:
        dataset_record = await database.first(
            f"SELECT candidate_storage_json FROM {parent} WHERE dataset_id=:dataset_id", dataset_id=dataset_id
        )
    _require(dataset_record is not None, "dataset_missing")
    encoded = dataset_record["candidate_storage_json"] if hasattr(dataset_record, "keys") else dataset_record[0]
    document = named_source_document(json.loads(encoded) if isinstance(encoded, str) else encoded)
    if document is None:
        return {root: {"schema": catalog_schema, "name": root} for root in roots}
    _require(type(document) is dict and type(document.get("tables")) is dict, "tables_changed")
    _require(document.get("identity", {}).get("dataset_id") == dataset_id, "dataset_changed")
    storage_schema = document.get("storage", {}).get("storage_schema", catalog_schema)
    relations_by_root = {}
    for root in roots:
        table = document["tables"].get(root)
        input_table = document.get("inputs", {}).get(root)
        if table is None and input_table is not None:
            table = input_table
            _require(type(table) is dict and table.get("schema") in {catalog_schema, storage_schema}, "schema_changed")
            relation_by_field = {"schema": table["schema"], "name": table.get("name")}
        elif table is None and root in canonical_roots:
            relation_by_field = {"schema": catalog_schema, "name": root}
        else:
            _require(type(table) is dict, "tables_changed")
            relation_by_field = {"schema": table.get("schema", storage_schema), "name": table.get("name")}
            _require(relation_by_field["schema"] == storage_schema, "schema_changed")
        storage_relation(relation_by_field["schema"], relation_by_field["name"])
        relations_by_root[root] = relation_by_field
    return relations_by_root
