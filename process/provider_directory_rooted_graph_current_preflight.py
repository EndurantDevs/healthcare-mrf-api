# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate immutable predecessor inputs before the brief exact-current publication fence."""

import json
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from db.migration_rooted_publication_freeze import (
    LEGACY,
    immutable_snapshot_sql,
    native_receipt_sql,
    validation_body_sql,
)
from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_scoped_publication as scoped
from process.provider_directory_dataset_scoped_publication_contract import LEGACY_PRACTITIONER_VARIANT
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from process.provider_directory_rooted_graph_legacy_validation import (
    legacy_metadata_query,
    require,
    validate_legacy_content,
)
from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime

_PROOFS = WeakKeyDictionary()
_MUTABLE = "'{status,is_current,superseded_at}'::text[]"


class CurrentRootMissingError(RuntimeError):
    """Distinguish an absent current header from invalid metadata or database failures."""


@dataclass(frozen=True)
class ValidatedCurrentRoot:
    """An authenticated runtime-local validation, independent of request or header declarations."""

    current: object
    snapshot_json: str
    input_fence_json: str
    dataset_sha256: str
    resource_count: int


def legacy_snapshot_sql(schema, dataset_parameter=":dataset_id"):
    """Bound only the exact predecessor's immutable metadata and authoritative lineage rows."""
    relation = lambda name: storage_relation(schema, name)
    return f"""SELECT jsonb_build_object(
        'header',to_jsonb(h)-{_MUTABLE},'parent',(to_jsonb(p)-{_MUTABLE})-'candidate_storage_json',
        'admission',to_jsonb(a),'candidate',to_jsonb(c),'cohort',to_jsonb(k),
        'official_parent',to_jsonb(o)-{_MUTABLE},
        'sources',(SELECT jsonb_agg(jsonb_build_object('source_id',s.source_id,'endpoint_id',s.endpoint_id,
            'canonical_api_base',s.canonical_api_base,'requires_registration',s.requires_registration,
            'requires_api_key',s.requires_api_key,'auth_type',s.auth_type,'metadata_json',s.metadata_json) ORDER BY s.source_id)
            FROM {relation("provider_directory_source")} s WHERE s.source_id IN(h.source_id,k.official_source_id)),
        'endpoints',(SELECT jsonb_agg(jsonb_build_object('endpoint_id',e.endpoint_id,'canonical_api_base',e.canonical_api_base,
            'endpoint_signature_hash',e.endpoint_signature_hash,'metadata_json',e.metadata_json) ORDER BY e.endpoint_id)
            FROM {relation("provider_directory_api_endpoint")} e WHERE e.endpoint_id IN(h.endpoint_id,k.official_endpoint_id)))
        FROM {relation(scoped.LEGACY_DATASET_TABLE)} h JOIN {relation(scoped.ENDPOINT_DATASET_TABLE)} p USING(dataset_id)
        JOIN {relation("provider_directory_uhc_flex_practitioner_twin_admission")} a USING(admission_id)
        JOIN {relation("provider_directory_uhc_flex_practitioner_acquisition")} c ON c.acquisition_id=h.candidate_acquisition_id
        JOIN {relation("provider_directory_uhc_flex_npi_cohort")} k ON k.cohort_id=h.cohort_id
        JOIN {relation(scoped.ENDPOINT_DATASET_TABLE)} o ON o.dataset_id=k.official_dataset_id
        WHERE h.dataset_id={dataset_parameter}"""


async def _current_metadata(database, *, lock_rows):
    """Preserve exact pair/parent/header consistency without invoking content validators."""
    pair = scoped.exact_uhc_dataset_pair()
    await scoped._lock_pair_registry(database, pair, lock_rows=lock_rows)
    parents = await scoped._locked_parent_by_id(database, pair, lock_rows=lock_rows)
    selected = await scoped._locked_current_header(database, pair, lock_rows=lock_rows)
    if selected is None:
        raise CurrentRootMissingError("provider_directory_rooted_graph_predecessor_invalid")
    variant, header = selected
    parent = parents.get(header.get("dataset_id"))
    require(len(parents) == 1 and parent is not None)
    require(header.get("status") == parent.get("status") == "published")
    require(header.get("is_current") is parent.get("is_current") is True)
    require(all(header.get(field) == parent.get(field) for field in ("endpoint_id", "dataset_hash", "resource_count")))
    return scoped._exact_current_from_header(variant, header, parent)


async def _snapshot(database, current):
    schema = scoped._schema_name()
    is_legacy = current.variant == LEGACY_PRACTITIONER_VARIANT
    if is_legacy:
        query = legacy_snapshot_sql(schema)
    else:
        from process.provider_directory_source_storage_binding import dataset_source_relations

        roots = tuple(
            "provider_directory_rooted_graph_" + root for root in ("acquisition", "twin_attempt", "twin_admission")
        )
        inputs = await dataset_source_relations(database, schema, current.dataset_id, roots)
        query = immutable_snapshot_sql(
            schema,
            ":dataset_id",
            own_relations={root: storage_relation(table["schema"], table["name"]) for root, table in inputs.items()},
        )
    parameters_by_name = {"dataset_id": current.dataset_id}
    encoded = await database.scalar(query, **parameters_by_name)
    document = json.loads(encoded) if type(encoded) is str else encoded
    require(type(document) is dict)
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


async def _ready_legacy(database, current):
    schema = scoped._schema_name()
    parent = storage_relation(schema, scoped.ENDPOINT_DATASET_TABLE)
    cohort = storage_relation(schema, "provider_directory_uhc_flex_npi_cohort")
    return await database.scalar(
        f"SELECT EXISTS(SELECT FROM {cohort} c JOIN {parent} o ON o.dataset_id=c.official_dataset_id "
        "WHERE c.cohort_id=:cohort_id AND o.endpoint_id=c.official_endpoint_id "
        "AND o.acquisition_root_run_id=c.official_acquisition_root_run_id "
        "AND o.dataset_hash=c.official_dataset_hash AND o.status='published' AND o.is_current)",
        cohort_id=current.root_cohort_id,
    )


async def _validate_current(database, current, runtime, snapshot):
    """Validate predecessor data before taking the brief publication fence."""
    schema = scoped._schema_name()
    if current.variant == LEGACY_PRACTITIONER_VARIANT:
        body = validation_body_sql(LEGACY, schema)
        require(await database.scalar(legacy_metadata_query(body, schema), dataset_id=current.dataset_id) is True)
        require(await _ready_legacy(database, current) is True)
        digest, total, _ = await validate_legacy_content(database, current, json.loads(snapshot)["header"])
    else:
        require(await database.scalar(native_receipt_sql(schema, ":dataset_id"), dataset_id=current.dataset_id) is True)
        digest, total = current.dataset_hash, current.resource_count
    encoded = await database.scalar(
        root_input_fence_sql(schema, ":dataset_id", variant=current.variant), dataset_id=current.dataset_id
    )
    fence = json.loads(encoded) if isinstance(encoded, str) else encoded
    require(type(fence) is dict)
    require(snapshot == await _snapshot(database, current))
    return ValidatedCurrentRoot(
        current, snapshot, json.dumps(fence, sort_keys=True, separators=(",", ":")), digest, total
    )


async def prepare_current_root(database):
    """Do bounded content work before any publication advisory or current-row lock."""
    runtime = current_rooted_graph_runtime(database)
    require(runtime is not None)
    async with runtime.transaction(database, isolation="repeatable read", readonly=True):
        current = await _current_metadata(database, lock_rows=False)
        snapshot = await _snapshot(database, current)
        previous = _PROOFS.get(runtime)
        if previous is not None and previous.current == current and previous.snapshot_json == snapshot:
            await require_current_root_fence(database, previous, lock_rows=False)
            return previous
        proof = await _validate_current(database, current, runtime, snapshot)
    _PROOFS[runtime] = proof
    return proof


async def require_current_root_fence(database, proof, *, lock_rows=True):
    """Compare exact logical currentness and immutable predecessor data."""
    require(type(proof) is ValidatedCurrentRoot)
    current = await _current_metadata(database, lock_rows=lock_rows)
    require(current == proof.current and proof.snapshot_json == await _snapshot(database, current))
    require(proof.input_fence_json == await capture_root_input_fence(database, current))
    return current


async def capture_root_input_fence(reader, current):
    """Read logical predecessor fields before or after an atomic supersession."""
    if hasattr(reader, "fetchval"):
        encoded = await reader.fetchval(
            root_input_fence_sql(scoped._schema_name(), "$1", variant=current.variant), current.dataset_id
        )
    else:
        encoded = await reader.scalar(
            root_input_fence_sql(scoped._schema_name(), ":dataset_id", variant=current.variant),
            dataset_id=current.dataset_id,
        )
    document = json.loads(encoded) if isinstance(encoded, str) else encoded
    require(type(document) is dict)
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def current_root_proof(database):
    """Expose only this issued runtime's genuinely prepared predecessor capability."""
    runtime = current_rooted_graph_runtime(database)
    proof = _PROOFS.get(runtime) if runtime is not None else None
    require(proof is not None)
    return proof
