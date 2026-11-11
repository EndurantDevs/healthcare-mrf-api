# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate immutable predecessor inputs before the brief exact-current publication fence."""

import json
from dataclasses import dataclass
from weakref import WeakKeyDictionary, ref

from db.migration_rooted_publication_freeze import LEGACY, immutable_snapshot_sql, native_receipt_sql
from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_scoped_publication as scoped
from process.provider_directory_dataset_scoped_publication_contract import LEGACY_PRACTITIONER_VARIANT
from process.provider_directory_rooted_graph_input_fence import capture_input_fence, root_input_fence_sql
from process.provider_directory_rooted_graph_legacy_validation import (
    legacy_metadata_query,
    require,
    validate_legacy_content,
)
from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime

_PROOFS = WeakKeyDictionary()
_BRAND = object()
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
    runtime_ref: object
    brand: object


def legacy_snapshot_sql(schema, dataset_parameter=":dataset_id"):
    """Bound only the exact predecessor's immutable metadata and authoritative lineage rows."""
    relation = lambda name: storage_relation(schema, name)
    validator = "'" + storage_relation(schema, LEGACY) + "(text)'"
    return f"""SELECT jsonb_build_object(
        'header',to_jsonb(h)-{_MUTABLE},'parent',to_jsonb(p)-{_MUTABLE},
        'admission',to_jsonb(a),'candidate',to_jsonb(c),'cohort',to_jsonb(k),
        'official_parent',to_jsonb(o)-{_MUTABLE},
        'sources',(SELECT jsonb_agg(jsonb_build_object('source_id',s.source_id,'endpoint_id',s.endpoint_id,
            'canonical_api_base',s.canonical_api_base,'requires_registration',s.requires_registration,
            'requires_api_key',s.requires_api_key,'auth_type',s.auth_type,'metadata_json',s.metadata_json) ORDER BY s.source_id)
            FROM {relation("provider_directory_source")} s WHERE s.source_id IN(h.source_id,k.official_source_id)),
        'endpoints',(SELECT jsonb_agg(jsonb_build_object('endpoint_id',e.endpoint_id,'canonical_api_base',e.canonical_api_base,
            'endpoint_signature_hash',e.endpoint_signature_hash,'metadata_json',e.metadata_json) ORDER BY e.endpoint_id)
            FROM {relation("provider_directory_api_endpoint")} e WHERE e.endpoint_id IN(h.endpoint_id,k.official_endpoint_id)),
        'validator',(SELECT jsonb_build_object('oid',f.oid::bigint,'xmin',f.xmin::text,
            'body',encode(sha256(convert_to(f.prosrc,'UTF8')),'hex')) FROM pg_proc f
            WHERE f.oid=to_regprocedure({validator})))
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
    query = legacy_snapshot_sql(schema) if is_legacy else immutable_snapshot_sql(schema, ":dataset_id")
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
    """Retain complete legacy predicates or authenticate the protected frozen rooted receipt."""
    schema = scoped._schema_name()
    if current.variant == LEGACY_PRACTITIONER_VARIANT:
        body = await database.scalar(
            "SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:signature)",
            signature=storage_relation(schema, LEGACY) + "(text)",
        )
        require(type(body) is str)
        require(await database.scalar(legacy_metadata_query(body, schema), dataset_id=current.dataset_id) is True)
        require(await _ready_legacy(database, current) is True)
        header = json.loads(snapshot)["header"]
        digest, total, leaves = await validate_legacy_content(database, current, header)
        fence = await capture_input_fence(database, schema, leaves, current.dataset_id)
    else:
        require(await database.scalar(native_receipt_sql(schema, ":dataset_id"), dataset_id=current.dataset_id) is True)
        require(
            await database.scalar(
                f"SELECT {storage_relation(schema, scoped.ROOTED_READY_FUNCTION)}(:dataset_id)",
                dataset_id=current.dataset_id,
            )
            is True
        )
        encoded = await database.scalar(
            root_input_fence_sql(schema, ":dataset_id", "CAST(:roots AS jsonb)", variant=current.variant),
            dataset_id=current.dataset_id,
            roots="{}",
        )
        document = json.loads(encoded) if type(encoded) is str else encoded
        require(type(document) is dict and document.get("roots") == {} and type(document.get("catalog")) is dict)
        require(type(document["catalog"].get("published_proof")) is dict)
        digest, total = current.dataset_hash, current.resource_count
        fence = json.dumps(document, sort_keys=True, separators=(",", ":"))
    require(snapshot == await _snapshot(database, current))
    return ValidatedCurrentRoot(current, snapshot, fence, digest, total, ref(runtime), _BRAND)


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
    """Recheck the validated physical generation and exact pointer using bounded metadata only."""
    runtime = current_rooted_graph_runtime(database)
    require(type(proof) is ValidatedCurrentRoot and proof.brand is _BRAND and proof.runtime_ref() is runtime)
    current = await _current_metadata(database, lock_rows=lock_rows)
    require(current == proof.current and proof.snapshot_json == await _snapshot(database, current))
    if current.variant == LEGACY_PRACTITIONER_VARIANT:
        roots = json.loads(proof.input_fence_json)["roots"]
        if lock_rows:
            names = await database.all(
                "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.oid=ANY(CAST(:oids AS oid[])) ORDER BY c.oid",
                oids=sorted({oid for oids in roots.values() for oid in oids}),
            )
            for entry in names:
                fields = scoped._row_fields(entry)
                await database.status(
                    "LOCK TABLE ONLY "
                    + storage_relation(fields["nspname"], fields["relname"])
                    + " IN ACCESS SHARE MODE NOWAIT"
                )
        require(
            proof.input_fence_json
            == await capture_input_fence(database, scoped._schema_name(), roots, current.dataset_id)
        )
    else:
        schema = scoped._schema_name()
        require(await database.scalar(native_receipt_sql(schema, ":dataset_id"), dataset_id=current.dataset_id) is True)
        require(
            await database.scalar(
                f"SELECT {storage_relation(schema, scoped.ROOTED_READY_FUNCTION)}(:dataset_id)",
                dataset_id=current.dataset_id,
            )
            is True
        )
        require(proof.input_fence_json == await capture_root_input_fence(database, current))
    return current


async def capture_root_input_fence(reader, current):
    """Capture the same physical fence after a legitimate source supersession in its original transaction."""
    from sqlalchemy import text

    from process.network_cms_registry_address_capture import NativeReceiptSession

    proof = current_root_proof(reader) if not hasattr(reader, "fetchval") else current_root_proof(None)
    require(proof.current.dataset_id == current.dataset_id)
    roots = json.loads(proof.input_fence_json).get("roots", {})
    query = root_input_fence_sql(scoped._schema_name(), ":dataset_id", "CAST(:roots AS jsonb)", variant=current.variant)
    parameters_by_name = dict(dataset_id=current.dataset_id, roots=json.dumps(roots))
    encoded = (
        await NativeReceiptSession(reader).scalar(text(query), parameters_by_name)
        if hasattr(reader, "fetchval")
        else await reader.scalar(query, **parameters_by_name)
    )
    document = json.loads(encoded) if type(encoded) is str else encoded
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def current_root_proof(database):
    """Expose only this issued runtime's genuinely prepared predecessor capability."""
    runtime = current_rooted_graph_runtime(database)
    proof = _PROOFS.get(runtime) if runtime is not None else None
    require(proof is not None and proof.brand is _BRAND and proof.runtime_ref() is runtime)
    return proof
