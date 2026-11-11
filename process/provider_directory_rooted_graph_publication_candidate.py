# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Detached rooted outputs awaiting the existing protected preparation lifecycle."""

import importlib
import json
import re
from dataclasses import asdict
from datetime import date, datetime
from uuid import UUID, uuid4

from db.migration_rooted_publication_freeze import CONTRACT, immutable_snapshot_sql
from db.plan_release_storage import storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_generic_source_storage import RELATIONS, RESOURCE, _capture, _require
from process.provider_directory_rooted_graph_source_contract import PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    rooted_graph_query_filter_validation_sql,
)
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

HEADER = "provider_directory_rooted_graph_dataset"
PARENT = "provider_directory_endpoint_dataset"
PROVENANCE = "provider_directory_rooted_graph_dataset_resource"
ROOTS = (*RELATIONS[:4], PROVENANCE, PARENT, HEADER)
_PRIVATE_NAME = re.compile(r"pdgs_[0-9a-f]{32}_[a-z]{1,4}\Z")
_FIELDS = frozenset(
    {
        "contract",
        "operation",
        "phase",
        "preparation_id",
        "storage",
        "identity",
        "admission",
        "tables",
        "catalog",
        "dataset_hash",
        "counts",
        "snapshot",
        "lineage",
        "predecessor_proof",
    }
)


def _date_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError("rooted_candidate_value_invalid")


def candidate_json(value):
    """Keep candidate identities canonical without accepting opaque objects."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_date_value
    )


def validate_rooted_handoff_document(document, expected_storage):
    """Authenticate only the seven detached native outputs of the original Source builder."""
    _require(type(document) is dict and set(document) == _FIELDS, "rooted_handoff_shape_changed")
    _require(document["contract"] == CONTRACT and document["phase"] == "ready_for_freeze", "rooted_handoff_changed")
    _require(str(UUID(document["preparation_id"])) == document["preparation_id"], "rooted_preparation_changed")
    _require(document["storage"] == expected_storage, "rooted_storage_changed")
    _require(set(document["tables"]) == set(ROOTS), "rooted_inventory_changed")
    tables = list(document["tables"].values())
    _require(
        len({table["oid"] for table in tables}) == len(ROOTS)
        and len({table["name"] for table in tables}) == len(ROOTS)
        and all(_PRIVATE_NAME.fullmatch(table["name"]) for table in tables)
        and all(table["owner"] == expected_storage["owner_oid"] for table in tables),
        "rooted_inventory_changed",
    )
    identity = document["identity"]
    _require(
        type(identity) is dict and re.fullmatch(r"pdrgpd_[0-9a-f]{48}", identity.get("dataset_id", "")),
        "rooted_identity_changed",
    )
    _require(re.fullmatch(r"[0-9a-f]{64}", document["dataset_hash"]), "rooted_hash_changed")
    metadata_by_field = identity.get("metadata", {})
    _require(
        metadata_by_field.get("source_ids") == [identity["source_id"]]
        and re.fullmatch(r"run_[0-9a-f]{32}", metadata_by_field.get("source_run_id", ""))
        and type(metadata_by_field.get("source_attempt_id")) is str
        and bool(metadata_by_field["source_attempt_id"]),
        "rooted_source_attempt_changed",
    )
    _require(
        metadata_by_field.get("query_filter_recipe_id") == PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
        "rooted_query_recipe_changed",
    )
    _require(
        type(document["counts"]) is dict
        and all(type(count) is int and count >= 0 for count in document["counts"].values())
        and type(document["snapshot"]) is dict
        and type(document["lineage"]) is dict,
        "rooted_proof_changed",
    )
    _require(len(candidate_json(document).encode()) <= 262144, "rooted_handoff_too_large")
    return document


async def validate_rooted_query_filter_recipes(connection, schema, admission):
    """Reject unauthenticated retained searches before candidate freeze, including both roots."""
    names = ("publication_acquisition_id", "comparison_acquisition_id")
    _require(type(admission) is dict and all(name in admission for name in names), "rooted_acquisition_changed")
    identities = [admission["publication_acquisition_id"]]
    if admission["comparison_acquisition_id"] is not None:
        identities.append(admission["comparison_acquisition_id"])
    _require(
        all(type(identity) is str and re.fullmatch(r"pdrga_[0-9a-f]{48}", identity) for identity in identities),
        "rooted_acquisition_changed",
    )
    acquisition_ids = sorted(set(identities))
    for acquisition_id in acquisition_ids:
        cursor = ""
        while True:
            page = await connection.fetchval(
                rooted_graph_query_filter_validation_sql(schema), acquisition_id, cursor, 4096
            )
            page = json.loads(page) if type(page) is str else page
            _require(
                type(page) is dict
                and page.get("closed") is True
                and page.get("valid") is True
                and type(page.get("rows")) is int
                and 0 <= page["rows"] <= 4096,
                "rooted_query_recipe_changed",
            )
            if page["rows"] == 0:
                _require(page.get("last_query_id") is None, "rooted_query_recipe_changed")
                break
            last = page.get("last_query_id")
            _require(
                type(last) is str and re.fullmatch(r"pdrgq_[0-9a-f]{48}", last) and last > cursor,
                "rooted_query_recipe_changed",
            )
            cursor = last


async def retained_rooted_handoff(runtime, connection, dataset_id=None, *, publication_acquisition_id=None):
    """Recover a committed candidate without rebuilding its physical output group."""
    if dataset_id is None:
        rows = await connection.fetch(
            f"SELECT candidate_storage_json FROM {storage_relation(runtime.candidate_schema, PARENT)} "
            "WHERE candidate_storage_json->'operation'->>'publication_acquisition_id'=$1 "
            "AND endpoint_id=$2 LIMIT 2",
            publication_acquisition_id,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        )
        _require(len(rows) <= 1, "rooted_handoff_ambiguous")
        encoded = rows[0]["candidate_storage_json"] if rows else None
    else:
        encoded = await connection.fetchval(
            f"SELECT candidate_storage_json FROM {storage_relation(runtime.candidate_schema, PARENT)} WHERE dataset_id=$1",
            dataset_id,
        )
    if encoded is None:
        return None
    document = json.loads(encoded) if isinstance(encoded, str) else encoded
    storage_by_field = {
        **asdict(runtime.manifest.storage_binding),
        "producer": list(runtime.manifest.producer_identity),
    }
    validate_rooted_handoff_document(document, storage_by_field)
    return FHIRSourceCandidateHandoff(candidate_json(document))


async def _lineage_snapshot(connection, schema, dataset_id, variant):
    """Follow only immutable header coordinates to the official cohort currentness fence."""
    visited_dataset_ids = set()
    for _ in range(1024):
        _require(dataset_id not in visited_dataset_ids, "rooted_lineage_cycle")
        visited_dataset_ids.add(dataset_id)
        if variant == "uhc_flex_practitioner":
            value = await connection.fetchval(
                f"SELECT jsonb_build_object('cohort_id',c.cohort_id,'cohort',to_jsonb(c)) "
                f"FROM {storage_relation(schema, 'provider_directory_uhc_flex_practitioner_dataset')} h "
                f"JOIN {storage_relation(schema, 'provider_directory_uhc_flex_npi_cohort')} c USING(cohort_id) "
                "WHERE h.dataset_id=$1",
                dataset_id,
            )
            _require(value is not None, "rooted_lineage_changed")
            return json.loads(value) if isinstance(value, str) else value
        _require(variant == "rooted_combined", "rooted_lineage_changed")
        row = await connection.fetchrow(
            f"SELECT root_dataset_id,root_dataset_variant FROM {storage_relation(schema, HEADER)} WHERE dataset_id=$1",
            dataset_id,
        )
        _require(row is not None, "rooted_lineage_changed")
        dataset_id, variant = row["root_dataset_id"], row["root_dataset_variant"]
    raise RuntimeError("provider_directory_rooted_lineage_too_deep")


async def _capture_rooted_tables(connection, policy, relations):
    """Capture the closed physical generations and their native catalog in one transaction."""
    tables = await _capture(connection, policy, list(relations.values()))
    filenode_by_oid = {
        table_record["oid"]: table_record["relfilenode"]
        for table_record in await connection.fetch(
            "SELECT oid::bigint,relfilenode::bigint FROM pg_class WHERE oid=ANY($1::oid[])",
            [table["oid"] for table in tables.values()],
        )
    }
    catalog_rows = await connection.fetch(
        CATALOG_SQL,
        [str(table["oid"]) for table in tables.values()],
        next(iter(tables.values()))["owner"],
        [],
    )
    catalog_records = []
    for table_record in catalog_rows:
        table_by_field = dict(table_record)
        for key in ("columns", "constraints", "indexes"):
            if isinstance(table_by_field[key], str):
                table_by_field[key] = json.loads(table_by_field[key])
        catalog_records.append(table_by_field)
    return tables, filenode_by_oid, catalog_records


async def _candidate_identity(connection, runtime, identity):
    """Bind the real current Source attempt and audited recipe to the detached identity."""
    identity_by_field = asdict(identity)
    attempt_id = await connection.fetchval(
        f"SELECT progress->>'attempt_id' FROM {storage_relation(runtime.catalog_schema, 'import_run')} WHERE run_id=$1",
        runtime.run_id,
    )
    identity_by_field["metadata"] = {
        "source_ids": [identity.source_id],
        "source_run_id": runtime.run_id,
        "source_attempt_id": attempt_id,
        "query_filter_recipe_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    }
    return identity_by_field


async def seal_rooted_handoff(
    runtime, transaction, identity, admission, relations, counts, dataset_hash, predecessor_proof
):
    """Persist the discoverable builder receipt in the same transaction as its private heaps."""
    connection = transaction.connection
    await validate_rooted_query_filter_recipes(connection, runtime.catalog_schema, asdict(admission))
    tables, filenode_by_oid, catalog_records = await _capture_rooted_tables(
        connection, runtime.candidate_policy, relations
    )
    relations_by_root = {root: storage_relation(runtime.candidate_schema, name) for root, name in relations.items()}
    snapshot = await connection.fetchval(
        immutable_snapshot_sql(runtime.catalog_schema, "$1", own_relations=relations_by_root), identity.dataset_id
    )
    snapshot = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
    identity_by_field = await _candidate_identity(connection, runtime, identity)
    document_by_field = {
        "contract": CONTRACT,
        "operation": runtime.operation,
        "phase": "ready_for_freeze",
        "preparation_id": str(uuid4()),
        "storage": {**asdict(runtime.manifest.storage_binding), "producer": list(runtime.manifest.producer_identity)},
        "identity": identity_by_field,
        "admission": asdict(admission),
        "tables": {
            root: {**tables[name], "relfilenode": filenode_by_oid[tables[name]["oid"]]}
            for root, name in relations.items()
        },
        "catalog": sorted(catalog_records, key=lambda table: table["oid"]),
        "dataset_hash": dataset_hash,
        "counts": counts,
        "predecessor_proof": {
            "snapshot": json.loads(predecessor_proof.snapshot_json),
            "input_fence": json.loads(predecessor_proof.input_fence_json),
            "dataset_hash": predecessor_proof.dataset_sha256,
            "resource_count": predecessor_proof.resource_count,
        },
        "snapshot": snapshot,
        "lineage": await _lineage_snapshot(
            connection, runtime.catalog_schema, identity.root_dataset_id, identity.root_dataset_variant
        ),
    }
    document_by_field = json.loads(candidate_json(document_by_field))
    document_by_field["admission"]["previous_dataset_id"] = identity.root_dataset_id
    validate_rooted_handoff_document(document_by_field, document_by_field["storage"])
    encoded = candidate_json(document_by_field)
    parent = storage_relation(runtime.candidate_schema, relations[PARENT])
    await connection.execute(
        f"UPDATE {parent} SET candidate_storage_json=$2::jsonb WHERE dataset_id=$1", identity.dataset_id, encoded
    )
    pointer = storage_relation(runtime.candidate_schema, PARENT)
    await connection.execute(f"INSERT INTO {pointer} SELECT * FROM {parent} WHERE dataset_id=$1", identity.dataset_id)
    return FHIRSourceCandidateHandoff(encoded)


def _frozen_validation_query(body, schema, document):
    """Replay every original candidate predicate against the detached output and its pinned root."""
    from db.migration_rooted_publication_freeze import (
        LEGACY,
        ORIGINAL_DIGESTS,
        ROOTED,
        body_digest,
        is_approved_successor,
        original_query,
    )

    expected = ORIGINAL_DIGESTS[ROOTED]
    if body_digest(body, schema) != expected:
        _require(is_approved_successor(ROOTED, schema, body, expected), "rooted_validator_changed")
        body = original_query(body)
    call = re.escape(storage_relation(schema, LEGACY)) + r"\(root_header\.dataset_id(?:::text)?\)"
    body, replacements = re.subn(call, "TRUE", body)
    _require(replacements == 1, "rooted_validator_changed")
    ctes = []
    for index, root in enumerate(ROOTS):
        name = '"rooted_frozen_' + str(index) + '"'
        canonical = storage_relation(schema, root)
        private = storage_relation(document["storage"]["storage_schema"], document["tables"][root]["name"])
        body = body.replace(canonical, name)
        selected = f"SELECT * FROM {private}"
        if root in {RESOURCE, HEADER, PARENT}:
            selected += f" UNION ALL SELECT * FROM {canonical} WHERE dataset_id=$2"
        ctes.append(name + " AS NOT MATERIALIZED (" + selected + ")")
    return "WITH " + ",".join(ctes) + " " + body.replace("candidate_dataset_id", "$1").rstrip().removesuffix(";")


async def _frozen_resource_hash(connection, document):
    """Conserve DEFAULT-order digest bytes and independently check every semantic payload."""
    import hashlib

    from process.provider_directory_resource_hash import SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT

    directory = importlib.import_module("process.provider_directory_fhir")
    relation = storage_relation(document["storage"]["storage_schema"], document["tables"][RESOURCE]["name"])
    query = f"SELECT * FROM {relation} WHERE dataset_id=$1 ORDER BY resource_type,resource_id"
    cursor = await connection.cursor(query, document["identity"]["dataset_id"])
    digest, count = hashlib.sha256(), 0
    while resources := await cursor.fetch(4096):
        for resource in resources:
            resource_by_field = dict(resource)
            payload_by_field = (
                json.loads(resource_by_field["payload_json"])
                if isinstance(resource_by_field["payload_json"], str)
                else resource_by_field["payload_json"]
            )
            resource_by_field["payload_json"] = payload_by_field
            contract = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            _require(
                resource_by_field["payload_hash"]
                == directory.resource_payload_sha256_for_contract(
                    payload_by_field, contract, resource_type=resource_by_field["resource_type"]
                ),
                "rooted_payload_hash_changed",
            )
            directory._validated_semantic_dataset_payload(
                resource_by_field, resource_by_field["resource_type"], contract
            )
            if resource_by_field["resource_type"] == "Practitioner":
                directory._assert_practitioner_semantic_projection_as_of(
                    directory.ProviderDirectoryPractitioner,
                    [payload_by_field],
                    contract,
                    document["identity"]["semantic_projection_as_of"],
                )
            if count:
                digest.update(b"\n")
            digest.update(
                candidate_json(
                    [resource_by_field[key] for key in ("resource_type", "resource_id", "payload_hash")]
                ).encode()
            )
            count += 1
    return digest.hexdigest(), count


async def validate_frozen_rooted_handoff(connection, handoff, issued, *, protected_owner_role):
    """Perform complete original content validation only after protected ownership closes writes."""
    from db.migration_rooted_publication_freeze import ROOTED
    from process.provider_directory_rooted_graph_current_preflight import current_root_proof
    from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql

    document = handoff.document
    storage_by_field = {**asdict(issued.storage_binding), "producer": list(issued.producer_identity)}
    validate_rooted_handoff_document(document, storage_by_field)
    _require(connection.is_in_transaction(), "transaction_required")
    await _require_frozen_inventory(connection, document, protected_owner_role)
    from process.provider_directory_rooted_graph_foreign_guards import capture_parent_foreign_guards

    _require(
        await capture_parent_foreign_guards(connection, issued.storage_binding.catalog_schema)
        == document["snapshot"]["foreign_guards"],
        "rooted_foreign_guard_changed",
    )
    from process.provider_directory_rooted_graph_header_checks import validate_rooted_header_check

    await validate_rooted_header_check(connection, document)
    proof = current_root_proof(None)
    expected_by_field = {
        "snapshot": json.loads(proof.snapshot_json),
        "input_fence": json.loads(proof.input_fence_json),
        "dataset_hash": proof.dataset_sha256,
        "resource_count": proof.resource_count,
    }
    _require(document["predecessor_proof"] == expected_by_field, "rooted_predecessor_changed")
    schema = issued.storage_binding.catalog_schema
    await validate_rooted_query_filter_recipes(connection, schema, document["admission"])
    fence = await connection.fetchval(
        root_input_fence_sql(schema, "$1", "$2::jsonb", variant=proof.current.variant),
        proof.current.dataset_id,
        candidate_json(expected_by_field["input_fence"].get("roots", {})),
    )
    _require(
        (json.loads(fence) if isinstance(fence, str) else fence) == expected_by_field["input_fence"],
        "rooted_input_changed",
    )
    body = await connection.fetchval(
        "SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure($1)", storage_relation(schema, ROOTED) + "(text)"
    )
    query = _frozen_validation_query(body, schema, document)
    _require(
        await connection.fetchval(query, document["identity"]["dataset_id"], proof.current.dataset_id) is True,
        "rooted_candidate_invalid",
    )
    digest, count = await _frozen_resource_hash(connection, document)
    _require(digest == document["dataset_hash"] and count == sum(document["counts"].values()), "rooted_hash_changed")
    await _require_frozen_inventory(connection, document, protected_owner_role)
    return {
        "handoff_sha256": handoff.document_sha256,
        "dataset_hash": digest,
        "resource_count": count,
        "resource_counts": document["counts"],
        "predecessor_input_fence": expected_by_field["input_fence"],
    }


async def _require_frozen_inventory(connection, document, owner_role):
    """Authenticate the native seven-table layout after its protected owner transfer."""
    from process.provider_directory_generic_source_storage import _table_proof

    owner = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", owner_role)
    names = [
        storage_relation(document["storage"]["storage_schema"], table["name"]) for table in document["tables"].values()
    ]
    await connection.execute("LOCK TABLE " + ",".join(sorted(names)) + " IN ACCESS SHARE MODE NOWAIT")
    rows = await connection.fetch(CATALOG_SQL, names, owner, [])
    receipts_by_oid = {table["oid"]: table for table in document["tables"].values()}
    _require(len(rows) == len(receipts_by_oid) == 7, "rooted_inventory_changed")
    for row in rows:
        receipt = receipts_by_oid[row["oid"]]
        expected_by_field = {key: receipt[key] for key in ("name", "oid", "owner", "layout_sha256")}
        _require(_table_proof(row, private=True) == {**expected_by_field, "owner": owner}, "rooted_inventory_changed")
        _require(
            await connection.fetchval("SELECT relfilenode::bigint FROM pg_class WHERE oid=$1", row["oid"])
            == receipt["relfilenode"],
            "rooted_physical_generation_changed",
        )
