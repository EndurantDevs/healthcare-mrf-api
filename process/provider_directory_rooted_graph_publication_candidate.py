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
from process.provider_directory_generic_source_storage import RELATIONS, RESOURCE, _capture, _require
from process.provider_directory_rooted_graph_source_contract import PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    rooted_graph_query_filter_validation_sql,
)
from process.provider_directory_source_storage_binding import (
    FHIRSourceCandidateHandoff,
    dataset_source_relations,
    named_source_document,
)

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
        "inputs",
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
    coordinates = []
    for table in document["tables"].values():
        _require(type(table) is dict and set(table) == {"schema", "name"}, "rooted_inventory_changed")
        _require(table["schema"] == expected_storage["storage_schema"], "rooted_inventory_changed")
        storage_relation(table["schema"], table["name"])
        coordinates.append((table["schema"], table["name"]))
    _require(len(set(coordinates)) == len(ROOTS), "rooted_inventory_changed")
    expected_inputs = {
        "provider_directory_rooted_graph_" + root
        for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
    }
    _require(
        type(document["inputs"]) is dict and set(document["inputs"]) == expected_inputs, "rooted_inventory_changed"
    )
    for root, table in document["inputs"].items():
        _require(
            type(table) is dict
            and set(table) == {"schema", "name"}
            and table["name"] == root
            and table["schema"] in {expected_storage["catalog_schema"], expected_storage["storage_schema"]},
            "rooted_inventory_changed",
        )
        storage_relation(table["schema"], table["name"])
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
    from process.provider_directory_rooted_graph_source_runtime import (
        current_rooted_graph_runtime,
        rooted_input_relations,
    )

    reader = connection
    if current_rooted_graph_runtime() is not None and hasattr(connection, "fetchval"):
        from process.provider_directory_generic_source_completion import _FrozenSourceRows

        reader = _FrozenSourceRows(connection)
    for acquisition_id in acquisition_ids:
        acquisition_schema = schema
        if current_rooted_graph_runtime() is not None:
            inputs = await rooted_input_relations(reader, acquisition_id)
            acquisition_schema = inputs["provider_directory_rooted_graph_work"]["schema"]
        cursor = ""
        while True:
            page = await connection.fetchval(
                rooted_graph_query_filter_validation_sql(acquisition_schema), acquisition_id, cursor, 4096
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
    document = named_source_document(json.loads(encoded) if isinstance(encoded, str) else encoded)
    storage_by_field = asdict(runtime.manifest.storage_binding)
    validate_rooted_handoff_document(document, storage_by_field)
    _require(dataset_id is None or document["identity"]["dataset_id"] == dataset_id, "rooted_identity_changed")
    _require(document["operation"] == runtime.operation, "rooted_operation_changed")
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
    """Resolve each output by schema-qualified name through ordinary SQL."""
    return await _capture(connection, policy, list(relations.values()))


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
    await validate_rooted_query_filter_recipes(connection, runtime.candidate_schema, asdict(admission))
    tables = await _capture_rooted_tables(connection, runtime.candidate_policy, relations)
    relations_by_root = {root: storage_relation(runtime.candidate_schema, name) for root, name in relations.items()}
    relations_by_root.update(
        {
            "provider_directory_rooted_graph_" + root: storage_relation(
                runtime.candidate_schema, "provider_directory_rooted_graph_" + root
            )
            for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
        }
    )
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
        "storage": asdict(runtime.manifest.storage_binding),
        "identity": identity_by_field,
        "admission": asdict(admission),
        "tables": {root: tables[name] for root, name in relations.items()},
        "inputs": {
            "provider_directory_rooted_graph_" + root: {
                "schema": runtime.candidate_schema,
                "name": "provider_directory_rooted_graph_" + root,
            }
            for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
        },
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
    return await _store_rooted_handoff(connection, runtime, identity, relations, document_by_field)


async def _store_rooted_handoff(connection, runtime, identity, relations, document_by_field):
    """Validate and persist the same canonical handoff before creating its discoverable pointer."""
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


def _frozen_validation_query(body, schema, document, *, predecessor_resource, comparison_inputs=None):
    """Replay every original candidate predicate against the detached output and its pinned root."""
    from db.migration_rooted_publication_freeze import LEGACY, ROOTED, validation_body_sql

    body = validation_body_sql(ROOTED, schema)
    call = re.escape(storage_relation(schema, LEGACY)) + r"\(root_header\.dataset_id(?:::text)?\)"
    body, replacements = re.subn(call, "TRUE", body)
    _require(replacements == 1, "rooted_validator_changed")
    input_ctes = []
    for root, table in document["inputs"].items():
        relation = storage_relation(table["schema"], table["name"])
        previous = comparison_inputs.get(root) if comparison_inputs else None
        if root == "provider_directory_rooted_graph_acquisition" and previous is not None and previous != table:
            selected = '"rooted_comparison_inputs"'
            older = storage_relation(previous["schema"], previous["name"])
            input_ctes.append(
                selected + f" AS NOT MATERIALIZED (SELECT * FROM {relation} UNION ALL "
                f"SELECT * FROM {older} WHERE acquisition_id=$3)"
            )
            relation = selected
        body = body.replace(storage_relation(schema, root), relation)
    from process.provider_directory_rooted_graph_bulk import acquisition_hash_expression

    hash_names = (
        "provider_directory_rooted_graph_terminal_set_sha256",
        "provider_directory_rooted_graph_resource_set_sha256",
        "provider_directory_rooted_graph_edge_set_sha256",
        "provider_directory_rooted_graph_sha256",
    )
    input_schema = document["inputs"]["provider_directory_rooted_graph_acquisition"]["schema"]
    for name in hash_names:
        expression = re.escape(storage_relation(schema, name)) + r"\(([^()]*)\)"
        body = re.sub(expression, lambda match: acquisition_hash_expression(input_schema, name, match.group(1)), body)
    coverage = (
        re.escape(storage_relation(schema, "provider_directory_rooted_graph_request_failure_coverage"))
        + r"\(([^()]*)\)"
    )
    body = re.sub(
        coverage,
        lambda match: match.group(1).removesuffix("::text").replace(".acquisition_id", ".request_failure_coverage"),
        body,
    )
    ctes = input_ctes
    for index, root in enumerate(ROOTS):
        name = '"rooted_frozen_' + str(index) + '"'
        canonical = storage_relation(schema, root)
        private = storage_relation(document["tables"][root]["schema"], document["tables"][root]["name"])
        body = body.replace(canonical, name)
        selected = f"SELECT * FROM {private}"
        if root in {RESOURCE, HEADER, PARENT}:
            predecessor = predecessor_resource if root == RESOURCE else canonical
            selected += f" UNION ALL SELECT * FROM {predecessor} WHERE dataset_id=$2"
        ctes.append(name + " AS NOT MATERIALIZED (" + selected + ")")
    return (
        "WITH "
        + ",".join(ctes)
        + " "
        + body.replace("candidate_dataset_id", "$1").replace("\\:", ":").rstrip().removesuffix(";")
    )


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
    from process.provider_directory_rooted_graph_current_preflight import current_root_proof
    from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql

    document = handoff.document
    storage_by_field = asdict(issued.storage_binding)
    validate_rooted_handoff_document(document, storage_by_field)
    _require(connection.is_in_transaction(), "transaction_required")
    await _require_frozen_inventory(connection, document, protected_owner_role)
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
    await validate_rooted_query_filter_recipes(
        connection, document["inputs"]["provider_directory_rooted_graph_work"]["schema"], document["admission"]
    )
    fence = await connection.fetchval(
        root_input_fence_sql(schema, "$1", variant=proof.current.variant),
        proof.current.dataset_id,
    )
    _require(
        (json.loads(fence) if isinstance(fence, str) else fence) == expected_by_field["input_fence"],
        "rooted_input_changed",
    )
    predecessor = await dataset_source_relations(connection, schema, proof.current.dataset_id, (RESOURCE,))
    predecessor_resource = storage_relation(predecessor[RESOURCE]["schema"], predecessor[RESOURCE]["name"])
    await _validate_frozen_rooted_relations(connection, document, proof, schema, predecessor_resource)
    digest, count = await _frozen_resource_hash(connection, document)
    _require(digest == document["dataset_hash"] and count == sum(document["counts"].values()), "rooted_hash_changed")
    await _require_frozen_inventory(connection, document, protected_owner_role)
    return {
        "dataset_hash": digest,
        "resource_count": count,
        "resource_counts": document["counts"],
        "predecessor_input_fence": expected_by_field["input_fence"],
    }


async def _validate_frozen_rooted_relations(connection, document, proof, schema, predecessor_resource):
    """Check complete request coverage and the same predecessor/comparison data relations."""
    from process.provider_directory_generic_source_completion import _FrozenSourceRows
    from process.provider_directory_rooted_graph_bulk import named_request_coverage

    coverage = await named_request_coverage(
        _FrozenSourceRows(connection), document["admission"]["publication_acquisition_id"]
    )
    _require(
        coverage == document["snapshot"]["acquisition"]["request_failure_coverage"], "rooted_request_coverage_changed"
    )
    from process.provider_directory_rooted_graph_source_runtime import rooted_input_relations

    comparison_id = document["admission"].get("comparison_acquisition_id")
    comparison = await rooted_input_relations(_FrozenSourceRows(connection), comparison_id) if comparison_id else None
    query = _frozen_validation_query(
        None, schema, document, predecessor_resource=predecessor_resource, comparison_inputs=comparison
    )
    arguments = [document["identity"]["dataset_id"], proof.current.dataset_id]
    if (
        comparison is not None
        and comparison["provider_directory_rooted_graph_acquisition"]
        != document["inputs"]["provider_directory_rooted_graph_acquisition"]
    ):
        arguments.append(comparison_id)
    _require(
        await connection.fetchval(query, *arguments) is True,
        "rooted_candidate_invalid",
    )


async def _require_frozen_inventory(connection, document, owner_role):
    """Lock named output relations and reject rows outside the candidate dataset."""
    names = sorted(storage_relation(table["schema"], table["name"]) for table in document["tables"].values())
    await connection.execute("LOCK TABLE " + ",".join(names) + " IN ACCESS SHARE MODE NOWAIT")
    for relation in names:
        invalid = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {relation} WHERE dataset_id IS DISTINCT FROM $1)",
            document["identity"]["dataset_id"],
        )
        _require(invalid is False, "rooted_inventory_changed")
