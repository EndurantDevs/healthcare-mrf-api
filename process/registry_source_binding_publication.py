# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve immutable binding batches into one explicitly reviewed publication."""

import asyncio
import hashlib
import json
import re
from dataclasses import asdict
from uuid import UUID

from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_approval_store import RegistryApprovalCommand, _validated_command
from process.registry_record_store import _bounded_text, _validated_actor
from process.registry_source_observation_store import _namespace

OPERATION = "publish_source_binding_batch"
APPROVAL_OPERATION = "approve_source_binding_batch"
OPERATIONS = (OPERATION, APPROVAL_OPERATION)
MAX_SELECTIONS = 5000
MAX_SELECTION_BYTES = 1048576
PAGE_SIZE = 100
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_FIELDS = {
    "operation",
    "batch_references",
    "source_coordinates",
    "metadata_generation",
    "selection_count",
    "selection_sha256",
    "expected_draft_revision",
    "expected_approved_revision",
    "expected_serving_generation",
    "reason",
    "idempotency_key",
}


def _require(condition):
    if not condition:
        raise ValueError("registry_source_batch_invalid")


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _counter(value, *, positive=False):
    _require(type(value) is int and (1 if positive else 0) <= value < 2**63 - 1)


def validated_source_batch_command(document, *, allow_unbound=False):
    """A new closed reference command leaves the existing selection wire unchanged."""
    _require(type(allow_unbound) is bool and type(document) is dict and set(document) == _FIELDS)
    _require(document["operation"] in OPERATIONS)
    for field in ("expected_draft_revision", "expected_approved_revision", "expected_serving_generation"):
        _counter(document[field])
    _require(document["expected_approved_revision"] <= document["expected_draft_revision"])
    for field, maximum in (("reason", 1000), ("idempotency_key", 128)):
        _require(_bounded_text(document[field], maximum, field) == document[field])
    references = document["batch_references"]
    _require(type(references) is list and 1 <= len(references) <= 100)
    for reference in references:
        _require(type(reference) is dict and set(reference) == {"idempotency_key", "request_sha256", "custom_revision"})
        _require(_bounded_text(reference["idempotency_key"], 128, "idempotency_key") == reference["idempotency_key"])
        _counter(reference["custom_revision"], positive=True)
        _require(reference["custom_revision"] <= document["expected_draft_revision"])
        _require(type(reference["request_sha256"]) is str and _SHA.fullmatch(reference["request_sha256"]) is not None)
    _require(len({reference["idempotency_key"] for reference in references}) == len(references))
    _require(len({reference["request_sha256"] for reference in references}) == len(references))
    _require(
        [reference["custom_revision"] for reference in references]
        == sorted({reference["custom_revision"] for reference in references})
    )
    _require(
        type(document["metadata_generation"]) is str and _SHA.fullmatch(document["metadata_generation"]) is not None
    )
    coordinates = document["source_coordinates"]
    _require(
        type(coordinates) is dict
        and set(coordinates)
        == {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
    )
    _require(
        asdict(RegistryNetworkSourceCoordinates(**coordinates)) == coordinates
        and coordinates["source_system"] == "fhir"
    )
    count, digest = document["selection_count"], document["selection_sha256"]
    if not (allow_unbound and count is None and digest is None):
        _require(type(count) is int and 1 <= count <= MAX_SELECTIONS)
        _require(type(digest) is str and _SHA.fullmatch(digest) is not None)
    _require(len(_json(document).encode()) <= 65536)
    return dict(document)


def selection_identity(selection):
    """Hash the same explicit canonical vector used by approval and authority."""
    _require(type(selection) in (list, tuple) and 1 <= len(selection) <= MAX_SELECTIONS)
    for row in selection:
        _require(type(row) is dict and set(row) == {"record_kind", "record_id", "revision"})
        _require(row["record_kind"] in ("network", "network_binding"))
        _counter(row["revision"], positive=True)
        if row["record_kind"] == "network":
            _require(type(row["record_id"]) is int and 0 < row["record_id"] <= 2147483647)
        else:
            _require(
                type(row["record_id"]) is str
                and UUID(row["record_id"]).int
                and str(UUID(row["record_id"])) == row["record_id"]
            )
    canonical = sorted(selection, key=lambda row: (row["record_kind"], str(row["record_id"])))
    _require(len({(row["record_kind"], str(row["record_id"])) for row in canonical}) == len(canonical))
    encoded = _json(canonical).encode()
    _require(len(encoded) <= MAX_SELECTION_BYTES)
    return canonical, hashlib.sha256(encoded).hexdigest()


def stored_source_batch(request_record):
    """Authenticate the stored expanded vector without consulting mutable draft heads."""
    from process.registry_publication_queue import publication_request_metadata

    stored = request_record["command_json"]
    stored = json.loads(stored) if type(stored) is str else stored
    _require(type(stored) is dict and set(stored) == (_FIELDS - {"idempotency_key"}) | {"selection"})
    compact = validated_source_batch_command(
        {key: field_value for key, field_value in stored.items() if key != "selection"}
        | {"idempotency_key": request_record["idempotency_key"]}
    )
    selected, digest = selection_identity(stored["selection"])
    _require(
        selected == stored["selection"]
        and digest == compact["selection_sha256"]
        and len(selected) == compact["selection_count"]
    )
    actor = request_record["actor_json"]
    actor = json.loads(actor) if type(actor) is str else actor
    metadata = publication_request_metadata(
        stored, actor, request_record["session_token_sha256"], compact["idempotency_key"]
    )
    _require(
        metadata["request_sha256"] == request_record["request_sha256"]
        and metadata["actor_key"] == request_record["actor_key"]
    )
    command = RegistryApprovalCommand(
        compact["expected_draft_revision"],
        compact["expected_approved_revision"],
        tuple(selected),
        compact["reason"],
        compact["idempotency_key"],
    )
    _validated_command(command)
    return compact, command


async def _binding_history(connection, namespace, document, actor):
    documents = []
    for reference in document["batch_references"]:
        documents.extend(
            await _one_batch_history(
                connection, namespace, document, actor, reference, remaining_records=MAX_SELECTIONS - len(documents)
            )
        )
    _require(len(documents) <= MAX_SELECTIONS and len({item["binding_id"] for item in documents}) == len(documents))
    return documents


async def _one_batch_history(connection, namespace, document, actor, reference, *, remaining_records=MAX_SELECTIONS):
    _require(type(remaining_records) is int and 1 <= remaining_records <= MAX_SELECTIONS)
    actor_key = hashlib.sha256(_json(_validated_actor(actor)).encode()).hexdigest()
    batch = await connection.fetchrow(
        f"SELECT request_sha256,receipt_json::text AS receipt FROM {namespace}.registry_network_binding_batch "
        "WHERE actor_key=$1 AND idempotency_key=$2",
        actor_key,
        reference["idempotency_key"],
    )
    _require(batch is not None and batch["request_sha256"] == reference["request_sha256"])
    receipt = json.loads(batch["receipt"])
    _require(
        set(receipt) == {"custom_revision", "records"} and receipt["custom_revision"] == reference["custom_revision"]
    )
    _require(type(receipt["records"]) is list and 1 <= len(receipt["records"]) <= remaining_records)
    history_rows = await connection.fetch(
        f"SELECT h.record_key,h.revision,h.custom_revision,h.request_sha256,h.record_json::text AS document "
        f"FROM {namespace}.registry_record_history h JOIN jsonb_to_recordset($1::jsonb) "
        "s(record_kind text,record_id text,revision bigint) ON h.record_kind=s.record_kind "
        "AND h.record_key=s.record_id AND h.revision=s.revision ORDER BY h.record_key LIMIT 5001",
        _json(receipt["records"]),
    )
    _require(
        len(history_rows) == len(receipt["records"])
        and len({history_row["record_key"] for history_row in history_rows}) == len(history_rows)
    )
    documents = []
    for history_row in history_rows:
        binding_document = json.loads(history_row["document"])
        _require(
            history_row["custom_revision"] == reference["custom_revision"]
            and history_row["request_sha256"] == reference["request_sha256"]
        )
        _require(
            binding_document["binding_id"] == history_row["record_key"]
            and binding_document["revision"] == history_row["revision"]
        )
        _require(
            all(
                binding_document[key] == coordinate_value
                for key, coordinate_value in document["source_coordinates"].items()
            )
        )
        scope = binding_document["source_scope_json"]
        if scope.get("kind") == "uhc_plan_tier_v1":
            from process.registry_fhir_plan_tier import source_key, validate_binding_scope
            from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

            _require(binding_document["source_id"] == UHC_PROVIDER_FILE_SOURCE_ID)
            _require(binding_document["producer_id"] == "registry-source-fhir-witness-v1")
            tier = validate_binding_scope(scope, binding_document["producer_id"])
            _require(binding_document["source_key"] == source_key(tier))
        else:
            _require(scope.get("organization_id") == binding_document["source_key"])
        documents.append(binding_document)
    return documents


async def _catalog_map(connection, namespace, document):
    from process.registry_fhir_catalog_custody import validate_fhir_catalog_snapshot_record

    snapshot = await connection.fetchrow(
        f"SELECT {','.join(SNAPSHOT_COLUMNS)} FROM {namespace}.registry_fhir_catalog_snapshot "
        "WHERE metadata_generation=$1 AND published_at IS NOT NULL",
        document["metadata_generation"],
    )
    _require(snapshot is not None)
    await validate_fhir_catalog_snapshot_record(connection, snapshot)
    coordinates = document["source_coordinates"]
    relation = _namespace(snapshot["schema_name"]) + ".registry_fhir_catalog_binding"
    rows = await connection.fetch(
        f"SELECT b.network_id,b.document_json->>'source_key' AS source_key,b.document_json::text AS document FROM {relation} b "
        f"JOIN {_namespace(snapshot['schema_name'])}.registry_fhir_catalog_network n USING(metadata_generation,network_id) "
        "WHERE b.metadata_generation=$1 AND b.source_id=$2 AND b.document_json->>'dataset_schema'=$3 "
        "AND b.document_json->>'dataset_id'=$4 AND NOT n.archived ORDER BY b.network_id,b.binding_id LIMIT 5001",
        document["metadata_generation"],
        coordinates["source_id"],
        coordinates["dataset_schema"],
        coordinates["dataset_id"],
    )
    _require(1 <= len(rows) <= MAX_SELECTIONS)
    catalog_by_pair = {(row["source_key"], row["network_id"]): json.loads(row["document"]) for row in rows}
    _require(len(catalog_by_pair) == len(rows) and len({key for key, _ in catalog_by_pair}) == len(catalog_by_pair))
    return catalog_by_pair


async def _prospective_bindings(connection, namespace, document, changes):
    coordinates = document["source_coordinates"]
    fields = tuple(coordinates)
    predicate = " AND ".join(f"record_json->>'{field}'=${index + 2}" for index, field in enumerate(fields))
    rows = await connection.fetch(
        f"SELECT record_key,record_revision,record_json::text AS document FROM {namespace}.registry_approved_record "
        f"WHERE approved_revision=$1 AND record_kind='network_binding' AND {predicate} ORDER BY record_key LIMIT 5001",
        document["expected_approved_revision"],
        *(coordinates[field] for field in fields),
    )
    _require(len(rows) <= MAX_SELECTIONS)
    existing_by_id = {row["record_key"]: json.loads(row["document"]) for row in rows}
    selections = [
        {"record_kind": "network_binding", "record_id": item["binding_id"], "revision": item["revision"]}
        for item in changes
        if existing_by_id.get(item["binding_id"]) != item
    ]
    prospective = existing_by_id | {item["binding_id"]: item for item in changes}
    active_bindings = [item for item in prospective.values() if item["archived"] is False]
    if coordinates["producer_id"] != "registry-fhir-catalog-v1":
        captures = {item["source_scope_json"].get("capture_sha256") for item in active_bindings}
        _require(len(captures) == 1 and all(type(value) is str and _SHA.fullmatch(value) for value in captures))
    bindings_by_pair = {(item["source_key"], item["network_id"]): item for item in active_bindings}
    _require(len(bindings_by_pair) == len(active_bindings))
    return selections, bindings_by_pair


def _require_published_catalog_bindings(coordinates, prospective, catalog):
    """Bind capture-free approval to the complete sealed metadata evidence."""
    from process.registry_network_catalog_read import catalog_binding_document

    fields = (*coordinates, "source_key", "evidence_id", "evidence_sha256")
    for pair, binding in prospective.items():
        expected = catalog[pair]
        document_by_field = {field: expected[field] for field in fields}
        document_by_field.update(
            namespace="source_binding",
            binding_id=expected["binding_id"],
            revision=expected["revision"],
            source_scope=expected["source_scope"],
        )
        catalog_binding_document(document_by_field, network_id=pair[1])
        _require(all(expected[field] == coordinates[field] for field in coordinates))
        _require(all(binding[field] == expected[field] for field in fields))
        _require(binding["source_scope_json"] == expected["source_scope"])


async def _missing_network_selections(connection, namespace, document, network_ids):
    """Keep existing approved targets; never approve an unrelated pending correction."""
    rows = await connection.fetch(
        f"SELECT record_key,(record_json->>'archived')::boolean AS archived FROM {namespace}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network' AND record_key=ANY($2::text[])",
        document["expected_approved_revision"],
        [str(key) for key in network_ids],
    )
    _require(all(row["archived"] is False for row in rows))
    missing = {str(key) for key in network_ids} - {row["record_key"] for row in rows}
    if not missing:
        return []
    histories = await connection.fetch(
        f"SELECT DISTINCT ON(record_key) record_key,revision,(record_json->>'archived')::boolean AS archived "
        f"FROM {namespace}.registry_record_history WHERE record_kind='network' AND record_key=ANY($1::text[]) "
        "AND custom_revision<=$2 ORDER BY record_key,revision DESC LIMIT 5001",
        sorted(missing),
        max(reference["custom_revision"] for reference in document["batch_references"]),
    )
    _require({row["record_key"] for row in histories} == missing and all(row["archived"] is False for row in histories))
    return [
        {"record_kind": "network", "record_id": int(row["record_key"]), "revision": row["revision"]}
        for row in histories
    ]


async def resolve_source_batch(connection, document, actor, *, control_schema=None, allow_unbound=False):
    """Resolve one exact complete Source map from immutable receipt/history and catalog custody."""
    compact = validated_source_batch_command(document, allow_unbound=allow_unbound)
    _require(connection.is_in_transaction())
    namespace = _namespace(control_schema)
    changes = await _binding_history(connection, namespace, compact, actor)
    catalog = await _catalog_map(connection, namespace, compact)
    selections, prospective = await _prospective_bindings(connection, namespace, compact, changes)
    _require(prospective.keys() == catalog.keys())
    if compact["source_coordinates"]["producer_id"] == "registry-fhir-catalog-v1":
        _require_published_catalog_bindings(compact["source_coordinates"], prospective, catalog)
    selections += await _missing_network_selections(connection, namespace, compact, {key for _, key in prospective})
    selected, digest = selection_identity(selections)
    if compact["selection_sha256"] is not None:
        _require((compact["selection_count"], compact["selection_sha256"]) == (len(selected), digest))
    compact.update(selection_count=len(selected), selection_sha256=digest)
    command = RegistryApprovalCommand(
        compact["expected_draft_revision"],
        compact["expected_approved_revision"],
        tuple(selected),
        compact["reason"],
        compact["idempotency_key"],
    )
    _validated_command(command)
    return compact, command


def source_batch_page(compact, command, offset):
    """Return one exact bounded page of the frozen approval vector."""
    _require(type(offset) is int and 0 <= offset < len(command.selection) and offset % PAGE_SIZE == 0)
    selected_versions = list(command.selection[offset : offset + PAGE_SIZE])
    return {
        "operation": compact["operation"],
        **{
            key: compact[key]
            for key in (
                "expected_draft_revision",
                "expected_approved_revision",
                "expected_serving_generation",
                "selection_count",
                "selection_sha256",
            )
        },
        "selection": selected_versions,
        "offset": offset,
        "next_offset": offset + len(selected_versions)
        if offset + len(selected_versions) < len(command.selection)
        else None,
    }


async def enqueue_source_batch(connection, document, actor, session_hash, *, control_schema=None):
    """Resolve references before using the unchanged bounded approval/queue storage."""
    from process.registry_publication_queue import _enqueue_validated_publication

    compact, command = await resolve_source_batch(connection, document, actor, control_schema=control_schema)
    stored_by_field = {key: value for key, value in compact.items() if key != "idempotency_key"}
    stored_by_field["selection"] = list(command.selection)
    _require(len(_json(stored_by_field).encode()) <= MAX_SELECTION_BYTES)
    return await _enqueue_validated_publication(
        connection,
        command,
        actor,
        session_token_sha256=session_hash,
        expected_serving_generation=compact["expected_serving_generation"],
        control_schema=control_schema,
        command_document=stored_by_field,
    )


async def queued_source_batch_page(connection, request_id, actor, offset, *, control_schema=None):
    """Read only the retained vector; completed requests need no current draft lookup."""
    _require(type(request_id) is UUID and request_id.int)
    row = await connection.fetchrow(
        f"SELECT * FROM {_namespace(control_schema)}.registry_publication_request WHERE request_id=$1", request_id
    )
    _require(row is not None)
    actor_document = _validated_actor(actor)
    owner = json.loads(row["actor_json"]) if type(row["actor_json"]) is str else row["actor_json"]
    _require(actor_document == owner or actor_document["kind"] == "platform_admin")
    compact, command = stored_source_batch(row)
    return source_batch_page(compact, command, offset)


async def authorize_source_batch(authorize, request_record):
    """Verify every existing authority receipt within one unchanged timeout budget."""
    compact, command = stored_source_batch(request_record)
    actor_by_field = request_record["actor_json"]
    actor_by_field = json.loads(actor_by_field) if type(actor_by_field) is str else actor_by_field
    if actor_by_field.get("impersonator_id") is None:
        actor_by_field = {key: value for key, value in actor_by_field.items() if key != "impersonator_id"}
    revision = None
    async with asyncio.timeout(15):
        for offset in range(0, compact["selection_count"], PAGE_SIZE):
            selected_versions = list(command.selection[offset : offset + PAGE_SIZE])
            receipt = await authorize(
                session_token_sha256=request_record["session_token_sha256"],
                actor=actor_by_field,
                selection=selected_versions,
            )
            _require(
                type(receipt) is dict and set(receipt) == {"authorized", "actor", "selection_sha256", "policy_revision"}
            )
            _require(
                receipt["authorized"] is True
                and receipt["actor"] == actor_by_field
                and receipt["selection_sha256"] == hashlib.sha256(_json(selected_versions).encode()).hexdigest()
            )
            _counter(receipt["policy_revision"])
            if revision is None:
                revision = receipt["policy_revision"]
            _require(receipt["policy_revision"] == revision)
    return revision


def validate_source_batch_approval_result(result_by_name, compact, command, state):
    """Keep approval completion bound to the exact selection and unchanged serving head."""
    if state != "completed":
        _require(
            result_by_name is None
            or state == "rejected"
            and result_by_name == {"error": {"code": "registry_publication_rejected"}}
        )
        return
    _require(
        type(result_by_name) is dict
        and set(result_by_name)
        == {
            "operation",
            "approved_revision",
            "previous_approved_revision",
            "selected_count",
            "selection_sha256",
            "serving_generation",
            "policy_revision",
        }
    )
    _require(result_by_name["operation"] == APPROVAL_OPERATION and compact["operation"] == APPROVAL_OPERATION)
    for field in (
        "approved_revision",
        "previous_approved_revision",
        "selected_count",
        "serving_generation",
        "policy_revision",
    ):
        _counter(result_by_name[field])
    _require(
        result_by_name["approved_revision"] == command.expected_draft_revision + 1
        and result_by_name["previous_approved_revision"] == command.expected_approved_revision
        and result_by_name["selected_count"] == len(command.selection) == compact["selection_count"]
        and result_by_name["selection_sha256"] == compact["selection_sha256"]
        and result_by_name["serving_generation"] == compact["expected_serving_generation"]
    )
