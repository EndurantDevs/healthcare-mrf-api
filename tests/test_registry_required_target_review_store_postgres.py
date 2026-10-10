# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed inventory evidence remains immutable and cannot bypass binding approval."""

import hashlib
import json
from uuid import uuid4

import pytest

from process.registry_required_target_review_store import admit_registry_required_target_review
from process.registry_required_target_store import RegistryRequiredTargetError, admit_registry_required_targets
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_registry_network_binding_approval_postgres import _bind, _binding
from tests.test_registry_required_target_store_postgres import _edition, _input, serving_schema

pytestmark = pytest.mark.asyncio
_SOURCE_FIELDS = (
    "binding_id",
    "source_system",
    "source_id",
    "dataset_schema",
    "dataset_id",
    "producer_id",
    "edition_id",
    "source_key",
    "source_scope_json",
)


async def _prepared(fixture):
    connection, schema, engine = fixture
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    input_bytes = _input()
    edition = _edition(input_bytes)
    async with connection.transaction():
        ledger = await admit_registry_required_targets(connection, input_bytes, edition, control_schema=schema)
    key = await connection.fetchval(
        f"SELECT observation_json#>>'{{ledger,targets,0,target_key}}' FROM \"{schema}\".registry_source_observation"
    )
    row = _binding(network["record_id"])
    review_document_dict = {
        "ledger_snapshot_id": str(edition.snapshot_id),
        "ledger_artifact_sha256": ledger["artifact_sha256"],
        "decisions": [
            {
                "target_key": key,
                "resolution_status": "resolved",
                "network_id": row["network_id"],
                "source_binding": {field: row[field] for field in _SOURCE_FIELDS},
                "evidence_reference": "https://example.test/review",
                "evidence_sha256": "b" * 64,
                "reason": "Reviewed exact source identity",
            }
        ],
    }
    return actor, network, edition, row, review_document_dict


async def _review(fixture, ledger_edition, document, review_edition=None):
    connection, schema, _ = fixture
    input_bytes = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    edition = _edition(input_bytes) if review_edition is None else review_edition
    async with connection.transaction():
        receipt = await admit_registry_required_target_review(
            connection, input_bytes, edition, ledger_edition.snapshot_id, control_schema=schema
        )
    return edition, receipt


async def test_complete_review_replay_and_typed_binding_require_explicit_approval(serving_schema):
    connection, schema, _ = serving_schema
    actor, network, ledger, reviewed_binding, document = await _prepared(serving_schema)
    edition, review = await _review(serving_schema, ledger, document)
    assert (review["decision_count"], review["resolved_count"], review["physical_records"], review["replayed"]) == (
        1,
        1,
        1,
        False,
    )
    _, replay = await _review(serving_schema, ledger, document, edition)
    assert replay == {**review, "replayed": True}
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (1, 0)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
    reviewed_binding.update(evidence_id=review["evidence_id"], evidence_sha256=review["artifact_sha256"])
    binding = (await _bind(connection, schema, reviewed_binding, actor))["records"][0]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0
    approved = await _approve(connection, schema, await _command(connection, schema, network, binding), actor)
    assert approved["selected_count"] == 2
    approved_binding = json.loads(
        await connection.fetchval(
            f"SELECT record_json::text FROM \"{schema}\".registry_approved_record WHERE record_kind='network_binding'"
        )
    )
    assert approved_binding["evidence_sha256"] == review["artifact_sha256"]
    retained = json.loads(
        await connection.fetchval(
            f'SELECT observation_json::text FROM "{schema}".registry_source_observation WHERE snapshot_id=$1',
            edition.snapshot_id,
        )
    )
    canonical = json.dumps(retained, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert hashlib.sha256(canonical).hexdigest() == review["artifact_sha256"]
    assert retained["decisions"][0]["source_binding"]["binding_key"] == approved_binding["binding_key"]


@pytest.mark.parametrize("changed", ["source_id", "network_id", "hash", "missing", "malformed"])
async def test_reserved_reference_must_match_full_retained_decision(serving_schema, changed):
    connection, schema, _ = serving_schema
    actor, network, ledger, row, document = await _prepared(serving_schema)
    _, review = await _review(serving_schema, ledger, document)
    row.update(evidence_id=review["evidence_id"], evidence_sha256=review["artifact_sha256"])
    if changed == "source_id":
        row["source_id"] = "different-source"
    elif changed == "network_id":
        other = await _draft(serving_schema[2], schema, _create("network"), actor)
        row["network_id"] = other["record_id"]
    elif changed == "hash":
        row["evidence_sha256"] = "0" * 64
    elif changed == "missing":
        row["evidence_id"] = "required-target-review:" + str(uuid4())
    else:
        row["evidence_id"] = "required-target-review:invalid"
    revision = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    with pytest.raises(ValueError, match="registry_source_binding_review_invalid"):
        await _bind(connection, schema, row, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == revision
    assert await connection.fetchval("SELECT 42") == 42


@pytest.mark.parametrize("changed", ["review", "ledger"])
async def test_body_drift_cannot_hide_behind_unchanged_snapshot_hash(serving_schema, changed):
    connection, schema, _ = serving_schema
    actor, _, ledger, row, document = await _prepared(serving_schema)
    edition, review = await _review(serving_schema, ledger, document)
    snapshot_id = edition.snapshot_id if changed == "review" else ledger.snapshot_id
    path = "{decisions,0,reason}" if changed == "review" else "{ledger,observations,0,raw_cells,0}"
    await connection.execute(
        f'UPDATE "{schema}".registry_source_observation '
        "SET observation_json=jsonb_set(observation_json,$2::text[],'\"changed\"'::jsonb) WHERE snapshot_id=$1",
        snapshot_id,
        path.strip("{}").split(","),
    )
    row.update(evidence_id=review["evidence_id"], evidence_sha256=review["artifact_sha256"])
    with pytest.raises(ValueError, match="registry_source_binding_review_invalid"):
        await _bind(connection, schema, row, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 1


async def test_invalid_last_review_decision_leaves_no_partial_evidence(serving_schema):
    connection, schema, _ = serving_schema
    _, _, ledger, _, document = await _prepared(serving_schema)
    document["decisions"].append({**document["decisions"][0], "target_key": "unknown-final-target"})
    with pytest.raises(RegistryRequiredTargetError, match="registry_required_target_review_input_invalid"):
        await _review(serving_schema, ledger, document)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0


async def test_unallocated_resolved_network_is_rejected_as_a_set_before_retention(serving_schema):
    connection, schema, _ = serving_schema
    _, _, ledger, _, document = await _prepared(serving_schema)
    document["decisions"][0]["network_id"] = 2147483647
    with pytest.raises(RegistryRequiredTargetError, match="registry_required_target_review_landing_invalid"):
        await _review(serving_schema, ledger, document)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 1
