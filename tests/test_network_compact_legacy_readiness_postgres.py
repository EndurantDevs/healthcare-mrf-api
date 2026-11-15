# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ready historical candidates rebuild named proofs through complete native validation."""

import asyncio
import json
from copy import deepcopy
from uuid import UUID

import pytest

from process.network_compact_readiness import compact_index_digest
from tests.test_network_compact_legacy_readiness import legacy_custom_receipt
from tests.test_network_compact_publication_postgres import _candidate, _prepare, _publish, _read, _seal
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db


async def _legacy_ready(fixture, target):
    prepared = await _prepare(fixture, target)
    report = json.loads(prepared["validation_json"])
    legacy = legacy_custom_receipt(report["compact_custom_offices"])
    report["compact_custom_offices"] = legacy
    for key in ("candidate_readiness", "serving_readiness"):
        report[key]["scope"]["compact_custom_offices"] = deepcopy(legacy)
    serving = report["serving_readiness"]
    serving.pop("index_definition_revision")
    serving["index_definition_sha256"] = "d" * 64
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1',
        UUID(target.candidate_id),
        json.dumps(report, sort_keys=True),
    )
    return report


async def test_legacy_ready_rebuilds_named_proof_preserves_evidence_and_publishes(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, [42, 88], 0)
    original = await _legacy_ready(fixture, target)
    before = deepcopy(original)
    prepared = await _prepare(fixture, target)
    report = json.loads(prepared["validation_json"])
    assert report["legacy_serving_readiness"] == before["serving_readiness"]
    assert report["compact_custom_offices"] == before["compact_custom_offices"]
    assert report["candidate_readiness"] == before["candidate_readiness"]
    named = await compact_index_digest(fixture.connection, target.schema_name, candidate=prepared)
    assert report["serving_readiness"] == {
        **before["serving_readiness"],
        "index_definition_revision": 2,
        "index_definition_sha256": named,
    }
    assert named != before["serving_readiness"]["index_definition_sha256"]
    assert json.loads((await _prepare(fixture, target))["validation_json"]) == report
    assert original == before
    await _seal(fixture, target)
    publication = await _publish(fixture, target)
    pin, rows = await _read(fixture)
    assert pin.generation_id == publication["generation_id"] and len(rows) == 1


@pytest.mark.parametrize("damage", ["slice", "index", "binding", "type", "nullability", "default", "extra-column"])
async def test_legacy_rebuild_refuses_native_drift_without_replacing_evidence(compact_publication_db, damage):
    fixture = compact_publication_db
    target = await _candidate(fixture, [42, 88], 0)
    original = await _legacy_ready(fixture, target)
    namespace = '"' + target.schema_name + '"'
    mutation_sql_by_kind = {
        "slice": f"UPDATE {namespace}.network_office_edge SET location_key='different-office'",
        "index": f"DROP INDEX {namespace}.network_office_edge_location_idx",
        "binding": f"UPDATE {namespace}.provider_location_binding SET location_key='different-office'",
        "type": f"ALTER TABLE {namespace}.network_slice_ref ALTER COLUMN row_count TYPE integer",
        "nullability": f"ALTER TABLE {namespace}.network_slice_ref ALTER COLUMN row_count DROP NOT NULL",
        "default": f"ALTER TABLE {namespace}.network_slice_ref ALTER COLUMN row_count SET DEFAULT 1",
        "extra-column": f"ALTER TABLE {namespace}.network_slice_ref ADD COLUMN extra_value integer",
    }
    await fixture.connection.execute(mutation_sql_by_kind[damage])
    await fixture.execution_connection.reload_schema_state()
    with pytest.raises(ValueError):
        await _prepare(fixture, target)
    retained = await fixture.connection.fetchrow(
        f'SELECT state,validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(target.candidate_id),
    )
    assert retained["state"] == "ready" and json.loads(retained["validation_json"]) == original
    assert not fixture.execution_connection.is_in_transaction()


async def test_legacy_rebuild_cancellation_keeps_original_evidence(compact_publication_db, monkeypatch):
    from process import network_compact_validation as validation

    fixture = compact_publication_db
    target = await _candidate(fixture, [42, 88], 0)
    original = await _legacy_ready(fixture, target)
    verify = validation.verify_compact_final_proof

    async def cancelled(*args, **kwargs):
        await verify(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(validation, "verify_compact_final_proof", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await _prepare(fixture, target)
    retained = await fixture.connection.fetchval(
        f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(target.candidate_id),
    )
    assert json.loads(retained) == original and not fixture.execution_connection.is_in_transaction()
