# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable replay evidence; synthetic scalar receipts do not exercise full builders."""

import asyncio
import dataclasses
import datetime
from copy import deepcopy

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.schema import MetaData

from alembic import op
from process import provider_directory_cms_replay as replay
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_contract as contract
from tests import provider_directory_profile_replay_seed as seed_support
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_cms_serving_receipt_postgres import _migration
from tests.test_provider_directory_profile_replay_postgres import (
    _replay_scenario,
    _seed_committed_replay,
    importer,
)
from tests.test_provider_directory_profile_selection_desired import _desired, _proof


def _execution(*, purge=False):
    """Use a canonical desired proof with the original seeded authority revision."""
    payload_by_field = {**_proof(_desired(day="2026-07-30")).payload, "authority_revision": 7}
    if purge:
        for name in contract._DESIRED_SELECTION_FIELDS:
            payload_by_field.pop(name)
        payload_by_field.update(
            contract_id=contract.PROFILE_SELECTION_ATTESTATION_CONTRACT_ID, operation="purge", pairs=[]
        )
        payload_by_field["proof_id"] = contract._proof_id(payload_by_field)
    return contract.ProviderDirectoryProfileExecution(
        attestation=contract.validated_profile_selection_attestation(payload_by_field),
        generation=7,
    )


async def _register(database, schema, execution):
    """Store the exact historical proof and observation in actual registry model tables."""
    identity = contract._identity_without_authority(execution.attestation.payload)
    digest = selection._input_identity_digest(identity)
    now = datetime.datetime.now(datetime.timezone.utc)
    metadata = MetaData()
    for model, values in (
        (
            selection.ProviderDirectoryProfileSelectionProof,
            {
                "input_identity_digest": digest,
                "proof_id": execution.attestation.proof_id,
                "identity_json": identity,
                "created_at": now,
            },
        ),
        (
            selection.ProviderDirectoryProfileSelectionObservation,
            {
                "authority_revision": execution.attestation.authority_revision,
                "input_identity_digest": digest,
                "payload_json": execution.attestation.payload,
                "created_at": now,
            },
        ),
    ):
        table = model.__table__.to_metadata(metadata, schema=schema)
        await database.create_table(table)
        await database.insert(table).values(**values).status()


async def _common_payload(database, schema, execution):
    """Bind synthetic native scalar dependencies to the actual signed Profile delta."""
    delta_by_field = dict(
        importer._pagination_checkpoint_row_mapping(
            await database.first(f'SELECT * FROM "{schema}".provider_directory_profile_delta_receipt')
        )
    )
    profile_by_field = {key: delta_by_field[key] for key in replay.common._PROFILE_FIELDS if key in delta_by_field}
    profile_by_field.update(
        status="purged" if execution.attestation.operation == "purge" else "published",
        profile_schema_version=execution.attestation.profile_schema_version,
        profile_strategy_version=execution.attestation.profile_strategy_version,
        source_vector_hash=delta_by_field["to_source_vector_hash"],
        source_context_vector_hash=delta_by_field["to_source_context_vector_hash"],
    )
    authority_by_field = {
        "local_lineage_id": "00000000-0000-4000-8000-000000000001",
        "local_generation": 1,
        "origin_lineage_id": "00000000-0000-4000-8000-000000000001",
        "origin_generation": 1,
        "published_at": "2026-07-30T00:00:00+00:00",
        "relation_oids": [1, 2, 3],
    }
    pin = lambda pair: {key: pair[key] for key in replay.common._PIN_FIELDS}
    attestation = execution.attestation
    return {
        "contract_version": 1,
        "predecessor_receipt_id": None,
        "expected_incumbent": pin(attestation.expected_cms_incumbent or _proof().expected_cms_incumbent),
        "cms": {
            **pin(attestation.desired_cms_dataset or _proof().desired_cms_dataset),
            "release_id": "a" * 64,
            "proof_version": 2,
        },
        "desired_datasets": [pin(pair) for pair in attestation.pairs],
        "selection": {
            "proof_id": attestation.proof_id,
            "fingerprint": attestation.selection_fingerprint,
            "catalog_digest": attestation.catalog_digest,
        },
        "profile": profile_by_field,
        "address": authority_by_field,
        "doctors": {**authority_by_field, "importer_id": "cms-doctors"},
        "alias_generation": 1,
        "overlay_oid": 4,
    }


async def _insert_common(database, schema, payload_by_field):
    """Insert a checksum-constrained immutable fixture using the production receipt writer."""
    async with database.session() as session:
        async with session.begin():
            return await replay.common.append_serving_receipt(session, schema, payload_by_field)


def _seed_purge_operation(monkeypatch):
    """Specialize only synthetic seed rows; all production replay verification remains real."""
    receipt_values = seed_support._replay_receipt_values
    serving_values = seed_support._replay_serving_values
    monkeypatch.setattr(
        seed_support, "_replay_receipt_values", lambda *args: {**receipt_values(*args), "operation": "purge"}
    )
    monkeypatch.setattr(
        seed_support,
        "_replay_serving_values",
        lambda *args: {**serving_values(*args), "operation": "purge", "status": "purged"},
    )


async def _setup(database, schema, monkeypatch, *, purge=False):
    """Seed real signature/geometry/consumption proof; native authority construction is out of scope."""
    execution = _execution(purge=purge)
    if purge:
        _seed_purge_operation(monkeypatch)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    monkeypatch.setattr(importer, "db", database)
    monkeypatch.setattr(selection, "db", database)
    source_pairs = tuple((pair["source_id"], pair["dataset_id"]) for pair in execution.attestation.pairs)
    scenario = dataclasses.replace(
        _replay_scenario(True),
        proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        source_vector=source_pairs,
        source_context_vector=tuple((source_id, "5" * 64) for source_id, _dataset in source_pairs),
    )
    envelope = await _seed_committed_replay(database, schema, scenario)
    monkeypatch.setattr(
        importer.profile_capacity_runtime,
        "configured_capacity_lease_trust",
        lambda: capacity_trust_from_envelope(envelope),
    )
    await _register(database, schema, execution)
    async with database.engine.begin() as connection:

        def create(sync):
            with Operations.context(MigrationContext.configure(sync)):
                _migration("20260930100000")._create_table(schema)
                op.execute(
                    f'CREATE INDEX cms_serving_profile_delta_proof ON "{schema}".provider_directory_profile_delta_receipt (selection_proof_id)'
                )

        await connection.run_sync(create)
    payload_by_field = await _common_payload(database, schema, execution)
    return execution, scenario, payload_by_field


async def _run(execution, scenario):
    return await replay.replay_committed_cms_profile(
        importer,
        run_id=scenario.requested_run_id,
        control_run_id=scenario.requested_run_id,
        execution=execution,
        metrics={"synthetic": True},
    )


@pytest.mark.asyncio
async def test_historical_composite_replay_survives_successors_and_removed_targets(monkeypatch):
    """Prove real retained signatures and earliest common receipt work without current pointers."""
    async with _delta_database(monkeypatch) as (database, schema):
        execution, scenario, payload_by_field = await _setup(database, schema, monkeypatch)
        receipt_id = await _insert_common(database, schema, payload_by_field)
        successor = deepcopy(payload_by_field)
        successor["predecessor_receipt_id"] = receipt_id
        successor["address"]["local_generation"] = 2
        await _insert_common(database, schema, successor)
        await database.status(f'DELETE FROM "{schema}".provider_directory_profile_serving_generation')
        await database.status(f'DROP TABLE "{schema}".provider_directory_profile')
        result = await _run(execution, scenario)
        assert result["cms_serving"]["receipt_id"] == receipt_id
        assert result["cms_serving"]["address_generation"] == 1
        assert result["profile"]["committed_replay"]["run_id"] == scenario.receipt_run_id
        assert result["profile_selection_result"]["proof_id"] == execution.attestation.proof_id
        assert result["synthetic"] is True


def _seed_corrupt_signature(monkeypatch):
    """Keep a structurally valid immutable row whose retained signature cannot authenticate."""
    original = seed_support._replay_capacity_consumption

    def corrupted(seed):
        values_by_field = original(seed)
        signature = values_by_field["signature"]
        values_by_field["signature"] = ("A" if signature[0] != "A" else "B") + signature[1:]
        return values_by_field

    monkeypatch.setattr(seed_support, "_replay_capacity_consumption", corrupted)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "missing_common",
        "wrong_context",
        "wrong_date",
        "wrong_owner",
        "missing_registration",
        "wrong_generation",
        "forged_attestation",
        "competing_owner",
        "corrupt_capacity",
    ],
)
async def test_composite_replay_rejects_incomplete_or_unbound_evidence(monkeypatch, mutation):
    """Reject incomplete composite publication and cross-execution scalar substitutions."""
    if mutation == "corrupt_capacity":
        _seed_corrupt_signature(monkeypatch)
    async with _delta_database(monkeypatch) as (database, schema):
        execution, scenario, payload_by_field = await _setup(database, schema, monkeypatch)
        if mutation == "wrong_context":
            payload_by_field["profile"]["source_context_vector_hash"] = "0" * 64
        if mutation == "wrong_date":
            payload_by_field["profile"]["profile_as_of"] = "2026-07-31"
        if mutation != "missing_common":
            await _insert_common(database, schema, payload_by_field)
        if mutation == "wrong_owner":
            await database.status(
                f"UPDATE \"{schema}\".import_run SET status='succeeded' WHERE run_id=:run_id",
                run_id=scenario.requested_run_id,
            )
        if mutation == "missing_registration":
            await database.status(f'DELETE FROM "{schema}".provider_directory_profile_selection_observation')
        if mutation == "wrong_generation":
            execution = dataclasses.replace(execution, generation=8)
        if mutation == "forged_attestation":
            execution = dataclasses.replace(
                execution, attestation=dataclasses.replace(execution.attestation, catalog_digest="0" * 64)
            )
        if mutation == "competing_owner":
            await database.status(
                f"UPDATE \"{schema}\".import_run SET status='running' WHERE run_id=:run_id",
                run_id=scenario.receipt_run_id,
            )
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
            await _run(execution, scenario)


@pytest.mark.asyncio
async def test_historical_purge_requires_original_delta_and_common_proof(monkeypatch):
    """Replay an exact signed empty-vector purge without relying on the current native pointer."""
    async with _delta_database(monkeypatch) as (database, schema):
        execution, scenario, payload_by_field = await _setup(database, schema, monkeypatch, purge=True)
        receipt_id = await _insert_common(database, schema, payload_by_field)
        await database.status(f'DELETE FROM "{schema}".provider_directory_profile_serving_generation')
        result = await _run(execution, scenario)
        assert result["cms_serving"]["receipt_id"] == receipt_id
        assert result["artifact_dataset_ids"] == []
        assert result["profile_selection_result"]["operation"] == "purge"


async def _pending_common(database, schema, payload_by_field, ready, release):
    """Hold only the synthetic receipt insertion uncommitted for the visibility race."""
    async with database.session() as session:
        async with session.begin():
            receipt_id = await replay.common.append_serving_receipt(session, schema, payload_by_field)
            ready.set()
            await release.wait()
        return receipt_id


@pytest.mark.asyncio
async def test_replay_requires_committed_common_receipt_visibility(monkeypatch):
    """An in-flight composite receipt cannot turn an incomplete result into success."""
    async with _delta_database(monkeypatch) as (database, schema):
        execution, scenario, payload_by_field = await _setup(database, schema, monkeypatch)
        ready, release = asyncio.Event(), asyncio.Event()
        writer = asyncio.create_task(_pending_common(database, schema, payload_by_field, ready, release))
        try:
            await asyncio.wait_for(ready.wait(), timeout=5)
            with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="common_receipt_missing"):
                await _run(execution, scenario)
        finally:
            release.set()
            receipt_id = await asyncio.wait_for(writer, timeout=5)
        assert (await _run(execution, scenario))["cms_serving"]["receipt_id"] == receipt_id
