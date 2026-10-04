# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native initial receipt replay using synthetic signed authority and sealed scalars.

This proves transaction/replay behavior, not dataset admission or Linux witnesses.
"""

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, text

from process import provider_directory_profile_capacity_attestation as leases
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot
from tests import test_provider_directory_cms_serving_receipt_postgres as common
from tests import test_provider_directory_import_run_guards as run_guards
from tests import test_provider_directory_profile_initial_cutover_postgres as cutover_test
from tests import test_provider_directory_profile_initial_migration as fixture
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_profile_delta_test_support import _prepared_delta
from tests.provider_directory_profile_initial_test_support import signed_initial_envelope
from tests.provider_directory_profile_replay_test_support import _capacity_guard_statements
from tests.test_provider_directory_cms_replay_postgres import _register
from tests.test_provider_directory_profile_selection_desired import _selection_rows

fhir, capacity = cutover_test.fhir, cutover_test.capacity


def _selection(day="2026-09-29", *, current=False):
    pair_by_field = {
        **common._PIN,
        "publication_status": "published" if current else "validated",
        "is_current": current,
        "lineage_authority": selection.PROFILE_SELECTION_LINEAGE_AUTHORITY,
    }
    desired_by_field = {
        "desired_cms_dataset": pair_by_field,
        "expected_cms_incumbent": pair_by_field if current else None,
        "desired_profile_as_of": day,
    }
    catalog, sources, rows, candidate = _selection_rows(desired_by_field)
    catalog["items"] = [item for item in catalog["items"] if item["source_ids"] == ["cms-npd"]]
    sources = [{**row, "endpoint_id": "endpoint"} if row["source_id"] == "cms-npd" else row for row in sources]
    computed = snapshot._computed_desired_selection_from_rows(
        catalog,
        node_id="dev-node",
        source_rows=sources,
        dataset_rows=rows,
        desired_dataset_row=candidate,
        desired_selection=desired_by_field,
    )
    payload = {**computed.identity_payload, "authority_revision": 2}
    payload["proof_id"] = selection._proof_id(payload)
    return selection.ProviderDirectoryProfileExecution(
        attestation=selection.validated_profile_selection_attestation(payload), generation=7
    )


async def _metadata(database, engine):
    schema = fixture._SCHEMA
    async with engine.begin() as connection:
        # Replace only this fixture's empty three-column run stub before authority is seeded.
        await connection.execute(text(f'DROP TABLE "{schema}".import_run'))
        await connection.run_sync(lambda sync: _delta_metadata(sync, schema))
        for sql in _capacity_guard_statements(schema):
            await connection.execute(text(sql))
        names = tuple(
            name
            for name in common.receipts._NATIVE_RELATIONS
            if name not in {fhir.profile_artifact.PROFILE_TABLE, fhir.profile_artifact.PROFILE_EVIDENCE_TABLE}
        )
        with patch.object(common.receipts, "_NATIVE_RELATIONS", names):
            await common._create_scalar_tables(connection, schema)
        await common._create_native_tables(connection, schema)
        await connection.run_sync(lambda sync: run_guards._install(sync, schema, create_schema=False))
        for trigger in ("cms_serving_profile_transition", "cms_serving_no_truncate"):
            await connection.execute(
                text(f'DROP TRIGGER {trigger} ON "{schema}".provider_directory_profile_serving_generation')
            )
            await connection.execute(text(f'DROP FUNCTION "{schema}".{trigger}()'))
        await connection.execute(text(f'DROP TABLE "{schema}".provider_directory_cms_serving_receipt'))
        await connection.execute(text(f'DROP FUNCTION "{schema}".cms_serving_receipt_immutable()'))
        await connection.run_sync(lambda sync: common._apply(sync, "20260930100000"))
        await connection.execute(
            text(
                f'ALTER TABLE "{schema}".provider_directory_source ADD COLUMN canonical_api_base text, ADD COLUMN org_name text, ADD COLUMN plan_name text'
            )
        )


def _delta_metadata(connection, schema):
    with Operations.context(MigrationContext.configure(connection)):
        fixture.delta._create_delta_receipt_table(schema)


async def _authority(database, engine, consumed, preflight, geometry, projection):
    """Prepare canonical signed initial authority for the existing public-entry replay proof."""
    consumed_by_field = consumed
    await _metadata(database, engine)
    execution = _selection()
    schema = fixture._SCHEMA
    async with engine.begin() as connection:
        await connection.execute(
            fhir.ImportRun.__table__.to_metadata(MetaData(), schema=schema)
            .insert()
            .values(
                run_id=consumed_by_field["run_id"],
                importer="provider-directory-fhir",
                status="running",
                params={},
                metrics={},
            )
        )
    database_identity, geometry, source_pairs, contexts, target_snapshot, projection = await _bind_replay_geometry(
        database, schema, geometry, execution, projection
    )
    accepted = datetime.now(timezone.utc).replace(microsecond=0)
    envelope, receipt = signed_initial_envelope(geometry, database_identity, target_snapshot, execution, accepted)
    trust = capacity_trust_from_envelope(envelope)
    verified, consumed_by_field = _consume_replay_authority(
        envelope, trust, accepted, geometry, consumed_by_field, preflight, receipt
    )
    generation = (
        "pdprofile_"
        + fhir.hashlib.sha256(f"{consumed_by_field['build_id']}:{geometry.profile_as_of}".encode()).hexdigest()[:32]
    )
    return (
        consumed_by_field,
        preflight,
        geometry,
        projection,
        {
            "build": {
                "generation_id": generation,
                "profile_as_of": geometry.profile_as_of,
                "selection_proof_id": geometry.selection_proof_id,
                "authority_revision": execution.attestation.authority_revision,
                "desired_source_vector": source_pairs,
                "desired_source_context_vector": contexts,
                "desired_source_vector_hash": geometry.desired_source_vector_hash,
                "desired_source_context_vector_hash": geometry.desired_context_vector_hash,
            },
            "verified_lease": verified,
            "database_identity": database_identity,
            "execution": replace(execution, capacity_attestation=envelope),
            "trust": trust,
        },
    )


async def _advance_delta(database, cutover):
    """Commit a zero-row successor through the production delta CAS and immutable receipt writers."""
    schema = fixture._SCHEMA
    old = await fhir._provider_directory_profile_serving_state(schema)
    execution = replace(_selection("2026-09-30", current=True), generation=8)
    token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    try:
        assert fhir._provider_directory_profile_delta_sources(old, old.source_vector, old.source_context_vector) == (
            ("cms-npd",),
            (),
        )
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)
    raw = capacity.capacity_geometry_payload(cutover["admission"].geometry)
    for field in contract.INITIAL_FIELDS:
        raw.pop(field)
    raw.update(
        contract_id=capacity.CAPACITY_GEOMETRY_CONTRACT_ID,
        materialization_mode="source_delta",
        current_source_vector_hash=old.source_vector_hash,
        current_context_vector_hash=old.source_context_vector_hash,
        selection_proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        profile_as_of=execution.attestation.desired_profile_as_of,
        evidence_target_oid=old.evidence_target_oid,
        profile_target_oid=old.profile_target_oid,
    )
    for role, oid in (("evidence", old.evidence_target_oid), ("profile", old.profile_target_oid)):
        raw[role + "_target_storage_fingerprint"] = (
            await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        ).exact_fingerprint
    geometry = capacity.validated_capacity_geometry(raw)
    build_id = "pdpb_" + "9" * 32
    generation = "pdprofile_" + fhir.hashlib.sha256(f"{build_id}:{geometry.profile_as_of}".encode()).hexdigest()[:32]
    delta = _successor_delta(schema, build_id, generation, old, execution, geometry)
    counts_by_field = {
        key: 0
        for key in (
            "evidence_rows",
            "profile_rows",
            "evidence_inserted",
            "evidence_deleted",
            "profile_inserted",
            "profile_deleted",
        )
    }
    forecast_by_field = {"contract_id": geometry.cutover_forecast_contract_id, "build_id": build_id}
    forecast_hash = fhir._identity_hash(forecast_by_field)
    await _commit_replay_successor(
        database, schema, delta, counts_by_field, forecast_hash, forecast_by_field, execution
    )
    return generation


async def _publish_initial_fixture(database, cutover, stages, admission, execution, fence):
    """Commit the initial Profile and common receipt in the production owner transaction."""
    async with database.transaction() as connection:
        await fhir._apply_provider_directory_profile_capacity_settings(admission)
        async with initial.swap_window(fhir, cutover):
            for stage in stages:
                await fhir._install_provider_directory_prepared_stage(stage)
            for stage in stages:
                await fhir._finish_provider_directory_prepared_stage(stage)
        await initial.finish_cutover(fhir, cutover, fence)
        await common._seed_source(connection, fixture._SCHEMA)
        await connection.execute(
            text(f"UPDATE \"{fixture._SCHEMA}\".provider_directory_source SET org_name='Synthetic directory'")
        )
        await common._advance_native(connection, fixture._SCHEMA, doctors=True)
        await common._advance_native(connection, fixture._SCHEMA)
        receipt_payload = await common._payload(connection, fixture._SCHEMA)
        receipt_payload["selection"] = {
            "proof_id": execution.attestation.proof_id,
            "fingerprint": execution.attestation.selection_fingerprint,
            "catalog_digest": execution.attestation.catalog_digest,
        }
        await common.receipts.append_serving_receipt(connection, fixture._SCHEMA, receipt_payload)
        await database.status("SET CONSTRAINTS ALL IMMEDIATE")
    identities = tuple(
        fhir.ProviderDirectoryArtifactPromotionIdentity(stage, cutover["oids"][stage.target_relation])
        for stage in stages
    )
    assert await initial.resolve_completion(fhir, stages, fence, identities)


async def _assert_current_replay(database, cutover, stages, admission, execution, fence, mutable_context):
    """Check public replay and reject receipt replacement or storage drift."""
    async with database.transaction():
        await database.status("SET TRANSACTION READ ONLY")
        replay = await initial.committed_replay(
            fhir,
            fixture._SCHEMA,
            f'"{fixture._SCHEMA}".provider_directory_profile_capacity_lease_consumption',
            admission.run_id,
            execution,
            fence,
        )
        assert replay["committed_replay"]["current"] is True
        assert await initial.is_committed(fhir, stages)
    with patch.object(fhir, "_provider_directory_profile_replay_source_context", mutable_context):
        replayed_result = await fhir._publish_attested_provider_directory_profile(
            run_id=admission.run_id,
            control_run_id=admission.run_id,
            execution=execution,
            metrics={"marker": "retained"},
        )
    assert replayed_result["marker"] == "retained"
    assert replayed_result["profile"]["committed_replay"]["status"] == "current"
    assert replayed_result["cms_serving"]["profile_generation_id"] == cutover["build"].generation_id
    for ddl in (
        f'ALTER TABLE "{fixture._SCHEMA}".{contract.RECEIPT_TABLE} SET (fillfactor=80)',
        f'ALTER TABLE "{fixture._SCHEMA}".{contract.RECEIPT_TABLE} RENAME TO replaced_receipt',
    ):
        with pytest.raises(Exception, match="initial_receipt|does not exist"):
            async with database.transaction():
                await database.status(ddl)
                await initial.committed_replay(
                    fhir,
                    fixture._SCHEMA,
                    f'"{fixture._SCHEMA}".provider_directory_profile_capacity_lease_consumption',
                    admission.run_id,
                    execution,
                    fence,
                )


async def _assert_superseded_replay(database, engine, cutover, admission, execution, fence, mutable_context):
    """Replay the original receipt after a real delta commit and a terminal original owner."""
    successor = await _advance_delta(database, cutover)
    before = await database.first(
        f'SELECT generation_id,xmin::text AS xmin FROM "{fixture._SCHEMA}".provider_directory_profile_serving_generation'
    )
    async with database.transaction():
        await database.status("SET TRANSACTION READ ONLY")
        replay = await initial.committed_replay(
            fhir,
            fixture._SCHEMA,
            f'"{fixture._SCHEMA}".provider_directory_profile_capacity_lease_consumption',
            admission.run_id,
            execution,
            fence,
        )
        assert replay["committed_replay"]["status"] == "superseded"
        assert replay["committed_replay"]["current_generation_id"] == successor
        assert replay["generation_id"] == cutover["build"].generation_id
    retry_run = "run_" + "7" * 32
    assert retry_run != admission.run_id
    await database.status(
        f"UPDATE \"{fixture._SCHEMA}\".import_run SET status='succeeded',finished_at=now() WHERE run_id=:run",
        run=admission.run_id,
    )
    async with engine.begin() as connection:
        await connection.execute(
            fhir.ImportRun.__table__.to_metadata(MetaData(), schema=fixture._SCHEMA)
            .insert()
            .values(run_id=retry_run, importer="provider-directory-fhir", status="running", params={}, metrics={})
        )
    with patch.object(fhir, "_provider_directory_profile_replay_source_context", mutable_context):
        replayed_result = await fhir._publish_attested_provider_directory_profile(
            run_id=retry_run, control_run_id=retry_run, execution=execution, metrics={}
        )
    assert replayed_result["profile"]["committed_replay"]["status"] == "superseded"
    assert replayed_result["profile"]["committed_replay"]["replayed_by_run_id"] == retry_run
    assert replayed_result["profile"]["committed_replay"]["current_generation_id"] == successor
    return before


async def _exercise_signed_replay(monkeypatch):
    """Keep authority contexts active across publication, drift refusal, and historical replay."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with fixture._database(monkeypatch) as engine:
        database, cutover, stages = await cutover_test._cutover_fixture(engine, monkeypatch, _authority)
        admission, execution = cutover["admission"], cutover["execution"]
        monkeypatch.setattr(selection, "db", database)
        await _register(database, fixture._SCHEMA, execution)
        current_selection = AsyncMock(side_effect=AssertionError("historical replay consulted current selection"))
        preparation = AsyncMock(side_effect=AssertionError("historical replay prepared datasets"))
        monkeypatch.setattr(fhir, "assert_registered_profile_selection_current", current_selection)
        monkeypatch.setattr(fhir, "_attested_profile_publication_fence", preparation)
        mutable_context = AsyncMock(side_effect=AssertionError("historical replay consulted current source context"))
        monkeypatch.setattr(fhir.profile_capacity_runtime, "configured_capacity_lease_trust", lambda: cutover["trust"])
        # Dataset scalar admission is a separate fixture boundary; common publication SQL is real.
        monkeypatch.setattr(fhir, "_is_provider_directory_dataset_cutover_committed", AsyncMock(return_value=True))
        token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
        execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
        fence = fhir.ProviderDirectoryArtifactDatasetFence(())
        try:
            await _publish_initial_fixture(database, cutover, stages, admission, execution, fence)
            await _assert_current_replay(database, cutover, stages, admission, execution, fence, mutable_context)
            before = await _assert_superseded_replay(
                database, engine, cutover, admission, execution, fence, mutable_context
            )
            current_selection.assert_not_awaited()
            preparation.assert_not_awaited()
            mutable_context.assert_not_awaited()
            assert before == await database.first(
                f'SELECT generation_id,xmin::text AS xmin FROM "{fixture._SCHEMA}".provider_directory_profile_serving_generation'
            )
            assert await database.scalar(f'SELECT count(*) FROM "{fixture._SCHEMA}".{contract.RECEIPT_TABLE}') == 1
            assert (
                await database.scalar(
                    f'SELECT count(*) FROM "{fixture._SCHEMA}".provider_directory_profile_delta_receipt'
                )
                == 1
            )
        finally:
            fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
            fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


def test_native_signed_initial_receipt_replay(monkeypatch):
    """Exercise current and cross-run superseded replay through the public publication entry."""
    asyncio.run(_exercise_signed_replay(monkeypatch))


def _consume_replay_authority(envelope, trust, accepted, geometry, consumed_by_field, preflight, receipt):
    """Verify the original synthetic signature and bind its unchanged consumption and receipt fields."""
    verified = leases.verify_database_capacity_lease(
        envelope,
        trust=trust,
        now=accepted,
        expected_capacity_geometry_hash=capacity.capacity_geometry_hash(geometry),
        expected_database_name=geometry.database_name,
        expected_database_oid=geometry.database_oid,
        expected_database_system_identifier=geometry.database_system_identifier,
    )
    binding = fhir.CapacityLeaseConsumptionBinding(
        run_id=consumed_by_field["run_id"],
        build_id=consumed_by_field["build_id"],
        executable_plan_hash=geometry.executable_plan_hash,
        selection_proof_id=geometry.selection_proof_id,
        source_vector_hash=geometry.desired_source_vector_hash,
        source_context_vector_hash=geometry.desired_context_vector_hash,
        profile_as_of=geometry.profile_as_of,
    )
    consumed_by_field = dict(fhir.capacity_lease_consumption_values(verified, binding, accepted_at=accepted))
    preflight.update(
        receipt_sha256=verified.nonce,
        request_sha256=receipt["request_sha256"],
        request_nonce=receipt["request_nonce"],
        control_plane_receipt_sha256=receipt["control_plane_receipt_sha256"],
        issued_at=accepted,
        expires_at=verified.expires_at,
        consumed_at=accepted,
        consumed_run_id=consumed_by_field["run_id"],
        consumed_attestation_id=verified.attestation_id,
        capacity_geometry_hash=capacity.capacity_geometry_hash(geometry),
        receipt_json=json.dumps(receipt),
    )
    return verified, consumed_by_field


async def _bind_replay_geometry(database, schema, geometry, execution, projection):
    """Bind canonical selected vectors and observed target layouts before signing fixture authority."""
    initial_targets = contract.InitialTargets(geometry.evidence_target_oid, geometry.profile_target_oid, {})
    database_identity = await fhir._provider_directory_profile_capacity_database_identity(schema, initial_targets)
    geometry = replace(
        geometry,
        **{
            key: context_value
            for key, context_value in vars(database_identity).items()
            if key in vars(geometry) and key not in {"evidence_target_oid", "profile_target_oid"}
        },
    )
    layout = await initial.receipt_layout(fhir, schema)
    async with database.transaction():
        target_snapshot = (await initial.capture_targets(fhir, schema)).payload
    source_pairs = (("cms-npd", "dataset"),)
    contexts = (
        (
            "cms-npd",
            fhir._source_context_digest(
                [
                    {
                        "source_id": "cms-npd",
                        "endpoint_id": "endpoint",
                        "canonical_api_base": None,
                        "org_name": "Synthetic directory",
                        "plan_name": None,
                        "authority_id": None,
                    }
                ]
            ),
        ),
    )
    geometry = replace(
        geometry,
        profile_as_of=execution.attestation.desired_profile_as_of,
        selection_proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        profile_schema_version=execution.attestation.profile_schema_version,
        profile_strategy_version=execution.attestation.profile_strategy_version,
        desired_source_vector_hash=fhir._provider_directory_profile_source_vector_hash(source_pairs),
        desired_context_vector_hash=fhir._provider_directory_profile_source_context_vector_hash(contexts),
        sql_contract_digest=fhir._provider_directory_profile_sql_contract_digest(),
        initial_target_state_sha256=contract.target_state_sha256(target_snapshot),
        initial_receipt_storage_fingerprint=layout.exact_fingerprint,
    )
    projection = replace(projection, capacity_geometry_hash=capacity.capacity_geometry_hash(geometry))
    return database_identity, geometry, source_pairs, contexts, target_snapshot, projection


async def _commit_replay_successor(
    database, schema, delta, counts_by_field, forecast_hash, forecast_by_field, execution
):
    """Commit the same real source-delta transition and common receipt in one transaction."""
    async with database.transaction() as connection:
        prior = await common.receipts.read_current_receipt(connection, schema)
        before = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
        await fhir._update_profile_delta_serving_generation(
            delta, counts_by_field, SimpleNamespace(forecast_hash=forecast_hash)
        )
        after = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")
        wal = int(
            await database.scalar(
                "SELECT pg_wal_lsn_diff(CAST(CAST(:after AS text) AS pg_lsn),CAST(CAST(:before AS text) AS pg_lsn))",
                after=after,
                before=before,
            )
        )
        actual_by_field = {
            "cutover_forecast_hash": forecast_hash,
            "cutover_forecast_json": json.dumps(forecast_by_field),
            "cutover_actual_hash": fhir._identity_hash({"before": before, "after": after}),
            "cutover_actual_json": json.dumps({"before": before, "after": after}),
            "wal_start_lsn": before,
            "wal_observed_lsn": after,
            "cutover_wal_bytes": wal,
            **{
                key: 0
                for key in (
                    "evidence_target_bytes_before",
                    "evidence_target_bytes_after",
                    "evidence_target_growth_bytes",
                    "profile_target_bytes_before",
                    "profile_target_bytes_after",
                    "profile_target_growth_bytes",
                )
            },
        }
        await fhir._insert_profile_delta_receipt(
            delta, f'"{schema}".provider_directory_profile_delta_receipt', counts_by_field, actual_by_field
        )
        receipt_payload = await common._payload(connection, schema, prior)
        receipt_payload["selection"] = {
            "proof_id": execution.attestation.proof_id,
            "fingerprint": execution.attestation.selection_fingerprint,
            "catalog_digest": execution.attestation.catalog_digest,
        }
        await common.receipts.append_serving_receipt(connection, schema, receipt_payload)
        await database.status("SET CONSTRAINTS ALL IMMEDIATE")


def _successor_delta(schema, build_id, generation, old, execution, geometry):
    """Construct the unchanged source-delta descriptor for the replay successor."""
    return _prepared_delta(
        schema=schema,
        build_id=build_id,
        generation_id=generation,
        from_generation_id=old.generation_id,
        selection_proof_id=execution.attestation.proof_id,
        control_generation=8,
        authority_revision=execution.attestation.authority_revision,
        from_source_vector_hash=old.source_vector_hash,
        to_source_vector_hash=old.source_vector_hash,
        to_source_vector=old.source_vector,
        from_source_context_vector_hash=old.source_context_vector_hash,
        to_source_context_vector_hash=old.source_context_vector_hash,
        to_source_context_vector=old.source_context_vector,
        evidence_target_oid=old.evidence_target_oid,
        profile_target_oid=old.profile_target_oid,
        profile_as_of=geometry.profile_as_of,
        from_profile_as_of=old.profile_as_of,
        executable_plan_hash=geometry.executable_plan_hash,
        from_capacity_geometry_status="verified",
        from_capacity_geometry_hash=old.capacity_geometry_hash,
        from_capacity_geometry_json=old.capacity_geometry_json,
        capacity_geometry_hash=capacity.capacity_geometry_hash(geometry),
        capacity_geometry_json=capacity.canonical_capacity_geometry_json(geometry),
    )
