# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native truth checks for bounded, atomic target replacement and replay."""

import copy
import json
import os
from dataclasses import replace

import pytest

from db.connection import Database
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from process.provider_directory_profile_capacity_cutover import _lsn_bytes
from tests.provider_directory_profile_artifact_pg_fixtures import _configure_database, _incompressible_text
from tests.provider_directory_profile_artifact_pg_support import (
    _admission,
    _artifact_fixture,
    _capacity_geometry,
    _materialize_two_worker_artifact_scope,
    _project_scope_budget,
    _scope_row_count,
)
from tests.provider_directory_profile_delta_publication import (
    _delta_capacity_admission,
    _prepared_scenario_delta,
    _publish_prepared_delta,
)
from tests.provider_directory_profile_delta_scenario import (
    _delta_capacity_context,
    _delta_lineage,
    _delta_relation_oid_by_name,
    _delta_relation_scenario,
    _insert_delta_checkpoint,
    _insert_delta_serving_generation,
)
from tests.provider_directory_profile_delta_test_support import (
    _bound_control_geometry,
    _delta_database,
    _insert_evidence,
)
from tests.test_provider_directory_profile_bounded_capacity import _authorized_recovery_build_ids, fhir
from tests.test_provider_directory_profile_capacity import _geometry_payload


def _assert_scratch_wal_tamper_rejected(receipt_by_field, geometry, run_id):
    forecast_by_field = receipt_by_field["cutover_forecast_json"]
    actual_by_field = receipt_by_field["cutover_actual_json"]
    forecast_by_field = json.loads(forecast_by_field) if isinstance(forecast_by_field, str) else forecast_by_field
    actual_by_field = json.loads(actual_by_field) if isinstance(actual_by_field, str) else actual_by_field
    inflated_actual_by_field = copy.deepcopy(actual_by_field)
    settled = inflated_actual_by_field["wal_ledger"]["settled_relation_wal_bytes"]
    observed = (
        _lsn_bytes(actual_by_field["wal_observed_lsn"])
        - _lsn_bytes(forecast_by_field["admission_wal_start_lsn"])
        + forecast_by_field["admission_wal_offset_bytes"]
    )
    settled["evidence_stage"] += observed - sum(settled.values()) + 1
    assert settled["evidence_stage"] <= next(
        cap.max_wal_bytes for cap in geometry.relation_byte_caps if cap.relation_name == "evidence_stage"
    )
    corrupt_by_field = dict(
        receipt_by_field,
        cutover_actual_json=inflated_actual_by_field,
        cutover_actual_hash=fhir._profile_cutover_hashes(forecast_by_field, inflated_actual_by_field)[1],
    )
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="receipt_semantics_invalid") as error:
        fhir._provider_directory_profile_cutover_receipt_identity(
            corrupt_by_field, geometry=geometry, expected_run_id=run_id
        )
    assert "cutover_settled_wal_exceeds_observed" in str(error.value.__cause__)


async def _wide_delta(database, schema):
    scenario = await _delta_relation_scenario(database, schema)
    for table, key, dataset in (
        (scenario.evidence_target_ref, "e", "dataset-a-old"),
        (scenario.evidence_stage_ref, "d", "dataset-a-new"),
    ):
        await _insert_evidence(
            database,
            table,
            evidence_key=key * 32,
            fact_type="contact",
            source_id="source-a",
            dataset_id=dataset,
            value_json={"value": _incompressible_text(key)},
        )
    await database.status(f"TRUNCATE {scenario.profile_stage_ref};")
    await database.status(
        profile.profile_delta_insert_sql(
            current_evidence_ref=scenario.evidence_target_ref,
            delta_evidence_ref=scenario.evidence_stage_ref,
            affected_npi_ref=scenario.affected_ref,
            target_ref=scenario.profile_stage_ref,
        ),
        refresh_and_removed_source_ids=["source-a"],
        retained_source_ids=["source-a", "source-b"],
        profile_as_of="2026-07-30",
        generation_id=scenario.new_generation,
    )
    return scenario


async def _prepare(database, scenario):
    lineage = _delta_lineage()
    oids = await _delta_relation_oid_by_name(database, scenario)
    seed = capacity.validated_capacity_geometry(
        _geometry_payload(
            evidence_target_oid=oids["evidence_target"],
            profile_target_oid=oids["profile_target"],
        )
    )
    await _insert_delta_checkpoint(database, scenario, lineage, oids, seed)
    await _insert_delta_serving_generation(database, scenario, lineage, oids)
    await database.status(f"UPDATE {scenario.serving_ref} SET evidence_rows = 3;")
    context = await _delta_capacity_context(database, scenario, lineage, oids)
    payload = capacity.capacity_geometry_payload(context.geometry)
    payload.update(physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID, artifact_scope_batch_size=1)
    context.geometry, context.control_projection = _bound_control_geometry(payload)
    await database.status(
        f"UPDATE {scenario.checkpoint_ref} SET capacity_geometry_hash = :hash, "
        "capacity_geometry_json = CAST(:geometry AS jsonb) WHERE build_id = :build;",
        hash=capacity.capacity_geometry_hash(context.geometry),
        geometry=capacity.canonical_capacity_geometry_json(context.geometry),
        build=lineage.build_id,
    )
    delta = replace(_prepared_scenario_delta(scenario, lineage, oids, context.geometry), expected_evidence_rows=3)
    admission = await _delta_capacity_admission(database, lineage, context)
    return delta, admission


def _configure(monkeypatch):
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN", "")
    if not dsn:
        pytest.skip("Bounded capacity checks need disposable PostgreSQL 18")
    _configure_database(monkeypatch, dsn)


async def _assert_bounded_receipt_replay(database, schema, delta, admission):
    """Check exact bounded accounting and replay before rejecting receipt tampering."""
    receipt = fhir._pagination_checkpoint_row_mapping(
        await database.first(
            f"SELECT * FROM {profile.qualified_table(schema, 'provider_directory_profile_delta_receipt')} "
            "WHERE build_id = :build;",
            build=delta.build_id,
        )
    )
    forecast = receipt["cutover_forecast_json"]
    actual = receipt["cutover_actual_json"]
    forecast = json.loads(forecast) if isinstance(forecast, str) else forecast
    actual = json.loads(actual) if isinstance(actual, str) else actual
    assert forecast["contract_id"] == capacity.BOUNDED_CUTOVER_FORECAST_CONTRACT_ID
    assert actual["contract_id"] == capacity.BOUNDED_CUTOVER_ACTUAL_CONTRACT_ID
    assert forecast["admission_wal_start_lsn"] == admission.initial_wal_lsn
    assert actual["target_windows"]["evidence_target"]["window_count"] == 4
    assert actual["target_windows"]["evidence_target"]["deleted_toast_chunks"] > 0
    assert actual["target_windows"]["evidence_target"]["inserted_toast_chunks"] > 0
    for name in ("evidence_target", "profile_target"):
        summary = actual["target_windows"][name]
        layout = forecast[name + "_layout"]
        for field in ("inserted_toast_chunks", "deleted_toast_chunks"):
            assert summary[field] == layout[field]
        projected = next(
            target_projection
            for target_projection in forecast["target_projection"]["targets"]
            if target_projection["relation_name"] == name
        )
        assert summary["deleted_logical_bytes"] == projected["deleted_logical_bytes"]
        prefix = name.removesuffix("_target")
        assert summary["inserted_rows"] == receipt[prefix + "_inserted"]
        assert summary["deleted_rows"] == receipt[prefix + "_deleted"]
    await fhir._promote_provider_directory_artifact_bundle_transaction((), profile_delta=delta)
    _assert_scratch_wal_tamper_rejected(receipt, admission.geometry, admission.run_id)
    corrupt_by_field = dict(receipt)
    corrupt_by_field["cutover_actual_json"] = {**actual, "contract_id": capacity.CUTOVER_ACTUAL_CONTRACT_ID}
    with pytest.raises(RuntimeError):
        fhir._provider_directory_profile_cutover_receipt_identity(corrupt_by_field, geometry=admission.geometry)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_bounded_target_windows_are_atomic_and_replay_exactly(monkeypatch, failure):
    """Readers retain the prior generation until every bounded window commits atomically."""
    _configure(monkeypatch)
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        scenario = await _wide_delta(database, schema)
        delta, admission = await _prepare(database, scenario)
        retained = await database.scalar(
            f"SELECT row_to_json(target)::text FROM {scenario.evidence_target_ref} "
            "AS target WHERE source_id = 'source-b';"
        )
        original_rows = await database.scalar(
            f"SELECT jsonb_agg(target ORDER BY evidence_key)::text FROM {scenario.evidence_target_ref} AS target;"
        )
        observer = Database()
        await observer.connect()
        original_status = database.status
        windows = []

        async def status(statement, **params):
            changed = await original_status(statement, **params)
            if statement.startswith(f"DELETE FROM {scenario.evidence_target_ref} AS target WHERE"):
                windows.append(changed)
                visible = await observer.scalar(
                    f"SELECT jsonb_agg(target ORDER BY evidence_key)::text "
                    f"FROM {scenario.evidence_target_ref} AS target;"
                )
                assert visible == original_rows
                assert (
                    await observer.scalar(f"SELECT generation_id FROM {scenario.serving_ref};")
                    == scenario.old_generation
                )
                if failure and len(windows) == 2:
                    raise RuntimeError("synthetic_window_failure")
            return changed

        monkeypatch.setattr(database, "status", status)
        try:
            if failure:
                await _assert_target_window_rollback(delta, admission, observer, scenario, original_rows)
                return
            await _publish_prepared_delta(delta, admission)
            assert windows == [1, 1]
            assert (
                await database.scalar(
                    f"SELECT row_to_json(target)::text FROM {scenario.evidence_target_ref} "
                    "AS target WHERE source_id = 'source-b';"
                )
                == retained
            )
            assert await database.scalar(f"SELECT count(*) FROM {scenario.evidence_target_ref};") == 3
            assert (
                await database.scalar(f"SELECT generation_id FROM {scenario.serving_ref};") == scenario.new_generation
            )
            assert await fhir._is_provider_directory_profile_delta_committed(delta)
            await _assert_bounded_receipt_replay(database, schema, delta, admission)
        finally:
            await observer.disconnect()


@pytest.mark.asyncio
async def test_bounded_artifact_windows_settle_below_the_whole_future_wal_forecast(monkeypatch):
    async with _artifact_fixture(monkeypatch) as fixture:
        base, growth, whole_wal, _ = await _project_scope_budget(fixture)
        legacy = _capacity_geometry(fixture, artifact_scratch_cap=base + growth, artifact_wal_cap=whole_wal - 1)
        start = str(await fixture.database.scalar("SELECT pg_current_wal_insert_lsn()::text;"))
        original = _admission(legacy, start)
        token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(original)
        try:
            with pytest.raises(RuntimeError, match="artifact_wal_projected"):
                await fhir._preflight_provider_directory_artifact_scope_capacity(
                    fixture.schema,
                    fixture.relation_by_table,
                    fixture.projection,
                )
        finally:
            fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
        assert await _scope_row_count(fixture) == 0
        bounded = replace(legacy, physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID)
        projection = capacity.project_profile_control_wal_capacity(bounded, original.control_wal_projection.plan_input)
        bounded = replace(
            bounded,
            control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
            control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
        )
        admission = _admission(bounded, start)
        token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
        try:
            await fhir._preflight_provider_directory_artifact_scope_capacity(
                fixture.schema,
                fixture.relation_by_table,
                fixture.projection,
            )
            await _materialize_two_worker_artifact_scope(fixture)
            await fhir._assert_provider_directory_artifact_scope_observed_capacity(
                fixture.schema,
                fixture.relation_by_table,
                fixture.projection,
            )
        finally:
            fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
        assert await _scope_row_count(fixture) == fixture.projection.projected_rows
        assert len(admission.wal_tracker.relation_refs_by_class["artifact_scope"]) == 9
        assert not admission.wal_tracker.pending_relation_wal_bytes
        assert not admission.wal_tracker.pending_control_wal_bytes
        assert admission.wal_tracker.accounted_relation_wal_bytes["artifact_scope"] < whole_wal


@pytest.mark.asyncio
async def test_bounded_build_restart_cannot_erase_a_prior_consumption(monkeypatch):
    _configure(monkeypatch)
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        consumption = profile.qualified_table(schema, "provider_directory_profile_capacity_lease_consumption")
        await database.status(f"CREATE TABLE {consumption} (build_id text, admission_purpose text);")
        old_build, new_build = _authorized_recovery_build_ids()
        await database.status(f"INSERT INTO {consumption} VALUES (:build, 'profile');", build=old_build)
        with pytest.raises(RuntimeError, match="prior_capacity_charge_reconstruction_unsupported"):
            await fhir._assert_profile_capacity_build_unconsumed(old_build)
        await fhir._assert_profile_capacity_build_unconsumed(new_build)
        assert await database.scalar(f"SELECT count(*) FROM {consumption};") == 1


async def _assert_target_window_rollback(delta, admission, observer, scenario, original_rows):
    """Keep every rollback assertion inside the owner transaction and observer lifetime."""
    with pytest.raises(RuntimeError, match="synthetic_window_failure"):
        await _publish_prepared_delta(delta, admission)
    assert (
        await observer.scalar(
            f"SELECT jsonb_agg(target ORDER BY evidence_key)::text FROM {scenario.evidence_target_ref} AS target;"
        )
        == original_rows
    )
    assert await observer.scalar(f"SELECT generation_id FROM {scenario.serving_ref};") == scenario.old_generation
    assert admission.wal_tracker.unresolved_window
    assert admission.wal_tracker.pending_relation_wal_bytes["evidence_target"] > 0
    assert admission.wal_tracker.accounted_relation_wal_bytes["evidence_target"] > 0
    assert admission.wal_tracker.pending_metadata_wal_bytes > 0
