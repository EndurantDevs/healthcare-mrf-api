# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plain maintenance preserves signed initial identity and independently funded disposal."""

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import MetaData

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_failed_cleanup as cleanup
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from tests import test_provider_directory_profile_failed_cleanup_postgres as common
from tests.cms_npd_admission_postgres_support import admission_database, fhir
from tests.cms_npd_admission_postgres_support import cms_admission_template as cms_admission_template
from tests.test_provider_directory_profile_failed_cleanup import sign
from tests.test_provider_directory_profile_initial_cleanup import BUILD, OWNER, _original_authority


async def _dead_tail(database):
    """Allocate and delete a bounded tail while preserving the authentic incumbent row."""
    await database.status(
        """INSERT INTO mrf.provider_directory_profile
        SELECT npi+g, jsonb_build_object('padding',md5(g::text)||md5((g+1)::text)),evidence_json,
               source_ids,endpoint_ids,dataset_ids,source_count,independent_source_count,fact_count,generation_id,published_at
          FROM mrf.provider_directory_profile CROSS JOIN generate_series(1,1000) g WHERE npi=1234567890"""
    )
    await database.status("DELETE FROM mrf.provider_directory_profile WHERE npi<>1234567890")


async def _signed_failed_initial(database, monkeypatch, *, fault=None):
    names, _target = await common._seed_initial_cleanup(database, legacy=True)
    await _dead_tail(database)
    async with database.transaction():
        original = (await initial.capture_targets(fhir, "mrf")).payload
    raw = (await database.first("SELECT capacity_geometry_json FROM mrf.provider_directory_profile_build_checkpoint"))[
        0
    ]
    geometry = capacity.validated_capacity_geometry(raw)
    geometry, checkpoint, owner, consumed, trust = _original_authority(geometry, original)
    # Faults are retained evidence errors, never an override of the real verifier.
    match fault:
        case "signature":
            consumed["signature"] = "A" * 86
        case "database":
            consumed["database_oid"] += 1
        case "source":
            consumed["source_vector_hash"] = "ff" * 32
        case "owner":
            consumed["run_id"] = "run_" + "1" * 32
        case "geometry":
            geometry = replace(geometry, profile_input_digest="ff" * 32)
    await database.status(
        """UPDATE mrf.provider_directory_profile_build_checkpoint SET
        profile_as_of=:date,executable_plan_hash=:plan,desired_source_vector_hash=:source,
        desired_source_context_vector_hash=:context,capacity_geometry_hash=:hash,
        capacity_geometry_json=CAST(:geometry AS jsonb)""",
        date=checkpoint["profile_as_of"],
        plan=checkpoint["executable_plan_hash"],
        source=checkpoint["desired_source_vector_hash"],
        context=checkpoint["desired_source_context_vector_hash"],
        hash=capacity.capacity_geometry_hash(geometry),
        geometry=capacity.canonical_capacity_geometry_json(geometry),
    )
    await database.status(
        "UPDATE mrf.import_run SET params=CAST(:params AS json) WHERE run_id=:owner",
        params=cleanup.canonical(owner["params"]),
        owner=OWNER,
    )
    if fault != "missing":
        async with database.engine.begin() as connection:
            await connection.execute(
                fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__table__.to_metadata(MetaData(), schema="mrf")
                .insert()
                .values(**consumed)
            )
    monkeypatch.setattr(fhir.profile_capacity_runtime, "configured_capacity_lease_trust", lambda: trust)
    return names, original, consumed


async def _vacuum_and_assert_size_only(database, original):
    async with database.engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await connection.exec_driver_sql("VACUUM mrf.provider_directory_profile")
    async with database.transaction():
        current = (await initial.capture_targets(fhir, "mrf")).payload
    sizes = {"profile_target_bytes", "evidence_target_bytes"}
    assert {key: value for key, value in original.items() if key not in sizes} == {
        key: value for key, value in current.items() if key not in sizes
    }
    assert current["profile_target_bytes"] != original["profile_target_bytes"]
    return current


async def _cleanup_replay_state(database, names):
    """Retain complete owned fixture rows, target identity and disposed stage absence."""
    async with database.transaction():
        rows_by_relation = {}
        for relation_name, key_column in (
            ("provider_directory_profile_build_checkpoint", "build_id"),
            ("provider_directory_profile_failed_cleanup_claim", "operation_id"),
            ("provider_directory_profile_capacity_lease_consumption", "attestation_id"),
            ("import_run", "run_id"),
        ):
            rows_by_relation[relation_name] = [
                row[0]
                for row in await database.all(
                    f"SELECT row_to_json(c)::text FROM mrf.{relation_name} c ORDER BY {key_column}"
                )
            ]
        assert all(rows_by_relation.values())
        initial_receipt_count = await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_profile_initial_receipt"
        )
        target_by_field = (await initial.capture_targets(fhir, "mrf")).payload
        stage_identities = [
            await fhir._provider_directory_profile_stage_relation_identity("mrf", name) for name in names
        ]
    return rows_by_relation, initial_receipt_count, target_by_field, stage_identities


@pytest.mark.asyncio
@pytest.mark.parametrize("vacuum_after_inspection", [False, True])
async def test_native_vacuum_size_drift_uses_expired_original_and_fresh_cleanup(
    monkeypatch, vacuum_after_inspection, record_property
):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with admission_database(monkeypatch) as database:
        names, original, consumed = await _signed_failed_initial(database, monkeypatch)
        if not vacuum_after_inspection:
            current = await _vacuum_and_assert_size_only(database, original)
        envelope, trust = await common._fresh_fixture_authorization(database)
        assert envelope["authorization"]["initial"]["target_state"] == original
        assert consumed["expires_at"] < datetime.now(timezone.utc)
        assert envelope["authorization"]["reservation_id"] != consumed["reservation_id"]
        if vacuum_after_inspection:
            current = await _vacuum_and_assert_size_only(database, original)
        record_property("original_profile_bytes", original["profile_target_bytes"])
        record_property("maintained_profile_bytes", current["profile_target_bytes"])
        receipt = await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
        )
        assert len(receipt["disposed_stages"]) == 2
        assert receipt["disposed_stages"] == envelope["authorization"]["stages"]
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is None for name in names]
        )
        async with database.transaction():
            assert (await initial.capture_targets(fhir, "mrf")).payload == current
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_initial_receipt") == 0
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 1
        # Completed spent authority reconciles without requiring expired build funding again.
        before = await _cleanup_replay_state(database, names)
        assert receipt == await cleanup.execute_failed_profile_cleanup(
            fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
        )
        assert await _cleanup_replay_state(database, names) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,error",
    [
        ("missing", "replay_consumption_missing"),
        ("owner", "replay_consumption_missing"),
        ("signature", "replay_capacity_invalid"),
        ("database", "replay_capacity_consumption_changed"),
        ("geometry", "replay_capacity_invalid"),
        ("source", "replay_capacity_consumption_changed"),
    ],
)
async def test_native_size_drift_refuses_invalid_original_before_claim_or_drop(monkeypatch, fault, error):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with admission_database(monkeypatch) as database:
        names, original, _consumed = await _signed_failed_initial(database, monkeypatch, fault=fault)
        # The exact-hash path remains usable without new historical dependencies.
        envelope, trust = await common._fresh_fixture_authorization(database)
        await _vacuum_and_assert_size_only(database, original)
        with pytest.raises(RuntimeError, match=error + "$"):
            await cleanup.inspect_failed_profile_cleanup(fhir, build_id=BUILD, owner_run_id=OWNER)
        with pytest.raises(RuntimeError, match=error + "$"):
            await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
            )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None for name in names]
        )


@pytest.mark.asyncio
async def test_native_size_drift_does_not_reuse_expired_cleanup_funding(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with admission_database(monkeypatch) as database:
        names, original, _consumed = await _signed_failed_initial(database, monkeypatch)
        await _vacuum_and_assert_size_only(database, original)
        envelope, trust = await common._fresh_fixture_authorization(database)
        body = copy.deepcopy(envelope["authorization"])
        for field in ("issued_at", "expires_at", "max_operation_deadline"):
            body[field] = (cleanup.timestamp(body[field]) - timedelta(minutes=20)).isoformat()
        expired = sign(body, common.authorization_fixture()[2])
        with pytest.raises(RuntimeError, match="authorization_expired$"):
            await cleanup.execute_failed_profile_cleanup(
                fhir, expired, cleanup_trust=trust, executor_identity=body["executor_identity"]
            )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None for name in names]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,error", [("layout", "initial_target_changed"), ("rows", "serving_adoption_target_invalid")]
)
async def test_native_size_exception_keeps_live_target_guards(monkeypatch, fault, error):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with admission_database(monkeypatch) as database:
        names, original, _consumed = await _signed_failed_initial(database, monkeypatch)
        await _vacuum_and_assert_size_only(database, original)
        envelope, trust = await common._fresh_fixture_authorization(database)
        if fault == "layout":
            await database.status("ALTER TABLE mrf.provider_directory_profile ADD COLUMN unexpected integer")
        else:
            await database.status("DELETE FROM mrf.provider_directory_profile WHERE npi=1234567890")
        with pytest.raises(RuntimeError, match=error + "$"):
            await cleanup.execute_failed_profile_cleanup(
                fhir, envelope, cleanup_trust=trust, executor_identity=envelope["authorization"]["executor_identity"]
            )
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_profile_failed_cleanup_claim") == 0
        assert all(
            [await fhir._provider_directory_profile_stage_relation_identity("mrf", name) is not None for name in names]
        )
