# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Forward migration preserves raw authority history and native semantic guards."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError

from process import provider_directory_profile_initial_guards as guards
from process.provider_directory_cms_receipt_guard import named_profile_transition_body
from tests.test_provider_directory_cms_serving_receipt_postgres import _advance_native, _database, _migration
from tests.test_provider_directory_profile_capacity_attestation import _historical_envelope
from tests.test_provider_directory_profile_capacity_preflight_postgres import _receipt_values
from tests.test_provider_directory_profile_initial_migration import _migration as _load_revision

REVISION = "20261116090000_profile_capacity_logical_names"
logical = _load_revision(REVISION + ".py")


def test_forward_sql_preserves_history_and_existing_guard_abis(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "example_results")
    monkeypatch.setenv("DB_SCHEMA", "example_results")
    output = StringIO()
    with Operations.context(
        MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    ):
        logical.upgrade()
    sql = output.getvalue()
    assert logical.down_revision == "20261115120000_named_output_recovery_document"
    assert sql.index("LOCK TABLE") < sql.index("ADD COLUMN targets_json") < sql.index("DROP COLUMN")
    assert "UPDATE " not in sql and "DELETE FROM" not in sql and "DISABLE TRIGGER" not in sql
    assert "CREATE TRIGGER" not in sql and "CREATE FUNCTION" not in sql and "REFERENCES" not in sql
    assert "CREATE OR REPLACE FUNCTION" in sql
    assert "DROP FUNCTION" not in sql and "DROP TABLE" not in sql
    assert logical._receipt_matches_sql("example_results") == guards._receipt_matches_sql("example_results")
    assert logical.named_profile_transition_body('"example_results"') == named_profile_transition_body(
        '"example_results"'
    )
    assert "evidence_target_oid" not in logical._receipt_matches_sql("example_results").split(" - ARRAY")[0]
    with pytest.raises(RuntimeError, match="forward_migration"):
        logical.downgrade()


def test_migration_refuses_conflicting_schema_before_sql(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "example_results")
    monkeypatch.setenv("DB_SCHEMA", "other_results")
    with pytest.raises(RuntimeError, match="must match"):
        logical.upgrade()


def _apply(connection, function):
    with Operations.context(MigrationContext.configure(connection)):
        function()


def _install_history_tables(connection, schema):
    checkpoint = _migration("20260720120000")
    delta = _migration("20260730110000")
    preflight = _migration("20260811010000")
    initial = _migration("20261001110000")
    with Operations.context(MigrationContext.configure(connection)):
        connection.exec_driver_sql(f'CREATE TABLE "{schema}".import_run (run_id varchar(64) PRIMARY KEY)')
        checkpoint.upgrade()
        delta._add_checkpoint_columns(schema)
        delta._create_capacity_lease_consumption_table(schema)
        connection.exec_driver_sql(
            f'ALTER TABLE "{schema}".{logical._CONSUMPTION} DROP CONSTRAINT pd_profile_capacity_consumption_values_check'
        )
        preflight._add_capacity_constraint(
            schema,
            "pd_profile_capacity_consumption_values_check",
            preflight._capacity_consumption_check(preflight._capacity_contract_predicate(preflight._V3_CONTRACT)),
        )
        _migration("20260930110000").upgrade()
        preflight._create_table(schema)
        preflight._create_guards(schema)
        initial._create_receipt(schema)
        initial._create_deferred_guards(schema)


async def _insert(connection, schema, table, values):
    by_field = dict(values)
    expressions = []
    for name, value in by_field.items():
        if isinstance(value, (dict, list)):
            by_field[name] = json.dumps(value)
            expressions.append(f"CAST(:{name} AS jsonb)")
        else:
            expressions.append(":" + name)
    await connection.execute(
        sa.text(f'INSERT INTO "{schema}".{table} ({",".join(by_field)}) VALUES ({",".join(expressions)})'), by_field
    )


def _consumed_values(*, current=False):
    envelope = _historical_envelope()
    lease = envelope["lease"]
    now = datetime.now(timezone.utc) if current else datetime(2026, 7, 30, 12, 0, 2, tzinfo=timezone.utc)
    by_field = {
        name: lease[name]
        for name in (
            "attestation_id",
            "reservation_id",
            "capacity_geometry_hash",
            "contract_id",
            "key_id",
            "environment_id",
            "attestor_id",
            "attestor_release_digest",
        )
    }
    by_field.update(
        lease_digest="1" * 64,
        executable_plan_hash="2" * 64,
        selection_proof_id="3" * 64,
        source_vector_hash="4" * 64,
        source_context_vector_hash="5" * 64,
        run_id="run_" + "6" * 32,
        build_id="pdpb_" + "7" * 32,
        profile_as_of="2026-07-30",
        public_key_fingerprint="8" * 64,
        signature=envelope["signature"],
        canonical_lease_json=json.dumps(lease, sort_keys=True, separators=(",", ":")),
        observed_at=now - timedelta(seconds=2),
        issued_at=now - timedelta(seconds=1),
        accepted_at=now,
        recorded_at=now,
        expires_at=now + timedelta(seconds=600),
        max_build_deadline=now + timedelta(seconds=300),
        admission_purpose="profile",
    )
    if current:
        by_field["contract_id"] = "provider-directory-database-capacity-lease-v4"
    else:
        by_field.update(
            {name: lease[name] for name in logical._DROP_COLUMNS[logical._CONSUMPTION] if name.startswith("database_")}
        )
        by_field.update(tablespace_identity_hash="9" * 64, volume_identity_hash="a" * 64)
    return by_field


def _serving_values(consumed, schema, *, current=False):
    by_field = {
        "singleton_key": "global",
        "status": "published",
        "operation": "publish",
        "control_generation": 1,
        "generation_id": "pdprofile_" + "b" * 32,
        "selection_proof_id": consumed["selection_proof_id"],
        "authority_revision": 1,
        "profile_schema_version": 1,
        "profile_strategy_version": "source-fact-role32-org32-member32-dataset-graph8-auth-npi5m-v6",
        "source_vector_hash": consumed["source_vector_hash"],
        "source_vector_json": [],
        "source_context_vector_hash": consumed["source_context_vector_hash"],
        "source_context_vector_json": [],
        "executable_plan_hash": consumed["executable_plan_hash"],
        "capacity_geometry_status": "verified",
        "capacity_geometry_hash": consumed["capacity_geometry_hash"],
        "capacity_geometry_json": {"retained": "unchanged"},
        "evidence_rows": 0,
        "profile_rows": 0,
        "profile_as_of": consumed["profile_as_of"],
        "published_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    if current:
        by_field["targets_json"] = logical._named_targets(schema)
    else:
        by_field.update(evidence_target_oid=11, profile_target_oid=12)
    return by_field


def _delta_values(consumed):
    by_field = {
        name: "a" * 64
        for name in (
            "executable_plan_hash",
            "from_source_vector_hash",
            "to_source_vector_hash",
            "from_source_context_vector_hash",
            "to_source_context_vector_hash",
            "selection_proof_id",
            "capacity_geometry_hash",
            "cutover_forecast_hash",
            "cutover_actual_hash",
        )
    }
    by_field.update(
        {
            name: 0
            for name in (
                "evidence_rows",
                "profile_rows",
                "evidence_inserted",
                "evidence_deleted",
                "profile_inserted",
                "profile_deleted",
                "cutover_wal_bytes",
                "evidence_target_bytes_before",
                "evidence_target_bytes_after",
                "evidence_target_growth_bytes",
                "profile_target_bytes_before",
                "profile_target_bytes_after",
                "profile_target_growth_bytes",
            )
        }
    )
    by_field.update(
        build_id=consumed["build_id"],
        from_capacity_geometry_status="legacy_unavailable",
        capacity_geometry_status="verified",
        capacity_geometry_json={"retained": "delta"},
        from_generation_id="pdprofile_" + "c" * 32,
        generation_id="pdprofile_" + "b" * 32,
        operation="publish",
        profile_as_of=consumed["profile_as_of"],
        control_generation=1,
        authority_revision=1,
        evidence_target_oid=11,
        profile_target_oid=12,
        cutover_forecast_json={},
        cutover_actual_json={},
        cutover_wal_start_lsn="0/1",
        cutover_wal_observed_lsn="0/1",
        committed_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    return by_field


async def _raw_history(connection, schema):
    lease = (
        await connection.execute(
            sa.text(f'SELECT canonical_lease_json,signature FROM "{schema}".{logical._CONSUMPTION}')
        )
    ).one()
    geometry = await connection.scalar(
        sa.text(f'SELECT capacity_geometry_json::text FROM "{schema}".{logical._SERVING}')
    )
    delta_geometry = await connection.scalar(
        sa.text(f'SELECT capacity_geometry_json::text FROM "{schema}".{logical._DELTA}')
    )
    return tuple(lease), geometry, delta_geometry


@pytest.mark.asyncio
async def test_native_migration_preserves_raw_history_and_immutable_rows(monkeypatch):
    """Forward DDL preserves signed bytes and immutable history while naming targets."""
    async with _database(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            await connection.run_sync(_install_history_tables, schema)
            consumed = _consumed_values()
            await connection.execute(
                sa.text(f'INSERT INTO "{schema}".import_run VALUES (:run)'), {"run": consumed["run_id"]}
            )
            await _insert(connection, schema, logical._CONSUMPTION, consumed)
            await _insert(connection, schema, logical._SERVING, _serving_values(consumed, schema))
            await _insert(connection, schema, logical._DELTA, _delta_values(consumed))
        async with engine.connect() as connection:
            before = await _raw_history(connection, schema)
        async with engine.begin() as connection:
            await connection.run_sync(_apply, logical.upgrade)
        async with engine.connect() as connection:
            assert await _raw_history(connection, schema) == before
            assert await connection.scalar(
                sa.text(f'SELECT targets_json FROM "{schema}".{logical._SERVING}')
            ) == logical._named_targets(schema)
            columns = (
                (
                    await connection.execute(
                        sa.text(
                            "SELECT column_name FROM information_schema.columns WHERE table_schema=:schema AND table_name=:table"
                        ),
                        {"schema": schema, "table": logical._CONSUMPTION},
                    )
                )
                .scalars()
                .all()
            )
            assert not set(logical._DROP_COLUMNS[logical._CONSUMPTION]) & set(columns)
        with pytest.raises(DBAPIError, match="immutable"):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f'UPDATE "{schema}".{logical._CONSUMPTION} SET canonical_lease_json=canonical_lease_json')
                )
        with pytest.raises(DBAPIError, match="immutable"):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f'UPDATE "{schema}".{logical._DELTA} SET capacity_geometry_json=capacity_geometry_json')
                )
        async with engine.begin() as connection:
            await _assert_named_native_snapshot(connection, schema)


async def _assert_named_native_snapshot(connection, schema):
    """A stored named edition matches; another name cannot reuse its receipt."""
    named_by_role = {
        "evidence": {"schema": "published_example", "name": "evidence_edition"},
        "profile": {"schema": "published_example", "name": "profile_edition"},
    }
    await connection.execute(
        sa.text(f'UPDATE "{schema}".{logical._SERVING} SET targets_json=CAST(:names AS jsonb)'),
        {"names": json.dumps(named_by_role)},
    )
    snapshot = await connection.scalar(sa.text(f'SELECT "{schema}".cms_serving_native_snapshot()'))
    assert snapshot["profile"]["evidence_target"] == {
        "schema_name": "published_example",
        "relation_name": "evidence_edition",
    }
    assert snapshot["profile"]["profile_target"] == {
        "schema_name": "published_example",
        "relation_name": "profile_edition",
    }
    async with connection.begin_nested() as nested:
        await _advance_native(connection, schema, doctors=True)
        snapshot = await connection.scalar(sa.text(f'SELECT "{schema}".cms_serving_native_snapshot()'))
        assert (
            await connection.scalar(
                sa.text(f'SELECT "{schema}".cms_serving_native_matches(CAST(:snapshot AS jsonb))'),
                {"snapshot": json.dumps(snapshot)},
            )
            is True
        )
        changed_snapshot = deepcopy(snapshot)
        changed_snapshot["profile"]["profile_target"]["relation_name"] = "other_edition"
        assert (
            await connection.scalar(
                sa.text(f'SELECT "{schema}".cms_serving_native_matches(CAST(:snapshot AS jsonb))'),
                {"snapshot": json.dumps(changed_snapshot)},
            )
            is False
        )
        await nested.rollback()


def _initial_authority(schema):
    consumed = _consumed_values(current=True)
    now = consumed["accepted_at"]
    geometry_by_field = {
        "contract_id": "healthporta.provider-directory-profile-initial-capacity-geometry.v2",
        "initial_target_state_sha256": "c" * 64,
    }
    serving = _serving_values(consumed, schema, current=True)
    serving["capacity_geometry_json"] = geometry_by_field
    preflight = _receipt_values(
        "logical-initial", issued_at=now - timedelta(seconds=1), expires_at=now + timedelta(seconds=600)
    )
    preflight.update(
        contract_id="healthporta.provider-directory-profile-capacity-preflight.v7",
        request_contract_id="healthporta.provider-directory-profile-capacity-preflight-request.v7",
        materialization_mode="full_swap",
        capacity_geometry_hash=consumed["capacity_geometry_hash"],
        consumed_at=now,
        consumed_run_id=consumed["run_id"],
        consumed_attestation_id=consumed["attestation_id"],
    )
    signed_receipt_by_field = {
        "profile_materialization": "initial_full_swap",
        "profile_execution_identity": {"materialization_mode": "full_swap"},
        "capacity_geometry": geometry_by_field,
        "serving_generation_preflight_sha256": geometry_by_field["initial_target_state_sha256"],
    }
    preflight["receipt_json"] = json.dumps(signed_receipt_by_field)
    consumed["canonical_lease_json"] = json.dumps(
        {
            "nonce": preflight["receipt_sha256"],
            "signing_preflight_guard": {
                "healthcare_request": {
                    "contract_id": preflight["request_contract_id"],
                    "profile_materialization": "initial_full_swap",
                },
                "healthcare_receipt": signed_receipt_by_field,
            },
        }
    )
    receipt_by_field = {
        "contract_id": "healthporta.provider-directory-profile-initial-receipt.v2",
        "build_id": consumed["build_id"],
        "run_id": consumed["run_id"],
        "attestation_id": consumed["attestation_id"],
        "lease_digest": consumed["lease_digest"],
        "preflight_receipt_sha256": preflight["receipt_sha256"],
        "initial_target_state_sha256": geometry_by_field["initial_target_state_sha256"],
        "capacity_geometry_hash": consumed["capacity_geometry_hash"],
        "capacity_geometry": geometry_by_field,
        "serving": {name: serving[name] for name in guards._PROFILE_FIELDS},
        "executable_plan_hash": consumed["executable_plan_hash"],
        "source_vector": [],
        "source_context_vector": [],
    }
    receipt_by_field["serving"]["targets"] = serving["targets_json"]
    return consumed, preflight, serving, receipt_by_field


async def _publish_initial(connection, schema, serving, receipt, *, include_receipt=True):
    await _insert(connection, schema, logical._SERVING, serving)
    if include_receipt:
        await connection.execute(
            sa.text(f'''INSERT INTO "{schema}".{logical._INITIAL} (build_id,attestation_id,generation_id,run_id,contract_id,payload,payload_sha256)
            SELECT :build,:attestation,:generation,:run,:contract,body,encode(sha256(convert_to(body::text,'UTF8')),'hex') FROM (SELECT CAST(:payload AS jsonb) body) signed'''),
            {
                "build": receipt["build_id"],
                "attestation": receipt["attestation_id"],
                "generation": serving["generation_id"],
                "run": receipt["run_id"],
                "contract": receipt["contract_id"],
                "payload": json.dumps(receipt),
            },
        )
    await connection.execute(sa.text("SET CONSTRAINTS ALL IMMEDIATE"))


@pytest.mark.asyncio
async def test_native_named_initial_guard_keeps_transaction_source_and_receipt_binding(monkeypatch):
    """These rows test SQL binding; application signature admission has separate verifier coverage."""
    async with _database(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            await connection.run_sync(_install_history_tables, schema)
            await connection.run_sync(_apply, logical.upgrade)
            consumed, preflight, serving, receipt = _initial_authority(schema)
            await connection.execute(
                sa.text(f'INSERT INTO "{schema}".import_run VALUES (:run)'), {"run": consumed["run_id"]}
            )
            await _insert(connection, schema, logical._CONSUMPTION, consumed)
            await _insert(connection, schema, logical._PREFLIGHT, preflight)
        for key in (
            "lease_digest",
            "preflight_receipt_sha256",
            "initial_target_state_sha256",
            "capacity_geometry_hash",
            "source_vector",
        ):
            changed = deepcopy(receipt)
            changed[key] = [{"source_id": "other"}] if key == "source_vector" else "f" * 64
            with pytest.raises(DBAPIError, match="initial_receipt_(binding_invalid|required)"):
                async with engine.begin() as connection:
                    await _publish_initial(connection, schema, serving, changed)
        changed = deepcopy(receipt)
        changed["serving"]["targets"]["profile"]["name"] = "other_edition"
        with pytest.raises(DBAPIError, match="initial_receipt_(binding_invalid|required)"):
            async with engine.begin() as connection:
                await _publish_initial(connection, schema, serving, changed)
        with pytest.raises(DBAPIError, match="initial_receipt_required"):
            async with engine.begin() as connection:
                await _publish_initial(connection, schema, serving, receipt, include_receipt=False)
        async with engine.begin() as connection:
            await _publish_initial(connection, schema, serving, receipt)
        for statement in ("UPDATE {table} SET payload=payload", "DELETE FROM {table}", "TRUNCATE {table}"):
            with pytest.raises(DBAPIError, match="initial_receipt_immutable"):
                async with engine.begin() as connection:
                    await connection.execute(sa.text(statement.format(table=f'"{schema}".{logical._INITIAL}')))
        async with engine.connect() as connection:
            assert await connection.scalar(sa.text(f'SELECT count(*) FROM "{schema}".{logical._INITIAL}')) == 1


@pytest.mark.asyncio
async def test_native_named_targets_refuse_null_extra_and_aliased_coordinates(monkeypatch):
    async with _database(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            await connection.run_sync(_install_history_tables, schema)
            await connection.run_sync(_apply, logical.upgrade)
            await _insert(
                connection,
                schema,
                logical._SERVING,
                _serving_values(_consumed_values(current=True), schema, current=True),
            )
        mutations = (
            lambda coordinates: coordinates["evidence"].update(schema=None),
            lambda coordinates: coordinates["evidence"].update(name=[]),
            lambda coordinates: coordinates["profile"].update(name=""),
            lambda coordinates: coordinates["profile"].update(extra="unrecognized"),
            lambda coordinates: coordinates.update(extra={"schema": schema, "name": "extra"}),
            lambda coordinates: coordinates.pop("evidence"),
            lambda coordinates: coordinates.update(profile=coordinates["evidence"]),
        )
        for mutate in mutations:
            coordinates = logical._named_targets(schema)
            mutate(coordinates)
            with pytest.raises(DBAPIError, match="targets_check"):
                async with engine.begin() as connection:
                    await connection.execute(
                        sa.text(f'UPDATE "{schema}".{logical._SERVING} SET targets_json=CAST(:coordinates AS jsonb)'),
                        {"coordinates": json.dumps(coordinates)},
                    )
