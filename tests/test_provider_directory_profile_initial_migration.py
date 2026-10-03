# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native schema and SQL provenance proof, separate from signed build admission."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.models.system import ProviderDirectoryProfileCapacityLeaseConsumption
from process import provider_directory_profile_initial_contract as initial
from process import provider_directory_profile_initial_guards as guards
from tests.cms_npd_admission_postgres_support import _DSN_ENV, _database_url
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_attestation_postgres import _consumption_values
from tests.test_provider_directory_profile_capacity_preflight_postgres import _receipt_values

_MIGRATIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_SCHEMA = "initial_contract"


@pytest.mark.parametrize("port", (5432, 55432))
def test_admission_database_url_accepts_explicit_local_ports(monkeypatch, port):
    monkeypatch.setenv(
        _DSN_ENV,
        f"postgresql+asyncpg://test_role@localhost:{port}/hc_cms_admission_test_" + "1" * 32,
    )
    assert _database_url().port == port


@pytest.mark.parametrize("port", (None, 0, 65536))
def test_admission_database_url_rejects_missing_or_invalid_ports(monkeypatch, port):
    port_suffix = "" if port is None else f":{port}"
    monkeypatch.setenv(
        _DSN_ENV,
        f"postgresql+asyncpg://test_role@localhost{port_suffix}/hc_cms_admission_test_" + "1" * 32,
    )
    with pytest.raises(pytest.fail.Exception, match="UUID-owned local PostgreSQL test database"):
        _database_url()


def _migration(filename):
    spec = importlib.util.spec_from_file_location("initial_test_" + filename, _MIGRATIONS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


migration = _migration("20261001110000_profile_initial_publication.py")
checkpoint = _migration("20260720120000_provider_directory_profile_build_checkpoint.py")
delta = _migration("20260730110000_provider_directory_profile_delta.py")


def test_predicates_preserve_old_branches_and_offline_lock_order(monkeypatch):
    assert migration._PREVIOUS._cms_values_check() in migration._initial_preflight_check()
    recorded_statements = []

    class Recorder:
        def create_check_constraint(self, name, _table, condition, **_kwargs):
            if name == migration._CHECKPOINT_CHECK:
                recorded_statements.append(condition)

        def add_column(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(delta, "op", Recorder())
    delta._add_checkpoint_columns(_SCHEMA)
    assert recorded_statements == [migration._CHECKPOINT_PREDECESSOR]
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
    output = io.StringIO()
    with Operations.context(
        MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    ):
        migration.upgrade()
    sql = output.getvalue()
    assert (
        sql.index("lock_timeout")
        < sql.index("pg_advisory_xact_lock")
        < sql.index("LOCK TABLE")
        < sql.index("ADD CONSTRAINT")
    )
    assert "CREATE CONSTRAINT TRIGGER" in sql
    assert "INSERT INTO" not in sql and "DROP TABLE" not in sql
    with pytest.raises(RuntimeError, match="explicit_plan"):
        migration.downgrade()


@pytest.mark.parametrize("schema", [_SCHEMA, "initial_\"quoted'schema"])
def test_migration_sql_is_frozen_against_runtime_changes(monkeypatch, schema):
    """Reloading a revision must preserve exact SQL even after runtime constructors change."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    before = io.StringIO()
    with Operations.context(
        MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": before})
    ):
        migration.upgrade()
    assert migration.IMMUTABLE_BODY == guards.IMMUTABLE_BODY
    for name in ("_receipt_insert_body", "_receipt_matches_sql", "_serving_insert_body"):
        assert getattr(migration, name)(schema) == getattr(guards, name)(schema)
        monkeypatch.setattr(guards, name, lambda _schema: "changed runtime SQL")
    monkeypatch.setattr(guards, "IMMUTABLE_BODY", "changed runtime immutable body")
    reloaded = _migration("20261001110000_profile_initial_publication.py")
    after = io.StringIO()
    with Operations.context(
        MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": after})
    ):
        reloaded.upgrade()
    assert after.getvalue() == before.getvalue()


def _bootstrap(connection):
    with Operations.context(MigrationContext.configure(connection)):
        op = Operations(MigrationContext.configure(connection))
        op.execute(f'CREATE SCHEMA "{_SCHEMA}"')
        op.execute(f'CREATE TABLE "{_SCHEMA}".import_run (run_id varchar(64) PRIMARY KEY)')
        checkpoint.upgrade()
        delta._add_checkpoint_columns(_SCHEMA)
        delta._create_serving_generation_table(_SCHEMA)
        ledger = ProviderDirectoryProfileCapacityLeaseConsumption.__table__.to_metadata(sa.MetaData(), schema=_SCHEMA)
        ledger.create(connection)
        migration._ORIGINAL._create_table(_SCHEMA)
        migration._ORIGINAL._create_guards(_SCHEMA)
        _migration("20260930120000_cms_native_input_revision.py").upgrade()
        _migration("20260930130000_cms_doctors_prepared_seal.py").upgrade()
        migration._PREVIOUS.upgrade()
        predecessor = _migration("20261001100000_profile_failed_cleanup_claim.py")
        reference_guard = _migration("20260929040000_reference_source_generation_guard.py")
        assert migration.down_revision == predecessor.revision
        address_guard = _migration("20261003000000_address_alias_generation_guard.py")
        assert predecessor.down_revision == address_guard.revision
        assert reference_guard.down_revision == migration._PREVIOUS.revision
        predecessor.upgrade()


@asynccontextmanager
async def _database(monkeypatch):
    engine = create_async_engine(_database_url())
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(_bootstrap)
        yield engine
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE'))
        finally:
            await engine.dispose()


async def _insert(connection, table, values, json_fields=()):
    fields = tuple(values)
    sql_values = [f"CAST(:{name} AS jsonb)" if name in json_fields else ":" + name for name in fields]
    await connection.execute(
        sa.text(f'INSERT INTO "{_SCHEMA}".{table} ({",".join(fields)}) VALUES ({",".join(sql_values)})'), values
    )


async def _upgrade(engine):
    async with engine.begin() as connection:

        def apply(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.upgrade()

        await connection.run_sync(apply)


def _serving_fixture_values(consumed, geometry, evidence_oid, profile_oid, now):
    serving_by_field = {
        "singleton_key": "global",
        "status": "published",
        "operation": "publish",
        "control_generation": 7,
        "generation_id": "pdprofile_" + "d" * 32,
        "selection_proof_id": consumed["selection_proof_id"],
        "authority_revision": 2,
        "profile_schema_version": 1,
        "profile_strategy_version": geometry["profile_strategy_version"],
        "source_vector_hash": consumed["source_vector_hash"],
        "source_vector_json": [],
        "source_context_vector_hash": consumed["source_context_vector_hash"],
        "source_context_vector_json": [],
        "executable_plan_hash": consumed["executable_plan_hash"],
        "capacity_geometry_status": "verified",
        "capacity_geometry_hash": consumed["capacity_geometry_hash"],
        "capacity_geometry_json": geometry,
        "cutover_forecast_hash": None,
        "evidence_target_oid": evidence_oid,
        "profile_target_oid": profile_oid,
        "evidence_rows": 0,
        "profile_rows": 0,
        "profile_as_of": consumed["profile_as_of"],
        "published_at": now,
    }
    return serving_by_field


def _preflight_fixture_values(consumed, geometry, now):
    preflight = _receipt_values(
        "initial-native", issued_at=now - timedelta(seconds=1), expires_at=now + timedelta(seconds=600)
    )
    preflight.update(
        contract_id=initial.RECEIPT_CONTRACT,
        request_contract_id=initial.REQUEST_CONTRACT,
        materialization_mode="full_swap",
        capacity_geometry_hash=consumed["capacity_geometry_hash"],
        consumed_at=now,
        consumed_run_id=consumed["run_id"],
        consumed_attestation_id=consumed["attestation_id"],
    )
    signed_receipt_by_field = {
        "capacity_geometry": geometry,
        "serving_generation_preflight_sha256": geometry["initial_target_state_sha256"],
        "serving_generation_preflight": {"contract_id": initial.TARGET_CONTRACT},
        "profile_materialization": initial.MATERIALIZATION,
        "profile_execution_identity": {"materialization_mode": "full_swap"},
    }
    preflight["receipt_json"] = json.dumps(signed_receipt_by_field)
    canonical_lease = json.loads(consumed["canonical_lease_json"])
    canonical_lease.update(
        nonce=preflight["receipt_sha256"],
        signing_preflight_guard={
            "healthcare_request": {
                "contract_id": initial.REQUEST_CONTRACT,
                "profile_materialization": initial.MATERIALIZATION,
            },
            "healthcare_receipt": signed_receipt_by_field,
        },
    )
    consumed["canonical_lease_json"] = json.dumps(canonical_lease)
    return preflight


async def _publication_values(engine):
    """Seed constraint-valid predecessor ledgers; signature/admission is tested by the build integration."""
    async with engine.connect() as connection:
        evidence_oid = await connection.scalar(
            sa.text(f"SELECT 'initial_contract.provider_directory_profile_evidence'::regclass::oid")
        )
        profile_oid = await connection.scalar(
            sa.text(f"SELECT 'initial_contract.provider_directory_profile'::regclass::oid")
        )
        receipt_oid = await connection.scalar(
            sa.text(f"SELECT 'initial_contract.provider_directory_profile_initial_receipt'::regclass::oid")
        )
    now = datetime.now(timezone.utc)
    consumed = _consumption_values()
    for name, offset in {
        "observed_at": -2,
        "issued_at": -1,
        "accepted_at": 0,
        "recorded_at": 0,
        "max_build_deadline": 300,
        "expires_at": 600,
    }.items():
        consumed[name] = now + timedelta(seconds=offset)
    consumed["admission_purpose"] = "profile"
    geometry = _geometry_payload(
        contract_id=initial.GEOMETRY_CONTRACT,
        materialization_mode="full_swap",
        current_source_vector_hash=None,
        current_context_vector_hash=None,
        initial_target_state_sha256="31" * 32,
        initial_receipt_oid=receipt_oid,
        initial_receipt_storage_fingerprint="32" * 32,
        evidence_target_oid=evidence_oid,
        profile_target_oid=profile_oid,
    )
    serving_by_field = _serving_fixture_values(consumed, geometry, evidence_oid, profile_oid, now)
    preflight = _preflight_fixture_values(consumed, geometry, now)
    receipt_by_field = {
        "contract_id": initial.COMMIT_CONTRACT,
        "build_id": consumed["build_id"],
        "run_id": consumed["run_id"],
        "attestation_id": consumed["attestation_id"],
        "lease_digest": consumed["lease_digest"],
        "preflight_receipt_sha256": preflight["receipt_sha256"],
        "initial_target_state_sha256": geometry["initial_target_state_sha256"],
        "capacity_geometry_hash": consumed["capacity_geometry_hash"],
        "capacity_geometry": geometry,
        "serving": {name: serving_by_field[name] for name in guards._PROFILE_FIELDS},
        "executable_plan_hash": consumed["executable_plan_hash"],
        "source_vector": [],
        "source_context_vector": [],
    }
    return consumed, preflight, serving_by_field, receipt_by_field


async def _seed_authority(engine, consumed, preflight):
    async with engine.begin() as connection:
        await _insert(connection, "provider_directory_profile_capacity_lease_consumption", consumed)
        await _insert(connection, migration._ORIGINAL._TABLE, preflight, {"receipt_json"})


async def _publish(connection, serving, payload, *, receipt=True):
    serving_by_field = {name: json.dumps(value) if name.endswith("_json") else value for name, value in serving.items()}
    await _insert(
        connection, migration._SERVING, serving_by_field, {name for name in serving_by_field if name.endswith("_json")}
    )
    if receipt:
        await connection.execute(
            sa.text(f'''INSERT INTO "{_SCHEMA}".{migration._TABLE}
            (build_id,attestation_id,generation_id,run_id,contract_id,payload,payload_sha256)
            SELECT :build_id,:attestation_id,:generation_id,:run_id,:contract_id,payload,
                encode(sha256(convert_to(payload::text,'UTF8')),'hex') FROM (SELECT CAST(:payload AS jsonb) payload) value'''),
            {
                "build_id": payload["build_id"],
                "attestation_id": payload["attestation_id"],
                "generation_id": serving["generation_id"],
                "run_id": payload["run_id"],
                "contract_id": initial.COMMIT_CONTRACT,
                "payload": json.dumps(payload),
            },
        )
    await connection.execute(sa.text("SET CONSTRAINTS ALL IMMEDIATE"))


class _CatalogDatabase:
    def __init__(self, connection):
        self.connection = connection

    async def all(self, query, **values):
        return (await self.connection.execute(sa.text(query), values)).all()


async def _assert_guard_catalog(engine):
    import os

    fhir = importlib.import_module("process.provider_directory_fhir")

    async with engine.connect() as connection:
        oid = await connection.scalar(sa.text(f"SELECT '{_SCHEMA}.{migration._TABLE}'::regclass::oid"))
        await guards.assert_initial_receipt_guards(
            _CatalogDatabase(connection), {"schema_name": _SCHEMA, "relation_oid": oid}
        )
        attributes, indexes, constraints = [
            [
                dict(catalog_row._mapping)
                for catalog_row in await _CatalogDatabase(connection).all(query, relation_oids=[oid])
            ]
            for query in (
                fhir._PROFILE_CAPACITY_ATTRIBUTE_SQL,
                fhir._PROFILE_CAPACITY_INDEX_SQL,
                fhir._PROFILE_CAPACITY_CONSTRAINT_SQL,
            )
        ]
        if os.getenv("HLTHPRT_INITIAL_CATALOG_TEST_RECEIPT"):
            Path(os.environ["HLTHPRT_INITIAL_CATALOG_TEST_RECEIPT"]).write_text(
                json.dumps({"attributes": attributes, "indexes": indexes, "constraints": constraints}, indent=2) + "\n"
            )
        guards.assert_initial_receipt_catalog(oid, attributes, indexes, constraints)
        for section, name, catalog_value in (
            (attributes, "default_expression", "unexpected_writer()"),
            (attributes, "attidentity", "a"),
            (attributes, "atttypid", 25),
            (indexes, "index_expressions", "unexpected_writer(build_id)"),
            (indexes, "indclass", "999999"),
            (constraints, "constraint_definition", "CHECK (unexpected_writer())"),
            (constraints, "convalidated", False),
        ):
            changed_attributes, changed_indexes, changed_constraints = deepcopy((attributes, indexes, constraints))
            changed = (
                changed_attributes
                if section is attributes
                else changed_indexes
                if section is indexes
                else changed_constraints
            )
            changed[0][name] = catalog_value
            with pytest.raises(RuntimeError, match="storage_shape_changed"):
                guards.assert_initial_receipt_catalog(oid, changed_attributes, changed_indexes, changed_constraints)


async def _native_guard_proof(engine):
    await _upgrade(engine)
    consumed, preflight, serving, receipt_payload = await _publication_values(engine)
    await _seed_authority(engine, consumed, preflight)
    await _assert_guard_catalog(engine)
    with pytest.raises(DBAPIError, match="initial_receipt_required"):
        async with engine.begin() as connection:
            await _publish(connection, serving, receipt_payload, receipt=False)
    for name in ("lease_digest", "preflight_receipt_sha256", "initial_target_state_sha256", "capacity_geometry_hash"):
        changed = deepcopy(receipt_payload)
        changed[name] = "ff" * 32
        with pytest.raises(DBAPIError, match="initial_receipt_(binding_invalid|required)"):
            async with engine.begin() as connection:
                await _publish(connection, serving, changed)
    async with engine.begin() as connection:
        await _publish(connection, serving, receipt_payload)
    for statement in (
        f"UPDATE {_SCHEMA}.{migration._TABLE} SET payload=payload",
        f"DELETE FROM {_SCHEMA}.{migration._TABLE}",
        f"TRUNCATE {_SCHEMA}.{migration._TABLE}",
    ):
        with pytest.raises(DBAPIError, match="initial_receipt_immutable"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(statement))
    async with engine.connect() as connection:
        assert await connection.scalar(sa.text(f"SELECT count(*) FROM {_SCHEMA}.{migration._TABLE}")) == 1
        assert await connection.scalar(sa.text(f"SELECT count(*) FROM {_SCHEMA}.{migration._SERVING}")) == 1
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(f'''CREATE OR REPLACE FUNCTION "{_SCHEMA}".pd_profile_initial_receipt_matches(
            receipt "{_SCHEMA}".{migration._TABLE}) RETURNS boolean LANGUAGE sql VOLATILE
            SET search_path=pg_catalog AS $$ SELECT true $$''')
        )
    with pytest.raises(RuntimeError, match="guard_shape_changed"):
        await _assert_guard_catalog(engine)


def test_native_initial_receipt_guards(monkeypatch):
    async def exercise():
        async with _database(monkeypatch) as engine:
            await _native_guard_proof(engine)

    asyncio.run(exercise())
