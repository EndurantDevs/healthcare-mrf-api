# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed native guard-component evidence, without claiming full inverse preflight or publication."""

import asyncio
import importlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateColumn

from db.models.system import ImportRun, ProviderDirectoryProfileServingGeneration
from process import provider_directory_serving_inverse_capacity as capacity
from process import provider_directory_serving_inverse_capacity_contract as inverse
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests import provider_directory_profile_capacity_signing_guard_test_support as timing_support
from tests import provider_directory_serving_inverse_capacity_support as wire
from tests import test_provider_directory_profile_initial_history_migration as historical
from tests import test_provider_directory_profile_initial_migration as initial
from tests.provider_directory_serving_recovery_postgres_support import _remove_owned_catalog
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope
from tests.test_provider_directory_profile_capacity_attestation_schema import _OperationsRecorder
from tests.test_provider_directory_serving_inverse_capacity_signing import verify

SCHEMA = initial._SCHEMA
SERVING = "provider_directory_profile_serving_generation"
JOURNAL = "provider_directory_serving_recovery"
SUCCESSOR = "20261031030000_profile_initial_inverse_guard.py"


async def migrate(engine, filename):
    await historical._apply(engine, initial._migration(filename).upgrade)


async def _predecessor(engine):
    await initial._upgrade(engine)
    reactivation = initial._migration("20261031010000_profile_initial_reactivation.py")
    ordinary, old_initial = reactivation._previous()
    await historical._apply(
        engine,
        lambda: ordinary._replace_check(
            old_initial._qt(SCHEMA, ordinary._TABLE),
            ordinary._CHECK,
            ordinary._purpose_check(ordinary=True),
            ordinary._purpose_check(ordinary=False),
        ),
    )
    await historical._apply(engine, ordinary.upgrade)
    await historical._apply(engine, reactivation.upgrade)
    await migrate(engine, "20261031020000_serving_inverse_capacity.py")


async def _complete_native_tables(engine):
    fhir = importlib.import_module("process.provider_directory_fhir")
    async with engine.begin() as connection:
        for name in ("engine", "node_id", "importer", "status", "params"):
            column = ImportRun.__table__.c[name]
            definition = str(CreateColumn(column).compile(dialect=connection.dialect))
            await connection.execute(text(f'ALTER TABLE "{SCHEMA}".import_run ADD COLUMN {definition}'))
        await connection.execute(text(fhir.provider_directory_address_overlay_table_sql(SCHEMA)))
        recorder = _OperationsRecorder()
        migration = initial._migration("20260730110000_provider_directory_profile_delta.py")
        migration.op = recorder
        migration._create_capacity_lease_consumption_table(SCHEMA)
        for statement in recorder.statements:
            await connection.execute(text(statement))
    await migrate(engine, "20261023100000_provider_directory_serving_recovery.py")


async def _own_catalog(engine, role, password):
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"CREATE ROLE \"{role}\" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
            ),
        )
        await connection.execute(text(f'ALTER SCHEMA "{SCHEMA}" OWNER TO "{role}"'))
        tables = (
            (
                await connection.execute(
                    text(
                        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname=:schema AND c.relkind='r' ORDER BY c.relname"
                    ),
                    {"schema": SCHEMA},
                )
            )
            .scalars()
            .all()
        )
        for name in tables:
            await connection.execute(text(f'ALTER TABLE "{SCHEMA}"."{name}" OWNER TO "{role}"'))


async def profile_row(connection):
    return await connection.scalar(text(f'SELECT to_jsonb(s) FROM "{SCHEMA}".{SERVING} s'))


async def replace_profile(connection, document):
    columns = tuple(name for name in document if name != "singleton_key")
    fields = ",".join('"' + name + '"' for name in columns)
    await connection.execute(
        text(
            f'UPDATE "{SCHEMA}".{SERVING} SET ({fields})=(SELECT {fields} '
            f'FROM jsonb_populate_record(NULL::"{SCHEMA}".{SERVING},CAST(:document AS jsonb))) '
            "WHERE singleton_key='global'"
        ),
        {"document": json.dumps(document)},
    )


async def _first_publication(builder):
    consumed, issued, serving, receipt = await initial._publication_values(builder)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=3)
    consumed["max_build_deadline"] = deadline
    consumed["expires_at"] = deadline
    issued["control_generation"] = serving["control_generation"]
    async with builder.begin() as connection:
        await initial._insert(
            connection,
            "import_run",
            dict(
                run_id=consumed["run_id"],
                engine="healthcare-mrf-api",
                node_id="dev-node",
                importer="provider-directory-fhir",
                status="running",
                params="{}",
            ),
            {"params"},
        )
    await initial._seed_authority(builder, consumed, issued)
    async with builder.begin() as connection:
        await initial._publish(connection, serving, receipt)
        before = await profile_row(connection)
        original = await connection.scalar(
            text(f'SELECT to_jsonb(r) FROM "{SCHEMA}".provider_directory_profile_initial_receipt r')
        )
        await connection.execute(
            text(f'UPDATE "{SCHEMA}".import_run SET status=:status WHERE run_id=:run'),
            {"run": consumed["run_id"], "status": "succeeded"},
        )
    return before, original, consumed


async def _advance_profile(builder, before, *, json_columns):
    async with builder.begin() as connection:
        if json_columns:
            # The essential geometry CHECK requires JSONB; the two vector columns use native JSON.
            for name in ("source_vector_json", "source_context_vector_json"):
                column = ProviderDirectoryProfileServingGeneration.__table__.c[name]
                native_type = column.type.compile(dialect=connection.dialect)
                assert native_type == "JSON"
                await connection.execute(
                    text(
                        f'ALTER TABLE "{SCHEMA}".{SERVING} ALTER COLUMN {name} TYPE {native_type} USING {name}::{native_type}'
                    )
                )
        after = deepcopy(before)
        after.update(generation_id="pdprofile_" + uuid4().hex, control_generation=before["control_generation"] + 1)
        after["capacity_geometry_json"]["contract_id"] = initial._geometry_payload()["contract_id"]
        await replace_profile(connection, after)
        await connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        return await profile_row(connection)


async def _append(connection, document):
    await connection.execute(
        text(
            f'INSERT INTO "{SCHEMA}".{JOURNAL} '
            "(generation,phase,source_id,document,document_sha256) "
            "SELECT :generation,:phase,:source,body,encode(sha256(convert_to(body::text,'UTF8')),'hex') "
            "FROM (SELECT CAST(:document AS jsonb) AS body) source"
        ),
        {
            "generation": document["generation"],
            "phase": document["phase"],
            "source": document["source"]["source_id"],
            "document": json.dumps(document),
        },
    )
    return await connection.scalar(
        text(f'SELECT document_sha256 FROM "{SCHEMA}".{JOURNAL} WHERE generation=:generation AND phase=:phase'),
        {"generation": document["generation"], "phase": document["phase"]},
    )


async def _published_event(builder, before, after):
    roots_by_name = {name: {"oid": before[name + "_target_oid"]} for name in ("profile", "evidence")}
    document_by_field = dict(
        version=1,
        generation=str(uuid4()),
        phase="published",
        source=dict(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            dataset_hash="a" * 64,
            parent_run_id="run_" + "a" * 32,
        ),
        before=dict(roots=roots_by_name, initial_targets=None, metadata={"profile": before}),
        after=dict(roots=deepcopy(roots_by_name), initial_targets=None, metadata={"profile": after}),
        preparation=dict(
            contract_id="provider-directory-common-serving-recovery.v1", qualified_source_ids=["source-a"]
        ),
        predecessor=None,
    )
    async with builder.begin() as connection:
        document_by_field["writer"] = await _actor(connection)
        event_hash = await _append(connection, document_by_field)
    return document_by_field, event_hash


async def _actor(connection):
    return dict(
        (
            await connection.execute(
                text(
                    "SELECT session_user::text AS session,current_user::text AS current,"
                    "current_user::regrole::oid::bigint AS owner,pg_backend_pid() AS backend,"
                    "pg_current_xact_id()::text AS xid,(SELECT oid::bigint FROM pg_database "
                    "WHERE datname=current_database()) AS database_oid"
                )
            )
        )
        .one()
        ._mapping
    )


async def _native_binding(administrator, role):
    async with administrator.connect() as connection:
        return dict(
            (
                await connection.execute(
                    text(
                        "SELECT (SELECT system_identifier::text FROM pg_control_system()) AS database_system_identifier,"
                        "d.oid::bigint AS database_oid,d.datname AS database_name,t.oid::bigint AS tablespace_oid,"
                        "t.spcname AS tablespace_name,(SELECT oid::bigint FROM pg_roles WHERE rolname=:role) AS owner_oid,"
                        "to_regnamespace(:schema)::oid::bigint AS schema_oid FROM pg_database d "
                        "JOIN pg_tablespace t ON t.oid=d.dattablespace WHERE d.datname=current_database()"
                    ),
                    {"role": role, "schema": SCHEMA},
                )
            )
            .one()
            ._mapping
        )


@asynccontextmanager
async def inverse_guard_case(monkeypatch, *, json_columns=False):
    """Use the maintained owned database and a real restricted Builder connection."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    async with initial._database(monkeypatch) as administrator, AsyncExitStack() as cleanup:
        await _predecessor(administrator)
        await _complete_native_tables(administrator)
        role = "initial_inverse_" + uuid4().hex[:16]
        password = uuid4().hex
        ownership = SimpleNamespace(schema=SCHEMA, role=role, is_registered=True)
        cleanup.push_async_callback(_remove_owned_catalog, administrator, ownership)
        await _own_catalog(administrator, role, password)
        builder = create_async_engine(administrator.url.set(username=role, password=password))
        cleanup.push_async_callback(builder.dispose)
        async with builder.connect() as connection:
            actor = await _actor(connection)
            assert actor["session"] == actor["current"] == role
            assert not await connection.scalar(
                text("SELECT rolsuper OR rolbypassrls OR rolcreaterole FROM pg_roles WHERE rolname=current_user")
            )
        before, receipt, consumed = await _first_publication(builder)
        after = await _advance_profile(builder, before, json_columns=json_columns)
        published, event_hash = await _published_event(builder, before, after)
        remaining = (consumed["expires_at"] - datetime.now(timezone.utc)).total_seconds()
        assert remaining < 4
        if remaining > 0:
            await asyncio.sleep(remaining + 0.05)
        yield SimpleNamespace(
            administrator=administrator,
            builder=builder,
            role=role,
            before=before,
            after=after,
            receipt=receipt,
            initial_consumed=consumed,
            published=published,
            event_hash=event_hash,
            native=await _native_binding(administrator, role),
        )


def _inverse_plan(case, operation, *, wrong_owner=False):
    plan = wire.geometry()
    scope = plan["scope"]
    scope.update(
        {
            name: case.native[name]
            for name in ("database_system_identifier", "database_oid", "database_name", "owner_oid", "schema_oid")
        }
    )
    scope.update(
        generation=operation["generation"],
        published_event_sha256=operation["published_event_sha256"],
        operation_sha256=inverse.operation_sha256(operation),
    )
    if wrong_owner:
        scope["owner_oid"] += 1
    provenance = scope["profile_provenance"]
    for name in provenance.keys() & case.after.keys():
        provenance[name] = case.after[name]
    scope["input_sha256"] = inverse.digest(
        inverse.INPUT_DOMAIN, {key: value for key, value in scope.items() if key != "input_sha256"}
    )
    plan["input_sha256"] = scope["input_sha256"]
    for kind in ("data", "temp"):
        for name in ("tablespace_oid", "tablespace_name"):
            plan[kind + "_" + name] = case.native[name]
    return plan


def _fresh_guard(case, plan, operation, timing):
    previous = wire.signing_guard()
    storage = timing_support._storage_observation(timing)
    storage["temp_tablespace"].update(
        tablespace_oid=case.native["tablespace_oid"], tablespace_name=case.native["tablespace_name"]
    )
    control_request, control_receipt = wire._control_preflight(operation, wire.limits(), storage, timing.request_nonce)
    request = wire._healthcare_request(operation, wire.limits(), control_receipt)
    receipt = wire._healthcare_receipt(request, control_receipt, plan, previous)
    previous.update(
        control_plane_request=control_request,
        control_plane_receipt=control_receipt,
        control_plane_request_sha256=control_receipt["request_sha256"],
        control_plane_receipt_sha256=control_receipt["receipt_sha256"],
        healthcare_request=request,
        healthcare_request_sha256=receipt["request_sha256"],
        healthcare_receipt=receipt,
        healthcare_receipt_sha256=receipt["receipt_sha256"],
        capacity_limits_sha256=receipt["capacity_limits_sha256"],
        storage_observation_sha256=control_receipt["storage_observation_sha256"],
        operation_preimage_sha256=inverse.operation_sha256(operation),
    )
    return previous


def signed_inverse(case, *, fault=None):
    """Sign the full existing wire with actual database/owner/event coordinates, not a new issuer."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    issued = now - timedelta(seconds=120) if fault == "expired_inverse" else now
    timing = timing_support._GuardTiming(
        issued - timedelta(seconds=1),
        issued,
        issued + timedelta(seconds=90),
        issued + timedelta(seconds=80),
        uuid4().hex * 2,
    )
    operation_by_field = {
        **wire.execution(),
        "generation": case.published["generation"],
        "published_event_sha256": case.event_hash,
    }
    if fault == "event":
        operation_by_field["published_event_sha256"] = "0" * 64
    plan = _inverse_plan(case, operation_by_field, wrong_owner=fault == "scope_owner")
    guard = _fresh_guard(case, plan, operation_by_field, timing)

    def update(body):
        body.update(
            **{name: case.native[name] for name in ("database_system_identifier", "database_oid", "database_name")},
            signing_preflight_guard=guard,
            signing_preflight_guard_sha256=inverse.digest(inverse.SIGNING_GUARD, guard),
            capacity_geometry_hash=guard["healthcare_receipt"]["capacity_geometry_hash"],
            nonce=guard["healthcare_receipt_sha256"],
            reservation_id="inverse-" + uuid4().hex,
            observed_at=timing_support._utc(timing.observed_at),
            issued_at=timing_support._utc(timing.issued_at),
            expires_at=timing_support._utc(timing.expires_at),
            max_build_deadline=timing_support._utc(timing.max_build_deadline),
        )
        for tablespace in body["tablespaces"]:
            tablespace.update(
                tablespace_oid=case.native["tablespace_oid"], tablespace_name=case.native["tablespace_name"]
            )
        for volume in body["volumes"]:
            volume["reserved_bytes"] = dict(plan["reservation_bytes"])[volume["volume_class"]]

    envelope = _signed_envelope(body_mutator=update)
    lease = verify(envelope)
    run = "run_" + uuid4().hex
    consumed = capacity_lease_consumption_values(lease, capacity._binding(run, plan), accepted_at=issued)
    consumed["admission_purpose"] = inverse.PURPOSE
    request = inverse.validated_inverse_preflight_request(guard["healthcare_request"])
    issued_row = preflight.receipt_row_values(request, guard["healthcare_receipt"], issued_at=issued)
    issued_row.update(consumed_at=issued, consumed_run_id=run, consumed_attestation_id=lease.attestation_id)
    return envelope, consumed, issued_row, operation_by_field


async def seed_inverse(case, *, fault=None):
    envelope, consumed, issued, operation = signed_inverse(case, fault=fault)
    params_by_field = {
        "provider_directory_serving_operation": "recover",
        "provider_directory_serving_generation": operation["generation"],
        inverse.EVENT_FIELD: operation["published_event_sha256"],
        "source_ids": operation["source_ids"],
        inverse.EXECUTION_FIELD: envelope,
    }
    worker_by_field = dict(
        run_id=consumed["run_id"],
        engine="healthcare-mrf-api",
        node_id=operation["node_id"],
        importer="provider-directory-fhir",
        status="running",
        params=json.dumps(params_by_field),
    )
    if fault == "producer":
        worker_by_field["node_id"] = "other-node"
    async with case.builder.begin() as connection:
        await initial._insert(connection, "provider_directory_profile_capacity_lease_consumption", consumed)
        if fault != "missing_preflight":
            await initial._insert(
                connection, "provider_directory_profile_capacity_preflight_receipt", issued, {"receipt_json"}
            )
        await initial._insert(connection, "import_run", worker_by_field, {"params"})
    return SimpleNamespace(envelope=envelope, consumed=consumed, issued=issued, operation=operation)


async def restore(case, authority, *, fault=None):
    async with case.builder.begin() as connection:
        recovered = deepcopy(case.published)
        recovered.update(
            phase="recovered",
            predecessor=case.event_hash,
            before=case.published["after"],
            after=deepcopy(case.published["before"]),
            writer=await _actor(connection),
            inverse={
                "operation_sha256": inverse.operation_sha256(authority.operation),
                "run_id": authority.consumed["run_id"],
                "lease_digest": authority.consumed["lease_digest"],
            },
        )
        if fault == "run":
            recovered["inverse"]["run_id"] = "run_" + "0" * 32
        elif fault == "writer":
            recovered["writer"]["backend"] += 1
        await _append(connection, recovered)
        restored = deepcopy(case.before)
        if fault == "metadata":
            restored["profile_rows"] += 1
        if fault == "insert":
            await connection.execute(text(f'DELETE FROM "{SCHEMA}".{SERVING}'))
            await connection.execute(
                text(
                    f'INSERT INTO "{SCHEMA}".{SERVING} SELECT * FROM jsonb_populate_record(NULL::"{SCHEMA}".{SERVING},CAST(:body AS jsonb))'
                ),
                {"body": json.dumps(restored)},
            )
        else:
            await replace_profile(connection, restored)
        await connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


async def immutable_state(case):
    async with case.builder.connect() as connection:
        return {
            name: (
                await connection.execute(text(f'SELECT to_jsonb(t) FROM "{SCHEMA}".{name} t ORDER BY attestation_id'))
            )
            .scalars()
            .all()
            for name in (
                "provider_directory_profile_initial_receipt",
                "provider_directory_profile_capacity_lease_consumption",
            )
        }
