# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sealed archive readiness, common transaction ownership, and commit recovery."""

import asyncio
import datetime
import importlib
import json
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_cms_archive as archive
from process import provider_directory_cms_native_registration as registration
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_observation as observation
from process.plan_release_storage_provision import _close_relation_grants
from process.provider_directory_projection_stage import _copy_driver
from tests import provider_directory_source_postgres_support as source_support
from tests import test_cms_serving_publication_postgres as common
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.cms_archive_writer_postgres_support import enroll_archive_writer
from tests.cms_native_inputs_postgres_support import source_input_custody
from tests.test_provider_directory_cms_preparation import _admission

fhir = importlib.import_module("process.provider_directory_fhir")


class _CommitAcknowledgementDatabase(Database):
    """Inject one acknowledgement fault after the real owning Session commits."""

    acknowledgement_failure = None

    @property
    def connection(self):
        """Let the existing scalar family applications use the actual owner Session."""
        return self._transaction_binding().session

    @asynccontextmanager
    async def transaction(self):
        is_owner = self._transaction_binding() is None
        transaction = getattr(self, "_source_transaction", super().transaction)
        async with transaction() as session:
            yield session
        if is_owner and self.acknowledgement_failure is not None:
            failure, self.acknowledgement_failure = self.acknowledgement_failure, None
            raise failure


@asynccontextmanager
async def _publication_database(monkeypatch, engine, schema):
    """Keep the real Source factory and acknowledgement faults on the same Database."""
    administrator = Database(engine, async_sessionmaker(engine, expire_on_commit=False))
    with monkeypatch.context() as settings:
        settings.setattr(source_support, "Database", _CommitAcknowledgementDatabase)
        async with source_input_custody(administrator, schema, settings) as component:
            await _install_epoch_controls(component, schema)
            async with engine.begin() as connection:
                await connection.execute(
                    text(f'GRANT EXECUTE ON FUNCTION "{schema}".cms_native_input_advance() TO "{component.owner}"')
                )
            database = component.database
            database._source_transaction = database.transaction
            settings.setattr(database, "transaction", lambda: _CommitAcknowledgementDatabase.transaction(database))
            yield database


def _epoch_migration_statements(schema):
    """Use original migration SQL for these two controls and their exact five guards."""
    statements = []
    for prefix, functions in (
        ("20260930020000", ("_create_evidence_guards", "_create_resource_guards", "_create_truncate_guards")),
        ("20260930050000", ("upgrade",)),
        ("20260930090000", ("_freeze_candidate_inputs",)),
    ):
        migration = native._migration(prefix)
        migration.op = SimpleNamespace(execute=statements.append)
        for function in functions:
            if function == "upgrade":
                migration.upgrade()
            else:
                getattr(migration, function)('"' + schema + '"')
    return statements


async def _install_epoch_controls(component, schema):
    """Complete scalar fixture prerequisites before real Source publication can read them."""
    from tests.cms_npd_admission_postgres_support import _provision_cms_epoch_controls

    roots = ("provider_directory_cms_npd_relationship_receipt", "provider_directory_cms_serving_coverage")
    rows_by_root = {}
    async with component.admin_engine.begin() as connection:
        for root in roots:
            rows_by_root[root] = (await connection.execute(text(f'SELECT * FROM "{schema}".{root}'))).mappings().all()
            assert len(rows_by_root[root]) == 1
            await connection.execute(text(f'DROP TABLE "{schema}".{root}'))
    statements = _epoch_migration_statements(schema)
    async with source.source_transaction(component.database) as session:
        assert (
            await session.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass(:header) "
                    "AND contype='p' AND conkey=ARRAY[1]::smallint[])"
                ),
                {"header": f'"{schema}".provider_directory_endpoint_dataset'},
            )
            is True
        )
        for statement in statements:
            if any(statement.startswith(f'CREATE TABLE "{schema}".{root} (') for root in roots):
                await session.execute(text(statement))
        connection = await session.connection()
        await connection.run_sync(lambda sync: native._apply(sync, "20260930080000"))
        await _restore_epoch_rows(session, schema, rows_by_root)
        await _install_epoch_guards(session, schema, statements, roots)
    await _provision_cms_epoch_controls(component)


async def _restore_epoch_rows(session, schema, rows_by_root):
    """Retain each original named scalar value before installing the existing immutable guards."""
    for root, previous_rows in rows_by_root.items():
        for previous in previous_rows:
            restored_by_column = dict(previous)
            if root == "provider_directory_cms_serving_coverage":
                restored_by_column["created_at"] = restored_by_column["published_at"].replace(
                    tzinfo=datetime.timezone.utc
                )
            columns = ",".join(restored_by_column)
            parameters = ",".join(":" + name for name in restored_by_column)
            await session.execute(
                text(f'INSERT INTO "{schema}".{root} ({columns}) VALUES ({parameters})'), restored_by_column
            )
            current = (
                (await session.execute(text(f'SELECT {",".join(previous)} FROM "{schema}".{root}'))).mappings().one()
            )
            assert dict(current) == dict(previous)


async def _install_epoch_guards(session, schema, statements, roots):
    from tests.cms_npd_admission_postgres_support import EPOCH_GUARD_FUNCTIONS

    for statement in statements:
        if any(
            statement.startswith(f'{prefix} "{schema}".{name}()')
            for name in EPOCH_GUARD_FUNCTIONS
            for prefix in ("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
        ) or (statement.startswith("CREATE TRIGGER") and any(f' ON "{schema}".{root}' in statement for root in roots)):
            await session.execute(text(statement))


async def _install_owned_archive(database, schema):
    """Create scalar component storage through its actual Source Owner, never raw Publisher DDL."""
    async with source.source_transaction(database) as session:
        connection = await session.connection()
        if not await session.scalar(text(f"SELECT to_regclass('{schema}.cms_native_input_revision') IS NOT NULL")):
            await connection.run_sync(lambda sync: native._apply(sync, "20260930120000"))
        await session.execute(
            text(f"CREATE TABLE {schema}.address_archive_v2 (address_key int PRIMARY KEY, marker text)")
        )
        from tests.cms_named_revision_postgres_support import upgrade_named_native_counter

        await upgrade_named_native_counter(connection, schema)
        await native.native_inputs._register_relation(session, schema, schema, "address_archive_v2")
        await connection.run_sync(lambda sync: native._apply(sync, "20260930130000"))


async def _prepare_sealed_archive(database, schema, admission):
    """Reuse the real logging/seal/view builder around one small registered canonical heap."""
    await _install_owned_archive(database, schema)
    prefix = "cms_archive_" + uuid4().hex
    delta_name, effective_name = prefix + "_delta", prefix + "_effective"
    relations = []
    async with preparation.nonprofile_sql_transaction(fhir, admission):
        await database.status(
            f"CREATE UNLOGGED TABLE {schema}.{delta_name} (LIKE {schema}.address_archive_v2 INCLUDING DEFAULTS)"
        )
        await archive._capture_heap(fhir, admission, schema, delta_name, relations)
        await database.status(f"INSERT INTO {schema}.{delta_name} VALUES (1,'new')")
        await database.status(f"ALTER TABLE {schema}.{delta_name} ADD PRIMARY KEY (address_key)")
        definition = await archive._seal_delta(fhir, admission, schema, delta_name, effective_name, relations)
        row_count, digest = await archive._digest(fhir, schema, delta_name)
        columns = await archive._columns(database, schema, "address_archive_v2")
    return archive.PreparedArchiveDelta(
        schema,
        delta_name,
        effective_name,
        0,
        admission.plan.native_address_input_hash,
        columns,
        row_count,
        digest,
        definition,
        admission,
    )


def _bind_publication_seams(monkeypatch, database, schema, prepared, address, outcome):
    """Keep source/native family fixture seams separate from actual archive and readiness code."""
    monkeypatch.setattr(fhir, "_provider_directory_artifact_transaction_timeout_seconds", lambda *_args, **_kw: 5)
    monkeypatch.setattr(fhir, "_provider_directory_artifact_bundle_context", lambda *_args: (schema, (), "5s", "5s"))

    async def configure_settings(*_args):
        await database.status("SET LOCAL statement_timeout='5s'")
        await database.status("SET LOCAL lock_timeout='5s'")

    def admitted_input_fence(prepared_family, address_family, native_dependencies):
        assert prepared_family is prepared and address_family is address
        assert address.native_dependencies.archive is prepared.archive_delta
        assert prepared.archive_delta.admission is prepared.nonprofile_admission
        assert isinstance(native_dependencies, dict)
        return prepared.archive_delta

    async def assert_input_fence(session, namespace, expected):
        assert session is database.connection and namespace == schema and expected is prepared.archive_delta
        assert await archive._revision(database, schema) == expected.from_revision
        assert await session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE pid=pg_backend_pid() AND relation=to_regclass(:relation) AND granted AND mode='ShareRowExclusiveLock')"
            ),
            {"relation": f"{schema}.address_archive_v2"},
        )

    monkeypatch.setattr(fhir, "_configure_provider_directory_artifact_promotion", configure_settings)
    monkeypatch.setattr(publication, "admitted_native_input_fence", admitted_input_fence)
    monkeypatch.setattr(publication, "assert_native_address_input_fence", assert_input_fence)
    common._bind_family_applications(monkeypatch, database, schema, outcome, prepared, address, None)
    apply_address = publication.publish_prepared_entity_address_generation

    async def guarded_address_apply(prepared_address):
        await prepared.archive_delta.assert_applied_backend(database)
        settings = await database.first("SELECT current_setting('statement_timeout'),current_setting('lock_timeout')")
        assert tuple(settings) == ("5s", "5s")
        await apply_address(prepared_address)

    monkeypatch.setattr(publication, "publish_prepared_entity_address_generation", guarded_address_apply)


async def _prepared_serving(monkeypatch, database, schema, outcome, phase_checks):
    """Use real admission measurement, archive seal, bundles, and serving readiness."""
    admission = _admission()
    admission._started = True

    async def approve(request):
        phase_checks.append(request)
        for relation in request.relations:
            await archive.capture_archive_layout(fhir, relation)
        return preparation.NonprofileAdmissionReceipt(
            request.phase,
            request.lease.lease_digest,
            request.lease.reservation_id,
            request.plan.capacity_geometry_hash,
            request.relations,
            request.logging_relations,
        )

    async def clock():
        return admission.lease.max_build_deadline - datetime.timedelta(minutes=5)

    admission.check_phase = approve

    @asynccontextmanager
    async def cutover_operation():
        assert database._transaction_binding() is None
        yield

    async def local_check(relations):
        assert database._transaction_binding() is not None
        for relation in relations or ():
            await archive.capture_archive_layout(fhir, relation)

    admission.cutover_operation, admission.check_cutover = cutover_operation, local_check
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", clock)
    delta = await _prepare_sealed_archive(database, schema, admission)
    execution = common._execution()
    execution.attestation.desired_profile_as_of = "2026-01-02"
    full_bundle = fhir.ProviderDirectoryArtifactBundle(archive_delta=delta)
    profile_bundle = fhir.ProviderDirectoryArtifactBundle(
        profile_delta=SimpleNamespace(generation_id="pdprofile_" + "2" * 32)
    )
    prepared = preparation.PreparedServingArtifacts(
        fhir,
        SimpleNamespace(datasets=()),
        execution,
        admission,
        full_bundle,
        profile_bundle,
        {"profile": {}},
        delta.relation_overrides,
        None,
    )
    address = SimpleNamespace(committed=False, context={}, swaps=[], native_dependencies=SimpleNamespace(archive=delta))
    prepared.address = address
    _bind_publication_seams(monkeypatch, database, schema, prepared, address, outcome)
    _bind_source_acknowledgement_fault(monkeypatch, database)
    return prepared, address


def _bind_source_acknowledgement_fault(monkeypatch, database):
    original = publication._publication_session

    @asynccontextmanager
    async def acknowledge(*arguments, **keywords):
        async with original(*arguments, **keywords) as session:
            yield session
        if database.acknowledgement_failure is not None:
            failure, database.acknowledgement_failure = database.acknowledgement_failure, None
            raise failure

    monkeypatch.setattr(publication, "_publication_session", acknowledge)


async def _assert_publication_result(database, schema, prepared, address, predecessor, before, outcome):
    """Read durable state independently after both the owner Session and recovery finish."""
    is_committed = outcome in {"success", "lost_ack", "cancel_after"}
    delta = prepared.archive_delta
    assert delta.committed is is_committed
    assert prepared.nonprofile_bundle.promoted is is_committed and prepared.profile_bundle.promoted is is_committed
    assert address.committed is is_committed and database._transaction_binding() is None
    assert await database.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2") == int(is_committed)
    assert await archive._relation_exists(database, schema, "address_archive_v2")
    assert await archive._revision(database, schema) == (2 if is_committed else 0)
    if not is_committed:
        assert await common._serving_state(database.engine, schema) == before
        return
    async with database.session() as session:
        current = await receipts.read_current_receipt(session, schema)
        assert current["receipt_id"] == prepared.metrics["cms_serving"]["receipt_id"]
        assert current["payload"]["predecessor_receipt_id"] == predecessor["receipt_id"]
        assert current["payload"]["archive"] == {
            "target_schema": schema,
            "target_table": "address_archive_v2",
            "from_revision": 0,
            "to_revision": 2,
            "native_input_hash": delta.native_input_hash,
            "delta_rows": delta.delta_rows,
            "delta_sha256": delta.delta_sha256,
        }
        assert await receipts.verify_historical_receipt(session, schema, current["receipt_id"], current["payload"])
    assert prepared.metrics["cms_serving"]["recovered_commit"] is (outcome != "success")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "profile", "lost_ack", "cancel_after", "seal"])
async def test_actual_archive_readiness_and_common_publication(monkeypatch, outcome):
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await common._enable_candidate_checks(engine, schema)
        before = await common._serving_state(engine, schema)
        async with _publication_database(monkeypatch, engine, schema) as database:
            monkeypatch.setattr(fhir, "db", database)
            monkeypatch.setenv("DB_SCHEMA", schema)
            phase_checks = []
            prepared, address = await _prepared_serving(monkeypatch, database, schema, outcome, phase_checks)
            delta = prepared.archive_delta
            cms_token = preparation._ACTIVE.set(None)
            profile_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
            assert fhir._provider_directory_profile_capacity_admission() is None
            try:

                async def no_rescan(*_args):
                    raise AssertionError("Profile and cutover readiness must use sealed metadata")

                monkeypatch.setattr(archive, "_digest", no_rescan)
                await prepared.assert_ready()
                assert phase_checks[-1].phase == "readiness"
                assert not any(check.phase == "cutover" for check in phase_checks)
                await _run_publication(database, schema, prepared, address, predecessor, before[0], outcome)
                await _assert_publication_result(database, schema, prepared, address, predecessor, before, outcome)
                assert [check.phase for check in phase_checks].count("cutover") == 1
            finally:
                fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(profile_token)
                preparation._ACTIVE.reset(cms_token)
                database.acknowledgement_failure = None
                await delta.cleanup(fhir)
            assert not prepared.nonprofile_admission._relations
            assert not await archive._relation_exists(database, schema, delta.delta_table)
            assert not await archive._relation_exists(database, schema, delta.effective_relation)


async def _run_publication(database, schema, prepared, address, predecessor, dependencies, outcome):
    """Fault only owned seal metadata, family application, or acknowledgement delivery."""
    if outcome == "seal":
        async with database._input_component.admin_engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{schema}"."{prepared.archive_delta.delta_table}" SET marker=\'changed-prepared-content\''
                )
            )
    if outcome == "lost_ack":
        database.acknowledgement_failure = OSError("synthetic acknowledgement lost")
    if outcome == "cancel_after":
        database.acknowledgement_failure = asyncio.CancelledError("synthetic cancellation after commit")
    candidate_proof_by_field = {
        key: predecessor["payload"]["cms"][key]
        for key in ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version")
    }

    async def publish():
        return await publication.commit_prepared_serving_generation(
            fhir,
            prepared.execution,
            prepared,
            address=address,
            candidate_proof=candidate_proof_by_field,
            native_dependencies=dependencies,
            predecessor=predecessor,
        )

    if outcome in {"profile", "seal"}:
        with pytest.raises(RuntimeError, match="synthetic profile failure|cms_archive_preparation_changed"):
            await publish()
    elif outcome == "cancel_after":
        with pytest.raises(asyncio.CancelledError):
            await publish()
    else:
        await publish()


async def _enroll_archive(component, schema):
    async with component.admin_engine.begin() as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        await connection.execute(text("SELECT 1"))
        candidate = await registration.capture_native_input_candidate(
            driver,
            ledger_schema=schema,
            target_schema=schema,
            target_table="address_archive_v2",
            stage_schema=schema,
            stage_table="address_archive_v2",
        )
        current = await enroll_archive_writer(driver, component.policy, candidate)
        evidence = await registration.register_native_input_candidates(
            driver,
            ledger_schema=schema,
            candidates=(current,),
        )
        assert evidence["registrations"][0]["revision"] == 0
        await _close_relation_grants(
            driver,
            {
                "oid": await driver.fetchval(
                    "SELECT to_regclass($1)::oid::bigint",
                    f'"{schema}".provider_directory_endpoint_dataset',
                ),
                "nspname": schema,
                "relname": "provider_directory_endpoint_dataset",
            },
            component.owner,
            component.policy.runtime_roles,
        )


async def _sealed_delta(component, schema):
    database, admission = component.database, _admission()
    admission._started = True
    prefix = "cms_archive_" + uuid4().hex
    delta, effective = prefix + "_delta", prefix + "_effective"
    relations = []
    async with source.source_transaction(database):
        await database.status(
            f'CREATE UNLOGGED TABLE "{schema}".{delta} (LIKE "{schema}".address_archive_v2 INCLUDING DEFAULTS)'
        )
        await archive._capture_heap(fhir, admission, schema, delta, relations)
        await database.status(f"INSERT INTO \"{schema}\".{delta} VALUES (1,'new')")
        await database.status(f'ALTER TABLE "{schema}".{delta} ADD PRIMARY KEY (address_key)')
        definition = await archive._seal_delta(fhir, admission, schema, delta, effective, relations)
        count, digest = await archive._digest(fhir, schema, delta)
        columns = await archive._columns(database, schema, "address_archive_v2")
    return archive.PreparedArchiveDelta(
        schema,
        delta,
        effective,
        0,
        admission.plan.native_address_input_hash,
        columns,
        count,
        digest,
        definition,
        admission,
    )


async def _assert_ordinary_writers_denied(component, schema):
    for role in component.policy.runtime_roles:
        engine = create_async_engine(component.admin_engine.url.set(username=role))
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                with pytest.raises(DBAPIError, match="permission denied"):
                    await session.execute(text(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (9,'forbidden')"))
        finally:
            await engine.dispose()


@asynccontextmanager
async def _publication(component):
    sessions = await source.source_publication_sessions()
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        async with (
            component.database.bind_existing_session(session),
            source.original_source_publication(component.database, session),
        ):
            yield session


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_source_archive_owner_gate_and_atomic_merge(monkeypatch, rollback):
    async with native._database(monkeypatch, install_source_guard=False) as (engine, schema):
        identity = await native._install_archive(engine, schema)
        async with source_support.native_source_component(
            engine, schema, monkeypatch, legacy_builder=True
        ) as component:
            component.factory.enroll_ordinary_native_inputs()
            monkeypatch.setattr(fhir, "db", component.database)
            async with source.source_transaction(component.database, isolation="repeatable read") as session:
                await archive.require_source_archive_owner(session, schema)
                assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".address_archive_v2')) == 0
                with pytest.raises(RuntimeError, match="cms_archive_source_binding_changed"):
                    await archive.require_source_archive_owner(session, "different_source_schema")
            async with observation.observation_transaction(fhir, isolation="repeatable read") as session:
                await archive.require_source_archive_owner(session, schema)
                assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            await _enroll_archive(component, schema)
            async with observation.observation_transaction(fhir, isolation="repeatable read") as session:
                await archive.require_source_archive_owner(session, schema)
                assert await session.scalar(text("SHOW transaction_read_only")) == "on"
                assert source._PUBLISHER.get() is None and source._OWNER.get() is None
            await _assert_ordinary_writers_denied(component, schema)
            async with source.source_transaction(component.database, isolation="repeatable read") as session:
                await archive.require_source_archive_owner(session, schema)
            delta = await _sealed_delta(component, schema)
            await _assert_atomic_merge(component, schema, delta, identity, rollback, monkeypatch)


@asynccontextmanager
async def _ordinary_archive_pool(component, monkeypatch):
    """Keep genuine Source sessions distinct from the ordinary runtime pool."""
    database = component.database
    engine = create_async_engine(component.admin_engine.url.set(username=component.reader))
    ordinary = Database(engine, async_sessionmaker(engine, expire_on_commit=False))
    try:
        with monkeypatch.context() as settings:
            settings.delattr(database, "transaction")
            settings.setattr(database, "session", ordinary.session)
            assert database.transaction.__func__ is Database.transaction
            assert database.session_factory.kw["bind"] is not ordinary.session_factory.kw["bind"]
            yield ordinary
    finally:
        await ordinary.disconnect()


async def _assert_ordinary_archive_denied(ordinary, component, delta):
    """Prove runtime SELECT cannot authorize the prepared heap lock or private view."""
    for statement in (
        f'DELETE FROM "{delta.schema}".{delta.delta_table} WHERE FALSE',
        f'SELECT * FROM "{delta.schema}".{delta.effective_relation}',
    ):
        with pytest.raises(DBAPIError, match="permission denied"):
            async with ordinary.transaction() as session:
                assert await session.scalar(text(f'SELECT count(*) FROM "{delta.schema}".{delta.delta_table}')) == 1
                await session.execute(text(statement))


async def _assert_archive_read_limits(component, delta, session):
    """Observe actual transaction state and admitted settings before reading prepared rows."""
    assert session.in_transaction() and component.database._transaction_binding() is not None
    driver = await _copy_driver(session)
    assert driver.is_in_transaction()
    assert await driver.fetchval("SELECT pg_size_bytes(current_setting('temp_file_limit'))") == (
        delta.admission.plan.temp_file_limit_bytes_per_backend
    )
    assert await driver.fetchval(
        "SELECT current_setting('max_parallel_workers_per_gather')::int=0 "
        "AND current_setting('max_parallel_maintenance_workers')::int=0 "
        "AND current_setting('statement_timeout')::interval>interval '0' "
        "AND current_setting('statement_timeout')::interval<=interval '30 seconds' "
        "AND current_setting('lock_timeout')::interval>interval '0' "
        "AND current_setting('lock_timeout')::interval<=interval '30 seconds'"
    )


def _archive_readiness_bundle(delta):
    """Use the actual serving readiness entry point without preparing unrelated families."""
    return preparation.PreparedServingArtifacts(
        fhir=fhir,
        fence=None,
        execution=None,
        nonprofile_admission=delta.admission,
        nonprofile_bundle=SimpleNamespace(archive_delta=delta),
        profile_bundle=None,
        metrics={},
        relation_overrides={},
        overlay_identity=None,
    )


async def _assert_borrowed_archive_readiness(component, delta, prepared, checked_sessions):
    """Readiness preserves the current logical revision and transaction resource settings."""
    async with _publication(component) as session:
        driver = await _copy_driver(session)
        revision = await archive._revision(component.database, delta.schema)
        settings_sql = "SELECT current_setting('temp_file_limit'),current_setting('lock_timeout')"
        before = await driver.fetchrow(settings_sql)
        await delta.assert_ready(fhir, cutover=True)
        await prepared.assert_ready(cutover=True)
        assert session.in_transaction() and driver.is_in_transaction()
        assert await archive._revision(component.database, delta.schema) == revision
        assert await driver.fetchrow(settings_sql) == before
        assert checked_sessions[-2:] == [True, True]


async def _assert_readiness_source_pools(component, delta, monkeypatch):
    """Detached checks enter Source; cutover checks borrow its exact existing owner."""
    admission, original = delta.admission, delta.assert_read_identity
    checked_sessions = []

    async def checked_read(directory, session):
        await _assert_archive_read_limits(component, delta, session)
        checked_sessions.append(session.in_transaction())
        await original(directory, session)

    prepared = _archive_readiness_bundle(delta)
    async with component.admin_engine.begin() as connection:
        await connection.execute(text(f'GRANT SET ON PARAMETER temp_file_limit TO "{component.owner}"'))
    with monkeypatch.context() as settings:
        settings.setattr(delta, "assert_read_identity", checked_read)
        settings.setattr(
            fhir,
            "_profile_capacity_preflight_clock",
            AsyncMock(return_value=admission.lease.max_build_deadline - datetime.timedelta(seconds=30)),
        )
        async with _ordinary_archive_pool(component, settings) as ordinary:
            await _assert_ordinary_archive_denied(ordinary, component, delta)
            token = preparation._ACTIVE.set(admission)
            try:
                await delta.assert_ready(fhir)
                preparation._ACTIVE.set(None)
                await prepared.assert_ready()
                preparation._ACTIVE.set(admission)
                assert component.database._transaction_binding() is None and source._PUBLISHER.get() is None
                await _assert_borrowed_archive_readiness(component, delta, prepared, checked_sessions)
            finally:
                preparation._ACTIVE.reset(token)
    assert len(checked_sessions) == 4


async def _assert_atomic_merge(component, schema, delta, identity, rollback, monkeypatch):
    try:
        await _assert_readiness_source_pools(component, delta, monkeypatch)
        with pytest.raises(RuntimeError, match="synthetic publication rollback") if rollback else nullcontext():
            async with _publication(component) as session:
                await delta.before_lock(fhir, session)
                async with session.begin_nested():
                    archive_result = await delta.apply(fhir, session)
                    await _assert_archive_matcher(session, schema, archive_result, owns_child=True)
                await delta.assert_applied_backend(component.database)
                await _assert_archive_matcher(session, schema, archive_result, owns_child=False)
                await _assert_archive_matcher_foreign(component, schema, archive_result)
                assert archive_result["to_revision"] == 2 and (
                    archive_result["target_schema"],
                    archive_result["target_table"],
                ) == (identity["target_schema"], identity["target_table"])
                if rollback:
                    raise RuntimeError("synthetic publication rollback")
        await _assert_archive_matcher_foreign(component, schema, archive_result)
        async with component.admin_engine.connect() as connection:
            assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}".address_archive_v2')) == int(
                not rollback
            )
            assert await connection.scalar(
                text(
                    f'SELECT revision FROM "{schema}".cms_native_input_revision WHERE schema_name=:schema AND table_name=:table'
                ),
                {"schema": schema, "table": "address_archive_v2"},
            ) == (0 if rollback else 2)
            stamp = await connection.scalar(
                text(
                    f'SELECT mutation_xid::text FROM "{schema}".cms_native_input_revision WHERE schema_name=:schema AND table_name=:table'
                ),
                {"schema": schema, "table": "address_archive_v2"},
            )
            assert (stamp is None) is rollback
    finally:
        await delta.cleanup(fhir)


async def _assert_archive_matcher(session, schema, archive_result, *, owns_child):
    diagnostics = (
        (
            await session.execute(
                text(f"""
        SELECT "{schema}".cms_serving_archive_result_matches(CAST(:payload AS jsonb)) AS matches,
        pg_current_xact_id()::text AS top_xid, revision.xmin::text AS raw_xmin,
        revision.mutation_xid::text AS mutation_xid,
        EXISTS(SELECT FROM pg_locks lock WHERE lock.locktype='transactionid'
          AND lock.transactionid=revision.xmin AND lock.pid=pg_backend_pid()
          AND lock.granted AND lock.mode='ExclusiveLock') AS owns_mutation
        FROM "{schema}".cms_native_input_revision revision WHERE schema_name=:schema AND table_name='address_archive_v2'
    """),
                {
                    "payload": json.dumps({"archive": archive_result, "predecessor_receipt_id": None}),
                    "schema": archive_result["target_schema"],
                },
            )
        )
        .mappings()
        .one()
    )
    assert diagnostics["matches"] is True, dict(diagnostics)
    assert diagnostics["mutation_xid"] == diagnostics["top_xid"]
    assert diagnostics["raw_xmin"] != diagnostics["top_xid"]
    assert diagnostics["owns_mutation"] is owns_child


async def _assert_archive_matcher_foreign(component, schema, archive_result):
    async with component.database.engine.connect() as connection:
        matched = await connection.scalar(
            text(f'SELECT "{schema}".cms_serving_archive_result_matches(CAST(:payload AS jsonb))'),
            {"payload": json.dumps({"archive": archive_result, "predecessor_receipt_id": None})},
        )
        assert matched is False


async def _historical_counter_baseline(engine, schema):
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: native._apply(sync, "20260930120000"))
        await connection.execute(
            text(f'CREATE TABLE "{schema}".address_archive_v2 (address_key int PRIMARY KEY,marker text)')
        )
        oid = await connection.scalar(text(f"SELECT to_regclass('{schema}.address_archive_v2')::oid::bigint"))
        await connection.execute(
            text(
                f"INSERT INTO {schema}.cms_native_input_revision(relation_oid,schema_name,table_name) VALUES (:oid,:schema,'address_archive_v2')"
            ),
            dict(oid=oid, schema=schema),
        )
        await connection.execute(
            text(
                f"CREATE TRIGGER cms_native_input_mutation AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {schema}.address_archive_v2 FOR EACH STATEMENT EXECUTE FUNCTION {schema}.cms_native_input_advance()"
            )
        )
        await connection.execute(
            text(f"ALTER TABLE {schema}.address_archive_v2 ENABLE ALWAYS TRIGGER cms_native_input_mutation")
        )
        await native._merge_archive(connection, schema)
        return dict(await native._counter_migration_snapshot(connection, schema))


@pytest.mark.asyncio
async def test_archive_transaction_migration_refuses_a_hidden_baseline_stamp(monkeypatch):
    async with native._database(monkeypatch, install_source_guard=False, named=False) as (engine, schema):
        before_by_field = await _historical_counter_baseline(engine, schema)
        with pytest.raises(DBAPIError, match="cms_native_input_mutation_identity_changed"):
            async with engine.begin() as connection:
                top = await connection.scalar(text("SELECT pg_current_xact_id()::text"))
                assert top.isdecimal()
                await connection.execute(
                    text(
                        f"""ALTER TABLE "{schema}".cms_native_input_revision ADD COLUMN mutation_xid xid8 DEFAULT '{top}'::xid8"""
                    )
                )
                await connection.execute(
                    text(f'ALTER TABLE "{schema}".cms_native_input_revision ALTER COLUMN mutation_xid DROP DEFAULT')
                )
                attribute = (
                    (
                        await connection.execute(
                            text(
                                "SELECT atthasdef,atthasmissing,attmissingval IS NOT NULL AS has_missing "
                                "FROM pg_attribute WHERE attrelid=to_regclass(:table) AND attname='mutation_xid'"
                            ),
                            {"table": f'"{schema}".cms_native_input_revision'},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert dict(attribute) == {"atthasdef": False, "atthasmissing": True, "has_missing": True}
                assert (
                    await connection.scalar(
                        text(f'SELECT mutation_xid::text FROM "{schema}".cms_native_input_revision')
                    )
                    == top
                )
                await connection.exec_driver_sql(native._migration("20261019130000")._counter_sql(schema))
        async with engine.connect() as connection:
            assert dict(await native._counter_migration_snapshot(connection, schema)) == before_by_field
            assert not await connection.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_attribute "
                    "WHERE attrelid=to_regclass(:table) AND attname='mutation_xid' AND NOT attisdropped)"
                ),
                {"table": f'"{schema}".cms_native_input_revision'},
            )
