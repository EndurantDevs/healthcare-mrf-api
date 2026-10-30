# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run optional retained FHIR capture only after the original Source owner commits."""

import asyncio
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_generic_source_policy as source
from process.network_cms_registry_address_capture import native_driver
from process.provider_directory_dataset_rehydrate_pin import _same_database
from process.provider_directory_output_snapshot_catalog import require_output
from process.registry_company_approval_fence import _source_engine
from process.registry_generic_accepted_capture import read_current_generic_fhir_head
from process.registry_generic_capture_continuation import (
    RegistryGenericCaptureTarget,
    capture_committed_generic_fhir_source,
)

_DATABASE_SQL = (
    "SELECT current_database(),oid::bigint,pg_postmaster_start_time(),inet_server_addr()::text,"
    "current_setting('port'),to_regnamespace($1)::oid::bigint FROM pg_database WHERE datname=current_database()"
)
_ACTOR_SQL = (
    "SELECT session_user::text AS login,current_user::text AS actor,current_setting('role') AS role,"
    "current_setting('transaction_read_only') AS readonly,current_setting('transaction_isolation') AS isolation,"
    "rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=session_user"
)


def _require_scope(directory, candidate, selected, continuation):
    from process.provider_directory_generic_source_artifacts import _SOURCE_ONLY

    require_output(
        _SOURCE_ONLY.get() is selected
        and selected[0] is directory
        and selected[1] is source._FACTORY.get()
        and selected[1].policy is selected[6]
        and selected[2] is asyncio.current_task()
        and not selected[2].done()
        and selected[3] == source.source_producer_identity()
        and selected[5]["active"] is True
        and candidate.source_ids == (selected[4],)
        and getattr(selected[1], "generic_fhir_capture_continuation", None) is continuation
        and directory._schema() == selected[6].schema
        and source._PUBLISHER.get() is None
        and source._OWNER.get() is None
        and directory.db._transaction_binding() is None,
        "rest_source_capture_commit_required",
    )


async def _database_identity(driver, schema):
    identity_fields = tuple(await driver.fetchrow(_DATABASE_SQL, schema))
    require_output(identity_fields[-1] is not None, "rest_source_capture_database_changed")
    return identity_fields


async def _actor(driver, login, readonly):
    actor = await driver.fetchrow(_ACTOR_SQL)
    require_output(
        actor is not None
        and actor["login"] == actor["actor"] == login
        and actor["role"] == "none"
        and actor["readonly"] == readonly
        and actor["isolation"] == "repeatable read"
        and actor["rolcanlogin"] is True
        and not any(
            actor[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        ),
        "rest_source_capture_actor_changed",
    )


@dataclass(frozen=True)
class GenericSourceCaptureContinuation:
    """Fixed trusted Reader/capture-Writer pools and retention policy, never task JSON."""

    reader_factory: object
    writer_factory: object
    target: RegistryGenericCaptureTarget
    _reader_engine: object = field(init=False, repr=False)
    _writer_engine: object = field(init=False, repr=False)

    def __post_init__(self):
        require_output(type(self.target) is RegistryGenericCaptureTarget, "rest_source_capture_target_invalid")
        reader, writer = _source_engine(self.reader_factory), _source_engine(self.writer_factory)
        require_output(
            reader is not writer
            and reader.pool is not writer.pool
            and type(reader.url.username) is str
            and bool(reader.url.username)
            and type(writer.url.username) is str
            and bool(writer.url.username)
            and reader.url.username != writer.url.username,
            "rest_source_capture_factories_invalid",
        )
        object.__setattr__(self, "_reader_engine", reader)
        object.__setattr__(self, "_writer_engine", writer)

    async def capture(self, directory, candidate, selected):
        """Own fresh RR transactions; a failed capture leaves the committed Source replayable."""
        _require_scope(directory, candidate, selected, self)
        require_output(
            _source_engine(self.reader_factory) is self._reader_engine
            and _source_engine(self.writer_factory) is self._writer_engine
            and directory.db.engine.pool is not self._reader_engine.pool
            and directory.db.engine.pool is not self._writer_engine.pool,
            "rest_source_capture_factories_changed",
        )
        schema = selected[6].schema
        async with directory.db.engine.connect() as observed:
            await observed.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            driver = (await observed.get_raw_connection()).driver_connection
            database = await _database_identity(driver, schema)
        _require_scope(directory, candidate, selected, self)
        async with self.reader_factory() as reader_session, self.writer_factory() as writer_session:
            require_output(
                type(reader_session) is AsyncSession
                and type(writer_session) is AsyncSession
                and reader_session is not writer_session
                and reader_session.bind is self._reader_engine
                and writer_session.bind is self._writer_engine
                and not reader_session.in_transaction()
                and not writer_session.in_transaction(),
                "rest_source_capture_sessions_invalid",
            )
            async with reader_session.begin(), writer_session.begin():
                await reader_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                await writer_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE"))
                reader, writer = await native_driver(reader_session), await native_driver(writer_session)
                require_output(reader is not writer, "rest_source_capture_sessions_invalid")
                await _actor(reader, self._reader_engine.url.username, "on")
                await _actor(writer, self._writer_engine.url.username, "off")
                await _same_database(writer, reader)
                require_output(
                    await _database_identity(reader, schema) == database
                    and await _database_identity(writer, schema) == database,
                    "rest_source_capture_database_changed",
                )
                _require_scope(directory, candidate, selected, self)
                head = await _capture_source_head(reader, schema, candidate, selected)
                _require_scope(directory, candidate, selected, self)
                capture = await capture_committed_generic_fhir_source(
                    reader,
                    writer,
                    schema_name=schema,
                    source_id=selected[4],
                    endpoint_id=candidate.endpoint_id,
                    target=self.target,
                    expected_head=head,
                    source_policy=selected[6],
                )
                await _actor(reader, self._reader_engine.url.username, "on")
                await _actor(writer, self._writer_engine.url.username, "off")
                _require_scope(directory, candidate, selected, self)
        _require_scope(directory, candidate, selected, self)
        return capture


async def _capture_source_head(reader, schema, candidate, selected):
    head = await read_current_generic_fhir_head(
        reader,
        schema_name=schema,
        source_id=selected[4],
        endpoint_id=candidate.endpoint_id,
    )
    require_output(
        (head.schema_name, head.source_id, head.endpoint_id, head.dataset_id, head.root_run_id)
        == (
            schema,
            selected[4],
            candidate.endpoint_id,
            candidate.dataset_id,
            candidate.acquisition_root_run_id,
        ),
        "rest_source_capture_head_changed",
    )
    return head
