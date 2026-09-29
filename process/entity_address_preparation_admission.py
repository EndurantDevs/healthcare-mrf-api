# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bound admitted native address execution and retain exact scratch ownership."""

from __future__ import annotations

import asyncio
import importlib
import os
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from process.provider_directory_cms_preparation import NonprofileAdmission


def _native():
    return importlib.import_module("process.entity_address_unified")


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value):
        raise ValueError("entity-address preparation relation is invalid")
    return value


@dataclass
class _AdmittedPreparation:
    """Keep the trusted capacity carrier and captured native ownership out of worker payloads."""

    admission: NonprofileAdmission
    native_input_hash: str
    workers: asyncio.Semaphore
    worker_tasks: set[asyncio.Task] = field(default_factory=set)
    db_schema: str | None = None
    stage_oids: tuple[tuple[str, str, int], ...] = ()
    owned_oids: dict[str, int] = field(default_factory=dict)
    retired_oids: list[tuple[str, str, int]] = field(default_factory=list)

    @property
    def cleanup_oids(self) -> tuple[tuple[str, str, int], ...]:
        """Retain every created relation, including unfinished and intermediate heaps."""
        return tuple((name, name, oid) for name, oid in self.owned_oids.items()) + tuple(self.retired_oids)


_ADMISSION: ContextVar[_AdmittedPreparation | None] = ContextVar("entity_address_preparation_admission", default=None)


def _admitted_preparation(admission, native_input_hash):
    """Accept only an in-process admitted plan with an explicit semantic input hash."""
    if admission is None and native_input_hash is None:
        return None
    module = importlib.import_module("process.provider_directory_cms_preparation")
    if (
        not isinstance(admission, module.NonprofileAdmission)
        or not isinstance(native_input_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", native_input_hash)
        or native_input_hash != admission.plan.native_address_input_hash
    ):
        raise ValueError("entity-address preparation admission is invalid")
    limit = admission.plan.temp_file_limit_bytes_per_backend
    worker_count = admission.plan.worker_count
    if type(limit) is not int or limit <= 0 or limit % 1024 or type(worker_count) is not int or worker_count < 1:
        raise ValueError("entity-address preparation temp bound is invalid")
    admission._assert_lease()
    return _AdmittedPreparation(admission, native_input_hash, asyncio.Semaphore(worker_count))


def assert_full_recipe(ctx, task) -> None:
    """Reject branches whose reused or partial scratch is outside this owned full build."""
    native = _native()
    context = ctx.get("context") or {}
    if (
        native._entity_address_refresh_mode(task) != native.ENTITY_ADDRESS_REFRESH_MODE_FULL
        or bool(task.get("test_mode", context.get("test_mode", False)))
        or any(context.get(name) for name in ("stage_prepared", "support_stage_prepared", "support_stage_populated"))
        or any(task.get(name) not in (None, "", 0) for name in ("limit_per_source", "source_limit"))
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE", "").strip()
        or native._is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", False)
        or native._should_keep_raw_stage()
        or native._is_task_or_env_enabled(
            task, "reuse_raw_stage", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE", False
        )
        or native._is_task_or_env_enabled(
            task,
            "serving_only_refresh",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY",
            native.DEFAULT_SERVING_ONLY_REFRESH,
        )
    ):
        raise RuntimeError("entity-address admitted preparation requires a full fresh build")


def _fhir():
    return importlib.import_module("process.provider_directory_fhir")


async def check_owned_storage() -> None:
    """Recheck observed aggregate storage before growth; this is not a growth projection."""
    scope = _ADMISSION.get()
    if scope is not None and scope.owned_oids:
        name, oid = next(iter(scope.owned_oids.items()))
        await scope.admission.assert_external_relation(_fhir(), scope.db_schema, name, oid)


async def lock_owned_relations(database) -> None:
    """Prevent a checked scratch name from being replaced on the executing backend."""
    await _native().candidate_preparation.lock_prepared_doctors(database)
    scope = _ADMISSION.get()
    if scope is None or not scope.owned_oids:
        return
    names = sorted(scope.owned_oids)
    relations = ", ".join(f"{scope.db_schema}.{name}" for name in names)
    await database.status(f"LOCK TABLE {relations} IN ACCESS SHARE MODE NOWAIT")
    for name in names:
        actual_oid = await database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint", relation=f"{scope.db_schema}.{name}"
        )
        if actual_oid != scope.owned_oids[name]:
            raise RuntimeError("entity-address owned stage changed")


async def _finish_owned_operation(operation):
    """Drain a bounded DDL ownership transition before propagating cancellation."""
    task = asyncio.create_task(_bounded_owned_operation(operation))
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation


async def _bounded_owned_operation(operation):
    """Keep cancellation shielding finite even if DDL or its admission check stalls."""
    async with asyncio.timeout(30):
        await operation()


async def create_stage_table(db_schema, stage_cls, *, unlogged=False):
    """Create the native model exactly once and capture its OID before releasing DDL locks."""
    native = _native()
    name = stage_cls.__tablename__
    if _ADMISSION.get() is None:
        await native.db.status(f"DROP TABLE IF EXISTS {db_schema}.{name};")
        await native.db.create_table(stage_cls.__table__, checkfirst=True)
        if unlogged:
            await native.db.status(native._set_unlogged_table_sql(db_schema, name))
        return

    async def create(session):
        """Create the model on the session that owns both DDL and its OID capture."""
        connection = await session.connection()
        await connection.run_sync(stage_cls.__table__.create, checkfirst=False)
        await native.db.status(native._set_unlogged_table_sql(db_schema, name))

    await _finish_owned_operation(lambda: _create_owned_relation(db_schema, name, create))


async def create_stage_sql(db_schema, name, statement, **phase_options):
    """Keep ordinary CREATE behavior while making admitted DDL and ownership atomic."""
    native = _native()
    if _ADMISSION.get() is None:
        await native.db.status(f"DROP TABLE IF EXISTS {db_schema}.{name};")
        if phase_options:
            await native._run_sql_phase(statement, **phase_options)
        else:
            await native.db.status(statement)
        return

    async def create(_session):
        """Keep admitted scratch unlogged until the complete family passes promotion."""
        await native.db.status(re.sub(r"^(\s*CREATE\s+)TABLE\b", r"\1UNLOGGED TABLE", statement, count=1))

    await _finish_owned_operation(lambda: _create_owned_relation(db_schema, name, create))


async def _create_owned_relation(db_schema, name, create):
    """Never adopt or delete an existing relation when a candidate name collides."""
    scope, native = _ADMISSION.get(), _native()
    db_schema, name = _identifier(db_schema), _identifier(name)
    if scope.db_schema not in (None, db_schema) or name in scope.owned_oids:
        raise RuntimeError("entity-address candidate stage ownership changed")
    scope.db_schema = db_schema
    async with _worker_slot():
        await check_owned_storage()
        async with native.db.transaction() as session:
            await native._apply_entity_address_transaction_settings()
            await create(session)
            oid = await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{db_schema}.{name}")
            if oid is None:
                raise RuntimeError("entity-address created stage is missing")
            scope.owned_oids[name] = int(oid)
        await scope.admission.register_external_relation(_fhir(), db_schema, name, int(oid))


async def _lock_owned_stage(db_schema, name):
    """Hold an exclusive native DDL lock and require the creation-time physical identity."""
    scope, native = _ADMISSION.get(), _native()
    expected_oid = scope.owned_oids.get(name)
    if scope.db_schema != db_schema or expected_oid is None:
        raise RuntimeError("entity-address stage was not created by this build")
    await native.db.status("SET LOCAL lock_timeout='500ms'")
    await native.db.status("SET LOCAL statement_timeout='5s'")
    await native.db.status(f"LOCK TABLE {db_schema}.{_identifier(name)} IN ACCESS EXCLUSIVE MODE NOWAIT")
    actual_oid = await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{db_schema}.{name}")
    if actual_oid != expected_oid:
        raise RuntimeError("entity-address owned stage changed")
    return expected_oid


async def drop_stage(db_schema, name):
    """Retire an intermediate heap only under its exact captured physical ownership."""
    if _ADMISSION.get() is None:
        await _native().db.status(f"DROP TABLE IF EXISTS {db_schema}.{name};")
        return
    await _finish_owned_operation(lambda: _drop_owned_relation(db_schema, name))


async def _drop_owned_relation(db_schema, name):
    """Commit removal before forgetting the owned OID or its storage registration."""
    scope, native = _ADMISSION.get(), _native()
    async with native.db.transaction():
        oid = await _lock_owned_stage(db_schema, name)
        await native.db.status(f"DROP TABLE {db_schema}.{name} RESTRICT")
    await scope.admission.retire_external_relation(_fhir(), db_schema, name, oid)
    scope.owned_oids.pop(name)


async def replace_compacted_stage(db_schema, stage, compact, context):
    """Replace only the two owned heaps, with one admitted atomic DROP/rename transition."""
    native = _native()
    if _ADMISSION.get() is None:
        for statement in (f"DROP TABLE {db_schema}.{stage};", f"ALTER TABLE {db_schema}.{compact} RENAME TO {stage};"):
            await native._run_sql_phase(
                statement, context=context, phase="entity-address-unified compacting hot rows swap"
            )
        return
    await _finish_owned_operation(lambda: _replace_owned_relation(db_schema, stage, compact))


async def _replace_owned_relation(db_schema, stage, compact):
    """Keep both possible names through commit acknowledgement for failure cleanup."""
    scope, native = _ADMISSION.get(), _native()
    async with native.db.transaction():
        for name in sorted((stage, compact)):
            await _lock_owned_stage(db_schema, name)
        old_oid, new_oid = scope.owned_oids[stage], scope.owned_oids[compact]
        scope.retired_oids.extend(((stage, stage, old_oid), (compact, compact, new_oid)))
        await native.db.status(f"DROP TABLE {db_schema}.{stage} RESTRICT")
        await scope.admission.retire_external_relation(_fhir(), db_schema, stage, old_oid)
        await native.db.status(f"ALTER TABLE {db_schema}.{compact} RENAME TO {stage}")
        await scope.admission.rename_external_relation(_fhir(), db_schema, compact, stage, new_oid)
        scope.owned_oids[stage] = scope.owned_oids.pop(compact)


@asynccontextmanager
async def _worker_slot():
    """Share one signed worker bound across nested source, shard, and index producers."""
    scope = _ADMISSION.get()
    if scope is None:
        yield
        return
    task = asyncio.current_task()
    if task in scope.worker_tasks:
        if _native().db._transaction_binding() is None:
            raise RuntimeError("entity-address nested work requires its owned backend")
        yield
        return
    async with scope.workers:
        scope.worker_tasks.add(task)
        try:
            yield
        finally:
            scope.worker_tasks.remove(task)


async def gather(*operations, return_exceptions=False):
    """Drain failed admitted sibling producers before their owner can remove scratch."""
    if _ADMISSION.get() is None:
        return await asyncio.gather(*operations, return_exceptions=return_exceptions)
    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(_gather_result(operation, return_exceptions)) for operation in operations]
    return [task.result() for task in tasks]


async def _gather_result(operation, return_exceptions):
    """Preserve native per-index exception handling while cancellation drains the group."""
    try:
        return await operation
    except Exception as error:
        if return_exceptions:
            return error
        raise


async def stage_status(statement, **params):
    """Apply captured ownership to direct native DDL and the nonchunked load path."""
    if _ADMISSION.get() is None:
        return await _native().db.status(statement, **params)
    return await tuned_status(statement, **params)


@asynccontextmanager
async def native_transaction():
    """Count the direct geo projection backend without reacquiring its slot in tuning."""
    async with _worker_slot():
        await check_owned_storage()
        async with _native().db.transaction() as session:
            await lock_owned_relations(_native().db)
            yield session


def sql_settings(settings: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Apply the signed native backend bounds only within admitted preparation."""
    scope = _ADMISSION.get()
    if scope is None:
        return settings
    settings_by_name = dict(settings)
    settings_by_name.update(
        temp_file_limit=f"{scope.admission.plan.temp_file_limit_bytes_per_backend // 1024}kB",
        max_parallel_workers_per_gather="0",
        max_parallel_maintenance_workers="0",
    )
    return list(settings_by_name.items())


def native_sql_settings() -> list[tuple[str, str]]:
    """Keep ordinary native tuning while enforcing admitted execution limits."""
    native = _native()
    candidates = (
        ("work_mem", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM", native.DEFAULT_SQL_WORK_MEM),
        (
            "maintenance_work_mem",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAINTENANCE_WORK_MEM",
            native.DEFAULT_SQL_MAINTENANCE_WORK_MEM,
        ),
        ("temp_file_limit", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT", native.DEFAULT_SQL_TEMP_FILE_LIMIT),
        ("lock_timeout", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_LOCK_TIMEOUT", native.DEFAULT_SQL_LOCK_TIMEOUT),
        ("statement_timeout", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STATEMENT_TIMEOUT", native.DEFAULT_SQL_STATEMENT_TIMEOUT),
        (
            "synchronous_commit",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SYNCHRONOUS_COMMIT",
            native.DEFAULT_SQL_SYNCHRONOUS_COMMIT,
        ),
        ("jit", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_JIT", native.DEFAULT_SQL_JIT),
        ("max_parallel_workers_per_gather", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER", None),
    )
    return sql_settings(
        [
            (setting, setting_value)
            for setting, env_name, default in candidates
            if (setting_value := native._env_sql_setting(env_name, default)) is not None
        ]
    )


async def verify_sql_settings(database) -> None:
    """Verify the actual executing backend after even a denied SET LOCAL attempt."""
    scope = _ADMISSION.get()
    if scope is None:
        return
    bounded = await database.scalar(
        "SELECT pg_size_bytes(current_setting('temp_file_limit')) = :temp_bytes "
        "AND current_setting('max_parallel_workers_per_gather')::int = 0 "
        "AND current_setting('max_parallel_maintenance_workers')::int = 0",
        temp_bytes=scope.admission.plan.temp_file_limit_bytes_per_backend,
    )
    if bounded is not True:
        raise RuntimeError("entity-address admitted executing-session settings changed")


async def tuned_status(statement: str, **params) -> int | None:
    """Reuse native statement execution under the shared admitted worker bound."""
    async with _worker_slot():
        await check_owned_storage()
        return await _execute_tuned_status(statement, **params)


async def _execute_tuned_status(statement, **params):
    """Preserve legacy savepoint tuning and verify admitted settings before executing SQL."""
    native, database = _native(), _native().db
    settings = native._entity_address_sql_settings()
    transaction_binding = getattr(database, "_transaction_binding", None)
    if callable(transaction_binding) and transaction_binding() is not None:
        async with native.entity_address_tuned_transaction(database, settings, native._sql_literal, native.logger):
            await verify_sql_settings(database)
            await lock_owned_relations(database)
            return native._coerce_rowcount(await database.status(statement, **params))
    acquire = getattr(database, "acquire", None)
    if _ADMISSION.get() is not None and not callable(acquire):
        raise RuntimeError("entity-address admitted executing-session binding unavailable")
    if not settings or not callable(acquire):
        await verify_sql_settings(database)
        await lock_owned_relations(database)
        return native._coerce_rowcount(await database.status(statement, **params))
    async with database.acquire() as connection:
        for index, (name, setting_value) in enumerate(settings):
            savepoint = f"entity_address_sql_setting_{index}"
            await connection.status(f"SAVEPOINT {savepoint};")
            try:
                await connection.status(f"SET LOCAL {name} = {native._sql_literal(setting_value)};")
                await connection.status(f"RELEASE SAVEPOINT {savepoint};")
            except Exception as exc:
                await connection.status(f"ROLLBACK TO SAVEPOINT {savepoint};")
                await connection.status(f"RELEASE SAVEPOINT {savepoint};")
                if "permission denied to set parameter" not in str(exc).lower():
                    raise
                native.logger.warning(
                    "Skipping unprivileged entity-address SQL setting %s=%s: %s", name, setting_value, exc
                )
        await verify_sql_settings(connection)
        await lock_owned_relations(connection)
        return native._coerce_rowcount(await connection.status(statement, **params))


async def _bounded_read(database, operation):
    """Keep direct validation reads within the same worker and spill limits as build SQL."""
    native = _native()
    async with _worker_slot():
        async with database.transaction():
            async with native.entity_address_tuned_transaction(
                database, native_sql_settings(), native._sql_literal, native.logger
            ):
                await verify_sql_settings(database)
                await lock_owned_relations(database)
                return await operation()


async def validation_operations(database, *operations):
    """Bound concurrent native validation without changing its ordinary behavior."""
    if _ADMISSION.get() is None:
        return await _native().run_publish_validation_operations(database, *operations)
    if database._transaction_binding() is not None:
        return tuple([await _bounded_read(database, operation) for operation in operations])
    return await gather(*(_bounded_read(database, operation) for operation in operations))


async def _native_read(database, operation):
    """Reuse an owned projection backend; child tasks cannot inherit its session binding."""
    scope = _ADMISSION.get()
    if scope is None and not _native().candidate_preparation.has_prepared_doctors():
        return await operation()
    if database._transaction_binding() is not None and (scope is None or asyncio.current_task() in scope.worker_tasks):
        await verify_sql_settings(database)
        await lock_owned_relations(database)
        return await operation()
    return await _bounded_read(database, operation)


async def read_first(database, statement, **params):
    """Bound native shard and metadata reads on their actual executing sessions."""
    return await _native_read(database, lambda: database.first(statement, **params))


async def read_scalar(database, statement, **params):
    """Keep aggregate validation and source counts inside the signed spill limit."""
    return await _native_read(database, lambda: database.scalar(statement, **params))


async def read_all(database, statement, **params):
    """Keep multirow native validation inside the same backend reservation."""
    return await _native_read(database, lambda: database.all(statement, **params))


async def before_stage_logging(db_schema: str, stage: str) -> None:
    """Check captured aggregate ownership and the signed reservation before native logging."""
    scope = _ADMISSION.get()
    if scope is None:
        return
    if db_schema != scope.db_schema or stage not in {name for _target, name, _oid in scope.stage_oids}:
        raise RuntimeError("entity-address logging stage is not admitted")
    fhir = importlib.import_module("process.provider_directory_fhir")
    await scope.admission.before_logging(fhir, db_schema, stage)


@asynccontextmanager
async def stage_logging_scope(db_schema: str, stage: str):
    """Bind logging to one budgeted backend and the captured physical stage under lock."""
    scope = _ADMISSION.get()
    if scope is None:
        yield
        return
    await before_stage_logging(db_schema, stage)
    native = _native()
    relation = f"{_identifier(db_schema)}.{_identifier(stage)}"
    async with native_transaction():
        async with native.entity_address_tuned_transaction(
            native.db, native_sql_settings(), native._sql_literal, native.logger
        ):
            await verify_sql_settings(native.db)
            await native.db.status(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
            actual_oid = await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=relation)
            if actual_oid != next(oid for _target, name, oid in scope.stage_oids if name == stage):
                raise RuntimeError("entity-address logging stage changed")
            yield
