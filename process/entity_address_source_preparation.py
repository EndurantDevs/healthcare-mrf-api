# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep admitted named Address candidates through native freeze and publication."""

import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source
from process.provider_directory_generic_source_storage import _close_grants


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_source_preparation_invalid")


@dataclass(eq=False)
class _SourcePreparation:
    factory: object
    admission: object
    input_hash: str
    phase: str = "building"
    capture: object = None
    reader: str = ""
    original_relations: set = field(default_factory=set)
    validation: object = None
    inputs: object = None
    relations: tuple = ()


def issue_source_preparation(admission, input_hash):
    """Use the configured factory with the active capacity admission and logical input hash."""
    if not source.is_source_storage_required():
        return None
    issued = _SourcePreparation(_active_factory(admission, input_hash), admission, input_hash)
    return issued


def _active_factory(admission, input_hash):
    from process.provider_directory_cms_preparation import _ACTIVE

    _require(_ACTIVE.get() is admission and admission._started)
    admission._assert_lease()
    _require(input_hash == admission.plan.native_address_input_hash)
    return source._publication_factory()


@asynccontextmanager
async def source_dependency_session(database, admission, input_hash):
    """Read fixed signed dependencies through the issued readonly Source observation."""
    from process.provider_directory_source_observation import observation_transaction

    factory = _active_factory(admission, input_hash)
    _require(database._transaction_binding() is None and callable(getattr(factory, "observation_transaction", None)))
    directory = importlib.import_module("process.provider_directory_fhir")
    _require(directory.db is database)
    async with observation_transaction(directory, isolation="repeatable read") as session:
        yield session


@asynccontextmanager
async def source_dependency_transaction(database, admission, input_hash):
    """Bind the original SOURCE session only for existing database-based dependency verifiers."""
    factory = _active_factory(admission, input_hash)
    async with source.source_transaction(database, isolation="repeatable read"):
        yield


def _original(issued, *, phases):
    from process.provider_directory_cms_preparation import _ACTIVE

    _require(type(issued) is _SourcePreparation and issued.phase in phases)
    _require(_ACTIVE.get() is issued.admission)
    _require(issued.input_hash == issued.admission.plan.native_address_input_hash)
    return issued


def _issued(issued, *, phases):
    issued = _original(issued, phases=phases)
    issued.admission._assert_lease()
    return issued


def require_building(scope):
    """Never let a closed candidate reenter a write or preparation operation."""
    issued = scope.source_preparation if scope is not None else None
    if issued is None:
        _require(not source.is_source_storage_required())
        return None
    _require(scope.admission is issued.admission and scope.native_input_hash == issued.input_hash)
    return _issued(issued, phases={"building"})


@asynccontextmanager
async def preparation_transaction(database, scope, *, phases=("building",)):
    """Open one same-task Factory transaction; the historical database path stays unchanged."""
    issued = scope.source_preparation if scope is not None else None
    if issued is None:
        _require(not source.is_source_storage_required())
        async with database.transaction() as session:
            yield session
        return
    _issued(issued, phases=set(phases))
    _require(scope.admission is issued.admission and scope.native_input_hash == issued.input_hash)
    _require(scope.db_schema in (None, issued.factory.policy.schema))
    async with source.source_transaction(database):
        yield source.source_copy_session()


async def _close_sequences(connection, policy, name):
    """Close native sequence grants by the table's named dependency selection."""
    from process.provider_directory_generic_source_provision import _snapshot_freeze_sequences

    sequences = await _snapshot_freeze_sequences(connection, policy.schema, (name,))
    for sequence in sequences:
        await _close_grants(connection, policy, sequence["relname"], sequence=True)
    return tuple((policy.schema, sequence["relname"]) for sequence in sequences)


async def register_source_relation(scope, name):
    """Close default grants before first COPY through the configured native transaction."""
    issued = require_building(scope)
    if issued is None:
        return
    publisher = source.bound_publisher()
    policy, connection = publisher.policy, publisher.connection
    _require(scope.db_schema == policy.schema)
    _require(
        await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE oid=to_regclass($1) AND relkind='r')",
            storage_relation(policy.schema, name),
        )
    )
    await _close_grants(connection, policy, name)
    await _close_sequences(connection, policy, name)
    issued.original_relations.add(name)


def require_owned_relation(scope, name):
    """Limit candidate work to the declared preparation names, with native access enforced by PostgreSQL."""
    issued = require_building(scope)
    if issued is not None:
        _require(scope.db_schema == issued.factory.policy.schema and name in issued.original_relations)


def record_source_rename(scope, old_name, new_name):
    """Retain explicit possible names through native rename acknowledgement or rollback."""
    issued = require_building(scope)
    if issued is not None:
        _require(old_name in issued.original_relations)
        issued.original_relations.add(new_name)


async def _freeze_native(database, prepared, scope, issued):
    from process import network_registry_cms_prepared_address as address
    from process.network_cms_registry_address_equivalence import capture_registry_cms_address_copy_source
    from process.provider_directory_cms_native_layout import capture_retained_native_source
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    publisher = source.bound_publisher()
    policy, connection = publisher.policy, publisher.connection
    stages = address.prepared_address_stages(prepared)
    _require(stages == tuple(sorted(scope.stage_relations)))
    _require(tuple(logical for logical, _ in stages) == scope.admission.plan.native_address_targets)
    names = {name for _, name in stages}
    _require(scope.owned_relations == names and names <= issued.original_relations)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for _, name in stages)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    directory = importlib.import_module("process.provider_directory_fhir")
    measured_by_name = {entry.relation: entry for entry in await scope.admission.measure(directory, policy.schema)}
    for _, name in stages:
        _require(name in measured_by_name)
        await capture_retained_native_source(
            directory, measured_by_name[name], scope.admission.plan.native_address_targets
        )
    reader_database = await database._connect_reader()
    reader = reader_database._reader_login[0]
    await freeze_fhir_source_snapshot(
        connection,
        policy.schema,
        namespace_owner=policy.owner_role,
        reader_roles=tuple(dict.fromkeys((policy.owner_role, policy.publisher_role, *policy.runtime_roles, reader))),
        relation_names=tuple(sorted(names)),
        freeze_schema=False,
    )
    capture = await capture_registry_cms_address_copy_source(
        source.source_copy_session(),
        source_schema=policy.schema,
        expected_relation_names=tuple(logical for logical, _ in stages),
        stage_table_names=dict(stages),
    )
    return reader, capture


async def freeze_source_generation(database, prepared, scope):
    """Commit the complete native seven-family handoff before Reader or Ret starts."""
    issued = require_building(scope)
    if issued is None:
        return
    _require(not scope.worker_tasks)
    _require(database._transaction_binding() is None and bool(prepared.publish_validation))
    _require(
        not prepared.context.get("publish_validation_deferred") and not prepared.publish_validation.get("deferred")
    )
    issued.phase = "closing"
    try:
        async with preparation_transaction(database, scope, phases=("closing",)):
            reader, capture = await _freeze_native(database, prepared, scope, issued)
        await scope.admission.assert_ready(
            importlib.import_module("process.provider_directory_fhir"), prepared.db_schema
        )
    except BaseException:
        issued.phase = "failed"
        raise
    issued.reader, issued.capture, issued.relations = reader, capture, tuple(prepared.stage_relations)
    issued.validation, issued.inputs = deepcopy(prepared.publish_validation), prepared.preparation_input
    issued.phase = "prepared"
    prepared.source_preparation = issued


def require_source_generation(prepared, admission, *, policy=None):
    """Recheck the complete named result and its original logical input and validation."""
    issued = _issued(prepared.source_preparation, phases={"prepared"})
    _require(issued.admission is admission)
    _require(policy is None or policy.schema == prepared.db_schema)
    _require(prepared.publish_validation == issued.validation and prepared.preparation_input == issued.inputs)
    _require(issued.relations == tuple(prepared.stage_relations))
    _require(
        issued.capture.relations
        == tuple((logical, prepared.db_schema, name) for logical, name in sorted(issued.relations))
    )
    return issued.capture


def require_source_cleanup(prepared):
    """Authenticate exact issued cleanup without renewing an expired spend reservation."""
    issued = _original(prepared.source_preparation, phases={"prepared"})
    _require(prepared.db_schema == issued.factory.policy.schema)
    _require(issued.relations == tuple(prepared.stage_relations))
    _require(
        issued.capture.relations
        == tuple((logical, prepared.db_schema, name) for logical, name in sorted(issued.relations))
    )
    return issued


@asynccontextmanager
async def cleanup_transaction(database, issued, *, relation=None):
    """Cleanup retains declared candidate names through transaction and scope teardown."""
    if issued is None:
        _require(not source.is_source_storage_required())
        async with database.transaction():
            yield
        return
    _original(issued, phases={"building", "closing", "failed", "prepared"})
    _require(
        type(relation) is tuple
        and len(relation) == 2
        and relation[0] == issued.factory.policy.schema
        and relation[1] in issued.original_relations
    )
    async with source.source_transaction(database):
        yield
