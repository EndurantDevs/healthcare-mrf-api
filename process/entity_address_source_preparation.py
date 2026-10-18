# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep admitted address candidates on their original SOURCE owner until sealed."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from weakref import WeakSet, ref

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source
from process.provider_directory_generic_source_storage import _close_grants

_ISSUED = WeakSet()


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_source_preparation_invalid")


@dataclass(eq=False)
class _SourcePreparation:
    factory: object
    admission: object
    input_hash: str
    task: object
    phase: str = "building"
    prepared: object = None
    capture: object = None
    reader: str = ""
    sequences: dict = field(default_factory=dict)
    original_relations: set = field(default_factory=set)
    validation: object = None
    inputs: object = None


def issue_source_preparation(admission, input_hash):
    """A configured factory and the active signed admission, never a mapping, issue this scope."""
    if not source.is_source_storage_required():
        return None
    issued = _SourcePreparation(_active_factory(admission, input_hash), admission, input_hash, asyncio.current_task())
    _ISSUED.add(issued)
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
        _require(source.bound_publisher().policy is factory.policy)
        yield


def _original(issued, *, phases):
    from process.provider_directory_cms_preparation import _ACTIVE

    _require(type(issued) is _SourcePreparation and issued in _ISSUED and issued.phase in phases)
    _require(source._FACTORY.get() is issued.factory and _ACTIVE.get() is issued.admission)
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
        publisher = source.bound_publisher()
        _require(publisher.policy is issued.factory.policy)
        yield source.source_copy_session()


async def _close_sequences(connection, policy, oid):
    """Close only native sequences owned by this exact created table."""
    sequences = await connection.fetch(
        "SELECT c.oid::bigint,n.nspname,c.relname,c.relowner::bigint FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_depend d "
        "ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.refclassid='pg_class'::regclass "
        "AND d.refobjid=$1::oid AND d.deptype IN ('a','i') WHERE c.relkind='S' ORDER BY c.oid",
        oid,
    )
    owner = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
    for sequence in sequences:
        _require(sequence["nspname"] == policy.schema and sequence["relowner"] == owner)
        grantees = await connection.fetch(
            "SELECT DISTINCT a.grantee,r.rolname FROM pg_class c CROSS JOIN LATERAL "
            "aclexplode(coalesce(c.relacl,acldefault('s',c.relowner))) a "
            "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=$1::oid AND a.grantee<>c.relowner",
            sequence["oid"],
        )
        for grantee in grantees:
            principal = "PUBLIC" if grantee["grantee"] == 0 else storage_identifier(grantee["rolname"])
            await connection.execute(
                f"REVOKE ALL ON SEQUENCE {storage_relation(policy.schema, sequence['relname'])} FROM {principal} CASCADE"
            )
    return tuple((sequence["relname"], sequence["oid"]) for sequence in sequences)


async def register_source_relation(scope, name, oid):
    """Close default grants in the first DDL transaction before any other writer can use them."""
    issued = require_building(scope)
    if issued is None:
        return
    publisher = source.bound_publisher()
    policy, connection = publisher.policy, publisher.connection
    _require(policy is issued.factory.policy and scope.db_schema == policy.schema)
    actual = await connection.fetchrow(
        "SELECT c.oid::bigint,c.relowner::bigint,r.oid::bigint AS owner FROM pg_class c "
        "JOIN pg_roles r ON r.rolname=$2 WHERE c.oid=to_regclass($1) AND c.relkind='r'",
        storage_relation(policy.schema, name),
        policy.owner_role,
    )
    _require(actual is not None and actual["oid"] == oid and actual["relowner"] == actual["owner"])
    await _close_grants(connection, policy, name)
    issued.sequences[name] = await _close_sequences(connection, policy, oid)
    issued.original_relations.add((name, oid))


def require_owned_relation(scope, name, oid):
    """A mutable scope name cannot authorize an unrelated SOURCE table."""
    issued = require_building(scope)
    if issued is not None:
        _require(scope.db_schema == issued.factory.policy.schema and (name, oid) in issued.original_relations)


def record_source_rename(scope, old_name, new_name, oid):
    """Retain both exact names through an acknowledged native rename or rollback."""
    issued = require_building(scope)
    if issued is not None:
        _require((old_name, oid) in issued.original_relations)
        issued.original_relations.add((new_name, oid))
        issued.sequences[new_name] = issued.sequences[old_name]


async def _freeze_native(database, prepared, scope, issued):
    from process import network_registry_cms_prepared_address as address
    from process.mrf_address_publication import require_native_read_catalog
    from process.network_cms_registry_address_equivalence import capture_registry_cms_address_copy_source
    from process.network_registry_cms_prepared_address_pin import _custody
    from process.provider_directory_cms_native_layout import capture_retained_native_source

    publisher = source.bound_publisher()
    policy, connection = publisher.policy, publisher.connection
    stages = address.prepared_address_stages(prepared)
    _require(stages == tuple(sorted(scope.stage_oids)))
    _require(tuple(logical for logical, _, _ in stages) == scope.admission.plan.native_address_targets)
    _require(scope.owned_oids == {name: oid for _, name, oid in stages})
    _require(all((name, oid) in issued.original_relations for _, name, oid in stages))
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for _, name, _ in stages)
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    directory = importlib.import_module("process.provider_directory_fhir")
    _require(directory.db is database)
    measured_by_identity = {
        (entry.relation, entry.oid): entry for entry in await scope.admission.measure(directory, policy.schema)
    }
    for _, name, oid in stages:
        _require((name, oid) in measured_by_identity)
        await capture_retained_native_source(
            directory, measured_by_identity[(name, oid)], scope.admission.plan.native_address_targets
        )
    reader_database = await database._connect_reader()
    reader = reader_database._reader_login[0]
    _require(reader in policy.runtime_roles and reader not in policy.control_runtime_roles)
    for _, name, oid in stages:
        _require(await _close_sequences(connection, policy, oid) == issued.sequences[name])
        await _close_grants(connection, policy, name)
        await connection.execute(
            f"GRANT SELECT ON TABLE {storage_relation(policy.schema, name)} TO {storage_identifier(reader)}"
        )
    await _custody(connection, policy, stages, reader)
    session = source.source_copy_session()
    await require_native_read_catalog(
        session, tuple(oid for _, _, oid in stages), executable_roles=(policy.owner_role, policy.publisher_role)
    )
    capture = await capture_registry_cms_address_copy_source(
        session,
        source_schema=policy.schema,
        expected_relation_oids=tuple((logical, oid) for logical, _, oid in stages),
        stage_table_names={logical: name for logical, name, _ in stages},
    )
    return reader, capture


async def freeze_source_generation(database, prepared, scope):
    """Commit the complete native seven-family handoff before Reader or Ret starts."""
    issued = require_building(scope)
    if issued is None:
        return
    _require(issued.task is asyncio.current_task() and not scope.worker_tasks)
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
    issued.reader, issued.capture, issued.prepared = reader, capture, ref(prepared)
    issued.validation, issued.inputs = deepcopy(prepared.publish_validation), prepared.preparation_input
    issued.phase = "prepared"
    prepared.source_preparation = issued


def require_source_generation(prepared, admission, *, policy=None):
    """A forged PreparedGeneration cannot substitute for the committed issued handoff."""
    issued = _issued(prepared.source_preparation, phases={"prepared"})
    _require(issued.task is asyncio.current_task() and issued.prepared() is prepared and issued.admission is admission)
    _require(policy is None or policy is issued.factory.policy)
    _require(prepared.publish_validation == issued.validation and prepared.preparation_input == issued.inputs)
    _require(issued.capture.relation_oids == tuple((logical, oid) for logical, _, oid in sorted(prepared.stage_oids)))
    return issued.capture


def require_source_cleanup(prepared):
    """Authenticate exact issued cleanup without renewing an expired spend reservation."""
    issued = _original(prepared.source_preparation, phases={"prepared"})
    _require(issued.task is asyncio.current_task() and issued.prepared() is prepared)
    _require(prepared.db_schema == issued.factory.policy.schema)
    _require(issued.capture.relation_oids == tuple((logical, oid) for logical, _, oid in sorted(prepared.stage_oids)))
    return issued


@asynccontextmanager
async def cleanup_transaction(database, issued, *, relation=None):
    """Cleanup keeps the original Factory and exact caller-owned OIDs after scope teardown."""
    if issued is None:
        _require(not source.is_source_storage_required())
        async with database.transaction():
            yield
        return
    _original(issued, phases={"building", "closing", "failed", "prepared"})
    _require(
        type(relation) is tuple
        and len(relation) == 3
        and relation[0] == issued.factory.policy.schema
        and relation[1:] in issued.original_relations
    )
    async with source.source_transaction(database):
        _require(source.bound_publisher().policy is issued.factory.policy)
        yield
