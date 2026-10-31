# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep ordinary affected candidates on their original admitted Builder lifecycle."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from weakref import WeakSet, ref

_ISSUED = WeakSet()


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_ordinary_preparation_invalid")


@dataclass(eq=False)
class OrdinaryPreparation:
    database: object
    admission: object
    capture: object
    input_hash: str
    task: object
    engine: object
    phase: str = "building"
    prepared: object = None
    validation: object = None
    inputs: object = None
    actor: str | None = None
    cleanup_oids: tuple = ()
    original_relations: set = field(default_factory=set)


def is_ordinary(issued):
    """Distinguish the original Builder carrier from the unchanged CMS SOURCE carrier."""
    return type(issued) is OrdinaryPreparation


def issue(admission, input_hash):
    """Issue only an exact ordinary plan with its original server capture."""
    from process.provider_directory_ordinary_address_admission import require_capture
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan

    if type(admission.plan) is not OrdinaryServingAdmissionPlan:
        return None
    directory = importlib.import_module("process.provider_directory_fhir")
    capture = require_capture(directory, admission.plan, admission.captured_inputs)
    _require(input_hash == admission.plan.native_address_input_hash and bool(capture.address_task))
    admission._assert_lease()
    issued = OrdinaryPreparation(
        directory.db, admission, capture, input_hash, asyncio.current_task(), directory.db.engine
    )
    _ISSUED.add(issued)
    return issued


def require_original(issued, phases):
    """Require the same database, engine, active admission and issued lifecycle phase."""
    from process.provider_directory_cms_preparation import _ACTIVE
    from process.provider_directory_ordinary_address_admission import require_capture

    _require(is_ordinary(issued) and issued in _ISSUED and issued.phase in phases)
    directory = importlib.import_module("process.provider_directory_fhir")
    _require(directory.db is issued.database and issued.database.engine is issued.engine)
    _require(_ACTIVE.get() is issued.admission and issued.admission._started)
    _require(require_capture(directory, issued.admission.plan, issued.admission.captured_inputs) is issued.capture)
    _require(issued.input_hash == issued.admission.plan.native_address_input_hash)
    return issued


def require_building(scope):
    """Permit writes only while the original admitted candidate remains building."""
    issued = require_original(scope.source_preparation, {"building"})
    _require(scope.admission is issued.admission and scope.native_input_hash == issued.input_hash)
    issued.admission._assert_lease()
    return issued


@asynccontextmanager
async def transaction(database, scope, phases):
    """Borrow only a bounded original Builder transaction and verify its actor."""
    from process.provider_directory_cms_preparation import nonprofile_sql_transaction

    issued = require_original(scope.source_preparation, set(phases))
    _require(database is issued.database)
    issued.admission._assert_lease()
    directory = importlib.import_module("process.provider_directory_fhir")
    async with nonprofile_sql_transaction(directory, issued.admission):
        actor = await database.scalar("SELECT current_user::text")
        if issued.actor is None:
            issued.actor = actor
        _require(type(actor) is str and bool(actor) and actor == issued.actor)
        binding = database._transaction_binding()
        _require(binding is not None)
        yield binding.session


async def register(scope, schema, name, oid):
    """Record a newly created native heap owned by the actual Builder actor."""
    issued = require_building(scope)
    database = issued.database
    actual = await database.scalar(
        "SELECT c.oid::bigint FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner "
        "WHERE c.oid=to_regclass(:relation) AND c.relkind='r' AND r.rolname=current_user",
        relation=f"{schema}.{name}",
    )
    _require(actual == oid)
    issued.original_relations.add((name, oid))


def require_owned(scope, name, oid):
    """Reject a relation name or OID not created by this build."""
    issued = require_building(scope)
    _require((name, oid) in issued.original_relations)


def renamed(scope, old_name, new_name, oid):
    """Keep the exact acknowledged OID through an owned native rename."""
    issued = require_building(scope)
    _require((old_name, oid) in issued.original_relations)
    issued.original_relations.add((new_name, oid))


async def freeze(database, prepared, scope):
    """Close the ordinary candidate lifecycle without claiming a CMS snapshot proof."""
    from process.entity_address_affected_patch import require_descriptor

    issued = require_building(scope)
    _require(database is issued.database and issued.task is asyncio.current_task() and not scope.worker_tasks)
    _require(database._transaction_binding() is None and bool(prepared.publish_validation))
    _require(
        not prepared.context.get("publish_validation_deferred") and not prepared.publish_validation.get("deferred")
    )
    descriptor = require_descriptor(prepared)
    _require(descriptor.admission is issued.admission and descriptor.capture is issued.capture)
    _require(set(scope.owned_oids.items()) == {(name, oid) for _logical, name, oid in descriptor.owned_relations})
    _require(all((name, oid) in issued.original_relations for _logical, name, oid in descriptor.owned_relations))
    issued.phase = "closing"
    try:
        async with transaction(database, scope, ("closing",)):
            await database.status(
                "LOCK TABLE "
                + ",".join(f"{prepared.db_schema}.{name}" for _logical, name, _oid in descriptor.owned_relations)
                + " IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            await assert_relations(database, descriptor)
        await issued.admission.assert_ready(
            importlib.import_module("process.provider_directory_fhir"), prepared.db_schema
        )
    except BaseException:
        issued.phase = "failed"
        raise
    issued.validation, issued.inputs, issued.prepared = (
        deepcopy(prepared.publish_validation),
        prepared.preparation_input,
        ref(prepared),
    )
    issued.cleanup_oids = tuple(scope.cleanup_oids)
    issued.phase = "prepared"
    prepared.source_preparation = issued


async def assert_relations(database, descriptor):
    """Verify every actual candidate and scope OID without live-name fallback."""
    for _logical, name, oid in descriptor.owned_relations:
        actual = await database.scalar(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r'",
            relation=f"{descriptor.schema}.{name}",
        )
        _require(actual == oid)


def require_prepared(prepared, admission):
    """Bind the prepared result to its original task, validation and inputs."""
    issued = require_original(prepared.source_preparation, {"prepared"})
    _require(issued.task is asyncio.current_task() and issued.prepared() is prepared and issued.admission is admission)
    _require(prepared.publish_validation == issued.validation and prepared.preparation_input == issued.inputs)
    return issued


def require_cleanup(prepared):
    """Authenticate cleanup without renewing an expired spending lease."""
    issued = require_original(prepared.source_preparation, {"prepared"})
    _require(issued.task is asyncio.current_task() and issued.prepared() is prepared)
    return issued


@asynccontextmanager
async def cleanup_transaction(database, issued, relation):
    """Drop only exact original owned relations on the same Builder actor."""
    require_original(issued, {"building", "closing", "failed", "prepared"})
    _require(database is issued.database and type(relation) is tuple and len(relation) == 3)
    _require(relation[1:] in issued.original_relations)
    async with database.transaction():
        actor = await database.scalar("SELECT current_user::text")
        _require(actor == issued.actor)
        yield
