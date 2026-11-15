# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Track temporary names for ordinary Address preparation and bounded cleanup."""

import asyncio
import importlib
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from process import entity_address_preparation_admission as admitted
from process import provider_directory_generic_source_policy as source
from process import provider_directory_serving_recovery_receipts as receipts

_ACTIVE = ContextVar("entity_address_ordinary_preparation", default=None)


def _native():
    return importlib.import_module("process.entity_address_unified")


def _require(condition, suffix):
    if not condition:
        raise RuntimeError("entity_address_ordinary_preparation_" + suffix)


def declared_names(native, import_date):
    """Declare the fixed native family and its actual intermediate-name builders."""
    names = {
        native.make_class(model, import_date).__tablename__
        for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS)
    }
    stage = native.make_class(native.EntityAddressUnified, import_date).__tablename__
    names.update(
        builder(stage)
        for builder in (
            native._raw_stage_table_name,
            native._evidence_stage_table_name,
            native._compact_stage_table_name,
            native._partial_affected_group_table,
            native._affected_live_location_table,
            native._provider_directory_replacement_stage_table_name,
        )
    )
    return frozenset(names)


@dataclass
class OrdinaryPreparation:
    """Bookkeeping belongs to its original active owner, including trusted DDL children."""

    database: object
    owner: object
    db_schema: str
    allowed_names: set = field(default_factory=set)
    owned_relations: set = field(default_factory=set)
    retired_relations: list = field(default_factory=list)
    kept_relations: set = field(default_factory=set)
    children: set = field(default_factory=set)
    pending_names: set = field(default_factory=set)
    active: bool = True

    @property
    def cleanup_relations(self):
        """Keep declared names and possible rename destinations through acknowledgement."""
        return tuple((name, name) for name in sorted(self.owned_relations)) + tuple(self.retired_relations)

    def declare(self, names):
        """Register exact internal UUID scratch names before any CREATE, never a name prefix."""
        _require(current() is self, "scope_changed")
        self.allowed_names.update(admitted._identifier(name) for name in names)

    def keep(self, stage_relations):
        """Preserve the declared finalized names after the normal preparation lifecycle."""
        _require(self.active and not self.pending_names, "scope_changed")
        _require(type(stage_relations) is tuple and bool(stage_relations), "finalized_invalid")
        preserved_names = set()
        for target, name in stage_relations:
            admitted._identifier(target)
            admitted._identifier(name)
            _require(name in self.owned_relations and name not in preserved_names, "finalized_changed")
            preserved_names.add(name)
        self.kept_relations = preserved_names


def current():
    """Reject expired inherited scopes rather than falling back to untracked native DDL."""
    owned = _ACTIVE.get()
    if owned is None:
        return None
    _require(
        owned.active and admitted._ADMISSION.get() is None and source._FACTORY.get() is None,
        "scope_changed",
    )
    task = asyncio.current_task()
    if task is not owned.owner and task not in owned.children:
        owned.children.add(task)
        task.add_done_callback(owned.children.discard)
    return owned


async def _ordinary(owned):
    session, _driver, _actor = await receipts.ordinary(owned.database)
    return session


async def _settle_children(owned):
    tasks = tuple(task for task in owned.children if not task.done())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _cleanup(owned, *, preserve_finalized=False):
    candidate = importlib.import_module("process.entity_address_candidate_preparation")
    await admitted._finish_owned_operation(lambda: _settle_children(owned))
    identities = tuple(
        relation
        for relation in owned.cleanup_relations
        if not preserve_finalized or relation[1] not in owned.kept_relations
    )
    await candidate._drain_owned_stages(owned.db_schema, identities)


@asynccontextmanager
async def scope(db_schema, allowed_names):
    """Clean exact failed scratch; preserve finalized heaps only after an explicit keep."""
    _require(
        _ACTIVE.get() is None and admitted._ADMISSION.get() is None and source._FACTORY.get() is None,
        "ordinary_scope_required",
    )
    native = _native()
    _require(native.db._transaction_binding() is None, "outer_transaction_present")
    db_schema = admitted._identifier(db_schema)
    owned = OrdinaryPreparation(native.db, asyncio.current_task(), db_schema)
    token = _ACTIVE.set(owned)
    try:
        owned.declare(allowed_names)
        yield owned
        owned.active = False
        await _cleanup(owned, preserve_finalized=True)
    except BaseException as error:
        owned.active = False
        try:
            await _cleanup(owned)
        except BaseException as cleanup_error:
            raise BaseExceptionGroup(
                "ordinary Address preparation and cleanup failed", [error, cleanup_error]
            ) from error
        raise
    finally:
        owned.active = False
        _ACTIVE.reset(token)


@contextmanager
def _names_in_use(owned, db_schema, names):
    _require(admitted._identifier(db_schema) == owned.db_schema, "schema_changed")
    for name in names:
        admitted._identifier(name)
    _require(set(names) <= owned.allowed_names and not set(names).intersection(owned.pending_names), "name_undeclared")
    owned.pending_names.update(names)
    try:
        yield
    finally:
        owned.pending_names.difference_update(names)


async def _exists(owned, name):
    return await owned.database.scalar(
        "SELECT EXISTS(SELECT FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r' AND NOT relispartition)",
        relation=receipts.qualified(owned.db_schema, name),
    )


async def create(db_schema, name, create_callback):
    """Return the handling scope after atomic candidate creation, or None outside preparation."""
    owned = current()
    if owned is None:
        return None
    with _names_in_use(owned, db_schema, (name,)):
        async with owned.database.transaction():
            session = await _ordinary(owned)
            _require(
                await owned.database.scalar(
                    "SELECT to_regclass(:relation)", relation=receipts.qualified(db_schema, name)
                )
                is None,
                "name_occupied",
            )
            await create_callback(session)
            _require(await _exists(owned, name), "created_stage_missing")
            owned.owned_relations.add(name)
    return owned


async def _lock_owned(owned, name):
    _require(name in owned.owned_relations, "stage_undeclared")
    await owned.database.status(f"LOCK TABLE {receipts.qualified(owned.db_schema, name)} IN ACCESS SHARE MODE NOWAIT")


async def _drop(owned, name):
    async with owned.database.transaction():
        await _ordinary(owned)
        await owned.database.status("SET LOCAL lock_timeout='500ms'")
        await owned.database.status("SET LOCAL statement_timeout='5s'")
        exists = await _exists(owned, name)
        if not exists:
            owned.owned_relations.discard(name)
            return
        await _lock_owned(owned, name)
        owned.retired_relations.append((name, name))
        await owned.database.status(f"DROP TABLE {receipts.qualified(owned.db_schema, name)} RESTRICT")
    owned.owned_relations.discard(name)


async def drop(db_schema, name):
    """Return the handling scope after exact-owned removal, or None outside preparation."""
    owned = current()
    if owned is None:
        return None
    with _names_in_use(owned, db_schema, (name,)):
        await admitted._finish_owned_operation(lambda: _drop(owned, name))
    return owned


async def _replace(owned, stage, compact):
    async with owned.database.transaction():
        await _ordinary(owned)
        await owned.database.status("SET LOCAL lock_timeout='500ms'")
        await owned.database.status("SET LOCAL statement_timeout='5s'")
        for name in sorted((stage, compact)):
            await _lock_owned(owned, name)
        owned.retired_relations.extend(((stage, stage), (compact, compact)))
        await owned.database.status(f"DROP TABLE {receipts.qualified(owned.db_schema, stage)} RESTRICT")
        await owned.database.status(
            f"ALTER TABLE {receipts.qualified(owned.db_schema, compact)} RENAME TO {admitted._identifier(stage)}"
        )
    owned.owned_relations.discard(compact)
    owned.owned_relations.add(stage)


async def replace(db_schema, stage, compact):
    """Return the handling scope after an owned atomic swap, or None outside preparation."""
    owned = current()
    if owned is None:
        return None
    _require(stage != compact, "replacement_invalid")
    with _names_in_use(owned, db_schema, (stage, compact)):
        await admitted._finish_owned_operation(lambda: _replace(owned, stage, compact))
    return owned
