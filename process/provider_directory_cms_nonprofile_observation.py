# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe admitted SOURCE and retained storage on their original actors."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from uuid import UUID
from weakref import WeakSet

from sqlalchemy import text

from db.connection import ConnectionProxy
from process.network_cms_registry_address_capture import NativeReceiptSession, native_driver

_ACTIVE = ContextVar("cms_retained_nonprofile_observation", default=None)
_ISSUED = WeakSet()


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_nonprofile_observation_changed")


class _CatalogObservation:
    """Forward only existing catalog constructors to an explicit read executor."""

    def __init__(self, directory, database):
        self.directory, self.db = directory, database

    def __getattr__(self, name):
        return getattr(self.directory, name)

    async def _profile_capacity_relation_row(self, *args):
        return await self.directory._profile_capacity_relation_row(*args, database=self.db)

    async def _profile_capacity_relation_catalog(self, *args):
        return await self.directory._profile_capacity_relation_catalog(*args, database=self.db)

    async def _profile_bucket_index_row(self, *args):
        return await self.directory._profile_bucket_index_row(*args, database=self.db)

    async def _provider_directory_profile_relation_storage_fingerprint(self, *args, **kwargs):
        return await self.directory._provider_directory_profile_relation_storage_fingerprint(
            *args, **kwargs, database=self.db
        )


def catalog_observation(directory, database):
    """Preserve the historical module unchanged when no executor was supplied."""
    if database is None:
        return directory
    original = directory.directory if type(directory) is _CatalogObservation else directory
    return _CatalogObservation(original, database)


class _ReaderCatalog:
    """Run named catalog SQL on the issued Reader without ambient bindings."""

    def __init__(self, source_input):
        self.source_input = source_input

    async def all(self, statement, **parameters):
        """Require the original Reader before every existing catalog query."""
        await self.source_input.verify_transaction()
        return await NativeReceiptSession(self.source_input.connection).execute(
            text(statement) if isinstance(statement, str) else statement, parameters
        )

    async def first(self, statement, **parameters):
        """Return a single native catalog row without changing its owner."""
        rows = await self.all(statement, **parameters)
        return rows[0] if rows else None

    async def scalar(self, statement, **parameters):
        """Read one native value on the same pinned Reader transaction."""
        row = await self.first(statement, **parameters)
        return next(iter(row.values())) if row else None


async def pinned_raw_attributes(directory, source_input, policy, root):
    """Use an issued closed child, retaining the original parent pin coordinates."""
    from process.network_fhir_source_epoch import _TABLES
    from process.network_fhir_source_epoch_pin import PinnedCMSFHIRSourceEpochInput

    _require(type(source_input) is PinnedCMSFHIRSourceEpochInput and root in _TABLES)
    await source_input.verify_transaction()
    _require_epoch_policy(source_input, policy)
    database = _ReaderCatalog(source_input)
    source_oid = await database.scalar(
        "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:name) AND relpersistence='p'",
        name=source_input.relation(root),
    )
    _require(type(source_oid) is int and source_oid > 0)
    rows = await database.all(directory._PROFILE_CAPACITY_ATTRIBUTE_SQL, relation_oids=[source_oid])
    attributes = [dict(directory._pagination_checkpoint_row_mapping(row)) for row in rows]
    await source_input.verify_transaction()
    return source_oid, attributes


def _require_epoch_policy(source_input, policy):
    """Bind the complete original pin and both actual seals to the signed recipe."""
    from process.network_fhir_source_epoch import _ORIGIN_FIELDS

    _require({name: getattr(source_input.source_pin, name) for name in _ORIGIN_FIELDS} == policy["source_pin"])
    _require(
        (source_input.admission_sha256, source_input.metadata_sha256)
        == (policy["expected_admission_sha256"], policy["expected_metadata_sha256"])
    )


@dataclass(frozen=True)
class _Observers:
    scope: object
    controls: object
    _reader: _ReaderCatalog = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        object.__setattr__(self, "_reader", _ReaderCatalog(self.scope.source_input))

    def database_for(self, relation):
        """Select only an original SOURCE member or an admitted retained root."""
        self.scope.require_active()
        coordinate = (relation.schema, relation.relation)
        if coordinate in self.scope.source_relations:
            _require(self.scope.source_relations[coordinate] == relation.oid)
            if coordinate in self.scope.address_relations:
                return self._reader
            return self.controls
        _require(relation.relation in self.scope.retained_tables.get(relation.schema, ()))
        _require(self.scope.retained_relations.get(coordinate, relation.oid) == relation.oid)
        return None  # The verified original Ret binding owns uncommitted heaps.

    def record(self, relation):
        """Keep every observed native heap in all subsequent aggregate checks."""
        coordinate = (relation.schema, relation.relation)
        if coordinate not in self.scope.source_relations:
            self.scope.retained_relations[coordinate] = relation.oid


class _RetainedObservations:
    """One same-task retained transaction and its already issued SOURCE inputs."""

    def __init__(self, prepared, source_input, address_input, session, factory):
        from process.entity_address_snapshot_source import entity_address_archive_stage_schema
        from process.network_fhir_source_epoch import _TABLES
        from process.provider_directory_cms_preparation import retained_raw_policy

        self.prepared, self.admission = prepared, prepared.nonprofile_admission
        self.directory, self.check_phase = prepared.fhir, self.admission.check_phase
        self.database = prepared.fhir.db
        self.source_input, self.address_input = source_input, address_input
        self.session, self.factory, self.task = session, factory, asyncio.current_task()
        self.source_relations = dict(self.admission._relations)
        self.retained_relations = {}
        self.address_relations = {(prepared.address.db_schema, name): oid for _, name, oid in address_input.stages}
        policy = retained_raw_policy(self.admission.lease)
        _require_epoch_policy(source_input, policy)
        capture = UUID(policy["capture_id"]).hex
        self.retained_tables = {
            "registry_cms_epoch_" + capture: tuple(_TABLES),
            entity_address_archive_stage_schema(UUID(policy["capture_id"])): tuple(policy["address_tables"]),
        }
        self.owner_role = policy["owner_role"]
        self.binding = prepared.fhir.db._transaction_binding()
        self.driver = self.virtual_xid = None

    def require_active(self):
        """Reject unissued, changed-admission and cross-task observation scopes."""
        from process.provider_directory_cms_preparation import _ACTIVE as active_admission

        _require(self in _ISSUED and _ACTIVE.get() is self and self.task is asyncio.current_task())
        _require(self.prepared.nonprofile_admission is self.admission and self.prepared.fhir is self.directory)
        _require(self.directory.db is self.database)
        _require(active_admission.get() is self.admission and self.admission.check_phase == self.check_phase)
        self.require_inventory()
        _require(self.prepared.fhir.db._transaction_binding() is self.binding and self.binding.session is self.session)
        self.source_input.require_active()
        self.address_input.require_active()
        self.admission._assert_lease()

    def require_inventory(self):
        """Never lose an original or already measured retained member from the budget."""
        _require(all(self.admission._relations.get(key) == oid for key, oid in self.source_relations.items()))
        _require(all(self.admission._relations.get(key) == oid for key, oid in self.retained_relations.items()))

    async def verify_transaction(self):
        """Keep the original Ret binding, actor and virtual transaction unchanged."""
        from process.network_fhir_source_epoch_pin import _virtual_xid

        self.require_active()
        _require(await native_driver(self.session) is self.driver)
        _require(await _virtual_xid(self.driver) == self.virtual_xid)
        await self.source_input.verify_transaction()
        await self.source_input.require_target(self.driver, self.owner_role)

    @asynccontextmanager
    async def observers(self):
        """Open a short authenticated SOURCE observation without ambient binding."""
        from process.network_fhir_source_epoch_pin import _same_database, _virtual_xid

        await self.verify_transaction()
        async with self.factory.observation_transaction(isolation="read committed") as session:
            connection = await native_driver(session)
            policy = self.source_input._receipt()["policy"]
            _require(connection is not self.driver and connection is not self.source_input.connection)
            actor = await connection.fetchrow(
                "SELECT session_user::text AS login,current_user::text AS actor,"
                "current_setting('transaction_read_only') AS read_only,"
                "current_setting('transaction_isolation') AS isolation"
            )
            _require(
                dict(actor)
                == dict(
                    login=policy.publisher_role, actor=policy.owner_role, read_only="on", isolation="read committed"
                )
            )
            await _same_database(connection, self.driver)
            transaction = await _virtual_xid(connection)
            yield _Observers(self, ConnectionProxy(None, session, connection))
            _require(await _virtual_xid(connection) == transaction)
            await self.verify_transaction()
        await self.verify_transaction()


def active_observations(*, admission=None, producer=None):
    """Expose only the issuing admission, never a caller-constructed actor map."""
    scope = _ACTIVE.get()
    if scope is None:
        return None
    scope.require_active()
    _require(
        admission is scope.admission if admission is not None else scope.admission.check_phase == producer.check_phase
    )
    return scope


@asynccontextmanager
async def retained_nonprofile_observations(prepared, source_input, address_input, session):
    """Keep SOURCE observations separate from the original retained writer."""
    if source_input is None:
        from process.provider_directory_generic_source_policy import is_source_storage_required

        _require(address_input is None)
        _require(not is_source_storage_required())
        yield
        return
    from process.network_fhir_source_epoch_pin import PinnedCMSFHIRSourceEpochInput, _virtual_xid
    from process.network_registry_cms_prepared_address_pin import PinnedRegistryCMSAddressInput
    from process.provider_directory_cms_preparation import _ACTIVE as active_admission
    from process.provider_directory_generic_source_policy import _publication_factory

    _require(_ACTIVE.get() is None and type(source_input) is PinnedCMSFHIRSourceEpochInput)
    _require(type(address_input) is PinnedRegistryCMSAddressInput and address_input.source_input is source_input)
    _require(address_input.prepared is prepared and active_admission.get() is prepared.nonprofile_admission)
    factory = _publication_factory()
    _require(factory.policy is source_input._receipt()["policy"])
    _require(callable(getattr(factory, "observation_transaction", None)))
    scope = _RetainedObservations(prepared, source_input, address_input, session, factory)
    _require(scope.binding is not None and scope.binding.session is session)
    _require(all(type(oid) is int and oid > 0 for oid in scope.source_relations.values()))
    _require(all(scope.source_relations.get(coordinate) == oid for coordinate, oid in scope.address_relations.items()))
    scope.driver = await native_driver(session)
    await source_input.require_target(scope.driver, scope.owner_role)
    scope.virtual_xid = await _virtual_xid(scope.driver)
    _ISSUED.add(scope)
    token = _ACTIVE.set(scope)
    try:
        await scope.verify_transaction()
        yield
        await scope.verify_transaction()
    finally:
        _ACTIVE.reset(token)
        _ISSUED.discard(scope)
