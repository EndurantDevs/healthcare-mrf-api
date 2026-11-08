# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pinned owner-side base headers; retained values still require their full reader.

The caller owns its live writer authority, heartbeat and final pointer check.
This read-only context proves the original base scope, never permission to copy
arbitrary headers or to publish the replacement.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import BigInteger, case, func, or_, select, text

from db.models import custom_import as models
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import import publication
from process.custom_import.build_source import (
    SourceBuildRequest,
    _close_contexts,
    _drain_cleanup,
    _enter_context,
    _set_timeout,
)
from process.custom_import.continuation_runtime import processing_deadline
from process.custom_import.family_block_binding import FamilyBlockUnavailable, resolve_pinned_family_block_binding
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_plan import BaseFamilyHeader
from process.custom_import.query_projection_finality import validate_sealed_projection
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_identity import (
    _is_same_read_transaction,
    _read_transaction_identity,
    resolve_generation_snapshot_binding,
    verified_definition,
)
from process.custom_import.runner_codec import digest_text, root_key_contract_hash
from process.custom_import.runner_registry import load_registry
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_tables

_SCOPE = ("dataset_id", "definition_revision_id", "schema_revision_id")
_HEADER_FIXED_BYTES = 2048
_HEADER_KEY_BYTES_MULTIPLIER = 8
_MAX_HEADER_FETCH_ROWS = 4096
_HEADER_MODELS = {
    "member": models.CustomImportGenerationFamily,
    "root": models.CustomImportRootRecord,
    "entity": models.CustomImportEntityBinding,
    "family": models.CustomImportFamilyRevision,
    "revision": models.CustomImportRootRevision,
}


def _read_deadline(request):
    now, started = datetime.now(timezone.utc), time.monotonic()
    deadlines = [request.operation_deadline] if request.operation_deadline is not None else []
    deadlines.extend(
        started + (expiry - now).total_seconds()
        for expiry in (processing_deadline(request), request.authorization_expires_at)
        if expiry is not None
    )
    return min(deadlines) if deadlines else None


def _require_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("base reading exceeded its existing operation deadline")


async def _base_identity(session, request):
    generation = await session.get(models.CustomImportGeneration, request.expected_base_generation_id)
    if generation is None or (generation.generation_id, generation.dataset_id, generation.schema_revision_id) != (
        request.expected_base_generation_id,
        request.dataset_id,
        request.schema_revision_id,
    ):
        raise FamilyBlockUnavailable("base generation scope differs")
    seal = await session.get(models.CustomImportGenerationSeal, generation.generation_id)
    producer = await session.get(models.CustomImportExecution, generation.execution_id)
    if seal is None or producer is None or producer.dataset_id != generation.dataset_id:
        raise FamilyBlockUnavailable("base generation seal or producer is missing")
    publication._validate_generation_seal_identity(seal, generation)
    publication._validate_generation_producer(producer, generation, seal)
    if (
        seal.generation_id != generation.generation_id
        or seal.root_count != generation.root_count
        or seal.family_count != generation.family_count
        or seal.generation_family_count != seal.family_count
        or seal.root_count != seal.family_count
    ):
        raise FamilyBlockUnavailable("base generation header census differs")
    await publication._validated_source_bundle_digest(session, generation)
    return generation, seal


async def _base_definition(session, generation):
    revision = await session.get(models.CustomImportDefinitionRevision, generation.definition_revision_id)
    schema = await session.get(models.CustomImportSchemaRevision, generation.schema_revision_id)
    if (
        revision is None
        or schema is None
        or revision.dataset_id != generation.dataset_id
        or schema.dataset_id != generation.dataset_id
        or revision.schema_revision_id != generation.schema_revision_id
    ):
        raise FamilyBlockUnavailable("base definition scope differs")
    definition = verified_definition(revision, schema)
    registry = await load_registry(
        session,
        SimpleNamespace(**{name: getattr(generation, name) for name in _SCOPE}, definition=definition),
    )
    return definition, registry


async def _canonical_names(session):
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    quote = connection.dialect.identifier_preparer
    names = []
    for model in SNAPSHOT_MODELS:
        schema = schema_map.get(model.__table__.schema, model.__table__.schema)
        if not schema:
            raise FamilyBlockUnavailable("canonical base requires its explicit control schema")
        names.append(quote.quote_schema(schema) + "." + quote.quote(model.__tablename__))
    return names


async def _pin_canonical_tables(session):
    names = await _canonical_names(session)
    await session.execute(text("LOCK TABLE " + ",".join(names) + " IN ACCESS SHARE MODE"))


async def _base_binding(session, generation, definition, registry):
    target = PinnedReadTarget(
        **{name: getattr(generation, name) for name in _SCOPE},
        generation_id=generation.generation_id,
        profile_id="default",
    )
    pin = await resolve_generation_snapshot_binding(session, target)
    binding = None
    if pin.family_id is None:
        await _pin_canonical_tables(session)
        # The old sealed-base helper reuses the same explicit native NULL branch.
        checked = await resolve_generation_snapshot_binding(session, target)
        if checked.family_id is not None:
            raise FamilyBlockUnavailable("canonical base pin changed")
        pin = checked
    elif not pin.legacy_snapshot:
        binding = await resolve_pinned_family_block_binding(session, pin, definition, registry.child_collection_slots)
        if binding is None:
            raise FamilyBlockUnavailable("base block layout is unresolved")
    return pin, binding


def _checked_oids(observed, expected_slots):
    if (
        len(observed) != len(expected_slots)
        or {slot for slot, _oid in observed} != expected_slots
        or any(type(slot) is not int or type(oid) is not int or oid <= 0 for slot, oid in observed)
        or len({oid for _slot, oid in observed}) != len(observed)
    ):
        raise FamilyBlockUnavailable("base native relation identity census differs")
    return {oid for _slot, oid in observed}


async def _registered_oids(session, generation, pin):
    family = await session.get(CustomImportSnapshotFamily, pin.family_id)
    names = (*_SCOPE, "generation_id", "execution_id", "capture_bundle_id", "producing_fence", "producing_token_sha256")
    if (
        family is None
        or family.family_id != pin.family_id
        or family.frozen_at is None
        or any(getattr(family, name) != getattr(generation, name) for name in names)
    ):
        raise FamilyBlockUnavailable("base native snapshot origin differs")
    if pin.legacy_snapshot:
        observed = (
            await session.execute(
                select(CustomImportSnapshotRelation.relation_slot, CustomImportSnapshotRelation.table_oid).where(
                    CustomImportSnapshotRelation.family_id == pin.family_id
                )
            )
        ).all()
        oids = _checked_oids(observed, set(range(1, 16)))
    else:
        oids = set((await pin.checked_block_oids(session)).values())
    origin = family.origin_table_oid
    if origin is not None:
        if type(origin) is not int or origin <= 0 or origin in oids:
            raise FamilyBlockUnavailable("base native origin relation differs")
        oids.add(origin)
    return oids


async def _verify_leaf_pins(session, generation, pin, transactions):
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise FamilyBlockUnavailable("base native pin transaction differs")
    if pin.family_id is None:
        observed = (
            await session.execute(
                text(
                    "SELECT ordinal, pg_catalog.to_regclass(name)::oid::bigint "
                    "FROM pg_catalog.unnest(CAST(:names AS text[])) WITH ORDINALITY AS t(name,ordinal)"
                ),
                {"names": await _canonical_names(session)},
            )
        ).all()
        oids = _checked_oids(observed, set(range(1, len(SNAPSHOT_MODELS) + 1)))
    else:
        oids = await _registered_oids(session, generation, pin)
    observed = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT relation::bigint FROM pg_catalog.pg_locks "
                    "WHERE pid=pg_catalog.pg_backend_pid() AND locktype='relation' AND granted "
                    "AND mode='AccessShareLock' AND relation::bigint=ANY(CAST(:oids AS bigint[])) "
                    "AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())"
                ),
                {"oids": sorted(oids)},
            )
        )
        .scalars()
        .all()
    )
    if not oids.issubset(set(observed)):
        raise FamilyBlockUnavailable("base native leaf locks are incomplete")
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise FamilyBlockUnavailable("base native pin transaction differs")


def _header_tables(pin, binding):
    if binding is not None:
        tables_by_role = {
            role: binding.tables_by_role[name]
            for role, name in (
                ("member", "generation_family"),
                ("root", "root_record"),
                ("entity", "entity_binding"),
                ("family", "family"),
            )
        }
    else:
        physical = None if pin.family_id is None else snapshot_tables(pin.family_id)
        tables_by_role = {
            role: model.__table__ if physical is None else physical[model.__tablename__]
            for role, model in _HEADER_MODELS.items()
        }
    return {role: table.alias("base_" + role) for role, table in tables_by_role.items()}


def _header_relations(tables_by_role):
    member = tables_by_role["member"]
    family = tables_by_role["family"]
    root = tables_by_role["root"]
    entity = tables_by_role["entity"]
    joined = member.outerjoin(family, family.c.family_revision_id == member.c.family_revision_id)
    joined = joined.outerjoin(root, root.c.root_record_id == family.c.root_record_id)
    joined = joined.outerjoin(entity, entity.c.entity_binding_id == family.c.entity_binding_id)
    if "revision" in tables_by_role:
        revision = tables_by_role["revision"]
        joined = joined.outerjoin(revision, revision.c.root_revision_id == family.c.root_revision_id)
    return joined


def _dictionary_mismatch(table, canonical, identity, names):
    expected = canonical.__table__.alias("global_" + identity)
    comparisons = []
    for name in names:
        left, right = expected.c[name], table.c[name]
        if name in {"canonical_logical_key", "canonical_value"}:
            left, right = left.collate("C"), right.collate("C")
        comparisons.append(left.is_not_distinct_from(right))
    return (
        ~select(1)
        .select_from(expected)
        .where(
            expected.c[identity] == table.c[identity],
            *comparisons,
        )
        .exists()
    )


def _header_failures(tables_by_role, generation, definition):
    member = tables_by_role["member"]
    family = tables_by_role["family"]
    root = tables_by_role["root"]
    entity = tables_by_role["entity"]
    failures = [
        *(member.c[name].is_distinct_from(getattr(generation, name)) for name in _SCOPE),
        family.c.family_revision_id.is_(None),
        root.c.root_record_id.is_(None),
        entity.c.entity_binding_id.is_(None),
        family.c.root_record_id.is_distinct_from(member.c.root_record_id),
        root.c.dataset_id.is_distinct_from(generation.dataset_id),
        entity.c.dataset_id.is_distinct_from(generation.dataset_id),
        root.c.key_contract_sha256.is_distinct_from(root_key_contract_hash(definition)),
        _dictionary_mismatch(
            root,
            models.CustomImportRootRecord,
            "root_record_id",
            ("dataset_id", "key_contract_sha256", "logical_key_sha256", "canonical_logical_key"),
        ),
        _dictionary_mismatch(
            entity,
            models.CustomImportEntityBinding,
            "entity_binding_id",
            ("dataset_id", "adapter_id", "canonical_value", "value_sha256"),
        ),
    ]
    if "revision" in tables_by_role:
        revision = tables_by_role["revision"]
        failures.extend(
            (
                family.c.dataset_id.is_distinct_from(generation.dataset_id),
                family.c.schema_revision_id.is_distinct_from(generation.schema_revision_id),
                revision.c.root_revision_id.is_(None),
                revision.c.dataset_id.is_distinct_from(generation.dataset_id),
                revision.c.schema_revision_id.is_distinct_from(generation.schema_revision_id),
                revision.c.root_record_id.is_distinct_from(family.c.root_record_id),
            )
        )
    return failures


def _header_census(tables_by_role, generation, definition):
    member = tables_by_role["member"]
    family = tables_by_role["family"]
    root = tables_by_role["root"]
    return (
        select(
            func.coalesce(func.bool_or(or_(*_header_failures(tables_by_role, generation, definition))), False).label(
                "invalid"
            ),
            func.count().label("family_count"),
            func.coalesce(func.sum(family.c.child_count), 0).cast(BigInteger).label("child_count"),
            func.coalesce(func.max(func.octet_length(root.c.canonical_logical_key)), 0).label("maximum_key_bytes"),
        )
        .select_from(_header_relations(tables_by_role))
        .where(member.c.generation_id == generation.generation_id)
    )


def _header_fetch_rows(census, seal, maximum_bytes, buffer_bytes):
    if census.invalid is not False:
        raise FamilyBlockUnavailable("base membership relationships differ")
    if any(
        type(number) is not int or not 0 <= number < 2**63
        for number in (census.family_count, census.child_count, census.maximum_key_bytes)
    ) or (census.family_count, census.family_count, census.family_count, census.child_count) != (
        seal.root_count,
        seal.family_count,
        seal.generation_family_count,
        seal.family_child_count,
    ):
        raise FamilyBlockUnavailable("base aggregate header census differs")
    if census.family_count == 0:
        if census.maximum_key_bytes != 0:
            raise FamilyBlockUnavailable("empty base key byte census differs")
        return 1
    if census.maximum_key_bytes > maximum_bytes:
        raise FamilyBlockResourceLimit("base header exceeds the existing owner page byte budget")
    # Charge the bounded native/Python row, up to four-byte Python characters,
    # driver bytes and temporary UTF-8 hash/order buffers. This is a resource
    # reservation, not an additional logical key-size or family-count limit.
    charge = _HEADER_FIXED_BYTES + _HEADER_KEY_BYTES_MULTIPLIER * census.maximum_key_bytes
    fetch_rows = min(_MAX_HEADER_FETCH_ROWS, buffer_bytes // charge)
    if fetch_rows < 1:
        raise FamilyBlockResourceLimit("base header exceeds the owner read buffer budget")
    return fetch_rows


def _header_statement(tables_by_role, generation, maximum_bytes, fetch_rows):
    member = tables_by_role["member"]
    family = tables_by_role["family"]
    root = tables_by_role["root"]
    key_bytes = func.octet_length(root.c.canonical_logical_key)
    return (
        select(
            member.c.root_record_id,
            family.c.family_revision_id,
            family.c.root_revision_id,
            family.c.entity_binding_id,
            family.c.family_sha256,
            family.c.child_count,
            root.c.logical_key_sha256,
            case((key_bytes <= maximum_bytes, root.c.canonical_logical_key), else_=None).label("logical_key"),
            key_bytes.label("key_bytes"),
        )
        .select_from(_header_relations(tables_by_role))
        .where(member.c.generation_id == generation.generation_id)
        .order_by(root.c.logical_key_sha256, root.c.canonical_logical_key.collate("C"), member.c.root_record_id)
        .execution_options(yield_per=fetch_rows)
    )


def _checked_header(native, scope, maximum_bytes):
    if type(native.key_bytes) is not int or native.key_bytes < 0:
        raise FamilyBlockUnavailable("base root key byte count differs")
    if native.key_bytes > maximum_bytes:
        raise FamilyBlockResourceLimit("base header exceeds the existing owner page byte budget")
    if (
        any(
            type(number) is not int or not 0 < number < 2**63
            for number in (
                native.root_record_id,
                native.family_revision_id,
                native.root_revision_id,
                native.entity_binding_id,
            )
        )
        or type(native.child_count) is not int
        or not 0 <= native.child_count < 2**63
    ):
        raise FamilyBlockUnavailable("base header identity or count differs")
    if any(
        not isinstance(digest, (bytes, bytearray, memoryview)) or len(digest) != 32
        for digest in (native.logical_key_sha256, native.family_sha256)
    ):
        raise FamilyBlockUnavailable("base header hash shape differs")
    if (
        type(native.logical_key) is not str
        or len(native.logical_key.encode("utf-8")) != native.key_bytes
        or digest_text("root-key", native.logical_key) != bytes(native.logical_key_sha256)
    ):
        raise FamilyBlockUnavailable("base root key exact bytes differ")
    return BaseFamilyHeader(
        (native.logical_key, bytes(native.logical_key_sha256)),
        native.root_record_id,
        native.family_revision_id,
        native.root_revision_id,
        native.entity_binding_id,
        bytes(native.family_sha256),
        native.child_count,
        *scope,
    )


@dataclass
class PinnedBase:
    """Borrowed original generation; only this live context owns its native pin."""

    session: object
    generation: object
    seal: object
    definition: object
    registry: object
    pin: object
    binding: object
    request: object
    _transactions: tuple = field(repr=False)
    _deadline: float | None = field(repr=False)
    _buffer_bytes: int = field(repr=False)
    _active: bool = field(default=True, init=False, repr=False)
    _complete: bool = field(default=False, init=False, repr=False)
    _headers: object = field(default=None, init=False, repr=False)

    @property
    def scope(self):
        """Keep empty canonical and physical bases distinct from absent bases."""
        return self.generation.generation_id, self.pin.family_id

    @property
    def complete(self):
        """Expose header EOF; consumers still check the full live pin before reuse."""
        return (
            self._active
            and self._complete
            and all(transaction is None or transaction.is_active for transaction in self._transactions)
        )

    async def check(self):
        """Refuse reuse after session, root transaction or savepoint replacement."""
        _require_deadline(self._deadline)
        if not self._active or not _is_same_read_transaction(
            self._transactions, await _read_transaction_identity(self.session)
        ):
            raise FamilyBlockUnavailable("base pin transaction is no longer live")

    def headers(self):
        """Open exactly one complete base cursor, including keys absent from SOURCE."""
        if not self._active or self._headers is not None:
            raise FamilyBlockUnavailable("base headers require one live unread cursor")
        self._headers = _stream_headers(self)
        return self._headers


async def _stream_headers(base):
    await base.check()
    tables_by_role = _header_tables(base.pin, base.binding)
    census = (await base.session.execute(_header_census(tables_by_role, base.generation, base.definition))).one()
    await base.check()
    fetch_rows = _header_fetch_rows(census, base.seal, base.request.page_byte_limit, base._buffer_bytes)
    cursor = await base.session.stream(
        _header_statement(tables_by_role, base.generation, census.maximum_key_bytes, fetch_rows)
    )
    primary, previous, count, child_count = None, None, 0, 0
    try:
        async for native in cursor:
            await base.check()
            header = _checked_header(native, base.scope, base.request.page_byte_limit)
            order = header.typed_key[1], header.typed_key[0].encode("utf-8"), header.root_record_id
            if previous is not None and (order <= previous or order[0] == previous[0]):
                raise FamilyBlockUnavailable("base root key order or collision differs")
            previous = order
            count += 1
            child_count += header.child_count
            yield header
        await base.check()
        if (count, count, count, child_count) != (
            base.seal.root_count,
            base.seal.family_count,
            base.seal.generation_family_count,
            base.seal.family_child_count,
        ):
            raise FamilyBlockUnavailable("base complete header census differs")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    await base.check()
    base._complete = True


async def _open_base(session, request, deadline, transactions, buffer_bytes):
    generation, seal = await _base_identity(session, request)
    definition, registry = await _base_definition(session, generation)
    pin, binding = await _base_binding(session, generation, definition, registry)
    await validate_sealed_projection(session, generation, seal, definition, family_block_binding=binding)
    await _verify_leaf_pins(session, generation, pin, transactions)
    base = PinnedBase(
        session, generation, seal, definition, registry, pin, binding, request, transactions, deadline, buffer_bytes
    )
    await base.check()
    return base


@asynccontextmanager
async def pinned_base(session_factory, request, *, buffer_bytes=None):
    """Pin one original base in a read-only repeatable transaction without writer locks.

    Successful exit never substitutes for header EOF or full retained-record
    verification. Current-pointer and writer-epoch checks remain the caller's
    short commit responsibility; its heartbeat runs independently of this read.
    The optional buffer reserves bounded decoded-header memory; it defaults to
    the existing page byte budget and can be enlarged within the owner's budget.
    """
    if type(request) is not SourceBuildRequest:
        raise ValueError("base reading requires its trusted owner request")
    buffer_bytes = request.page_byte_limit if buffer_bytes is None else buffer_bytes
    if type(buffer_bytes) is not int or buffer_bytes <= 0:
        raise ValueError("base read buffer must be a positive byte budget")
    if request.expected_base_generation_id is None:
        if request.expected_pointer_version != 0:
            raise FamilyBlockUnavailable("absent base pointer identity differs")
        yield None
        return
    if request.expected_pointer_version <= 0:
        raise FamilyBlockUnavailable("retained base pointer identity differs")
    deadline = _read_deadline(request)
    _require_deadline(deadline)
    contexts, base, primary = [], None, None
    try:
        async with asyncio.timeout_at(deadline):
            session = await _enter_context(contexts, session_factory())
            if session.in_transaction() or session.new or session.dirty or session.deleted:
                raise FamilyBlockUnavailable("base reading requires its clean owned session")
            await _enter_context(contexts, session.begin())
            transactions = await _read_transaction_identity(session)
            if transactions is None:
                raise FamilyBlockUnavailable("base pin requires its live transaction")
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await _set_timeout(session, request.statement_timeout_ms)
            base = await _open_base(session, request, deadline, transactions, buffer_bytes)
            yield base
            await base.check()
    except BaseException as error:
        primary = error
    if base is not None:
        base._active = False
        if base._headers is not None:
            primary = await _drain_cleanup(base._headers.aclose(), primary)
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary
