# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Early original query keys under a live base pin, before final selection.

The borrowed replay feeds the single global SOURCE/base dictionary. It assigns
no ranks and proves neither retained selection nor complete source scope.
"""

import os
import stat
from contextlib import AsyncExitStack, asynccontextmanager, closing
from dataclasses import replace
from types import SimpleNamespace

from sqlalchemy import and_, or_, select

from process.custom_import import family_shard_base_values_sql as sql
from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockUnavailable, resolve_pinned_family_block_binding
from process.custom_import.family_block_keys import _key
from process.custom_import.family_block_materialization import _verify_group
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_block_validate import require_readonly_snapshot
from process.custom_import.family_shard_admission import _digest_file
from process.custom_import.family_shard_base import PinnedBase, _require_deadline
from process.custom_import.family_shard_facts import _Replay, _sorted
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.read_identity import _is_same_read_transaction

_SCOPE = ("dataset_id", "definition_revision_id", "schema_revision_id")


def _scope(base):
    if type(base) is not PinnedBase:
        raise ValueError("base query keys require the live pinned base")
    request, generation = base.request, base.generation
    if (
        generation.generation_id != request.expected_base_generation_id
        or generation.dataset_id != request.dataset_id
        or generation.schema_revision_id != request.schema_revision_id
        or request.expected_pointer_version <= 0
    ):
        raise FamilyBlockUnavailable("base query-key producer scope differs")
    collection = request.definition.query.child_collection
    if collection is not None and collection not in base.definition.collections_by_name:
        raise FamilyBlockUnavailable("base query-key collection is absent from its original schema")
    return collection


def _legacy_statements(base, collection):
    native = sql.tables(base)
    member = native["member"].alias("base_key_member")
    family = native["family"].alias("base_key_family")
    edge = native["edge"].alias("base_key_edge")
    child = native["child"].alias("base_key_child")
    slot = base.registry.child_collection_slots[collection]
    joined = member.outerjoin(family, family.c.family_revision_id == member.c.family_revision_id)
    joined = joined.outerjoin(
        edge, and_(edge.c.family_revision_id == family.c.family_revision_id, edge.c.collection_slot == slot)
    )
    joined = joined.outerjoin(child, child.c.child_revision_id == edge.c.child_revision_id)
    failures = [
        *(member.c[name].is_distinct_from(getattr(base.generation, name)) for name in _SCOPE),
        family.c.family_revision_id.is_(None),
        family.c.dataset_id.is_distinct_from(base.generation.dataset_id),
        family.c.schema_revision_id.is_distinct_from(base.generation.schema_revision_id),
        family.c.root_record_id.is_distinct_from(member.c.root_record_id),
        and_(
            edge.c.child_revision_id.is_not(None),
            or_(
                edge.c.dataset_id.is_distinct_from(base.generation.dataset_id),
                edge.c.schema_revision_id.is_distinct_from(base.generation.schema_revision_id),
                edge.c.root_record_id.is_distinct_from(family.c.root_record_id),
                child.c.child_revision_id.is_(None),
                *(child.c[name].is_distinct_from(getattr(base.generation, name)) for name in _SCOPE),
                child.c.root_record_id.is_distinct_from(edge.c.root_record_id),
                child.c.collection_slot.is_distinct_from(slot),
                child.c.child_key_sha256.is_(None),
                child.c.canonical_child_key.is_(None),
            ),
        ),
    ]
    relation = (
        select(
            child.c.child_key_sha256,
            child.c.canonical_child_key.collate("C").label("canonical_child_key"),
            or_(*failures).label("invalid"),
        )
        .select_from(joined)
        .where(member.c.generation_id == base.generation.generation_id)
        .subquery("base_query_keys")
    )
    guard = select(1).select_from(relation).where(relation.c.invalid).limit(1)
    canonical = relation.c.canonical_child_key.collate("C")
    statement = (
        select(relation.c.child_key_sha256, canonical.label("canonical_child_key"))
        .where(relation.c.child_key_sha256.is_not(None))
        .distinct()
        .order_by(relation.c.child_key_sha256, canonical)
    )
    return guard, statement


def _block_family_statements(base):
    native = base.binding.tables_by_role
    family, member = native["family"], native["generation_family"]
    joined = family.outerjoin(
        member,
        and_(
            member.c.family_revision_id == family.c.family_revision_id,
            member.c.generation_id == base.generation.generation_id,
        ),
    )
    invalid = or_(
        member.c.family_revision_id.is_(None),
        *(member.c[name].is_distinct_from(getattr(base.generation, name)) for name in _SCOPE),
        member.c.root_record_id.is_distinct_from(family.c.root_record_id),
    )
    return select(1).select_from(joined).where(invalid).limit(1), select(family).order_by(family.c.family_revision_id)


async def _guard(base, work, statement):
    await base.check()
    work.current()
    invalid = (await base.session.execute(statement)).first()
    await base.check()
    work.current()
    if invalid is not None:
        raise FamilyBlockUnavailable("base query-key membership or revision scope differs")


@asynccontextmanager
async def _rows(base, work, statement):
    await base.check()
    work.current()
    census = (await base.session.execute(sql.census_statement(statement))).one()
    await base.check()
    if (census.row_count == 0) != (census.row_bytes == 0):
        raise FamilyBlockUnavailable("base query-key native row width census differs")
    fetch = sql.fetch_rows(census, work.limits)
    cursor = await base.session.stream(statement.execution_options(yield_per=fetch))
    observed, primary = SimpleNamespace(count=0, is_complete=False), None

    async def records():
        """Require native EOF and fresh pin identity beyond the last data row."""
        async for row in cursor.mappings():
            await base.check()
            work.current()
            observed.count += 1
            yield dict(row)
        await base.check()
        work.current()
        if observed.count != census.row_count:
            raise FamilyBlockUnavailable("base query-key native cursor EOF census differs")
        observed.is_complete = True

    iterator = records()
    try:
        yield iterator
        if not observed.is_complete:
            raise FamilyBlockUnavailable("base query-key native cursor was not exhausted")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(iterator.aclose(), primary)
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    await base.check()


async def _binding(base):
    observed = await resolve_pinned_family_block_binding(
        base.session, base.pin, base.definition, base.registry.child_collection_slots
    )
    expected = base.binding
    if (
        observed.family_id != expected.family_id
        or observed.layout.digest != expected.layout.digest
        or observed.owner_oid != expected.owner_oid
        or dict(observed.oids_by_role) != dict(expected.oids_by_role)
    ):
        raise FamilyBlockUnavailable("base query-key native block binding changed")
    await base.check()


async def _direct_keys(base, work, target, path, collection):
    is_block = base.binding is not None
    if is_block:
        table = base.binding.tables_by_role["key_dictionary"]
        statement = select(table).order_by(table.c.child_key_sha256, table.c.canonical_child_key.collate("C"))
    else:
        guard, statement = _legacy_statements(base, collection)
        await _guard(base, work, guard)
    previous, rank, variant = None, 0, 0
    async with _rows(base, work, statement) as rows:
        async for row in rows:
            digest, canonical = _key((row["child_key_sha256"], row["canonical_child_key"]))
            order = digest, canonical.encode("utf-8")
            if previous is not None and order <= previous:
                raise FamilyBlockUnavailable("base query keys repeat or are not in byte order")
            if previous is None or digest != previous[0]:
                rank, variant = rank + 1, 1
            else:
                variant += 1
            if is_block and (
                type(row["key_rank"]) is not int
                or type(row["key_variant"]) is not int
                or (row["key_rank"], row["key_variant"]) != (rank, variant)
            ):
                raise FamilyBlockUnavailable("base query-key dictionary rank or variant differs")
            work.frame(target, (digest, canonical), path)
            previous = order


class _FamilyRows:
    def __init__(self, rows):
        self.rows, self.current, self.started = rows, None, False

    async def group(self, family_id, replay):
        """Spill this family only, refusing an unowned native prefix."""
        if not self.started:
            self.current, self.started = await anext(self.rows, None), True
        if self.current is not None and self.current["family_revision_id"] < family_id:
            raise FamilyBlockUnavailable("base query-key block has an orphan role row")
        while self.current is not None and self.current["family_revision_id"] == family_id:
            replay.append(tuple(self.current.values()))
            self.current = await anext(self.rows, None)

    async def finish(self):
        """Require actual role EOF after the last verified family."""
        if not self.started:
            self.current, self.started = await anext(self.rows, None), True
        if self.current is not None:
            raise FamilyBlockUnavailable("base query-key block has trailing role rows")


def _family_keys(base, work, family, fragments, memberships, collection, target, path):
    bound = SimpleNamespace(binding=base.binding, work=work)
    with _verify_group(bound, family, fragments, memberships, work.current) as verified:
        work.charge(verified.manifest.raw_bytes)
        try:
            for record in verified.records(collection):
                work.current()
                _wire, (digest, canonical, _payload) = record_parts(
                    base.definition, collection, record, retained=family["retained"]
                )
                work.frame(target, (digest, canonical), path)
        finally:
            work.used -= verified.manifest.raw_bytes


async def _decoded_keys(base, work, target, path, collection):
    guard, family_statement = _block_family_statements(base)
    await _guard(base, work, guard)
    count, previous = 0, None
    async with AsyncExitStack() as stack:
        groups_by_role = {}
        for role in ("fragment", "revision_run"):
            table = base.binding.tables_by_role[role]
            rows = await stack.enter_async_context(
                _rows(base, work, select(table).order_by(*table.primary_key.columns))
            )
            groups_by_role[role] = _FamilyRows(rows)
        families = await stack.enter_async_context(_rows(base, work, family_statement))
        async for family in families:
            identity = family["family_revision_id"]
            if type(identity) is not int or identity <= 0 or (previous is not None and identity <= previous):
                raise FamilyBlockUnavailable("base query-key family identity or order differs")
            with _Replay(work) as fragments, _Replay(work) as memberships:
                await groups_by_role["fragment"].group(identity, fragments)
                await groups_by_role["revision_run"].group(identity, memberships)
                await _cpu(work, _family_keys, base, work, family, fragments, memberships, collection, target, path)
                await base.check()
            previous, count = identity, count + 1
        for group in groups_by_role.values():
            await group.finish()
    if count != base.seal.family_count:
        raise FamilyBlockUnavailable("base query-key complete family census differs")


def _ordered_keys(work, source):
    previous = None
    with closing(_sorted(work, work.rows(source), lambda row: (row[0], row[1].encode("utf-8")))) as keys:
        for key in keys:
            if key != previous:
                yield key
            previous = key


class _BorrowedKeys:
    def __init__(self, base, connection, work, path):
        self.base, self.connection, self.work, self.path = base, connection, work, path
        self.identity = path.stat()
        self.active, self.readers = True, set()
        with path.open("rb") as source:
            self.sha256 = _digest_file(source, work.limits.memory_bytes, self.check)

    def check(self):
        """Refuse replay after cancellation, closure or any pin replacement."""
        self.work.current()
        if not self.active:
            raise FamilyBlockUnavailable("base query-key replay outlived its owner context")
        if self.base is not None:
            _require_deadline(self.base._deadline)
            session, connection = self.base.session, self.connection
            current = (
                session.get_transaction(),
                session.get_nested_transaction(),
                connection.get_transaction(),
                connection.get_nested_transaction(),
            )
            if (
                not self.base._active
                or not _is_same_read_transaction(self.base._transactions, current)
                or any(item is not None and not item.is_active for item in current)
            ):
                raise FamilyBlockUnavailable("base query-key replay pin transaction changed")

    def _rows(self):
        self.check()
        root, observed = self.work.root.lstat(), self.path.lstat()
        if not stat.S_ISDIR(root.st_mode) or (root.st_dev, root.st_ino) != (
            self.work.identity.st_dev,
            self.work.identity.st_ino,
        ):
            raise FamilyBlockUnavailable("base query-key replay directory custody differs")
        with os.fdopen(os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            opened = os.fstat(source.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (self.identity.st_dev, self.identity.st_ino, self.identity.st_size)
                or (observed.st_dev, observed.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise FamilyBlockUnavailable("base query-key replay file custody differs")
            if _digest_file(source, self.work.limits.memory_bytes, self.check) != self.sha256:
                raise FamilyBlockUnavailable("base query-key replay bytes changed")
            source.seek(0)
            with closing(self.work.read(source)) as reader:
                for key in reader:
                    self.check()
                    yield key
            self.check()
            source.seek(0)
            if _digest_file(source, self.work.limits.memory_bytes, self.check) != self.sha256:
                raise FamilyBlockUnavailable("base query-key replay bytes changed")

    def __call__(self):
        self.check()

        def rows():
            """Release the exact borrowed iterator on EOF, failure or early close."""
            try:
                yield from self._rows()
            finally:
                self.readers.discard(reader)

        reader = rows()
        self.readers.add(reader)
        return reader

    def close(self):
        """Close borrowed readers before removing the exact owned scratch."""
        self.active = False
        for reader in tuple(self.readers):
            reader.close()


@asynccontextmanager
async def pinned_base_query_keys(base, *, work):
    """Stage full original keys before yielding a borrowed synchronous factory.

    Keep this context and the original base context alive through metadata and
    proof. A NULL base yields an empty replay; header EOF remains independent.
    """
    if type(work) is not PlanWork:
        raise ValueError("base query keys require explicit accounted PlanWork")
    collection = None if base is None else _scope(base)
    # Three native cursors, two group replays and the codec share this reservation.
    buffer_bytes = work.limits.memory_bytes if base is None else min(work.limits.memory_bytes, base._buffer_bytes)
    bounded = replace(work, limits=replace(work.limits, memory_bytes=max(1, buffer_bytes // 8)))
    scratch, borrowed, primary = _Scratch(bounded), None, None
    try:
        connection = None
        if base is not None:
            await base.check()
            await require_readonly_snapshot(base.session)
            connection = await base.session.connection()
            await base.check()
            if base.binding is not None:
                await _binding(base)
        path = scratch.path()
        with path.open("xb") as key_file:
            if collection is not None:
                if base.binding is None or collection == base.definition.query.child_collection:
                    await _direct_keys(base, scratch, key_file, path, collection)
                else:
                    await _decoded_keys(base, scratch, key_file, path, collection)
        if base is not None:
            await base.check()
        ordered = await _cpu(scratch, scratch.write, _ordered_keys(scratch, path))
        scratch.remove(path)
        ordered.chmod(0o400)
        borrowed = await _cpu(scratch, _BorrowedKeys, base, connection, scratch, ordered)
        yield borrowed
        borrowed.check()
        if base is not None:
            await base.check()
    except BaseException as error:
        primary = error
    if borrowed is not None:
        try:
            borrowed.close()
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
