# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded sealed-block API reads and a separate full family verification path."""

import asyncio
import math
import struct
from collections import OrderedDict
from dataclasses import dataclass
from datetime import timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext
from types import SimpleNamespace

import msgpack
from sqlalchemy import BigInteger, and_, column, func, or_, select, true, union_all, values

from process.custom_import.family_block_codec import FamilyBlockError, FamilyBlockLimits
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_read_sql import field_scalar_model
from process.custom_import.family_block_records import record_from_wire
from process.custom_import.family_block_scalar_layout import query_scalar_fields
from process.custom_import.family_block_storage_codec import decode_fragment, verify_stored_family
from process.custom_import.read_contracts import MAX_DETAIL_CHILDREN, CustomImportReadUnavailableError

_DIRECTORY = (
    "fragment_ordinal",
    "raw_bytes",
    "first_record_ordinal",
    "starting_record_count",
    "first_record_offset",
    "continuation_bytes",
)
_MEMBERSHIP = (
    "collection_slot",
    "first_revision_id",
    "last_revision_id",
    "record_count",
    "first_record_ordinal",
    "encoded_membership",
)
_LANES = {
    "string": "string_value",
    "integer": "integer_value",
    "decimal": "decimal_value",
    "boolean": "boolean_value",
    "date": "date_value",
    "timestamp": "timestamp_value",
}
_LENGTH = struct.Struct("!Q")


@dataclass(frozen=True)
class FamilyBlockReadLimits:
    rows: int = 4096
    bytes: int = 8 * 1024 * 1024
    max_record_bytes: int = 268_435_456
    fragment_bytes: int = 64 * 1024

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 for value in vars(self).values()):
            raise ValueError("family read budgets differ")

    def codec_limits(self):
        """Retain the original singleton allowance and bounded fragment budget."""
        return FamilyBlockLimits(
            max(self.bytes, self.max_record_bytes * 2),
            self.max_record_bytes,
            self.fragment_bytes,
            min(self.bytes, 1024 * 1024),
        )


def _binding(context):
    binding = context.family_block_binding
    if binding is None or binding.layout.definition.digest != context.definition.digest:
        raise CustomImportReadUnavailableError("pinned family block definition differs")
    if dict(binding.layout.collection_slots) != dict(context.collection_slots_by_name):
        raise CustomImportReadUnavailableError("pinned family block collection scope differs")
    return binding


def _directory(row):
    return tuple(row[name] for name in _DIRECTORY)


def _membership(row):
    return RevisionMembership(*(row[name] for name in _MEMBERSHIP))


async def family_metadata(session, binding, family_id):
    """Read only the exact pinned family metadata row."""
    table = binding.tables_by_role["family"]
    row = (await session.execute(select(table).where(table.c.family_revision_id == family_id))).mappings().one_or_none()
    if row is None:
        raise FamilyBlockError("selected family is absent")
    return row


def _stream_rows(session, statement):
    result = session.execute(statement.execution_options(stream_results=True, yield_per=32))
    try:
        yield from result.mappings()
    finally:
        result.close()


async def verified_family(session, binding, family_row, *, limits):
    """Full envelope/semantic/sparse/directory proof BEFORE exposing any record.

    Callers already hold the protected generation/build pin. The returned owned
    spool must be closed; candidate/retained callers supply their accounted root.
    No ordinary point read implicitly enters this complete-family path.
    """
    table = binding.tables_by_role["fragment"]
    runs = binding.tables_by_role["revision_run"]
    fragments = (
        select(table)
        .where(table.c.family_revision_id == family_row["family_revision_id"])
        .order_by(table.c.fragment_ordinal)
    )
    memberships = (
        select(runs)
        .where(runs.c.family_revision_id == family_row["family_revision_id"])
        .order_by(runs.c.collection_slot, runs.c.first_revision_id)
    )

    def _verify(sync):
        fragment_rows = _stream_rows(sync, fragments)
        membership_rows = _stream_rows(sync, memberships)
        try:
            return verify_stored_family(
                binding.layout.definition,
                binding.layout.collection_slots,
                ((_directory(item), item["encoded_fragment"]) for item in fragment_rows),
                (_membership(item) for item in membership_rows),
                binding.manifest(family_row),
                limits=limits,
            )
        finally:
            fragment_rows.close()
            membership_rows.close()

    return await session.run_sync(_verify)


class SelectedRecordWindow:
    """One admitted record; checksum-checked fragments, never whole-family proof."""

    def __init__(self, definition, collection, revision_id, ordinal, *, retained, limits):
        self.definition, self.retained, self.limits = definition, retained, limits
        self._target(collection, revision_id, ordinal)
        self.next_fragment = self.current_ordinal = None
        self.pending = bytearray()
        self.result = None
        self.started = False
        self.last_directory = None

    def _target(self, collection, revision_id, ordinal):
        if (collection is None) != (ordinal == 0) or (
            collection is not None and collection not in self.definition.collections_by_name
        ):
            raise FamilyBlockError("selected record collection differs")
        if type(revision_id) is not int or revision_id <= 0 or type(ordinal) is not int or ordinal < 0:
            raise FamilyBlockError("selected record identity differs")
        self.collection, self.revision_id, self.ordinal = collection, revision_id, ordinal

    def advance(self, collection, revision_id, ordinal):
        """Select a later record from the current checked fragment's unread tail."""
        if self.result is None or type(ordinal) is not int or ordinal <= self.ordinal:
            raise FamilyBlockError("selected records are not increasing")
        first, count = self.last_directory[2:4]
        if first is None or not first <= ordinal < first + count:
            raise FamilyBlockError("selected fragment record extent differs")
        self._target(collection, revision_id, ordinal)
        self.current_ordinal += 1
        self.result = None
        return self._consume()

    @property
    def needed(self):
        """Return bytes needed for the next bounded framing decision."""
        if len(self.pending) < 8:
            return 8 - len(self.pending)
        length = _LENGTH.unpack(self.pending[:8])[0]
        if not 0 < length <= self.limits.max_record_bytes:
            raise FamilyBlockError("selected record exceeds the admitted resource budget")
        return max(1, length + 8 - len(self.pending))

    def feed(self, metadata, frame):
        """Check one contiguous fragment before exposing the selected exact record."""
        if self.result is not None:
            raise FamilyBlockError("selected record already completed")
        if len(metadata) != 6 or any(type(metadata[index]) is not int for index in (0, 1, 3, 5)):
            raise FamilyBlockError("selected fragment directory differs")
        fragment, size, first, count, offset, continuation = metadata
        if fragment < 0 or not 0 <= continuation <= size or count < 0:
            raise FamilyBlockError("selected fragment directory differs")
        if (count == 0 and (first is not None or offset is not None)) or (
            count > 0
            and (type(first) is not int or first < 0 or type(offset) is not int or not continuation <= offset < size)
        ):
            raise FamilyBlockError("selected fragment directory differs")
        if self.next_fragment is None:
            if (
                type(first) is not int
                or type(offset) is not int
                or not first <= self.ordinal < first + count
                or not continuation <= offset < size
            ):
                raise FamilyBlockError("selected fragment record extent differs")
            self.current_ordinal = first
            self.next_fragment = fragment
        elif fragment != self.next_fragment:
            raise FamilyBlockError("selected fragments are not contiguous")
        raw = decode_fragment(frame, metadata, limits=self.limits.codec_limits())
        # Only the initial frame starts at its typed record offset.
        if not self.started:
            self.started = True
            raw = raw[offset:]
        self.next_fragment = fragment + 1
        self.last_directory = tuple(metadata)
        self.pending.extend(raw)
        return self._consume()

    def _consume(self):
        while len(self.pending) >= 8:
            length = _LENGTH.unpack(self.pending[:8])[0]
            if not 0 < length <= self.limits.max_record_bytes:
                raise FamilyBlockError("selected record exceeds the admitted resource budget")
            if len(self.pending) < length + 8:
                break
            try:
                wire = msgpack.unpackb(self.pending[8 : 8 + length], raw=False, strict_map_key=True)
            except (ValueError, TypeError) as error:
                raise FamilyBlockError("selected record framing differs") from error
            del self.pending[: 8 + length]
            if type(wire) is list and wire and wire[0] in ("collection", "end"):
                continue
            if self.current_ordinal == self.ordinal:
                if self.collection is not None:
                    if type(wire) is not list or len(wire) != 2 or wire[0] != "record":
                        raise FamilyBlockError("selected child framing differs")
                    wire = wire[1]
                self.result = record_from_wire(self.definition, self.collection, wire, retained=self.retained)[0]
                if self.result.revision_id != self.revision_id:
                    raise FamilyBlockError("selected revision differs from membership")
                return self.result
            self.current_ordinal += 1
        return None


async def _membership_ordinals(session, binding, family_row, requested, limits):
    """Prove exact selected actual IDs and ordinals within the bounded run chunks."""
    table = binding.tables_by_role["revision_run"]
    clauses = [
        and_(
            table.c.collection_slot == slot, table.c.first_revision_id <= revision, table.c.last_revision_id >= revision
        )
        for slot, revision in requested
    ]
    statement = select(table).where(table.c.family_revision_id == family_row["family_revision_id"], or_(*clauses))
    sized = statement.with_only_columns(
        table.c.collection_slot,
        table.c.first_revision_id,
        table.c.last_revision_id,
        table.c.record_count,
        table.c.first_record_ordinal,
        func.coalesce(func.octet_length(table.c.encoded_membership), 0).label("size"),
    ).limit(len(requested) + 1)
    metadata = (await session.execute(sized)).mappings().all()
    if (
        len(metadata) > len(requested)
        or sum(membership_row["size"] + 256 for membership_row in metadata) > limits.bytes
    ):
        raise FamilyBlockError("selected membership exceeds the admitted read budget")
    membership_rows = (await session.execute(statement)).mappings().all()
    if len(membership_rows) != len(metadata):
        raise FamilyBlockError("pinned membership population differs")
    ordinal_by_key = {}
    for membership_row in membership_rows:
        membership = _membership(membership_row)
        selected_keys = {
            (slot, revision)
            for slot, revision in requested
            if slot == membership.collection_slot
            and membership.first_revision_id <= revision <= membership.last_revision_id
        }
        if membership.encoded_membership is None:
            if membership.last_revision_id - membership.first_revision_id != membership.record_count - 1:
                raise FamilyBlockError("selected membership extent differs")
            pairs = (
                (revision, membership.first_record_ordinal + revision - membership.first_revision_id)
                for _slot, revision in selected_keys
            )
        else:
            pairs = membership.records()
        for revision, ordinal in pairs:
            key = membership.collection_slot, revision
            if key in selected_keys:
                if key in ordinal_by_key or not 0 <= ordinal <= family_row["child_count"]:
                    raise FamilyBlockError("selected membership overlaps or exceeds the family")
                ordinal_by_key[key] = ordinal
    if ordinal_by_key.keys() != requested or len(set(ordinal_by_key.values())) != len(ordinal_by_key):
        raise FamilyBlockError("selected membership is missing or ambiguous")
    return ordinal_by_key


async def _fragment_rows(session, binding, family_id, start, count, limits):
    table = binding.tables_by_role["fragment"]
    statement = (
        select(table)
        .where(table.c.family_revision_id == family_id, table.c.fragment_ordinal >= start)
        .order_by(table.c.fragment_ordinal)
        .limit(count)
    )
    metadata = (
        (
            await session.execute(
                statement.with_only_columns(
                    *(table.c[name] for name in _DIRECTORY), func.octet_length(table.c.encoded_fragment).label("size")
                )
            )
        )
        .mappings()
        .all()
    )
    if not metadata or [item["fragment_ordinal"] for item in metadata] != list(range(start, start + len(metadata))):
        raise FamilyBlockError("selected fragment is missing")
    if sum(item["size"] + 256 for item in metadata) > limits.bytes:
        raise FamilyBlockError("selected fragments exceed the admitted read budget")
    rows = (await session.execute(statement)).mappings().all()
    if [(_directory(item), len(item["encoded_fragment"])) for item in rows] != [
        (_directory(item), item["size"]) for item in metadata
    ]:
        raise FamilyBlockError("pinned fragment population differs")
    return rows


async def selected_records(session, binding, row, requested, *, limits=FamilyBlockReadLimits()):
    """Return exactly selected actual IDs after bounded sealed-record checks.

    The existing pin/closed native authority protects the directory. This never
    claims to recompute the whole-family envelope or scans unrelated children.
    """
    requested = frozenset(requested)
    if not requested:
        return {}
    if len(requested) > limits.rows:
        raise FamilyBlockError("selected revisions exceed the admitted read budget")
    if any(
        type(slot) is not int or slot < 0 or type(revision) is not int or not 0 < revision < 2**63
        for slot, revision in requested
    ):
        raise FamilyBlockError("selected revision scope differs")
    ordinals = await _membership_ordinals(session, binding, row, requested, limits)
    return await _selected_records(session, binding, row, ordinals, limits)


async def _record_starts(session, binding, family_row, ordinals):
    table = binding.tables_by_role["fragment"]
    wanted = (
        values(column("record_ordinal", BigInteger))
        .data([(ordinal,) for ordinal in sorted(ordinals.values())])
        .cte("requested_block_records")
    )
    located = (
        select(table.c.fragment_ordinal)
        .where(
            table.c.family_revision_id == family_row["family_revision_id"],
            table.c.first_record_ordinal <= wanted.c.record_ordinal,
        )
        .order_by(table.c.first_record_ordinal.desc())
        .limit(1)
        .lateral("record_start")
    )
    starts = (
        await session.execute(
            select(wanted.c.record_ordinal, located.c.fragment_ordinal).select_from(wanted.outerjoin(located, true()))
        )
    ).all()
    first_by_ordinal = dict(starts)
    if (
        len(starts) != len(ordinals)
        or len(first_by_ordinal) != len(ordinals)
        or any(
            fragment_ordinal is None or not 0 <= fragment_ordinal < family_row["fragment_count"]
            for fragment_ordinal in first_by_ordinal.values()
        )
    ):
        raise FamilyBlockError("selected record has no fragment directory")
    return first_by_ordinal


async def _selected_records(session, binding, family_row, ordinals, limits):
    first_by_ordinal = await _record_starts(session, binding, family_row, ordinals)
    record_by_key, total = {}, 0
    fragments_by_ordinal, buffer_bytes = OrderedDict(), 0
    collection_by_slot = {slot: name for name, slot in binding.layout.collection_slots.items()}
    window = None
    for index, (key, ordinal) in enumerate(sorted(ordinals.items(), key=lambda item: item[1])):
        if index and index % 64 == 0:
            await asyncio.sleep(0)
        slot, revision = key
        if slot and slot not in collection_by_slot:
            raise FamilyBlockError("selected collection is undeclared")
        next_fragment = first_by_ordinal[ordinal]
        if window is not None and next_fragment == window.next_fragment - 1:
            window.advance(collection_by_slot.get(slot), revision, ordinal)
            next_fragment = window.next_fragment
        else:
            window = SelectedRecordWindow(
                binding.layout.definition,
                collection_by_slot.get(slot),
                revision,
                ordinal,
                retained=family_row["retained"],
                limits=limits,
            )
        while window.result is None:
            if next_fragment >= family_row["fragment_count"]:
                raise FamilyBlockError("selected family ends inside a record")
            count = min(16, max(1, math.ceil(window.needed / limits.fragment_bytes)))
            if next_fragment in fragments_by_ordinal:
                fragments = (fragments_by_ordinal[next_fragment],)
            else:
                fragments = await _fragment_rows(
                    session,
                    binding,
                    family_row["family_revision_id"],
                    next_fragment,
                    min(count, family_row["fragment_count"] - next_fragment),
                    limits,
                )
                total += sum(len(fragment["encoded_fragment"]) for fragment in fragments)
                if total > limits.codec_limits().max_spool_bytes:
                    raise FamilyBlockError("selected records exceed the cumulative read budget")
                buffer_bytes = _buffer_fragments(fragments_by_ordinal, fragments, buffer_bytes, limits.bytes)
            for fragment in fragments:
                if window.feed(_directory(fragment), fragment["encoded_fragment"]) is not None:
                    break
            next_fragment = window.next_fragment
        record_by_key[key] = window.result
    return record_by_key


def present_scalar(scalar):
    """Apply only the original native scalar presentation boundary."""
    value = scalar.logical_value
    if scalar.value_state == "value":
        if scalar.field_type == "decimal":
            with localcontext() as context:
                context.prec = 64
                value = value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)
                if value.is_zero():
                    value = abs(value)
        elif scalar.field_type == "timestamp":
            value = value.astimezone(timezone.utc)
    return SimpleNamespace(
        field_type=scalar.field_type, value_state=scalar.value_state, **{_LANES[scalar.field_type]: value}
    )


async def scalar_rows(session, context, identifiers, fields, *, child):
    """Fetch all declared query scalars in one existing read statement."""
    if not identifiers or not fields:
        return {}
    _binding(context)
    statements = []
    for field in fields:
        # Every ordinary query/profile field is exposed by the definition parser.
        model = field_scalar_model(context, field)
        identifier = model.child_revision_id if child else model.root_revision_id
        statements.append(
            select(
                identifier,
                model.field_slot,
                model.field_type,
                model.value_state,
                *(getattr(model, name) for name in _LANES.values()),
            ).where(identifier.in_(identifiers))
        )
    statement = statements[0] if len(statements) == 1 else union_all(*statements)
    rows = (await session.execute(statement)).all()
    return {(row[0], row.field_slot): row for row in rows}


async def hydrate_families(session, context, selected, scope_digest):
    """Hydrate complete bounded details in their original public child order."""
    from process.custom_import.family_block_hydration import hydrate_page

    return await hydrate_page(session, context, selected, scope_digest)


def _project_family(context, winner, family, record_by_key, scope_digest):
    from process.custom_import import read_core as core

    root_fields = core._detail_root_fields(context.definition)
    fields_by_slot = core._detail_child_fields_by_slot(context)
    root_record = record_by_key[0, family.root_revision_id]
    root_scalars_by_key = {
        (root_record.revision_id, field.field_slot): present_scalar(root_record.scalars[field.field_id])
        for field in root_fields
        if field.field_id in root_record.scalars
    }
    child_projections = []
    collection_by_slot = context.collection_names_by_slot
    for (slot, _revision), child_record in record_by_key.items():
        if slot == 0:
            continue
        fields = fields_by_slot[slot]
        scalars_by_key = {
            (child_record.revision_id, field.field_slot): present_scalar(child_record.scalars[field.field_id])
            for field in fields
            if field.field_id in child_record.scalars
        }
        projected = core.ReadChild(
            collection=collection_by_slot[slot],
            child_revision_id=child_record.revision_id,
            fields=core._project_field_values(fields, child_record.revision_id, scalars_by_key),
        )
        child_projections.append(((slot, child_record.source_ordinal, child_record.revision_id), projected))
    return core.RootDetail(
        target=context.target,
        winner=core._winner_locator(winner, family),
        root_fields=core._project_field_values(root_fields, root_record.revision_id, root_scalars_by_key),
        children=tuple(child for _order, child in sorted(child_projections)),
        authorization_scope_sha256=scope_digest,
    )


def _buffer_fragments(fragments_by_ordinal, fragment_rows, buffer_bytes, byte_limit):
    for fragment in fragment_rows:
        fragments_by_ordinal[fragment["fragment_ordinal"]] = fragment
        buffer_bytes += len(fragment["encoded_fragment"]) + 256
        while buffer_bytes > byte_limit:
            _key, evicted = fragments_by_ordinal.popitem(last=False)
            buffer_bytes -= len(evicted["encoded_fragment"]) + 256
    return buffer_bytes
