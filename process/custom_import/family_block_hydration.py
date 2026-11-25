# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native pages for complete families in an already pinned read context."""

import asyncio
import struct
from collections import deque
from contextlib import aclosing
from dataclasses import replace

import msgpack
from sqlalchemy import func, select, tuple_

from process.custom_import import family_block_read as reader

_CONTRACT = "custom-import/family-block/v1"
_LENGTH = struct.Struct("!Q")
_NATIVE_IDS = 1024
_COORDINATES = 4096
_FAMILY_FIELDS = (
    "family_revision_id",
    "root_record_id",
    "root_revision_id",
    "entity_binding_id",
    "retained",
    "child_count",
    "fragment_count",
    "raw_bytes",
    "stored_bytes",
)
_FRAGMENT_KEYS = ("family_revision_id", "fragment_ordinal")
_FRAGMENT_BLOB = "encoded_fragment"


async def _mappings(session, statement):
    query_result = await session.execute(statement)
    try:
        return query_result.mappings().all()
    finally:
        query_result.close()


def _coordinates(native_row, names):
    return tuple(native_row[name] for name in names)


def _fit_page(metadata, byte_limit):
    admitted, used = [], 0
    for native_row in metadata:
        size = native_row["size"]
        if type(size) is not int or size < 0:
            raise reader.FamilyBlockError("native page size differs")
        if used + size + 256 > byte_limit:
            break
        admitted.append(native_row)
        used += size + 256
    if not admitted:
        raise reader.FamilyBlockError("native row exceeds the admitted read budget")
    return admitted


async def _blob_pages(session, binding, family_ids, limits):
    """Preflight and fetch exact coordinates when a family batch exceeds one direct page."""
    table = binding.tables_by_role["fragment"]
    columns = tuple(name for name in table.c.keys() if name != _FRAGMENT_BLOB)
    key_columns = tuple(table.c[name] for name in _FRAGMENT_KEYS)
    cursor = None
    while True:
        statement = select(
            *(table.c[name] for name in columns),
            func.coalesce(func.octet_length(table.c[_FRAGMENT_BLOB]), 0).label("size"),
        )
        statement = statement.where(table.c.family_revision_id.in_(family_ids)).order_by(*key_columns)
        if cursor is not None:
            statement = statement.where(tuple_(*key_columns) > cursor)
        row_limit = min(_COORDINATES, limits.rows, max(1, limits.bytes // 256))
        metadata = await _mappings(session, statement.limit(row_limit))
        if not metadata:
            return
        observed_count = len(metadata)
        metadata = _fit_page(metadata, limits.bytes)
        has_more = observed_count == row_limit or len(metadata) != observed_count
        coordinates = [_coordinates(native_row, _FRAGMENT_KEYS) for native_row in metadata]
        if any(coordinate[0] not in family_ids for coordinate in coordinates) or (
            any(left >= right for left, right in zip(coordinates, coordinates[1:]))
            or (cursor is not None and coordinates[0] <= cursor)
        ):
            raise reader.FamilyBlockError("native page coordinate order differs")
        native_rows = await _mappings(
            session, select(table).where(tuple_(*key_columns).in_(coordinates)).order_by(*key_columns)
        )
        observed_cells = [
            (_coordinates(native_row, columns), len(native_row[_FRAGMENT_BLOB] or b"")) for native_row in native_rows
        ]
        expected_cells = [(_coordinates(native_row, columns), native_row["size"]) for native_row in metadata]
        if observed_cells != expected_cells:
            raise reader.FamilyBlockError("pinned native page population differs")
        cursor = coordinates[-1]
        try:
            yield native_rows
        finally:
            native_rows.clear()
        if not has_more:
            return


async def _direct_fragment_pages(session, binding, families, limits):
    """Fetch all fragments for a bounded batch in one native query."""
    table = binding.tables_by_role["fragment"]
    family_ids = tuple(family["family_revision_id"] for family in families)
    key_columns = tuple(table.c[name] for name in _FRAGMENT_KEYS)
    native_rows = await _mappings(
        session, select(table).where(table.c.family_revision_id.in_(family_ids)).order_by(*key_columns)
    )
    try:
        expected_coordinates = [
            (family["family_revision_id"], ordinal)
            for family in families
            for ordinal in range(family["fragment_count"])
        ]
        observed_coordinates = [_coordinates(native_row, _FRAGMENT_KEYS) for native_row in native_rows]
        if observed_coordinates != expected_coordinates:
            raise reader.FamilyBlockError("selected fragment is missing")
        if sum(len(native_row[_FRAGMENT_BLOB] or b"") + 256 for native_row in native_rows) > limits.bytes:
            raise reader.FamilyBlockError("selected fragments exceed the admitted read budget")
        yield native_rows
    finally:
        native_rows.clear()


def _family_batches(families, limits):
    """Group bounded families for single-query fetch; isolate large families for paged fetch."""
    row_cap = min(_COORDINATES, limits.rows)
    grouped, used_bytes, used_fragments = [], 0, 0
    for family in families:
        fragment_count = family["fragment_count"]
        family_budget = family["stored_bytes"] + fragment_count * 256
        if family_budget > limits.bytes or fragment_count > row_cap:
            if grouped:
                yield grouped, True
                grouped, used_bytes, used_fragments = [], 0, 0
            yield [family], False
            continue
        if grouped and (used_bytes + family_budget > limits.bytes or used_fragments + fragment_count > row_cap):
            yield grouped, True
            grouped, used_bytes, used_fragments = [], 0, 0
        grouped.append(family)
        used_bytes += family_budget
        used_fragments += fragment_count
    if grouped:
        yield grouped, True


class _FragmentPages:
    """One shared compressed page; decoding remains strictly forward per family."""

    def __init__(self, pages, families, limits):
        self.pages, self.limits = pages, limits
        self.family_by_id = {family["family_revision_id"]: family for family in families}
        self.charged_by_family = dict.fromkeys(self.family_by_id, 0)
        self.pending = deque()

    async def take(self, family_id, ordinal):
        """Return one exact forward coordinate after the page's native checks."""
        requested = family_id, ordinal
        while True:
            if not self.pending:
                page = await anext(self.pages, None)
                if page is None:
                    raise reader.FamilyBlockError("selected fragment is missing")
                for native_row in page:
                    owner = native_row["family_revision_id"]
                    self.charged_by_family[owner] += len(native_row[_FRAGMENT_BLOB])
                    if (
                        not 0 <= native_row["fragment_ordinal"] < self.family_by_id[owner]["fragment_count"]
                        or self.charged_by_family[owner] > self.limits.codec_limits().max_spool_bytes
                    ):
                        raise reader.FamilyBlockError("selected records exceed the cumulative read budget")
                self.pending.extend(page)
            native_row = self.pending.popleft()
            coordinate = native_row["family_revision_id"], native_row["fragment_ordinal"]
            if coordinate == requested:
                return native_row
            if coordinate > requested:
                raise reader.FamilyBlockError("selected fragment is missing")
            del native_row


class _FamilyStreamDecoder:
    """Decode one complete sealed family directly from its ordered checksummed fragments."""

    def __init__(self, binding, family, fragments, limits):
        self.binding, self.family, self.fragments = binding, family, fragments
        self.limits, self.codec_limits = limits, limits.codec_limits()
        self.pending = bytearray()
        self.next_fragment = self.raw_total = self.stored_total = 0

    async def _pull_fragment(self):
        if self.next_fragment >= self.family["fragment_count"]:
            raise reader.FamilyBlockError("selected family ends inside a record")
        fragment = await self.fragments.take(self.family["family_revision_id"], self.next_fragment)
        frame = fragment[_FRAGMENT_BLOB]
        raw = reader.decode_fragment(frame, reader._directory(fragment), limits=self.codec_limits)
        self.stored_total += 8 + len(frame)
        self.raw_total += len(raw)
        self.pending.extend(raw)
        self.next_fragment += 1

    async def _next_object(self):
        while len(self.pending) < 8:
            await self._pull_fragment()
        length = _LENGTH.unpack(self.pending[:8])[0]
        if not 0 < length <= self.limits.max_record_bytes:
            raise reader.FamilyBlockError("selected record exceeds the admitted resource budget")
        while len(self.pending) < 8 + length:
            await self._pull_fragment()
        try:
            unpacked = msgpack.unpackb(self.pending[8 : 8 + length], raw=False, strict_map_key=True)
        except (ValueError, TypeError) as error:
            raise reader.FamilyBlockError("selected record framing differs") from error
        del self.pending[: 8 + length]
        return unpacked

    async def decode(self):
        """Stream root and child projections directly without reparsing canonical JSON."""
        definition = self.binding.layout.definition
        retained = self.family["retained"]
        header = await self._next_object()
        if (
            type(header) is not list
            or len(header) != 4
            or header[0] != _CONTRACT
            or header[1] != definition.digest
            or type(header[2]) is not bytes
            or len(header[2]) != 32
            or header[3] is not retained
        ):
            raise reader.FamilyBlockError("selected family header differs")
        root_wire = await self._next_object()
        root_entry = reader.record_from_wire(definition, None, root_wire, retained=retained, include_payload=False)[0]
        if root_entry.revision_id != self.family["root_revision_id"]:
            raise reader.FamilyBlockError("selected revision differs from membership")
        records_by_key = {(0, root_entry.revision_id): root_entry}
        decoded_children = await self._decode_collections(definition, retained, records_by_key)
        trailer = await self._next_object()
        if (
            trailer != ["complete", self.family["child_count"]]
            or decoded_children != self.family["child_count"]
            or self.pending
            or self.next_fragment != self.family["fragment_count"]
            or self.raw_total != self.family["raw_bytes"]
            or self.stored_total != self.family["stored_bytes"]
        ):
            raise reader.FamilyBlockError("selected family extent differs")
        return records_by_key

    async def _decode_collections(self, definition, retained, records_by_key):
        slots_by_name = self.binding.layout.collection_slots
        decoded_children = 0
        for collection_name in sorted(definition.collections_by_name):
            if await self._next_object() != ["collection", collection_name]:
                raise reader.FamilyBlockError("selected family collection header differs")
            collection_slot = slots_by_name[collection_name]
            collection_count = 0
            while True:
                entry = await self._next_object()
                if type(entry) is list and len(entry) == 2 and entry[0] == "end":
                    if type(entry[1]) is not int or entry[1] != collection_count:
                        raise reader.FamilyBlockError("selected family collection count differs")
                    break
                if type(entry) is not list or len(entry) != 2 or entry[0] != "record":
                    raise reader.FamilyBlockError("selected child framing differs")
                if decoded_children and decoded_children % 64 == 0:
                    await asyncio.sleep(0)
                child_entry = reader.record_from_wire(
                    definition, collection_name, entry[1], retained=retained, include_payload=False
                )[0]
                key = (collection_slot, child_entry.revision_id)
                if key in records_by_key:
                    raise reader.FamilyBlockError("selected family membership overlaps")
                records_by_key[key] = child_entry
                collection_count += 1
                decoded_children += 1
        return decoded_children


def _verify_families(families, selected_by_id):
    if (
        len(families) != len(selected_by_id)
        or {family["family_revision_id"] for family in families} != selected_by_id.keys()
    ):
        raise reader.FamilyBlockError("selected family is absent or ambiguous")
    for family in families:
        for winner, identity, root, _context_child in selected_by_id[family["family_revision_id"]]:
            if (
                family["root_record_id"],
                family["root_revision_id"],
                family["entity_binding_id"],
                family["child_count"],
            ) != (identity.root_record_id, root.root_revision_id, winner.entity_binding_id, identity.child_count):
                raise reader.FamilyBlockError("selected family identity differs")
        if any(
            type(family[name]) is not int or family[name] <= 0
            for name in ("fragment_count", "raw_bytes", "stored_bytes")
        ):
            raise reader.FamilyBlockError("selected family extent differs")
        if not 0 <= family["child_count"] <= reader.MAX_DETAIL_CHILDREN:
            raise reader.CustomImportReadUnavailableError("selected families exceed the bounded detail child limit")


async def _hydrate_group(session, context, families, selected_by_id, scope_digest, limits, *, use_direct):
    binding = context.family_block_binding
    details_by_id = {}
    family_ids = tuple(family["family_revision_id"] for family in families)
    page_stream = (
        _direct_fragment_pages(session, binding, families, limits)
        if use_direct
        else _blob_pages(session, binding, family_ids, limits)
    )
    async with aclosing(page_stream) as pages:
        fragments = _FragmentPages(pages, families, limits)
        for index, family in enumerate(families):
            if index and index % 64 == 0:
                await asyncio.sleep(0)
            family_id = family["family_revision_id"]
            record_by_key = await _FamilyStreamDecoder(binding, family, fragments, limits).decode()
            winner, identity, _root, _child = selected_by_id[family_id][0]
            details_by_id[family_id] = reader._project_family(context, winner, identity, record_by_key, scope_digest)
            del record_by_key
    return details_by_id


async def hydrate_page(session, context, selected, scope_digest):
    """Project bounded native groups, preserving the caller's exact result order."""
    binding, limits = reader._binding(context), reader.FamilyBlockReadLimits()
    selected_by_id, details_by_id = {}, {}
    for selected_row in selected:
        selected_by_id.setdefault(selected_row[1].family_revision_id, []).append(selected_row)
    identifiers = sorted(selected_by_id)
    table = binding.tables_by_role["family"]
    chunk_size = min(_NATIVE_IDS, limits.rows, max(1, limits.bytes // 256))
    for offset in range(0, len(identifiers), chunk_size):
        selected_by_id_chunk = {
            family_id: selected_by_id[family_id] for family_id in identifiers[offset : offset + chunk_size]
        }
        families = await _mappings(
            session,
            select(*(table.c[name] for name in _FAMILY_FIELDS))
            .where(table.c.family_revision_id.in_(tuple(selected_by_id_chunk)))
            .order_by(table.c.family_revision_id),
        )
        _verify_families(families, selected_by_id_chunk)
        for grouped, use_direct in _family_batches(families, limits):
            details_by_id.update(
                await _hydrate_group(
                    session, context, grouped, selected_by_id_chunk, scope_digest, limits, use_direct=use_direct
                )
            )
    from process.custom_import import read_core

    return tuple(
        replace(details_by_id[family.family_revision_id], winner=read_core._winner_locator(winner, family))
        for winner, family, _root, _child in selected
    )
