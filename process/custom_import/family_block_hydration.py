# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native pages for complete families in an already pinned read context."""

import asyncio
from collections import deque
from contextlib import aclosing
from dataclasses import replace

from sqlalchemy import BigInteger, column, func, select, true, tuple_, values

from process.custom_import import family_block_read as reader

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
)
_PAGE_KEYS = {
    "revision_run": ("family_revision_id", "collection_slot", "first_revision_id"),
    "fragment": ("family_revision_id", "fragment_ordinal"),
}
_BLOBS = {"revision_run": "encoded_membership", "fragment": "encoded_fragment"}


async def _mappings(session, statement):
    result = await session.execute(statement)
    try:
        return result.mappings().all()
    finally:
        result.close()


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


async def _blob_pages(session, binding, family_ids, role, limits):
    """Preflight and fetch exact coordinates, never an unchecked blob population."""
    table = binding.tables_by_role[role]
    keys, blob = _PAGE_KEYS[role], _BLOBS[role]
    columns = tuple(name for name in table.c.keys() if name != blob)
    key_columns = tuple(table.c[name] for name in keys)
    cursor = None
    while True:
        statement = select(
            *(table.c[name] for name in columns), func.coalesce(func.octet_length(table.c[blob]), 0).label("size")
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
        coordinates = [_coordinates(native_row, keys) for native_row in metadata]
        if any(coordinate[0] not in family_ids for coordinate in coordinates) or (
            any(left >= right for left, right in zip(coordinates, coordinates[1:]))
            or (cursor is not None and coordinates[0] <= cursor)
        ):
            raise reader.FamilyBlockError("native page coordinate order differs")
        native_rows = await _mappings(
            session, select(table).where(tuple_(*key_columns).in_(coordinates)).order_by(*key_columns)
        )
        observed_cells = [
            (_coordinates(native_row, columns), len(native_row[blob] or b"")) for native_row in native_rows
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


def _family_groups(families):
    grouped, count = [], 0
    for family in families:
        records = family["child_count"] + 1
        if grouped and count + records > _COORDINATES:
            yield grouped
            grouped, count = [], 0
        grouped.append(family)
        count += records
    if grouped:
        yield grouped


def _add_membership(native_row, family_by_id, ordinal_by_family, sizes_by_family, limits):
    family_id = native_row["family_revision_id"]
    count = family_by_id[family_id]["child_count"] + 1
    ordinals = ordinal_by_family[family_id]
    sizes_by_family[family_id] += len(native_row["encoded_membership"] or b"") + 256
    membership = reader._membership(native_row)
    if sizes_by_family[family_id] > limits.bytes or len(ordinals) + membership.record_count > count:
        raise reader.FamilyBlockError("selected family membership census differs")
    for revision, ordinal in membership.records():
        key = membership.collection_slot, revision
        if key in ordinals or not 0 <= ordinal < count:
            raise reader.FamilyBlockError("selected family membership overlaps")
        ordinals[key] = ordinal


async def _memberships(session, binding, families, limits):
    family_by_id = {family["family_revision_id"]: family for family in families}
    ordinal_by_family = {family_id: {} for family_id in family_by_id}
    sizes_by_family = dict.fromkeys(family_by_id, 0)
    async with aclosing(_blob_pages(session, binding, tuple(family_by_id), "revision_run", limits)) as pages:
        async for membership_rows in pages:
            for native_row in membership_rows:
                _add_membership(native_row, family_by_id, ordinal_by_family, sizes_by_family, limits)
            del native_row
    for family_id, ordinals in ordinal_by_family.items():
        family = family_by_id[family_id]
        count = family["child_count"] + 1
        if (
            ordinals.get((0, family["root_revision_id"])) != 0
            or len(ordinals) != count
            or set(ordinals.values()) != set(range(count))
        ):
            raise reader.FamilyBlockError("selected family membership is incomplete")
    return ordinal_by_family


async def _record_starts(session, binding, families, ordinal_by_family):
    table = binding.tables_by_role["fragment"]
    requested_coordinates = [
        (family_id, ordinal)
        for family_id, ordinals in ordinal_by_family.items()
        for ordinal in sorted(ordinals.values())
    ]
    wanted = values(column("family_revision_id", BigInteger), column("record_ordinal", BigInteger)).data(
        requested_coordinates
    )
    wanted = wanted.cte("requested_family_records")
    located = (
        select(table.c.fragment_ordinal)
        .where(
            table.c.family_revision_id == wanted.c.family_revision_id,
            table.c.first_record_ordinal <= wanted.c.record_ordinal,
        )
        .order_by(table.c.first_record_ordinal.desc())
        .limit(1)
        .lateral("record_start")
    )
    native_rows = await _mappings(
        session, select(wanted, located.c.fragment_ordinal).select_from(wanted.outerjoin(located, true()))
    )
    start_by_record = {
        (native_row["family_revision_id"], native_row["record_ordinal"]): native_row["fragment_ordinal"]
        for native_row in native_rows
    }
    counts_by_family = {family["family_revision_id"]: family["fragment_count"] for family in families}
    if (
        len(native_rows) != len(requested_coordinates)
        or start_by_record.keys() != set(requested_coordinates)
        or any(
            type(start) is not int or not 0 <= start < counts_by_family[family_id]
            for (family_id, _ordinal), start in start_by_record.items()
        )
    ):
        raise reader.FamilyBlockError("selected record has no fragment directory")
    return start_by_record


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
                    self.charged_by_family[owner] += len(native_row["encoded_fragment"])
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


async def _decode_family(binding, family, ordinals, starts, fragments, limits):
    record_by_key, window = {}, None
    collection_by_slot = {slot: name for name, slot in binding.layout.collection_slots.items()}
    family_id = family["family_revision_id"]
    for index, (key, ordinal) in enumerate(sorted(ordinals.items(), key=lambda entry: entry[1])):
        if index and index % 64 == 0:
            await asyncio.sleep(0)
        slot, revision = key
        if slot and slot not in collection_by_slot:
            raise reader.FamilyBlockError("selected collection is undeclared")
        next_fragment = starts[family_id, ordinal]
        if window is not None and next_fragment == window.next_fragment - 1:
            window.advance(collection_by_slot.get(slot), revision, ordinal)
            next_fragment = window.next_fragment
        else:
            window = reader.SelectedRecordWindow(
                binding.layout.definition,
                collection_by_slot.get(slot),
                revision,
                ordinal,
                retained=family["retained"],
                limits=limits,
            )
        while window.result is None:
            if next_fragment >= family["fragment_count"]:
                raise reader.FamilyBlockError("selected family ends inside a record")
            fragment = await fragments.take(family_id, next_fragment)
            window.feed(reader._directory(fragment), fragment["encoded_fragment"])
            del fragment
            next_fragment = window.next_fragment
        record_by_key[key] = window.result
    return record_by_key


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
        if not 0 <= family["child_count"] <= reader.MAX_DETAIL_CHILDREN:
            raise reader.CustomImportReadUnavailableError("selected families exceed the bounded detail child limit")


async def _hydrate_group(session, context, families, selected_by_id, scope_digest, limits):
    binding = context.family_block_binding
    ordinals = await _memberships(session, binding, families, limits)
    starts = await _record_starts(session, binding, families, ordinals)
    details_by_id = {}
    async with aclosing(
        _blob_pages(session, binding, tuple(family["family_revision_id"] for family in families), "fragment", limits)
    ) as pages:
        fragments = _FragmentPages(pages, families, limits)
        for index, family in enumerate(families):
            if index and index % 64 == 0:
                await asyncio.sleep(0)
            family_id = family["family_revision_id"]
            record_by_key = await _decode_family(binding, family, ordinals[family_id], starts, fragments, limits)
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
        for grouped in _family_groups(families):
            details_by_id.update(
                await _hydrate_group(session, context, grouped, selected_by_id_chunk, scope_digest, limits)
            )
    from process.custom_import import read_core

    return tuple(
        replace(details_by_id[family.family_revision_id], winner=read_core._winner_locator(winner, family))
        for winner, family, _root, _child in selected
    )
