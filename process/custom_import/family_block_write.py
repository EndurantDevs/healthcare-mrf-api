# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native writes to one authorized, still-open compact candidate."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import count

from sqlalchemy import select, text
from sqlalchemy.schema import CreateTable

from db.models.custom_import import CustomImportBuildCandidateContext, CustomImportBuildFamily
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.family_block_binding import FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_codec import (
    FamilyBlockResourceLimit,
    FamilyRecord,
    encode_family,
    encode_retained_family,
)
from process.custom_import.family_block_layout import READ_CAPABILITIES, candidate_layout_statements, copy_columns
from process.custom_import.family_block_query_input import query_input_table
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_revision_order import RevisionOrder
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_storage_codec import stored_manifest
from process.custom_import.query_projection_initialize import _ACL, _close_and_copy_acl
from process.custom_import.storage_layout import snapshot_tables

CONTEXT_COPY_COLUMNS = tuple(
    name for name in CustomImportBuildCandidateContext.__table__.c.keys() if name != "candidate_context_id"
)


@dataclass(frozen=True)
class AllocatedChildCollection:
    """One frozen SOURCE collection, already in canonical codec order."""

    count: int
    records: Iterable[FamilyRecord]

    def __post_init__(self):
        _integer(self.count, "child count", zero=True)

    def indexed_records(self, slot, ordinals, revision_order):
        """Collect actual revision IDs as the original child values are consumed."""
        consumed = 0
        for consumed, child in enumerate(self.records, 1):
            if consumed > self.count:
                raise FamilyBlockUnavailable("child collection exceeds the SOURCE count")
            _integer(child.revision_id, "child revision identity")
            revision_order.add(slot, child.revision_id, next(ordinals))
            yield child
        if consumed != self.count:
            raise FamilyBlockUnavailable("child collection ends before the SOURCE count")


@dataclass(frozen=True)
class AllocatedBlockFamily:
    family_revision_id: int
    root_record_id: int
    entity_binding_id: int
    family_sha256: bytes | None
    root: FamilyRecord
    children: Mapping[str, AllocatedChildCollection]


def pack_family_rows(layout, family, *, limits, account=None, retained=False):
    """Emit validated COPY cells from the original ordered records and encoded fragments."""
    if set(family.children) != set(layout.collection_slots):
        raise FamilyBlockUnavailable("compact family collections differ from the layout")
    child_count = sum(collection.count for collection in family.children.values())
    with RevisionOrder(limits, child_count) as revision_order:
        ordinals = count(1)
        children_by_name = {
            name: collection.indexed_records(layout.collection_slots[name], ordinals, revision_order)
            for name, collection in family.children.items()
        }
        encoder = encode_retained_family if retained else encode_family
        with encoder(
            layout.definition,
            family.root,
            children_by_name,
            family.family_sha256,
            limits=limits,
            reserved_spool_bytes=revision_order.reserved_spool_bytes,
        ) as encoded:
            yield from _packed_rows(layout, family, encoded, revision_order)
            if account is not None:
                account(encoded.root_scalar_count, encoded.child_scalar_count)


def _packed_rows(layout, family, encoded, revision_order):
    family_id = family.family_revision_id
    manifest = stored_manifest(
        encoded.manifest,
        (directory for directory, _ in encoded.indexed_fragments()),
        revision_order.memberships(family.root.revision_id),
    )
    yield (
        "family",
        (
            family_id,
            family.root_record_id,
            family.root.revision_id,
            family.entity_binding_id,
            manifest.family_sha256,
            manifest.storage_sha256,
            manifest.retained,
            manifest.child_count,
            manifest.fragment_count,
            manifest.raw_bytes,
            manifest.stored_bytes,
            manifest.membership_count,
            encoded.root_payload_sha256,
            family.root.source_ordinal,
            *pack_query_scalars(query_scalar_fields(layout.definition), family.root.scalars),
        ),
    )
    for directory, frame in encoded.indexed_fragments():
        yield "fragment", (family_id, directory[0], directory[1], frame, *directory[2:])
    for member in revision_order.memberships(family.root.revision_id):
        yield "revision_run", (family_id, *member.cells())


def _wire_bytes(cell):
    """Upper-bound one PostgreSQL binary COPY cell including its length prefix."""
    if cell is None:
        return 4
    if type(cell) is bytes:
        return 4 + len(cell)
    if type(cell) is str:
        return 4 + len(cell.encode("utf-8"))
    if type(cell) in (bool, int, date, datetime):
        return 12
    if type(cell) is Decimal:
        return 16 + len(str(cell))
    raise ValueError("unsupported compact COPY cell")


class BlockCopyBuffer:
    """Accumulate mixed output roles within the existing native COPY caps."""

    def __init__(self):
        self.rows_by_role = {}
        self.row_count = self.byte_count = 0

    def append(self, role, cells):
        """Return a full batch before admitting the next row."""
        row_bytes = 2 + sum(_wire_bytes(cell) for cell in cells)
        if row_bytes + 21 > MAX_BATCH_BYTES:
            raise FamilyBlockResourceLimit("compact COPY row exceeds the native byte budget")
        previous = None
        if (
            self.row_count == MAX_BATCH_ROWS
            or self.byte_count + row_bytes + (21 if role not in self.rows_by_role else 0) > MAX_BATCH_BYTES
        ):
            previous = self.take()
        if role not in self.rows_by_role:
            self.rows_by_role[role] = []
            self.byte_count += 21
        self.rows_by_role[role].append(cells)
        self.row_count += 1
        self.byte_count += row_bytes
        return previous

    def take(self):
        """Detach the prepared batch and reset all charges."""
        batch_by_role = {role: tuple(rows) for role, rows in self.rows_by_role.items()}
        self.rows_by_role.clear()
        self.row_count = self.byte_count = 0
        return batch_by_role


def _check_batch(tables_by_role, rows_by_role, *, row_limit, byte_limit):
    """Reject oversized batches before acquiring the build locks."""
    if not rows_by_role or rows_by_role.keys() - tables_by_role.keys():
        raise ValueError("compact COPY requires known roles")
    row_count = byte_count = 0
    for role, batch in rows_by_role.items():
        if type(batch) is not tuple or not batch:
            raise ValueError("compact COPY requires nonempty prepared batches")
        width = len(_copy_role_columns(role, tables_by_role[role]))
        row_count += len(batch)
        byte_count += 21  # Signature, flags, extension length and EOF of one COPY stream.
        if row_count > row_limit:
            raise FamilyBlockResourceLimit("compact COPY exceeds the row budget")
        for cells in batch:
            if type(cells) is not tuple or len(cells) != width:
                raise ValueError("compact COPY row shape differs")
            byte_count += 2 + sum(_wire_bytes(cell) for cell in cells)
            if byte_count > byte_limit:
                raise FamilyBlockResourceLimit("compact COPY exceeds the byte budget")


def _copy_role_columns(role, table):
    if role == "candidate_context":
        # The existing native sequence allocates scratch context IDs.
        return CONTEXT_COPY_COLUMNS
    return copy_columns(table)


def _producer_scope(build):
    family = CustomImportSnapshotFamily
    return (
        family.frozen_at.is_(None),
        family.dataset_id == build.dataset_id,
        family.definition_revision_id == build.definition_revision_id,
        family.schema_revision_id == build.schema_revision_id,
        family.execution_id == build.execution_id,
        family.capture_bundle_id == build.capture_bundle_id,
        family.producing_fence == build.producing_fence,
        family.producing_token_sha256 == build.producing_token_sha256,
    )


async def _legacy_readers(session, table_names, schema_name):
    """Copy only SELECT grants shared by every legacy contributor."""
    common = None
    for name in table_names:
        await _prepare_statement(session)
        parameters_by_name = {"schema_name": schema_name, "table_name": name, "select_only": True}
        rows = (await session.execute(_ACL, parameters_by_name)).mappings()
        by_grantee = {row["grantee"]: row for row in rows if row["grantee"]}
        common = by_grantee if common is None else {key: common[key] for key in common.keys() & by_grantee.keys()}
    return tuple(common.values())


async def initialize_block_layout(session, build, layout, *, allow_retained=False):
    """Install compact leaves, or leave a retained or started legacy plan untouched."""
    if not session.in_transaction() or build.phase != "graph" or build.refresh_mode != "snapshot":
        raise FamilyBlockUnavailable("compact layout requires an authorized snapshot graph")
    if not build.complete_scope or build.plan_complete_at is None:
        raise FamilyBlockUnavailable("compact layout requires a complete snapshot plan")
    family = CustomImportSnapshotFamily
    await _prepare_statement(session)
    parent = await session.scalar(select(family).where(*_producer_scope(build)).with_for_update())
    if parent is None or parent.generation_id is not None:
        raise FamilyBlockUnavailable("compact layout candidate is missing or already assigned")
    plan = snapshot_tables(parent.family_id)[CustomImportBuildFamily.__tablename__]
    await _prepare_statement(session)
    has_started = plan.c.family_revision_id.is_not(None)
    if not allow_retained:
        has_started = has_started | (plan.c.selection_kind == "retained")
    use_legacy = await session.scalar(
        select(select(plan.c.root_record_id).where(plan.c.build_id == build.build_id, has_started).exists())
    )
    if use_legacy:
        return None
    binding = checked_binding(parent.family_id, layout.definition, layout.collection_slots)
    for role, ddl in candidate_layout_statements(binding):
        await _prepare_statement(session)
        await session.execute(text(ddl))
        if role != "winner":
            table = binding.tables_by_role[role]
            readers = await _legacy_readers(session, READ_CAPABILITIES[role], table.schema)
            await _close_and_copy_acl(session, parent, table.name, readers)
    if layout.definition.query.child_collection is not None:
        query_input = query_input_table(parent.family_id, layout)
        await _prepare_statement(session)
        await session.execute(CreateTable(query_input))
        await _close_and_copy_acl(session, parent, query_input.name, ())
    session.add(
        CustomImportFamilyBlockLayout(
            family_id=parent.family_id,
            layout_contract="custom-import/family-block-layout/v1",
            definition_sha256=bytes.fromhex(layout.definition.digest),
            collection_slots=dict(layout.collection_slots),
            phase="loading",
        )
    )
    return binding


async def _lock_open_layout(session, build, family_id, layout):
    family, descriptor = CustomImportSnapshotFamily, CustomImportFamilyBlockLayout
    await _prepare_statement(session)
    locked = await session.scalar(
        select(descriptor.family_id)
        .join(family, family.family_id == descriptor.family_id)
        .where(
            descriptor.family_id == family_id,
            descriptor.phase == "loading",
            descriptor.layout_contract == "custom-import/family-block-layout/v1",
            descriptor.definition_sha256 == bytes.fromhex(layout.definition.digest),
            descriptor.collection_slots == dict(layout.collection_slots),
            *_producer_scope(build),
        )
        .with_for_update(of=(family, descriptor))
    )
    if locked is None:
        raise FamilyBlockUnavailable("compact COPY candidate differs or is closed")


def _batch_tables(family_id, layout, rows_by_role, physical):
    tables_by_role = layout.tables(family_id)
    if physical and "query_input" in rows_by_role:
        tables_by_role = dict(tables_by_role) | {"query_input": query_input_table(family_id, layout)}
    if physical and "candidate_context" in rows_by_role:
        table = snapshot_tables(family_id)[CustomImportBuildCandidateContext.__tablename__]
        tables_by_role = dict(tables_by_role) | {"candidate_context": table}
    return tables_by_role


async def _copy_batch_rows(session, tables_by_role, rows_by_role):
    connection = await session.connection()
    raw = await connection.get_raw_connection()
    for role, batch in rows_by_role.items():
        await _prepare_statement(session)
        table = tables_by_role[role]
        status = await raw.driver_connection.copy_records_to_table(
            table.name, schema_name=table.schema, columns=_copy_role_columns(role, table), records=batch
        )
        if status != f"COPY {len(batch)}":
            raise FamilyBlockUnavailable("compact COPY count differs from the batch")


async def copy_block_batch(session_factory, request, build_id, family_id, layout, rows_by_role, *, physical=False):
    """COPY one bounded batch atomically under the existing build authorization."""
    if request.definition.digest != layout.definition.digest:
        raise FamilyBlockUnavailable("compact COPY definition differs from the request")
    rows_by_role = dict(rows_by_role)
    tables_by_role = _batch_tables(family_id, layout, rows_by_role, physical)
    row_limit, byte_limit = (
        (MAX_BATCH_ROWS, MAX_BATCH_BYTES) if physical else (request.page_row_limit, request.page_byte_limit)
    )
    _check_batch(tables_by_role, rows_by_role, row_limit=row_limit, byte_limit=byte_limit)
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, family_id, layout)
        await _copy_batch_rows(session, tables_by_role, rows_by_role)
    return {role: len(batch) for role, batch in rows_by_role.items()}
