# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native retirement views for both physical layouts, never writer authority."""

from dataclasses import dataclass

from sqlalchemy import case, exists, func, select, text, tuple_

from db.models.custom_import import CustomImportDefinitionRevision
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from db.models.custom_import_storage import CustomImportRevisionHome, CustomImportSnapshotRelation
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import resolve_family_block_binding
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT
from process.custom_import.storage_layout import snapshot_tables

MAX_MEMBERSHIP_PAGE_ROWS = 256
MAX_MEMBERSHIP_PAGE_BYTES = 8 * 1024**2
MAX_MEMBERSHIP_IDS = 50_000
_MEMBERSHIP_KEYS = ("family_revision_id", "collection_slot", "first_revision_id")


@dataclass(frozen=True)
class RetirementStorage:
    """Fresh native physical custody scoped to the caller's locked transaction."""

    tables_by_name: dict
    block_binding: object = None

    @property
    def is_block(self):
        """Return whether this exact native registry selected the block layout."""
        return self.block_binding is not None

    @property
    def registry_model(self):
        """Select the original immutable relation audit for the physical layout."""
        return CustomImportFamilyBlockRelation if self.is_block else CustomImportSnapshotRelation


def _refuse(message):
    from process.custom_import.retirement import SnapshotRetirementRefused

    raise SnapshotRetirementRefused("snapshot retirement " + message)


async def resolve_storage(session, quoted, family):
    """Reuse the existing resolver's exact native role, ACL and catalog checks."""
    relation_rows = (
        await session.execute(
            text(f"SELECT * FROM {quoted}.resolve_custom_import_snapshot_relations(:family)"),
            {"family": family.family_id},
        )
    ).all()
    oids_by_slot = dict(relation_rows)
    if len(relation_rows) != len(oids_by_slot):
        _refuse("relation registry repeats a role")
    if set(oids_by_slot) == set(range(1, 16)):
        return RetirementStorage(dict(snapshot_tables(family.family_id)))
    descriptor = await session.get(CustomImportFamilyBlockLayout, family.family_id)
    revision = await session.get(CustomImportDefinitionRevision, family.definition_revision_id)
    if (
        descriptor is None
        or revision is None
        or (
            descriptor.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
            or revision.dataset_id != family.dataset_id
            or revision.schema_revision_id != family.schema_revision_id
        )
    ):
        _refuse("block definition or layout is missing")
    definition = CustomImportDefinition.from_json(revision.canonical_definition)
    if bytes(revision.definition_sha256) != bytes.fromhex(definition.digest) or (
        bytes(descriptor.definition_sha256) != bytes.fromhex(definition.digest)
    ):
        _refuse("block definition identity differs")
    binding = await resolve_family_block_binding(
        session, family.family_id, definition, descriptor.collection_slots, persistence="candidate"
    )
    if binding is None or bytes(descriptor.layout_sha256) != binding.layout.digest:
        _refuse("block native binding differs")
    return RetirementStorage({table.name: table for table in binding.tables_by_role.values()}, binding)


def reference_columns(storage, table):
    """Add actual block revision fields to the unchanged legacy reference census."""
    from process.custom_import.retirement import _REFERENCES

    if storage.is_block:
        columns_by_table = {
            "custom_import_block_family": {"root_revision_id": 1},
            "custom_import_block_query_record": {"child_revision_id": 2},
        }
        if table.name in columns_by_table:
            return columns_by_table[table.name]
    return _REFERENCES.get(table.name, {})


def reference_tables(storage):
    """Include compressed membership even when it has no scalar revision field."""
    return {
        name: table
        for name, table in storage.tables_by_name.items()
        if reference_columns(storage, table) or name == "custom_import_block_revision_run"
    }


async def assert_no_membership_references(session, quoted, storage, family_id):
    """Validate sparse actual IDs; dense native ranges never infer absent members."""
    if not storage.is_block:
        return
    table = storage.block_binding.tables_by_role["revision_run"]
    home = CustomImportRevisionHome
    upper = case((table.c.last_revision_id == 2**63 - 1, None), else_=table.c.last_revision_id + 1)
    covered = func.int8multirange(func.int8range(table.c.first_revision_id, upper, "[)"))
    dense = (
        select(1)
        .select_from(table)
        .where(
            table.c.encoded_membership.is_(None),
            exists().where(
                home.family_id == family_id,
                home.revision_kind == case((table.c.collection_slot == 0, 1), else_=2),
                home.revision_ids.op("&&")(covered),
            ),
        )
    )
    if await session.scalar(select(exists(dense))):
        _refuse("retains a dense block revision reference")
    await _sparse_references(session, quoted, table, family_id)


async def _membership_page(session, table, after_keys):
    columns = tuple(table.c[name] for name in _MEMBERSHIP_KEYS)
    statement = (
        select(*columns, func.octet_length(table.c.encoded_membership).label("encoded_bytes"))
        .where(table.c.encoded_membership.is_not(None))
        .order_by(*columns)
        .limit(MAX_MEMBERSHIP_PAGE_ROWS)
    )
    if after_keys is not None:
        statement = statement.where(tuple_(*columns) > tuple_(*after_keys))
    metadata = (await session.execute(statement)).mappings().all()
    admitted, total = [], 0
    for header in metadata:
        size = header["encoded_bytes"]
        if type(size) is not int or size <= 0 or size + 256 > MAX_MEMBERSHIP_PAGE_BYTES:
            _refuse("block membership exceeds bounded retirement memory")
        if total + size + 256 > MAX_MEMBERSHIP_PAGE_BYTES:
            break
        admitted.append(header)
        total += size + 256
    if not admitted:
        return (), after_keys
    last_keys = tuple(admitted[-1][name] for name in _MEMBERSHIP_KEYS)
    membership_rows = (
        select(table)
        .where(table.c.encoded_membership.is_not(None), tuple_(*columns) <= tuple_(*last_keys))
        .order_by(*columns)
    )
    if after_keys is not None:
        membership_rows = membership_rows.where(tuple_(*columns) > tuple_(*after_keys))
    metadata_rows = (await session.execute(membership_rows)).mappings().all()
    observed_keys = [
        tuple(membership_row[name] for name in _MEMBERSHIP_KEYS) + (len(membership_row["encoded_membership"]),)
        for membership_row in metadata_rows
    ]
    expected_keys = [
        tuple(header[name] for name in _MEMBERSHIP_KEYS) + (header["encoded_bytes"],) for header in admitted
    ]
    if observed_keys != expected_keys or last_keys == after_keys:
        _refuse("block membership page changed or did not advance")
    return metadata_rows, last_keys


async def _assert_ids(session, quoted, family_id, ids_by_kind):
    for kind, identities in ids_by_kind.items():
        if identities and await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {quoted}.custom_import_revision_home h, "
                "unnest(CAST(:ids AS bigint[])) i(revision_id) WHERE h.family_id=:family "
                "AND h.revision_kind=:kind AND h.revision_ids @> i.revision_id)"
            ),
            {"ids": identities, "family": family_id, "kind": kind},
        ):
            _refuse("retains a sparse block revision reference")
        identities.clear()


async def _sparse_references(session, quoted, table, family_id):
    after_keys = None
    ids_by_kind = {1: [], 2: []}
    while True:
        rows, next_keys = await _membership_page(session, table, after_keys)
        if not rows:
            return
        for row in rows:
            membership = RevisionMembership(
                *(
                    row[name]
                    for name in (
                        "collection_slot",
                        "first_revision_id",
                        "last_revision_id",
                        "record_count",
                        "first_record_ordinal",
                        "encoded_membership",
                    )
                )
            )
            kind = 1 if membership.collection_slot == 0 else 2
            for revision_id, _ordinal in membership.records():
                ids_by_kind[kind].append(revision_id)
                if sum(map(len, ids_by_kind.values())) >= MAX_MEMBERSHIP_IDS:
                    await _assert_ids(session, quoted, family_id, ids_by_kind)
        await _assert_ids(session, quoted, family_id, ids_by_kind)
        after_keys = next_keys
