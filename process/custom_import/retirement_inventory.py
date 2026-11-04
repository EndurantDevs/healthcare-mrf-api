# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connection-owned, bounded reference preparation for frozen snapshots."""

from dataclasses import dataclass

from sqlalchemy import BigInteger, Column, MetaData, String, Table, exists, select, text

from db.models.custom_import_storage import (
    CustomImportSnapshotFamily,
    CustomImportSnapshotRelation,
    CustomImportSnapshotRetirement,
)
from process.custom_import.read_core import _is_same_read_transaction, _read_transaction_identity
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

_TABLE = "custom_import_retirement_reference_inventory"
_INDEX = "custom_import_retirement_reference_inventory_ranges"
_KEY = "custom_import_retirement_reference_inventory"
_OWNED_KEY = "custom_import_retirement_reference_inventory_owned"


@dataclass(frozen=True)
class _Inventory:
    connection: object
    schema: str
    identity: tuple
    families: dict
    coverage: dict
    rows: int
    references: int
    transactions: tuple


def _refuse(message):
    from process.custom_import.retirement import SnapshotRetirementRefused

    raise SnapshotRetirementRefused("snapshot retirement inventory " + message)


async def _temporary_identity(session):
    row = (
        await session.execute(
            text(f"""
        SELECT pg_backend_pid(), pg_my_temp_schema(), c.oid, c.relowner, c.relfilenode,
            (SELECT i.indexrelid FROM pg_catalog.pg_index i
             JOIN pg_catalog.pg_class ix ON ix.oid=i.indexrelid
             WHERE i.indrelid=c.oid AND ix.relname='{_INDEX}' AND i.indisvalid AND i.indisready)
        FROM pg_catalog.pg_class c
        WHERE c.oid=to_regclass('pg_temp.{_TABLE}') AND c.relnamespace=pg_my_temp_schema()
            AND c.relpersistence='t' AND c.relkind='r'
    """)
        )
    ).one_or_none()
    return None if row is None else tuple(row)


async def cleanup_frozen_reference_inventory(session):
    """Drop only this connection's recorded temporary relation; caller commits."""
    connection = await session.connection()
    owned = connection.info.get(_OWNED_KEY)
    if owned is None:
        return
    identity = await _temporary_identity(session)
    if identity is not None:
        if owned[0] is not connection or identity[:4] != owned[1][:4]:
            _refuse("temporary relation ownership changed")
        await session.execute(text(f"DROP TABLE pg_temp.{_TABLE}"))
    connection.info.pop(_KEY, None)
    connection.info.pop(_OWNED_KEY, None)


async def _frozen_families(session):
    return (
        await session.scalars(
            select(CustomImportSnapshotFamily)
            .where(
                CustomImportSnapshotFamily.frozen_at.is_not(None),
                ~exists().where(
                    CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
                ),
            )
            .order_by(CustomImportSnapshotFamily.family_id)
            .execution_options(populate_existing=True)
        )
    ).all()


def _reference_tables(family):
    tables_by_name = dict(snapshot_tables(family.family_id))
    if family.origin_table_oid is not None:
        tables_by_name["legacy_copy_origin"] = Table(
            "legacy_copy_origin",
            MetaData(),
            Column("kind", String(5), primary_key=True),
            Column("revision_id", BigInteger, primary_key=True),
            Column("root_revision_id", BigInteger),
            Column("base_root_revision_id", BigInteger),
            Column("base_child_revision_id", BigInteger),
            schema=snapshot_schema(family.family_id),
        )
    return tables_by_name


async def _pin_family(session, quoted, family):
    """Validate the native registry on both sides of read-compatible leaf pins."""
    resolve = text(f"SELECT * FROM {quoted}.resolve_custom_import_snapshot_relations(:family)")
    await session.execute(resolve, {"family": family.family_id})
    names = list(_reference_tables(family))
    if family.landing_table_oid is not None:
        names.append("source_bulk_landing")
    namespace = snapshot_schema(family.family_id)
    relation_sql = ",".join(f'"{namespace}"."{name}"' for name in names)
    await session.execute(text(f"LOCK TABLE {relation_sql} IN ACCESS SHARE MODE NOWAIT"))
    from process.custom_import.retirement import _close_frozen_reference_heaps

    await _close_frozen_reference_heaps(session, family)
    await session.execute(resolve, {"family": family.family_id})
    physical = (
        await session.execute(
            text("""
        SELECT c.relname,c.oid,c.relowner,c.relfilenode FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:namespace AND c.relname=ANY(CAST(:names AS text[]))
            AND c.relkind='r' ORDER BY c.relname
    """),
            {"namespace": namespace, "names": names},
        )
    ).all()
    if len(physical) != len(names):
        _refuse("physical relations are incomplete")
    registry = (
        await session.execute(
            select(*CustomImportSnapshotRelation.__table__.columns)
            .where(CustomImportSnapshotRelation.family_id == family.family_id)
            .order_by(CustomImportSnapshotRelation.relation_slot)
        )
    ).all()
    family_values = tuple(getattr(family, column.name) for column in CustomImportSnapshotFamily.__table__.columns)
    return family_values, tuple(tuple(entry) for entry in registry), tuple(tuple(entry) for entry in physical)


async def _page_columns(session, table, table_oid):
    primary_columns = tuple(column.name for column in table.primary_key.columns)
    actual = (
        await session.execute(
            text("""
        SELECT a.attname,a.attnotnull FROM pg_catalog.pg_index i,
            unnest(i.indkey) WITH ORDINALITY k(attnum,ordinality)
        JOIN pg_catalog.pg_attribute a ON a.attrelid=:oid AND a.attnum=k.attnum
        WHERE i.indrelid=:oid AND i.indisprimary AND i.indisvalid AND i.indisready
        ORDER BY k.ordinality
    """),
            {"oid": table_oid},
        )
    ).all()
    if (
        not primary_columns
        or tuple(row.attname for row in actual) != primary_columns
        or not all(row.attnotnull for row in actual)
    ):
        _refuse("reference relation primary key differs")
    return primary_columns


def _reference_page_statement(table, primary_columns, reference_kinds_by_column, has_after, quote):
    """Aggregate one bounded keyset page without exporting its reference rows."""
    relation = f"{quote(table.schema)}.{quote(table.name)}"
    names = tuple(
        dict.fromkeys(
            (
                *primary_columns,
                *reference_kinds_by_column,
                *(("kind",) if None in reference_kinds_by_column.values() else ()),
            )
        )
    )
    selected = ",".join(quote(name) for name in names)
    order = ",".join(quote(name) for name in primary_columns)
    descending = ",".join(quote(name) + " DESC" for name in primary_columns)
    reference_queries = []
    for name, kind in reference_kinds_by_column.items():
        expression = str(kind) if kind is not None else "CASE kind WHEN 'root' THEN 1 WHEN 'child' THEN 2 END"
        reference_queries.append(
            f"SELECT '{name}' AS reference_column,{expression} AS revision_kind,{quote(name)} AS revision_id "
            f"FROM page WHERE {quote(name)} IS NOT NULL"
        )
    predicate = (
        f"WHERE ROW({order}) > ROW({','.join(':key_' + str(i) for i in range(len(primary_columns)))})"
        if has_after
        else ""
    )
    return text(f"""
            WITH page AS MATERIALIZED (
                SELECT {selected} FROM {relation} {predicate} ORDER BY {order} LIMIT :limit
            ), reference_values AS ({" UNION ALL ".join(reference_queries)}), inserted AS (
                INSERT INTO pg_temp.{_TABLE}
                    (source_family_id,source_table_oid,reference_column,revision_kind,page_number,reference_count,revision_ids)
                SELECT :family,:oid,reference_column,revision_kind,:page,count(*),
                    range_agg(int8range(revision_id,
                        CASE WHEN revision_id=9223372036854775807 THEN NULL ELSE revision_id+1 END,'[)'))
                FROM reference_values GROUP BY reference_column,revision_kind
                RETURNING reference_count
            )
            SELECT {order},(SELECT count(*) FROM page) AS inventory_source_rows,
                (SELECT count(*) FROM inserted) AS inventory_rows,
                (SELECT coalesce(sum(reference_count),0) FROM inserted) AS inventory_references
            FROM page ORDER BY {descending} LIMIT 1
        """)


async def _copy_reference_pages(session, family_id, table, table_oid, reference_kinds_by_column):
    """Return only page accounting and the last PK; reference IDs stay in SQL."""
    from process.custom_import.retirement import REFERENCE_PAGE_ROWS

    primary_columns = await _page_columns(session, table, table_oid)
    connection = await session.connection()
    after_keys = None
    page_number = inventory_rows = reference_count = source_rows = 0
    while True:
        statement = _reference_page_statement(
            table,
            primary_columns,
            reference_kinds_by_column,
            after_keys is not None,
            connection.dialect.identifier_preparer.quote,
        )
        parameters_by_name = {"family": family_id, "oid": table_oid, "page": page_number, "limit": REFERENCE_PAGE_ROWS}
        if after_keys is not None:
            parameters_by_name.update({"key_" + str(index): key for index, key in enumerate(after_keys)})
        cursor = (await session.execute(statement, parameters_by_name)).mappings().one_or_none()
        if cursor is None:
            return inventory_rows, reference_count, (page_number, source_rows)
        next_keys = tuple(cursor[name] for name in primary_columns)
        if next_keys == after_keys or not 0 < cursor["inventory_source_rows"] <= REFERENCE_PAGE_ROWS:
            _refuse("reference page did not advance")
        after_keys = next_keys
        page_number += 1
        source_rows += cursor["inventory_source_rows"]
        inventory_rows += cursor["inventory_rows"]
        reference_count += cursor["inventory_references"]


async def _create_inventory(session, connection):
    """Create only the exact owner-tracked temporary relation in this transaction."""
    await cleanup_frozen_reference_inventory(session)
    if await _temporary_identity(session) is not None:
        _refuse("refuses an unowned temporary relation")
    await session.execute(
        text(f"""
        CREATE TEMP TABLE {_TABLE} (
            source_family_id bigint NOT NULL,source_table_oid oid NOT NULL,reference_column text NOT NULL,
            revision_kind smallint NOT NULL CHECK (revision_kind IN (1,2)),page_number bigint NOT NULL,
            reference_count bigint NOT NULL CHECK (reference_count>0),revision_ids int8multirange NOT NULL,
            PRIMARY KEY(source_family_id,source_table_oid,reference_column,revision_kind,page_number)
        ) ON COMMIT PRESERVE ROWS
    """)
    )
    connection.info[_OWNED_KEY] = (connection, await _temporary_identity(session))


async def prepare_frozen_reference_inventory(session):
    """Prepare under read-compatible write exclusion, valid only until this transaction ends."""
    from process.custom_import.retirement import (
        _REFERENCES,
        _indexed_reference_columns,
        _owner_schema,
    )

    schema, quoted = await _owner_schema(session)
    connection = await session.connection()
    await _create_inventory(session, connection)
    families, coverage = {}, {}
    inventory_rows = reference_count = 0
    for family in await _frozen_families(session):
        binding = await _pin_family(session, quoted, family)
        families[family.family_id] = binding
        oid_by_name = {entry[0]: entry[1] for entry in binding[2]}
        for name, table in sorted(_reference_tables(family).items()):
            if name not in _REFERENCES:
                continue
            indexed = await _indexed_reference_columns(session, table)
            reference_kinds_by_column = {
                column: kind for column, kind in _REFERENCES[name].items() if column not in indexed or kind is None
            }
            if not reference_kinds_by_column:
                continue
            inserted_rows, count, pages = await _copy_reference_pages(
                session, family.family_id, table, oid_by_name[name], reference_kinds_by_column
            )
            inventory_rows += inserted_rows
            reference_count += count
            coverage[family.family_id, name] = (frozenset(reference_kinds_by_column.items()), pages)
    await session.execute(text(f"CREATE INDEX {_INDEX} ON pg_temp.{_TABLE} USING gist(revision_ids)"))
    await session.execute(text(f"ANALYZE pg_temp.{_TABLE}"))
    identity = await _temporary_identity(session)
    if identity is None or identity[-1] is None:
        _refuse("range index is unavailable")
    connection.info[_KEY] = _Inventory(
        connection,
        schema,
        identity,
        families,
        coverage,
        inventory_rows,
        reference_count,
        await _read_transaction_identity(session),
    )
    return {"families": len(families), "pages": sum(entry[1][0] for entry in coverage.values()), "rows": inventory_rows}


async def can_prove_frozen_reference_inventory(session, family, retained):
    """Use only live same-transaction preparation, rechecking every retained source."""
    from process.custom_import.retirement import _owner_schema

    connection = await session.connection()
    inventory = connection.info.get(_KEY)
    identity = await _temporary_identity(session)
    if inventory is None:
        if identity is not None or connection.info.get(_OWNED_KEY) is not None:
            _refuse("preparation is incomplete")
        return False
    schema, quoted = await _owner_schema(session)
    if inventory.connection is not connection or inventory.schema != schema or identity != inventory.identity:
        _refuse("connection or temporary relation changed")
    if not _is_same_read_transaction(inventory.transactions, await _read_transaction_identity(session)):
        # Previous SHARE locks ended. Owner DML may have added references without
        # changing any OID or relfilenode; the caller must perform a fresh scan.
        return False
    totals = (
        await session.execute(text(f"SELECT count(*),coalesce(sum(reference_count),0) FROM pg_temp.{_TABLE}"))
    ).one()
    if tuple(totals) != (inventory.rows, inventory.references):
        _refuse("page accounting changed")
    frozen_families = [entry for entry in await _frozen_families(session) if entry.family_id != family.family_id]
    expected_ids = {
        entry.family_id for entry in retained if entry.family_id != family.family_id and entry.frozen_at is not None
    }
    if {entry.family_id for entry in frozen_families} != expected_ids:
        _refuse("frozen reference census changed")
    await _check_frozen_reference_coverage(session, quoted, family, frozen_families, inventory)
    return True


async def _check_frozen_reference_coverage(session, quoted, family, frozen_families, inventory):
    """Check every retained source and its complete actual revision references."""
    from process.custom_import.retirement import _REFERENCES, _indexed_revision_references

    expected_ids = {entry.family_id for entry in frozen_families}
    removed = set(inventory.families) - expected_ids - {family.family_id}
    if removed and await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM unnest(CAST(:removed AS bigint[])) old(family_id)
            WHERE NOT EXISTS(SELECT 1 FROM {quoted}.custom_import_snapshot_retirement r WHERE r.family_id=old.family_id)
            OR to_regnamespace('ci_snapshot_' || old.family_id::text) IS NOT NULL)
    """),
        {"removed": sorted(removed)},
    ):
        _refuse("previously frozen source disappeared without retirement")
    for kept_family in frozen_families:
        if await _pin_family(session, quoted, kept_family) != inventory.families.get(kept_family.family_id):
            _refuse("frozen family binding changed or is missing")
        for name, table in sorted(_reference_tables(kept_family).items()):
            if name not in _REFERENCES:
                continue
            remaining = await _indexed_revision_references(session, table, family.family_id, _REFERENCES[name])
            scanned = inventory.coverage.get((kept_family.family_id, name), (frozenset(),))[0]
            if not remaining.items() <= scanned:
                _refuse("reference column coverage changed")
    if frozen_families and await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {quoted}.custom_import_revision_home h
            JOIN pg_temp.{_TABLE} r ON r.revision_kind=h.revision_kind AND r.revision_ids && h.revision_ids
            WHERE h.family_id=:family AND r.source_family_id=ANY(CAST(:sources AS bigint[])))
    """),
        {"family": family.family_id, "sources": [entry.family_id for entry in frozen_families]},
    ):
        _refuse("retains a revision reference")
