# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected, transactional retirement of unreferenced physical snapshot families.

The caller owns the transaction and must provide its external control-plane
reference check on this same session. Ordinary readers and workers cannot own
this operation. No authority, external-reference proof, or commit is inferred.

The complete reference census pins every retained family's relations and holds
SHARE locks on frozen reference heaps until caller commit. Its cost grows with
the retained catalog, and the final closure has a one-second deadline. Exhausted
lock capacity or a timeout requires a full rollback, not a partial reclaim.
"""

import asyncio
import re
from dataclasses import dataclass

from sqlalchemy import (
    BigInteger,
    Column,
    MetaData,
    String,
    Table,
    case,
    delete,
    exists,
    func,
    or_,
    select,
    text,
    true,
    tuple_,
)

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportExecution,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportPublicationEvent,
)
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import (
    CustomImportRevisionHome,
    CustomImportSnapshotFamily,
    CustomImportSnapshotRelation,
    CustomImportSnapshotRetirement,
)
from process.custom_import.retirement_history import (
    assert_closed_continuations,
    lock_historical_projection,
)
from process.custom_import.storage_layout import (
    SNAPSHOT_MODELS,
    snapshot_schema,
    snapshot_tables,
)

REFERENCE_PAGE_ROWS = 50_000
MAX_RETIREMENT_SECONDS = 120
MAX_REFERENCE_CLOSURE_SECONDS = 1
_IDENTITY_NAMES = (
    "family_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "execution_id",
    "capture_bundle_id",
    "producing_fence",
    "producing_token_sha256",
    "generation_id",
)
_REFERENCES = {
    "custom_import_root_revision": {"root_revision_id": 1},
    "custom_import_child_revision": {"child_revision_id": 2},
    "custom_import_family_revision": {"root_revision_id": 1},
    "custom_import_family_child": {"child_revision_id": 2},
    "custom_import_root_scalar": {"root_revision_id": 1},
    "custom_import_child_scalar": {"child_revision_id": 2},
    "custom_import_winner": {"context_child_revision_id": 2},
    "custom_import_build_occurrence": {
        "root_revision_id": 1,
        "base_root_revision_id": 1,
        "child_revision_id": 2,
        "base_child_revision_id": 2,
    },
    "custom_import_build_family": {"last_input_child_revision_id": 2},
    "custom_import_build_candidate_context": {"context_child_revision_id": 2},
    "legacy_copy_origin": {
        "revision_id": None,
        "root_revision_id": 1,
        "base_root_revision_id": 1,
        "base_child_revision_id": 2,
    },
}


class SnapshotRetirementRefused(ValueError):
    """The exact family cannot be retired with the available native evidence."""


@dataclass(frozen=True)
class SnapshotRetirementReceipt:
    family_id: int
    reason: str
    retained_bytes: int
    replayed: bool


def lifecycle_lock_key(engine_id, dataset_id):
    """Shared mutation scope; reader pinning remains the existing leaf locks."""
    snapshot_schema(dataset_id)
    if type(engine_id) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", engine_id) is None:
        raise SnapshotRetirementRefused("snapshot retirement engine identity is invalid")
    return f"custom-import/snapshot-lifecycle/v1:{engine_id}:{dataset_id}"


async def lock_snapshot_lifecycle(session, *, engine_id, dataset_id):
    """Control writers take this lock before observing engine evidence, until commit."""
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(:scope, 0))"),
        {"scope": lifecycle_lock_key(engine_id, dataset_id)},
    )


async def _owner_schema(session):
    if not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise SnapshotRetirementRefused("snapshot retirement requires a clean caller transaction")
    connection = await session.connection()
    schema = CustomImportSnapshotFamily.__table__.schema
    translations = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = translations.get(schema, schema)
    if not isinstance(schema, str) or not schema or len(schema.encode()) > 63:
        raise SnapshotRetirementRefused("snapshot retirement schema is invalid")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    allowed = await session.scalar(
        text("""
        SELECT current_user = session_user
          AND current_setting('transaction_isolation') = 'read committed'
          AND current_setting('transaction_read_only') = 'off'
          AND current_setting('statement_timeout')::interval > interval '0'
          AND current_setting('lock_timeout')::interval > interval '0'
          AND EXISTS (
              SELECT 1 FROM pg_catalog.pg_roles r JOIN pg_catalog.pg_class c
                ON c.oid=pg_catalog.to_regclass(:registry)
              WHERE r.rolname=current_user AND r.rolsuper
          )
    """),
        {"registry": f"{quoted}.custom_import_snapshot_family"},
    )
    if allowed is not True:
        raise SnapshotRetirementRefused(
            "snapshot retirement requires protected native owner authority and finite bounds"
        )
    return schema, quoted


async def _retained_pointers(session, family):
    current = await session.scalar(
        select(CustomImportCurrentGeneration)
        .where(
            CustomImportCurrentGeneration.dataset_id == family.dataset_id,
        )
        .execution_options(populate_existing=True)
    )
    event = await session.scalar(
        select(CustomImportPublicationEvent)
        .where(
            CustomImportPublicationEvent.dataset_id == family.dataset_id,
            CustomImportPublicationEvent.event_kind.in_(("activated", "rolled_back")),
        )
        .order_by(CustomImportPublicationEvent.publication_event_id.desc())
        .limit(1)
    )
    if (current is None) != (event is None) or (
        current is not None
        and (
            event.to_generation_id != current.generation_id
            or event.committed_pointer_version != current.pointer_version
        )
    ):
        raise SnapshotRetirementRefused("snapshot retirement publication history disagrees with the current pointer")
    return (
        None if current is None else current.generation_id,
        None if event is None else event.from_generation_id,
    )


async def _eligibility(session, family, current, rollback):
    execution = await session.scalar(
        select(CustomImportExecution)
        .where(
            CustomImportExecution.execution_id == family.execution_id,
            CustomImportExecution.dataset_id == family.dataset_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    lease = await session.scalar(
        select(CustomImportLease)
        .where(
            CustomImportLease.execution_id == family.execution_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if execution is None or lease is None:
        raise SnapshotRetirementRefused("snapshot retirement producer identity is missing")
    if family.generation_id is not None and family.generation_id in (current, rollback):
        raise SnapshotRetirementRefused("snapshot retirement target is current or retained for rollback")
    if family.frozen_at is None and lease.fence > family.producing_fence:
        return "replaced_fence"
    if family.generation_id is None:
        if family.frozen_at is None and execution.state in ("canceled", "failed"):
            now = await session.scalar(select(func.clock_timestamp()))
            if lease.expires_at is not None and lease.expires_at <= now:
                return "abandoned"
    elif execution.state == "no_change":
        if await session.scalar(
            select(
                exists().where(
                    CustomImportNoChangeSeal.execution_id == family.execution_id,
                    CustomImportNoChangeSeal.candidate_generation_id == family.generation_id,
                )
            )
        ):
            return "no_change"
    elif execution.state in ("completed", "canceled", "failed"):
        if execution.state == "completed" and not await _previously_published(session, family):
            raise SnapshotRetirementRefused("snapshot retirement completed generation has no publication history")
        return "superseded"
    raise SnapshotRetirementRefused("snapshot retirement producer is not eligible")


async def _previously_published(session, family):
    """A completed candidate remains usable until it has actually been published."""
    return await session.scalar(
        select(
            exists().where(
                CustomImportPublicationEvent.dataset_id == family.dataset_id,
                CustomImportPublicationEvent.definition_revision_id == family.definition_revision_id,
                CustomImportPublicationEvent.schema_revision_id == family.schema_revision_id,
                CustomImportPublicationEvent.execution_id == family.execution_id,
                CustomImportPublicationEvent.to_generation_id == family.generation_id,
                CustomImportPublicationEvent.event_kind.in_(("activated", "rolled_back")),
            )
        )
    )


async def _indexed_reference_columns(session, table):
    """Use existing unconditional leading B-trees without changing live indexes."""
    connection = await session.connection()
    translations = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    preparer = connection.dialect.identifier_preparer
    schema = translations.get(table.schema, table.schema)
    identity = f"{preparer.quote_schema(schema)}.{preparer.quote(table.name)}"
    return set(
        await session.scalars(
            text("""
        SELECT a.attname FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
        JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
        JOIN pg_catalog.pg_am m ON m.oid=c.relam
        WHERE i.indrelid=pg_catalog.to_regclass(:identity) AND m.amname='btree'
          AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL
    """),
            {"identity": identity},
        )
    )


async def _indexed_revision_references(session, table, family_id, reference_kinds_by_column):
    indexed_columns = await _indexed_reference_columns(session, table)
    home = CustomImportRevisionHome
    ranges = func.unnest(home.revision_ids).table_valued("revision_ids").render_derived().lateral()
    for name, kind in reference_kinds_by_column.items():
        if name not in indexed_columns or kind is None:
            continue
        match = exists().where(
            table.c[name] >= func.lower(ranges.c.revision_ids),
            table.c[name]
            <= case((func.upper_inf(ranges.c.revision_ids), 2**63 - 1), else_=func.upper(ranges.c.revision_ids) - 1),
        )
        references = (
            select(1)
            .select_from(home)
            .join(ranges, true())
            .where(home.family_id == family_id, home.revision_kind == kind, match)
        )
        if await session.scalar(select(exists(references))):
            raise SnapshotRetirementRefused("snapshot retirement retains a revision reference")
    return {
        name: kind for name, kind in reference_kinds_by_column.items() if name not in indexed_columns or kind is None
    }


async def _reference_pages(session, table, family_id, reference_kinds_by_column=None):
    """Probe indexed ID intervals first; page only the remaining reference columns."""
    columns = _REFERENCES[table.name] if reference_kinds_by_column is None else reference_kinds_by_column
    reference_kinds_by_column = await _indexed_revision_references(session, table, family_id, columns)
    if not reference_kinds_by_column:
        return
    primary_columns = tuple(table.primary_key.columns)
    if not primary_columns:
        raise SnapshotRetirementRefused("snapshot retirement reference relation has no primary_columns key")
    columns = tuple(dict.fromkeys((*primary_columns, *(table.c[name] for name in reference_kinds_by_column))))
    after_keys = None
    while True:
        statement = (
            select(*columns)
            .where(or_(*(table.c[name].is_not(None) for name in reference_kinds_by_column)))
            .order_by(*primary_columns)
            .limit(REFERENCE_PAGE_ROWS)
        )
        if after_keys is not None:
            statement = statement.where(tuple_(*primary_columns) > tuple_(*after_keys))
        page = statement.cte("reference_page").prefix_with("MATERIALIZED")
        predicates = [
            exists().where(
                CustomImportRevisionHome.family_id == family_id,
                CustomImportRevisionHome.revision_kind
                == (case((page.c.kind == "root", 1), else_=2) if kind is None else kind),
                CustomImportRevisionHome.revision_ids.op("@>")(page.c[name]),
            )
            for name, kind in reference_kinds_by_column.items()
        ]
        hit = select(exists(select(1).select_from(page).where(or_(*predicates)))).scalar_subquery()
        reference_cursor = (
            await session.execute(
                select(*(page.c[column.name] for column in primary_columns), hit.label("referenced"))
                .select_from(page)
                .order_by(*(page.c[column.name].desc() for column in primary_columns))
                .limit(1)
            )
        ).first()
        if reference_cursor is None:
            return
        if reference_cursor.referenced:
            raise SnapshotRetirementRefused("snapshot retirement retains a revision reference")
        after_keys = tuple(reference_cursor[:-1])


async def _revision_references(session, family, retained, *, include_canonical=True):
    from process.custom_import.retirement_storage import (
        assert_no_membership_references,
        reference_columns,
        resolve_storage,
    )

    if not await session.scalar(select(exists().where(CustomImportRevisionHome.family_id == family.family_id))):
        return
    canonical_tables_by_name = {model.__tablename__: model.__table__ for model in SNAPSHOT_MODELS}
    if include_canonical:
        for name in set(_REFERENCES) & canonical_tables_by_name.keys():
            await _reference_pages(session, canonical_tables_by_name[name], family.family_id)
    _schema, quoted = await _owner_schema(session)
    for retained_family in retained:
        if retained_family.family_id == family.family_id:
            continue
        storage = await resolve_storage(session, quoted, retained_family)
        for table in storage.tables_by_name.values():
            columns = reference_columns(storage, table)
            if columns:
                await _reference_pages(session, table, family.family_id, columns)
        await assert_no_membership_references(session, quoted, storage, family.family_id)
    for retained_family in retained:
        if retained_family.family_id != family.family_id and retained_family.origin_table_oid is not None:
            origin = Table(
                "legacy_copy_origin",
                MetaData(),
                Column("kind", String(5), primary_key=True),
                Column("revision_id", BigInteger, primary_key=True),
                Column("root_revision_id", BigInteger),
                Column("base_root_revision_id", BigInteger),
                Column("base_child_revision_id", BigInteger),
                schema=snapshot_schema(retained_family.family_id),
            )
            await _reference_pages(session, origin, family.family_id)


async def _locked_relations(session, quoted, family, retained):
    from process.custom_import.retirement_storage import resolve_storage

    projection_names = await lock_historical_projection(session, family)
    for row in retained:
        storage = await resolve_storage(session, quoted, row)
        namespace = snapshot_schema(row.family_id)
        names = list(storage.tables_by_name)
        if row.landing_table_oid is not None:
            names.append("source_bulk_landing")
        if row.origin_table_oid is not None:
            names.append("legacy_copy_origin")
        if row.family_id == family.family_id:
            names.extend(projection_names)
        targets = ",".join(f'"{namespace}"."{name}"' for name in dict.fromkeys(names))
        mode = "ACCESS EXCLUSIVE" if row.family_id == family.family_id else "ACCESS SHARE"
        await session.execute(text(f"LOCK TABLE {targets} IN {mode} MODE NOWAIT"))
        if row.family_id != family.family_id and row.frozen_at is not None:
            await _close_frozen_reference_heaps(session, row, quoted)
        await resolve_storage(session, quoted, row)


async def _close_frozen_reference_heaps(session, family, quoted=None):
    """Keep frozen reference scans stable against owner DML while allowing reads."""
    from process.custom_import.retirement_storage import reference_tables, resolve_storage

    if quoted is None:
        _schema, quoted = await _owner_schema(session)
    storage = await resolve_storage(session, quoted, family)
    names = sorted(reference_tables(storage))
    if family.origin_table_oid is not None:
        names.append("legacy_copy_origin")
    namespace = snapshot_schema(family.family_id)
    targets = ",".join(f'"{namespace}"."{name}"' for name in names)
    await session.execute(text(f"LOCK TABLE {targets} IN SHARE MODE NOWAIT"))


async def _close_mutable_references(session, quoted, retained):
    """Close writes only for the short final proof; never wait behind a writer."""
    from process.custom_import.retirement_storage import reference_tables, resolve_storage

    tables = [f'{quoted}."{model.__tablename__}"' for model in SNAPSHOT_MODELS if model.__tablename__ in _REFERENCES]
    for row in retained:
        if row.frozen_at is not None:
            continue
        namespace = snapshot_schema(row.family_id)
        storage = await resolve_storage(session, quoted, row)
        names = reference_tables(storage)
        tables.extend(f'"{namespace}"."{name}"' for name in sorted(names))
        if row.origin_table_oid is not None:
            tables.append(f'"{namespace}".legacy_copy_origin')
    await session.execute(text("LOCK TABLE " + ",".join(tables) + " IN SHARE MODE NOWAIT"))


async def _registry_targets(session, quoted):
    """Close the additive block audit registries when their migration is installed."""
    names = ["custom_import_snapshot_family", "custom_import_snapshot_relation"]
    block_names = ("custom_import_family_block_layout", "custom_import_family_block_relation")
    installed = (
        await session.execute(
            text("SELECT pg_catalog.to_regclass(:layout),pg_catalog.to_regclass(:relations)"),
            {"layout": quoted + "." + block_names[0], "relations": quoted + "." + block_names[1]},
        )
    ).one()
    if any(value is not None for value in installed):
        if any(value is None for value in installed):
            raise SnapshotRetirementRefused("snapshot retirement block registry is incomplete")
        names.extend(block_names)
    return ",".join(quoted + "." + name for name in names)


async def _commit_reference_closure(session, quoted, family, retained, engine_id, reason):
    """Recheck mutable data under bounded write exclusion immediately before DDL."""
    async with asyncio.timeout(MAX_REFERENCE_CLOSURE_SECONDS):
        await session.execute(
            text("""
        SELECT pg_catalog.set_config('statement_timeout',
            least(extract(epoch FROM current_setting('statement_timeout')::interval)*1000,
                  :budget_ms)::bigint::text || 'ms', true)
    """),
            {"budget_ms": int(MAX_REFERENCE_CLOSURE_SECONDS * 1000)},
        )
        family_columns = tuple(CustomImportSnapshotFamily.__table__.columns)
        census = [tuple(getattr(entry, column.name) for column in family_columns) for entry in retained]
        await session.execute(
            text(f"LOCK TABLE {await _registry_targets(session, quoted)} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        )
        fresh_census = (
            await session.execute(
                select(*family_columns)
                .where(
                    ~exists().where(
                        CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
                    )
                )
                .order_by(CustomImportSnapshotFamily.family_id)
            )
        ).all()
        if [tuple(entry) for entry in fresh_census] != census:
            raise SnapshotRetirementRefused("snapshot retirement reference census changed")
        for entry in retained:
            await session.execute(
                text(f"SELECT * FROM {quoted}.resolve_custom_import_snapshot_relations(:family)"),
                {"family": entry.family_id},
            )
        await assert_closed_continuations(session, family)
        mutable_families = [
            entry for entry in retained if entry.family_id != family.family_id and entry.frozen_at is None
        ]
        await _close_mutable_references(session, quoted, mutable_families)
        await _revision_references(session, family, mutable_families)
        return await _retire_verified_family(session, quoted, family, engine_id, reason)


async def _physical_drop(session, quoted, family):
    from process.custom_import.retirement_storage import resolve_storage

    storage = await resolve_storage(session, quoted, family)
    namespace = snapshot_schema(family.family_id)
    routines = (
        await session.execute(
            text("""
        SELECT p.oid, p.oid::regprocedure::text signature, p.prokind::text prokind,
               p.proowner = n.nspowner AS owned
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname=:namespace ORDER BY p.oid
    """),
            {"namespace": namespace},
        )
    ).all()
    if any(not routine.owned or routine.prokind != "f" for routine in routines):
        raise SnapshotRetirementRefused("snapshot retirement routine ownership is incompatible")
    dependencies = (
        await session.execute(
            text("""
        SELECT d.objid, d.refobjid FROM pg_catalog.pg_depend d
        WHERE d.classid='pg_catalog.pg_proc'::regclass AND d.refclassid=d.classid
          AND d.objid=ANY(CAST(:oids AS oid[])) AND d.refobjid=ANY(CAST(:oids AS oid[]))
    """),
            {"oids": [routine.oid for routine in routines]},
        )
    ).all()
    signatures_by_oid = {routine.oid: routine.signature for routine in routines}
    while signatures_by_oid:
        ready_oids = [
            oid
            for oid in signatures_by_oid
            if not any(dependent in signatures_by_oid and dependency == oid for dependent, dependency in dependencies)
        ]
        if not ready_oids:
            raise SnapshotRetirementRefused("snapshot retirement routine dependencies are cyclic")
        for oid in ready_oids:
            await session.execute(text(f"DROP FUNCTION {signatures_by_oid.pop(oid)} RESTRICT"))
    names = list(storage.tables_by_name)
    if family.landing_table_oid is not None:
        names.append("source_bulk_landing")
    if family.origin_table_oid is not None:
        names.append("legacy_copy_origin")
    if not storage.is_block and await session.scalar(
        select(exists().where(CustomImportQueryProjection.family_id == family.family_id))
    ):
        names.extend(("custom_import_derived_root", "custom_import_derived_child"))
    qualified_tables_sql = ",".join(f'"{namespace}"."{name}"' for name in names)
    # Native RESTRICT protects external dependents, including row-type consumers.
    await session.execute(text(f"DROP TABLE {qualified_tables_sql} RESTRICT"))
    await session.execute(text(f'DROP SCHEMA "{namespace}" RESTRICT'))
    models = (
        (CustomImportRevisionHome,) if storage.is_block else (CustomImportRevisionHome, CustomImportSnapshotRelation)
    )
    for model in models:
        delete_result = await session.execute(delete(model).where(model.family_id == family.family_id))
        if model is CustomImportSnapshotRelation and delete_result.rowcount != 15:
            raise SnapshotRetirementRefused("snapshot retirement relation census changed")


async def _locked_family(session, family_id, engine_id):
    """Reload both possible identities after acquiring the shared mutation lock."""
    family = await session.get(CustomImportSnapshotFamily, family_id)
    tombstone = await session.get(CustomImportSnapshotRetirement, family_id)
    identity = family or tombstone
    if identity is None:
        raise SnapshotRetirementRefused("snapshot retirement family is unknown")
    await lock_snapshot_lifecycle(session, engine_id=engine_id, dataset_id=identity.dataset_id)
    await session.scalar(
        select(CustomImportDataset.dataset_id)
        .where(
            CustomImportDataset.dataset_id == identity.dataset_id,
        )
        .with_for_update()
    )
    family = await session.scalar(
        select(CustomImportSnapshotFamily)
        .where(
            CustomImportSnapshotFamily.family_id == family_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    tombstone = await session.scalar(
        select(CustomImportSnapshotRetirement)
        .where(
            CustomImportSnapshotRetirement.family_id == family_id,
        )
        .execution_options(populate_existing=True)
    )
    identity = family or tombstone
    if identity is None:
        raise SnapshotRetirementRefused("snapshot retirement identity disappeared while locking")
    return family, tombstone


async def _retirement_replay(session, family, tombstone, engine_id):
    if family is None or any(getattr(family, name) != getattr(tombstone, name) for name in _IDENTITY_NAMES):
        raise SnapshotRetirementRefused("snapshot retirement replay retained identity differs")
    if tombstone.engine_id != engine_id:
        raise SnapshotRetirementRefused("snapshot retirement replay engine identity differs")
    if (
        await session.scalar(
            text("SELECT pg_catalog.to_regnamespace(:namespace)"), {"namespace": snapshot_schema(tombstone.family_id)}
        )
    ) is not None:
        raise SnapshotRetirementRefused("snapshot retirement replay found physical storage")
    for model in (CustomImportSnapshotRelation, CustomImportRevisionHome):
        if await session.scalar(select(exists().where(model.family_id == tombstone.family_id))):
            raise SnapshotRetirementRefused("snapshot retirement replay found registry references")
    return SnapshotRetirementReceipt(tombstone.family_id, tombstone.reason, tombstone.retained_bytes, True)


async def _retire_verified_family(session, quoted, family, engine_id, reason):
    family_id = family.family_id
    retained_bytes = await session.scalar(
        text("""
        SELECT coalesce(sum(pg_catalog.pg_total_relation_size(c.oid)),0)::bigint
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:namespace AND c.relkind='r'
    """),
        {"namespace": snapshot_schema(family_id)},
    )
    session.add(
        CustomImportSnapshotRetirement(
            **{name: getattr(family, name) for name in _IDENTITY_NAMES},
            engine_id=engine_id,
            reason=reason,
            retained_bytes=retained_bytes,
        )
    )
    await session.flush()
    await _physical_drop(session, quoted, family)
    return SnapshotRetirementReceipt(family_id, reason, retained_bytes, False)


async def _retire_snapshot(session, family_id, engine_id, assert_external_unreferenced):
    """Retire one exact family atomically; caller must roll back any refusal/error.

    The required external check runs under the common lifecycle lock on this
    exact session. It must compare the supplied current/rollback pointers with
    its own attachment history and refuse every retained external reference.
    """
    from process.custom_import.retirement_inventory import (
        can_prove_frozen_reference_inventory,
    )

    snapshot_schema(family_id)
    lifecycle_lock_key(engine_id, 1)
    if not callable(assert_external_unreferenced):
        raise SnapshotRetirementRefused("snapshot retirement requires an external reference check")
    _schema, quoted = await _owner_schema(session)
    family, tombstone = await _locked_family(session, family_id, engine_id)
    identity = family or tombstone
    current, rollback = await _retained_pointers(session, identity)
    if identity.generation_id is not None and identity.generation_id in (current, rollback):
        raise SnapshotRetirementRefused("snapshot retirement target is current or retained for rollback")
    reason = None if tombstone is not None else await _eligibility(session, family, current, rollback)
    await assert_external_unreferenced(session, identity, current, rollback)
    if session.new or session.dirty or session.deleted:
        raise SnapshotRetirementRefused("snapshot retirement external check changed pending state")
    if tombstone is not None:
        return await _retirement_replay(session, family, tombstone, engine_id)
    await assert_closed_continuations(session, family)
    retained = (
        await session.scalars(
            select(CustomImportSnapshotFamily)
            .where(
                ~exists().where(
                    CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
                )
            )
            .order_by(
                CustomImportSnapshotFamily.family_id,
            )
        )
    ).all()
    await _locked_relations(session, quoted, family, retained)
    frozen_families = [
        entry for entry in retained if entry.family_id != family.family_id and entry.frozen_at is not None
    ]
    if not await can_prove_frozen_reference_inventory(session, family, retained):
        await _revision_references(session, family, frozen_families, include_canonical=False)
    return await _commit_reference_closure(session, quoted, family, retained, engine_id, reason)


async def retire_snapshot_family(session, family_id, *, engine_id, assert_external_unreferenced):
    """Bound the complete owner operation; the caller rolls back all refusals/errors."""
    async with asyncio.timeout(MAX_RETIREMENT_SECONDS):
        return await _retire_snapshot(session, family_id, engine_id, assert_external_unreferenced)
