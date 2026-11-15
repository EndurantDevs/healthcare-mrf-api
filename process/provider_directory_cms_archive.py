# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare canonical archive changes without changing the signed live input."""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from dataclasses import dataclass, field
from importlib import import_module

from sqlalchemy import text

from db.connection import ConnectionProxy, Database
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_generic_source_policy as source
from process.ext.address_canon import _qtable
from process.provider_directory_projection_stage import _copy_driver

_TARGET = "address_archive_v2"
_PREFIX = "cms_archive_"
_SOURCE_FIELDS = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city_name",
    "state": "state_name",
    "zip": "postal_code",
    "country": "COALESCE(NULLIF(country_code, ''), 'US')",
}


async def require_source_archive_owner(session, schema):
    """Refuse an unwritable canonical target before Source signing or preparation."""
    if not source.is_source_storage_required():
        return
    from process import provider_directory_source_observation as observation

    directory = import_module("process.provider_directory_fhir")
    observed = observation.current_observation(directory)
    authority = observed if observed is not None else source.bound_publisher()
    policy = authority.policy
    source._require_active_source_session(session)
    if policy.schema != schema:
        raise RuntimeError("cms_archive_source_binding_changed")
    connection = await _copy_driver(session)
    await connection.execute(f"SELECT 1 FROM {_qtable(schema, _TARGET)} LIMIT 0")


async def _relation_exists(database, schema, name):
    """Resolve the declared qualified name through native PostgreSQL access."""
    return await database.scalar("SELECT to_regclass(:name) IS NOT NULL", name=_qtable(schema, name))


async def _revision(database, schema):
    """Read the registered archive preimage without altering its history."""
    revision = await database.scalar(
        f"SELECT revision FROM {_qtable(schema, 'cms_native_input_revision')} WHERE schema_name=:schema AND table_name=:name",
        schema=schema,
        name=_TARGET,
    )
    if type(revision) is not int or revision < 0:
        raise RuntimeError("cms_archive_input_not_registered")
    return revision


async def _columns(database, schema, name):
    """Retain every ordinary archive column, rejecting generated or identity columns."""
    rows = await database.all(
        "SELECT attname::text,attgenerated::text,attidentity::text FROM pg_attribute "
        "WHERE attrelid=to_regclass(:relation) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        relation=_qtable(schema, name),
    )
    values = [dict(row._mapping) for row in rows]
    if not values or any(row["attgenerated"] or row["attidentity"] for row in values):
        raise RuntimeError("cms_archive_columns_unsupported")
    return tuple(row["attname"] for row in values)


async def _digest_database(database, schema, name):
    """Hash ordered complete rows with bounded client memory in the owner's transaction."""
    digest, row_count = hashlib.sha256(b"cms-prepared-archive-rows-v1\0"), 0
    # A native worker proxy streams on the connection it already owns.
    owner = database._connection if isinstance(database, ConnectionProxy) else database
    result = await owner.stream(
        text(f"SELECT to_jsonb(archived)::text FROM {_qtable(schema, name)} archived ORDER BY address_key")
    )
    try:
        async for row in result:
            digest.update(row[0].encode("utf8"))
            digest.update(b"\n")
            row_count += 1
    finally:
        await result.close()
    return row_count, digest.hexdigest()


async def _digest(fhir, schema, name):
    return await _digest_database(fhir.db, schema, name)


@dataclass
class PreparedArchiveDelta:
    """Owned canonical rows and an effective view retained until the caller commits."""

    schema: str
    delta_table: str
    effective_relation: str
    from_revision: int
    native_input_hash: str
    columns: tuple[str, ...]
    delta_rows: int
    delta_sha256: str
    effective_definition: str
    admission: preparation.NonprofileAdmission = field(repr=False)
    committed: bool = False
    _applied_revision: int | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def relation_overrides(self):
        """Route all candidate archive reads through the complete desired relation."""
        return {_TARGET: self.effective_relation}

    async def lock_read_backend(self, database):
        """Guard the actual native worker connection before reading prepared archive rows."""
        _require_backend(database)
        await self._assert_backend_identity(database, applied=False)

    async def assert_applied_backend(self, database):
        """Require this archive merge to be visible inside the common publication owner."""
        _require_backend(database)
        await self._assert_backend_identity(database, applied=True)
        if self._applied_revision is None or await self._applied_backend_identity(database) != self._applied_revision:
            raise RuntimeError("cms_archive_publication_revision_changed")

    async def _applied_backend_identity(self, database):
        """Read the declared mutable counter in the caller's existing transaction."""
        _require_backend(database)
        return await _revision(database, self.schema)

    async def _assert_backend_identity(self, database, *, applied):
        """Pin declared relations and verify complete content, layout and revision."""
        await database.status(
            f"LOCK TABLE {_qtable(self.schema, self.delta_table)}, {_qtable(self.schema, self.effective_relation)} IN ACCESS SHARE MODE NOWAIT"
        )
        if (
            not await _relation_exists(database, self.schema, _TARGET)
            or await _columns(database, self.schema, _TARGET) != self.columns
            or await _revision(database, self.schema) != self.from_revision + (2 if applied else 0)
            or await database.scalar(
                "SELECT pg_get_viewdef(to_regclass(:relation))", relation=_qtable(self.schema, self.effective_relation)
            )
            != self.effective_definition
            or await _digest_database(database, self.schema, self.delta_table) != (self.delta_rows, self.delta_sha256)
        ):
            raise RuntimeError("cms_archive_preparation_changed")
        await _assert_backend_seal(database, self.schema, self.delta_table)

    async def assert_read_identity(self, fhir, session):
        """Pin and validate the named archive content in the existing transaction."""
        _require_owner(fhir, session)
        await fhir.db.status(
            f"LOCK TABLE {fhir._unscoped_qt(self.schema, self.delta_table)} IN ACCESS SHARE MODE NOWAIT"
        )
        await self.lock_read_backend(fhir.db)

    async def assert_ready(self, fhir, *, cutover=False):
        """Recheck complete named content and the logical revision before publication."""
        async with source.source_transaction(fhir.db) as session:
            async with preparation.active_nonprofile_sql_transaction(fhir):
                await self.assert_read_identity(fhir, session)
                is_cms_build = (
                    preparation._ACTIVE.get() is self.admission
                    and fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
                )
                if not cutover and is_cms_build:
                    if await _digest(fhir, self.schema, self.delta_table) != (self.delta_rows, self.delta_sha256):
                        raise RuntimeError("cms_archive_preparation_changed")

    async def before_lock(self, fhir, session):
        """Take archive write intent before the caller locks signed input revision ledgers."""
        _require_owner(fhir, session)
        await fhir.db.status(f"LOCK TABLE {fhir._unscoped_qt(self.schema, _TARGET)} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        await self.assert_ready(fhir, cutover=True)

    async def apply(self, fhir, session):
        """Merge exact prepared rows into the canonical QName in the current transaction."""
        _require_owner(fhir, session)
        await fhir.db.status(f"LOCK TABLE {fhir._unscoped_qt(self.schema, _TARGET)} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        await self.assert_ready(fhir, cutover=True)
        target_ref = fhir._unscoped_qt(self.schema, _TARGET)
        delta_ref = fhir._unscoped_qt(self.schema, self.delta_table)
        columns_sql = ",".join(fhir._q(column) for column in self.columns)
        assignments_sql = ",".join(
            f"{fhir._q(column)}=EXCLUDED.{fhir._q(column)}" for column in self.columns if column != "address_key"
        )
        await fhir.db.status(f"""INSERT INTO {target_ref} AS incumbent ({columns_sql})
            SELECT {columns_sql} FROM {delta_ref} ORDER BY address_key
            ON CONFLICT (address_key) DO UPDATE SET {assignments_sql}
            WHERE to_jsonb(incumbent) IS DISTINCT FROM to_jsonb(EXCLUDED)""")
        applied = await self._applied_backend_identity(fhir.db)
        to_revision = applied
        # INSERT .. ON CONFLICT fires both statement triggers, including an empty delta.
        if to_revision != self.from_revision + 2:
            raise RuntimeError("cms_archive_publication_revision_changed")
        self._applied_revision = applied
        return {
            "target_schema": self.schema,
            "target_table": _TARGET,
            "from_revision": self.from_revision,
            "to_revision": to_revision,
            "native_input_hash": self.native_input_hash,
            "delta_rows": self.delta_rows,
            "delta_sha256": self.delta_sha256,
        }

    async def mark_committed(self, fhir, result):
        """Consume only the exact immutable result verified by the publication owner."""
        expected_by_field = {
            "target_schema": self.schema,
            "target_table": _TARGET,
            "from_revision": self.from_revision,
            "to_revision": self.from_revision + 2,
            "native_input_hash": self.native_input_hash,
            "delta_rows": self.delta_rows,
            "delta_sha256": self.delta_sha256,
        }
        if fhir.db._transaction_binding() is not None or result != expected_by_field:
            raise RuntimeError("cms_archive_commit_result_changed")
        self.committed = True

    async def cleanup(self, fhir):
        """Remove the declared candidate view and heap, including cancelled builds."""
        await _cleanup(
            fhir,
            self.admission,
            self.schema,
            [
                (self.delta_table, "TABLE"),
                (self.effective_relation, "VIEW"),
            ],
        )


def _require_owner(fhir, session):
    """Never open or commit an independent archive publication transaction."""
    binding = fhir.db._transaction_binding()
    if binding is None or not binding.session.in_transaction() or not session.in_transaction():
        raise RuntimeError("cms_archive_publication_requires_owner_transaction")


def _require_backend(database):
    """Accept only the native transport's actual already-owned transaction."""
    if isinstance(database, Database) and database._transaction_binding() is not None:
        return
    if isinstance(database, ConnectionProxy) and database._connection.in_transaction():
        return
    raise RuntimeError("cms_archive_read_requires_owner_transaction")


def _require_source_entry(database):
    """Require a fresh issued factory before any preparation or cleanup SQL."""
    if source._PUBLISHER.get() is not None or database._transaction_binding() is not None:
        raise RuntimeError("cms_archive_source_transaction_active")
    factory = source._FACTORY.get()
    require_current = getattr(factory, "_require_current_policy", None)
    if factory is None or not callable(require_current):
        raise RuntimeError("cms_archive_source_factory_required")
    require_current()


async def _cleanup(fhir, admission, schema, relations):
    """Drain declared candidate names with native access and dependency protection."""
    if not relations:
        return
    _require_source_entry(fhir.db)
    for name, kind in relations:
        if re.fullmatch(
            r"cms_archive_[0-9a-f]{32}_(input|delta)" if kind == "TABLE" else r"cms_archive_[0-9a-f]{32}_effective",
            name,
        ) is None or kind not in {"TABLE", "VIEW"}:
            raise RuntimeError("cms_archive_cleanup_scope_changed")

    async def remove():
        """Remove captured scratch relations without following view dependencies."""
        for name, kind in reversed(relations):
            _require_source_entry(fhir.db)
            async with source.source_transaction(fhir.db):
                if not await _relation_exists(fhir.db, schema, name):
                    if kind == "TABLE" and (schema, name) in admission._external_relations:
                        await admission.retire_external_relation(fhir, schema, name)
                    continue
                # The frozen owner keeps SELECT only; DROP takes its own lock through ownership.
                await fhir.db.status("SET LOCAL lock_timeout='1s'")
                await fhir.db.status(f"DROP {kind} {fhir._unscoped_qt(schema, name)} RESTRICT")
                if kind == "TABLE":
                    await admission.retire_external_relation(fhir, schema, name)

    task = asyncio.create_task(remove())
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation


async def _capture_heap(fhir, admission, schema, name, relations):
    """Register the actual declared candidate name before its first heap growth."""
    if not await _relation_exists(fhir.db, schema, name):
        raise RuntimeError("cms_archive_created_relation_missing")
    relations.append((name, "TABLE"))
    await admission.register_external_relation(fhir, schema, name)


async def _seed_delta(fhir, admission, schema, input_name, delta_name, relations, options_by_name):
    """Copy exactly incoming originals and approved alias targets before canonical resolution."""
    params_by_name = {}
    if options_by_name["run_id"] is not None:
        params_by_name["run_id"] = options_by_name["run_id"]
    if options_by_name["source_ids"]:
        params_by_name["source_ids"] = list(options_by_name["source_ids"])
    await fhir.db.status(
        fhir.provider_directory_location_archive_stage_sql(
            schema,
            input_name,
            **options_by_name,
        ),
        **params_by_name,
    )
    await _capture_heap(fhir, admission, schema, input_name, relations)
    target_ref = fhir._unscoped_qt(schema, _TARGET)
    input_ref = fhir._unscoped_qt(schema, input_name)
    delta_ref = fhir._unscoped_qt(schema, delta_name)
    await fhir.db.status(f"CREATE UNLOGGED TABLE {delta_ref} (LIKE {target_ref} INCLUDING DEFAULTS)")
    await _capture_heap(fhir, admission, schema, delta_name, relations)
    aliases_ref = fhir._unscoped_qt(schema, fhir.address_alias_sql.ADDRESS_ALIAS_TABLE)
    await fhir.db.status(f"""INSERT INTO {delta_ref} SELECT archived.* FROM {target_ref} archived
        WHERE archived.address_key IN (SELECT address_key FROM {input_ref}
            UNION SELECT active.target_address_key FROM {aliases_ref} active
            JOIN {input_ref} incoming ON incoming.address_key=active.source_address_key
            WHERE active.revoked_at IS NULL)""")
    await fhir.db.status(f"ALTER TABLE {delta_ref} ADD PRIMARY KEY (address_key)")
    await admission.assert_external_relation(fhir, schema, delta_name)


async def _seal_delta(fhir, admission, schema, delta_name, effective_name, relations):
    """Log the measured delta and seal its effective read relation before native preparation."""
    from process.cms_doctors_immutable import _freeze_named_model_relations, _writer
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

    target_ref = fhir._unscoped_qt(schema, _TARGET)
    delta_ref = fhir._unscoped_qt(schema, delta_name)
    await admission.before_logging(fhir, schema, delta_name)
    await fhir.db.status(f"ALTER TABLE {delta_ref} SET LOGGED")
    await fhir.db.status(f"ANALYZE {delta_ref}")
    session = source_copy_session()
    _require_owner(fhir, session)
    await _writer(session)
    publisher = bound_publisher()
    if publisher.policy.schema != schema:
        raise RuntimeError("cms_archive_write_seal_changed")
    await _freeze_named_model_relations(session, schema, (delta_name,), publisher.policy)
    await fhir.db.status(f"""CREATE VIEW {fhir._unscoped_qt(schema, effective_name)} AS
        SELECT archived.* FROM {target_ref} archived
        WHERE NOT EXISTS (SELECT 1 FROM {delta_ref} staged WHERE staged.address_key=archived.address_key)
        UNION ALL SELECT * FROM {delta_ref}""")
    relations.append((effective_name, "VIEW"))
    return await fhir.db.scalar(
        "SELECT pg_get_viewdef(to_regclass(:relation))", relation=fhir._unscoped_qt(schema, effective_name)
    )


async def _enrich_delta(fhir, admission, schema, input_name, delta_name):
    """Resolve archive fields and optional coordinates within the paired remaining deadline."""
    timeout = f"{max(1, int(await preparation.remaining_build_seconds(fhir, admission) * 1000))}ms"
    stats = await fhir.resolve_into_archive(
        input_name,
        _SOURCE_FIELDS,
        source_bit=fhir.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT,
        priority=fhir.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY,
        schema=schema,
        archive_table=delta_name,
        strict_source_predicate="TRUE",
        timeout=timeout,
    )
    metrics_by_name = dict(stats.__dict__)
    metrics_by_name["openaddresses_coordinate_backfill_rows"] = await fhir._backfill_archive_openaddresses_coordinates(
        schema,
        input_name,
        archive_table=delta_name,
    )
    return metrics_by_name


async def prepare_archive_delta(fhir, schema, *, run_id=None, source_ids=None, seen_table=None):
    """Run the existing archive publisher against a seeded, separately admitted delta."""
    admission = preparation._ACTIVE.get()
    if not isinstance(admission, preparation.NonprofileAdmission) or not admission._started:
        raise RuntimeError("cms_archive_nonprofile_admission_required")
    _require_source_entry(fhir.db)
    prefix = _PREFIX + uuid.uuid4().hex
    input_name, delta_name, effective_name = prefix + "_input", prefix + "_delta", prefix + "_effective"
    relations = []
    try:
        async with source.source_transaction(fhir.db) as session, preparation.active_nonprofile_sql_transaction(fhir):
            await require_source_archive_owner(session, schema)
            from_revision = await _revision(fhir.db, schema)
            columns = await _columns(fhir.db, schema, _TARGET)
            await _seed_delta(
                fhir,
                admission,
                schema,
                input_name,
                delta_name,
                relations,
                {"run_id": run_id, "source_ids": source_ids, "seen_table": seen_table},
            )
            metrics_by_name = await _enrich_delta(fhir, admission, schema, input_name, delta_name)
            definition = await _seal_delta(
                fhir,
                admission,
                schema,
                delta_name,
                effective_name,
                relations,
            )
            row_count, digest = await _digest(fhir, schema, delta_name)
            prepared = PreparedArchiveDelta(
                schema,
                delta_name,
                effective_name,
                from_revision,
                admission.plan.native_address_input_hash,
                columns,
                row_count,
                digest,
                definition,
                admission,
            )
            await prepared.assert_ready(fhir)
            await admission.assert_ready(fhir, schema)
        await _cleanup(fhir, admission, schema, relations[:1])
        return prepared, metrics_by_name
    except BaseException:
        await _cleanup(fhir, admission, schema, relations)
        raise


def is_archive_relation(name):
    """Recognize only these owned input and delta heaps, never effective views."""
    return re.fullmatch(r"cms_archive_[0-9a-f]{32}_(input|delta)", name) is not None


async def _assert_backend_seal(database, schema, name):
    """Validate native heap shape and inert code by its declared name."""
    safe = await database.scalar(
        "SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
        "AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
        "AND NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) "
        "AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) FROM pg_class c WHERE c.oid=to_regclass(:relation)",
        relation=_qtable(schema, name),
    )
    if safe is not True:
        raise RuntimeError("cms_archive_storage_shape_unsupported")


async def capture_archive_layout(fhir, relation, *, database=None):
    """Reuse the native heap/TOAST/B-tree catalog validator for exact archive-owned storage."""
    from process.provider_directory_cms_native_layout import NativeRelationLayout
    from process.provider_directory_cms_nonprofile_observation import catalog_observation

    fhir = catalog_observation(fhir, database)

    if not is_archive_relation(relation.relation):
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    relation_oid = await fhir.db.scalar(
        "SELECT to_regclass(:name)::oid::bigint", name=fhir._unscoped_qt(relation.schema, relation.relation)
    )
    if type(relation_oid) is not int or not 0 < relation_oid < 2**32:
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    triggers = await fhir.db.scalar(
        "SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal",
        oid=relation_oid,
    )
    if triggers:
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    if relation.relation.endswith("_delta") and relation.persistence == "p":
        await _assert_backend_seal(fhir.db, relation.schema, relation.relation)
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation_oid, relation.persistence, triggers)
    if relation_map["schema_name"] != relation.schema or relation_map["relation_name"] != relation.relation:
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    attributes, indexes, constraints, trigger_rows = await fhir._profile_capacity_relation_catalog(
        [relation_oid] + ([toast_oid] if toast_oid else [])
    )
    if any(
        constraint_row["constraint_type"] not in ("p", "n") or constraint_row["condeferrable"]
        for constraint_row in constraints
    ):
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    return NativeRelationLayout(relation.schema, relation.relation)
