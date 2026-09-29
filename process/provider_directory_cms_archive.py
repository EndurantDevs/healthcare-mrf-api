# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare canonical archive changes without changing the signed live input."""

from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from dataclasses import dataclass, field

from sqlalchemy import text

from db.connection import ConnectionProxy, Database
from process import provider_directory_cms_preparation as preparation
from process.ext.address_canon import _qtable

_TARGET = "address_archive_v2"
_PREFIX = "cms_archive_"
_SEAL = "cms_archive_prepared_write_seal"
_SOURCE_FIELDS = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city_name",
    "state": "state_name",
    "zip": "postal_code",
    "country": "COALESCE(NULLIF(country_code, ''), 'US')",
}


async def _oid(database, schema, name):
    """Resolve one fixed, quoted relation identity."""
    return await database.scalar("SELECT to_regclass(:name)::oid::bigint", name=_qtable(schema, name))


async def _revision(database, schema, oid):
    """Read the registered archive preimage without altering its history."""
    revision = await database.scalar(
        f"SELECT revision FROM {_qtable(schema, 'cms_native_input_revision')} WHERE relation_oid=:oid",
        oid=oid,
    )
    if type(revision) is not int or revision < 0:
        raise RuntimeError("cms_archive_input_not_registered")
    return revision


async def _columns(database, oid):
    """Retain every ordinary archive column, rejecting generated or identity columns."""
    rows = await database.all(
        "SELECT attname::text,attgenerated::text,attidentity::text FROM pg_attribute "
        "WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        oid=oid,
    )
    values = [dict(row._mapping) for row in rows]
    if not values or any(row["attgenerated"] or row["attidentity"] for row in values):
        raise RuntimeError("cms_archive_columns_unsupported")
    return tuple(row["attname"] for row in values)


async def _digest(fhir, schema, name):
    """Hash ordered complete rows with bounded client memory in the owner's transaction."""
    digest, row_count = hashlib.sha256(b"cms-prepared-archive-rows-v1\0"), 0
    result = await fhir.db.stream(
        f"SELECT to_jsonb(archived)::text FROM {fhir._unscoped_qt(schema, name)} archived ORDER BY address_key"
    )
    try:
        async for row in result:
            digest.update(row[0].encode("utf8"))
            digest.update(b"\n")
            row_count += 1
    finally:
        await result.close()
    return row_count, digest.hexdigest()


@dataclass
class PreparedArchiveDelta:
    """Owned canonical rows and an effective view retained until the caller commits."""

    schema: str
    delta_table: str
    delta_oid: int
    effective_relation: str
    effective_oid: int
    target_oid: int
    from_revision: int
    native_input_hash: str
    columns: tuple[str, ...]
    delta_rows: int
    delta_sha256: str
    effective_definition: str
    sealed_filenode: int
    admission: preparation.NonprofileAdmission = field(repr=False)
    committed: bool = False

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
        is_owned_merge = await database.scalar(
            f"SELECT xmin=pg_current_xact_id()::xid "
            f"FROM {_qtable(self.schema, 'cms_native_input_revision')} WHERE relation_oid=:oid",
            oid=self.target_oid,
        )
        if is_owned_merge is not True:
            raise RuntimeError("cms_archive_applied_owner_changed")

    async def _assert_backend_identity(self, database, *, applied):
        """Lock exact read relations and validate immutable metadata without a row scan."""
        await database.status(
            f"LOCK TABLE {_qtable(self.schema, self.delta_table)}, {_qtable(self.schema, self.effective_relation)} "
            "IN ACCESS SHARE MODE NOWAIT"
        )
        if (
            await _oid(database, self.schema, self.delta_table) != self.delta_oid
            or await _oid(database, self.schema, self.effective_relation) != self.effective_oid
            or await _oid(database, self.schema, _TARGET) != self.target_oid
            or await _columns(database, self.target_oid) != self.columns
            or await _revision(database, self.schema, self.target_oid) != self.from_revision + (2 if applied else 0)
            or await database.scalar("SELECT pg_relation_filenode(CAST(:oid AS oid))::bigint", oid=self.delta_oid)
            != self.sealed_filenode
            or await database.scalar("SELECT pg_get_viewdef(CAST(:oid AS oid))", oid=self.effective_oid)
            != self.effective_definition
        ):
            raise RuntimeError("cms_archive_preparation_changed")
        await _assert_backend_seal(database, self.schema, self.delta_oid)

    async def assert_read_identity(self, fhir, session):
        """Bind an executing backend to exact sealed catalog identities without scanning rows."""
        _require_owner(fhir, session)
        await fhir.db.status(f"LOCK TABLE {fhir._unscoped_qt(self.schema, self.delta_table)} IN SHARE MODE NOWAIT")
        await self.lock_read_backend(fhir.db)

    async def assert_ready(self, fhir, *, cutover=False):
        """Recheck complete rows during the build and only immutable identities at cutover."""
        async with fhir.db.transaction() as session:
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
        await session.execute(
            text(f"LOCK TABLE {fhir._unscoped_qt(self.schema, _TARGET)} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        )
        await self.assert_ready(fhir, cutover=True)

    async def apply(self, fhir, session):
        """Merge exact prepared rows in the caller's common transaction, retaining the live OID."""
        _require_owner(fhir, session)
        has_write_intent = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND relation=:oid AND granted "
                "AND mode IN ('ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock'))"
            ),
            {"oid": self.target_oid},
        )
        if has_write_intent is not True:
            raise RuntimeError("cms_archive_publication_write_lock_required")
        await self.assert_ready(fhir, cutover=True)
        target_ref = fhir._unscoped_qt(self.schema, _TARGET)
        delta_ref = fhir._unscoped_qt(self.schema, self.delta_table)
        columns_sql = ",".join(fhir._q(column) for column in self.columns)
        assignments_sql = ",".join(
            f"{fhir._q(column)}=EXCLUDED.{fhir._q(column)}" for column in self.columns if column != "address_key"
        )
        await session.execute(
            text(f"""INSERT INTO {target_ref} AS incumbent ({columns_sql})
            SELECT {columns_sql} FROM {delta_ref} ORDER BY address_key
            ON CONFLICT (address_key) DO UPDATE SET {assignments_sql}
            WHERE to_jsonb(incumbent) IS DISTINCT FROM to_jsonb(EXCLUDED)""")
        )
        to_revision = await _revision(fhir.db, self.schema, self.target_oid)
        # INSERT .. ON CONFLICT fires both statement triggers, including an empty delta.
        if to_revision != self.from_revision + 2:
            raise RuntimeError("cms_archive_publication_revision_changed")
        return {
            "target_oid": self.target_oid,
            "from_revision": self.from_revision,
            "to_revision": to_revision,
            "native_input_hash": self.native_input_hash,
            "delta_rows": self.delta_rows,
            "delta_sha256": self.delta_sha256,
        }

    async def mark_committed(self, fhir, result):
        """Consume only the exact immutable result verified by the publication owner."""
        expected_by_field = {
            "target_oid": self.target_oid,
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
        """Remove only original owned view and heap identities, including cancelled builds."""
        await _cleanup(
            fhir,
            self.admission,
            self.schema,
            [
                (self.delta_table, self.delta_oid, "TABLE"),
                (self.effective_relation, self.effective_oid, "VIEW"),
            ],
        )


def _require_owner(fhir, session):
    """Never open or commit an independent archive publication transaction."""
    binding = fhir.db._transaction_binding()
    if binding is None or binding.session is not session or not session.in_transaction():
        raise RuntimeError("cms_archive_publication_requires_owner_transaction")


def _require_backend(database):
    """Accept only the native transport's actual already-owned transaction."""
    if isinstance(database, Database) and database._transaction_binding() is not None:
        return
    if isinstance(database, ConnectionProxy) and database._connection.in_transaction():
        return
    raise RuntimeError("cms_archive_read_requires_owner_transaction")


async def _cleanup(fhir, admission, schema, identities):
    """Drain exact cleanup without dropping replacements or following view dependencies."""

    async def remove():
        """Remove captured scratch identities without following view dependencies."""
        for name, oid, kind in reversed(identities):
            async with fhir.db.transaction():
                current = await _oid(fhir.db, schema, name)
                if current is None:
                    if kind == "TABLE" and (schema, name) in admission._external_relations:
                        await admission.retire_external_relation(fhir, schema, name, oid)
                    continue
                if current != oid:
                    if (schema, name) not in admission.cleanup_preserved:
                        admission.cleanup_preserved.append((schema, name))
                    continue
                if kind == "TABLE":
                    await fhir.db.status(
                        f"LOCK TABLE {fhir._unscoped_qt(schema, name)} IN ACCESS EXCLUSIVE MODE NOWAIT"
                    )
                if await _oid(fhir.db, schema, name) != oid:
                    raise RuntimeError("cms_archive_cleanup_identity_changed")
                await fhir.db.status(f"DROP {kind} {fhir._unscoped_qt(schema, name)}")
                if kind == "TABLE":
                    await admission.retire_external_relation(fhir, schema, name, oid)

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


async def _capture_heap(fhir, admission, schema, name, identities):
    """Register the original CREATE identity before its first heap growth."""
    oid = await _oid(fhir.db, schema, name)
    identities.append((name, oid, "TABLE"))
    await admission.register_external_relation(fhir, schema, name, oid)
    return oid


async def _seed_delta(fhir, admission, schema, input_name, delta_name, identities, options_by_name):
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
    await _capture_heap(fhir, admission, schema, input_name, identities)
    target_ref = fhir._unscoped_qt(schema, _TARGET)
    input_ref = fhir._unscoped_qt(schema, input_name)
    delta_ref = fhir._unscoped_qt(schema, delta_name)
    await fhir.db.status(f"CREATE UNLOGGED TABLE {delta_ref} (LIKE {target_ref} INCLUDING DEFAULTS)")
    delta_oid = await _capture_heap(fhir, admission, schema, delta_name, identities)
    aliases_ref = fhir._unscoped_qt(schema, fhir.address_alias_sql.ADDRESS_ALIAS_TABLE)
    await fhir.db.status(f"""INSERT INTO {delta_ref} SELECT archived.* FROM {target_ref} archived
        WHERE archived.address_key IN (SELECT address_key FROM {input_ref}
            UNION SELECT active.target_address_key FROM {aliases_ref} active
            JOIN {input_ref} incoming ON incoming.address_key=active.source_address_key
            WHERE active.revoked_at IS NULL)""")
    await fhir.db.status(f"ALTER TABLE {delta_ref} ADD PRIMARY KEY (address_key)")
    await admission.assert_external_relation(fhir, schema, delta_name, delta_oid)
    return delta_oid


async def _seal_delta(fhir, admission, schema, delta_name, effective_name, identities):
    """Log the measured delta and seal its effective read relation before native preparation."""
    from process.cms_doctors_preparation import _assert_seal_function

    target_ref = fhir._unscoped_qt(schema, _TARGET)
    delta_ref = fhir._unscoped_qt(schema, delta_name)
    await admission.before_logging(fhir, schema, delta_name)
    await fhir.db.status(f"ALTER TABLE {delta_ref} SET LOGGED")
    await fhir.db.status(f"ANALYZE {delta_ref}")
    await _assert_seal_function(fhir.db._transaction_binding().session, schema)
    await fhir.db.status(
        f"CREATE TRIGGER {_SEAL} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {delta_ref} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {fhir._q(schema)}.cms_doctors_prepared_immutable()"
    )
    await fhir.db.status(f"ALTER TABLE {delta_ref} ENABLE ALWAYS TRIGGER {_SEAL}")
    delta_oid = await _oid(fhir.db, schema, delta_name)
    filenode = await fhir.db.scalar("SELECT pg_relation_filenode(CAST(:oid AS oid))::bigint", oid=delta_oid)
    await fhir.db.status(f"""CREATE VIEW {fhir._unscoped_qt(schema, effective_name)} AS
        SELECT archived.* FROM {target_ref} archived
        WHERE NOT EXISTS (SELECT 1 FROM {delta_ref} staged WHERE staged.address_key=archived.address_key)
        UNION ALL SELECT * FROM {delta_ref}""")
    effective_oid = await _oid(fhir.db, schema, effective_name)
    identities.append((effective_name, effective_oid, "VIEW"))
    definition = await fhir.db.scalar("SELECT pg_get_viewdef(CAST(:oid AS oid))", oid=effective_oid)
    return filenode, effective_oid, definition


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
    prefix = _PREFIX + uuid.uuid4().hex
    input_name, delta_name, effective_name = prefix + "_input", prefix + "_delta", prefix + "_effective"
    identities = []
    try:
        async with preparation.active_nonprofile_sql_transaction(fhir):
            target_oid = await _oid(fhir.db, schema, _TARGET)
            from_revision = await _revision(fhir.db, schema, target_oid)
            columns = await _columns(fhir.db, target_oid)
            delta_oid = await _seed_delta(
                fhir,
                admission,
                schema,
                input_name,
                delta_name,
                identities,
                {"run_id": run_id, "source_ids": source_ids, "seen_table": seen_table},
            )
            metrics_by_name = await _enrich_delta(fhir, admission, schema, input_name, delta_name)
            filenode, effective_oid, definition = await _seal_delta(
                fhir,
                admission,
                schema,
                delta_name,
                effective_name,
                identities,
            )
            row_count, digest = await _digest(fhir, schema, delta_name)
            prepared = PreparedArchiveDelta(
                schema,
                delta_name,
                delta_oid,
                effective_name,
                effective_oid,
                target_oid,
                from_revision,
                admission.plan.native_address_input_hash,
                columns,
                row_count,
                digest,
                definition,
                filenode,
                admission,
            )
            await prepared.assert_ready(fhir)
            await admission.assert_ready(fhir, schema)
        await _cleanup(fhir, admission, schema, identities[:1])
        return prepared, metrics_by_name
    except BaseException:
        await _cleanup(fhir, admission, schema, identities)
        raise


def is_archive_relation(name):
    """Recognize only these owned input and delta heaps, never effective views."""
    return re.fullmatch(r"cms_archive_[0-9a-f]{32}_(input|delta)", name) is not None


async def _assert_seal(fhir, schema, oid):
    """Reuse the existing validated immutable-function capability for one exact delta guard."""
    await _assert_backend_seal(fhir.db, schema, oid)


async def _assert_backend_seal(database, schema, oid):
    """Validate the migrated immutable function and seal on the executing connection."""
    from process.cms_doctors_preparation import _SEAL_BODY

    function_oid = await database.scalar(
        "SELECT p.oid::bigint FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
        "WHERE p.oid=to_regprocedure(:function) AND p.prosrc=:body AND l.lanname='plpgsql' "
        "AND p.prorettype='trigger'::regtype AND p.pronargs=0 AND p.prokind='f' "
        "AND NOT p.prosecdef AND NOT p.proisstrict AND NOT p.proleakproof AND NOT p.proretset "
        "AND p.provolatile='v' AND p.proparallel='u' AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]",
        function=f"{_qtable(schema, 'cms_doctors_prepared_immutable')}()",
        body=_SEAL_BODY,
    )
    if function_oid is None:
        raise RuntimeError("cms_archive_write_seal_changed")
    valid = await database.scalar(
        "SELECT count(*)=1 AND bool_and(tgname=:name AND tgfoid=:function_oid AND tgtype=62 "
        "AND tgenabled='A' AND tgnargs=0 AND tgqual IS NULL) FROM pg_trigger "
        "WHERE tgrelid=:oid AND NOT tgisinternal",
        oid=oid,
        name=_SEAL,
        function_oid=function_oid,
    )
    if valid is not True:
        raise RuntimeError("cms_archive_write_seal_changed")


async def capture_archive_layout(fhir, relation):
    """Reuse the native heap/TOAST/B-tree catalog validator for exact archive-owned storage."""
    from process.provider_directory_cms_native_layout import NativeRelationLayout

    if not is_archive_relation(relation.relation):
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    triggers = await fhir.db.scalar(
        "SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal",
        oid=relation.oid,
    )
    if triggers:
        if not relation.relation.endswith("_delta"):
            raise RuntimeError("cms_archive_storage_shape_unsupported")
        await _assert_seal(fhir, relation.schema, relation.oid)
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, relation.persistence, triggers)
    if relation_map["schema_name"] != relation.schema or relation_map["relation_name"] != relation.relation:
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    attributes, indexes, constraints, trigger_rows = await fhir._profile_capacity_relation_catalog(
        [relation.oid] + ([toast_oid] if toast_oid else [])
    )
    if any(
        constraint_row["constraint_type"] not in ("p", "n") or constraint_row["condeferrable"]
        for constraint_row in constraints
    ):
        raise RuntimeError("cms_archive_storage_shape_unsupported")
    exact, _structural = fhir._profile_capacity_fingerprint_payloads(
        relation_map,
        attributes,
        indexes,
        constraints,
        trigger_rows,
        relation.oid,
    )
    return NativeRelationLayout(
        relation.oid,
        fhir._profile_capacity_tablespaces(relation_map, indexes, toast_oid),
        fhir._identity_hash(exact),
    )
