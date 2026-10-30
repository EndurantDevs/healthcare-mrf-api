# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain exact ordinary serving slices through the existing native COPY admission."""

import asyncio
import importlib
import uuid
from dataclasses import asdict, dataclass, field

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_ordinary_retention_capacity as capacity
from process import provider_directory_serving_recovery_receipts as receipts
from process.entity_address_snapshot_receipt import _row_identity
from process.provider_directory_ordinary_address_admission import require_capture
from process.reference_family_archive import native_copy_projection

_PAGE_ROWS = 512
_PAGE_BYTES = 16 * 1024 * 1024


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_row_retention_changed")


@dataclass(frozen=True)
class RetainedRows:
    """Native row identity; transport cursors never become durable recovery identity."""

    kind: str
    family: str
    schema: str
    source: str
    source_oid: int
    name: str
    oid: int
    layout_sha256: str
    columns: tuple[str, ...]
    rows: int
    data_sha256: str | None
    copied_bytes: int
    reused_initial: bool = False


@dataclass(eq=False)
class RetainedServingGeneration:
    """Actual owner and distinct preparation/cut transaction proofs."""

    prepared: object = field(repr=False)
    database: object = field(repr=False)
    admission: object = field(repr=False)
    capture: object = field(repr=False)
    task: object = field(repr=False)
    creator_session: object = field(repr=False)
    creator_driver: object = field(repr=False)
    creator_actor: dict = field(repr=False)
    scopes: tuple[RetainedRows, ...]
    families: tuple[RetainedRows, ...]
    cutover_actor: dict | None = field(default=None, init=False, repr=False)

    @property
    def payload(self):
        """Evidence for the existing private recovery journal conveys no authority."""
        return {
            "contract_id": "provider-directory-ordinary-retained-rows.v1",
            "affected_scope_sha256": self.capture.scope_payload["affected_scope_sha256"],
            "retention_plan_sha256": self.capture.scope_payload["retention_plan_sha256"],
            "creator": dict(self.creator_actor),
            "scopes": [asdict(relation) for relation in self.scopes],
            "families": [asdict(relation) for relation in self.families],
        }


def retained_scope_source(prepared, kind):
    """Resolve actual issued Profile/Address scopes; caller names grant no custody."""
    _require(type(prepared) is preparation.PreparedServingArtifacts)
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    _require(kind in {"npi", "location", "group"})
    if kind != "npi":
        affected = importlib.import_module("process.entity_address_affected_patch")
        descriptor = affected.require_descriptor(prepared.address)
        _require(descriptor.admission is admission and descriptor.capture is capture)
        return (
            descriptor.schema,
            getattr(descriptor, f"affected_{kind}_table"),
            getattr(descriptor, f"affected_{kind}_oid"),
        )
    delta = prepared.profile_delta
    if delta is None:
        targets = {stage.target_relation for stage in prepared.profile_bundle.stages}
        profile_targets = {"provider_directory_profile", "provider_directory_profile_evidence"}
        if capture.predecessor["kind"] == "initial":
            _require(fhir.profile_initial.build_from_stages(fhir, prepared.profile_bundle.stages) is not None)
        else:
            _require(not targets.intersection(profile_targets))
        return None
    _require(type(delta) is fhir.ProviderDirectoryPreparedProfileDelta)
    _require(
        capture.predecessor["kind"] != "initial" and delta.selection_proof_id == prepared.execution.attestation.proof_id
    )
    return delta.schema, delta.affected_npi_stage, delta.affected_npi_stage_oid


async def _origin(prepared):
    _require(type(prepared) is preparation.PreparedServingArtifacts)
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    session, driver, actor = await receipts.ordinary(fhir.db)
    _require(actor["owner"] == capture.scope_payload["owner_oid"])
    _require(actor["database_oid"] == admission.lease.database_oid)
    return session, driver, actor


async def _same_origin(prepared, origin):
    session, driver, actor = await _origin(prepared)
    _require(session is origin[0] and driver is origin[1] and actor == origin[2])


async def _native_relation(prepared, schema, name, expected_oid=None, *, initial=False):
    _session, driver, _actor = await _origin(prepared)
    row = await driver.fetchrow(
        "SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner_oid,c.relnamespace::bigint AS schema_oid,"
        "c.relpersistence::text AS persistence,pg_total_relation_size(c.oid)::bigint AS bytes "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r' AND NOT c.relispartition "
        "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity",
        schema,
        name,
    )
    _require(row is not None and (expected_oid is None or row["oid"] == expected_oid))
    scope = prepared.nonprofile_admission.captured_inputs.scope_payload
    _require((initial or row["owner_oid"] == scope["owner_oid"]) and row["schema_oid"] == scope["schema_oid"])
    _require(row["persistence"] in {"u", "p"})
    return preparation.OwnedRelation(schema, name, row["oid"], row["bytes"], row["persistence"])


async def _columns(driver, oid):
    rows = await driver.fetch(
        "SELECT attname::text,attgenerated::text,attidentity::text FROM pg_attribute "
        "WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        oid,
    )
    _require(rows and all(not row["attgenerated"] and not row["attidentity"] for row in rows))
    columns = tuple(row["attname"] for row in rows)
    for name in columns:
        storage_identifier(name)
    return columns


def _page_sql(source, predicate, columns, keys, arguments, cursor):
    cursor_expressions = ["live." + storage_identifier(key) for key in keys]
    column_sql = ",".join("live." + storage_identifier(name) for name in columns)
    order = ",".join(cursor_expressions)
    comparison = "(" + order + ")"
    page_arguments = list(arguments)
    if cursor is not None:
        placeholders = ",".join("$" + str(len(arguments) + ordinal + 1) for ordinal in range(len(keys)))
        predicate += f" AND {comparison}>({placeholders})"
        page_arguments.extend(cursor)
    projection = ",".join(
        expression + " AS key_" + str(ordinal) for ordinal, expression in enumerate(cursor_expressions)
    )
    query = f"SELECT {projection} FROM {source} live WHERE {predicate} ORDER BY {order} LIMIT {_PAGE_ROWS}"
    return query, page_arguments, predicate, comparison, column_sql, order


async def _copy_rows(prepared, origin, source_ref, retained_relation, columns, keys, predicate, arguments):
    """Keep actual PK keysets, or frozen local scope CTIDs, inside one owner transaction."""
    session, driver, _actor = origin
    cursor, copied_bytes = None, 0
    while True:
        query, page_arguments, where, comparison, column_sql, order = _page_sql(
            source_ref, predicate, columns, keys, arguments, cursor
        )
        page = await driver.fetch(query, *page_arguments)
        _require(len(page) <= _PAGE_ROWS)
        if not page:
            break
        last_keys = tuple(page[-1]["key_" + str(ordinal)] for ordinal in range(len(keys)))
        _require(last_keys != cursor)
        placeholders = ",".join("$" + str(len(page_arguments) + ordinal + 1) for ordinal in range(len(keys)))
        projection = f"SELECT {column_sql} FROM {source_ref} live WHERE {where} AND {comparison}<=({placeholders}) ORDER BY {order} LIMIT {_PAGE_ROWS}"
        remaining = await preparation.remaining_build_seconds(prepared.fhir, prepared.nonprofile_admission)
        copied_bytes += await native_copy_projection(
            session,
            projection,
            *page_arguments,
            *last_keys,
            schema_name=retained_relation.schema,
            table_name=retained_relation.relation,
            columns=columns,
            max_bytes=_PAGE_BYTES,
            timeout=remaining,
        )
        await _same_origin(prepared, origin)
        await capacity.check_ordinary_retained_growth(prepared.nonprofile_admission, prepared.fhir)
        cursor = last_keys
    return copied_bytes


async def _assert_parity(driver, source, retained, predicate, arguments):
    """Compare complete native rows as jsonb, including deployed native json columns."""
    original = f"SELECT to_jsonb(live) FROM {source} live WHERE {predicate}"
    copied = f"SELECT to_jsonb(saved) FROM {retained} saved"
    valid = await driver.fetchval(
        f"SELECT NOT EXISTS (({original} EXCEPT ALL {copied}) UNION ALL ({copied} EXCEPT ALL {original}))",
        *arguments,
    )
    _require(valid is True)


async def _layout(prepared, kind, family, relation):
    if kind == "scope":
        return await capacity.retained_scope_layout_sha256(prepared.fhir, family.removesuffix("_scope"), relation)
    return await capacity.retention_layout_sha256(prepared.fhir, relation)


async def _clone(prepared, origin, kind, family, coordinate, keys, predicate, arguments):
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    schema, source_name, source_oid = coordinate
    source_relation = await _native_relation(prepared, schema, source_name, source_oid)
    columns = await _columns(origin[1], source_relation.oid)
    name = "pd_" + family + "_before_" + uuid.uuid4().hex[:20]
    source_ref, retained_ref = storage_relation(schema, source_name), storage_relation(schema, name)
    await fhir.db.status(
        f"CREATE UNLOGGED TABLE {retained_ref} (LIKE {source_ref} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING INDEXES)"
    )
    retained_relation = await _native_relation(prepared, schema, name)
    if kind == "scope":
        await capacity.register_ordinary_retained_scope(
            admission,
            fhir,
            prepared=prepared,
            kind=family.removesuffix("_scope"),
            schema=schema,
            name=name,
            oid=retained_relation.oid,
        )
    else:
        await capacity.register_ordinary_retained_relation(
            admission, fhir, family=family, schema=schema, name=name, oid=retained_relation.oid
        )
    copied_bytes = await _copy_rows(
        prepared, origin, source_ref, retained_relation, columns, keys, predicate, arguments
    )
    await _assert_parity(origin[1], source_ref, retained_ref, predicate, arguments)
    async with receipts.normalized_receipt_scope(fhir.db, origin[0], schema):
        retained_rows, digest = await _row_identity(origin[0], schema, name)
    await admission.before_logging(fhir, schema, name)
    await fhir.db.status(f"ALTER TABLE {retained_ref} SET LOGGED")
    retained_relation = await _native_relation(prepared, schema, name, retained_relation.oid)
    _require(retained_relation.persistence == "p")
    layout = await _layout(prepared, kind, family, retained_relation)
    await admission.assert_ready(fhir, schema)
    await _same_origin(prepared, origin)
    return RetainedRows(
        kind,
        family,
        schema,
        source_name,
        source_oid,
        name,
        retained_relation.oid,
        layout,
        columns,
        retained_rows,
        digest,
        copied_bytes,
    )


async def _prepare_scopes(prepared, origin):
    scopes = []
    for kind, keys in (("npi", ("npi",)), ("location", ("location_key",)), ("group", ("ctid",))):
        coordinate = retained_scope_source(prepared, kind)
        if coordinate is None:
            continue
        source_ref = storage_relation(*coordinate[:2])
        await prepared.fhir.db.status(f"LOCK TABLE {source_ref} IN SHARE MODE NOWAIT")
        scopes.append(await _clone(prepared, origin, "scope", kind + "_scope", coordinate, keys, "TRUE", ()))
    return tuple(scopes)


def _family_scope(prepared, family, scopes):
    by_kind = {relation.family: relation for relation in scopes}
    if family in {"profile", "evidence"}:
        npi = by_kind.get("npi_scope")
        if npi is None:
            return "FALSE", ()
        return f"EXISTS(SELECT 1 FROM {storage_relation(npi.schema, npi.name)} scope WHERE scope.npi=live.npi)", ()
    if family == "overlay":
        return "live.source_id=ANY($1::varchar[])", (
            prepared.nonprofile_admission.captured_inputs.affected_scope["source_ids"],
        )
    locations = by_kind["location_scope"]
    return (
        f"EXISTS(SELECT 1 FROM {storage_relation(locations.schema, locations.name)} scope WHERE scope.location_key=live.location_key)",
        (),
    )


async def _reuse_initial(prepared, entry):
    _require(entry["family"] in {"profile", "evidence"} and entry["scope_kind"] == "initial_full_root")
    relation = await _native_relation(
        prepared, entry["source_schema"], entry["source_relation"], entry["source_oid"], initial=True
    )
    _require(relation.persistence == "p")
    admission = prepared.nonprofile_admission
    await capacity.register_ordinary_retained_relation(
        admission,
        prepared.fhir,
        family=entry["family"],
        schema=relation.schema,
        name=relation.relation,
        oid=relation.oid,
    )
    layout = await capacity.retention_layout_sha256(prepared.fhir, relation)
    _require(layout == entry["source_layout_sha256"])
    retained_rows = admission.captured_inputs.predecessor["profile"][entry["family"] + "_rows"]
    return RetainedRows(
        "family",
        entry["family"],
        relation.schema,
        relation.relation,
        relation.oid,
        relation.relation,
        relation.oid,
        layout,
        (),
        retained_rows,
        None,
        0,
        True,
    )


async def _prepare_families(prepared, origin, scopes):
    families = []
    admission = prepared.nonprofile_admission
    entries = capacity._captured_plan(prepared.fhir, admission.plan, admission.captured_inputs)
    for family, entry in entries.items():
        if entry["retention_mode"] == "existing_initial_target":
            families.append(await _reuse_initial(prepared, entry))
            continue
        source = await _native_relation(prepared, entry["source_schema"], entry["source_relation"], entry["source_oid"])
        _require(await capacity.retention_layout_sha256(prepared.fhir, source) == entry["source_layout_sha256"])
        predicate, arguments = _family_scope(prepared, family, scopes)
        coordinate = source.schema, source.relation, source.oid
        families.append(
            await _clone(
                prepared, origin, "family", family, coordinate, tuple(entry["key_columns"]), predicate, arguments
            )
        )
    return tuple(families)


async def _retire_rolled_back(prepared, coordinates):
    """Rollback removes transactional clones; retire only now-absent exact registrations."""
    pending_coordinates = []
    for schema, name, oid in reversed(coordinates):
        try:
            await prepared.nonprofile_admission.retire_external_relation(prepared.fhir, schema, name, oid)
        except BaseException:
            pending_coordinates.append((schema, name, oid))
    prepared.retention_cleanup_pending = tuple(pending_coordinates)


async def prepare_retained_generation(prepared):
    """Complete every bounded COPY and logging write before Profile resumes."""
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    _require(
        preparation._ACTIVE.get() is admission and fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
    )
    _require(admission.profile_admission is not None and getattr(prepared, "retained_generation", None) is None)
    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    prior_coordinates = set(admission._external_relations)
    try:
        async with preparation.nonprofile_sql_transaction(fhir, admission):
            origin = await _origin(prepared)
            await capture.recheck(fhir)
            scopes = await _prepare_scopes(prepared, origin)
            families = await _prepare_families(prepared, origin, scopes)
            await capture.recheck(fhir)
            await _same_origin(prepared, origin)
            retained = RetainedServingGeneration(
                prepared, fhir.db, admission, capture, asyncio.current_task(), *origin, scopes, families
            )
    except BaseException:
        coordinates = [
            (schema, name, admission._relations[(schema, name)])
            for schema, name in admission._external_relations - prior_coordinates
            if name.startswith("pd_") and "_before_" in name
        ]
        await _retire_rolled_back(prepared, coordinates)
        raise
    prepared.retained_generation = retained
    return retained


def _require_retained(prepared):
    retained = prepared.retained_generation
    _require(type(retained) is RetainedServingGeneration and retained.prepared is prepared)
    _require(retained.database is prepared.fhir.db and retained.admission is prepared.nonprofile_admission)
    _require(retained.task is asyncio.current_task() and retained.capture is retained.admission.captured_inputs)
    require_capture(prepared.fhir, retained.admission.plan, retained.capture)
    return retained


async def recheck_retained_generation(prepared):
    """Bind a distinct genuine cut transaction before its first live row mutation."""
    retained = _require_retained(prepared)
    origin = await _origin(prepared)
    _require(retained.cutover_actor is None or retained.cutover_actor == origin[2])
    names = {relation.source for relation in retained.families} | {
        relation.name for relation in (*retained.scopes, *retained.families)
    }
    await receipts.lock(prepared.fhir.db, prepared.fhir._schema(), tuple(names), mode="SHARE")
    await retained.capture.recheck(prepared.fhir)
    for relation in (*retained.scopes, *retained.families):
        current = await _native_relation(
            prepared, relation.schema, relation.name, relation.oid, initial=relation.reused_initial
        )
        _require(
            current.persistence == "p"
            and await _layout(prepared, relation.kind, relation.family, current) == relation.layout_sha256
        )
        if relation.reused_initial:
            continue
        async with receipts.normalized_receipt_scope(prepared.fhir.db, origin[0], relation.schema):
            _require(
                await _row_identity(origin[0], relation.schema, relation.name) == (relation.rows, relation.data_sha256)
            )
        if relation.kind == "family":
            predicate, arguments = _family_scope(prepared, relation.family, retained.scopes)
            await _assert_parity(
                origin[1],
                storage_relation(relation.schema, relation.source),
                storage_relation(relation.schema, relation.name),
                predicate,
                arguments,
            )
    await _same_origin(prepared, origin)
    retained.cutover_actor = dict(origin[2])
    return retained.payload


async def discard_unpublished_retention(prepared):
    """Drop only exact owned copies which have never entered a publication attempt."""
    retained = _require_retained(prepared)
    _require(retained.cutover_actor is None)
    for relation in reversed((*retained.scopes, *retained.families)):
        if relation.reused_initial:
            continue
        await preparation._drop_owned_relation(prepared.fhir, retained.admission, relation.schema, relation.name)
        await retained.admission.retire_external_relation(prepared.fhir, relation.schema, relation.name, relation.oid)
