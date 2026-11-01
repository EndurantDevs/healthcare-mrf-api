# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close an original Source export in a distinct, independently verified Core capture."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime
from functools import partial
from types import SimpleNamespace
from uuid import UUID

from process import network_fhir_retained_capture as retained
from process.network_address_projection import _identifier
from process.network_bootstrap_sources import _content as table_content
from process.network_custom_address_source import _closed, _require_transaction
from process.network_fhir_source_epoch import (
    _CONTENT_SETTINGS,
    _borrowed_epoch_settings,
    _catalog_relations,
    _set_content_settings,
)
from process.network_fhir_source_epoch_pin import _virtual_xid
from process.network_fhir_source_witness_contract import (
    COMMENT_PREFIX,
    CONTRACT,
    SCHEMA_PREFIX,
    TABLES,
    SourceWitnessCapture,
    SourceWitnessCaptureAnchor,
    canonical,
    canonical_timestamp,
    capture_digest,
    database_identity,
    digest,
    require,
    require_capture_anchor,
    source_origin,
    validate_capture,
)
from process.network_membership_writer_closure import _check_roles, _protected_owner, _transfer_and_grant
from process.uhc_flex_practitioner_async_safety import drain_operation

_RESOURCE_FIELDS = (
    "dataset_id",
    "resource_type",
    "resource_id",
    "payload_hash",
    "payload_json",
    "acquired_resource_sha256",
)
_RESOURCE_DDL = (
    "dataset_id text NOT NULL,resource_type text NOT NULL,resource_id text NOT NULL,"
    "payload_hash text NOT NULL,payload_json jsonb NOT NULL,acquired_resource_sha256 text"
)
_COLUMNS = {
    TABLES[0]: _RESOURCE_FIELDS,
    TABLES[1]: ("source_id", "resource_type", "resource_id", "organization_id", "site_id", "created_at"),
    TABLES[2]: (
        "source_id",
        "resource_type",
        "resource_id",
        "release_id",
        "payload_sha256",
        "payload_json",
        "observed_at",
    ),
    TABLES[3]: (
        "source_id",
        "release_id",
        "network_resource_type",
        "network_resource_id",
        "insurance_plan_resource_id",
        "network_refs",
        "owned_by_ref",
        "administered_by_ref",
        "plan_payload_sha256",
        "plan_payload_json",
        "observed_at",
    ),
}
_READ_BRAND = object()
_MAX_PAGE_BYTES = 8 * 1024**2
_MAX_DERIVED_BYTES = 64 * 1024**2


def _relation(schema, table):
    return _identifier(schema) + "." + _identifier(table)


def _original_source_factory(factory, origin):
    """Invoke the installed Source adapter; a caller-supplied witness document has no authority."""
    from directory_membership_runtime import (
        CatalogSnapshotRuntime,
        FHIRSourceMembershipInput,
        open_membership_source_export,
    )

    require(type(origin) is FHIRSourceMembershipInput)
    require(
        type(factory) is partial
        and factory.func is open_membership_source_export
        and len(factory.args) == 1
        and not factory.keywords
        and type(factory.args[0]) is CatalogSnapshotRuntime
        and factory.args[0].config is not None
    )
    publisher_role = factory.args[0].config.publisher_role
    _identifier(publisher_role)
    return publisher_role


@dataclass
class _CaptureWriter:
    writer: object
    task: object
    virtual_xid: str

    async def verify(self):
        """Keep COPY on its original writer transaction and task."""
        require(self.task is asyncio.current_task() and self.writer.is_in_transaction())
        require(await _virtual_xid(self.writer) == self.virtual_xid)


@dataclass
class _CaptureCopyState:
    pair: _CaptureWriter
    schema: str
    origin: object
    release_id: str
    observed_at: datetime
    hashes_by_family: dict
    counts_by_family: dict
    root: object
    total: int = 0


async def _create(writer, schema, owner_role):
    await writer.execute(f"CREATE SCHEMA {_identifier(schema)} AUTHORIZATION {_identifier(owner_role)}")
    await writer.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    for table in TABLES:
        definition = _RESOURCE_DDL if table == TABLES[0] else retained._DERIVED_DDL[table]
        await writer.execute(f"CREATE TABLE {_relation(schema, table)} ({definition})")


async def _copy_batch(copy_state, resource_rows):
    """Retain existing split and bounded-reference fallback for native expansion limits."""
    try:
        prepared = await drain_operation(
            asyncio.to_thread(
                retained._prepare_native_resources,
                resource_rows,
                copy_state.origin,
                copy_state.release_id,
                copy_state.observed_at,
                True,
            ),
            preserve_cancellation=True,
        )
    except retained._CopyBatchTooLarge:
        if len(resource_rows) > 1:
            middle = len(resource_rows) // 2
            for batch in (resource_rows[:middle], resource_rows[middle:]):
                copy_state.total = await _copy_batch(copy_state, batch)
            return copy_state.total
        await drain_operation(
            asyncio.to_thread(retained._validate_reference_rows, resource_rows, copy_state.origin),
            preserve_cancellation=True,
        )
        prepared = None
    return await _store_resource_batch(copy_state, resource_rows, prepared)


async def _store_resource_batch(copy_state, resource_rows, prepared):
    """Copy one validated batch and fold either native or bounded reference output."""
    await copy_state.pair.verify()
    resource_copy_records = [
        tuple(
            canonical(resource_record[name])
            if name == "payload_json" and type(resource_record[name]) is not str
            else resource_record[name]
            for name in _RESOURCE_FIELDS
        )
        for resource_record in resource_rows
    ]
    copied = await drain_operation(
        copy_state.pair.writer.copy_records_to_table(
            TABLES[0], schema_name=copy_state.schema, columns=_RESOURCE_FIELDS, records=resource_copy_records
        ),
        preserve_cancellation=True,
    )
    require(copied == f"COPY {len(resource_rows)}")
    if prepared is None:
        await retained._derived(
            copy_state.pair.writer,
            copy_state.schema,
            copy_state.origin,
            copy_state.release_id,
            resource_rows,
            copy_state.observed_at,
            verify=copy_state.pair.verify,
        )
        return retained._fold_rows(
            resource_rows,
            copy_state.origin,
            copy_state.hashes_by_family,
            copy_state.counts_by_family,
            copy_state.root,
            copy_state.total,
        )
    await retained._copy_prepared(copy_state.pair, copy_state.schema, prepared)
    return retained._fold_prepared(
        prepared, copy_state.hashes_by_family, copy_state.counts_by_family, copy_state.root, copy_state.total
    )


async def _copy_resources(writer, schema, origin, export, release_id, observed_at):
    """Consume one real Source iterator to EOF; validate complete batches before bounded COPY."""
    pair = _CaptureWriter(writer, asyncio.current_task(), await _virtual_xid(writer))
    hashes_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    copy_state = _CaptureCopyState(
        pair,
        schema,
        origin,
        release_id,
        observed_at,
        hashes_by_family,
        dict.fromkeys(hashes_by_family, 0),
        hashlib.sha256(),
    )
    after = None
    iterator = export["resources"]()
    try:
        async for page in iterator:
            require(type(page) in {tuple, list} and 0 < len(page) <= 1000)
            require(len(canonical(page).encode()) <= _MAX_PAGE_BYTES)
            for resource_record in page:
                require(type(resource_record) is dict and set(resource_record) == set(_RESOURCE_FIELDS))
                require(
                    resource_record["dataset_id"] == origin.dataset_id
                    and resource_record["acquired_resource_sha256"] is None
                )
                key = resource_record["resource_type"], resource_record["resource_id"]
                require(all(type(key_part) is str and key_part.isascii() for key_part in key))
                require(after is None or after < key)
                after = key
            copy_state.total = await _copy_batch(copy_state, page)
            require(copy_state.total <= sum(count for _, count in origin.resource_counts))
    finally:
        await drain_operation(iterator.aclose(), preserve_cancellation=True)
    retained._require_roots(origin, copy_state.hashes_by_family, copy_state.counts_by_family, copy_state.root)


async def _indexes(writer, schema):
    for table in TABLES:
        keys = ",".join(_identifier(key) for key in retained._KEYS[table])
        await writer.execute(f"ALTER TABLE {_relation(schema, table)} ADD PRIMARY KEY ({keys})")
        for number, keys in enumerate(retained._LOOKUPS.get(table, ())):
            name = f"retained_lookup_{retained.TABLES.index(table)}_{number}"
            columns = ",".join(retained._lookup_key(table, key) for key in keys)
            await writer.execute(f"CREATE INDEX {_identifier(name)} ON {_relation(schema, table)} ({columns})")
        await writer.execute(f"ANALYZE {_relation(schema, table)}")


async def _catalog(connection, schema, owner_role, runtime_roles):
    """Validate the distinct four-table profile without locking live Source or serving relations."""
    relations = await _catalog_relations(connection, schema)
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    require(_protected_owner(owner) and tuple(relation["relname"] for relation in relations) == tuple(sorted(TABLES)))
    for relation in relations:
        require(
            relation["relkind"] == b"r"
            and relation["relpersistence"] == b"p"
            and relation["relowner"] == relation["nspowner"] == owner["oid"]
            and not any(
                relation[key]
                for key in ("relhastriggers", "relrowsecurity", "relforcerowsecurity", "inherited", "defaults")
            )
        )
        columns = json.loads(relation["columns"])
        require(tuple(column[0] for column in columns) == _COLUMNS[relation["relname"]])
        require(all(not column[4] and not column[5] for column in columns))
    require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid) AND NOT EXISTS("
            "SELECT FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class WHERE c.relnamespace=$1::oid)",
            relations[0]["schema_oid"],
        )
        is True
    )
    await _closed(connection, relations[0]["schema_oid"], owner["oid"], runtime_roles, list(TABLES))
    require(
        await connection.fetchval(
            "WITH configured AS (SELECT r.* FROM unnest($1::text[]) selected(name) LEFT JOIN pg_roles r ON r.rolname=selected.name) "
            "SELECT NOT EXISTS(SELECT FROM configured r WHERE r.oid IS NULL OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls "
            "OR has_parameter_privilege(r.oid,'session_replication_role','SET')) AND NOT EXISTS(SELECT FROM pg_proc p "
            "WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype) "
            "AND EXISTS(SELECT FROM pg_roles definer WHERE definer.oid=p.proowner AND (definer.rolsuper OR definer.rolcreaterole "
            "OR pg_has_role(definer.oid,$2::oid,'MEMBER') OR pg_has_role(definer.oid,$2::oid,'SET'))) "
            "AND EXISTS(SELECT FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') "
            "AND has_function_privilege(r.oid,p.oid,'EXECUTE')))",
            list(runtime_roles),
            owner["oid"],
        )
        is True
    )
    retained._require_indexes(relations, owner["oid"], schema)
    await retained._require_cursor_index(
        connection, dict((relation["relname"], relation["oid"]) for relation in relations)[TABLES[0]]
    )
    catalog_hash = digest(
        [
            {
                key: catalog_value.decode("ascii") if type(catalog_value) is bytes else catalog_value
                for key, catalog_value in relation.items()
            }
            for relation in relations
        ]
    )
    return (
        relations[0]["schema_oid"],
        tuple((relation["relname"], relation["oid"]) for relation in relations),
        catalog_hash,
    )


async def _capture_content(connection, schema):
    async with _borrowed_epoch_settings(connection):
        await _set_content_settings(
            connection, [key for key, _ in _CONTENT_SETTINGS], [value for _, value in _CONTENT_SETTINGS]
        )
        return digest([(table, *(await table_content(connection, schema, table))) for table in sorted(TABLES)])


def _record_field(name, field_value):
    if field_value is None:
        return None
    if name in {"payload_json", "network_refs", "plan_payload_json"} and type(field_value) is str:
        return json.loads(field_value)
    if isinstance(field_value, UUID):
        return str(field_value)
    if isinstance(field_value, datetime):
        return canonical_timestamp(field_value)
    return field_value


def _record(table, values):
    normalized_fields = [
        _record_field(name, field_value) for name, field_value in zip(_COLUMNS[table], values, strict=True)
    ]
    return canonical(normalized_fields)


def _expected_entity_rows(origin, release_id, resource_rows, observed_at):
    expected_by_table = {table: [] for table in TABLES[1:3]}
    encoded_bytes = 0
    entity_rows = [
        resource_record
        for resource_record in resource_rows
        if resource_record["resource_type"] in {"Organization", "Location"}
    ]
    for table, derived_record in retained._derived_records(origin, release_id, entity_rows, observed_at):
        encoded = _record(table, derived_record)
        encoded_bytes += len(encoded.encode())
        require(encoded_bytes <= _MAX_DERIVED_BYTES)
        expected_by_table[table].append(encoded)
    return expected_by_table


async def _actual_entity_rows(connection, schema, table, source_id, resource_rows, release_id):
    kinds = [resource_record["resource_type"] for resource_record in resource_rows]
    identities = [resource_record["resource_id"] for resource_record in resource_rows]
    predicate = "source_id=$1 AND (resource_type,resource_id) IN (SELECT * FROM unnest($2::text[],$3::text[]))"
    query_arguments = [source_id, kinds, identities]
    if table == TABLES[2]:
        predicate += " AND release_id=$4"
        query_arguments.append(release_id)
    selected = ",".join(_identifier(name) for name in _COLUMNS[table])
    derived_rows = await connection.fetch(
        f"SELECT {selected} FROM {_relation(schema, table)} WHERE {predicate}", *query_arguments
    )
    return [_record(table, derived_record) for derived_record in derived_rows]


async def _verify_derived(connection, schema, origin, release_id, observed_at):
    """Replay reference entities and SQL plan validation without expanding plan payloads in memory."""
    after, limit = None, 1000
    totals_by_table = dict.fromkeys(TABLES[1:3], 0)
    while True:
        query, arguments, bounds = await retained._resources(
            connection, replace(origin, schema_name=schema), after, limit
        )
        if not bounds["count"]:
            break
        if bounds["bytes"] + 21 > _MAX_PAGE_BYTES:
            require(limit > 1)
            limit = max(1, limit // 2)
            continue
        resource_rows = await connection.fetch(query, *arguments)
        require(len(resource_rows) == bounds["count"])
        expected_by_table = _expected_entity_rows(origin, release_id, resource_rows, observed_at)
        for table in TABLES[1:3]:
            actual_rows = await _actual_entity_rows(
                connection, schema, table, origin.source_id, resource_rows, release_id
            )
            require(sorted(actual_rows) == sorted(expected_by_table[table]))
            totals_by_table[table] += len(expected_by_table[table])
        after = resource_rows[-1]["resource_type"], resource_rows[-1]["resource_id"]
    for table, count in totals_by_table.items():
        require(await connection.fetchval(f"SELECT count(*) FROM {_relation(schema, table)}") == count)
    await _verify_plan_rows(connection, schema, origin, release_id, observed_at)


async def _verify_plan_rows(connection, schema, origin, release_id, observed_at):
    """Check exact plan evidence after resource equality, without expanding plan payloads in Python."""
    await retained._require_modern_plan_evidence(connection, schema, origin, release_id)
    require(
        await connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM {_relation(schema, TABLES[3])} WHERE observed_at IS DISTINCT FROM $1)",
            observed_at,
        )
        is True
    )


def _selection(origin):
    return {name: getattr(origin, name) for name in ("source_id", "endpoint_id", "dataset_id")}


def _export_identity(export, reviewed, expected_identity=None):
    require(type(export) is dict and set(export) == {"contract", "origin", "database_identity", "resources"})
    require(export["contract"] == CONTRACT and source_origin(export["origin"]) == reviewed)
    identity = database_identity(export["database_identity"])
    require(
        identity.database_oid == reviewed.database_oid
        and identity.system_identifier == reviewed.database_system_identifier
    )
    require(expected_identity is None or identity == expected_identity)
    return identity


@dataclass
class _SourceReplay:
    origin: object
    schema: str
    release_id: str
    observed_at: datetime
    hashes_by_family: dict
    counts_by_family: dict
    root: object
    entity_totals: dict
    total: int = 0
    after: tuple | None = None


def _fold_source_page(replay, resource_rows):
    """Validate actual second-stream payload commitments independently of retained rows or receipts."""
    require(type(resource_rows) in {tuple, list} and 0 < len(resource_rows) <= 1000)
    require(len(canonical(resource_rows).encode()) <= _MAX_PAGE_BYTES)
    for resource_row in resource_rows:
        require(type(resource_row) is dict and set(resource_row) == set(_RESOURCE_FIELDS))
        require(
            resource_row["dataset_id"] == replay.origin.dataset_id and resource_row["acquired_resource_sha256"] is None
        )
        cursor = resource_row["resource_type"], resource_row["resource_id"]
        require(all(type(part) is str and part.isascii() for part in cursor))
        require(replay.after is None or replay.after < cursor)
        replay.after = cursor
    replay.total = retained._fold_rows(
        resource_rows, replay.origin, replay.hashes_by_family, replay.counts_by_family, replay.root, replay.total
    )
    require(replay.total <= sum(count for _, count in replay.origin.resource_counts))


async def _compare_source_page(connection, replay, resource_rows):
    """Compare original and derived candidate values with a fresh authenticated Source page."""
    await drain_operation(asyncio.to_thread(_fold_source_page, replay, resource_rows), preserve_cancellation=True)
    selected = ",".join(_identifier(name) for name in _RESOURCE_FIELDS)
    kinds = [resource_row["resource_type"] for resource_row in resource_rows]
    identities = [resource_row["resource_id"] for resource_row in resource_rows]
    original_rows = await connection.fetch(
        f"SELECT {selected} FROM {_relation(replay.schema, TABLES[0])} WHERE dataset_id=$1 "
        "AND (resource_type,resource_id) IN (SELECT * FROM unnest($2::text[],$3::text[])) "
        'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C"',
        replay.origin.dataset_id,
        kinds,
        identities,
    )
    require(
        [_record(TABLES[0], row) for row in original_rows]
        == [
            _record(TABLES[0], tuple(resource_row[name] for name in _RESOURCE_FIELDS)) for resource_row in resource_rows
        ]
    )
    expected_by_table = _expected_entity_rows(replay.origin, replay.release_id, resource_rows, replay.observed_at)
    for table in TABLES[1:3]:
        actual_rows = await _actual_entity_rows(
            connection, replay.schema, table, replay.origin.source_id, resource_rows, replay.release_id
        )
        require(sorted(actual_rows) == sorted(expected_by_table[table]))
        replay.entity_totals[table] += len(expected_by_table[table])


async def _finish_source_replay(connection, replay):
    """Reject absent or extra rows across the complete four-table candidate."""
    retained._require_roots(replay.origin, replay.hashes_by_family, replay.counts_by_family, replay.root)
    require(await connection.fetchval(f"SELECT count(*) FROM {_relation(replay.schema, TABLES[0])}") == replay.total)
    for table, count in replay.entity_totals.items():
        require(await connection.fetchval(f"SELECT count(*) FROM {_relation(replay.schema, table)}") == count)
    await _verify_plan_rows(connection, replay.schema, replay.origin, replay.release_id, replay.observed_at)


async def _verify_source_rows(
    source_export, connection, *, source_profile, reviewed_origin, expected_anchor, expected_identity
):
    """Require actual second Source EOF and successful context exit, independent of candidate receipts."""
    _original_source_factory(source_export, reviewed_origin)
    require(type(expected_anchor) is SourceWitnessCaptureAnchor)
    expected_anchor.__post_init__()
    reviewed = source_origin(reviewed_origin)
    require(source_profile.schema_name == reviewed.schema_name)
    hashes_by_family = {kind: hashlib.sha256() for kind, _ in reviewed.resource_counts}
    replay = _SourceReplay(
        reviewed,
        SCHEMA_PREFIX + UUID(expected_anchor.capture_id).hex,
        expected_anchor.release_id,
        datetime.fromisoformat(expected_anchor.observed_at),
        hashes_by_family,
        dict.fromkeys(hashes_by_family, 0),
        hashlib.sha256(),
        dict.fromkeys(TABLES[1:3], 0),
    )
    async with source_export(source_profile=source_profile, **_selection(reviewed_origin)) as export:
        identity = _export_identity(export, reviewed, expected_identity)
        iterator = export["resources"]()
        try:
            async for resource_rows in iterator:
                await _compare_source_page(connection, replay, resource_rows)
        finally:
            await drain_operation(iterator.aclose(), preserve_cancellation=True)
        await _finish_source_replay(connection, replay)
    return identity


async def _close_candidate(writer, schema, owner_role, runtime_roles):
    """Close ordinary candidate writers before independent Source replay."""
    relations = await _catalog_relations(writer, schema)
    await _transfer_and_grant(
        writer,
        SimpleNamespace(schema_name=schema),
        {
            "owner_role": owner_role,
            "loader_roles": [],
            "reader_roles": list(runtime_roles),
        },
        {
            "namespace": {"oid": relations[0]["schema_oid"], "nspowner": relations[0]["nspowner"]},
            "relations": [dict(relation) for relation in relations],
            "heaps": {relation["relname"]: relation["oid"] for relation in relations},
        },
    )
    await writer.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")


async def _seal(writer, origin, identity, schema, capture_id, owner_role, runtime_roles, observed_at):
    """Record closed custody only after the caller completes fresh Source replay."""
    schema_oid, relation_oids, catalog_hash = await _catalog(writer, schema, owner_role, runtime_roles)
    await retained._verify_resources(writer, schema, origin)
    release_id = "registry-source-witness:" + str(capture_id)
    await _verify_derived(writer, schema, origin, release_id, observed_at)
    capture = SourceWitnessCapture(
        origin=origin,
        source_database=identity,
        capture_id=str(capture_id),
        schema_name=schema,
        schema_oid=schema_oid,
        relation_oids=relation_oids,
        owner_role=owner_role,
        runtime_roles=runtime_roles,
        observed_at=canonical_timestamp(observed_at),
        catalog_sha256=catalog_hash,
        content_sha256=await _capture_content(writer, schema),
        capture_sha256="0" * 64,
    )
    capture = validate_capture(replace(capture, capture_sha256=capture_digest(capture)))
    await writer.execute(
        "COMMENT ON SCHEMA "
        + _identifier(schema)
        + " IS '"
        + (COMMENT_PREFIX + canonical(capture.as_dict())).replace("'", "''")
        + "'"
    )
    return capture


async def _require_capture_writer(writer, source_roles, owner_role, runtime_roles):
    """Keep writer and capture owner outside both original Source authority roles."""
    await _require_transaction(writer)
    actor = await writer.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,"
        "rolreplication,rolbypassrls,EXISTS(SELECT FROM unnest($1::name[]) source_roles(name) WHERE "
        "pg_has_role(session_user,source_roles.name,'MEMBER') OR pg_has_role(session_user,source_roles.name,'USAGE') "
        "OR pg_has_role(session_user,source_roles.name,'SET') OR pg_has_role($2::name,source_roles.name,'MEMBER') "
        "OR pg_has_role($2::name,source_roles.name,'USAGE') OR pg_has_role($2::name,source_roles.name,'SET')) "
        "AS source_authority_reachable FROM pg_roles WHERE rolname=session_user",
        source_roles,
        owner_role,
    )
    require(
        actor
        and actor["login"] == actor["actor"]
        and actor["rolcanlogin"]
        and not any(
            actor[name]
            for name in (
                "rolsuper",
                "rolcreatedb",
                "rolcreaterole",
                "rolreplication",
                "rolbypassrls",
                "source_authority_reachable",
            )
        )
    )
    await _check_roles(writer, {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)})


async def capture_source_fhir_witness(
    source_export, writer, *, source_profile, origin, capture_id, owner_role, runtime_roles, expected_anchor
):
    """Close only after authenticated construction, fresh Source replay and an independent Core anchor."""
    publisher_role = _original_source_factory(source_export, origin)
    reviewed = source_origin(origin)
    require(type(capture_id) is UUID and capture_id.int and type(runtime_roles) is tuple)
    require(type(expected_anchor) is SourceWitnessCaptureAnchor)
    expected_anchor.__post_init__()
    require(expected_anchor.capture_id == str(capture_id))
    require(tuple(sorted(set(runtime_roles))) == runtime_roles and source_profile.schema_name == reviewed.schema_name)
    require(owner_role != source_profile.owner_role)
    source_roles = (source_profile.owner_role, publisher_role)
    await _require_capture_writer(writer, source_roles, owner_role, runtime_roles)
    original_role = await writer.fetchval(
        "SELECT CASE WHEN current_setting('role')='none' THEN 'NONE' ELSE quote_ident(current_user) END"
    )
    schema = SCHEMA_PREFIX + capture_id.hex
    try:
        async with writer.transaction():
            require(await writer.fetchval("SELECT to_regnamespace($1)", schema) is None)
            await _create(writer, schema, owner_role)
            observed_at = await writer.fetchval("SELECT transaction_timestamp()")
            require(canonical_timestamp(observed_at) == expected_anchor.observed_at)
            async with source_export(source_profile=source_profile, **_selection(origin)) as export:
                identity = _export_identity(export, reviewed)
                await _copy_resources(
                    writer, schema, reviewed, export, "registry-source-witness:" + str(capture_id), observed_at
                )
            await _indexes(writer, schema)
            await writer.execute(f"SET LOCAL ROLE {original_role}")
            await _close_candidate(writer, schema, owner_role, runtime_roles)
            await _verify_source_rows(
                source_export,
                writer,
                source_profile=source_profile,
                reviewed_origin=origin,
                expected_anchor=expected_anchor,
                expected_identity=identity,
            )
            capture = await _seal(
                writer, reviewed, identity, schema, capture_id, owner_role, runtime_roles, observed_at
            )
            return require_capture_anchor(capture, expected_anchor)
    finally:
        if writer.is_in_transaction():
            await writer.execute(f"SET LOCAL ROLE {original_role}")


async def require_source_fhir_witness_capture(connection, capture, *, verify_content=True):
    """Recheck retained custody only; this alone does not authenticate a self-recomputed receipt to Source."""
    capture = validate_capture(capture)
    await _require_transaction(connection)
    require(type(verify_content) is bool)
    observed = await _catalog(connection, capture.schema_name, capture.owner_role, capture.runtime_roles)
    require(observed == (capture.schema_oid, capture.relation_oids, capture.catalog_sha256))
    comment = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", capture.schema_oid)
    require(comment == COMMENT_PREFIX + canonical(capture.as_dict()))
    if verify_content:
        require(await _capture_content(connection, capture.schema_name) == capture.content_sha256)
        await retained._verify_resources(connection, capture.schema_name, capture.origin)
        await _verify_derived(
            connection,
            capture.schema_name,
            capture.origin,
            capture.release_id,
            datetime.fromisoformat(capture.observed_at),
        )
    return capture


async def verify_source_fhir_witness_capture(
    source_export, connection, *, source_profile, reviewed_origin, capture, expected_anchor
):
    """Verify committed candidate data against fresh Source and independently anchored Core coordinates."""
    capture = require_capture_anchor(capture, expected_anchor)
    require(source_origin(reviewed_origin) == capture.origin)
    await require_source_fhir_witness_capture(connection, capture, verify_content=False)
    await _verify_source_rows(
        source_export,
        connection,
        source_profile=source_profile,
        reviewed_origin=reviewed_origin,
        expected_anchor=expected_anchor,
        expected_identity=capture.source_database,
    )
    require(await _capture_content(connection, capture.schema_name) == capture.content_sha256)
    return capture


@dataclass(frozen=True, eq=False)
class SourceWitnessRead:
    connection: object
    capture: SourceWitnessCapture
    task: object
    virtual_xid: str
    brand: object

    async def verify(self, connection, capture):
        """Keep this native read on its original task, connection and transaction."""
        require(self.brand is _READ_BRAND and connection is self.connection and capture == self.capture)
        require(self.task is asyncio.current_task() and connection.is_in_transaction())
        require(await _virtual_xid(connection) == self.virtual_xid)
        await require_source_fhir_witness_capture(connection, capture, verify_content=False)


async def pin_source_fhir_witness_read(connection, capture):
    """Issue a transaction-local capability after full retained-content verification."""
    capture = await require_source_fhir_witness_capture(connection, capture)
    return SourceWitnessRead(connection, capture, asyncio.current_task(), await _virtual_xid(connection), _READ_BRAND)
