# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close an original Source export in a distinct, independently verified Core capture."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

from process import network_fhir_retained_capture as retained
from process.network_address_projection import _identifier
from process.network_bootstrap_sources import _content as table_content
from process.network_fhir_source_epoch import (
    _CONTENT_SETTINGS,
    _borrowed_epoch_settings,
    _set_content_settings,
)
from process.network_fhir_source_witness_contract import (
    COMMENT_PREFIX,
    COMPLETION_CONTRACT,
    CONTRACT,
    SCHEMA_PREFIX,
    TABLES,
    SourceWitnessCapture,
    SourceWitnessCaptureAnchor,
    SourceWitnessCompletionOrigin,
    canonical,
    canonical_timestamp,
    capture_digest,
    digest,
    is_completion_v3_origin,
    require,
    require_capture_anchor,
    source_origin,
    source_resource_marker_valid,
    validate_capture,
)
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
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
_MAX_PAGE_BYTES = 8 * 1024**2
_MAX_DERIVED_BYTES = 64 * 1024**2


def _relation(schema, table):
    return _identifier(schema) + "." + _identifier(table)


@dataclass
class _CaptureCopyState:
    writer: object
    schema: str
    origin: object
    release_id: str
    observed_at: datetime
    hashes_by_family: dict
    counts_by_family: dict
    root: object
    total: int = 0


async def _create(writer, schema):
    await writer.execute(f"CREATE SCHEMA {_identifier(schema)}")
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
        copy_state.writer.copy_records_to_table(
            TABLES[0], schema_name=copy_state.schema, columns=_RESOURCE_FIELDS, records=resource_copy_records
        ),
        preserve_cancellation=True,
    )
    require(copied == f"COPY {len(resource_rows)}")
    if prepared is None:
        await retained._derived(
            copy_state.writer,
            copy_state.schema,
            copy_state.origin,
            copy_state.release_id,
            resource_rows,
            copy_state.observed_at,
        )
        return retained._fold_rows(
            resource_rows,
            copy_state.origin,
            copy_state.hashes_by_family,
            copy_state.counts_by_family,
            copy_state.root,
            copy_state.total,
        )
    await retained._copy_prepared(copy_state.writer, copy_state.schema, prepared)
    return retained._fold_prepared(
        prepared, copy_state.hashes_by_family, copy_state.counts_by_family, copy_state.root, copy_state.total
    )


async def _copy_resources(writer, schema, origin, export, release_id, observed_at):
    """Consume one real Source iterator to EOF; validate complete batches before bounded COPY."""
    is_completion_v3 = is_completion_v3_origin(origin)
    hashes_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    copy_state = _CaptureCopyState(
        writer,
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
                    and source_resource_marker_valid(
                        resource_record["acquired_resource_sha256"], completion_v3=is_completion_v3
                    )
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
            connection,
            replace(origin, schema_name=schema, resource_relation={"schema": schema, "name": TABLES[0]}),
            after,
            limit,
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


def _validate_source_export(export, reviewed):
    require(type(export) is dict and set(export) == {"contract", "origin", "resources"})
    contract = COMPLETION_CONTRACT if type(reviewed) is SourceWitnessCompletionOrigin else CONTRACT
    require(export["contract"] == contract and source_origin(export["origin"]) == reviewed)
    require(callable(export["resources"]))


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
    is_completion_v3 = is_completion_v3_origin(replay.origin)
    for resource_row in resource_rows:
        require(type(resource_row) is dict and set(resource_row) == set(_RESOURCE_FIELDS))
        require(
            resource_row["dataset_id"] == replay.origin.dataset_id
            and source_resource_marker_valid(resource_row["acquired_resource_sha256"], completion_v3=is_completion_v3)
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


async def _verify_source_rows(source_export, connection, *, source_profile, reviewed_origin, expected_anchor):
    """Require actual second Source EOF and successful context exit, independent of candidate receipts."""
    require(type(expected_anchor) is SourceWitnessCaptureAnchor)
    expected_anchor.__post_init__()
    reviewed = source_origin(reviewed_origin)
    require(source_profile.catalog_schema == reviewed.header_relation["schema"])
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
    async with source_export(source_profile=source_profile, origin=reviewed_origin) as export:
        _validate_source_export(export, reviewed)
        iterator = export["resources"]()
        try:
            async for resource_rows in iterator:
                await _compare_source_page(connection, replay, resource_rows)
        finally:
            await drain_operation(iterator.aclose(), preserve_cancellation=True)
        await _finish_source_replay(connection, replay)


async def capture_source_fhir_witness(
    source_export, writer, *, source_profile, origin, capture_id, owner_role, runtime_roles, expected_anchor
):
    """Build a named capture through the existing administrator preparation connection."""
    reviewed = source_origin(origin)
    require(type(capture_id) is UUID and capture_id.int and type(runtime_roles) is tuple)
    require(type(expected_anchor) is SourceWitnessCaptureAnchor)
    expected_anchor.__post_init__()
    require(expected_anchor.capture_id == str(capture_id))
    require(
        tuple(sorted(set(runtime_roles))) == runtime_roles
        and source_profile.catalog_schema == reviewed.header_relation["schema"]
    )
    schema = SCHEMA_PREFIX + capture_id.hex
    observed_at = datetime.fromisoformat(expected_anchor.observed_at)
    async with writer.transaction():
        await _create(writer, schema)
        async with source_export(source_profile=source_profile, **_selection(reviewed)) as export:
            _validate_source_export(export, reviewed)
            await _copy_resources(writer, schema, reviewed, export, expected_anchor.release_id, observed_at)
        await _indexes(writer, schema)
        await freeze_fhir_source_snapshot(
            writer, schema, namespace_owner=owner_role, reader_roles=runtime_roles, application_roles=runtime_roles
        )
        await _verify_source_rows(
            source_export,
            writer,
            source_profile=source_profile,
            reviewed_origin=origin,
            expected_anchor=expected_anchor,
        )
        await retained._verify_resources(writer, schema, reviewed)
        await _verify_derived(writer, schema, reviewed, expected_anchor.release_id, observed_at)
        capture = SourceWitnessCapture(
            origin=reviewed,
            capture_id=str(capture_id),
            schema_name=schema,
            tables={name: {"schema": schema, "name": name} for name in TABLES},
            observed_at=canonical_timestamp(observed_at),
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
        return require_capture_anchor(capture, expected_anchor)


async def require_source_fhir_witness_capture(connection, capture, *, verify_content=True):
    """Validate the selected edition and optionally recheck its complete data."""
    capture = validate_capture(capture)
    require(connection.is_in_transaction() and type(verify_content) is bool)
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
    """Compare committed capture data with a complete fresh Source replay."""
    capture = require_capture_anchor(capture, expected_anchor)
    require(source_origin(reviewed_origin) == capture.origin)
    await _verify_source_rows(
        source_export,
        connection,
        source_profile=source_profile,
        reviewed_origin=reviewed_origin,
        expected_anchor=expected_anchor,
    )
    require(await _capture_content(connection, capture.schema_name) == capture.content_sha256)
    return capture


@dataclass(frozen=True)
class SourceWitnessRead:
    """The logical capture selected for a named data read."""

    capture: SourceWitnessCapture

    def require_ready(self, capture):
        """Require the caller's capture to match this selected logical edition."""
        require(validate_capture(capture) == validate_capture(self.capture))


async def pin_source_fhir_witness_read(connection, capture):
    """Validate complete capture content before preparing a network candidate."""
    return SourceWitnessRead(await require_source_fhir_witness_capture(connection, capture))
