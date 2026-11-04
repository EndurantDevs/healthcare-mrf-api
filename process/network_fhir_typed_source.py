# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consume genuine Source exports into bounded typed graph cells and independently rederive them."""

import asyncio
import json
import re
from contextlib import asynccontextmanager
from datetime import date, datetime
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_source_epoch_pin import _virtual_xid
from process.network_fhir_source_witness_capture import _export_identity, _original_source_factory
from process.network_fhir_source_witness_contract import database_identity, source_origin
from process.network_fhir_typed_contract import (
    _BRAND,
    ADDRESS_FIELDS,
    CELL_COLUMNS,
    KINDS,
    PAIR_COLUMNS,
    TypedFHIRGraphStage,
    graph_document,
    policy_date,
    require,
    validate_graph_document,
)
from process.network_fhir_typed_sql import (
    ADDRESS_DDL,
    CELL_DDL,
    CHECKER_DDL,
    CHECKER_PAIR_SQL,
    PAIR_DDL,
    PAIR_SQL,
    PROJECT_SQL,
)
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.uhc_flex_practitioner_async_safety import drain_operation

_RESOURCE_FIELDS = {
    "dataset_id",
    "resource_type",
    "resource_id",
    "payload_hash",
    "payload_json",
    "acquired_resource_sha256",
}
_DDL = {"cells": CELL_DDL, "addresses": ADDRESS_DDL, "pairs": PAIR_DDL, "checks": CHECKER_DDL}
_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")
_DATE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2}))?"
)


async def bind_typed_graph_stage(connection, *, schema_name, table_names, expected_oids=None, document=None):
    """Bind caller-owned tables to this task/transaction; native ACL/anchor admission stays with the caller."""
    require(connection.is_in_transaction(), "transaction_required")
    _identifier(schema_name)
    require(type(table_names) is dict and set(table_names) in ({"cells", "addresses", "pairs"}, set(_DDL)), "tables")
    for name in table_names.values():
        _identifier(name)
    oids_by_kind = {
        kind: await connection.fetchval("SELECT to_regclass($1)::oid::bigint", schema_name + "." + name)
        for kind, name in table_names.items()
    }
    require(all(type(oid) is int and oid > 0 for oid in oids_by_kind.values()), "tables_missing")
    require(expected_oids is None or expected_oids == oids_by_kind, "tables_changed")
    return TypedFHIRGraphStage(
        connection,
        asyncio.current_task(),
        await _virtual_xid(connection),
        dict(table_names),
        oids_by_kind,
        schema_name=schema_name,
        document=document,
        brand=_BRAND,
    )


def _raw_key(prefix=""):
    fields = ",".join(prefix + field for field in ADDRESS_FIELDS)
    return "encode(sha256(convert_to(jsonb_build_array(" + fields + ")::text,'UTF8')),'hex')"


@asynccontextmanager
async def _workspace(connection):
    """A savepoint owns every temporary object; any incomplete pass restarts from zero."""
    require(connection.is_in_transaction(), "transaction_required")
    names_by_kind = {kind: "fhir_typed_" + kind + "_" + uuid4().hex for kind in _DDL}
    stage, transaction = None, connection.transaction()
    await drain_operation(transaction.start(), preserve_cancellation=True)
    try:
        for kind, definition in _DDL.items():
            await connection.execute(
                "CREATE TEMP TABLE " + _identifier(names_by_kind[kind]) + "(" + definition + ") ON COMMIT DROP"
            )
        await connection.execute("CREATE INDEX ON " + _identifier(names_by_kind["addresses"]) + "(address_key)")
        stage = await bind_typed_graph_stage(connection, schema_name="pg_temp", table_names=names_by_kind)
        yield stage
        stage.active = False
        for name in reversed(tuple(names_by_kind.values())):
            await drain_operation(
                connection.execute("DROP TABLE pg_temp." + _identifier(name)), preserve_cancellation=True
            )
        await drain_operation(transaction.commit(), preserve_cancellation=True)
    except BaseException:
        if stage is not None:
            stage.active = False
        try:
            await drain_operation(transaction.rollback(), preserve_cancellation=False)
        except BaseException:
            connection.terminate()
        raise


async def _require_writer(connection, publisher):
    identity = await connection.fetchrow(
        "SELECT session_user::text actor,current_user::text effective,current_setting('transaction_read_only') readonly,"
        "current_setting('session_replication_role') replication"
    )
    require(identity["actor"] == identity["effective"] and identity["actor"] != publisher, "writer_actor")
    require(identity["readonly"] == "off" and identity["replication"] == "origin", "writer_scope")


def _page_rows(page, origin, counts, after):
    require(type(page) is tuple and 0 < len(page) <= 1000, "page_bounds")
    require(
        len(json.dumps(page, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()) <= 8 * 1024**2,
        "page_bounds",
    )
    wire_rows = []
    for resource_row in page:
        require(type(resource_row) is dict and set(resource_row) == _RESOURCE_FIELDS, "resource_shape")
        require(
            resource_row["dataset_id"] == origin.dataset_id and resource_row["acquired_resource_sha256"] is None,
            "resource_scope",
        )
        kind, identity = resource_row["resource_type"], resource_row["resource_id"]
        require(
            type(kind) is str and kind in counts and type(identity) is str and _ID.fullmatch(identity),
            "resource_identity",
        )
        key = kind, identity
        require(after is None or after < key, "resource_order")
        require(
            type(resource_row["payload_hash"]) is str and re.fullmatch(r"[0-9a-f]{64}", resource_row["payload_hash"]),
            "resource_hash",
        )
        counts[kind] += 1
        require(counts[kind] <= dict(origin.resource_counts)[kind], "resource_counts")
        after = key
        if kind in KINDS:
            wire_rows.append(
                {
                    "resource_type": kind,
                    "resource_id": identity,
                    "payload_hash": resource_row["payload_hash"],
                    "payload_json": resource_row["payload_json"]
                    if type(resource_row["payload_json"]) is str
                    else json.dumps(resource_row["payload_json"]),
                    "site_id": str(registry_location_id(origin.source_id, identity)) if kind == "Location" else None,
                }
            )
    return wire_rows, after


async def _project_page(connection, stage, rows):
    if not rows:
        return
    inserted = await connection.execute(
        PROJECT_SQL.format(cells=stage.relation("cells")), json.dumps(rows, separators=(",", ":")), list(KINDS)
    )
    require(inserted == "INSERT 0 " + str(len(rows)), "projected_shape")


def _text(payload, field):
    value = payload.get(field)
    require(value is None or type(value) is str, "raw_field")
    return value


def _refs(payload, field):
    values = payload.get(field)
    if values is None:
        return []
    require(type(values) is list and all(type(value) is str for value in values), "raw_reference")
    return values


def _target(value, kind, base):
    if value is None:
        return None
    prefix = base + "/"
    relative = value[len(prefix) :] if value.startswith(prefix) else value
    match = re.fullmatch(re.escape(kind) + r"/([A-Za-z0-9.-]{1,64})", relative)
    return match[1] if match else None


def _is_period_allowed(start, end, as_of):
    converted_dates = []
    for value in (start, end):
        if value is None:
            converted_dates.append(None)
            continue
        if _DATE.fullmatch(value) is None:
            return False
        try:
            if len(value) > 10:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            converted_dates.append(date.fromisoformat(value[:10]))
        except ValueError:
            return False
    start_date, end_date = converted_dates
    return (
        (start_date is None or start_date <= as_of)
        and (end_date is None or end_date >= as_of)
        and (start_date is None or end_date is None or start_date <= end_date)
    )


def _raw_projection(resource_row, base, as_of):
    """Independent raw parser: no producer cells, overlay NPI or Source record-id cache is consulted."""
    raw_payload = json.loads(resource_row["payload_json"])
    require(type(raw_payload) is dict, "raw_payload")
    kind = resource_row["resource_type"]
    is_entity, is_role = (
        kind in {"Organization", "Practitioner"},
        kind in {"PractitionerRole", "OrganizationAffiliation"},
    )
    npi, active, subject = None, None, None
    networks, locations, start, end, status = [], [], None, None, None
    address = [None] * len(ADDRESS_FIELDS)
    if is_entity or is_role:
        raw_npi = raw_payload.get("npi")
        require(raw_npi is None or type(raw_npi) in {str, int}, "raw_npi")
        npi = str(raw_npi) if raw_npi is not None else None
        active = raw_payload.get("active")
        require(active is None or type(active) is bool, "raw_active")
    if is_role:
        subject = _text(
            raw_payload, "practitioner_ref" if kind == "PractitionerRole" else "participating_organization_ref"
        )
        networks, locations = _refs(raw_payload, "network_refs"), _refs(raw_payload, "location_refs")
        start, end = _text(raw_payload, "period_start"), _text(raw_payload, "period_end")
    if kind == "Location":
        status = _text(raw_payload, "status")
        address = [_text(raw_payload, field) for field in ADDRESS_FIELDS]
    cells = (
        kind,
        resource_row["resource_id"],
        resource_row["payload_hash"],
        npi,
        active,
        subject,
        networks,
        locations,
        start,
        end,
        status,
        *address,
        UUID(resource_row["site_id"]) if resource_row["site_id"] else None,
    )
    subject_kind = "Practitioner" if kind == "PractitionerRole" else "Organization"
    checks = (
        kind,
        resource_row["resource_id"],
        subject_kind,
        _target(subject, subject_kind, base),
        [reference_target for ref in networks if (reference_target := _target(ref, "Organization", base)) is not None],
        [reference_target for ref in locations if (reference_target := _target(ref, "Location", base)) is not None],
        active is not False and status != "inactive" and (not is_role or _is_period_allowed(start, end, as_of)),
        npi is not None and is_valid_npi(npi) and re.fullmatch(r"[12][0-9]{9}", npi) is not None,
    )
    return cells, checks


async def _check_page(connection, stage, rows, base, as_of):
    projected_rows = [_raw_projection(row, base, as_of) for row in rows]
    if projected_rows:
        for table, records in (
            ("cells", [item[0] for item in projected_rows]),
            ("checks", [item[1] for item in projected_rows]),
        ):
            copied = await drain_operation(
                connection.copy_records_to_table(stage.names[table], schema_name=stage.schema_name, records=records),
                preserve_cancellation=True,
            )
            require(copied == "COPY " + str(len(rows)), "copy_count")


async def _consume(source_export, connection, stage, profile, reviewed, *, checker, expected_identity, as_of):
    origin = source_origin(reviewed)
    counts, after = dict.fromkeys(dict(origin.resource_counts), 0), None
    selection = (
        {"origin": reviewed}
        if checker
        else {key: getattr(reviewed, key) for key in ("source_id", "endpoint_id", "dataset_id")}
    )
    async with source_export(source_profile=profile, **selection) as export:
        identity = _export_identity(export, origin, expected_identity)
        iterator = export["resources"]()
        try:
            async for page in iterator:
                await stage.verify(connection, complete=False)
                rows, after = _page_rows(page, origin, counts, after)
                if checker:
                    await _check_page(connection, stage, rows, origin.source_uri.rstrip("/"), as_of)
                else:
                    await _project_page(connection, stage, rows)
        finally:
            await drain_operation(iterator.aclose(), preserve_cancellation=True)
        require(counts == dict(origin.resource_counts), "source_counts")
    # Only normal return after true iterator EOF and successful Source context exit is accepted.
    return identity, sum(counts.values())


def _valid_canonical_address_records(pending_by_key, canonical):
    return [
        (raw_key, *address_values, UUID(result["address_key"]), result["identity_key"])
        for (raw_key, address_values), result in zip(pending_by_key.items(), canonical, strict=True)
        if result["address_key"] is not None and result["identity_key"] is not None
    ]


async def _canonical_address_records(connection, stage, address_rows, fields):
    import ptg2_address_canon as native

    pending_by_key = {}
    for address_row in address_rows:
        address_values = tuple(address_row[field] for field in ADDRESS_FIELDS)
        require(
            address_row["raw_key"] not in pending_by_key or pending_by_key[address_row["raw_key"]] == address_values,
            "raw_address_collision",
        )
        pending_by_key[address_row["raw_key"]] = address_values
    known = await connection.fetch(
        "SELECT raw_key," + fields + " FROM " + stage.relation("addresses") + " WHERE raw_key=ANY($1::text[])",
        list(pending_by_key),
    )
    for address_row in known:
        require(
            pending_by_key.pop(address_row["raw_key"]) == tuple(address_row[field] for field in ADDRESS_FIELDS),
            "raw_address_collision",
        )
    if not pending_by_key:
        return None
    address_values = list(pending_by_key.values())
    canonical = await drain_operation(
        asyncio.to_thread(native.canonicalize_batch, address_values), preserve_cancellation=True
    )
    require(len(canonical) == len(pending_by_key), "canonical_count")
    address_records = _valid_canonical_address_records(pending_by_key, canonical)
    identities_by_key = {}
    for address_record in address_records:
        key, identity = address_record[-2:]
        require(key not in identities_by_key or identities_by_key[key] == identity, "canonical_collision")
        identities_by_key[key] = identity
    existing = await connection.fetch(
        "SELECT address_key,canonical_identity FROM "
        + stage.relation("addresses")
        + " WHERE address_key=ANY($1::uuid[])",
        list(identities_by_key),
    )
    require(
        all(
            identities_by_key[address_row["address_key"]] == address_row["canonical_identity"]
            for address_row in existing
        ),
        "canonical_collision",
    )
    return address_records


async def _canonicalize(connection, stage):
    """Canonicalize bounded location pages while refusing raw-key and full-identity collisions."""

    fields = ",".join(ADDRESS_FIELDS)
    query = (
        "SELECT resource_id,"
        + _raw_key()
        + " AS raw_key,"
        + fields
        + " FROM "
        + stage.relation("cells")
        + " WHERE resource_type='Location' AND resource_id>$1 ORDER BY resource_id LIMIT $2"
    )
    after, limit = "", 1000
    while True:
        size = await connection.fetchval(
            "SELECT coalesce(sum(octet_length(record_send(page))),0) FROM (" + query + ") page", after, limit
        )
        if size > 8 * 1024**2:
            require(limit > 1, "canonical_bounds")
            limit = max(1, limit // 2)
            continue
        address_rows = await connection.fetch(query, after, limit)
        if not address_rows:
            break
        after = address_rows[-1]["resource_id"]
        address_records = await _canonical_address_records(connection, stage, address_rows, fields)
        if address_records is None:
            continue
        copied = await drain_operation(
            connection.copy_records_to_table(
                stage.names["addresses"], schema_name=stage.schema_name, records=address_records
            ),
            preserve_cancellation=True,
        )
        require(copied == "COPY " + str(len(address_records)), "canonical_copy")


async def _pairs(connection, stage, origin, as_of, *, checker=False):
    query = CHECKER_PAIR_SQL if checker else PAIR_SQL
    arguments = () if checker else (origin.source_uri.rstrip("/"), as_of)
    for name in ("cells", "addresses", "checks"):
        if "{" + name + "}" in query:
            query = query.replace("{" + name + "}", stage.relation(name))
    after = ("", "")
    while rows := await connection.fetch(
        "SELECT resource_type,resource_id FROM "
        + stage.relation("cells")
        + " WHERE resource_type IN ('PractitionerRole','OrganizationAffiliation') AND (resource_type,resource_id)>($1,$2) ORDER BY resource_type,resource_id LIMIT 128",
        *after,
    ):
        last_keys = tuple(rows[-1])
        await connection.execute("INSERT INTO " + stage.relation("pairs") + " " + query, *arguments, *after, *last_keys)
        after = last_keys


async def _same_rows(connection, left, right, columns, keys):
    join = " AND ".join("a." + key + "=b." + key for key in keys)
    compare = (
        "ROW("
        + ",".join("a." + field for field in columns)
        + ") IS DISTINCT FROM ROW("
        + ",".join("b." + field for field in columns)
        + ")"
    )
    query = (
        "SELECT NOT EXISTS(SELECT FROM "
        + left
        + " a LEFT JOIN LATERAL (SELECT * FROM "
        + right
        + " b WHERE "
        + join
        + " OFFSET 0) b ON true WHERE b."
        + keys[0]
        + " IS NULL OR "
        + compare
        + ") AND NOT EXISTS(SELECT FROM "
        + right
        + " b WHERE NOT EXISTS(SELECT FROM "
        + left
        + " a WHERE "
        + join
        + " OFFSET 0))"
    )
    require(await connection.fetchval(query), "raw_graph_differs")


async def _verify_in_replay(source_export, connection, stage, replay, source_profile, reviewed_origin, identity, as_of):
    _, total = await _consume(
        source_export,
        connection,
        replay,
        source_profile,
        reviewed_origin,
        checker=True,
        expected_identity=identity,
        as_of=as_of,
    )
    await _same_rows(
        connection, stage.relation("cells"), replay.relation("cells"), CELL_COLUMNS, ("resource_type", "resource_id")
    )
    await _canonicalize(connection, replay)
    await _same_rows(
        connection,
        stage.relation("addresses"),
        replay.relation("addresses"),
        ("raw_key", *ADDRESS_FIELDS, "address_key", "canonical_identity"),
        ("raw_key",),
    )
    await _pairs(connection, replay, source_origin(reviewed_origin), as_of, checker=True)
    await _same_rows(
        connection,
        stage.relation("pairs"),
        replay.relation("pairs"),
        PAIR_COLUMNS,
        ("resource_type", "resource_id", "network_source_key", "location_resource_id"),
    )
    return total


async def verify_typed_source_graph(
    source_export, connection, stage, *, source_profile, reviewed_origin, identity, membership_as_of, replay_stage=None
):
    """Reopen original Source, independently project policy cells and check exact graph multiplicities."""
    _original_source_factory(source_export, reviewed_origin)
    await stage.verify(connection, complete=False)
    as_of = policy_date(membership_as_of)
    document = validate_graph_document(stage.document)
    identity = database_identity(identity)
    require(
        source_origin(document["origin"]) == source_origin(reviewed_origin)
        and database_identity(document["database_identity"]) == identity
        and document["membership_as_of"] == membership_as_of,
        "verification_scope",
    )
    if replay_stage is not None:
        await replay_stage.verify(connection, complete=False)
        require(replay_stage.schema_name == "pg_temp" and "checks" in replay_stage.names, "replay_workspace")
        await _empty_tables(connection, replay_stage)
        total = await _verify_in_replay(
            source_export, connection, stage, replay_stage, source_profile, reviewed_origin, identity, as_of
        )
    else:
        async with _workspace(connection) as replay:
            total = await _verify_in_replay(
                source_export, connection, stage, replay, source_profile, reviewed_origin, identity, as_of
            )
    require(document["counts"] == await _counts(connection, stage, total), "verification_counts")
    stage.checked = True
    return total


async def _empty_tables(connection, stage):
    for kind in stage.names:
        require(
            await connection.fetchval("SELECT NOT EXISTS(SELECT FROM " + stage.relation(kind) + ")"),
            "restart_from_zero",
        )


async def _counts(connection, stage, total):
    pair_counts = await connection.fetchrow(
        "SELECT count(*)::bigint rows,coalesce(sum(reference_rows),0)::bigint refs FROM " + stage.relation("pairs")
    )
    return {
        "source_rows": total,
        "projected_rows": await connection.fetchval("SELECT count(*) FROM " + stage.relation("cells")),
        "pair_rows": pair_counts["rows"],
        "reference_rows": pair_counts["refs"],
        "provider_only_roles": await connection.fetchval(
            "SELECT count(*) FROM "
            + stage.relation("cells")
            + " WHERE resource_type IN ('PractitionerRole','OrganizationAffiliation') AND cardinality(network_refs)=0"
        ),
    }


async def produce_typed_source_graph(
    source_export, connection, stage, *, source_profile, reviewed_origin, membership_as_of
):
    """Populate already-authenticated candidate tables once; this does not seal or approve their custody."""
    from dataclasses import asdict

    publisher = _original_source_factory(source_export, reviewed_origin)
    origin, as_of = source_origin(reviewed_origin), policy_date(membership_as_of)
    require(source_profile.schema_name == origin.schema_name, "source_profile")
    await _require_writer(connection, publisher)
    await stage.verify(connection, complete=False)
    await _empty_tables(connection, stage)
    identity, total = await _consume(
        source_export,
        connection,
        stage,
        source_profile,
        reviewed_origin,
        checker=False,
        expected_identity=None,
        as_of=as_of,
    )
    await _canonicalize(connection, stage)
    await _pairs(connection, stage, origin, as_of)
    counts = await _counts(connection, stage, total)
    stage.document = graph_document(reviewed_origin, asdict(identity), membership_as_of, counts)
    return stage.document


@asynccontextmanager
async def typed_source_graph(source_export, connection, *, source_profile, reviewed_origin, membership_as_of):
    """Produce and independently verify one transient Source graph; failed/lost scopes restart at zero."""
    async with _workspace(connection) as stage:
        document = await produce_typed_source_graph(
            source_export,
            connection,
            stage,
            source_profile=source_profile,
            reviewed_origin=reviewed_origin,
            membership_as_of=membership_as_of,
        )
        require(
            await verify_typed_source_graph(
                source_export,
                connection,
                stage,
                source_profile=source_profile,
                reviewed_origin=reviewed_origin,
                identity=document["database_identity"],
                membership_as_of=membership_as_of,
            )
            == document["counts"]["source_rows"],
            "source_counts",
        )
        stage.checked = True
        await stage.verify(connection)
        yield stage
