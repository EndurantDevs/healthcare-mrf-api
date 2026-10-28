# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Aggregate normalized provider reference facts in bounded native SQL pages."""

import asyncio
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

import asyncpg

from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    FHIRCatalogReferenceEdge,
    fhir_catalog_text,
    fhir_network_reference_target,
    fhir_source_uri,
    verify_fhir_catalog_retained_read,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

RESOURCE_TYPES = ("OrganizationAffiliation", "PractitionerRole")
MAX_SOURCE_ROWS = 5000
MAX_PAGE_REFERENCES = 100_000
MAX_OUTPUT_ROWS = 1000
_STAGE_COLUMNS = """resource_type varchar(64) COLLATE "C" NOT NULL,
reference_key text COLLATE "C" NOT NULL,network_ref text,
resource_id varchar(64) NOT NULL,payload_hash varchar(64) NOT NULL,reference_ordinal integer NOT NULL,
reference_rows bigint NOT NULL,PRIMARY KEY(resource_type,reference_key)"""


class FHIRCatalogReferenceRollupError(ValueError):
    """Reject a malformed source page without replacing last-good metadata."""


@dataclass(frozen=True)
class FHIRCatalogReferenceAccounting:
    source_rows: int
    reference_rows: int
    distinct_references: int
    source_without_references: int
    source_bytes: int
    resource_counts: dict[str, int]


@dataclass(frozen=True)
class FHIRCatalogReferenceRollup:
    edge: FHIRCatalogReferenceEdge
    reference_rows: int
    reference_ordinal: int


@dataclass
class FHIRCatalogReferenceWorkspace:
    connection: object
    task: object
    table_name: str
    table_oid: int
    active: bool = True

    async def verify(self, connection):
        """Require the active workspace, original task, connection and unchanged temporary table."""
        if (
            not self.active
            or connection is not self.connection
            or self.task is not asyncio.current_task()
            or await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + self.table_name)
            != self.table_oid
        ):
            _fail("workspace_closed")


@asynccontextmanager
async def fhir_catalog_reference_workspace(connection):
    """Create session-local staging before the caller opens its readonly snapshot."""
    if connection.is_in_transaction():
        _fail("workspace_transaction")
    name = "fhir_catalog_refs_" + uuid4().hex
    workspace = None
    try:
        await connection.execute(f"CREATE TEMP TABLE {_identifier(name)} ({_STAGE_COLUMNS}) ON COMMIT PRESERVE ROWS")
        oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + name)
        workspace = FHIRCatalogReferenceWorkspace(connection, asyncio.current_task(), name, oid)
        yield workspace
    finally:
        if workspace is not None:
            workspace.active = False
        await drain_operation(_drop_stage(connection, name), preserve_cancellation=False)


def _fail(reason="invalid"):
    raise FHIRCatalogReferenceRollupError("registry_fhir_catalog_reference_" + reason)


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) is None:
        _fail()
    return '"' + value + '"'


_PAGE_SQL = """WITH page AS MATERIALIZED (
 SELECT resource_type,resource_id,payload_hash,payload_json::jsonb->'network_refs' AS refs,
 jsonb_typeof(payload_json::jsonb) AS payload_kind,
 octet_length(payload_json::text)+128 AS source_bytes
 FROM {namespace}.provider_directory_dataset_resource
 WHERE dataset_id=$1 AND resource_type=ANY($2::text[])
   AND (resource_type COLLATE "C",resource_id COLLATE "C")>($3::text COLLATE "C",$4::text COLLATE "C")
 ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $5
), sized AS (
 SELECT *,sum(source_bytes) OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS page_bytes,
 row_number() OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS ordinal FROM page
), selected AS MATERIALIZED (
 SELECT * FROM sized WHERE ordinal=1 OR page_bytes<=$6
), accounting AS MATERIALIZED (
 SELECT count(*)::bigint AS source_rows,coalesce(sum(source_bytes),0)::bigint AS source_bytes,
 count(*) FILTER(WHERE refs IS NULL OR refs='null'::jsonb OR refs='[]'::jsonb)::bigint AS without_refs,
 count(*) FILTER(WHERE source_bytes>$6 OR resource_id IS NULL OR resource_id !~ '^[A-Za-z0-9.-]{{1,64}}$'
   OR payload_kind IS DISTINCT FROM 'object'
   OR payload_hash !~ '^[0-9a-f]{{64}}$' OR payload_hash IS NULL
   OR (refs IS NOT NULL AND refs<>'null'::jsonb AND jsonb_typeof(refs)<>'array'))::bigint AS invalid_rows,
 coalesce(sum(CASE WHEN jsonb_typeof(refs)='array' THEN jsonb_array_length(refs) ELSE 0 END),0)::bigint AS reference_rows
 FROM selected
), expanded AS MATERIALIZED (
 SELECT selected.resource_type,resource_id,payload_hash,reference.value AS reference_json,
 reference.ordinality-1 AS reference_ordinal
 FROM selected,accounting CROSS JOIN LATERAL jsonb_array_elements(
   CASE WHEN jsonb_typeof(refs)='array' AND accounting.invalid_rows=0 AND accounting.reference_rows<=$7
     THEN refs ELSE '[]'::jsonb END) WITH ORDINALITY reference(value,ordinality)
), invalid_references AS MATERIALIZED (
 SELECT count(*)::bigint AS invalid FROM expanded WHERE jsonb_typeof(reference_json) NOT IN ('string','null')
 OR (jsonb_typeof(reference_json)='string' AND
   (octet_length(reference_json#>>'{{}}')>2048 OR (reference_json#>>'{{}}') ~ '[[:cntrl:]]'))
), counts AS MATERIALIZED (
 SELECT resource_type,reference_json::text AS reference_key,count(*)::bigint AS reference_rows
 FROM expanded GROUP BY resource_type,reference_json::text
), representatives AS (
 SELECT DISTINCT ON(resource_type COLLATE "C",reference_json::text COLLATE "C")
 resource_type,reference_json::text AS reference_key,reference_json#>>'{{}}' AS network_ref,
 resource_id,payload_hash,reference_ordinal
 FROM expanded ORDER BY resource_type COLLATE "C",reference_json::text COLLATE "C",resource_id COLLATE "C",reference_ordinal
), staged AS (
 INSERT INTO {staging} AS previous(resource_type,reference_key,network_ref,resource_id,payload_hash,reference_ordinal,reference_rows)
 SELECT r.resource_type,r.reference_key,r.network_ref,r.resource_id,r.payload_hash,r.reference_ordinal,c.reference_rows
 FROM representatives r JOIN counts c USING(resource_type,reference_key),invalid_references invalid WHERE invalid.invalid=0
 ON CONFLICT(resource_type,reference_key) DO UPDATE SET
 reference_rows=previous.reference_rows+excluded.reference_rows,
 resource_id=CASE WHEN (excluded.resource_id COLLATE "C",excluded.reference_ordinal)<
   (previous.resource_id COLLATE "C",previous.reference_ordinal) THEN excluded.resource_id ELSE previous.resource_id END,
 payload_hash=CASE WHEN (excluded.resource_id COLLATE "C",excluded.reference_ordinal)<
   (previous.resource_id COLLATE "C",previous.reference_ordinal) THEN excluded.payload_hash ELSE previous.payload_hash END,
 reference_ordinal=CASE WHEN (excluded.resource_id COLLATE "C",excluded.reference_ordinal)<
   (previous.resource_id COLLATE "C",previous.reference_ordinal) THEN excluded.reference_ordinal ELSE previous.reference_ordinal END
 RETURNING 1
)
SELECT accounting.*,invalid_references.invalid AS invalid_references,(SELECT count(*) FROM staged) AS staged_rows,
 (SELECT resource_type FROM selected ORDER BY resource_type COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1) AS last_type,
 (SELECT resource_id FROM selected ORDER BY resource_type COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1) AS last_id,
 (SELECT coalesce(jsonb_object_agg(resource_type,source_rows),'{{}}'::jsonb)::text FROM
   (SELECT resource_type,count(*)::bigint AS source_rows FROM selected GROUP BY resource_type) groups) AS resource_counts
FROM accounting,invalid_references"""


def _edge(record, source_uri):
    network_ref = fhir_catalog_text(record["network_ref"], 2048, strip=False)
    target, reason = fhir_network_reference_target(network_ref, source_uri)
    ordinal = record["reference_ordinal"]
    edge = FHIRCatalogReferenceEdge(
        owner_resource_type=record["resource_type"],
        owner_resource_id=record["resource_id"],
        scope_path="/normalized_aggregate/" + str(ordinal),
        network_ref=network_ref,
        network_resource_id=target,
        resolution_reason=reason,
        plan_resource_id=None,
        plan_name=None,
        plan_aliases=(),
        plan_identifiers=(),
        owner_ref=None,
        administrator_ref=None,
        valid_from=None,
        valid_to=None,
        status=None,
        scope_identifiers=(),
        scope_type_codes=(),
        benefits=(),
        geography=(),
        source_hash=record["payload_hash"],
    )
    return FHIRCatalogReferenceRollup(edge, record["reference_rows"], ordinal)


@dataclass
class FHIRCatalogReferenceStage:
    """A task-owned temporary summary; it never grants Source admission or mapping authority."""

    connection: object
    task: object
    table_name: str
    source_uri: str | None
    accounting: FHIRCatalogReferenceAccounting
    active: bool = True
    retained_read: object | None = None

    async def iter_batches(self, *, page_rows=MAX_OUTPUT_ROWS):
        """Yield bounded genuine representative edges while this stage remains active."""
        if type(page_rows) is not int or not 1 <= page_rows <= MAX_OUTPUT_ROWS:
            _fail()
        cursor = ("", "")
        while True:
            if not self.active or self.task is not asyncio.current_task() or not self.connection.is_in_transaction():
                _fail("stage_closed")
            if self.retained_read is not None:
                await self.retained_read.verify(self.connection, self.retained_read.capture)
            records = await self.connection.fetch(
                f"SELECT * FROM pg_temp.{_identifier(self.table_name)} "
                'WHERE (resource_type COLLATE "C",reference_key COLLATE "C")>($1::text COLLATE "C",$2::text COLLATE "C") '
                'ORDER BY resource_type COLLATE "C",reference_key COLLATE "C" LIMIT $3',
                *cursor,
                page_rows,
            )
            if not records:
                return
            cursor = (records[-1]["resource_type"], records[-1]["reference_key"])
            yield tuple(_edge(record, self.source_uri) for record in records)


async def _fill_stage(connection, namespace, table_name, dataset_id, page_rows, page_bytes, retained_read=None):
    query = _PAGE_SQL.format(namespace=namespace, staging="pg_temp." + _identifier(table_name))
    totals_by_kind = {"source_rows": 0, "reference_rows": 0, "without_refs": 0, "source_bytes": 0}
    counts_by_type, cursor = {kind: 0 for kind in RESOURCE_TYPES}, ("", "")
    while True:
        if retained_read is not None:
            await retained_read.verify(connection, retained_read.capture)
        report = await drain_operation(
            connection.fetchrow(query, dataset_id, RESOURCE_TYPES, *cursor, page_rows, page_bytes, MAX_PAGE_REFERENCES),
            preserve_cancellation=True,
        )
        if report["reference_rows"] > MAX_PAGE_REFERENCES and page_rows > 1:
            page_rows = max(1, page_rows // 2)
            continue
        if report["reference_rows"] > MAX_PAGE_REFERENCES:
            _fail("expansion_bounds")
        if report["source_rows"] == 1 and report["source_bytes"] > page_bytes:
            _fail("source_page_bounds")
        if report["invalid_rows"] or report["invalid_references"]:
            _fail("source_page_invalid")
        if report["source_rows"] == 0:
            break
        for name in totals_by_kind:
            totals_by_kind[name] += report[name]
        for kind, count in json.loads(report["resource_counts"]).items():
            counts_by_type[kind] += count
        cursor = (report["last_type"], report["last_id"])
    distinct = await connection.fetchval(f"SELECT count(*)::bigint FROM pg_temp.{_identifier(table_name)}")
    return FHIRCatalogReferenceAccounting(
        totals_by_kind["source_rows"],
        totals_by_kind["reference_rows"],
        distinct,
        totals_by_kind["without_refs"],
        totals_by_kind["source_bytes"],
        counts_by_type,
    )


async def _drop_stage(connection, table_name):
    if connection.is_closed():
        return
    try:
        await connection.execute("DROP TABLE IF EXISTS pg_temp." + _identifier(table_name))
    except asyncpg.InFailedSQLTransactionError:
        # Creation belongs to the caller's failed transaction; its rollback removes the temp table.
        return


@asynccontextmanager
async def staged_fhir_catalog_references(
    connection,
    *,
    schema_name,
    dataset_id,
    page_rows=MAX_SOURCE_ROWS,
    page_bytes=DEFAULT_BYTES,
    source_uri=None,
    retained_read=None,
    workspace=None,
):
    """Roll up admitted normalized rows in the caller's pinned snapshot, without provider payload transfer."""
    namespace = _identifier(schema_name)
    if not dataset_id or fhir_catalog_text(dataset_id, 128) != dataset_id:
        _fail()
    if type(page_rows) is not int or not 1 <= page_rows <= MAX_SOURCE_ROWS:
        _fail()
    if type(page_bytes) is not int or not 1 <= page_bytes <= MAX_BYTES:
        _fail()
    if not connection.is_in_transaction():
        _fail("transaction_required")
    settings = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS read_only"
    )
    if settings["isolation"] not in {"repeatable read", "serializable"}:
        _fail("transaction_required")
    if workspace is None and settings["read_only"] != "off":
        _fail("workspace_required")
    if workspace is not None:
        if type(workspace) is not FHIRCatalogReferenceWorkspace:
            _fail("workspace_invalid")
        await workspace.verify(connection)
    if retained_read is not None:
        await verify_fhir_catalog_retained_read(
            connection, schema_name=schema_name, dataset_id=dataset_id, retained_read=retained_read
        )
    source_uri = fhir_source_uri(source_uri)
    table_name = workspace.table_name if workspace is not None else "fhir_catalog_refs_" + uuid4().hex
    stage = None
    try:
        statement = (
            "DELETE FROM pg_temp." + _identifier(table_name)
            if workspace is not None
            else f"CREATE TEMP TABLE {_identifier(table_name)} ({_STAGE_COLUMNS}) ON COMMIT DROP"
        )
        await drain_operation(connection.execute(statement), preserve_cancellation=True)
        accounting = await _fill_stage(
            connection, namespace, table_name, dataset_id, page_rows, page_bytes, retained_read
        )
        stage = FHIRCatalogReferenceStage(
            connection, asyncio.current_task(), table_name, source_uri, accounting, retained_read=retained_read
        )
        yield stage
    finally:
        if stage is not None:
            stage.active = False
        if workspace is None:
            await drain_operation(_drop_stage(connection, table_name), preserve_cancellation=False)
