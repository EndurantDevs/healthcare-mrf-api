# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Aggregate normalized provider reference facts in bounded native SQL pages."""

import json
import re
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from uuid import uuid4

import asyncpg

from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    FHIRCatalogPlanTierEdge,
    FHIRCatalogReferenceEdge,
    fhir_catalog_resource_relation,
    fhir_catalog_text,
    fhir_network_reference_target,
    fhir_source_uri,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

RESOURCE_TYPES = ("OrganizationAffiliation", "PractitionerRole")
MAX_SOURCE_ROWS = 5000
MAX_PAGE_REFERENCES = 100_000
MAX_OUTPUT_ROWS = 1000
_STAGE_COLUMNS = """resource_type varchar(64) COLLATE "C" NOT NULL,
reference_key text COLLATE "C" NOT NULL,network_ref text,
resource_id varchar(64) COLLATE "C" NOT NULL,payload_hash varchar(64) NOT NULL,reference_ordinal integer NOT NULL,
 reference_rows bigint NOT NULL,PRIMARY KEY(resource_type,reference_key),
UNIQUE(resource_type,resource_id,reference_ordinal)"""


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
    table_name: str
    active: bool = True


@asynccontextmanager
async def fhir_catalog_reference_workspace(connection):
    """Create session-local staging before the caller opens its readonly snapshot."""
    if connection.is_in_transaction():
        _fail("workspace_transaction")
    name = "fhir_catalog_refs_" + uuid4().hex
    workspace = None
    try:
        await connection.execute(f"CREATE TEMP TABLE {_identifier(name)} ({_STAGE_COLUMNS}) ON COMMIT PRESERVE ROWS")
        workspace = FHIRCatalogReferenceWorkspace(connection, name)
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


_PAGE_SQL = """WITH page AS (
 SELECT resource_type,resource_id,payload_hash,payload_json,
 octet_length(payload_json::text)+128 AS source_bytes
 FROM {resources}
 WHERE dataset_id=$1 AND resource_type=ANY($2::text[])
   AND (resource_type COLLATE "C",resource_id COLLATE "C")>($3::text COLLATE "C",$4::text COLLATE "C")
 ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $5
), sized AS (
 SELECT *,sum(source_bytes) OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS page_bytes,
 row_number() OVER(ORDER BY resource_type COLLATE "C",resource_id COLLATE "C") AS ordinal FROM page
), selected AS MATERIALIZED (
 SELECT resource_type,resource_id,payload_hash,parsed.payload->'network_refs' AS raw_refs,
 CASE WHEN $8::boolean AND (parsed.payload->>'network_key_id' IS NOT NULL OR parsed.payload->>'network_tier' IS NOT NULL)
 THEN (CASE WHEN jsonb_typeof(parsed.payload->'network_refs')='array' THEN parsed.payload->'network_refs' ELSE '[]'::jsonb END)
 ||jsonb_build_array(jsonb_build_object(
   'kind','uhc_plan_tier_v1','plan_scope',parsed.payload->'plan_scope',
   'network_tier',parsed.payload->'network_tier','network_key_id',parsed.payload->'network_key_id',
   'insurance_plan_refs',parsed.payload->'insurance_plan_refs'))
 ELSE parsed.payload->'network_refs' END AS refs,
 jsonb_typeof(parsed.payload) AS payload_kind,source_bytes,page_bytes,ordinal
 FROM sized
 CROSS JOIN LATERAL (SELECT payload_json::jsonb AS payload OFFSET 0) parsed
 WHERE ordinal=1 OR page_bytes<=$6
), accounting AS MATERIALIZED (
 SELECT count(*)::bigint AS source_rows,coalesce(sum(source_bytes),0)::bigint AS source_bytes,
 count(*) FILTER(WHERE refs IS NULL OR refs='null'::jsonb OR refs='[]'::jsonb)::bigint AS without_refs,
 count(*) FILTER(WHERE source_bytes>$6 OR resource_id IS NULL OR length(resource_id) NOT BETWEEN 1 AND 64
   OR resource_id ~ '[^A-Za-z0-9.-]'
   OR payload_kind IS DISTINCT FROM 'object'
   OR length(payload_hash)<>64 OR payload_hash ~ '[^0-9a-f]' OR payload_hash IS NULL
   OR (raw_refs IS NOT NULL AND raw_refs<>'null'::jsonb AND jsonb_typeof(raw_refs)<>'array')
   OR EXISTS(SELECT FROM jsonb_array_elements(CASE WHEN jsonb_typeof(raw_refs)='array' THEN raw_refs ELSE '[]'::jsonb END) item
     WHERE jsonb_typeof(item) NOT IN ('string','null')))::bigint AS invalid_rows,
 coalesce(sum(CASE WHEN jsonb_typeof(refs)='array' THEN jsonb_array_length(refs) ELSE 0 END),0)::bigint AS reference_rows
 FROM selected
), expanded AS MATERIALIZED (
 SELECT selected.resource_type,resource_id,payload_hash,reference.value AS reference_json,
 reference.ordinality-1 AS reference_ordinal
 FROM selected,accounting CROSS JOIN LATERAL jsonb_array_elements(
   CASE WHEN jsonb_typeof(refs)='array' AND accounting.invalid_rows=0 AND accounting.reference_rows<=$7
     THEN refs ELSE '[]'::jsonb END) WITH ORDINALITY reference(value,ordinality)
), invalid_references AS MATERIALIZED (
 SELECT count(*)::bigint AS invalid FROM expanded WHERE jsonb_typeof(reference_json) NOT IN ('string','null','object')
 OR (jsonb_typeof(reference_json)='object' AND octet_length(reference_json::text)>2048)
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


def _plan_tier_edge(record, source_uri):
    from process.registry_fhir_plan_tier import role_plan_tier

    tier = role_plan_tier(json.loads(record["reference_key"]))
    if tier is None:
        _fail()
    base = _edge({**dict(record), "reference_key": "null", "network_ref": None}, source_uri)
    edge = FHIRCatalogPlanTierEdge(
        **{
            **asdict(base.edge),
            "scope_path": "/plan_tier",
            "resolution_reason": None,
            "plan_resource_id": tier["plan_resource_id"],
        },
        plan_scope=tier["plan_scope"],
        plan_tier=tier,
    )
    return FHIRCatalogReferenceRollup(edge, record["reference_rows"], record["reference_ordinal"])


def _edge(record, source_uri):
    if record.get("reference_key", "").startswith("{"):
        return _plan_tier_edge(record, source_uri)
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
    """A bounded temporary reference summary for an active snapshot."""

    connection: object
    table_name: str
    source_uri: str | None
    accounting: FHIRCatalogReferenceAccounting
    active: bool = True

    async def iter_batches(self, *, page_rows=MAX_OUTPUT_ROWS):
        """Yield bounded genuine representative edges while this stage remains active."""
        if type(page_rows) is not int or not 1 <= page_rows <= MAX_OUTPUT_ROWS:
            _fail()
        cursor = ("", "", -1)
        while True:
            if not self.active or not self.connection.is_in_transaction():
                _fail("stage_closed")
            records = await self.connection.fetch(
                f"SELECT * FROM pg_temp.{_identifier(self.table_name)} "
                'WHERE (resource_type COLLATE "C",resource_id COLLATE "C",reference_ordinal)>'
                '($1::text COLLATE "C",$2::text COLLATE "C",$3::integer) '
                'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",reference_ordinal LIMIT $4',
                *cursor,
                page_rows,
            )
            if not records:
                return
            cursor = (records[-1]["resource_type"], records[-1]["resource_id"], records[-1]["reference_ordinal"])
            yield tuple(_edge(record, self.source_uri) for record in records)


async def _fill_stage(connection, resources, table_name, dataset_id, page_rows, page_bytes, *, plan_tiers=False):
    query = _PAGE_SQL.format(resources=resources, staging="pg_temp." + _identifier(table_name))
    totals_by_kind = {"source_rows": 0, "reference_rows": 0, "without_refs": 0, "source_bytes": 0}
    counts_by_type, cursor = {kind: 0 for kind in RESOURCE_TYPES}, ("", "")
    while True:
        report = await drain_operation(
            connection.fetchrow(
                query, dataset_id, RESOURCE_TYPES, *cursor, page_rows, page_bytes, MAX_PAGE_REFERENCES, plan_tiers
            ),
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


def _validate_reference_page(schema_name, dataset_id, page_rows, page_bytes):
    _identifier(schema_name)
    if not dataset_id or fhir_catalog_text(dataset_id, 128) != dataset_id:
        _fail()
    if type(page_rows) is not int or not 1 <= page_rows <= MAX_SOURCE_ROWS:
        _fail()
    if type(page_bytes) is not int or not 1 <= page_bytes <= MAX_BYTES:
        _fail()


async def _verify_reference_workspace(connection, workspace):
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
        if not workspace.active:
            _fail("workspace_closed")


def staged_fhir_catalog_references(
    connection,
    *,
    schema_name,
    dataset_id,
    page_rows=MAX_SOURCE_ROWS,
    page_bytes=DEFAULT_BYTES,
    source_uri=None,
    resource_relation=None,
    workspace=None,
):
    """Stage bounded references using the original Organization policy."""
    return _staged_references(
        connection, schema_name, dataset_id, (page_rows, page_bytes), (source_uri, resource_relation, workspace), False
    )


def staged_plan_tier_references(
    connection,
    *,
    schema_name,
    dataset_id,
    page_rows=MAX_SOURCE_ROWS,
    page_bytes=DEFAULT_BYTES,
    source_uri=None,
    resource_relation=None,
    workspace=None,
):
    """Stage bounded references using the typed published policy."""
    return _staged_references(
        connection, schema_name, dataset_id, (page_rows, page_bytes), (source_uri, resource_relation, workspace), True
    )


@asynccontextmanager
async def _staged_references(connection, schema_name, dataset_id, bounds, readers, plan_tiers):
    """Roll up admitted normalized rows in the caller's pinned snapshot, without provider payload transfer."""
    page_rows, page_bytes = bounds
    source_uri, resource_relation, workspace = readers
    _validate_reference_page(schema_name, dataset_id, page_rows, page_bytes)
    await _verify_reference_workspace(connection, workspace)
    resources = fhir_catalog_resource_relation(schema_name=schema_name, resource_relation=resource_relation)
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
            connection, resources, table_name, dataset_id, page_rows, page_bytes, plan_tiers=plan_tiers
        )
        stage = FHIRCatalogReferenceStage(
            connection,
            table_name,
            source_uri,
            accounting,
        )
        yield stage
    finally:
        if stage is not None:
            stage.active = False
        if workspace is None:
            await drain_operation(_drop_stage(connection, table_name), preserve_cancellation=False)
