# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture already published normalized FHIR without inventing ingest authority."""

import asyncio
import hashlib
import importlib
import json
import re
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from uuid import UUID

from ijson.backends import python as ijson_python
from sqlalchemy import text

from process.network_address_projection import _identifier
from process.network_bootstrap_sources import _content, _copy_native_batch, _CopyBatchTooLarge
from process.network_custom_address_source import _closed, _require_transaction
from process.network_fhir_retained_capture_contract import (
    LEGACY_PROFILE,
    SEMANTIC_PROFILE,
    TABLES,
    RetainedFHIRCapture,
    RetainedFHIRCaptureError,
    RetainedFHIRInput,
    canonical,
    capture_digest,
    digest,
    registry_location_id,
    require,
    validate_capture,
)
from process.network_fhir_retained_native import (
    RetainedFhirNativeError,
    postgres_micros,
    prepare_retained_fhir_batch,
)
from process.network_fhir_source_epoch import (
    _CONTENT_SETTINGS,
    _borrowed_epoch_settings,
    _catalog_relations,
    _set_content_settings,
)
from process.network_fhir_source_epoch_pin import _columns, _virtual_xid
from process.network_generic_source_published_snapshot import published_generic_binding, require_native_generic_snapshot
from process.network_membership_writer_closure import _check_roles, _protected_owner, _transfer_and_grant
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    validate_generic_admission_copy,
)
from process.provider_directory_admission_stream import _copy_field_reader
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from process.provider_directory_dataset_rehydrate_pin import _same_database
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.provider_directory_source_summary import validate_fhir_source_summary
from process.uhc_flex_practitioner_async_safety import drain_operation

_KEYS = {
    TABLES[0]: ("dataset_id",),
    TABLES[1]: ("dataset_id", "resource_type", "resource_id"),
    TABLES[2]: ("source_id", "resource_type", "resource_id"),
    TABLES[3]: ("source_id", "resource_type", "resource_id", "release_id"),
    TABLES[4]: ("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
}
_DERIVED_DDL = {
    TABLES[
        2
    ]: "source_id varchar(64) NOT NULL,resource_type varchar(16) NOT NULL,resource_id varchar(256) NOT NULL,organization_id uuid,site_id uuid,created_at timestamptz NOT NULL",
    TABLES[
        3
    ]: "source_id varchar(64) NOT NULL,resource_type varchar(16) NOT NULL,resource_id varchar(256) NOT NULL,release_id varchar(256) NOT NULL,payload_sha256 varchar(64) NOT NULL,payload_json jsonb NOT NULL,observed_at timestamptz NOT NULL",
    TABLES[
        4
    ]: "source_id varchar(64) NOT NULL,release_id varchar(256) NOT NULL,network_resource_type varchar(16) NOT NULL,network_resource_id varchar(256) NOT NULL,insurance_plan_resource_id varchar(256) NOT NULL,network_refs jsonb NOT NULL,owned_by_ref text,administered_by_ref text,plan_payload_sha256 varchar(64) NOT NULL,plan_payload_json jsonb NOT NULL,observed_at timestamptz NOT NULL",
}
_LOOKUPS = {
    TABLES[1]: (("dataset_id", "resource_type", "resource_id"),),
    TABLES[2]: (("site_id",),),
    TABLES[3]: (("source_id", "release_id"),),
    TABLES[4]: (("source_id", "release_id", "insurance_plan_resource_id"),),
}
_READ_BRAND = object()
_DERIVED_BATCH_ROWS = 5000
_DERIVED_BATCH_BYTES = 64 * 1024**2


@dataclass(frozen=True, eq=False)
class RetainedFHIRRead:
    """One caller-owned content admission, bound to its original native transaction."""

    connection: object
    capture: RetainedFHIRCapture
    task: object
    virtual_xid: str
    brand: object

    async def verify(self, connection, capture):
        """Recheck native custody per page without a global cache or full-table rehash."""
        require(self.brand is _READ_BRAND and connection is self.connection and capture == self.capture)
        require(self.task is asyncio.current_task() and connection.is_in_transaction())
        require(await _virtual_xid(connection) == self.virtual_xid)
        await require_retained_fhir_capture(connection, capture, verify_content=False)


async def pin_retained_fhir_read(connection, capture):
    """Verify complete immutable content once before this operation's page traversal."""
    capture = await require_retained_fhir_capture(connection, capture)
    return RetainedFHIRRead(connection, capture, asyncio.current_task(), await _virtual_xid(connection), _READ_BRAND)


def _relation(schema, table):
    return _identifier(schema) + "." + _identifier(table)


class _ReaderCopy:
    """Export only the original Reader; import only the caller's target savepoint."""

    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.task = asyncio.current_task()
        self.virtual_xid = None

    async def verify(self):
        """Refuse export/import after either original transaction or task changed."""
        require(
            self.task is asyncio.current_task() and self.reader.is_in_transaction() and self.writer.is_in_transaction()
        )
        require(await _virtual_xid(self.reader) == self.virtual_xid)

    def transaction(self):
        """Nest only the target COPY import in a writer savepoint."""
        return self.writer.transaction()

    async def copy_from_query(self, *arguments, **options):
        """Export exact retained rows through the genuine Reader connection."""
        await self.verify()
        return await _reader_export(self.reader, *arguments, **options)

    async def copy_to_table(self, *arguments, **options):
        """Import only into the caller-owned candidate transaction."""
        await self.verify()
        return await drain_operation(self.writer.copy_to_table(*arguments, **options), preserve_cancellation=True)


async def _reader_export(reader, *arguments, **options):
    """Drain native COPY cancellation through a read-only savepoint before returning."""
    transaction = reader.transaction()
    task, has_started = asyncio.current_task(), False
    cancellations = task.cancelling()
    try:
        await drain_operation(transaction.start(), preserve_cancellation=False)
        has_started = True
        if task.cancelling() > cancellations:
            raise asyncio.CancelledError
        return await reader.copy_from_query(*arguments, **options)
    finally:
        if has_started:
            await drain_operation(transaction.rollback(), preserve_cancellation=True)


async def _reader_identity(reader, origin):
    await _require_transaction(reader)
    actor = await reader.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,rolsuper,rolcreatedb,rolcreaterole,"
        "rolreplication,rolbypassrls,current_setting('transaction_read_only') AS readonly,"
        "current_setting('session_replication_role') AS replication_role FROM pg_roles WHERE rolname=session_user"
    )
    require(actor and actor["login"] == actor["actor"] == origin.reader_role and actor["readonly"] == "on")
    require(
        actor["replication_role"] == "origin"
        and not any(
            actor[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
    )
    source_oid = await reader.fetchval(
        "SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname='provider_directory_source' AND c.relkind='r' "
        "AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity",
        origin.schema_name,
    )
    require(type(source_oid) is int and source_oid > 0)
    require(
        await reader.fetchval(
            "SELECT NOT has_schema_privilege(current_user,$1,'CREATE') "
            "AND NOT has_parameter_privilege(current_user,'session_replication_role','SET') "
            "AND NOT EXISTS(SELECT FROM pg_class c "
            "WHERE c.oid=ANY($2::oid[]) AND (has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
            "OR has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES') "
            "OR pg_has_role(current_user,c.relowner,'MEMBER') OR pg_has_role(current_user,c.relowner,'USAGE') "
            "OR pg_has_role(current_user,c.relowner,'SET')))",
            origin.schema_name,
            [origin.header_oid, origin.resource_parent_oid, origin.resource_leaf_oid, source_oid],
        )
        is True
    )


async def _origin_header(reader, origin):
    """Bind original bytes and published coordinates, never fill a missing admission seal."""
    header = _relation(origin.schema_name, TABLES[0])
    await reader.execute(
        f"LOCK TABLE ONLY {header}, ONLY {_relation(origin.schema_name, TABLES[1])}, "
        f"ONLY {_relation(origin.schema_name, 'provider_directory_source')} IN ACCESS SHARE MODE NOWAIT"
    )
    header = await reader.fetchrow(
        f"SELECT tableoid::oid::bigint AS oid,endpoint_id,acquisition_root_run_id,dataset_hash,status,published_at,resource_count,"
        "candidate_storage_json,completion_proof_required_version,publication_metadata_sha256,"
        "content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256,content_proof_resource_types,"
        "CASE WHEN octet_length(publication_metadata_summary_json::text)<="
        + str(ADMISSION_METADATA_SUMMARY_MAX_BYTES)
        + " THEN publication_metadata_summary_json::text END AS admitted_summary,"
        "publication_metadata_summary_json IS NULL AND publication_metadata_sha256 IS NULL "
        "AND content_proof_admission_version IS NULL AND content_proof_admission_kind IS NULL "
        "AND content_proof_admission_sha256 IS NULL AND content_proof_resource_types IS NULL AS admission_absent,"
        "encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256,"
        "octet_length(publication_metadata_json::text)::bigint AS metadata_bytes,"
        "publication_metadata_json->'source_summary_v1' AS summary,"
        "publication_metadata_json->>'resource_hash_contract' AS hash_contract,"
        "publication_metadata_json->>'semantic_projection_as_of' AS semantic_as_of,"
        "publication_metadata_json->'provider_directory_content_proof_v1'->>'proof_sha256' AS proof_sha256 "
        f"FROM {header} WHERE dataset_id=$1",
        origin.dataset_id,
    )
    binding = _require_header_fields(header, origin)
    if origin.admission_profile == SEMANTIC_PROFILE:
        await _validate_admitted_proof(reader, origin)
    await _origin_resource(reader, origin)
    return binding


def _require_header_fields(header, origin):
    """Verify original publication bytes and its complete source summary."""
    require(
        header
        and (
            header["oid"],
            header["endpoint_id"],
            header["acquisition_root_run_id"],
            header["dataset_hash"],
            header["resource_count"],
        )
        == (
            origin.header_oid,
            origin.endpoint_id,
            origin.root_run_id,
            origin.dataset_sha256,
            sum(dict(origin.resource_counts).values()),
        )
    )
    require(header["status"] in {"published", "superseded"} and header["published_at"] is not None)
    binding = published_generic_binding(
        header,
        dataset_id=origin.dataset_id,
        endpoint_id=origin.endpoint_id,
        root_run_id=origin.root_run_id,
        source_id=origin.source_id,
    )
    if origin.admission_profile == LEGACY_PROFILE:
        require(binding is None and header["hash_contract"] is None and header["admission_absent"] is True)
    else:
        _require_admitted_header(header, origin)
    require(
        (header["metadata_sha256"], header["metadata_bytes"], header["proof_sha256"])
        == (origin.metadata_text_sha256, origin.metadata_bytes, origin.proof_sha256)
    )
    summary = json.loads(header["summary"]) if type(header["summary"]) is str else header["summary"]
    _require_summary(summary, origin)
    return binding


def _require_summary(summary, origin):
    """Bind either original or stored source summary to the complete reviewed roots."""
    summary = validate_fhir_source_summary(
        summary,
        expected_by_field={
            "source_ids": [origin.source_id],
            "dataset_id": origin.dataset_id,
            "endpoint_id": origin.endpoint_id,
            "acquisition_root_run_id": origin.root_run_id,
            "dataset_hash": origin.dataset_sha256,
        },
    )
    require(
        summary["summary_sha256"] == origin.summary_sha256
        and summary["resource_counts"] == dict(origin.resource_counts)
        and summary["resource_hashes"] == dict(origin.resource_hashes)
    )


def _require_admitted_header(header, origin):
    """Authenticate the existing current summary without replacing original raw metadata."""
    require(
        header["hash_contract"] == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        and header["semantic_as_of"] == origin.semantic_projection_as_of
        and header["publication_metadata_sha256"] == origin.admission_metadata_sha256
        and header["content_proof_admission_version"] == 1
        and header["content_proof_admission_kind"] == "generic"
        and header["content_proof_admission_sha256"] == origin.proof_sha256
        and header["content_proof_resource_types"] == list(dict(origin.resource_counts))
    )
    summary = json.loads(header["admitted_summary"]) if type(header["admitted_summary"]) is str else None
    require(type(summary) is dict)
    require(
        summary.get("resource_hash_contract") == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        and summary.get("semantic_projection_as_of") == origin.semantic_projection_as_of
        and summary.get(ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
        == {
            "dataset_hash": origin.dataset_sha256,
            "resource_count": sum(dict(origin.resource_counts).values()),
            "resource_counts": dict(origin.resource_counts),
            "resource_hashes": dict(origin.resource_hashes),
        }
    )
    _require_summary(summary.get("source_summary_v1"), origin)
    _require_plan_summary(summary, origin)
    seal = ProviderDirectoryAdmissionSeal(
        summary,
        origin.admission_metadata_sha256,
        1,
        "generic",
        origin.proof_sha256,
        tuple(dict(origin.resource_counts)),
    )
    require(canonical_payload_sha256(seal.digest_envelope()) == origin.admission_metadata_sha256)


def _require_plan_summary(metadata, origin):
    """Preserve the admitted source's exact incomplete-reference accounting."""
    plan = metadata.get("dataset_network_plan")
    require(
        type(plan) is dict
        and plan.get("version") == 1
        and plan.get("complete") is True
        and plan.get("dataset_id") == origin.dataset_id
        and type(plan.get("unresolved_reference_count")) is int
        and plan["unresolved_reference_count"] == origin.unresolved_plan_network_count
    )


async def _validate_admitted_proof(reader, origin):
    """Run the existing complete raw-COPY validator; never backfill or rewrite Source seals."""
    with TemporaryDirectory(prefix="retained-fhir-metadata-") as directory:
        path = Path(directory) / "metadata.copy"
        with path.open("wb") as output:

            async def bounded_write(block):
                """Bound every native chunk before writing task-local metadata bytes."""
                require(output.tell() + len(block) <= origin.metadata_bytes + 27)
                output.write(block)

            status = await _reader_export(
                reader,
                f"SELECT publication_metadata_json::text FROM {_relation(origin.schema_name, TABLES[0])} WHERE dataset_id=$1",
                origin.dataset_id,
                output=bounded_write,
                format="binary",
            )
        require(status == "COPY 1" and path.stat().st_size == origin.metadata_bytes + 27)
        with path.open("rb") as raw:
            raw.seek(25)
            metadata_digest = hashlib.sha256()
            remaining = origin.metadata_bytes
            while remaining:
                block = raw.read(min(1024 * 1024, remaining))
                require(block)
                metadata_digest.update(block)
                remaining -= len(block)
        require(metadata_digest.hexdigest() == origin.metadata_text_sha256)
        try:
            seal = validate_generic_admission_copy(
                path,
                dataset_id=origin.dataset_id,
                endpoint_id=origin.endpoint_id,
                evidence_run_id=origin.root_run_id,
                dataset_hash=origin.dataset_sha256,
                resource_count=sum(dict(origin.resource_counts).values()),
                expected_resource_hashes=dict(origin.resource_hashes),
                expected_resource_counts=dict(origin.resource_counts),
            )
        except AdmissionSealError:
            raise RetainedFHIRCaptureError("retained_fhir_capture_unavailable") from None
        require(
            seal.admission_version == 1
            and seal.admission_kind == "generic"
            and seal.proof_sha256 == origin.proof_sha256
            and seal.resource_types == tuple(dict(origin.resource_counts))
            and seal.metadata_summary.get("resource_hash_contract") == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            and seal.metadata_summary.get("semantic_projection_as_of") == origin.semantic_projection_as_of
        )
        _require_summary(seal.metadata_summary.get("source_summary_v1"), origin)
        _require_plan_summary(seal.metadata_summary, origin)
        _require_descriptor_counts(path, origin)


def _require_descriptor_counts(path, origin):
    """Keep the admitted profile's descriptor inventory exact with bounded streaming memory."""
    counts_by_family = dict.fromkeys(dict(origin.resource_counts), 0)
    source, field = _copy_field_reader(path)
    with source:
        for shard_counts in ijson_python.items(
            field, "provider_directory_content_proof_v1.shards.item.resource_counts"
        ):
            for kind, count in shard_counts.items():
                require(kind in counts_by_family and type(count) is int and count >= 0)
                counts_by_family[kind] += count
    require(tuple(sorted(counts_by_family.items())) == origin.descriptor_counts)


async def _origin_resource(reader, origin):
    """Retain exact parent/leaf routing and the Reader's native relation lock."""
    require(
        await reader.fetchval(
            f"SELECT endpoint_id FROM {_relation(origin.schema_name, 'provider_directory_source')} WHERE source_id=$1",
            origin.source_id,
        )
        == origin.endpoint_id
    )
    require(
        await reader.fetchval("SELECT to_regclass($1)::oid::bigint", _relation(origin.schema_name, TABLES[1]))
        == origin.resource_parent_oid
    )
    require(
        await reader.fetchval(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhparent=$1::oid AND inhrelid=$2::oid)",
            origin.resource_parent_oid,
            origin.resource_leaf_oid,
        )
        is True
    )
    require(
        await reader.fetchval(
            "SELECT p.partstrat='l' AND p.partnatts=1 AND a.attname='dataset_id' FROM pg_partitioned_table p "
            "JOIN pg_attribute a ON a.attrelid=p.partrelid AND a.attnum=p.partattrs[0] WHERE p.partrelid=$1::oid",
            origin.resource_parent_oid,
        )
        is True
    )
    actual_leaf = await reader.fetchval(
        f"SELECT tableoid::oid::bigint FROM {_relation(origin.schema_name, TABLES[1])} "
        'WHERE dataset_id=$1 ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT 1',
        origin.dataset_id,
    )
    require(actual_leaf == origin.resource_leaf_oid)
    require(
        await reader.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' AND relation=$1::oid AND mode='AccessShareLock' AND granted)",
            actual_leaf,
        )
        is True
    )


async def _origin_guard(reader, origin):
    """Retain the existing exact ALWAYS statement guard protecting published rows."""
    from process.network_fhir_source_custody import _REACHABLE_FUNCTIONS
    from process.provider_directory_cms_receipt_guard import _normalized

    guard = await reader.fetchrow(
        "SELECT p.prosrc,p.prosecdef,p.proconfig,l.lanname,p.provolatile::text,"
        "count(*) FILTER(WHERE t.tgenabled='A' AND NOT t.tgisinternal AND t.tgattr=''::int2vector "
        "AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgconstraint=0 AND t.tgparentid=0 "
        "AND ((t.tgtype=4 AND t.tgoldtable IS NULL AND t.tgnewtable='new_rows') "
        "OR(t.tgtype=16 AND t.tgoldtable='old_rows' AND t.tgnewtable='new_rows') "
        "OR(t.tgtype=8 AND t.tgoldtable='old_rows' AND t.tgnewtable IS NULL) "
        "OR(t.tgtype=34 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL))) AS guards "
        "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_language l ON l.oid=p.prolang WHERE t.tgrelid=$1::oid AND n.nspname=$2 "
        "AND p.proname='guard_tin_npi_connector_dataset_resource' "
        "GROUP BY p.prosrc,p.prosecdef,p.proconfig,l.lanname,p.provolatile",
        origin.resource_parent_oid,
        origin.schema_name,
    )
    require(
        guard
        and guard["guards"] == 4
        and guard["prosecdef"]
        and guard["proconfig"] in approved_cms_routine_settings(["search_path=pg_catalog"])
        and guard["lanname"] == "plpgsql"
        and guard["provolatile"] == "v"
    )
    normalized = _normalized(guard["prosrc"].replace(_identifier(origin.schema_name) + ".", '"__schema__".'))
    require(
        hashlib.sha256(normalized.encode()).hexdigest()
        == _REACHABLE_FUNCTIONS["guard_tin_npi_connector_dataset_resource"][0]
    )
    await _origin_candidate_guards(reader, origin)


async def _origin_candidate_guards(reader, origin):
    """Require existing canonical-copy parent authority and exact direct-leaf guards."""
    try:
        function_oid = await _candidate_parent_proof(reader, origin)
    except RuntimeError:
        raise RetainedFHIRCaptureError("retained_fhir_capture_unavailable") from None
    guards = await reader.fetch(
        "SELECT t.tgname,t.tgtype,t.tgenabled::text,t.tgargs,t.tgoldtable,t.tgnewtable,"
        "t.tgattr=''::int2vector AND t.tgqual IS NULL AND t.tgnargs=2 AND t.tgconstraint=0 "
        "AND t.tgparentid=0 AND NOT t.tgdeferrable AND NOT t.tginitdeferred AS unconditional "
        "FROM pg_trigger t WHERE t.tgrelid=$1::oid AND NOT t.tgisinternal AND "
        "(t.tgfoid=$2::oid OR t.tgtype & 1=1)",
        origin.resource_leaf_oid,
        function_oid,
    )
    expected_guards = {
        (
            "a_pd_candidate_" + operation + "_" + image.lower(),
            flag,
            (TABLES[1] + "\0" + image + "\0").encode(),
            "affected_rows" if image == "OLD" else None,
            "affected_rows" if image == "NEW" else None,
        )
        for operation, image, flag in (
            ("insert", "NEW", 4),
            ("update", "NEW", 16),
            ("update", "OLD", 16),
            ("delete", "OLD", 8),
        )
    }
    require(len(guards) == 4 and all(guard["tgenabled"] in {"O", "A"} and guard["unconditional"] for guard in guards))
    require(
        {tuple(guard[name] for name in ("tgname", "tgtype", "tgargs", "tgoldtable", "tgnewtable")) for guard in guards}
        == expected_guards
    )


async def _candidate_parent_proof(reader, origin):
    """Reuse exact native guard predicates without extra private-schema USAGE."""
    from db import migration_generic_source_candidates as guards
    from process.network_cms_registry_address_capture import NativeReceiptSession

    session = NativeReceiptSession(reader)
    signature = _relation(origin.schema_name, "guard_pd_dataset_candidate_parent") + "()"
    observed = (await session.execute(guards._COPY_GUARD_SNAPSHOT, {"signature": signature})).one_or_none()
    require(
        observed is not None
        and observed["body"]
        in (
            guards._canonical_copy_guard_bodies(origin.schema_name, origin.preparation_schema)[1],
            guards._cms_canonical_copy_guard_body(origin.schema_name, origin.preparation_schema),
        )
    )
    preparation_oid = await reader.fetchval(
        "SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname='preparation'",
        origin.preparation_schema,
    )
    require(type(preparation_oid) is int and preparation_oid > 0)
    reader_sql = str(guards._COPY_GUARD_READER)
    lookup = "to_regclass(:preparation)"
    require(reader_sql.count(lookup) == 1)
    # Only name resolution changes; the complete restricted privilege predicate stays exact.
    reader_sql = text(reader_sql.replace(lookup, "CAST(:preparation_oid AS oid)"))
    _, trigger_parameters = guards._canonical_copy_guard_input_parameters(
        origin.schema_name, origin.preparation_schema, observed["identity"]
    )
    guards._require_canonical_copy_guard_observations(
        await session.scalar(
            reader_sql, {"owner": int(observed["identity"]["proowner"]), "preparation_oid": preparation_oid}
        ),
        await session.execute(guards._COPY_GUARD_TRIGGERS, trigger_parameters),
    )
    return int(observed["identity"]["oid"])


async def _copied_header_bytes(connection, schema, origin):
    """Retain raw original metadata bytes in addition to canonical row content."""
    observed = await connection.fetchrow(
        "SELECT encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS sha,"
        "octet_length(publication_metadata_json::text)::bigint AS bytes "
        f"FROM {_relation(schema, TABLES[0])} WHERE dataset_id=$1",
        origin.dataset_id,
    )
    require(observed and (observed["sha"], observed["bytes"]) == (origin.metadata_text_sha256, origin.metadata_bytes))


async def _catalog(connection, schema, owner_role, runtime_roles):
    """Pin fixed heap identities and close every ordinary runtime write path."""
    await connection.execute(
        "LOCK TABLE " + ",".join(_relation(schema, table) for table in TABLES) + " IN ACCESS SHARE MODE NOWAIT"
    )
    relations = await _catalog_relations(connection, schema)
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    require(_protected_owner(owner) and tuple(relation["relname"] for relation in relations) == tuple(sorted(TABLES)))
    require(
        all(
            relation["relkind"] == b"r"
            and relation["relpersistence"] == b"p"
            and relation["relowner"] == relation["nspowner"] == owner["oid"]
            and not any(
                relation[key]
                for key in ("relhastriggers", "relrowsecurity", "relforcerowsecurity", "inherited", "defaults")
            )
            for relation in relations
        )
    )
    require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid) AND NOT EXISTS(SELECT FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class WHERE c.relnamespace=$1::oid)",
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
            "AND EXISTS(SELECT FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))",
            list(runtime_roles),
            owner["oid"],
        )
        is True
    )
    _require_indexes(relations, owner["oid"], schema)
    await _require_cursor_index(
        connection, dict((relation["relname"], relation["oid"]) for relation in relations)[TABLES[1]]
    )
    return (
        relations[0]["schema_oid"],
        tuple((relation["relname"], relation["oid"]) for relation in relations),
        digest(
            [
                {
                    name: catalog_value.decode("ascii") if type(catalog_value) is bytes else catalog_value
                    for name, catalog_value in relation.items()
                }
                for relation in relations
            ]
        ),
    )


def _require_indexes(relations, owner_oid, schema):
    """Require fixed primary/lookup index shapes and no extra constraints."""
    for relation in relations:
        indexes = json.loads(relation["indexes"] or "[]")
        profiles = [
            (_KEYS[relation["relname"]], True),
            *[(keys, False) for keys in _LOOKUPS.get(relation["relname"], ())],
        ]
        require(len(indexes) == len(profiles))
        for keys, primary in profiles:
            selected_indexes = [
                index for index in indexes if tuple(index["columns"]) == keys and index["primary"] is primary
            ]
            require(len(selected_indexes) == 1)
            index = selected_indexes[0]
            require(
                index["primary"] is primary
                and index["unique"] is primary
                and index["valid"]
                and index["ready"]
                and index["live"]
            )
            require(
                index["owner_oid"] == owner_oid
                and index["method"] == "btree"
                and index["key_count"] == index["attribute_count"] == len(keys)
                and index["options"] == " ".join("0" for _ in keys)
                and not index["expressions"]
                and not index["predicate"]
            )
            if relation["relname"] == TABLES[1] and not primary:
                require(index["name"] == "retained_lookup_1_0" and index["definition"] == _cursor_definition(schema))
        constraints = json.loads(relation["constraints"] or "[]")
        require(len(constraints) == 1 and constraints[0][1] == "p")


def _cursor_definition(schema):
    """Pin the target-only canonical-order index without changing copied columns."""
    return (
        "CREATE INDEX retained_lookup_1_0 ON " + schema + ".provider_directory_dataset_resource USING btree "
        '(dataset_id, resource_type COLLATE "C", resource_id COLLATE "C")'
    )


async def _require_cursor_index(connection, resource_oid):
    """Require exact native C collations and built-in text operator classes."""
    require(
        await connection.fetchval(
            "SELECT count(*)=1 AND bool_and(i.indcollation[0]=(SELECT a.attcollation FROM pg_attribute a "
            "WHERE a.attrelid=i.indrelid AND a.attname='dataset_id') "
            "AND i.indcollation[1]='pg_catalog.\"C\"'::regcollation "
            "AND i.indcollation[2]='pg_catalog.\"C\"'::regcollation "
            "AND NOT EXISTS(SELECT FROM unnest(i.indclass::oid[]) selected(oid) LEFT JOIN pg_opclass o ON o.oid=selected.oid "
            "WHERE o.oid IS NULL OR o.opcnamespace<>'pg_catalog'::regnamespace OR o.opcname<>'text_ops' "
            "OR o.opcintype<>'text'::regtype OR o.opckeytype<>0 OR o.opcmethod<>(SELECT oid FROM pg_am WHERE amname='btree'))) "
            "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=$1::oid AND c.relname='retained_lookup_1_0'",
            resource_oid,
        )
        is True
    )


async def _content_tables(connection, schema):
    async with _borrowed_epoch_settings(connection):
        await _set_content_settings(
            connection, [name for name, _ in _CONTENT_SETTINGS], [value for _, value in _CONTENT_SETTINGS]
        )
        return digest([(table, *(await _content(connection, schema, table))) for table in sorted(TABLES)])


async def _create(writer, reader, origin, schema, owner_role):
    columns = await _columns(reader, {table: _relation(origin.schema_name, table) for table in TABLES[:2]})
    await writer.execute(f"CREATE SCHEMA {_identifier(schema)} AUTHORIZATION {_identifier(owner_role)}")
    await writer.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    for table in TABLES:
        definition = columns[table] if table in columns else _DERIVED_DDL[table]
        await writer.execute(f"CREATE TABLE {_relation(schema, table)} ({definition})")


async def _resources(reader, origin, after, limit):
    predicate = "dataset_id=$1"
    arguments = (origin.dataset_id,)
    if after is not None:
        predicate += ' AND (resource_type COLLATE "C",resource_id COLLATE "C")>($2 COLLATE "C",$3 COLLATE "C")'
        arguments += after
    query = f'SELECT * FROM {_relation(origin.schema_name, TABLES[1])} WHERE {predicate} ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT {limit}'
    bounds = await reader.fetchrow(
        "WITH batch AS MATERIALIZED ("
        + query
        + ") SELECT count(*)::bigint AS count,coalesce(sum(octet_length(record_send(batch))),0)::bigint AS bytes FROM batch",
        *arguments,
    )
    return query, arguments, bounds


def _plan_evidence(source_id, release_id, resource_row, normalized_payload, observed_at):
    """Derive only source-local Organization links from an exact normalized plan."""
    identity = resource_row["resource_id"]
    references = normalized_payload.get("network_refs", [])
    require(
        type(references) is list
        and all(type(ref) is str and re.fullmatch(r"Organization/[A-Za-z0-9.-]{1,64}", ref) for ref in references)
    )
    for ref in sorted(set(references)):
        yield (
            source_id,
            release_id,
            "Organization",
            ref.split("/", 1)[1],
            identity,
            canonical(references),
            normalized_payload.get("owned_by_ref"),
            normalized_payload.get("administered_by_ref"),
            resource_row["payload_hash"],
            canonical(normalized_payload),
            observed_at,
        )


def _derived_records(origin, release_id, resource_rows, observed_at):
    """Stream exact derived rows without accumulating expanded plan references."""
    for resource_row in resource_rows:
        kind, identity = resource_row["resource_type"], resource_row["resource_id"]
        normalized_payload = (
            json.loads(resource_row["payload_json"])
            if type(resource_row["payload_json"]) is str
            else resource_row["payload_json"]
        )
        if kind in {"Organization", "Location"}:
            yield (
                TABLES[3],
                (
                    origin.source_id,
                    kind,
                    identity,
                    release_id,
                    resource_row["payload_hash"],
                    canonical(normalized_payload),
                    observed_at,
                ),
            )
        if kind == "Location":
            yield (
                TABLES[2],
                (
                    origin.source_id,
                    "Location",
                    identity,
                    None,
                    registry_location_id(origin.source_id, identity),
                    observed_at,
                ),
            )
        if kind == "InsurancePlan":
            for plan_evidence_row in _plan_evidence(
                origin.source_id, release_id, resource_row, normalized_payload, observed_at
            ):
                yield TABLES[4], plan_evidence_row


async def _copy_derived_batch(writer, schema, table, records, verify=None):
    """Require exact native COPY accounting for every bounded derived batch."""
    if verify is not None:
        await verify()
    copied = await drain_operation(
        writer.copy_records_to_table(table, schema_name=schema, records=records), preserve_cancellation=True
    )
    require(copied == f"COPY {len(records)}")


async def _derived(writer, schema, origin, release_id, resource_rows, observed_at, *, verify=None):
    """COPY expanded evidence within the existing per-batch row and byte bounds."""
    batches_by_table = {table: [] for table in TABLES[2:]}
    bytes_by_table = dict.fromkeys(batches_by_table, 0)
    for table, record in _derived_records(origin, release_id, resource_rows, observed_at):
        size = len(canonical([str(field_value) for field_value in record]).encode())
        require(size <= _DERIVED_BATCH_BYTES)
        if batches_by_table[table] and (
            len(batches_by_table[table]) == _DERIVED_BATCH_ROWS or bytes_by_table[table] + size > _DERIVED_BATCH_BYTES
        ):
            await _copy_derived_batch(writer, schema, table, batches_by_table[table], verify)
            batches_by_table[table], bytes_by_table[table] = [], 0
        batches_by_table[table].append(record)
        bytes_by_table[table] += size
    for table, records in batches_by_table.items():
        if records:
            await _copy_derived_batch(writer, schema, table, records, verify)


def _fold_rows(resource_rows, origin, hash_by_family, counts, root, total):
    """Use the original normalized payload and ordered dataset-root algorithms."""
    for resource_row in resource_rows:
        kind, identity, stored_hash = (
            resource_row["resource_type"],
            resource_row["resource_id"],
            resource_row["payload_hash"],
        )
        require(
            kind in hash_by_family
            and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", identity)
            and resource_row["acquired_resource_sha256"] is None
        )
        normalized_payload = (
            json.loads(resource_row["payload_json"])
            if type(resource_row["payload_json"]) is str
            else resource_row["payload_json"]
        )
        require(
            type(normalized_payload) is dict
            and normalized_payload.get("resource_id") == identity
            and normalized_payload.get("source_id", origin.source_id) == origin.source_id
        )
        require(
            resource_payload_sha256_for_contract(
                normalized_payload,
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
                if origin.admission_profile == SEMANTIC_PROFILE
                else LEGACY_RESOURCE_HASH_CONTRACT,
                resource_type=kind,
            )
            == stored_hash
        )
        encoded = json.dumps([kind, identity, stored_hash], sort_keys=True, separators=(",", ":")).encode()
        if total:
            root.update(b"\n")
        if counts[kind]:
            hash_by_family[kind].update(b"\n")
        root.update(encoded)
        hash_by_family[kind].update(encoded)
        total += 1
        counts[kind] += 1
    return total


def _require_roots(origin, hash_by_family, counts, root):
    require(counts == dict(origin.resource_counts) and root.hexdigest() == origin.dataset_sha256)
    require({kind: value.hexdigest() for kind, value in hash_by_family.items()} == dict(origin.resource_hashes))


def _native_payload_encoder(input_bytes):
    """Retry only explicit resource bounds; semantic failures remain terminal."""
    try:
        encoder = importlib.import_module("ptg2_address_canon").encode_retained_fhir_capture_batch
    except ImportError, AttributeError:
        require(False)
    try:
        return encoder(input_bytes)
    except ValueError as error:
        try:
            failure_by_field = json.loads(str(error))
        except ValueError, TypeError:
            failure_by_field = None
        code = failure_by_field.get("code") if type(failure_by_field) is dict else None
        if code in {"input_limit", "output_limit", "row_limit", "collection_limit"}:
            raise _CopyBatchTooLarge() from None
        require(False)


def _validate_reference_rows(resource_rows, origin):
    """Validate scratch commitments without advancing the accepted capture roots."""
    hash_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    _fold_rows(resource_rows, origin, hash_by_family, dict.fromkeys(hash_by_family, 0), hashlib.sha256(), 0)


def _prepare_native_resources(resource_rows, origin, release_id, observed_at, derive):
    """Reuse the unchanged reference only for rows outside exact native semantics."""
    context_by_field = {
        "source_id": origin.source_id,
        "release_id": release_id,
        "observed_at_pg_micros": postgres_micros(observed_at) if derive else 0,
        "hash_contract": SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        if origin.admission_profile == SEMANTIC_PROFILE
        else LEGACY_RESOURCE_HASH_CONTRACT,
        "resource_types": [kind for kind, _ in origin.resource_counts],
        "derive": derive,
    }
    try:
        return prepare_retained_fhir_batch(
            resource_rows,
            context_by_field,
            native_encoder=_native_payload_encoder,
            reference_validator=lambda rows: _validate_reference_rows(rows, origin),
            reference_records=lambda rows: _derived_records(origin, release_id, rows, observed_at),
            location_id=registry_location_id,
        )
    except RetainedFhirNativeError as error:
        if str(error) in {"input_limit", "output_limit", "row_limit"}:
            raise _CopyBatchTooLarge() from None
        require(False)


def _fold_prepared(prepared, hash_by_family, counts, root, total):
    """Append already validated bytes in original dataset and family order."""
    require(set(prepared.families) == set(hash_by_family))
    if prepared.row_count:
        if total:
            root.update(b"\n")
        root.update(prepared.root_bytes)
    for kind, (encoded, count) in prepared.families.items():
        if count:
            if counts[kind]:
                hash_by_family[kind].update(b"\n")
            hash_by_family[kind].update(encoded)
            counts[kind] += count
    return total + prepared.row_count


async def _copy_prepared(pair, schema, prepared):
    """Drain each bounded binary import and require exact native COPY accounting."""
    for table, batches in zip(TABLES[2:], prepared.copy_batches):
        for encoded, count in batches:
            await pair.verify()
            copied = await drain_operation(
                pair.writer.copy_to_table(table, schema_name=schema, source=memoryview(encoded), format="binary"),
                preserve_cancellation=True,
            )
            require(copied == f"COPY {count}")


async def _verify_resources(connection, schema, origin):
    """Revalidate every copied payload/root independently after closing target writers."""
    hash_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    counts, root, total, after, limit = dict.fromkeys(hash_by_family, 0), hashlib.sha256(), 0, None, 1000
    while True:
        query, arguments, bounds = await _resources(connection, replace(origin, schema_name=schema), after, limit)
        if bounds["count"] == 0:
            break
        if bounds["bytes"] + 21 > 64 * 1024**2:
            require(limit > 1)
            limit = max(1, limit // 2)
            continue
        rows = await connection.fetch(query, *arguments)
        require(len(rows) == bounds["count"] and all(row["dataset_id"] == origin.dataset_id for row in rows))
        try:
            prepared = await drain_operation(
                asyncio.to_thread(_prepare_native_resources, rows, origin, "retained-verification", None, False),
                preserve_cancellation=True,
            )
        except _CopyBatchTooLarge:
            if len(rows) > 1:
                limit = max(1, len(rows) // 2)
                continue
            total = _fold_rows(rows, origin, hash_by_family, counts, root, total)
        else:
            total = _fold_prepared(prepared, hash_by_family, counts, root, total)
        after = rows[-1]["resource_type"], rows[-1]["resource_id"]
    _require_roots(origin, hash_by_family, counts, root)


async def _copy_resources(pair, origin, schema, release_id):
    hash_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    counts = dict.fromkeys(hash_by_family, 0)
    root = hashlib.sha256()
    after, limit, total = None, 1000, 0
    observed_at = await pair.writer.fetchval("SELECT transaction_timestamp()")
    while True:
        await pair.verify()
        query, arguments, bounds = await _resources(pair.reader, origin, after, limit)
        if bounds["count"] == 0:
            break
        if bounds["bytes"] + 21 > 64 * 1024**2:
            require(limit > 1)
            limit = max(1, limit // 2)
            continue
        resource_rows = await pair.reader.fetch(query, *arguments)
        require(
            len(resource_rows) == bounds["count"]
            and all(resource_row["dataset_id"] == origin.dataset_id for resource_row in resource_rows)
        )
        try:
            prepared = await drain_operation(
                asyncio.to_thread(_prepare_native_resources, resource_rows, origin, release_id, observed_at, True),
                preserve_cancellation=True,
            )
        except _CopyBatchTooLarge:
            if len(resource_rows) > 1:
                limit = max(1, len(resource_rows) // 2)
                continue
            await drain_operation(
                asyncio.to_thread(_validate_reference_rows, resource_rows, origin), preserve_cancellation=True
            )
            prepared = None
        try:
            await _copy_native_batch(pair, query, arguments, schema, TABLES[1], len(resource_rows))
        except _CopyBatchTooLarge:
            require(len(resource_rows) > 1)
            limit = max(1, len(resource_rows) // 2)
            continue
        if prepared is None:
            await _derived(pair.writer, schema, origin, release_id, resource_rows, observed_at, verify=pair.verify)
            total = _fold_rows(resource_rows, origin, hash_by_family, counts, root, total)
        else:
            await _copy_prepared(pair, schema, prepared)
            total = _fold_prepared(prepared, hash_by_family, counts, root, total)
        after = resource_rows[-1]["resource_type"], resource_rows[-1]["resource_id"]
    _require_roots(origin, hash_by_family, counts, root)


async def _prepare_capture_tables(pair, origin, schema, capture_id, owner_role):
    """COPY bounded retained rows, derive exact evidence, then validate references."""
    writer = pair.writer
    await _create(writer, pair.reader, origin, schema, owner_role)
    await _copy_native_batch(
        pair,
        f"SELECT * FROM {_relation(origin.schema_name, TABLES[0])} WHERE dataset_id=$1",
        (origin.dataset_id,),
        schema,
        TABLES[0],
        1,
    )
    await _copy_resources(pair, origin, schema, "registry-capture:" + str(capture_id))
    for table in TABLES:
        await writer.execute(
            f"ALTER TABLE {_relation(schema, table)} ADD PRIMARY KEY ({','.join(_identifier(key) for key in _KEYS[table])})"
        )
        for number, keys in enumerate(_LOOKUPS.get(table, ())):
            await writer.execute(
                f"CREATE INDEX {_identifier('retained_lookup_' + str(TABLES.index(table)) + '_' + str(number))} ON {_relation(schema, table)} ({','.join(_lookup_key(table, key) for key in keys)})"
            )
        await writer.execute(f"ANALYZE {_relation(schema, table)}")
    # References are checked after all bounded writes, without per-record hooks or FKs.
    if origin.admission_profile == SEMANTIC_PROFILE:
        await _require_modern_plan_evidence(writer, schema, origin, "registry-capture:" + str(capture_id))
    else:
        require(
            await writer.fetchval(
                f"SELECT NOT EXISTS(SELECT FROM {_relation(schema, TABLES[4])} p LEFT JOIN {_relation(schema, TABLES[3])} e ON e.source_id=p.source_id AND e.release_id=p.release_id AND e.resource_type='Organization' AND e.resource_id=p.network_resource_id WHERE e.resource_id IS NULL)"
            )
            is True
        )


async def _require_modern_plan_evidence(writer, schema, origin, release_id):
    """Keep every admitted edge, including accounted missing Organizations, without inventing evidence."""
    resources, plans = _relation(schema, TABLES[1]), _relation(schema, TABLES[4])
    require(
        await writer.fetchval(
            "WITH expected AS (SELECT DISTINCT $2::varchar AS source_id,$3::varchar AS release_id,"
            "'Organization'::varchar AS network_resource_type,split_part(ref.value,'/',2)::varchar AS network_resource_id,"
            "r.resource_id AS insurance_plan_resource_id,r.payload_json::jsonb->'network_refs' AS network_refs,"
            "r.payload_json::jsonb->>'owned_by_ref' AS owned_by_ref,r.payload_json::jsonb->>'administered_by_ref' AS administered_by_ref,"
            "r.payload_hash AS plan_payload_sha256,r.payload_json::jsonb AS plan_payload_json "
            f"FROM {resources} r CROSS JOIN LATERAL jsonb_array_elements_text(r.payload_json::jsonb->'network_refs') ref(value) "
            "WHERE r.dataset_id=$1 AND r.resource_type='InsurancePlan'),observed AS ("
            "SELECT source_id,release_id,network_resource_type,network_resource_id,insurance_plan_resource_id,network_refs,"
            f"owned_by_ref,administered_by_ref,plan_payload_sha256,plan_payload_json FROM {plans}) "
            "SELECT NOT EXISTS((TABLE expected EXCEPT TABLE observed) UNION ALL (TABLE observed EXCEPT TABLE expected))",
            origin.dataset_id,
            origin.source_id,
            release_id,
        )
        is True
    )
    missing = await writer.fetchval(
        f"SELECT count(*)::bigint FROM {plans} p LEFT JOIN {resources} r ON r.dataset_id=$1 "
        "AND r.resource_type='Organization' AND r.resource_id=p.network_resource_id WHERE r.resource_id IS NULL",
        origin.dataset_id,
    )
    require(missing == origin.unresolved_plan_network_count)


def _lookup_key(table, key):
    """Use C only in the private resource lookup; preserve the original PK/columns."""
    return _identifier(key) + (' COLLATE "C"' if table == TABLES[1] and key in {"resource_type", "resource_id"} else "")


async def _seal_capture(writer, origin, schema, capture_id, owner_role, runtime_roles):
    """Close writers before verifying copied content and recording native custody."""
    relations = await _catalog_relations(writer, schema)
    catalog_by_field = {
        "namespace": {"oid": relations[0]["schema_oid"], "nspowner": relations[0]["nspowner"]},
        "relations": [dict(relation) for relation in relations],
        "heaps": {relation["relname"]: relation["oid"] for relation in relations},
    }
    await _transfer_and_grant(
        writer,
        SimpleNamespace(schema_name=schema),
        {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)},
        catalog_by_field,
    )
    await writer.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    schema_oid, relation_oids, catalog_sha = await _catalog(writer, schema, owner_role, runtime_roles)
    await _verify_resources(writer, schema, origin)
    await _copied_header_bytes(writer, schema, origin)
    capture = RetainedFHIRCapture(
        origin,
        str(capture_id),
        schema,
        schema_oid,
        relation_oids,
        owner_role,
        runtime_roles,
        catalog_sha,
        await _content_tables(writer, schema),
        "0" * 64,
    )
    capture = validate_capture(replace(capture, capture_sha256=capture_digest(capture)))
    await writer.execute(
        "COMMENT ON SCHEMA "
        + _identifier(schema)
        + " IS '"
        + ("retained-normalized-fhir-v1:" + canonical(capture.as_dict())).replace("'", "''")
        + "'"
    )
    return capture


async def capture_retained_fhir_source(
    reader, writer, origin, *, capture_id, owner_role, runtime_roles, source_policy=None
):
    """Freeze a genuine Reader's complete supported snapshot in a distinct registry domain."""
    require(type(origin) is RetainedFHIRInput and type(capture_id) is UUID and capture_id.int)
    require(
        reader is not writer and type(runtime_roles) is tuple and tuple(sorted(set(runtime_roles))) == runtime_roles
    )
    await _reader_identity(reader, origin)
    await _require_transaction(writer)
    actor = await writer.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=session_user"
    )
    require(
        actor
        and actor["login"] == actor["actor"]
        and actor["rolcanlogin"]
        and not any(
            actor[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
    )
    await _check_roles(writer, {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)})
    await _same_database(writer, reader)
    binding = await _origin_header(reader, origin)
    if binding is None:
        await _origin_guard(reader, origin)
        source_family = None
    else:
        source_family = await require_native_generic_snapshot(reader, writer, origin, source_policy, binding)
    schema = "registry_fhir_capture_" + capture_id.hex
    original_role = await writer.fetchval(
        "SELECT CASE WHEN current_setting('role')='none' THEN 'NONE' ELSE quote_ident(current_user) END"
    )
    pair = _ReaderCopy(reader, writer)
    pair.virtual_xid = await _virtual_xid(reader)
    try:
        async with writer.transaction():
            require(await writer.fetchval("SELECT to_regnamespace($1)", schema) is None)
            await _prepare_capture_tables(pair, origin, schema, capture_id, owner_role)
            await writer.execute(f"SET LOCAL ROLE {original_role}")
            capture = await _seal_capture(writer, origin, schema, capture_id, owner_role, runtime_roles)
            await pair.verify()
            if binding is not None:
                await writer.execute(f"SET LOCAL ROLE {original_role}")
                require(
                    await require_native_generic_snapshot(reader, writer, origin, source_policy, binding)
                    == source_family
                )
            return capture
    finally:
        if writer.is_in_transaction():
            await writer.execute(f"SET LOCAL ROLE {original_role}")


async def require_retained_fhir_capture(connection, capture, *, verify_content=True):
    """Require authentic closed native custody, independent of a later source head."""
    capture = validate_capture(capture)
    await _require_transaction(connection)
    require(type(verify_content) is bool)
    observed = await _catalog(connection, capture.schema_name, capture.owner_role, capture.runtime_roles)
    require(observed == (capture.schema_oid, capture.relation_oids, capture.catalog_sha256))
    comment = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", capture.schema_oid)
    require(comment == "retained-normalized-fhir-v1:" + canonical(capture.as_dict()))
    if verify_content:
        await _copied_header_bytes(connection, capture.schema_name, capture.origin)
        require(await _content_tables(connection, capture.schema_name) == capture.content_sha256)
    return capture
