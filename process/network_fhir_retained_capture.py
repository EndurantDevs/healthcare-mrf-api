# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture already published normalized FHIR without inventing ingest authority."""

import asyncio
import hashlib
import importlib
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

from ijson.backends import python as ijson_python

from process.network_address_projection import _identifier
from process.network_bootstrap_sources import (
    _COPY_BATCH_BYTES,
    _content,
    _copy_native_batch,
    _copy_native_spool,
    _CopyBatchTooLarge,
)
from process.network_custom_address_source import _require_transaction
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
    retained_metadata_max_bytes,
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
    _set_content_settings,
)
from process.network_fhir_source_epoch_pin import _columns
from process.network_generic_source_published_snapshot import published_generic_binding, require_native_generic_snapshot
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    validate_generic_admission_copy,
)
from process.provider_directory_admission_stream import _copy_field_reader
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    resource_content_hash_payload,
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
_DERIVED_BATCH_ROWS = 5000
_DERIVED_BATCH_BYTES = 64 * 1024**2


@dataclass(frozen=True)
class RetainedFHIRRead:
    """Named retained data selected for a bounded read operation."""

    capture: RetainedFHIRCapture

    def require_ready(self, capture=None):
        """Validate the selected logical capture and any explicitly requested edition."""
        selected = validate_capture(self.capture)
        if capture is not None:
            require(selected == validate_capture(capture))


async def pin_retained_fhir_read(connection, capture):
    """Validate copied metadata bytes and complete content before reading retained rows."""
    return RetainedFHIRRead(await require_retained_fhir_capture(connection, capture))


def _relation(schema, table):
    return _identifier(schema) + "." + _identifier(table)


class _ReaderCopy:
    """Export only the original Reader; import only the caller's target savepoint."""

    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def verify(self):
        """Require both caller-owned transactions to remain open during bounded COPY."""
        require(self.reader.is_in_transaction() and self.writer.is_in_transaction())

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


async def _origin_header(reader, origin):
    """Bind original bytes and published coordinates, never fill a missing admission seal."""
    header = _relation(origin.header_relation["schema"], origin.header_relation["name"])
    header = await reader.fetchrow(
        "SELECT endpoint_id,acquisition_root_run_id,dataset_hash,status,published_at,is_current,resource_count,"
        "to_jsonb(d)->>'previous_dataset_id' AS previous_dataset_id,"
        "to_jsonb(d)->>'completion_proof_sha256' AS completion_proof_sha256,"
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
        f"FROM {header} d WHERE dataset_id=$1",
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
            header["endpoint_id"],
            header["acquisition_root_run_id"],
            header["dataset_hash"],
            header["resource_count"],
        )
        == (
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
        schema_name=origin.header_relation["schema"],
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


@asynccontextmanager
async def _publication_metadata_copy(reader, origin):
    """Yield the bounded original metadata only after its exact COPY bytes authenticate."""
    with TemporaryDirectory(prefix="retained-fhir-metadata-") as directory:
        path = Path(directory) / "metadata.copy"
        with path.open("wb") as output:

            async def bounded_write(block):
                """Bound every native chunk before writing task-local metadata bytes."""
                require(output.tell() + len(block) <= origin.metadata_bytes + 27)
                output.write(block)

            status = await _reader_export(
                reader,
                f"SELECT publication_metadata_json::text FROM {_relation(origin.header_relation['schema'], origin.header_relation['name'])} WHERE dataset_id=$1",
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
        yield path


async def _validate_admitted_proof(reader, origin):
    """Run the existing complete raw-COPY validator; never backfill or rewrite Source seals."""
    async with _publication_metadata_copy(reader, origin) as path:
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
            f"SELECT endpoint_id FROM {_relation(origin.header_relation['schema'], 'provider_directory_source')} WHERE source_id=$1",
            origin.source_id,
        )
        == origin.endpoint_id
    )


async def _copied_header_bytes(connection, schema, origin):
    """Retain raw original metadata bytes in addition to canonical row content."""
    observed = await connection.fetchrow(
        "SELECT encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS sha,"
        "octet_length(publication_metadata_json::text)::bigint AS bytes "
        f"FROM {_relation(schema, TABLES[0])} WHERE dataset_id=$1",
        origin.dataset_id,
    )
    require(observed and (observed["sha"], observed["bytes"]) == (origin.metadata_text_sha256, origin.metadata_bytes))


async def _content_tables(connection, schema):
    async with _borrowed_epoch_settings(connection):
        await _set_content_settings(
            connection, [name for name, _ in _CONTENT_SETTINGS], [value for _, value in _CONTENT_SETTINGS]
        )
        return digest([(table, *(await _content(connection, schema, table))) for table in sorted(TABLES)])


async def _create(writer, reader, origin, schema):
    columns = await _columns(
        reader,
        {
            TABLES[0]: _relation(origin.header_relation["schema"], origin.header_relation["name"]),
            TABLES[1]: _relation(origin.resource_relation["schema"], origin.resource_relation["name"]),
        },
    )
    await writer.execute(f"CREATE SCHEMA {_identifier(schema)}")
    for table in TABLES:
        definition = columns[table] if table in columns else _DERIVED_DDL[table]
        await writer.execute(f"CREATE TABLE {_relation(schema, table)} ({definition})")


async def _resources(reader, origin, after, limit):
    predicate = "dataset_id=$1"
    arguments = (origin.dataset_id,)
    if after is not None:
        predicate += ' AND (resource_type COLLATE "C",resource_id COLLATE "C")>($2 COLLATE "C",$3 COLLATE "C")'
        arguments += after
    query = f'SELECT * FROM {_relation(origin.resource_relation["schema"], origin.resource_relation["name"])} WHERE {predicate} ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT {limit}'
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


def _resource_hash_contract(origin):
    """Choose only declared supported profiles; unknown profiles cannot fall back to legacy hashing."""
    from process.network_fhir_source_witness_contract import HASH_CONTRACTS

    if origin.admission_profile in HASH_CONTRACTS:
        return HASH_CONTRACTS[origin.admission_profile]
    require(origin.admission_profile == LEGACY_PROFILE)
    return LEGACY_RESOURCE_HASH_CONTRACT


def _fold_rows(resource_rows, origin, hash_by_family, counts, root, total):
    """Use the original normalized payload and ordered dataset-root algorithms."""
    from process.network_fhir_source_witness_contract import is_completion_v3_origin, source_resource_marker_valid

    hash_contract = _resource_hash_contract(origin)
    is_completion_v3 = is_completion_v3_origin(origin)
    for resource_row in resource_rows:
        kind, identity, stored_hash = (
            resource_row["resource_type"],
            resource_row["resource_id"],
            resource_row["payload_hash"],
        )
        require(
            kind in hash_by_family
            and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", identity)
            and source_resource_marker_valid(resource_row["acquired_resource_sha256"], completion_v3=is_completion_v3)
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
            (
                canonical_payload_sha256(resource_content_hash_payload(normalized_payload))
                if is_completion_v3
                else resource_payload_sha256_for_contract(normalized_payload, hash_contract, resource_type=kind)
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
    from process.network_fhir_source_witness_contract import is_completion_v3_origin

    context_by_field = {
        "source_id": origin.source_id,
        "release_id": release_id,
        "observed_at_pg_micros": postgres_micros(observed_at) if derive else 0,
        "hash_contract": _resource_hash_contract(origin),
        "resource_types": [kind for kind, _ in origin.resource_counts],
        "derive": derive,
    }
    if is_completion_v3_origin(origin):
        context_by_field["completion_required_version"] = origin.completion_required_version
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


async def _copy_prepared(writer, schema, prepared):
    """Drain each bounded binary import and require exact native COPY accounting."""
    for table, batches in zip(TABLES[2:], prepared.copy_batches):
        for encoded, count in batches:
            copied = await drain_operation(
                writer.copy_to_table(table, schema_name=schema, source=memoryview(encoded), format="binary"),
                preserve_cancellation=True,
            )
            require(copied == f"COPY {count}")


async def _verify_resources(connection, schema, origin):
    """Revalidate every copied payload/root independently after closing target writers."""
    hash_by_family = {kind: hashlib.sha256() for kind, _ in origin.resource_counts}
    counts, root, total, after, limit = dict.fromkeys(hash_by_family, 0), hashlib.sha256(), 0, None, 1000
    while True:
        local_origin = replace(
            origin,
            schema_name=schema,
            resource_relation={"schema": schema, "name": TABLES[1]},
        )
        query, arguments, bounds = await _resources(connection, local_origin, after, limit)
        if bounds["count"] == 0:
            break
        if bounds["bytes"] + 21 > 64 * 1024**2:
            require(limit > 1)
            limit = max(1, limit // 2)
            continue
        resource_rows = await connection.fetch(query, *arguments)
        require(
            len(resource_rows) == bounds["count"]
            and all(resource["dataset_id"] == origin.dataset_id for resource in resource_rows)
        )
        try:
            prepared = await drain_operation(
                asyncio.to_thread(
                    _prepare_native_resources, resource_rows, origin, "retained-verification", None, False
                ),
                preserve_cancellation=True,
            )
        except _CopyBatchTooLarge:
            if len(resource_rows) > 1:
                limit = max(1, len(resource_rows) // 2)
                continue
            total = _fold_rows(resource_rows, origin, hash_by_family, counts, root, total)
        else:
            total = _fold_prepared(prepared, hash_by_family, counts, root, total)
        after = resource_rows[-1]["resource_type"], resource_rows[-1]["resource_id"]
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
            await _copy_prepared(pair.writer, schema, prepared)
            total = _fold_prepared(prepared, hash_by_family, counts, root, total)
        after = resource_rows[-1]["resource_type"], resource_rows[-1]["resource_id"]
    _require_roots(origin, hash_by_family, counts, root)


async def _copy_capture_header(pair, origin, schema):
    """Keep raw admitted metadata separate from the unchanged native row budget."""
    require(type(origin) is RetainedFHIRInput)
    require(
        type(origin.metadata_bytes) is int
        and 0 < origin.metadata_bytes <= retained_metadata_max_bytes(origin.admission_profile)
    )
    arguments = (
        pair,
        f"SELECT * FROM {_relation(origin.header_relation['schema'], origin.header_relation['name'])} WHERE dataset_id=$1",
        (origin.dataset_id,),
        schema,
        TABLES[0],
        1,
    )
    if origin.admission_profile == SEMANTIC_PROFILE:
        await _copy_native_spool(*arguments, byte_limit=origin.metadata_bytes + _COPY_BATCH_BYTES)
    else:
        await _copy_native_batch(*arguments)


async def _prepare_capture_tables(pair, origin, schema, capture_id):
    """COPY bounded retained rows, derive exact evidence, then validate references."""
    writer = pair.writer
    await _create(writer, pair.reader, origin, schema)
    await _copy_capture_header(pair, origin, schema)
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
            f"""
WITH expected_keys AS MATERIALIZED (
  SELECT DISTINCT r.resource_id AS plan_id, split_part(ref.value,'/',2) AS network_id
  FROM {resources} r
  CROSS JOIN LATERAL jsonb_array_elements_text(r.payload_json::jsonb->'network_refs') ref(value)
  WHERE r.dataset_id=$1 AND r.resource_type='InsurancePlan'
)
SELECT NOT EXISTS (
  SELECT FROM expected_keys e
  LEFT JOIN {plans} p ON p.source_id=$2 AND p.release_id=$3
    AND p.network_resource_id=e.network_id AND p.insurance_plan_resource_id=e.plan_id
  WHERE p.source_id IS NULL
) AND NOT EXISTS (
  SELECT FROM {plans} p
  LEFT JOIN {resources} r ON r.dataset_id=$1 AND r.resource_type='InsurancePlan'
    AND r.resource_id=p.insurance_plan_resource_id
  WHERE r.resource_id IS NULL OR p.source_id<>$2 OR p.release_id<>$3
    OR p.network_resource_type<>'Organization'
    OR NOT EXISTS (
      SELECT FROM jsonb_array_elements_text(r.payload_json::jsonb->'network_refs') ref(value)
      WHERE split_part(ref.value,'/',2)=p.network_resource_id
    )
    OR p.network_refs IS DISTINCT FROM r.payload_json::jsonb->'network_refs'
    OR p.owned_by_ref IS DISTINCT FROM r.payload_json::jsonb->>'owned_by_ref'
    OR p.administered_by_ref IS DISTINCT FROM r.payload_json::jsonb->>'administered_by_ref'
    OR p.plan_payload_sha256 IS DISTINCT FROM r.payload_hash
    OR p.plan_payload_json IS DISTINCT FROM r.payload_json::jsonb
)
""",
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
    """Freeze through the existing administrator connection and validate the complete copied data."""
    await freeze_fhir_source_snapshot(
        writer, schema, namespace_owner=owner_role, reader_roles=runtime_roles, application_roles=runtime_roles
    )
    await _verify_resources(writer, schema, origin)
    await _copied_header_bytes(writer, schema, origin)
    capture = RetainedFHIRCapture(
        origin=origin,
        capture_id=str(capture_id),
        schema_name=schema,
        tables={name: {"schema": schema, "name": name} for name in TABLES},
        content_sha256=await _content_tables(writer, schema),
        capture_sha256="0" * 64,
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
    """Copy a complete named edition through the existing administrator preparation connection."""
    require(type(origin) is RetainedFHIRInput and type(capture_id) is UUID and capture_id.int)
    origin.__post_init__()
    require(type(runtime_roles) is tuple and bool(runtime_roles) and tuple(sorted(set(runtime_roles))) == runtime_roles)
    _identifier(owner_role)
    require(owner_role not in runtime_roles)
    for role in runtime_roles:
        _identifier(role)
    await _require_transaction(reader)
    await _require_transaction(writer)
    binding = await _origin_header(reader, origin)
    source_family = None
    if binding is not None:
        source_family = await require_native_generic_snapshot(reader, writer, origin, source_policy, binding)
    schema = "registry_fhir_capture_" + capture_id.hex
    pair = _ReaderCopy(reader, writer)
    async with writer.transaction():
        require(await writer.fetchval("SELECT to_regnamespace($1)", schema) is None)
        await _prepare_capture_tables(pair, origin, schema, capture_id)
        capture = await _seal_capture(writer, origin, schema, capture_id, owner_role, runtime_roles)
        await pair.verify()
        if binding is not None:
            require(
                await require_native_generic_snapshot(reader, writer, origin, source_policy, binding) == source_family
            )
        return capture


async def require_retained_fhir_capture(connection, capture, *, verify_content=True):
    """Validate the selected named edition and its complete content accounting."""
    capture = validate_capture(capture)
    await _require_transaction(connection)
    require(type(verify_content) is bool)
    if verify_content:
        await _copied_header_bytes(connection, capture.schema_name, capture.origin)
        require(await _content_tables(connection, capture.schema_name) == capture.content_sha256)
    return capture
