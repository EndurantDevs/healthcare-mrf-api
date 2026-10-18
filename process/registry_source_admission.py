# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Land a complete bounded CMS edition with native binary COPY and set SQL."""

import asyncio
import csv
import hashlib
import importlib
import io
import json
import os
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from uuid import UUID, uuid4

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_membership_copy import _COPY_HEADER, MAX_COPY_BYTES, MAX_INPUT_BYTES, MAX_ROWS
from process.uhc_flex_practitioner_async_safety import drain_operation

_COPY_COLUMNS = ("snapshot_id", "source_record_key", "source_row_number", "status", "observation_json", "issues_json")


class RegistrySourceAdmissionError(ValueError):
    """Rejected editions leave existing source and custom data intact."""


@dataclass(frozen=True)
class RegistrySourceEdition:
    snapshot_id: UUID
    source_system: str
    source_id: str
    edition_id: str
    source_url: str
    artifact_sha256: str
    input_sha256: str
    parser_version: str
    reporting_year: int
    published_at: datetime | None
    retrieved_at: datetime | None = field(default=None, compare=False)

    def __post_init__(self):
        if type(self.snapshot_id) is not UUID or not self.snapshot_id.int:
            raise RegistrySourceAdmissionError("Source snapshot identity is invalid")
        for field, limit in (
            ("source_system", 64),
            ("source_id", 128),
            ("edition_id", 128),
            ("source_url", 2048),
            ("parser_version", 128),
        ):
            field_value = getattr(self, field)
            if (
                type(field_value) is not str
                or not field_value.strip()
                or len(field_value) > limit
                or any(not character.isprintable() for character in field_value)
            ):
                raise RegistrySourceAdmissionError("Source edition metadata is invalid")
        if (self.source_system, self.source_id) not in {
            ("cms", "commercial-mlr"),
            ("cms", "plan-finder"),
            ("naic", "company-listings"),
        }:
            raise RegistrySourceAdmissionError("Admission requires its explicit source identity")
        if self.source_system == "naic" and (
            self.reporting_year != 2026 or self.parser_version != "naic-company-listings-byte-projection-v1"
        ):
            raise RegistrySourceAdmissionError("Company listing edition is unsupported")
        if type(self.reporting_year) is not int or not 2010 <= self.reporting_year <= 2100:
            raise RegistrySourceAdmissionError("Source reporting year is invalid")
        if self.published_at is not None and (
            type(self.published_at) is not datetime or self.published_at.utcoffset() is None
        ):
            raise RegistrySourceAdmissionError("Source publication date requires an explicit timezone")
        if self.retrieved_at is not None and (
            type(self.retrieved_at) is not datetime or self.retrieved_at.utcoffset() is None
        ):
            raise RegistrySourceAdmissionError("Source retrieval date requires an explicit timezone")
        for digest in (self.artifact_sha256, self.input_sha256):
            if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise RegistrySourceAdmissionError("Source digest is invalid")


def _encode_edition(input_bytes, edition):
    try:
        module = importlib.import_module("ptg2_address_canon")
        encoder = (
            module.encode_cms_planfinder_observations
            if edition.source_id == "plan-finder"
            else module.encode_cms_mlr_observations
        )
    except ImportError, AttributeError:
        raise RegistrySourceAdmissionError("Native CMS encoder is unavailable") from None
    metadata = {
        "snapshot_id": str(edition.snapshot_id),
        "reporting_year": edition.reporting_year,
        "input_sha256": edition.input_sha256,
    }
    copy_bytes, row_count, encoded_metadata = encoder(input_bytes, json.dumps(metadata, separators=(",", ":")).encode())
    if (
        type(copy_bytes) is not bytes
        or not 21 <= len(copy_bytes) <= MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
        or type(row_count) is not int
        or not 1 <= row_count <= MAX_ROWS
        or type(encoded_metadata) is not bytes
        or len(encoded_metadata) > MAX_COPY_BYTES
    ):
        raise RegistrySourceAdmissionError("Native CMS batch framing or bounds are invalid")
    result_by_name = json.loads(encoded_metadata)
    if (
        type(result_by_name) is not dict
        or set(result_by_name) != {"edition", "counts", "conflicts"}
        or result_by_name["edition"] != metadata
    ):
        raise RegistrySourceAdmissionError("Native CMS batch edition does not match")
    return copy_bytes, row_count, result_by_name


async def _register_edition(connection, namespace, edition):
    metadata_by_name = asdict(edition)
    retrieved_at = metadata_by_name.pop("retrieved_at")
    # A later fetch is not a new edition. Retain only the first supplied time;
    # legacy callers with unknown fetch time keep the existing database default.
    insert_by_name = dict(metadata_by_name)
    if retrieved_at is not None:
        insert_by_name["retrieved_at"] = retrieved_at
    insert_columns = ",".join(insert_by_name)
    fields = tuple(metadata_by_name)
    columns = ",".join(fields)
    placeholders = ",".join(f"${index}" for index in range(1, len(insert_by_name) + 1))
    await connection.execute(
        f"INSERT INTO {namespace}.registry_source_snapshot ({insert_columns}) VALUES({placeholders}) ON CONFLICT(snapshot_id) DO NOTHING",
        *insert_by_name.values(),
    )
    snapshot = await connection.fetchrow(
        f"SELECT {columns} FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1 FOR UPDATE",
        edition.snapshot_id,
    )
    if snapshot is None or any(snapshot[field] != metadata_by_name[field] for field in fields):
        raise RegistrySourceAdmissionError("Source edition metadata is immutable")


async def _load_edition_landing(connection, namespace, input_bytes, edition):
    """COPY one native batch into a connection-private landing with exact accounting."""
    from process.registry_source_observation_store import RegistryObservationLanding

    copy_bytes, row_count, metadata = await drain_operation(
        asyncio.to_thread(_encode_edition, input_bytes, edition), preserve_cancellation=True
    )
    table_name = "registry_landing_" + uuid4().hex
    await connection.execute(
        f'CREATE TEMP TABLE "{table_name}" (LIKE {namespace}.registry_source_observation INCLUDING ALL) ON COMMIT DROP'
    )
    with io.BytesIO(copy_bytes) as copy_source:
        status = await connection.copy_to_table(table_name, columns=_COPY_COLUMNS, format="binary", source=copy_source)
        if status != f"COPY {row_count}" or copy_source.tell() != len(copy_bytes):
            raise RegistrySourceAdmissionError("CMS COPY accounting is incomplete")
    relation = await connection.fetchrow(
        "SELECT c.oid,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relnamespace=pg_my_temp_schema() AND c.relname=$1",
        table_name,
    )
    landing = RegistryObservationLanding(
        relation["nspname"],
        table_name,
        relation["oid"],
        edition.snapshot_id,
        edition.source_system,
        edition.source_id,
        edition.edition_id,
        edition.input_sha256,
        edition.parser_version,
        row_count,
    )
    return landing, copy_bytes, metadata


async def admit_cms_mlr_edition(connection, input_bytes, edition, *, control_schema=None):
    """Use one caller transaction for the edition, landing and durable observations.

    This is trusted operator orchestration. The caller owns source admission and
    permission checks; the temporary relation is private to this connection and
    receives exactly one complete native batch before set validation. Source
    imports can create strongly identified organizations; existing manual heads
    and approved serving revisions remain unchanged.
    """
    if (
        type(edition) is not RegistrySourceEdition
        or edition.source_id != "commercial-mlr"
        or type(input_bytes) is not bytes
        or len(input_bytes) > MAX_INPUT_BYTES
    ):
        raise RegistrySourceAdmissionError("A bounded source edition is required")
    if not connection.is_in_transaction():
        raise RegistrySourceAdmissionError("Source admission requires a caller-owned transaction")
    if hashlib.sha256(input_bytes).hexdigest() != edition.input_sha256:
        raise RegistrySourceAdmissionError("Source input digest does not match")
    from process.registry_identity_materialization import materialize_registry_source_identities
    from process.registry_source_admission_receipt import seal_registry_source_admission
    from process.registry_source_observation_store import (
        persist_registry_source_observations,
    )

    schema = control_schema or registry_schema()
    namespace = _identifier(schema)
    async with connection.transaction():
        original_search_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        await _register_edition(connection, namespace, edition)
        landing, copy_bytes, metadata = await _load_edition_landing(connection, namespace, input_bytes, edition)
        identities = await materialize_registry_source_identities(connection, landing, control_schema=schema)
        receipt = await persist_registry_source_observations(connection, landing, control_schema=schema)
        admission_by_field = {
            **receipt,
            "identity_materialization": identities,
            "native_counts": metadata["counts"],
            "copy_sha256": hashlib.sha256(copy_bytes).hexdigest(),
            "copy_bytes": len(copy_bytes),
        }
        headers = next(csv.reader(io.StringIO(input_bytes.decode("utf-8-sig"), newline=""), strict=True))
        await seal_registry_source_admission(
            connection, namespace, schema, edition, _source_header_sha256(headers), admission_by_field
        )
        await connection.execute(f'DROP TABLE "{landing.table_name}"')
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_search_path)
    return admission_by_field


async def _copy_planfinder_batches(connection, table_name, batches, edition):
    """Exhaust and close one decoder; COPY and accounting remain bounded per batch."""
    from process.registry_source_observation_store import MAX_PLANFINDER_ROWS

    counters_by_name = dict.fromkeys(
        ("input_rows", "accepted_rows", "unresolved_rows", "rejected_rows", "issuer_rows"), 0
    )
    copy_digest = hashlib.sha256()
    copy_size = batch_count = 0
    try:
        while (
            input_bytes := await drain_operation(asyncio.to_thread(next, batches, None), preserve_cancellation=True)
        ) is not None:
            copy_bytes, row_count, metadata = await drain_operation(
                asyncio.to_thread(_encode_edition, input_bytes, edition), preserve_cancellation=True
            )
            if counters_by_name["input_rows"] + row_count > MAX_PLANFINDER_ROWS:
                raise RegistrySourceAdmissionError("Plan Finder edition exceeds its row bound")
            for field in counters_by_name:
                counter_value = metadata["counts"].get(field)
                if type(counter_value) is not int or counter_value < 0:
                    raise RegistrySourceAdmissionError("Native Plan Finder accounting is invalid")
                counters_by_name[field] += counter_value
            if metadata["counts"]["input_rows"] != row_count:
                raise RegistrySourceAdmissionError("Native Plan Finder input accounting differs")
            with io.BytesIO(copy_bytes) as copy_stream:
                status = await connection.copy_to_table(
                    table_name, columns=_COPY_COLUMNS, format="binary", source=copy_stream
                )
                if status != f"COPY {row_count}" or copy_stream.tell() != len(copy_bytes):
                    raise RegistrySourceAdmissionError("Plan Finder COPY accounting is incomplete")
            copy_digest.update(copy_bytes)
            copy_size += len(copy_bytes)
            batch_count += 1
    finally:
        batches.close()
    if not counters_by_name["input_rows"]:
        raise RegistrySourceAdmissionError("Plan Finder edition is empty")
    return counters_by_name, copy_digest.hexdigest(), copy_size, batch_count


async def _load_planfinder_landing(connection, namespace, workbook_path, edition):
    from process.cms_planfinder_workbook_input import iter_cms_planfinder_issuer_batches
    from process.registry_source_observation_store import RegistryObservationLanding

    table_name = "registry_landing_" + uuid4().hex
    await connection.execute(
        f'CREATE TEMP TABLE "{table_name}" (LIKE {namespace}.registry_source_observation INCLUDING ALL) ON COMMIT DROP'
    )
    batches = iter_cms_planfinder_issuer_batches(
        workbook_path,
        expected_workbook_sha256=edition.input_sha256,
        artifact_sha256=edition.artifact_sha256,
    )
    counters_by_name, copy_sha256, copy_size, batch_count = await _copy_planfinder_batches(
        connection, table_name, batches, edition
    )
    relation = await connection.fetchrow(
        "SELECT c.oid,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relnamespace=pg_my_temp_schema() AND c.relname=$1",
        table_name,
    )
    landing = RegistryObservationLanding(
        relation["nspname"],
        table_name,
        relation["oid"],
        edition.snapshot_id,
        edition.source_system,
        edition.source_id,
        edition.edition_id,
        edition.input_sha256,
        edition.parser_version,
        counters_by_name["input_rows"],
    )
    return landing, counters_by_name, copy_sha256, copy_size, batch_count


async def admit_cms_planfinder_edition(connection, workbook_path, edition, *, control_schema=None):
    """Exhaust bounded workbook batches, then validate one complete isolated edition.

    All COPY calls share the caller transaction. Identity/conflict reduction runs
    over the closed complete landing, including rows from different batches.
    The COPY digest describes concatenated complete batch frames in source order.
    Publication and approved custom revisions remain separate operations.
    """
    if type(edition) is not RegistrySourceEdition or edition.source_id != "plan-finder":
        raise RegistrySourceAdmissionError("An explicit Plan Finder edition is required")
    if not connection.is_in_transaction():
        raise RegistrySourceAdmissionError("Source admission requires a caller-owned transaction")
    from process.registry_identity_materialization import materialize_registry_source_identities
    from process.registry_source_admission_receipt import seal_registry_source_admission
    from process.registry_source_observation_store import persist_registry_source_observations

    schema = control_schema or registry_schema()
    namespace = _identifier(schema)
    async with connection.transaction():
        original_search_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        await _register_edition(connection, namespace, edition)
        landing, counters, copy_sha256, copy_size, batch_count = await _load_planfinder_landing(
            connection, namespace, workbook_path, edition
        )
        identities = await materialize_registry_source_identities(connection, landing, control_schema=schema)
        receipt = await persist_registry_source_observations(connection, landing, control_schema=schema)
        from process.cms_planfinder_workbook_input import HEADERS

        admission_by_field = {
            **receipt,
            "identity_materialization": identities,
            "native_counts": counters,
            "copy_sha256": copy_sha256,
            "copy_bytes": copy_size,
            "copy_batches": batch_count,
        }
        await seal_registry_source_admission(
            connection, namespace, schema, edition, _source_header_sha256(HEADERS), admission_by_field
        )
        await connection.execute(f'DROP TABLE "{landing.table_name}"')
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_search_path)
    return admission_by_field


def _source_header_sha256(headers):
    return hashlib.sha256(json.dumps(headers, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


async def read_registry_source_edition_identity(
    connection, edition, *, control_schema=None
) -> RegistrySourceEdition | None:
    """Read an exact edition's original identity, without asserting completion.

    The provisional snapshot UUID and fetch time may differ; all immutable
    edition metadata must match. The returned retrieval time is the first stored
    time; legacy snapshots may contain the admission-time database default.
    Missing identities remain absent and are never registered here.
    """
    if type(edition) is not RegistrySourceEdition or not connection.is_in_transaction():
        raise RegistrySourceAdmissionError("Source identity lookup requires a complete edition and caller transaction")
    try:
        edition_by_field = asdict(replace(edition))
    except AttributeError, TypeError:
        raise RegistrySourceAdmissionError("Source identity lookup requires a complete edition") from None
    namespace = _identifier(control_schema or registry_schema())
    identity_fields = ("source_system", "source_id", "edition_id", "input_sha256", "parser_version")
    columns = ",".join(edition_by_field)
    identity_predicate = " AND ".join(f"{field}=${index}" for index, field in enumerate(identity_fields, 1))
    async with connection.transaction():
        original_search_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        snapshot = await connection.fetchrow(
            f"SELECT {columns} FROM {namespace}.registry_source_snapshot WHERE {identity_predicate}",
            *(edition_by_field[field] for field in identity_fields),
        )
        original_edition = (
            RegistrySourceEdition(**{**dict(snapshot), "snapshot_id": UUID(str(snapshot["snapshot_id"]))})
            if snapshot is not None
            else None
        )
        if original_edition is not None and original_edition != replace(
            edition, snapshot_id=original_edition.snapshot_id
        ):
            raise RegistrySourceAdmissionError("Source edition metadata is immutable")
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_search_path)
    return original_edition


async def read_registry_source_admission_receipt(connection, edition, *, header_sha256, control_schema=None):
    """Verify exact native completion; historical or changed evidence requires replay."""
    if (
        type(edition) is not RegistrySourceEdition
        or type(header_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", header_sha256) is None
        or not connection.is_in_transaction()
    ):
        raise RegistrySourceAdmissionError("Source admission lookup requires a complete edition and caller transaction")
    from process.registry_source_admission_receipt import read_completed_registry_source_admission

    schema = control_schema or registry_schema()
    namespace = _identifier(schema)
    async with connection.transaction():
        original_search_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)")
        receipt = await read_completed_registry_source_admission(connection, namespace, schema, edition, header_sha256)
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_search_path)
    return receipt
