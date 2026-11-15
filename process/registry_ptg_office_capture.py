# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare a closed exact-office capture; authenticated review and pins are separate."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_ptg_producer_scope import read_registry_ptg_producer_scope
from process.registry_ptg_published_office_scope import (
    is_published_office_context,
    published_office_scope,
    require_published_office_path,
)
from process.registry_retained_site_adoption import resolve_retained_site_adoptions
from process.uhc_flex_practitioner_async_safety import drain_operation

COPY_COLUMNS = (
    "ordinal",
    "source_record_key",
    "binding_source_key",
    "company_key",
    "cohort_id",
    "snapshot_id",
    "provider_system",
    "provider_id",
    "location_id",
    "location_key",
    "location_hash",
    "address_row_sha256",
    "dense_source_key",
    "source_record_ordinal",
    "provider_group_ref",
    "provider_witness_sha256",
    "office_evidence_kind",
    "office_evidence_json",
    "office_evidence_sha256",
    "evidence_id",
)
_COLUMN_TYPES = (
    ("bigint",)
    + ("text",) * 7
    + ("uuid",)
    + ("text",) * 3
    + ("integer", "bigint")
    + ("text",) * 3
    + ("jsonb", "text", "text")
)
MAX_ROWS, MAX_INPUT_BYTES, MAX_COPY_BYTES, MAX_TOTAL_ROWS = (
    5000,
    8 * 1024 * 1024,
    16 * 1024 * 1024,
    1_000_000,
)


class RegistryPTGOfficeCaptureError(ValueError):
    """The complete capture failed without yielding an admission receipt."""


@dataclass(frozen=True)
class RegistryPTGOfficeCaptureRequest:
    capture_id: UUID
    canonical_input_sha256: str
    input_row_count: int
    office_evidence_kind: str
    retained_generation_id: int
    reason: str
    idempotency_key: str


@dataclass(frozen=True)
class RegistryPTGOfficeCaptureContext:
    """Server-selected source/store/roles, never populated from a browser envelope."""

    scope_id: UUID
    client_id: str
    scope_approval_sha256: str
    coordinates: object
    source_specification: object
    frozen_authority: object
    graph_identity: object
    scope_store: object
    control_schema: str
    owner_role: str
    reader_roles: tuple[str, ...]
    office_custody: object = None
    preparation_request_sha256: str | None = None


@dataclass(frozen=True)
class RegistryPTGOfficeCaptureDescriptor:
    """Immutable prepared facts, with no review, publication or admission status."""

    capture_id: UUID
    schema_name: str
    manifest_sha256: str
    manifest_json: bytes
    storage_json: bytes

    def as_dict(self):
        """Return fresh decoded facts without mutating this prepared descriptor."""
        return {
            "capture_id": str(self.capture_id),
            "schema_name": self.schema_name,
            "manifest_sha256": self.manifest_sha256,
            "manifest": json.loads(self.manifest_json),
            "storage": descriptor_storage(self),
        }


_OFFICE_TABLES = ("capture_manifest", "office_assertion")
_LEGACY_CUSTODY_FIELDS = frozenset(
    {
        "schema_oid",
        "owner_oid",
        "table_oid",
        "manifest_table_oid",
        "valid",
        "heaps",
        "routines",
        "triggers",
        "columns_sha256",
        "manifest_columns_sha256",
        "owner_role",
        "reader_roles",
    }
)


def office_storage(schema_name, manifest_sha256=None):
    """Return positive names and the stored logical manifest edition."""
    if type(schema_name) is not str or re.fullmatch(r"registry_ptg_office_[0-9a-f]{32}", schema_name) is None:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
    storage_by_field = {
        "schema_name": schema_name,
        "tables": [{"schema": schema_name, "name": name} for name in _OFFICE_TABLES],
    }
    if manifest_sha256 is not None:
        if type(manifest_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
        storage_by_field["manifest_sha256"] = manifest_sha256
    return storage_by_field


def descriptor_storage(descriptor):
    """Decode the exact old record as a named view without changing its stored edition."""
    if type(descriptor.storage_json) is not bytes or not 1 <= len(descriptor.storage_json) <= 16384:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
    stored = json.loads(descriptor.storage_json)
    expected = office_storage(descriptor.schema_name, descriptor.manifest_sha256)
    if stored != expected and (type(stored) is not dict or set(stored) != _LEGACY_CUSTODY_FIELDS):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
    return expected


def named_office_evidence(evidence, command):
    """Project exact historical storage and serving fields without changing stored editions."""
    from process.registry_ptg_office_review_contract import (
        _retained_serving,
        is_legacy_office_review_command,
        named_office_review_command,
    )

    if type(evidence) is not dict:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
    expected = office_storage(evidence["schema_name"], evidence["manifest_sha256"])
    if "custody" in evidence:
        if (
            "storage" in evidence
            or type(evidence["custody"]) is not dict
            or set(evidence["custody"]) != _LEGACY_CUSTODY_FIELDS
        ):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
        evidence_by_field = {name: field_value for name, field_value in evidence.items() if name != "custody"} | {
            "storage": expected
        }
    else:
        if evidence.get("storage") != expected:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_descriptor_invalid")
        evidence_by_field = dict(evidence)
    evidence_by_field["retained_serving"] = _retained_serving(
        {**named_office_review_command(command), "retained_serving": evidence["retained_serving"]},
        is_legacy_office_review_command(command),
    )
    return evidence_by_field


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _validated(request, context, *, approval_context=False):
    context_type = RegistryPTGOfficeCaptureContext
    if approval_context:
        from process.registry_ptg_office_approval import RegistryPTGOfficeApprovalContext

        context_type = RegistryPTGOfficeApprovalContext
    if type(request) is not RegistryPTGOfficeCaptureRequest or type(context) is not context_type:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_input_invalid")
    for supplied in (request.capture_id, context.scope_id):
        if type(supplied) is not UUID or not supplied.int:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_input_invalid")
    if (
        type(request.input_row_count) is not int
        or not 1 <= request.input_row_count <= MAX_TOTAL_ROWS
        or type(request.retained_generation_id) is not int
        or not 1 <= request.retained_generation_id < 2**63
        or request.office_evidence_kind not in ("payer_exact_office", "reviewed_exact_office")
        or type(context.reader_roles) is not tuple
        or not 1 <= len(context.reader_roles) <= 32
        or len(set(context.reader_roles)) != len(context.reader_roles)
        or context.owner_role in context.reader_roles
    ):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_input_invalid")
    for supplied in (request.canonical_input_sha256, context.scope_approval_sha256):
        if type(supplied) is not str or re.fullmatch(r"[0-9a-f]{64}", supplied) is None:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_input_invalid")
    for supplied, limit in ((context.client_id, 64), (request.reason, 1000), (request.idempotency_key, 128)):
        if (
            type(supplied) is not str
            or not 1 <= len(supplied.encode()) <= limit
            or supplied.strip() != supplied
            or any(ord(character) < 32 for character in supplied)
        ):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_input_invalid")
    for role in (*context.reader_roles, context.owner_role, context.control_schema):
        _identifier(role)
    return "registry_ptg_office_" + request.capture_id.hex


def _encoder():
    try:
        encoder = importlib.import_module("ptg2_address_canon").encode_registry_ptg_capture_batch
    except ImportError, AttributeError:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_native_unavailable") from None
    if not callable(encoder):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_native_unavailable")
    return encoder


def _batch(encoder, raw, expected_context, after):
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_INPUT_BYTES:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_batch_bounds")
    copy_bytes, canonical, count, last = encoder(raw, expected_context, after)
    if (
        type(copy_bytes) is not bytes
        or not 1 <= len(copy_bytes) <= MAX_COPY_BYTES
        or type(canonical) is not bytes
        or not 1 <= len(canonical) <= MAX_COPY_BYTES
        or type(count) is not int
        or not 1 <= count <= MAX_ROWS
        or type(last) is not int
        or last != after + count
        or canonical.count(b"\n") != count
        or not canonical.endswith(b"\n")
    ):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_native_changed")
    rows = [json.loads(line) for line in canonical.splitlines()]
    if len(rows) != count or any(set(row) != set(COPY_COLUMNS) for row in rows):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_native_changed")
    return copy_bytes, canonical, count, last, rows


async def _driver(session):
    if not session.in_transaction():
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_transaction_required")
    driver = await native_driver(session)
    if not driver.is_in_transaction() or await driver.fetchval(
        "SELECT current_setting('transaction_isolation')"
    ) not in ("repeatable read", "serializable"):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_transaction_required")
    return driver


async def _scope(session, context):
    if is_published_office_context(context):
        return await published_office_scope(session, context)
    document = await read_registry_ptg_producer_scope(
        session,
        context.source_specification,
        scope_id=context.scope_id,
        client_id=context.client_id,
        coordinates=context.coordinates,
        frozen_authority=context.frozen_authority,
        graph_identity=context.graph_identity,
        store=context.scope_store,
    )
    if document["approval_sha256"] != context.scope_approval_sha256:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_scope_changed")
    return document


def _office_source_scope(scope):
    if "source_scope" in scope:
        return dict(scope["source_scope"])
    return {name: scope[name] for name in ("company_key", "cohort_id", "snapshot_id")}


def _codec_context(scope, request):
    return _canonical(
        {
            "binding_coordinates": scope["coordinates"],
            "binding_source_key": scope["binding_source_key"],
            "source_scope": _office_source_scope(scope),
            "office_evidence_kind": request.office_evidence_kind,
            "snapshot_key": scope["evidence"]["graph_identity"]["snapshot_key"],
        }
    )


def _ddl(schema_name, *, is_published=False):
    namespace = _identifier(schema_name)
    columns = ",".join(
        f"{name} {kind}" + ("" if is_published and name in {"company_key", "cohort_id"} else " NOT NULL")
        for name, kind in zip(COPY_COLUMNS, _COLUMN_TYPES, strict=True)
    )
    scope_check = ",CHECK(company_key IS NULL AND cohort_id IS NULL)" if is_published else ""
    return (
        f"CREATE SCHEMA {namespace}",
        f"CREATE TABLE {namespace}.office_assertion ({columns},PRIMARY KEY(ordinal),UNIQUE(source_record_key),UNIQUE(provider_system,provider_id,location_id),CHECK(ordinal>0 AND source_record_ordinal>=0 AND dense_source_key>=0){scope_check})",
        f"CREATE TABLE {namespace}.capture_manifest (id integer PRIMARY KEY CHECK(id=1),manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{{64}}$'),manifest_json jsonb NOT NULL CHECK(jsonb_typeof(manifest_json)='object'))",
    )


async def _adopt_batch(driver, serving, rows, control_schema):
    fields = ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
    selected_rows = [{name: row[name] for name in fields} for row in rows]
    receipt = await resolve_retained_site_adoptions(
        driver, serving, _canonical(selected_rows), control_schema=control_schema
    )
    if len(receipt.records) != len(rows):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_retained_changed")
    return receipt


def _retained_accounting(receipt, rows, digest, previous):
    envelope = receipt.as_dict()
    identity_by_field = {name: value for name, value in envelope.items() if name != "records"}
    if previous is not None and previous != identity_by_field:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_retained_changed")
    records_by_pair = {
        (row["provider_system"], row["provider_id"], row["location_id"]): row for row in envelope["records"]
    }
    if len(records_by_pair) != len(rows):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_retained_changed")
    for row in rows:
        record = records_by_pair.get((row["provider_system"], row["provider_id"], row["location_id"]))
        if record is None or any(record[name] != row[name] for name in ("location_key", "address_row_sha256")):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_retained_changed")
        digest.update(_canonical(record) + b"\n")
    return identity_by_field


def _same_transaction(session, driver, transaction):
    if not session.in_transaction() or session.get_transaction() is not transaction or not driver.is_in_transaction():
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_transaction_changed")


async def _copy_office_batches(driver, schema_name, batches, encoder, scope, request, retained_source, guard):
    serving, control_schema = retained_source
    digest, adoption_digest, ordinal = hashlib.sha256(), hashlib.sha256(), 0
    retained_identity = None
    expected_context = _codec_context(scope, request)
    async for raw in batches:
        guard()
        encoded, canonical, count, last, office_rows = await drain_operation(
            asyncio.to_thread(_batch, encoder, raw, expected_context, ordinal), preserve_cancellation=True
        )
        guard()
        if last > request.input_row_count:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_accounting_invalid")
        receipt = await _adopt_batch(driver, serving, office_rows, control_schema)
        guard()
        retained_identity = _retained_accounting(receipt, office_rows, adoption_digest, retained_identity)
        status = await drain_operation(
            driver.copy_to_table(
                "office_assertion",
                schema_name=schema_name,
                columns=COPY_COLUMNS,
                source=memoryview(encoded),
                format="binary",
            ),
            preserve_cancellation=True,
        )
        guard()
        if status != "COPY " + str(count):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_accounting_invalid")
        digest.update(canonical)
        ordinal = last
    if ordinal != request.input_row_count or digest.hexdigest() != request.canonical_input_sha256:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_accounting_invalid")
    return {
        "input_row_count": ordinal,
        "canonical_input_sha256": digest.hexdigest(),
        "retained_site_rows_sha256": adoption_digest.hexdigest(),
        "retained_site_identity": retained_identity,
    }


async def _validate_rows(driver, schema_name, scope, count):
    namespace = _identifier(schema_name)
    result = await driver.fetchrow(
        f"""SELECT count(*) AS rows,count(DISTINCT source_record_key) AS source_records,
      count(DISTINCT (provider_system,provider_id,location_id)) AS offices,min(ordinal) AS first,max(ordinal) AS last,
      count(*) FILTER(WHERE NOT(dense_source_key=ANY($1::integer[])) OR binding_source_key IS DISTINCT FROM $2 OR company_key IS DISTINCT FROM $3 OR cohort_id IS DISTINCT FROM $4 OR snapshot_id IS DISTINCT FROM $5) AS invalid
      FROM {namespace}.office_assertion""",
        scope["evidence"]["selected_dense_source_keys"],
        scope["binding_source_key"],
        scope.get("company_key"),
        scope.get("cohort_id"),
        scope["snapshot_id"],
    )
    if dict(result) != {
        "rows": count,
        "source_records": count,
        "offices": count,
        "first": 1,
        "last": count,
        "invalid": 0,
    }:
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_accounting_invalid")


async def _require_office_layout(driver, schema_name, *, is_published):
    """Validate essential columns by qualified name; PostgreSQL enforces access."""
    for table, names, kinds, nullable in (
        ("office_assertion", COPY_COLUMNS, _COLUMN_TYPES, ("company_key", "cohort_id") if is_published else ()),
        ("capture_manifest", ("id", "manifest_sha256", "manifest_json"), ("integer", "text", "jsonb"), ()),
    ):
        relation = _identifier(schema_name) + "." + _identifier(table)
        columns = await driver.fetch(
            "SELECT attname,atttypid::regtype::text AS type,attnotnull,atthasdef FROM pg_attribute "
            "WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            relation,
        )
        expected_columns = tuple(
            (name, kind, name not in nullable, False) for name, kind in zip(names, kinds, strict=True)
        )
        if (
            tuple((row["attname"], row["type"], row["attnotnull"], row["atthasdef"]) for row in columns)
            != expected_columns
        ):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_layout_invalid")


def _office_review_source(scope):
    names = (
        "coordinates",
        "binding_source_key",
        "snapshot_id",
        "legal_company_id",
        "approved_revision",
        "file_versions",
        "evidence",
    )
    if "source_scope" in scope:
        return {
            **{name: scope[name] for name in names},
            "source_scope": _office_source_scope(scope),
            "network_id": scope["network_id"],
        }
    return {name: scope[name] for name in (*names, "company_key", "cohort_id")}


def office_review_command(request, scope, serving, accounting):
    """Fresh whole-command approval must recheck current company under its fence.

    Durable source/office pins remain prerequisites for publication or admission.
    """
    return {
        "operation": "review_registry_ptg_office_capture",
        "capture_id": str(request.capture_id),
        "scope_id": scope["scope_id"],
        "client_id": scope["client_id"],
        "scope_approval_sha256": scope["approval_sha256"],
        "canonical_input_sha256": accounting["canonical_input_sha256"],
        "input_row_count": accounting["input_row_count"],
        "office_evidence_kind": request.office_evidence_kind,
        "retained_generation_id": serving.generation_id,
        "retained_serving": asdict(serving),
        "retained_serving_sha256": _digest(asdict(serving)),
        "retained_sites": {
            "identity": accounting["retained_site_identity"],
            "rows_sha256": accounting["retained_site_rows_sha256"],
        },
        "source": _office_review_source(scope),
        "reason": request.reason,
        "idempotency_key": request.idempotency_key,
    }


def _prepared_capture_manifest(request, scope, serving, accounting, preparation_request_sha256=None):
    command = office_review_command(request, scope, serving, accounting)
    return {
        "contract": "registry_ptg_office_capture.v1",
        "state": "prepared",
        "command": command,
        "command_sha256": _digest(command),
        "accounting": accounting,
        "storage": office_storage("registry_ptg_office_" + request.capture_id.hex),
        "preparation_request_sha256": preparation_request_sha256 or _digest(command),
    }


async def prepare_registry_ptg_office_capture(session, context, request, batches):
    """Exhaust/corroborate/copy one candidate in a savepoint; never commit or admit.

    Caller rollback cancels this candidate; commit only persists prepared facts.
    Publication requires separate fresh review/current-company fencing and pins.
    """
    schema_name, encoder = _validated(request, context), _encoder()
    driver = await _office_driver(session, context, publisher=True)
    transaction = session.get_transaction()
    async with session.begin_nested():
        if await driver.fetchval("SELECT to_regnamespace($1)", schema_name) is not None:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_capture_exists")
        scope = await _scope(session, context)
        serving = await resolve_network_serving_manifest(
            driver,
            generation_id=request.retained_generation_id,
            control_schema=context.control_schema,
        )
        for statement in _ddl(schema_name, is_published=is_published_office_context(context)):
            await driver.execute(statement)
        accounting = await _copy_office_batches(
            driver,
            schema_name,
            batches,
            encoder,
            scope,
            request,
            (serving, context.control_schema),
            lambda: _same_transaction(session, driver, transaction),
        )
        await _validate_rows(driver, schema_name, scope, accounting["input_row_count"])
        if (
            await _scope(session, context) != scope
            or await resolve_network_serving_manifest(
                driver,
                generation_id=serving.generation_id,
                control_schema=context.control_schema,
            )
            != serving
        ):
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_source_changed")
        manifest_by_field = _prepared_capture_manifest(
            request, scope, serving, accounting, context.preparation_request_sha256
        )
        pending = await _handoff_office_capture(driver, schema_name, context, request, manifest_by_field)
        _same_transaction(session, driver, transaction)
        return pending


async def _handoff_office_capture(driver, schema_name, context, request, manifest_by_field):
    from process.registry_ptg_office_preparation import admit_office_preparation

    await driver.execute(
        f"INSERT INTO {_identifier(schema_name)}.capture_manifest VALUES(1,$1,$2::jsonb)",
        _digest(manifest_by_field),
        _canonical(manifest_by_field).decode(),
    )
    _, preparation, _ = await admit_office_preparation(driver, request.capture_id, context=context)
    if preparation["state"] != "handed_off":
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_preparation_changed")
    return {
        "capture_id": str(request.capture_id),
        "state": "handed_off",
        "command": manifest_by_field["command"],
        "command_sha256": manifest_by_field["command_sha256"],
        "manifest_sha256": _digest(manifest_by_field),
    }


async def _office_driver(session, context, *, publisher=False):
    if is_published_office_context(context):
        await require_published_office_path(session)
    driver = await _driver(session)
    from process.registry_ptg_office_custody import office_preparation_roles

    if publisher:
        office_preparation_roles(context)
    return driver
