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
from process.network_custom_address_source import _closed
from process.network_membership_writer_closure import _check_roles
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


@dataclass(frozen=True)
class RegistryPTGOfficeCaptureDescriptor:
    """Immutable prepared facts, with no review, publication or admission status."""

    capture_id: UUID
    schema_name: str
    manifest_sha256: str
    manifest_json: bytes
    custody_json: bytes

    def as_dict(self):
        """Return fresh decoded facts without mutating this prepared descriptor."""
        return {
            "capture_id": str(self.capture_id),
            "schema_name": self.schema_name,
            "manifest_sha256": self.manifest_sha256,
            "manifest": json.loads(self.manifest_json),
            "custody": json.loads(self.custody_json),
        }


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
        encoded, canonical, count, last, office_rows = await asyncio.to_thread(
            _batch, encoder, raw, expected_context, ordinal
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


async def _close(driver, schema_name, context):
    """Transfer this isolated family; revoke table AND column grants before closure."""
    roles_by_name = {"owner_role": context.owner_role, "loader_roles": [], "reader_roles": list(context.reader_roles)}
    await _check_roles(driver, roles_by_name)
    namespace, owner = _identifier(schema_name), _identifier(context.owner_role)
    grantees = await driver.fetch(
        """SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS name
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
      LEFT JOIN pg_attribute att ON att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped
      CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))||coalesce(c.relacl,acldefault('r',c.relowner))||coalesce(att.attacl,'{}'::aclitem[])) a
      LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE n.nspname=$1 AND a.grantee<>n.nspowner""",
        schema_name,
    )
    publisher = await driver.fetchval("SELECT quote_ident(current_user)")
    await driver.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    await driver.execute(f"SET LOCAL ROLE {owner}")
    await driver.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {publisher}")
    await driver.execute(f"SET LOCAL ROLE {publisher}")
    for table, columns in (
        ("office_assertion", COPY_COLUMNS),
        ("capture_manifest", ("id", "manifest_sha256", "manifest_json")),
    ):
        await driver.execute(f"ALTER TABLE {namespace}.{table} OWNER TO {owner}")
    await driver.execute(f"SET LOCAL ROLE {owner}")
    grantees = [{"name": publisher}, *grantees]
    for table, columns in (
        ("office_assertion", COPY_COLUMNS),
        ("capture_manifest", ("id", "manifest_sha256", "manifest_json")),
    ):
        for grantee in grantees:
            await driver.execute(f"REVOKE ALL ON {namespace}.{table} FROM {grantee['name']} CASCADE")
            await driver.execute(
                f"REVOKE ALL ({','.join(columns)}) ON {namespace}.{table} FROM {grantee['name']} CASCADE"
            )
    for grantee in grantees:
        await driver.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM {grantee['name']} CASCADE")
    for role in context.reader_roles:
        await driver.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(role)}")
        await driver.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {_identifier(role)}")
    await driver.execute(f"SET LOCAL ROLE {publisher}")
    return await _custody(driver, schema_name, context)


async def _custody(driver, schema_name, context):
    catalog = await driver.fetchrow(
        """SELECT n.oid::bigint AS schema_oid,n.nspowner::bigint AS owner_oid,r.oid::bigint AS table_oid,m.oid::bigint AS manifest_table_oid,
      n.nspowner=owner.oid AND r.relowner=owner.oid AND r.relkind='r' AND r.relpersistence='p'
      AND NOT r.relrowsecurity AND NOT r.relforcerowsecurity
      AND m.relowner=owner.oid AND m.relkind='r' AND m.relpersistence='p'
      AND NOT m.relrowsecurity AND NOT m.relforcerowsecurity
      AND NOT EXISTS(SELECT FROM pg_class c WHERE c.relnamespace=n.oid
        AND (c.relkind NOT IN ('r','i') OR c.relpersistence<>'p' OR c.relowner<>owner.oid
          OR c.relkind='r' AND c.relname NOT IN ('office_assertion','capture_manifest'))) AS valid,
      (SELECT count(*) FROM pg_class c WHERE c.relnamespace=n.oid AND c.relkind='r') AS heaps,
      EXISTS(SELECT FROM pg_proc WHERE pronamespace=n.oid) AS routines,
      EXISTS(SELECT FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace=n.oid AND NOT t.tgisinternal) AS triggers
      FROM pg_namespace n JOIN pg_class r ON r.relnamespace=n.oid AND r.relname='office_assertion'
      JOIN pg_class m ON m.relnamespace=n.oid AND m.relname='capture_manifest'
      JOIN pg_roles owner ON owner.rolname=$2 WHERE n.nspname=$1""",
        schema_name,
        context.owner_role,
    )
    if (
        catalog is None
        or catalog["valid"] is not True
        or catalog["heaps"] != 2
        or catalog["routines"]
        or catalog["triggers"]
    ):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_custody_invalid")
    await _closed(
        driver,
        catalog["schema_oid"],
        catalog["owner_oid"],
        context.reader_roles,
        ["office_assertion", "capture_manifest"],
    )
    columns_sha256 = await _column_identity(
        driver,
        catalog["table_oid"],
        COPY_COLUMNS,
        _COLUMN_TYPES,
        nullable_names=("company_key", "cohort_id") if is_published_office_context(context) else (),
    )
    manifest_columns_sha256 = await _column_identity(
        driver,
        catalog["manifest_table_oid"],
        ("id", "manifest_sha256", "manifest_json"),
        ("integer", "text", "jsonb"),
    )
    return {
        **dict(catalog),
        "columns_sha256": columns_sha256,
        "manifest_columns_sha256": manifest_columns_sha256,
        "owner_role": context.owner_role,
        "reader_roles": list(context.reader_roles),
    }


async def _column_identity(driver, table_oid, names, kinds, *, nullable_names=()):
    columns = await driver.fetch(
        "SELECT attname,atttypid::regtype::text AS type,attnotnull,atthasdef FROM pg_attribute WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        table_oid,
    )
    expected_columns = tuple(
        (name, kind, name not in nullable_names, False) for name, kind in zip(names, kinds, strict=True)
    )
    if (
        tuple(
            (
                column["attname"],
                column["type"],
                column["attnotnull"],
                column["atthasdef"],
            )
            for column in columns
        )
        != expected_columns
    ):
        raise RegistryPTGOfficeCaptureError("registry_ptg_office_custody_invalid")
    return _digest([dict(column) for column in columns])


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


def _prepared_capture_manifest(request, scope, serving, accounting):
    command = office_review_command(request, scope, serving, accounting)
    return {
        "contract": "registry_ptg_office_capture.v1",
        "state": "prepared",
        "command": command,
        "command_sha256": _digest(command),
        "accounting": accounting,
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
        manifest_by_field = _prepared_capture_manifest(request, scope, serving, accounting)
        await driver.execute(
            f"INSERT INTO {_identifier(schema_name)}.capture_manifest VALUES(1,$1,$2::jsonb)",
            _digest(manifest_by_field),
            _canonical(manifest_by_field).decode(),
        )
        custody = await _close(driver, schema_name, context)
        if not session.in_transaction() or session.get_transaction() is not transaction:
            raise RegistryPTGOfficeCaptureError("registry_ptg_office_transaction_changed")
        return RegistryPTGOfficeCaptureDescriptor(
            request.capture_id,
            schema_name,
            _digest(manifest_by_field),
            _canonical(manifest_by_field),
            _canonical(custody),
        )


async def _office_driver(session, context, *, publisher=False):
    if is_published_office_context(context):
        await require_published_office_path(session)
    driver = await _driver(session)
    if publisher and is_published_office_context(context):
        from process.registry_ptg_office_custody import verify_registry_ptg_office_custody

        await verify_registry_ptg_office_custody(driver, context, publisher=True)
    return driver
