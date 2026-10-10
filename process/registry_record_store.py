# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Management draft heads and full history; this does not publish serving data."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Integer,
    MetaData,
    String,
    and_,
    any_,
    cast,
    column,
    func,
    insert,
    literal,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from db.models import NPIData
from db.models.company_group_registry import CompanyGroupRegistry
from db.models.company_registry import CompanyRegistry
from db.models.company_registry_assertions import CompanyRegistryIdentifierAssertion, CompanyRegistryRoleAssertion
from db.models.company_registry_links import CompanyRegistryLinks
from db.models.manual_directory_registry import (
    ManualLocationRegistry,
    ManualProviderLocationBinding,
    ManualProviderRegistry,
)
from db.models.network_membership_draft import NetworkMembershipDraft
from db.models.network_registry import NetworkRegistryIdentity, NetworkRegistryRecord
from db.models.registry_network_binding import RegistryNetworkBinding
from db.models.registry_revision import RegistryRecordHistory, RegistryRevisionControl
from db.models.registry_site_binding import RegistrySiteBinding
from process.company_registry_assertion_values import validated_company_registry_assertions
from process.ext.address_fast import _fast_module
from process.network_membership_copy import MAX_INPUT_BYTES, MembershipCopyError, _encode
from process.network_registry_identity import allocate_network_ids
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.registry_site_binding_store import resolve_site_binding_fields, validated_site_binding_fields

_RECORD_MODELS = {
    "network_binding": (RegistryNetworkBinding, "binding_id", set()),
    "group": (CompanyGroupRegistry, "group_id", {"group_kind", "display_name", "aliases"}),
    "company": (CompanyRegistry, "company_id", {"display_name", "roles", "aliases"}),
    "network": (NetworkRegistryRecord, "network_id", {"display_name", "aliases"}),
    "company_links": (CompanyRegistryLinks, "company_id", {"network_ids", "group_id"}),
    "provider": (ManualProviderRegistry, "provider_id", {"display_name", "provider_kind", "aliases", "npi"}),
    "location": (ManualLocationRegistry, "location_id", {"display_name", "aliases", "address_json"}),
    "membership": (NetworkMembershipDraft, "network_id", {"memberships_json"}),
    "site_binding": (
        RegistrySiteBinding,
        "binding_id",
        {"source_generation", "provider_system", "provider_id", "location_id", "location_key", "address_row_sha256"},
    ),
}
_COMPANY_ROLES = {"insurer", "employer", "network_operator"}


class RegistryRecordConflict(ValueError):
    """A stale revision or conflicting retry cannot mutate a draft."""


class RegistryAddressUnavailable(RuntimeError):
    """A compatible native encoder is required for a site or membership edit."""


@dataclass(frozen=True)
class RegistryActor:
    """Server-derived actor; the caller verifies explicit action grants."""

    kind: str
    user_id: UUID
    client_id: str
    impersonator_id: UUID | None = None


@dataclass(frozen=True)
class RegistryRecordCommand:
    record_kind: str
    record_id: UUID | int | None
    operation: str
    expected_revision: int
    fields: dict
    reason: str
    idempotency_key: str
    allocation_key: UUID | None = None


def _bounded_text(value, maximum, field):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or "\0" in value:
        raise ValueError(f"registry_{field}_invalid")
    return value.strip()


def _bounded_name(value, maximum, field):
    normalized = _bounded_text(value, maximum, field)
    if any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value):
        raise ValueError(f"registry_{field}_invalid")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ValueError(f"registry_{field}_invalid") from None
    return normalized


def _uuid(value):
    if not isinstance(value, UUID) or not value.int:
        raise ValueError("registry_uuid_invalid")
    return value


def _record_identity(kind, value):
    if kind in {"network", "membership"}:
        if type(value) is not int or not 0 < value <= 2147483647:
            raise ValueError("registry_network_id_invalid")
        return value
    return _uuid(value)


def _validated_actor(actor):
    if (
        not isinstance(actor, RegistryActor)
        or not isinstance(actor.kind, str)
        or actor.kind not in {"platform_admin", "client_owner"}
    ):
        raise ValueError("registry_actor_invalid")
    _uuid(actor.user_id)
    if actor.impersonator_id is not None:
        _uuid(actor.impersonator_id)
    if _bounded_text(actor.client_id, 64, "client_id") != actor.client_id or any(
        ord(character) < 32 or ord(character) == 127 for character in actor.client_id
    ):
        raise ValueError("registry_client_id_invalid")
    return {key: str(value) if isinstance(value, UUID) else value for key, value in asdict(actor).items()}


def _validated_fields(kind, fields):
    if kind == "company_links":
        if type(fields) is not dict or set(fields) not in (
            _RECORD_MODELS[kind][2],
            _RECORD_MODELS[kind][2] | {"network_assertions"},
        ):
            raise ValueError("registry_editable_fields_invalid")
        return _validated_company_links(fields)
    allowed = _RECORD_MODELS[kind][2]
    if not isinstance(fields, dict) or set(fields) not in (
        allowed,
        allowed | {"assertions"}
        if kind == "company"
        else allowed | {"catalog_evidence_json"}
        if kind == "network"
        else allowed,
    ):
        raise ValueError("registry_editable_fields_invalid")
    if kind == "site_binding":
        return validated_site_binding_fields(fields)
    if kind == "membership":
        if type(fields["memberships_json"]) is not list:
            raise ValueError("registry_membership_rows_invalid")
        return {"memberships_json": fields["memberships_json"]}
    maximum = 256 if kind in {"provider", "location"} else 512
    fields_by_name = {"display_name": _bounded_name(fields["display_name"], maximum, "display_name")}
    aliases = fields["aliases"]
    if not isinstance(aliases, list) or len(aliases) > 100:
        raise ValueError("registry_aliases_invalid")
    fields_by_name["aliases"] = sorted({_bounded_name(alias, 512, "alias") for alias in aliases})
    if kind == "location":
        fields_by_name["address_json"] = _validated_location_address(fields["address_json"])
    if kind == "provider":
        provider_kind = fields["provider_kind"]
        if type(provider_kind) is not str or provider_kind not in {"individual", "organization"}:
            raise ValueError("registry_provider_kind_invalid")
        npi = fields["npi"]
        if npi is not None and (type(npi) is not str or len(npi) != 10 or not is_valid_npi(npi)):
            raise ValueError("registry_provider_npi_invalid")
        fields_by_name.update(provider_kind=provider_kind, npi=npi)
    if kind == "group":
        if not isinstance(fields["group_kind"], str) or fields["group_kind"] not in {"corporate_parent", "naic_group"}:
            raise ValueError("registry_group_kind_invalid")
        fields_by_name["group_kind"] = fields["group_kind"]
    if kind == "company":
        roles = fields["roles"]
        if (
            not isinstance(roles, list)
            or not roles
            or any(not isinstance(role, str) or role not in _COMPANY_ROLES for role in roles)
        ):
            raise ValueError("registry_company_roles_invalid")
        fields_by_name["roles"] = sorted(set(roles))
        if "assertions" in fields:
            fields_by_name["assertions"] = validated_company_registry_assertions(fields["assertions"]).as_dict()
    if kind == "network" and "catalog_evidence_json" in fields:
        fields_by_name["catalog_evidence_json"] = _validated_catalog_evidence(fields["catalog_evidence_json"])
    return fields_by_name


def _validated_catalog_evidence(evidence):
    """Require the native closed syntax, not physical pricing or benefit proof.

    The 16 KiB bound applies to compact sorted UTF-8 parser input. PostgreSQL
    JSONB text has a separate 32 KiB storage bound for added whitespace.
    """
    if evidence is None:
        return None
    if type(evidence) is not dict:
        raise ValueError("registry_network_evidence_invalid")
    try:
        encoded = json.dumps(
            evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    except TypeError, ValueError, UnicodeError, RecursionError:
        raise ValueError("registry_network_evidence_invalid") from None
    if len(encoded) > 16384:
        raise ValueError("registry_network_evidence_invalid")
    native = _fast_module()
    parser = getattr(native, "parse_registry_network_evidence", None)
    if not callable(parser):
        raise RegistryAddressUnavailable("registry_network_evidence_native_unavailable")
    output = parser(encoded)
    if type(output) is not bytes or len(output) > 16384:
        raise ValueError("registry_network_evidence_invalid")
    document = json.loads(output)
    if type(document) is not dict or set(document) != {
        "network_id",
        "expected_record_revision",
        "pricing_refs",
        "benefit_refs",
    }:
        raise ValueError("registry_network_evidence_invalid")
    return document


def _validated_location_address(address):
    """Retain six bounded native input fields, with an optional second line."""
    limits_by_field = {"first_line": 512, "second_line": 256, "city": 128, "state": 64, "zip": 32, "country": 64}
    if type(address) is not dict or set(address) != set(limits_by_field):
        raise ValueError("registry_location_address_fields_invalid")
    return {
        key: None
        if key == "second_line" and address[key] is None
        else _bounded_text(address[key], limit, "location_address")
        for key, limit in limits_by_field.items()
    }


def _canonical_location_address(address):
    """Require compatible native street identity; never use a Python fallback."""
    native = _fast_module()
    if native is None:
        raise RegistryAddressUnavailable("registry_location_address_native_unavailable")
    try:
        canonical = native.canonicalize_batch(
            [tuple(address[key] for key in ("first_line", "second_line", "city", "state", "zip", "country"))]
        )[0]
    except AttributeError, RuntimeError, TypeError, ValueError:
        raise RegistryAddressUnavailable("registry_location_address_native_unavailable") from None
    if not all(canonical.get(key) for key in ("address_key", "identity_key", "premise_key", "line1_norm", "city_norm")):
        raise ValueError("registry_location_address_invalid")
    return canonical


def _validated_company_links(fields):
    network_ids = fields["network_ids"]
    if (
        type(network_ids) is not list
        or len(network_ids) > 5000
        or any(type(network_id) is not int or not 1 <= network_id <= 2147483647 for network_id in network_ids)
        or len(set(network_ids)) != len(network_ids)
    ):
        raise ValueError("registry_company_links_networks_invalid")
    group_id = fields["group_id"]
    if group_id is not None:
        if type(group_id) is not str:
            raise ValueError("registry_company_links_group_invalid")
        parsed = _uuid(UUID(group_id))
        if str(parsed) != group_id:
            raise ValueError("registry_company_links_group_invalid")
        group_id = parsed
    validated_by_field = {"network_ids": sorted(network_ids), "group_id": group_id}
    if "network_assertions" in fields:
        assertions = fields["network_assertions"]
        if type(assertions) is not list or len(assertions) > 5000:
            raise ValueError("registry_company_assertions_invalid")
        if (
            len(json.dumps(assertions, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode())
            > MAX_INPUT_BYTES
        ):
            raise ValueError("registry_company_assertions_invalid")
        validated_by_field["network_assertions"] = assertions
    return validated_by_field


def _validated_network_assertions(assertions):
    """Require bounded native relationship validation with explicit conflict review."""
    native = _fast_module()
    if native is None or not callable(getattr(native, "validate_company_network_assertions", None)):
        raise RegistryAddressUnavailable("registry_company_assertions_native_unavailable")
    encoded = json.dumps(assertions, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if len(encoded) > MAX_INPUT_BYTES:
        raise ValueError("registry_company_assertions_invalid")
    validated = json.loads(native.validate_company_network_assertions(encoded))
    if validated["conflicts"]:
        raise ValueError("registry_company_assertions_review_required")
    return validated["assertions"]


def _validated_command(command):
    if (
        not isinstance(command, RegistryRecordCommand)
        or not isinstance(command.record_kind, str)
        or command.record_kind not in _RECORD_MODELS
        or command.record_kind == "network_binding"
    ):
        raise ValueError("registry_record_kind_invalid")
    if not isinstance(command.operation, str) or command.operation not in {"create", "correct", "archive", "restore"}:
        raise ValueError("registry_operation_invalid")
    if type(command.expected_revision) is not int or not 0 <= command.expected_revision < 9223372036854775807:
        raise ValueError("registry_expected_revision_invalid")
    if command.operation == "create" and command.expected_revision != 0:
        raise ValueError("registry_creation_revision_invalid")
    if command.record_kind == "network" and command.operation == "create":
        if command.record_id is not None:
            raise ValueError("registry_network_create_requires_allocation_key")
        _uuid(command.allocation_key)
        if isinstance(command.fields, dict) and "catalog_evidence_json" in command.fields:
            raise ValueError("registry_network_create_evidence_forbidden")
    else:
        _record_identity(command.record_kind, command.record_id)
        if command.allocation_key is not None:
            raise ValueError("registry_allocation_key_not_supported")
    _bounded_text(command.reason, 1000, "reason")
    if _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key:
        raise ValueError("registry_idempotency_key_invalid")
    if command.operation in {"create", "correct"}:
        fields = _validated_fields(command.record_kind, command.fields)
        if command.record_kind == "network" and fields.get("catalog_evidence_json") is not None:
            evidence = fields["catalog_evidence_json"]
            if (evidence["network_id"], evidence["expected_record_revision"]) != (
                command.record_id,
                command.expected_revision,
            ):
                raise ValueError("registry_network_evidence_context_invalid")
        return fields
    if not isinstance(command.fields, dict) or command.fields:
        raise ValueError("registry_archive_fields_must_be_empty")
    return {}


def _table(model, schema):
    table = model.__table__
    return table if schema is None else table.to_metadata(MetaData(), schema=schema)


def _record_table(kind, schema):
    if not isinstance(kind, str) or kind not in _RECORD_MODELS:
        raise ValueError("registry_record_kind_invalid")
    model, identity_column, _ = _RECORD_MODELS[kind]
    return _table(model, schema), identity_column


def _snapshot(row):
    snapshot_by_field = {
        key: str(value) if isinstance(value, UUID) else value.isoformat() if isinstance(value, datetime) else value
        for key, value in row.items()
    }
    if "catalog_evidence_json" in snapshot_by_field:
        _validate_network_evidence_snapshot(snapshot_by_field)
    return snapshot_by_field


def _validate_network_evidence_snapshot(snapshot):
    """Keep retained authoring context unchanged while refusing substitution."""
    evidence = snapshot.get("catalog_evidence_json")
    if evidence is None:
        return
    if (
        type(evidence) is not dict
        or type(evidence.get("network_id")) is not int
        or evidence["network_id"] != snapshot.get("network_id")
        or type(evidence.get("expected_record_revision")) is not int
        or type(snapshot.get("revision")) is not int
        or not 0 < evidence["expected_record_revision"] < snapshot["revision"]
    ):
        raise ValueError("registry_network_evidence_context_invalid")


def _history_result(history):
    record = history["record_json"]
    if history["record_kind"] == "network":
        _validate_network_evidence_snapshot(record)
    return {
        "record_kind": history["record_kind"],
        "record_id": record[_RECORD_MODELS[history["record_kind"]][1]],
        "revision": history["revision"],
        "custom_revision": history["custom_revision"],
        "record": record,
    }


async def _write_record_head(session, command, table, identity_column, record_id, fields):
    if command.operation == "create":
        statement = (
            pg_insert(table)
            .values(**{identity_column: record_id}, **fields, archived=False, revision=1)
            .on_conflict_do_nothing(index_elements=[table.c[identity_column]])
        )
    else:
        values_by_column = {**fields, "revision": command.expected_revision + 1}
        if command.operation in {"archive", "restore"}:
            values_by_column["archived"] = command.operation == "archive"
        statement = (
            update(table)
            .where(table.c[identity_column] == record_id, table.c.revision == command.expected_revision)
            .values(**values_by_column)
        )
    try:
        row = (await session.execute(statement.returning(*table.c))).mappings().one_or_none()
    except IntegrityError as error:
        native_error = error.orig.__cause__
        if (
            command.record_kind == "provider"
            and getattr(native_error, "constraint_name", None) == "manual_provider_npi_unique"
        ):
            raise RegistryRecordConflict("registry_provider_npi_claimed") from error
        raise
    if row is None:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    return _snapshot(row)


async def _append_record_history(session, command, actor, record, control, history_table, request_sha256):
    custom_revision = await session.scalar(
        update(control)
        .where(control.c.id == 1)
        .values(draft_revision=control.c.draft_revision + 1)
        .returning(control.c.draft_revision)
    )
    identity_column = _RECORD_MODELS[command.record_kind][1]
    history_by_column = {
        "record_kind": command.record_kind,
        "record_key": str(record[identity_column]),
        "revision": record["revision"],
        "custom_revision": custom_revision,
        "record_json": record,
        "actor_json": actor,
        "reason": command.reason.strip(),
        "idempotency_key": command.idempotency_key,
        "request_sha256": request_sha256,
    }
    await session.execute(insert(history_table).values(**history_by_column))
    return _history_result(history_by_column)


async def _read_record_replay(session, history_table, command, record_id):
    statement = select(history_table).where(
        history_table.c.record_kind == command.record_kind,
        history_table.c.record_key == str(record_id),
        history_table.c.idempotency_key == command.idempotency_key,
    )
    return (await session.execute(statement)).mappings().one_or_none()


async def _resolve_creation_identity(session, command, schema):
    if command.record_kind != "network" or command.operation != "create":
        return command.record_id
    identity_table = _table(NetworkRegistryIdentity, schema)
    existing_id = await session.scalar(
        select(identity_table.c.network_id).where(identity_table.c.allocation_key == command.allocation_key)
    )
    if existing_id is not None:
        return existing_id
    return (await allocate_network_ids(session, [command.allocation_key], schema=schema))[command.allocation_key]


def _company_assertion_scope(links, command, fields, network_ids):
    """Validate explicit assertion targets without expanding a legacy correction."""
    retained_assertions = (
        select(links.c.network_assertions)
        .where(links.c.company_id == command.record_id, links.c.revision == command.expected_revision)
        .scalar_subquery()
    )
    assertions = (
        literal(fields.get("network_assertions", []), type_=links.c.network_assertions.type)
        if command.operation == "create" or "network_assertions" in fields
        else func.coalesce(retained_assertions, literal([], type_=links.c.network_assertions.type))
    )
    entries = func.jsonb_array_elements(assertions).table_valued(column("value", links.c.network_assertions.type))
    company_id = entries.c.value["company_id"].astext
    network_id = cast(entries.c.value["network_id"].astext, Integer)
    return (
        ~select(literal(1))
        .select_from(entries)
        .where(
            or_(
                company_id.is_distinct_from(str(command.record_id)),
                network_id.is_(None),
                ~(network_id == any_(network_ids)),
            )
        )
        .exists()
    )


async def _validate_company_link_targets(session, command, fields, schema):
    """Validate active references and allocated networks together under the edit lock."""
    if "network_assertions" in fields:
        fields["network_assertions"] = _validated_network_assertions(fields["network_assertions"])
    companies = _table(CompanyRegistry, schema)
    groups = _table(CompanyGroupRegistry, schema)
    networks = _table(NetworkRegistryRecord, schema)
    identities = _table(NetworkRegistryIdentity, schema)
    links = _table(CompanyRegistryLinks, schema)
    if command.operation in {"create", "correct"}:
        network_ids = literal(fields["network_ids"], type_=ARRAY(Integer))
        group_id = literal(fields["group_id"], type_=PG_UUID(as_uuid=True))
    else:
        links = _table(CompanyRegistryLinks, schema)
        retained = select(links.c.network_ids, links.c.group_id).where(links.c.company_id == command.record_id).cte()
        network_ids, group_id = retained.c.network_ids, retained.c.group_id
    company_active = (
        select(companies.c.company_id)
        .where(companies.c.company_id == command.record_id, companies.c.archived.is_(False))
        .exists()
    )
    group_active = select(groups.c.group_id).where(groups.c.group_id == group_id, groups.c.archived.is_(False)).exists()
    active_network_count = (
        select(func.count())
        .select_from(networks.join(identities, identities.c.network_id == networks.c.network_id))
        .where(networks.c.network_id == any_(network_ids), networks.c.archived.is_(False))
        .scalar_subquery()
    )
    valid = await session.scalar(
        select(
            and_(
                company_active,
                or_(group_id.is_(None), group_active),
                active_network_count == func.cardinality(network_ids),
                _company_assertion_scope(links, command, fields, network_ids),
            )
        )
    )
    if valid is None and command.operation not in {"create", "correct"}:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    if valid is not True:
        raise ValueError("registry_company_links_target_invalid")


async def apply_registry_record_command(session, command, actor, *, schema=None, source_schema=None):
    """Atomically retain one draft in the caller transaction and a savepoint.

    Retries must retain the caller's UUID or network allocation_key. Replay is
    record-scoped and returns its original snapshot even after later changes.
    """
    fields = _validated_command(command)
    actor_document = _validated_actor(actor)
    if command.record_kind == "company" and "assertions" in fields:
        if (fields["assertions"]["company_id"], fields["assertions"]["expected_revision"]) != (
            str(command.record_id),
            command.expected_revision,
        ):
            raise ValueError("registry_company_assertion_context_invalid")
    if not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValueError("registry_requires_clean_caller_transaction")
    request = _command_request(command, actor_document)
    request_sha256 = hashlib.sha256(request.encode()).hexdigest()
    table, identity_column = _record_table(command.record_kind, schema)
    control = _table(RegistryRevisionControl, schema)
    history_table = _table(RegistryRecordHistory, schema)
    async with session.begin_nested():
        # ponytail: one control lock for low-volume edits; partition only if measured contention requires it.
        control_row = await session.scalar(select(control.c.id).where(control.c.id == 1).with_for_update())
        if control_row != 1:
            raise RegistryRecordConflict("registry_control_unavailable")
        record_id = await _resolve_creation_identity(session, command, schema)
        replay = await _read_record_replay(session, history_table, command, record_id)
        if replay is not None:
            if replay["request_sha256"] != request_sha256:
                raise RegistryRecordConflict("registry_idempotency_conflict")
            return _history_result(replay)
        if command.record_kind == "company_links":
            await _validate_company_link_targets(session, command, fields, schema)
        if command.record_kind == "membership" and command.operation != "archive":
            await _validate_membership_targets(session, command, fields, schema, source_schema)
        if command.record_kind == "provider":
            await _validate_provider_npi_claim(session, command, fields, schema, source_schema)
        if command.record_kind == "location" and command.operation in {"create", "correct"}:
            fields["canonical_address_json"] = _canonical_location_address(fields["address_json"])
        if command.record_kind == "site_binding" and command.operation != "archive":
            fields = await _resolve_site_binding(session, command, fields, schema)
        assertions = None
        if command.record_kind == "company":
            assertions = await _company_revision_assertions(session, command, fields, history_table)
            if command.operation != "archive":
                await _validate_company_assertions(session, command, assertions, schema)
        record_snapshot = await _write_record_head(session, command, table, identity_column, record_id, fields)
        if assertions is not None:
            await _retain_company_assertions(session, record_snapshot, assertions, schema)
            record_snapshot.update(assertions)
        return await _append_record_history(
            session, command, actor_document, record_snapshot, control, history_table, request_sha256
        )


async def _resolve_site_binding(session, command, fields_by_name, schema):
    retained_receipt = None
    if command.operation == "restore":
        table = _table(RegistrySiteBinding, schema)
        retained = (
            (
                await session.execute(
                    select(table).where(
                        table.c.binding_id == command.record_id, table.c.revision == command.expected_revision
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if retained is None:
            raise RegistryRecordConflict("registry_record_revision_conflict")
        retained_receipt = retained["source_receipt_json"]
        fields_by_name = {
            key: str(retained[key]) if isinstance(retained[key], UUID) else retained[key]
            for key in _RECORD_MODELS["site_binding"][2]
        }
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    resolved = await resolve_site_binding_fields(driver, fields_by_name, control_schema=schema)
    if retained_receipt is not None and retained_receipt != resolved["source_receipt_json"]:
        raise RegistryRecordConflict("registry_site_binding_source_conflict")
    return resolved


def _command_request(command, actor_document):
    """Preserve exact JSON membership content while encoding command UUIDs."""
    command_document = _snapshot(asdict(command))
    try:
        return json.dumps({"command": command_document, "actor": actor_document}, sort_keys=True, allow_nan=False)
    except TypeError, ValueError:
        raise ValueError("registry_command_json_invalid") from None


async def _validate_membership_targets(session, command, fields, schema, source_schema):
    """Encode once after replay, then validate every reference in one native set query."""
    if command.operation == "restore":
        memberships = _table(NetworkMembershipDraft, schema)
        membership_rows = await session.scalar(
            select(memberships.c.memberships_json).where(
                memberships.c.network_id == command.record_id, memberships.c.revision == command.expected_revision
            )
        )
        if membership_rows is None:
            raise RegistryRecordConflict("registry_record_revision_conflict")
    else:
        membership_rows = fields["memberships_json"]
    input_bytes = json.dumps(membership_rows, separators=(",", ":"), ensure_ascii=False).encode()
    if len(input_bytes) > MAX_INPUT_BYTES:
        raise ValueError("registry_membership_rows_invalid")
    try:
        await asyncio.to_thread(_encode, input_bytes)
    except MembershipCopyError as error:
        if str(error) == "Native membership encoder is unavailable":
            raise RegistryAddressUnavailable("registry_membership_native_unavailable") from None
        raise ValueError("registry_membership_rows_invalid") from None
    except TypeError, ValueError:
        raise ValueError("registry_membership_rows_invalid") from None
    preparer = session.bind.dialect.identifier_preparer
    relations_by_name = {
        name: preparer.format_table(_table(model, namespace))
        for name, model, namespace in (
            ("networks", NetworkRegistryRecord, schema),
            ("identities", NetworkRegistryIdentity, schema),
            ("providers", ManualProviderRegistry, schema),
            ("locations", ManualLocationRegistry, schema),
            ("bindings", ManualProviderLocationBinding, schema),
            ("source_bindings", RegistrySiteBinding, schema),
            ("npis", NPIData, source_schema),
        )
    }
    valid = await session.scalar(
        text(_MEMBERSHIP_TARGETS_SQL.format(**relations_by_name)),
        {"network_id": command.record_id, "memberships_json": input_bytes.decode()},
    )
    if valid is not True:
        raise ValueError("registry_membership_target_invalid")


_MEMBERSHIP_TARGETS_SQL = """WITH members AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(CAST(:memberships_json AS jsonb)) AS member(
    network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), checked AS (
  SELECT member.*,
    EXISTS (SELECT 1 FROM {bindings} binding WHERE NOT binding.archived
      AND binding.provider_system=member.provider_system AND binding.provider_id=member.provider_id
      AND binding.location_id=member.location_id) AS has_binding,
    EXISTS (SELECT 1 FROM {source_bindings} binding WHERE NOT binding.archived
      AND binding.provider_system=member.provider_system AND binding.provider_id=member.provider_id
      AND binding.location_id=member.location_id) AS has_source_binding
  FROM members member
)
SELECT EXISTS (SELECT 1 FROM {networks} network JOIN {identities} identity USING(network_id)
  WHERE network.network_id=:network_id AND NOT network.archived)
AND NOT EXISTS (SELECT 1 FROM members WHERE network_id<>:network_id)
AND NOT EXISTS (SELECT 1 FROM members GROUP BY provider_system,provider_id,location_id HAVING count(*)>1)
AND NOT EXISTS (SELECT 1 FROM checked member WHERE
  NOT (CASE member.provider_system
    WHEN 'manual' THEN EXISTS (SELECT 1 FROM {providers} provider WHERE NOT provider.archived
      AND provider.provider_id=CASE WHEN member.provider_id ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
        THEN member.provider_id::uuid END)
    WHEN 'npi' THEN member.has_source_binding OR EXISTS (SELECT 1 FROM {npis} npi WHERE npi.npi=member.provider_id::bigint)
    WHEN 'provider_directory' THEN member.has_binding OR member.has_source_binding
    ELSE false END)
  OR NOT (member.has_binding OR member.has_source_binding OR EXISTS (SELECT 1 FROM {locations} location
    WHERE location.location_id=member.location_id AND NOT location.archived)))
"""


async def _validate_provider_npi_claim(session, command, fields, schema, source_schema):
    """Require explicit source reuse and one durable manual claim under the draft lock."""
    if command.operation == "archive" or (command.operation in {"create", "correct"} and fields["npi"] is None):
        return
    providers = _table(ManualProviderRegistry, schema)
    source = _table(NPIData, source_schema)
    npi = (
        select(providers.c.npi).where(providers.c.provider_id == command.record_id).scalar_subquery()
        if command.operation == "restore"
        else literal(fields["npi"], type_=String(10))
    )
    source_exists = select(source.c.npi).where(source.c.npi == cast(npi, BigInteger)).exists()
    manual_exists = (
        select(providers.c.provider_id)
        .where(providers.c.npi == npi, providers.c.provider_id != command.record_id)
        .exists()
    )
    source_claimed, manual_claimed = (await session.execute(select(source_exists, manual_exists))).one()
    if source_claimed:
        raise RegistryRecordConflict("registry_provider_npi_reuse_required")
    if manual_claimed:
        raise RegistryRecordConflict("registry_provider_npi_claimed")


async def get_registry_record(session, record_kind, record_id, *, schema=None):
    """Read a management draft head; public readers use approved composition."""
    table, identity_column = _record_table(record_kind, schema)
    record_id = _record_identity(record_kind, record_id)
    row = (await session.execute(select(table).where(table.c[identity_column] == record_id))).mappings().one_or_none()
    records = [] if row is None else [_snapshot(row)]
    if record_kind == "company":
        await _attach_company_assertions(session, records, schema)
    return records[0] if records else None


async def list_registry_records(session, record_kind, *, limit=50, offset=0, record_ids=None, schema=None):
    """Bounded management-only heads, ordered by durable identity."""
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 1000000:
        raise ValueError("registry_page_invalid")
    table, identity_column = _record_table(record_kind, schema)
    statement = select(table)
    if record_ids is not None:
        if type(record_ids) not in {list, tuple} or len(record_ids) > 100:
            raise ValueError("registry_selector_invalid")
        identities = tuple(_record_identity(record_kind, value) for value in record_ids)
        if len(set(identities)) != len(identities):
            raise ValueError("registry_selector_duplicate")
        statement = statement.where(table.c[identity_column].in_(identities))
    rows = (await session.execute(statement.order_by(table.c[identity_column]).limit(limit).offset(offset))).mappings()
    records = [_snapshot(row) for row in rows]
    if record_kind == "company":
        await _attach_company_assertions(session, records, schema)
    return records


async def _company_revision_assertions(session, command, fields, history):
    document = fields.pop("assertions", None)
    if document is not None:
        return {key: document[key] for key in ("role_assertions", "identifier_assertions")}
    if command.operation == "create":
        return {"role_assertions": [], "identifier_assertions": []}
    retained = await session.scalar(
        select(history.c.record_json).where(
            history.c.record_kind == "company",
            history.c.record_key == str(command.record_id),
            history.c.revision == command.expected_revision,
        )
    )
    if retained is None:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    return {key: retained.get(key, []) for key in ("role_assertions", "identifier_assertions")}


_COMPANY_ASSERTION_TARGETS_SQL = """WITH proposed AS MATERIALIZED (
  SELECT value AS assertion FROM jsonb_array_elements(CAST(:roles AS jsonb))
  UNION ALL SELECT value FROM jsonb_array_elements(CAST(:identifiers AS jsonb))
), identifiers AS MATERIALIZED (
  SELECT value AS assertion FROM jsonb_array_elements(CAST(:identifiers AS jsonb))
), current_claims AS MATERIALIZED (
  SELECT retained.* FROM {namespace}.company_registry_identifier_assertion retained
  JOIN {namespace}.company_registry company
    ON (company.company_id,company.revision)=(retained.company_id,retained.company_revision)
  WHERE NOT company.archived
  UNION
  SELECT retained.* FROM {namespace}.company_registry_identifier_assertion retained
  JOIN {namespace}.registry_approved_record approved
    ON approved.record_kind='company' AND approved.record_key=retained.company_id::text
      AND approved.record_revision=retained.company_revision
  JOIN {namespace}.registry_revision_control control
    ON control.id=1 AND control.approved_revision=approved.approved_revision
  WHERE approved.record_json->'archived'='false'::jsonb
)
SELECT NOT EXISTS (
  SELECT 1 FROM proposed p WHERE p.assertion->'provenance'->>'kind'='source_reference' AND NOT EXISTS (
    SELECT 1 FROM {namespace}.registry_source_snapshot snapshot
    JOIN {namespace}.registry_source_observation observation USING(snapshot_id)
    WHERE snapshot.snapshot_id=(p.assertion->'provenance'->>'snapshot_id')::uuid
      AND observation.source_record_key=p.assertion->'provenance'->>'source_record_key'
      AND observation.status IN ('accepted','unresolved'))
) AND NOT EXISTS (
  SELECT 1 FROM identifiers p WHERE EXISTS (
    SELECT 1 FROM current_claims retained
    WHERE retained.company_id<>CAST(:company_id AS uuid)
      AND (retained.identifier_system,retained.identifier_scope,retained.identifier_value)=
        (p.assertion->>'identifier_system',p.assertion->>'identifier_scope',p.assertion->>'identifier_value')
      AND retained.valid_from<=COALESCE((p.assertion->>'valid_to')::date,'infinity'::date)
      AND COALESCE(retained.valid_to,'infinity'::date)>=(p.assertion->>'valid_from')::date)
    OR EXISTS (
      SELECT 1 FROM {namespace}.registry_identifier_binding binding
      WHERE binding.entity_kind='company' AND binding.entity_id<>CAST(:company_id AS uuid)
        AND (binding.identifier_system,binding.identifier_value)=
          (p.assertion->>'identifier_system',p.assertion->>'identifier_value'))
    OR (p.assertion->'provenance'->>'kind'='source_reference'
      AND p.assertion->>'identifier_system' IN ('ein','naic_company') AND NOT EXISTS (
        SELECT 1 FROM {namespace}.registry_identifier_observation observation
        WHERE observation.snapshot_id=(p.assertion->'provenance'->>'snapshot_id')::uuid
          AND observation.source_record_key=p.assertion->'provenance'->>'source_record_key'
          AND observation.entity_kind='company'
          AND (observation.identifier_system,observation.identifier_value)=
            (p.assertion->>'identifier_system',p.assertion->>'identifier_value')
          AND observation.resolution_status<>'conflicting'
          AND (observation.entity_id IS NULL OR observation.entity_id=CAST(:company_id AS uuid))))
)"""


async def _validate_company_assertions(session, command, assertions, schema):
    if not any(assertions.values()):
        return
    from process.registry_source_observation_store import _namespace

    valid = await session.scalar(
        text(_COMPANY_ASSERTION_TARGETS_SQL.format(namespace=_namespace(schema))),
        {
            "company_id": command.record_id,
            "roles": json.dumps(assertions["role_assertions"]),
            "identifiers": json.dumps(assertions["identifier_assertions"]),
        },
    )
    if valid is not True:
        raise RegistryRecordConflict("registry_company_assertion_target_conflict")


def _company_assertion_row(assertion, company_id, revision):
    provenance = assertion["provenance"]
    return {key: value for key, value in assertion.items() if key not in {"provenance", "valid_from", "valid_to"}} | {
        "assertion_id": UUID(assertion["assertion_id"]),
        "company_id": UUID(company_id),
        "company_revision": revision,
        "valid_from": date.fromisoformat(assertion["valid_from"]),
        "valid_to": None if assertion["valid_to"] is None else date.fromisoformat(assertion["valid_to"]),
        "provenance_kind": provenance["kind"],
        "evidence_ref": provenance["evidence_ref"],
        "source_snapshot_id": None if provenance["snapshot_id"] is None else UUID(provenance["snapshot_id"]),
        "source_record_key": provenance["source_record_key"],
    }


async def _retain_company_assertions(session, record, assertions, schema):
    for model, key in (
        (CompanyRegistryRoleAssertion, "role_assertions"),
        (CompanyRegistryIdentifierAssertion, "identifier_assertions"),
    ):
        rows = [_company_assertion_row(item, record["company_id"], record["revision"]) for item in assertions[key]]
        if rows:
            await session.execute(insert(_table(model, schema)), rows)


def _company_assertion_document(row):
    excluded_columns = {
        "company_id",
        "company_revision",
        "created_at",
        "provenance_kind",
        "evidence_ref",
        "source_snapshot_id",
        "source_record_key",
    }
    return {
        key: value.isoformat() if isinstance(value, date) else str(value) if isinstance(value, UUID) else value
        for key, value in row.items()
        if key not in excluded_columns
    } | {
        "provenance": {
            "kind": row["provenance_kind"],
            "evidence_ref": row["evidence_ref"],
            "snapshot_id": None if row["source_snapshot_id"] is None else str(row["source_snapshot_id"]),
            "source_record_key": row["source_record_key"],
        }
    }


async def _attach_company_assertions(session, records, schema):
    if not records:
        return
    by_identity = {(UUID(row["company_id"]), row["revision"]): row for row in records}
    for model, key in (
        (CompanyRegistryRoleAssertion, "role_assertions"),
        (CompanyRegistryIdentifierAssertion, "identifier_assertions"),
    ):
        for record in records:
            record[key] = []
        table = _table(model, schema)
        rows = (
            await session.execute(
                select(table)
                .where(tuple_(table.c.company_id, table.c.company_revision).in_(tuple(by_identity)))
                .order_by(table.c.assertion_id)
            )
        ).mappings()
        for row in rows:
            by_identity[(row["company_id"], row["company_revision"])][key].append(_company_assertion_document(row))


def _company_record_json_sql(namespace):
    """Compose the exact current revision without laundering legacy history rows."""
    parts = []
    for table, key in (
        ("company_registry_role_assertion", "role_assertions"),
        ("company_registry_identifier_assertion", "identifier_assertions"),
    ):
        document = """to_jsonb(assertion)-ARRAY['company_id','company_revision','created_at','provenance_kind',
          'evidence_ref','source_snapshot_id','source_record_key']||jsonb_build_object('provenance',jsonb_build_object(
          'kind',assertion.provenance_kind,'evidence_ref',assertion.evidence_ref,
          'snapshot_id',assertion.source_snapshot_id,'source_record_key',assertion.source_record_key))"""
        parts.append(
            f"'{key}',COALESCE((SELECT jsonb_agg({document} ORDER BY assertion.assertion_id) "
            f"FROM {namespace}.{table} assertion WHERE assertion.company_id=head.company_id "
            "AND assertion.company_revision=head.revision),'[]'::jsonb)"
        )
    return "to_jsonb(head)||jsonb_build_object(" + ",".join(parts) + ")"
