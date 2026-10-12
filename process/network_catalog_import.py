# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adopt one reviewed source alias batch and seed its missing named drafts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from db.connection import Database
from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
from process.network_address_projection import _identifier
from process.network_catalog_store import (
    MAX_CATALOG_BYTES,
    NetworkCatalogDraftRow,
    NetworkCatalogSeedError,
    NetworkCatalogSourceContext,
    seed_network_catalog_drafts,
)
from process.network_legacy_alias_adoption import (
    MAX_ADOPTION_ROWS,
    LegacyNetworkAdoptionRow,
    adopt_legacy_network_aliases,
)
from process.registry_management_permissions import (
    assume_registry_source_owner,
    verify_registry_management_permissions,
)
from process.registry_record_store import RegistryActor, _bounded_text, _validated_actor, _validated_fields
from process.registry_source_import import _disconnect_database, _error_json, _ReceiptArgumentParser

_ALIAS_FIELDS = {"source_system", "source_id", "scope_key", "evidence_id", "legacy_uuid", "reviewed_network_id"}


def _prepare_input(catalog_rows, actor, source_context):
    if type(catalog_rows) not in {list, tuple} or not 0 < len(catalog_rows) <= MAX_ADOPTION_ROWS:
        raise ValueError("Network catalog batch exceeds row limit")
    if len(json.dumps(catalog_rows, ensure_ascii=False, separators=(",", ":")).encode()) > MAX_CATALOG_BYTES:
        raise ValueError("Network catalog input exceeds byte limit")
    if type(source_context) is not NetworkCatalogSourceContext:
        raise ValueError("Network catalog requires an explicit source context")
    if _validated_actor(actor)["kind"] != "platform_admin":
        raise ValueError("Network catalog requires a platform administrator source operator")
    _bounded_text(source_context.reason, 1000, "reason")
    _bounded_text(source_context.idempotency_key, 115, "idempotency_key")
    prepared_rows = []
    for catalog_row in catalog_rows:
        if type(catalog_row) is not dict or set(catalog_row) != _ALIAS_FIELDS | {"display_name", "aliases"}:
            raise ValueError("Network catalog row fields are invalid")
        alias = LegacyNetworkAdoptionRow(**{field: catalog_row[field] for field in _ALIAS_FIELDS})
        if (alias.source_system, alias.source_id, alias.scope_key) != (
            source_context.source_system,
            source_context.source_id,
            source_context.alias_scope,
        ):
            raise ValueError("Network catalog source context differs from its alias")
        fields_by_name = _validated_fields(
            "network", {"display_name": catalog_row["display_name"], "aliases": catalog_row["aliases"]}
        )
        prepared_rows.append((alias, fields_by_name))
    return prepared_rows


async def import_network_catalog(session, catalog_rows, actor, source_context, *, api_role, owner_role, control_schema):
    """Keep alias allocation, missing draft heads and initial history atomic."""
    prepared_rows = _prepare_input(catalog_rows, actor, source_context)
    if not session.in_transaction():
        raise ValueError("Network catalog requires a caller-owned transaction")
    caller_role = await session.scalar(text("SELECT quote_ident(current_user)"))
    connection = (await (await session.connection()).get_raw_connection()).driver_connection
    try:
        async with session.begin_nested():
            await verify_registry_management_permissions(
                connection, api_role=api_role, owner_role=owner_role, control_schema=control_schema
            )
            await assume_registry_source_owner(connection, owner_role, control_schema=control_schema)
            # Serialize before adoption takes write locks needed by the later catalog validation.
            control = await connection.fetchrow(
                f"SELECT id FROM {_identifier(control_schema)}.registry_revision_control WHERE id=1 FOR UPDATE"
            )
            if control is None:
                raise NetworkCatalogSeedError("Network catalog revision control is missing")
            adopted_aliases = await adopt_legacy_network_aliases(
                session, [alias for alias, _fields in prepared_rows], schema=control_schema
            )
            draft_rows = [
                NetworkCatalogDraftRow(alias, adopted.network_id, **fields_by_name)
                for (alias, fields_by_name), adopted in zip(prepared_rows, adopted_aliases, strict=True)
            ]
            return await seed_network_catalog_drafts(
                connection,
                draft_rows,
                actor,
                source_context,
                api_role=api_role,
                owner_role=owner_role,
                control_schema=control_schema,
            )
    finally:
        primary_error = sys.exception()
        try:
            await connection.execute(f"SET LOCAL ROLE {caller_role}")
        except BaseException:
            if primary_error is not None:
                raise primary_error from None
            raise


def _parser():
    parser = _ReceiptArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--actor-user-id", type=UUID, required=True)
    for field in (
        "input-sha256",
        "source-system",
        "source-id",
        "alias-scope",
        "reason",
        "idempotency-key",
        "actor-client-id",
        "api-role",
        "owner-role",
        "control-schema",
    ):
        parser.add_argument("--" + field, required=True)
    return parser


def _unique_fields(field_pairs):
    fields_by_name = {}
    for name, field_value in field_pairs:
        if name in fields_by_name:
            raise ValueError("Duplicate catalog field")
        fields_by_name[name] = field_value
    return fields_by_name


def _read_input(parsed):
    with parsed.input_file.open("rb") as input_stream:
        input_bytes = input_stream.read(MAX_CATALOG_BYTES + 1)
    if len(input_bytes) > MAX_CATALOG_BYTES:
        raise ValueError("Network catalog input exceeds byte limit")
    if hashlib.sha256(input_bytes).hexdigest() != parsed.input_sha256:
        raise ValueError("Network catalog input digest differs")
    catalog_rows = json.loads(input_bytes, object_pairs_hook=_unique_fields)
    actor = RegistryActor("platform_admin", parsed.actor_user_id, parsed.actor_client_id)
    source_context = NetworkCatalogSourceContext(
        parsed.source_system, parsed.source_id, parsed.alias_scope, parsed.reason, parsed.idempotency_key
    )
    _prepare_input(catalog_rows, actor, source_context)
    return catalog_rows, actor, source_context


async def _run_import(parsed, catalog_rows, actor, source_context):
    database = Database()
    with _receipt_only_database_output(database):
        try:
            await database.connect()
            _set_engine_echo(database, False)
            async with database.transaction() as session:
                receipt_by_name = await import_network_catalog(
                    session,
                    catalog_rows,
                    actor,
                    source_context,
                    api_role=parsed.api_role,
                    owner_role=parsed.owner_role,
                    control_schema=parsed.control_schema,
                )
                receipt = json.dumps(
                    {"status": "ok", "input_sha256": parsed.input_sha256, **receipt_by_name},
                    sort_keys=True,
                    separators=(",", ":"),
                )
            return receipt
        finally:
            await _disconnect_database(database)


def run_command(arguments=None):
    """Use existing operator credentials; emit only one bounded receipt."""
    parsed = _parser().parse_args(arguments)
    try:
        catalog_rows, actor, source_context = _read_input(parsed)
    except OSError:
        print(_error_json("input_unavailable"), file=sys.stderr)
        return 1
    except ValueError, TypeError, RecursionError:
        print(_error_json("invalid_catalog_input"), file=sys.stderr)
        return 2
    try:
        receipt = asyncio.run(_run_import(parsed, catalog_rows, actor, source_context))
    except KeyboardInterrupt, asyncio.CancelledError:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except Exception:
        print(_error_json("catalog_import_failed"), file=sys.stderr)
        return 1
    print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
