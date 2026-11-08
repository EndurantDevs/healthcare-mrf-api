# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve current or independently sealed logical offices for source correspondence."""

import re
from dataclasses import asdict, dataclass

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _artifact_dependency_identity, _require_transaction
from process.network_membership_validation import _json_object
from process.registry_retained_generation import resolve_registry_retained_generation


@dataclass(frozen=True)
class ResolvedSourceOfficeOrigin:
    """Call-local SQL from native authentication; this DTO alone grants no authority.

    A retained identity describes the complete logical manifest scope. Its OIDs
    locate the protected candidate and base; its digest binds all logical heaps.
    """

    address_identity: tuple[int, int, int, str]
    relation_sql: str
    arguments: tuple
    origin_manifest_sha256: str | None


def source_office_pin_document(pin):
    """Keep legacy pin hashes exact while binding every declared origin locator."""
    document_by_field = asdict(pin)
    if document_by_field.get("origin_manifest_sha256") is None:
        document_by_field.pop("origin_manifest_sha256", None)
    return document_by_field


def _source(source, runtime_roles, locator, first_parameter):
    if type(source) is not PinnedAddressSource or source.table_name != "entity_address_unified":
        raise ValueError("registry_source_office_origin_invalid")
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or type(first_parameter) is not int
        or not 1 <= first_parameter < 32768
    ):
        raise ValueError("registry_source_office_origin_invalid")
    for role in runtime_roles:
        _identifier(role)
    if locator is not None and (type(locator) is not str or re.fullmatch("[0-9a-f]{64}", locator) is None):
        raise ValueError("registry_source_office_origin_invalid")


async def _retained(connection, address_source, runtime_roles, locator, control_schema, first_parameter):
    resolved = await resolve_registry_retained_generation(
        connection, {"source_generations": {"retained_network_source": locator}}, control_schema=control_schema
    )
    if resolved is None:
        raise ValueError("registry_source_office_origin_unavailable")
    manifest, candidate = resolved
    report = _json_object(candidate["validation_json"])
    descriptor = report.get("serving_readiness", {}).get("address_artifact")
    if (
        manifest.compact_layout is None
        or report.get("address_source") != asdict(address_source)
        or manifest.source_generations.get("unified_address") != address_source.generation_id
        or type(descriptor) is not dict
        or descriptor.get("runtime_roles") != list(runtime_roles)
    ):
        raise ValueError("registry_source_office_origin_differs")
    identity = await connection.fetchrow(
        "SELECT n.oid::bigint AS schema_oid,n.nspowner::bigint AS owner_oid FROM pg_namespace n WHERE n.nspname=$1",
        manifest.schema_name,
    )
    # Native custody checks the final owner; the producer closure can retain
    # the loading owner's name from before the independent transfer.
    if identity is None:
        raise ValueError("registry_source_office_origin_changed")
    from api.network_compact_address import compact_address_native_sql

    relation, arguments = compact_address_native_sql(
        manifest.compact_layout, first_parameter=first_parameter, visible_membership_sql="TRUE"
    )
    return ResolvedSourceOfficeOrigin(
        (identity["schema_oid"], manifest.address_table_oid, identity["owner_oid"], manifest.manifest_sha256),
        relation,
        arguments,
        locator,
    )


async def resolve_source_office_origin(
    connection, address_source, *, runtime_roles, control_schema, origin_manifest_sha256=None, first_parameter=1
):
    """Reauthenticate before each match/COPY; a locator never grants source authority."""
    _source(address_source, runtime_roles, origin_manifest_sha256, first_parameter)
    _identifier(control_schema)
    await _require_transaction(connection)
    if origin_manifest_sha256 is not None:
        return await _retained(
            connection, address_source, runtime_roles, origin_manifest_sha256, control_schema, first_parameter
        )
    identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
    return ResolvedSourceOfficeOrigin(
        identity,
        _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name),
        (),
        None,
    )
