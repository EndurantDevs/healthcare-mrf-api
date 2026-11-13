# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve named logical office snapshots for complete source correspondence."""

import re
from dataclasses import asdict, dataclass

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _require_transaction
from process.network_membership_validation import _json_object
from process.registry_retained_generation import resolve_registry_retained_generation


@dataclass(frozen=True)
class ResolvedSourceOfficeOrigin:
    address_source: PinnedAddressSource
    relation_sql: str
    arguments: tuple
    origin_manifest_sha256: str | None

    @property
    def address_scope(self):
        """Describe the selected logical address edition independently of physical storage."""
        return (
            self.address_source.schema_name,
            self.address_source.table_name,
            self.address_source.generation_id,
            self.origin_manifest_sha256,
        )


def source_office_pin_document(pin):
    """Serialize logical input data and its optional retained manifest locator."""
    document_by_field = asdict(pin)
    if document_by_field.get("origin_manifest_sha256") is None:
        document_by_field.pop("origin_manifest_sha256", None)
    return document_by_field


def _source(source, locator, first_parameter):
    if type(source) is not PinnedAddressSource or source.table_name != "entity_address_unified":
        raise ValueError("registry_source_office_origin_invalid")
    source.__post_init__()
    if type(first_parameter) is not int or not 1 <= first_parameter < 32768:
        raise ValueError("registry_source_office_origin_invalid")
    if locator is not None and (type(locator) is not str or re.fullmatch("[0-9a-f]{64}", locator) is None):
        raise ValueError("registry_source_office_origin_invalid")


async def _retained(connection, address_source, locator, control_schema, first_parameter):
    resolved = await resolve_registry_retained_generation(
        connection, {"source_generations": {"retained_network_source": locator}}, control_schema=control_schema
    )
    if resolved is None:
        raise ValueError("registry_source_office_origin_unavailable")
    manifest, candidate = resolved
    report = _json_object(candidate["validation_json"])
    if (
        report.get("address_source") != asdict(address_source)
        or manifest.source_generations.get("unified_address") != address_source.generation_id
        or manifest.manifest_sha256 != locator
    ):
        raise ValueError("registry_source_office_origin_differs")
    if manifest.compact_layout is None:
        relation = manifest.address_relation
        query, arguments = _identifier(relation.schema_name) + "." + _identifier(relation.table_name), ()
    else:
        from api.network_compact_address import compact_address_native_sql

        query, arguments = compact_address_native_sql(
            manifest.compact_layout, first_parameter=first_parameter, visible_membership_sql="TRUE"
        )
    return ResolvedSourceOfficeOrigin(address_source, query, arguments, locator)


async def resolve_source_office_origin(
    connection, address_source, *, runtime_roles, control_schema, origin_manifest_sha256=None, first_parameter=1
):
    """Resolve exact named source data in the caller's repeatable-read operation."""
    _source(address_source, origin_manifest_sha256, first_parameter)
    _identifier(control_schema)
    await _require_transaction(connection)
    if origin_manifest_sha256 is not None:
        return await _retained(connection, address_source, origin_manifest_sha256, control_schema, first_parameter)
    return ResolvedSourceOfficeOrigin(
        address_source,
        _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name),
        (),
        None,
    )
