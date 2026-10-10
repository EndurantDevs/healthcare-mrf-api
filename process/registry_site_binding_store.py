# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate one reviewed binding and resolve its proof from retained native rows."""

from __future__ import annotations

import json
import re
from uuid import UUID

from process.network_serving_read import resolve_network_serving_manifest
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions

_EDITABLE_FIELDS = frozenset(
    {"source_generation", "provider_system", "provider_id", "location_id", "location_key", "address_row_sha256"}
)


def validated_site_binding_fields(fields: dict) -> dict:
    """Return the exact canonical wire fields without reading or changing a database."""
    if type(fields) is not dict or set(fields) != _EDITABLE_FIELDS:
        raise ValueError("registry_site_binding_fields_invalid")
    source_generation = fields["source_generation"]
    provider_system, provider_id = fields["provider_system"], fields["provider_id"]
    if (
        type(source_generation) is not int
        or not 0 < source_generation <= 9223372036854775807
        or type(provider_system) is not str
        or provider_system not in ("npi", "provider_directory")
        or type(provider_id) is not str
        or not 1 <= len(provider_id) <= 128
        or provider_id.strip() != provider_id
        or not provider_id.isprintable()
        or (provider_system == "npi" and not is_valid_npi(provider_id))
        or any(
            type(fields[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", fields[field]) is None
            for field in ("location_key", "address_row_sha256")
        )
    ):
        raise ValueError("registry_site_binding_fields_invalid")
    try:
        location_id = UUID(fields["location_id"]) if type(fields["location_id"]) is str else None
    except ValueError:
        location_id = None
    if location_id is None or location_id.int == 0 or str(location_id) != fields["location_id"]:
        raise ValueError("registry_site_binding_fields_invalid")
    return fields.copy()


async def resolve_site_binding_fields(connection, fields: dict, *, control_schema=None) -> dict:
    """Resolve only server-produced proof in the caller's repeatable transaction.

    The generic record store owns draft CAS, actor checks, history and writes.
    A retained generation must still be eligible and physically closed whenever
    these fields are resolved. No caller-supplied receipt or live binding is used.
    """
    fields_by_name = validated_site_binding_fields(fields)
    source = await resolve_network_serving_manifest(
        connection, generation_id=fields_by_name["source_generation"], control_schema=control_schema
    )
    input_bytes = json.dumps(
        [{field: value for field, value in fields_by_name.items() if field != "source_generation"}],
        separators=(",", ":"),
    ).encode()
    receipt = await resolve_retained_site_adoptions(connection, source, input_bytes, control_schema=control_schema)
    source_receipt_json = receipt.as_dict()
    if len(json.dumps(source_receipt_json, ensure_ascii=False).encode()) > 65536:
        raise RetainedSiteAdoptionError("registry_site_binding_receipt_invalid")
    return {
        **fields_by_name,
        "location_id": UUID(fields_by_name["location_id"]),
        "source_receipt_json": source_receipt_json,
    }
