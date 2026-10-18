# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replace legacy office associations with exact approved membership facts."""

import json
import re
from copy import deepcopy
from uuid import UUID

from sqlalchemy import text

from api.network_address_scope import (
    current_network_address_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_network_catalog_read import RegistryNetworkCatalogError

MAX_METADATA_BYTES = 32 * 1024 * 1024
MAX_MEMBERSHIP_FACTS = 100_000
_LEGACY_ASSOCIATIONS = {
    "aca_plan_array",
    "aca_network_array",
    "ptg_plan_array",
    "ptg_source_array",
    "group_plan_array",
    "plans_network_array",
    "canonical_network_ids",
    "source_record_ids",
    "phone_source_record_ids",
    "provider_directory_sources",
    "mrf_sources",
    "mrf_source_count",
    "network_refs",
    "insurance_plan_refs",
    "network_tier",
    "network_key_id",
    "plan_scope",
    "network_memberships",
}
_FACT_FIELDS = {"npi", "address_key", "network_id", "provider_system", "provider_id", "location_id", "evidence_id"}
_READ_SQL = """WITH requested_offices AS MATERIALIZED (
 SELECT * FROM unnest(CAST(:metadata_npis AS bigint[]),CAST(:metadata_address_keys AS uuid[]))
 AS requested(npi,address_key)
), offices AS MATERIALIZED (
 SELECT DISTINCT requested.npi,address.address_key,binding.provider_system,binding.provider_id,binding.location_id
 FROM requested_offices requested JOIN {addresses} address
 ON (coalesce(address.npi,address.inferred_npi),address.address_key)=(requested.npi,requested.address_key)
 JOIN {candidate}.provider_location_binding binding
 ON (binding.location_key,binding.entity_type,binding.entity_id)=
    (address.location_key,address.entity_type,address.entity_id)
), fact_rows AS MATERIALIZED (
 SELECT DISTINCT office.npi,office.address_key::text,member.network_id,member.provider_system,
   member.provider_id,member.location_id::text,member.evidence_id
 FROM offices office JOIN {candidate}.network_membership member
 ON (member.provider_system,member.provider_id,member.location_id)=
    (office.provider_system,office.provider_id,office.location_id)
 JOIN {control}.registry_approved_record network ON network.approved_revision=:metadata_revision
 AND network.record_kind='network' AND network.record_key=member.network_id::text
 AND network.record_json->'archived'='false'::jsonb
 AND jsonb_typeof(network.record_json->'network_id')='number'
 AND network.record_json->>'network_id'=member.network_id::text
 WHERE NOT member.network_id=ANY(CAST(:metadata_excluded AS integer[]))
 AND (CAST(:metadata_selected AS integer[]) IS NULL OR member.network_id=ANY(CAST(:metadata_selected AS integer[])))
 LIMIT :metadata_fact_limit
), documents AS MATERIALIZED (SELECT to_jsonb(fact_rows) AS document FROM fact_rows), bounds AS (
 SELECT count(*)<=:metadata_max_facts AND
 coalesce(sum(octet_length(document::text)+2),0)+2<=:metadata_max_bytes AS bounded FROM documents
)
SELECT bounded,CASE WHEN bounded THEN
 (SELECT coalesce(jsonb_agg(document ORDER BY document::text),'[]'::jsonb)::text FROM documents)
 END AS facts_json FROM bounds"""


def _unavailable():
    return RegistryNetworkCatalogError("registry_network_catalog_source_unavailable")


def _addresses(detail_by_field):
    rows = detail_by_field.get("address_list", [])
    groups = detail_by_field.get("address_groups", [])
    if type(rows) is not list or type(groups) is not list:
        raise _unavailable()
    rows = list(rows)
    if "address_key" in detail_by_field:
        rows.append(detail_by_field)
    for group in groups:
        if type(group) is not dict or type(group.get("members")) is not list:
            raise _unavailable()
        rows.extend(group["members"])
    if any(type(row) is not dict for row in rows):
        raise _unavailable()
    return rows


def _address_key(address, *, selected):
    value = address.get("address_key")
    if value in (None, "") and not selected:
        return None
    try:
        parsed = value if isinstance(value, UUID) else UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    if parsed is None or not parsed.int or type(value) is str and str(parsed) != value:
        raise _unavailable()
    return str(parsed)


def _bounded_text(value):
    return (
        type(value) is str
        and value.strip() == value
        and bool(value)
        and len(value.encode("utf-8")) <= 1024
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _membership_map(document, office_keys, scope, include_evidence):
    if type(document) is not str or len(document.encode("utf-8")) > MAX_METADATA_BYTES:
        raise _unavailable()
    try:
        fact_rows = json.loads(document)
    except (ValueError, RecursionError) as error:
        raise _unavailable() from error
    if type(fact_rows) is not list or len(fact_rows) > MAX_MEMBERSHIP_FACTS:
        raise _unavailable()
    evidence_ids_by_office_and_membership = {}
    for fact_by_field in fact_rows:
        if type(fact_by_field) is not dict or set(fact_by_field) != _FACT_FIELDS:
            raise _unavailable()
        network_id = fact_by_field["network_id"]
        npi = fact_by_field["npi"]
        address_key = _address_key(fact_by_field, selected=True)
        if (
            type(npi) is not int
            or not 0 <= npi <= 9999999999
            or (npi, address_key) not in office_keys
            or type(network_id) is not int
            or not 1 <= network_id <= 2147483647
            or network_id in (scope.excluded_network_ids or ())
            or scope.network_ids is not None
            and network_id not in scope.network_ids
            or fact_by_field["provider_system"] not in ("npi", "provider_directory", "manual")
            or not _bounded_text(fact_by_field["provider_id"])
            or not _bounded_text(fact_by_field["evidence_id"])
            or fact_by_field["provider_system"] == "npi"
            and fact_by_field["provider_id"] != f"{npi:010d}"
        ):
            raise _unavailable()
        _address_key({"address_key": fact_by_field["location_id"]}, selected=True)
        membership_values = tuple(
            fact_by_field[field] for field in ("network_id", "provider_system", "provider_id", "location_id")
        )
        evidence_ids = evidence_ids_by_office_and_membership.setdefault((npi, address_key), {}).setdefault(
            membership_values, set()
        )
        evidence_ids.add(fact_by_field["evidence_id"])
    return {
        office_key: [
            dict(zip(("network_id", "provider_system", "provider_id", "location_id"), membership_values, strict=True))
            | ({"evidence_ids": sorted(evidence_ids)} if include_evidence else {})
            for membership_values, evidence_ids in sorted(evidence_ids_by_membership.items())
        ]
        for office_key, evidence_ids_by_membership in evidence_ids_by_office_and_membership.items()
    }


def _office_copies(provider_rows, scope):
    if len(provider_rows) > MAX_MEMBERSHIP_FACTS:
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    copies_by_office = {}
    address_count = 0
    for provider_by_field in provider_rows:
        if (
            type(provider_by_field) is not dict
            or type(provider_by_field.get("npi")) is not int
            or not 0 <= provider_by_field["npi"] <= 9999999999
        ):
            raise _unavailable()
        addresses = _addresses(provider_by_field)
        address_count += len(addresses)
        if address_count > MAX_MEMBERSHIP_FACTS:
            raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
        for address in addresses:
            address_key = _address_key(address, selected=scope.network_ids is not None)
            if address_key is not None:
                office_key = (provider_by_field["npi"], address_key)
                copies_by_office[office_key] = copies_by_office.get(office_key, 0) + 1
    return copies_by_office, address_count


async def _read_memberships(session, manifest, copies_by_office, scope, include_evidence):
    memberships_by_office = {}
    if copies_by_office:
        ordered_office_keys = sorted(copies_by_office)
        parameters_by_name = scoped_address_parameters(
            {
                "metadata_npis": [npi for npi, _ in ordered_office_keys],
                "metadata_address_keys": [address_key for _, address_key in ordered_office_keys],
                "metadata_revision": manifest.approved_custom_revision,
                "metadata_excluded": list(scope.excluded_network_ids or ()),
                "metadata_selected": list(scope.network_ids) if scope.network_ids is not None else None,
                "metadata_fact_limit": MAX_MEMBERSHIP_FACTS + 1,
                "metadata_max_facts": MAX_MEMBERSHIP_FACTS,
                "metadata_max_bytes": MAX_METADATA_BYTES,
            }
        )
        statement = _READ_SQL.format(
            addresses=scoped_address_relation_sql("mrf.entity_address_unified"),
            candidate=_identifier(manifest.schema_name),
            control=_identifier(registry_schema()),
        )
        summary_by_field = (await session.execute(text(statement), parameters_by_name)).mappings().one_or_none()
        if summary_by_field is None or summary_by_field["bounded"] is not True:
            raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
        memberships_by_office = _membership_map(
            summary_by_field["facts_json"], copies_by_office, scope, include_evidence
        )
    return memberships_by_office


async def project_managed_collection_metadata(session, manifest, provider_rows, *, include_sources, include_evidence):
    """Use the caller's verified snapshot and row scope; never infer by address alone."""
    scope = current_network_address_scope()
    if (
        type(manifest) is not PinnedNetworkServingManifest
        or scope is None
        or scope.manifest != manifest
        or not session.in_transaction()
        or not scope.access_scope_sha256
        or type(provider_rows) is not list
        or type(include_sources) is not bool
        or type(include_evidence) is not bool
    ):
        raise _unavailable()
    copies_by_office, address_count = _office_copies(provider_rows, scope)
    memberships_by_office = await _read_memberships(session, manifest, copies_by_office, scope, include_evidence)
    membership_count = 0
    metadata_bytes = 2 + 4 * address_count
    for office_key, copies in copies_by_office.items():
        memberships = memberships_by_office.get(office_key, [])
        if scope.network_ids is not None and not memberships:
            raise _unavailable()
        membership_count += copies * len(memberships)
        metadata_bytes += copies * (
            len(json.dumps(memberships, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) - 2
        )
        if membership_count > MAX_MEMBERSHIP_FACTS or metadata_bytes > MAX_METADATA_BYTES:
            raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    projected_rows = deepcopy(provider_rows)
    for provider_by_field in projected_rows:
        addresses = _addresses(provider_by_field)
        for key in _LEGACY_ASSOCIATIONS:
            provider_by_field.pop(key, None)
        for address in addresses:
            for key in _LEGACY_ASSOCIATIONS:
                address.pop(key, None)
            address_key = _address_key(address, selected=scope.network_ids is not None)
            address_memberships = memberships_by_office.get((provider_by_field["npi"], address_key), [])
            address["network_memberships"] = deepcopy(address_memberships)
    return projected_rows


async def project_managed_detail_metadata(
    session, manifest, npi, detail_by_field, *, include_sources, include_evidence
):
    """Keep the detail contract while sharing the collection's exact office projection."""
    if (
        type(npi) is not str
        or re.fullmatch(r"[0-9]{10}", npi) is None
        or type(detail_by_field) is not dict
        or type(detail_by_field.get("npi")) is not int
        or detail_by_field["npi"] != int(npi)
    ):
        raise _unavailable()
    return (
        await project_managed_collection_metadata(
            session, manifest, [detail_by_field], include_sources=include_sources, include_evidence=include_evidence
        )
    )[0]


async def project_provider_network_metadata(provider_rows, *, session, include_sources=False, include_evidence=False):
    """Replace managed associations once for the complete returned page."""
    scope = current_network_address_scope()
    if scope is None or scope.access_scope_sha256 is None:
        return provider_rows
    try:
        return await project_managed_collection_metadata(
            session, scope.manifest, provider_rows, include_sources=include_sources, include_evidence=include_evidence
        )
    except RegistryNetworkCatalogError as error:
        from sanic.exceptions import ServiceUnavailable

        raise ServiceUnavailable(
            "Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"}
        ) from error


async def attach_provider_source_details(addresses, *, include_sources, include_role_evidence=False, session=None):
    """Use approved snapshot associations for managed responses and retain legacy source reads."""
    if current_network_address_scope() is not None:
        return
    from api.endpoint import npi

    await npi._attach_provider_directory_source_details(
        addresses, include_role_evidence=include_role_evidence, session=session
    )
    if include_sources:
        await npi._attach_mrf_source_details(addresses, session=session)
