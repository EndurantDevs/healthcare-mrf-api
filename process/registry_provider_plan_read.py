# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-qualified ACA provider plans with exact retained office evidence."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from db.json_mixin import JSONOutputMixin
from db.models import Issuer, NPIData, PlanNPIRaw
from db.registry_schema import registry_schema
from process.ext.address_canon import address_key_v1
from process.initial import _normalize_marketplace_address_entry_base
from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_legacy_membership_source import PinnedACAMembershipSource, _require_archive, _reviewed_generation
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_candidate_composition import _retained_memberships
from process.registry_network_catalog_read import (
    MAX_RESPONSE_BYTES,
    RegistryNetworkCatalogError,
    _exclusions,
    _integer,
    _pin,
)
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes


@dataclass(frozen=True)
class RegistryProviderPlansQuery:
    """Provider identity and optional independent source filtering and manifest pin."""

    npi: str
    year: int | None = None
    generation_id: int | None = None

    def __post_init__(self):
        if type(self.npi) is not str or re.fullmatch(r"[0-9]{10}", self.npi) is None:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if self.year is not None:
            _integer(self.year, 2100, minimum=2000)
        if self.generation_id is not None:
            _integer(self.generation_id, 9223372036854775807)


_SOURCE_SQL = """SELECT to_jsonb(provider) AS npi_info,to_jsonb(issuer) AS issuer_info,
 jsonb_build_object('checksum',evidence.checksum,'type',evidence.type,
   'source_record_id',evidence.source_record_id,'address_key',evidence.address_key,
   'first_line',evidence.first_line,'second_line',evidence.second_line,'city_name',evidence.city_name,
   'state_name',evidence.state_name,'postal_code',evidence.postal_code,'country_code',evidence.country_code) AS proof
FROM {source}.plan_npi_raw provider
JOIN {source}.mrf_address_evidence evidence ON evidence.npi=provider.npi
 AND evidence.checksum_network=provider.checksum_network AND evidence.issuer_id=provider.issuer_id
 AND evidence.year=provider.year AND evidence.network_tier=provider.network_tier
JOIN {source}.plan_networktier tier ON tier.checksum_network=provider.checksum_network
 AND tier.issuer_id=provider.issuer_id AND tier.year=provider.year AND tier.network_tier=provider.network_tier
JOIN {source}.plan plan ON (plan.plan_id,plan.issuer_id,plan.year)=(tier.plan_id,tier.issuer_id,tier.year)
JOIN {source}.issuer issuer ON issuer.issuer_id=provider.issuer_id
JOIN {control}.registry_approved_record binding ON binding.approved_revision=$1
 AND binding.record_kind='network_binding' AND binding.record_json->'archived'='false'::jsonb
 AND {coordinates} AND binding.record_json->>'source_key'=plan.plan_id
 AND binding.record_json->'source_scope_json'=jsonb_build_object('issuer_id',lpad(plan.issuer_id::text,5,'0'),
   'plan_id',plan.plan_id,'plan_year',plan.year,'state',plan.state,'checksum_network',provider.checksum_network)
JOIN eligible ON eligible.network_id=(binding.record_json->>'network_id')::integer
JOIN {retained_relation} retained ON retained.network_id=eligible.network_id
 AND retained.provider_system='npi' AND retained.provider_id=$3 AND retained.location_id=evidence.address_key
JOIN {candidate}.network_membership effective ON
 (effective.network_id,effective.provider_system,effective.provider_id,effective.location_id)=
 (retained.network_id,retained.provider_system,retained.provider_id,retained.location_id)
JOIN {candidate}.provider_location_binding office ON
 (office.provider_system,office.provider_id,office.location_id)=
 (retained.provider_system,retained.provider_id,retained.location_id)
JOIN {candidate}.entity_address_unified address
 ON (address.location_key,address.entity_type,address.entity_id)=(office.location_key,office.entity_type,office.entity_id)
WHERE provider.npi=$3::bigint AND office.entity_type='npi' AND office.entity_id=$3 AND ({retained_predicate})
 AND eligible.network_id=ANY(address.canonical_network_ids)
 AND ($4::integer IS NULL OR provider.year=$4)
 AND evidence.import_id={import_id} AND evidence.issuer_id={issuer_id} AND evidence.year={year}
 AND evidence.source_url={source_url} AND evidence.address_source='marketplace_provider'
 AND evidence.source_table='plan_npi_raw'
 AND retained.evidence_id=encode(sha256(convert_to(jsonb_build_array({generation}::text,
   to_jsonb(evidence)||jsonb_build_object('fact_npi',provider.npi,'fact_checksum',provider.checksum_network,
   'retained_npi',provider.npi,'retained_plan_id',tier.plan_id,'plan_id',plan.plan_id,'plan_state',plan.state,
   'plan_issuer',plan.issuer_id,'plan_year',plan.year,'network_id',eligible.network_id,
   'binding_record_key',binding.record_key,'omitted',false,'unresolved',false))::text,'UTF8')),'hex')
"""

_READ_SQL = """WITH eligible AS MATERIALIZED (
 SELECT record_key::integer AS network_id FROM {control}.registry_approved_record
 WHERE approved_revision=$1 AND record_kind='network' AND record_json->'archived'='false'::jsonb
 AND NOT record_key::integer=ANY($2::integer[])
 AND ($6::integer[] IS NULL OR record_key::integer=ANY($6::integer[]))
), selected AS MATERIALIZED (SELECT DISTINCT * FROM ({sources}) qualified), groups AS MATERIALIZED (
 SELECT npi_info,issuer_info FROM selected GROUP BY npi_info,issuer_info
), bounds AS (
 SELECT coalesce((SELECT sum(octet_length(npi_info::text)+octet_length(issuer_info::text)+128) FROM groups),0)
 +coalesce((SELECT sum(octet_length(proof::text)+2) FROM selected),0)+1024<=$5 AS bounded
), documents AS (
 SELECT jsonb_build_object('npi_info',npi_info,'issuer_info',issuer_info,'proofs',jsonb_agg(proof)) AS document
 FROM selected,bounds WHERE bounded GROUP BY npi_info,issuer_info
)
SELECT bounded,CASE WHEN bounded THEN
 (SELECT coalesce(jsonb_agg(document ORDER BY document::text),'[]'::jsonb)::text FROM documents) END AS rows_json
FROM bounds"""


def _source_query(recipe, approved, control, candidate, parameters, retained_relation, retained_predicate):
    source = recipe.source_pin
    coordinates = []
    for field, value in zip(
        ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
        recipe.binding_coordinates.sql_parameters,
        strict=True,
    ):
        parameters.append(value)
        coordinates.append(f"binding.record_json->>'{field}'=${len(parameters)}")
    values_by_field = {}
    for field in ("import_id", "issuer_id", "year", "source_url"):
        parameters.append(getattr(source, field))
        values_by_field[field] = f"${len(parameters)}"
    parameters.append(_reviewed_generation(source, approved, recipe.binding_coordinates, True, True))
    return _SOURCE_SQL.format(
        source=_identifier(source.prepared.ownership.schema_name),
        control=control,
        candidate=candidate,
        coordinates=" AND ".join(coordinates),
        retained_relation=retained_relation,
        retained_predicate=retained_predicate,
        generation=f"${len(parameters)}",
        **values_by_field,
    )


def _project_addresses(document):
    raw = document["npi_info"]
    issuer = document["issuer_info"]
    if set(raw) != {column.name for column in PlanNPIRaw.__table__.columns} or set(issuer) != {
        column.name for column in Issuer.__table__.columns
    }:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_unavailable")
    if raw["last_updated_on"] is not None:
        raw = raw | {
            "last_updated_on": JSONOutputMixin.prepare_for_json(datetime.fromisoformat(raw["last_updated_on"]))
        }
    addresses = raw.get("addresses")
    if type(addresses) is not list:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_unavailable")
    selected_addresses = []
    for entry in addresses:
        normalized = _normalize_marketplace_address_entry_base(entry)
        if normalized is None:
            continue
        key = address_key_v1(
            *(normalized[field] for field in ("first_line", "second_line", "city_name", "state_name", "postal_code")),
            normalized["country_code"] or "US",
        )
        for proof in document["proofs"]:
            if (
                normalized["checksum"] == proof["checksum"]
                and str(key) == proof["address_key"]
                and proof["source_record_id"]
                == f"{raw['npi']}:{proof['type']}:{normalized['checksum']}:{raw['checksum_network']}"
                and all(
                    (normalized[field] or "") == (proof[field] or "")
                    for field in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
                )
            ):
                selected_addresses.append(entry)
                break
    if not selected_addresses:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_unavailable")
    return {"npi_info": raw | {"addresses": selected_addresses}, "issuer_info": document["issuer_info"]}


async def _pin_provider_plans(connection, query, manifest, control_schema):
    """Revalidate supplied identity: a manifest dataclass alone proves no database integrity.

    Select its explicit generation instead of another current head. Complete directory
    accounting and same-transaction pin validation remain mandatory, as does equality.
    """
    generation_id = query.generation_id
    if manifest is not None:
        if type(manifest) is not PinnedNetworkServingManifest:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        _integer(manifest.generation_id, 9223372036854775807)
        if generation_id is not None and generation_id != manifest.generation_id:
            raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        generation_id = manifest.generation_id
    pinned_manifest = await _pin(connection, generation_id, control_schema)
    if manifest is not None and manifest != pinned_manifest:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    return pinned_manifest


def _provider_plan_network_ids(network_ids):
    if network_ids is None:
        return None
    if type(network_ids) is not tuple or not 1 <= len(network_ids) <= 100:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    for network_id in network_ids:
        _integer(network_id, 2147483647)
    if network_ids != tuple(sorted(set(network_ids))):
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    return network_ids


async def read_registry_provider_plans(
    connection, query, *, excluded_network_ids, control_schema=None, manifest=None, network_ids=None
):
    """Read retained ACA office facts under a current or explicitly supplied manifest."""
    if type(query) is not RegistryProviderPlansQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    network_ids = _provider_plan_network_ids(network_ids)
    control_schema = control_schema if control_schema is not None else registry_schema()
    control = _identifier(control_schema)
    try:
        manifest = await _pin_provider_plans(connection, query, manifest, control_schema)
        identity = _identifier(str(NPIData.__table__.schema)) + "." + _identifier(str(NPIData.__table__.name))
        if not await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {identity} WHERE npi=$1)", int(query.npi)):
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        recipes = await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)
        approved = await pin_retained_approved_membership_source(
            connection, approved_revision=manifest.approved_custom_revision, control_schema=control_schema
        )
        revision = manifest.approved_custom_revision
        candidate = _identifier(manifest.schema_name)
        sql_args = [revision, exclusions, query.npi, query.year, MAX_RESPONSE_BYTES, network_ids]
        retained_relation, retained_predicate, _ = await _retained_memberships(connection, manifest, control_schema)
        retained_predicate = retained_predicate.replace("evidence_id", "retained.evidence_id")
        source_queries = []
        for recipe in recipes:
            if type(recipe.source_pin) is PinnedACAMembershipSource:
                await _require_archive(connection, recipe.source_pin)
                source_queries.append(
                    _source_query(recipe, approved, control, candidate, sql_args, retained_relation, retained_predicate)
                )
        npi_rows = []
        if source_queries:
            summary = await connection.fetchrow(
                _READ_SQL.format(control=control, sources=" UNION ALL ".join(source_queries)), *sql_args
            )
            if summary is None or summary["bounded"] is not True:
                raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
            projected_rows = (_project_addresses(document) for document in json.loads(summary["rows_json"]))
            npi_rows = list(
                {
                    json.dumps(document, sort_keys=True, separators=(",", ":")): document for document in projected_rows
                }.values()
            )
        return {
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
            "npi_data": npi_rows,
            "plan_data": [],
            "issuer_data": [],
        }
    except (asyncpg.PostgresError, ValueError, TypeError, KeyError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_source_unavailable") from error
