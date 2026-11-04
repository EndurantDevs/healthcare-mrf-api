# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Map independently verified typed graph rows through approved bindings and pinned offices."""

import asyncio
import re
from dataclasses import asdict, dataclass
from datetime import date
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    require_approved_network_source_bindings,
)
from process.network_custom_address_source import _artifact_dependency_identity
from process.network_fhir_source_witness_contract import digest, source_origin
from process.network_fhir_typed_contract import ADDRESS_FIELDS, POLICY, require
from process.network_fhir_typed_sql import PROVIDER_ONLY_SQL
from process.network_membership_candidate_lifecycle import _control_namespace
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

MEMBER_COLUMNS = ("network_id", "provider_system", "provider_id", "location_id", "evidence_id")
BINDING_COLUMNS = ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
_PAIR_KEYS = ("resource_type", "resource_id", "network_source_key", "location_resource_id")
_MAPPING_DDL = {
    "provider_only": "network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,evidence_id text NOT NULL,PRIMARY KEY(network_id,provider_system,provider_id,evidence_id)",
    "offices": "provider_id text NOT NULL,location_id uuid NOT NULL,address_key uuid NOT NULL,canonical_identity text NOT NULL,location_key text,PRIMARY KEY(provider_id,location_id)",
    "memberships": "network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL",
    "bindings": "provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,location_key text NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,PRIMARY KEY(provider_system,provider_id,location_id)",
}


@dataclass
class TypedGraphMapping:
    source_stage: object
    table_names: dict
    oids: dict
    metadata: dict

    def relation(self, kind):
        """Resolve a declared mapping relation in the current temporary workspace."""
        return "pg_temp." + _identifier(self.table_names[kind])

    async def verify(self, connection):
        """Recheck source custody and every temporary mapping relation identity."""
        await self.source_stage.verify(connection)
        for kind, oid in self.oids.items():
            require(
                await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + self.table_names[kind])
                == oid,
                "mapping_workspace_changed",
            )


async def create_typed_mapping_tables(connection):
    """Precreate scratch before a read-only custodian snapshot, or in the producer transaction."""
    names_by_kind = {kind: "typed_map_" + kind + "_" + uuid4().hex for kind in _MAPPING_DDL}
    for kind, name in names_by_kind.items():
        await connection.execute("CREATE TEMP TABLE " + _identifier(name) + "(" + _MAPPING_DDL[kind] + ")")
    return names_by_kind


def require_catalog_graph_scope(stage, coordinates, descriptor):
    """The approved catalog generation and authenticated Source origin must identify the same edition."""
    descriptor = validate_fhir_catalog_source_descriptor(descriptor)
    origin = source_origin(stage.document["origin"])
    require(
        descriptor["profile"] == "published_normalized" and descriptor["coordinates"] == asdict(coordinates),
        "catalog_scope",
    )
    expected = (
        "fhir",
        origin.source_id,
        origin.schema_name,
        origin.dataset_id,
        "registry-fhir-catalog-v1",
        origin.dataset_id,
    )
    require(coordinates.sql_parameters == expected, "catalog_coordinates")
    require(
        (
            descriptor["endpoint_id"],
            descriptor["dataset_sha256"],
            descriptor["source_uri"],
            tuple(descriptor["source_aliases"]),
            descriptor["header_table_oid"],
            descriptor["resource_table_oid"],
            descriptor["resource_leaf_oids"],
            descriptor["native_catalog_sha256"],
            descriptor["database_oid"],
        )
        == (
            origin.endpoint_id,
            origin.dataset_sha256,
            origin.source_uri,
            origin.source_aliases,
            origin.header_oid,
            origin.resource_parent_oid,
            [origin.resource_leaf_oid],
            origin.native_catalog_sha256,
            origin.database_oid,
        ),
        "catalog_origin",
    )
    admission = descriptor["admission"]
    if admission["kind"] == "catalog_observation":
        publication = admission["publication"]
        require(
            publication["root_run_id"] == origin.root_run_id
            and publication["original_metadata_sha256"] == origin.admission_metadata_sha256
            and publication["claimed_resource_count"] == sum(dict(origin.resource_counts).values()),
            "catalog_publication",
        )
    else:
        require(
            admission["proof_sha256"] == origin.proof_sha256
            and admission["metadata_sha256"] == origin.admission_metadata_sha256
            and admission["resource_counts"] == dict(origin.resource_counts),
            "catalog_admission",
        )
    return fhir_catalog_source_generation(descriptor)


async def _pair_ranges(connection, stage):
    """One small boundary row per indexed 5,000-edge range; never hydrate edges in Python."""
    after = ("", "", "", "")
    keys = ",".join(_PAIR_KEYS)
    while True:
        row = await connection.fetchrow(
            "SELECT "
            + keys
            + " FROM (SELECT "
            + keys
            + " FROM "
            + stage.relation("pairs")
            + " WHERE ("
            + keys
            + ")>($1,$2,$3,$4) ORDER BY "
            + keys
            + " LIMIT 5000) page ORDER BY "
            + ",".join(key + " DESC" for key in _PAIR_KEYS)
            + " LIMIT 1",
            *after,
        )
        if row is None:
            return
        last_keys = tuple(row)
        yield after, last_keys
        after = last_keys


def _range(prefix, start):
    keys = ",".join(prefix + key for key in _PAIR_KEYS)
    low = ",".join("$" + str(start + index) + "::text" for index in range(4))
    high = ",".join("$" + str(start + 4 + index) + "::text" for index in range(4))
    return "(" + keys + ")>(" + low + ") AND (" + keys + ")<=(" + high + ")"


async def _office_inputs(connection, stage, mapping, approved_keys):
    async for low, high in _pair_ranges(connection, stage):
        await connection.execute(
            "INSERT INTO "
            + mapping.relation("offices")
            + "(provider_id,location_id,address_key,canonical_identity) SELECT DISTINCT provider_id,location_id,address_key,canonical_identity FROM "
            + stage.relation("pairs")
            + " p WHERE "
            + _range("p.", 1)
            + " AND network_source_key=ANY($9::text[]) ON CONFLICT DO NOTHING",
            *low,
            *high,
            approved_keys,
        )
    require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM "
            + stage.relation("pairs")
            + " p LEFT JOIN LATERAL (SELECT address_key,canonical_identity FROM "
            + mapping.relation("offices")
            + " o WHERE (o.provider_id,o.location_id)=(p.provider_id,p.location_id) OFFSET 0) o ON true WHERE p.network_source_key=ANY($1::text[]) AND (o.address_key IS DISTINCT FROM p.address_key OR o.canonical_identity IS DISTINCT FROM p.canonical_identity))",
            approved_keys,
        ),
        "office_collision",
    )


def _accepted_office_rows(matched_rows, canonical):
    accepted_rows = []
    for office_row, canonical_address in zip(matched_rows, canonical, strict=True):
        require(
            UUID(canonical_address["address_key"]) == office_row["address_key"]
            and canonical_address["identity_key"] == office_row["canonical_identity"],
            "office_identity_differs",
        )
        accepted_rows.append((office_row["provider_id"], office_row["location_id"], office_row["matched_key"]))
    return accepted_rows


async def _match_offices(connection, mapping, address_source):
    """Match bounded office pages by approved NPI, address key and full canonical identity."""
    import ptg2_address_canon as native

    address = _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name)
    fields = ",".join("a." + field for field in ADDRESS_FIELDS)
    query = (
        "WITH requested AS MATERIALIZED (SELECT * FROM "
        + mapping.relation("offices")
        + " WHERE (provider_id,location_id)>($1::text,$2::uuid) ORDER BY provider_id,location_id LIMIT $3) SELECT o.*,a.location_key AS matched_key,"
        + fields
        + " FROM requested o LEFT JOIN LATERAL (SELECT * FROM "
        + address
        + " a WHERE a.entity_type='npi' AND a.entity_id=o.provider_id AND a.npi=o.provider_id::bigint AND a.address_key=o.address_key AND a.type='practice' AND a.address_precision='street' AND a.inferred_npi IS NULL LIMIT 2) a ON true ORDER BY o.provider_id,o.location_id"
    )
    after, limit = ("", UUID(int=0)), 1000
    while True:
        size = await connection.fetchval(
            "SELECT coalesce(sum(octet_length(record_send(page))),0) FROM (" + query + ") page", *after, limit
        )
        if size > 8 * 1024**2:
            require(limit > 1, "office_bounds")
            limit = max(1, limit // 2)
            continue
        office_rows = await connection.fetch(query, *after, limit)
        if not office_rows:
            break
        after = office_rows[-1]["provider_id"], office_rows[-1]["location_id"]
        identities = [(office_row["provider_id"], office_row["location_id"]) for office_row in office_rows]
        require(len(identities) == len(set(identities)), "ambiguous_office")
        matched_rows = [office_row for office_row in office_rows if office_row["matched_key"] is not None]
        address_values = [tuple(office_row[field] for field in ADDRESS_FIELDS) for office_row in matched_rows]
        canonical = (
            await drain_operation(
                asyncio.to_thread(native.canonicalize_batch, address_values), preserve_cancellation=True
            )
            if address_values
            else []
        )
        accepted_rows = _accepted_office_rows(matched_rows, canonical)
        if accepted_rows:
            await connection.execute(
                "UPDATE "
                + mapping.relation("offices")
                + " o SET location_key=v.location_key FROM unnest($1::text[],$2::uuid[],$3::text[]) v(provider_id,location_id,location_key) WHERE (o.provider_id,o.location_id)=(v.provider_id,v.location_id)",
                [office_row[0] for office_row in accepted_rows],
                [office_row[1] for office_row in accepted_rows],
                [office_row[2] for office_row in accepted_rows],
            )
    await connection.execute(
        "INSERT INTO "
        + mapping.relation("bindings")
        + " SELECT 'npi',provider_id,location_id,location_key,'npi',provider_id FROM "
        + mapping.relation("offices")
        + " WHERE location_key IS NOT NULL"
    )


async def _mapping_workspace(connection, stage, table_names):
    names = await create_typed_mapping_tables(connection) if table_names is None else table_names
    require(type(names) is dict and set(names) == set(_MAPPING_DDL), "mapping_tables")
    oids_by_kind = {}
    for kind, name in names.items():
        _identifier(name)
        oids_by_kind[kind] = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + name)
        require(
            type(oids_by_kind[kind]) is int
            and await connection.fetchval("SELECT NOT EXISTS(SELECT FROM pg_temp." + _identifier(name) + ")"),
            "restart_from_zero",
        )
    mapping = TypedGraphMapping(stage, names, oids_by_kind, {})
    return mapping


async def _approved_graph_keys(connection, bindings, arguments):
    valid = await connection.fetchval(
        "WITH "
        + bindings
        + " SELECT count(*)>0 AND bool_and(record_json->'source_scope_json'=jsonb_build_object('organization_id',record_json->>'source_key','source_generation',$8::text)) FROM approved_network_bindings",
        *arguments,
    )
    require(valid is True, "approved_catalog_scope")
    approved_keys = await connection.fetchval(
        "WITH " + bindings + " SELECT array_agg(record_json->>'source_key') FROM approved_network_bindings",
        *arguments[:7],
    )
    return approved_keys


async def _map_pair_rows(connection, stage, mapping, bindings, arguments):
    joins = (
        " FROM "
        + stage.relation("pairs")
        + " p LEFT JOIN approved_network_bindings b ON b.record_json->>'source_key'=p.network_source_key AND b.record_json->'source_scope_json'->>'source_generation'=$8::text LEFT JOIN LATERAL (SELECT location_key FROM "
        + mapping.relation("offices")
        + " o WHERE (o.provider_id,o.location_id)=(p.provider_id,p.location_id) OFFSET 0) o ON true "
    )
    totals = dict.fromkeys(
        (
            "mapped_pair_rows",
            "mapped_reference_rows",
            "unapproved_pair_rows",
            "unapproved_reference_rows",
            "unmatched_office_pair_rows",
            "unmatched_office_reference_rows",
        ),
        0,
    )
    async for low, high in _pair_ranges(connection, stage):
        scope = " WHERE " + _range("p.", 9)
        counts = await connection.fetchrow(
            "WITH "
            + bindings
            + " SELECT count(*) FILTER(WHERE b.network_id IS NOT NULL AND o.location_key IS NOT NULL) mapped_pair_rows,coalesce(sum(p.reference_rows) FILTER(WHERE b.network_id IS NOT NULL AND o.location_key IS NOT NULL),0)::bigint mapped_reference_rows,count(*) FILTER(WHERE b.network_id IS NULL) unapproved_pair_rows,coalesce(sum(p.reference_rows) FILTER(WHERE b.network_id IS NULL),0)::bigint unapproved_reference_rows,count(*) FILTER(WHERE b.network_id IS NOT NULL AND o.location_key IS NULL) unmatched_office_pair_rows,coalesce(sum(p.reference_rows) FILTER(WHERE b.network_id IS NOT NULL AND o.location_key IS NULL),0)::bigint unmatched_office_reference_rows"
            + joins
            + scope,
            *arguments,
            *low,
            *high,
        )
        for key, count_value in counts.items():
            totals[key] += count_value
        evidence = "encode(sha256(convert_to(jsonb_build_array('fhir-direct-network-office-v3',$8::text,p.resource_type,p.resource_id,p.network_source_key,p.location_resource_id,p.evidence_basis)::text,'UTF8')),'hex')"
        await connection.execute(
            "WITH "
            + bindings
            + " INSERT INTO "
            + mapping.relation("memberships")
            + " SELECT b.network_id,p.provider_system,p.provider_id,p.location_id,"
            + evidence
            + joins
            + scope
            + " AND b.network_id IS NOT NULL AND o.location_key IS NOT NULL",
            *arguments,
            *low,
            *high,
        )
    return totals


async def _map_provider_rows(connection, stage, mapping, bindings, arguments):
    origin = source_origin(stage.document["origin"])
    after = ("", "")
    while resource_rows := await connection.fetch(
        "SELECT resource_type,resource_id FROM "
        + stage.relation("cells")
        + " WHERE resource_type IN ('PractitionerRole','OrganizationAffiliation') AND (resource_type,resource_id)>($1,$2) ORDER BY resource_type,resource_id LIMIT 128",
        *after,
    ):
        last_keys = tuple(resource_rows[-1])
        provider_sql = PROVIDER_ONLY_SQL.replace("{cells}", stage.relation("cells"))
        # Shift Source query parameters after the closed approved coordinates and generation.
        provider_sql = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + 8), provider_sql)
        await connection.execute(
            "WITH "
            + bindings
            + " INSERT INTO "
            + mapping.relation("provider_only")
            + " SELECT b.network_id,'npi',p.provider_id,encode(sha256(convert_to(jsonb_build_array('fhir-provider-only-v3',$8::text,p.resource_type,p.resource_id,p.network_source_key)::text,'UTF8')),'hex') FROM ("
            + provider_sql
            + ") p JOIN approved_network_bindings b ON b.record_json->>'source_key'=p.network_source_key ON CONFLICT DO NOTHING",
            *arguments,
            origin.source_uri.rstrip("/"),
            date.fromisoformat(stage.document["membership_as_of"]),
            *after,
            *last_keys,
        )
        after = last_keys


async def _require_mapping_counts(connection, stage, mapping, totals):
    office_counts = await connection.fetchrow(
        "SELECT count(*) FILTER(WHERE location_key IS NOT NULL) matched_offices,count(*) FILTER(WHERE location_key IS NULL) missing_offices FROM "
        + mapping.relation("offices")
    )
    require(
        sum(totals[key] for key in ("mapped_pair_rows", "unapproved_pair_rows", "unmatched_office_pair_rows"))
        == stage.document["counts"]["pair_rows"],
        "mapping_counts",
    )
    require(
        sum(
            totals[key]
            for key in ("mapped_reference_rows", "unapproved_reference_rows", "unmatched_office_reference_rows")
        )
        == stage.document["counts"]["reference_rows"],
        "mapping_reference_counts",
    )
    return office_counts


@dataclass(frozen=True)
class TypedGraphApproval:
    """Exact approved binding and catalog coordinates required by typed mapping."""

    approved_source: object
    binding_coordinates: object
    catalog_descriptor: dict


async def prepare_typed_graph_mapping(
    connection,
    stage,
    *,
    approval,
    address_source,
    runtime_roles,
    control_schema=None,
    table_names=None,
):
    """Return existing membership/binding row shapes only after raw graph, approval and office custody checks."""
    approved_source = approval.approved_source
    binding_coordinates, catalog_descriptor = approval.binding_coordinates, approval.catalog_descriptor
    await stage.verify(connection)
    generation = require_catalog_graph_scope(stage, binding_coordinates, catalog_descriptor)
    await require_approved_network_source_bindings(
        connection, approved_source, binding_coordinates, control_schema=control_schema
    )
    address_identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
    mapping = await _mapping_workspace(connection, stage, table_names)
    bindings = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema))
    arguments = (approved_source.approved_revision, *binding_coordinates.sql_parameters, generation)
    approved_keys = await _approved_graph_keys(connection, bindings, arguments)
    await _office_inputs(connection, stage, mapping, approved_keys)
    await _match_offices(connection, mapping, address_source)
    totals = await _map_pair_rows(connection, stage, mapping, bindings, arguments)
    await _map_provider_rows(connection, stage, mapping, bindings, arguments)
    office_counts = await _require_mapping_counts(connection, stage, mapping, totals)
    mapping.metadata = {
        "office_policy": POLICY,
        "canonicalizer_version": 2,
        "catalog_generation": generation,
        "approved_revision": approved_source.approved_revision,
        "approved_generation_sha256": approved_source.generation_id,
        "address_source": asdict(address_source),
        "address_identity": dict(
            zip(("schema_oid", "table_oid", "owner_oid", "identity_sha256"), address_identity, strict=True)
        ),
        "counters": {**stage.document["counts"], **totals, **dict(office_counts)},
    }
    mapping.metadata["counters"]["strict_pair_rows"] = mapping.metadata["counters"].pop("pair_rows")
    mapping.metadata["mapping_sha256"] = digest(mapping.metadata)
    await mapping.verify(connection)
    return mapping
