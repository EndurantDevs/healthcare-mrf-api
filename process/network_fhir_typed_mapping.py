# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Map independently verified typed graph rows through approved bindings and pinned offices."""

import asyncio
import re
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL
from process.network_fhir_source_witness_contract import digest, source_origin
from process.network_fhir_typed_contract import ADDRESS_FIELDS, PLAN_TIER_CONTRACT, POLICY, require
from process.network_fhir_typed_sql import PLAN_TIER_PROVIDER_ONLY_SQL, PROVIDER_ONLY_PAGE_SIZE, PROVIDER_ONLY_SQL
from process.network_membership_candidate_lifecycle import _control_namespace
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_network_import_progress import network_source_mapped
from process.uhc_flex_practitioner_async_safety import drain_operation

MEMBER_COLUMNS = ("network_id", "provider_system", "provider_id", "location_id", "evidence_id")
BINDING_COLUMNS = ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
_PAIR_KEYS = ("resource_type", "resource_id", "network_source_key", "location_resource_id")
_PAIR_PAGE_SIZE = 5000
_OFFICE_PAGE_BYTES = 8 * 1024**2
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
    metadata: dict

    def relation(self, kind):
        """Resolve a declared mapping relation in the current temporary workspace."""
        return "pg_temp." + _identifier(self.table_names[kind])

    def require_ready(self):
        """Require a validated Source edition and declared temporary mapping names."""
        self.source_stage.require_ready()
        require(
            type(self.table_names) is dict
            and set(self.table_names) == {*set(_MAPPING_DDL), "office_keys", "office_matches"},
            "mapping_tables",
        )
        for name in self.table_names.values():
            _identifier(name)


async def create_typed_mapping_tables(connection, *, membership_table=None):
    """Precreate scratch before a read-only custodian snapshot, or in the producer transaction."""
    from process.network_fhir_typed_office_lookup import create_office_lookup_tables

    names_by_kind = {
        kind: "typed_map_" + kind + "_" + uuid4().hex
        for kind in _MAPPING_DDL
        if kind != "memberships" or membership_table is None
    }
    for kind, name in names_by_kind.items():
        await connection.execute("CREATE TEMP TABLE " + _identifier(name) + "(" + _MAPPING_DDL[kind] + ")")
    if membership_table is not None:
        names_by_kind["memberships"] = membership_table
    await create_office_lookup_tables(connection, names_by_kind["offices"])
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
            descriptor["tables"]["header"],
            descriptor["tables"]["resource"],
        )
        == (
            origin.endpoint_id,
            origin.dataset_sha256,
            origin.source_uri,
            origin.source_aliases,
            origin.header_relation,
            origin.resource_relation,
        ),
        "catalog_origin",
    )
    admission = descriptor["admission"]
    if admission["kind"] == "catalog_observation":
        publication = admission["publication"]
        require(
            publication["root_run_id"] == origin.root_run_id
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


async def _office_inputs(connection, stage, mapping, approved):
    # The office primary key removes repeats without sorting the complete graph.
    if isinstance(approved, _PreparedBindingRows):
        selection = f"EXISTS (SELECT FROM {approved.relation} b WHERE b.source_key=p.network_source_key)"
        parameters = ()
    else:
        selection = "p.network_source_key=ANY($1::text[])"
        parameters = (approved,)
    column_sql = "provider_id,location_id,address_key,canonical_identity"
    await connection.execute(
        f"INSERT INTO {mapping.relation('offices')} ({column_sql}) SELECT {column_sql} FROM {stage.relation('pairs')} p "
        f"WHERE {selection} ON CONFLICT DO NOTHING",
        *parameters,
    )
    require(
        await connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM {stage.relation('pairs')} p LEFT JOIN LATERAL ("
            f"SELECT address_key,canonical_identity FROM {mapping.relation('offices')} o "
            "WHERE (o.provider_id,o.location_id)=(p.provider_id,p.location_id) OFFSET 0) o ON true "
            f"WHERE {selection} AND (o.address_key IS DISTINCT FROM p.address_key OR o.canonical_identity IS DISTINCT FROM p.canonical_identity))",
            *parameters,
        ),
        "office_collision",
    )


def _single_office_matches(office_rows):
    counts_by_office = Counter((office_row["provider_id"], office_row["location_id"]) for office_row in office_rows)
    return [
        office_row
        for office_row in office_rows
        if office_row["matched_key"] is not None
        and counts_by_office[office_row["provider_id"], office_row["location_id"]] == 1
    ]


def _accepted_office_rows(matched_rows, canonical):
    accepted_rows = []
    for office_row, canonical_address in zip(matched_rows, canonical, strict=True):
        if (
            canonical_address["address_key"] is not None
            and canonical_address["identity_key"] is not None
            and UUID(canonical_address["address_key"]) == office_row["address_key"]
            and canonical_address["identity_key"] == office_row["canonical_identity"]
        ):
            accepted_rows.append((office_row["provider_id"], office_row["location_id"], office_row["matched_key"]))
    return accepted_rows


async def _match_offices(connection, mapping, address_source, matches):
    """Match bounded office pages by approved NPI, address key and full canonical identity."""
    import ptg2_address_canon as native

    address = _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name)
    fields = ",".join("a." + field for field in ADDRESS_FIELDS)
    query = (
        "WITH requested AS MATERIALIZED (SELECT * FROM "
        + mapping.relation("offices")
        + " WHERE (provider_id,location_id)>($1::text,$2::uuid) ORDER BY provider_id,location_id LIMIT $3), "
        + "page AS MATERIALIZED (SELECT o.*,a.location_key AS matched_key,"
        + fields
        + " FROM requested o LEFT JOIN LATERAL (SELECT a.* FROM "
        + matches
        + " matched JOIN LATERAL (SELECT * FROM "
        + address
        + " a WHERE a.location_key=matched.location_key OFFSET 0) a ON true WHERE (matched.provider_id,matched.address_key)=(o.provider_id,o.address_key) AND a.entity_type='npi' AND a.entity_id=o.provider_id AND a.npi=o.provider_id::bigint AND a.address_key=o.address_key AND a.type='practice' AND a.address_precision='street' AND a.inferred_npi IS NULL LIMIT 2) a ON true) "
        + "SELECT p.*,bounds.page_fits FROM (SELECT coalesce(sum(octet_length(record_send(p))),0)"
        + f"<={_OFFICE_PAGE_BYTES} AS page_fits FROM page p) bounds LEFT JOIN page p ON bounds.page_fits "
        + "ORDER BY p.provider_id,p.location_id"
    )
    after, limit = ("", UUID(int=0)), 1000
    while True:
        # An oversized or empty page returns only the bounds row.
        office_rows = await connection.fetch(query, *after, limit)
        if not office_rows[0]["page_fits"]:
            require(limit > 1, "office_bounds")
            limit = max(1, limit // 2)
            continue
        if office_rows[0]["provider_id"] is None:
            break
        after = office_rows[-1]["provider_id"], office_rows[-1]["location_id"]
        matched_rows = _single_office_matches(office_rows)
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


async def _mapping_workspace(connection, stage, table_names, *, membership_table=None):
    from process.network_fhir_typed_office_lookup import office_lookup_names

    require(table_names is None or membership_table is None, "mapping_tables")
    names_by_kind = (
        await create_typed_mapping_tables(connection, membership_table=membership_table)
        if table_names is None
        else table_names
    )
    require(type(names_by_kind) is dict and set(names_by_kind) == set(_MAPPING_DDL), "mapping_tables")
    names_by_kind = {**names_by_kind, **office_lookup_names(names_by_kind["offices"])}
    for kind, name in names_by_kind.items():
        _identifier(name)
        require(
            await connection.fetchval("SELECT NOT EXISTS(SELECT FROM pg_temp." + _identifier(name) + ")"),
            "restart_from_zero",
        )
    mapping = TypedGraphMapping(stage, names_by_kind, {})
    return mapping


def _graph_scope_valid(allow_empty, plan_tiers, *, typed=False):
    source_key = "source_key" if typed else "record_json->>'source_key'"
    source_scope = "source_scope" if typed else "record_json->'source_scope_json'"
    tier_condition = (
        f" OR ({source_scope}->>'kind'='uhc_plan_tier_v1' AND {source_scope}->>'source_generation'=$8::text)"
        if plan_tiers
        else ""
    )
    return (
        ("" if allow_empty else "count(*)>0 AND ")
        + f"coalesce(bool_and(coalesce({source_scope}=jsonb_build_object('organization_id',{source_key},'source_generation',$8::text)"
        + tier_condition
        + ",false)),true)"
    )


async def _approved_graph_keys(connection, bindings, arguments, *, allow_empty=False, plan_tiers=False):
    if plan_tiers:
        from process.network_fhir_plan_tier_graph import validate_plan_tier_graph_bindings

        await validate_plan_tier_graph_bindings(connection, bindings, arguments)
    selected = await connection.fetchrow(
        "WITH " + bindings + " SELECT " + _graph_scope_valid(allow_empty, plan_tiers) + " AS valid,"
        "coalesce(array_agg(record_json->>'source_key' ORDER BY record_json->>'source_key') "
        "FILTER (WHERE $8::text IS NOT NULL),ARRAY[]::text[]) AS approved_keys FROM approved_network_bindings",
        *arguments,
    )
    require(selected["valid"] is True, "approved_catalog_scope")
    return selected["approved_keys"]


@dataclass(frozen=True)
class _PreparedBindingRows:
    relation: str
    count: int


def _mapping_binding_rows(bindings):
    if isinstance(bindings, _PreparedBindingRows):
        return bindings.relation
    return (
        "(WITH " + bindings + " SELECT record_json->>'source_key' AS source_key, "
        "record_json->'source_scope_json'->>'source_generation' AS source_generation, network_id "
        "FROM approved_network_bindings)"
    )


async def _prepare_mapping_bindings(connection, bindings, arguments, *, typed):
    from process.registry_imported_catalog_bindings import require_valid_catalog_bindings

    relation = "pg_temp." + _identifier("typed_network_bindings_" + uuid4().hex)
    if typed:
        await connection.execute(
            "CREATE TEMP TABLE "
            + relation
            + "(source_key text,source_generation text,network_id integer) ON COMMIT DROP"
        )
        selected = await connection.fetchrow(
            "WITH "
            + bindings
            + ", stored AS (INSERT INTO "
            + relation
            + " SELECT source_key,source_scope->>'source_generation',network_id FROM approved_network_bindings "
            "WHERE NOT (SELECT invalid FROM binding_check)) SELECT (SELECT invalid FROM binding_check) AS invalid,"
            + _graph_scope_valid(True, False, typed=True)
            + " AS valid,count(*) AS binding_count FROM approved_network_bindings "
            "WHERE NOT (SELECT invalid FROM binding_check)",
            *arguments,
        )
        require_valid_catalog_bindings(selected["invalid"])
    else:
        from process.network_fhir_plan_tier_graph import validate_plan_tier_graph_bindings

        await validate_plan_tier_graph_bindings(connection, bindings, arguments)
        selected = await connection.fetchrow(
            "WITH "
            + bindings
            + " SELECT "
            + _graph_scope_valid(True, True)
            + " AS valid,count(*) AS binding_count FROM approved_network_bindings",
            *arguments,
        )
    require(selected["valid"] is True, "approved_catalog_scope")
    if not typed:
        await connection.execute(
            "CREATE TEMP TABLE " + relation + " ON COMMIT DROP AS SELECT * FROM " + _mapping_binding_rows(bindings),
            *arguments[:-1],
        )
    await connection.execute("CREATE INDEX ON " + relation + "(source_key,source_generation)")
    await connection.execute("ANALYZE " + relation)
    return _PreparedBindingRows(relation, selected["binding_count"]), (arguments[-1],)


async def _map_pair_rows(connection, stage, mapping, bindings, arguments):
    async with connection.transaction():
        if isinstance(bindings, _PreparedBindingRows):
            return await _map_pair_pages(connection, stage, mapping, bindings, arguments)
        # Inline approval queries benefit from a hash join. The savepoint restores this setting on failure.
        original = await connection.fetchval("SHOW enable_nestloop")
        await connection.execute("SET LOCAL enable_nestloop=off")
        totals = await _map_pair_pages(connection, stage, mapping, bindings, arguments)
        await connection.execute("SELECT set_config('enable_nestloop',$1,true)", original)
        return totals


def _pair_binding_join(bindings, generation_parameter):
    # A hash over a small approval map is bounded by the page size.
    if isinstance(bindings, _PreparedBindingRows) and bindings.count > _PAIR_PAGE_SIZE:
        return (
            " LEFT JOIN LATERAL (SELECT network_id FROM "
            + bindings.relation
            + " WHERE source_key=p.network_source_key AND source_generation="
            + generation_parameter
            + " OFFSET 0) b ON true "
        )
    return (
        " LEFT JOIN "
        + _mapping_binding_rows(bindings)
        + " b ON b.source_key=p.network_source_key AND b.source_generation="
        + generation_parameter
    )


def _pair_page_sql(stage, mapping, bindings, arguments):
    generation_parameter = "$" + str(len(arguments)) + "::text"
    keys = ",".join(_PAIR_KEYS)
    descending = ",".join(key + " DESC" for key in _PAIR_KEYS)
    lower = ",".join(f"${len(arguments) + offset}::text" for offset in range(1, len(_PAIR_KEYS) + 1))
    evidence = (
        "encode(sha256(convert_to(jsonb_build_array('fhir-direct-network-office-v3',"
        + generation_parameter
        + ",resource_type,resource_id,network_source_key,location_resource_id,evidence_basis)::text,'UTF8')),'hex')"
    )
    joins = (
        " FROM page p"
        + _pair_binding_join(bindings, generation_parameter)
        + " LEFT JOIN LATERAL (SELECT location_key FROM "
        + mapping.relation("offices")
        + " o WHERE b.network_id IS NOT NULL AND (o.provider_id,o.location_id)=(p.provider_id,p.location_id) OFFSET 0) o ON true "
    )
    return (
        f"WITH page AS MATERIALIZED (SELECT {keys},provider_system,provider_id,location_id,evidence_basis,reference_rows "
        f"FROM {stage.relation('pairs')} WHERE ({keys})>({lower}) ORDER BY {keys} LIMIT {_PAIR_PAGE_SIZE}), "
        "resolved AS MATERIALIZED (SELECT p.*,b.network_id,o.location_key"
        + joins
        + "), mapped AS (INSERT INTO "
        + mapping.relation("memberships")
        + " SELECT network_id,provider_system,provider_id,location_id,"
        + evidence
        + f""" FROM resolved WHERE network_id IS NOT NULL AND location_key IS NOT NULL)
        SELECT (SELECT ARRAY[{keys}] FROM page ORDER BY {descending} LIMIT 1) AS last_keys,
          count(*) FILTER(WHERE network_id IS NOT NULL AND location_key IS NOT NULL) mapped_pair_rows,
          coalesce(sum(reference_rows) FILTER(WHERE network_id IS NOT NULL AND location_key IS NOT NULL),0)::bigint mapped_reference_rows,
          count(*) FILTER(WHERE network_id IS NULL) unapproved_pair_rows,
          coalesce(sum(reference_rows) FILTER(WHERE network_id IS NULL),0)::bigint unapproved_reference_rows,
          count(*) FILTER(WHERE network_id IS NOT NULL AND location_key IS NULL) unmatched_office_pair_rows,
          coalesce(sum(reference_rows) FILTER(WHERE network_id IS NOT NULL AND location_key IS NULL),0)::bigint unmatched_office_reference_rows
        FROM resolved"""
    )


async def _map_pair_pages(connection, stage, mapping, bindings, arguments):
    page_sql = _pair_page_sql(stage, mapping, bindings, arguments)
    after = ("", "", "", "")
    totals_by_kind = {}
    while True:
        counts_by_kind = dict(await connection.fetchrow(page_sql, *arguments, *after))
        after = counts_by_kind.pop("last_keys")
        for key, count_value in counts_by_kind.items():
            totals_by_kind[key] = totals_by_kind.get(key, 0) + count_value
        if after is None:
            return totals_by_kind


async def _map_provider_rows(connection, stage, mapping, bindings, arguments):
    origin = source_origin(stage.document["origin"])
    provider_sql = (
        PLAN_TIER_PROVIDER_ONLY_SQL if stage.document["contract"] == PLAN_TIER_CONTRACT else PROVIDER_ONLY_SQL
    ).replace("{cells}", stage.relation("cells"))
    # Source parameters follow the mapping generation and any live approval coordinates.
    generation_parameter = "$" + str(len(arguments)) + "::text"
    provider_sql = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + len(arguments)), provider_sql)
    for resource_type in ("OrganizationAffiliation", "PractitionerRole"):
        after = (resource_type, "")
        while upper := await connection.fetchrow(
            "SELECT resource_type,resource_id FROM (SELECT resource_type,resource_id FROM "
            + stage.relation("cells")
            + f" WHERE resource_type=$1 AND resource_id>$2 ORDER BY resource_id LIMIT {PROVIDER_ONLY_PAGE_SIZE}) page "
            "ORDER BY resource_id DESC LIMIT 1",
            *after,
        ):
            last_keys = tuple(upper)
            await connection.execute(
                "INSERT INTO "
                + mapping.relation("provider_only")
                + " SELECT b.network_id,'npi',p.provider_id,encode(sha256(convert_to(jsonb_build_array('fhir-provider-only-v3',"
                + generation_parameter
                + ",p.resource_type,p.resource_id,p.network_source_key)::text,'UTF8')),'hex') FROM ("
                + provider_sql
                + ") p JOIN "
                + _mapping_binding_rows(bindings)
                + " b ON b.source_key=p.network_source_key ON CONFLICT DO NOTHING",
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
    catalog_generation: str | None = None
    catalog_bindings: object = None


async def typed_graph_bindings(connection, approval, *, control_schema):
    """Select explicit imported authority, preserving the legacy manual approval path."""
    if approval.catalog_bindings is None:
        from process import network_approved_source_bindings

        await network_approved_source_bindings.require_approved_network_source_bindings(
            connection, approval.approved_source, approval.binding_coordinates, control_schema=control_schema
        )
        return APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema))
    from process.registry_imported_catalog_bindings import published_fhir_catalog_bindings_sql

    return await published_fhir_catalog_bindings_sql(
        connection,
        approval.catalog_bindings,
        approval.approved_source,
        approval.binding_coordinates,
        approval.catalog_descriptor,
        control_schema=control_schema,
    )


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
    """Prepare one graph through the same bounded lookup used by complete recipe batches."""
    mappings = await map_typed_graph_batch(
        connection,
        [(stage, approval, table_names)],
        address_source=address_source,
        runtime_roles=runtime_roles,
        control_schema=control_schema,
    )
    return mappings[0]


async def _prepare_mapping_offices(connection, stage, approval, table_names, control_schema, *, membership_table=None):
    approved_source = approval.approved_source
    binding_coordinates, catalog_descriptor = approval.binding_coordinates, approval.catalog_descriptor
    stage.require_ready()
    generation = require_catalog_graph_scope(stage, binding_coordinates, catalog_descriptor)
    if approval.catalog_generation is not None and approval.catalog_bindings is None:
        require(
            type(approval.catalog_generation) is str and re.fullmatch(r"[0-9a-f]{64}", approval.catalog_generation),
            "catalog_generation",
        )
        generation = approval.catalog_generation
    arguments = (approved_source.approved_revision, *binding_coordinates.sql_parameters, generation)
    is_plan_tier = stage.document["contract"] == PLAN_TIER_CONTRACT
    use_typed_rows = approval.catalog_bindings is not None and table_names is None and not is_plan_tier
    if use_typed_rows:
        from process.registry_imported_catalog_bindings import published_catalog_binding_query

        bindings = await published_catalog_binding_query(
            connection,
            approval.catalog_bindings,
            approved_source,
            binding_coordinates,
            catalog_descriptor,
            control_schema=control_schema,
        )
    else:
        bindings = await typed_graph_bindings(connection, approval, control_schema=control_schema)
    mapping = await _mapping_workspace(connection, stage, table_names, membership_table=membership_table)
    if approval.catalog_bindings is not None and table_names is None:
        bindings, arguments = await _prepare_mapping_bindings(connection, bindings, arguments, typed=use_typed_rows)
        approved = bindings
    else:
        approved = await _approved_graph_keys(
            connection, bindings, arguments, allow_empty=approval.catalog_bindings is not None, plan_tiers=is_plan_tier
        )
    await _office_inputs(connection, stage, mapping, approved)
    return mapping, approval, generation, arguments, bindings


async def _finish_mapping(connection, prepared, address_source, matches, control_schema):
    mapping, approval, generation, arguments, bindings = prepared
    stage, approved_source = mapping.source_stage, approval.approved_source
    await _match_offices(connection, mapping, address_source, matches)
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
        "counters": {**stage.document["counts"], **totals, **dict(office_counts)},
    }
    mapping.metadata["counters"]["strict_pair_rows"] = mapping.metadata["counters"].pop("pair_rows")
    mapping.metadata["mapping_sha256"] = digest(mapping.metadata)
    mapping.require_ready()
    return mapping


async def map_typed_graph_batch(
    connection, inputs, *, address_source, runtime_roles, control_schema=None, membership_table=None
):
    """Resolve every approved office before scanning the pinned address snapshot once for this batch."""
    from process.network_fhir_typed_office_lookup import typed_office_lookup

    require(bool(inputs), "mapping_inputs")
    async with connection.transaction():
        prepared_mappings = [
            await _prepare_mapping_offices(
                connection, stage, approval, names, control_schema, membership_table=membership_table
            )
            for stage, approval, names in inputs
        ]
        office_names = [selected[0].table_names["offices"] for selected in prepared_mappings]
        has_membership_stage = membership_table is not None
        async with typed_office_lookup(
            connection, office_names, address_source, owned_workspace=has_membership_stage
        ) as matches:
            mappings = []
            for selected in prepared_mappings:
                mapped = await _finish_mapping(connection, selected, address_source, matches, control_schema)
                if has_membership_stage:
                    # Truncate in the allocating savepoint to release the relation's storage now.
                    await connection.execute("TRUNCATE " + mapped.relation("offices"))
                mappings.append(mapped)
                await network_source_mapped()
        return mappings
