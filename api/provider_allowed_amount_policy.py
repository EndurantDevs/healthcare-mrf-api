# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Role-specific allowed evidence admission and exact retained office inputs."""

import asyncpg
from sqlalchemy.exc import DBAPIError

from api import provider_service_source_policy as source_policy
from api.network_address_scope import (
    current_network_address_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_readiness import has_allowed_amount_binding_coverage
from process.network_address_projection import _identifier
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.domain import PTG2_DOMAIN_ALLOWED_AMOUNT
from process.registry_network_catalog_read import RegistryNetworkCatalogError


async def _allowed_snapshots(driver, bindings):
    schema = _identifier(source_policy.PTG2_SCHEMA)
    snapshot_rows = await source_policy._bounded_rows(
        driver,
        f"SELECT snapshot_id,status,manifest FROM {schema}.ptg2_snapshot WHERE snapshot_id=ANY($1::text[])",
        sorted({binding.snapshot_id for binding in bindings}),
    )
    snapshots_by_id = {entry["snapshot_id"]: entry for entry in snapshot_rows}
    if len(snapshots_by_id) != len(snapshot_rows) or set(snapshots_by_id) != {
        binding.snapshot_id for binding in bindings
    }:
        raise ValueError("allowed snapshot identity is incomplete")
    for binding in bindings:
        entry = snapshots_by_id[binding.snapshot_id]
        index = entry["manifest"].get("allowed_amount_index") if type(entry["manifest"]) is dict else None
        if (
            entry["status"] != "published"
            or type(index) is not dict
            or any(
                index.get(name) != required_value
                for name, required_value in {
                    "contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
                    "arch_version": "postgres_binary_v3",
                    "storage": "postgresql",
                    "data_domain": PTG2_DOMAIN_ALLOWED_AMOUNT,
                    "source_key": binding.source_key,
                    "snapshot_scoped": True,
                    "allowed_amount_evidence": True,
                }.items()
            )
        ):
            raise ValueError("allowed snapshot contract differs")
        if any(index.get(name) is not True for name in ("snapshot_scoped", "allowed_amount_evidence")):
            raise ValueError("allowed snapshot flags are malformed")
        if any(
            type(index.get(name)) is not int or index[name] <= 0
            for name in ("allowed_amount_payments", "allowed_amount_provider_payments")
        ):
            raise ValueError("allowed snapshot has no attested evidence")
    return snapshots_by_id


def _allowed_review_commands(encoded):
    """Select the closed role grammar before parsing negotiated-only evidence."""
    from process.registry_published_plan_binding_refs import _review_commands

    return _review_commands(encoded)


def _mapped_allowed_units(mappings, reviews, snapshots, exclusions, *, organization_policy, network_ids=None):
    from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256

    reviewed_units = set()
    negotiated_by_reference = {}
    for reference, command in reviews.items():
        if command["review_type"] != "published_allowed_amount_plan":
            negotiated_by_reference[reference] = command
            continue
        identity = command["published_identity"]
        snapshot = snapshots[identity["snapshot_id"]]
        if (
            command["source"]["ptg_schema_name"] != source_policy.PTG2_SCHEMA
            or identity["snapshot_manifest_sha256"] != result_archive_manifest_sha256(snapshot["manifest"])
            or identity["source_key"] != snapshot["manifest"]["allowed_amount_index"]["source_key"]
        ):
            raise ValueError("allowed review source identity differs")
        reviewed_units.add(
            (identity["source_key"], identity["snapshot_id"], command["plan_id"], command["plan_market_type"])
        )
    reviewed_units.update(source_policy._reviewed_units(negotiated_by_reference, snapshots))
    eligible = source_policy._eligible_units(
        mappings, reviews, reviewed_units, exclusions, organization_policy=organization_policy, network_ids=network_ids
    )
    return reviewed_units, eligible


async def admitted_allowed_amount_bindings(session, selection):
    """Admit original role ordinals before readiness reads; never synthesize a V4 review."""
    bindings = selection.allowed_amount_bindings
    scope = current_network_address_scope()
    if scope is None:
        return bindings
    try:
        bindings = source_policy._original_role_bindings(selection, "allowed_amounts", scope)
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        await source_policy._catalog_manifest(
            driver, scope.manifest.generation_id, source_policy.registry_schema(), scope.manifest
        )
        snapshots = await _allowed_snapshots(driver, bindings)
        reviews, mappings = await source_policy._source_evidence(
            driver,
            bindings,
            scope.manifest.approved_custom_revision,
            _identifier(source_policy.registry_schema()),
            review_loader=_allowed_review_commands,
        )
        reviewed_units, eligible = _mapped_allowed_units(
            mappings,
            reviews,
            snapshots,
            scope.excluded_network_ids or (),
            organization_policy=scope.organization_policy,
            network_ids=scope.network_ids,
        )
        admitted_bindings = tuple(
            binding
            for binding in bindings
            if source_policy._unit(binding) in eligible
            or (scope.network_ids is None and source_policy._unit(binding) not in reviewed_units)
        )
        from api.provider_service_legacy_selection import restrict_service_bindings

        admitted_bindings = restrict_service_bindings(selection, admitted_bindings)
        for binding in admitted_bindings:
            if not await has_allowed_amount_binding_coverage(session, binding):
                raise ValueError("admitted allowed binding is not ready")
        return admitted_bindings
    except (
        ValueError,
        KeyError,
        TypeError,
        asyncpg.PostgresError,
        asyncpg.InterfaceError,
        DBAPIError,
        RegistryNetworkCatalogError,
        source_policy.PtgResultArchiveSourceAuthorityError,
    ):
        raise PlanPricingProjectionUnavailable("managed allowed source admission unavailable") from None


async def allowed_office_inputs(session):
    """Resolve the same immutable provider dependencies used by retained office readers."""
    scope = current_network_address_scope()
    if scope is None:
        return None
    from process.network_em_provider_inputs import resolve_em_provider_inputs

    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    return await resolve_em_provider_inputs(driver, scope.manifest)


def allowed_address_relation(schema):
    """A managed candidate must match an eligible office even without a locator."""
    return scoped_address_relation_sql(f"{schema}.entity_address_unified")


def allowed_policy_parameters(parameters):
    """Bind the exact approved revision and exclusions to count/page SQL."""
    return scoped_address_parameters(parameters)


def allowed_geo_joins(inputs, schema):
    """Join held postal and polygon references for this office."""
    from api import ptg2_geo_policy as geo

    bindings = inputs.geo_bindings(schema)
    return (
        geo.provider_address_identity_reference_joins_sql(
            "addr",
            schema_name=schema,
            geo_zip_alias="allowed_zip_ref",
            zip_state_alias="allowed_state_ref",
            dependency_bindings=bindings,
        )
        + "\nLEFT "
        + geo.provider_address_point_reference_join_sql(
            "addr", zcta_alias="allowed_zcta_ref", schema_name=schema, dependency_bindings=bindings
        )
    )


def allowed_geo_evidence(inputs, schema):
    """Classify this exact office against held provider evidence."""
    from api.ptg2_geo_projection import legacy_evidence_source_id_sql

    scope = current_network_address_scope()
    return f"({legacy_evidence_source_id_sql('addr', schema_name=schema, dependency_bindings=inputs.geo_bindings(schema), unified_schema_name=scope.manifest.schema_name)})<>0"


def allowed_spatial_predicate(inputs, schema, zip_predicate, radius_predicates):
    """Preserve exact ZIP-without-point semantics on held postal and polygon references."""
    from api import ptg2_geo_policy as geo

    identity = geo.provider_address_identity_coherence_sql(
        "addr", schema_name=schema, use_projection=False, reference_aliases=("allowed_zip_ref", "allowed_state_ref")
    )
    point = geo.provider_address_point_coherence_sql("addr", use_projection=False, zcta_alias="allowed_zcta_ref")
    missing = "(addr.lat IS NULL AND addr.long IS NULL)"
    radius = f"({point} AND ({' AND '.join(radius_predicates)}))" if radius_predicates else None
    if zip_predicate and radius:
        return f"({identity} AND (({zip_predicate} AND {missing}) OR {radius}))"
    if zip_predicate:
        return f"({zip_predicate} AND {identity} AND ({missing} OR {point}))"
    return f"({identity} AND {radius})" if radius else None


def allowed_contributors(selection, args):
    """Private typed coordinates cannot be supplied by the public native wire."""
    if current_network_address_scope() is None:
        return selection.allowed_amount_bindings
    bindings = args.get("_managed_allowed_bindings")
    if type(bindings) is not tuple or any(binding not in selection.allowed_amount_bindings for binding in bindings):
        raise PlanPricingProjectionUnavailable("managed allowed binding coordinates are unavailable")
    from api.provider_service_legacy_selection import restrict_service_bindings

    return restrict_service_bindings(selection, bindings)


def allowed_detail_statement(scope_ctes):
    """Render bounded detail evidence only for the admitted page NPIs."""
    from sqlalchemy import text

    return text(
        f"""
        WITH {scope_ctes},
        ranked_evidence AS (
            SELECT
                evidence.*,
                ROW_NUMBER() OVER (
                    PARTITION BY evidence.npi
                    ORDER BY
                          evidence.allowed_amount ASC NULLS LAST,
                          evidence.billed_charge ASC NULLS LAST,
                          evidence.source_key,
                          evidence.snapshot_id,
                          evidence.tin_type,
                          evidence.tin_value
                ) AS evidence_rank
              FROM matching_evidence evidence
             WHERE evidence.npi = ANY(CAST(:page_npis AS bigint[]))
        )
        SELECT *
          FROM ranked_evidence
         WHERE evidence_rank <= :detail_limit
         ORDER BY npi, evidence_rank
        """
    )


def allowed_location_join_sql(
    *,
    address_table: str,
    zip5_sql: str,
    address_predicates: list[str],
    address_rank_sql: str | None,
    distance_sql: str,
    reference_join_sql: str = "",
) -> str:
    """Render the indexed best-address lateral lookup."""

    address_rank_order_sql = f"{address_rank_sql},\n               " if address_rank_sql else ""
    return f"""
    LEFT JOIN LATERAL (
        SELECT
            addr.npi,
            addr.state_name AS state,
            addr.city_name AS city,
            {zip5_sql} AS zip5,
            {distance_sql} AS distance_miles,
            jsonb_build_object(
                'first_line', addr.first_line, 'second_line', addr.second_line,
                'city', addr.city_name, 'state', addr.state_name,
                'postal_code', addr.postal_code, 'country_code', addr.country_code,
                'telephone_number', addr.telephone_number, 'fax_number', addr.fax_number,
                'phone_number', addr.phone_number, 'phone_extension', addr.phone_extension,
                'fax_number_digits', addr.fax_number_digits, 'fax_extension', addr.fax_extension,
                'address_key', addr.address_key::text, 'lat', addr.lat,
                'long', addr.long
            )::text AS address_payload
          FROM {address_table} addr{reference_join_sql}
         WHERE {" AND ".join(address_predicates)}
         ORDER BY
               {address_rank_order_sql}{distance_sql} ASC NULLS LAST,
               CASE addr.type
                   WHEN 'practice' THEN 0
                   WHEN 'site' THEN 1
                   WHEN 'primary' THEN 2
                   ELSE 3
               END,
               addr.checksum
         LIMIT 1
    ) allowed_location ON TRUE
    """


def unavailable_route():
    """An unresolved managed release is unavailable rather than an unscoped fallback."""
    if current_network_address_scope() is not None:
        from sanic.exceptions import ServiceUnavailable

        raise ServiceUnavailable("Managed allowed release is unavailable.")
    return None
