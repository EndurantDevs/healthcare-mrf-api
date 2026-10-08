# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose imported scores before native release-bound provider-rate paging."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from api import custom_import_plan_sql as plan_sql
from api import custom_import_plan_staging as plan_staging
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.custom_import_provider_sql import compile_npi_entity_relation
from api.plan_pricing_projection_contract import (
    COST_ORDER_FIELDS,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    row_mapping,
)
from api.plan_release_serving import annotate_plan_release_response, binding_query_args
from api.ptg2_code_scope import load_sealed_code_rows
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

NATIVE_ENTRY_IDS = "custom_import_native_entry_ids"
_PRICE_FILTERS = (
    "pos",
    "place_of_service",
    "service_code",
    "modifier",
    "modifiers",
    "billing_code_modifier",
    "rate",
    "negotiated_rate",
)


@dataclass(frozen=True)
class _NativeScope:
    binding: Any
    serving_tables: Any
    args: Mapping[str, Any]
    location_context: Any


@dataclass
class _NativeEntry:
    scope: _NativeScope
    candidate: Mapping[str, Any]
    key: tuple[Any, ...]
    serving_rows: list[dict[str, Any]] = field(default_factory=list)
    minimum_rate: Any = None


async def _native_scopes(session, selection, args, serving):
    scopes = []
    for binding in selection.in_network_bindings:
        serving_tables = selection.serving_tables_for_snapshot(binding.snapshot_id)
        if serving_tables is None:
            raise PlanPricingProjectionUnavailable("the selected release has no validated serving tables")
        binding_args = binding_query_args(args, binding)
        location_context = None
        if serving._has_location_filter(dict(binding_args), include_npi=False):
            # Offset disables KNN prefixes and keeps exact taxonomy admission.
            location_context = await serving._membership_location_query(
                session, serving_tables, dict(binding_args), candidate_npis=None, limit=1, offset=1
            )
            if location_context is None:
                raise PlanPricingProjectionUnavailable("the selected release has no exact address relation")
        scopes.append(_NativeScope(binding, serving_tables, binding_args, location_context))
    return scopes


async def _stage_imported_scores(session, import_context, compiled):
    """Index exact imported membership and ordering once for every native binding."""

    terms = import_context.prepared.normalized_order_terms
    source_sql = f"SELECT DISTINCT score.entity_value FROM ({compiled.sql}) score"
    rank_sql = "NULL::bigint"
    if terms:
        source_sql = compiled.sql
        imported_order = ", ".join(
            f"score.sort_{index} {term.direction.upper()} NULLS {term.nulls.upper()}"
            for index, term in enumerate(terms)
        )
        rank_sql = f"ROW_NUMBER() OVER (ORDER BY {imported_order}, score.entity_value)"
    statement = text(f"""
        INSERT INTO pg_temp.{plan_sql.IMPORTED} (npi, import_rank)
        SELECT CAST(score.entity_value AS bigint), {rank_sql} FROM ({source_sql}) score
    """).bindparams(*compiled.typed_binds)
    try:
        await session.execute(statement, dict(compiled.values))
    except IntegrityError:
        raise PTG2ManifestArtifactError("the imported eligibility relation repeated an NPI") from None
    await session.execute(plan_sql.plan_analyze_statement(plan_sql.IMPORTED))


def _candidate_query(scope, import_context, serving):
    """Join indexed imported identities to complete pinned native eligibility."""

    parameters_by_name = {"shared_snapshot_key": serving._required_shared_snapshot_key(scope.serving_tables)}
    predicates = ["native.snapshot_key = :shared_snapshot_key"]
    predicates.extend(serving._manifest_provider_predicates(scope.args, parameters_by_name, npi_sql="native.npi"))
    if scope.args.get("npi") not in (None, "", "null"):
        predicates.append("native.npi = :provider_npi")
        parameters_by_name["provider_npi"] = int(scope.args["npi"])
    location_join, location_columns = "", "NULL::double precision AS distance_miles, NULL::varchar AS location_hash"
    if scope.location_context is not None:
        location_sql = serving._unpaged_membership_location_sql(scope.location_context)
        location_join = f"JOIN ({location_sql}) location ON location.npi = native.npi"
        location_columns = "location.distance_miles, location.location_hash"
        parameters_by_name.update(scope.location_context.parameter_map)
    require_match_sql = "WHERE imported.npi IS NOT NULL" if import_context.require_match else ""
    sql = f"""
        WITH eligible_native AS MATERIALIZED (
            SELECT native.npi, {location_columns}
              FROM {serving._ptg2_npi_scope_table(scope.serving_tables)} native
              {location_join}
             WHERE {" AND ".join(predicates)}
        )
        SELECT native.npi, imported.npi IS NULL AS import_missing,
               native.distance_miles, native.location_hash, imported.import_rank
          FROM eligible_native native
          LEFT JOIN pg_temp.{plan_sql.IMPORTED} imported ON imported.npi = native.npi
         {require_match_sql}
    """
    return sql, parameters_by_name


async def _stage_scope(session, scope, code_rows, import_context, serving, needs_prices):
    """Stage factorized native inputs, with no provider or occurrence match cap."""

    sql, parameters_by_name = _candidate_query(scope, import_context, serving)
    statement = text(f"""
        INSERT INTO pg_temp.{plan_sql.CANDIDATES}
            (binding_ordinal, npi, import_missing, import_rank, distance_miles, location_hash)
        SELECT :native_binding_ordinal, npi, import_missing, import_rank, distance_miles, location_hash
          FROM ({sql}) eligible
    """)
    try:
        await session.execute(
            statement, {**parameters_by_name, "native_binding_ordinal": scope.binding.binding_ordinal}
        )
    except IntegrityError:
        raise PTG2ManifestArtifactError("the native eligibility relation repeated an NPI") from None
    await plan_staging.stage_native_memberships(session, scope, serving)
    await plan_staging.stage_native_occurrences(session, scope, code_rows, serving)
    if needs_prices:
        await plan_staging.stage_native_prices(session, scope, serving)


async def _select_native_entries(session, scopes, import_context, args, pagination, needs_prices):
    parameters_by_name = dict(needs_prices=needs_prices, page_limit=pagination.limit, page_offset=pagination.offset)
    query_result = await session.execute(plan_sql.plan_page_statement(import_context, args), parameters_by_name)
    page_rows = [row_mapping(query_row) for query_row in query_result]
    total_count = int(page_rows[0]["total"])
    if page_rows[0]["npi"] is None:
        return total_count, []
    scopes_by_ordinal = {scope.binding.binding_ordinal: scope for scope in scopes}
    selected_entries = [
        _NativeEntry(
            scopes_by_ordinal[int(page_by_field["binding_ordinal"])],
            page_by_field,
            plan_sql.native_group_key(page_by_field),
            minimum_rate=page_by_field["minimum_rate"],
        )
        for page_by_field in page_rows
    ]
    selected_records = (
        (
            page_ordinal,
            page_by_field["binding_ordinal"],
            page_by_field["npi"],
            page_by_field["location_key"],
            *(page_by_field[column_name] for column_name in plan_sql.GROUP_COLUMNS),
        )
        for page_ordinal, page_by_field in enumerate(page_rows)
    )
    await plan_staging.copy_plan_rows(session, plan_sql.SELECTED, plan_sql.SELECTED_COPY_COLUMNS, selected_records)
    query_stream = await session.stream(plan_sql.plan_completion_statement(), {"needs_prices": needs_prices})
    async for query_row in query_stream:
        occurrence_by_field = row_mapping(query_row)
        selected_entries[int(occurrence_by_field["page_ordinal"])].serving_rows.append(
            occurrence_by_field["serving_data"]
        )
    if any(not entry.serving_rows for entry in selected_entries):
        raise PTG2ManifestArtifactError("the selected native group has no complete occurrences")
    return total_count, selected_entries


async def _providers_for_scope(session, scope, npis, serving):
    providers_by_npi = await serving._enriched_provider_rows_for_npis(
        session,
        npis=npis,
        limit=len(npis),
        plan_id=scope.binding.plan_id,
        snapshot_id=scope.binding.snapshot_id,
        source_key=scope.binding.source_key,
    )
    if providers_by_npi is None:
        raise PlanPricingProjectionUnavailable("native provider completion is unavailable")
    providers_by_npi = {int(provider["npi"]): provider for provider in providers_by_npi}
    if scope.location_context is not None:
        witnesses = await serving._membership_location_rows(
            session, scope.serving_tables, dict(scope.args), candidate_npis=npis, limit=len(npis)
        )
        locations_by_npi = {
            int(serving_row["npi"]): serving_row
            for serving_row in witnesses or ()
            if serving_row.get("npi") is not None
        }
        if set(locations_by_npi) != set(npis):
            raise PTG2ManifestArtifactError("imported plan page lost a selected native address witness")
        providers_by_npi = {
            npi: serving._graph_provider_data(
                locations_by_npi[npi], providers_by_npi.get(npi), "entity_address_unified"
            )
            for npi in npis
        }
    if set(providers_by_npi) != set(npis):
        raise PTG2ManifestArtifactError("imported plan page lost a selected provider")
    return providers_by_npi


async def _hydrate_scope(session, scope, entries, cached_prices, serving):
    npis = tuple(sorted({int(entry.candidate["npi"]) for entry in entries}))
    providers_by_npi = await _providers_for_scope(session, scope, npis, serving)
    serving_rows = [serving_row for entry in entries for serving_row in entry.serving_rows]
    prices_by_key = cached_prices or await serving._version_three_prices_by_key(
        session, scope.serving_tables, tuple(sorted({int(serving_row["price_key"]) for serving_row in serving_rows}))
    )
    if any(not prices_by_key.get(int(serving_row["price_key"])) for serving_row in serving_rows):
        raise PTG2ManifestArtifactError("imported plan page has an incomplete native price set")
    prices_by_key = {
        price_key: serving._ptg2_manifest_filter_prices(prices, dict(scope.args))
        for price_key, prices in prices_by_key.items()
    }
    if any(not prices_by_key.get(int(serving_row["price_key"])) for serving_row in serving_rows):
        raise PTG2ManifestArtifactError("imported plan page lost native price eligibility")
    procedure_details_by_key = await serving._procedure_details_for_rows(session, serving_rows)
    provenance_by_key = (
        await serving._ptg2_source_provenance_for_rows(session, scope.serving_tables, serving_rows)
        if serving._include_ptg2_sources(dict(scope.args))
        else {}
    )
    billing_associations_by_set = await serving._billing_associations_for_exact_npi_request(
        session,
        scope.serving_tables,
        include_providers=True,
        explicit_npi_scope=await serving._version_three_explicit_npi_graph_scope(
            session, scope.serving_tables, dict(scope.args)
        ),
        serving_rows=serving_rows,
    )
    return [
        _complete_native_entry(
            scope,
            entry,
            providers_by_npi,
            prices_by_key,
            procedure_details_by_key,
            provenance_by_key,
            billing_associations_by_set,
            serving,
        )
        for entry in entries
    ]


def _complete_native_entry(
    scope,
    entry,
    providers_by_npi,
    prices_by_key,
    procedure_details_by_key,
    provenance_by_key,
    billing_associations_by_set,
    serving,
):
    """Use native row shaping and verify the complete selected source identity."""
    provider_rates = []
    for serving_by_field in entry.serving_rows:
        serving_by_field = {
            **serving_by_field,
            "source_artifact_key": serving_by_field.get("source_artifact_key", serving_by_field.get("source_key")),
            "logical_source_key": scope.binding.source_key,
        }
        source_provenance = provenance_by_key.get(int(serving_by_field["source_artifact_key"]))
        if source_provenance is not None:
            serving_by_field.update(serving._item_source_provenance(source_provenance))
        catalog_key = serving._catalog_key(
            serving_by_field.get("reported_code_system"), serving_by_field.get("reported_code")
        ) or ("", "")
        provider_rate = serving._ptg2_manifest_provider_procedure_item(
            npi=int(entry.candidate["npi"]),
            serving_data=serving_by_field,
            prices=prices_by_key[int(serving_by_field["price_key"])],
            procedure_detail=procedure_details_by_key.get(catalog_key, {}),
            provider_context=providers_by_npi[int(entry.candidate["npi"])],
            args=dict(scope.args),
        )
        if providers_by_npi[int(entry.candidate["npi"])].get("provider_sex_code") is not None:
            provider_rate["provider_sex_code"] = providers_by_npi[int(entry.candidate["npi"])]["provider_sex_code"]
        provider_rates.append(provider_rate)
    provider_rates = serving._merge_provider_rates_for_request(provider_rates, billing_associations_by_set)
    if len(provider_rates) != 1:
        raise PTG2ManifestArtifactError("imported plan completion changed native entry identity")
    key = serving._ptg2_provider_rate_group_key(provider_rates[0])
    if key[0] != entry.key[0] or key[2:] != entry.key[2:]:
        raise PTG2ManifestArtifactError("imported plan completion changed native source identity")
    return provider_rates[0]


async def search_imported_plan_providers(session, args, pagination, selection, import_context):
    """Count and page complete native groups in SQL, then hydrate only the page."""

    from api import ptg2_serving as serving

    if type(import_context) is not ProviderServiceImportQuery or selection is None:
        raise PlanPricingProjectionUnavailable("imported plan queries require a pinned release")
    if args.get("q"):
        raise PlanPricingProjectionUnsupported("the sealed native plan lane requires an exact procedure code")
    if str(args.get("view") or "full") != "full" or not serving._is_request_flag_enabled(
        args.get("include_providers"), default=True
    ):
        raise PlanPricingProjectionUnsupported("imported provider scores require full provider entries")
    scopes = await _native_scopes(session, selection, args, serving)
    compiled = compile_npi_entity_relation(import_context.prepared.statement)
    scope_code_rows = []
    for scope in scopes:
        code_rows = await load_sealed_code_rows(session, scope.serving_tables, scope.args)
        if code_rows:
            scope_code_rows.append((scope, code_rows))
    if scope_code_rows:
        await _stage_imported_scores(session, import_context, compiled)
    needs_prices = any(args.get(name) for name in _PRICE_FILTERS) or (
        not import_context.prepared.normalized_order_terms
        and str(args.get("order_by") or "total_allowed_amount") in COST_ORDER_FIELDS
    )
    for scope, code_rows in scope_code_rows:
        await _stage_scope(session, scope, code_rows, import_context, serving, needs_prices)
    for table_name in (plan_sql.CANDIDATES, plan_sql.MEMBERSHIPS, plan_sql.OCCURRENCES, plan_sql.PRICES):
        await session.execute(plan_sql.plan_analyze_statement(table_name))
    total, selected = await _select_native_entries(session, scopes, import_context, args, pagination, needs_prices)
    provider_rates = await _hydrate_selected_entries(session, scopes, selected, serving)
    return _plan_response(selection, args, pagination, total, selected, provider_rates, serving)


async def _hydrate_selected_entries(session, scopes, selected, serving):
    """Batch completion by binding and restore the globally selected entry order."""
    completed_by_entry = {}
    for scope in scopes:
        scoped_entries = [entry for entry in selected if entry.scope is scope]
        if not scoped_entries:
            continue
        provider_rates = await _hydrate_scope(session, scope, scoped_entries, {}, serving)
        for entry, provider_rate in zip(scoped_entries, provider_rates, strict=True):
            if len(scopes) > 1:
                provider_rate.setdefault("network", scope.binding.source_key)
            completed_by_entry[id(entry)] = provider_rate
    return [completed_by_entry[id(entry)] for entry in selected]


def _plan_response(selection, args, pagination, total, selected, provider_rates, serving):
    """Bind unique native identities and exact totals to the pinned release."""
    identities = [
        hashlib.sha256(
            json.dumps(
                (entry.scope.binding.binding_ordinal, serving._ptg2_provider_rate_group_key(provider_rate)),
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        for entry, provider_rate in zip(selected, provider_rates, strict=True)
    ]
    if len(set(identities)) != len(identities):
        raise PTG2ManifestArtifactError("imported plan completion repeated a native entry")
    serving._hide_source_artifact_key_unless_requested(provider_rates, args)
    query_by_field = serving._plan_release_no_match_query(selection, args)
    query_by_field["status"] = "matched" if total else "no_match"
    response_by_field = dict(
        items=provider_rates,
        pagination=dict(
            total=total,
            total_is_exact=True,
            limit=pagination.limit,
            offset=pagination.offset,
            page=pagination.page,
            has_more=pagination.offset + len(provider_rates) < total,
        ),
        query=query_by_field,
        pricing_scope="plan_scoped_ptg",
        resolved=True,
        result_state="matched" if total else "no_matching_rates",
    )
    response_by_field[NATIVE_ENTRY_IDS] = identities
    return annotate_plan_release_response(serving._shape_ptg2_response(response_by_field, args), selection)
