# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact sealed code metadata under the existing logical plan authority."""

from sqlalchemy import text

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


async def load_sealed_code_rows(session, serving_tables, args):
    """Preserve native plan, market, snapshot, and compatible-code predicates."""

    from api import ptg2_serving as serving

    system = serving._normalize_code_system(args.get("code_system") or args.get("reported_code_system"))
    raw_code = args.get("code") or args.get("reported_code")
    code = serving.canonical_catalog_code(system, raw_code) if system else str(raw_code or "").strip()
    scope_join, predicates, code_parameters_by_name, plan_order = serving._shared_v3_code_scope_sql(
        serving_tables,
        requested_plan=str(args.get("plan_id") or args.get("plan_external_id") or "").strip(),
        plan_market_type=args.get("plan_market_type") or args.get("market_type") or "",
    )
    predicates.append("code_metadata.snapshot_key = :shared_snapshot_key")
    code_parameters_by_name["shared_snapshot_key"] = serving._required_shared_snapshot_key(serving_tables)
    serving._append_reported_code_system_filter(
        predicates, code_parameters_by_name, column="code_metadata.reported_code_system", code_system=system
    )
    reported_code_values = serving._ptg2_reported_code_lookup_values(system, code)
    if not reported_code_values:
        return []
    serving._append_reported_code_value_filter(
        predicates,
        code_parameters_by_name,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=reported_code_values,
    )
    code_query_result = await session.execute(
        text(f"""
        SELECT code_metadata.code_key, logical_scope.plan_id, logical_scope.plan_market_type,
               code_metadata.reported_code_system, code_metadata.reported_code,
               code_metadata.negotiation_arrangement, code_metadata.billing_code_type_version,
               code_metadata.source_name, code_metadata.source_description, code_metadata.rate_count
          FROM {serving._shared_v3_code_table(serving_tables)} code_metadata {scope_join}
         WHERE {" AND ".join(predicates)}
         ORDER BY {plan_order}, CASE WHEN code_metadata.reported_code = :reported_code THEN 0 ELSE 1 END,
                  code_metadata.code_key
    """),
        code_parameters_by_name,
    )
    code_rows = [serving._canonical_code_metadata_row(code_record) for code_record in code_query_result]
    if any(code_record.get("code_key") is None for code_record in code_rows):
        raise PTG2ManifestArtifactError("PTG2 shared code dictionary contains an invalid key")
    return code_rows
