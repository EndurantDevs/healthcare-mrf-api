# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed resolution for published, sealed shared PTG V3 snapshots."""

from __future__ import annotations

import os

from sqlalchemy import text

from api.ptg2_candidate_audit import (
    PTG2CandidateAuditAccess,
    candidate_audit_access_from_args,
)
from api.ptg2_serving_utils import ein_plan_id_variants
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS,
)

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
_PTG2_ATTESTATION_CONTRACT_SQL = ", ".join(
    f"'{contract}'"
    for contract in PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
)


def _serving_relation_available_sql(
    snapshot_alias: str,
    *,
    require_activated_attestation: bool = True,
) -> str:
    """Return relational availability SQL without inflating snapshot JSON."""

    attestation_join = ""
    attestation_checks = ""
    if require_activated_attestation:
        attestation_join = f"""
              JOIN {PTG2_SCHEMA}.ptg2_v3_candidate_audit_attestation shared_attestation
                ON shared_attestation.snapshot_id = shared_binding.snapshot_id
               AND shared_attestation.snapshot_key = shared_binding.snapshot_key
               AND shared_attestation.coverage_scope_id = shared_scope.coverage_scope_id
               AND shared_attestation.plan_id = shared_scope.plan_id
               AND shared_attestation.plan_market_type
                   = shared_scope.plan_market_type
        """
        attestation_checks = f"""
               AND shared_attestation.contract
                   IN ({_PTG2_ATTESTATION_CONTRACT_SQL})
               AND shared_attestation.activated_at IS NOT NULL
        """
    return f"""
        EXISTS (
            SELECT 1
              FROM {PTG2_SCHEMA}.ptg2_v3_snapshot_binding shared_binding
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_layout shared_layout
                ON shared_layout.snapshot_key = shared_binding.snapshot_key
              JOIN {PTG2_SCHEMA}.ptg2_v3_snapshot_scope shared_scope
                ON shared_scope.snapshot_id = shared_binding.snapshot_id
              {attestation_join}
             WHERE shared_binding.snapshot_id = {snapshot_alias}.snapshot_id
               AND shared_layout.state = 'sealed'
               AND shared_layout.generation = 'shared_blocks_v3'
               {attestation_checks}
        )
    """


def _logical_network_key_sql(pointer_alias: str, snapshot_alias: str) -> str:
    """Group dated source files that publish the same logical plan network."""
    network_names = f"{snapshot_alias}.manifest->'serving_index'->>'network_names'"
    return (
        f"COALESCE(NULLIF(NULLIF({network_names}, ''), '[]'), "
        f"{pointer_alias}.source_key)"
    )


def _source_effective_month_sql(pointer_alias: str, snapshot_alias: str) -> str:
    """Prefer the month encoded in a source URL over the orchestration month."""
    source_url = f"{snapshot_alias}.manifest->'successful_files'->0->>'url'"
    source_month = f"substring({source_url} FROM '(20[0-9]{{2}}-(0[1-9]|1[0-2]))')"
    return f"COALESCE(({source_month} || '-01')::date, {pointer_alias}.import_month)"


async def current_snapshot_id(
    session,
    requested_snapshot_id: str | None = None,
    requested_source_key: str | None = None,
    requested_plan_id: str | None = None,
    requested_plan_market_type: str | None = None,
    candidate_audit_access: PTG2CandidateAuditAccess | None = None,
) -> str | None:
    """Return a published snapshot only when every explicit selector binds."""
    if requested_snapshot_id:
        query_params_by_name: dict[str, object] = {
            "snapshot_id": str(requested_snapshot_id),
        }
        status_sql = "status = 'published'"
        if candidate_audit_access is not None:
            if not candidate_audit_access.matches(
                snapshot_id=requested_snapshot_id,
                source_key=requested_source_key,
                plan_id=requested_plan_id,
                plan_market_type=requested_plan_market_type,
            ):
                return None
            query_params_by_name.update(
                candidate_activation_contract=PTG2_CANDIDATE_ACTIVATION_CONTRACT,
                candidate_source_key=candidate_audit_access.source_key,
            )
            status_sql = """
                status = 'validated'
                AND manifest->'activation'->>'contract'
                    = :candidate_activation_contract
                AND manifest->'activation'->>'state' = 'validated'
                AND lower(btrim(COALESCE(
                    manifest->'activation'->>'source_key', ''
                ))) = :candidate_source_key
            """
            relation_available_sql = _serving_relation_available_sql(
                "ptg2_snapshot",
                require_activated_attestation=False,
            )
        else:
            relation_available_sql = _serving_relation_available_sql(
                "ptg2_snapshot"
            )
        source_sql = ""
        normalized_source_key: str | None = None
        if requested_source_key is not None:
            normalized_source_key = str(requested_source_key).strip().lower()
            if not normalized_source_key:
                return None
            query_params_by_name["source_key"] = normalized_source_key
            if candidate_audit_access is not None:
                source_sql = """
                       AND lower(btrim(COALESCE(
                           ptg2_snapshot.manifest->'serving_index'->>'source_key',
                           ''
                       ))) = :source_key
                """
            else:
                source_sql = f"""
                       AND EXISTS (
                           SELECT 1
                             FROM {PTG2_SCHEMA}.ptg2_v3_candidate_audit_attestation source_attestation
                            WHERE source_attestation.snapshot_id = ptg2_snapshot.snapshot_id
                              AND source_attestation.source_key = :source_key
                              AND source_attestation.contract
                                  IN ({_PTG2_ATTESTATION_CONTRACT_SQL})
                              AND source_attestation.activated_at IS NOT NULL
                       )
                """
        plan_sql = ""
        if requested_plan_id is not None:
            normalized_plan_id = str(requested_plan_id).strip()
            if not normalized_plan_id:
                return None
            query_params_by_name["plan_ids"] = ein_plan_id_variants(
                normalized_plan_id
            )
            market_sql = ""
            normalized_market_type = str(
                requested_plan_market_type or ""
            ).strip().lower()
            if normalized_market_type:
                query_params_by_name["plan_market_type"] = normalized_market_type
                market_sql = (
                    "AND snapshot_scope.plan_market_type = :plan_market_type"
                )
            plan_sql = f"""
                   AND EXISTS (
                       SELECT 1
                         FROM {PTG2_SCHEMA}.ptg2_v3_snapshot_plan_scope snapshot_scope
                        WHERE snapshot_scope.snapshot_id = ptg2_snapshot.snapshot_id
                          AND snapshot_scope.plan_id = ANY(CAST(:plan_ids AS text[]))
                          {market_sql}
                   )
            """
        snapshot_result = await session.execute(
            text(
                f"""
                SELECT snapshot_id
                 FROM {PTG2_SCHEMA}.ptg2_snapshot
                 WHERE snapshot_id = :snapshot_id
                   AND {status_sql}
                   AND {relation_available_sql}
                   {source_sql}
                   {plan_sql}
                 LIMIT 1
                """
            ),
            query_params_by_name,
        )
        snapshot_value = snapshot_result.scalar()
        return str(snapshot_value) if snapshot_value else None
    snapshot_result = await session.execute(
        text(
            f"""
            SELECT pointer.snapshot_id
              FROM {PTG2_SCHEMA}.ptg2_current_snapshot pointer
              JOIN {PTG2_SCHEMA}.ptg2_snapshot published_snapshot
                ON published_snapshot.snapshot_id = pointer.snapshot_id
             WHERE pointer.slot = 'current'
               AND published_snapshot.status = 'published'
               AND {_serving_relation_available_sql('published_snapshot')}
            """
        )
    )
    snapshot_value = snapshot_result.scalar()
    return str(snapshot_value) if snapshot_value else None


async def current_source_snapshot_id(session, source_key: str) -> str | None:
    """Return the published serving snapshot currently selected for one source."""
    normalized_source_key = str(source_key or "").strip().lower()
    if not normalized_source_key:
        return None
    result = await session.execute(
        text(
            f"""
            SELECT pointer.snapshot_id
              FROM {PTG2_SCHEMA}.ptg2_current_source_snapshot pointer
              JOIN {PTG2_SCHEMA}.ptg2_snapshot published_snapshot
                ON published_snapshot.snapshot_id = pointer.snapshot_id
             WHERE pointer.source_key = :source_key
               AND published_snapshot.status = 'published'
               AND {_serving_relation_available_sql('published_snapshot')}
             LIMIT 1
            """
        ),
        {"source_key": normalized_source_key},
    )
    value = result.scalar()
    return str(value) if value else None


async def current_source_snapshot_id_for_plan(session, args: dict[str, object]) -> str | None:
    """Resolve the current strict snapshot for a plan and optional source."""

    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return None
    plan_variants = ein_plan_id_variants(requested_plan)
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    # Plan-source pointers are updated by importer worker processes while API
    # servers keep their own memory. Do not cache this lookup in-process, or a
    # just-published network can stay invisible until the API pod's TTL expires.
    query_parameters_by_name: dict[str, object] = {"plan_ids": plan_variants}
    market_sql = ""
    if market_type:
        query_parameters_by_name["plan_market_type"] = market_type
        market_sql = "AND cps.plan_market_type = :plan_market_type"
    source_sql = ""
    if source_key:
        query_parameters_by_name["source_key"] = source_key
        source_sql = "AND cps.source_key = :source_key"
    effective_month_sql = _source_effective_month_sql("cps", "s")
    snapshot_query = await session.execute(
        text(
            f"""
             SELECT cps.snapshot_id
              FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
              JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
             WHERE cps.plan_id = ANY(CAST(:plan_ids AS text[]))
               {market_sql}
               {source_sql}
               AND s.status = 'published'
               AND {_serving_relation_available_sql('s')}
             ORDER BY {effective_month_sql} DESC NULLS LAST,
                      cps.import_month DESC NULLS LAST,
                      cps.updated_at DESC NULLS LAST
             LIMIT 1
            """
        ),
        query_parameters_by_name,
    )
    snapshot_id_value = snapshot_query.scalar()
    return str(snapshot_id_value) if snapshot_id_value else None


async def current_network_snapshots_for_plan(
    session, args: dict[str, object]
) -> list[tuple[str, str]]:
    """Resolve the newest sealed shared V3 snapshot for each logical plan network.

    Dated files receive different source keys, so source key alone cannot identify
    a network across months. Manifest network names define that stable grouping;
    the source URL month selects its newest retained snapshot. Sources without
    either metadata retain the source-key and orchestration-month fallback.
    """
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return []
    plan_variants = ein_plan_id_variants(requested_plan)
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    # This list changes whenever a source/network publish completes. API pods
    # cannot see cache invalidation from importer workers, so resolve it live.
    query_parameters_by_name: dict[str, object] = {"plan_ids": plan_variants}
    market_sql = ""
    if market_type:
        query_parameters_by_name["plan_market_type"] = market_type
        market_sql = "AND cps.plan_market_type = :plan_market_type"
    source_sql = ""
    if source_key:
        query_parameters_by_name["source_key"] = source_key
        source_sql = "AND cps.source_key = :source_key"
    logical_network_sql = _logical_network_key_sql("cps", "s")
    effective_month_sql = _source_effective_month_sql("cps", "s")
    source_snapshot_query = await session.execute(
        text(
            f"""
             WITH candidate_snapshots AS (
                 SELECT cps.source_key,
                        cps.snapshot_id,
                        {logical_network_sql} AS logical_network_key,
                        {effective_month_sql} AS source_effective_month,
                        cps.import_month,
                        cps.updated_at
                   FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
                   JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
                  WHERE cps.plan_id = ANY(CAST(:plan_ids AS text[]))
                    {market_sql}
                    {source_sql}
                    AND s.status = 'published'
                    AND {_serving_relation_available_sql('s')}
             )
             SELECT DISTINCT ON (logical_network_key) source_key, snapshot_id
               FROM candidate_snapshots
              ORDER BY logical_network_key,
                       source_effective_month DESC NULLS LAST,
                       import_month DESC NULLS LAST,
                       updated_at DESC NULLS LAST
            """
        ),
        query_parameters_by_name,
    )
    source_snapshot_pairs = [
        (str(snapshot_record[0] or ""), str(snapshot_record[1]))
        for snapshot_record in source_snapshot_query
        if snapshot_record[1]
    ]
    return source_snapshot_pairs


async def resolve_current_ptg2_snapshot_id(session, args: dict[str, object]) -> str | None:
    """Resolve the published snapshot selected by explicit or scoped arguments."""
    if args.get("snapshot_id"):
        requested_plan_id = str(
            args.get("plan_id") or args.get("plan_external_id") or ""
        ).strip()
        requested_market_type = str(
            args.get("plan_market_type") or ""
        ).strip()
        return await current_snapshot_id(
            session,
            requested_snapshot_id=str(args["snapshot_id"]),
            requested_source_key=(
                None
                if args.get("source_key") is None
                else str(args["source_key"])
            ),
            requested_plan_id=requested_plan_id or None,
            requested_plan_market_type=requested_market_type or None,
            candidate_audit_access=candidate_audit_access_from_args(args),
        )
    if args.get("plan_id") or args.get("plan_external_id"):
        return await current_source_snapshot_id_for_plan(session, args)
    if args.get("source_key"):
        return await current_source_snapshot_id(session, str(args["source_key"]))
    return await current_snapshot_id(session)
