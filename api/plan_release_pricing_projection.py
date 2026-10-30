# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional pricing-projection SQL for plan-release resolution."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from api.plan_release_source_bindings import canonical_binding_relation

_PRICING_PROJECTION_SQL_TEMPLATE = """
       CASE
           WHEN pricing_projection.state = 'ready'
            AND pricing_projection.contract_version =
                revision.source_manifest
                    -> 'pricing_projection' ->> 'contract'
            AND pricing_projection.binding_manifest_digest =
                revision.binding_set_digest
            AND pricing_projection.binding_manifest_digest =
                revision.source_manifest
                    -> 'pricing_projection' ->> 'binding_manifest_digest'
            AND pricing_projection.provider_signature =
                revision.source_manifest
                    -> 'pricing_projection' ->> 'provider_signature'
            AND pricing_projection.content_digest =
                revision.source_manifest
                    -> 'pricing_projection' ->> 'content_digest'
           THEN pricing_projection.{selected_column}
       END
"""
_PRICING_PROJECTION_ID_SQL = _PRICING_PROJECTION_SQL_TEMPLATE.format(
    selected_column="projection_id",
)
_PRICING_PROJECTION_CONTRACT_SQL = _PRICING_PROJECTION_SQL_TEMPLATE.format(
    selected_column="contract_version",
)
_PLAN_RELEASE_SERVING_SQL_TEMPLATE = """
SELECT revision.serving_revision_id,
       revision.published_at AS serving_revision_published_at,
       revision.plan_release_id,
       revision.healthporta_plan_id,
       revision.plan_version_id,
       revision.release_month,
       revision.release_status,
       revision.expected_binding_count,
       revision.binding_set_digest,
{pricing_projection_id_sql} AS pricing_projection_id,
{pricing_projection_contract_sql} AS pricing_projection_contract,
       binding.binding_ordinal,
       binding.snapshot_id,
       binding.source_key,
       binding.plan_id,
       binding.plan_market_type,
       binding.role,
       binding.required,
       snapshot.status AS snapshot_status,
       EXISTS (
           SELECT 1
             FROM {schema}.ptg2_v3_snapshot_plan_scope scope
            WHERE scope.snapshot_id = binding.snapshot_id
              AND scope.plan_id = binding.plan_id
              AND scope.plan_market_type = binding.plan_market_type
       ) AS logical_scope_present,
       EXISTS (
           SELECT 1
             FROM {old_pin_relation} pin
            WHERE pin.owner_type = {pin_owner_parameter}
              AND pin.owner_id = revision.serving_revision_id
              AND pin.snapshot_id = binding.snapshot_id
       ){native_pin_sql} AS is_pinned
  FROM {revision_relation} revision
  JOIN {binding_relation} binding
    ON binding.serving_revision_id = revision.serving_revision_id
  LEFT JOIN {schema}.ptg2_snapshot snapshot
    ON snapshot.snapshot_id = binding.snapshot_id
{pricing_projection_join_sql}
 WHERE {release_predicate}
   AND revision.serving_status = 'published'
   AND revision.release_status = 'published'
   AND revision.is_current
 ORDER BY CASE binding.role WHEN 'in_network' THEN 0 ELSE 1 END,
          binding.binding_ordinal
"""

_NATIVE_PIN_SQL = """ OR (
           {pin_owner_parameter} = 'plan_release_serving_revision'
           AND EXISTS (
               SELECT 1 FROM {schema}.plan_release_snapshot_pin pin
                WHERE pin.serving_revision_id = revision.serving_revision_id
                  AND pin.snapshot_id = binding.snapshot_id
           )
       )"""


def pricing_projection_relation(schema: str) -> str:
    """Return the optional projection candidate relation name."""

    return f"{schema}.plan_pricing_projection_candidate"


def _plan_release_serving_sql(
    schema: str,
    *,
    include_pricing_projection: bool,
    release_predicate: str,
    pin_owner_parameter: str,
    retained: bool = False,
    source_bindings: bool = False,
) -> str:
    """Build release resolution SQL with an optional projection binding."""

    if retained and source_bindings:
        raise ValueError("plan_release_binding_route_conflict")
    relation_name = pricing_projection_relation(schema)
    projection_join_sql = (
        "  LEFT JOIN "
        f"{relation_name} pricing_projection\n"
        "    ON pricing_projection.projection_id =\n"
        "       revision.source_manifest -> 'pricing_projection' "
        "->> 'projection_id'"
        if include_pricing_projection
        else ""
    )
    projection_id_sql = _PRICING_PROJECTION_ID_SQL if include_pricing_projection else "       NULL::varchar(64)"
    projection_contract_sql = (
        _PRICING_PROJECTION_CONTRACT_SQL if include_pricing_projection else "       NULL::varchar(64)"
    )
    return _PLAN_RELEASE_SERVING_SQL_TEMPLATE.format(
        schema=schema,
        revision_relation=("ONLY " if retained else "") + f"{schema}.plan_release_serving_revision",
        old_pin_relation=("ONLY " if retained else "") + f"{schema}.ptg2_snapshot_pin",
        binding_relation=(
            canonical_binding_relation(schema)
            if source_bindings
            else f"ONLY {schema}.plan_release_snapshot_binding_legacy"
            if retained
            else f"{schema}.plan_release_snapshot_binding"
        ),
        native_pin_sql=""
        if retained or source_bindings
        else _NATIVE_PIN_SQL.format(schema=schema, pin_owner_parameter=pin_owner_parameter),
        pricing_projection_id_sql=projection_id_sql,
        pricing_projection_contract_sql=projection_contract_sql,
        pricing_projection_join_sql=projection_join_sql,
        release_predicate=release_predicate,
        pin_owner_parameter=pin_owner_parameter,
    )


def plan_release_serving_sql(schema: str, *, include_pricing_projection: bool, retained: bool = False) -> str:
    """Build the original singleton query without changing its predicates."""
    return _plan_release_serving_sql(
        schema,
        include_pricing_projection=include_pricing_projection,
        retained=retained,
        release_predicate="revision.plan_release_id = :plan_release_id",
        pin_owner_parameter=":pin_owner_type",
    )


def plan_release_binding_set_sql(
    schema: str,
    *,
    include_pricing_projection: bool,
    retained: bool = False,
    source_bindings: bool = False,
) -> str:
    """Build complete native-driver binding metadata for one bound release set."""
    from api.plan_release_source_bindings import current_source_predicate

    return _plan_release_serving_sql(
        schema,
        include_pricing_projection=include_pricing_projection,
        retained=retained,
        source_bindings=source_bindings,
        release_predicate="revision.plan_release_id = ANY($1::text[])"
        + (" AND " + current_source_predicate() if source_bindings else ""),
        pin_owner_parameter="$2",
    )


def plan_release_serving_queries(schema: str) -> tuple[str, str]:
    """Return relation-present and additive-migration fallback queries."""

    return (
        plan_release_serving_sql(schema, include_pricing_projection=True),
        plan_release_serving_sql(schema, include_pricing_projection=False),
    )


async def has_pricing_projection_relation(session: Any, schema: str) -> bool:
    """Return whether code may safely reference the additive relation."""

    relation_result = await session.execute(
        text("SELECT to_regclass(:relation_name) IS NOT NULL"),
        {"relation_name": pricing_projection_relation(schema)},
    )
    return bool(relation_result.scalar_one())


def source_plan_release_serving_sql(schema: str, *, include_pricing_projection: bool) -> str:
    """Load exact Source coordinates through the existing request Reader snapshot."""
    from api.plan_release_source_bindings import source_release_predicate

    return _plan_release_serving_sql(
        schema,
        include_pricing_projection=include_pricing_projection,
        source_bindings=True,
        release_predicate=source_release_predicate() + " AND revision.expected_binding_count=:expected_binding_count",
        pin_owner_parameter=":pin_owner_type",
    )
