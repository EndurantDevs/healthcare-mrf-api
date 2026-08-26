# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional pricing-projection SQL for plan-release resolution."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text


_PRICING_PROJECTION_ID_SQL = """
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
           THEN pricing_projection.projection_id
       END
"""
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
             FROM {schema}.ptg2_snapshot_pin pin
            WHERE pin.owner_type = :pin_owner_type
              AND pin.owner_id = revision.serving_revision_id
              AND pin.snapshot_id = binding.snapshot_id
       ) AS is_pinned
  FROM {schema}.plan_release_serving_revision revision
  JOIN {schema}.plan_release_snapshot_binding binding
    ON binding.serving_revision_id = revision.serving_revision_id
  LEFT JOIN {schema}.ptg2_snapshot snapshot
    ON snapshot.snapshot_id = binding.snapshot_id
{pricing_projection_join_sql}
 WHERE revision.plan_release_id = :plan_release_id
   AND revision.serving_status = 'published'
   AND revision.release_status = 'published'
   AND revision.is_current
 ORDER BY CASE binding.role WHEN 'in_network' THEN 0 ELSE 1 END,
          binding.binding_ordinal
"""


def pricing_projection_relation(schema: str) -> str:
    """Return the optional projection candidate relation name."""

    return f"{schema}.plan_pricing_projection_candidate"


def plan_release_serving_sql(
    schema: str,
    *,
    include_pricing_projection: bool,
) -> str:
    """Build release resolution SQL with an optional projection binding."""

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
    projection_id_sql = (
        _PRICING_PROJECTION_ID_SQL
        if include_pricing_projection
        else "       NULL::varchar(64)"
    )
    return _PLAN_RELEASE_SERVING_SQL_TEMPLATE.format(
        schema=schema,
        pricing_projection_id_sql=projection_id_sql,
        pricing_projection_join_sql=projection_join_sql,
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
