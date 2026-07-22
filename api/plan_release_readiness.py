# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Role-aware physical readiness checks for canonical plan releases."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from api.ptg2_serving_utils import ein_plan_id_variants
from api.ptg2_snapshot import current_snapshot_id
from api.ptg2_tables import PTG2_SCHEMA, snapshot_serving_tables
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.domain import PTG2_DOMAIN_ALLOWED_AMOUNT

if TYPE_CHECKING:
    from api.plan_release_serving import PlanReleaseSnapshotBinding


_ALLOWED_AMOUNT_BINDING_READINESS_SQL = f"""
SELECT EXISTS (
    SELECT 1
      FROM {PTG2_SCHEMA}.ptg2_snapshot snapshot
      JOIN {PTG2_SCHEMA}.ptg2_allowed_amount_plan plan_coverage
        ON plan_coverage.snapshot_id = snapshot.snapshot_id
       AND plan_coverage.plan_id = ANY(CAST(:plan_ids AS text[]))
       AND (
            :market_type = ''
            OR COALESCE(plan_coverage.plan_market_type, '') = ''
            OR COALESCE(plan_coverage.plan_market_type, '') = :market_type
       )
     WHERE snapshot.snapshot_id = :snapshot_id
       AND snapshot.status = 'published'
       AND json_typeof(snapshot.manifest->'allowed_amount_index') = 'object'
       AND snapshot.manifest->'allowed_amount_index'->>'contract'
           = :allowed_contract
       AND snapshot.manifest->'allowed_amount_index'->>'arch_version'
           = 'postgres_binary_v3'
       AND snapshot.manifest->'allowed_amount_index'->>'storage'
           = 'postgresql'
       AND snapshot.manifest->'allowed_amount_index'->>'snapshot_scoped'
           = 'true'
       AND snapshot.manifest->'allowed_amount_index'->>'data_domain'
           = :allowed_data_domain
       AND snapshot.manifest->'allowed_amount_index'->>'allowed_amount_evidence'
           = 'true'
       AND CASE
               WHEN snapshot.manifest->'allowed_amount_index'
                        ->>'allowed_amount_payments' ~ '^[0-9]+$'
               THEN (
                   snapshot.manifest->'allowed_amount_index'
                       ->>'allowed_amount_payments'
               )::numeric
               ELSE 0
           END > 0
       AND CASE
               WHEN snapshot.manifest->'allowed_amount_index'
                        ->>'allowed_amount_provider_payments' ~ '^[0-9]+$'
               THEN (
                   snapshot.manifest->'allowed_amount_index'
                       ->>'allowed_amount_provider_payments'
               )::numeric
               ELSE 0
           END > 0
       AND LOWER(COALESCE(
               snapshot.manifest->'allowed_amount_index'->>'source_key',
               ''
           )) = :source_key
       AND EXISTS (
           SELECT 1
             FROM {PTG2_SCHEMA}.ptg2_allowed_amount_item allowed_item
             JOIN {PTG2_SCHEMA}.ptg2_allowed_amount_payment allowed_payment
               ON allowed_payment.snapshot_id = allowed_item.snapshot_id
              AND allowed_payment.allowed_item_hash
                  = allowed_item.allowed_item_hash
             JOIN {PTG2_SCHEMA}.ptg2_allowed_amount_provider_payment
                  provider_payment
               ON provider_payment.snapshot_id = allowed_payment.snapshot_id
              AND provider_payment.payment_hash = allowed_payment.payment_hash
            WHERE allowed_item.snapshot_id = plan_coverage.snapshot_id
              AND allowed_item.file_id = plan_coverage.file_id
              AND cardinality(provider_payment.npi) > 0
       )
)
"""


def is_release_binding_serving_scope_exact(
    serving_tables: PTG2ServingTables,
    binding: PlanReleaseSnapshotBinding,
) -> bool:
    """Match the attested physical scope used by provider pricing."""

    serving_plan_id = str(serving_tables.plan_id or "").strip()
    serving_market_type = str(
        serving_tables.plan_market_type or ""
    ).strip().lower()
    serving_source_key = str(serving_tables.source_key or "").strip().lower()
    return bool(
        serving_plan_id in set(ein_plan_id_variants(binding.plan_id))
        and serving_market_type
        and serving_market_type == binding.plan_market_type.strip().lower()
        and serving_source_key
        and serving_source_key == binding.source_key.strip().lower()
    )


async def has_allowed_amount_binding_coverage(
    session: Any,
    binding: PlanReleaseSnapshotBinding,
) -> bool:
    """Require the strict allowed index and its matching plan coverage row."""

    return bool(
        await session.scalar(
            text(_ALLOWED_AMOUNT_BINDING_READINESS_SQL),
            {
                "snapshot_id": binding.snapshot_id,
                "source_key": binding.source_key.strip().lower(),
                "plan_ids": list(ein_plan_id_variants(binding.plan_id)),
                "market_type": binding.plan_market_type.strip().lower(),
                "allowed_contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
                "allowed_data_domain": PTG2_DOMAIN_ALLOWED_AMOUNT,
            },
        )
    )


async def is_release_binding_serving_ready(
    session: Any,
    binding: PlanReleaseSnapshotBinding,
) -> bool:
    """Validate selectors, strict V3 tables, scope, and role artifacts."""

    if binding.role == "allowed_amounts":
        return await has_allowed_amount_binding_coverage(session, binding)
    if binding.role != "in_network":
        return False
    resolved_snapshot_id = await current_snapshot_id(
        session,
        requested_snapshot_id=binding.snapshot_id,
        requested_source_key=binding.source_key,
        requested_plan_id=binding.plan_id,
        requested_plan_market_type=binding.plan_market_type or None,
    )
    if resolved_snapshot_id != binding.snapshot_id:
        return False
    serving_tables = await snapshot_serving_tables(session, binding.snapshot_id)
    if (
        str(serving_tables.snapshot_id or "").strip() != binding.snapshot_id
        or not serving_tables.uses_shared_blocks
        or not is_release_binding_serving_scope_exact(serving_tables, binding)
    ):
        return False
    return True
