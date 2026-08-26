# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public facade for immutable plan-pricing projection build and serving."""

import hashlib

from api.plan_pricing_projection_build import (
    build_in_session as _build_plan_pricing_projection,
    build_plan_pricing_projection,
    receipt as _receipt,
)
from api.plan_pricing_projection_contract import (
    PROJECTION_CONTRACT,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    canonical_json as _canonical_json,
    lock_provider_generation as _lock_provider_generation,
    normalized_bindings as _normalized_bindings,
    projection_code_identity as _projection_code_identity,
    projection_id as _projection_id,
    provider_signature as _provider_signature,
    row_mapping as _row_mapping,
    table as _table,
)
from api.plan_pricing_projection_materialize import (
    CardStats as _CardStats,
    aggregate_fragment as _aggregate_fragment,
    card_fragment as _card_fragment,
    digest_row as _digest_row,
    insert_batches as _insert_batches,
    project_code as _project_code,
    rate_fragment as _rate_fragment,
)
from api.plan_pricing_projection_read import (
    geo_cells as _geo_cells,
    projection_result_type,
    search_plan_pricing_projection,
    unsupported_projection_fields as _unsupported_projection_fields,
)
from api.plan_pricing_projection_source import (
    BindingProjection as _BindingProjection,
    binding_projection as _binding_projection,
    eligible_projection_providers as _eligible_projection_providers,
    numeric_rates as _numeric_rates,
    projection_provider_rows_for_npis as _projection_provider_rows_for_npis,
    snapshot_serving_tables,
)


__all__ = [
    "PROJECTION_CONTRACT",
    "PlanPricingProjectionUnavailable",
    "PlanPricingProjectionUnsupported",
    "build_plan_pricing_projection",
    "projection_result_type",
    "search_plan_pricing_projection",
]
