# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic source fixtures for opaque billing-reference resolution."""

from __future__ import annotations

from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_billing_entity_refs import encode_billing_entity_ref
from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256

POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic"
SNAPSHOT_ID = "ptg2:synthetic-billing-selector"
SERVING_REVISION_ID = "hpserve_" + "2" * 26
GROUP_REF = "aa" * 16

_PLAN_ID = "hpplan_" + "0" * 26
_PLAN_RELEASE_ID = "hprelease_" + "0" * 26


def source_publication(**overrides: object) -> TaxIdentitySourcePublication:
    metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": ("ptg2_provider_group_tax_identity_source_content_v1"),
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": ("ptg2_tax_identity_source_binding_vector_v1"),
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": token_policy_descriptor_sha256(POLICY_ID),
        "source_ordinal_map_digest": "2" * 64,
        "source_count": 2,
        "provider_group_occurrence_count": 1,
        "matched_ein_count": 1,
        "missing_count": 0,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
        "artifact_byte_count": 256,
        "binding_vector_digest": "4" * 64,
    }
    metadata_by_field.update(overrides)
    return tax_identity_source_publication_from_metadata(metadata_by_field)


def serving_tables(
    *,
    publication: TaxIdentitySourcePublication | None = None,
    include_publication: bool = True,
    **overrides: object,
) -> PTG2ServingTables:
    fields_by_name = {
        "snapshot_id": SNAPSHOT_ID,
        "arch_version": "postgres_binary_v3",
        "shared_snapshot_key": 17,
        "storage_generation": "shared_blocks_v4",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 32,
        "provider_shard_span": 1024,
        "plan_id": "synthetic-plan-token",
        "plan_market_type": "group",
        "source_key": "synthetic-network",
        "provider_tax_identity_source_publication": (
            publication or source_publication() if include_publication else None
        ),
    }
    fields_by_name.update(overrides)
    return PTG2ServingTables(**fields_by_name)


def release_binding(**overrides: object) -> PlanReleaseSnapshotBinding:
    fields_by_name = {
        "binding_ordinal": 0,
        "snapshot_id": SNAPSHOT_ID,
        "source_key": "synthetic-network",
        "plan_id": "synthetic-plan-token",
        "plan_market_type": "group",
        "role": "in_network",
        "required": True,
    }
    fields_by_name.update(overrides)
    return PlanReleaseSnapshotBinding(**fields_by_name)


def source_pinned_selection(
    *,
    tables: PTG2ServingTables | None = None,
    include_source_proof: bool = True,
    healthporta_plan_id: str = _PLAN_ID,
    plan_release_id: str = _PLAN_RELEASE_ID,
) -> PlanReleaseServingSelection:
    selected_tables = tables or serving_tables()
    return PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        plan_release_id=plan_release_id,
        healthporta_plan_id=healthporta_plan_id,
        plan_version_id=None,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=(release_binding(),),
        _validated_serving_tables=((SNAPSHOT_ID, selected_tables),),
        _includes_billing_tax_identity_source=include_source_proof,
    )


def billing_entity_reference() -> str:
    token = b"x" * 32
    return encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=token[:16],
        tin_hmac_sha256=token,
    )


def resolved_source_scope(
    *,
    publication: TaxIdentitySourcePublication | None = None,
    snapshot_key: int = 17,
) -> ResolvedBillingEntitySourceScope:
    return ResolvedBillingEntitySourceScope(
        snapshot_key=snapshot_key,
        publication=publication or source_publication(),
        witnesses=(BillingEntitySourceWitness(0, 0, GROUP_REF),),
    )
