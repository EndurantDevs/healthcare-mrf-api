# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic contracts shared only by billing-search POST tests."""

from __future__ import annotations

from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_billing_code_reader import BillingCodeWitness
from api.ptg2_billing_entity_refs import encode_billing_entity_ref
from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_billing_geo_contract import (
    BillingAddressProvenance,
    BillingProviderAddress,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
    BillingProviderRateWitness,
)
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SELECTOR_MATCHED,
    BillingSearchBindingPin,
    BillingSearchResolvedQuery,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
)
from api.ptg2_billing_search_result import (
    BillingSearchMatchedProvider,
    BillingSearchProviderCandidate,
    BillingSearchServiceResult,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

PLAN_RELEASE_ID = "hprelease_" + "0" * 26
HEALTHPORTA_PLAN_ID = "hpplan_" + "1" * 26
SERVING_REVISION_ID = "hpserve_" + "2" * 26
SNAPSHOT_ID = "ptg2:synthetic-billing-search"
GROUP_REF = "aa" * 16
NPI = 1000000004


def publication(**overrides: object) -> TaxIdentitySourcePublication:
    metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": "ptg2_provider_group_tax_identity_source_content_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": "ptg2_tax_identity_source_binding_vector_v1",
        "token_policy_id": "ptg-tin-hmac-sha256-v1:synthetic",
        "token_policy_descriptor_sha256": "1" * 64,
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
    source_publication: TaxIdentitySourcePublication | None = None,
    include_publication: bool = True,
) -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id=SNAPSHOT_ID,
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
        provider_shard_span=1024,
        plan_id="synthetic-plan-token",
        plan_market_type="group",
        source_key="synthetic-network",
        provider_tax_identity_source_publication=(
            source_publication or publication() if include_publication else None
        ),
    )


def binding() -> PlanReleaseSnapshotBinding:
    return PlanReleaseSnapshotBinding(
        binding_ordinal=0,
        snapshot_id=SNAPSHOT_ID,
        source_key="synthetic-network",
        plan_id="synthetic-plan-token",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def selection(
    *,
    tables: PTG2ServingTables | None = None,
    bindings: tuple[PlanReleaseSnapshotBinding, ...] | None = None,
) -> PlanReleaseServingSelection:
    selected_bindings = (binding(),) if bindings is None else bindings
    selected_tables = serving_tables() if tables is None else tables
    validated_tables = (
        ((SNAPSHOT_ID, selected_tables),)
        if any(item.role == "in_network" for item in selected_bindings)
        else ()
    )
    return PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id=HEALTHPORTA_PLAN_ID,
        plan_version_id=None,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=selected_bindings,
        _validated_serving_tables=validated_tables,
    )


def billing_entity_ref() -> str:
    token = b"x" * 32
    return encode_billing_entity_ref(
        snapshot_key=17,
        tin_id_128=token[:16],
        tin_hmac_sha256=token,
    )


def source_scope(
    *,
    source_publication: TaxIdentitySourcePublication | None = None,
) -> ResolvedBillingEntitySourceScope:
    return ResolvedBillingEntitySourceScope(
        snapshot_key=17,
        publication=source_publication or publication(),
        witnesses=(BillingEntitySourceWitness(0, 0, GROUP_REF),),
    )


def selector_scope(
    *,
    source_publication: TaxIdentitySourcePublication | None = None,
) -> BillingSearchSelectorScope:
    return BillingSearchSelectorScope(
        selector_kind="tax_identity",
        bindings=(
            BillingSearchSelectorBindingScope(
                binding_ordinal=0,
                snapshot_id=SNAPSHOT_ID,
                state=BILLING_SELECTOR_MATCHED,
                source_scope=source_scope(source_publication=source_publication),
                billing_entity_ref=billing_entity_ref(),
            ),
        ),
    )


def query(**overrides: object) -> BillingSearchResolvedQuery:
    fields_by_name = {
        "plan_release_id": PLAN_RELEASE_ID,
        "selector_kind": "tax_identity",
        "tax_identity_type": "ein",
        "code_system": "CPT",
        "code": "99213",
        "zip5": "25000",
        "latitude": None,
        "longitude": None,
        "radius_miles": None,
        "provider_npi": None,
        "modifiers": (),
        "place_of_service": (),
        "include_evidence": False,
        "limit": 25,
        "after_sort_key": None,
    }
    fields_by_name.update(overrides)
    return BillingSearchResolvedQuery(**fields_by_name)


def code_witness() -> BillingCodeWitness:
    return BillingCodeWitness(
        code_key=5,
        code_system="CPT",
        code="99213",
        negotiation_arrangement="ffs",
        billing_code_type_version="2026",
        source_name=None,
        source_description=None,
    )


def provider_rate() -> BillingProviderRateWitness:
    return BillingProviderRateWitness(
        snapshot_key=17,
        code_key=5,
        source_key=0,
        source_record_ordinal=0,
        provider_group_ref=GROUP_REF,
        provider_set_key=3,
        price_key=10,
        occurrence_ordinal=0,
        npi=NPI,
    )


def address(*, distance_miles: float | None = None) -> BillingProviderAddress:
    return BillingProviderAddress(
        npi=NPI,
        location_hash="entity_address_unified:" + "6" * 64,
        distance_miles=distance_miles,
        address_key="00000000-0000-0000-0000-000000000001",
        address_site_key="00000000-0000-0000-0000-000000000002",
        location_key="6" * 64,
        address_purpose="practice",
        display={
            "first_line": "10 Example Ave",
            "second_line": "Suite 2",
            "city": "EXAMPLE",
            "state": "WV",
            "postal_code": "25000",
            "country_code": "US",
        },
        geo_evidence_level="nppes_registry_address",
        geo_evidence_source_id=1,
        provenance=(
            BillingAddressProvenance(
                dataset_id="cms_nppes_registry",
                source_id=1,
                source_record_id="synthetic:provider",
                record_version_id="20260801",
                record_version_ids=("20260801",),
                retrieved_at="2026-08-01T00:00:00+00:00",
                issuer_names=(),
                source_urls=(),
            ),
        ),
    )


def matched_result(
    *,
    include_evidence: bool = False,
    prices: tuple[dict[str, object], ...] | None = None,
) -> BillingSearchServiceResult:
    tables = serving_tables()
    pin = BillingSearchBindingPin(binding(), tables)
    geo_witness = BillingProviderGeoWitness(provider_rate(), address())
    candidate = BillingSearchProviderCandidate(
        binding_pin=pin,
        billing_entity_ref=billing_entity_ref(),
        address=geo_witness.address,
        geo_witnesses=(geo_witness,),
        code_witnesses_by_key=((5, code_witness()),),
    )
    price_witness = BillingProviderGeoPriceWitness(
        geo_witness,
        prices
        or (
            {
                "negotiated_rate": "20.50",
                "service_code": ["11"],
                "billing_code_modifier": [],
            },
        ),
    )
    provider = BillingSearchMatchedProvider(candidate, (price_witness,))
    return BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_MATCHED,
        request=query(include_evidence=include_evidence),
        selection=selection(tables=tables),
        selector_scope=selector_scope(source_publication=pin.source_publication),
        binding_pins=(pin,),
        providers=(provider,),
        has_more=False,
        next_sort_key=None,
    )
