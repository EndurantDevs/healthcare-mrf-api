#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit exclusions for Healthcare MRF functional monitoring."""

STABLE_CANARY_REQUIRED = (
    "reviewed: stable domain-specific canary identifiers or queries are not yet declared"
)
BOUNDED_DEFAULT_REQUIRED = (
    "reviewed: default response is data-dependent, expensive, or lacks a bounded contract"
)

_STABLE_CANARY_REQUIRED_OPERATIONS = (
        "getClinicalClinicalAreasClinicalAreaId",
        "getClinicalClinicalAreasClinicalAreaIdConditions",
        "getClinicalClinicalAreasClinicalAreaIdTreatments",
        "getClinicalConceptsSystemCode",
        "getClinicalConditionsSystemCode",
        "getClinicalTreatmentsSystemCode",
        "getFormularyDrugsRxnormId",
        "getFHIRFormularyDetail",
        "listFHIRFormularyAliases",
        "listFHIRFormularyDrugs",
        "getFHIRFormularyDrug",
        "getFormularyIdFormularyId",
        "getFormularyIdFormularyIdDrugs",
        "getFormularyIdFormularyIdDrugsRxnormId",
        "getFormularyIdFormularyIdSummary",
        "getFormularyPartdPharmaciesNpiActivity",
        "getFormularyPartdPharmaciesNpiMedicationsCodeSystemCodeCosts",
        "getFormularyPlanPlanIdDrugRxnormId",
        "getGeoCity",
        "getGeoGet",
        "getImportIssuerIssuerId",
        "getIssuerIdIssuerId",
        "getNpiIdNpiProviderProfile",
        "getNpiIdNpiProviderDirectoryObservations",
        "getPharmacyLicensePharmaciesNpi",
        "getPlanIdPlanId",
        "getPlanIdPlanIdYear",
        "getPlanIdPlanIdYearVariant",
        "getPlanNetworkAutocomplete",
        "getPlanNetworkIdChecksum",
        "getPlanNetworkMultipleChecksums",
        "getPlanPricePlanId",
        "getPlanPricePlanIdYear",
        "getPricingMedicationsResolve",
        "getPricingPrescriptionsAutocomplete",
        "getPricingPrescriptionsRxCodeSystemRxCodeBenchmarks",
        "getPricingPrescriptionsRxCodeSystemRxCodeProviders",
        "getPricingProceduresCodeSystemCodeBenchmarks",
        "getPricingProceduresCodeSystemCodeGeoBenchmarks",
        "getPricingProceduresCodeSystemCodeProviders",
        "getPricingProviderSpecialtiesAutocomplete",
        "getPricingProviderSpecialtiesResolve",
        "getPricingProviderAuditOccurrences",
        "getPricingProvidersNpi",
        "getPricingProvidersNpiPrescriptions",
        "getPricingProvidersNpiPrescriptionsRxCodeSystemRxCode",
        "getPricingProvidersNpiProcedures",
        "getPricingProvidersNpiProceduresCodeSystemCodeEstimatedCostLevel",
        "getPricingProvidersNpiProceduresProcedureCode",
        "getPricingProvidersNpiProceduresProcedureCodeEstimatedCostLevel",
        "getPricingProvidersNpiProceduresProcedureCodeLocations",
        "getPricingProvidersNpiScore",
        "getReportsPharmaciesChainsSummary",
        "getReportsPharmaciesMarketsMarketId",
        "getReportsPharmaciesNpiMarketContext",
        "getSiteIntelligenceScore",
)

_BOUNDED_DEFAULT_REQUIRED_OPERATIONS = (
    "getCoverageStatistics",
    "getFormularyIds",
    "getFormularyPartdSnapshots",
    "getImport",
    "getNpi",
    "getNpiActivePharmacists",
    "getNpiAll",
    "getNpiFacilitiesProviders",
    "getNpiMatchCandidates",
    "getNpiNear",
    "getNpiPharmacistsInPharmacies",
    "getNpiPharmacistsPerPharmacy",
    "getNucc",
    "getNuccAll",
    "getPlan",
    "getPlanAll",
    "getPlanAllVariants",
    "getPlanSearch",
    "getPricingGroupPlanProviders",
    "getPricingProviderSpecialties",
    "getPricingProviders",
    "getPricingProvidersByPrescription",
    "getPricingProvidersByProcedure",
    "getPricingProvidersSpecialties",
    "getPricingStatistics",
    "getReportsPharmaciesMarkets",
    "getReportsPharmaciesRankingsAccess",
    "getReportsPharmaciesStateStats",
    "listFHIRFormularies",
    "searchPricingProvidersByProcedure",
)

EXCLUDED_MONITORING_OPERATIONS: dict[str, str] = {
    **{
        operation_id: STABLE_CANARY_REQUIRED
        for operation_id in _STABLE_CANARY_REQUIRED_OPERATIONS
    },
    **{
        operation_id: BOUNDED_DEFAULT_REQUIRED
        for operation_id in _BOUNDED_DEFAULT_REQUIRED_OPERATIONS
    },
}
