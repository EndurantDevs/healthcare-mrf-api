import pytest

from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryError,
    ProviderDirectorySourceSummaryBinding,
    SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
    build_source_summary,
    source_summary_sha256,
    validate_semantic_source_summary,
)


UHC_RESOURCE_COUNTS = {
    resource_type: resource_index
    for resource_index, resource_type in enumerate(
        SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
        start=1,
    )
}
UHC_RESOURCE_HASHES = {
    resource_type: f"{resource_index:x}" * 64
    for resource_index, resource_type in enumerate(
        SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
        start=1,
    )
}
UHC_COUNT_BY_FIELD = {
    "raw_provider_records": 18_696,
    "raw_plan_records": 24,
    "raw_individual_records": 18_000,
    "raw_facility_records": 696,
    "raw_address_rows": 46_768,
    "raw_provider_plan_rows": 18_696,
    "raw_formulary_entries": 120,
    "named_facility_records": 690,
    "facility_type_values": 12,
    "dated_records": 18_400,
    "distinct_npis": 18_257,
    "duplicate_npi_groups": 436,
    "conflicting_npi_groups": 5,
    "accepting_newpt_records": 9_000,
    "accepting_nopt_records": 8_000,
    "accepting_null_records": 1_696,
    "invalid_npi_count": 0,
    "valid_phone_count": 18_500,
    "invalid_phone_count": 196,
    "multi_address_provider_records": 7_000,
    "plan_year_rows": 24,
    "provider_file_count": 78,
    "plan_file_count": 24,
    "membership_plan_key_count": 20,
    "detail_plan_key_count": 20,
    "matched_plan_key_count": 20,
    "missing_plan_detail_count": 0,
    "orphan_plan_detail_count": 0,
}


def _uhc_summary():
    """Build one complete setwise UHC summary fixture."""
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id="uhc-dataset",
            endpoint_id="uhc-endpoint",
            acquisition_root_run_id="uhc-root",
            dataset_hash="d" * 64,
        ),
        source_ids=("provider-directory-uhc",),
        selected_resources=SOURCE_SUMMARY_UHC_SELECTED_RESOURCES,
        count_by_resource=UHC_RESOURCE_COUNTS,
        hash_by_resource=UHC_RESOURCE_HASHES,
        count_by_field=UHC_COUNT_BY_FIELD,
        count_by_category={
            "conflict_counts": {"name": 5},
            "intentional_drop_counts": {},
            "unknown_field_counts": {},
        },
        identity_by_field={
            "semantic_contract_id": SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
            "input_set_sha256": "7" * 64,
            "layout_set_sha256": "8" * 64,
            "encoder_digest": "9" * 64,
        },
    )


def test_validate_semantic_source_summary_dispatches_complete_uhc_facts():
    summary = _uhc_summary()

    assert validate_semantic_source_summary(
        summary,
        expected_by_field={
            "dataset_id": "uhc-dataset",
            "source_ids": ["provider-directory-uhc"],
        },
    ) == summary


@pytest.mark.parametrize(
    ("mutation", "error_code"),
    (
        (lambda summary: summary.pop("valid_phone_count"), "semantic_fields_missing"),
        (lambda summary: summary.pop("input_set_sha256"), "semantic_fields_missing"),
        (lambda summary: summary.pop("layout_set_sha256"), "semantic_fields_missing"),
        (lambda summary: summary.pop("encoder_digest"), "semantic_fields_missing"),
        (
            lambda summary: summary.__setitem__(
                "individual_practitioners", 18_696
            ),
            "semantic_fields_mismatch",
        ),
        (
            lambda summary: summary.__setitem__(
                "unknown_field_counts", {"mystery": 1}
            ),
            "uhc_unaccounted_fields",
        ),
        (
            lambda summary: summary.__setitem__(
                "selected_resources",
                list(SOURCE_SUMMARY_UHC_SELECTED_RESOURCES[:-1]),
            ),
            "uhc_resource_scope_invalid",
        ),
        (
            lambda summary: summary.__setitem__("invalid_npi_count", 1),
            "uhc_metrics_inconsistent",
        ),
        (
            lambda summary: summary.__setitem__("raw_facility_records", 695),
            "uhc_metrics_inconsistent",
        ),
        (
            lambda summary: summary.__setitem__(
                "conflict_counts", {"name": 6}
            ),
            "uhc_metrics_inconsistent",
        ),
        (
            lambda summary: summary.__setitem__(
                "membership_plan_key_count", 21
            ),
            "uhc_metrics_inconsistent",
        ),
    ),
)
def test_validate_semantic_source_summary_rejects_inexact_uhc_facts(
    mutation,
    error_code,
):
    summary = _uhc_summary()
    mutation(summary)
    if error_code == "uhc_resource_scope_invalid":
        summary["resource_counts"].pop("PractitionerRole")
        summary["resource_hashes"].pop("PractitionerRole")
        summary["total_resources"] = sum(summary["resource_counts"].values())
    summary["summary_sha256"] = source_summary_sha256(summary)

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match=f"provider_directory_source_summary_{error_code}",
    ):
        validate_semantic_source_summary(summary, expected_by_field={})
