from copy import deepcopy

import pytest

from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryError,
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
    source_summary_sha256,
    validate_source_summary,
)
from process.provider_directory_source_summary_sql import (
    source_summary_metrics_sql,
)


DATASET_HASH = "a" * 64
RESOURCE_HASHES = {
    "Location": "b" * 64,
    "Practitioner": "c" * 64,
}
RESOURCE_COUNTS = {"Location": 2, "Practitioner": 3}


def _summary():
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id="dataset-1",
            endpoint_id="endpoint-1",
            acquisition_root_run_id="root-1",
            dataset_hash=DATASET_HASH,
        ),
        source_ids=("source-b", "source-a"),
        selected_resources=("Practitioner", "Location"),
        count_by_resource=RESOURCE_COUNTS,
        hash_by_resource=RESOURCE_HASHES,
        count_by_field={
            "addressed_locations": 2,
            "distinct_npis": 3,
            "raw_formulary_entries": 0,
        },
        count_by_category={
            "intentional_drop_counts": {},
            "unknown_field_counts": {},
        },
        identity_by_field={
            "semantic_contract_id": (
                "healthporta.provider-directory.fhir-normalized-resource.v1"
            )
        },
    )


def test_build_source_summary_is_canonical_and_self_hashed():
    summary = _summary()

    assert summary["source_ids"] == ["source-a", "source-b"]
    assert summary["selected_resources"] == ["Location", "Practitioner"]
    assert summary["total_resources"] == 5
    assert summary["summary_sha256"] == source_summary_sha256(summary)
    assert validate_source_summary(summary) == summary


@pytest.mark.parametrize(
    ("field_name", "replacement", "error_code"),
    (
        (
            "summary_sha256",
            "f" * 64,
            "provider_directory_source_summary_hash_mismatch",
        ),
        (
            "total_resources",
            4,
            "provider_directory_source_summary_total_mismatch",
        ),
        (
            "selected_resources",
            ["Practitioner", "Location"],
            "provider_directory_source_summary_selected_resources_not_canonical",
        ),
        (
            "resource_hashes",
            {"Practitioner": "c" * 64},
            "provider_directory_source_summary_resource_proof_mismatch",
        ),
    ),
)
def test_validate_source_summary_rejects_tamper(
    field_name,
    replacement,
    error_code,
):
    summary = deepcopy(_summary())
    summary[field_name] = replacement

    with pytest.raises(ProviderDirectorySourceSummaryError, match=error_code):
        validate_source_summary(summary)


def test_validate_source_summary_rejects_unknown_fields_fail_closed():
    summary = _summary()
    summary["silently_ignored_rows"] = 7
    summary["summary_sha256"] = source_summary_sha256(summary)

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_field_unsupported",
    ):
        validate_source_summary(summary)


def test_validate_source_summary_requires_sealed_hash():
    summary = _summary()
    summary.pop("summary_sha256")

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_hash_missing",
    ):
        validate_source_summary(summary)


def test_validate_source_summary_binds_expected_dataset_identity():
    summary = _summary()

    assert validate_source_summary(
        summary,
        expected_by_field={
            "dataset_id": "dataset-1",
            "endpoint_id": "endpoint-1",
            "dataset_hash": DATASET_HASH,
            "source_ids": ["source-a", "source-b"],
            "selected_resources": ["Location", "Practitioner"],
            "total_resources": 5,
            "resource_counts": RESOURCE_COUNTS,
        },
    ) == summary

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_dataset_id_mismatch",
    ):
        validate_source_summary(
            summary,
            expected_by_field={"dataset_id": "stale"},
        )


def test_validate_source_summary_binds_expected_null_exactly():
    summary = _summary()

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_dataset_hash_mismatch",
    ):
        validate_source_summary(
            summary,
            expected_by_field={"dataset_hash": None},
        )


def test_validate_source_summary_rejects_boolean_contract_version():
    summary = _summary()
    summary["contract_version"] = True
    summary["summary_sha256"] = source_summary_sha256(summary)

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_contract_invalid",
    ):
        validate_source_summary(summary)


def test_validate_source_summary_normalizes_jsonb_object_key_order():
    summary = _summary()
    summary["resource_counts"] = {
        "Practitioner": 3,
        "Location": 2,
    }
    summary["resource_hashes"] = {
        "Practitioner": "c" * 64,
        "Location": "b" * 64,
    }
    summary["summary_sha256"] = source_summary_sha256(summary)

    validated = validate_source_summary(summary)

    assert list(validated["resource_counts"]) == ["Location", "Practitioner"]
    assert list(validated["resource_hashes"]) == ["Location", "Practitioner"]


@pytest.mark.parametrize(
    "mutate_summary",
    (
        lambda summary: summary.__setitem__("dataset_id", " dataset-1 "),
        lambda summary: summary["source_ids"].__setitem__(0, " source-a "),
        lambda summary: summary.__setitem__(
            "resource_counts",
            {" Location ": 2, "Practitioner": 3},
        ),
        lambda summary: summary.__setitem__(
            "summary_sha256",
            f" {summary['summary_sha256']} ",
        ),
    ),
)
def test_validate_source_summary_rejects_noncanonical_whitespace(
    mutate_summary,
):
    summary = deepcopy(_summary())
    mutate_summary(summary)

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_.*_invalid",
    ):
        validate_source_summary(summary)


@pytest.mark.parametrize(
    ("field_name", "replacement", "error_code"),
    (
        ("source_ids", None, "source_ids_invalid"),
        ("total_resources", True, "total_resources_invalid"),
        ("resource_counts", [], "resource_counts_invalid"),
        ("resource_hashes", [], "resource_hashes_invalid"),
        ("dataset_hash", "short", "dataset_hash_invalid"),
        (
            "dataset_hash",
            "g" * 64,
            "dataset_hash_invalid",
        ),
        (
            "selected_resources",
            ["Endpoint", "Location", "Practitioner"],
            "selected_resources_missing",
        ),
    ),
)
def test_validate_source_summary_rejects_invalid_field_shapes(
    field_name,
    replacement,
    error_code,
):
    summary = deepcopy(_summary())
    summary[field_name] = replacement

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match=f"provider_directory_source_summary_{error_code}",
    ):
        validate_source_summary(summary)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract_id", "foreign"),
        ("contract_version", 2),
        ("complete", False),
    ),
)
def test_validate_source_summary_rejects_foreign_contract(
    field_name,
    replacement,
):
    summary = deepcopy(_summary())
    summary[field_name] = replacement

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_contract_invalid",
    ):
        validate_source_summary(summary)


def test_validate_source_summary_rejects_nonmapping_and_missing_fields():
    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_invalid",
    ):
        validate_source_summary([])

    summary = _summary()
    summary.pop("endpoint_id")
    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_field_missing",
    ):
        validate_source_summary(summary)


@pytest.mark.parametrize(
    ("argument_name", "argument_value", "error_code"),
    (
        (
            "identity_by_field",
            {"foreign_identity": "value"},
            "identity_field_unsupported",
        ),
        (
            "count_by_field",
            {"foreign_count": 1},
            "count_field_unsupported",
        ),
        (
            "count_by_category",
            {"foreign_counts": {}},
            "count_map_field_unsupported",
        ),
    ),
)
def test_build_source_summary_rejects_unsupported_fields(
    argument_name,
    argument_value,
    error_code,
):
    build_argument_by_name = {
        "binding": ProviderDirectorySourceSummaryBinding(
            dataset_id="dataset-1",
            endpoint_id="endpoint-1",
            acquisition_root_run_id="root-1",
            dataset_hash=DATASET_HASH,
        ),
        "source_ids": ("source-a",),
        "selected_resources": ("Practitioner",),
        "count_by_resource": {"Practitioner": 1},
        "hash_by_resource": {"Practitioner": "c" * 64},
        argument_name: argument_value,
    }

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match=f"provider_directory_source_summary_{error_code}",
    ):
        build_source_summary(**build_argument_by_name)


def test_optional_identity_hashes_and_sql_binding_are_canonical():
    summary = build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id="dataset-1",
            endpoint_id="endpoint-1",
            acquisition_root_run_id="root-1",
            dataset_hash=DATASET_HASH,
        ),
        source_ids=("source-a",),
        selected_resources=("Practitioner",),
        count_by_resource={"Practitioner": 1},
        hash_by_resource={"Practitioner": "c" * 64},
        identity_by_field={
            "input_set_sha256": "d" * 64,
            "layout_set_sha256": "e" * 64,
            "encoder_digest": "f" * 64,
        },
    )

    assert summary["input_set_sha256"] == "d" * 64
    assert 'FROM "mrf"."dataset_resource"' in source_summary_metrics_sql(
        '"mrf"."dataset_resource"'
    )
