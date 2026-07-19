# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _bulk_identity():
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint_a",
        canonical_api_base="https://bulk.example.test/fhir",
        resource_type="Practitioner",
        source_scope_hash="scope_hash",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root_run",
        owner_run_id="owner_run",
        retry_of_run_id=None,
        endpoint_id="endpoint_a",
        dataset_id="dataset_a",
        start_url="https://bulk.example.test/fhir/$export",
        start_url_hash="start_hash",
    )


def _bulk_checkpoint(identity):
    return {
        "checkpoint_id": identity.checkpoint_id,
        "canonical_api_base": identity.canonical_api_base,
        "resource_type": identity.resource_type,
        "source_scope_hash": identity.source_scope_hash,
        "strategy_version": identity.strategy_version,
        "acquisition_root_run_id": identity.acquisition_root_run_id,
        "endpoint_id": identity.endpoint_id,
        "dataset_id": identity.dataset_id,
        "start_url_hash": identity.start_url_hash,
    }


@pytest.mark.parametrize(
    ("manifest_by_field", "expected_error"),
    [
        ({"output": None}, "bulk_export_manifest_invalid_output"),
        ({"output": [None]}, "bulk_export_manifest_invalid_output"),
        (
            {"output": [{"type": "Practitioner"}]},
            "bulk_export_manifest_invalid_output",
        ),
        (
            {
                "output": [
                    {"type": "Practitioner", "url": "http://files.example.test/a"}
                ]
            },
            "bulk_export_untrusted_output_url",
        ),
        (
            {
                "output": [
                    {
                        "type": "Practitioner",
                        "url": "https://files.example.test/a",
                    },
                    {
                        "type": "Practitioner",
                        "url": "https://files.example.test/a",
                    },
                ]
            },
            "bulk_export_manifest_duplicate_output",
        ),
    ],
)
def test_bulk_manifest_entries_reject_ambiguous_outputs(
    manifest_by_field,
    expected_error,
):
    """Reject malformed, unsafe, and duplicate bulk output descriptors."""
    with pytest.raises(ValueError, match=expected_error):
        importer._normalized_bulk_manifest_entries(
            manifest_by_field,
            "output",
            is_required=True,
        )


def test_bulk_utc_datetime_rejects_empty_and_invalid_values():
    """Normalize naive datetimes but reject absent and malformed timestamps."""
    with pytest.raises(ValueError, match="invalid_time"):
        importer._bulk_export_utc_datetime("", error="invalid_time")
    with pytest.raises(ValueError, match="invalid_time"):
        importer._bulk_export_utc_datetime("not-a-time", error="invalid_time")

    parsed_time = importer._bulk_export_utc_datetime(
        datetime.datetime(2026, 7, 20, 1, 2, 3),
        error="invalid_time",
    )
    assert parsed_time == datetime.datetime(
        2026,
        7,
        20,
        1,
        2,
        3,
        tzinfo=datetime.UTC,
    )


@pytest.mark.parametrize(
    ("payload_by_field", "expected_error"),
    [
        (None, "bulk_export_invalid_status_payload"),
        ({"error": ["invalid"]}, "bulk_export_error_output"),
        (
            {"error": [{"responseCode": "429", "type": "throttled"}]},
            "bulk_export_error_http_429",
        ),
        (
            {"error": [{"type": "processing"}]},
            "bulk_export_error_processing",
        ),
        ({"error": [{}]}, "bulk_export_error_output"),
    ],
)
def test_bulk_export_payload_error_uses_safe_normalized_codes(
    payload_by_field,
    expected_error,
):
    """Return bounded diagnostic codes without preserving arbitrary payload text."""
    assert importer._bulk_export_payload_error(payload_by_field) == expected_error


def test_bulk_checkpoint_identity_detects_drift_and_accepts_exact_match():
    """Bind resumed bulk work to every durable checkpoint identity field."""
    identity = _bulk_identity()
    checkpoint_by_field = _bulk_checkpoint(identity)

    assert importer._bulk_checkpoint_identity_error(
        {**checkpoint_by_field, "dataset_id": "dataset_other"},
        identity,
    ) == "bulk_export_checkpoint_identity_mismatch_dataset_id"
    assert importer._bulk_checkpoint_identity_error(
        checkpoint_by_field,
        identity,
    ) is None


@pytest.mark.parametrize(
    ("raw_value", "default", "expected"),
    [
        (True, False, True),
        ("yes", False, True),
        ("off", True, False),
        ("unexpected", False, True),
    ],
)
def test_boolean_normalization_covers_explicit_and_fallback_values(
    raw_value,
    default,
    expected,
):
    """Respect booleans, common tokens, and legacy truthy fallback values."""
    assert importer._bool_or_default(raw_value, default) is expected


@pytest.mark.parametrize(
    "raw_resources",
    [b"Practitioner", {"Practitioner": True}, 3],
)
def test_dataset_rehydrate_resource_types_reject_non_sequences(raw_resources):
    """Reject byte, mapping, and scalar resource selectors."""
    with pytest.raises(
        ValueError,
        match="provider_directory_dataset_rehydrate_resources_invalid",
    ):
        importer._dataset_rehydrate_resource_types(raw_resources)


def test_dataset_rehydrate_resource_types_reject_duplicates_and_keep_order():
    """Require unique nonblank resource types while preserving operator order."""
    assert importer._dataset_rehydrate_resource_types(None) == ()
    with pytest.raises(
        ValueError,
        match="provider_directory_dataset_rehydrate_resources_invalid",
    ):
        importer._dataset_rehydrate_resource_types(
            ["Practitioner", "Practitioner"]
        )
    assert importer._dataset_rehydrate_resource_types(
        "Practitioner,Organization"
    ) == ("Practitioner", "Organization")


def test_dataset_rehydration_request_enforces_exact_exclusive_scope():
    """Require one source, one run, retained dataset IDs, and no live import flags."""
    base_task_by_field = {
        "rehydrate_dataset_id": "dataset_a",
        "rehydrate_acquisition_root_run_id": "root_run",
    }
    with pytest.raises(ValueError, match="requires_exactly_one_source_id"):
        importer._dataset_rehydration_request(base_task_by_field, "owner_run", [])
    with pytest.raises(ValueError, match="run_id_required"):
        importer._dataset_rehydration_request(base_task_by_field, None, ["source_a"])
    with pytest.raises(ValueError, match="dataset_and_root_required"):
        importer._dataset_rehydration_request(
            {"rehydrate_dataset_id": "dataset_a"},
            "owner_run",
            ["source_a"],
        )
    with pytest.raises(ValueError, match="incompatible_parameters"):
        importer._dataset_rehydration_request(
            {**base_task_by_field, "import_resources": True},
            "owner_run",
            ["source_a"],
        )

    request = importer._dataset_rehydration_request(
        {
            **base_task_by_field,
            "rehydrate_resources": "Practitioner,Organization",
            "rehydrate_batch_size": 25,
        },
        "owner_run",
        ["source_a"],
    )
    assert request.source_id == "source_a"
    assert request.dataset_id == "dataset_a"
    assert request.acquisition_root_run_id == "root_run"
    assert request.resource_types == ("Practitioner", "Organization")
    assert request.batch_size == 25


@pytest.mark.asyncio
async def test_address_overlay_prerequisites_cover_function_and_table_gates(
    monkeypatch,
):
    """Fail closed on missing canonical functions or source tables."""
    monkeypatch.setattr(
        importer,
        "_address_canon_functions_available",
        AsyncMock(return_value=False),
    )
    assert await importer._address_overlay_missing_requirement("mrf") == (
        "canonical_functions_unavailable"
    )

    monkeypatch.setattr(
        importer,
        "_address_canon_functions_available",
        AsyncMock(return_value=True),
    )
    table_exists = AsyncMock(side_effect=[False])
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    assert await importer._address_overlay_missing_requirement("mrf") == (
        "provider_directory_organization_unavailable"
    )

    table_exists = AsyncMock(return_value=True)
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    assert await importer._address_overlay_missing_requirement("mrf") is None
    assert table_exists.await_count == len(
        importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_REQUIRED_TABLES
    )


def _graph_bundle(*resource_items, total=1):
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
        "entry": [{"resource": item} for item in resource_items],
    }


def _merge_graph_rows(monkeypatch, bundle_by_field, parsed_resource, **overrides):
    class ExpectedModel:
        pass

    class WrongModel:
        pass

    monkeypatch.setitem(
        importer.RESOURCE_TYPES_BY_MODEL,
        ExpectedModel,
        "Practitioner",
    )
    monkeypatch.setitem(
        importer.RESOURCE_TYPES_BY_MODEL,
        WrongModel,
        "Organization",
    )
    parsed_result = (
        parsed_resource(ExpectedModel, WrongModel)
        if callable(parsed_resource)
        else parsed_resource
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: parsed_result,
    )
    return importer._merge_uhc_plan_graph_bundle_rows(
        {"source_id": "source_a"},
        "Practitioner",
        bundle_by_field,
        fetch_url="https://example.test/fhir/Practitioner",
        network_reference=overrides.get("network_reference"),
        run_id="run_a",
        rows_by_id=overrides.get("rows_by_id", {}),
        fingerprints_by_id=overrides.get("fingerprints_by_id", {}),
    )


def test_uhc_graph_merge_rejects_bundle_and_predicate_failures(monkeypatch):
    """Reject non-bundles, capped queries, and cross-network graph rows."""
    assert _merge_graph_rows(monkeypatch, {}, None) == "non_bundle_payload"
    assert _merge_graph_rows(
        monkeypatch,
        _graph_bundle(total=importer.UHC_ADAPTIVE_PARTITION_TOTAL_CAP),
        None,
    ) == "uhc_plan_graph_practitioner_cap_exhausted"

    monkeypatch.setattr(
        importer,
        "_uhc_plan_graph_network_predicate_error",
        lambda *_args: "uhc_network_predicate_rejected",
    )
    assert _merge_graph_rows(
        monkeypatch,
        _graph_bundle({"resourceType": "Practitioner", "id": "p1"}),
        None,
        network_reference="Organization/network-a",
    ) == "uhc_network_predicate_rejected"


def test_uhc_graph_merge_rejects_type_identity_and_mutation(monkeypatch):
    """Require parsed type, stable resource identity, and immutable row content."""
    bundle_by_field = _graph_bundle(
        {"resourceType": "Practitioner", "id": "p1"}
    )
    assert _merge_graph_rows(monkeypatch, bundle_by_field, None) == (
        "uhc_plan_graph_resource_type_mismatch"
    )
    assert _merge_graph_rows(
        monkeypatch,
        bundle_by_field,
        lambda _expected, wrong: (wrong, {"resource_id": "p1"}),
    ) == "uhc_plan_graph_resource_type_mismatch"
    assert _merge_graph_rows(
        monkeypatch,
        bundle_by_field,
        lambda expected, _wrong: (expected, {}),
    ) == "uhc_plan_graph_resource_id_missing"

    mutated_row_by_field = {"resource_id": "p1", "name": "new"}
    assert _merge_graph_rows(
        monkeypatch,
        bundle_by_field,
        lambda expected, _wrong: (expected, mutated_row_by_field),
        fingerprints_by_id={"p1": "old-fingerprint"},
    ) == "uhc_plan_graph_resource_mutated"


def test_uhc_graph_merge_accepts_stable_rows(monkeypatch):
    """Merge a valid parsed row and retain its deterministic fingerprint."""
    rows_by_id = {}
    fingerprints_by_id = {}
    resource_row_by_field = {"resource_id": "p1", "name": "Provider"}

    assert _merge_graph_rows(
        monkeypatch,
        _graph_bundle({"resourceType": "Practitioner", "id": "p1"}),
        lambda expected, _wrong: (expected, resource_row_by_field),
        rows_by_id=rows_by_id,
        fingerprints_by_id=fingerprints_by_id,
    ) is None
    assert rows_by_id == {"p1": resource_row_by_field}
    assert len(fingerprints_by_id["p1"]) == 64
