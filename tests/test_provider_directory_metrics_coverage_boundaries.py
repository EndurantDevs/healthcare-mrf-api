# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib


fhir = importlib.import_module("process.provider_directory_fhir")


def _fetch_result(**overrides):
    fetch_fields_by_name = {
        "model": dict,
        "rows": [],
        "rows_fetched": 0,
        "rows_written": 0,
        "pages_fetched": 1,
        "complete": False,
        "row_limit_reached": False,
        "page_limit_reached": False,
        "hard_page_limit_reached": False,
        "next_url_remaining": False,
    }
    fetch_fields_by_name.update(overrides)
    return fhir.ResourceFetchResult(**fetch_fields_by_name)


def test_provider_directory_metrics_record_partition_outcomes():
    stats = fhir._empty_resource_stats()
    cooldown = _fetch_result(
        pagination_cooldown_retries=2,
        pagination_cooldown_wait_seconds=1.5,
        is_pagination_cooldown_recovered=True,
        is_pagination_cooldown_exhausted=True,
        is_pagination_cooldown_deadline_blocked=True,
    )
    fhir._record_pagination_cooldown_stats(stats, cooldown)
    assert stats["pagination_cooldown_exhausted_sources"] == 1

    fhir._record_last_updated_partition_stats(stats, _fetch_result(fetch_mode="paged"))
    fhir._record_last_updated_partition_stats(
        stats,
        _fetch_result(
            fetch_mode=fhir.LAST_UPDATED_PARTITION_FETCH_MODE,
            fetch_diagnostic={"verified": False},
        ),
    )
    fhir._record_last_updated_partition_stats(
        stats,
        _fetch_result(
            fetch_mode=fhir.LAST_UPDATED_PARTITION_FETCH_MODE,
            fetch_diagnostic={
                "verified": True,
                "unfiltered_pre": 3,
                "ranged_root_pre": True,
            },
        ),
    )
    assert stats["last_updated_unfiltered_pre"] == 3

    fhir._record_caresource_opaque_cursor_stats(
        stats,
        _fetch_result(fetch_mode="paged"),
    )
    fhir._record_caresource_opaque_cursor_stats(
        stats,
        _fetch_result(
            fetch_mode=fhir.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE,
            fetch_diagnostic={"verified": False},
        ),
    )
    fhir._record_caresource_opaque_cursor_stats(
        stats,
        _fetch_result(
            fetch_mode=fhir.CARESOURCE_OPAQUE_CURSOR_FETCH_MODE,
            fetch_diagnostic={"verified": True, "pre_count": 4},
        ),
    )
    assert stats["caresource_opaque_cursor_pre_count"] == 4


def test_provider_directory_metrics_record_bulk_export_outcomes():
    stats_by_type = {}
    bounded = _fetch_result(
        complete=True,
        row_limit_reached=True,
        error="bounded",
        fetch_mode="bulk_export",
    )
    fhir._record_resource_fetch_stats(
        stats_by_type,
        "Location",
        bounded,
        bulk_export_selection=fhir.BULK_EXPORT_SELECTION_SOURCE_INELIGIBLE,
    )
    fhir._record_resource_fetch_stats(
        stats_by_type,
        "Location",
        _fetch_result(fetch_mode="paged"),
        bulk_export_selection=fhir.BULK_EXPORT_SELECTION_CHECKPOINT_REQUIRED,
    )
    fhir._record_resource_fetch_stats(
        stats_by_type,
        "Location",
        _fetch_result(fetch_mode="checkpoint_complete"),
        bulk_export_selection=fhir.BULK_EXPORT_SELECTION_EFFECTIVE,
    )
    assert stats_by_type["Location"]["bulk_export_requested_sources"] == 3
    assert stats_by_type["Location"]["bulk_export_rest_fallback_sources"] == 2
