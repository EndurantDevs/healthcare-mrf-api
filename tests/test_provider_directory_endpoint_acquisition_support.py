import pytest

from scripts.research import provider_directory_endpoint_acquisition_support as support


def _scan_acquisition_entry():
    return {
        "canonical_base": support.SCAN_PROVIDER_DIRECTORY_BASE,
        "classification": "acquisition",
        "source_ids": ["pdfhir_736ccaa7958218d4daeaf2e6"],
        "resources": ["Practitioner"],
    }


def _scan_success_metrics(entry):
    source_ids = list(entry["source_ids"])
    return {
        "source_ids": source_ids,
        "source_import_sources_selected": len(source_ids),
        "source_import_groups_attempted": 1,
        "resource_fetch_completed_source_ids": {
            "Practitioner": source_ids,
        },
        "resource_fetch_stats": {
            "Practitioner": {
                "sources_completed": 1,
                "sources_bounded": 0,
                "sources_failed": 0,
            }
        },
    }


def _caresource_acquisition_entry():
    return {
        "canonical_base": support.CARESOURCE_PROVIDER_DIRECTORY_BASE,
        "classification": "acquisition",
        "source_ids": ["pdfhir_b627b38e07cae99151baa4b7"],
        "resources": ["PractitionerRole"],
    }


def _add_last_updated_proof(resource_stats, count):
    resource_stats.update(
        {
            "last_updated_partition_sources": 1,
            "last_updated_completeness_verified_sources": 1,
            **{
                metric_name: count
                for metric_name in support.LAST_UPDATED_PROOF_METRIC_NAMES
            },
        }
    )


def _add_caresource_proof(resource_stats, count):
    resource_stats.update(
        {
            "caresource_opaque_cursor_sources": 1,
            "caresource_opaque_cursor_verified_sources": 1,
            **{
                metric_name: count
                for metric_name in support.CARESOURCE_PROOF_METRIC_NAMES
            },
        }
    )


@pytest.mark.parametrize("resource_count", [0, 17])
def test_scan_metrics_require_reconciled_last_updated_proof(resource_count):
    entry = _scan_acquisition_entry()
    metrics = _scan_success_metrics(entry)
    _add_last_updated_proof(
        metrics["resource_fetch_stats"]["Practitioner"],
        resource_count,
    )

    assert support.acquisition_metric_errors(entry, metrics) == []


def test_scan_metrics_reject_missing_or_drifted_last_updated_proof():
    entry = _scan_acquisition_entry()
    metrics = _scan_success_metrics(entry)

    missing_errors = support.acquisition_metric_errors(entry, metrics)

    assert any(
        "lacks verified last-updated completeness proof" in error
        for error in missing_errors
    )
    _add_last_updated_proof(
        metrics["resource_fetch_stats"]["Practitioner"],
        17,
    )
    metrics["resource_fetch_stats"]["Practitioner"][
        "last_updated_unfiltered_post"
    ] = 18

    assert (
        "Practitioner last-updated completeness counts do not reconcile"
        in support.acquisition_metric_errors(entry, metrics)
    )


@pytest.mark.parametrize("resource_count", [0, 1828628])
def test_caresource_metrics_require_reconciled_opaque_cursor_census(resource_count):
    entry = _caresource_acquisition_entry()
    metrics = _scan_success_metrics(entry)
    role_stats = metrics["resource_fetch_stats"].pop("Practitioner")
    metrics["resource_fetch_stats"]["PractitionerRole"] = role_stats
    metrics["resource_fetch_completed_source_ids"] = {
        "PractitionerRole": entry["source_ids"]
    }
    _add_caresource_proof(role_stats, resource_count)

    assert support.acquisition_metric_errors(entry, metrics) == []


def test_caresource_metrics_reject_missing_or_drifted_census_proof():
    entry = _caresource_acquisition_entry()
    metrics = _scan_success_metrics(entry)
    role_stats = metrics["resource_fetch_stats"].pop("Practitioner")
    metrics["resource_fetch_stats"]["PractitionerRole"] = role_stats
    metrics["resource_fetch_completed_source_ids"] = {
        "PractitionerRole": entry["source_ids"]
    }

    missing_errors = support.acquisition_metric_errors(entry, metrics)

    assert any("lacks verified CareSource census proof" in error for error in missing_errors)
    _add_caresource_proof(role_stats, 1828628)
    role_stats["caresource_opaque_cursor_post_count"] = 1828629

    assert (
        "PractitionerRole CareSource census counts do not reconcile"
        in support.acquisition_metric_errors(entry, metrics)
    )
