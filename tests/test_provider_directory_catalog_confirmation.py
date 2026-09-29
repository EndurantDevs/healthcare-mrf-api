import copy

import pytest

from scripts import generate_provider_directory_support_docs as generator
from tests.provider_directory_endpoint_acquisition_test_support import synthetic_support_manifest


def test_public_manifest_has_no_catalog_inventory_claim():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    assert manifest["catalog_confirmation"] is None
    rendered = generator.render_markdown(manifest)
    assert "Live catalog confirmation is not recorded" in rendered
    assert "sources confirmed in" not in rendered
    assert "| Probe status | Sources |" not in rendered


def test_missing_confirmation_is_not_an_explicit_absence():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    manifest.pop("catalog_confirmation")
    with pytest.raises(generator.SupportDocumentationError, match="catalog_confirmation must contain"):
        generator.validate_manifest(manifest)


def test_rendered_catalog_snapshot_distinguishes_full_catalog_from_curated_matrix():
    manifest = synthetic_support_manifest()
    blockers = generator.validate_blocker_registry(
        generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY)
    )
    curated_entry_count = len(manifest["entries"]) + len(blockers)
    acquisition_entry_count = sum(
        support_record["support_level"] == "acquisition-configured"
        for support_record in manifest["support_documentation"]["entry_support"].values()
    )

    rendered_document = generator.render_markdown(manifest)

    assert "does not claim that a live probe succeeded" in rendered_document
    assert "`reports/provider-directory-endpoint-acquisition/report.json`" in rendered_document
    assert "selected `--output` path with `--verification-report`" in rendered_document
    assert "## Catalog Inventory Snapshot" in rendered_document
    assert "entire live catalog: `3` sources confirmed in `test`" in rendered_document
    assert (
        "not the curated support matrix below, which tracks "
        f"`{curated_entry_count}` entries, including `{acquisition_entry_count}` "
        "acquisition-configured entries"
    ) in rendered_document
    assert (
        "`2` valid source rows collapse to `1` canonical bases after removing `1` aliases"
    ) in rendered_document
    assert "`1` bases are represented by maintained entries; `0` is not" in rendered_document
    assert "Synthetic catalog aliases share one canonical base" in rendered_document
    assert all(
        f"| `{status}` | {count} |" in rendered_document
        for status, count in manifest["catalog_confirmation"]["probe_status_counts"].items()
    )
    assert "| Never probed | 1 |" in rendered_document
    assert (
        "Missing operational evidence is displayed as not recorded and establishes no live status"
        in rendered_document
    )
    assert "CI rejects expired evidence" in rendered_document


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("source_count", None, "catalog_confirmation must contain"),
        ("source_count", 865, "source_count must equal probe_status_counts"),
        ("probe_status_counts", {"valid": 797}, "probe_status_counts must contain exactly"),
        (
            "never_probed_source_count",
            -1,
            "never_probed_source_count must be a non-negative integer",
        ),
        (
            "collapsed_valid_alias_source_count",
            77,
            "valid bases plus aliases must equal valid sources",
        ),
        (
            "represented_valid_canonical_base_count",
            23,
            "represented and unrepresented bases",
        ),
        ("coverage_note", "", "coverage_note must be non-empty"),
    ],
)
def test_validate_manifest_rejects_inconsistent_catalog_inventory(
    field_name,
    value,
    message,
):
    manifest = copy.deepcopy(synthetic_support_manifest())
    if value is None:
        manifest["catalog_confirmation"].pop(field_name)
    else:
        manifest["catalog_confirmation"][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_manifest(manifest)
