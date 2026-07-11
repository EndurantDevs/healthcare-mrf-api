import copy

import pytest

from scripts import generate_provider_directory_support_docs as generator


def test_rendered_catalog_snapshot_distinguishes_full_catalog_from_curated_matrix():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)

    rendered_document = generator.render_markdown(manifest)

    assert "does not claim that a live probe succeeded" in rendered_document
    assert "`reports/provider-directory-endpoint-acquisition/report.json`" in rendered_document
    assert "selected `--report` path; the report is not tracked" in rendered_document
    assert "## Catalog Inventory Snapshot" in rendered_document
    assert "entire live catalog: `866` sources confirmed in `healthporta-dev`" in rendered_document
    assert (
        "not the curated support matrix below, which tracks `31` entries, including `23` "
        "acquisition-configured entries"
    ) in rendered_document
    assert (
        "`103` valid source rows collapse to `25` canonical bases after removing `78` aliases"
    ) in rendered_document
    assert "`24` bases are represented by maintained entries; `1` is not" in rendered_document
    assert "Aetna's credentialed, targeted Medicaid directory" in rendered_document
    assert all(
        f"| `{status}` | {count} |" in rendered_document
        for status, count in manifest["catalog_confirmation"]["probe_status_counts"].items()
    )
    assert "| Never probed | 69 |" in rendered_document
    assert (
        "The tracked verification snapshot remains the authority for terminal per-endpoint live status"
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
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    if value is None:
        manifest["catalog_confirmation"].pop(field_name)
    else:
        manifest["catalog_confirmation"][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_manifest(manifest)
