# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete CMS aggregate keeps source lineage and avoids wide resolved spooling."""

import re
from types import SimpleNamespace

import pytest

from process import registry_imported_selection_preview as preview
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, _extraction_sql


def _recipe():
    source = PinnedFHIRMembershipSource(
        "synthetic_source",
        "cms-npd",
        "endpoint-example",
        "dataset-example",
        "a" * 64,
        "b" * 64,
        1,
        "cms-npd",
        "2026-01-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "producer-example", "edition-example"
    )
    return SimpleNamespace(source_pin=source, binding_coordinates=coordinates)


def test_cms_aggregate_preserves_extractor_lineage_and_full_accounting():
    recipe = _recipe()
    extracted = _extraction_sql(recipe.source_pin, '"synthetic_registry"', reviewed=True, approved_only=True)
    aggregated = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    # Compare every expansion, source-witness and resolution predicate against the existing extractor.
    expected_body = extracted.split("providers AS (", 1)[1].split(", encoded AS (", 1)[0]
    expected_body = expected_body.replace(preview._FHIR_EXPANDED_PAGE, "\n", 1)
    expected_body = re.sub(
        r"\$(\d+)", lambda match: "$" + str(int(match[1]) - (3 if int(match[1]) > 11 else 0)), expected_body
    )
    aggregate_body = aggregated.split("providers AS (", 1)[1].split("\n      SELECT (SELECT count(*) FROM page)", 1)[0]
    assert aggregate_body.replace("resolved AS NOT MATERIALIZED", "resolved AS MATERIALIZED") == expected_body
    assert aggregated.count("resolved AS NOT MATERIALIZED") == 1
    assert "page AS MATERIALIZED" in aggregated and "expanded AS MATERIALIZED" in aggregated
    assert "LIMIT" not in aggregated and "ORDER BY resource_type,resource_id" not in aggregated
    assert "encoded AS" not in aggregated and "input_json" not in aggregated
    assert "count(*) FILTER(WHERE NOT unresolved AND NOT omitted) AS mapped_rows" in aggregated
    assert "count(*) FILTER(WHERE omitted) AS omitted_rows" in aggregated
    assert "count(*) FILTER(WHERE unresolved AND NOT omitted) AS unresolved_rows" in aggregated
    assert "SELECT count(*) FROM page" in aggregated and "count(*) AS expanded_rows" in aggregated
    assert preview.PREVIEW_DEADLINE_SECONDS == 2.5


@pytest.mark.parametrize("change", ["duplicate", "missing", "already_inline"])
def test_cms_resolved_shape_drift_refuses_aggregation(monkeypatch, change):
    recipe = _recipe()
    extracted = _extraction_sql(recipe.source_pin, '"synthetic_registry"', reviewed=True, approved_only=True)
    marker = ", resolved AS MATERIALIZED ("
    if change == "duplicate":
        changed = extracted + marker
    elif change == "missing":
        changed = extracted.replace(marker, ", changed_resolution AS MATERIALIZED (")
    else:
        changed = extracted.replace(marker, ", resolved AS NOT MATERIALIZED (")
    monkeypatch.setattr(preview, "fhir_extraction_sql", lambda *_args, **_options: changed)
    with pytest.raises(preview.RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")


def test_aca_aggregate_retains_existing_materialization():
    recipe = SimpleNamespace(
        source_pin=None,
        binding_coordinates=RegistryNetworkSourceCoordinates(
            "aca", "source-example", "synthetic_source", "dataset-example", "producer-example", "edition-example"
        ),
    )
    aggregated = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    assert aggregated.count("resolved AS MATERIALIZED") == 1
    assert "resolved AS NOT MATERIALIZED" not in aggregated
