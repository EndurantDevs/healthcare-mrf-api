# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from process import uhc_drug_file_catalog as drug_catalog
from process import uhc_provider_file_catalog_types as provider_catalog
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


RAW_SET_SHA256 = "a" * 64


def _catalog(payloads_by_family=None, *, raw_set_sha256=RAW_SET_SHA256):
    return drug_catalog.observed_drug_catalog_from_payloads(
        payloads_by_family or live_catalog_payloads(),
        source_raw_set_sha256=raw_set_sha256,
    )


def test_drug_catalog_derives_exact_ifp_and_cs_set_without_provider_churn():
    payloads_by_family = live_catalog_payloads()
    provider_set = provider_catalog.observed_catalog_from_payloads(payloads_by_family)

    observed = _catalog(payloads_by_family)

    assert len(provider_set.files) == 102
    assert len(observed.files) == 48
    assert [summary["file_count"] for summary in observed.collection_summary] == [
        24,
        24,
    ]
    assert {catalog_file.family for catalog_file in observed.files} == {
        "cs",
        "ifp",
    }
    assert {catalog_file.collection_kind for catalog_file in observed.files} == {
        "drug_formulary"
    }
    assert observed.source_raw_set_sha256 == RAW_SET_SHA256


def test_drug_semantic_identity_is_independent_of_provider_only_changes():
    original_payloads = live_catalog_payloads()
    changed_payloads = deepcopy(original_payloads)
    changed_payloads["cs"]["providers"][0]["date"] = "2026-07-21T00:00:00Z"

    original = _catalog(original_payloads)
    changed = _catalog(changed_payloads, raw_set_sha256="b" * 64)

    assert changed.drug_set_sha256 == original.drug_set_sha256
    assert (
        changed.raw_listing_projection_sha256 == original.raw_listing_projection_sha256
    )
    assert changed.acquisition_contract_sha256 == (original.acquisition_contract_sha256)
    assert changed.source_raw_set_sha256 != original.source_raw_set_sha256


def test_provider_semantic_identity_is_independent_of_drug_only_changes():
    original_payloads = live_catalog_payloads()
    changed_payloads = deepcopy(original_payloads)
    changed_payloads["ifp"]["drugs"][0]["date"] = "2026-07-21T00:00:00Z"

    original = provider_catalog.observed_catalog_from_payloads(original_payloads)
    changed = provider_catalog.observed_catalog_from_payloads(changed_payloads)

    assert changed.files == original.files
    assert changed.collection_summary == original.collection_summary
    assert changed.catalog_set_sha256 == original.catalog_set_sha256


def test_drug_file_change_rotates_set_and_acquisition_identities():
    changed_payloads = live_catalog_payloads()
    changed_payloads["ifp"]["drugs"][0]["date"] = "2026-07-21T00:00:00Z"

    original = _catalog()
    changed = _catalog(changed_payloads)

    assert changed.drug_set_sha256 != original.drug_set_sha256
    assert (
        changed.raw_listing_projection_sha256 != original.raw_listing_projection_sha256
    )


def test_listing_projection_change_rotates_acquisition_not_semantic_set():
    changed_payloads = live_catalog_payloads()
    changed_payloads["ifp"]["drugs"][0]["review_marker"] = "new-proof-field"

    original = _catalog()
    changed = _catalog(changed_payloads)

    assert changed.drug_set_sha256 == original.drug_set_sha256
    assert (
        changed.raw_listing_projection_sha256 != original.raw_listing_projection_sha256
    )
    assert changed.acquisition_contract_sha256 != original.acquisition_contract_sha256
    assert changed.acquisition_contract_sha256 != original.acquisition_contract_sha256


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payloads: payloads["cs"].update({"drugs": []}), "count"),
        (lambda payloads: payloads["cs"]["drugs"].pop(), "count"),
        (lambda payloads: payloads["ifp"].pop("drugs"), "collection"),
        (
            lambda payloads: payloads["cs"]["drugs"].append(
                dict(payloads["cs"]["drugs"][0])
            ),
            "duplicate",
        ),
        (
            lambda payloads: payloads["ifp"]["drugs"][0].update(
                {"blobPath": "ui/ifp/providers/not-a-drug.json"}
            ),
            "blob path",
        ),
        (
            lambda payloads: payloads["ifp"]["drugs"].append("invalid"),
            "not an object",
        ),
        (
            lambda payloads: payloads["ifp"]["drugs"][0].update({"size": 0}),
            "byte count",
        ),
    ],
)
def test_drug_catalog_fails_closed_at_collection_boundaries(mutation, message):
    payloads_by_family = live_catalog_payloads()
    mutation(payloads_by_family)

    with pytest.raises(provider_catalog.UHCFileCatalogError, match=message):
        _catalog(payloads_by_family)


@pytest.mark.parametrize("raw_set_sha256", ["", "A" * 64, "a" * 63, object()])
def test_drug_catalog_rejects_invalid_retained_listing_identity(raw_set_sha256):
    with pytest.raises(
        provider_catalog.UHCFileCatalogError,
        match="source catalog proof",
    ):
        _catalog(raw_set_sha256=raw_set_sha256)


def test_drug_catalog_accepts_bound_external_url_and_skips_non_json():
    payloads_by_family = live_catalog_payloads()
    external_entry = payloads_by_family["cs"]["drugs"][0]
    external_entry.update(
        {
            "isExternal": True,
            "url": (
                "https://legacy.providerlookuponline.com/" + external_entry["name"]
            ),
        }
    )
    payloads_by_family["cs"]["drugs"].append(
        {"name": "README.txt", "date": "2026-07-20T00:00:00Z"}
    )

    observed = _catalog(payloads_by_family)

    assert len(observed.files) == 48
    assert any(
        catalog_file.source_url.startswith("https://legacy.providerlookuponline.com/")
        for catalog_file in observed.files
    )


def test_drug_catalog_rejects_one_normalized_url_for_distinct_families():
    payloads_by_family = live_catalog_payloads()
    cs_entry = payloads_by_family["cs"]["drugs"][0]
    ifp_entry = payloads_by_family["ifp"]["drugs"][0]
    shared_name = cs_entry["name"]
    cs_url = f"https://legacy.providerlookuponline.com/{shared_name}"
    ifp_url = f"https://legacy.providerlookuponline.com/{shared_name}"
    cs_entry.update(
        {
            "isExternal": True,
            "url": cs_url,
        }
    )
    ifp_entry.update(
        {
            "isExternal": True,
            "name": shared_name,
            "url": ifp_url,
        }
    )

    with pytest.raises(
        provider_catalog.UHCFileCatalogError,
        match="reuses one source URL",
    ):
        _catalog(payloads_by_family)


@pytest.mark.parametrize(
    "path_prefix",
    (
        "https://legacy.providerlookuponline.com:443/safe/",
        "https://legacy.providerlookuponline.com/%4a/",
        "https://legacy.providerlookuponline.com/%7e/",
        "https://legacy.providerlookuponline.com/%2E/",
        "https://legacy.providerlookuponline.com/safe/%2e%2e/",
    ),
)
def test_drug_catalog_rejects_noncanonical_transport_urls(path_prefix):
    payloads_by_family = live_catalog_payloads()
    external_entry = payloads_by_family["cs"]["drugs"][0]
    external_entry.update(
        {
            "isExternal": True,
            "url": path_prefix + external_entry["name"],
        }
    )

    with pytest.raises(provider_catalog.UHCFileCatalogError):
        _catalog(payloads_by_family)


def test_drug_catalog_accepts_canonical_percent_encoded_space():
    payloads_by_family = live_catalog_payloads()
    external_entry = payloads_by_family["cs"]["drugs"][0]
    expected_url = (
        "https://legacy.providerlookuponline.com/safe%20space/" + external_entry["name"]
    )
    external_entry.update(
        {
            "isExternal": True,
            "url": expected_url,
        }
    )

    observed = _catalog(payloads_by_family)

    assert expected_url in {item.source_url for item in observed.files}


def test_drug_catalog_rejects_decoded_dot_segment_source_paths():
    payloads_by_family = live_catalog_payloads()
    external_entry = payloads_by_family["cs"]["drugs"][0]
    external_entry.update(
        {
            "isExternal": True,
            "url": (
                "https://legacy.providerlookuponline.com/safe/%2e%2e/"
                + external_entry["name"]
            ),
        }
    )

    with pytest.raises(
        provider_catalog.UHCFileCatalogError,
        match="dot segment",
    ):
        _catalog(payloads_by_family)
