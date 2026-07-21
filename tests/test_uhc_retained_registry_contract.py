# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.uhc_provider_file_catalog_contract import UHCFileCatalogItem
from process.uhc_provider_file_catalog_types import _expected_file_hashes
from process.uhc_retained_registry_contract import expected_catalog_file_hash_pair


@pytest.mark.parametrize(
    ("source_url", "size_bytes"),
    (
        (
            "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
            "JSON_Providers_AZDC.json",
            None,
        ),
        (
            "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
            "JSON_Providers_AZDC.json",
            16_754_456,
        ),
        (
            "https://legacy.providerlookuponline.com/api/providers/"
            "JSON_Providers_AZDC.json",
            None,
        ),
        (
            "https://legacy.providerlookuponline.com/api/providers/"
            "JSON_Providers_AZDC.json",
            16_754_456,
        ),
    ),
)
def test_retained_hashes_match_live_catalog_contract(source_url, size_bytes):
    catalog_file = UHCFileCatalogItem(
        family="ifp",
        collection_kind="provider_membership",
        file_id="0" * 64,
        file_name="JSON_Providers_AZDC.json",
        source_url=source_url,
        catalog_modified_at="2026-07-20T08:00:00Z",
        catalog_entry_sha256="0" * 64,
        size_bytes=size_bytes,
    )
    catalog_hashes = _expected_file_hashes(catalog_file)
    retained_hashes = expected_catalog_file_hash_pair(
        family=catalog_file.family,
        collection_kind=catalog_file.collection_kind,
        file_name=catalog_file.file_name,
        source_url=catalog_file.source_url,
        catalog_modified_at=catalog_file.catalog_modified_at,
        size_bytes=catalog_file.size_bytes,
    )

    assert retained_hashes == catalog_hashes
