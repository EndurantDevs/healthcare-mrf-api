# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded transport SQL and metadata accounting, not native authority proof."""

from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryCanonicalResource
from process import provider_directory_dataset_rehydrate_pages as pages
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError


def _header(identifier, size):
    return SimpleNamespace(_mapping={"id": identifier, "record_bytes": size})


def test_header_budget_keeps_complete_rows_and_rejects_oversize():
    first, second = _header("one", 10), _header("two", pages.READ_MAX_BYTES - 21)
    assert pages._admitted_headers([first, second]) == [first]
    with pytest.raises(DatasetRehydrationError, match="read_record_too_large"):
        pages._admitted_headers([_header("large", pages.READ_MAX_BYTES)])


@pytest.mark.parametrize("size", [None, 0, -1, True])
def test_header_budget_rejects_invalid_native_size(size):
    with pytest.raises(DatasetRehydrationError, match="read_size_invalid"):
        pages._admitted_headers([_header("invalid", size)])


def test_keyset_header_uses_fixed_model_keys_and_expanded_bytes():
    keys, columns, source, where, params, statement = pages._page_sql(
        "SELECT 1", ProviderDirectoryCanonicalResource, ("base", "Location", "last")
    )
    assert keys == ("canonical_api_base", "resource_type", "resource_id")
    assert "row_to_json(p)" in statement and "pg_column_size" not in statement
    assert "LIMIT 512" in statement and "ORDER BY" in statement
    assert params == {"after_0": "base", "after_1": "Location", "after_2": "last"}
    assert "(:after_0,:after_1,:after_2)" in where
