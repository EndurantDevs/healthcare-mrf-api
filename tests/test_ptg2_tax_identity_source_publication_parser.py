# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict public parser tests for source-publication metadata."""

import pytest

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    tax_identity_source_publication_from_metadata,
)
from tests.test_ptg2_tax_identity_source_artifact import _ERROR
from tests.test_ptg2_tax_identity_source_publication_edges import _sealed_metadata


class _ExplodingMapping(dict):
    def get(self, _key, _default=None):
        raise RuntimeError("synthetic mapping failure")


def test_public_source_publication_parser_round_trips_canonical_metadata():
    metadata = _sealed_metadata()

    publication = tax_identity_source_publication_from_metadata(metadata)

    assert publication.as_dict() == metadata


@pytest.mark.parametrize(
    "invalid_metadata",
    [
        {**_sealed_metadata(), "contract": "other"},
        {**_sealed_metadata(), "unexpected": "field"},
        {
            key: value
            for key, value in _sealed_metadata().items()
            if key != "content_digest"
        },
        {**_sealed_metadata(), "source_count": True},
        {**_sealed_metadata(), "content_digest": "A" * 64},
        _ExplodingMapping(),
    ],
    ids=(
        "wrong-contract",
        "extra-field",
        "missing-field",
        "boolean-count",
        "noncanonical-digest",
        "hostile-mapping",
    ),
)
def test_public_source_publication_parser_rejects_malformed_metadata(
    invalid_metadata,
):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        tax_identity_source_publication_from_metadata(invalid_metadata)
