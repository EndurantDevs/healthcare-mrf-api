# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict public parser tests for source-publication metadata."""

import subprocess
import sys
from typing import get_type_hints

import pytest

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)
from process.ptg_parts.ptg2_tax_identity_source_publication_parser import (
    parse_tax_identity_source_publication,
)
from tests.test_ptg2_tax_identity_source_artifact import _ERROR
from tests.test_ptg2_tax_identity_source_publication_edges import _sealed_metadata


class _ExplodingMapping(dict):
    def get(self, _key, _default=None):
        raise RuntimeError("synthetic mapping failure")


def test_publication_parser_exposes_runtime_type_hints():
    hints_by_name = get_type_hints(parse_tax_identity_source_publication)

    assert hints_by_name["return"] is TaxIdentitySourcePublication


@pytest.mark.parametrize(
    "import_script",
    (
        """
from typing import get_type_hints
from process.ptg_parts.ptg2_tax_identity_source_publication_parser import (
    parse_tax_identity_source_publication,
)
import process.ptg_parts.ptg2_tax_identity_source_validation
assert (
    get_type_hints(parse_tax_identity_source_publication)["return"].__name__
    == "TaxIdentitySourcePublication"
)
""",
        """
from typing import get_type_hints
import process.ptg_parts.ptg2_tax_identity_source_validation
from process.ptg_parts.ptg2_tax_identity_source_publication_parser import (
    parse_tax_identity_source_publication,
)
assert (
    get_type_hints(parse_tax_identity_source_publication)["return"].__name__
    == "TaxIdentitySourcePublication"
)
""",
    ),
    ids=("parser-first", "validation-first"),
)
def test_publication_parser_import_orders_in_fresh_interpreter(
    import_script: str,
) -> None:
    completed = subprocess.run(
        [sys.executable, "-c", import_script],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr


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
