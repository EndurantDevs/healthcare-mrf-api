# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from process.custom_import.execution import MAX_BIGINT
from process.custom_import.publication import (
    PublicationConflict,
    _event_document,
    _generation_publication_request,
    _increment_pointer_version,
    _pointer_version,
    _positive_integer,
    _PublicationEventDetails,
)


def test_publication_event_is_canonical_and_domain_separated():
    canonical_event, digest = _event_document(
        _PublicationEventDetails(
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            execution_id=4,
            event_kind="activated",
            from_generation_id=None,
            to_generation_id=5,
            expected_pointer_version=0,
            committed_pointer_version=1,
        )
    )

    assert json.loads(canonical_event) == {
        "committed_pointer_version": 1,
        "contract": "custom-import-publication-event/v1",
        "dataset_id": 1,
        "definition_revision_id": 2,
        "event_kind": "activated",
        "execution_id": 4,
        "expected_pointer_version": 0,
        "from_generation_id": None,
        "schema_revision_id": 3,
        "to_generation_id": 5,
    }
    assert len(digest) == 32
    assert digest != bytes.fromhex("00" * 32)


@pytest.mark.parametrize("value", [True, False, 0, -1, "1", None])
def test_positive_publication_identifiers_reject_ambiguous_values(value):
    with pytest.raises(PublicationConflict, match="positive integer"):
        _positive_integer(value, "synthetic id")


@pytest.mark.parametrize("value", [True, False, -1, "0", None])
def test_pointer_version_rejects_ambiguous_values(value):
    with pytest.raises(PublicationConflict, match="non-negative integer"):
        _pointer_version(value)


def test_pointer_version_accepts_empty_and_existing_versions():
    assert _pointer_version(0) == 0
    assert _pointer_version(7) == 7


def test_publication_identifiers_and_pointer_increment_reject_postgresql_bigint_overflow():
    assert _positive_integer(MAX_BIGINT, "synthetic id") == MAX_BIGINT
    assert _pointer_version(MAX_BIGINT) == MAX_BIGINT
    with pytest.raises(PublicationConflict, match="positive integer"):
        _positive_integer(MAX_BIGINT + 1, "synthetic id")
    with pytest.raises(PublicationConflict, match="non-negative integer"):
        _pointer_version(MAX_BIGINT + 1)
    with pytest.raises(PublicationConflict, match="cannot advance"):
        _increment_pointer_version(MAX_BIGINT)
    with pytest.raises(PublicationConflict, match="cannot advance"):
        _generation_publication_request(
            event_kind="activated",
            dataset_id=1,
            target_generation_id=2,
            expected_generation_id=None,
            expected_pointer_version=MAX_BIGINT,
        )
