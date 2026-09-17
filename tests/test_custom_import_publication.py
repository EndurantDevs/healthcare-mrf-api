# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from process.custom_import.publication import (
    PublicationConflict,
    _event_document,
    _pointer_version,
    _positive_integer,
)


def test_publication_event_is_canonical_and_domain_separated():
    canonical_event, digest = _event_document(
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
