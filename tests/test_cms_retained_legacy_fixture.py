# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The legacy seed keeps real terminal proof separate from mutable fixture loading."""

import pytest

from tests.cms_retained_legacy_postgres_support import loading_header


def test_loading_seed_does_not_mutate_or_reissue_terminal_admission():
    columns = (
        "dataset_id",
        "publication_metadata_json",
        "status",
        "is_current",
        "resource_count",
        "dataset_hash",
        "publication_metadata_summary_json",
        "publication_metadata_sha256",
        "content_proof_admission_sha256",
        "completion_proof_required_version",
        "completion_proof_json",
        "completion_proof_sha256",
    )
    accepted = (
        "retained-one",
        '{"source_ids":["cms-npd"]}',
        "validated",
        False,
        13,
        "a" * 64,
        '{"proof":"actual-producer"}',
        "b" * 64,
        "c" * 64,
        1,
        '{"complete":true}',
        "d" * 64,
    )
    loading_by_field = dict(zip(columns, loading_header(columns, accepted), strict=True))
    assert loading_by_field == {
        **dict(zip(columns, accepted, strict=True)),
        "status": "acquiring",
        "resource_count": 0,
        "dataset_hash": None,
        "publication_metadata_summary_json": None,
        "publication_metadata_sha256": None,
        "content_proof_admission_sha256": None,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
    }
    assert accepted[2:6] == ("validated", False, 13, "a" * 64)
    assert accepted[-3:] == (1, '{"complete":true}', "d" * 64)


@pytest.mark.parametrize("status,current", [("acquiring", False), ("published", True), ("validated", True)])
def test_loading_seed_refuses_an_uncompleted_or_current_producer(status, current):
    with pytest.raises(AssertionError):
        loading_header(("status", "is_current"), (status, current))


def test_loading_seed_requires_exact_column_coverage():
    with pytest.raises(ValueError):
        loading_header(("status", "is_current", "dataset_id"), ("validated", False))
