# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed edge coverage for Profile source and dataset selection contracts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import pytest

from process import provider_directory_profile as profile
from process import provider_directory_profile_selection_dataset as selection_dataset
from process import provider_directory_profile_source_spec_contract as source_spec
from process import provider_directory_profile_uhc_flex_contract as flex_contract


class _BrokenResourceCounts(Mapping[str, object]):
    def __getitem__(self, key: str) -> object:
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        raise TypeError("synthetic iteration failure")

    def __len__(self) -> int:
        return 1


def test_source_spec_group_helpers_reject_invalid_container_shapes() -> None:
    assert not source_spec._is_variant_group(None, [])
    assert not source_spec._is_variant_group_collection_valid(None, [])


def test_profile_retained_sources_reject_invalid_matrix(monkeypatch) -> None:
    invalid_spec_by_field = {
        "retained_entry_ids": [],
        "source_ids": [],
        "verification_matrix": None,
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: invalid_spec_by_field,
    )

    with pytest.raises(RuntimeError, match="profile_source_spec_invalid"):
        profile.configured_retained_profile_source_ids()


def test_profile_retained_sources_reject_incomplete_coordinates(monkeypatch) -> None:
    incomplete_spec_by_field = {
        "retained_entry_ids": ["synthetic-retained"],
        "source_ids": ["pdfhir_synthetic"],
        "verification_matrix": {"sources": []},
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: incomplete_spec_by_field,
    )

    with pytest.raises(RuntimeError, match="profile_source_spec_invalid"):
        profile.configured_retained_profile_source_ids()


def test_profile_variant_groups_reject_invalid_matrix(monkeypatch) -> None:
    invalid_spec_by_field = {
        "dataset_scoped_entry_ids": [],
        "source_ids": [],
        "verification_matrix": None,
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: invalid_spec_by_field,
    )

    with pytest.raises(RuntimeError, match="profile_source_spec_invalid"):
        profile.configured_dataset_scoped_profile_variant_groups()


def test_profile_variant_groups_reject_duplicate_entry_coordinates(monkeypatch) -> None:
    duplicate_spec_by_field = {
        "dataset_scoped_entry_ids": ["synthetic-entry"],
        "dataset_scoped_variant_groups": [],
        "source_ids": ["pdfhir_synthetic"],
        "verification_matrix": {
            "sources": [
                {
                    "entry_id": "synthetic-entry",
                    "source_id": "pdfhir_synthetic",
                },
                {
                    "entry_id": "synthetic-entry",
                    "source_id": "pdfhir_synthetic",
                },
            ]
        },
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: duplicate_spec_by_field,
    )

    with pytest.raises(RuntimeError, match="profile_source_spec_invalid"):
        profile.configured_dataset_scoped_profile_variant_groups()


def test_profile_variant_groups_reject_missing_entry_coordinates(monkeypatch) -> None:
    incomplete_spec_by_field = {
        "dataset_scoped_entry_ids": ["synthetic-a", "synthetic-b"],
        "dataset_scoped_variant_groups": [
            {
                "group_id": "synthetic-group",
                "entry_ids": ["synthetic-a", "synthetic-b"],
            }
        ],
        "source_ids": ["pdfhir_synthetic_a", "pdfhir_synthetic_b"],
        "verification_matrix": {
            "sources": [
                {
                    "entry_id": "synthetic-a",
                    "source_id": "pdfhir_synthetic_a",
                }
            ]
        },
    }
    monkeypatch.setattr(
        profile,
        "load_profile_source_spec",
        lambda _path=None: incomplete_spec_by_field,
    )

    with pytest.raises(RuntimeError, match="profile_source_spec_invalid"):
        profile.configured_dataset_scoped_profile_variant_groups()


def test_profile_count_delegates_to_insert_preflight(monkeypatch) -> None:
    observed_keyword_by_name: dict[str, object] = {}

    def _profile_insert_sql(**kwargs: object) -> str:
        observed_keyword_by_name.update(kwargs)
        return "SELECT synthetic_count"

    monkeypatch.setattr(profile, "profile_insert_sql", _profile_insert_sql)

    assert (
        profile.profile_count_sql(
            evidence_ref='"synthetic"."evidence"',
            target_ref='"synthetic"."profile"',
        )
        == "SELECT synthetic_count"
    )
    assert observed_keyword_by_name == {
        "evidence_ref": '"synthetic"."evidence"',
        "target_ref": '"synthetic"."profile"',
        "count_only": True,
    }


def test_variant_selection_rejects_shared_reviewed_endpoint() -> None:
    dataset_rows = [{"endpoint_id": "synthetic-shared-endpoint"}]
    reviewed_endpoint_by_source_id = {
        "pdfhir_synthetic_a": "synthetic-shared-endpoint",
        "pdfhir_synthetic_b": "synthetic-shared-endpoint",
    }

    with pytest.raises(RuntimeError, match="dataset_variant_invalid"):
        selection_dataset._current_variant_dataset(
            dataset_rows,
            ("pdfhir_synthetic_a", "pdfhir_synthetic_b"),
            reviewed_endpoint_by_source_id,
        )


def test_flex_contract_rejects_unknown_and_malformed_dataset_coordinates() -> None:
    assert (
        flex_contract.uhc_flex_profile_expected_resources("synthetic-unknown-dataset")
        is None
    )
    assert not flex_contract._is_rooted_resource_counts_valid([])
    assert not flex_contract._is_rooted_resource_counts_valid(_BrokenResourceCounts())
    assert not flex_contract.is_uhc_flex_publication_metadata_valid(
        {},
        dataset_id="synthetic-unknown-dataset",
        endpoint_id="synthetic-endpoint",
        evidence_run_id="synthetic-run",
    )
