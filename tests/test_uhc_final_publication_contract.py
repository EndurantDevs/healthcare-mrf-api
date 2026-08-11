# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections.abc import Mapping
import copy
from dataclasses import replace
import json

import pytest

from process.provider_directory_source_summary import SOURCE_SUMMARY_METADATA_KEY
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
)
from process.uhc_final_publication_contract import (
    PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY,
    UhcFinalPublicationError,
    validate_uhc_final_publication,
)
from process.uhc_retained_dataset import (
    UHC_RETAINED_PUBLICATION_METADATA_KEY,
    UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY,
)
from tests.uhc_final_publication_test_support import (
    final_publication_fixture,
)


class _BrokenMetadataMapping(Mapping):
    def __getitem__(self, key):
        del key
        raise ValueError("synthetic broken mapping")

    def __iter__(self):
        return iter(("broken",))

    def __len__(self):
        return 1


def test_exact_final_publication_contract_accepts_one_valid_current_dataset():
    state, expectation = final_publication_fixture()

    proof = validate_uhc_final_publication(state, expectation)

    assert proof.dataset_id == state["dataset_id"]
    assert proof.dataset_hash == state["dataset_hash"]
    assert proof.resource_count == state["resource_count"]
    assert sum(proof.resource_counts.values()) == proof.resource_count


def _remove_metadata_key(state, metadata_key):
    state["publication_metadata_json"].pop(metadata_key)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda state: state.update(status="validated"),
        lambda state: state.update(is_current=False),
        lambda state: state.update(dataset_id="changed-dataset"),
        lambda state: state.update(endpoint_id="changed-endpoint"),
        lambda state: state.update(acquisition_root_run_id="changed-root"),
        lambda state: state.update(dataset_hash="0" * 64),
        lambda state: state.update(resource_count=7),
        lambda state: _remove_metadata_key(
            state, UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY
        ),
        lambda state: _remove_metadata_key(state, SOURCE_SUMMARY_METADATA_KEY),
        lambda state: _remove_metadata_key(
            state, UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY
        ),
        lambda state: _remove_metadata_key(
            state, PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
        ),
        lambda state: _remove_metadata_key(
            state, UHC_RETAINED_PUBLICATION_METADATA_KEY
        ),
    ],
)
def test_exact_final_publication_contract_rejects_each_required_boundary(
    mutate,
):
    state, expectation = final_publication_fixture()
    invalid_state = copy.deepcopy(state)
    mutate(invalid_state)

    with pytest.raises(UhcFinalPublicationError):
        validate_uhc_final_publication(invalid_state, expectation)


def test_exact_final_publication_contract_accepts_database_json_text():
    state, expectation = final_publication_fixture()
    state["publication_metadata_json"] = json.dumps(
        state["publication_metadata_json"],
        sort_keys=True,
    )

    assert validate_uhc_final_publication(state, expectation).dataset_id == (
        state["dataset_id"]
    )


@pytest.mark.parametrize(
    "expectation_change_by_field",
    [
        {"source_id": ""},
        {"selected_resources": ()},
        {"selected_resources": ("Practitioner", "Practitioner")},
        {"selected_resources": (" Practitioner",)},
        {"catalog_set_sha256": "not-a-sha256"},
    ],
)
def test_exact_final_publication_contract_rejects_invalid_expectations(
    expectation_change_by_field,
):
    state, expectation = final_publication_fixture()
    invalid_expectation = replace(expectation, **expectation_change_by_field)

    with pytest.raises(UhcFinalPublicationError):
        validate_uhc_final_publication(state, invalid_expectation)


@pytest.mark.parametrize(
    "invalid_metadata",
    ["{not-json", [], object(), _BrokenMetadataMapping()],
)
def test_exact_final_publication_contract_rejects_invalid_metadata(
    invalid_metadata,
):
    state, expectation = final_publication_fixture()
    state["publication_metadata_json"] = invalid_metadata

    with pytest.raises(UhcFinalPublicationError):
        validate_uhc_final_publication(state, expectation)


def test_exact_final_publication_contract_rejects_non_mapping_state():
    _state, expectation = final_publication_fixture()

    with pytest.raises(UhcFinalPublicationError):
        validate_uhc_final_publication([], expectation)
