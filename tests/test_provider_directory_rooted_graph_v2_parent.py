# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Rooted v2 parents replay without changing historical graph/query contracts."""

from dataclasses import fields, replace

import pytest

from process import provider_directory_dataset_scoped_publication as publication
from process.provider_directory_rooted_graph_acquisition_runtime import (
    _snapshot_from_identity,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
    has_matching_rooted_graph_root_publication,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    derive_single_root_identity,
)
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    exact_current,
    readiness,
    sealed_roots,
)
from tests.test_provider_directory_dataset_scoped_publication_contract import (
    _header_for,
    _parent_for,
)
from tests.test_provider_directory_rooted_graph_identity import _scope
from tests.test_provider_directory_rooted_graph_request_admission import (
    _admit,
    _coverage,
)

ROOTED_V2 = "healthporta.provider-directory.rooted-graph-publication.v2"


@pytest.mark.parametrize(
    ("variant", "contract_id", "expected"),
    (
        ("rooted_combined", ROOTED_V2, True),
        (
            "rooted_combined",
            "healthporta.provider-directory.rooted-graph-publication.v1",
            True,
        ),
        ("uhc_flex_practitioner", ROOTED_V2, False),
        (
            "rooted_combined",
            "healthporta.provider-directory.rooted-graph-publication.v3",
            False,
        ),
        ("other", ROOTED_V2, False),
        (None, None, False),
        ({}, ROOTED_V2, False),
    ),
)
def test_parent_publication_version_is_closed_by_variant(
    variant, contract_id, expected
) -> None:
    assert has_matching_rooted_graph_root_publication(variant, contract_id) is expected


def test_v2_parent_replays_exact_scope_run_and_input_snapshot() -> None:
    legacy_parent = exact_current(variant="rooted_combined")
    current = replace(legacy_parent, root_publication_contract_id=ROOTED_V2)
    identity = derive_single_root_identity(current, operation_key="f" * 64)
    replay = derive_single_root_identity(current, operation_key="f" * 64)
    previous = derive_single_root_identity(legacy_parent, operation_key="f" * 64)

    assert replay == identity
    assert identity.scope.scope_id != previous.scope.scope_id
    assert identity.candidate.root_publication_contract_id == ROOTED_V2
    assert _snapshot_from_identity(identity.candidate, "building").is_identity_match(
        identity.candidate
    )
    assert publication.exact_current_matches_root(current, identity.candidate)
    assert not publication.exact_current_matches_root(legacy_parent, identity.candidate)
    sealed = replace(
        sealed_roots(variant="rooted_combined")[1],
        root_publication_contract_id=ROOTED_V2,
    )
    assert sealed.root_publication_contract_id == ROOTED_V2


def test_v1_graph_mapping_hash_and_existing_scope_are_unchanged() -> None:
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT == {
        "uhc_flex_practitioner": "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1",
        "rooted_combined": "healthporta.provider-directory.rooted-graph-publication.v1",
    }
    assert (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
        == "66b9a3c04ecb2368db3a6cbc33de3e8d9203b4e0002cc80a6147a09ba2f61351"
    )
    assert _scope().scope_id == "pdrgs_c40b8491569d5bc427c984c04eb56bf488fe1b5aa4e9fe89"


def test_v2_parent_flows_through_single_admission_publication_and_readiness() -> None:
    source_coverage = _coverage(rooted_failed=0)
    current = replace(
        exact_current(variant="rooted_combined", retry_exhausted_count=1),
        root_publication_contract_id=ROOTED_V2,
    )
    candidate = derive_single_root_identity(current, operation_key="e" * 64).candidate
    sealed = sealed_roots(variant="rooted_combined")[1]
    root = replace(
        sealed,
        **{
            field.name: getattr(candidate, field.name)
            for field in fields(candidate)
            if hasattr(sealed, field.name)
        },
        request_failure_coverage=source_coverage,
    )
    dataset_identity = build_provider_directory_rooted_graph_dataset_identity(
        _admit(root), current
    )
    assert dataset_identity.root_publication_contract_id == ROOTED_V2
    assert dataset_identity.publication_contract_id == ROOTED_V2
    ready = replace(
        readiness(variant="rooted_combined", retry_exhausted_count=1),
        root_publication_contract_id=ROOTED_V2,
        publication_contract_id=ROOTED_V2,
        request_failure_coverage=source_coverage,
    )
    assert ready.rooted_graph_complete is True
    assert ready.request_failure_coverage["failed_requests"] == 1


class _ProofDatabase:
    def __init__(self, *, valid=True):
        self.valid = valid
        self.proof_reads = 0

    async def scalar(self, _statement, **_parameters):
        self.proof_reads += 1
        return self.valid


@pytest.mark.asyncio
async def test_v2_current_requires_database_identity_and_ready_proofs(
    monkeypatch,
) -> None:
    current = replace(
        exact_current(variant="rooted_combined"), root_publication_contract_id=ROOTED_V2
    )

    async def lock_registry(*_args):
        return None

    async def parent_by_id(*_args):
        return {current.dataset_id: _parent_for(current)}

    async def current_header(*_args):
        return current.variant, _header_for(current)

    monkeypatch.setattr(publication, "_lock_pair_registry", lock_registry)
    monkeypatch.setattr(publication, "_locked_parent_by_id", parent_by_id)
    monkeypatch.setattr(publication, "_locked_current_header", current_header)
    database = _ProofDatabase()
    observed = await publication.lock_exact_current_dataset(
        database, pair=publication.exact_uhc_dataset_pair()
    )
    assert observed == current
    assert database.proof_reads == 2
    with pytest.raises(publication.ProviderDirectoryDatasetScopedPublicationError):
        await publication.lock_exact_current_dataset(
            _ProofDatabase(valid=False), pair=publication.exact_uhc_dataset_pair()
        )
