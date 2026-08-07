# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge contracts for exact billing source coordinates."""

from __future__ import annotations

import pytest

from api import ptg2_billing_exact_contract as contract
from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.ptg2_billing_exact_reader_support import (
    GROUP_A,
    GROUP_B,
    _publication,
    _scope,
)


def _source_scope(
    witnesses: tuple[BillingEntitySourceWitness, ...],
) -> ResolvedBillingEntitySourceScope:
    return ResolvedBillingEntitySourceScope(
        snapshot_key=17,
        publication=_publication(),
        witnesses=witnesses,
    )


def test_canonical_ref_rejects_bad_hex_and_short_decoding(monkeypatch) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="reference is malformed"):
        contract.canonical_ref("g" * 32, category="provider group")

    class ShortBytes:
        @staticmethod
        def fromhex(_value):
            return b"short"

    monkeypatch.setattr(contract, "bytes", ShortBytes, raising=False)
    with pytest.raises(PTG2ManifestArtifactError, match="reference is malformed"):
        contract.canonical_ref(GROUP_A, category="provider group")


def test_source_coordinate_rejects_invalid_numeric_geometry() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="invalid witness"):
        contract._source_coordinate(
            BillingEntitySourceWitness(True, 0, GROUP_A),
            source_count=2,
        )


def test_source_geometry_wraps_publication_parser_failure(monkeypatch) -> None:
    def reject_publication(_metadata):
        raise TaxIdentitySourceProjectionError("synthetic parser failure")

    monkeypatch.setattr(
        contract,
        "tax_identity_source_publication_from_metadata",
        reject_publication,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="geometry is unavailable"):
        contract._require_source_geometry(
            _scope(),
            snapshot_key=17,
            source_count=2,
            source_publication=_publication(),
        )


def test_source_coordinates_require_at_least_one_witness() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="no witnesses"):
        contract._validated_source_coordinates(
            _source_scope(()),
            source_count=2,
        )


def test_source_coordinates_enforce_the_witness_cap(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MAX_SOURCE_WITNESSES", 1)

    with pytest.raises(PTG2ManifestArtifactError, match="witness limit"):
        contract._validated_source_coordinates(
            _scope(),
            source_count=2,
        )


def test_source_coordinates_require_canonical_order() -> None:
    scope = _source_scope(
        (
            BillingEntitySourceWitness(1, 0, GROUP_B),
            BillingEntitySourceWitness(0, 0, GROUP_A),
        )
    )

    with pytest.raises(PTG2ManifestArtifactError, match="inconsistent witnesses"):
        contract._validated_source_coordinates(scope, source_count=2)


def test_source_coordinates_reject_one_record_bound_to_multiple_groups() -> None:
    scope = _source_scope(
        (
            BillingEntitySourceWitness(0, 0, GROUP_A),
            BillingEntitySourceWitness(0, 0, GROUP_B),
        )
    )

    with pytest.raises(PTG2ManifestArtifactError, match="inconsistent witnesses"):
        contract._validated_source_coordinates(scope, source_count=2)


def test_source_groups_reject_repeated_group_with_distinct_ordinals() -> None:
    scope = _source_scope(
        (
            BillingEntitySourceWitness(0, 0, GROUP_A),
            BillingEntitySourceWitness(0, 1, GROUP_A),
        )
    )

    with pytest.raises(PTG2ManifestArtifactError, match="inconsistent witnesses"):
        contract.source_groups(
            scope,
            snapshot_key=17,
            source_count=2,
            source_publication=scope.publication,
        )
