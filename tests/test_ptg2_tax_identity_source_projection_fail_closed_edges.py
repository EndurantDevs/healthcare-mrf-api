# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Residual defensive branches for tax-identity source projections."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_tax_identity_source_projection as projection
from process.ptg_parts import ptg2_tax_identity_source_validation as validation
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.ptg2_tax_identity_source_projection_fixture import POLICY, write_sidecar
from tests.test_ptg2_tax_identity_source_publication_edges import _sealed_metadata


class _ExplodingIterable:
    def __iter__(self):
        raise RuntimeError("synthetic iterator failure")


class _ExplodingOrdinalMap(dict[str, int]):
    def __contains__(self, _key):
        return True

    def __getitem__(self, _key):
        raise RuntimeError("synthetic lookup failure")


class _ExplodingCanonicalMetadata(dict[str, object]):
    def get(self, _key, _default=None):
        raise RuntimeError("synthetic metadata failure")


class _InvalidPersistedBinding:
    def persisted_values(self, **_kwargs):
        return {}


def _descriptor(tmp_path):
    return write_sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="1",
        state_codes=(1,),
        matched_hmac=b"h" * 32,
    )


def test_projection_policy_and_ascii_framing_reject_invalid_values() -> None:
    with pytest.raises(TaxIdentitySourceProjectionError):
        projection._strict_policy("INVALID")
    with pytest.raises(TaxIdentitySourceProjectionError):
        projection._digest_ascii(hashlib.sha256(), "café")


def test_source_binding_representation_redacts_descriptor(tmp_path) -> None:
    binding = projection._descriptor_binding(
        _descriptor(tmp_path),
        source_ordinal_by_shard={"file:a": 0},
        expected_policy_id=POLICY,
    )

    assert repr(binding) == "<tax-identity-source-binding source=<redacted>>"


def test_binding_vector_failure_is_reframed_as_projection_error() -> None:
    projection_like = SimpleNamespace(
        bindings=(_InvalidPersistedBinding(),),
        token_policy_id=POLICY,
        token_policy_descriptor_sha256=b"p" * 32,
    )

    with pytest.raises(TaxIdentitySourceProjectionError):
        projection.PreparedTaxIdentitySourceProjection.binding_vector_digest.fget(
            projection_like
        )


@pytest.mark.parametrize(
    "raw_entries",
    (
        (),
        (object(),),
        ({"shard_id": "file:a", "ordinal": 1},),
        (
            {"shard_id": "file:b", "ordinal": 0},
            {"shard_id": "file:a", "ordinal": 1},
        ),
    ),
    ids=("empty", "wrong-record", "wrong-ordinal", "unsorted-shards"),
)
def test_source_ordinal_map_rejects_every_noncanonical_shape(raw_entries) -> None:
    with pytest.raises(TaxIdentitySourceProjectionError):
        projection._source_ordinal_by_shard(raw_entries)


def test_descriptor_contract_failure_is_value_free(tmp_path) -> None:
    descriptor = _descriptor(tmp_path)
    descriptor["final"] = False

    with pytest.raises(TaxIdentitySourceProjectionError) as raised:
        projection._descriptor_binding(
            descriptor,
            source_ordinal_by_shard={"file:a": 0},
            expected_policy_id=POLICY,
        )

    assert str(raised.value) == "ptg2_tax_identity_source_projection_invalid"


def test_descriptor_unexpected_lookup_failure_is_value_free(tmp_path) -> None:
    with pytest.raises(TaxIdentitySourceProjectionError) as raised:
        projection._descriptor_binding(
            _descriptor(tmp_path),
            source_ordinal_by_shard=_ExplodingOrdinalMap(),
            expected_policy_id=POLICY,
        )

    assert str(raised.value) == "ptg2_tax_identity_source_projection_invalid"


def test_binding_collection_redacts_iterator_failures() -> None:
    with pytest.raises(TaxIdentitySourceProjectionError):
        projection._validated_bindings(
            _ExplodingIterable(),
            source_ordinal_by_shard={"file:a": 0},
            token_policy_id=POLICY,
        )


def test_publication_parser_redacts_unexpected_mapping_failures() -> None:
    metadata = _ExplodingCanonicalMetadata(_sealed_metadata())

    with pytest.raises(TaxIdentitySourceProjectionError) as raised:
        projection.tax_identity_source_publication_from_metadata(metadata)

    assert str(raised.value) == "ptg2_tax_identity_source_projection_invalid"


def test_reused_binding_identity_mismatch_fails_closed() -> None:
    stored = (
        {
            "source_key": 1,
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "1" * 64,
        },
    )
    expected = (
        {
            "source_key": 0,
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "1" * 64,
        },
    )

    with pytest.raises(TaxIdentitySourceProjectionError):
        validation._validate_reused_binding_identities(
            stored,
            expected_bindings=expected,
        )
