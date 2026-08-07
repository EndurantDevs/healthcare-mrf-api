# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Residual fail-closed branches for exact billing readers."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from api import ptg2_billing_entity_refs as billing_refs
from api import ptg2_billing_entity_source_resolution as source_resolution
from api import ptg2_billing_exact_reader as exact_reader
from api import ptg2_billing_geo_reader as geo_reader
from api import ptg2_billing_price_reader as price_reader
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.ptg2_billing_geo_reader_support import (
    GROUP_A,
    NPI_A,
    _provider_rate,
    _rate,
    _tables,
)
from tests.test_ptg2_billing_entity_source_resolution import _publication
from tests.test_ptg2_billing_price_reader import _geo_witness


class _RepeatedSetItems(dict[int, tuple[int, ...]]):
    """Expose a repeated logical key through a hostile Mapping view."""

    def items(self):
        return iter(((0, (1,)), (0, (1,))))


@pytest.mark.parametrize(
    ("decoded_payload", "error_line"),
    ((ValueError("invalid"), "decode"), (b"", "length"), (b"\x01" * 48, "canonical")),
    ids=("decoder-error", "wrong-length", "noncanonical"),
)
def test_billing_reference_decoder_rejects_post_alphabet_failures(
    monkeypatch,
    decoded_payload,
    error_line,
) -> None:
    """Reject decoder, length, and canonicalization failures generically."""

    if isinstance(decoded_payload, Exception):

        def fail_decode(*_args, **_kwargs):
            raise decoded_payload

        monkeypatch.setattr(billing_refs.base64, "b64decode", fail_decode)
    else:
        monkeypatch.setattr(
            billing_refs.base64,
            "b64decode",
            lambda *_args, **_kwargs: decoded_payload,
        )

    with pytest.raises(
        billing_refs.PTG2BillingAssociationDataError,
        match=r"^billing entity reference is invalid$",
    ):
        billing_refs.decode_billing_entity_ref("be1_" + "A" * 64)

    assert error_line in {"decode", "length", "canonical"}


def test_source_witness_repr_and_empty_state_are_fail_closed() -> None:
    witness = source_resolution.BillingEntitySourceWitness(0, 0, "a" * 32)
    assert repr(witness) == "<billing-entity-source-witness value=<redacted>>"

    with pytest.raises(billing_refs.PTG2BillingAssociationDataError):
        source_resolution._validated_source_publication((), expected=_publication())
    with pytest.raises(billing_refs.PTG2BillingAssociationDataError):
        source_resolution._normalized_source_witnesses((), source_count=1)


def test_source_publication_requires_the_exact_canonical_type() -> None:
    with pytest.raises(billing_refs.PTG2BillingAssociationDataError):
        source_resolution._canonical_source_publication(object())


def test_source_publication_redacts_parser_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        source_resolution,
        "tax_identity_source_publication_from_metadata",
        lambda _metadata: (_ for _ in ()).throw(
            TaxIdentitySourceProjectionError("synthetic")
        ),
    )

    with pytest.raises(billing_refs.PTG2BillingAssociationDataError):
        source_resolution._canonical_source_publication(_publication())


def test_source_publication_rejects_nonidentical_canonical_result(monkeypatch) -> None:
    publication = _publication()
    monkeypatch.setattr(
        source_resolution,
        "tax_identity_source_publication_from_metadata",
        lambda _metadata: replace(
            publication, source_count=publication.source_count + 1
        ),
    )

    with pytest.raises(billing_refs.PTG2BillingAssociationDataError):
        source_resolution._canonical_source_publication(publication)


def test_exact_group_to_set_projection_rejects_malformed_and_overflow() -> None:
    group_ref = "a" * 32
    set_ref = "b" * 32
    with pytest.raises(PTG2ManifestArtifactError, match="malformed"):
        exact_reader._validated_sets_by_group(
            {group_ref: [set_ref]},
            provider_group_refs=(group_ref,),
        )
    with pytest.raises(PTG2ManifestArtifactError, match="edge limit"):
        exact_reader._validated_sets_by_group(
            {group_ref: (set_ref,) * (exact_reader._MAX_ASSOCIATION_EDGES + 1)},
            provider_group_refs=(group_ref,),
        )


def test_exact_group_dictionary_and_reverse_projection_reject_ambiguity() -> None:
    first_group = "a" * 32
    second_group = "b" * 32
    set_ref = "c" * 32
    with pytest.raises(PTG2ManifestArtifactError, match="keys are inconsistent"):
        exact_reader._group_refs_by_key({first_group: 1, second_group: 1})

    projection_arguments_by_name = {
        "sets_by_group": {first_group: (set_ref,)},
        "provider_set_keys_by_id": {set_ref: 0},
        "group_keys_by_id": {first_group: 1},
    }
    with pytest.raises(PTG2ManifestArtifactError, match="projections are inconsistent"):
        exact_reader._validated_exact_groups_by_set(
            _RepeatedSetItems(),
            **projection_arguments_by_name,
        )
    with pytest.raises(PTG2ManifestArtifactError, match="projections are inconsistent"):
        exact_reader._validated_exact_groups_by_set(
            {0: [1]},
            **projection_arguments_by_name,
        )


def test_exact_source_coordinate_limits_are_enforced_before_lookup() -> None:
    group_ref = "a" * 32
    set_ref = "b" * 32
    with pytest.raises(PTG2ManifestArtifactError, match="source/group/set scope"):
        exact_reader._source_set_coordinates(
            group_refs_by_source={0: {group_ref: 0}},
            sets_by_group={
                group_ref: (set_ref,) * (exact_reader._MAX_SOURCE_GROUP_SET_EDGES + 1)
            },
            provider_set_keys_by_id={set_ref: 0},
            code_keys=(0,),
        )
    with pytest.raises(PTG2ManifestArtifactError, match="forward filter scope"):
        exact_reader._source_set_coordinates(
            group_refs_by_source={0: {group_ref: 0}},
            sets_by_group={group_ref: (set_ref,)},
            provider_set_keys_by_id={set_ref: 0},
            code_keys=tuple(range(exact_reader._MAX_FORWARD_FILTER_COORDINATES + 1)),
        )


def test_exact_forward_occurrence_and_price_size_shapes_are_strict() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="projection is malformed"):
        exact_reader._validated_occurrences_by_code(
            {0: []},
            code_keys=(0,),
            provider_set_keys=frozenset({1}),
            source_keys=frozenset({2}),
            allowed_set_source_coordinates=frozenset({(1, 2)}),
            price_item_count=1,
        )
    with pytest.raises(PTG2ManifestArtifactError, match="dictionary size"):
        exact_reader._price_item_count({"price_dictionary_item_count": True})


def test_group_npi_projection_requires_tuple_members() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="projection is malformed"):
        geo_reader._npi_members_by_group(
            {GROUP_A: []},
            provider_group_refs=(GROUP_A,),
        )


def test_provider_rate_expansion_enforces_its_output_bound(monkeypatch) -> None:
    monkeypatch.setattr(geo_reader, "_MAX_PROVIDER_RATE_WITNESSES", 0)
    with pytest.raises(PTG2ManifestArtifactError, match="provider/rate scope"):
        geo_reader._expanded_provider_rates(
            (_rate(),),
            {GROUP_A: (NPI_A,)},
            None,
        )


@pytest.mark.asyncio
async def test_provider_expansion_requires_v4_and_handles_empty_scope() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="sealed V4 graph"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            SimpleNamespace(uses_v4_graph=False),
            rate_witnesses=(),
        )

    assert (
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            _tables(),
            rate_witnesses=(),
        )
        == ()
    )


@pytest.mark.asyncio
async def test_provider_expansion_rejects_snapshot_and_group_fanout(
    monkeypatch,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="another snapshot"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            _tables(),
            rate_witnesses=(replace(_rate(), snapshot_key=18),),
        )

    monkeypatch.setattr(geo_reader, "_MAX_PROVIDER_GROUPS", 0)
    with pytest.raises(PTG2ManifestArtifactError, match="group limit"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            _tables(),
            rate_witnesses=(_rate(),),
        )


@pytest.mark.asyncio
async def test_geo_reader_handles_empty_scope_and_rejects_snapshot_drift() -> None:
    assert await geo_reader.load_exact_billing_geo_witnesses(
        object(),
        _tables(),
        provider_rate_witnesses=(),
        geo_args={"zip5": "25000"},
    ) == geo_reader.BillingGeoSelection(True, ())

    with pytest.raises(PTG2ManifestArtifactError, match="another snapshot"):
        await geo_reader.load_exact_billing_geo_witnesses(
            object(),
            _tables(),
            provider_rate_witnesses=(replace(_provider_rate(), snapshot_key=18),),
            geo_args={"zip5": "25000"},
        )


def test_price_reader_rejects_malformed_payload_and_geo_witness() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="hydration is malformed"):
        price_reader._validated_prices_by_key({10: [object()]}, price_keys=(10,))
    with pytest.raises(PTG2ManifestArtifactError, match="geo witness scope"):
        price_reader._normalized_geo_witnesses(_tables(), (object(),))


def test_price_reader_skips_witnesses_without_matching_atoms(monkeypatch) -> None:
    monkeypatch.setattr(
        price_reader.ptg2_serving,
        "_ptg2_manifest_filter_prices",
        lambda _prices, _filters: [],
    )
    assert (
        price_reader._hydrated_price_witnesses(
            (_geo_witness(),),
            {10: [{"negotiated_rate": 20}]},
            {},
            atom_budget=1,
        )
        == ()
    )


@pytest.mark.asyncio
async def test_price_reader_rejects_atom_and_key_bounds(monkeypatch) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="invalid atom budget"):
        await price_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(),
            atom_budget=False,
        )

    monkeypatch.setattr(price_reader, "MAX_PRICE_KEYS", 0)
    with pytest.raises(PTG2ManifestArtifactError, match="key limit"):
        await price_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(_geo_witness(),),
        )


@pytest.mark.asyncio
async def test_price_reader_returns_before_io_for_empty_scope() -> None:
    assert (
        await price_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(),
        )
        == ()
    )
