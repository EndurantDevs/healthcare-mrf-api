# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact billing provider, geo, and price hydration tests."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_geo_reader as geo_reader
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_billing_geo_reader_support import (
    GROUP_A,
    GROUP_B,
    NPI_A,
    NPI_B,
    NPI_C,
    _location_row,
    _member,
    _provider_rate,
    _rate,
    _replace_location_payload,
    _tables,
)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("snapshot_key", True),
        ("code_key", -1),
        ("source_key", True),
        ("source_record_ordinal", -1),
        ("provider_group_ref", GROUP_A.upper()),
        ("provider_set_key", True),
        ("price_key", 2**32),
        ("occurrence_ordinal", -1),
    ),
)
def test_rate_witnesses_reject_every_invalid_coordinate(
    field_name,
    invalid_value,
) -> None:
    invalid_witness = replace(_rate(), **{field_name: invalid_value})

    with pytest.raises(PTG2ManifestArtifactError, match="invalid rate witness"):
        geo_reader.validated_rate_witnesses((invalid_witness,))


@pytest.mark.parametrize("witnesses", ("noncanonical", "duplicate"))
def test_rate_witnesses_require_canonical_unique_coordinates(witnesses) -> None:
    first = _rate(price_key=10)
    second = _rate(price_key=11)
    invalid_witnesses = (
        (second, first) if witnesses == "noncanonical" else (first, first)
    )

    with pytest.raises(PTG2ManifestArtifactError, match="canonical and unique"):
        geo_reader.validated_rate_witnesses(invalid_witnesses)


@pytest.mark.parametrize(
    "invalid_witness",
    (
        object(),
        replace(_provider_rate(), price_key=2**32),
        replace(_provider_rate(), npi=1234567890),
    ),
    ids=("wrong-type", "invalid-rate-coordinate", "invalid-npi"),
)
def test_provider_rate_witnesses_reject_invalid_complete_coordinates(
    invalid_witness,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="provider/rate scope"):
        geo_reader.validated_provider_rate_witnesses((invalid_witness,))


@pytest.mark.parametrize("witnesses", ("noncanonical", "duplicate"))
def test_provider_rate_witnesses_require_canonical_unique_coordinates(
    witnesses,
) -> None:
    first = _provider_rate(npi=NPI_A)
    second = _provider_rate(npi=NPI_B)
    invalid_witnesses = (
        (second, first) if witnesses == "noncanonical" else (first, first)
    )

    with pytest.raises(PTG2ManifestArtifactError, match="inconsistent"):
        geo_reader.validated_provider_rate_witnesses(invalid_witnesses)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_rates",
    (
        (replace(_provider_rate(), price_key=-1),),
        (
            _provider_rate(npi=NPI_B),
            _provider_rate(npi=NPI_A),
        ),
        (_provider_rate(), _provider_rate()),
    ),
    ids=("invalid-coordinate", "noncanonical", "duplicate"),
)
async def test_geo_rejects_invalid_provider_witnesses_before_address_io(
    monkeypatch,
    provider_rates,
) -> None:
    location_lookup = AsyncMock()
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_membership_location_rows",
        location_lookup,
    )

    with pytest.raises(PTG2ManifestArtifactError):
        await geo_reader.load_exact_billing_geo_witnesses(
            object(),
            _tables(),
            provider_rate_witnesses=provider_rates,
            geo_args={"zip5": "25000"},
        )

    location_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_group_first_expansion_preserves_shared_npi_witnesses(
    monkeypatch,
) -> None:
    graph = AsyncMock(
        return_value={
            GROUP_A: (_member(NPI_A), _member(NPI_B)),
            GROUP_B: (_member(NPI_A), _member(NPI_C)),
        }
    )
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_shared_graph_members_by_id",
        graph,
    )
    rates = (
        _rate(),
        _rate(
            group_ref=GROUP_B,
            source_key=1,
            occurrence_ordinal=1,
        ),
    )

    witnesses = await geo_reader.expand_billing_rate_witnesses_to_npis(
        object(),
        _tables(),
        rate_witnesses=rates,
    )

    assert [
        (
            witness.npi,
            witness.provider_group_ref,
            witness.source_key,
            witness.source_record_ordinal,
        )
        for witness in witnesses
    ] == [
        (NPI_A, GROUP_A, 0, 0),
        (NPI_A, GROUP_B, 1, 1),
        (NPI_B, GROUP_A, 0, 0),
        (NPI_C, GROUP_B, 1, 1),
    ]
    assert graph.await_args.args[2:] == (
        "provider_group_npi",
        (GROUP_A, GROUP_B),
    )
    assert graph.await_args.kwargs == {
        "max_members": 2049,
        "max_projection_members": 8193,
    }
    assert GROUP_A not in repr(witnesses[0])


@pytest.mark.asyncio
async def test_optional_npi_is_exact_same_group_intersection(monkeypatch) -> None:
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_shared_graph_members_by_id",
        AsyncMock(
            return_value={
                GROUP_A: (_member(NPI_A), _member(NPI_B)),
                GROUP_B: (_member(NPI_A), _member(NPI_C)),
            }
        ),
    )
    witnesses = await geo_reader.expand_billing_rate_witnesses_to_npis(
        object(),
        _tables(),
        rate_witnesses=(
            _rate(),
            _rate(group_ref=GROUP_B, source_key=1, occurrence_ordinal=1),
        ),
        provider_npi=NPI_A,
    )
    assert [(witness.provider_group_ref, witness.npi) for witness in witnesses] == [
        (GROUP_A, NPI_A),
        (GROUP_B, NPI_A),
    ]

    with pytest.raises(ValueError, match="checksum-valid"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            _tables(),
            rate_witnesses=(),
            provider_npi=1234567890,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("members", "error"),
    [
        ({GROUP_A: (_member(NPI_A),)}, "incomplete"),
        (
            {GROUP_A: ("not-a-member",), GROUP_B: ()},
            "invalid NPI member",
        ),
        (
            {
                GROUP_A: (_member(NPI_B), _member(NPI_A)),
                GROUP_B: (),
            },
            "inconsistent",
        ),
    ],
)
async def test_group_npi_projection_fails_closed(
    monkeypatch,
    members,
    error,
) -> None:
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_shared_graph_members_by_id",
        AsyncMock(return_value=members),
    )
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(),
            _tables(),
            rate_witnesses=(
                _rate(),
                _rate(group_ref=GROUP_B, source_key=1, occurrence_ordinal=1),
            ),
        )


@pytest.mark.asyncio
async def test_group_npi_per_owner_and_total_caps_are_checked(monkeypatch) -> None:
    monkeypatch.setattr(geo_reader, "_MAX_PROVIDER_NPIS_PER_GROUP", 1)
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_shared_graph_members_by_id",
        AsyncMock(
            return_value={
                GROUP_A: (_member(NPI_A), _member(NPI_B)),
            }
        ),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="NPI limit"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(), _tables(), rate_witnesses=(_rate(),)
        )

    monkeypatch.setattr(geo_reader, "_MAX_PROVIDER_NPIS_PER_GROUP", 2048)
    monkeypatch.setattr(geo_reader, "_MAX_GROUP_NPI_EDGES", 1)
    with pytest.raises(PTG2ManifestArtifactError, match="edge limit"):
        await geo_reader.expand_billing_rate_witnesses_to_npis(
            object(), _tables(), rate_witnesses=(_rate(),)
        )


@pytest.mark.parametrize(
    ("geo_args", "error"),
    [
        ({}, "required"),
        ({"zip5": "2500"}, "five ASCII"),
        ({"zip5": "２５０００"}, "five ASCII"),
        ({"lat": 38.0}, "supplied together"),
        ({"lat": 91.0, "long": -82.0}, "outside"),
        ({"lat": 38.0, "long": -82.0, "radius_miles": -1}, "bounded"),
        ({"radius_miles": 25}, "requires lat"),
    ],
)
def test_geo_args_are_strictly_bounded(geo_args, error) -> None:
    with pytest.raises(ValueError, match=error):
        geo_reader._validated_geo_args(geo_args)


def test_geo_args_accept_exact_zip_or_bounded_coordinates() -> None:
    assert geo_reader._validated_geo_args({"zip5": "25000"}) == {
        "mode": "exact_source",
        "include_evidence": True,
        "zip5": "25000",
    }
    assert geo_reader._validated_geo_args({"lat": 38, "long": -82}) == {
        "mode": "exact_source",
        "include_evidence": True,
        "lat": 38.0,
        "long": -82.0,
        "radius_miles": 25.0,
    }


@pytest.mark.asyncio
async def test_geo_joins_only_each_npis_own_selected_address(monkeypatch) -> None:
    location_lookup = AsyncMock(return_value=[_location_row(NPI_B)])
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_membership_location_rows",
        location_lookup,
    )
    selection = await geo_reader.load_exact_billing_geo_witnesses(
        object(),
        _tables(),
        provider_rate_witnesses=(
            _provider_rate(npi=NPI_A),
            _provider_rate(npi=NPI_B),
        ),
        geo_args={"zip5": "25000"},
    )

    assert selection.address_projection_available is True
    assert [witness.provider_rate.npi for witness in selection.witnesses] == [NPI_B]
    selected_address = selection.witnesses[0].address
    assert selected_address.address_key == ("00000000-0000-0000-0000-000000000001")
    assert selected_address.address_site_key == ("00000000-0000-0000-0000-000000000002")
    assert selected_address.provenance[0].record_version_id == "20260101"
    assert "synthetic:" not in repr(selected_address.provenance[0])
    assert selected_address.selection_contract == (
        "ptg2_billing_provider_address_selection_v1"
    )
    assert selected_address.display["first_line"] == "10 Example Ave"
    assert "source_record_ids" not in selected_address.display
    assert "address_sources" not in selected_address.display
    assert selected_address.geo_evidence_level == "nppes_registry_address"
    assert selected_address.geo_evidence_source_id == 1
    assert location_lookup.await_args.kwargs == {
        "candidate_npis": (NPI_A, NPI_B),
        "limit": 2,
        "offset": 0,
        "stored_address_provenance_only": True,
    }
    assert location_lookup.await_args.args[2] == {
        "mode": "exact_source",
        "include_evidence": True,
        "zip5": "25000",
    }


@pytest.mark.asyncio
async def test_geo_distinguishes_unavailable_projection_from_no_match(
    monkeypatch,
) -> None:
    location_lookup = AsyncMock(side_effect=(None, []))
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_membership_location_rows",
        location_lookup,
    )
    provider_rates = (_provider_rate(),)
    unavailable = await geo_reader.load_exact_billing_geo_witnesses(
        object(),
        _tables(),
        provider_rate_witnesses=provider_rates,
        geo_args={"zip5": "25000"},
    )
    no_match = await geo_reader.load_exact_billing_geo_witnesses(
        object(),
        _tables(),
        provider_rate_witnesses=provider_rates,
        geo_args={"zip5": "25000"},
    )
    assert unavailable == geo_reader.BillingGeoSelection(False, ())
    assert no_match == geo_reader.BillingGeoSelection(True, ())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "error"),
    [
        ([_location_row(NPI_C)], "escaped its NPI scope"),
        (
            [_location_row(NPI_A), _location_row(NPI_A)],
            "contains duplicates",
        ),
        (
            [_location_row(NPI_A, distance=-1)],
            "distance is malformed",
        ),
        (
            [_location_row(NPI_A, distance=True)],
            "distance is malformed",
        ),
    ],
)
async def test_geo_projection_rows_fail_closed(monkeypatch, rows, error) -> None:
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_membership_location_rows",
        AsyncMock(return_value=rows),
    )
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await geo_reader.load_exact_billing_geo_witnesses(
            object(),
            _tables(),
            provider_rate_witnesses=(_provider_rate(),),
            geo_args={"zip5": "25000"},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "error"),
    [
        (
            _replace_location_payload(
                _location_row(NPI_A),
                address_provenance=None,
            ),
            "provenance is incomplete",
        ),
        (
            _replace_location_payload(
                _location_row(NPI_A),
                address_provenance=[
                    {
                        "dataset_id": "marketplace_provider_directory",
                        "source_id": 2,
                        "source_record_id": "synthetic:other-source",
                        "record_version_id": "20260101",
                        "record_version_ids": ["20260101"],
                        "retrieved_at": "2026-01-01",
                    }
                ],
            ),
            "provenance is incomplete",
        ),
        (
            _replace_location_payload(
                _location_row(NPI_A),
                address_site_key="00000000-0000-0000-0000-00000000000A",
            ),
            "site key is malformed",
        ),
        (
            _replace_location_payload(
                _location_row(NPI_A),
                location_key="not-a-location-key",
            ),
            "location key is malformed",
        ),
        (
            {**_location_row(NPI_A), "type": "mail"},
            "not a physical location",
        ),
    ],
)
async def test_geo_requires_complete_immutable_address_lineage(
    monkeypatch,
    row,
    error,
) -> None:
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_membership_location_rows",
        AsyncMock(return_value=[row]),
    )

    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await geo_reader.load_exact_billing_geo_witnesses(
            object(),
            _tables(),
            provider_rate_witnesses=(_provider_rate(),),
            geo_args={"zip5": "25000"},
        )
