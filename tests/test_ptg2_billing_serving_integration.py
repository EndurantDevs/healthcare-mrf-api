# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full exact-NPI serving coverage for public billing associations."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


NPI = 1234567890
SET_ONE = "11" * 16
SET_TWO = "22" * 16
PRICE_ONE = "33" * 16
PRICE_TWO = "44" * 16
PACK_ONE = "55" * 16
PACK_TWO = "66" * 16
GROUP_ONE = "aa" * 16
GROUP_TWO = "bb" * 16
UNRELATED_GROUP = "cc" * 16


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def mappings(self):
        return self


class _RoutingSession:
    def __init__(self) -> None:
        first_hmac = bytes(range(32))
        second_hmac = bytes(range(32, 64))
        self.sidecar_rows = {
            GROUP_ONE: _sidecar_row(GROUP_ONE, first_hmac),
            GROUP_TWO: _sidecar_row(GROUP_TWO, second_hmac),
        }
        self.sidecar_requests: list[tuple[str, ...]] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, parameters):
        sql = str(statement)
        parameters_by_name = dict(parameters)
        self.calls.append((sql, parameters_by_name))
        if "ptg2_provider_tax_identity_manifest" not in sql:
            return _Result([_code_metadata_row()])
        requested_group_refs = tuple(
            sorted(
                bytes(value).hex()
                for value in parameters_by_name["provider_group_refs"]
            )
        )
        self.sidecar_requests.append(requested_group_refs)
        return _Result(
            [self.sidecar_rows[group_ref] for group_ref in requested_group_refs]
        )


def _sidecar_row(group_ref: str, full_hmac: bytes) -> dict[str, Any]:
    policy_id = "ptg-tin-hmac-sha256-v1:2026-07"
    return {
        "provider_group_ref": group_ref,
        "manifest_count": 1,
        "legacy_count": 1,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "token_policy_id": policy_id,
        "token_policy_descriptor_sha256": bytes.fromhex(
            token_policy_descriptor_sha256(policy_id)
        ),
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "tax_identity_state": "matched_ein",
        "tin_id_128": full_hmac[:16],
        "tin_hmac_sha256": full_hmac,
    }


def _code_metadata_row() -> dict[str, Any]:
    return {
        "code_key": 7,
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "rate_count": 2,
    }


def _tables() -> serving.PTG2ServingTables:
    return serving.PTG2ServingTables(
        snapshot_id="logical-plan-a",
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_snapshot_key=41,
        serving_table_layout="lean_provider_key_v1",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        atom_key_bits=24,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=65_536,
        price_key_block_span=512,
        atom_key_block_span=512,
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
    )


def _build_rate_row(
    provider_set_ref: str,
    price_set_ref: str,
    rate_pack_ref: str,
    provider_set_key: int,
    price_key: int,
) -> dict[str, Any]:
    return {
        "serving_content_hash_128": rate_pack_ref,
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "provider_set_global_id_128": provider_set_ref,
        "provider_count": 1,
        "price_set_global_id_128": price_set_ref,
        "price_key": price_key,
        "source_key": 0,
        "network_names": [],
        "_ptg_provider_set_key": provider_set_key,
    }


def _serving_rows() -> list[dict[str, Any]]:
    return [
        _build_rate_row(SET_ONE, PRICE_ONE, PACK_ONE, 3, 9),
        _build_rate_row(SET_TWO, PRICE_TWO, PACK_TWO, 4, 10),
    ]


def _provider_rows() -> dict[str, list[dict[str, Any]]]:
    provider_by_field = {
        "npi": NPI,
        "provider_name": "Synthetic clinician",
        "location_hash": "synthetic-location",
    }
    return {
        SET_ONE: [dict(provider_by_field)],
        SET_TWO: [dict(provider_by_field)],
    }


def _install_search_stubs(monkeypatch) -> None:
    scope = serving._ExplicitNpiGraphScope(NPI, (3, 4))
    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=scope),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=_serving_rows()),
    )
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(
            return_value={
                PRICE_ONE: [{"negotiated_rate": "100.00"}],
                PRICE_TWO: [{"negotiated_rate": "200.00"}],
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        serving,
        "_exact_npi_provider_rows_by_set",
        AsyncMock(return_value=_provider_rows()),
    )


def _install_billing_stubs(monkeypatch) -> AsyncMock:
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={SET_ONE: 3, SET_TWO: 4}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_graph_members_for_id",
        AsyncMock(return_value=(GROUP_ONE, GROUP_TWO, UNRELATED_GROUP)),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={GROUP_ONE: 7, GROUP_TWO: 8, UNRELATED_GROUP: 9}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=SimpleNamespace(representation="direct_v1")),
    )
    intersection = AsyncMock(return_value={3: (7,), 4: (8,)})
    monkeypatch.setattr(serving, "lookup_v4_relation_intersections", intersection)
    return intersection


def _payload_keys(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _payload_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _payload_keys(item)


def _payload_values(value: Any):
    if isinstance(value, dict):
        for item in value.values():
            yield from _payload_values(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _payload_values(item)
    else:
        yield value


def _assert_public_billing_item(item: dict[str, Any]) -> None:
    expected_by_set = {
        SET_ONE: (PRICE_ONE, PACK_ONE),
        SET_TWO: (PRICE_TWO, PACK_TWO),
    }
    options_by_set = {
        option["provider_set_ref"]: option for option in item["rate_options"]
    }
    assert set(options_by_set) == set(expected_by_set)
    for set_ref, (price_ref, pack_ref) in expected_by_set.items():
        option = options_by_set[set_ref]
        assert (option["price_set_ref"], option["rate_pack_ref"]) == (
            price_ref,
            pack_ref,
        )
        assert option["billing_association_status"] == "resolved"
        assert option["billing_association_count"] == 1
        association = option["billing_associations"][0]
        assert association["association_ordinal"] == 1
        assert "provider_group_ref" not in association
        assert association["tin_type"] == "ein"
        assert association["billing_entity_ref"].startswith("be1_")
    assert item["rate_option_count"] == 2
    assert item["billing_association_count"] == 2
    assert item["resolved_billing_entity_count"] == 2
    assert item["billing_entity_count"] == 2
    assert item["billing_entity_count_status"] == "exact"


def _billing_option_fingerprint(item: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        sorted(
            (
                option["provider_set_ref"],
                option["price_set_ref"],
                option["rate_pack_ref"],
                option["billing_association_status"],
                option["billing_association_count"],
                tuple(
                    sorted(
                        (
                            association["association_ordinal"],
                            association["billing_entity_ref"],
                            association["tin_type"],
                            association["tax_identity_status"],
                        )
                        for association in option["billing_associations"]
                    )
                ),
            )
            for option in item["rate_options"]
        )
    )


def _assert_no_private_payload_values(response: dict[str, Any]) -> None:
    forbidden_keys = {
        "tin_id_128",
        "tin_hmac_sha256",
        "tin_key",
        "provider_group_ref",
        "_ptg_provider_set_key",
        "_ptg_price_key",
        "source_artifact_key",
    }
    assert forbidden_keys.isdisjoint(set(_payload_keys(response)))
    assert not any(
        isinstance(value, (bytes, bytearray, memoryview))
        for value in _payload_values(response)
    )


@pytest.mark.asyncio
async def test_v4_exact_npi_search_attaches_intersected_billing_associations(
    monkeypatch,
) -> None:
    _install_search_stubs(monkeypatch)
    intersection = _install_billing_stubs(monkeypatch)
    session = _RoutingSession()

    response = await serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": str(NPI),
            "include_providers": True,
        },
        SimpleNamespace(limit=25, offset=0),
        _tables(),
        "exact_source",
    )

    assert response is not None
    assert len(response["items"]) == 1
    assert response["items"][0]["npi"] == NPI
    _assert_public_billing_item(response["items"][0])
    _assert_no_private_payload_values(response)
    assert session.sidecar_requests == [tuple(sorted((GROUP_ONE, GROUP_TWO)))]
    intersection.assert_awaited_once()
    assert intersection.await_args.kwargs["relation"] == "set_groups_direct"
    assert intersection.await_args.kwargs["owner_keys"] == (3, 4)
    assert intersection.await_args.kwargs["allowed_member_keys"] == (7, 8, 9)


@pytest.mark.asyncio
async def test_v4_exact_npi_geo_search_preserves_billing_associations(
    monkeypatch,
) -> None:
    _install_search_stubs(monkeypatch)
    intersection = _install_billing_stubs(monkeypatch)
    location = AsyncMock(return_value=({SET_ONE, SET_TWO}, _provider_rows()))
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        location,
    )
    provider_wide_session = _RoutingSession()
    geo_session = _RoutingSession()
    common_args_by_name = {
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "code_system": "CPT",
        "code": "99213",
        "npi": str(NPI),
        "include_providers": True,
    }

    provider_wide_response = await serving._search_manifest_serving_table(
        provider_wide_session,
        "logical-plan-a",
        common_args_by_name,
        SimpleNamespace(limit=25, offset=0),
        _tables(),
        "exact_source",
    )
    geo_response = await serving._search_manifest_serving_table(
        geo_session,
        "logical-plan-a",
        {**common_args_by_name, "state": "ZZ"},
        SimpleNamespace(limit=25, offset=0),
        _tables(),
        "exact_source",
    )

    assert provider_wide_response is not None
    assert geo_response is not None
    provider_wide_item = provider_wide_response["items"][0]
    geo_item = geo_response["items"][0]
    _assert_public_billing_item(geo_item)
    _assert_no_private_payload_values(geo_response)
    assert _billing_option_fingerprint(geo_item) == _billing_option_fingerprint(
        provider_wide_item
    )
    assert geo_item["npi"] == NPI
    assert geo_response["query"]["state"] == "ZZ"
    assert provider_wide_session.sidecar_requests == [
        tuple(sorted((GROUP_ONE, GROUP_TWO)))
    ]
    assert geo_session.sidecar_requests == [tuple(sorted((GROUP_ONE, GROUP_TWO)))]
    assert intersection.await_count == 2
    location.assert_awaited_once()
    assert location.await_args.kwargs["explicit_npi_scope"] == (
        serving._ExplicitNpiGraphScope(NPI, (3, 4))
    )


@pytest.mark.asyncio
async def test_v4_exact_npi_geo_no_match_skips_billing_sidecar(
    monkeypatch,
) -> None:
    _install_search_stubs(monkeypatch)
    location = AsyncMock(return_value=(set(), {}))
    intersection = AsyncMock()
    billing_resolver = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        location,
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_intersections", intersection)
    monkeypatch.setattr(
        serving,
        "_exact_npi_billing_associations_by_set",
        billing_resolver,
    )
    session = _RoutingSession()

    response = await serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": str(NPI),
            "state": "ZZ",
            "include_providers": True,
        },
        SimpleNamespace(limit=25, offset=0),
        _tables(),
        "exact_source",
    )

    assert response is not None
    assert response["items"] == []
    assert response["pagination"]["total"] == 0
    assert response["query"]["state"] == "ZZ"
    assert location.await_args.kwargs["explicit_npi_scope"] == (
        serving._ExplicitNpiGraphScope(NPI, (3, 4))
    )
    assert session.sidecar_requests == []
    intersection.assert_not_awaited()
    billing_resolver.assert_not_awaited()
