# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject altered release identities and retained price receipts before use."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import plan_pricing_em_attribution as attribution
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from tests.test_network_em_api_metadata import _rates_reader
from tests.test_plan_pricing_em_attribution import (
    _binding,
    _receipt,
    _relations,
    _selection,
)


def _unopened_session():
    return SimpleNamespace(get_transaction=Mock(side_effect=AssertionError("Invalid identity must not open a read")))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"plan_release_id": "unpublished-release"},
        {"serving_revision_id": "unpublished-revision"},
        {"binding_set_digest": "not-a-digest"},
        {"bindings": ()},
    ],
)
async def test_invalid_release_identity_is_refused_before_database_access(changes):
    session = _unopened_session()
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution is unavailable$"):
        await attribution.resolve_em_attribution(session, replace(_selection(), **changes))
    session.get_transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"binding_ordinal": True},
        {"binding_ordinal": -1},
        {"binding_ordinal": 2**31},
        {"binding_ordinal": 3},
        {"role": "unreviewed"},
        {"snapshot_id": ""},
        {"source_key": None},
        {"plan_id": 42},
        {"required": 1},
        {"logical_scope_present": 0},
    ],
)
async def test_invalid_source_coordinates_cannot_reach_retained_price_resolution(changes):
    selection = _selection(_binding(), replace(_binding(17, "source-b"), **changes))
    session = _unopened_session()
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution is unavailable$"):
        await attribution.resolve_em_attribution(session, selection)
    session.get_transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"identity_json": "[]"},
        {"identity_sha256": "d" * 64},
        {"index_identities": [["rates_lookup", 12], ["rates_pkey", 13, 23]]},
        {"index_identities": [["rates_lookup", True, 22], ["rates_pkey", 13, 23]]},
        {"index_identities": [["rates_pkey", 13, 23], ["rates_lookup", 12, 22]]},
        {"catalog_sha256": "not-a-digest"},
        {"content_sha256": None},
        {"receipt_sha256": "0" * 64},
        {"runtime_roles": "artifact_reader"},
        {"code_accounting": [1, 2, 3]},
    ],
)
async def test_malformed_publication_receipts_fail_before_retained_data_access(monkeypatch, changes):
    session, selection, receipt = _rates_reader(monkeypatch)
    document = receipt.as_dict() | changes
    attribution._namespace.return_value = {"receipt": attribution._canonical(document)}
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution is unavailable$"):
        await attribution.resolve_em_attribution(session, selection)
    session.driver.execute.assert_not_awaited()
    attribution._content.assert_not_awaited()
    assert session.info == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"provider_count": True},
        {"provider_count": 0},
        {"provider_count": 13},
        {"binding_ordinals": ()},
        {"binding_ordinals": (3, True)},
        {"binding_ordinals": (3, 3)},
        {"code_accounting": tuple((index, True, "38") for index in range(6))},
        {"code_accounting": tuple((index, 2, "0" + str(index + 1)) for index in range(6))},
    ],
)
async def test_valid_digest_does_not_authorize_invalid_price_accounting(monkeypatch, changes):
    session, selection, _ = _rates_reader(monkeypatch, receipt=_receipt(**changes))
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution is unavailable$"):
        await attribution.resolve_em_attribution(session, selection)
    session.driver.execute.assert_not_awaited()
    attribution._content.assert_not_awaited()
    assert session.info == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", [None, {"oid": 5, "nspowner": 7}])
async def test_catalog_rejects_missing_namespace_or_added_database_behavior(monkeypatch, namespace):
    monkeypatch.setattr(attribution, "_namespace", AsyncMock(return_value=namespace))
    shape_reader = AsyncMock(side_effect=AssertionError("Unsafe catalog must stop before table inspection"))
    monkeypatch.setattr(attribution, "_table_shape", shape_reader)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=_relations()), fetchval=AsyncMock(return_value=True))
    with pytest.raises(ValueError, match="namespace is absent|routines or inherited"):
        await attribution._catalog(connection, "em_attribution_" + "a" * 40)
    shape_reader.assert_not_awaited()


def test_price_catalog_rejects_partitioned_heap_and_unknown_scalar_encoding():
    relations = _relations()
    relations[0]["relkind"] = b"p"
    with pytest.raises(ValueError, match="heap differs"):
        attribution._validate_relations(relations)
    with pytest.raises(TypeError, match="unexpected native catalog scalar"):
        attribution._catalog_digest({"relkind": b"unexpected"})
