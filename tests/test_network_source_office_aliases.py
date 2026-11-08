# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Alias layout is explicit without relabeling any existing address receipt."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_custom_address_source as custom
from process.network_address_projection import PinnedAddressSource
from tests.test_registry_ptg_address_carry import _artifact, _receipt


@pytest.mark.parametrize(
    "ptg,aca,digest",
    [
        (None, None, "b862cee9ee1c0ccef4660be2a0cb77666952b18dc1de9d59507ed714ec18f6bc"),
        ("f" * 64, None, "df8e20ce06910d6a5530fda0e54dc7957ec5027035655dedbcf25e41591eedf7"),
        (None, "e" * 64, "cea3f9a418a8e08ca886eb3327fdab17d397d32028f7fb4975d2f0ae48e099dc"),
        ("f" * 64, "e" * 64, "4c9ddd9a4f7ec4cd520245651d88a6de650699029662c842239ccf34d962dafb"),
    ],
)
def test_existing_receipt_generation_hashes_remain_byte_identical(ptg, aca, digest):
    receipt = replace(_artifact(), ptg_carry_sha256=ptg, aca_carry_sha256=aca)
    assert custom._generation(receipt) == digest
    assert "source_aliases" not in receipt.as_dict()


@pytest.mark.parametrize("ptg", [None, "f" * 64])
@pytest.mark.parametrize("aca", [None, "e" * 64])
def test_v5_alias_layout_preserves_carry_contracts(ptg, aca):
    old = replace(_artifact(), ptg_carry_sha256=ptg, aca_carry_sha256=aca)
    receipt = replace(old, source_aliases=True)
    assert "source_aliases" not in old.as_dict() and old.as_dict()["revision"] in (2, 3, 4)
    assert receipt.as_dict()["revision"] == 5 and receipt.as_dict()["source_aliases"] is True
    assert custom._from_document(json.dumps(receipt.as_dict())) == receipt
    assert custom._generation(old) != custom._generation(receipt)
    assert custom._recipe_identity(old) != custom._recipe_identity(receipt)


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 5},
        {"revision": 5, "source_aliases": False},
        {"revision": 5, "source_aliases": 1},
        {"revision": 5, "source_aliases": True, "aca_carry_sha256": None},
        {"revision": 5, "source_aliases": True, "ptg_carry_sha256": "F" * 64},
        {"source_aliases": True},
        {"source_aliases": False},
    ],
)
def test_alias_layout_cannot_be_smuggled_into_an_old_or_open_receipt(changes):
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(json.dumps({**_artifact().as_dict(), **changes}))


def test_layout_requirement_compares_aliases_across_authenticated_pages():
    first = _receipt()
    alias = replace(first.records[0], location_id="00000000-0000-0000-0000-000000000009")
    assert not custom._has_source_aliases((first, first))
    assert custom._has_source_aliases((first, replace(first, records=(alias,))))
    assert not custom._has_source_aliases((first, replace(first, records=(replace(alias, location_key="a" * 64),))))


@pytest.mark.asyncio
@pytest.mark.parametrize("native_aliases", [False, True])
async def test_supplied_layout_flag_is_rederived_before_native_receipt_acceptance(monkeypatch, native_aliases):
    receipt = replace(_artifact(), source_aliases=True)
    digest = custom._generation(receipt)
    receipt = replace(
        receipt,
        generation_sha256=digest,
        address_source=PinnedAddressSource(
            "network_composition_" + receipt.composition_id.replace("-", ""), "entity_address_unified", digest
        ),
    )
    monkeypatch.setattr(custom, "_require_transaction", AsyncMock())
    monkeypatch.setattr(
        custom,
        "_approved_identity",
        AsyncMock(
            return_value={
                "fingerprint": receipt.approved_records_sha256,
                "pairs": receipt.custom_pair_count,
            }
        ),
    )
    monkeypatch.setattr(custom, "verify_approved_site_sources", AsyncMock(return_value=(_receipt(),)))
    monkeypatch.setattr(custom, "_source_alias_layout", AsyncMock(return_value=native_aliases))
    native = AsyncMock()
    monkeypatch.setattr(custom, "_artifact_identity", native)
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="source alias layout differs"):
        await custom._verify_custom_address_source(object(), receipt)
    native.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("aliases,invalid", [(False, False), (True, False), (True, True)])
async def test_native_layout_closes_unique_after_all_carries(aliases, invalid):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"source_aliases": aliases, "invalid": invalid}),
        execute=AsyncMock(),
    )
    if invalid:
        with pytest.raises(custom.NetworkCustomAddressSourceError, match="identities collide"):
            await custom._source_alias_layout(connection, "synthetic_mixed", writing=True)
        connection.execute.assert_not_awaited()
    else:
        assert await custom._source_alias_layout(connection, "synthetic_mixed", writing=True) is aliases
        if aliases:
            connection.execute.assert_not_awaited()
        else:
            connection.execute.assert_awaited_once_with(
                'ALTER TABLE "synthetic_mixed".provider_location_binding ADD UNIQUE(location_key)'
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_aliases", [False, True])
@pytest.mark.parametrize("unique", [False, True])
async def test_sealed_binding_constraint_layout_remains_exact(source_aliases, unique):
    columns = [
        dict(zip(("attname", "type", "atttypmod", "attnotnull"), values, strict=True))
        for values in (
            ("provider_system", "text", -1, True),
            ("provider_id", "text", -1, True),
            ("location_id", "uuid", -1, True),
            ("location_key", "character varying", 68, True),
            ("entity_type", "text", -1, True),
            ("entity_id", "text", -1, True),
        )
    ]
    constraints = [{"definition": "PRIMARY KEY (provider_system, provider_id, location_id)"}]
    if unique:
        constraints.append({"definition": "UNIQUE (location_key)"})
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[columns, constraints]))
    if unique is not source_aliases:
        await custom._binding_shape(connection, 1, source_aliases=source_aliases)
    else:
        with pytest.raises(custom.NetworkCustomAddressSourceError, match="binding shape differs"):
            await custom._binding_shape(connection, 1, source_aliases=source_aliases)
