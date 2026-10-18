# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound exact reviewed office carry, recipe replay and conflicting full rows."""

import asyncio
import hashlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_custom_address_source as custom
from process import registry_ptg_address_carry as carry
from process import registry_source_recipe_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_approved_site_source import copy_approved_site_rows
from process.registry_retained_site_adoption import (
    RetainedSiteAdoption,
    RetainedSiteAdoptionError,
    RetainedSiteAdoptionReceipt,
)
from tests.test_registry_ptg_office_membership_recipe import _recipe


def _receipt():
    adoption = RetainedSiteAdoption("npi", "1234567893", str(UUID(int=8)), "e" * 64, "npi", "1234567893", "f" * 64)
    return RetainedSiteAdoptionReceipt(
        3, str(UUID(int=9)), "retained_fixture", 1, (("source", "edition"),), 4, "d" * 64, 11, 12, 13, 14, (adoption,)
    )


@pytest.fixture
def copy_connection():
    columns = [
        dict(
            position=position,
            attname=name,
            atttypid=type_oid,
            atttypmod=-1,
            attcollation=0,
            attndims=int(type_oid == 1007),
            attnotnull=True,
            attgenerated="",
            attidentity="",
        )
        for position in (1, 2)
        for name, type_oid in (("location_key", 25), ("checksum", 20), ("canonical_network_ids", 1007))
    ]
    return SimpleNamespace(
        fetchval=AsyncMock(return_value=False), fetch=AsyncMock(return_value=columns), execute=AsyncMock()
    )


def _artifact():
    base = PinnedAddressSource("base_fixture", "entity_address_unified", "edition")
    return custom.CustomAddressSourceReceipt(
        str(UUID(int=10)),
        4,
        "control_fixture",
        base,
        None,
        (1, 2, 3, "a" * 64),
        None,
        "owner_fixture",
        ("reader_fixture",),
        11,
        12,
        13,
        14,
        15,
        0,
        "b" * 64,
        "c" * 64,
        base,
    )


def test_empty_carry_preserves_exact_v2_encoding_and_generation():
    receipt = _artifact()
    old = asdict(receipt)
    old.pop("ptg_carry_sha256")
    old.pop("aca_carry_sha256")
    expected_by_field = {"component": "network_custom_address_source", "revision": 2, **old}
    assert receipt.as_dict() == expected_by_field
    assert custom._from_document(json.dumps(expected_by_field)) == receipt
    expected_by_field.pop("generation_sha256")
    expected_by_field.pop("address_source")
    encoded = json.dumps(expected_by_field, sort_keys=True, separators=(",", ":")).encode()
    assert custom._generation(receipt) == hashlib.sha256(encoded).hexdigest()


def test_v3_binds_carry_and_roundtrips_without_relabeling_v2():
    original = _artifact()
    changed = replace(original, ptg_carry_sha256="f" * 64)
    assert changed.as_dict()["revision"] == 3
    assert custom._from_document(json.dumps(changed.as_dict())) == changed
    assert custom._generation(original) != custom._generation(changed)
    assert custom._recipe_identity(original) != custom._recipe_identity(changed)


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": True},
        {"revision": 4},
        {"revision": 3},
        {"ptg_carry_sha256": "f" * 64},
        {"revision": 3, "ptg_carry_sha256": None},
        {"revision": 3, "ptg_carry_sha256": "F" * 64},
        {"unknown": True},
    ],
)
def test_closed_receipt_refuses_revision_digest_and_field_smuggling(changes):
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(json.dumps({**_artifact().as_dict(), **changes}))


def test_receipt_refuses_duplicate_fields():
    encoded = json.dumps(_artifact().as_dict())
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(encoded[:-1] + ',"revision":2}')


def _pages(monkeypatch, *, receipt=None, first=None):
    recipe = _recipe()
    receipt = receipt or _receipt()
    batch = first or SimpleNamespace(
        source=recipe.source_pin, source_rows=1, membership_rows=1, office_records=receipt.records
    )
    terminal = SimpleNamespace(source=recipe.source_pin, source_rows=0, membership_rows=0, office_records=())
    verified = SimpleNamespace(serving=object())
    custody = AsyncMock(return_value=verified)
    reader = AsyncMock(side_effect=[(batch, 1, 1000), (terminal, None, 1000)])
    resolver = AsyncMock(return_value=receipt)
    monkeypatch.setattr(composition, "_require_recipe_custody", custody)
    monkeypatch.setattr(composition, "_read_recipe_page", reader)
    monkeypatch.setattr(carry, "resolve_retained_site_adoptions", resolver)
    return recipe, custody, reader, resolver


@pytest.mark.asyncio
async def test_exact_recipe_custody_shared_budget_and_selected_rows(monkeypatch):
    recipe, custody, reader, resolver = _pages(monkeypatch)
    connection, approved, authority = object(), object(), object()
    result = await carry.prepare_ptg_address_carry(connection, (recipe,), approved, "control_fixture", authority)
    assert (result.pages, result.rows) == (1, 1)
    assert not hasattr(result, "receipts")
    assert len(result.sha256) == 64
    custody.assert_awaited_once_with(connection, recipe, approved, "control_fixture", authority)
    assert reader.await_count == 2
    assert resolver.await_args.kwargs == {"control_schema": "control_fixture"}
    assert json.loads(resolver.await_args.args[2]) == [
        {
            name: getattr(_receipt().records[0], name)
            for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
        }
    ]


@pytest.mark.asyncio
async def test_absent_recipe_has_no_custody_or_native_row_expansion(monkeypatch):
    _, custody, reader, resolver = _pages(monkeypatch)
    assert (
        await carry.prepare_ptg_address_carry(object(), (), object(), "control_fixture", None)
        == carry.RegistryPTGAddressCarry()
    )
    custody.assert_not_awaited()
    reader.assert_not_awaited()
    resolver.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["generation", "record", "count", "scope", "empty", "cursor", "cancel"])
async def test_page_proofs_fail_closed(monkeypatch, fault):
    recipe, _, reader, resolver = _pages(monkeypatch)
    if fault == "generation":
        resolver.return_value = replace(_receipt(), generation_id=9)
    elif fault == "record":
        resolver.return_value = replace(_receipt(), records=())
    elif fault in {"count", "scope", "empty"}:
        first = SimpleNamespace(
            source=recipe.source_pin, source_rows=1, membership_rows=1, office_records=_receipt().records
        )
        changes_by_fault = {
            "count": {"membership_rows": 0},
            "scope": {"source": replace(recipe.source_pin, network_id=72)},
            "empty": {"source_rows": 0},
        }
        first.__dict__.update(changes_by_fault[fault])
        reader.side_effect = [(first, 1, 1000)]
    elif fault == "cursor":
        first = SimpleNamespace(
            source=recipe.source_pin, source_rows=1, membership_rows=1, office_records=_receipt().records
        )
        reader.side_effect = [(first, None, 1000)]
    else:
        reader.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError if fault == "cancel" else ValueError):
        await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())


@pytest.mark.asyncio
async def test_manifest_and_row_hash_are_bound_to_carry_identity(monkeypatch):
    recipe, _, _, _ = _pages(monkeypatch)
    first = await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())
    altered = replace(_receipt(), manifest_sha256="a" * 64)
    _pages(monkeypatch, receipt=altered)
    second = await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())
    _pages(
        monkeypatch, receipt=replace(_receipt(), records=(replace(_receipt().records[0], address_row_sha256="a" * 64),))
    )
    third = await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())
    assert len({first.sha256, second.sha256, third.sha256}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, None])
async def test_full_row_or_binding_conflict_refused_before_copy(invalid):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=invalid), execute=AsyncMock())
    with pytest.raises(RetainedSiteAdoptionError):
        await copy_approved_site_rows(connection, (_receipt(),), "target_fixture", exact_rows=True)
    connection.execute.assert_not_awaited()
    sql = connection.fetchval.await_args.args[0]
    assert "to_jsonb(address)" in sql
    assert "binding.location_key=site.location_key" in sql
    assert "IS DISTINCT FROM" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("exact", [False, True])
async def test_set_copy_allows_identical_overlap_and_preserves_legacy_path(exact, copy_connection):
    connection = copy_connection
    await copy_approved_site_rows(connection, (_receipt(),), "target_fixture", exact_rows=exact)
    assert connection.execute.await_count == 2
    assert connection.fetchval.await_count == int(exact)
    assert connection.fetch.await_count == 1
    address_sql = connection.execute.await_args_list[0].args[0]
    assert '("location_key","checksum","canonical_network_ids")' in address_sql
    assert 'SELECT address."location_key",address."checksum",address."canonical_network_ids"' in address_sql
    assert "address.*" not in address_sql
    assert ("ON CONFLICT DO NOTHING" in connection.execute.await_args.args[0]) is exact


@pytest.mark.asyncio
async def test_streaming_copy_and_replay_share_identity_without_retaining_pages(monkeypatch):
    from process import network_approved_site_source as sites

    recipe, _, reader, _ = _pages(monkeypatch)
    first = SimpleNamespace(
        source=recipe.source_pin, source_rows=1, membership_rows=1, office_records=_receipt().records
    )
    terminal = SimpleNamespace(source=recipe.source_pin, source_rows=0, membership_rows=0, office_records=())
    reader.side_effect = [(first, ordinal, 1000) for ordinal in range(1, 101)] + [(terminal, None, 1000)]
    copy = AsyncMock()
    monkeypatch.setattr(sites, "copy_approved_site_rows", copy)
    result = await carry.prepare_ptg_address_carry(
        object(), (recipe,), object(), "control_fixture", object(), "target_fixture"
    )
    assert (result.pages, result.rows, copy.await_count) == (100, 100, 100)
    assert all(
        call.args[1] == (_receipt(),) and call.kwargs == {"exact_rows": True, "reproject_network_ids": True}
        for call in copy.await_args_list
    )
    _pages(monkeypatch)
    written = await carry.prepare_ptg_address_carry(
        object(), (recipe,), object(), "control_fixture", object(), "target_fixture"
    )
    _pages(monkeypatch)
    replayed = await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())
    assert written == replayed


@pytest.mark.asyncio
async def test_existing_artifact_replay_rechecks_carry_before_returning(monkeypatch):
    original = replace(_artifact(), ptg_carry_sha256="a" * 64)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(original.as_dict())))
    monkeypatch.setattr(
        custom, "_ptg_address_carry", AsyncMock(return_value=carry.RegistryPTGAddressCarry("a" * 64, 1, 1))
    )
    verify = AsyncMock(return_value=original)
    monkeypatch.setattr(custom, "verify_custom_address_source", verify)
    assert (
        await custom._prepare_artifact(connection, _artifact(), "target_fixture", '"control_fixture"', ((), None))
        == original
    )
    custom._ptg_address_carry.return_value = carry.RegistryPTGAddressCarry("b" * 64, 1, 1)
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="recipe differs"):
        await custom._prepare_artifact(connection, _artifact(), "target_fixture", '"control_fixture"', ((), None))
    assert verify.await_count == 1


@pytest.mark.asyncio
async def test_composer_passes_full_resolved_recipes_and_same_office_authority(monkeypatch):
    from process import registry_candidate_composition as candidate

    artifact = _artifact()
    prepare = AsyncMock(return_value=artifact)
    monkeypatch.setattr(candidate, "prepare_recipe_address_source", prepare)
    source = SimpleNamespace(base_source=artifact.base_source, npi_source=None, introduced_source_recipes=())
    approved = SimpleNamespace(approved_revision=4, generation_id="a" * 64)
    writer_by_role = {
        "owner_role": "owner_fixture",
        "loader_roles": ("loader_fixture",),
        "reader_roles": ("reader_fixture",),
    }
    recipes, authority = (_recipe(),), object()
    actual, summary = await candidate._composition_artifact(
        object(), source, UUID(int=10), approved, writer_by_role, "control_fixture", authority, recipes
    )
    assert actual is artifact and summary is None
    assert prepare.await_args.args[3] == (recipes, authority)
    assert prepare.await_args.args[2].approved_revision == 4


@pytest.mark.parametrize("encoded", ["null", "[]", "3", "true", '"receipt"'])
def test_receipt_requires_closed_object(encoded):
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(encoded)


@pytest.mark.asyncio
async def test_canonical_recipe_is_bound_even_for_empty_office_selection(monkeypatch):
    recipe, _, reader, _ = _pages(monkeypatch)
    terminal = SimpleNamespace(source=recipe.source_pin, source_rows=0, membership_rows=0, office_records=())
    reader.side_effect = [(terminal, None, 1000)]
    first = await carry.prepare_ptg_address_carry(object(), (recipe,), object(), "control_fixture", object())
    changed = replace(recipe, source_pin=replace(recipe.source_pin, approval_sha256="f" * 64))
    reader.side_effect = [(terminal, None, 1000)]
    second = await carry.prepare_ptg_address_carry(object(), (changed,), object(), "control_fixture", object())
    assert first.sha256 != second.sha256
    assert first.sha256 is not None and first.pages == first.rows == 0


@pytest.mark.asyncio
async def test_network_array_reprojection_keeps_full_retained_proof_and_immutable_target_comparison(copy_connection):
    connection = copy_connection
    await copy_approved_site_rows(
        connection, (_receipt(),), "target_fixture", exact_rows=True, reproject_network_ids=True
    )
    sql = connection.fetchval.await_args.args[0]
    assert 'LEFT JOIN "retained_fixture".entity_address_unified retained' in sql
    assert "retained.location_key IS NULL" in sql
    assert "to_jsonb(retained)::text" in sql
    assert "(to_jsonb(address)-'canonical_network_ids') IS DISTINCT FROM" in sql
    assert "(to_jsonb(retained)-'canonical_network_ids')" in sql
    assert "binding.location_key=site.location_key" in sql
    assert connection.execute.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("attname", "unknown"),
        ("atttypid", 1016),
        ("atttypmod", 4),
        ("attgenerated", "s"),
        ("attidentity", "d"),
        ("attnotnull", False),
    ],
)
async def test_copy_refuses_logical_column_mismatch_before_dml(copy_connection, field, value):
    copy_connection.fetch.return_value[-1][field] = value
    with pytest.raises(RetainedSiteAdoptionError, match="copy"):
        await copy_approved_site_rows(copy_connection, (_receipt(),), "target_fixture")
    copy_connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_accepts_like_array_dimensions_without_changing_native_type(copy_connection):
    copy_connection.fetch.return_value[-1]["attndims"] = 0
    await copy_approved_site_rows(copy_connection, (_receipt(),), "target_fixture")
    assert copy_connection.execute.await_count == 2


@pytest.mark.asyncio
async def test_network_array_reprojection_requires_exact_row_validation():
    connection = SimpleNamespace(fetchval=AsyncMock(), execute=AsyncMock())
    with pytest.raises(RetainedSiteAdoptionError, match="requires exact retained rows"):
        await copy_approved_site_rows(connection, (_receipt(),), "target_fixture", reproject_network_ids=True)
    connection.fetchval.assert_not_awaited()
    connection.execute.assert_not_awaited()
