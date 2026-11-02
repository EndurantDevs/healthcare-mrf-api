# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preparation regressions; mocks do not establish native Source or office custody."""

import asyncio
import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_initial_cms_office_bindings as legacy
from process import registry_verified_witness_offices as mapping
from process.ext.address_canon import current_canon_version
from process.network_address_projection import PinnedAddressSource
from tests.test_verified_inventory_v2 import context, rows


def _source(*, memberships=1):
    recipe, descriptor, _ = rows()
    descriptor.update(membership_rows=memberships, unresolved_rows=0, fact_rows=memberships + 1)
    return mapping.WitnessOfficeSource(recipe, descriptor, "synthetic_mapped_members")


def _member(**changes):
    return dict(
        network_id=7,
        provider_system="npi",
        provider_id="1000000491",
        location_id=str(UUID(int=1)),
        evidence_id="e" * 64,
        **changes,
    )


@pytest.mark.parametrize(
    "field,value",
    [("identity_version", True), ("identity_prefix", "wrong"), ("ruleset_version", 99), ("pub28_sha256", "f" * 64)],
)
async def test_canonicalizer_difference_refuses_before_address_read(monkeypatch, field, value):
    version_by_field = {**current_canon_version(), field: value}
    monkeypatch.setattr(
        mapping.importlib, "import_module", lambda name: SimpleNamespace(canon_version=lambda: version_by_field)
    )
    dependency = AsyncMock()
    monkeypatch.setattr(mapping, "_artifact_dependency_identity", dependency)
    with pytest.raises(legacy.InitialCMSOfficeBindingError, match="canonicalizer_differs"):
        await mapping._policy_context(object(), object(), (), object(), "registry")
    dependency.assert_not_awaited()


async def test_context_uses_actual_address_and_current_four_field_canonicalizer(monkeypatch):
    version = current_canon_version()
    monkeypatch.setattr(mapping.importlib, "import_module", lambda name: SimpleNamespace(canon_version=lambda: version))
    identity = (11, 12, 13, "d" * 64)
    dependency = AsyncMock(return_value=identity)
    monkeypatch.setattr(mapping, "_artifact_dependency_identity", dependency)
    address = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "a" * 64)
    approved = SimpleNamespace(approved_revision=7, generation_id="b" * 64)
    document, native = await mapping._policy_context(object(), address, ("reader",), approved, "registry")
    assert native == identity and document["canonicalizer_version"] == version
    assert document["address_identity"]["table_oid"] == 12 and document["origin_manifest_sha256"] is None
    assert dependency.await_count == 1


def test_shared_member_policy_keeps_duplicates_and_prunes_only_invalid_npi():
    member = _member()
    invalid_member_by_field = {**member, "provider_id": "1234567890"}
    native_member_by_field = {**member, "provider_system": "provider_directory", "provider_id": "source-provider"}
    selected, encoded = legacy._verified_member_input(
        json.dumps([member, member, invalid_member_by_field, native_member_by_field]).encode(), 4
    )
    assert selected == [member, member, native_member_by_field] and json.loads(encoded) == selected


@pytest.mark.parametrize("blob,count", [(b"[]", True), (b"[]", 5001), (b"[]", -1), ("[]", 0)])
def test_shared_member_policy_keeps_closed_native_bounds(blob, count):
    with pytest.raises(legacy.InitialCMSOfficeBindingError, match="input_invalid"):
        legacy._verified_member_input(blob, count)


async def test_mapping_savepoint_rolls_back_on_failure_without_replacing_error(monkeypatch):
    marker = RuntimeError("synthetic policy failure")
    transaction = SimpleNamespace(__aenter__=AsyncMock(), __aexit__=AsyncMock())

    class Savepoint:
        async def __aenter__(self):
            return await transaction.__aenter__()

        async def __aexit__(self, *args):
            return await transaction.__aexit__(*args)

    transaction.__aexit__.return_value = False
    connection = SimpleNamespace(transaction=Mock(return_value=Savepoint()))
    monkeypatch.setattr(mapping, "_require_transaction", AsyncMock())
    monkeypatch.setattr(mapping, "_prepare_witness_office_mapping", AsyncMock(side_effect=marker))
    with pytest.raises(RuntimeError) as failure:
        await mapping.prepare_witness_office_mapping_v2(
            connection, None, (), (), "mapped", approved=None, control_schema="registry", recipes_sha256="a" * 64
        )
    assert failure.value is marker
    assert transaction.__aexit__.await_args.args[1] is marker


async def test_real_member_copy_is_drained_before_cancellation_returns(monkeypatch):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def copy_members(*args, **kwargs):
        entered.set()
        await finish.wait()
        return "COPY 1"

    connection = SimpleNamespace(
        execute=AsyncMock(), copy_records_to_table=AsyncMock(side_effect=copy_members), fetchrow=AsyncMock()
    )
    member = _member()
    source = _source().recipe.source_pin
    operation = asyncio.create_task(
        legacy._stage_verified_member_page(
            connection, source, [member], json.dumps([member]).encode(), "p", "inputs", "members", {}
        )
    )
    await entered.wait()
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    connection.fetchrow.assert_not_awaited()


async def test_source_members_cannot_reach_location_lookup_without_genuine_read(monkeypatch):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=1), fetch=AsyncMock())
    verifier = AsyncMock(side_effect=ValueError("source admission differs"))
    monkeypatch.setattr(mapping, "_verify_retained_office_read", verifier)
    with pytest.raises(ValueError, match="source admission differs"):
        await mapping._stage_source_members(connection, _source(), object(), "inputs", "members", {})
    connection.fetch.assert_not_awaited()


async def test_derivation_counts_duplicate_members_instead_of_offices(monkeypatch):
    source = _source(memberships=3)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=2))
    matches = SimpleNamespace(page_proofs_by_key={"page": (1, "f" * 64)})
    raw = json.dumps([{"location_key": "a" * 64}])
    monkeypatch.setattr(mapping, "_matched_page", AsyncMock(return_value=raw))
    document = await mapping._derivation(connection, source, ("page",), matches, "members", context())
    assert (
        document["input_membership_rows"],
        document["accepted_membership_rows"],
        document["omitted_membership_rows"],
        document["office_rows"],
    ) == (3, 2, 1, 1)
    expected = hashlib.sha256(("page" + hashlib.sha256(raw.encode()).hexdigest() + "\n").encode()).hexdigest()
    assert document["correspondence_sha256"] == expected
    assert all(
        document["fact_artifact"][key] is None for key in ("fact_candidate_id", "fact_table_oid", "fact_seal_sha256")
    )


async def test_reused_derivation_preserves_exact_anchor_and_full_descriptor_commitment(monkeypatch):
    original = _source()
    descriptor = copy.deepcopy(original.descriptor)
    descriptor.update(fact_candidate_id=str(UUID(int=9)), fact_table_oid=71, fact_seal_sha256="f" * 64)
    source = replace(original, descriptor=descriptor)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=1))
    matches = SimpleNamespace(page_proofs_by_key={})
    result = await mapping._derivation(connection, source, (), matches, "members", context())
    assert result["fact_artifact"]["fact_candidate_id"] == str(UUID(int=9))
    assert result["fact_artifact"]["fact_table_oid"] == 71
    assert result["contribution_sha256"] == mapping._digest(descriptor)


async def test_all_recipes_share_one_global_pruning_pass(monkeypatch):
    from process import registry_source_recipe_composition as composition
    from tests.test_verified_inventory_v2 import rows as metadata_rows
    from tests.test_verified_inventory_v2 import source as source_pin

    witness_sources = []
    for index in (0, 1):
        recipe, descriptor, _ = metadata_rows(source_pin(index))
        witness_sources.append(mapping.WitnessOfficeSource(recipe, descriptor, "mapped_" + str(index)))
    connection = SimpleNamespace(execute=AsyncMock())
    current_context = context()
    address_identity_values = tuple(current_context["address_identity"].values())
    monkeypatch.setattr(mapping, "_require_transaction", AsyncMock())
    monkeypatch.setattr(mapping, "_policy_context", AsyncMock(return_value=(current_context, address_identity_values)))
    monkeypatch.setattr(composition, "_require_recipe_custody", AsyncMock(return_value=object()))

    async def stage(connection, witness_source, read, input_name, source_table, proofs):
        page_key = witness_source.descriptor["recipe_sha256"]
        proofs[page_key] = (1, "a" * 64)
        return (page_key,)

    monkeypatch.setattr(mapping, "_stage_source_members", AsyncMock(side_effect=stage))
    match_set = object()
    finish = AsyncMock(return_value=match_set)
    monkeypatch.setattr(mapping, "_bind_matches", finish)
    monkeypatch.setattr(mapping, "verified_retained_offices", AsyncMock())
    derive = AsyncMock(side_effect=[{"recipe": 0}, {"recipe": 1}])
    monkeypatch.setattr(mapping, "_derivation", derive)
    prepared_matches, derivations, _ = await mapping._prepare_witness_office_mapping(
        connection,
        None,
        (),
        witness_sources,
        "members",
        approved=SimpleNamespace(generation_id="a" * 64),
        control_schema="registry",
        recipes_sha256="b" * 64,
    )
    assert prepared_matches is match_set and derivations == ({"recipe": 0}, {"recipe": 1})
    finish.assert_awaited_once()
    assert set(finish.await_args.args[-1]) == {entry.descriptor["recipe_sha256"] for entry in witness_sources}
    assert derive.await_count == 2


async def test_shared_page_renders_the_retained_source_sql_before_location_read(monkeypatch):
    source = _source().recipe.source_pin
    connection = SimpleNamespace(
        execute=AsyncMock(),
        copy_records_to_table=AsyncMock(side_effect=["COPY 1", "COPY 0"]),
        fetchrow=AsyncMock(return_value={"offices_json": "[]"}),
    )
    monkeypatch.setattr(legacy, "_canonicalize", AsyncMock(return_value="[]"))
    member = _member()
    proofs_by_key = {}
    await legacy._stage_verified_member_page(
        connection, source, [member], json.dumps([member]).encode(), "page", "inputs", "members", proofs_by_key
    )
    assert f'"{source.read_schema_name}".provider_directory_dataset_resource' in connection.fetchrow.await_args.args[0]
    assert "{source}" not in connection.fetchrow.await_args.args[0]
    assert proofs_by_key == {"page": (0, None)}
    connection.execute.assert_awaited_once_with('CREATE INDEX ON pg_temp."members"(page_key)')
    connection.copy_records_to_table.side_effect = ["COPY 1", "COPY 0"]
    await legacy._stage_verified_member_page(
        connection, source, [member], json.dumps([member]).encode(), "next", "inputs", "members", proofs_by_key
    )
    assert connection.execute.await_count == 1


async def test_member_prune_materializes_only_page_identity_before_exact_uuid_comparison():
    connection = SimpleNamespace(execute=AsyncMock())
    assert mapping._prune_unmatched_members is legacy._prune_unmatched_members
    await legacy._prune_unmatched_members(connection, "members", "matches", {"first": (0, None), "empty": (0, None)})
    cleanup, *pages = connection.execute.await_args_list
    assert "page_key IS NULL OR NOT(page_key=ANY($1::text[]))" in cleanup.args[0]
    assert cleanup.args[1] == ["first", "empty"]
    assert [page.args[1] for page in pages] == ["first", "empty"]
    statement = pages[0].args[0]
    projection, deletion = statement.split(") DELETE FROM", 1)
    assert projection.startswith("WITH matched_keys AS MATERIALIZED (")
    assert "office->>'provider_system' AS provider_system" in projection
    assert "office->>'provider_id' AS provider_id" in projection
    assert "office->>'location_id' AS location_id" in projection
    assert 'FROM pg_temp."matches" WHERE page_key=$1' in projection
    assert 'pg_temp."members" member WHERE member.page_key=$1 AND NOT EXISTS' in deletion
    assert "(matched.provider_system,matched.provider_id,matched.location_id::uuid)" in deletion
    assert "=(member.provider_system,member.provider_id,member.location_id)" in deletion
    assert "office" not in deletion and "DISTINCT" not in statement


async def test_global_pruning_freezes_all_identities_before_any_page_delete():
    connection = SimpleNamespace(execute=AsyncMock(), transaction=Mock(return_value=AsyncMock()))
    await legacy._prune_verified_offices(connection, "matches", True, {"first": None, "second": None})
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert statements[0].startswith("CREATE TEMP TABLE")
    assert all(statement.startswith("CREATE INDEX") for statement in statements[1:4])
    assert statements[4].startswith("INSERT INTO")
    assert all(statement.startswith('DELETE FROM pg_temp."matches"') for statement in statements[5:7])
    assert statements[7].startswith("DROP TABLE")
    assert not any(statement.startswith("UPDATE") for statement in statements)
    assert 'provider_identity text COLLATE "C"' in statements[0]
    assert "jsonb_build_array(provider_system,provider_id)::text" in statements[4]
    assert "ORDER BY provider_identity LIMIT 1" in statements[5]
    assert "ORDER BY provider_identity DESC LIMIT 1" in statements[5]
    assert "ORDER BY location_key LIMIT 1" in statements[5]
    assert "ORDER BY location_key DESC LIMIT 1" in statements[5]
    assert "(other.provider_system,other.provider_id,other.location_id)=" in statements[5]
    assert all("ctid=ANY(ARRAY(" in statement for statement in statements[5:7])
