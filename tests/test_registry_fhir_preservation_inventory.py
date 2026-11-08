# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure preservation protocol checks; synthetic coordinates issue no native Source or custody authority."""

import asyncio
import hashlib
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_fhir_serving_preservation as preservation
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_fhir_membership_source import with_fhir_successor
from process.network_serving_read import PinnedNetworkServingManifest, _attach_manifest_custody
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_recipe_store import RegistrySourceRecipeError
from tests.test_network_fhir_retained_capture import pin
from tests.test_registry_fhir_serving_preservation import receipt
from tests.test_registry_generic_accepted_capture import semantic_capture
from tests.test_registry_generic_fhir_facts import _recipe
from tests.test_registry_source_recipe_store_postgres import _example


def _receipt(recipes, *, inventory):
    legacy = receipt()
    counters_by_name = {
        name: legacy[name] for name in preservation._COUNTS if name not in {"approved_revision", "expected_head"}
    }
    counters_by_name.update(
        {
            name: legacy[name]
            for name in ("missing_networks_sha256", "membership_sha256", "bindings_sha256", "addresses_sha256")
        }
    )
    predecessor = SimpleNamespace(generation_id=2, candidate_id=str(UUID(int=2)), manifest_sha256="a" * 64)
    return preservation._carry_receipt(
        predecessor, recipes, recipes, (UUID(int=4), 2, 4, None), counters_by_name, "d" * 64, inventory=inventory
    )


@pytest.mark.parametrize("count", [0, 1, 101, 1000])
def test_explicit_inventory_commitment_uses_existing_closed_recipe_bounds(count):
    recipes = tuple(_example(index) for index in range(count))
    document = _receipt(recipes, inventory=True)
    root = asdict(SourceInventoryPreparation(recipes).recipe_root)
    assert document["revision"] == 2
    assert document["previous_recipe_root"] == document["current_recipe_root"] == root
    assert document["current_recipes_sha256"] == root["recipes_sha256"]
    assert preservation.validate_fhir_serving_preservation(document) == document
    if count > 100:
        with pytest.raises(RegistrySourceRecipeError):
            _receipt(recipes, inventory=False)


def test_inventory_commitment_refuses_entry_overflow_without_changing_legacy_cap():
    recipes = tuple(_example(index) for index in range(1001))
    with pytest.raises(ValueError):
        _receipt(recipes, inventory=True)
    with pytest.raises(RegistrySourceRecipeError):
        preservation._recipe_commitment(recipes[:101], False)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("revision", True),
        ("revision", 3),
        ("previous_recipe_root", None),
        ("source_authority", True),
        ("current_recipe_root.entry_count", 1001),
        ("current_recipe_root.recipes_sha256", "0" * 64),
        ("current_recipe_root.contribution_count", 1),
        ("current_recipe_root.encoded_bytes", 64 * 1024 * 1024 + 1),
    ],
)
def test_inventory_receipt_is_explicit_closed_and_bound_to_recipe_roots(field, replacement):
    document = _receipt((_example(),), inventory=True)
    if "." in field:
        scope, name = field.split(".")
        document[scope][name] = replacement
    elif replacement is None:
        document.pop(field)
    else:
        document[field] = replacement
    with pytest.raises(preservation.FHIRServingPreservationError, match="receipt_invalid"):
        preservation.validate_fhir_serving_preservation(document)


def test_legacy_receipt_hash_domain_and_bytes_are_unchanged():
    document = receipt()
    assert preservation.validate_fhir_serving_preservation(document) == document
    assert (
        preservation.fhir_serving_preservation_sha256(document)
        == hashlib.sha256(preservation._canonical(document).encode()).hexdigest()
    )
    legacy = _receipt((_example(),), inventory=False)
    assert legacy["revision"] == 1 and not preservation._ROOT_FIELDS.intersection(legacy)


@pytest.mark.parametrize("supplied", [None, "0" * 64, True])
def test_exact_predecessor_seal_cannot_be_replaced_by_manifest_dto_equality(supplied):
    actual = SimpleNamespace(_custody_seal_sha256="a" * 64)
    with pytest.raises(preservation.FHIRServingPreservationError, match="predecessor_changed"):
        preservation._require_predecessor_seal(actual, SimpleNamespace(_custody_seal_sha256=supplied))
    with pytest.raises(preservation.FHIRServingPreservationError):
        preservation._require_predecessor_seal(SimpleNamespace())
    preservation._require_predecessor_seal(actual, actual)


def _legacy_predecessor():
    manifest = PinnedNetworkServingManifest(
        2, str(UUID(int=2)), "network_candidate_" + UUID(int=2).hex, 1, {}, 1, "a" * 64, 31
    )
    closure_by_field = {"relation_oids": {"entity_address_unified": 31, "network_member_effective": 32}}
    _attach_manifest_custody(manifest, None, (None, None, None, closure_by_field), False)
    return manifest


@pytest.mark.parametrize(
    "damage", ["missing", "address_oid", "membership_oid", "schema", "control", "compact", "null_seal"]
)
def test_legacy_predecessor_requires_exact_native_relation_pins(damage):
    actual, supplied = _legacy_predecessor(), _legacy_predecessor()
    preservation._require_predecessor_seal(actual, supplied)
    if damage == "missing":
        supplied = replace(supplied)
    elif damage in {"address_oid", "membership_oid", "schema"}:
        pins = list(supplied.candidate_relation_pins)
        index = 1 if damage == "membership_oid" else 0
        pins[index] = replace(
            pins[index],
            **({"schema_name": "other_schema"} if damage == "schema" else {"table_oid": 99}),
        )
        object.__setattr__(supplied, "_candidate_relation_pins", tuple(pins))
    else:
        attribute = {"control": "_custody_control", "compact": "_compact_layout", "null_seal": "_custody_seal_sha256"}[
            damage
        ]
        object.__setattr__(supplied, attribute, None if damage == "null_seal" else object())
    with pytest.raises(preservation.FHIRServingPreservationError, match="predecessor_changed"):
        preservation._require_predecessor_seal(actual, supplied)


@pytest.mark.parametrize("selection", ["no_fhir", "same_fhir", "removed_fhir"])
async def test_legacy_publication_derives_obligation_after_exact_native_pin(monkeypatch, selection):
    actual = _legacy_predecessor()
    previous = () if selection == "no_fhir" else (_example(),)
    current = () if selection == "removed_fhir" else previous
    if previous:
        actual.source_generations[preservation.RECIPE_DIGEST_KEY] = "b" * 64
    resolve = AsyncMock(return_value=actual)
    recipes = AsyncMock(return_value=previous)
    monkeypatch.setattr(preservation, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", recipes)
    monkeypatch.setattr(preservation, "_candidate_recipes", AsyncMock(return_value=(current, None)))
    candidate_by_field = {
        "expected_head": actual.generation_id,
        "approved_custom_revision": 3,
        "source_generations": {},
        "validation_json": {},
    }
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"validation_json": {}, "source_generations": {}}))
    if selection == "removed_fhir":
        with pytest.raises(preservation.FHIRServingPreservationError, match="proof_required"):
            await preservation.fhir_preservation_obligation(connection, candidate_by_field, control_schema="registry")
    else:
        assert (
            await preservation.fhir_preservation_obligation(connection, candidate_by_field, control_schema="registry")
            is None
        )
    resolve.assert_awaited_once_with(connection, generation_id=actual.generation_id, control_schema="registry")
    assert recipes.await_count == bool(previous)


async def test_legacy_preparation_rechecks_native_pins_before_unchanged_recipe_return(monkeypatch):
    from process import registry_publication_queue as queue

    actual, supplied = _legacy_predecessor(), _legacy_predecessor()
    actual.source_generations[preservation.RECIPE_DIGEST_KEY] = "b" * 64
    supplied.source_generations.update(actual.source_generations)
    recipes = (_example(),)
    resolve = AsyncMock(return_value=actual)
    rollback = AsyncMock(return_value=None)
    monkeypatch.setattr(preservation, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=recipes))
    monkeypatch.setattr(queue, "require_registry_source_rollback_composition", rollback)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"validation_json": {}, "source_generations": {}}))
    assert (
        await preservation._preservation_baseline(connection, supplied, recipes, UUID(int=4), 2, 3, "registry") is None
    )
    resolve.assert_awaited_once_with(connection, generation_id=supplied.generation_id, control_schema="registry")
    rollback.assert_awaited_once_with(connection, UUID(int=4), supplied, 3, 2, control_schema="registry")


async def test_coverage_reads_authenticated_physical_successor_dataset_with_original_slot(monkeypatch):
    original = replace(pin(semantic_capture()), as_of="2026-01-01")
    physical = replace(pin(semantic_capture("dataset-two", "2026-02-01")), as_of="2026-02-01")
    recipe = _recipe(with_fhir_successor(original, physical))
    custody = AsyncMock()
    bindings = AsyncMock()
    monkeypatch.setattr(preservation, "require_registry_source_recipe_custody", custody)
    monkeypatch.setattr(preservation, "require_retained_approved_network_source_bindings", bindings)
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=False))
    approved = object()
    await preservation._coverage(connection, (recipe,), approved, "registry", "current", "inventory", "coverage")
    custody.assert_awaited_once_with(connection, recipe, approved, "registry")
    assert bindings.await_args.args[2] == recipe.binding_coordinates
    inserted = connection.execute.await_args_list[-1]
    assert physical.read_schema_name in inserted.args[0] and inserted.args[-1] == physical.dataset_id
    assert inserted.args[-1] != original.dataset_id
    assert inserted.args[2] == preservation._canonical(preservation._slot(recipe))


async def test_compact_predecessor_uses_all_logical_addresses_and_native_artifact_arguments():
    artifact = CompactEdgeArtifact(str(UUID(int=1)), "network_candidate_" + UUID(int=1).hex, 13)
    layout = CompactNetworkLayout(
        CompactAddressRelation("previous", "network_slice_ref", 11),
        CompactAddressRelation("base", "retained_addresses", 12),
        (artifact,),
        CompactAddressRelation("previous", "entity_address_unified", 14),
    )
    oids_by_relation = {
        '"previous"."network_slice_ref"': 11,
        '"base"."retained_addresses"': 12,
        '"network_candidate_' + UUID(int=1).hex + '"."network_office_edge"': 13,
        '"previous"."entity_address_unified"': 14,
    }
    connection = SimpleNamespace(
        execute=AsyncMock(), fetchval=AsyncMock(side_effect=lambda _sql, relation: oids_by_relation[relation])
    )
    relation, arguments = await preservation._predecessor_addresses(connection, SimpleNamespace(compact_layout=layout))
    assert arguments == (str(UUID(int=1)),) and "$1" in relation
    assert '"base"."retained_addresses"' in relation and '"previous"."entity_address_unified"' in relation
    assert "OR (TRUE)" in relation and "canonical_network_ids" in relation
    assert connection.execute.await_count == len(oids_by_relation)
    membership, bindings, addresses = preservation._carry_queries(
        SimpleNamespace(schema_name="previous"), "missing", "raw", relation
    )
    assert "$1" not in membership and "$1" not in bindings and "$1" in addresses


async def test_address_digest_forwards_exact_compact_parameters():
    calls = []

    async def cursor(query, *arguments, **kwargs):
        calls.append((query, arguments, kwargs))
        yield 7, b"x" * 32

    count, digest = await preservation._ordered_digest(
        SimpleNamespace(cursor=cursor), "SELECT $1::text", (UUID(int=1),)
    )
    assert count == 1 and digest == hashlib.sha256((7).to_bytes(8, "big") + b"x" * 32).hexdigest()
    assert calls[0][1:] == ((UUID(int=1),), {"prefetch": 1000})


async def test_bounded_address_copy_reserves_compact_binds_before_page_keys(monkeypatch):
    from process import network_bootstrap_sources as bootstrap

    copier = AsyncMock()
    monkeypatch.setattr(bootstrap, "_copy_native_batch", copier)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=[{"row_count": 2, "last_key": "b" * 64, "native_bytes": 100}, {"row_count": 0}])
    )
    assert (
        await preservation._copy_carry_address_rows(
            connection, "candidate", "SELECT a.* FROM addresses($1) a", (UUID(int=1),)
        )
        == 2
    )
    assert copier.await_args.args[2] == (UUID(int=1), "b" * 64)
    assert "a.location_key<=$2" in copier.await_args.args[1]
    assert connection.fetchrow.await_args.args[1:] == (UUID(int=1), "b" * 64)
    assert "a.location_key>$2" in connection.fetchrow.await_args.args[0]


async def test_target_carry_parity_uses_both_source_and_target_native_arguments():
    proof = SimpleNamespace(
        address_query="SELECT * FROM old($1)",
        binding_query="SELECT * FROM bindings",
        address_arguments=(UUID(int=1),),
        receipt=receipt(),
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    assert await preservation.carry_fhir_preservation_addresses(
        connection, proof, "candidate", address_relation="(SELECT * FROM current($2))", address_arguments=(UUID(int=2),)
    ) == preservation.fhir_serving_preservation_sha256(proof.receipt)
    assert all(call.args[1:] == (UUID(int=1), UUID(int=2)) for call in connection.fetchval.await_args_list)


@pytest.mark.parametrize("failure", [None, ValueError("current inventory differs"), asyncio.CancelledError()])
async def test_inventory_wrapper_uses_one_independent_current_stage_and_cleans_phase(monkeypatch, failure):
    from process import network_source_contribution_custody as custody

    proof = SimpleNamespace(receipt={"revision": 2})
    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", AsyncMock(return_value=proof))
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", AsyncMock())
    entries = (object(),)
    monkeypatch.setattr(preservation, "_candidate_recipes", AsyncMock(return_value=((), entries)))
    independent = AsyncMock(side_effect=failure)
    monkeypatch.setattr(custody, "verify_independent_source_contributions", independent)
    legacy = AsyncMock(side_effect=AssertionError("Inventory must not use legacy reconstruction"))
    monkeypatch.setattr(preservation, "_verify_current_fhir_rows", legacy)
    cleanup = AsyncMock()
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", cleanup)
    operation = preservation.verify_fhir_preservation_rows(
        object(), object(), {}, control_schema="registry", inventory_entries=entries
    )
    if failure is None:
        assert await operation == proof.receipt
    else:
        with pytest.raises(type(failure)) as observed:
            await operation
        assert observed.value is failure
    assert independent.await_count == 1 and independent.await_args.kwargs["preservation"] is proof
    assert independent.await_args.kwargs["inventory_entries"] is entries
    legacy.assert_not_awaited()
    assert cleanup.await_args.kwargs == {"primary": failure}


def test_inventory_commitment_handles_recipe_bytes_beyond_the_legacy_vector_limit():
    from tests.test_registry_source_inventory_store_postgres import _capture_recipe

    recipes = []
    for index in range(100):
        recipe = _capture_recipe()
        alias = str(index) + "s" * 490
        original = replace(recipe.source_pin, retained_successor=None, alias_scope=alias)
        successor = replace(recipe.source_pin.retained_successor, alias_scope=alias)
        recipes.append(replace(recipe, source_pin=with_fhir_successor(original, successor)))
    with pytest.raises(RegistrySourceRecipeError):
        preservation._recipe_commitment(tuple(recipes), False)
    digest, root = preservation._recipe_commitment(tuple(recipes), True)
    assert root["entry_count"] == 100 and root["recipe_bytes"] > 524288
    assert root["recipes_sha256"] == digest


@pytest.mark.parametrize("changed", [False, True])
def test_v2_receipt_is_bound_to_current_registered_inventory_recipe_root(changed):
    from process.registry_source_inventory_preparation import prepare_source_inventory
    from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY
    from process.registry_source_recipe_store import RECIPE_DIGEST_KEY
    from tests.test_verified_inventory_v2 import rows

    recipe, descriptor, derivation = rows()
    reference, _entries = prepare_source_inventory((recipe,), (descriptor,), (derivation,))
    document = _receipt((recipe,), inventory=True)
    candidate_by_field = {
        "source_recipes_json": reference.as_dict(),
        "approved_custom_revision": document["approved_revision"],
        "validation_json": {preservation.SUMMARY_KEY: document},
        "source_generations": {
            RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
            preservation.SUMMARY_KEY: preservation.fhir_serving_preservation_sha256(document),
        },
    }
    if changed:
        document["current_recipe_root"]["recipes_sha256"] = document["current_recipes_sha256"] = "0" * 64
        candidate_by_field["source_generations"][preservation.SUMMARY_KEY] = (
            preservation.fhir_serving_preservation_sha256(document)
        )
        with pytest.raises(preservation.FHIRServingPreservationError, match="receipt_invalid"):
            preservation.verify_fhir_serving_preservation(candidate_by_field)
    else:
        assert preservation.verify_fhir_serving_preservation(candidate_by_field) == document


async def test_cleanup_drains_only_owned_phase_tables_and_preserves_primary_error():
    suffix = UUID(int=7).hex
    relation = 'pg_temp."fhir_preserve_recipes_' + suffix + '"'
    connection = SimpleNamespace(execute=AsyncMock())
    proof = SimpleNamespace(temporary_tables=(relation,))
    await preservation.cleanup_fhir_serving_preservation(connection, proof)
    connection.execute.assert_awaited_once_with("DROP TABLE IF EXISTS " + relation)
    connection.execute.reset_mock()
    with pytest.raises(preservation.FHIRServingPreservationError):
        await preservation.cleanup_fhir_serving_preservation(
            connection, SimpleNamespace(temporary_tables=('"outside".data',))
        )
    connection.execute.assert_not_awaited()
    primary = ValueError("original refusal")
    connection.execute.side_effect = RuntimeError("cleanup failed")
    await preservation.cleanup_fhir_serving_preservation(connection, proof, primary=primary)
    assert isinstance(primary._registry_preservation_cleanup_error, RuntimeError)
