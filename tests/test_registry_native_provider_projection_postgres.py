# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native capture, complete inventory and provider projection stay separate from offices."""

import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process import network_provider_only_projection as projection
from process.network_address_projection import PinnedAddressSource
from process.network_membership_copy import MembershipCopyTarget
from process.registry_effective_network_catalog import effective_catalog_sql
from process.registry_native_provider_office_derivation import (
    native_provider_office_derivation,
    validate_native_provider_office_derivation,
)
from process.registry_source_inventory_contract import RegistrySourceInventoryError
from process.registry_source_inventory_preparation import prepare_source_inventory
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, encode_source_inventory_reference
from process.registry_source_recipe_composition import SourceRecipeReadContext, stage_registry_source_recipes
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY
from process.registry_typed_source_metadata import typed_office_summary
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database
from tests.test_registry_native_provider_source_postgres import native_provider_case as native_provider_case
from tests.test_registry_typed_source_recipe import _metadata as typed_metadata
from tests.test_registry_typed_source_recipe import _recipe as typed_recipe


async def _candidate(case, reference):
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    case.fixture.schemas.append(copy_target.schema_name)
    namespace = '"' + case.fixture.control + '"'
    await case.writer.execute(f'''CREATE SCHEMA "{copy_target.schema_name}";
      CREATE TABLE {namespace}.network_membership_candidate(dataset_id uuid,schema_id uuid,producer_id uuid,
        candidate_id uuid PRIMARY KEY,schema_name text,state text,approved_custom_revision bigint,source_generations jsonb,
        source_recipes_json jsonb,validation_json jsonb DEFAULT '{{}}')''')
    generations_by_source = {
        "custom_membership": case.approved.generation_id,
        "fhir_catalog": case.catalog_scope.metadata_generation,
        INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
        RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
    }
    await case.writer.execute(
        f"INSERT INTO {namespace}.network_membership_candidate VALUES($1,$2,$3,$4,$5,'open',$6,$7::jsonb,$8::jsonb,'{{}}')",
        *(UUID(getattr(copy_target, key)) for key in ("dataset_id", "schema_id", "producer_id", "candidate_id")),
        copy_target.schema_name,
        case.approved.approved_revision,
        json.dumps(generations_by_source),
        encode_source_inventory_reference(reference),
    )
    return copy_target, generations_by_source


async def _stage(case):
    context = SourceRecipeReadContext(source_witness_runtime=SimpleNamespace(catalog_bindings=case.catalog_scope))
    return await stage_registry_source_recipes(
        case.writer,
        (case.recipe,),
        case.approved,
        request_id=uuid4(),
        read_context=context,
        control_schema=case.fixture.control,
        inventory=True,
    )


async def _provider_page(case, copy_target, generations_by_source):
    manifest = SimpleNamespace(
        source_generations=generations_by_source, approved_custom_revision=case.approved.approved_revision
    )
    catalog_records = await effective_catalog_sql(case.writer, manifest, case.fixture.control)
    statement = projection.provider_only_page_sql(
        copy_target.schema_name, control_schema=case.fixture.control, catalog_records=catalog_records
    )
    network = await case.writer.fetchval(
        'SELECT network_id FROM "' + case.catalog.schema_name + '".registry_fhir_catalog_network'
    )
    page = await case.writer.fetchrow(statement, case.approved.approved_revision, [network], 25, 0, 1000, 1048576, 9)
    assert page["valid"] is True and page["bounded"] is True
    return [entry["provider_id"] for entry in json.loads(page["result"])["providers"]]


@pytest.mark.asyncio
async def test_mixed_typed_and_native_inventory_keeps_native_office_counters_zero(native_provider_case):
    case = native_provider_case
    typed = typed_recipe()
    contribution, derivation = typed_metadata(typed)
    native = native_provider_office_derivation(case.recipe)
    reference, entries = prepare_source_inventory((typed, case.recipe), (contribution,), (derivation, native))
    assert reference.root.entry_count == reference.root.derivation_count == 2 and reference.root.contribution_count == 1
    summary = typed_office_summary(reference.root, sorted((derivation, native), key=lambda item: item["recipe_sha256"]))
    assert summary["typed_source_count"] == 1 and summary["source_count"] == 2
    assert summary["counters"] == {"matched_offices": 1, "missing_offices": 0}
    assert summary["legacy"] == [
        {
            "version": 4,
            "source_count": 1,
            "counters": {"office_rows": 0, "page_count": 0, "matched_offices": 0, "missing_offices": 0},
        }
    ]
    assert all(entry.office_derivation_json is not None for entry in entries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"office_rows": 1},
        {"matched_offices": 1},
        {"address_source": {}},
        {"address_identity": {}},
        {"capture_sha256": "f" * 64},
    ],
)
async def test_native_zero_office_contract_refuses_inferred_or_mismatched_data(native_provider_case, change):
    case = native_provider_case
    document = native_provider_office_derivation(case.recipe) | change
    if "capture_sha256" in change:
        with pytest.raises(RegistrySourceInventoryError):
            prepare_source_inventory((case.recipe,), (), (document,))
    else:
        with pytest.raises(ValueError, match="native_provider_offices"):
            validate_native_provider_office_derivation(document)


@pytest.mark.asyncio
async def test_native_only_dispatch_needs_no_typed_office_capability(native_provider_case):
    case = native_provider_case
    context = SourceRecipeReadContext(
        verified_office_inputs=(PinnedAddressSource("absent_addresses", "entity_address_unified", "e" * 64), ()),
        source_witness_runtime=SimpleNamespace(catalog_bindings=case.catalog_scope),
    )
    async with case.writer.transaction(isolation="repeatable_read"):
        stage = await stage_registry_source_recipes(
            case.writer,
            (case.recipe,),
            case.approved,
            request_id=uuid4(),
            control_schema=case.fixture.control,
            read_context=context,
            inventory=True,
            contribution_revision=4,
        )
        assert stage.contribution_revision == 2 and stage.office_matches is None and stage.membership_rows == 0
        reference, _entries = prepare_source_inventory(
            (case.recipe,), stage.contribution_receipts, stage.office_derivations
        )
        assert reference.root.derivation_count == 1 and reference.root.contribution_count == 0
