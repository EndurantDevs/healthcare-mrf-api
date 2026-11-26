# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current Source metadata, native family definitions and cohort identity stay distinct."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import date, timedelta
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from asyncpg import UndefinedTableError

from process import registry_fhir_catalog_sync as catalog_sync
from process import registry_imported_catalog_preparation as catalog_preparation
from process import registry_imported_membership_refresh as refresh
from process import registry_imported_source_inventory as inventory
from process import registry_native_network_catalog as native
from process.registry_fhir_catalog_observation import catalog_observation_descriptor_digest
from process.registry_fhir_catalog_validation import metadata_generation, snapshot_inputs
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from tests.test_delta_dental_directory_normalization import delta_positive_descriptor as delta_positive_descriptor
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor
from tests.test_registry_fhir_catalog_validation import snapshot
from tests.test_registry_typed_source_recipe import _recipe


def native_aca_source():
    return {
        "contract": native.CONTRACT,
        "source_system": "aca",
        "source_id": "aca:10001:2027",
        "schema_name": "source_plans",
        "edition_id": "edition-one",
        "policy": inventory.NATIVE_POLICY,
        "definitions": [
            {
                "issuer_id": "10001",
                "plan_year": 2027,
                "network_tier": "Preferred",
                "checksum_network": 23,
                "plan_id": "10001NY0000001",
                "state": "NY",
                "marketing_name": "Example plan",
                "issuer_name": "Example insurer",
            }
        ],
    }


def native_ptg_source():
    return {
        "contract": native.CONTRACT,
        "source_system": "ptg",
        "source_id": "source-graph",
        "schema_name": "source_rates",
        "edition_id": "snapshot-one",
        "policy": inventory.NATIVE_POLICY,
        "graph": {
            "snapshot_key": 31,
            "import_run_id": "import-one",
            "source_set": {
                "contract": "sorted_raw_container_sha256_bytes_v1",
                "source_count": 1,
                "raw_container_sha256_digest": "e" * 64,
            },
            "provider_graph": {"npi_count": 4, "owner_count": 5, "provider_group_count": 2},
        },
        "definitions": [
            {
                "source_key": "source-graph",
                "network_names": [],
                "plans": [{"plan_id": "plan-one", "plan_market_type": "group"}],
            }
        ],
    }


def inventory_config(descriptor):
    return inventory.parse_imported_source_inventory_config(
        {
            "aca_source_schema": "source_plans",
            "ptg_source_schema": "source_rates",
            "fhir_source_profiles": [descriptor["native_read_profile"]],
        }
    )


@pytest.fixture
def inventory_case():
    descriptor = _recipe().source_pin.catalog_descriptor
    configured = inventory_config(descriptor)
    aca = native_aca_source()
    ptg = native_ptg_source()

    async def fetch(sql, *parameters):
        if "plan_networktier" in sql:
            return deepcopy(aca["definitions"])
        if "snapshot.manifest" in sql:
            graph = ptg["graph"]
            return [
                {
                    "source_key": ptg["source_id"],
                    "snapshot_id": ptg["edition_id"],
                    "snapshot_key": graph["snapshot_key"],
                    "import_run_id": graph["import_run_id"],
                    "serving_index": graph,
                    "allowed_amount_index": None,
                },
                {
                    "source_key": "allowed-only",
                    "snapshot_id": "allowed-snapshot",
                    "snapshot_key": None,
                    "import_run_id": "allowed-import",
                    "serving_index": None,
                    "allowed_amount_index": {"data_domain": "allowed_amounts"},
                },
            ]
        if "current_plan_source" in sql:
            return [
                {"source_key": ptg["source_id"], "snapshot_id": ptg["edition_id"], **ptg["definitions"][0]["plans"][0]}
            ]
        raise AssertionError(sql)

    async def pages():
        yield (SimpleNamespace(status="accepted", descriptor=descriptor),)

    @asynccontextmanager
    async def factory(*, source_profile):
        yield {
            "native_read_profile": source_profile.document(),
            "inventory": pages,
            "replay": AsyncMock(),
            "legacy_bindings": AsyncMock(),
        }

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(side_effect=fetch),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(return_value={"import_id": "edition-one", "state": "complete"}),
    )
    return SimpleNamespace(
        connection=connection, config=configured, factory=factory, descriptor=descriptor, aca=aca, ptg=ptg
    )


async def observe(case):
    return await inventory.observe_imported_source_inventory(
        case.connection, config=case.config, catalog_export=case.factory
    )


def edition(dataset, endpoint, uri, day, resource_count):
    descriptor = observed_descriptor(dataset=dataset)
    descriptor.update(endpoint_id=endpoint, source_uri=uri)
    descriptor["admission"]["publication"].update(
        published_at=f"2026-10-{day:02}T12:00:00+00:00", claimed_resource_count=resource_count
    )
    digest = catalog_observation_descriptor_digest(descriptor)
    descriptor.update(
        accepted_receipt_id=f"registry-fhir-catalog-observation-v1:{digest}", accepted_receipt_sha256=digest
    )
    return descriptor


def test_latest_complete_edition_wins_across_registrations_without_merging_distinct_paths():
    old = edition("old", "endpoint-old", "https://Directory.example.test/tenant", 1, 10)
    new = edition("new", "endpoint-new", "https://directory.example.test/tenant/", 2, 20)
    same_date_smaller = edition("smaller", "endpoint-z", "https://directory.example.test/tenant", 2, 15)
    other = edition("other", "endpoint-other", "https://directory.example.test/other", 1, 10)
    without_url = edition("no-url", "endpoint-missing-url", None, 1, 10)
    unfinished_edition = edition("incomplete", "endpoint-incomplete", new["source_uri"], 3, 25)
    unfinished_edition["complete"] = False
    current, carried = inventory.select_fhir_editions(
        [old, new, same_date_smaller, other, without_url, unfinished_edition], [old]
    )
    assert {source["endpoint_id"] for source in current} == {"endpoint-new", "endpoint-other", "endpoint-missing-url"}
    assert not carried
    current, carried = inventory.select_fhir_editions([old, other], [old, new])
    assert current == [other]
    assert carried == [new]

    observed = SimpleNamespace(fhir_descriptors=(new, other, without_url), carried_fhir_descriptors=())
    dispositions = [
        SimpleNamespace(descriptor=descriptor)
        for descriptor in (old, new, same_date_smaller, other, without_url, unfinished_edition)
    ]
    selected = catalog_preparation.select_preparation_fhir_sources(observed, dispositions)
    assert {source.descriptor["endpoint_id"] for source in selected} == {
        "endpoint-new",
        "endpoint-other",
        "endpoint-missing-url",
    }


def test_practitioner_enrichment_does_not_replace_or_become_a_network_edition():
    uri = "https://directory.example.test/tenant"
    network = edition("network", "endpoint-network", uri, 1, 10)
    enrichment = edition("enrichment", "endpoint-enrichment", uri, 2, 20)
    enrichment["coordinates"]["source_id"] = UHC_FLEX_PRACTITIONER_SOURCE_ID
    enrichment["source_aliases"] = [UHC_FLEX_PRACTITIONER_SOURCE_ID]

    assert inventory.select_fhir_editions([network, enrichment], ()) == ([network], [])
    assert inventory.select_fhir_editions([network], [enrichment]) == ([network], [])
    assert inventory.select_fhir_editions([enrichment], [network]) == ([], [network])


def test_catalog_keeps_distinct_practitioner_enrichment_without_displacing_network():
    uri = "https://directory.example.test/tenant"
    network = edition("network", "endpoint-network", uri, 1, 10)
    enrichment = edition("enrichment", "endpoint-enrichment", uri, 2, 20)
    enrichment["coordinates"]["source_id"] = UHC_FLEX_PRACTITIONER_SOURCE_ID
    enrichment["source_aliases"] = [UHC_FLEX_PRACTITIONER_SOURCE_ID]

    select_catalog = partial(inventory.select_fhir_editions, include_practitioner_enrichment=True)
    assert select_catalog([network, enrichment], ()) == ([network], [])
    assert select_catalog([enrichment], [network]) == ([], [network])
    assert select_catalog([network], [enrichment]) == ([network], [])

    enrichment["source_uri"] = "https://directory.example.test/enrichment"
    assert select_catalog([network, enrichment], ()) == ([enrichment, network], [])


def test_current_feed_retires_typed_predecessor_from_a_different_registration():
    previous = _recipe()
    descriptor = previous.source_pin.catalog_descriptor
    winner = edition("winner", "another-registration", descriptor["source_uri"], 2, 20)
    observed = SimpleNamespace(fhir_descriptors=(winner,), carried_fhir_descriptors=(), superseded_fhir_endpoints=())
    assert inventory.retained_unavailable_fhir_recipes([previous], observed) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_carried", [False, True])
async def test_practitioner_enrichment_preserves_legacy_network_on_same_feed(is_carried):
    previous = _recipe()
    descriptor = previous.source_pin.catalog_descriptor
    enrichment = edition("enrichment", "enrichment-registration", descriptor["source_uri"], 2, 20)
    enrichment["coordinates"]["source_id"] = UHC_FLEX_PRACTITIONER_SOURCE_ID
    enrichment["source_aliases"] = [UHC_FLEX_PRACTITIONER_SOURCE_ID]
    observed = SimpleNamespace(
        fhir_descriptors=() if is_carried else (enrichment,),
        carried_fhir_descriptors=(enrichment,) if is_carried else (),
        superseded_fhir_endpoints=(),
    )
    assert inventory.retained_unavailable_fhir_recipes([previous], observed) == (previous,)

    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"source_generation": "legacy", "descriptor": json.dumps(descriptor)}])
    )
    prior_catalog_by_field = {
        "schema_name": "published_catalog",
        "source_vector_json": [{"source_generation": "legacy", "status": "current"}],
    }
    catalog_token = catalog_preparation._INVENTORY.set(observed)
    try:
        assert await catalog_preparation.replaced_fhir_generations(connection, prior_catalog_by_field, ()) == ()
    finally:
        catalog_preparation._INVENTORY.reset(catalog_token)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_delta", [False, True])
async def test_catalog_retains_one_copy_of_the_latest_eligible_edition_if_unavailable(
    delta_positive_descriptor, is_delta
):
    uri = "https://directory.example.test/tenant"
    old = edition("old", "endpoint-old", uri, 1, 10)
    latest = delta_positive_descriptor if is_delta else edition("latest", "endpoint-new", uri, 2, 20)
    incomplete = edition("incomplete", "endpoint-incomplete", "https://directory.example.test/other", 3, 30)
    incomplete["complete"] = False
    observed = SimpleNamespace(
        fhir_descriptors=(), carried_fhir_descriptors=(latest,), superseded_fhir_endpoints=("endpoint-old",)
    )
    descriptor_by_generation = {"old": old, "duplicate": latest, "current": latest, "incomplete": incomplete}
    connection = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[
                {"source_generation": generation, "descriptor": json.dumps(descriptor)}
                for generation, descriptor in descriptor_by_generation.items()
            ]
        )
    )
    previous_by_field = dict(
        schema_name="published_catalog",
        source_vector_json=[{"source_generation": "current", "status": "current"}],
    )
    token = catalog_preparation._INVENTORY.set(observed)
    try:
        assert set(await catalog_preparation.replaced_fhir_generations(connection, previous_by_field, ())) == {
            "old",
            "duplicate",
            "incomplete",
        }
    finally:
        catalog_preparation._INVENTORY.reset(token)


@pytest.mark.asyncio
async def test_ordinary_inventory_never_discovers_ptg_provider_sets(inventory_case):
    case = inventory_case
    result = await observe(case)
    assert result.fhir_descriptors == (case.descriptor,)
    assert [source["source_system"] for source in result.native_sources] == ["aca"]
    generation, sources, descriptors = await refresh.imported_catalog_inputs(object(), "registry", inventory=result)
    assert generation == result.token and descriptors == [case.descriptor]
    assert len(sources) == 2 and {source["capture_generation"] for source in sources} == {result.token}
    assert all("ptg2_" not in call.args[0] for call in case.connection.fetch.call_args_list)
    assert all("network_serving_control" not in call.args[0] for call in case.connection.fetch.call_args_list)


@pytest.mark.asyncio
async def test_source_token_tracks_named_data_and_policy(inventory_case, monkeypatch):
    case = inventory_case
    before = await observe(case)
    case.ptg["edition_id"] = "snapshot-two"
    assert (await observe(case)).token == before.token
    case.descriptor["tables"]["resource"] = {"schema": "edition_resources", "name": "resources"}
    assert (await observe(case)).token != before.token
    monkeypatch.setattr(inventory, "graph_policy", lambda source_id: {"office_policy": "next-policy"})
    assert (await observe(case)).token != before.token


@pytest.mark.asyncio
@pytest.mark.parametrize(("reuse", "saved_days"), [(False, 0), (True, 0), (False, 1)])
async def test_native_catalog_preparation_returns_the_exact_input_cohort(
    inventory_case, monkeypatch, reuse, saved_days
):
    case = inventory_case
    observed = await observe(case)
    saved_date = date.fromisoformat(observed.effective_date) - timedelta(days=saved_days)
    pinned = observed.at_date(saved_date.isoformat())
    accounting = catalog_preparation.imported_catalog_accounting(pinned)
    metrics = {"resumed_snapshot_as_of": pinned.effective_date} if saved_days else accounting
    prepared_snapshot_by_field = {
        "accounting_json": accounting,
        "metadata_generation": "e" * 64,
        "schema_name": "candidate_catalog",
    }
    previous = prepared_snapshot_by_field if reuse else None

    async def prepare(*_arguments, source_export):
        assert source_export is case.factory
        assert catalog_preparation.current_catalog_inventory() is observed
        return (None if reuse else prepared_snapshot_by_field), previous, metrics

    monkeypatch.setattr(catalog_sync, "prepare_fhir_catalog_snapshot", prepare)
    preparation = await catalog_preparation.prepare_imported_catalog_snapshot(
        case.config, None, None, None, source_export=case.factory, inventory=observed
    )

    assert preparation.inventory_token == pinned.token
    assert preparation.snapshot is prepared_snapshot_by_field and preparation.previous is previous
    assert preparation.accounting == metrics
    assert catalog_preparation.current_catalog_inventory() is None


@pytest.mark.asyncio
async def test_native_catalog_preparation_refuses_reuse_after_source_discovery_changes(inventory_case, monkeypatch):
    case = inventory_case
    before = await observe(case)
    previous_by_field_by_field = {"accounting_json": catalog_preparation.imported_catalog_accounting(before)}
    case.connection.fetchrow.return_value["import_id"] = "successor-edition"
    current = await observe(case)
    assert current.token != before.token
    monkeypatch.setattr(
        catalog_sync, "prepare_fhir_catalog_snapshot", AsyncMock(return_value=(None, previous_by_field_by_field, {}))
    )

    with pytest.raises(ValueError, match="registry_imported_catalog_preparation_changed"):
        await catalog_preparation.prepare_imported_catalog_snapshot(
            case.config, None, None, None, source_export=case.factory, inventory=current
        )
    assert catalog_preparation.current_catalog_inventory() is None


@pytest.mark.asyncio
async def test_incomplete_native_publication_refuses_inventory(inventory_case):
    case = inventory_case
    case.connection.fetchrow.return_value["state"] = "pending"
    with pytest.raises(ValueError, match="aca_incomplete"):
        await observe(case)


def test_native_definition_identity_and_unnamed_labels_preserve_proven_scope():
    aca = native.native_catalog_descriptor(native_aca_source())
    definitions = native.native_network_definitions(aca)
    assert (
        len(definitions) == 1 and next(iter(definitions.values()))["display_name"] == "Example insurer 2027 Preferred"
    )
    changed = deepcopy(aca)
    changed["coordinates"].update(dataset_id="edition-two", edition_id="edition-two")
    changed["input"]["edition_id"] = "edition-two"
    assert list(native.native_network_definitions(changed)) == list(definitions)
    ptg = native.native_catalog_descriptor(native_ptg_source())
    network_ids_by_key = {key: 42 for key in native.native_network_definitions(ptg)}
    projection = native.native_catalog_rows(
        ptg,
        generation="a" * 64,
        accepted_at="2026-11-17T00:00:00Z",
        metadata_generation="b" * 64,
        networks=network_ids_by_key,
    )
    assert projection.network_rows[0][2] == "Provider set source-graph"
    binding = json.loads(projection.binding_rows[0][-1])
    assert binding["source_system"] == "ptg" and binding["source_scope"]["snapshot_key"] == 31
    assert "organization_id" not in binding["source_scope"]


def test_aca_conflicting_checksum_or_tier_identity_refuses_definition():
    source = native_aca_source()
    source["definitions"].append({**source["definitions"][0], "network_tier": "Different"})
    with pytest.raises(ValueError, match="aca_ambiguous"):
        native.native_catalog_descriptor(source)


def test_native_cohort_hash_excludes_membership_association_but_retains_inputs():
    selected = snapshot(standing_grant())
    selected["accounting_json"] = {
        "imported_source_inventory": {
            "contract": "registry-imported-catalog-inputs-v1",
            "token": "d" * 64,
            "native_sources": [{"source_id": "source-graph", "source_generation": "f" * 64}],
        }
    }
    before = metadata_generation(selected)
    selected.update(network_generation=10, manifest_sha256="c" * 64)
    assert metadata_generation(selected) == before
    assert metadata_generation(snapshot_inputs(selected)) == before
    selected["accounting_json"]["imported_source_inventory"]["token"] = "e" * 64
    assert metadata_generation(selected) != before


@pytest.mark.asyncio
async def test_missing_server_inventory_capability_never_falls_back_to_serving_pointer():
    with pytest.raises(ValueError, match="native_inventory_required"):
        await refresh.imported_catalog_inputs(AsyncMock(), "registry")


def test_large_plan_inventory_keeps_control_pins_compact(record_property):
    from time import perf_counter

    from process.network_fhir_source_witness_contract import canonical, digest
    from process.registry_imported_catalog_preparation import imported_catalog_accounting
    from process.registry_native_provider_source import native_provider_source_data

    started = perf_counter()
    native_sources = []
    for ordinal in range(304):
        source_document = native_ptg_source()
        source_document["source_id"] = "selected-source-" + str(ordinal)
        definition = source_document["definitions"][0]
        definition["source_key"] = source_document["source_id"]
        definition["plans"] = [
            {"plan_id": digest([ordinal, plan]), "plan_market_type": "group"}
            for plan in range(101261 // 304 + (ordinal < 101261 % 304))
        ]
        native_sources.append(source_document)
    document_by_field = {
        "contract": "registry-imported-catalog-inputs-v1",
        "fhir_semantics": [],
        "fhir_descriptors": [],
        "native_sources": native_sources,
        "unavailable": [],
        "effective_date": "2026-11-17",
        "search_policy": "native-network-search-v1",
        "catalog_projector_version": inventory.PROJECTOR_VERSION,
    }
    observed = inventory.ImportedSourceInventory(canonical(document_by_field))
    accounting_bytes = len(canonical(imported_catalog_accounting(observed)).encode())
    descriptor_bytes = [
        len(canonical(native.native_catalog_descriptor(source_document)).encode()) for source_document in native_sources
    ]
    recipe_input_bytes = [
        len(canonical(native_provider_source_data(native.native_catalog_descriptor(source_document))).encode())
        for source_document in native_sources
    ]
    evidence_by_field = {
        "scope": "metadata preparation component only",
        "sources": 304,
        "plan_pointers": 101261,
        "full_inventory_bytes": len(observed.document_json.encode()),
        "control_accounting_bytes": accounting_bytes,
        "max_source_descriptor_bytes": max(descriptor_bytes),
        "max_recipe_source_input_bytes": max(recipe_input_bytes),
        "seconds": perf_counter() - started,
    }
    record_property("metadata_scale", json.dumps(evidence_by_field, sort_keys=True))
    print(json.dumps(evidence_by_field, sort_keys=True))
    assert sum(len(source_document["definitions"][0]["plans"]) for source_document in native_sources) == 101261
    assert accounting_bytes < 8 * 1024 * 1024 and max(descriptor_bytes) < 8 * 1024 * 1024
    assert max(recipe_input_bytes) < 4096


@pytest.mark.asyncio
async def test_catalogue_policy_change_invalidates_inventory_token(inventory_case, monkeypatch):
    before = await observe(inventory_case)
    monkeypatch.setattr(inventory, "PROJECTOR_VERSION", "fhir-catalog/next-version")
    after = await observe(inventory_case)
    assert before.token != after.token
    assert before.fhir_descriptors == after.fhir_descriptors
    assert before.native_sources == after.native_sources


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attribute", "override_value"),
    [
        ("AUTOMATIC_NETWORKS_PER_SOURCE", 1),
        ("AUTOMATIC_NETWORKS_PER_CATALOG", 1),
        ("AUTOMATIC_NETWORK_HYGIENE_VERSION", "fhir-automatic-org-hygiene-v2"),
    ],
)
async def test_automatic_network_limit_change_invalidates_inventory_token(
    inventory_case, monkeypatch, attribute, override_value
):
    before = await observe(inventory_case)
    before_token = before.token
    monkeypatch.setattr(inventory, attribute, override_value)
    after = await observe(inventory_case)
    assert before.document == after.document
    assert before_token != after.token


@pytest.mark.asyncio
async def test_serving_catalog_observations_returns_only_complete_observations():
    admitted = observed_descriptor()
    incomplete_by_field = {**observed_descriptor(dataset="other-dataset"), "complete": False}
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value="published_catalog"),
        fetch=AsyncMock(
            return_value=[{"descriptor": json.dumps(admitted)}, {"descriptor": json.dumps(incomplete_by_field)}]
        ),
    )
    assert await inventory._serving_catalog_observations(connection, "registry") == (admitted,)
    assert "network_serving_control" in connection.fetchval.call_args.args[0]
    connection.fetchval.return_value = None
    assert await inventory._serving_catalog_observations(connection, "registry") == ()


@pytest.mark.asyncio
async def test_checkpoint_catalog_date_recovers_serving_predecessor_date():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=date(2026, 11, 17)))
    assert await inventory._checkpoint_catalog_date(connection, "registry") == "2026-11-17"
    selection = connection.fetchval.call_args.args[0]
    assert "published_at IS NULL" in selection and "accounting_json ? 'registry_fhir_catalog_source'" in selection
    assert "s.generation_id IS NOT DISTINCT FROM c.network_generation" in selection
    connection.fetchval.return_value = None
    assert await inventory._checkpoint_catalog_date(connection, "registry") is None


@pytest.mark.asyncio
async def test_native_imported_source_reader_disables_idle_in_transaction_timeout(inventory_case, monkeypatch):
    executed_statements = []
    driver = SimpleNamespace(execute=AsyncMock(side_effect=executed_statements.append))

    @asynccontextmanager
    async def session_scope():
        @asynccontextmanager
        async def begin():
            yield

        yield SimpleNamespace(begin=begin)

    monkeypatch.setattr(inventory, "native_driver", AsyncMock(return_value=driver))
    runtime = inventory.ImportedSourceInventoryRuntime(inventory_case.config, session_scope, inventory_case.factory)
    with inventory.imported_source_inventory_runtime(runtime):
        async with inventory.native_imported_source_reader() as connection:
            assert connection is driver
    assert executed_statements == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET LOCAL idle_in_transaction_session_timeout = '0'",
    ]
