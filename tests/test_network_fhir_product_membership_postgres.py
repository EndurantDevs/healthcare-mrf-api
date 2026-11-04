# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed products union complete witnessed plans under genuine retained custody."""

import asyncio
import hashlib
import json
from dataclasses import asdict
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_fhir_typed_mapping as mapping
from process import network_membership_slices as slices
from process import registry_imported_selection_preview as preview
from process import registry_membership_composition_stage as composition
from process import registry_typed_membership_parity as membership_parity
from process.network_approved_membership_source import ApprovedMembershipSource, pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    FHIRMembershipSourceError,
    PinnedFHIRMembershipSource,
    read_retained_fhir_membership_batch,
)
from process.network_fhir_retained_capture_contract import PRODUCER
from process.network_fhir_typed_contract import TypedFHIRGraphError
from process.network_fhir_typed_sql import PAIR_DDL
from process.network_membership_copy import MembershipCopyTarget
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_source_recipe_composition import _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.registry_source_retention import RegistrySourceRetentionError, registry_source_retention_sha256
from tests import test_network_fhir_retained_capture_postgres as retained
from tests.test_network_fhir_retained_capture_postgres import legacy_capture_db as legacy_capture_db
from tests.test_network_provider_only_plan_postgres import planner_connection as planner_connection
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

pytestmark = pytest.mark.asyncio


@pytest.fixture
def product_resources(monkeypatch, request):
    mode = getattr(request, "param", "direct")
    original = retained._contents
    build_summary = retained.build_source_summary

    def summary(**arguments):
        metrics_by_field = {
            **arguments["count_by_field"],
            "organization_resources": arguments["count_by_resource"]["Organization"],
        }
        return build_summary(**{**arguments, "count_by_field": metrics_by_field})

    def contents(source_id):
        resources = original(source_id)
        for kind, _identity, payload in resources:
            if kind == "InsurancePlan":
                payload["network_refs"] = ["Organization/network-one", "Organization/network-two"]
            if kind == "PractitionerRole":
                payload["network_refs"] = (
                    [] if mode == "plan" else ["Organization/network-one", "Organization/network-two"]
                )
                payload["insurance_plan_refs"] = ["InsurancePlan/plan-one"] if mode == "plan" else []
                if mode == "inactive":
                    payload["active"] = False
                if mode == "expired":
                    payload["period_end"] = "2020-01-01"
                if mode == "oversized":
                    payload["location_refs"] = ["Location/office-one"] * 1001
        resources.extend(
            [
                ("Organization", "network-two", {"source_id": source_id, "resource_id": "network-two"}),
                (
                    "InsurancePlan",
                    "plan-two",
                    {"source_id": source_id, "resource_id": "plan-two", "network_refs": ["Organization/network-two"]},
                ),
            ]
        )
        return resources

    monkeypatch.setattr(retained, "_contents", contents)
    monkeypatch.setattr(retained, "build_source_summary", summary)
    return mode


@pytest.fixture
async def product_capture_db(product_resources, legacy_capture_db):
    return legacy_capture_db


async def _product_recipe(fixture, serving, capture, plans=("plan-one", "plan-one", "plan-two")):
    """Approve one complete immutable plan selector for each synthetic product."""
    connection, control, engine = serving
    actor = _actor()
    networks = [await _draft(engine, control, _create("network"), actor) for _ in plans]
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        fixture.origin.source_id,
        fixture.origin.schema_name,
        fixture.origin.dataset_id,
        PRODUCER,
        capture.capture_id,
    )
    binding_rows = [
        {
            "binding_id": str(uuid4()),
            **asdict(coordinates),
            "source_key": "InsurancePlan/" + plan,
            "source_scope_json": {
                "insurance_plan_id": plan,
                "capture_sha256": capture.capture_sha256,
                "product_network_id": network["record_id"],
            },
            "network_id": network["record_id"],
            "evidence_id": "reviewed-synthetic-product",
            "evidence_sha256": "e" * 64,
            "operation": "bind",
            "expected_revision": 0,
            "expected_network_id": None,
        }
        for plan, network in zip(plans, networks, strict=True)
    ]
    command = NetworkSourceBindingBatchCommand(
        json.dumps(binding_rows).encode(), "Reviewed exact product plans", uuid4().hex
    )
    async with connection.transaction():
        bindings = (await apply_network_source_binding_batch(connection, command, actor, control_schema=control))[
            "records"
        ]
    await _approve(connection, control, await _command(connection, control, *networks, *bindings), actor)
    async with connection.transaction(isolation="repeatable_read"):
        revision = await connection.fetchval(f'SELECT approved_revision FROM "{control}".registry_revision_control')
        approved = await pin_approved_membership_source(connection, approved_revision=revision, control_schema=control)
    await connection.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{fixture.role_by_kind["reader"]}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control}" TO "{fixture.role_by_kind["reader"]}"')
    source_pin = PinnedFHIRMembershipSource(
        fixture.origin.schema_name,
        fixture.origin.source_id,
        fixture.origin.endpoint_id,
        fixture.origin.dataset_id,
        fixture.origin.dataset_sha256,
        capture.release_id,
        fixture.origin.resource_parent_oid,
        "medical",
        "2026-10-01",
        retained_capture=capture,
    )
    product_ids = tuple(network["record_id"] for network in networks)
    return RegistrySourceMembershipRecipe(source_pin, coordinates), approved, product_ids


@pytest.mark.parametrize("product_resources", ["direct", "plan", "inactive", "expired"], indirect=True)
async def test_whole_plan_product_union_matches_preview_without_provider_page_plan_inference(
    product_capture_db, product_resources, serving_schema
):
    fixture = product_capture_db
    capture = await retained._capture(fixture)
    recipe, approved, product_ids = await _product_recipe(fixture, serving_schema, capture)
    reader = await fixture.connect("reader")
    await reader.execute(
        "CREATE TEMP TABLE selected_product_bindings(record_kind text,record_key text,revision bigint)"
    )
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        batch = await read_retained_fhir_membership_batch(
            reader,
            recipe.source_pin,
            read,
            registry_schema=serving_schema[1],
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        memberships = json.loads(batch.input_bytes)
        expected = 0 if product_resources in {"inactive", "expired"} else 3
        assert batch.source_rows == 1 and batch.membership_rows == len(memberships) == expected
        assert (
            batch.unresolved_rows == 0
            and len({json.dumps(membership, sort_keys=True) for membership in memberships}) == expected
        )
        assert {membership["network_id"] for membership in memberships} == (set(product_ids) if expected else set())
        counts = await preview._recipe_impact(
            reader,
            recipe,
            SimpleNamespace(expected_approved_revision=approved.approved_revision),
            '"selected_product_bindings"',
            serving_schema[1],
        )
        assert counts["mapped_rows"] == expected and counts["unresolved_rows"] == 0
        assert batch.accepted_source_rows == int(expected > 0)
        assert batch.omitted_source_rows == int(expected == 0)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_complete_selected_plan_refuses_a_missing_org_even_outside_provider_page(
    legacy_capture_db, serving_schema
):
    fixture = legacy_capture_db
    capture = await retained._capture(fixture)
    recipe, approved, _targets = await _product_recipe(fixture, serving_schema, capture, ("unresolved-plan",))
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        with pytest.raises(FHIRMembershipSourceError, match="product plan is unavailable"):
            await read_retained_fhir_membership_batch(
                reader,
                recipe.source_pin,
                read,
                registry_schema=serving_schema[1],
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
            )


@pytest.mark.parametrize("product_resources", ["oversized"], indirect=True)
async def test_post_product_fanout_bound_refuses_one_oversized_source_row(product_capture_db, serving_schema):
    fixture = product_capture_db
    capture = await retained._capture(fixture)
    recipe, approved, _targets = await _product_recipe(fixture, serving_schema, capture)
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        read = await _require_recipe_custody(reader, recipe)
        with pytest.raises(FHIRMembershipBatchBoundsError, match="exceeds bounds"):
            await read_retained_fhir_membership_batch(
                reader,
                recipe.source_pin,
                read,
                registry_schema=serving_schema[1],
                limit=1,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
            )


_ARGUMENTS = (
    2,
    "fhir",
    "sample-source",
    "sample_schema",
    "sample-dataset",
    "sample-producer",
    "sample-edition",
    "sample-generation",
)
_BINDINGS = mapping.APPROVED_NETWORK_BINDINGS_SQL.format(namespace="pg_temp")


async def _fixture(connection):
    await connection.execute(
        "CREATE TEMP TABLE registry_approved_record(approved_revision bigint,record_kind text,record_key text,"
        "record_revision bigint,custom_revision bigint,record_json jsonb)"
    )
    await connection.execute(
        "INSERT INTO registry_approved_record VALUES(2,'network','1',1,1,'{\"network_id\":1,\"archived\":false}');"
        "INSERT INTO registry_approved_record SELECT 2,'network_binding','binding-one',1,1,"
        "jsonb_build_object('network_id',1,'archived',false,'source_system','fhir','source_id','sample-source',"
        "'dataset_schema','sample_schema','dataset_id','sample-dataset','producer_id','sample-producer',"
        "'edition_id','sample-edition','source_key','network-one',"
        "'source_scope_json',jsonb_build_object('organization_id','network-one','source_generation','sample-generation'))"
    )
    await connection.execute("CREATE TEMP TABLE pair_plan_input(" + PAIR_DDL + ")")
    await connection.execute(
        "INSERT INTO pair_plan_input SELECT CASE WHEN g%3=0 THEN 'unapproved' ELSE 'network-one' END,"
        "'npi','1234567893',md5(g::text)::uuid,md5(g::text)::uuid,'sample|street','PractitionerRole',"
        "lpad(g::text,5,'0'),g::text,'direct',1+g%3 FROM generate_series(1,5001) g"
    )
    names = await mapping.create_typed_mapping_tables(connection)
    stage = SimpleNamespace(
        relation=lambda _: "pg_temp.pair_plan_input",
        document={"counts": {"pair_rows": 5001, "reference_rows": 10002}},
    )
    selected = mapping.TypedGraphMapping(stage, names, {}, {})
    await connection.execute(
        "INSERT INTO "
        + selected.relation("offices")
        + " SELECT provider_id,location_id,address_key,canonical_identity,"
        "CASE WHEN resource_id::integer%7=0 THEN NULL ELSE location_resource_id END FROM pair_plan_input "
        "WHERE resource_id::integer%5<>0"
    )
    return stage, selected


def _expected():
    totals = dict.fromkeys(
        (
            "mapped_pair_rows",
            "mapped_reference_rows",
            "unapproved_pair_rows",
            "unapproved_reference_rows",
            "unmatched_office_pair_rows",
            "unmatched_office_reference_rows",
        ),
        0,
    )
    membership_rows = []
    for number in range(1, 5002):
        classification = (
            "unapproved" if number % 3 == 0 else "unmatched_office" if number % 5 == 0 or number % 7 == 0 else "mapped"
        )
        totals[classification + "_pair_rows"] += 1
        totals[classification + "_reference_rows"] += 1 + number % 3
        if classification == "mapped":
            evidence_fields = [
                "fhir-direct-network-office-v3",
                "sample-generation",
                "PractitionerRole",
                f"{number:05d}",
                "network-one",
                str(number),
                "direct",
            ]
            membership_rows.append(
                (
                    1,
                    "npi",
                    "1234567893",
                    UUID(hashlib.md5(str(number).encode()).hexdigest()),
                    hashlib.sha256(json.dumps(evidence_fields).encode()).hexdigest(),
                )
            )
    return totals, sorted(membership_rows)


@pytest.mark.parametrize("original", ["on", "off"])
@pytest.mark.parametrize("readonly", [False, True])
async def test_pair_plan_exact_counts_evidence_and_nested_scope(planner_connection, original, readonly):
    connection, _admin = planner_connection
    stage, selected = await _fixture(connection)
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    async with connection.transaction(isolation="repeatable_read", readonly=readonly):
        nested = "off" if original == "on" else "on"
        await connection.execute("SELECT set_config('enable_nestloop',$1,true)", nested)
        totals = await mapping._map_pair_rows(connection, stage, selected, _BINDINGS, _ARGUMENTS)
        expected_totals, expected_rows = _expected()
        assert totals == expected_totals
        await mapping._require_mapping_counts(connection, stage, selected, totals)
        rows = await connection.fetch(
            "SELECT * FROM "
            + selected.relation("memberships")
            + " ORDER BY network_id,provider_system,provider_id,location_id,evidence_id"
        )
        assert [tuple(row) for row in rows] == expected_rows
        assert await connection.fetchval("SHOW enable_nestloop") == nested
        assert await connection.fetchval("SHOW transaction_read_only") == ("on" if readonly else "off")
    assert await connection.fetchval("SHOW enable_nestloop") == original


async def test_pair_plan_duplicate_bindings_still_fail_count_closure_and_rollback(planner_connection):
    connection, _admin = planner_connection
    stage, selected = await _fixture(connection)
    await connection.execute(
        "INSERT INTO registry_approved_record SELECT * FROM registry_approved_record WHERE record_kind='network_binding'"
    )
    with pytest.raises(TypedFHIRGraphError, match="mapping_counts"):
        async with connection.transaction():
            totals = await mapping._map_pair_rows(connection, stage, selected, _BINDINGS, _ARGUMENTS)
            await mapping._require_mapping_counts(connection, stage, selected, totals)
    assert await connection.fetchval("SELECT count(*) FROM " + selected.relation("memberships")) == 0
    assert await connection.fetchval("SHOW enable_nestloop") == "on"


@pytest.mark.parametrize("original", ["on", "off"])
async def test_pair_plan_database_error_restores_scope(planner_connection, original):
    connection, _admin = planner_connection
    stage, selected = await _fixture(connection)
    await connection.execute("DROP TABLE " + selected.relation("offices"))
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    async with connection.transaction():
        with pytest.raises(asyncpg.UndefinedTableError):
            await mapping._map_pair_rows(connection, stage, selected, _BINDINGS, _ARGUMENTS)
        assert await connection.fetchval("SHOW enable_nestloop") == original
        assert await connection.fetchval("SELECT count(*) FROM " + selected.relation("memberships")) == 0


class _PausedPage:
    def __init__(self, connection):
        self.connection, self.pages = connection, 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetchrow(self, query, *arguments):
        if "mapped_pair_rows" in query:
            assert await self.connection.fetchval("SHOW enable_nestloop") == "off"
            self.pages += 1
            if self.pages == 2:
                await self.connection.execute("SELECT pg_sleep(30)")
        return await self.connection.fetchrow(query, *arguments)


async def _wait_for_sleep(admin, pid):
    while not await admin.fetchval("SELECT wait_event='PgSleep' FROM pg_stat_activity WHERE pid=$1", pid):
        await asyncio.sleep(0.01)


@pytest.mark.parametrize("original", ["on", "off"])
async def test_pair_plan_real_cancellation_rolls_back_completed_pages(planner_connection, original):
    connection, admin = planner_connection
    stage, selected = await _fixture(connection)
    await connection.execute("SELECT set_config('enable_nestloop',$1,false)", original)
    observed = _PausedPage(connection)
    async with connection.transaction():
        task = asyncio.create_task(mapping._map_pair_rows(observed, stage, selected, _BINDINGS, _ARGUMENTS))
        try:
            await asyncio.wait_for(_wait_for_sleep(admin, connection.get_server_pid()), 5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert observed.pages == 2
        assert await connection.fetchval("SHOW enable_nestloop") == original
        assert await connection.fetchval("SELECT count(*) FROM " + selected.relation("memberships")) == 0


def _composition_candidate(target, stage, additions, removals, effective_rows):
    receipt_by_field = {
        "component": "registry_source_retention",
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": "b" * 64,
        "source_rows": 3,
        "add_rows": additions,
        "remove_rows": removals,
        "suppressed_source_rows": 2 if removals else 0,
        "effective_rows": effective_rows,
    }
    candidate_by_field = {
        name: getattr(target, name)
        for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_field.update(
        expected_rows=effective_rows,
        approved_custom_revision=2,
        source_generations={
            "custom_membership": stage.source.generation_id,
            "registry_source_retention": registry_source_retention_sha256(receipt_by_field),
        },
        validation_json={"registry_source_retention": receipt_by_field},
    )
    return candidate_by_field


@pytest.fixture
async def composition_parity_case(planner_connection, request):
    connection, admin = planner_connection
    additions, removals = getattr(request, "param", (0, 0))
    identities = [str(uuid4()) for _ in range(4)]
    copy_target = MembershipCopyTarget(*identities, "network_candidate_" + UUID(identities[3]).hex)
    schema = '"' + copy_target.schema_name + '"'
    owner = await connection.fetchval("SELECT current_user")
    source_rows = [
        (1, "npi", "1234567893", UUID(int=1), "first"),
        (1, "npi", "1234567893", UUID(int=1), "first"),
        (1, "npi", "1234567893", UUID(int=2), "second"),
    ]
    added_row = (1, "npi", "1234567893", UUID(int=3), "custom")
    suffix = uuid4().hex
    stage = composition.RegistryMembershipCommandStage(
        ApprovedMembershipSource(2, "a" * 64, additions + removals),
        "registry_command_add_" + suffix,
        "registry_command_remove_" + suffix,
        additions,
        removals,
    )
    ddl = "(network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL)"
    try:
        await admin.execute(f'CREATE SCHEMA {schema} AUTHORIZATION "{owner}"')
        for table in ("network_membership_source", "network_membership"):
            relation = schema + "." + table
            await connection.execute("CREATE TABLE " + relation + ddl)
            await membership_parity.create_order_index(connection, relation, table + "_order_idx")
        for table in (stage.add_table, stage.remove_table):
            await connection.execute('CREATE TEMP TABLE "' + table + '"' + ddl)
        await connection.copy_records_to_table(
            "network_membership_source", schema_name=copy_target.schema_name, records=source_rows
        )
        if additions:
            await connection.copy_records_to_table(stage.add_table, schema_name="pg_temp", records=[added_row])
        if removals:
            await connection.copy_records_to_table(stage.remove_table, schema_name="pg_temp", records=source_rows[:1])
        effective_rows = [source_row for source_row in source_rows if not removals or source_row[3] != UUID(int=1)]
        effective_rows += [added_row] if additions else []
        await connection.copy_records_to_table(
            "network_membership", schema_name=copy_target.schema_name, records=effective_rows
        )
        candidate = _composition_candidate(copy_target, stage, additions, removals, len(effective_rows))
        yield SimpleNamespace(
            connection=connection, target=copy_target, candidate=candidate, stage=stage, schema=schema
        )
    finally:
        await admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.mark.parametrize("composition_parity_case", [(0, 0), (1, 0), (0, 1), (1, 1)], indirect=True)
@pytest.mark.parametrize("typed", [False, True])
async def test_composition_empty_arms_keep_nonempty_intent_and_duplicate_evidence(
    composition_parity_case, monkeypatch, typed
):
    fixture = composition_parity_case
    monkeypatch.setattr(membership_parity, "is_typed_candidate", lambda _candidate: typed)
    original = membership_parity.has_multiset_difference
    compared_queries = []

    async def compare(connection, actual, expected):
        compared_queries.append(expected)
        return await original(connection, actual, expected)

    monkeypatch.setattr(membership_parity, "has_multiset_difference", compare)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await composition.verify_registry_composition_rows(
            fixture.connection, fixture.target, fixture.candidate, fixture.stage
        )
    if typed:
        assert len(compared_queries) == 1
        assert (fixture.stage.add_table in compared_queries[0]) == bool(fixture.stage.add_rows)
        assert (fixture.stage.remove_table in compared_queries[0]) == bool(fixture.stage.remove_rows)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute(
            f"DELETE FROM {fixture.schema}.network_membership WHERE ctid IN "
            f"(SELECT ctid FROM {fixture.schema}.network_membership LIMIT 1)"
        )
        with pytest.raises(RegistrySourceRetentionError, match="effective_parity_invalid"):
            await composition.verify_registry_composition_rows(
                fixture.connection, fixture.target, fixture.candidate, fixture.stage
            )


@pytest.mark.parametrize("table_kind", ["add_table", "remove_table"])
async def test_composition_fresh_command_counts_are_checked_before_pruning(
    composition_parity_case, monkeypatch, table_kind
):
    fixture = composition_parity_case
    monkeypatch.setattr(membership_parity, "is_typed_candidate", lambda _candidate: True)

    async def unexpected_comparison(*_arguments):
        pytest.fail("Changed command counts must refuse before choosing a multiset query")

    monkeypatch.setattr(membership_parity, "has_multiset_difference", unexpected_comparison)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        table = composition._relation(getattr(fixture.stage, table_kind))
        await fixture.connection.execute(f"INSERT INTO {table} VALUES(1,'npi','1234567893',$1,'unexpected')", uuid4())
        with pytest.raises(RegistrySourceRetentionError, match="effective_parity_invalid"):
            await composition.verify_registry_composition_rows(
                fixture.connection, fixture.target, fixture.candidate, fixture.stage
            )


@pytest.mark.parametrize("mode", ["changed", "reused", "mixed"])
@pytest.mark.parametrize("page_size", [1, 3])
async def test_changed_edges_advance_over_reused_work_pages(composition_parity_case, monkeypatch, mode, page_size):
    fixture = composition_parity_case
    connection = fixture.connection
    monkeypatch.setattr(slices, "_BATCH_ROWS", page_size)
    await slices._create_slice_relations(connection, fixture.schema, typed=True)
    work_name = "slice_page_" + uuid4().hex
    work = 'pg_temp."' + work_name + '"'
    await connection.execute(
        f'CREATE TEMP TABLE {work}(network_id integer,office_bucket integer,location_key text COLLATE "C",'
        "PRIMARY KEY(network_id,office_bucket,location_key))"
    )
    work_rows = [(1, 0, office_key) for office_key in ("a", "b", "c")]
    work_rows += [(2, 0, office_key) for office_key in ("d", "e", "f")] + [(3, 0, "g"), (4, 0, "h")]
    await connection.copy_records_to_table(work_name, schema_name="pg_temp", records=work_rows)
    expected_edges = []
    for network_id in range(1, 5):
        slice_id = uuid4()
        is_changed = mode == "changed" or mode == "mixed" and network_id in (2, 4)
        artifact_id = UUID(fixture.target.candidate_id) if is_changed else uuid4()
        office_keys = [
            office_key for selected_network, _bucket, office_key in work_rows if selected_network == network_id
        ]
        await connection.execute(
            f"INSERT INTO {fixture.schema}.network_slice_ref VALUES($1,0,$2,$3,$4,$5)",
            network_id,
            artifact_id,
            slice_id,
            len(office_keys),
            "a" * 64,
        )
        if is_changed:
            expected_edges.extend((slice_id, office_key) for office_key in office_keys)
    async with connection.transaction(isolation="repeatable_read"):
        copied = await slices._copy_changed_edges(connection, fixture.target, work)
    actual_edges = await connection.fetch(f"SELECT * FROM {fixture.schema}.network_office_edge")
    assert copied == len(expected_edges) == len(actual_edges)
    assert {tuple(edge_row) for edge_row in actual_edges} == set(expected_edges)
