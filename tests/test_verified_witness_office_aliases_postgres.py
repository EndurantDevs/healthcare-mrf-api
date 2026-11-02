# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine independent Source aliases in one owned database; SQL guards have separate scope."""

import asyncio
import inspect
import json
import re
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_initial_cms_office_bindings as legacy
from process import registry_verified_witness_offices as offices
from process.network_address_projection import _identifier
from process.network_fhir_retained_capture_contract import registry_location_id
from process.registry_source_recipe_composition import SourceRecipeReadContext, stage_registry_source_recipes
from tests import generic_fhir_capture_postgres_support as retained
from tests import generic_source_successor_postgres_support as source
from tests import test_network_custom_address_source_postgres as custom
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_generic_fhir_reuse_postgres import _native_generic_offices
from tests.test_verified_office_source_witnesses_v2_postgres import _reader, _registry_on_source

pytestmark = pytest.mark.asyncio


async def _register_second_source(fixture):
    """Only synthetic Source/endpoint configuration is seeded; publication/admission remain production-owned."""
    database = fixture.component.database
    async with source.source.source_transaction(database):
        await database.status(
            "INSERT INTO mrf.provider_directory_api_endpoint "
            "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
            "VALUES (:endpoint,:base,repeat('c',64),repeat('d',64))",
            endpoint=source.ENDPOINT,
            base=source.BASE,
        )
        await database.status(
            "INSERT INTO mrf.provider_directory_source "
            "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
            "VALUES (:source,'Synthetic independent alias',:endpoint,:base,false,false)",
            source=source.SOURCE,
            endpoint=source.ENDPOINT,
            base=source.BASE,
        )


def _independent_candidate():
    """The second independent endpoint has no incumbent, unlike edition two of the successor fixture."""
    dataset = "generic-independent-alias-edition-2"
    resources = source._edition_rows(1, dataset)
    kinds = tuple(sorted({resource["resource_type"] for resource in resources}))
    candidate = source.directory.EndpointDatasetCandidate(
        endpoint_id=source.ENDPOINT,
        dataset_id=dataset,
        acquisition_root_run_id="generic-independent-alias-root-2",
        source_ids=(source.SOURCE,),
        selected_resources=kinds,
        expected_resources=kinds,
        import_run_id="generic-independent-alias-root-2",
        previous_dataset_id=None,
        resource_hash_contract=source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-10-01",
        proof_resource_scope=source.directory._provider_directory_proof_resource_scope(kinds),
    )
    return candidate, resources


async def _publish_independent_source(fixture):
    """Use the same genuine bounded acquisition, validation, publication and postcommit capture as publish_edition."""
    candidate, resource_rows = _independent_candidate()
    database = fixture.component.database
    async with source.source.source_transaction(database), database.acquire() as connection:
        await source.directory._ensure_endpoint_dataset_candidate(candidate)
        for kind in candidate.selected_resources:
            await source.directory._upsert_dataset_resource_rows_on_connection(
                connection,
                [resource for resource in resource_rows if resource["resource_type"] == kind],
                persist_content_proof=True,
                resource_hash_contract=candidate.resource_hash_contract,
                semantic_projection_as_of=candidate.semantic_projection_as_of,
            )
    validated = await source.directory._validate_endpoint_dataset_candidate(
        candidate,
        {
            kind: dict(rows_fetched=sum(resource["resource_type"] == kind for resource in resource_rows), complete=True)
            for kind in candidate.selected_resources
        },
    )
    assert validated["validated"] is True and validated["published"] is False
    with source.artifacts.source_publication_request(source.directory, [{"source_id": source.SOURCE}], True):
        await source.artifacts.publish_source_candidate(source.directory, candidate)
    assert database._transaction_binding() is None and source.source._PUBLISHER.get() is None
    capture = fixture.captures[-1]
    assert capture.origin.dataset_id == candidate.dataset_id and capture.origin.source_id == source.SOURCE
    assert (
        capture.origin.endpoint_id == source.ENDPOINT
        and capture.origin.root_run_id == candidate.acquisition_root_run_id
    )
    assert capture.capture_id == str(source.generic_fhir_capture_id(capture.origin))
    fixture.candidates.append(candidate)
    return capture


async def _publish_pair(fixture, monkeypatch):
    first = await source.publish_edition(fixture, 1)
    with monkeypatch.context() as settings:
        settings.setattr(source, "SOURCE", "generic-independent-alias-source")
        settings.setattr(source, "ENDPOINT", "generic-independent-alias-endpoint")
        settings.setattr(source, "BASE", "https://alias-directory.example.test/fhir")
        await _register_second_source(fixture)
        second = await _publish_independent_source(fixture)
    assert first.origin.source_id != second.origin.source_id
    assert first.origin.endpoint_id != second.origin.endpoint_id
    assert first.origin.dataset_id != second.origin.dataset_id
    assert len(fixture.captures) == len(fixture.commits) == 2
    assert fixture.source_writer_rejections == fixture.commits
    return first, second


async def _approved_pair(fixture, serving, captures):
    recipes, networks = [], []
    original = fixture.origin
    try:
        for capture in captures:
            fixture.origin = capture.origin
            recipe, approved, network_id = await retained._approved_capture_recipe(fixture, serving, capture)
            recipes.append(recipe)
            networks.append(network_id)
    finally:
        fixture.origin = original
    assert len(set(networks)) == 2 and approved.total_rows == 0
    return tuple(recipes), approved, tuple(sorted(networks))


@asynccontextmanager
async def _alias_case(fixture, monkeypatch):
    """All native fixtures share the SourceFixture UUID database and register their exact cleanup."""
    assert re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", fixture.component.admin_engine.url.database)
    async with _registry_on_source(fixture, monkeypatch) as serving, AsyncExitStack() as stack:
        for extension in ("intarray", "btree_gin", "postgis"):
            await fixture.admin.execute(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
        custom_generator = await stack.enter_async_context(aclosing(custom.custom_db.__wrapped__(serving)))
        custom_fixture = await anext(custom_generator)
        seed = SimpleNamespace(captures=fixture.namespaces, role_by_kind=fixture.role_by_kind)
        runtime_roles = (fixture.component.reader,)
        address = await _native_generic_offices(custom_fixture, seed, serving, runtime_roles, stack)
        yield serving, address, runtime_roles


async def _assert_stage_facts(reader, stage, recipes):
    facts = await reader.fetch(
        f"SELECT recipe_sha256,fact_json::text AS fact FROM pg_temp.{_identifier(stage.contribution_table_name)} "
        "ORDER BY recipe_sha256,ordinal"
    )
    documents = [json.loads(fact["fact"]) for fact in facts]
    assert len(documents) == stage.own_fact_rows == 8
    assert stage.reused_fact_rows == stage.copied_reused_fact_rows == 0
    assert {document["evidence_basis"][1] for document in documents if document["kind"] == "edge"} == {
        recipe.source_pin.source_id for recipe in recipes
    }
    provider_only_facts = [
        document for document in documents if document["kind"] == "edge" and document["provider_only"]
    ]
    assert len(provider_only_facts) == stage.provider_only_rows == 2
    assert {document["provider_id"] for document in provider_only_facts} == {"1000000004"}
    assert all(document["location_id"] is None for document in provider_only_facts)
    expected_locations = {str(registry_location_id(recipe.source_pin.source_id, "office-one")) for recipe in recipes}
    assert len(expected_locations) == 2
    assert {
        document["location_id"]
        for document in documents
        if document["kind"] == "edge" and not document["provider_only"]
    } == expected_locations
    assert all(
        (descriptor["membership_rows"], descriptor["provider_only_rows"], descriptor["raw_source_rows"]) == (1, 1, 2)
        for descriptor in stage.contribution_receipts
    )
    assert all(descriptor["descriptor_version"] == 2 for descriptor in stage.contribution_receipts)
    assert stage.membership_rows == 2 and stage.omitted_rows == stage.unresolved_rows == 0


async def _assert_alias_bindings(reader, stage, network_ids):
    members = await reader.fetch(f"SELECT input_json FROM pg_temp.{_identifier(stage.table_name)} ORDER BY ordinal")
    memberships = [json.loads(member["input_json"]) for member in members]
    assert len(memberships) == 2 and len({member["location_id"] for member in memberships}) == 2
    assert {member["network_id"] for member in memberships} == set(network_ids)
    assert {(member["provider_system"], member["provider_id"]) for member in memberships} == {("npi", "1234567893")}
    # This supported temporary target is a native COPY mechanism check, not a candidate or custody receipt.
    await offices.copy_witness_offices(reader, stage.office_matches, SimpleNamespace(schema_name="pg_temp"))
    bindings = await reader.fetch("SELECT * FROM pg_temp.provider_location_binding ORDER BY location_id")
    assert len(bindings) == 2 and len({binding["location_id"] for binding in bindings}) == 2
    assert {binding["location_key"] for binding in bindings} == {"1" * 64}
    union = await reader.fetch(
        f"""WITH members AS (
          SELECT (input_json::jsonb->>'network_id')::integer AS network_id,
            input_json::jsonb->>'provider_system' AS provider_system,
            input_json::jsonb->>'provider_id' AS provider_id,
            (input_json::jsonb->>'location_id')::uuid AS location_id
          FROM pg_temp.{_identifier(stage.table_name)}
        ) SELECT location_key,array_agg(DISTINCT network_id ORDER BY network_id) AS networks
        FROM members JOIN pg_temp.provider_location_binding USING(provider_system,provider_id,location_id)
        GROUP BY location_key"""
    )
    assert [tuple(record) for record in union] == [("1" * 64, list(network_ids))]


async def _assert_pruning_mechanisms(reader):
    """Synthetic encoded rows qualify native predicate mechanics only, not Source admission or correspondence."""
    table_name = "witness_alias_pruning_" + uuid4().hex
    await reader.execute(
        f"CREATE TEMP TABLE {_identifier(table_name)}(page_key text DEFAULT 'page',office jsonb NOT NULL) ON COMMIT DROP"
    )
    first_by_field = dict(
        provider_system="npi", provider_id="1234567893", location_id=str(UUID(int=1)), location_key="1" * 64
    )
    alias_by_field = {**first_by_field, "location_id": str(UUID(int=2))}
    unrelated_by_field = {
        **first_by_field,
        "provider_id": "1999999984",
        "location_id": str(UUID(int=3)),
        "location_key": "3" * 64,
    }
    null_provider_by_field = {**first_by_field, "provider_system": None}
    null_key_by_field = {**first_by_field, "location_key": None}
    null_provider_other_key_by_field = {**null_provider_by_field, "location_key": "2" * 64}
    cases = [
        (True, [first_by_field, alias_by_field], [first_by_field, alias_by_field]),
        (False, [first_by_field, alias_by_field], []),
        (True, [first_by_field, first_by_field], [first_by_field, first_by_field]),
        (False, [first_by_field, first_by_field], [first_by_field, first_by_field]),
        (True, [first_by_field, {**alias_by_field, "provider_id": "1000000004"}], []),
        (True, [first_by_field, {**first_by_field, "location_key": "2" * 64}], []),
        (True, [null_provider_by_field, first_by_field], []),
        (True, [null_key_by_field, first_by_field], [null_key_by_field, first_by_field]),
        (
            True,
            [null_provider_by_field, null_provider_other_key_by_field],
            [null_provider_by_field, null_provider_other_key_by_field],
        ),
        (
            True,
            [first_by_field, {**first_by_field, "location_key": "2" * 64}, unrelated_by_field],
            [unrelated_by_field],
        ),
    ]
    for source_aliases, documents, expected_offices in cases:
        await reader.execute(f"TRUNCATE pg_temp.{_identifier(table_name)}")
        await reader.copy_records_to_table(
            table_name,
            schema_name="pg_temp",
            columns=("office",),
            records=[(json.dumps(document),) for document in documents],
        )
        await legacy._prune_verified_offices(reader, table_name, source_aliases, {"page": None})
        actual_offices = [
            json.loads(matched_office_row["office"])
            for matched_office_row in await reader.fetch(
                f"SELECT office::text AS office FROM pg_temp.{_identifier(table_name)}"
            )
        ]
        assert sorted(json.dumps(office, sort_keys=True) for office in actual_offices) == sorted(
            json.dumps(office, sort_keys=True) for office in expected_offices
        )
    assert inspect.signature(legacy._finish_verified_office_matches).parameters["source_aliases"].default is False


async def test_genuine_source_aliases_union_networks_at_one_native_office(monkeypatch, record_property):
    async with source.native_generic_source(monkeypatch) as fixture:
        captures = await _publish_pair(fixture, monkeypatch)
        async with _alias_case(fixture, monkeypatch) as (serving, address, runtime_roles):
            recipes, approved, network_ids = await _approved_pair(fixture, serving, captures)
            async with _reader(fixture) as reader:
                backend = await reader.fetchval("SELECT pg_backend_pid()")
                async with reader.transaction(isolation="repeatable_read"):
                    assert (
                        await reader.fetchval("SELECT current_database()")
                        == fixture.component.admin_engine.url.database
                    )
                    assert await reader.fetchval(
                        "SELECT current_user=session_user AND current_user=$1", fixture.component.reader
                    )
                    stage = await stage_registry_source_recipes(
                        reader,
                        recipes,
                        approved,
                        request_id=uuid4(),
                        control_schema=serving[1],
                        inventory=True,
                        read_context=SourceRecipeReadContext(
                            verified_office_inputs=(address, runtime_roles), source_manifest=None
                        ),
                    )
                    await _assert_stage_facts(reader, stage, recipes)
                    await _assert_alias_bindings(reader, stage, network_ids)
                    await _assert_pruning_mechanisms(reader)
                    temporary_namespace = await reader.fetchval("SELECT pg_my_temp_schema()")
            assert not await fixture.admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", backend)
            assert not await fixture.admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=$1::oid)", temporary_namespace
            )
            record_property(
                "genuine_qualification",
                "two independently published/admitted/postcommit-captured Sources, real approvals, native closed EAU, actual v2 stage, alias COPY and network union",
            )
            record_property(
                "sql_mechanism_only",
                "different providers sharing a key and one source pair spanning two keys use synthetic encoded rows; they are not Source/candidate acceptance",
            )
            record_property("not_qualified", "compact composition, independent custody, publication, deployed serving")


async def _member_prune_inputs(connection):
    await connection.execute(
        "CREATE TEMP TABLE member_prune_rows(row_id integer,page_key text,provider_system text,provider_id text,"
        "location_id uuid,evidence_id text) ON COMMIT DROP"
    )
    await connection.execute("CREATE TEMP TABLE member_prune_matches(page_key text,office jsonb) ON COMMIT DROP")
    await connection.execute("CREATE INDEX ON member_prune_matches(page_key)")
    await connection.execute("CREATE INDEX ON member_prune_rows(page_key)")
    matched_by_field = dict(
        provider_system="provider_directory", provider_id="provider-one", location_id=str(UUID(int=1))
    )
    documents = [
        matched_by_field,
        matched_by_field,
        {**matched_by_field, "location_id": "{" + str(UUID(int=2)).upper() + "}"},
    ]
    documents.extend([{**matched_by_field, field: None} for field in matched_by_field])
    documents.append(
        {field: property_value for field, property_value in matched_by_field.items() if field != "location_id"}
    )
    await connection.executemany(
        "INSERT INTO member_prune_matches VALUES('page-a',$1::jsonb)",
        [(json.dumps(document),) for document in documents],
    )
    dimensions = [
        ("page-a", "provider_directory", "provider-one", UUID(int=1)),
        ("page-a", "provider_directory", "provider-one", UUID(int=1)),
        ("page-a", "provider_directory", "provider-one", UUID(int=2)),
        ("page-b", "provider_directory", "provider-one", UUID(int=1)),
        ("unlisted", "provider_directory", "provider-one", UUID(int=1)),
        ("empty", "provider_directory", "provider-one", UUID(int=1)),
        ("page-a", "npi", "provider-one", UUID(int=1)),
        ("page-a", "provider_directory", "different-provider", UUID(int=1)),
        ("page-a", "provider_directory", "provider-one", UUID(int=3)),
        (None, "provider_directory", "provider-one", UUID(int=1)),
        ("page-a", None, "provider-one", UUID(int=1)),
        ("page-a", "provider_directory", None, UUID(int=1)),
        ("page-a", "provider_directory", "provider-one", None),
    ]
    await connection.copy_records_to_table(
        "member_prune_rows",
        records=[(index, *identity, f"evidence-{index}") for index, identity in enumerate(dimensions)],
    )
    return await connection.fetch("SELECT * FROM member_prune_rows ORDER BY row_id")


async def test_native_member_pruning_keeps_exact_tuple_multiset_and_rolls_back_failures(serving_schema):
    connection, _, _ = serving_schema
    async with connection.transaction():
        original = await _member_prune_inputs(connection)
        proofs_by_page = {key: (0, None) for key in ("page-a", "page-b", "empty")}
        async with connection.transaction():
            await legacy._prune_unmatched_members(
                connection, "member_prune_rows", "member_prune_matches", proofs_by_page
            )
            assert await connection.fetch("SELECT * FROM member_prune_rows ORDER BY row_id") == original[:3]
        await connection.execute("TRUNCATE member_prune_rows")
        await connection.copy_records_to_table("member_prune_rows", records=[tuple(member) for member in original])
        with pytest.raises(asyncpg.InvalidTextRepresentationError):
            async with connection.transaction():
                await connection.execute(
                    "INSERT INTO member_prune_matches VALUES('page-b',"
                    "jsonb_build_object('provider_system','provider_directory','provider_id','provider-one',"
                    "'location_id','invalid-uuid'))"
                )
                await legacy._prune_unmatched_members(
                    connection, "member_prune_rows", "member_prune_matches", proofs_by_page
                )
        assert await connection.fetch("SELECT * FROM member_prune_rows ORDER BY row_id") == original

        async def cancel_after_first_page(statement, *arguments):
            if arguments == ("page-b",):
                assert await connection.fetchval("SELECT count(*) FROM member_prune_rows") < len(original)
                raise asyncio.CancelledError
            return await connection.execute(statement, *arguments)

        with pytest.raises(asyncio.CancelledError):
            async with connection.transaction():
                await legacy._prune_unmatched_members(
                    SimpleNamespace(execute=cancel_after_first_page),
                    "member_prune_rows",
                    "member_prune_matches",
                    proofs_by_page,
                )
        assert await connection.fetch("SELECT * FROM member_prune_rows ORDER BY row_id") == original
        await connection.execute("DELETE FROM member_prune_matches")
        await legacy._prune_unmatched_members(connection, "member_prune_rows", "member_prune_matches", proofs_by_page)
        assert await connection.fetchval("SELECT count(*) FROM member_prune_rows") == 0
    assert await connection.fetchval("SELECT to_regclass('pg_temp.member_prune_rows')") is None
    assert await connection.fetchval("SELECT to_regclass('pg_temp.member_prune_matches')") is None


def _global_prune_documents():
    first_by_field = dict(provider_system="provider_directory", provider_id="one", location_id=str(UUID(int=1)))
    documents = [("first", {**first_by_field, "location_key": "a"})] * 2
    documents.extend(
        [
            ("second", {**first_by_field, "location_id": str(UUID(int=2)), "location_key": "a"}),
            ("first", {**first_by_field, "provider_id": "two", "location_key": "b"}),
            ("second", {**first_by_field, "provider_id": "three", "location_key": "b"}),
            ("first", {**first_by_field, "provider_id": "four", "location_key": "c"}),
            ("second", {**first_by_field, "provider_id": "four", "location_key": "d"}),
            ("first", {**first_by_field, "provider_system": None, "location_key": "e"}),
            ("second", {**first_by_field, "location_key": "e"}),
            ("first", {**first_by_field, "provider_id": "five", "location_key": None}),
            ("second", {**first_by_field, "provider_id": "six", "location_key": None}),
            ("first", {**first_by_field, "provider_system": None, "location_key": "f"}),
            ("second", {**first_by_field, "provider_system": None, "location_key": "g"}),
            ("first", {**first_by_field, "location_id": None, "location_key": "h"}),
            ("second", {**first_by_field, "location_id": None, "location_key": "i"}),
        ]
    )
    escaped = '"\\' * 512
    documents.extend(
        [
            ("first", {**first_by_field, "provider_id": escaped, "location_key": "m"}),
            ("second", {**first_by_field, "provider_id": escaped, "location_key": "m"}),
            ("first", {**first_by_field, "provider_id": None, "location_key": "n"}),
            (
                "second",
                {
                    field: property_value
                    for field, property_value in {**first_by_field, "location_key": "n"}.items()
                    if field != "provider_id"
                },
            ),
        ]
    )
    original = "01234567-89ab-cdef-0123-456789abcdef"
    for key, identity in (("j", original), ("j", "{" + original.upper() + "}")):
        documents.append(
            (
                "first" if identity == original else "second",
                {**first_by_field, "location_id": identity, "location_key": key},
            )
        )
    for key, identity in (("k", original), ("l", "{" + original.upper() + "}")):
        documents.append(
            (
                "first" if identity == original else "second",
                {**first_by_field, "provider_id": "seven", "location_id": identity, "location_key": key},
            )
        )
    return documents


def _global_prune_oracle(documents, source_aliases):
    identities_by_location, locations_by_source = {}, {}
    for _, office in documents:
        source_fields = tuple(office.get(field) for field in ("provider_system", "provider_id", "location_id"))
        key = office.get("location_key")
        identities_by_location.setdefault(key, set()).add(source_fields[:2] if source_aliases else source_fields)
        if key is not None:
            locations_by_source.setdefault(source_fields, set()).add(key)
    ambiguous_keys = {
        key for key, identities in identities_by_location.items() if key is not None and len(identities) > 1
    }
    ambiguous_sources = {
        source_fields
        for source_fields, keys in locations_by_source.items()
        if None not in source_fields and len(keys) > 1
    }
    return [
        (page, office)
        for page, office in documents
        if office.get("location_key") not in ambiguous_keys
        and tuple(office.get(field) for field in ("provider_system", "provider_id", "location_id"))
        not in ambiguous_sources
    ]


async def _global_prune_table(connection, documents):
    name = "global_prune_" + uuid4().hex
    await connection.execute(f'CREATE TEMP TABLE "{name}"(page_key text,office jsonb) ON COMMIT DROP')
    await connection.execute(f'CREATE INDEX ON "{name}"(page_key)')
    await connection.copy_records_to_table(name, records=[(page, json.dumps(office)) for page, office in documents])
    return name


async def _assert_global_prune_rows(connection, name, expected):
    actual = await connection.fetch(f'SELECT page_key,office::text AS office FROM "{name}"')
    normalize = lambda page, office: (page, json.dumps(office, sort_keys=True))
    assert sorted(normalize(row["page_key"], json.loads(row["office"])) for row in actual) == sorted(
        normalize(page, office) for page, office in expected
    )
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'office_prune_%')"
    )


@pytest.mark.parametrize("source_aliases", [False, True])
@pytest.mark.parametrize("reverse_pages", [False, True])
async def test_native_global_pruning_preserves_cross_page_null_and_text_semantics(
    serving_schema, source_aliases, reverse_pages
):
    connection, _, _ = serving_schema
    documents = _global_prune_documents()
    expected = _global_prune_oracle(documents, source_aliases)
    async with connection.transaction():
        name = await _global_prune_table(connection, documents)
        pages = ("second", "first") if reverse_pages else ("first", "second")
        await legacy._prune_verified_offices(connection, name, source_aliases, dict.fromkeys(pages))
        await _assert_global_prune_rows(connection, name, expected)


async def test_native_global_pruning_rolls_back_population_and_late_delete_failures(serving_schema):
    connection, _, _ = serving_schema
    documents = _global_prune_documents()
    async with connection.transaction():
        name = await _global_prune_table(connection, documents)
        for phase in ("INSERT", "DELETE"):
            for cancelled in (False, True):

                async def fail_late(statement, *arguments):
                    if phase == "INSERT":
                        result = await connection.execute(statement, *arguments)
                    if statement.startswith(phase) and (phase == "INSERT" or arguments == ("second",)):
                        if phase == "DELETE":
                            assert await connection.fetchval(f'SELECT count(*) FROM "{name}"') < len(documents)
                        if cancelled:
                            raise asyncio.CancelledError
                        await connection.execute("SELECT 1/0")
                    return result if phase == "INSERT" else await connection.execute(statement, *arguments)

                controlled = SimpleNamespace(execute=fail_late, transaction=connection.transaction)
                error = asyncio.CancelledError if cancelled else asyncpg.DivisionByZeroError
                with pytest.raises(error):
                    await legacy._prune_verified_offices(controlled, name, True, {"first": None, "second": None})
                await _assert_global_prune_rows(connection, name, documents)
