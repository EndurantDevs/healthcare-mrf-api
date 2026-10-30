# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent Sources retain exact aliases while sharing one physical office."""

import json
from contextlib import AsyncExitStack, aclosing
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.network_address_scope import network_address_read_scope
from api.network_provider_read import NetworkOfficeSelection, read_network_provider_page
from process.network_approved_site_source import copy_approved_site_rows
from process.network_compact_address_carriers import _scratch
from process.network_compact_custom_preparation import compact_carrier_receipt
from process.network_custom_address_source import (
    NetworkCustomAddressSourceError,
    _binding_shape,
    _source_alias_layout,
    prepare_custom_address_source,
    verify_custom_address_source,
)
from process.network_initial_cms_office_bindings import _CHECK_SQL, _copy_source_office_records
from process.network_source_binding_store import apply_network_source_binding_batch
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_management_permissions import install_registry_management_permissions
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_inventory_store import source_inventory_entry
from process.registry_source_inventory_summaries import contribution_summary_v4
from process.registry_source_site_catalog import read_registry_source_sites
from tests import generic_fhir_capture_postgres_support as support
from tests import test_registry_generic_fhir_reuse_postgres as generic
from tests.test_network_custom_address_source_postgres import _arguments, _draft, _prepare, _seed, _verify
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_membership_draft_store_postgres import _command as _membership
from tests.test_network_membership_draft_store_postgres import _member
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create
from tests.test_registry_required_target_coverage_postgres import _read as _coverage
from tests.test_registry_required_target_coverage_postgres import coverage_db as coverage_db

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def alias_sources_db(custom_db, serving_schema):
    """Capture distinct original source identities before composing either recipe."""
    async with AsyncExitStack() as stack:
        sources = []
        for source_id in ("source-one", "source-two"):
            generator = await stack.enter_async_context(
                aclosing(support.legacy_capture_db.__wrapped__(SimpleNamespace(param=None, source_id=source_id)))
            )
            sources.append(await anext(generator))
        generator = await stack.enter_async_context(
            aclosing(generic.generic_composition_db.__wrapped__(sources[0], serving_schema, custom_db))
        )
        context = await anext(generator)
        fixture = context.fixture
        runtime_roles = tuple(
            sorted((fixture.executor_role, fixture.roles["reader"], fixture.custody_roles["custodian"]))
        )
        capture = await support._capture(
            sources[1], runtime_roles=tuple(sorted((*runtime_roles, sources[1].role_by_kind["reader"])))
        )
        other, approved, network_id = await support._approved_capture_recipe(sources[1], serving_schema, capture)
        context.recipes = (context.recipe, other)
        context.revision = approved.approved_revision
        context.network_ids = (context.network_id, network_id)
        context.native_stack = stack
        context.source_fixtures = sources
        context.serving_fixture = serving_schema
        context.runtime_roles = runtime_roles
        yield context


async def _compose(context, revision, previous=None, *, replacement_recipes=()):
    fixture = context.fixture
    request_id = uuid4()
    context.requests.append(request_id)
    target, address = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=request_id,
        approved_revision=revision,
        expected_head=0 if previous is None else previous.generation_id,
        source_manifest=previous,
        address_sources=RegistryCompositionAddressSources(
            context.address,
            compact=True,
            initial_source_recipes=context.recipes if previous is None else (),
            replacement_source_recipes=replacement_recipes,
        ),
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    context.targets.append(target)
    assert address == context.address
    return target


async def _read(context, manifest, networks, location_id=None, connection=None):
    connection = connection or context.fixture.execution_connection
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        with network_address_read_scope(manifest, tuple(sorted(networks))) as scope:
            return await read_network_provider_page(
                connection,
                scope,
                office_filters=NetworkOfficeSelection(location_id=location_id),
                control_schema=context.fixture.control_schema,
            )


async def _candidate(context, manifest):
    return dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(manifest.candidate_id),
        )
    )


async def _close_source(context, recipe):
    fixture, actor = context.fixture, _actor()
    binding_record = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.control_schema}".registry_network_binding WHERE NOT archived AND source_id=$1',
        recipe.source_pin.source_id,
    )
    command_by_field = dict(
        binding_id=str(binding_record["binding_id"]),
        **{
            key: binding_record[key]
            for key in (
                "source_system",
                "source_id",
                "dataset_schema",
                "dataset_id",
                "producer_id",
                "edition_id",
                "source_key",
                "network_id",
                "evidence_id",
                "evidence_sha256",
            )
        },
        source_scope_json=json.loads(binding_record["source_scope_json"]),
        operation="close",
        expected_revision=binding_record["revision"],
        expected_network_id=binding_record["network_id"],
    )
    command = support.NetworkSourceBindingBatchCommand(
        json.dumps([command_by_field]).encode(), "Close reviewed mapping", uuid4().hex
    )
    async with fixture.connection.transaction():
        binding = (
            await apply_network_source_binding_batch(
                fixture.connection,
                command,
                actor,
                control_schema=fixture.control_schema,
            )
        )["records"][0]
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, binding),
        actor,
    )
    return approved["approved_revision"]


async def _assert_adopted_alias_copy(context, manifest, receipt):
    """The physical COPY is bounded independently from retained alias accounting."""
    connection = context.fixture.execution_connection
    async with connection.transaction(isolation="repeatable_read"):
        async with _scratch(connection, manifest.compact_layout.base):
            for selected in (receipt, *(replace(receipt, records=(record,)) for record in receipt.records)):
                await copy_approved_site_rows(
                    connection,
                    (selected,),
                    "pg_temp",
                    exact_rows=True,
                    reproject_network_ids=True,
                    control_schema=context.fixture.control_schema,
                )
            assert await connection.fetchval("SELECT count(*) FROM pg_temp.entity_address_unified") == 1
            assert await connection.fetchval("SELECT count(*) FROM pg_temp.provider_location_binding") == 2
            assert {
                row["location_id"]
                for row in await connection.fetch("SELECT location_id FROM pg_temp.provider_location_binding")
            } == {UUID(record.location_id) for record in receipt.records}


async def test_old_sealed_receipt_still_refuses_removal_of_its_native_unique_guard(custom_db):
    fixture = custom_db
    receipt = await _prepare(fixture, (await _seed(fixture)).revision)
    assert receipt.as_dict()["revision"] == 2 and not receipt.source_aliases
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        name = await fixture.connection.fetchval(
            "SELECT quote_ident(conname) FROM pg_constraint WHERE conrelid=$1::oid AND contype='u'",
            receipt.binding_table_oid,
        )
        await fixture.connection.execute(
            f'ALTER TABLE "{receipt.address_source.schema_name}".provider_location_binding DROP CONSTRAINT {name}'
        )
        with pytest.raises(NetworkCustomAddressSourceError, match="binding shape differs"):
            await verify_custom_address_source(fixture.connection, receipt)
    finally:
        await transaction.rollback()
    assert await _verify(fixture, receipt) == receipt


@pytest.mark.parametrize("mode", ["single", "aliases", "manual", "cross_provider", "orphan"])
async def test_native_layout_refuses_incoherent_aliases_before_sealing(serving_schema, mode):
    connection, _, _ = serving_schema
    namespace = "mixed_alias_layout_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{namespace}"')
    try:
        await connection.execute(f'''CREATE TABLE "{namespace}".provider_location_binding(
            provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
            location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
            PRIMARY KEY(provider_system,provider_id,location_id))''')
        await connection.execute(f'''CREATE TABLE "{namespace}".entity_address_unified(
            location_key varchar(64) PRIMARY KEY,entity_type text NOT NULL,entity_id text NOT NULL)''')
        system = "manual" if mode == "manual" else "npi"
        identity = str(uuid4()) if mode == "manual" else "1234567893"
        if mode != "orphan":
            await connection.execute(
                f'INSERT INTO "{namespace}".entity_address_unified VALUES($1,$2,$3)', "a" * 64, system, identity
            )
        for ordinal in range(1 if mode == "single" else 2):
            await connection.execute(
                f'INSERT INTO "{namespace}".provider_location_binding VALUES($1,$2,$3,$4,$5,$6)',
                system,
                "1000000491" if mode == "cross_provider" and ordinal else identity,
                uuid4(),
                "a" * 64,
                system,
                identity,
            )
        if mode in ("manual", "cross_provider", "orphan"):
            with pytest.raises(NetworkCustomAddressSourceError, match="identities collide"):
                await _source_alias_layout(connection, namespace, writing=True)
        else:
            assert await _source_alias_layout(connection, namespace, writing=True) is (mode == "aliases")
            oid = await connection.fetchval("SELECT to_regclass($1)::oid", namespace + ".provider_location_binding")
            await _binding_shape(connection, oid, source_aliases=mode == "aliases")
    finally:
        await connection.execute(f'DROP SCHEMA "{namespace}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None


async def test_coverage_counts_exact_aliases_without_reverse_orphan_or_duplicate_office_failures(coverage_db):
    """Exercise the native coverage query; source authentication has its own fixture."""
    context, connection = coverage_db, coverage_db.fixture.connection
    namespace = '"' + context.schema + '"'
    aliases = (uuid4(), uuid4())
    for alias in aliases:
        await connection.execute(
            f"INSERT INTO {namespace}.provider_location_binding VALUES('npi','1000000004',$1,$2,'npi','1000000004')",
            alias,
            "a" * 64,
        )
    await connection.execute(
        f"UPDATE {namespace}.entity_address_unified SET canonical_network_ids=ARRAY[$1::integer] WHERE location_key=$2",
        context.seed.network["record_id"],
        "a" * 64,
    )
    for count, alias in enumerate(aliases, 2):
        await connection.execute(
            f"INSERT INTO {namespace}.network_membership(network_id,provider_system,provider_id,location_id,evidence_id) "
            "VALUES($1,'npi','1000000004',$2,$3)",
            context.seed.network["record_id"],
            alias,
            str(alias),
        )
        report = await _coverage(context)
        assert report["totals"]["exact_membership_count"] == count
        for evidence in report["provenance"]["target_evidence"]:
            if evidence["mapping_status"] == "resolved":
                assert evidence["serving_integrity_valid"]
                assert evidence["orphan_selected_binding_count"] == 0
                assert not evidence["duplicated_join_or_office"]


async def _approve_retained_aliases(context, manifest):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        page = await read_registry_source_sites(
            fixture.execution_connection,
            generation_id=manifest.generation_id,
            provider_system="npi",
            provider_id="1234567893",
            control_schema=fixture.control_schema,
        )
    return await _approve_alias_fields(context, [site["fields"] for site in page["sites"]])


async def _approve_alias_fields(context, choices):
    fixture = context.fixture
    actor, site_records = _actor(), []
    sessions = await _alias_management_sessions(context)
    for fields_by_name in choices:
        command = RegistryRecordCommand(
            "site_binding", uuid4(), "create", 0, fields_by_name, "Review retained office", uuid4().hex
        )
        async with sessions() as session, session.begin():
            site_records.append(
                await apply_registry_record_command(
                    session,
                    command,
                    actor,
                    schema=fixture.control_schema,
                    source_schema=fixture.source_schema,
                )
            )
    network = await _draft(fixture, _create("network"), actor)
    members = [
        _member(network["record_id"], "1234567893", fields_by_name["location_id"], system="npi")
        for fields_by_name in choices
    ]
    membership = await _draft(fixture, _membership(network["record_id"], members), actor)
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, network, membership, *site_records),
        actor,
    )
    return network["record_id"], approved["approved_revision"]


async def _cleanup_alias_management(fixture, resources):
    if resources.engine is not None:
        await resources.engine.dispose()
        assert resources.engine.pool.checkedout() == 0
    actual_oid = await fixture.connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", resources.role)
    if actual_oid is not None:
        assert actual_oid == resources.oid
        await fixture.connection.execute(f'DROP OWNED BY "{resources.role}"')
        await fixture.connection.execute(f'DROP ROLE "{resources.role}"')
    assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", resources.role)


async def _alias_management_sessions(context):
    fixture = context.fixture
    resources = SimpleNamespace(role="alias_management_" + uuid4().hex, oid=None, engine=None)
    context.native_stack.push_async_callback(_cleanup_alias_management, fixture, resources)
    password = uuid4().hex
    async with fixture.connection.transaction():
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", resources.role
        )
        await fixture.connection.execute(
            f'CREATE ROLE "{resources.role}" LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE '
            f"NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
        )
        resources.oid = await fixture.connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", resources.role)
        await fixture.connection.execute(
            f'GRANT "{fixture.roles["reader"]}" TO "{resources.role}" WITH INHERIT TRUE, SET FALSE'
        )
        await install_registry_management_permissions(
            fixture.connection,
            api_role=resources.role,
            owner_role=fixture.roles["owner"],
            control_schema=fixture.control_schema,
        )
    resources.engine = create_async_engine(
        fixture.execution_url.set(username=resources.role, password=password),
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
        isolation_level="REPEATABLE READ",
    )
    sessions = async_sessionmaker(resources.engine)
    async with sessions() as session, session.begin():
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        identity = await driver.fetchrow("""SELECT current_user::text,session_user::text,rolcanlogin,
          rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls AS elevated
          FROM pg_roles WHERE rolname=current_user""")
        assert tuple(identity) == (resources.role, resources.role, True, False)
        assert not await driver.fetchval("""SELECT EXISTS(SELECT FROM pg_proc
          WHERE oid=ANY(ARRAY['bttextcmp(text,text)'::regprocedure,'btboolcmp(boolean,boolean)'::regprocedure])
            AND pg_has_role(current_user,proowner,'MEMBER'))""")
    return sessions


async def _prepare_alias_artifact(context, arguments):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        return await prepare_custom_address_source(fixture.execution_connection, fixture.base, **arguments)


async def _verify_alias_artifact(context, receipt):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        return await verify_custom_address_source(fixture.execution_connection, receipt)


async def _changed_native_generation(context, monkeypatch, field):
    from tests import registry_compact_source_office_postgres_support as native

    original_storage = native._native_storage

    async def storage(fixture, schema, roles_by_kind, namespaces, roles, office_setup=None):
        async def changed_offices(connection, selected_schema):
            await office_setup(connection, selected_schema)
            await connection.execute(
                f'UPDATE "{selected_schema}".entity_address_unified SET {field}=$1 WHERE location_key=$2',
                42.0 if field == "lat" else "Synthetic alternate display",
                "1" * 64,
            )

        await original_storage(fixture, schema, roles_by_kind, namespaces, roles, changed_offices)

    with monkeypatch.context() as selected:
        selected.setattr(native, "_native_storage", storage)
        return await generic._native_generic_offices(
            context.fixture,
            context.source_fixtures[0],
            context.serving_fixture,
            context.runtime_roles,
            context.native_stack,
        )


@pytest.mark.parametrize("field", ["lat", "entity_name"])
async def test_mixed_aliases_refuse_conflicting_full_rows_from_authenticated_generations(
    alias_sources_db, monkeypatch, field
):
    context, fixture = alias_sources_db, alias_sources_db.fixture
    original = await generic._publish(context, await _compose(context, context.revision), first=True)
    context.address = await _changed_native_generation(context, monkeypatch, field)
    current = await generic._publish(
        context, await _compose(context, context.revision, original, replacement_recipes=context.recipes)
    )
    first = await _catalog_alias_receipt(context, original, await _network_aliases(context, original))
    second = await _catalog_alias_receipt(context, current, await _network_aliases(context, current))
    choices = [
        {
            "source_generation": pin.generation_id,
            **{
                name: getattr(adoption, name)
                for name in (
                    "provider_system",
                    "provider_id",
                    "location_id",
                    "location_key",
                    "address_row_sha256",
                )
            },
        }
        for pin, adoption in ((original, first.records[0]), (current, second.records[1]))
    ]
    assert choices[0]["location_key"] == choices[1]["location_key"]
    assert choices[0]["address_row_sha256"] != choices[1]["address_row_sha256"]
    _, revision = await _approve_alias_fields(context, choices)
    arguments = _arguments(fixture, revision, owner_role=fixture.writer_roles["owner_role"])
    with pytest.raises(RetainedSiteAdoptionError, match="conflicts with replacement"):
        await _prepare_alias_artifact(context, arguments)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_composition_" + arguments["composition_id"].replace("-", "")
        )
        is None
    )


async def _network_aliases(context, manifest):
    aliases_by_network = {}
    for network_id in context.network_ids:
        page = await _read(context, manifest, (network_id,))
        aliases_by_network[network_id] = page["providers"][0]["locations"][0]["location_id"]
    return aliases_by_network


async def test_v5_recipe_preserves_both_aliases_and_one_office(alias_sources_db):
    context, fixture = alias_sources_db, alias_sources_db.fixture
    manifest = await generic._publish(context, await _compose(context, context.revision), first=True)
    network_id, revision = await _approve_retained_aliases(context, manifest)
    arguments = _arguments(fixture, revision, owner_role=fixture.writer_roles["owner_role"])
    receipt = await _prepare_alias_artifact(context, arguments)
    assert receipt.as_dict()["revision"] == 5 and receipt.source_aliases
    assert await _verify_alias_artifact(context, receipt) == receipt
    assert await _prepare_alias_artifact(context, arguments) == receipt
    namespace = '"' + receipt.address_source.schema_name + '"'
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM {namespace}.entity_address_unified WHERE entity_id='1234567893'"
        )
        == 1
    )
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM {namespace}.provider_location_binding WHERE provider_id='1234567893'"
        )
        == 2
    )
    retained = await generic._publish(context, await _compose(context, revision, manifest))
    carrier = compact_carrier_receipt(await _candidate(context, retained))
    assert carrier.selected_address_rows == 1 and carrier.selected_binding_rows == 2
    assert carrier.materialized_address_rows == 0
    page = await _read(context, retained, (network_id,))
    assert page["total_count"] == 1 and len(page["providers"][0]["locations"]) == 1


async def _source_descriptors(context, manifest):
    candidate = await _candidate(context, manifest)
    inventory = verify_source_recipe_metadata(candidate).inventory
    assert inventory is not None
    assert (inventory.root.entry_count, inventory.root.contribution_count, inventory.root.derivation_count) == (2, 2, 2)
    rows = await context.fixture.connection.fetch(
        f'SELECT * FROM "{manifest.schema_name}".network_source_inventory ORDER BY ordinal'
    )
    entries = tuple(source_inventory_entry(row) for row in rows)
    assert source_inventory_root(entries) == inventory.root
    descriptors = tuple(json.loads(entry.contribution_json) for entry in entries)
    summary = verify_registry_source_contributions(candidate)
    assert summary["revision"] == 4 and summary["source_count"] == 2 and "sources" not in summary
    assert (
        contribution_summary_v4(inventory.root, sorted(descriptors, key=lambda item: item["recipe_sha256"])) == summary
    )
    return descriptors


async def _assert_selected_aliases(context, original):
    fixture = context.fixture
    binding_records = await fixture.connection.fetch(
        f'SELECT * FROM "{original.schema_name}".provider_location_binding ORDER BY location_id'
    )
    assert len(binding_records) == 2 and len({binding_record["location_id"] for binding_record in binding_records}) == 2
    assert len({binding_record["location_key"] for binding_record in binding_records}) == 1
    assert {recipe.source_pin.source_id for recipe in context.recipes} == {"source-one", "source-two"}
    candidate = await _candidate(context, original)
    summary = verify_initial_source_office_receipt(candidate)
    assert (summary["revision"], summary["recipe_count"], summary["office_rows"]) == (5, 2, 2)
    contributions = await _source_descriptors(context, original)
    assert all(contribution["membership_rows"] >= 1 for contribution in contributions)
    aliases_by_network = {}
    for network_id in context.network_ids:
        page = await _read(context, original, (network_id,))
        assert page["total_count"] == 1 and len(page["providers"][0]["locations"]) == 1
        aliases_by_network[network_id] = page["providers"][0]["locations"][0]["location_id"]
    assert len(set(aliases_by_network.values())) == 2
    source_rows = await fixture.connection.fetch(
        f'SELECT location_id,evidence_id FROM "{original.schema_name}".network_membership_source'
    )
    assert {str(binding_record["location_id"]) for binding_record in source_rows} == set(aliases_by_network.values())
    assert len({binding_record["evidence_id"] for binding_record in source_rows}) >= 2
    union = await _read(context, original, context.network_ids)
    assert len(union["providers"][0]["locations"]) == 1
    assert union["providers"][0]["locations"][0]["location_id"] == min(aliases_by_network.values())
    for network_id, alias in aliases_by_network.items():
        exact = await _read(context, original, context.network_ids, alias)
        assert exact["providers"][0]["locations"][0]["location_id"] == alias
        other = next(key for key in context.network_ids if key != network_id)
        assert (await _read(context, original, (other,), alias))["total_count"] == 0

    return aliases_by_network, union, contributions


async def _catalog_alias_receipt(context, original, aliases_by_network):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        arguments_by_name = dict(
            generation_id=original.generation_id,
            provider_system="npi",
            provider_id="1234567893",
            limit=1,
            control_schema=fixture.control_schema,
        )
        first = await read_registry_source_sites(fixture.execution_connection, **arguments_by_name)
        second = await read_registry_source_sites(fixture.execution_connection, **arguments_by_name, offset=1)
        assert first["has_more"] and not second["has_more"]
        fields = [site["fields"] for page in (first, second) for site in page["sites"]]
        assert {field["location_id"] for field in fields} == set(aliases_by_network.values())
        receipt = await resolve_retained_site_adoptions(
            fixture.execution_connection,
            original,
            json.dumps(
                [{key: value for key, value in field.items() if key != "source_generation"} for field in fields]
            ).encode(),
            control_schema=fixture.control_schema,
        )
        assert len(receipt.records) == 2 and len({row.address_row_sha256 for row in receipt.records}) == 1
    return receipt


async def test_two_authenticated_sources_share_office_without_sharing_alias_membership(alias_sources_db, monkeypatch):
    context, fixture = alias_sources_db, alias_sources_db.fixture
    copy_target = await _compose(context, context.revision)
    original = await generic._publish(context, copy_target, first=True)
    assert await generic._publish(context, copy_target) == original
    aliases, union, contributions = await _assert_selected_aliases(context, original)
    receipt = await _catalog_alias_receipt(context, original, aliases)
    await _assert_adopted_alias_copy(context, original, receipt)

    async def forbidden(*arguments, **options):
        pytest.fail("unchanged authenticated source facts must retain their original owner")

    from process import registry_source_recipe_composition as composition

    monkeypatch.setattr(composition, "_read_recipe_page", forbidden)
    monkeypatch.setattr(composition, "reuse_fhir_facts", forbidden)
    async with generic._pinned_reader(context, original) as old_reader:
        revision = await _close_source(context, context.recipes[0])
        retained = await generic._publish(context, await _compose(context, revision, original))
        assert (await _read(context, retained, (context.network_ids[0],)))["total_count"] == 0
        assert (await _read(context, retained, (context.network_ids[1],)))["total_count"] == 1
        assert len((await _read(context, retained, context.network_ids))["providers"][0]["locations"]) == 1
        assert await _read(context, original, context.network_ids, connection=old_reader) == union
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{retained.schema_name}".network_source_contribution'
            )
            == 0
        )
        rebound = await _source_descriptors(context, retained)
        assert {contribution["fact_candidate_id"] for contribution in rebound} == {original.candidate_id}
        assert {contribution["fact_sha256"] for contribution in rebound} == {
            contribution["fact_sha256"] for contribution in contributions
        }
        summary = verify_initial_source_office_receipt(await _candidate(context, retained))
        assert (summary["revision"], summary["recipe_count"], summary["office_rows"]) == (5, 2, 1)
    revision = await _close_source(context, context.recipes[1])
    removed = await generic._publish(context, await _compose(context, revision, retained))
    assert (await _read(context, removed, context.network_ids))["total_count"] == 0
    assert await _read(context, original, context.network_ids) == union


@pytest.mark.parametrize("damage", [None, "ambiguous_alias", "cross_provider", "address", "hash"])
async def test_native_alias_validation_retains_identity_and_full_address_guards(serving_schema, damage):
    connection, _, _ = serving_schema
    canonical_by_field = {"identity_key": "synthetic|street", "address_key": str(uuid4())}
    office_by_field = dict(
        provider_id="1234567893",
        location_id=str(uuid4()),
        location_key="a" * 64,
        canonical=canonical_by_field,
        address=["123 Example St", None, "Chicago", "IL", "60601", "US"],
        address_row_sha256="b" * 64,
    )
    alias_by_field = {**office_by_field, "location_id": str(uuid4())}
    if damage == "ambiguous_alias":
        alias_by_field.update(location_id=office_by_field["location_id"], location_key="c" * 64)
    elif damage == "cross_provider":
        alias_by_field["provider_id"] = "1000000491"
    elif damage == "address":
        alias_by_field["address"] = ["999 Other St", None, "Chicago", "IL", "60601", "US"]
    elif damage == "hash":
        alias_by_field["address_row_sha256"] = "d" * 64
    checked = await connection.fetchrow(
        _CHECK_SQL, json.dumps([office_by_field, alias_by_field]), json.dumps([canonical_by_field, canonical_by_field])
    )
    assert checked["valid"] is (damage is None)
    if damage is None:
        assert len(json.loads(checked["bindings_json"])) == 2
        namespace = "source_office_alias_" + uuid4().hex
        await connection.execute(f'CREATE SCHEMA "{namespace}"')
        try:
            async with connection.transaction():
                for alias in (office_by_field, alias_by_field):
                    await _copy_source_office_records(
                        connection,
                        SimpleNamespace(schema_name=namespace),
                        [
                            (
                                "npi",
                                alias["provider_id"],
                                UUID(alias["location_id"]),
                                alias["location_key"],
                                "npi",
                                alias["provider_id"],
                            )
                        ],
                    )
                assert await connection.fetchval(f'SELECT count(*) FROM "{namespace}".provider_location_binding') == 2
        finally:
            await connection.execute(f'DROP SCHEMA "{namespace}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
