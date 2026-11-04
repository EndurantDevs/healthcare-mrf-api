# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Source export, typed projection, independent raw checker and rollback."""

import asyncio
import copy
import json
from dataclasses import replace

import pytest

from process import network_fhir_typed_source as typed
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_typed_contract import CELL_COLUMNS, PAIR_COLUMNS, TypedFHIRGraphError, validate_graph_document
from tests import generic_source_successor_postgres_support as source
from tests.source_witness_capture_postgres_support import source_witness_case
from tests.test_verified_office_source_witnesses_v2_postgres import _resource

pytestmark = pytest.mark.asyncio
_ORIGINAL_ROWS = source._edition_rows
_AS_OF = "2026-10-01"


def _arguments(case):
    return {"source_profile": case.source_profile, "reviewed_origin": case.origin, "membership_as_of": _AS_OF}


async def _stages(connection):
    return await connection.fetchval(
        "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname ~ '^fhir_typed_'"
    )


async def test_genuine_typed_source_complete_export_and_independent_replay(monkeypatch):
    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction(isolation="repeatable_read"):
            async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
                document = validate_graph_document(json.loads(json.dumps(stage.document)))
                assert document["derivation_version"] == 3
                assert document["origin"]["dataset_id"] == case.origin.dataset_id
                rows = await case.writer.fetch("SELECT " + ",".join(PAIR_COLUMNS) + " FROM " + stage.relation("pairs"))
                assert len(rows) == 1
                pair = dict(rows[0])
                assert pair["network_source_key"] == "network-one" and pair["provider_id"] == "1234567893"
                assert pair["location_id"] == registry_location_id(source.SOURCE, "office-one")
                assert pair["reference_rows"] == 1 and pair["canonical_identity"].endswith("|street")
                assert document["counts"]["source_rows"] == sum(dict(case.origin.resource_counts).values())
                assert document["counts"]["projected_rows"] == document["counts"]["source_rows"] - 1
                assert (
                    await case.writer.fetchval(
                        "SELECT count(*) FROM "
                        + stage.relation("cells")
                        + " WHERE resource_type='PractitionerRole' AND cardinality(location_refs)=0"
                    )
                    == 1
                )
                assert "payload_json" not in CELL_COLUMNS
            assert await _stages(case.writer) == 0
            with pytest.raises(TypedFHIRGraphError, match="workspace_closed"):
                await stage.verify(case.writer)


def _strict_rows(edition, dataset):
    rows = _ORIGINAL_ROWS(edition, dataset)
    template = copy.deepcopy(next(row["payload_json"] for row in rows if row["resource_id"] == "role-office"))
    changes = {
        "absolute": {
            "practitioner_ref": source.BASE + "/Practitioner/provider-one",
            "network_refs": [source.BASE + "/Organization/network-one"],
            "location_refs": [source.BASE + "/Location/office-one"],
        },
        "foreign-network": {"network_refs": ["https://foreign.example.test/fhir/Organization/network-one"]},
        "foreign-location": {"location_refs": ["https://foreign.example.test/fhir/Location/office-one"]},
        "missing-parent": {"practitioner_ref": "Practitioner/missing", "organization_ref": "Organization/provider-org"},
        "substituted-npi": {"npi": "1000000004"},
        "wrong-kind": {"practitioner_ref": "Organization/provider-org"},
        "query-ref": {"location_refs": ["Location/office-one?version=1"]},
        "inactive": {"active": False},
        "expired": {"period_end": "2026-09-30"},
        "future": {"period_start": "2026-10-02"},
        "acceptance": {"network_refs": [], "accepting_patients": [{"from_network_ref": "Organization/network-one"}]},
        "service": {"location_refs": [], "healthcare_service_refs": ["HealthcareService/service-one"]},
        "organization-address": {"location_refs": [], "organization_ref": "Organization/provider-org"},
    }
    rows += [_resource("PractitionerRole", name, {**template, **fields}, dataset) for name, fields in changes.items()]
    rows += [
        _resource(
            "Organization",
            "provider-org",
            {"npi": "1000000004", "name": "Provider", "first_line": "123 Example St"},
            dataset,
        ),
        _resource("Organization", "carrier-org", {"npi": "1000000491", "name": "Carrier"}, dataset),
        _resource("HealthcareService", "service-one", {"location_refs": ["Location/office-one"]}, dataset),
        _resource(
            "OrganizationAffiliation",
            "participant",
            {
                "participating_organization_ref": "Organization/provider-org",
                "organization_ref": "Organization/carrier-org",
                "network_refs": ["Organization/network-one"],
                "location_refs": ["Location/office-one"],
            },
            dataset,
        ),
        _resource(
            "OrganizationAffiliation",
            "missing-participant",
            {
                "organization_ref": "Organization/carrier-org",
                "network_refs": ["Organization/network-one"],
                "location_refs": ["Location/office-one"],
            },
            dataset,
        ),
    ]
    return rows


async def test_strict_subject_reference_and_acceptance_boundaries(monkeypatch):
    monkeypatch.setattr(source, "_edition_rows", _strict_rows)
    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction(isolation="repeatable_read"):
            async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
                actual = await case.writer.fetch(
                    "SELECT resource_id,provider_id,reference_rows FROM "
                    + stage.relation("pairs")
                    + " ORDER BY resource_id"
                )
                assert [tuple(row) for row in actual] == [
                    ("absolute", "1234567893", 1),
                    ("participant", "1000000004", 1),
                    ("role-office", "1234567893", 1),
                ]
                assert stage.document["counts"]["provider_only_roles"] == 1


async def test_raw_checker_refuses_producer_npi_substitution(monkeypatch):
    original = typed._project_page

    async def corrupt(connection, stage, rows):
        await original(connection, stage, rows)
        await connection.execute(
            "UPDATE " + stage.relation("cells") + " SET npi='1000000004' WHERE resource_type='Practitioner'"
        )

    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction(isolation="repeatable_read"):
            with monkeypatch.context() as patch:
                patch.setattr(typed, "_project_page", corrupt)
                with pytest.raises(TypedFHIRGraphError, match="raw_graph_differs"):
                    async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)):
                        pytest.fail("producer substitution admitted")
            assert await _stages(case.writer) == 0
            async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
                assert stage.document["counts"]["pair_rows"] == 1


async def test_authority_alias_and_cancelled_source_restart_at_zero(monkeypatch):
    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction(isolation="repeatable_read"):
            with pytest.raises(ValueError):
                async with typed.typed_source_graph(lambda **_kwargs: None, case.writer, **_arguments(case)):
                    pytest.fail("forged factory admitted")
            changed = replace(case.origin, source_aliases=tuple(sorted((*case.origin.source_aliases, "foreign-alias"))))
            with pytest.raises(ValueError):
                async with typed.typed_source_graph(
                    case.source_export, case.writer, **{**_arguments(case), "reviewed_origin": changed}
                ):
                    pytest.fail("different alias scope admitted")
            original = typed._project_page

            async def cancel(connection, stage, rows):
                await original(connection, stage, rows)
                asyncio.current_task().cancel()
                await asyncio.sleep(0)

            with monkeypatch.context() as patch:
                patch.setattr(typed, "_project_page", cancel)
                with pytest.raises(asyncio.CancelledError):
                    async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)):
                        pytest.fail("cancelled Source pass completed")
            assert await _stages(case.writer) == 0
            async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
                assert stage.document["counts"]["source_rows"] == sum(dict(case.origin.resource_counts).values())


def _fanout_rows(edition, dataset):
    rows = _ORIGINAL_ROWS(edition, dataset)
    template = copy.deepcopy(next(row["payload_json"] for row in rows if row["resource_id"] == "role-office"))
    location = copy.deepcopy(next(row["payload_json"] for row in rows if row["resource_id"] == "office-one"))
    rows += [
        _resource("Organization", "network-" + str(number), {"name": "Network " + str(number)}, dataset)
        for number in range(6)
    ]
    rows += [
        _resource("Location", "fanout-" + str(number), {**location, "second_line": "Suite " + str(number)}, dataset)
        for number in range(2257)
    ]
    rows.append(
        _resource(
            "PractitionerRole",
            "fanout",
            {
                **template,
                "network_refs": ["Organization/network-" + str(number) for number in range(6)],
                "location_refs": ["Location/fanout-" + str(number) for number in range(2257)],
            },
            dataset,
        )
    )
    rows.append(
        _resource(
            "PractitionerRole", "repeated", {**template, "location_refs": ["Location/office-one"] * 13542}, dataset
        )
    )
    return rows


async def test_native_13542_unique_and_repeated_edges_remain_streamed(monkeypatch):
    monkeypatch.setattr(source, "_edition_rows", _fanout_rows)
    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction(isolation="repeatable_read"):
            async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
                assert (
                    await case.writer.fetchval(
                        "SELECT count(*) FROM " + stage.relation("pairs") + " WHERE resource_id='fanout'"
                    )
                    == 13542
                )
                assert (
                    await case.writer.fetchval(
                        "SELECT reference_rows FROM " + stage.relation("pairs") + " WHERE resource_id='repeated'"
                    )
                    == 13542
                )
                from datetime import date

                query = typed.PAIR_SQL.replace("{cells}", stage.relation("cells")).replace(
                    "{addresses}", stage.relation("addresses")
                )
                plan = json.loads(
                    await case.writer.fetchval(
                        "EXPLAIN (FORMAT JSON) " + query,
                        case.origin.source_uri.rstrip("/"),
                        date.fromisoformat(_AS_OF),
                        "",
                        "",
                        "PractitionerRole",
                        "zzzz",
                    )
                )[0]["Plan"]
                pending, keyed = [plan], []
                while pending:
                    node = pending.pop()
                    pending.extend(node.get("Plans", ()))
                    if (
                        node.get("Alias", "").split("_", 1)[0] in {"p", "net", "loc", "l"}
                        and node["Node Type"].endswith("Scan")
                        and "Index Cond" in node
                    ):
                        keyed.append(node)
                assert {node["Alias"].split("_", 1)[0] for node in keyed} == {"p", "net", "loc", "l"}, json.dumps(plan)
                assert all("resource_id" in node["Index Cond"] for node in keyed)
                # Independent declared truth: every integer network/location product, plus two existing offices.
                assert await case.writer.fetchval(
                    "SELECT NOT EXISTS((SELECT 'network-'||n::text,'fanout-'||l::text FROM generate_series(0,5) n CROSS JOIN generate_series(0,2256) l EXCEPT ALL SELECT network_source_key,location_resource_id FROM "
                    + stage.relation("pairs")
                    + " WHERE resource_id='fanout') UNION ALL (SELECT network_source_key,location_resource_id FROM "
                    + stage.relation("pairs")
                    + " WHERE resource_id='fanout' EXCEPT ALL SELECT 'network-'||n::text,'fanout-'||l::text FROM generate_series(0,5) n CROSS JOIN generate_series(0,2256) l))"
                )


async def test_persistent_typed_capture_closed_native_reader_and_independent_replay(monkeypatch):
    from uuid import uuid4

    import asyncpg

    from process import network_fhir_typed_capture as custody
    from process.network_fhir_source_witness_contract import canonical_timestamp

    async with source_witness_case(monkeypatch) as case:
        from tests.source_witness_capture_postgres_support import _observed_source_descriptor, source_modules

        catalog, _ = source_modules()
        descriptor, _ = await _observed_source_descriptor(
            case.source_runtime, case.source_profile, case.fixture.candidates[-1], catalog
        )
        identity = uuid4()
        schema = custody.PREFIX + identity.hex
        case.fixture.namespaces.append(schema)
        async with case.writer.transaction(isolation="repeatable_read"):
            anchor = custody.TypedGraphAnchor(
                str(identity), canonical_timestamp(await case.writer.fetchval("SELECT transaction_timestamp()"))
            )
            document = await custody.capture_typed_source_graph(
                case.source_export,
                case.writer,
                **{
                    **_arguments(case),
                    "membership_as_of": case.origin.semantic_projection_as_of or anchor.observed_at[:10],
                },
                expected_anchor=anchor,
                owner_role=case.fixture.role_by_kind["owner"],
                runtime_roles=(case.fixture.component.reader,),
                catalog_descriptor=descriptor,
            )
            assert document["descriptor_version"] == 4
            assert set(document["catalog"]["relation_oids"]) == {"cells", "addresses", "pairs"}
        # Precreate only temporary checker scratch before the genuine Reader read-only snapshot.
        names = {kind: "typed_replay_" + kind + "_" + uuid4().hex for kind in typed._DDL}
        for kind, definition in typed._DDL.items():
            await case.reader.execute('CREATE TEMP TABLE "' + names[kind] + '"(' + definition + ")")
        await case.reader.execute('CREATE INDEX ON "' + names["addresses"] + '"(address_key)')
        try:
            async with case.reader.transaction(isolation="repeatable_read", readonly=True):
                replay = await typed.bind_typed_graph_stage(case.reader, schema_name="pg_temp", table_names=names)
                stage = await custody.verify_typed_graph_capture(
                    case.source_export,
                    case.reader,
                    document,
                    source_profile=case.source_profile,
                    reviewed_origin=case.origin,
                    expected_anchor=anchor,
                    replay_stage=replay,
                )
                assert stage.checked and stage.document["counts"]["pair_rows"] == 1
                from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

                resolved = await custody.resolve_typed_graph_capture_reference(
                    case.reader,
                    expected_anchor=anchor,
                    capture_sha256=document["capture_sha256"],
                    catalog_generation=fhir_catalog_source_generation(descriptor),
                    owner_role=case.fixture.role_by_kind["owner"],
                    runtime_roles=(case.fixture.component.reader,),
                )
                assert resolved == document
                with pytest.raises(TypedFHIRGraphError, match="reference_context"):
                    await custody.resolve_typed_graph_capture_reference(
                        case.reader,
                        expected_anchor=anchor,
                        capture_sha256="0" * 64,
                        catalog_generation=fhir_catalog_source_generation(descriptor),
                        owner_role=case.fixture.role_by_kind["owner"],
                        runtime_roles=(case.fixture.component.reader,),
                    )
                assert await case.reader.fetchval("SELECT session_user::text=current_user::text")
                assert await case.reader.fetchval("SHOW transaction_read_only") == "on"
                wrong = custody.TypedGraphAnchor(str(uuid4()), anchor.observed_at)
                with pytest.raises(TypedFHIRGraphError, match="anchor"):
                    await custody.require_typed_graph_capture(case.reader, document, expected_anchor=wrong)
            async with case.reader.transaction():
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with case.reader.transaction():
                        await case.reader.execute('DELETE FROM "' + schema + '".source_pairs')
            async with case.writer.transaction(isolation="repeatable_read"):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with case.writer.transaction():
                        await case.writer.execute('UPDATE "' + schema + '".source_cells SET npi=NULL')
        finally:
            for name in names.values():
                await case.reader.execute('DROP TABLE "' + name + '"')


async def test_typed_approved_catalog_and_native_office_mapping(monkeypatch):
    from dataclasses import asdict

    from process import network_fhir_typed_mapping as mapping
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
    from tests.source_witness_capture_postgres_support import _observed_source_descriptor, source_modules
    from tests.test_network_approved_source_bindings_postgres import _apply, _binding, _pin
    from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

    async with source_witness_case(monkeypatch) as case:
        async with source.generic_refresh_execution(case.fixture, monkeypatch) as (execution, serving, _stack, _head):
            connection, schema, engine = serving
            catalog, _ = source_modules()
            descriptor, _ = await _observed_source_descriptor(
                case.source_runtime, case.source_profile, case.fixture.candidates[-1], catalog
            )
            coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
            actor = _actor()
            network = await _draft(engine, schema, _create("network"), actor)
            generation = fhir_catalog_source_generation(descriptor)
            bindings = [
                _binding(
                    network["record_id"],
                    **asdict(coordinates),
                    source_key="network-one",
                    source_scope_json={"organization_id": "network-one", "source_generation": generation},
                    evidence_id=generation + ":Organization/network-one",
                )
            ]
            receipt = await _apply(connection, schema, actor, bindings)
            await _approve(connection, schema, await _command(connection, schema, network, *receipt["records"]), actor)
            approved = await _pin(connection, schema)
            await connection.execute(
                'GRANT USAGE ON SCHEMA "' + schema + '" TO "' + case.fixture.component.reader + '"'
            )
            await connection.execute(
                'GRANT SELECT ON ALL TABLES IN SCHEMA "' + schema + '" TO "' + case.fixture.component.reader + '"'
            )
            # This is the genuine Source Reader; no Source writer or catalog credentials are borrowed.
            async with case.reader.transaction(isolation="repeatable_read"):
                async with typed.typed_source_graph(case.source_export, case.reader, **_arguments(case)) as stage:
                    result = await mapping.prepare_typed_graph_mapping(
                        case.reader,
                        stage,
                        approved_source=approved,
                        binding_coordinates=coordinates,
                        catalog_descriptor=descriptor,
                        address_source=execution.address,
                        runtime_roles=(case.fixture.component.reader,),
                        control_schema=schema,
                    )
                    assert result.metadata["counters"]["mapped_pair_rows"] == 1
                    assert result.metadata["counters"]["matched_offices"] == 1
                    assert (
                        await case.reader.fetchval("SELECT network_id FROM " + result.relation("memberships"))
                        == network["record_id"]
                    )
                    assert await case.reader.fetchval(
                        "SELECT location_id FROM " + result.relation("bindings")
                    ) == registry_location_id(source.SOURCE, "office-one")
                    changed = copy.deepcopy(descriptor)
                    changed["native_catalog_sha256"] = "0" * 64
                    with pytest.raises(ValueError):
                        mapping.require_catalog_graph_scope(stage, coordinates, changed)
                    for name in result.table_names.values():
                        await case.reader.execute('DROP TABLE "' + name + '"')


async def _make_typed_publication_recipe(case, serving, runtime_roles, *, target_network_id=None):
    from dataclasses import asdict
    from types import SimpleNamespace
    from uuid import uuid4

    from process import network_fhir_typed_capture as custody
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_source_witness_contract import canonical_timestamp
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
    from tests.source_witness_capture_postgres_support import _observed_source_descriptor, source_modules
    from tests.test_network_approved_source_bindings_postgres import _apply, _binding, _pin
    from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

    catalog, _ = source_modules()
    descriptor, _ = await _observed_source_descriptor(
        case.source_runtime,
        case.source_profile,
        SimpleNamespace(dataset_id=case.origin.dataset_id, endpoint_id=case.origin.endpoint_id),
        catalog,
    )
    capture_id = uuid4()
    case.fixture.namespaces.append(custody.PREFIX + capture_id.hex)
    async with case.writer.transaction(isolation="repeatable_read"):
        anchor = custody.TypedGraphAnchor(
            str(capture_id), canonical_timestamp(await case.writer.fetchval("SELECT transaction_timestamp()"))
        )
        capture = await custody.capture_typed_source_graph(
            case.source_export,
            case.writer,
            source_profile=case.source_profile,
            reviewed_origin=case.origin,
            membership_as_of=case.origin.semantic_projection_as_of or anchor.observed_at[:10],
            expected_anchor=anchor,
            owner_role=case.fixture.role_by_kind["owner"],
            runtime_roles=runtime_roles,
            catalog_descriptor=descriptor,
        )
    coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
    recipe = RegistrySourceMembershipRecipe(PinnedTypedFHIRMembershipSource(capture, anchor, descriptor), coordinates)
    connection, schema, engine = serving
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor) if target_network_id is None else None
    network_id = network["record_id"] if network is not None else target_network_id
    generation = fhir_catalog_source_generation(descriptor)
    bindings = [
        _binding(
            network_id,
            **asdict(coordinates),
            source_key="network-one",
            source_scope_json={"organization_id": "network-one", "source_generation": generation},
            evidence_id=generation + ":Organization/network-one",
        )
    ]
    bound = await _apply(connection, schema, actor, bindings)
    selected = (*((network,) if network is not None else ()), *bound["records"])
    await _approve(connection, schema, await _command(connection, schema, *selected), actor)
    return SimpleNamespace(
        capture=capture, recipe=recipe, approved=await _pin(connection, schema), network_id=network_id
    )


@pytest.mark.parametrize(
    "source_profile_kind,substitute_execution_output",
    [("modern", False), ("modern", True), ("legacy_missing_source_id", False)],
)
async def test_typed_source_ordinary_compact_publication(monkeypatch, source_profile_kind, substitute_execution_output):
    from types import SimpleNamespace

    from process import network_fhir_typed_capture as custody
    from process.registry_source_recipe_composition import SourceWitnessRuntime
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    async def typed_capture(
        case, worker, operator, candidate_roles, source_roles, address_reader_roles, serving, stack
    ):
        address_schemas = []
        stack.push_async_callback(publication._remove_address_inputs, worker.fixture.connection, address_schemas)
        address = await publication._native_generic_offices(
            worker.fixture,
            SimpleNamespace(role_by_kind=case.fixture.role_by_kind, captures=address_schemas),
            serving,
            address_reader_roles,
            stack,
        )
        prepared = await _make_typed_publication_recipe(case, serving, source_roles)
        return SimpleNamespace(
            case=case,
            worker=worker,
            operator=operator,
            candidate_roles=candidate_roles,
            address_reader_roles=address_reader_roles,
            runtime=SourceWitnessRuntime(
                case.source_export, (case.source_profile,), case.fixture.role_by_kind["owner"], source_roles
            ),
            capture=prepared.capture,
            recipe=prepared.recipe,
            approved=prepared.approved,
            network_id=prepared.network_id,
            address=address,
            ready_targets=[],
            sealed_targets=[],
        )

    legacy = source_profile_kind == "legacy_missing_source_id"
    if legacy:
        from tests import source_witness_legacy_postgres_support as legacy_fixture

        original_resources = legacy_fixture.legacy_resources

        def resources_without_embedded_source():
            return [
                (kind, identity, {key: value for key, value in payload.items() if key != "source_id"})
                for kind, identity, payload in original_resources()
            ]

        monkeypatch.setattr(legacy_fixture, "legacy_resources", resources_without_embedded_source)

    checked_actors = []
    original_verify = custody.verify_typed_graph_capture

    async def observe_raw_verification(factory, connection, document, **options):
        result = await original_verify(factory, connection, document, **options)
        checked_actors.append(await connection.fetchval("SELECT session_user"))
        return result

    monkeypatch.setattr(custody, "verify_typed_graph_capture", observe_raw_verification)
    monkeypatch.setattr(publication, "_source_capture_for_workers", typed_capture)
    if substitute_execution_output:
        from process import registry_typed_source_stage as staging
        from process.network_fhir_typed_capture import SealedTypedGraphRead

        original_mapping = staging.prepare_typed_graph_mapping

        async def substitute(connection, source_stage, **options):
            result = await original_mapping(connection, source_stage, **options)
            if type(source_stage) is SealedTypedGraphRead:
                await connection.execute("UPDATE " + result.relation("memberships") + " SET evidence_id=repeat('0',64)")
            return result

        monkeypatch.setattr(staging, "prepare_typed_graph_mapping", substitute)
    async with publication._publication_case(monkeypatch, legacy=legacy) as context:
        if legacy:
            from process.network_fhir_source_witness_contract import OBSERVED_LEGACY_PROFILE

            assert context.capture["graph"]["origin"]["admission_profile"] == OBSERVED_LEGACY_PROFILE
            assert context.capture["graph"]["origin"]["semantic_projection_as_of"] is None
            assert context.capture["catalog_descriptor"]["admission"]["publication"]["original_metadata_sha256"] is None
        if substitute_execution_output:
            async with publication._consumer(context.worker) as (arguments, _calls):
                arguments["initial_address_sources"] = publication._initial_sources(context)
                queued = await publication._enqueue_ordinary(context.worker)
                with pytest.raises(ValueError, match="registry_publication_request_refused"):
                    await publication.consume_registry_publication_once(**arguments)
                assert (await publication._status(context.worker, queued))["state"] == "rejected"
                assert await publication._head(context.worker) is None
                assert checked_actors == [await context.operator.connection.fetchval("SELECT session_user")]
                assert context.ready_targets and not context.sealed_targets
            for _ in range(2):
                async with publication._consumer(context.worker) as (arguments, _calls):
                    assert await publication.consume_registry_publication_once(**arguments) is None
            return
        manifest = await publication._publish_once(context)
        assert manifest.compact_layout is not None and manifest.generation_id > 0
        assert checked_actors == [await context.operator.connection.fetchval("SELECT session_user")]
        assert context.worker.roles_by_kind["execution"] not in checked_actors
        candidate = context.ready_targets[-1]
        assert (
            await context.worker.fixture.connection.fetchval(
                'SELECT count(*) FROM "' + candidate.schema_name + '".network_membership'
            )
            == 1
        )
        assert (
            await context.worker.fixture.connection.fetchval(
                'SELECT count(*) FROM "' + candidate.schema_name + '".network_provider_only_evidence'
            )
            == 1
        )
        if not legacy:
            # A newer, unselected Source publication must not invalidate the last-good closed contribution.
            await source.publish_edition(context.case.fixture, 2)
            second = await publication._publish_once(context, head=manifest.generation_id)
            assert second.generation_id > manifest.generation_id
            assert checked_actors == [await context.operator.connection.fetchval("SELECT session_user")]
            async with publication._login(context.worker, "api") as reader:
                async with reader.transaction(isolation="repeatable_read", readonly=True):
                    assert await publication._membership_snapshot(
                        reader, second
                    ) == await publication._membership_snapshot(reader, manifest)
        # Reopen the real worker twice to exercise unchanged permission verification and an empty queue.
        for _ in range(2):
            async with publication._consumer(context.worker) as (arguments, _calls):
                assert await publication.consume_registry_publication_once(**arguments) is None


@pytest.mark.parametrize("replacement", [False, True], ids=["append", "replace_typed_vector"])
async def test_typed_source_addition_to_legacy_generation_keeps_readers_and_rollback(monkeypatch, replacement):
    from dataclasses import asdict, replace
    from types import SimpleNamespace

    from process.network_fhir_typed_recipe import TypedFHIRRecipeReference
    from process.network_serving_read import resolve_network_serving_manifest
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    async with publication._publication_case(monkeypatch) as context:
        context.replay_actors = []
        first = await publication._publish_once(context)
        other = await publication._additional_source_capture(context, monkeypatch)
        second_case = SimpleNamespace(
            **{**vars(context.case), "origin": type(context.case.origin)(**asdict(other.capture.origin))}
        )
        first_typed = None
        if replacement:
            first_typed = await _make_typed_publication_recipe(
                context.case, context.serving, context.runtime.runtime_roles, target_network_id=context.network_id
            )
        typed_recipe = await _make_typed_publication_recipe(second_case, context.serving, context.runtime.runtime_roles)
        context.approved = typed_recipe.approved
        sources = await publication._append_source_input(
            context,
            first,
            replace(publication._initial_sources(context), initial_source_recipes=()),
            typed_recipe.recipe,
        )
        capture = typed_recipe.capture
        reference = TypedFHIRRecipeReference(
            capture["capture_id"],
            capture["observed_at"],
            capture["capture_sha256"],
            typed_recipe.recipe.source_pin.catalog_generation,
        )
        sources = replace(
            sources,
            initial_source_recipes=(),
            source_recipe_additions=replace(sources.source_recipe_additions, recipes=(reference,)),
        )
        if replacement:
            from process.registry_source_recipe_additions import RegistryTypedSourceRecipeReplacement

            first_capture = first_typed.capture
            first_reference = TypedFHIRRecipeReference(
                first_capture["capture_id"],
                first_capture["observed_at"],
                first_capture["capture_sha256"],
                first_typed.recipe.source_pin.catalog_generation,
            )
            previous = sources.source_recipe_additions
            sources = replace(
                sources,
                source_recipe_additions=RegistryTypedSourceRecipeReplacement(
                    previous.expected_serving_generation,
                    previous.manifest_sha256,
                    previous.inventory,
                    (first_reference, reference),
                ),
            )
        schema = context.worker.fixture.control_schema
        async with publication._login(context.worker, "api") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                original = await publication._membership_snapshot(reader, first)
                assert all(len(family) == 1 for family in original)
                second = await publication._publish_once(context, head=first.generation_id, sources=sources)
                assert await publication._membership_snapshot(reader, first) == original
                assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == first
        async with publication._login(context.worker, "api") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                current = await publication._membership_snapshot(reader, second)
                assert all(len(family) == 2 for family in current)
                for before, after in zip(original, current, strict=True):
                    retained = tuple(row for row in after if row[0] != typed_recipe.network_id)
                    assert tuple(row[:-1] for row in retained) == tuple(row[:-1] for row in before)
                    if not replacement:
                        assert retained == before
                if replacement:
                    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
                    from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

                    inventory = await resolve_retained_registry_source_recipes(
                        reader, second, control_schema=schema, api_read=True
                    )
                    assert len(inventory) == 2
                    assert all(type(recipe.source_pin) is PinnedTypedFHIRMembershipSource for recipe in inventory)
                restored = await publication._publish_source_rollback(
                    context, first, second, require_witness_replay=False
                )
                assert await publication._membership_snapshot(reader, second) == current
                assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == second
        async with publication._login(context.worker, "api") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == restored
                assert await publication._membership_snapshot(reader, restored) == original


async def test_typed_bounded_multiset_parity_hot_duplicate_bucket(monkeypatch):
    from process.registry_typed_membership_parity import create_order_index, multiset_differs, require_order_index

    async with source_witness_case(monkeypatch) as case:
        async with case.writer.transaction():
            for name in ("typed_parity_actual", "typed_parity_expected"):
                await case.writer.execute(
                    "CREATE TEMP TABLE "
                    + name
                    + "(network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL)"
                )
                await create_order_index(case.writer, name, name + "_idx")
                await require_order_index(case.writer, name)
                # Independently declared identical multisets: one hot duplicate key plus 12,000 wide keys.
                await case.writer.execute(
                    "INSERT INTO "
                    + name
                    + " SELECT 1,'npi','1234567893','00000000-0000-4000-8000-000000000001'::uuid,repeat('a',900) FROM generate_series(1,20000) UNION ALL SELECT 1,'provider_directory',repeat('z',900)||n::text,md5(n::text)::uuid,repeat('b',900) FROM generate_series(1,12000) n"
                )
            actual, expected = "SELECT * FROM typed_parity_actual", "SELECT * FROM typed_parity_expected"
            for memory in ("4MB", "16MB"):
                await case.writer.execute("SET LOCAL work_mem='" + memory + "'")
                assert await case.writer.fetchval("SHOW temp_file_limit") == "256MB"
                assert not await multiset_differs(case.writer, actual, expected)
                async with case.writer.transaction():
                    await case.writer.execute(
                        "DELETE FROM typed_parity_actual WHERE ctid=(SELECT ctid FROM typed_parity_actual WHERE provider_system='npi' LIMIT 1)"
                    )
                    assert await multiset_differs(case.writer, actual, expected)
                    await case.writer.execute(
                        "INSERT INTO typed_parity_actual SELECT * FROM typed_parity_expected WHERE provider_system='npi' LIMIT 1"
                    )
                for field, changed in (
                    ("network_id", "2"),
                    ("provider_system", "'manual'"),
                    ("provider_id", "'1000000004'"),
                    ("location_id", "'00000000-0000-4000-8000-000000000002'::uuid"),
                    ("evidence_id", "repeat('c',900)"),
                ):
                    transaction = case.writer.transaction()
                    await transaction.start()
                    try:
                        await case.writer.execute(
                            "UPDATE typed_parity_actual SET "
                            + field
                            + "="
                            + changed
                            + " WHERE ctid=(SELECT ctid FROM typed_parity_actual WHERE provider_system='npi' LIMIT 1)"
                        )
                        assert await multiset_differs(case.writer, actual, expected)
                    finally:
                        await transaction.rollback()
                assert not await multiset_differs(case.writer, actual, expected)
                transaction = case.writer.transaction()
                await transaction.start()
                try:
                    await case.writer.execute("DELETE FROM typed_parity_actual WHERE provider_system='npi'")
                    await case.writer.execute(
                        "INSERT INTO typed_parity_actual VALUES(9,'manual','custom-provider','00000000-0000-4000-8000-000000000003','approved-custom')"
                    )
                    effective = (
                        expected
                        + " WHERE provider_system<>'npi' UNION ALL SELECT 9,'manual','custom-provider','00000000-0000-4000-8000-000000000003'::uuid,'approved-custom'"
                    )
                    assert not await multiset_differs(case.writer, actual, effective)
                finally:
                    await transaction.rollback()
