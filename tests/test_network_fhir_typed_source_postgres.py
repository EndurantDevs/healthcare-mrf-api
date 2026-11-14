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
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
    ):
        async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
            document = validate_graph_document(json.loads(json.dumps(stage.document)))
            assert document["derivation_version"] == 3
            assert document["origin"]["dataset_id"] == case.origin.dataset_id
            rows = await case.writer.fetch("SELECT " + ",".join(PAIR_COLUMNS) + " FROM " + stage.relation("pairs"))
            assert len(rows) == 1
            pair_by_field = dict(rows[0])
            assert pair_by_field["network_source_key"] == "network-one" and pair_by_field["provider_id"] == "1234567893"
            assert pair_by_field["location_id"] == registry_location_id(source.SOURCE, "office-one")
            assert pair_by_field["reference_rows"] == 1 and pair_by_field["canonical_identity"].endswith("|street")
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


def _affiliation_fixture_rows(dataset):
    return [
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


def _strict_rows(edition, dataset):
    resource_rows = _ORIGINAL_ROWS(edition, dataset)
    template = copy.deepcopy(
        next(
            resource_row["payload_json"]
            for resource_row in resource_rows
            if resource_row["resource_id"] == "role-office"
        )
    )
    changes_by_resource = {
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
    resource_rows += [
        _resource("PractitionerRole", name, {**template, **fields}, dataset)
        for name, fields in changes_by_resource.items()
    ]
    resource_rows += _affiliation_fixture_rows(dataset)
    return resource_rows


async def test_strict_subject_reference_and_acceptance_boundaries(monkeypatch):
    monkeypatch.setattr(source, "_edition_rows", _strict_rows)
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
        typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage,
    ):
        actual = await case.writer.fetch(
            "SELECT resource_id,provider_id,reference_rows FROM " + stage.relation("pairs") + " ORDER BY resource_id"
        )
        assert [tuple(row) for row in actual] == [
            ("absolute", "1234567893", 1),
            ("participant", "1000000004", 1),
            ("role-office", "1234567893", 1),
        ]
        assert stage.document["counts"]["provider_only_roles"] == 1


def _role_page_boundary_rows(edition, dataset):
    resource_rows = _ORIGINAL_ROWS(edition, dataset) + _affiliation_fixture_rows(dataset)
    resource_rows.extend(
        _resource("Organization", f"unused-{index:04d}", {"active": False}, dataset) for index in range(1000)
    )
    for kind, subject, field in (
        ("OrganizationAffiliation", "Organization/provider-org", "participating_organization_ref"),
        ("PractitionerRole", "Practitioner/provider-one", "practitioner_ref"),
    ):
        resource_rows.extend(
            _resource(
                kind,
                f"range-{index:04d}",
                {
                    field: subject,
                    "network_refs": ["Organization/network-one"] * 2,
                    "location_refs": ["Location/office-one"] * 3,
                },
                dataset,
            )
            for index in range(130)
        )
    return resource_rows


async def _assert_checker_lookup_indexes(connection, stage):
    from process.network_fhir_typed_sql import CHECKER_PAIR_SQL

    query = CHECKER_PAIR_SQL.format(**{name: stage.relation(name) for name in ("cells", "addresses", "checks")})
    plan = await connection.fetchval(
        "EXPLAIN (FORMAT JSON) " + query, "PractitionerRole", "", "PractitionerRole", "range-0127"
    )
    plan = json.loads(plan) if isinstance(plan, str) else plan
    pending, checker_scans = [plan[0]["Plan"]], []
    while pending:
        node = pending.pop()
        if node.get("Relation Name") == stage.names["checks"]:
            checker_scans.append(node)
        pending.extend(node.get("Plans", ()))
    assert len(checker_scans) == 4
    assert all(
        scan["Node Type"] in {"Index Scan", "Index Only Scan"}
        and all(field in scan.get("Index Cond", "") for field in ("resource_type", "resource_id"))
        for scan in checker_scans
    )


async def test_role_pages_preserve_type_boundaries_and_reference_multiplicity(monkeypatch):
    original_pairs, checked_plans = typed._pairs, []

    async def indexed_pairs(connection, stage, origin, as_of, *, checker=False):
        result = await original_pairs(connection, stage, origin, as_of, checker=checker)
        if checker:
            await _assert_checker_lookup_indexes(connection, stage)
            checked_plans.append(True)
        return result

    monkeypatch.setattr(source, "_edition_rows", _role_page_boundary_rows)
    monkeypatch.setattr(typed, "_pairs", indexed_pairs)
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
        typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage,
    ):
        rows = await case.writer.fetch(
            "SELECT resource_type,resource_id,provider_id,reference_rows FROM "
            + stage.relation("pairs")
            + " ORDER BY resource_type,resource_id"
        )
        expected_rows = [("OrganizationAffiliation", "participant", "1000000004", 1)]
        expected_rows.extend(("OrganizationAffiliation", f"range-{index:04d}", "1000000004", 6) for index in range(130))
        expected_rows.extend(("PractitionerRole", f"range-{index:04d}", "1234567893", 6) for index in range(130))
        expected_rows.append(("PractitionerRole", "role-office", "1234567893", 1))
        assert [tuple(row) for row in rows] == expected_rows
    assert checked_plans == [True]


async def test_raw_checker_refuses_producer_npi_substitution(monkeypatch):
    original = typed._project_page

    async def corrupt(connection, stage, rows):
        await original(connection, stage, rows)
        await connection.execute(
            "UPDATE " + stage.relation("cells") + " SET npi='1000000004' WHERE resource_type='Practitioner'"
        )

    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(typed, "_project_page", corrupt)
            with pytest.raises(TypedFHIRGraphError, match="raw_graph_differs"):
                async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)):
                    pytest.fail("producer substitution admitted")
        assert await _stages(case.writer) == 0
        async with typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage:
            assert stage.document["counts"]["pair_rows"] == 1


async def test_authority_alias_and_cancelled_source_restart_at_zero(monkeypatch):
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
    ):
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
    resource_rows = _ORIGINAL_ROWS(edition, dataset)
    template = copy.deepcopy(
        next(
            resource_row["payload_json"]
            for resource_row in resource_rows
            if resource_row["resource_id"] == "role-office"
        )
    )
    location = copy.deepcopy(
        next(
            resource_row["payload_json"]
            for resource_row in resource_rows
            if resource_row["resource_id"] == "office-one"
        )
    )
    resource_rows += [
        _resource("Organization", "network-" + str(number), {"name": "Network " + str(number)}, dataset)
        for number in range(6)
    ]
    resource_rows += [
        _resource("Location", "fanout-" + str(number), {**location, "second_line": "Suite " + str(number)}, dataset)
        for number in range(2257)
    ]
    resource_rows.append(
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
    resource_rows.append(
        _resource(
            "PractitionerRole", "repeated", {**template, "location_refs": ["Location/office-one"] * 13542}, dataset
        )
    )
    return resource_rows


async def test_native_13542_unique_and_repeated_edges_remain_streamed(monkeypatch):
    monkeypatch.setattr(source, "_edition_rows", _fanout_rows)
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        case.writer.transaction(isolation="repeatable_read"),
        typed.typed_source_graph(case.source_export, case.writer, **_arguments(case)) as stage,
    ):
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
                "EXPLAIN (ANALYZE, FORMAT JSON) " + query,
                case.origin.source_uri.rstrip("/"),
                date.fromisoformat(_AS_OF),
                "PractitionerRole",
                "",
                "PractitionerRole",
                "zzzz",
            )
        )[0]["Plan"]
        assert plan["Actual Rows"] > 0, json.dumps(plan)
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


def _invalid_location_rows(edition, dataset):
    resource_rows = _ORIGINAL_ROWS(edition, dataset)
    role_payload = copy.deepcopy(
        next(row["payload_json"] for row in resource_rows if row["resource_id"] == "role-office")
    )
    for name, fields_by_name in (
        ("missing-address", {}),
        ("null-address", dict.fromkeys(typed.ADDRESS_FIELDS)),
        ("empty-address", dict.fromkeys(typed.ADDRESS_FIELDS, "")),
    ):
        resource_rows.append(_resource("Location", name, {"status": "active", **fields_by_name}, dataset))
    resource_rows.append(
        _resource(
            "PractitionerRole",
            "invalid-addresses",
            {
                **role_payload,
                "location_refs": ["Location/" + name for name in ("missing-address", "null-address", "empty-address")],
            },
            dataset,
        )
    )
    return resource_rows


async def _assert_invalid_locations_retained(connection, stage):
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM "
            + stage.relation("cells")
            + " WHERE resource_type='Location' AND resource_id IN ('missing-address','null-address','empty-address')"
        )
        == 3
    )
    assert await connection.fetchval("SELECT count(*) FROM " + stage.relation("addresses")) == 2
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM " + stage.relation("pairs") + " WHERE resource_id='invalid-addresses'"
        )
        == 0
    )


async def _check_persistent_capture_reader(case, document, anchor, descriptor, names_by_kind, invalid_locations=False):
    from uuid import uuid4

    from process import network_fhir_typed_capture as custody

    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        replay = await typed.bind_typed_graph_stage(case.reader, schema_name="pg_temp", table_names=names_by_kind)
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
        if invalid_locations:
            await _assert_invalid_locations_retained(case.reader, stage)
        from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

        resolved = await custody.resolve_typed_graph_capture_reference(
            case.reader,
            expected_anchor=anchor,
            capture_sha256=document["capture_sha256"],
            catalog_generation=fhir_catalog_source_generation(descriptor),
        )
        assert resolved == document
        with pytest.raises(TypedFHIRGraphError, match="reference_context"):
            await custody.resolve_typed_graph_capture_reference(
                case.reader,
                expected_anchor=anchor,
                capture_sha256="0" * 64,
                catalog_generation=fhir_catalog_source_generation(descriptor),
            )
        assert await case.reader.fetchval("SHOW transaction_read_only") == "on"
        wrong = custody.TypedGraphAnchor(str(uuid4()), anchor.observed_at)
        with pytest.raises(TypedFHIRGraphError, match="anchor"):
            await custody.require_typed_graph_capture(case.reader, document, expected_anchor=wrong)


async def _make_persistent_capture(case):
    from uuid import uuid4

    from process import network_fhir_typed_capture as custody
    from process.network_fhir_source_witness_contract import canonical_timestamp
    from tests.source_witness_capture_postgres_support import _observed_source_descriptor, source_modules

    catalog, _ = source_modules()
    descriptor, _ = await _observed_source_descriptor(
        case.source_runtime, case.source_profile, case.fixture.candidates[-1], catalog
    )
    identity = uuid4()
    schema = custody.PREFIX + identity.hex
    case.fixture.namespaces.append(schema)
    administrator = case.fixture.admin
    async with administrator.transaction(isolation="repeatable_read"):
        anchor = custody.TypedGraphAnchor(
            str(identity), canonical_timestamp(await administrator.fetchval("SELECT transaction_timestamp()"))
        )
        document = await custody.capture_typed_source_graph(
            case.source_export,
            administrator,
            **{
                **_arguments(case),
                "membership_as_of": case.origin.semantic_projection_as_of or anchor.observed_at[:10],
            },
            capture_options=custody.TypedGraphCaptureOptions(
                expected_anchor=anchor,
                owner_role=case.fixture.issued.namespace_owner_role,
                runtime_roles=(case.fixture.component.reader,),
            ),
            catalog_descriptor=descriptor,
        )
        assert document["descriptor_version"] == 5
        assert document["tables"] == {kind: {"schema": schema, "name": name} for kind, name in custody.TABLES.items()}
    return document, anchor, descriptor, schema


async def _assert_closed_capture_writes(case, schema):
    import asyncpg

    async with case.reader.transaction():
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with case.reader.transaction():
                await case.reader.execute('DELETE FROM "' + schema + '".source_pairs')
    writer = await asyncpg.connect(
        case.fixture.component.admin_engine.url.set(
            drivername="postgresql", username=case.fixture.component.retained_publisher
        ).render_as_string(hide_password=False)
    )
    try:
        async with writer.transaction(isolation="repeatable_read"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with writer.transaction():
                    await writer.execute('UPDATE "' + schema + '".source_cells SET npi=NULL')
    finally:
        await writer.close()


@pytest.mark.parametrize("invalid_locations", [False, True], ids=["valid-locations", "invalid-locations"])
async def test_persistent_typed_capture_closed_native_reader_and_independent_replay(monkeypatch, invalid_locations):
    """Verify named content, read-only replay, changed-pin refusal and denied writes."""
    from uuid import uuid4

    if invalid_locations:
        monkeypatch.setattr(source, "_edition_rows", _invalid_location_rows)
    async with source_witness_case(monkeypatch, named=True) as case:
        document, anchor, descriptor, schema = await _make_persistent_capture(case)
        # Precreate only temporary checker scratch before the genuine Reader read-only snapshot.
        names_by_kind = {kind: "typed_replay_" + kind + "_" + uuid4().hex for kind in typed._DDL}
        for kind, definition in typed._DDL.items():
            await case.reader.execute('CREATE TEMP TABLE "' + names_by_kind[kind] + '"(' + definition + ")")
        await case.reader.execute('CREATE INDEX ON "' + names_by_kind["addresses"] + '"(address_key)')
        try:
            await _check_persistent_capture_reader(case, document, anchor, descriptor, names_by_kind, invalid_locations)
            await _assert_closed_capture_writes(case, schema)
        finally:
            for name in names_by_kind.values():
                await case.reader.execute('DROP TABLE "' + name + '"')


async def _approve_typed_catalog_network(case, serving):
    from dataclasses import asdict

    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
    from tests.source_witness_capture_postgres_support import _observed_source_descriptor, source_modules
    from tests.test_network_approved_source_bindings_postgres import _apply, _binding, _pin
    from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

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
    await connection.execute('GRANT USAGE ON SCHEMA "' + schema + '" TO "' + case.fixture.component.reader + '"')
    await connection.execute(
        'GRANT SELECT ON ALL TABLES IN SCHEMA "' + schema + '" TO "' + case.fixture.component.reader + '"'
    )
    return descriptor, coordinates, network, approved


async def test_typed_approved_catalog_and_native_office_mapping(monkeypatch):
    """Map an approved catalog through the genuine Reader and native office identity."""

    from process import network_fhir_typed_mapping as mapping

    async with (
        source_witness_case(monkeypatch, named=True) as case,
        source.generic_refresh_execution(case.fixture, monkeypatch) as (execution, serving, _stack, _head),
    ):
        descriptor, coordinates, network, approved = await _approve_typed_catalog_network(case, serving)
        schema = serving[1]
        # This is the genuine Source Reader; no Source writer or catalog credentials are borrowed.
        async with (
            case.reader.transaction(isolation="repeatable_read"),
            typed.typed_source_graph(case.source_export, case.reader, **_arguments(case)) as stage,
        ):
            graph_mapping = await mapping.prepare_typed_graph_mapping(
                case.reader,
                stage,
                approval=mapping.TypedGraphApproval(
                    approved_source=approved, binding_coordinates=coordinates, catalog_descriptor=descriptor
                ),
                address_source=execution.address,
                runtime_roles=(case.fixture.component.reader,),
                control_schema=schema,
            )
            assert graph_mapping.metadata["counters"]["mapped_pair_rows"] == 1
            assert graph_mapping.metadata["counters"]["matched_offices"] == 1
            assert (
                await case.reader.fetchval("SELECT network_id FROM " + graph_mapping.relation("memberships"))
                == network["record_id"]
            )
            assert await case.reader.fetchval(
                "SELECT location_id FROM " + graph_mapping.relation("bindings")
            ) == registry_location_id(source.SOURCE, "office-one")
            changed = copy.deepcopy(descriptor)
            changed["native_catalog_sha256"] = "0" * 64
            with pytest.raises(ValueError):
                mapping.require_catalog_graph_scope(stage, coordinates, changed)
            for name in graph_mapping.table_names.values():
                await case.reader.execute('DROP TABLE "' + name + '"')


def _provider_page_boundary_rows(edition, dataset, include_affiliations):
    resource_rows = _role_page_boundary_rows(edition, dataset)
    for kind, subject, field in (
        ("OrganizationAffiliation", "Organization/provider-org", "participating_organization_ref"),
        ("PractitionerRole", "Practitioner/provider-one", "practitioner_ref"),
    ):
        resource_rows.extend(
            _resource(
                kind,
                f"provider-range-{index:04d}",
                {field: subject, "network_refs": ["Organization/network-one"] * 2, "location_refs": []},
                dataset,
            )
            for index in range(130)
        )
    if not include_affiliations:
        resource_rows = [row for row in resource_rows if row["resource_type"] != "OrganizationAffiliation"]
    return resource_rows


@pytest.mark.parametrize("include_affiliations", [False, True])
async def test_provider_only_pages_keep_networked_rows_and_type_boundaries(monkeypatch, include_affiliations):
    """Retain distinct provider evidence without admitting networked offices as provider-only rows."""
    from process import network_fhir_typed_mapping as mapping

    monkeypatch.setattr(
        source,
        "_edition_rows",
        lambda edition, dataset: _provider_page_boundary_rows(edition, dataset, include_affiliations),
    )
    async with (
        source_witness_case(monkeypatch, named=True) as case,
        source.generic_refresh_execution(case.fixture, monkeypatch) as (execution, serving, _stack, _head),
    ):
        descriptor, coordinates, _network, approved = await _approve_typed_catalog_network(case, serving)
        async with (
            case.reader.transaction(isolation="repeatable_read"),
            typed.typed_source_graph(case.source_export, case.reader, **_arguments(case)) as stage,
        ):
            graph_mapping = await mapping.prepare_typed_graph_mapping(
                case.reader,
                stage,
                approval=mapping.TypedGraphApproval(
                    approved_source=approved, binding_coordinates=coordinates, catalog_descriptor=descriptor
                ),
                address_source=execution.address,
                runtime_roles=(case.fixture.component.reader,),
                control_schema=serving[1],
            )
            assert stage.document["counts"]["pair_rows"] == (262 if include_affiliations else 131)
            provider_counts = await case.reader.fetch(
                "SELECT provider_id,count(*),count(DISTINCT evidence_id) FROM "
                + graph_mapping.relation("provider_only")
                + " GROUP BY provider_id ORDER BY provider_id"
            )
            assert [tuple(provider_count) for provider_count in provider_counts] == [
                ("1000000004", 131 if include_affiliations else 1, 131 if include_affiliations else 1),
                ("1234567893", 130, 130),
            ]
            for name in graph_mapping.table_names.values():
                await case.reader.execute('DROP TABLE "' + name + '"')


async def _produce_recipe_capture(case, descriptor, runtime_roles):
    from uuid import uuid4

    from process import network_fhir_typed_capture as custody
    from process.network_fhir_source_witness_contract import canonical_timestamp

    capture_id = uuid4()
    case.fixture.namespaces.append(custody.PREFIX + capture_id.hex)
    administrator = case.fixture.admin
    async with administrator.transaction(isolation="repeatable_read"):
        anchor = custody.TypedGraphAnchor(
            str(capture_id), canonical_timestamp(await administrator.fetchval("SELECT transaction_timestamp()"))
        )
        capture = await custody.capture_typed_source_graph(
            case.source_export,
            administrator,
            source_profile=case.source_profile,
            reviewed_origin=case.origin,
            membership_as_of=case.origin.semantic_projection_as_of or anchor.observed_at[:10],
            capture_options=custody.TypedGraphCaptureOptions(
                expected_anchor=anchor,
                owner_role=(
                    case.fixture.issued.namespace_owner_role
                    if getattr(case.fixture, "named", False)
                    else case.fixture.role_by_kind["owner"]
                ),
                runtime_roles=runtime_roles,
            ),
            catalog_descriptor=descriptor,
        )
    return capture, anchor


async def _make_typed_publication_recipe(case, serving, runtime_roles, *, target_network_id=None):
    """Create a frozen graph and approve its exact catalog network for publication."""
    from dataclasses import asdict
    from types import SimpleNamespace

    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
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
    capture, anchor = await _produce_recipe_capture(case, descriptor, runtime_roles)
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


async def _typed_worker_capture(
    case, worker, operator, candidate_roles, source_roles, address_reader_roles, serving, stack
):
    from types import SimpleNamespace

    from process.registry_source_recipe_composition import SourceWitnessRuntime
    from tests import test_network_fhir_source_witness_publication_postgres as publication

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


def _observe_typed_verification(monkeypatch):
    from process import network_fhir_typed_capture as custody

    checked_captures = []
    original_verify = custody.verify_typed_graph_capture

    async def observe_raw_verification(factory, connection, document, **options):
        result = await original_verify(factory, connection, document, **options)
        checked_captures.append(document["capture_id"])
        return result

    monkeypatch.setattr(custody, "verify_typed_graph_capture", observe_raw_verification)
    return checked_captures


def _substitute_typed_mapping(monkeypatch):
    from process import registry_typed_source_stage as staging
    from process.network_fhir_typed_capture import TypedGraphRead

    original_mapping = staging.map_typed_graph_batch

    async def substitute(connection, inputs, **options):
        results = await original_mapping(connection, inputs, **options)
        for result in results:
            if type(result.source_stage) is TypedGraphRead:
                await connection.execute("UPDATE " + result.relation("memberships") + " SET evidence_id=repeat('0',64)")
        return results

    monkeypatch.setattr(staging, "map_typed_graph_batch", substitute)


def _observe_typed_accounting(monkeypatch):
    from process import network_typed_membership_accounting as accounting
    from process import network_typed_slice_native as native

    work_builds = []
    original_build = native.build_matched_facts

    async def observe(connection, schema, addresses, work, affected):
        result = await original_build(connection, schema, addresses, work, affected)
        work_builds.append((schema, work))
        return result

    monkeypatch.setattr(native, "build_matched_facts", observe)
    monkeypatch.setattr(accounting, "build_matched_facts", observe)
    return work_builds


async def _assert_rejected_typed_publication(context, checked_captures):
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    async with publication._consumer(context.worker) as (arguments, _calls):
        arguments["initial_address_sources"] = publication._initial_sources(context)
        queued = await publication._enqueue_ordinary(context.worker)
        with pytest.raises(ValueError, match="registry_publication_request_refused"):
            await publication.consume_registry_publication_once(**arguments)
        assert (await publication._status(context.worker, queued))["state"] == "rejected"
        assert await publication._head(context.worker) is None
        assert checked_captures == [context.capture["capture_id"]]
        assert context.ready_targets and not context.sealed_targets
    for _ in range(2):
        async with publication._consumer(context.worker) as (arguments, _calls):
            assert await publication.consume_registry_publication_once(**arguments) is None
    return


async def _assert_typed_candidate_counts(context):
    candidate = context.ready_targets[-1]
    estimates = await context.worker.fixture.connection.fetch(
        "SELECT c.relname,c.reltuples FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
        candidate.schema_name,
        ["network_membership", "network_membership_source"],
    )
    assert {row["relname"]: row["reltuples"] for row in estimates} == {
        "network_membership": 1,
        "network_membership_source": 1,
    }
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


@pytest.mark.parametrize(
    "source_profile_kind,substitute_execution_output",
    [("modern", False), ("modern", True), ("legacy_missing_source_id", False)],
)
async def test_typed_source_ordinary_compact_publication(monkeypatch, source_profile_kind, substitute_execution_output):
    """Publish typed inputs through named preparation and reject substituted producer output."""

    from tests import test_network_fhir_source_witness_publication_postgres as publication

    is_legacy = source_profile_kind == "legacy_missing_source_id"
    if is_legacy:
        from tests import source_witness_legacy_postgres_support as legacy_fixture

        original_resources = legacy_fixture.legacy_resources

        def resources_without_embedded_source():
            return [
                (kind, identity, {key: value for key, value in payload.items() if key != "source_id"})
                for kind, identity, payload in original_resources()
            ]

        monkeypatch.setattr(legacy_fixture, "legacy_resources", resources_without_embedded_source)

    checked_captures = _observe_typed_verification(monkeypatch)
    built_work = _observe_typed_accounting(monkeypatch)
    monkeypatch.setattr(publication, "_source_capture_for_workers", _typed_worker_capture)
    if substitute_execution_output:
        _substitute_typed_mapping(monkeypatch)
    async with publication._publication_case(monkeypatch, legacy=is_legacy) as context:
        if is_legacy:
            from process.network_fhir_source_witness_contract import OBSERVED_LEGACY_PROFILE

            assert context.capture["graph"]["origin"]["admission_profile"] == OBSERVED_LEGACY_PROFILE
            assert context.capture["graph"]["origin"]["semantic_projection_as_of"] is None
            assert context.capture["catalog_descriptor"]["admission"]["publication"]["original_metadata_sha256"] is None
        if substitute_execution_output:
            await _assert_rejected_typed_publication(context, checked_captures)
            return
        manifest = await publication._publish_once(context)
        assert manifest.compact_layout is not None and manifest.generation_id > 0
        assert checked_captures == [context.capture["capture_id"]]
        assert len(built_work) == 2 and len({work for _, work in built_work}) == 2
        await _assert_typed_candidate_counts(context)
        if not is_legacy:
            # A newer, unselected Source publication must not invalidate the last-good closed contribution.
            await source.publish_edition(context.case.fixture, 2)
            second = await publication._publish_once(context, head=manifest.generation_id)
            assert second.generation_id > manifest.generation_id
            assert checked_captures == [context.capture["capture_id"]]
            async with (
                publication._login(context.worker, "api") as reader,
                reader.transaction(isolation="repeatable_read", readonly=True),
            ):
                assert await publication._membership_snapshot(reader, second) == await publication._membership_snapshot(
                    reader, manifest
                )
        # Reopen the real worker twice to exercise unchanged configuration and an empty queue.
        for _ in range(2):
            async with publication._consumer(context.worker) as (arguments, _calls):
                assert await publication.consume_registry_publication_once(**arguments) is None


async def _assert_sealed_resume_failure(context, address_sources, head):
    from uuid import UUID, uuid5

    from process import registry_publication_consumer as consumer
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    connection = context.worker.fixture.connection
    namespace = '"' + context.worker.fixture.control_schema + '"'
    request_sql = (
        f"SELECT request_sha256,command_json,actor_key,session_token_sha256,idempotency_key "
        f"FROM {namespace}.registry_publication_request WHERE request_id=$1"
    )
    async with publication._consumer(context.worker) as (arguments, _calls):
        arguments["initial_address_sources"] = address_sources
        queued = await publication._enqueue_ordinary(context.worker, head)
        request = UUID(queued["request_id"])
        arguments["config"] = replace(arguments["config"], only_request_id=request)
        request_by_field = dict(await connection.fetchrow(request_sql, request))
        approval_count = await connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_approval_history")
        with pytest.raises(RuntimeError, match="^registry_publication_consumer_unavailable$"):
            await consumer.consume_registry_publication_once(**arguments)
        candidate_by_field = dict(
            await connection.fetchrow(
                f"SELECT * FROM {namespace}.network_membership_candidate WHERE dataset_id=$1", uuid5(request, "dataset")
            )
        )
        report = json.loads(candidate_by_field["validation_json"])
        assert candidate_by_field["state"] == "sealed" and candidate_by_field["expected_rows"] > 0
        assert "address_source" not in report and "serving_readiness" not in report
        assert (await publication._status(context.worker, queued))["state"] == "running"
        assert await publication._head(context.worker) == head
        revision_by_field = dict(
            await connection.fetchrow(
                f"SELECT draft_revision,approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
            )
        )
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_approval_history")
            == approval_count + 1
        )
    return {
        "queued": queued,
        "candidate": candidate_by_field,
        "request": request_by_field,
        "revision": revision_by_field,
        "approval_count": approval_count + 1,
    }


async def _assert_sealed_resume_publication(context, address_sources, failed_by_field):
    """Wait for the real lease, then verify unchanged approval and named candidate data."""
    from uuid import UUID

    from process import registry_publication_consumer as consumer
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    connection = context.worker.fixture.connection
    namespace = '"' + context.worker.fixture.control_schema + '"'
    queued = failed_by_field["queued"]
    request = UUID(queued["request_id"])
    expiry_sql = (
        f"SELECT lease_expires_at>clock_timestamp() FROM {namespace}.registry_publication_request WHERE request_id=$1"
    )
    deadline = asyncio.get_running_loop().time() + 45
    while await connection.fetchval(expiry_sql, request):
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(1)
    async with publication._consumer(context.worker) as (arguments, _calls):
        arguments["initial_address_sources"] = address_sources
        arguments["config"] = replace(arguments["config"], only_request_id=request)
        completed = await publication._consume_expected_success(context, arguments, queued)
        assert completed["request_id"] == queued["request_id"] and completed["state"] == "completed"
        revision_record = await connection.fetchrow(
            f"SELECT draft_revision,approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        )
        assert dict(revision_record) == failed_by_field["revision"]
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_approval_history")
            == failed_by_field["approval_count"]
        )
        request_record = await connection.fetchrow(
            f"SELECT request_sha256,command_json,actor_key,session_token_sha256,idempotency_key "
            f"FROM {namespace}.registry_publication_request WHERE request_id=$1",
            request,
        )
        assert dict(request_record) == failed_by_field["request"]
        candidate_by_field = failed_by_field["candidate"]
        published_by_field = dict(
            await connection.fetchrow(
                f"SELECT * FROM {namespace}.network_membership_candidate WHERE dataset_id=$1",
                candidate_by_field["dataset_id"],
            )
        )
        identity_fields = ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
        for field in identity_fields + ("expected_rows", "source_generations", "source_recipes_json"):
            assert published_by_field[field] == candidate_by_field[field]
        assert published_by_field["state"] == "published"
        assert await publication._head(context.worker) == completed["result"]["serving_generation"]
        assert completed["result"]["serving_generation"] > candidate_by_field["expected_head"]
        assert context.ready_targets[-1] == context.sealed_targets[-1]
        assert context.sealed_targets[-1].candidate_id == str(candidate_by_field["candidate_id"])
        assert await consumer.consume_registry_publication_once(**arguments) is None


async def test_sealed_typed_candidate_resumes_after_transient_readiness_failure(monkeypatch):
    """Fresh workers resume the same sealed candidate before its validation report exists."""
    from process import registry_candidate_composition as composition
    from process import registry_publication_consumer as consumer
    from process import registry_typed_office_parity as parity
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    monkeypatch.setattr(publication, "_source_capture_for_workers", _typed_worker_capture)
    monkeypatch.setattr(consumer, "_LEASE_SECONDS", 30)
    monkeypatch.setattr(consumer, "_HEARTBEAT_SECONDS", 10)
    checked_captures = _observe_typed_verification(monkeypatch)
    async with publication._publication_case(monkeypatch) as context:
        initial_sources = publication._initial_sources(context)
        previous = await publication._publish_once(context, sources=replace(initial_sources, initial_source_recipes=()))
        address_sources = replace(
            initial_sources, initial_source_recipes=(), replacement_source_recipes=(context.recipe,)
        )
        original_domain = parity.has_binding_domain_difference
        original_approve = consumer.approve_queued_registry_publication
        approvals = []
        readiness_calls = []

        async def approve(*args, **kwargs):
            result = await original_approve(*args, **kwargs)
            approvals.append(result)
            return result

        async def fail_readiness(active, receipt, carrier):
            readiness_calls.append(True)
            raise RuntimeError("synthetic_readiness_unavailable")

        monkeypatch.setattr(consumer, "approve_queued_registry_publication", approve)
        monkeypatch.setattr(parity, "has_binding_domain_difference", fail_readiness)
        failed_by_field = await _assert_sealed_resume_failure(context, address_sources, previous.generation_id)
        assert checked_captures == []
        assert readiness_calls == [True]
        monkeypatch.setattr(parity, "has_binding_domain_difference", original_domain)

        async def refuse_recopy(*args, **kwargs):
            raise AssertionError("A sealed candidate must not copy raw memberships again")

        monkeypatch.setattr(composition, "_copy_composition", refuse_recopy)
        await _assert_sealed_resume_publication(context, address_sources, failed_by_field)
        assert approvals[0]["replayed"] is False and approvals[1]["replayed"] is True
        assert approvals[0]["approved_revision"] == approvals[1]["approved_revision"]
        assert checked_captures == [context.capture["capture_id"]]


def _typed_replacement_inputs(source_inputs, first_typed, reference):
    from dataclasses import replace

    from process.network_fhir_typed_recipe import TypedFHIRRecipeReference
    from process.registry_source_recipe_additions import RegistryTypedSourceRecipeReplacement

    first_capture = first_typed.capture
    first_reference = TypedFHIRRecipeReference(
        first_capture["capture_id"],
        first_capture["observed_at"],
        first_capture["capture_sha256"],
        first_typed.recipe.source_pin.catalog_generation,
    )
    previous = source_inputs.source_recipe_additions
    source_inputs = replace(
        source_inputs,
        source_recipe_additions=RegistryTypedSourceRecipeReplacement(
            previous.expected_serving_generation,
            previous.manifest_sha256,
            previous.inventory,
            (first_reference, reference),
        ),
    )
    return source_inputs


async def _typed_addition_inputs(context, first, monkeypatch, replacement):
    from dataclasses import asdict, replace
    from types import SimpleNamespace

    from process.network_fhir_typed_recipe import TypedFHIRRecipeReference
    from tests import test_network_fhir_source_witness_publication_postgres as publication

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
    source_inputs = await publication._append_source_input(
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
    source_inputs = replace(
        source_inputs,
        initial_source_recipes=(),
        source_recipe_additions=replace(source_inputs.source_recipe_additions, recipes=(reference,)),
    )
    if replacement:
        source_inputs = _typed_replacement_inputs(source_inputs, first_typed, reference)
        if replacement == "ordinary":
            source_inputs = replace(
                source_inputs,
                replacement_source_recipes=source_inputs.source_recipe_additions.recipes,
                source_recipe_additions=None,
            )
    return source_inputs, typed_recipe


async def _assert_ordinary_reference_refusals(context, first, source_inputs):
    from dataclasses import replace

    from process.registry_composition_addresses import _resolve_source_recipe_additions
    from process.registry_fhir_serving_preservation import fhir_vector_succession
    from process.registry_source_recipe_store import resolve_retained_registry_source_recipes
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    schema = context.worker.fixture.control_schema
    async with (
        publication._login(context.worker, "execution") as connection,
        connection.transaction(isolation="repeatable_read", readonly=True),
    ):
        references = source_inputs.replacement_source_recipes
        forged = replace(
            source_inputs,
            replacement_source_recipes=(replace(references[0], capture_sha256="0" * 64), *references[1:]),
        )
        with pytest.raises(ValueError):
            await _resolve_source_recipe_additions(connection, forged, first, first.generation_id, schema)
        hydrated = await _resolve_source_recipe_additions(connection, source_inputs, first, first.generation_id, schema)
        previous = await resolve_retained_registry_source_recipes(connection, first, control_schema=schema)
        omitted_recipes = tuple(
            recipe
            for recipe in hydrated.replacement_source_recipes
            if recipe.binding_coordinates.source_id != context.recipe.binding_coordinates.source_id
        )
        assert omitted_recipes and len(omitted_recipes) < len(hydrated.replacement_source_recipes)
        with pytest.raises(ValueError, match="vector_incomplete"):
            fhir_vector_succession(previous, omitted_recipes)


@pytest.mark.parametrize(
    "replacement", [False, True, "ordinary"], ids=["append", "replace_typed_vector", "ordinary_replacement"]
)
async def test_typed_source_addition_to_legacy_generation_keeps_readers_and_rollback(monkeypatch, replacement):
    """Keep pinned reads and exact rollback valid across typed append or replacement."""

    from process.network_serving_read import resolve_network_serving_manifest
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    async with publication._publication_case(monkeypatch) as context:
        context.replay_actors = []
        first = await publication._publish_once(context)
        source_inputs, typed_recipe = await _typed_addition_inputs(context, first, monkeypatch, replacement)
        if replacement == "ordinary":
            await _assert_ordinary_reference_refusals(context, first, source_inputs)
        schema = context.worker.fixture.control_schema
        async with (
            publication._login(context.worker, "api") as reader,
            reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            original = await publication._membership_snapshot(reader, first)
            assert all(len(family) == 1 for family in original)
            second = await publication._publish_once(context, head=first.generation_id, sources=source_inputs)
            assert await publication._membership_snapshot(reader, first) == original
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == first
        async with (
            publication._login(context.worker, "api") as reader,
            reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            current = await publication._membership_snapshot(reader, second)
            assert all(len(family) == 2 for family in current)
            for before, after in zip(original, current, strict=True):
                retained_pins = tuple(
                    inventory_row for inventory_row in after if inventory_row[0] != typed_recipe.network_id
                )
                assert tuple(inventory_row[:-1] for inventory_row in retained_pins) == tuple(
                    inventory_row[:-1] for inventory_row in before
                )
                if not replacement:
                    assert retained_pins == before
            if replacement:
                from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
                from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

                inventory = await resolve_retained_registry_source_recipes(
                    reader, second, control_schema=schema, api_read=True
                )
                assert len(inventory) == 2
                assert all(type(recipe.source_pin) is PinnedTypedFHIRMembershipSource for recipe in inventory)
            restored = await publication._publish_source_rollback(context, first, second, require_witness_replay=False)
            assert await publication._membership_snapshot(reader, second) == current
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == second
        async with (
            publication._login(context.worker, "api") as reader,
            reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == restored
            assert await publication._membership_snapshot(reader, restored) == original


async def _assert_typed_field_corruption(connection, actual, expected):
    from process.registry_typed_membership_parity import has_multiset_difference

    for field, changed in (
        ("network_id", "2"),
        ("provider_system", "'manual'"),
        ("provider_id", "'1000000004'"),
        ("location_id", "'00000000-0000-4000-8000-000000000002'::uuid"),
        ("evidence_id", "repeat('c',900)"),
    ):
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.execute(
                "UPDATE typed_parity_actual SET "
                + field
                + "="
                + changed
                + " WHERE ctid=(SELECT ctid FROM typed_parity_actual WHERE provider_system='npi' LIMIT 1)"
            )
            assert await has_multiset_difference(connection, actual, expected)
        finally:
            await transaction.rollback()


async def test_typed_bounded_multiset_parity_hot_duplicate_bucket(monkeypatch):
    """Compare duplicate-heavy multisets and reject each full-row mutation at bounded memory."""
    from process.registry_typed_membership_parity import (
        create_order_index,
        has_multiset_difference,
        require_order_index,
    )

    async with source_witness_case(monkeypatch, named=True) as case, case.reader.transaction():
        for name in ("typed_parity_actual", "typed_parity_expected"):
            await case.reader.execute(
                "CREATE TEMP TABLE "
                + name
                + "(network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL)"
            )
            await create_order_index(case.reader, name, name + "_idx")
            await require_order_index(case.reader, name)
            # Independently declared identical multisets: one hot duplicate key plus 12,000 wide keys.
            await case.reader.execute(
                "INSERT INTO "
                + name
                + " SELECT 1,'npi','1234567893','00000000-0000-4000-8000-000000000001'::uuid,repeat('a',900) FROM generate_series(1,20000) UNION ALL SELECT 1,'provider_directory',repeat('z',900)||n::text,md5(n::text)::uuid,repeat('b',900) FROM generate_series(1,12000) n"
            )
            await case.reader.execute("CREATE INDEX ON " + name + "(network_id)")
            await case.reader.execute("ANALYZE " + name)
        actual, expected = "SELECT * FROM typed_parity_actual", "SELECT * FROM typed_parity_expected"
        for memory in ("4MB", "16MB"):
            await case.reader.execute("SET LOCAL work_mem='" + memory + "'")
            assert await case.reader.fetchval("SHOW temp_file_limit") == "256MB"
            assert not await has_multiset_difference(case.reader, actual, expected)
            async with case.reader.transaction():
                await case.reader.execute(
                    "DELETE FROM typed_parity_actual WHERE ctid=(SELECT ctid FROM typed_parity_actual WHERE provider_system='npi' LIMIT 1)"
                )
                assert await has_multiset_difference(case.reader, actual, expected)
                await case.reader.execute(
                    "INSERT INTO typed_parity_actual SELECT * FROM typed_parity_expected WHERE provider_system='npi' LIMIT 1"
                )
            await _assert_typed_field_corruption(case.reader, actual, expected)
            assert not await has_multiset_difference(case.reader, actual, expected)
            transaction = case.reader.transaction()
            await transaction.start()
            try:
                await case.reader.execute("DELETE FROM typed_parity_actual WHERE provider_system='npi'")
                await case.reader.execute(
                    "INSERT INTO typed_parity_actual VALUES(9,'manual','custom-provider','00000000-0000-4000-8000-000000000003','approved-custom')"
                )
                effective = (
                    expected
                    + " WHERE provider_system<>'npi' UNION ALL SELECT 9,'manual','custom-provider','00000000-0000-4000-8000-000000000003'::uuid,'approved-custom'"
                )
                assert not await has_multiset_difference(case.reader, actual, effective)
            finally:
                await transaction.rollback()


async def test_unsealed_inline_fhir_upgrade_keeps_pinned_readers(monkeypatch):
    """Upgrade a genuine legacy FHIR generation through the ordinary typed path."""
    from tests.network_typed_inline_legacy_support import (
        test_unsealed_inline_retained_source_to_typed_reference_preserves_readers as verify_upgrade,
    )

    await verify_upgrade(monkeypatch)
