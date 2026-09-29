# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Desired retained-resource SELECT parity with the complete physical overlay population."""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_overlay_projection as projection
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_address_format_db import _load_migration
from tests.test_provider_directory_practitioner_address_overlay_db import _install_canonical_functions

fhir = importlib.import_module("process.provider_directory_fhir")
SOURCE_KEY = "00000000-0000-0000-0000-000000000001"
TARGET_KEY = "00000000-0000-0000-0000-000000000002"
PREMISE_KEY = "10000000-0000-0000-0000-000000000001"
STALE_PREMISE = "20000000-0000-0000-0000-000000000001"


def _fence():
    return fhir.ProviderDirectoryArtifactDatasetFence(
        tuple(
            fhir.ProviderDirectoryArtifactDataset(
                source_id=source,
                endpoint_id=source + "-endpoint",
                dataset_id=source + "-desired",
                evidence_run_id=source + "-root",
                retained_resources=tuple(projection._INPUT_TYPES.values()),
            )
            for source in ("cms-npd", "selected-source")
        )
    )


def _address(line, **extra):
    return {
        "line": [line],
        "city": "Example",
        "state": "NY",
        "postalCode": "10001",
        "country": "United States",
        "use": "work",
        "type": "physical",
        **extra,
    }


async def _resource(database, schema, source, kind, identifier, payload, *, dataset=None):
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource VALUES (:dataset,:kind,:identifier,CAST(:payload AS jsonb))",
        dataset=dataset or source + "-desired",
        kind=kind,
        identifier=identifier,
        payload=json.dumps(payload),
    )


async def _seed_people(database, schema):
    """Seed organization and practitioner address/telecom fixtures."""
    for identifier, npi in (("org-one", 1000000002), ("org-two", 1000000001)):
        await _resource(
            database,
            schema,
            "cms-npd",
            "Organization",
            identifier,
            {
                "npi": npi,
                "active": True,
                "address_json": [_address("10 Café Road"), _address("Home", use="home")],
                "telecom": [{"system": "phone", "value": "202-555-0100"}],
            },
        )
    await _resource(
        database,
        schema,
        "cms-npd",
        "Practitioner",
        "person",
        {
            "npi": 1000000003,
            "active": True,
            "addresses": [_address("50 Work Road", postal_code="10002"), _address("Home", use="home")],
            "telecom": [{"system": "fax", "value": "202-555-0101"}],
        },
    )


async def _seed_sites(database, schema):
    """Seed aliased, coordinate-bearing and zero-coordinate Location fixtures."""
    for identifier, line, latitude, longitude, key in (
        ("site", "20 Grid Road", "0", "0", SOURCE_KEY),
        ("coordinates", "30 Coordinates Road", "41", "-74", None),
        ("zero", "40 Zero Road", None, None, None),
    ):
        await _resource(
            database,
            schema,
            "cms-npd",
            "Location",
            identifier,
            {
                "status": "active",
                "mode": "instance",
                "addresses": [_address(line)],
                "first_line": line,
                "city_name": "Example",
                "state_name": "NY",
                "state_code": "NY",
                "postal_code": "10001",
                "country_code": "USA",
                "latitude": latitude,
                "longitude": longitude,
                "address_key": key,
            },
        )


async def _seed_relationships(database, schema):
    """Seed healthcare service, practitioner role and organization affiliation links."""
    await _resource(database, schema, "cms-npd", "HealthcareService", "service", {"location_refs": ["Location/site"]})
    await _resource(
        database,
        schema,
        "cms-npd",
        "PractitionerRole",
        "role",
        {
            "active": True,
            "practitioner_ref": "Practitioner/person",
            "location_refs": ["Location/site", "Location/coordinates", "Location/zero"],
            "healthcare_service_refs": ["HealthcareService/service"],
        },
    )
    await _resource(
        database,
        schema,
        "cms-npd",
        "OrganizationAffiliation",
        "affiliation",
        {
            "active": True,
            "organization_ref": "Organization/org-one",
            "participating_organization_ref": "Organization/org-two",
            "location_refs": ["Location/site"],
        },
    )


async def _seed_resources(database, schema):
    """Seed retained resources for selected and unchanged source parity."""
    await database.status(f"""CREATE TABLE {schema}.provider_directory_endpoint_dataset (
        dataset_id text,published_at timestamp,validated_at timestamp,created_at timestamp)""")
    await database.status(f"""CREATE TABLE {schema}.provider_directory_dataset_resource (
        dataset_id text,resource_type text,resource_id text,payload_json jsonb)""")
    for source_id in ("cms-npd", "selected-source"):
        await database.status(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES (:dataset,NULL,'2026-01-02','2026-01-01')",
            dataset=source_id + "-desired",
        )
    await _seed_people(database, schema)
    await _seed_sites(database, schema)
    await _seed_relationships(database, schema)
    await _resource(
        database,
        schema,
        "selected-source",
        "Organization",
        "unchanged",
        {
            "npi": 1000000004,
            "address_json": [_address("60 Unchanged Road", country="USA")],
        },
    )
    # An incumbent dataset and an unselected source must never enter selected typed inputs.
    await _resource(
        database,
        schema,
        "cms-npd",
        "Organization",
        "stale",
        {
            "npi": 1000000005,
            "address_json": [_address("Excluded Road")],
        },
        dataset="cms-old",
    )
    await _resource(
        database,
        schema,
        "unselected-source",
        "Organization",
        "excluded",
        {
            "npi": 1000000006,
            "address_json": [_address("Excluded Road")],
        },
    )


async def _seed_incumbent(database, schema):
    await database.status(fhir.provider_directory_address_overlay_table_sql(schema))
    for identifier, source, country, key, lat in (
        ("cms-stale", "cms-npd", "US", "3", None),
        ("selected-stale", "selected-source", "US", "4", None),
        ("copied", "unselected-source", " United States ", "5", 11),
        ("missing", "unselected-source", " zz ", "6", None),
        ("merged", "unselected-source", "", "7", None),
        ("revoked", "unselected-source", "", "8", None),
    ):
        await database.status(
            f"""INSERT INTO {schema}.provider_directory_address_overlay
            (source_record_id,source_id,resource_type,resource_id,npi,address_key,premise_key,
             first_line,city_name,state_name,postal_code,country_code,lat,formatted_address,
             formatted_address_version,formatted_address_source,published_at)
            VALUES (:id,:source,'Organization',:id,1000000007,CAST(:key AS uuid),CAST(:premise AS uuid),
             '70 Copied Road','Example','NY','10001',:country,:lat,'Retained label',99,'incumbent','2025-01-01')""",
            id=identifier,
            source=source,
            key=f"00000000-0000-0000-0000-{int(key):012d}",
            premise=STALE_PREMISE,
            country=country,
            lat=lat,
        )


async def _seed_archive(database, schema):
    for statement in (
        f"""CREATE TABLE {schema}.address_archive_v2 (
            address_key uuid PRIMARY KEY,premise_key uuid,identity_key text,merged_into uuid,lat numeric,long numeric)""",
        f"""CREATE TABLE {schema}.address_alias_v1 (
            source_address_key uuid PRIMARY KEY,target_address_key uuid,source_identity_key text,target_identity_key text,revoked_at timestamp)""",
        f"""CREATE TABLE {schema}.address_alias_state_v1 (
            singleton boolean,schema_version integer,active_ruleset_version integer,generation bigint)""",
        f"INSERT INTO {schema}.address_alias_state_v1 VALUES (true,2,1,3)",
    ):
        await database.status(statement)
    await database.status(
        f"""INSERT INTO {schema}.address_archive_v2 VALUES
        (CAST(:target AS uuid),CAST(:premise AS uuid),CAST(:target AS text),NULL,40,-75),
        ('00000000-0000-0000-0000-000000000005',CAST(:premise AS uuid),NULL,NULL,42,-76),
        ('00000000-0000-0000-0000-000000000007',CAST(:premise AS uuid),NULL,CAST(:target AS uuid),43,-77),
        ('00000000-0000-0000-0000-000000000008',NULL,NULL,NULL,0,0),
        ({schema}.addr_key_v1('40 Zero Road',NULL,'Example','NY','10001','US'),NULL,NULL,NULL,0,0)""",
        target=TARGET_KEY,
        premise=PREMISE_KEY,
    )
    await database.status(
        f"""INSERT INTO {schema}.address_alias_v1 VALUES
        (CAST(:source AS uuid),CAST(:target AS uuid),
         {schema}.addr_identity_key_v1('20 Grid Road',NULL,'Example','NY','10001','US'),CAST(:target AS text),NULL),
        ('00000000-0000-0000-0000-000000000008',CAST(:source AS uuid),NULL,NULL,now())""",
        source=SOURCE_KEY,
        target=TARGET_KEY,
    )


async def _setup(database, schema, monkeypatch):
    monkeypatch.setattr(fhir, "db", database)
    await _install_canonical_functions(database, schema)
    formatter = _load_migration()
    await database.status(formatter._humanize_component_function_sql(schema))
    await database.status(formatter._formatted_address_function_sql(schema))
    await _seed_resources(database, schema)
    await _seed_incumbent(database, schema)
    await _seed_archive(database, schema)
    fence = _fence()
    for resource_type in projection._INPUT_TYPES.values():
        model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
        await database.status(fhir._provider_directory_artifact_scope_table_sql(model, schema, model.__tablename__))
        await database.status(
            fhir._provider_directory_artifact_resource_insert_sql(model, schema, model.__tablename__),
            source_ids=[item.source_id for item in fence.datasets],
            dataset_ids=[item.dataset_id for item in fence.datasets],
            evidence_run_ids=[item.evidence_run_id for item in fence.datasets],
            resource_type=resource_type,
        )
    await database.status(
        f"CREATE UNLOGGED TABLE {schema}.physical_overlay (LIKE {schema}.provider_directory_address_overlay INCLUDING DEFAULTS)"
    )
    return fence


async def _physical(database, schema, fence):
    sources = [item.source_id for item in fence.datasets]
    return await fhir._populate_address_overlay_stage(
        schema,
        "physical_overlay",
        f"{schema}.physical_overlay",
        None,
        sources,
        {"source_ids": sources},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("coordinates", ["archive", "source", "alias_zero"])
async def test_desired_overlay_select_equals_complete_physical_population(monkeypatch, coordinates):
    async with _delta_database(monkeypatch) as (database, schema):
        fence = await _setup(database, schema, monkeypatch)
        if coordinates == "source":
            await database.status(f"""UPDATE {schema}.provider_directory_dataset_resource
                SET payload_json=payload_json || '{{"latitude":"41","longitude":"-74"}}'::jsonb
                WHERE resource_type='Location' AND resource_id='site'""")
            await database.status(
                f"UPDATE {schema}.provider_directory_location SET latitude='41',longitude='-74' WHERE resource_id='site'"
            )
        if coordinates == "alias_zero":
            await database.status(
                f"UPDATE {schema}.address_archive_v2 SET lat=0,long=0 WHERE address_key=CAST(:key AS uuid)",
                key=TARGET_KEY,
            )
        async with database.transaction():
            metrics = await _physical(database, schema, fence)
            statement = projection.desired_overlay_statement_sql(fhir, schema, fence)
            assert (
                await database.scalar(f"""WITH virtual AS MATERIALIZED ({statement}), difference AS (
                (SELECT * FROM virtual EXCEPT ALL SELECT * FROM {schema}.physical_overlay)
                UNION ALL (SELECT * FROM {schema}.physical_overlay EXCEPT ALL SELECT * FROM virtual)
            ) SELECT count(*) FROM difference""")
                == 0
            )
        assert metrics["duplicates_removed"] == 1
        assert metrics["copied_existing"] == 4
        rows_by_id = {
            overlay_row.source_record_id: overlay_row
            for overlay_row in await database.all(f"SELECT * FROM {schema}.physical_overlay")
        }
        assert len(rows_by_id) == 12
        assert "cms-stale" not in rows_by_id and "selected-stale" not in rows_by_id
        assert rows_by_id["copied"].country_code == "US"
        assert str(rows_by_id["copied"].premise_key) == PREMISE_KEY
        assert (rows_by_id["copied"].lat, rows_by_id["copied"].long) == (11, -76)
        assert rows_by_id["copied"].formatted_address == "Retained label"
        assert rows_by_id["missing"].country_code == "ZZ" and rows_by_id["missing"].premise_key is None
        assert rows_by_id["merged"].premise_key is None and rows_by_id["merged"].lat is None
        assert rows_by_id["revoked"].lat is None and rows_by_id["revoked"].long is None
        alias = rows_by_id["provider_directory_fhir:practitioner_role:cms-npd:role:site"]
        assert str(alias.address_key) == TARGET_KEY and str(alias.premise_key) == PREMISE_KEY
        coordinates_by_case = {"archive": (40, -75), "source": (41, -74), "alias_zero": (0, 0)}
        assert (alias.lat, alias.long) == coordinates_by_case[coordinates]
        assert alias.formatted_address_version == fhir.ADDRESS_FORMAT_VERSION
        affiliation = rows_by_id["provider_directory_fhir:organization_affiliation:cms-npd:affiliation:site"]
        assert affiliation.npi == 1000000001
        practitioner_rows = [
            overlay_row for overlay_row in rows_by_id.values() if overlay_row.resource_type == "Practitioner"
        ]
        assert len(practitioner_rows) == 1 and practitioner_rows[0].postal_code == "10002"
        unchanged_rows = [
            overlay_row for overlay_row in rows_by_id.values() if overlay_row.source_id == "selected-source"
        ]
        assert len(unchanged_rows) == 1 and unchanged_rows[0].last_seen_run_id == "selected-source-root"


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", ["source", "target", "missing", "merged", "multi_hop"])
async def test_desired_overlay_alias_validation_fails_even_when_consumer_is_empty(monkeypatch, violation):
    async with _delta_database(monkeypatch) as (database, schema):
        fence = await _setup(database, schema, monkeypatch)
        statement = projection.desired_overlay_statement_sql(fhir, schema, fence)
        if violation in ("source", "target"):
            await database.status(
                f"UPDATE {schema}.address_alias_v1 SET {violation}_identity_key=NULL WHERE revoked_at IS NULL"
            )
        elif violation == "missing":
            await database.status(
                f"DELETE FROM {schema}.address_archive_v2 WHERE address_key=CAST(:key AS uuid)", key=TARGET_KEY
            )
        elif violation == "merged":
            await database.status(
                f"UPDATE {schema}.address_archive_v2 SET merged_into=CAST(:key AS uuid) WHERE address_key=CAST(:target AS uuid)",
                key=SOURCE_KEY,
                target=TARGET_KEY,
            )
        else:
            await database.status(
                f"INSERT INTO {schema}.address_alias_v1 VALUES (CAST(:target AS uuid),CAST(:source AS uuid),NULL,NULL,NULL)",
                target=TARGET_KEY,
                source=SOURCE_KEY,
            )
        with pytest.raises(RuntimeError, match="alias integrity violation"):
            await _physical(database, schema, fence)
        for predicate in ("TRUE", "source_id='absent-source'"):
            with pytest.raises(DBAPIError, match="provider_directory_overlay_alias_integrity_violation"):
                await database.scalar(f"SELECT count(*) FROM ({statement}) desired WHERE {predicate}")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["READ COMMITTED READ ONLY", "REPEATABLE READ READ WRITE"])
async def test_desired_overlay_factory_rejects_an_unsafe_borrowed_snapshot(monkeypatch, mode):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(fhir, "db", database)
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + mode))
            with pytest.raises(RuntimeError, match="cms_overlay_projection_requires_unmodified_read_snapshot"):
                await projection.desired_overlay_projection(fhir, object(), _fence(), object())


def _bind_expected_inputs(monkeypatch, fence):
    execution = object()
    expected_by_field = {
        "desired_fence_hash": projection.desired_fence_hash(fence),
        "native_dependencies": {"alias_generation": 3},
        "native_input_fence": {"version": 1},
    }
    address = SimpleNamespace(
        fhir=fhir, execution=execution, input_hash="a" * 64, input_json=json.dumps(expected_by_field)
    )
    address._current_input = lambda _fence, _expected_by_field: address.input_json
    monkeypatch.setattr(projection, "resolve_desired_fence", AsyncMock(return_value=fence))
    monkeypatch.setattr(
        projection, "capture_native_dependencies", AsyncMock(return_value=expected_by_field["native_dependencies"])
    )
    monkeypatch.setattr(
        projection,
        "capture_native_address_input_fence",
        AsyncMock(return_value=expected_by_field["native_input_fence"]),
    )
    return execution, address


@pytest.mark.asyncio
@pytest.mark.parametrize("borrowed", [False, True])
async def test_desired_overlay_factory_returns_the_complete_read_only_query(monkeypatch, borrowed):
    async with _delta_database(monkeypatch) as (database, schema):
        fence = await _setup(database, schema, monkeypatch)
        await _physical(database, schema, fence)
        execution, address = _bind_expected_inputs(monkeypatch, fence)
        if borrowed:
            async with database.transaction() as session:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                result = await projection.desired_overlay_projection(fhir, execution, fence, address)
        else:
            result = await projection.desired_overlay_projection(fhir, execution, fence, address)
        assert result.statement_sql == projection.desired_overlay_statement_sql(fhir, schema, fence)
        assert result.native_address_input_hash == address.input_hash
        assert result.desired_fence_hash == projection.desired_fence_hash(fence)
        # now() follows each builder transaction; the same-transaction proof above compares it too.
        columns = ",".join(
            column for column in fhir._provider_directory_address_overlay_columns() if column != "published_at"
        )
        assert (
            await database.scalar(f"""WITH virtual AS MATERIALIZED ({result.statement_sql}), difference AS (
            (SELECT {columns} FROM virtual EXCEPT ALL SELECT {columns} FROM {schema}.physical_overlay)
            UNION ALL (SELECT {columns} FROM {schema}.physical_overlay EXCEPT ALL SELECT {columns} FROM virtual)
        ) SELECT count(*) FROM difference""")
            == 0
        )
        assert projection.capture_native_dependencies.await_count == 2
        assert projection.capture_native_address_input_fence.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["input", "fence", "dependencies", "native", "aliases", "overrides"])
async def test_desired_overlay_factory_fails_closed_on_changed_inputs(monkeypatch, changed):
    async with _delta_database(monkeypatch) as (database, schema):
        fence = await _setup(database, schema, monkeypatch)
        execution, address = _bind_expected_inputs(monkeypatch, fence)
        if changed == "input":
            address._current_input = lambda _fence, _expected: "changed"
        if changed == "fence":
            projection.resolve_desired_fence.return_value = fhir.ProviderDirectoryArtifactDatasetFence(())
        if changed == "dependencies":
            projection.capture_native_dependencies.return_value = {"alias_generation": 4}
        if changed == "native":
            projection.capture_native_address_input_fence.return_value = {"version": 2}
        if changed == "aliases":
            await database.status(f"UPDATE {schema}.address_alias_state_v1 SET generation=4")
        token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
            {"any": "scratch"} if changed == "overrides" else {}
        )
        try:
            with pytest.raises(RuntimeError):
                await projection.desired_overlay_projection(fhir, execution, fence, address)
        finally:
            fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)


@pytest.mark.asyncio
async def test_desired_overlay_empty_resources_and_incumbent_produce_no_rows(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        fence = await _setup(database, schema, monkeypatch)
        await database.status(
            f"TRUNCATE {schema}.provider_directory_dataset_resource,{schema}.provider_directory_address_overlay"
        )
        for resource_type in projection._INPUT_TYPES.values():
            await database.status(f"TRUNCATE {schema}.{fhir.RESOURCE_MODELS_BY_TYPE[resource_type].__tablename__}")
        assert (await _physical(database, schema, fence))["stage_rows"] == 0
        statement = projection.desired_overlay_statement_sql(fhir, schema, fence)
        assert await database.scalar(f"SELECT count(*) FROM ({statement}) desired") == 0
