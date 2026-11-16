# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database

importer = importlib.import_module("process.provider_directory_fhir")
unified = importlib.import_module("process.entity_address_unified")


CANONICAL_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260611100000_address_canonical_foundation.py"
)
PRACTITIONER_FIXTURE_ROWS = (
    (
        "source-target",
        "practitioner-good",
        1234567890,
        True,
        "run-current",
        [
            {
                "line": ["100 Main Street", "Suite 200"],
                "city": "Austin",
                "state": "Texas",
                "postalCode": "78701",
            },
            {"line": ["Missing City"], "state": "TX", "postalCode": "78701"},
            {"city": "Austin", "state": "TX", "postalCode": "78701"},
            {"line": ["Missing ZIP"], "city": "Austin", "state": "TX"},
        ],
        [
            {"system": "phone", "value": "(312) 555-1212"},
            {"system": "fax", "value": "+1 (312) 555-0199"},
        ],
    ),
    (
        "source-target",
        "practitioner-old-run",
        1234567891,
        True,
        "run-old",
        [{"line": ["200 Old Run Road"], "city": "Austin", "state": "TX", "postalCode": "78702"}],
        [],
    ),
    (
        "source-other",
        "practitioner-other-source",
        1234567892,
        True,
        "run-current",
        [
            {
                "line": ["300 Other Source Road"],
                "city": "Austin",
                "state": "TX",
                "postalCode": "78703",
            }
        ],
        [],
    ),
    (
        "source-target",
        "practitioner-invalid-npi",
        123456789,
        True,
        "run-current",
        [
            {
                "line": ["400 Invalid NPI Road"],
                "city": "Austin",
                "state": "TX",
                "postalCode": "78704",
            }
        ],
        [],
    ),
    (
        "source-target",
        "practitioner-inactive",
        1234567893,
        False,
        "run-current",
        [
            {
                "line": ["500 Inactive Road"],
                "city": "Austin",
                "state": "TX",
                "postalCode": "78705",
            }
        ],
        [],
    ),
)


def _canonical_migration():
    module_name = f"address_canonical_foundation_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, CANONICAL_MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _require_disposable_database() -> None:
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.rsplit("/", 1)[-1].lower():
        pytest.skip(
            "Practitioner address overlay DB test requires HLTHPRT_DB_DATABASE to name a disposable test database"
        )


@asynccontextmanager
async def _temporary_schema(monkeypatch):
    _require_disposable_database()
    database = Database()
    schema = f"practitioner_address_overlay_{uuid.uuid4().hex[:12]}"
    is_schema_created = False
    try:
        try:
            await database.connect()
            actual_database = str(await database.scalar("SELECT current_database();") or "")
        except (OSError, OperationalError) as exc:
            pytest.skip(f"PostgreSQL is not available for practitioner address overlay test: {exc}")
        except Exception as exc:
            pytest.skip(f"PostgreSQL test connection is unavailable: {exc}")
        if "test" not in actual_database.lower():
            pytest.skip("Practitioner address overlay DB test connected to a non-disposable database")
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await database.disconnect()


async def _install_canonical_functions(database: Database, schema: str) -> None:
    migration = _canonical_migration()
    assert database.engine is not None
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: migration._exec_sql_batch(
                sync_connection,
                migration._create_functions_sql(schema),
            )
        )


async def _create_fixture_tables(database: Database, schema: str, stage_table: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}"."provider_directory_practitioner" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            active boolean,
            telecom jsonb,
            addresses jsonb,
            last_seen_run_id varchar(64),
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        );
        """
    )
    await database.status(importer.provider_directory_address_overlay_table_sql(schema, stage_table))


async def _insert_practitioner_rows(database: Database, schema: str) -> None:
    """Insert practitioner rows used by the overlay fixture."""
    for source_id, resource_id, npi, active, run_id, addresses, telecom in PRACTITIONER_FIXTURE_ROWS:
        await database.status(
            f"""
            INSERT INTO "{schema}"."provider_directory_practitioner" (
                source_id, resource_id, npi, active, telecom, addresses,
                last_seen_run_id, updated_at
            ) VALUES (
                :source_id, :resource_id, :npi, :active,
                CAST(:telecom AS jsonb), CAST(:addresses AS jsonb),
                :run_id, TIMESTAMP '2026-07-14 12:00:00'
            );
            """,
            source_id=source_id,
            resource_id=resource_id,
            npi=npi,
            active=active,
            telecom=json.dumps(telecom),
            addresses=json.dumps(addresses),
            run_id=run_id,
        )


def _assert_practitioner_overlay_row(
    overlay_stage_mapping,
    expected_address_key,
) -> None:
    """Assert the normalized practitioner overlay fixture."""
    assert overlay_stage_mapping["source_record_id"] == (
        "provider_directory_fhir:practitioner_address:source-target:practitioner-good:1"
    )
    assert overlay_stage_mapping["source_id"] == "source-target"
    assert overlay_stage_mapping["last_seen_run_id"] == "run-current"
    assert overlay_stage_mapping["resource_type"] == "Practitioner"
    assert overlay_stage_mapping["resource_id"] == "practitioner-good"
    assert overlay_stage_mapping["npi"] == 1234567890
    assert overlay_stage_mapping["address_key"] == expected_address_key
    assert overlay_stage_mapping["state_code"] == "TX"
    assert overlay_stage_mapping["country_code"] == "US"
    assert overlay_stage_mapping["telephone_number"] == "(312) 555-1212"
    assert overlay_stage_mapping["fax_number"] == "+1 (312) 555-0199"
    assert overlay_stage_mapping["phone_number"] == "3125551212"
    assert overlay_stage_mapping["fax_number_digits"] == "3125550199"


async def _assert_absent_fixture_table(database, schema, table):
    """Prove the component query cannot rely on an unrelated input family."""
    assert (
        await database.scalar("SELECT to_regclass(:relation_name) IS NULL;", relation_name=f"{schema}.{table}") is True
    )


@pytest.mark.asyncio
async def test_practitioner_address_overlay_executes_scoped_sql_in_isolated_schema(
    monkeypatch,
):
    """Verify overlay SQL remains scoped to the isolated schema."""
    stage_table = "provider_directory_practitioner_address_stage"
    async with _temporary_schema(monkeypatch) as (database, schema):
        await _install_canonical_functions(database, schema)
        await _create_fixture_tables(database, schema, stage_table)
        await _insert_practitioner_rows(database, schema)

        await _assert_absent_fixture_table(database, schema, "provider_directory_location")
        await _assert_absent_fixture_table(database, schema, "provider_directory_practitioner_role")

        sql = importer._address_overlay_component_insert_sql(
            schema,
            stage_table,
            component="practitioner_address",
            run_id="run-current",
            source_ids=["source-target"],
        )
        inserted = await database.status(
            sql,
            run_id="run-current",
            source_ids=["source-target"],
        )

        assert inserted == 1
        overlay_stage_row = await database.first(
            f"""
            SELECT source_record_id, source_id, last_seen_run_id, resource_type,
                   resource_id, npi, address_key::text, first_line, second_line,
                   city_name, state_name, state_code, postal_code, country_code,
                   telephone_number, fax_number, phone_number, fax_number_digits
              FROM "{schema}"."{stage_table}";
            """
        )
        assert overlay_stage_row is not None
        overlay_stage_mapping = overlay_stage_row._mapping
        expected_address_key = await database.scalar(
            f"""
            SELECT "{schema}".addr_key_v1(
                '100 Main Street', 'Suite 200', 'Austin', 'Texas', '78701', 'US'
            )::text;
            """
        )
        _assert_practitioner_overlay_row(
            overlay_stage_mapping,
            expected_address_key,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_id,postal_field,use,address_type,expected_count",
    [
        ("source-other", "postal_code", "home", "physical", 0),
        ("source-other", "postal_code", "billing", "postal", 0),
        ("source-other", "postal_code", "work", "physical", 0),
        ("source-other", "postalCode", "work", "physical", 1),
        ("cms-npd", "postal_code", "work", "physical", 1),
        ("cms-npd", "postal_code", "billing", "postal", 0),
        ("cms-npd", "postalCode", "work", "physical", 1),
        ("cms-npd", "postalCode", "home", "physical", 0),
    ],
)
async def test_normalized_postal_code_is_cms_scoped(
    monkeypatch, source_id, postal_field, use, address_type, expected_count
):
    """CMS normalization cannot expand another source's accepted addresses."""
    async with _temporary_schema(monkeypatch) as (database, schema):
        await _install_canonical_functions(database, schema)
        await _create_fixture_tables(database, schema, "postal_scope_stage")
        address_by_name = {
            "line": ["100 Main Street"],
            "city": "Austin",
            "state": "TX",
            postal_field: "78701",
            "use": use,
            "type": address_type,
        }
        await database.status(
            f'INSERT INTO "{schema}"."provider_directory_practitioner" '
            "(source_id, resource_id, npi, active, telecom, addresses) "
            "VALUES (:source_id, 'synthetic-practitioner', 1234567893, true, '[]'::jsonb, CAST(:addresses AS jsonb))",
            source_id=source_id,
            addresses=json.dumps([address_by_name]),
        )
        sql = importer._address_overlay_component_insert_sql(
            schema, "postal_scope_stage", component="practitioner_address", source_ids=[source_id]
        )
        assert await database.status(sql, source_ids=[source_id]) == expected_count


async def _seed_cms_dataset_scope_tables(database: Database, schema_name: str) -> None:
    for scoped_relation in ("cms_old_resources", "cms_new_resources", "cms_empty_resources"):
        await database.status(
            f'CREATE TABLE "{schema_name}"."{scoped_relation}" '
            f'(LIKE "{schema_name}"."provider_directory_practitioner" INCLUDING ALL);'
        )
    for scoped_relation, run_id, first_line in (
        ("cms_old_resources", "run-old", "10 Old Road"),
        ("cms_new_resources", "run-new", "20 New Road"),
    ):
        addresses = [
            {
                "line": [first_line],
                "city": "Austin",
                "state": "TX",
                "postal_code": "78701",
                "use": "work",
                "type": "physical",
            },
            {
                "line": ["PO Box 15"],
                "city": "Austin",
                "state": "TX",
                "postal_code": "78701",
                "use": "billing",
                "type": "postal",
            },
        ]
        await database.status(
            f'INSERT INTO "{schema_name}"."{scoped_relation}" '
            "(source_id, resource_id, npi, active, telecom, addresses, last_seen_run_id) "
            "VALUES ('cms-npd', 'practitioner-1', 1234567890, true, '[]'::jsonb, "
            "CAST(:addresses AS jsonb), :run_id)",
            addresses=json.dumps(addresses),
            run_id=run_id,
        )


async def _insert_selected_cms_address(
    database: Database,
    schema_name: str,
    scoped_relation: str,
    overlay_relation: str,
) -> int:
    token = importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
        {"provider_directory_practitioner": scoped_relation}
    )
    try:
        sql = importer._address_overlay_component_insert_sql(
            schema_name,
            overlay_relation,
            component="practitioner_address",
            source_ids=["cms-npd"],
        )
        return await database.status(sql, source_ids=["cms-npd"])
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)


async def _seed_current_cms_dataset(database: Database, schema_name: str) -> None:
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_source" (source_id varchar PRIMARY KEY, endpoint_id varchar)'
    )
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_endpoint_dataset" '
        "(dataset_id varchar PRIMARY KEY, endpoint_id varchar, is_current boolean, "
        "acquisition_root_run_id varchar, import_run_id varchar, status varchar, "
        "published_at timestamp, superseded_at timestamp)"
    )
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_dataset_resource" '
        "(dataset_id varchar, resource_type varchar, resource_id varchar)"
    )
    await database.status(
        f"INSERT INTO \"{schema_name}\".\"provider_directory_source\" VALUES ('cms-npd', 'cms-endpoint')"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_endpoint_dataset" '
        "VALUES ('dataset-new', 'cms-endpoint', true, 'run-new', 'run-new', "
        "'published', TIMESTAMP '2026-07-14 12:00:00', NULL)"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_dataset_resource" '
        "VALUES ('dataset-new', 'Practitioner', 'practitioner-1')"
    )


async def _cms_partial_first_lines(database: Database, schema_name: str) -> list[str]:
    partial_sql = unified._provider_directory_partial_overlay_source_select(
        f'"{schema_name}"',
        {},
        source_ids=["cms-npd"],
        run_id="run-new",
    )
    selected_rows = await database.all(f"SELECT first_line FROM ({partial_sql}) AS selected")
    return [selected_row[0] for selected_row in selected_rows]


@pytest.mark.asyncio
async def test_cms_physical_address_overlay_replaces_only_its_selected_generation(monkeypatch):
    """A selected dataset changes CMS practice rows without removing other sources."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await _create_fixture_tables(database, schema_name, "cms_new_stage")
        await database.status(importer.provider_directory_address_overlay_table_sql(schema_name))
        await _seed_cms_dataset_scope_tables(database, schema_name)
        published_overlay = f'"{schema_name}"."provider_directory_address_overlay"'
        overlay_columns = ", ".join(importer._provider_directory_address_overlay_columns())
        await database.status(
            f"INSERT INTO {published_overlay} (source_record_id, source_id, last_seen_run_id, "
            "resource_type, resource_id, npi, address_key, address_precision) "
            f"VALUES ('other:practice', 'other-source', 'run-other', 'Practitioner', "
            f"'other-1', 1234567891, \"{schema_name}\".addr_key_v1("
            "'30 Other Road', NULL, 'Austin', 'TX', '78701', 'US'), 'street')"
        )
        assert (
            await _insert_selected_cms_address(
                database, schema_name, "cms_old_resources", "provider_directory_address_overlay"
            )
            == 1
        )
        assert await database.scalar(f"SELECT count(*) FROM {published_overlay} WHERE source_id='cms-npd'") == 1
        new_overlay_stage = f'"{schema_name}"."cms_new_stage"'
        assert (
            await importer._copy_existing_address_overlay(
                new_overlay_stage, published_overlay, overlay_columns, ["cms-npd"]
            )
            == 1
        )
        assert await _insert_selected_cms_address(database, schema_name, "cms_new_resources", "cms_new_stage") == 1
        overlay_rows = await database.all(f"SELECT source_id, first_line FROM {new_overlay_stage} ORDER BY source_id")
        assert [(overlay_row[0], overlay_row[1]) for overlay_row in overlay_rows] == [
            ("cms-npd", "20 New Road"),
            ("other-source", None),
        ]
        await _seed_current_cms_dataset(database, schema_name)
        assert await _cms_partial_first_lines(database, schema_name) == []
        await database.status(f"DELETE FROM {published_overlay} WHERE source_id='cms-npd'")
        await database.status(
            f"INSERT INTO {published_overlay} ({overlay_columns}) "
            f"SELECT {overlay_columns} FROM {new_overlay_stage} WHERE source_id='cms-npd'"
        )
        assert await _cms_partial_first_lines(database, schema_name) == ["20 New Road"]
        await database.status(f'DELETE FROM "{schema_name}"."provider_directory_dataset_resource"')
        assert await _cms_partial_first_lines(database, schema_name) == []
        await database.status(importer.provider_directory_address_overlay_table_sql(schema_name, "cms_empty_stage"))
        empty_overlay_stage = f'"{schema_name}"."cms_empty_stage"'
        assert (
            await importer._copy_existing_address_overlay(
                empty_overlay_stage, published_overlay, overlay_columns, ["cms-npd"]
            )
            == 1
        )
        assert await _insert_selected_cms_address(database, schema_name, "cms_empty_resources", "cms_empty_stage") == 0
        assert await database.scalar(f"SELECT count(*) FROM {empty_overlay_stage}") == 1


@pytest.mark.asyncio
async def test_cms_organization_overlay_excludes_postal_address(monkeypatch):
    """CMS work/physical addresses may serve; billing/postal addresses may not."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await database.status(
            f'CREATE TABLE "{schema_name}"."provider_directory_organization" ('
            "source_id varchar, resource_id varchar, npi bigint, active boolean, "
            "address_json jsonb, telecom jsonb, last_seen_run_id varchar, updated_at timestamp)"
        )
        await database.status(importer.provider_directory_address_overlay_table_sql(schema_name, "cms_org_stage"))
        addresses = [
            {
                "line": ["10 Care Lane"],
                "city": "Austin",
                "state": "TX",
                "postalCode": "78701",
                "use": "work",
                "type": "physical",
            },
            {
                "line": ["PO Box 20"],
                "city": "Austin",
                "state": "TX",
                "postalCode": "78701",
                "use": "billing",
                "type": "postal",
            },
        ]
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_organization" '
            "(source_id, resource_id, npi, active, address_json, telecom, last_seen_run_id) "
            "VALUES ('cms-npd', 'organization-1', 1234567890, true, "
            "CAST(:addresses AS jsonb), '[]'::jsonb, 'run-current')",
            addresses=json.dumps(addresses),
        )
        inserted = await database.status(
            importer._address_overlay_component_insert_sql(
                schema_name,
                "cms_org_stage",
                component="organization_address",
                source_ids=["cms-npd"],
            ),
            source_ids=["cms-npd"],
        )
        assert inserted == 1
        assert await database.scalar(f'SELECT first_line FROM "{schema_name}"."cms_org_stage"') == "10 Care Lane"


async def _seed_cms_role_location_rows(database: Database, schema_name: str) -> None:
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_practitioner_role" ('
        "source_id varchar, resource_id varchar, npi bigint, active boolean, "
        "practitioner_ref varchar, location_refs jsonb, healthcare_service_refs jsonb, "
        "telecom jsonb, last_seen_run_id varchar, updated_at timestamp)"
    )
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_healthcare_service" ('
        "source_id varchar, resource_id varchar, location_refs jsonb)"
    )
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_location" ('
        "source_id varchar, resource_id varchar, mode varchar, status varchar, "
        "addresses jsonb, address_key varchar, first_line varchar, second_line varchar, "
        "city_name varchar, state_name varchar, state_code varchar, postal_code varchar, "
        "country_code varchar, telephone_number varchar, fax_number varchar, "
        "phone_number varchar, fax_number_digits varchar, latitude varchar, "
        "longitude varchar, updated_at timestamp)"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_practitioner" '
        "(source_id, resource_id, npi, active, telecom) "
        "VALUES ('cms-npd', 'practitioner-1', 1234567890, true, '[]'::jsonb)"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_practitioner_role" '
        "(source_id, resource_id, practitioner_ref, location_refs, "
        "healthcare_service_refs, telecom, last_seen_run_id) "
        "VALUES ('cms-npd', 'role-1', 'Practitioner/practitioner-1', "
        '\'["Location/kind","Location/postal","Location/physical"]\'::jsonb, '
        "'[]'::jsonb, '[]'::jsonb, 'run-current')"
    )
    for resource_id, mode, use, address_type in (
        ("kind", "kind", "work", "physical"),
        ("postal", "instance", "billing", "postal"),
        ("physical", "instance", "work", "physical"),
    ):
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_location" '
            "(source_id, resource_id, mode, addresses, first_line, city_name, "
            "state_name, postal_code) VALUES "
            "('cms-npd', :resource_id, :mode, CAST(:addresses AS jsonb), "
            ":first_line, 'Austin', 'TX', '78701')",
            resource_id=resource_id,
            mode=mode,
            first_line=f"{resource_id} Road",
            addresses=json.dumps([{"use": use, "type": address_type}]),
        )


@pytest.mark.asyncio
async def test_cms_role_overlay_ignores_abstract_and_postal_locations(monkeypatch):
    """A role points at several locations; only its concrete work site serves."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await _create_fixture_tables(database, schema_name, "cms_role_stage")
        await _seed_cms_role_location_rows(database, schema_name)
        inserted = await database.status(
            importer._address_overlay_component_insert_sql(
                schema_name,
                "cms_role_stage",
                component="practitioner_role",
                source_ids=["cms-npd"],
            ),
            source_ids=["cms-npd"],
        )
        assert inserted == 1
        assert await database.scalar(f'SELECT first_line FROM "{schema_name}"."cms_role_stage"') == "physical Road"


@pytest.mark.asyncio
async def test_cms_role_overlay_rejects_nonlocal_or_wrong_kind_references(monkeypatch):
    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await _create_fixture_tables(database, schema_name, "cms_role_stage")
        await _seed_cms_role_location_rows(database, schema_name)
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_healthcare_service" '
            "VALUES ('cms-npd', 'service-1', '[\"Location/physical\"]'::jsonb)"
        )
        insert_sql = importer._address_overlay_component_insert_sql(
            schema_name, "cms_role_stage", component="practitioner_role", source_ids=["cms-npd"]
        )
        for practitioner_ref, location_refs, service_refs in (
            ("https://other.invalid/Practitioner/practitioner-1", ["Location/physical"], []),
            ("Practitioner/practitioner-1", ["Organization/physical"], []),
            ("Practitioner/practitioner-1", ["https://other.invalid/Location/physical"], []),
            ("Practitioner/practitioner-1", [], ["Organization/service-1"]),
            ("Practitioner/practitioner-1", [], ["https://other.invalid/HealthcareService/service-1"]),
        ):
            await database.status(
                f'UPDATE "{schema_name}"."provider_directory_practitioner_role" '
                "SET practitioner_ref=:practitioner_ref, location_refs=CAST(:location_refs AS jsonb), "
                "healthcare_service_refs=CAST(:service_refs AS jsonb)",
                practitioner_ref=practitioner_ref,
                location_refs=json.dumps(location_refs),
                service_refs=json.dumps(service_refs),
            )
            assert await database.status(insert_sql, source_ids=["cms-npd"]) == 0


@pytest.mark.asyncio
async def test_native_overlay_labels_match_without_rewriting(monkeypatch):
    from tests import provider_directory_address_batch_support as support

    async with support.overlay_fixture(monkeypatch) as (database, schema):
        await support.insert_practitioner_labels(database, schema, "sql_labels")
        await importer._backfill_address_overlay_stage_formatted_addresses(schema, f'"{schema}".sql_labels')
        async with support.batch.scope(importer, schema, "run-control", ["source-target"], ("practitioner_address",)):
            assert support.batch.current() is not None
            await support.insert_practitioner_labels(database, schema, "native_labels")
            assert (
                await importer._backfill_address_overlay_stage_formatted_addresses(
                    schema, f'"{schema}".native_labels', source_ids=["source-target"]
                )
                == 0
            )
        assert await support.label_rows(database, schema, "native_labels") == await support.label_rows(
            database, schema, "sql_labels"
        )
        assert await support.remaining_batch_tables(database, schema) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("move_address", [False, True])
async def test_native_overlay_repairs_changed_and_unsupported_labels(monkeypatch, move_address):
    from tests import provider_directory_address_batch_support as support

    async with support.overlay_fixture(monkeypatch) as (database, schema):
        await support.insert_practitioner_labels(database, schema, "sql_labels")
        async with support.batch.scope(importer, schema, "run-control", ["source-target"], ("practitioner_address",)):
            await support.insert_practitioner_labels(database, schema, "native_labels")
            await database.status(
                f"UPDATE \"{schema}\".native_labels SET formatted_address='stale', "
                "formatted_address_version=0,formatted_address_source='old'"
            )
            if move_address:
                for table in ("sql_labels", "native_labels"):
                    await database.status(
                        f"UPDATE \"{schema}\".{table} SET first_line='300 New Street' "
                        "WHERE resource_id='practitioner-good'"
                    )
            expected = await database.scalar(f'SELECT count(*) FROM "{schema}".native_labels')
            assert expected > 0
            assert (
                await importer._backfill_address_overlay_stage_formatted_addresses(
                    schema, f'"{schema}".native_labels', source_ids=["source-target"]
                )
                == expected
            )
        await importer._backfill_address_overlay_stage_formatted_addresses(schema, f'"{schema}".sql_labels')
        assert await support.label_rows(database, schema, "native_labels") == await support.label_rows(
            database, schema, "sql_labels"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError, asyncio.CancelledError])
async def test_native_overlay_failure_preserves_serving_rows(monkeypatch, error):
    from tests import provider_directory_address_batch_support as support

    async with support.overlay_fixture(monkeypatch) as (database, schema):
        with pytest.raises(error):
            async with support.batch.scope(
                importer, schema, "run-control", ["source-target"], ("practitioner_address",)
            ):
                await support.insert_practitioner_labels(database, schema, "native_labels")
                raise error()
        assert await support.label_rows(database, schema, "sql_labels") == []
        assert await support.remaining_batch_tables(database, schema) == 0


@pytest.mark.asyncio
async def test_native_overlay_ignores_excluded_invalid_json(monkeypatch):
    from tests import provider_directory_address_batch_support as support

    async with support.overlay_fixture(monkeypatch) as (database, schema):
        await database.status(
            f'UPDATE "{schema}".provider_directory_practitioner SET addresses=\'{{"not":"an array"}}\'::jsonb '
            "WHERE resource_id IN ('practitioner-invalid-npi','practitioner-inactive','practitioner-old-run')"
        )
        await support.insert_practitioner_labels(database, schema, "sql_labels", run_id="run-current")
        await importer._backfill_address_overlay_stage_formatted_addresses(schema, f'"{schema}".sql_labels')
        async with support.batch.scope(importer, schema, "run-current", ["source-target"], ("practitioner_address",)):
            await support.insert_practitioner_labels(database, schema, "native_labels", run_id="run-current")
        assert await support.label_rows(database, schema, "native_labels") == await support.label_rows(
            database, schema, "sql_labels"
        )


@pytest.mark.asyncio
async def test_native_overlay_keeps_progress_writes_independent(monkeypatch):
    from tests import provider_directory_address_batch_support as support

    async with support.overlay_fixture(monkeypatch) as (database, schema):
        await database.status(f'CREATE TABLE "{schema}".control_progress(id int PRIMARY KEY,message text)')
        await database.status(f"INSERT INTO \"{schema}\".control_progress VALUES(1,'queued')")
        observer = Database()
        await observer.connect()
        try:
            async with support.batch.scope(
                importer, schema, "run-control", ["source-target"], ("practitioner_address",)
            ):
                assert support.batch.current() is not None
                await database.status(f"UPDATE \"{schema}\".control_progress SET message='publishing' WHERE id=1")
                assert (
                    await observer.scalar(f'SELECT message FROM "{schema}".control_progress WHERE id=1') == "publishing"
                )
                async with observer.transaction():
                    await observer.status("SET LOCAL lock_timeout='100ms'")
                    assert (
                        await observer.status(
                            f"UPDATE \"{schema}\".control_progress SET message='heartbeat' WHERE id=1"
                        )
                        == 1
                    )
        finally:
            await observer.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("move_location", [False, True])
async def test_native_location_fields_match_current_sql(monkeypatch, move_location):
    from tests import provider_directory_address_batch_support as support
    from tests.test_provider_directory_address_overlay_keys_db import _publish_sql_keyed_rows

    async with support.location_overlay_fixture(monkeypatch) as (database, schema, components):
        stage_pages = importer.overlay_keys._stage_location_key_pages

        async def prepare_then_move(*arguments):
            count = await stage_pages(*arguments)
            if move_location:
                await database.status(
                    f"UPDATE \"{schema}\".provider_directory_location SET first_line='200 Oak Street' "
                    "WHERE resource_id='physical'"
                )
            return count

        monkeypatch.setattr(importer.overlay_keys, "_stage_location_key_pages", prepare_then_move)
        with importer.overlay_keys.build_scope("run-control", None):
            async with support.batch.scope(importer, schema, None, ["cms-npd"], components):
                await importer.overlay_keys.insert_components(
                    importer, schema, "batch_stage", None, ["cms-npd"], {"source_ids": ["cms-npd"]}, components
                )
                assert (
                    await importer._backfill_address_overlay_stage_formatted_addresses(
                        schema, f'"{schema}".batch_stage', source_ids=["cms-npd"]
                    )
                    == 0
                )
        await _publish_sql_keyed_rows(database, schema, "sql_current", components, {"source_ids": ["cms-npd"]})
        await importer._backfill_address_overlay_stage_formatted_addresses(schema, f'"{schema}".sql_current')
        assert await support.label_rows(database, schema, "batch_stage") == await support.label_rows(
            database, schema, "sql_current"
        )
        assert await support.remaining_batch_tables(database, schema) == 0


@pytest.mark.asyncio
async def test_native_overlay_respects_unsupported_case_fallback(monkeypatch):
    from tests import provider_directory_address_batch_support as support
    from tests.test_provider_directory_address_overlay_keys_db import _publish_sql_keyed_rows

    async with support.location_overlay_fixture(monkeypatch) as (database, schema, components):
        collation = await database.scalar(
            "SELECT collname FROM pg_collation WHERE collname IN ('tr-x-icu','tr-TR-x-icu') LIMIT 1"
        )
        if collation is None:
            pytest.skip("Turkish ICU collation is unavailable")
        await database.status(
            f'ALTER TABLE "{schema}".provider_directory_location ALTER city_name TYPE varchar '
            f"COLLATE {importer._q(collation)}"
        )
        await database.status(f"UPDATE \"{schema}\".provider_directory_location SET city_name='IZMIR'")
        messages = []

        async def record_step(*, message):
            messages.append(message)

        with importer.overlay_keys.build_scope("run-control", record_step):
            async with support.batch.scope(importer, schema, None, ["cms-npd"], components):
                assert support.batch.current() is None
                await importer.overlay_keys.insert_components(
                    importer, schema, "batch_stage", None, ["cms-npd"], {"source_ids": ["cms-npd"]}, components
                )
        assert "keyed locations rows=0" in messages[0]
        await _publish_sql_keyed_rows(database, schema, "sql_current", components, {"source_ids": ["cms-npd"]})
        assert await support.label_rows(database, schema, "batch_stage") == await support.label_rows(
            database, schema, "sql_current"
        )
        assert await support.remaining_batch_tables(database, schema) == 0


@pytest.mark.asyncio
async def test_cms_affiliation_overlay_rejects_nonlocal_or_wrong_kind_references(monkeypatch):
    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await _create_fixture_tables(database, schema_name, "cms_affiliation_stage")
        await _seed_cms_role_location_rows(database, schema_name)
        await database.status(
            f'CREATE TABLE "{schema_name}"."provider_directory_organization" ('
            "source_id varchar, resource_id varchar, npi bigint, active boolean, "
            "address_json jsonb, telecom jsonb, updated_at timestamp)"
        )
        await database.status(
            f'CREATE TABLE "{schema_name}"."provider_directory_organization_affiliation" ('
            "source_id varchar, resource_id varchar, active boolean, organization_ref varchar, "
            "participating_organization_ref varchar, location_refs jsonb, healthcare_service_refs jsonb, "
            "telecom jsonb, last_seen_run_id varchar, updated_at timestamp)"
        )
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_organization" '
            "VALUES ('cms-npd', 'org-1', 1234567890, true, '[]'::jsonb, '[]'::jsonb, now())"
        )
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_organization_affiliation" '
            "VALUES ('cms-npd', 'aff-1', true, 'Organization/org-1', NULL, "
            '\'["Location/kind","Location/postal","Location/physical"]\'::jsonb, '
            "'[]'::jsonb, '[]'::jsonb, 'run-current', now())"
        )
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_healthcare_service" '
            "VALUES ('cms-npd', 'service-1', '[\"Location/physical\"]'::jsonb)"
        )
        insert_sql = importer._address_overlay_component_insert_sql(
            schema_name, "cms_affiliation_stage", component="organization_affiliation", source_ids=["cms-npd"]
        )
        assert await database.status(insert_sql, source_ids=["cms-npd"]) == 1
        assert (
            await database.scalar(f'SELECT first_line FROM "{schema_name}"."cms_affiliation_stage"') == "physical Road"
        )
        await database.status(f'DELETE FROM "{schema_name}"."cms_affiliation_stage"')
        for organization_ref, location_refs, service_refs in (
            ("https://other.invalid/Organization/org-1", ["Location/physical"], []),
            ("Location/org-1", ["Location/physical"], []),
            ("Organization/org-1", ["Organization/physical"], []),
            ("Organization/org-1", ["https://other.invalid/Location/physical"], []),
            ("Organization/org-1", [], ["Location/service-1"]),
            ("Organization/org-1", [], ["https://other.invalid/HealthcareService/service-1"]),
        ):
            await database.status(
                f'UPDATE "{schema_name}"."provider_directory_organization_affiliation" '
                "SET organization_ref=:organization_ref, location_refs=CAST(:location_refs AS jsonb), "
                "healthcare_service_refs=CAST(:service_refs AS jsonb)",
                organization_ref=organization_ref,
                location_refs=json.dumps(location_refs),
                service_refs=json.dumps(service_refs),
            )
            assert await database.status(insert_sql, source_ids=["cms-npd"]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("desired", [False, True])
async def test_entity_partial_projection_matches_current_or_desired_pins(monkeypatch, desired):
    from tests.entity_address_partial_projection_support import assert_projection_parity

    await assert_projection_parity(monkeypatch, desired)


@pytest.mark.asyncio
async def test_entity_partial_projection_keeps_cached_membership_guard(monkeypatch):
    from tests.test_entity_address_partial_projection import test_projection_keeps_membership_guard_on_cached_reads

    await test_projection_keeps_membership_guard_on_cached_reads(monkeypatch)


@pytest.mark.asyncio
async def test_entity_partial_projection_preserves_preexisting_context(monkeypatch):
    from tests.test_entity_address_partial_projection import (
        test_projection_context_failure_preserves_preexisting_relation,
    )

    await test_projection_context_failure_preserves_preexisting_relation(monkeypatch)


@pytest.mark.asyncio
async def test_entity_partial_projection_cancellation_cleans_exact_sidecars(monkeypatch):
    from tests.test_entity_address_partial_projection import (
        test_partial_projection_rolls_back_create_and_cleans_cancelled_consumer,
    )

    await test_partial_projection_rolls_back_create_and_cleans_cancelled_consumer(monkeypatch)


@pytest.mark.asyncio
async def test_entity_partial_projection_uses_indexed_scope_and_prunes_history(monkeypatch):
    from tests.entity_address_partial_projection_support import assert_projection_plan

    await assert_projection_plan(monkeypatch)


@pytest.mark.asyncio
async def test_entity_partial_projection_requires_existing_npi_index(monkeypatch):
    from tests.test_entity_address_partial_projection import (
        test_partial_projection_without_npi_index_keeps_original_sql,
    )

    await test_partial_projection_without_npi_index_keeps_original_sql(monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_number", [1, 2])
async def test_entity_partial_projection_handles_commit_cancellation(monkeypatch, commit_number):
    from tests.test_entity_address_partial_projection import (
        test_partial_projection_commit_cancellation_retires_created_tables,
    )

    await test_partial_projection_commit_cancellation_retires_created_tables(monkeypatch, commit_number)


@pytest.mark.asyncio
async def test_entity_orphan_cleanup_preserves_active_and_published_tables(monkeypatch):
    async with _temporary_schema(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        await database.status(
            f'CREATE TABLE "{schema}".import_run(run_id text,importer text,status text, '
            "created_at timestamp,finished_at timestamp)"
        )
        await database.status(
            f'INSERT INTO "{schema}".import_run VALUES '
            "('terminal','entity-address-unified','failed',now()-interval '8 hours',now()-interval '7 hours'),"
            "('active','provider-directory-fhir','running',now()-interval '8 hours',NULL),"
            "('recent','entity-address-unified','succeeded',now()-interval '8 hours',now()),"
            "('current','provider-directory-fhir','failed',now()-interval '8 hours',now()-interval '7 hours'),"
            "('other','other-import','failed',now()-interval '8 hours',now()-interval '7 hours')"
        )
        stages_by_run = {
            run: importer._address_overlay_stage_table_name(run)
            for run in ("terminal", "active", "recent", "current", "unknown", "other")
        }
        preserved_tables = [name for run, name in stages_by_run.items() if run != "terminal"] + [
            "provider_directory_address_overlay",
            "entity_address_unified",
        ]
        for name in [stages_by_run["terminal"], *preserved_tables]:
            await database.status(f'CREATE TABLE "{schema}"."{name}"(marker int)')
            await database.status(f'INSERT INTO "{schema}"."{name}" VALUES(7)')
        counts = await importer._cleanup_orphan_address_overlay_stages(
            schema, current_stage_table=stages_by_run["current"]
        )
        assert counts["removed"] == 1
        assert counts["active_stage_retained"] == counts["recent_stage_retained"] == 1
        assert counts["unowned_stage_retained"] == 2
        assert counts["current_stage_retained"] == 1
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.{stages_by_run['terminal']}")
        for name in preserved_tables:
            assert await database.scalar(f'SELECT marker FROM "{schema}"."{name}"') == 7


@pytest.mark.asyncio
async def test_entity_partial_projection_preserves_control_named_collision(monkeypatch):
    from tests.test_entity_address_partial_projection import (
        test_control_named_projection_collision_preserves_existing_table,
    )

    await test_control_named_projection_collision_preserves_existing_table(monkeypatch)
