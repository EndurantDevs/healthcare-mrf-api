# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL correctness and Phase 2 network-membership parity tests for compact-v2."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process import network_fhir_membership_source as membership
from process import provider_directory_compact_edition as edition
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_v2 as compact_v2
from process import provider_directory_generic_candidate as candidate
from process import registry_fhir_catalog_extraction as catalog
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, read_fhir_membership_batch
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.test_network_fhir_membership_source_postgres import _evidence, _generation, _resource, _source_tables
from tests.test_provider_directory_compact_payload import resource

pytestmark = pytest.mark.asyncio

_BASE_URL = "https://api.example.test/fhir/r4"
_LEGACY_NET_ONE = "11111111-1111-4111-8111-111111111111"
_LEGACY_NET_TWO = "22222222-2222-4222-8222-222222222222"


@asynccontextmanager
async def _v2_storage(*, with_index=True):
    """Create an isolated PostgreSQL database containing compact-v2 candidate tables."""
    async with _owned_database(_database_url().set(database="postgres")) as (database_url, _admin):
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        tables = compact.CompactPayloadTables(
            "compact_v2_test",
            "aliases",
            "sets",
            "blocks",
            "shared_bodies",
            "reference_tokens",
            shared_body_index="shared_body_index" if with_index else None,
            is_v2=True,
        )
        try:
            await connection.execute("CREATE SCHEMA compact_v2_test")
            for statement in compact.compact_payload_ddl(tables):
                await connection.execute(statement)
            await connection.execute(
                "CREATE TABLE compact_v2_test.checkpoint(id integer PRIMARY KEY,rows_processed bigint)"
            )
            await connection.execute("INSERT INTO compact_v2_test.checkpoint VALUES(1,0)")
            yield connection, tables
        finally:
            await connection.close()


def _shared_role_observations():
    service_one = "HealthcareService/" + "a" * 32
    service_two = "HealthcareService/" + "b" * 32
    network_refs = [f"Organization/net-{index}" for index in range(25)]
    shared_fields_by_name = {
        "npi": "1234567890",
        "network_refs": network_refs,
        "resource_url": f"{_BASE_URL}/PractitionerRole/role-shared",
        "fhir_self_url": f"{_BASE_URL}/PractitionerRole/role-shared",
        "floats": [-0.0, 1e20, 1e-20],
    }
    return [
        {
            **resource(
                "role-shared",
                **shared_fields_by_name,
                healthcare_service_refs=[service_one],
                fhir_meta={"lastUpdated": "2026-11-26T00:00:00Z"},
            ),
            "resource_type": "PractitionerRole",
        },
        {
            **resource(
                "role-shared",
                **shared_fields_by_name,
                healthcare_service_refs=[],
                fhir_meta={"lastUpdated": "2026-11-26T01:00:00Z"},
            ),
            "resource_type": "PractitionerRole",
        },
        {
            **resource(
                "role-services",
                npi=1987654321,
                healthcare_service_refs=[service_one, service_two, service_one, "HealthcareService/custom"],
                network_refs=["Organization/net-a"],
                resource_url=f"{_BASE_URL}/PractitionerRole/role-services",
                fhir_self_url=f"{_BASE_URL}/PractitionerRole/role-services",
            ),
            "resource_type": "PractitionerRole",
        },
    ]


def _sample_observations():
    """Build representative observations covering dedup, URL elision, floats, nulls, and malformed refs."""
    return [
        *_shared_role_observations(),
        {
            **resource(
                "loc-nulls",
                healthcare_service_refs=None,
                network_refs=None,
                resource_url=f"{_BASE_URL}/Location/loc-nulls",
                fhir_self_url="https://other.example.test/Location/loc-nulls",
            ),
            "resource_type": "Location",
        },
        {
            **resource(
                "role-malformed",
                network_refs=["Organization/net-1", 42],
                resource_url=None,
                fhir_self_url=f"{_BASE_URL}/PractitionerRole/role-malformed",
            ),
            "resource_type": "PractitionerRole",
        },
        {
            **resource(
                "prac-1",
                npi="1234567890",
                resource_url=f"{_BASE_URL}/Practitioner/prac-1",
                fhir_self_url=f"{_BASE_URL}/Practitioner/prac-1",
            ),
            "resource_type": "Practitioner",
        },
    ]


async def _append_with_rollback_and_replay(connection, tables, sample_rows, cache):
    """Commit batch 1, abort batch 2 without polluting cache, then replay batch 1 and commit batch 2."""
    async with connection.transaction():
        first_batch = await compact.append_compact_payload_batch(
            connection,
            tables,
            sample_rows[:3],
            dataset_id="dataset",
            batch_ordinal=1,
            format_id=compact.COMPACT_V2_FORMAT,
            cache=cache,
        )
        assert first_batch["rows"] == 3 and first_batch["is_replay"] is False
        await connection.execute("UPDATE compact_v2_test.checkpoint SET rows_processed=3")
    cache.apply(first_batch["pending_cache"])

    with pytest.raises(asyncpg.UniqueViolationError):
        async with connection.transaction():
            await compact.append_compact_payload_batch(
                connection,
                tables,
                sample_rows[3:],
                dataset_id="dataset",
                batch_ordinal=2,
                format_id=compact.COMPACT_V2_FORMAT,
                cache=cache,
            )
            await connection.execute("INSERT INTO compact_v2_test.checkpoint VALUES(1,99)")

    async with connection.transaction():
        replay_batch = await compact.append_compact_payload_batch(
            connection,
            tables,
            sample_rows[:3],
            dataset_id="dataset",
            batch_ordinal=1,
            format_id=compact.COMPACT_V2_FORMAT,
            cache=cache,
        )
        assert replay_batch["is_replay"] is True
        second_batch = await compact.append_compact_payload_batch(
            connection,
            tables,
            sample_rows[3:],
            dataset_id="dataset",
            batch_ordinal=2,
            format_id=compact.COMPACT_V2_FORMAT,
            cache=cache,
        )
        assert second_batch["rows"] == 3 and second_batch["is_replay"] is False
    cache.apply(second_batch["pending_cache"])


async def _assert_v2_tamper_and_bounds(connection, tables):
    """Verify body tamper, standalone prefix-token tamper, and 1-based text[] bounds are rejected."""
    alias_sql = f"SELECT * FROM {tables.relation('aliases')}"
    async with connection.transaction():
        await connection.execute(
            "UPDATE compact_v2_test.shared_bodies "
            "SET bodies[1]=((bodies[1]::jsonb || '{\"tampered\":true}'::jsonb)::text) "
            "WHERE shared_block_ordinal=0"
        )
        with pytest.raises(candidate.GenericCandidateError, match="compact_shared_body_changed"):
            await compact_v2.validate_v2_storage(connection, tables, alias_sql, "dataset")
        await connection.execute("ROLLBACK")

    async with connection.transaction():
        await connection.execute(
            "UPDATE compact_v2_test.reference_tokens "
            "SET reference_text='https://tampered.example.test' "
            "WHERE reference_text=$1",
            _BASE_URL,
        )
        with pytest.raises(candidate.GenericCandidateError, match="compact_reference_changed"):
            await compact_v2.validate_v2_storage(connection, tables, alias_sql, "dataset")
        await connection.execute("ROLLBACK")

    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            "INSERT INTO compact_v2_test.shared_bodies VALUES('dataset',999,'[0:0]={\"{}\"}'::text[])"
        )


async def test_compact_v2_roundtrip_cache_replay_and_tamper():
    """Verify lossless v2 round-trip, rollback-safe cache replay, and set-based tamper detection."""
    sample_rows = _sample_observations()
    cache = compact_v2.CompactV2Cache()
    async with _v2_storage() as (connection, tables):
        await _append_with_rollback_and_replay(connection, tables, sample_rows, cache)
        assert await connection.fetchval("SELECT count(*) FROM compact_v2_test.shared_body_index") == 5
        assert await connection.fetchval("SELECT count(*) FROM compact_v2_test.aliases WHERE block_ordinal=-1") >= 2
        await compact_v2.validate_v2_storage(
            connection, tables, f"SELECT * FROM {tables.relation('aliases')}", "dataset"
        )

        async with connection.transaction(isolation="repeatable_read", readonly=True):
            stored_rows = await compact.read_compact_payload_page(connection, tables, "dataset", limit=50)
            expected_entries = sorted(
                [(1, index, sample_row) for index, sample_row in enumerate(sample_rows[:3])]
                + [(2, index, sample_row) for index, sample_row in enumerate(sample_rows[3:])],
                key=lambda item: (item[2]["resource_type"], item[2]["resource_id"], item[0], item[1]),
            )
            assert len(stored_rows) == len(expected_entries) == 6
            for stored, (batch_ord, row_ord, source_row) in zip(stored_rows, expected_entries, strict=True):
                assert (stored["batch_ordinal"], stored["row_ordinal"]) == (batch_ord, row_ord)
                assert compact._json(json.loads(stored["payload_json"])) == compact._json(source_row["payload_json"])
            npi_matches = await compact.read_compact_payload_page(connection, tables, "dataset", npi="1234567890")
            assert len(npi_matches) == 3

        await _assert_v2_tamper_and_bounds(connection, tables)


async def test_compact_v2_bounded_cache_and_missing_block(monkeypatch):
    """Preserve DB token IDs across cache overflow and reject replay when a shared block is missing."""
    monkeypatch.setattr(compact_v2, "_MAX_CACHED_TOKENS", 2)
    cache = compact_v2.CompactV2Cache()
    sample_rows = _sample_observations()
    async with _v2_storage() as (connection, tables):
        async with connection.transaction():
            first_write = await compact.append_compact_payload_batch(
                connection,
                tables,
                sample_rows[:2],
                dataset_id="dataset",
                batch_ordinal=1,
                format_id=compact.COMPACT_V2_FORMAT,
                cache=cache,
            )
        cache.apply(first_write["pending_cache"])
        assert len(cache.token_id_by_ref) == 2 and not cache.is_token_cache_complete
        tokens_before = await connection.fetchval("SELECT count(*) FROM compact_v2_test.reference_tokens")

        async with connection.transaction():
            second_write = await compact.append_compact_payload_batch(
                connection,
                tables,
                sample_rows[:1],
                dataset_id="dataset",
                batch_ordinal=2,
                format_id=compact.COMPACT_V2_FORMAT,
                cache=cache,
            )
        cache.apply(second_write["pending_cache"])
        assert await connection.fetchval("SELECT count(*) FROM compact_v2_test.reference_tokens") == tokens_before

        async with connection.transaction():
            await connection.execute("DELETE FROM compact_v2_test.shared_bodies WHERE shared_block_ordinal=0")
            with pytest.raises(candidate.GenericCandidateError, match="body_hash_collision"):
                await compact.append_compact_payload_batch(
                    connection,
                    tables,
                    sample_rows[:2],
                    dataset_id="dataset",
                    batch_ordinal=1,
                    format_id=compact.COMPACT_V2_FORMAT,
                )
            await connection.execute("ROLLBACK")


async def _publish_v2_and_flat_schemas(connection, loading_tables, distinct_rows):
    """Promote loaded v2 tables into a published edition without shared_body_index beside a flat copy."""
    async with connection.transaction():
        await compact.append_compact_payload_batch(
            connection,
            loading_tables,
            distinct_rows,
            dataset_id="dataset",
            batch_ordinal=1,
            format_id=compact.COMPACT_V2_FORMAT,
        )
        await connection.execute("DROP TABLE compact_v2_test.shared_body_index")
        for source_name, target_name in (
            ("aliases", edition.OUTPUT_ALIASES),
            ("sets", edition.REFERENCE_SETS),
            ("blocks", edition.BODY_BLOCKS),
            ("shared_bodies", edition.SHARED_BODIES),
            ("reference_tokens", edition.REFERENCE_TOKENS),
        ):
            await connection.execute(f'ALTER TABLE compact_v2_test."{source_name}" RENAME TO "{target_name}"')
        await connection.execute(f"CREATE TABLE compact_v2_test.{edition.FORMAT_TABLE}(format_id text PRIMARY KEY)")
        await connection.execute(
            f"INSERT INTO compact_v2_test.{edition.FORMAT_TABLE} VALUES($1)", compact.COMPACT_V2_FORMAT
        )
        published_tables = await edition.compact_edition_tables(connection, "compact_v2_test")
        assert published_tables.is_v2 and published_tables.shared_body_index is None
        await connection.execute(
            "CREATE VIEW compact_v2_test.provider_directory_dataset_resource AS "
            + compact.compact_resource_sql(published_tables)
        )
        await connection.execute("CREATE SCHEMA plain_v2_test")
        await connection.execute(
            "CREATE TABLE plain_v2_test.provider_directory_dataset_resource("
            "dataset_id text,resource_type text,resource_id text,payload_hash text,payload_json json)"
        )
        await connection.copy_records_to_table(
            "provider_directory_dataset_resource",
            schema_name="plain_v2_test",
            records=[
                (
                    entry["dataset_id"],
                    entry["resource_type"],
                    entry["resource_id"],
                    entry["payload_hash"],
                    json.dumps(entry["payload_json"]),
                )
                for entry in distinct_rows
            ],
        )
    return published_tables


async def test_compact_v2_published_edition_without_body_index():
    """Verify published compact-v2 editions omit shared_body_index while matching flat catalog scans."""
    distinct_rows = [entry for index, entry in enumerate(_sample_observations()) if index != 1]
    async with _v2_storage() as (connection, loading_tables):
        published_tables = await _publish_v2_and_flat_schemas(connection, loading_tables, distinct_rows)
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            compact_rows = [
                catalog_row
                async for page in catalog.iter_fhir_catalog_pages(
                    connection, schema_name="compact_v2_test", dataset_id="dataset", page_rows=2, page_bytes=8192
                )
                for catalog_row in page
            ]
            plain_rows = [
                catalog_row
                async for page in catalog.iter_fhir_catalog_pages(
                    connection, schema_name="plain_v2_test", dataset_id="dataset", page_rows=2, page_bytes=8192
                )
                for catalog_row in page
            ]
            assert compact_rows == plain_rows
            npi_hits = await edition.compact_edition_page(
                connection,
                published_tables,
                "dataset",
                resource_types=("Practitioner", "PractitionerRole", "Location"),
                page_bytes=8192,
                npi="1234567890",
            )
            assert [(entry["resource_type"], entry["resource_id"]) for entry in npi_hits] == [
                ("Practitioner", "prac-1"),
                ("PractitionerRole", "role-shared"),
            ]


def _parity_role_entries():
    base_role_by_field = {"practitioner_ref": "Practitioner/p1", "location_refs": ["Location/loc-1"]}
    return [
        (
            "PractitionerRole",
            "r1",
            {
                **base_role_by_field,
                "network_refs": ["Organization/net-1"],
                "healthcare_service_refs": ["HealthcareService/" + "f" * 32] * 10,
                "active": True,
                "resource_url": f"{_BASE_URL}/PractitionerRole/r1",
                "fhir_self_url": f"{_BASE_URL}/PractitionerRole/r1",
            },
        ),
        (
            "OrganizationAffiliation",
            "a1",
            {
                "participating_organization_ref": "Organization/org-p",
                "location_refs": ["Location/loc-1"],
                "network_refs": [],
                "insurance_plan_refs": ["InsurancePlan/plan-1"],
                "active": True,
            },
        ),
        (
            "PractitionerRole",
            "r2-inactive",
            {**base_role_by_field, "network_refs": ["Organization/net-1"], "active": False},
        ),
        ("PractitionerRole", "r3-nullnet", {**base_role_by_field, "network_refs": None, "active": True}),
        (
            "PractitionerRole",
            "r4-badnet",
            {**base_role_by_field, "network_refs": ["Organization/net-1", 99], "active": True},
        ),
    ]


def _parity_resources():
    """Return synthetic FHIR resources exercising direct, plan-mediated, inactive, null, and malformed roles."""
    return [
        ("Practitioner", "p1", {"npi": "1234567890", "resource_url": f"{_BASE_URL}/Practitioner/p1"}),
        ("Organization", "org-p", {"npi": "1987654321", "resource_url": f"{_BASE_URL}/Organization/org-p"}),
        ("Organization", "net-1", {"name": "Net One", "resource_url": f"{_BASE_URL}/Organization/net-1"}),
        ("Organization", "net-2", {"name": "Net Two", "resource_url": f"{_BASE_URL}/Organization/net-2"}),
        ("Location", "loc-1", {"name": "Main Office", "resource_url": f"{_BASE_URL}/Location/loc-1"}),
        (
            "InsurancePlan",
            "plan-1",
            {"network_refs": ["Organization/net-2"], "resource_url": f"{_BASE_URL}/InsurancePlan/plan-1"},
        ),
        *_parity_role_entries(),
    ]


async def _seed_registry_schema(connection):
    """Create minimal network registry identity and alias tables for parity reads."""
    await connection.execute("CREATE SCHEMA reg_test")
    await connection.execute("CREATE TABLE reg_test.network_registry_identity(network_id integer PRIMARY KEY)")
    await connection.execute(
        "CREATE TABLE reg_test.network_registry_alias("
        "source_system text,source_id text,alias_type text,alias_value text,scope_key text,network_id integer)"
    )
    await connection.execute("INSERT INTO reg_test.network_registry_identity VALUES (101), (102)")
    await connection.execute(
        "INSERT INTO reg_test.network_registry_alias VALUES "
        "('fhir','source-example','legacy_fhir_uuid',$1,'scope-1',101),"
        "('fhir','source-example','legacy_fhir_uuid',$2,'scope-1',102)",
        _LEGACY_NET_ONE,
        _LEGACY_NET_TWO,
    )


async def _convert_schema_to_compact_v2(connection, schema_name):
    """Replace the flat resource table in schema_name with a published compact-v2 edition."""
    flat_rows = await connection.fetch(
        f"SELECT dataset_id,resource_type,resource_id,payload_hash,payload_json,acquired_resource_sha256 "
        f'FROM "{schema_name}".provider_directory_dataset_resource ORDER BY resource_type,resource_id'
    )
    load_tables = compact.CompactPayloadTables(
        schema_name,
        edition.OUTPUT_ALIASES,
        edition.REFERENCE_SETS,
        edition.BODY_BLOCKS,
        edition.SHARED_BODIES,
        edition.REFERENCE_TOKENS,
        shared_body_index="tmp_body_index",
        is_v2=True,
    )
    async with connection.transaction():
        for statement in compact.compact_payload_ddl(load_tables):
            await connection.execute(statement)
        await compact.append_compact_payload_batch(
            connection,
            load_tables,
            [dict(flat_row) for flat_row in flat_rows],
            dataset_id="edition-one",
            batch_ordinal=1,
            format_id=compact.COMPACT_V2_FORMAT,
        )
        await connection.execute(f'DROP TABLE "{schema_name}".tmp_body_index')
        await connection.execute(
            f'ALTER TABLE "{schema_name}".{edition.OUTPUT_ALIASES} '
            f"DROP CONSTRAINT {edition.OUTPUT_ALIASES}_pkey, "
            "ADD PRIMARY KEY(dataset_id,resource_type,resource_id)"
        )
        await connection.execute(f'DROP TABLE "{schema_name}".provider_directory_dataset_resource')
        published_tables = compact.CompactPayloadTables(
            schema_name,
            edition.OUTPUT_ALIASES,
            edition.REFERENCE_SETS,
            edition.BODY_BLOCKS,
            edition.SHARED_BODIES,
            edition.REFERENCE_TOKENS,
            is_v2=True,
        )
        await connection.execute(
            f'CREATE VIEW "{schema_name}".provider_directory_dataset_resource AS '
            + compact.compact_resource_sql(published_tables)
        )
        await connection.execute(f'CREATE TABLE "{schema_name}".{edition.FORMAT_TABLE}(format_id text PRIMARY KEY)')
        await connection.execute(
            f'INSERT INTO "{schema_name}".{edition.FORMAT_TABLE} VALUES($1)', compact.COMPACT_V2_FORMAT
        )


async def _seed_parity_schema(connection, engine, monkeypatch, schema_name, *, is_compact):
    """Populate a flat edition schema and optionally convert it in place to compact-v2."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    await connection.execute(f'CREATE SCHEMA "{schema_name}"')
    await _source_tables(engine, schema_name)
    await connection.execute(
        f'INSERT INTO "{schema_name}".provider_directory_api_endpoint '
        "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
        f"VALUES('endpoint-example','{_BASE_URL}',$1,$1)",
        "e" * 64,
    )
    await _generation(connection, schema_name)
    digest = "a" * 64
    await _evidence(connection, schema_name, "Location", "loc-1", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    await _evidence(connection, schema_name, "Organization", "net-1", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    await _evidence(connection, schema_name, "Organization", "net-2", "cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    await connection.execute(
        f'INSERT INTO "{schema_name}".provider_directory_insurance_network_identity VALUES($1,now()),($2,now())',
        _LEGACY_NET_ONE,
        _LEGACY_NET_TWO,
    )
    await connection.execute(
        f'INSERT INTO "{schema_name}".provider_directory_insurance_network_source_binding '
        "VALUES('source-example','Organization','net-1',$1,now()),"
        "('source-example','Organization','net-2',$2,now())",
        _LEGACY_NET_ONE,
        _LEGACY_NET_TWO,
    )
    await connection.execute(
        f'INSERT INTO "{schema_name}".provider_directory_insurance_network_plan_evidence VALUES '
        "('source-example','release-one','Organization','net-2','plan-1','[\"Organization/net-2\"]',NULL,NULL,$1,'{}',now())",
        digest,
    )
    for resource_type, resource_id, resource_payload in _parity_resources():
        await _resource(connection, schema_name, resource_type, resource_id, resource_payload, digest=digest)
    if is_compact:
        await _convert_schema_to_compact_v2(connection, schema_name)


async def _assert_membership_and_witness_parity(connection, flat_pin, compact_pin):
    """Compare batch extraction, witness facts, and compact alias rewrites between flat and compact-v2."""
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        flat_batch = await read_fhir_membership_batch(connection, flat_pin, registry_schema="reg_test", limit=100)
        compact_batch = await read_fhir_membership_batch(connection, compact_pin, registry_schema="reg_test", limit=100)
        assert compact_batch.source_rows == flat_batch.source_rows == 5
        assert compact_batch.membership_rows == flat_batch.membership_rows == 2
        assert compact_batch.unresolved_rows == flat_batch.unresolved_rows
        assert compact_batch.accepted_source_rows == flat_batch.accepted_source_rows == 2
        assert compact_batch.omitted_source_rows == flat_batch.omitted_source_rows == 1
        assert compact_batch.unresolved_source_rows == flat_batch.unresolved_source_rows == 2
        assert compact_batch.input_bytes == flat_batch.input_bytes
        assert compact_batch.input_sha256 == flat_batch.input_sha256
        assert [
            (entry.resource_type, entry.resource_id, entry.outcome, entry.reason)
            for entry in compact_batch.source_outcomes
        ] == [
            (entry.resource_type, entry.resource_id, entry.outcome, entry.reason)
            for entry in flat_batch.source_outcomes
        ]

        published_tables = await edition.compact_edition_tables(connection, "compact_ed")
        flat_witness = await connection.fetchrow(
            membership._source_witness_sql_v2(flat_pin), *membership._witness_query_arguments(flat_pin, None, 100)
        )
        compact_witness = await connection.fetchrow(
            membership._source_witness_sql_v2(compact_pin, compact_tables=published_tables),
            *membership._witness_query_arguments(compact_pin, None, 100),
        )
        assert compact_witness["source_rows"] == flat_witness["source_rows"] == 5
        assert (compact_witness["last_type"], compact_witness["last_id"]) == (
            flat_witness["last_type"],
            flat_witness["last_id"],
        )
        flat_facts = [
            {field_name: field_val for field_name, field_val in fact.items() if field_name != "source_binding"}
            for fact in json.loads(flat_witness["source_facts"])
        ]
        compact_facts = [
            {field_name: field_val for field_name, field_val in fact.items() if field_name != "source_binding"}
            for fact in json.loads(compact_witness["source_facts"])
        ]
        assert compact_facts == flat_facts

        compact_sql = membership._extraction_sql(compact_pin, '"reg_test"', False, compact_tables=published_tables)
        assert "provider.npi_text" in compact_sql
        assert f'"compact_ed"."{edition.OUTPUT_ALIASES}" provider' in compact_sql
        assert f'"compact_ed"."{edition.OUTPUT_ALIASES}" location' in compact_sql
        assert f'"compact_ed"."{edition.OUTPUT_ALIASES}" network' in compact_sql
        assert "page_network_sets" in compact_sql


async def test_compact_v2_membership_and_witness_parity(monkeypatch):
    """Verify exact byte-for-byte parity between flat and compact-v2 membership and witness extraction."""
    async with _owned_database(_database_url().set(database="postgres")) as (database_url, _admin):
        engine = create_async_engine(database_url.render_as_string(hide_password=False))
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        try:
            await _seed_registry_schema(connection)
            await _seed_parity_schema(connection, engine, monkeypatch, "flat_ed", is_compact=False)
            await _seed_parity_schema(connection, engine, monkeypatch, "compact_ed", is_compact=True)
            flat_pin = PinnedFHIRMembershipSource(
                schema_name="flat_ed",
                source_id="source-example",
                endpoint_id="endpoint-example",
                dataset_id="edition-one",
                dataset_sha256="d" * 64,
                release_id="release-one",
                resource_relation={"schema": "flat_ed", "name": "provider_directory_dataset_resource"},
                alias_scope="scope-1",
                as_of="2026-11-26",
            )
            compact_pin = replace(
                flat_pin,
                schema_name="compact_ed",
                resource_relation={"schema": "compact_ed", "name": "provider_directory_dataset_resource"},
            )
            await _assert_membership_and_witness_parity(connection, flat_pin, compact_pin)
        finally:
            await connection.close()
            await engine.dispose()
