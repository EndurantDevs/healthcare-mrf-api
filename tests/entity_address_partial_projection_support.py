# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected partial-projection fixtures without changing shared relations."""

import importlib
from contextlib import asynccontextmanager

from process import entity_address_partial_projection as projection
from tests.address_batch_postgres_support import stamp_database

native = importlib.import_module("process.entity_address_unified")
AVAILABLE = dict.fromkeys(native.PROVIDER_DIRECTORY_DATASET_FENCE_TABLES, True) | {"npi_address": True}


async def _create_tables(database, schema):
    await database.status(
        f'CREATE TABLE "{schema}".provider_directory_source(source_id varchar PRIMARY KEY,endpoint_id varchar)'
    )
    await database.status(
        f'CREATE TABLE "{schema}".provider_directory_endpoint_dataset(dataset_id varchar PRIMARY KEY,'
        "endpoint_id varchar,acquisition_root_run_id varchar,import_run_id varchar,status varchar,is_current boolean,"
        "published_at timestamp,validated_at timestamp,superseded_at timestamp,dataset_hash varchar,"
        "publication_metadata_summary_json jsonb,publication_metadata_json jsonb)"
    )
    await database.status(
        f'CREATE TABLE "{schema}".provider_directory_dataset_resource(dataset_id varchar,resource_type varchar,resource_id varchar) PARTITION BY LIST(dataset_id)'
    )
    for suffix, dataset in (("a", "dataset-a"), ("b", "dataset-b")):
        await database.status(
            f'CREATE TABLE "{schema}".members_{suffix} PARTITION OF "{schema}".provider_directory_dataset_resource FOR VALUES IN (\'{dataset}\')'
        )
    await database.status(
        f'CREATE TABLE "{schema}".members_history PARTITION OF "{schema}".provider_directory_dataset_resource DEFAULT'
    )
    await database.status(
        f'CREATE INDEX ON "{schema}".provider_directory_dataset_resource(dataset_id,resource_type,resource_id)'
    )
    types_by_field = dict(
        npi="bigint",
        address_key="uuid",
        lat="numeric",
        long="numeric",
        source_updated_at="timestamp",
        published_at="timestamp",
    )
    columns = ",".join(f"{field} {types_by_field.get(field, 'varchar')}" for field in projection._FIELDS)
    await database.status(f'CREATE TABLE "{schema}".provider_directory_address_overlay({columns})')
    await database.status(
        f'CREATE INDEX ON "{schema}".provider_directory_address_overlay(source_id,last_seen_run_id,resource_type,resource_id)'
    )
    await database.status(f'CREATE INDEX ON "{schema}".provider_directory_address_overlay(npi)')
    await database.status(f'CREATE TABLE "{schema}".affected_groups(entity_npi bigint)')
    await database.status(f'INSERT INTO "{schema}".affected_groups VALUES(1111111111)')
    await database.status(
        f'CREATE TABLE "{schema}".npi_address(npi bigint,type varchar,checksum bigint,taxonomy_array int[],'
        "plans_network_array int[],procedures_array int[],medications_array int[])"
    )
    await database.status(f'CREATE INDEX ON "{schema}".npi_address(npi,type,checksum)')
    await database.status(
        f"INSERT INTO \"{schema}\".npi_address VALUES (1111111111,'primary',2,ARRAY[20],ARRAY[2],ARRAY[3],ARRAY[4]),"
        "(1111111111,'primary',1,ARRAY[10],ARRAY[2],ARRAY[3],ARRAY[4])"
    )


async def _seed(database, schema):
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_source VALUES ('selected','endpoint-a'),"
        "('sibling','endpoint-a'),('other','endpoint-b'),('ambiguous','endpoint-c'),('detached',NULL)"
    )
    await database.status(
        f'INSERT INTO "{schema}".provider_directory_endpoint_dataset VALUES '
        "('dataset-a','endpoint-a','run-a','import-a','published',true,'2026-01-02','2026-01-01',NULL,'hash-a','{\"source_ids\":[\"selected\",\"sibling\"]}','{}'),"
        "('dataset-b','endpoint-b','run-b','import-b','published',true,'2026-01-02','2026-01-01',NULL,'hash-b','{}','{}'),"
        "('dataset-c','endpoint-c','run-c','import-c','published',true,'2026-01-02','2026-01-01',NULL,'hash-c','{}','{}'),"
        "('dataset-c-duplicate','endpoint-c','run-c','import-c','failed',true,NULL,NULL,NULL,'hash-d','{}','{}')"
    )
    inputs = [
        ("selected", "one", "run-a", 1111111111),
        ("sibling", "two", "run-a", 1111111111),
        ("other", "three", "run-b", 1111111111),
        ("other", "four", "run-b", 2222222222),
        ("selected", "stale", "old-run", 1111111111),
        ("selected", "missing", "run-a", 1111111111),
        ("ambiguous", "ambiguous", "run-c", 1111111111),
        ("selected", "invalid-npi", "run-a", 123),
    ]
    for source_id, resource, run_id, npi in inputs:
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_address_overlay '
            "(source_record_id,source_id,last_seen_run_id,resource_type,resource_id,npi,first_line,city_name,state_code,postal_code,country_code,source_updated_at,published_at) "
            "VALUES(:rid,:source,:run,'PractitionerRole',:resource,:npi,'10 Main Street','Boston','MA','02101','US','2026-01-01','2026-01-02')",
            rid=f"provider_directory_fhir:practitioner_role:{source_id}:{resource}",
            source=source_id,
            run=run_id,
            resource=resource,
            npi=npi,
        )
        if resource != "missing":
            await database.status(
                f"INSERT INTO \"{schema}\".provider_directory_dataset_resource VALUES(:dataset,'PractitionerRole',:resource)",
                dataset={
                    "selected": "dataset-a",
                    "sibling": "dataset-a",
                    "other": "dataset-b",
                    "ambiguous": "dataset-c",
                }[source_id],
                resource=resource,
            )


@asynccontextmanager
async def projection_database(monkeypatch, *, desired=False):
    async with stamp_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(native, "db", database)
        await _create_tables(database, schema)
        await _seed(database, schema)
        token = None
        if desired:
            await database.status(
                f'CREATE TABLE "{schema}".desired_overlay (LIKE "{schema}".provider_directory_address_overlay INCLUDING ALL)'
            )
            await database.status(
                f'INSERT INTO "{schema}".desired_overlay SELECT * FROM "{schema}".provider_directory_address_overlay'
            )
            await database.status(f'TRUNCATE "{schema}".provider_directory_address_overlay')
            await database.status(
                f"UPDATE \"{schema}\".provider_directory_endpoint_dataset SET status='validated',is_current=false,published_at=NULL WHERE dataset_id='dataset-a'"
            )
            preparation = native.candidate_preparation
            selected = preparation.ProviderDirectoryAddressDatasetPin(
                "selected", "endpoint-a", "dataset-a", "hash-a", "run-a"
            )
            sibling = preparation.ProviderDirectoryAddressDatasetPin(
                "sibling", "endpoint-a", "dataset-a", "hash-a", "run-a"
            )
            other = preparation.ProviderDirectoryAddressDatasetPin(
                "other", "endpoint-b", "dataset-b", "hash-b", "run-b"
            )
            inputs = preparation.ProviderDirectoryAddressPreparationInput(
                dataset_pins=(selected, sibling, other),
                overlay_table="desired_overlay",
                address_alias_generation=0,
                semantic_as_of="2026-01-03",
                ordinary_serving_peer_pins=(other,),
            )
            token = preparation._PREPARATION.set(inputs)
        try:
            yield database, schema
        finally:
            if token is not None:
                native.candidate_preparation._PREPARATION.reset(token)


async def selected_rows(database, statement):
    rows = await database.all(statement)
    return sorted(tuple(row) for row in rows)


async def assert_projection_parity(monkeypatch, desired):
    async with projection_database(monkeypatch, desired=desired) as (database, schema):
        expected_scoped = native._provider_directory_partial_overlay_source_select(
            schema, AVAILABLE, source_ids=["selected"], run_id="run-a"
        )
        expected_replacement = native._provider_directory_partial_overlay_source_select(
            schema, AVAILABLE, affected_group_table="affected_groups"
        )
        async with projection.scoped_projection(
            native, schema, "test_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
        ) as relation:
            assert relation is not None
            scoped = projection.cached_overlay_source(
                native, schema, AVAILABLE, source_ids=["selected"], run_id="run-a"
            )
            await projection.prepare_load(native, schema, AVAILABLE, "affected_groups")
            replacement = projection.cached_overlay_source(
                native, schema, AVAILABLE, affected_group_table="affected_groups"
            )
            assert await selected_rows(database, scoped) == await selected_rows(database, expected_scoped)
            assert await selected_rows(database, replacement) == await selected_rows(database, expected_replacement)
            assert len(await selected_rows(database, scoped)) == 2
            assert len(await selected_rows(database, replacement)) == 3
            groups = projection.group_input_sql(native, schema)
            assert "npi_address" not in groups and "taxonomy" not in groups
            assert await database.scalar(f"SELECT count(*) FROM ({groups}) groups") == 2
            assert projection.is_cached_source_sql(replacement)
            assert ".npi_address" not in replacement and "npi_context" in replacement
            assert await database.scalar(f"SELECT taxonomy_array[1] FROM ({replacement}) source LIMIT 1") == 10
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=relation)


def plan_nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from plan_nodes(child)


async def _seed_plan_inputs(database, schema):
    await database.status(f'''INSERT INTO "{schema}".provider_directory_dataset_resource
        SELECT 'dataset-a','PractitionerRole','filler-'||i::text FROM generate_series(1,5000) i
        UNION ALL SELECT 'history-dataset','PractitionerRole','history-'||i::text FROM generate_series(1,5000) i''')
    await database.status(f'''INSERT INTO "{schema}".desired_overlay(source_record_id,source_id,last_seen_run_id,resource_type,resource_id,npi,first_line,city_name,state_name,postal_code,country_code)
        SELECT 'unaffected-'||i::text,'other','run-b','PractitionerRole','unaffected-'||i::text,2222222000+i,'1 Street','Boston','MA','02101','US' FROM generate_series(1,5000) i''')
    for table in ("provider_directory_dataset_resource", "desired_overlay"):
        await database.status(f'ANALYZE "{schema}"."{table}"')


async def assert_projection_plan(monkeypatch):
    import json
    from dataclasses import replace

    async with projection_database(monkeypatch, desired=True) as (database, schema):
        await _seed_plan_inputs(database, schema)
        preparation = native.candidate_preparation
        inputs = preparation.current()
        peers = tuple(replace(inputs.dataset_pins[-1], source_id=f"peer-{number}") for number in range(47))
        for peer in peers:
            await database.status(
                f"INSERT INTO \"{schema}\".provider_directory_source VALUES(:source,'endpoint-b')",
                source=peer.source_id,
            )
        token = preparation._PREPARATION.set(
            replace(
                inputs,
                dataset_pins=inputs.dataset_pins + peers,
                ordinary_serving_peer_pins=inputs.ordinary_serving_peer_pins + peers,
            )
        )
        try:
            async with projection.scoped_projection(
                native, schema, "plan_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ):
                scope = projection._ACTIVE.get().scope_relation
                scope_plan = await database.scalar(f"EXPLAIN (ANALYZE,FORMAT JSON) SELECT * FROM {scope}")
                if isinstance(scope_plan, str):
                    scope_plan = json.loads(scope_plan)
                assert scope_plan[0]["Plan"]["Plan Rows"] == scope_plan[0]["Plan"]["Actual Rows"] == 50
                statement = projection._raw_sql(native, schema, affected_group_table="affected_groups")
                plan = await database.scalar("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + statement)
                if isinstance(plan, str):
                    plan = json.loads(plan)
                nodes = list(plan_nodes(plan[0]["Plan"]))
                assert any(
                    node.get("Relation Name") == "desired_overlay" and "Index" in node["Node Type"] for node in nodes
                )
                assert any("dataset_id" in node.get("Index Cond", "") for node in nodes)
                history_nodes = [node for node in nodes if node.get("Relation Name") == "members_history"]
                assert history_nodes and all(node["Actual Loops"] == 0 for node in history_nodes)
        finally:
            preparation._PREPARATION.reset(token)
