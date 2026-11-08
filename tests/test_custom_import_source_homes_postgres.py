# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual composed migrations, SOURCE homes, candidate index lifecycle and plans."""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_source as source
from process.custom_import.storage_layout import snapshot_schema
from tests.custom_import_postgres_support import (
    _migration,
    _seed_publication_identity,
    isolated_publication_case,
    lease_digest,
)
from tests.test_custom_import_bulk_snapshot_writers_postgres import _bootstrap
from tests.test_custom_import_materialization_postgres import _live_generation
from tests.test_custom_import_snapshot_finality import _rejection_reference_query

_ROOT = Path(__file__).resolve().parents[1]
_CHAIN = (
    "20261005040000_custom_import_bulk_snapshot_writers",
    "20261005050000_custom_import_legacy_snapshot_writers",
    "20261005060000_custom_import_snapshot_finality",
    "20261005070000_custom_import_materialization_storage",
    "20261005080000_custom_import_writer_cutover",
    "20261007000000_custom_import_rejection_anti_joins",
)
_OWNER_INPUTS = """
    WITH expected_build(build_id) AS (VALUES (7)),
    custom_import_build_stream(build_id,stream_slot) AS (VALUES (7,1),(7,2),(7,4)),
    streams(stream_slot,record_kind,collection_slot) AS (VALUES (1,'root',NULL),(2,'child',1),(3,'root',NULL)),
    custom_import_pack AS (
        SELECT p.*,5 AS execution_id,2 AS dataset_id,3 AS definition_revision_id,4 AS schema_revision_id,
            6 AS capture_bundle_id,7 AS producing_fence,decode(repeat('00',32),'hex') AS producing_token_sha256
        FROM (VALUES (10,1),(20,2),(30,3),(40,4)) p(pack_id,stream_slot)
    ), custom_import_root_record(root_record_id,dataset_id) AS (VALUES (100,2),(101,99)),
    custom_import_build_occurrence(occurrence_id,build_id,stream_slot,pack_id,record_kind,collection_slot,root_record_id)
    AS MATERIALIZED (VALUES
        (1,7,1,10,'root',0,100), (2,7,2,20,'child',1,100),
        (3,8,1,10,'root',0,100), (4,7,3,30,'root',0,100),
        (5,7,4,40,'root',0,100), (6,7,1,99,'root',0,100),
        (7,7,1,10,'root',0,101), (8,7,1,99,'root',0,999),
        (9,7,1,10,'root',0,NULL), (10,7,1,10,'root',0,999),
        (11,7,1,10,'child',1,100), (12,7,2,20,'child',2,100), (13,7,1,20,'root',0,100)
    )
"""
_RESOLVED_INPUTS = """
    WITH cases(case_id,mode) AS (VALUES
        (1,'same'),(2,'same-both'),(3,'parent-initial'),(4,'parent-resolved'),
        (5,'wrong-build'),(6,'wrong-pack'),(7,'wrong-slot'),(8,'wrong-token'),
        (9,'null-references'),(10,'wrong-parent-hash'),(11,'wrong-parent-canonical'),
        (12,'wrong-parent-kind'),(13,'wrong-event-kind'),(14,'retained-event'),
        (15,'null-parent-hash'),(16,'null-parent-canonical'),(17,'null-resolved'),
        (18,'missing-rejection'),(19,'wrong-source-ordinal'),(20,'different-initial')
    ), custom_import_rejection AS (
        SELECT 100+case_id AS rejection_id,5 AS execution_id,2 AS dataset_id,3 AS definition_revision_id,
            4 AS schema_revision_id,7 AS producing_fence,
            decode(repeat(CASE WHEN mode='wrong-token' THEN '01' ELSE '00' END,32),'hex') AS producing_token_sha256,
            1000+case_id AS pack_id,3000+case_id AS source_ordinal,1 AS collection_slot
        FROM cases WHERE mode<>'missing-rejection'
    ), custom_import_build_occurrence AS MATERIALIZED (
        SELECT case_id AS occurrence_id,7 AS build_id,'source' AS origin,
            CASE WHEN mode IN ('same','same-both') THEN 1000 ELSE 2000 END+case_id AS pack_id,
            CASE WHEN mode IN ('same','same-both') THEN 3000 ELSE 4000 END+case_id AS source_ordinal,
            CASE WHEN mode IN ('same','same-both') THEN 1 ELSE 0 END AS collection_slot,
            CASE WHEN mode='same-both' THEN 100+case_id END AS rejection_id,
            CASE WHEN mode<>'null-resolved' THEN 100+case_id END AS resolved_rejection_id,
            CASE WHEN mode IN ('same','same-both','wrong-parent-kind') THEN 'child' ELSE 'root' END AS record_kind,
            CASE WHEN mode<>'null-parent-hash' THEN decode('01','hex') END AS raw_parent_key_sha256,
            CASE WHEN mode<>'null-parent-canonical' THEN 'parent-'||case_id END AS raw_parent_key_canonical
        FROM cases
        UNION ALL
        SELECT 100+case_id,CASE WHEN mode='wrong-build' THEN 8 ELSE 7 END,
            CASE WHEN mode='retained-event' THEN 'retained' ELSE 'source' END,
            1000+case_id+CASE WHEN mode='wrong-pack' THEN 1 ELSE 0 END,
            3000+case_id+CASE WHEN mode='wrong-source-ordinal' THEN 1 ELSE 0 END,
            CASE WHEN mode='wrong-slot' THEN 2 ELSE 1 END,
            CASE WHEN mode='different-initial' THEN 10000+case_id
                WHEN mode NOT IN ('parent-resolved','null-references') THEN 100+case_id END,
            CASE WHEN mode IN ('parent-resolved','different-initial') THEN 100+case_id END,
            CASE WHEN mode='wrong-event-kind' THEN 'root' ELSE 'child' END,
            CASE WHEN mode='wrong-parent-hash' THEN decode('02','hex')
                WHEN mode<>'null-parent-hash' THEN decode('01','hex') END,
            CASE WHEN mode='wrong-parent-canonical' THEN 'other-'||case_id
                WHEN mode<>'null-parent-canonical' THEN 'parent-'||case_id END
        FROM cases WHERE mode NOT IN ('same','same-both')
    )
"""
_SCALAR_INPUTS = """
    WITH cases(case_id,mode) AS (VALUES
        (1,'valid'),(2,'missing-revision'),(3,'missing-field'),(4,'wrong-type'),
        (5,'wrong-projection'),(6,'unprojected'),(7,'wrong-dataset'),(8,'wrong-schema'),
        (9,'wrong-root'),(10,'wrong-child-collection'),(11,'wrong-field-collection'),
        (12,'null-type'),(13,'null-projection'),(14,'null-root'),(15,'wrong-definition'),
        (16,'second-field'),(17,'foreign-field-collection')
    ), custom_import_root_revision AS MATERIALIZED (
        SELECT case_id AS root_revision_id,2 AS dataset_id,4 AS schema_revision_id,
            CASE WHEN mode='wrong-definition' THEN 99 ELSE 3 END AS definition_revision_id,
            1000+case_id AS root_record_id FROM cases WHERE mode<>'missing-revision'
    ), custom_import_child_revision AS MATERIALIZED (
        SELECT root_revision_id AS child_revision_id,r.*,1 AS collection_slot FROM custom_import_root_revision r
    ), custom_import_field(schema_revision_id,dataset_id,field_slot,collection_slot,field_type,projection_slot)
    AS MATERIALIZED (VALUES (4,2,1,0,'number',1),(4,2,2,0,'string',2),(4,2,3,0,'number',0),
        (4,2,11,1,'number',11),(4,2,12,1,'string',12),(4,2,13,1,'number',0)
    ), custom_import_root_scalar AS MATERIALIZED (
        SELECT case_id AS root_revision_id,mode,
            CASE mode WHEN 'missing-field' THEN 99 WHEN 'unprojected' THEN 3
                WHEN 'second-field' THEN 2 WHEN 'foreign-field-collection' THEN 11 ELSE 1 END AS field_slot,
            CASE WHEN mode='wrong-dataset' THEN 99 ELSE 2 END AS dataset_id,
            CASE WHEN mode='wrong-schema' THEN 99 ELSE 4 END AS schema_revision_id,
            CASE WHEN mode='wrong-root' THEN 999 WHEN mode<>'null-root' THEN 1000+case_id END AS root_record_id,
            CASE WHEN mode='wrong-field-collection' THEN 99 ELSE 0 END AS field_collection_slot,
            CASE WHEN mode IN ('wrong-type','second-field') THEN 'string'
                WHEN mode<>'null-type' THEN 'number' END AS field_type,
            CASE WHEN mode='wrong-projection' THEN 99 WHEN mode='unprojected' THEN 0
                WHEN mode='second-field' THEN 2 WHEN mode<>'null-projection' THEN 1 END AS projection_slot
        FROM cases
    ), custom_import_child_scalar AS MATERIALIZED (
        SELECT root_revision_id AS child_revision_id,dataset_id,schema_revision_id,root_record_id,
            CASE WHEN mode='wrong-child-collection' THEN 2 ELSE 1 END AS collection_slot,
            field_collection_slot+1 AS field_collection_slot,field_type,
            CASE WHEN projection_slot=0 THEN 0 ELSE projection_slot+10 END AS projection_slot,
            CASE WHEN mode='foreign-field-collection' THEN 1 ELSE field_slot+10 END AS field_slot
        FROM custom_import_root_scalar
    )
"""
_SELECTION_INPUTS = """
    WITH cases(case_id,mode,is_child) AS (VALUES
        (1,'root',false),(2,'child',true),(3,'missing-family',false),(4,'missing-member',false),
        (5,'missing-profile',false),(6,'wrong-entity',false),(7,'missing-build',false),
        (8,'wrong-profile-context',true),(9,'missing-edge',true),(10,'wrong-child',true),
        (11,'wrong-collection',true),(12,'null-child',true),(13,'root-with-child',false),
        (14,'root-with-collection',false),(15,'wrong-scope',false),(16,'null-entities',false),
        (17,'null-entity',false),(18,'edge-scope',true)
    ), custom_import_family_revision AS MATERIALIZED (
        SELECT case_id AS family_revision_id,1000+case_id AS root_record_id,
            CASE WHEN mode<>'null-entities' THEN 77 END AS entity_binding_id
        FROM cases WHERE mode<>'missing-family'
    ), custom_import_generation_family AS MATERIALIZED (
        SELECT 1 AS generation_id,2 AS dataset_id,3 AS definition_revision_id,4 AS schema_revision_id,
            case_id AS family_revision_id FROM cases WHERE mode<>'missing-member'
    ), custom_import_selection_profile(definition_revision_id,dataset_id,schema_revision_id,profile_slot,context_collection_slot)
    AS MATERIALIZED (VALUES (3,2,4,1,NULL),(3,2,4,2,1),(3,2,4,3,2)),
    custom_import_build_attempt AS MATERIALIZED (
        SELECT build_id,5 AS execution_id,7 AS producing_fence,decode(repeat('00',32),'hex') AS producing_token_sha256,
            1 AS generation_id,2 AS dataset_id,definition_revision_id,4 AS schema_revision_id
        FROM (VALUES (7,3),(8,99)) builds(build_id,definition_revision_id)
    ), custom_import_winner AS MATERIALIZED (
        SELECT case_id AS family_revision_id,mode,1 AS generation_id,2 AS dataset_id,4 AS schema_revision_id,
            CASE WHEN mode='wrong-scope' THEN 99 ELSE 3 END AS definition_revision_id,
            CASE WHEN mode='missing-profile' THEN 99 WHEN mode='wrong-profile-context' THEN 3
                WHEN is_child THEN 2 ELSE 1 END AS profile_slot,
            CASE WHEN mode='wrong-entity' THEN 88 WHEN mode NOT IN ('null-entities','null-entity') THEN 77 END AS entity_binding_id,
            CASE WHEN mode='wrong-collection' THEN 2 WHEN is_child OR mode='root-with-collection' THEN 1 ELSE 0 END AS context_collection_slot,
            CASE WHEN mode='wrong-child' THEN 999 WHEN (is_child AND mode<>'null-child') OR mode='root-with-child'
                THEN 500+case_id END AS context_child_revision_id
        FROM cases
    ), custom_import_build_candidate_context AS MATERIALIZED (
        SELECT family_revision_id AS candidate_context_id,family_revision_id,profile_slot,entity_binding_id,
            context_collection_slot,context_child_revision_id,
            CASE WHEN mode='missing-build' THEN 99 WHEN mode='wrong-scope' THEN 8 ELSE 7 END AS build_id
        FROM custom_import_winner
    ), custom_import_family_child AS MATERIALIZED (
        SELECT case_id AS family_revision_id,CASE WHEN mode='edge-scope' THEN 99 ELSE 2 END AS dataset_id,
            4 AS schema_revision_id,1000+case_id AS root_record_id,1 AS collection_slot,500+case_id AS child_revision_id
        FROM cases WHERE is_child AND mode<>'missing-edge'
    )
"""


def _install(connection, schema):
    for name in _CHAIN:
        migration = _migration(_ROOT / "alembic/versions" / f"{name}.py", f"source_homes_{name}")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


def test_output_query_keeps_exact_body_inside_materialized_limit():
    migration = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[-1]}.py", "output_materialized_contract")
    query = migration._output_query()
    assert query.count("WITH violations AS MATERIALIZED (") == 1
    assert query.endswith(") SELECT * FROM violations LIMIT 1;\n")
    unwrapped = query.replace("WITH violations AS MATERIALIZED (", "SELECT violation.* FROM (", 1)
    unwrapped = unwrapped.replace(") SELECT * FROM violations LIMIT 1;", ") violation LIMIT 1;", 1)
    # Pin the reviewed predicate-only rewrite; the planning boundary changes no check.
    assert (
        hashlib.sha256(unwrapped.encode()).hexdigest()
        == "363734372d9812a5f6f1aa535b4db27e1faf21b9f02ba345ed9dcc9d0b0f1ec7"
    )


@asynccontextmanager
async def _case():
    async with isolated_publication_case(migration_through="20261005030000") as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        yield case


async def test_native_source_fresh_home_replay_and_missing_home_rejection():
    async with _case() as case:
        context, page, family_id = await _bootstrap(case)
        assert await source._store_single_page(case.sessions, context, page) == 1
        await source._compare_committed_page(case.sessions, context, page)
        async with source._page_session(case.sessions, context.request, context.build_id) as (session, _):
            homes = await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_revision_home'))
            assert homes == 1
            transaction = await session.begin_nested()
            await session.execute(
                text(f'DELETE FROM "{case.schema_name}".custom_import_revision_home WHERE family_id=:family'),
                dict(family=family_id),
            )
            with pytest.raises(DBAPIError, match="source_replay_home_mismatch"):
                await source._call(
                    session,
                    "check_custom_import_source_replay_homes",
                    (
                        ("bigint", context.build_id),
                        ("smallint", context.stream_slot),
                        ("integer", page.part_ordinal),
                        ("bigint", page.first_row),
                        ("integer", len(page.records)),
                    ),
                )
            await transaction.rollback()
        await source._compare_committed_page(case.sessions, context, page)
        outcome = await source.stage_segmented_source(case.sessions, context.request)
        assert outcome.phase == "graph"
        async with case.sessions() as session:
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_revision_home'))
                == homes
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_indexes WHERE schemaname=:schema AND indexname LIKE 'custom_import_build_%_idx'"
                    ),
                    dict(schema=snapshot_schema(family_id)),
                )
                >= 5
            )


async def test_native_all_five_finality_queries_and_serving_index_lifecycle():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            seed = await _seed_publication_identity(session, uuid4().hex)
            graph, attempt = await _live_generation(session, seed, uuid4().hex)
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            family_id = await session.scalar(
                text(f'SELECT "{case.schema_name}".resolve_custom_import_legacy_generation_snapshot(:generation)'),
                dict(generation=attempt.generation_id),
            )
            await session.execute(
                text(
                    f"SELECT \"{case.schema_name}\".freeze_custom_import_snapshot_family(:execution,:fence,sha256(convert_to(:token,'UTF8')))"
                ),
                dict(execution=attempt.execution_id, fence=attempt.fence, token=attempt.token),
            )
        namespace = snapshot_schema(family_id)
        finality = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[2]}.py", "source_home_finality_plan")
        serving_indexes = await _prepare_serving_indexes(case, family_id, finality)
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            assert (
                await session.scalar(
                    text(f"SELECT \"{case.schema_name}\".verify_custom_import_snapshot_indexes(:family,'serving')"),
                    dict(family=family_id),
                )
                is True
            )
            with pytest.raises(DBAPIError, match="custom_import_snapshot_index_mismatch"):
                async with session.begin_nested():
                    name = serving_indexes[0]["name"]
                    await session.execute(text(f'DROP INDEX "{namespace}"."{name}"'))
                    await session.execute(
                        text(f'CREATE INDEX "{name}" ON "{namespace}".custom_import_root_scalar(root_revision_id)')
                    )
                    await session.scalar(
                        text(f"SELECT \"{case.schema_name}\".verify_custom_import_snapshot_indexes(:family,'serving')"),
                        dict(family=family_id),
                    )
            await _explain_finality_queries(session, case, finality, namespace, graph, attempt)
            await _explain_rejection_reference_probe(session)
            await _assert_lineage_branch_parity(session)
            await _assert_output_branch_parity(session)
            counts = await session.scalar(
                text(f'SELECT "{case.schema_name}".verify_custom_import_snapshot_structure(:generation)'),
                dict(generation=attempt.generation_id),
            )
            assert counts["root_count"] == counts["winner_count"] == 0


async def _prepare_serving_indexes(case, family_id, finality):
    specifications = json.loads(finality._INDEX_SPECIFICATIONS)
    serving_indexes = [entry for entry in specifications if entry["phase"] == "serving"]
    counts = []
    for _ in serving_indexes:
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            is_complete = await session.scalar(
                text(f"SELECT \"{case.schema_name}\".prepare_custom_import_snapshot_indexes(:family,'serving')"),
                dict(family=family_id),
            )
            counts.append(
                await session.scalar(
                    text("SELECT count(*) FROM pg_indexes WHERE schemaname=:schema AND indexname LIKE '%scalar%_idx'"),
                    dict(schema=snapshot_schema(family_id)),
                )
            )
    assert counts == list(range(1, len(serving_indexes) + 1)) and is_complete is True
    return serving_indexes


async def _explain_rejection_reference_probe(session):
    """Plan the source-bound probe over finite synthetic rows without loading them."""
    query = """
        WITH expected_build AS (SELECT 7::bigint AS build_id),
        custom_import_rejection AS (
            SELECT value AS rejection_id FROM generate_series(1,1024) value
        ), custom_import_build_occurrence AS MATERIALIZED (
            SELECT 7::bigint AS build_id,value % 512 AS rejection_id,
                (value + 256) % 512 AS resolved_rejection_id
            FROM generate_series(1,16384) value
        )
    """ + _rejection_reference_query().replace("__CANDIDATE__.", "")
    result = await session.scalar(text("EXPLAIN (FORMAT JSON) " + query))
    plan = json.loads(result) if isinstance(result, str) else result
    pending, anti_joins = [plan[0]["Plan"]], []
    while pending:
        node = pending.pop()
        pending.extend(node.get("Plans", ()))
        if node.get("Join Type") in {"Anti", "Right Anti"}:
            anti_joins.append(node)
    assert len(anti_joins) == 2
    assert all(node["Node Type"] in {"Hash Join", "Merge Join"} for node in anti_joins)


def _branch_query(query, failure_code, inputs):
    marker = f"SELECT '{failure_code}'"
    branch = marker + re.split(r"\n\s+UNION ALL\n", query.split(marker, 1)[1], maxsplit=1)[0]
    branch = branch.replace("__CANDIDATE__.", "").replace("__CONTROL__.", "")
    return inputs + re.sub(r"\$(\d+)", _typed_plan_binding, branch)


async def _assert_branch_results(session, previous, current, expected_failures):
    arguments_by_name = dict(p1=1, p2=2, p3=3, p4=4, p5=5, p6=6, p7=7, p8=bytes(32))
    previous_rows = (await session.execute(text(previous), arguments_by_name)).all()
    current_rows = (await session.execute(text(current), arguments_by_name)).all()
    assert set(previous_rows) == set(current_rows) == expected_failures
    assert len(previous_rows) == len(current_rows) == len(expected_failures)
    result = await session.scalar(text("EXPLAIN (ANALYZE, FORMAT JSON) " + current), arguments_by_name)
    plan = json.loads(result) if isinstance(result, str) else result
    return _plan_nodes(plan[0]["Plan"])


def _plan_nodes(plan):
    pending_nodes, plan_nodes = [plan], []
    while pending_nodes:
        node = pending_nodes.pop()
        pending_nodes.extend(node.get("Plans", ()))
        plan_nodes.append(node)
    return plan_nodes


async def _assert_lineage_branch_parity(session):
    """Compare complete failure sets, including double owner failures and null links."""
    migration = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[-1]}.py", "source_lineage_parity")
    for failure_code, inputs, missing in (
        ("occurrence_owner", _OWNER_INPUTS, {3, 4, 5, 6, 7, 8, 10, 11, 12, 13}),
        ("occurrence_resolved_rejection", _RESOLVED_INPUTS, {*range(5, 17), 18, 19}),
    ):
        previous = _branch_query(migration._finality()._resource("source_lineage"), failure_code, inputs)
        current = _branch_query(migration._source_query(), failure_code, inputs)
        expected_failures = {(failure_code, identity) for identity in missing}
        for node in await _assert_branch_results(session, previous, current, expected_failures):
            assert node.get("Parent Relationship") != "SubPlan" or node["Actual Loops"] <= 1


async def _assert_output_branch_parity(session):
    """Keep scalar grants, context modes and every original winner scope check."""
    migration = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[-1]}.py", "output_relationship_parity")
    for failure_code, inputs, missing in (
        ("root_scalar_reference", _SCALAR_INPUTS, {*range(2, 10), *range(11, 16), 17}),
        ("child_scalar_reference", _SCALAR_INPUTS, {*range(2, 16), 17}),
        ("winner_reference", _SELECTION_INPUTS, {3, 4, 5, 6, *range(8, 16), 17, 18}),
        ("context_reference", _SELECTION_INPUTS, {*range(3, 16), 17}),
    ):
        previous = _branch_query(migration._finality()._resource("output_relationships"), failure_code, inputs)
        current = _branch_query(migration._output_query(), failure_code, inputs)
        expected_failures = {(failure_code, identity) for identity in missing}
        plan_nodes = await _assert_branch_results(session, previous, current, expected_failures)
        assert any(node.get("Join Type") in {"Anti", "Right Anti"} for node in plan_nodes)
        if failure_code.endswith("scalar_reference"):
            assert all(node.get("Parent Relationship") != "SubPlan" or node["Actual Loops"] <= 1 for node in plan_nodes)


async def _explain_finality_queries(session, case, finality, namespace, graph, attempt):
    bulk = finality._bulk()
    migration = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[-1]}.py", "current_finality_plan")
    plans_by_name = {}
    arguments_by_name = dict(
        p1=attempt.generation_id,
        p2=graph.dataset_id,
        p3=graph.definition_revision_id,
        p4=graph.schema_revision_id,
        p5=attempt.execution_id,
        p6=graph.capture_bundle_id,
        p7=attempt.fence,
        p8=lease_digest(attempt.token),
        p9=None,
    )
    for name in finality._QUERY_NAMES:
        query = finality._resource(name)
        if name == "source_lineage":
            query = migration._source_query()
        elif name == "output_relationships":
            query = migration._output_query()
        sql = bulk._control_sql(bulk._storage(), case.schema_name, query)
        sql = (
            sql.replace("__CANDIDATE__", f'"{namespace}"')
            .replace("__BASE__", f'"{case.schema_name}"')
            .replace("__ORIGIN_QUERY__", "")
        )
        sql = re.sub(r"\$(\d+)", _typed_plan_binding, sql)
        plan = await session.scalar(text("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) " + sql), arguments_by_name)
        plans_by_name[name] = json.loads(plan) if isinstance(plan, str) else plan
        assert plans_by_name[name][0]["Plan"]["Node Type"] == "Limit"
        if name == "output_relationships":
            assert any(node.get("CTE Name") == "violations" for node in _plan_nodes(plans_by_name[name][0]["Plan"]))
    assert len(plans_by_name) == 5
    if directory := os.getenv("CUSTOM_IMPORT_TEST_PLAN_DIRECTORY"):
        (Path(directory) / "finality-plans.json").write_text(json.dumps(plans_by_name, indent=2, sort_keys=True) + "\n")


def _typed_plan_binding(match):
    native_type = "bytea" if match[1] == "8" else "bigint"
    return f"CAST(:p{match[1]} AS {native_type})"
