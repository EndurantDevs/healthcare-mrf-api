# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Threshold constants keep selective candidate plans usable after preparation."""

import json

from api.address_source_filter import address_source_filter_sql


def _plan_nodes(plan):
    return [plan] + [node for child in plan.get("Plans", ()) for node in _plan_nodes(child)]


async def _prepared_addresses(connection, schema):
    relation = f'"{schema}".entity_address_unified'
    await connection.execute(f"""CREATE TABLE {relation} (
        npi bigint, inferred_npi bigint, type text, source_count integer NOT NULL DEFAULT 0
    )""")
    await connection.execute(f"""INSERT INTO {relation}
        SELECT sequence_id, NULL, 'primary', CASE WHEN sequence_id % 10 = 0 THEN 3 ELSE 1 END
        FROM generate_series(1, 100000) sequence_id
    """)
    await connection.execute(f"CREATE INDEX candidate_npi ON {relation} (COALESCE(npi, inferred_npi))")
    await connection.execute(f"CREATE INDEX candidate_source_count ON {relation} (source_count)")
    await connection.execute(f"ANALYZE {relation}")
    return relation


async def _explain_prepared(connection, name):
    document = await connection.fetchval(f"EXPLAIN (FORMAT JSON) EXECUTE {name}(5)")
    return (json.loads(document) if isinstance(document, str) else document)[0]["Plan"]


async def _check_prepared_threshold(connection, relation, minimum, expected):
    name = f"source_candidates_{minimum}"
    predicate = address_source_filter_sql("candidate", minimum)
    sql = f"""SELECT DISTINCT COALESCE(candidate.npi, candidate.inferred_npi) AS npi
        FROM {relation} candidate
        WHERE candidate.type IN ('primary','secondary','practice','site') AND {predicate}
        ORDER BY npi LIMIT $1"""
    await connection.execute(f"PREPARE {name}(integer) AS {sql}")
    try:
        for mode in ("force_custom_plan", "force_generic_plan", "auto"):
            await connection.execute(f"SET LOCAL plan_cache_mode = {mode}")
            if mode == "auto":
                for _ in range(8):
                    await _explain_prepared(connection, name)
            plan = await _explain_prepared(connection, name)
            if minimum == 2147483647:
                assert any(node.get("Index Name") == "candidate_source_count" for node in _plan_nodes(plan))
                assert not any(node["Node Type"] == "Seq Scan" for node in _plan_nodes(plan))
            rows = await connection.fetch(f"EXECUTE {name}(5)")
            assert [row["npi"] for row in rows] == expected
    finally:
        await connection.execute(f"DEALLOCATE {name}")


async def check_threshold_specific_prepared_candidates(migrated_v3_database):
    """Exercise candidate ORDER/LIMIT planning, separately from full API latency."""
    async with migrated_v3_database.engine.begin() as owner:
        raw = await owner.get_raw_connection()
        connection = raw.driver_connection
        relation = await _prepared_addresses(connection, migrated_v3_database.schema)
        for minimum, expected in ((0, [1, 2, 3, 4, 5]), (3, [10, 20, 30, 40, 50]), (2147483647, [])):
            await _check_prepared_threshold(connection, relation, minimum, expected)
