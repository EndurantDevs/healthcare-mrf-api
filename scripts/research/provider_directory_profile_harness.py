#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute generated Provider Directory profile SQL in a disposable schema."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import asyncpg

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SQL_PATH = (
    ROOT
    / "scripts"
    / "research"
    / "sql"
    / "provider_directory_profile_fixture.sql"
)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from process import provider_directory_profile as profile


FIXTURE_NPI = 1588616783
INITIAL_EVIDENCE_TABLE = "profile_evidence"
INITIAL_PROFILE_TABLE = "profile"
INCREMENTAL_EVIDENCE_TABLE = "profile_evidence_incremental"
INCREMENTAL_PROFILE_TABLE = "profile_incremental"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.getenv("HLTHPRT_DB_DSN"))
    parser.add_argument(
        "--host",
        default=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
    )
    parser.add_argument("--user", default=os.getenv("HLTHPRT_DB_USER"))
    parser.add_argument(
        "--database",
        default=os.getenv("HLTHPRT_DB_DATABASE"),
    )
    parser.add_argument(
        "--password",
        default=os.getenv("HLTHPRT_DB_PASSWORD") or os.getenv("PGPASSWORD"),
    )
    parser.add_argument("--keep-schema", action="store_true")
    return parser.parse_args()


def _schema_name() -> str:
    token = f"{os.getpid()}:{time.time_ns()}"
    digest = hashlib.sha1(token.encode("ascii")).hexdigest()[:12]
    return f"pd_profile_harness_{digest}"


def _bind(sql: str, *parameter_names: str) -> str:
    bound_sql = sql
    for index, parameter_name in enumerate(parameter_names, start=1):
        bound_sql = re.sub(
            rf":{re.escape(parameter_name)}\b",
            "$" + str(index),
            bound_sql,
        )
    unresolved_tokens = re.findall(
        r"(?<!:):[a-zA-Z_][a-zA-Z0-9_]*",
        bound_sql,
    )
    if unresolved_tokens:
        raise RuntimeError(
            "provider_directory_profile_harness_unbound_parameters:"
            + ",".join(sorted(set(unresolved_tokens)))
        )
    return bound_sql


def _decoded(json_value: Any) -> Any:
    return json.loads(json_value) if isinstance(json_value, str) else json_value


async def _connect(
    arguments: argparse.Namespace,
) -> asyncpg.Connection:
    if arguments.dsn:
        return await asyncpg.connect(arguments.dsn)
    missing_settings = [
        setting_name
        for setting_name in ("user", "database", "password")
        if not getattr(arguments, setting_name)
    ]
    if missing_settings:
        raise RuntimeError(
            "provider_directory_profile_harness_db_config_missing:"
            + ",".join(missing_settings)
        )
    return await asyncpg.connect(
        host=arguments.host,
        port=arguments.port,
        user=arguments.user,
        password=arguments.password,
        database=arguments.database,
    )


def _fixture_sql(schema: str) -> str:
    return FIXTURE_SQL_PATH.read_text(encoding="utf-8").replace(
        "{{SCHEMA}}",
        schema,
    )


def _ref(schema: str, table_name: str) -> str:
    return profile.qualified_table(schema, table_name)


async def _initialize_fixture(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await connection.execute(
        f"CREATE SCHEMA {profile.quote_identifier(schema)};"
    )
    await connection.execute(_fixture_sql(schema))


async def _create_evidence_artifact(
    connection: asyncpg.Connection,
    schema: str,
    table_name: str,
    source_ids: list[str],
    dataset_ids: list[str],
) -> None:
    evidence_ref = _ref(schema, table_name)
    await connection.execute(
        profile.profile_evidence_table_sql(schema, table_name)
    )
    evidence_insert_sql = profile.profile_evidence_insert_sql(
        target_ref=evidence_ref,
        source_ref=_ref(schema, "source"),
        practitioner_ref=_ref(schema, "practitioner"),
        role_ref=_ref(schema, "role"),
        organization_ref=_ref(schema, "organization"),
        service_ref=_ref(schema, "service"),
    )
    await connection.execute(
        _bind(evidence_insert_sql, "source_ids", "dataset_ids"),
        source_ids,
        dataset_ids,
    )
    for index_statement in profile.profile_index_statements(
        schema,
        table_name,
        evidence=True,
    ):
        await connection.execute(index_statement)


async def _create_compact_artifact(
    connection: asyncpg.Connection,
    schema: str,
    table_name: str,
    evidence_table: str,
    generation_id: str,
    *,
    old_profile_table: str | None = None,
    old_evidence_table: str | None = None,
    refreshed_source_ids: list[str] | None = None,
) -> None:
    profile_ref = _ref(schema, table_name)
    evidence_ref = _ref(schema, evidence_table)
    has_existing_artifacts = bool(
        old_profile_table
        and old_evidence_table
        and refreshed_source_ids
    )
    await connection.execute(profile.profile_table_sql(schema, table_name))
    if has_existing_artifacts:
        await connection.execute(
            _bind(
                profile.copy_unaffected_profiles_sql(
                    profile_source_ref=_ref(schema, old_profile_table or ""),
                    evidence_source_ref=_ref(schema, old_evidence_table or ""),
                    evidence_stage_ref=evidence_ref,
                    profile_stage_ref=profile_ref,
                ),
                "source_ids",
            ),
            refreshed_source_ids,
        )
    aggregate_sql = profile.profile_insert_sql(
        evidence_ref=evidence_ref,
        target_ref=profile_ref,
        old_evidence_ref=(
            _ref(schema, old_evidence_table or "")
            if has_existing_artifacts
            else None
        ),
        rebuild_all=not has_existing_artifacts,
    )
    if has_existing_artifacts:
        await connection.execute(
            _bind(aggregate_sql, "source_ids", "generation_id"),
            refreshed_source_ids,
            generation_id,
        )
    else:
        await connection.execute(
            _bind(aggregate_sql, "generation_id"),
            generation_id,
        )
    for index_statement in profile.profile_index_statements(
        schema,
        table_name,
        evidence=False,
    ):
        await connection.execute(index_statement)


async def _build_initial_artifacts(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await _create_evidence_artifact(
        connection,
        schema,
        INITIAL_EVIDENCE_TABLE,
        ["source_a", "source_b"],
        ["dataset_a", "dataset_b"],
    )
    await _create_compact_artifact(
        connection,
        schema,
        INITIAL_PROFILE_TABLE,
        INITIAL_EVIDENCE_TABLE,
        "profile_harness_generation",
    )


async def _build_incremental_artifacts(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    await connection.execute(
        f"""
        UPDATE {_ref(schema, "practitioner")}
           SET telecom = '[{{"system":"phone","value":"3125550199","use":"work"}}]'::jsonb,
               updated_at = '2026-07-13 12:10:00'
         WHERE source_id = 'source_b';
        """
    )
    await connection.execute(
        profile.profile_evidence_table_sql(
            schema,
            INCREMENTAL_EVIDENCE_TABLE,
        )
    )
    await connection.execute(
        _bind(
            profile.copy_existing_evidence_sql(
                source_ref=_ref(schema, INITIAL_EVIDENCE_TABLE),
                target_ref=_ref(schema, INCREMENTAL_EVIDENCE_TABLE),
            ),
            "source_ids",
        ),
        ["source_b"],
    )
    await _create_evidence_rows_for_refresh(connection, schema)
    await _create_compact_artifact(
        connection,
        schema,
        INCREMENTAL_PROFILE_TABLE,
        INCREMENTAL_EVIDENCE_TABLE,
        "profile_harness_incremental_generation",
        old_profile_table=INITIAL_PROFILE_TABLE,
        old_evidence_table=INITIAL_EVIDENCE_TABLE,
        refreshed_source_ids=["source_b"],
    )


async def _create_evidence_rows_for_refresh(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    evidence_insert_sql = profile.profile_evidence_insert_sql(
        target_ref=_ref(schema, INCREMENTAL_EVIDENCE_TABLE),
        source_ref=_ref(schema, "source"),
        practitioner_ref=_ref(schema, "practitioner"),
        role_ref=_ref(schema, "role"),
        organization_ref=_ref(schema, "organization"),
        service_ref=_ref(schema, "service"),
    )
    await connection.execute(
        _bind(evidence_insert_sql, "source_ids", "dataset_ids"),
        ["source_b"],
        ["dataset_b"],
    )
    for index_statement in profile.profile_index_statements(
        schema,
        INCREMENTAL_EVIDENCE_TABLE,
        evidence=True,
    ):
        await connection.execute(index_statement)


async def _set_and_assert_logged(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    table_names = [
        INCREMENTAL_EVIDENCE_TABLE,
        INCREMENTAL_PROFILE_TABLE,
    ]
    for table_name in table_names:
        await connection.execute(
            f"ALTER TABLE {_ref(schema, table_name)} SET LOGGED;"
        )
    persistence_rows = await connection.fetch(
        """
        SELECT relname, relpersistence
          FROM pg_class AS relation
          JOIN pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = $1
           AND relation.relname = ANY($2::text[])
         ORDER BY relname;
        """,
        schema,
        table_names,
    )
    persistence_codes = {
        raw_code.decode("ascii")
        if isinstance(raw_code, bytes)
        else str(raw_code)
        for persistence_row in persistence_rows
        for raw_code in (persistence_row["relpersistence"],)
    }
    if persistence_codes != {"p"}:
        raise RuntimeError(
            "provider_directory_profile_harness_artifact_not_logged"
        )


async def _profile_db_row(
    connection: asyncpg.Connection,
    schema: str,
) -> asyncpg.Record:
    profile_db_row = await connection.fetchrow(
        f"""
        SELECT profile_json, evidence_json, source_count,
               independent_source_count, fact_count
          FROM {_ref(schema, INCREMENTAL_PROFILE_TABLE)}
         WHERE npi = $1;
        """,
        FIXTURE_NPI,
    )
    if profile_db_row is None:
        raise RuntimeError("provider_directory_profile_harness_profile_missing")
    return profile_db_row


def _validated_facts_by_type(
    profile_db_row: asyncpg.Record,
) -> dict[str, Any]:
    compact_profile = _decoded(profile_db_row["profile_json"])
    evidence_profile = _decoded(profile_db_row["evidence_json"])
    if "1970-" in json.dumps(compact_profile, sort_keys=True):
        raise RuntimeError(
            "provider_directory_profile_harness_birth_date_leaked"
        )
    facts_by_type = compact_profile["facts"]
    required_fact_types = {
        "age",
        "years_of_practice",
        "credential",
        "taxonomy_qualification",
        "language",
        "contact",
        "specialty",
        "organization",
        "service",
    }
    missing_fact_types = sorted(required_fact_types - set(facts_by_type))
    if missing_fact_types:
        raise RuntimeError(
            "provider_directory_profile_harness_facts_missing:"
            + ",".join(missing_fact_types)
        )
    practice_fact = facts_by_type["years_of_practice"]["items"][0]["value"]
    if practice_fact.get("years") != 25 or not practice_fact.get("estimated"):
        raise RuntimeError(
            "provider_directory_profile_harness_practice_years_invalid"
        )
    age_evidence_source_ids = {
        evidence_entry["source_id"]
        for evidence_entry in evidence_profile["facts"]["age"]["items"][0][
            "evidence"
        ]
    }
    if age_evidence_source_ids != {"source_a", "source_b"}:
        raise RuntimeError(
            "provider_directory_profile_harness_evidence_sources_invalid"
        )
    contact_numbers = {
        contact_entry["value"].get("value")
        for contact_entry in facts_by_type["contact"]["items"]
    }
    if "3125550199" not in contact_numbers:
        raise RuntimeError(
            "provider_directory_profile_harness_incremental_refresh_missing"
        )
    return facts_by_type


async def _lookup_execution_ms(
    connection: asyncpg.Connection,
    schema: str,
) -> float:
    explain_json = await connection.fetchval(
        f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT profile_json
          FROM {_ref(schema, INCREMENTAL_PROFILE_TABLE)}
         WHERE npi = {FIXTURE_NPI};
        """
    )
    execution_ms = float(_decoded(explain_json)[0]["Execution Time"])
    if execution_ms >= 40.0:
        raise RuntimeError(
            "provider_directory_profile_harness_lookup_slow:"
            f"{execution_ms:.3f}ms"
        )
    return execution_ms


async def _execute_profile_harness(
    arguments: argparse.Namespace,
) -> dict[str, Any]:
    schema = _schema_name()
    connection = await _connect(arguments)
    try:
        await _initialize_fixture(connection, schema)
        await _build_initial_artifacts(connection, schema)
        await _build_incremental_artifacts(connection, schema)
        await _set_and_assert_logged(connection, schema)
        profile_db_row = await _profile_db_row(connection, schema)
        facts_by_type = _validated_facts_by_type(profile_db_row)
        execution_ms = await _lookup_execution_ms(connection, schema)
        return {
            "ok": True,
            "schema": schema,
            "npi": FIXTURE_NPI,
            "profile_rows": 1,
            "evidence_rows": await connection.fetchval(
                f"SELECT count(*) FROM {_ref(schema, INCREMENTAL_EVIDENCE_TABLE)};"
            ),
            "source_count": profile_db_row["source_count"],
            "independent_source_count": profile_db_row[
                "independent_source_count"
            ],
            "fact_count": profile_db_row["fact_count"],
            "fact_types": sorted(facts_by_type),
            "lookup_execution_ms": execution_ms,
            "incremental_refresh_verified": True,
            "logged_artifacts_verified": True,
            "schema_retained": arguments.keep_schema,
        }
    finally:
        if not arguments.keep_schema:
            await connection.execute(
                f"DROP SCHEMA IF EXISTS {profile.quote_identifier(schema)} CASCADE;"
            )
        await connection.close()


def main() -> None:
    """Run the disposable PostgreSQL profile self-harness."""
    harness_metrics = asyncio.run(_execute_profile_harness(_arguments()))
    print(json.dumps(harness_metrics, sort_keys=True))


if __name__ == "__main__":
    main()
