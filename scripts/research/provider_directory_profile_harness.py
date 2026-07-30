#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute generated Provider Directory profile SQL in a disposable schema."""

from __future__ import annotations

import json
import sys
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
from scripts.research.provider_directory_profile_harness_support import (
    arguments as harness_arguments,
    bind as _bind,
    connect as harness_connect,
    decoded as _decoded,
    run as run_harness,
    schema_name as harness_schema_name,
    table_ref as _ref,
)


FIXTURE_NPI = 1588616783
PROFILE_AS_OF = "2026-07-13"
ALL_SOURCE_IDS = ["source_a", "source_b"]
INITIAL_EVIDENCE_TABLE = "profile_evidence"
INITIAL_PROFILE_TABLE = "profile"
INCREMENTAL_EVIDENCE_TABLE = "profile_evidence_incremental"
INCREMENTAL_PROFILE_TABLE = "profile_incremental"


def _fixture_sql(schema: str) -> str:
    return FIXTURE_SQL_PATH.read_text(encoding="utf-8").replace(
        "{{SCHEMA}}",
        schema,
    )


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
        endpoint_ref=_ref(schema, "endpoint"),
        affiliation_ref=_ref(schema, "affiliation"),
        affiliation_organization_ref=_ref(
            schema,
            "affiliation_organization",
        ),
    )
    await connection.execute(
        _bind(
            evidence_insert_sql,
            "source_ids",
            "dataset_ids",
            "profile_as_of",
        ),
        source_ids,
        dataset_ids,
        PROFILE_AS_OF,
    )
    for index_statement in profile.profile_index_statements(
        schema,
        table_name,
        evidence=True,
    ):
        await connection.execute(index_statement)


async def _copy_retained_profiles(
    connection: asyncpg.Connection,
    schema: str,
    artifact_refs: tuple[str, str],
    prior_tables: tuple[str, str],
    refreshed_source_ids: list[str],
) -> None:
    """Copy profiles outside the refreshed source set into the new stage."""
    evidence_ref, profile_ref = artifact_refs
    old_profile_table, old_evidence_table = prior_tables
    await connection.execute(
        _bind(
            profile.copy_unaffected_profiles_sql(
                profile_source_ref=_ref(schema, old_profile_table),
                evidence_source_ref=_ref(schema, old_evidence_table),
                evidence_stage_ref=evidence_ref,
                profile_stage_ref=profile_ref,
            ),
            "source_ids",
            "retained_source_ids",
            "profile_as_of",
        ),
        refreshed_source_ids,
        ALL_SOURCE_IDS,
        PROFILE_AS_OF,
    )


async def _populate_compact_profiles(
    connection: asyncpg.Connection,
    schema: str,
    artifact_refs: tuple[str, str],
    generation_id: str,
    prior_tables: tuple[str, str] | None,
    refreshed_source_ids: list[str] | None,
) -> None:
    """Aggregate exact evidence into the compact profile stage."""
    evidence_ref, profile_ref = artifact_refs
    old_evidence_ref = (
        _ref(schema, prior_tables[1]) if prior_tables is not None else None
    )
    aggregate_sql = profile.profile_insert_sql(
        evidence_ref=evidence_ref,
        target_ref=profile_ref,
        old_evidence_ref=old_evidence_ref,
        rebuild_all=prior_tables is None,
    )
    if prior_tables is None:
        await connection.execute(
            _bind(aggregate_sql, "generation_id"),
            generation_id,
        )
        return
    await connection.execute(
        _bind(
            aggregate_sql,
            "source_ids",
            "retained_source_ids",
            "profile_as_of",
            "generation_id",
        ),
        refreshed_source_ids,
        ALL_SOURCE_IDS,
        PROFILE_AS_OF,
        generation_id,
    )


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
    """Create and index one initial or incremental compact artifact."""
    profile_ref = _ref(schema, table_name)
    evidence_ref = _ref(schema, evidence_table)
    prior_tables = (
        (old_profile_table, old_evidence_table)
        if old_profile_table and old_evidence_table and refreshed_source_ids
        else None
    )
    artifact_refs = (evidence_ref, profile_ref)
    await connection.execute(profile.profile_table_sql(schema, table_name))
    if prior_tables is not None and refreshed_source_ids is not None:
        await _copy_retained_profiles(
            connection,
            schema,
            artifact_refs,
            prior_tables,
            refreshed_source_ids,
        )
    await _populate_compact_profiles(
        connection,
        schema,
        artifact_refs,
        generation_id,
        prior_tables,
        refreshed_source_ids,
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
            "retained_source_ids",
            "profile_as_of",
        ),
        ["source_b"],
        ALL_SOURCE_IDS,
        PROFILE_AS_OF,
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
        endpoint_ref=_ref(schema, "endpoint"),
        affiliation_ref=_ref(schema, "affiliation"),
        affiliation_organization_ref=_ref(
            schema,
            "affiliation_organization",
        ),
    )
    await connection.execute(
        _bind(
            evidence_insert_sql,
            "source_ids",
            "dataset_ids",
            "profile_as_of",
        ),
        ["source_b"],
        ["dataset_b"],
        PROFILE_AS_OF,
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
    connection_options: Any,
) -> dict[str, Any]:
    schema = harness_schema_name()
    connection = await harness_connect(connection_options)
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
            "schema_retained": connection_options.keep_schema,
        }
    finally:
        if not connection_options.keep_schema:
            await connection.execute(
                f"DROP SCHEMA IF EXISTS {profile.quote_identifier(schema)} CASCADE;"
            )
        await connection.close()


def main() -> None:
    """Run the disposable PostgreSQL profile self-harness."""
    harness_metrics = run_harness(
        _execute_profile_harness(harness_arguments(__doc__ or ""))
    )
    print(json.dumps(harness_metrics, sort_keys=True))


if __name__ == "__main__":
    main()
