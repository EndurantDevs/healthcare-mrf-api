# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL schema fixtures for profile-delta tests."""

from __future__ import annotations

import datetime
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import (
    _control_wal_plan_input,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def _create_delta_profile_tables(
    database: Database,
    schema: str,
    evidence_stage: str,
    profile_stage: str,
    affected_stage: str,
) -> None:
    """Create target and scratch tables plus their exact indexes."""
    table_statements = (
        profile.profile_evidence_table_sql(
            schema, profile.PROFILE_EVIDENCE_TABLE, logged=True
        ),
        profile.profile_table_sql(
            schema, profile.PROFILE_TABLE, logged=True
        ),
        profile.profile_evidence_table_sql(
            schema, evidence_stage, logged=True
        ),
        profile.profile_table_sql(schema, profile_stage, logged=True),
        f"CREATE TABLE {profile.qualified_table(schema, affected_stage)} "
        "(npi bigint PRIMARY KEY);",
    )
    for statement in table_statements:
        await database.status(statement)
    indexed_tables = (
        (profile.PROFILE_EVIDENCE_TABLE, True),
        (evidence_stage, True),
        (profile.PROFILE_TABLE, False),
        (profile_stage, False),
    )
    for table_name, is_evidence in indexed_tables:
        for statement in profile.profile_index_statements(
            schema, table_name, evidence=is_evidence
        ):
            await database.status(statement)


def _immutable_guard_statements(
    table_ref: str,
    function_ref: str,
    *,
    error_message: str,
    trigger_prefix: str,
) -> tuple[str, ...]:
    """Return write, delete, and truncate refusal DDL for one table."""
    write_trigger = f"{trigger_prefix}_write_guard"
    truncate_trigger = f"{trigger_prefix}_truncate_guard"
    return (
        f"""
        CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
        AS $$ BEGIN RAISE EXCEPTION '{error_message}'; END; $$;
        """,
        f"""
        CREATE TRIGGER {write_trigger}
        BEFORE UPDATE OR DELETE ON {table_ref}
        FOR EACH STATEMENT EXECUTE FUNCTION {function_ref}();
        """,
        f"""
        CREATE TRIGGER {truncate_trigger}
        BEFORE TRUNCATE ON {table_ref}
        FOR EACH STATEMENT EXECUTE FUNCTION {function_ref}();
        """,
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER {write_trigger};",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER {truncate_trigger};",
    )


async def _create_delta_control_stubs(
    database: Database,
    schema: str,
) -> None:
    """Create import-run and immutable capacity-consumption stubs."""
    import_run_ref = profile.qualified_table(schema, "import_run")
    consumption_ref = profile.qualified_table(
        schema, "provider_directory_profile_capacity_lease_consumption"
    )
    guard_ref = profile.qualified_table(
        schema, "provider_directory_profile_capacity_guard_test"
    )
    await database.status(
        f"CREATE TABLE {import_run_ref} ("
        "run_id varchar(64) PRIMARY KEY, progress jsonb, metrics jsonb);"
    )
    await database.status(
        f"CREATE TABLE {consumption_ref} ("
        "attestation_id varchar(64) PRIMARY KEY);"
    )
    for statement in _immutable_guard_statements(
        consumption_ref,
        guard_ref,
        error_message=(
            "provider_directory_profile_capacity_consumption_immutable"
        ),
        trigger_prefix="provider_directory_profile_capacity",
    ):
        await database.status(statement)


def _checkpoint_table_sql(schema: str) -> str:
    """Return the delta checkpoint truth-table DDL."""
    table_ref = profile.qualified_table(
        schema, "provider_directory_profile_build_checkpoint"
    )
    return f"""
    CREATE TABLE {table_ref} (
        build_id varchar(64) PRIMARY KEY,
        resume_lineage_hash varchar(64) NOT NULL,
        executable_plan_hash varchar(64),
        owner_run_id varchar(64),
        state varchar(32) NOT NULL,
        materialization_mode varchar(16) NOT NULL,
        refresh_source_ids jsonb,
        removed_source_ids jsonb,
        evidence_stage varchar(63) NOT NULL,
        profile_stage varchar(63) NOT NULL,
        affected_npi_stage varchar(63),
        evidence_stage_oid bigint NOT NULL,
        profile_stage_oid bigint NOT NULL,
        affected_npi_stage_oid bigint,
        evidence_target_oid bigint,
        profile_target_oid bigint,
        current_source_vector_hash varchar(64),
        desired_source_vector_hash varchar(64),
        current_source_context_vector_hash varchar(64),
        desired_source_context_vector_hash varchar(64),
        capacity_geometry_status varchar(32) NOT NULL,
        capacity_geometry_hash varchar(64),
        capacity_geometry_json jsonb,
        cutover_forecast_status varchar(32) NOT NULL DEFAULT 'not_started',
        cutover_forecast_hash varchar(64),
        cutover_forecast_json jsonb,
        profile_as_of varchar(10) NOT NULL,
        evidence_next_batch integer NOT NULL,
        evidence_total_batches integer NOT NULL,
        profile_next_batch integer NOT NULL,
        profile_total_batches integer NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
    );
    """


def _serving_generation_table_sql(schema: str) -> str:
    """Return the serving-generation truth-table DDL."""
    table_ref = profile.qualified_table(
        schema, "provider_directory_profile_serving_generation"
    )
    return f"""
    CREATE TABLE {table_ref} (
        singleton_key varchar(16) PRIMARY KEY,
        status varchar(16) NOT NULL,
        operation varchar(16) NOT NULL,
        control_generation bigint NOT NULL,
        generation_id varchar(64) NOT NULL,
        selection_proof_id varchar(64) NOT NULL,
        authority_revision bigint NOT NULL,
        profile_schema_version integer NOT NULL,
        profile_strategy_version varchar(128) NOT NULL,
        source_vector_hash varchar(64) NOT NULL,
        source_vector_json jsonb NOT NULL,
        source_context_vector_hash varchar(64) NOT NULL,
        source_context_vector_json jsonb NOT NULL,
        executable_plan_hash varchar(64) NOT NULL,
        capacity_geometry_status varchar(32) NOT NULL,
        capacity_geometry_hash varchar(64),
        capacity_geometry_json jsonb,
        cutover_forecast_hash varchar(64),
        evidence_target_oid bigint NOT NULL,
        profile_target_oid bigint NOT NULL,
        evidence_rows bigint NOT NULL,
        profile_rows bigint NOT NULL,
        profile_as_of varchar(10) NOT NULL,
        published_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL
    );
    """


def _delta_receipt_table_sql(schema: str) -> str:
    """Return the immutable delta-receipt truth-table DDL."""
    table_ref = profile.qualified_table(
        schema, "provider_directory_profile_delta_receipt"
    )
    return f"""
    CREATE TABLE {table_ref} (
        build_id varchar(64) PRIMARY KEY,
        executable_plan_hash varchar(64) NOT NULL,
        from_capacity_geometry_status varchar(32) NOT NULL,
        from_capacity_geometry_hash varchar(64),
        from_capacity_geometry_json jsonb,
        capacity_geometry_status varchar(32) NOT NULL,
        capacity_geometry_hash varchar(64) NOT NULL,
        capacity_geometry_json jsonb NOT NULL,
        from_source_vector_hash varchar(64) NOT NULL,
        to_source_vector_hash varchar(64) NOT NULL,
        from_source_context_vector_hash varchar(64) NOT NULL,
        to_source_context_vector_hash varchar(64) NOT NULL,
        from_generation_id varchar(64) NOT NULL,
        generation_id varchar(64) NOT NULL,
        operation varchar(16) NOT NULL,
        profile_as_of varchar(10) NOT NULL,
        selection_proof_id varchar(64) NOT NULL,
        control_generation bigint NOT NULL,
        authority_revision bigint NOT NULL,
        evidence_target_oid bigint NOT NULL,
        profile_target_oid bigint NOT NULL,
        evidence_rows bigint NOT NULL,
        profile_rows bigint NOT NULL,
        evidence_inserted bigint NOT NULL,
        evidence_deleted bigint NOT NULL,
        profile_inserted bigint NOT NULL,
        profile_deleted bigint NOT NULL,
        cutover_forecast_hash varchar(64),
        cutover_forecast_json jsonb,
        cutover_actual_hash varchar(64),
        cutover_actual_json jsonb,
        cutover_wal_start_lsn varchar(64),
        cutover_wal_observed_lsn varchar(64),
        cutover_wal_bytes bigint,
        evidence_target_bytes_before bigint,
        evidence_target_bytes_after bigint,
        evidence_target_growth_bytes bigint,
        profile_target_bytes_before bigint,
        profile_target_bytes_after bigint,
        profile_target_growth_bytes bigint,
        committed_at timestamptz NOT NULL
    );
    """


async def _create_delta_contract_tables(
    database: Database,
    schema: str,
    *,
    evidence_stage: str,
    profile_stage: str,
    affected_stage: str,
) -> None:
    """Create the bounded delta targets, scratch, control, and guard tables."""
    await _create_delta_profile_tables(
        database, schema, evidence_stage, profile_stage, affected_stage
    )
    await _create_delta_control_stubs(database, schema)
    await database.status(_checkpoint_table_sql(schema))
    await database.status(_serving_generation_table_sql(schema))
    await database.status(_delta_receipt_table_sql(schema))
    receipt_ref = profile.qualified_table(
        schema, "provider_directory_profile_delta_receipt"
    )
    guard_ref = profile.qualified_table(
        schema, "provider_directory_profile_delta_receipt_immutable_test"
    )
    for statement in _immutable_guard_statements(
        receipt_ref,
        guard_ref,
        error_message="provider_directory_profile_delta_receipt_immutable",
        trigger_prefix="provider_directory_profile_delta_receipt",
    ):
        await database.status(statement)
