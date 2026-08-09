# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable catalog-upsert parity for an activated reviewed source."""

from __future__ import annotations

import importlib
import json

from sqlalchemy import Column, DateTime, MetaData, String, Table, Text, cast
from sqlalchemy import func, literal
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from db.models.system import ProviderDirectorySource
from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    PENDING_STATUS,
    VERIFIED_STATUS,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


def _source_table(schema_name: str) -> Table:
    """Model the reduced PostgreSQL fixture for expression compilation."""

    return Table(
        "provider_directory_source",
        MetaData(),
        Column("source_id", String, primary_key=True),
        Column("endpoint_id", String),
        Column("canonical_api_base", Text),
        Column("metadata_json", JSONB),
        Column("updated_at", DateTime),
        schema=schema_name,
    )


def _incoming_metadata(note: str) -> dict[str, object]:
    return {
        "provider_directory_candidate_status": PENDING_STATUS,
        ACTIVATION_METADATA_KEY: {"spoofed": True},
        "ordinary_catalog_note": note,
    }


def _values_upsert_sql(
    scenario,
    *,
    note: str,
    canonical_api_base: str = "https://directory.example.test/fhir",
) -> str:
    """Compile the production SQLAlchemy VALUES-upsert expression."""

    importer = importlib.import_module("process.provider_directory_fhir")
    source_table = _source_table(scenario.schema)
    statement = pg_insert(source_table).values(
        source_id="synthetic-source",
        endpoint_id="endpoint-a",
        canonical_api_base=canonical_api_base,
        metadata_json=cast(
            literal(json.dumps(_incoming_metadata(note), sort_keys=True)),
            JSONB,
        ),
        updated_at=func.transaction_timestamp(),
    )
    metadata_expression = importer._effective_update_expression(
        source_table,
        statement,
        "metadata_json",
    )
    statement = statement.on_conflict_do_update(
        index_elements=[source_table.c.source_id],
        set_={
            "canonical_api_base": statement.excluded.canonical_api_base,
            "metadata_json": metadata_expression,
            "updated_at": func.transaction_timestamp(),
        },
    )
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def _copy_upsert_sql(scenario) -> str:
    """Render the production COPY-conflict metadata expression."""

    importer = importlib.import_module("process.provider_directory_fhir")
    metadata_expression = importer._effective_update_sql(
        ProviderDirectorySource.__table__,
        "metadata_json",
        target_prefix="provider_directory_source",
        incoming_prefix="EXCLUDED",
    )
    return f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            metadata_json, updated_at
        ) VALUES (
            'synthetic-source', 'endpoint-a',
            'https://directory.example.test/fhir',
            $1::jsonb, pg_catalog.transaction_timestamp()
        )
        ON CONFLICT (source_id) DO UPDATE
            SET metadata_json = {metadata_expression},
                updated_at = pg_catalog.transaction_timestamp()
    """


async def _assert_marker_and_note(
    scenario,
    marker_by_field,
    expected_note: str,
) -> None:
    source_state = await scenario.connection.fetchrow(
        f"""
        SELECT metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' AS status,
               metadata_json::jsonb -> '{ACTIVATION_METADATA_KEY}' AS marker,
               metadata_json::jsonb ->> 'ordinary_catalog_note' AS note
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    assert source_state["status"] == VERIFIED_STATUS
    assert json.loads(source_state["marker"]) == marker_by_field
    assert source_state["note"] == expected_note


async def prove_catalog_upserts_preserve_activation(
    scenario,
    marker_by_field,
) -> None:
    """Execute both normal upsert expressions and reject contract drift."""

    await scenario.connection.execute(
        _values_upsert_sql(scenario, note="values-path")
    )
    await _assert_marker_and_note(scenario, marker_by_field, "values-path")
    await scenario.connection.execute(
        _copy_upsert_sql(scenario),
        json.dumps(_incoming_metadata("copy-path"), sort_keys=True),
    )
    await _assert_marker_and_note(scenario, marker_by_field, "copy-path")
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        _values_upsert_sql(
            scenario,
            note="drifted-path",
            canonical_api_base="https://drift.example.test/fhir",
        ),
    )
    await _assert_marker_and_note(scenario, marker_by_field, "copy-path")
